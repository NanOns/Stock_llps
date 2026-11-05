"""Stock A命令行source_diagnostics职责模块。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import math
import os
from pathlib import Path
import stat
import time
from typing import Any
from stock_a.contracts import (
    PriceProfileContractError,
    load_price_profile_contract,
)
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.actions import (
    GBBQ_PROTOCOL_VERSION,
    GBBQ_UNIT_VERSION,
    GbbqDecodeError,
    decode_gbbq_bytes,
)
from stock_a.sources.tdx.day_source import open_day_file, read_protected_bytes
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.storage.postgres.identity import inspect_postgres
from stock_a.storage.postgres.binding import (
    BindingCommittedConfigError,
    BindingRefused,
    bind_stock_namespace,
    registered_migration_ids,
    registered_task_scopes,
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
    _configuration_snapshot,
    _emit,
    _module_mode,
    _safe_child,
    _safe_file,
    _source_file_size,
)


def _collect_source_phase0(args: argparse.Namespace) -> dict[str, Any]:
    package = _module_mode()
    config, config_path = _load_runtime_config()
    roots = config["source_policy"]["configured_tdx_source_roots"]
    if not roots:
        raise DiagnosticError(
            "SOURCE_NOT_CONFIGURED",
            "配置中没有绑定TDX来源保护根，阶段0在目录枚举前拒绝",
        )
    if len(roots) != 1:
        raise DiagnosticError(
            "SOURCE_ROOT_AMBIGUOUS", "阶段0要求一个明确绑定的本地TDX来源根"
        )
    if (
        type(args.max_entries) is not int
        or args.max_entries <= 0
        or type(args.max_file_bytes) is not int
        or args.max_file_bytes <= 0
        or type(args.max_total_bytes) is not int
        or args.max_total_bytes <= 0
        or type(args.max_records) is not int
        or args.max_records <= 0
    ):
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID",
            "阶段0目录、字节与记录预算必须是拒绝布尔值的正整数",
            _EXIT_ARGUMENT,
        )
    try:
        seconds = Decimal(args.max_seconds)
        if not seconds.is_finite() or seconds <= 0:
            raise ValueError
    except (InvalidOperation, ValueError) as error:
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID", "阶段0耗时预算必须是有限正数", _EXIT_ARGUMENT
        ) from error
    started = time.monotonic()
    try:
        root = Path(roots[0]).resolve(strict=True)
        root_mode = root.stat().st_mode
    except FileNotFoundError as error:
        raise DiagnosticError("SOURCE_PATH_INVALID", "已绑定TDX来源根不存在") from error
    except PermissionError as error:
        raise DiagnosticError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝核验已绑定来源根", _EXIT_PERMISSION
        ) from error
    except OSError as error:
        raise DiagnosticError(
            "SOURCE_IO_ERROR", "核验已绑定来源根时发生工程I/O错误", _EXIT_ENGINEERING
        ) from error
    if not stat.S_ISDIR(root_mode):
        raise DiagnosticError("SOURCE_PATH_INVALID", "已绑定TDX来源根不是目录")
    sample_rule = "对sh/sz/bj协议目录分别按文件名排序，完整只读扫描首个.day文件；每个目录最多枚举max_entries项"
    markets: dict[str, dict[str, Any]] = {}
    total_bytes = 0
    total_records = 0
    for market in ("sh", "sz", "bj"):
        optional_market = market == "bj"
        directory = _safe_child(
            root / "vipdoc" / market / "lday", root, optional=optional_market
        )
        if directory is None:
            markets[market] = {
                "directory": str(root / "vipdoc" / market / "lday"),
                "day_file_count": None,
                "enumerated_entry_count": 0,
                "excluded_link_count": 0,
                "scope_complete": False,
                "market_status": "OPTIONAL_DEFERRED",
                "reason": "BJ_DIRECTORY_UNAVAILABLE_OR_INVALID",
                "sample_rule": "北交所目录未通过核验，沪深结果保留",
                "sample": None,
            }
            continue
        names: list[str] = []
        entry_count = 0
        excluded_links = 0
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if time.monotonic() - started > float(seconds):
                        raise DiagnosticError(
                            "READ_TIMEOUT",
                            "来源目录枚举超过耗时预算",
                            _EXIT_UNAVAILABLE,
                        )
                    entry_count += 1
                    if entry_count > args.max_entries:
                        raise DiagnosticError(
                            "READ_LIMIT_EXCEEDED",
                            "行情目录项数超过预算，不能将截断枚举报告为完整",
                            _EXIT_UNAVAILABLE,
                        )
                    if entry.is_symlink():
                        excluded_links += 1
                        continue
                    if entry.is_file(
                        follow_symlinks=False
                    ) and entry.name.lower().endswith(".day"):
                        names.append(entry.name)
        except PermissionError as error:
            if optional_market:
                markets[market] = {
                    "directory": str(directory),
                    "day_file_count": None,
                    "enumerated_entry_count": entry_count,
                    "excluded_link_count": excluded_links,
                    "scope_complete": False,
                    "market_status": "OPTIONAL_DEFERRED",
                    "reason": "SOURCE_ACCESS_DENIED",
                    "sample_rule": "北交所目录读取失败，沪深结果保留",
                    "sample": None,
                }
                continue
            raise DiagnosticError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝枚举行情目录", _EXIT_PERMISSION
            ) from error
        except OSError as error:
            if optional_market:
                markets[market] = {
                    "directory": str(directory),
                    "day_file_count": None,
                    "enumerated_entry_count": entry_count,
                    "excluded_link_count": excluded_links,
                    "scope_complete": False,
                    "market_status": "OPTIONAL_DEFERRED",
                    "reason": "SOURCE_IO_ERROR",
                    "sample_rule": "北交所目录读取失败，沪深结果保留",
                    "sample": None,
                }
                continue
            raise DiagnosticError(
                "SOURCE_IO_ERROR", "枚举行情目录时发生工程I/O错误", _EXIT_ENGINEERING
            ) from error
        except DiagnosticError as error:
            if not optional_market:
                raise
            markets[market] = {
                "directory": str(directory),
                "day_file_count": None,
                "enumerated_entry_count": entry_count,
                "excluded_link_count": excluded_links,
                "scope_complete": False,
                "market_status": "OPTIONAL_DEFERRED",
                "reason": error.code,
                "sample_rule": "北交所目录枚举预算未完成，沪深结果保留",
                "sample": None,
            }
            continue
        names.sort(key=str.casefold)
        market_result: dict[str, Any] = {
            "directory": str(directory),
            "day_file_count": len(names),
            "enumerated_entry_count": entry_count,
            "excluded_link_count": excluded_links,
            "scope_complete": True,
            "market_status": "ENUMERATED",
            "sample_rule": "字典序首个.day文件，读取整个文件",
            "sample": None,
        }
        if names:
            try:
                sample_path = _safe_file(
                    root / "vipdoc" / market / "lday" / names[0],
                    root,
                    optional=optional_market,
                )
                if sample_path is None:
                    raise DiagnosticError("SOURCE_NOT_FOUND", "北交所样本在枚举后消失")
                size = _source_file_size(sample_path)
            except DiagnosticError as error:
                if not optional_market:
                    raise
                market_result.update(
                    {
                        "market_status": "OPTIONAL_DEFERRED",
                        "reason": error.code,
                        "sample": None,
                    }
                )
                markets[market] = market_result
                continue
            if size > args.max_file_bytes or total_bytes + size > args.max_total_bytes:
                if optional_market:
                    market_result.update(
                        {
                            "market_status": "OPTIONAL_DEFERRED",
                            "reason": "READ_LIMIT_EXCEEDED",
                            "sample": None,
                        }
                    )
                    markets[market] = market_result
                    continue
                raise DiagnosticError(
                    "READ_LIMIT_EXCEEDED",
                    "日线样本超过本次文件或总字节预算",
                    _EXIT_UNAVAILABLE,
                )
            units = UnitContract("TDX_RAW_DIAGNOSTIC_ONLY", Decimal(1), 1)
            remaining_total = args.max_total_bytes - total_bytes
            if remaining_total <= 0:
                if optional_market:
                    market_result.update(
                        {
                            "market_status": "OPTIONAL_DEFERRED",
                            "reason": "READ_LIMIT_EXCEEDED",
                            "sample": None,
                        }
                    )
                    markets[market] = market_result
                    continue
                raise DiagnosticError(
                    "READ_LIMIT_EXCEEDED",
                    "日线样本没有剩余总字节预算",
                    _EXIT_UNAVAILABLE,
                )
            limits = ReadLimits(
                min(args.max_file_bytes, remaining_total),
                args.max_records,
                1024,
                seconds,
            )
            try:
                with open_day_file(sample_path, roots, units, limits) as scan:
                    for _record in scan:
                        if time.monotonic() - started > float(seconds):
                            raise DayReadError("READ_TIMEOUT", "BUDGET")
                    summary = scan.summary
            except DayReadError as error:
                if optional_market:
                    market_result.update(
                        {
                            "market_status": "OPTIONAL_DEFERRED",
                            "reason": error.code,
                            "sample": None,
                        }
                    )
                    markets[market] = market_result
                    continue
                code = error.code
                exit_code = _READ_ERROR_EXIT_CODES.get(code, _EXIT_ENGINEERING)
                raise DiagnosticError(
                    code, f"日线真实样本未能完整核验：{error}", exit_code
                ) from error
            total_bytes += summary.scan.byte_count
            total_records += summary.scan.record_count
            market_result["sample"] = {
                "path": str(sample_path),
                "raw_byte_sha256": summary.scan.raw_byte_sha256,
                "byte_count": summary.scan.byte_count,
                "record_count": summary.scan.record_count,
                "first_date": summary.scan.first_date.isoformat(),
                "last_date": summary.scan.last_date.isoformat(),
                "source_identity": asdict(summary.source),
                "read_started_at": summary.scan.read_started_at.isoformat(),
                "read_completed_at": summary.scan.read_completed_at.isoformat(),
                "price_diagnostic_scale": "raw_price_integer/100 per existing decoder; candidate protocol observation only",
                "amount_unit": "UNVERIFIED_RAW_FLOAT32",
                "volume_unit": "UNVERIFIED_RAW_UINT32",
                "field_quality": {
                    name: asdict(getattr(summary.scan, name))
                    for name in ("prices", "amount", "volume")
                },
            }
            market_result["market_status"] = "SAMPLED"
        else:
            market_result["market_status"] = "NO_DAY_FILES"
        markets[market] = market_result

    capabilities: list[dict[str, Any]] = [
        {
            "capability": "TDX_DAILY_RAW",
            "dataset_id": "TDX_DAILY_BARS",
            "version": "TDX_DAY_RECORD_32B_CANDIDATE",
            "source_authority": "UNVERIFIED_LOCAL_TDX_INSTALLATION",
            "producer": "stock_a.sources.tdx.day.decode_day_record + day_stream.DayRecordScan",
            "field_contract": [
                {
                    "field": "trade_date",
                    "unit": "YYYYMMDD calendar date",
                    "time_role": "trade_date only; not publication or availability",
                    "quality": "date validation in existing parser",
                    "consumer_admission": "DENY_UNTIL_OFFICIAL_CALENDAR",
                },
                {
                    "field": "open/high/low/close",
                    "unit": "candidate raw price integer / 100",
                    "time_role": "bar trade_date",
                    "quality": "group validation counts included per sample",
                    "consumer_admission": "DENY_NORMALIZED_CORE_PENDING_SOURCE_AND_UNIT_AUTHORITY",
                },
                {
                    "field": "amount",
                    "unit": "UNVERIFIED_RAW_FLOAT32",
                    "time_role": "bar trade_date",
                    "quality": "finite/nonnegative group validation counts included per sample",
                    "consumer_admission": "DENY_NORMALIZED_CORE",
                },
                {
                    "field": "volume",
                    "unit": "UNVERIFIED_RAW_UINT32",
                    "time_role": "bar trade_date",
                    "quality": "nonnegative integer group validation counts included per sample",
                    "consumer_admission": "DENY_NORMALIZED_CORE",
                },
            ],
            "clock_contract": {
                "observed_at": "read_started_at/read_completed_at with local timezone per sample",
                "available_at": "UNKNOWN",
                "published_at": "UNKNOWN",
                "captured_at": "this local CLI read observation only; not historical first capture",
            },
            "required": True,
            "status": "BLOCKED",
            "reason": "真实文件可读，但仅每市场一个样本；成交额/成交量单位及其来源权威未独立成立，未注册规范化消费者",
            "sample_scope": sample_rule,
            "consumer_admission": "DENY_NORMALIZED_CORE",
        }
    ]

    action_path = _safe_file(root / "T0002" / "hq_cache" / "gbbq", root, optional=True)
    action_result: dict[str, Any] = {
        "status": "UNAVAILABLE",
        "reason": "来源文件不存在",
    }
    if action_path is not None:
        action_size = _source_file_size(action_path)
        remaining_total = args.max_total_bytes - total_bytes
        if (
            remaining_total <= 0
            or action_size > args.max_file_bytes
            or action_size > remaining_total
        ):
            raise DiagnosticError(
                "READ_LIMIT_EXCEEDED",
                "GBBQ超过本次文件或总字节预算，未生成完成摘要",
                _EXIT_UNAVAILABLE,
            )
        try:
            raw, identity = read_protected_bytes(
                action_path,
                roots,
                max_bytes=min(args.max_file_bytes, remaining_total),
                max_seconds=float(seconds),
            )
        except DayReadError as error:
            exit_code = _READ_ERROR_EXIT_CODES.get(error.code, _EXIT_ENGINEERING)
            raise DiagnosticError(
                error.code, f"GBBQ真实文件未能完整核验：{error}", exit_code
            ) from error
        total_bytes += len(raw)
        try:
            parsed = decode_gbbq_bytes(
                raw,
                source_ref="TDX_LOCAL_GBBQ_CANDIDATE",
                protocol_version=GBBQ_PROTOCOL_VERSION,
                unit_version=GBBQ_UNIT_VERSION,
                max_bytes=args.max_file_bytes,
                max_records=args.max_records,
            )
        except GbbqDecodeError as error:
            action_result = {
                "status": "BLOCKED",
                "reason": "GBBQ内容合同未闭合",
                "error_code": error.code,
                "raw_byte_sha256": hashlib.sha256(raw).hexdigest(),
                "byte_count": len(raw),
                "handle_path": identity.handle_path,
            }
        else:
            categories: dict[str, int] = {}
            for record in parsed.records:
                categories[record.category_name] = (
                    categories.get(record.category_name, 0) + 1
                )
            action_result = {
                "status": "DEGRADED_PASS",
                "path": str(action_path),
                "handle_path": identity.handle_path,
                "raw_byte_sha256": hashlib.sha256(raw).hexdigest(),
                "byte_count": len(raw),
                "record_count": parsed.parsed_count,
                "category_counts": categories,
                "protocol_version": parsed.protocol_version,
                "unit_version": parsed.unit_version,
                "producer": "stock_a.sources.tdx.actions.decode_gbbq_bytes",
                "source_authority": "UNVERIFIED_LOCAL_TDX_INSTALLATION",
                "identity_status": "SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY",
                "pit_status": "NOT_ASSERTED_LOCAL_READ_CLOCK_ONLY",
                "consumer_admission": "CATEGORY_1_CANDIDATE_ONLY_NO_PIT_OR_OFFICIAL_IDENTITY",
            }
    capabilities.append(
        {
            "capability": "TDX_GBBQ_CORPORATE_ACTIONS",
            "required": False,
            "status": action_result["status"],
            "reason": "本地字节可解析但不构成身份或PIT证明"
            if action_result["status"] == "DEGRADED_PASS"
            else action_result.get("reason"),
            "evidence": action_result,
        }
    )

    classification_candidates = []
    for relative in ("T0002/hq_cache/tdxhy.cfg", "T0002/hq_cache/infoharbor_block.dat"):
        candidate = _safe_file(root.joinpath(*relative.split("/")), root, optional=True)
        if candidate is not None:
            classification_candidates.append(
                {"path": str(candidate), "byte_count": _source_file_size(candidate)}
            )
    identity_candidates = []
    for relative in (
        "T0002/hq_cache/ds_stk.dat",
        "T0002/hq_cache/base.dbf",
        "T0002/hq_cache/code2name.ini",
    ):
        candidate = _safe_file(root.joinpath(*relative.split("/")), root, optional=True)
        if candidate is not None:
            identity_candidates.append(
                {"path": str(candidate), "byte_count": _source_file_size(candidate)}
            )
    capabilities.extend(
        [
            {
                "capability": "TDX_INDUSTRY_CONCEPT_MEMBERSHIP",
                "required": False,
                "status": "BLOCKED",
                "reason": "分类候选文件存在但未有新项目解析/有效时钟合同；U03解析内核待后续身份与成员任务接入",
                "candidates": classification_candidates,
                "consumer_admission": "DENY",
            },
            {
                "capability": "SECURITY_ENTITY_AND_LIFECYCLE",
                "required": True,
                "status": "BLOCKED",
                "reason": "本地目录/代码/首条日线不是官方稳定实体、上市/退市或换码证明；U06候选未在本目标引入",
                "candidates": identity_candidates,
                "consumer_admission": "DENY_UNMAPPED",
            },
            {
                "capability": "OFFICIAL_TRADING_CALENDAR_AND_STATUS",
                "required": True,
                "status": "BLOCKED",
                "reason": "未识别并验证版本化交易所日历或日期有效状态来源；禁止由连续价格或缺bar推断",
                "consumer_admission": "DENY_UNKNOWN_GAPS",
            },
            {
                "capability": "BAOSTOCK_SUPPLEMENTAL",
                "required": False,
                "status": "UNAVAILABLE",
                "reason": "本任务禁止联网或调用第三方接口，未验证逐字段覆盖与单位",
                "consumer_admission": "DENY_SUPPLEMENTAL_ONLY",
            },
        ]
    )
    required_blocked = any(
        item["required"] and item["status"] not in ("FULL_PASS", "DEGRADED_PASS")
        for item in capabilities
    )
    overall = (
        "BLOCKED"
        if required_blocked
        else (
            "DEGRADED_PASS"
            if any(item["status"] != "FULL_PASS" for item in capabilities)
            else "FULL_PASS"
        )
    )
    elapsed = time.monotonic() - started
    return {
        "ok": overall != "BLOCKED",
        "status": overall,
        "message": "已按声明预算完成本地只读阶段0核验；BLOCKED能力不进入scanner/消费者，观测时钟不代表available_at、published_at、封口或PIT",
        "package": package,
        "configuration_path": str(config_path),
        "source_root": str(root),
        "source_root_final_path": str(root),
        "observed_at": datetime.now().astimezone().isoformat(),
        "clock_role": "LOCAL_OBSERVED_AT_ONLY",
        "sample_rule": sample_rule,
        "market_directories": markets,
        "capabilities": capabilities,
        "coverage": {
            "known_market_day_file_count": sum(
                item["day_file_count"]
                for item in markets.values()
                if type(item.get("day_file_count")) is int
            ),
            "market_day_file_count_complete": all(
                item.get("scope_complete") is True for item in markets.values()
            ),
            "markets_with_unknown_day_file_count": [
                market
                for market, item in markets.items()
                if type(item.get("day_file_count")) is not int
            ],
            "sampled_day_files": sum(
                item["sample"] is not None for item in markets.values()
            ),
            "sampled_records": total_records,
            "sampled_source_bytes": total_bytes,
        },
        "budgets": {
            "max_entries_per_market_directory": args.max_entries,
            "max_file_bytes": args.max_file_bytes,
            "max_total_bytes": args.max_total_bytes,
            "max_records_per_file": args.max_records,
            "max_seconds": str(seconds),
            "elapsed_seconds": round(elapsed, 6),
            "elapsed_budget_exceeded": elapsed > float(seconds),
        },
        "business_admission": "DENY" if overall == "BLOCKED" else "SCOPED_ONLY",
        "database_connected": False,
        "production_switched": False,
    }


def _run_source_phase0(args: argparse.Namespace) -> int:
    result = _collect_source_phase0(args)
    _emit(result, error=result.get("status") == "BLOCKED")
    return _EXIT_UNAVAILABLE if result.get("status") == "BLOCKED" else _EXIT_OK


def _run_doctor(args: argparse.Namespace) -> int:
    package = _module_mode()
    config, config_path = _load_runtime_config(args.config)
    snapshot = _configuration_snapshot(config, config_path)
    try:
        loaded_contract = load_price_profile_contract(
            config["paths"]["configuration_root"]
        )
    except PriceProfileContractError as error:
        raise DiagnosticError(error.code, str(error)) from error
    contract_document = loaded_contract.document
    _emit(
        {
            "ok": False,
            "status": "LOCAL_DIAGNOSTIC_ONLY",
            "message": "本地包与配置结构已检查；未核验真实来源或数据库连接，不允许据此启动业务",
            "package": package,
            **snapshot,
            "price_profile_semantics": {
                "contract_id": contract_document["contract_id"],
                "version": contract_document["version"],
                "assembled_sha256": loaded_contract.assembled_sha256,
                "module_count": len(loaded_contract.module_paths),
                "module_paths": list(loaded_contract.module_paths),
                "field_count": len(contract_document["field_registry"]),
                "core_field_count": sum(
                    item["field_id"].startswith("core.")
                    for item in contract_document["field_registry"]
                ),
                "profile_expression_count": len(
                    contract_document["profile_expression_registry"]
                ),
            },
            "business_start_permitted": False,
            "database_connection_attempted": False,
        }
    )
    return _EXIT_UNAVAILABLE


def _run_pg_identity(args: argparse.Namespace) -> int:
    if (
        type(args.timeout_seconds) not in (int, float)
        or not math.isfinite(args.timeout_seconds)
        or not 0 < args.timeout_seconds <= 30
    ):
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID",
            "PG核验超时必须大于0且不超过30秒",
            _EXIT_ARGUMENT,
        )
    package = _module_mode()
    config, config_path = _load_runtime_config()
    result = inspect_postgres(config, timeout_seconds=args.timeout_seconds)
    result.update(
        {
            "ok": result.get("status") == "DB_MIGRATION_READY",
            "message": "PG只读连接、账号迁移身份与物理存储核验状态分别返回；数据库状态不代表业务流水线已完成",
            "package": package,
            "configuration_path": str(config_path),
            "business_write_admission": "DENY_GENERAL_WRITE_AND_PUBLISH",
            "task_scoped_migration_admission": "REQUIRES_EXPLICIT_TASK_SCOPE",
            "production_switched": False,
        }
    )
    _emit(result, error=result.get("status") != "DB_MIGRATION_READY")
    if result.get("status") == "INVALID_ARGUMENT":
        return _EXIT_ARGUMENT
    if result.get("status") == "DB_MIGRATION_READY":
        return _EXIT_OK
    if result.get("status") in ("PERMISSION_DENIED", "AUTHENTICATION_FAILED"):
        return _EXIT_PERMISSION
    if result.get("status") == "ENGINEERING_ERROR":
        return _EXIT_ENGINEERING
    return _EXIT_UNAVAILABLE


def _run_pg_bind(args: argparse.Namespace) -> int:
    if (
        type(args.timeout_seconds) not in (int, float)
        or not math.isfinite(args.timeout_seconds)
        or not 0 < args.timeout_seconds <= 30
    ):
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID",
            "PG绑定超时必须大于0且不超过30秒",
            _EXIT_ARGUMENT,
        )
    package = _module_mode()
    if package.get("mode") != "DEV":
        raise DiagnosticError(
            "BIND_MODE_REJECTED",
            "固定身份迁移仅允许从批准源码包的显式开发CLI执行",
            _EXIT_PERMISSION,
        )
    config, config_path = _load_runtime_config()
    try:
        result = bind_stock_namespace(
            config,
            config_path,
            task_scope=args.task_scope,
            migration_id=args.migration_id,
            timeout_seconds=args.timeout_seconds,
        )
    except BindingCommittedConfigError as error:
        _emit(
            {
                "ok": False,
                "status": "CONFIG_SYNC_REQUIRED",
                "code": "PG_BIND_CONFIG_SYNC_REQUIRED",
                "message": str(error),
                "business_write_admission": "DENY",
                "task_scope": args.task_scope,
                "migration_id": args.migration_id,
                "database_transaction_outcome": "COMMITTED",
                "database_write_attempted": "YES",
                "configuration_path": str(config_path),
                "production_switched": False,
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    except BindingRefused as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "PG_BIND_REFUSED",
                "message": str(error),
                "business_write_admission": "DENY",
                "task_scope": args.task_scope,
                "migration_id": args.migration_id,
                "database_transaction_outcome": "ROLLED_BACK_OR_NOT_STARTED",
                "database_write_attempted": "NO_COMMITTED_CHANGE",
                "configuration_path": str(config_path),
                "production_switched": False,
            },
            error=True,
        )
        return _EXIT_PERMISSION
    result.update(
        {
            "ok": result.get("status") == "BOUND",
            "package": package,
            "configuration_path": str(config_path),
            "production_switched": False,
        }
    )
    _emit(result)
    return _EXIT_OK
