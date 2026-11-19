"""Stock A命令行source_diagnostics职责模块。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import math
from stock_a.contracts import (
    PriceProfileContractError,
    load_price_profile_contract,
)
from stock_a.storage.postgres.identity import inspect_postgres
from stock_a.storage.postgres.binding import (
    BindingCommittedConfigError,
    BindingRefused,
    apply_stock_migration,
    registered_migration_ids,
    registered_task_scopes,
)
from stock_a.application.runtime_config import (
    DiagnosticError,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_PERMISSION,
    _EXIT_UNAVAILABLE,
    load_runtime_config as _load_runtime_config,
)

from stock_a.application.cli_commands.common import (
    _configuration_snapshot,
    _emit,
    _module_mode,
)


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
                "contract_version": loaded_contract.document["version"],
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
    max_timeout_seconds = 14400 if args.migration_id == 22 else 900
    if (
        type(args.timeout_seconds) not in (int, float)
        or not math.isfinite(args.timeout_seconds)
        or not 0 < args.timeout_seconds <= max_timeout_seconds
    ):
        raise DiagnosticError(
            "ARGUMENT_CONTRACT_INVALID",
            f"集中PG迁移超时必须大于0且不超过{max_timeout_seconds}秒",
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
        result = apply_stock_migration(
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
