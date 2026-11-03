"""Stock A命令行source_ingress职责模块。"""

from __future__ import annotations

import argparse
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
from stock_a.application.source_set_candidate import (
    SourceSetCandidateApplicationError,
    _official_calendar,
    persist_source_set_candidate,
    prepare_source_set_candidate,
    read_source_set_candidate,
)
from stock_a.storage.postgres.source_set_candidate import SourceSetCandidatePgError
from stock_a.application.source_ingress import (
    SourceIngressError,
    build_source_ingress_plan,
    persist_formal_source_set,
)
from stock_a.storage.postgres.source_set_writer import SourceSetWriteError
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
from stock_a.application.cli_commands.source.source_diagnostics import (
    _collect_source_phase0,
)


def _run_source_set_candidate(
    args: argparse.Namespace, *, compute_candidate: bool
) -> int:
    config, config_path = _load_runtime_config()
    target_session = _parse_source_set_date(args.target_session)
    try:
        if compute_candidate and getattr(args, "persist", False):
            result = persist_source_set_candidate(
                config,
                target_session=target_session,
                package_sha256=args.package_sha256,
            )
        elif compute_candidate:
            result = prepare_source_set_candidate(
                config,
                target_session=target_session,
                package_sha256=args.package_sha256,
            )
        else:
            result = read_source_set_candidate(
                config,
                target_session=target_session,
                package_sha256=args.package_sha256,
            )
    except (
        SourceSetCandidateApplicationError,
        SourceSetCandidatePgError,
        SourceIngressError,
    ) as error:
        code = getattr(error, "code", "SOURCE_SET_CANDIDATE_REJECTED")
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": code,
                "message": str(error),
                "database_write_attempted": False,
                "dml_executed": False,
                "ddl_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if code.endswith("INVALID") else _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": result.get("status") not in {"NO_MATCH", "REJECTED"},
            "analysis_scope": "P03_SOURCE_SET_CANDIDATE_NON_PIT",
            "configuration_path": str(config_path),
            "database_write": bool(result.get("dml_executed", False)),
            "formal_p03_complete": False,
        }
    )
    _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE


def _run_source_ingress_plan(args: argparse.Namespace) -> int:
    config, config_path = _load_runtime_config()
    target_session = _parse_source_set_date(args.target_session)
    phase0_result = None
    if not args.skip_phase0_scan:
        phase0_result = _collect_source_phase0(
            argparse.Namespace(
                max_entries=args.max_entries,
                max_file_bytes=args.max_file_bytes,
                max_total_bytes=args.max_total_bytes,
                max_records=args.max_records,
                max_seconds=args.max_seconds,
            )
        )
    try:
        result = build_source_ingress_plan(
            config,
            target_session=target_session,
            package_sha256=args.package_sha256,
            phase0_result=phase0_result,
        )
    except SourceIngressError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "SOURCE_INGRESS_REJECTED",
                "message": str(error),
                "database_write_attempted": False,
                "network_accessed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result["configuration_path"] = str(config_path)
    _emit(result)
    return (
        _EXIT_OK
        if result["environment_gate"]["status"] == "READY"
        else _EXIT_UNAVAILABLE
    )


def _run_source_p03_write(args: argparse.Namespace) -> int:
    config, config_path = _load_runtime_config()
    target_session = _parse_source_set_date(args.target_session)
    phase0_result = _collect_source_phase0(
        argparse.Namespace(
            max_entries=args.max_entries,
            max_file_bytes=args.max_file_bytes,
            max_total_bytes=args.max_total_bytes,
            max_records=args.max_records,
            max_seconds=args.max_seconds,
        )
    )
    try:
        result = persist_formal_source_set(
            config,
            target_session=target_session,
            package_sha256=args.package_sha256,
            phase0_result=phase0_result,
        )
    except (
        SourceIngressError,
        SourceSetWriteError,
        RawSourcePgError,
        SourceSetCandidatePgError,
    ) as error:
        _emit(
            {
                "ok": False,
                "status": "BLOCKED"
                if isinstance(error, SourceIngressError)
                else "FAILED",
                "code": getattr(error, "code", "SOURCE_P03_WRITE_FAILED"),
                "message": str(error),
                "target_session": target_session.isoformat(),
                "package_sha256": args.package_sha256,
                "phase0_status": phase0_result.get("status"),
                "database_write_attempted": bool(
                    isinstance(error, SourceSetWriteError)
                    or getattr(error, "metadata_committed", False)
                ),
                "ddl_executed": False,
                "head_changed": False,
                "production_switched": False,
                "configuration_path": str(config_path),
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result["configuration_path"] = str(config_path)
    result["phase0_status"] = phase0_result.get("status")
    _emit(result)
    return _EXIT_OK if result.get("formal_p03_complete") else _EXIT_UNAVAILABLE


def _run_source_p04_identity_coverage(args: argparse.Namespace) -> int:
    from stock_a.storage.postgres.source_raw import RawSourcePgError
    from stock_a.storage.postgres.source_set_ingress import (
        SourceSetIngressError,
        read_p04_identity_coverage,
    )

    config, _ = _load_runtime_config()
    try:
        result = read_p04_identity_coverage(
            config,
            target_session=_parse_source_set_date(args.target_session),
            research_cutoff=_parse_source_set_date(args.research_cutoff),
            source_set_id=args.source_set_id,
            package_sha256=args.package_sha256,
            source_symbol=args.symbol,
            offset=args.offset,
            limit=args.limit,
        )
    except (ValueError, RawSourcePgError, SourceSetIngressError) as error:
        _emit(
            {
                "ok": False,
                "status": "BLOCKED",
                "code": getattr(error, "code", "P04_COVERAGE_FAILED"),
                "message": str(error),
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result["ok"] = True
    _emit(result)
    return _EXIT_OK


def _run_source_p04_read(args: argparse.Namespace) -> int:
    config, config_path = _load_runtime_config()
    target_session = _parse_source_set_date(args.target_session)
    from stock_a.storage.postgres.source_set_ingress import read_p04_source_input

    try:
        result = read_p04_source_input(
            config,
            target_session=target_session,
            package_sha256=args.package_sha256,
            audit_mode=args.audit_mode,
        )
    except Exception as error:
        _emit(
            {
                "ok": False,
                "status": "BLOCKED",
                "code": getattr(error, "code", "P04_READ_FAILED"),
                "message": str(error),
                "target_session": target_session.isoformat(),
                "package_sha256": args.package_sha256,
                "database_write": False,
                "ddl_executed": False,
                "candidate_fallback": False,
                "configuration_path": str(config_path),
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": result.get("status") == "AVAILABLE",
            "configuration_path": str(config_path),
        }
    )
    _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE
