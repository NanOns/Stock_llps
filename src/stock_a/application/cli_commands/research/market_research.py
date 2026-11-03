"""Stock A命令行market_research职责模块。"""

from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
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
    read_sealed_source_market_breadth_history,
)
from stock_a.application.source_set_candidate import (
    SourceSetCandidateApplicationError,
    _official_calendar,
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
from stock_a.research.source_market_breadth import (
    RawPriceObservation,
    SourceCohortIdentity,
    SourceMarketBreadthInput,
    calculate_source_market_breadth,
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


def _fraction_payload(value: Fraction | None) -> dict[str, Any] | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 40
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        display = f"{decimal_value:.12f}"
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "fraction": str(value),
        "display_decimal_12dp": display,
        "unit": "fraction",
    }


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
        calendar = _official_calendar(target_session)
        target_index = calendar["sessions"].index(target_session)
    except (SourceSetCandidateApplicationError, ValueError) as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "MARKET_BREADTH_CALENDAR_INVALID",
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    if target_index < 4:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "MARKET_BREADTH_HISTORY_TOO_SHORT",
                "message": "版本化官方日历中目标日前不足四个会话",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT

    session_window = calendar["sessions"][target_index - 4 : target_index + 1]
    try:
        pg_result = read_sealed_source_market_breadth_history(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_set_id=args.source_set_id,
            package_sha256=args.package_sha256,
            session_window=session_window,
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

    source_set = pg_result["source_set"]
    if (
        source_set["calendar_id"] != calendar["calendar_id"]
        or source_set["calendar_revision"] != calendar["calendar_revision"]
        or source_set["calendar_sha256"] != calendar["calendar_sha256"]
    ):
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "MARKET_BREADTH_CALENDAR_SOURCE_SET_MISMATCH",
                "message": "SEALED来源集合所绑定的日历身份与当前官方日历不一致",
                "configuration_path": str(config_path),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE

    cohort = SourceCohortIdentity(
        namespace_id=pg_result["namespace_id"],
        source_set_id=args.source_set_id,
        ingress_attempt_id=source_set["ingress_attempt_id"],
        target_session=target_session,
        package_sha256=args.package_sha256,
        revision_id=int(source_set["revision_id"]),
        source_content_id=source_set["source_content_id"],
        source_capture_id=source_set["source_capture_id"],
        source_dataset_id=source_set["source_dataset_id"],
        source_byte_count=int(source_set["source_byte_count"]),
        parser_contract=source_set["parser_contract"],
        seal_status=source_set["seal_status"],
        durability_status=source_set["durability_status"],
        durability_source_capture_id=source_set["durability_source_capture_id"],
        durability_source_digest=source_set["durability_source_digest"],
        commit_ack_observed_at=source_set["commit_ack_observed_at"],
        seal_digest_sha256=source_set["seal_digest_sha256"],
        seal_writer_transaction_id=source_set["seal_writer_transaction_id"],
        source_time_quality=source_set["source_time_quality"],
        source_available_at=source_set["source_available_at"],
        capture_read_started_at=source_set["capture_read_started_at"],
        capture_validated_at=source_set["capture_validated_at"],
        source_clock_role=source_set["source_clock_role"],
        durable_at=source_set["durable_at"],
        sealed_at=source_set["sealed_at"],
        raw_row_count=int(source_set["raw_row_count"]),
        target_present_count=int(source_set["target_present_count"]),
        target_missing_count=int(source_set["target_missing_count"]),
        read_failed_count=int(source_set["read_failed_count"]),
        source_digest=source_set["source_digest"],
        source_raw_digest=source_set["source_raw_digest"],
        observed_raw_digest=pg_result["target_raw_digest"],
        calendar_id=calendar["calendar_id"],
        calendar_revision=calendar["calendar_revision"],
        calendar_sha256=calendar["calendar_sha256"],
    )
    observations = tuple(
        RawPriceObservation(**row) for row in pg_result["history_rows"]
    )
    try:
        calculation = calculate_source_market_breadth(
            SourceMarketBreadthInput(
                cohort=cohort,
                source_symbols=pg_result["source_symbols"],
                research_cutoff=research_cutoff,
                calendar_id=calendar["calendar_id"],
                calendar_revision=calendar["calendar_revision"],
                calendar_sha256=calendar["calendar_sha256"],
                official_sessions=calendar["sessions"],
                observations=observations,
            )
        )
    except ValueError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "MARKET_BREADTH_INPUT_REJECTED",
                "message": str(error),
                "configuration_path": str(config_path),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE

    window = calculation.session_window
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
    eligible_classes = (
        "COMPARABLE_GAIN",
        "COMPARABLE_LOSS",
        "COMPARABLE_BOTH_POSITIVE",
        "COMPARABLE_BOTH_NONPOSITIVE",
    )
    status = (
        "UNKNOWN_ZERO_ELIGIBLE"
        if calculation.n_eligible == 0
        else "PARTIAL_WITH_UNKNOWN"
        if calculation.counts["UNKNOWN"]
        else "COMPLETE"
    )
    document = {
        "ok": True,
        "status": status,
        "contract_version": calculation.contract_version,
        "analysis_scope": "SOURCE_SYMBOL_COHORT_FIXED_AT_T",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "official_calendar": {
            "calendar_id": calendar["calendar_id"],
            "revision": calendar["calendar_revision"],
            "sha256": calendar["calendar_sha256"],
            "lookback_sessions": {
                "t_minus_4": window[0].isoformat(),
                "t_minus_3": window[1].isoformat(),
                "t_minus_2": window[2].isoformat(),
                "t_minus_1": window[3].isoformat(),
                "t": window[4].isoformat(),
            },
            "raw_price_sessions": [
                {"role": "t", "session_date": window[4].isoformat()},
                {"role": "t_minus_1", "session_date": window[3].isoformat()},
                {"role": "t_minus_3", "session_date": window[1].isoformat()},
                {"role": "t_minus_4", "session_date": window[0].isoformat()},
            ],
        },
        "source_cohort": {
            "source_set_id": cohort.source_set_id,
            "ingress_attempt_id": cohort.ingress_attempt_id,
            "revision_id": cohort.revision_id,
            "namespace_id": cohort.namespace_id,
            "target_session": target_session.isoformat(),
            "package_sha256": cohort.package_sha256,
            "source_content_id": cohort.source_content_id,
            "source_capture_id": cohort.source_capture_id,
            "source_dataset_id": cohort.source_dataset_id,
            "source_byte_count": cohort.source_byte_count,
            "parser_contract": cohort.parser_contract,
            "seal_status": cohort.seal_status,
            "durability_status": cohort.durability_status,
            "durability_source_capture_id": cohort.durability_source_capture_id,
            "durability_source_digest": cohort.durability_source_digest,
            "commit_ack_observed_at": cohort.commit_ack_observed_at,
            "seal_digest_sha256": cohort.seal_digest_sha256,
            "seal_writer_transaction_id": cohort.seal_writer_transaction_id,
            "source_digest": cohort.source_digest,
            "source_raw_digest": cohort.source_raw_digest,
            "recomputed_target_raw_digest": cohort.observed_raw_digest,
            "raw_digest_verified": cohort.source_raw_digest
            == cohort.observed_raw_digest,
            "raw_row_count": cohort.raw_row_count,
            "target_present_count": cohort.target_present_count,
            "target_missing_count": cohort.target_missing_count,
            "read_failed_count": cohort.read_failed_count,
            "price_unit_status": source_set["price_unit_status"],
            "source_time_quality": cohort.source_time_quality,
            "source_available_at": cohort.source_available_at,
            "capture_read_started_at": cohort.capture_read_started_at,
            "capture_validated_at": cohort.capture_validated_at,
            "source_clock_role": cohort.source_clock_role,
            "durable_at": cohort.durable_at,
            "sealed_at": cohort.sealed_at,
        },
        "metric": {
            "name": "B_fixed_source",
            "formula": "MEAN(I(ret1_t>0)-I(ret1_t_minus_3>0))",
            "exact": _fraction_payload(calculation.b_fixed_source),
            "unit": "fraction",
            "range": ["-1", "1"],
            "sign_return_difference_is_main_value": False,
            "ma20_profile_breadth_is_main_value": False,
        },
        "denominator": {
            "N_source": calculation.n_source,
            "N_eligible": calculation.n_eligible,
            "UNKNOWN": calculation.counts["UNKNOWN"],
            "eligible_partition_sum": sum(
                calculation.counts[name] for name in eligible_classes
            ),
            "counts": calculation.counts,
            "coverage": _fraction_payload(calculation.coverage),
            "unknown_reason_counts": calculation.unknown_reason_counts,
        },
        "source_symbol_results": symbol_results,
        "formal_market_breadth": {
            "status": "NOT_READY_HISTORICAL_UNIVERSE",
            "reason": "本值只描述t日封存来源代码集合，不代表t与t-3各自正式上市总体。",
            "missing_dependencies": [
                "t与t-3官方有效上市身份总体及其精确比较集合",
                "稳定security_id和日期有效身份映射",
                "按当时available_at核定的历史成员资格与PIT证据",
            ],
        },
        "database_read": {
            "database": "stock_a",
            "schema": "stock",
            "account": "stock_a",
            "server": "127.0.0.1:5432",
            "transaction": pg_result["transaction"],
            "history_raw_row_count": pg_result["history_row_count"],
            "source_zip_opened": pg_result["source_zip_opened"],
            "database_write_attempted": pg_result["database_write_attempted"],
            "ddl_executed": pg_result["ddl_executed"],
            "dml_executed": pg_result["dml_executed"],
            "dml_rows_affected": 0,
        },
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
