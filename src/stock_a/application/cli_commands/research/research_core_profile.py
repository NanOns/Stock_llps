"""Stock A命令行research_core_profile职责模块。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal, InvalidOperation, localcontext
import json
import os
import sys
import time
from typing import Any
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

from stock_a.application.cli_commands.common import _json_default


def _run_full_source_core_profile(
    args: argparse.Namespace,
    *,
    package: Any,
    pool: Any,
    units: UnitContract,
    profile_contract: dict[str, Any],
    scan_budget: VipdataTargetScanBudget,
) -> tuple[dict[str, Any], Any, float, float]:
    """让全范围ZIP扫描逐条经过现役research_stock消费者，只保留有限统计与样本。"""
    if not isinstance(profile_contract, dict):
        raise DiagnosticError(
            "PROFILE_PREVIEW_CONTRACT_REQUIRED",
            "全范围Core/Profile生产必须装载唯一day-chain合同",
            _EXIT_ARGUMENT,
        )
    target = _parse_target(args.target_session)
    _calendar, _metadata, _revision, sessions = _load_calendar(target)
    if target not in sessions:
        raise DiagnosticError(
            "TARGET_DATE_INVALID", "目标日期不在已核验官方交易日历内", _EXIT_ARGUMENT
        )

    filtered = tuple(
        item
        for item in pool.candidates
        if (args.market == "ALL" or item["market"] == args.market)
        and (args.board == "ALL" or item["board"] == args.board)
    )
    candidate_total = len(filtered)
    if (
        type(args.offset) is not int
        or args.offset < 0
        or type(args.limit) is not int
        or not 1 <= args.limit <= 100
    ):
        raise DiagnosticError(
            "UNIVERSE_PAGE_INVALID", "ZIP来源分页或过滤条件无效", _EXIT_ARGUMENT
        )
    requested_page = filtered[args.offset : args.offset + args.limit]
    sample_positions = (
        {0, len(requested_page) // 2, len(requested_page) - 1}
        if requested_page
        else set()
    )
    sample_symbols = {
        item["symbol"]
        for index, item in enumerate(requested_page)
        if index in sample_positions
    }
    sampled_groups: set[tuple[str, str]] = set()
    for item in filtered:
        group = (item["market"], item["board"])
        if group not in sampled_groups:
            sample_symbols.add(item["symbol"])
            sampled_groups.add(group)

    field_registry = profile_contract.get("field_registry", [])
    rules = profile_contract.get("profile_expression_registry", {})
    core_fields = sorted(
        {
            item.get("field_id")
            for item in field_registry
            if isinstance(item, dict)
            and isinstance(item.get("field_id"), str)
            and item["field_id"].startswith("core.")
        }
    )
    profile_fields = {
        rule_id: rule.get("field_id")
        for rule_id, rule in rules.items()
        if isinstance(rule_id, str)
        and isinstance(rule, dict)
        and isinstance(rule.get("field_id"), str)
    }
    if len(core_fields) != 43 or len(profile_fields) != 11:
        raise DiagnosticError(
            "PROFILE_PREVIEW_CONTRACT_INVALID",
            "现役合同未提供43个Core字段和11条画像规则",
            _EXIT_UNAVAILABLE,
        )

    source_status_counts: dict[str, int] = {}
    source_reason_counts: dict[str, int] = {}
    core_stats = {
        name: {"quality_counts": {}, "unavailable_counts": {}, "reason_counts": {}}
        for name in core_fields
    }
    profile_stats = {
        field_id: {
            "rule_id": rule_id,
            "status_counts": {},
            "quality_counts": {},
            "unavailable_counts": {},
            "reason_counts": {},
        }
        for rule_id, field_id in profile_fields.items()
    }
    samples: dict[str, dict[str, Any]] = {}
    consumer_failure_reasons: dict[str, int] = {}
    produced_candidates = 0
    nonproduced_candidates = 0
    consumer_failed = 0
    core_profile_elapsed = 0.0
    peak_working_set_bytes: int | None = None

    def process_working_set_bytes() -> int | None:
        if os.name != "nt":
            return None
        import ctypes
        from ctypes import wintypes

        class _ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
                ("PrivateUsage", ctypes.c_size_t),
            ]

        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        if not psapi.GetProcessMemoryInfo(
            kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
        ):
            return None
        return int(counters.WorkingSetSize)

    def increment(target_map: dict[str, int], key: str) -> None:
        target_map[key] = target_map.get(key, 0) + 1

    def save_sample(
        symbol: str,
        row: Any,
        result: dict[str, Any] | None,
        unavailable_status: str | None = None,
    ) -> None:
        if symbol not in sample_symbols:
            return
        sample: dict[str, Any] = {
            "symbol": symbol,
            "market": row.market,
            "board": row.board,
            "source_bar_status": row.status,
            "source_reason": row.reason,
            "target_session": target.isoformat(),
        }
        if result is None:
            sample["consumer_status"] = unavailable_status
        else:
            sample.update(
                {
                    "consumer_status": result.get("status"),
                    "source": result.get("source_package"),
                    "price_coordinate": result.get("adjustment", {}).get("coordinate"),
                    "research_mode": result.get("research_mode"),
                    "target_bar_present": result.get("target_bar_present"),
                    "raw_availability": result.get("raw_availability"),
                    "core_availability": result.get("core_availability"),
                    "target_raw_fields": result.get("target_raw_fields"),
                    "core_fields": result.get("core_fields"),
                    "profile_components": [
                        {
                            key: item.get(key)
                            for key in (
                                "field_id",
                                "rule_id",
                                "status",
                                "quality",
                                "value",
                                "reason",
                            )
                        }
                        for item in result.get("profile_preview", {}).get(
                            "components", []
                        )
                    ],
                }
            )
        samples[symbol] = sample

    def record_unavailable(
        symbol: str, row: Any, status: str, reason: str | None
    ) -> None:
        nonlocal nonproduced_candidates
        nonproduced_candidates += 1
        for stats in core_stats.values():
            increment(stats["unavailable_counts"], status)
            if reason:
                increment(stats["reason_counts"], reason)
        for stats in profile_stats.values():
            increment(stats["status_counts"], status)
            increment(stats["unavailable_counts"], status)
            if reason:
                increment(stats["reason_counts"], reason)
        if status == "CONSUMER_FAILED":
            increment(consumer_failure_reasons, reason or "UNKNOWN_CONSUMER_FAILURE")
        save_sample(symbol, row, None, status)

    def record_observation(row: Any, result: dict[str, Any]) -> None:
        nonlocal produced_candidates
        symbol = row.symbol
        produced_candidates += 1
        output_core = result.get("core_fields", {})
        for name, stats in core_stats.items():
            field = output_core.get(name)
            if not isinstance(field, dict):
                increment(stats["unavailable_counts"], "CONSUMER_FIELD_OMITTED")
                continue
            quality = field.get("quality") or "QUALITY_UNSPECIFIED"
            increment(stats["quality_counts"], quality)
            if field.get("reason"):
                increment(stats["reason_counts"], field["reason"])
        components = result.get("profile_preview", {}).get("components", [])
        by_field = {
            item.get("field_id"): item for item in components if isinstance(item, dict)
        }
        for field_id, stats in profile_stats.items():
            component = by_field.get(field_id)
            if component is None:
                increment(stats["status_counts"], "CONSUMER_FIELD_OMITTED")
                increment(stats["unavailable_counts"], "CONSUMER_FIELD_OMITTED")
                continue
            component_status = component.get("status") or "STATUS_UNSPECIFIED"
            increment(stats["status_counts"], component_status)
            if component.get("quality"):
                increment(stats["quality_counts"], component["quality"])
            if component.get("reason"):
                increment(stats["reason_counts"], component["reason"])
        save_sample(symbol, row, result)

    def consume_candidate(row: Any, capture: Any | None) -> None:
        nonlocal consumer_failed, core_profile_elapsed, peak_working_set_bytes
        current_rss = process_working_set_bytes()
        if current_rss is not None:
            peak_working_set_bytes = max(peak_working_set_bytes or 0, current_rss)
        increment(source_status_counts, row.status)
        if row.reason:
            increment(source_reason_counts, row.reason)
        if capture is None:
            status = {
                "READ_FAILED": "SOURCE_READ_FAILED",
                "NOT_EVALUATED": "SOURCE_NOT_EVALUATED",
                "TARGET_MISSING": "SOURCE_EMPTY_PLACEHOLDER",
            }.get(row.status, "SOURCE_UNAVAILABLE")
            record_unavailable(row.symbol, row, status, row.reason)
            return
        started = time.monotonic()
        try:
            result = research_stock(
                symbol=row.symbol,
                target_session=target.isoformat(),
                source_roots=(),
                units=units,
                limits=ReadLimits(
                    scan_budget.max_candidate_bytes,
                    scan_budget.max_candidate_records,
                    scan_budget.chunk_records,
                    Decimal(str(scan_budget.max_candidate_seconds)),
                ),
                profile_contract=profile_contract,
                vipdata_capture=capture,
            )
        except ResearchEntryError as error:
            consumer_failed += 1
            record_unavailable(row.symbol, row, "CONSUMER_FAILED", error.code)
        else:
            record_observation(row, result)
        finally:
            core_profile_elapsed += time.monotonic() - started
            current_rss = process_working_set_bytes()
            if current_rss is not None:
                peak_working_set_bytes = max(peak_working_set_bytes or 0, current_rss)

    scan_started = time.monotonic()
    scan_report = scan_vipdata_target_quality(
        package,
        pool,
        target_session=target,
        units=units,
        budget=scan_budget,
        market=args.market,
        board=args.board,
        on_candidate=consume_candidate,
        on_page=lambda page: sys.stderr.write(
            json.dumps(
                {"page_progress": asdict(page)},
                ensure_ascii=False,
                default=_json_default,
            )
            + "\n"
        ),
    )
    scan_wall = time.monotonic() - scan_started
    current_rss = process_working_set_bytes()
    if current_rss is not None:
        peak_working_set_bytes = max(peak_working_set_bytes or 0, current_rss)
    field_partition_complete = all(
        sum(stats["quality_counts"].values())
        + sum(stats["unavailable_counts"].values())
        == candidate_total
        for stats in core_stats.values()
    )
    profile_partition_complete = all(
        sum(stats["status_counts"].values()) == candidate_total
        for stats in profile_stats.values()
    )
    scan_complete = scan_report.not_evaluated == 0 and scan_report.read_failed == 0
    consumer_complete = nonproduced_candidates == 0

    summary = {
        "contract": "TDX_VIPDATA_FULL_SOURCE_CORE_PROFILE_NON_PIT_R1",
        "status": "COMPLETE_WITH_DISCLOSURES"
        if scan_complete
        and consumer_complete
        and field_partition_complete
        and profile_partition_complete
        else "PARTIAL",
        "target_session": target.isoformat(),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "formal_universe_status": "UNKNOWN_NOT_PROVED_BY_ZIP_SOURCE_CODES",
        "source_candidate_total": candidate_total,
        "source_status_counts": dict(sorted(source_status_counts.items())),
        "source_reason_counts": dict(sorted(source_reason_counts.items())),
        "source_partition_complete": sum(source_status_counts.values())
        == candidate_total,
        "core_profile_produced": produced_candidates,
        "core_profile_not_produced": nonproduced_candidates,
        "consumer_failed": consumer_failed,
        "consumer_failure_reason_counts": dict(
            sorted(consumer_failure_reasons.items())
        ),
        "core_field_count": len(core_fields),
        "core_fields": core_stats,
        "profile_rule_count": len(profile_fields),
        "profile_rules": profile_stats,
        "core_field_partitions_complete": field_partition_complete,
        "profile_rule_partitions_complete": profile_partition_complete,
        "profile_contract_unbound_disclosures": {
            "closed_week_trend": "NOT_IMPLEMENTED_NO_TRUSTED_PERIOD_INPUT",
            "closed_month_trend": "NOT_IMPLEMENTED_NO_TRUSTED_PERIOD_INPUT",
            "relative": "CONTRACT_UNBOUND_NO_ENTITY_BOUND_SAME_ENDPOINT_UNIVERSE_OR_INDEX",
        },
        "samples": [samples[key] for key in sorted(samples)],
        "quality_denominators": {
            "source_candidate": candidate_total,
            "core_field_each_candidate": candidate_total,
            "profile_rule_each_candidate": candidate_total,
            "evaluated_listing_universe": "UNKNOWN",
        },
        "source_identity_and_clock": {
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "observed_at_role": "LOCAL_REVALIDATED_ZIP_ENTRY_READ_NON_PIT",
            "security_id": "NOT_ISSUED",
            "amount_volume_units": "CALLER_DECLARED_RESEARCH_ASSUMPTION",
        },
        "read_usage": {
            "bytes": scan_report.bytes_read,
            "records": scan_report.records_read,
            "entry_read_seconds": scan_report.entry_read_elapsed_seconds,
            "core_profile_consumer_seconds": round(core_profile_elapsed, 6),
            "scan_elapsed_seconds": scan_report.elapsed_seconds,
            "scan_wall_seconds": round(scan_wall, 6),
            "budget": asdict(scan_report.budget),
            "decoded_capture_retained_for_consumer_symbols": 0,
            "decoded_history_retention": "ONE_CANDIDATE_CALLBACK_AT_A_TIME",
            "peak_process_working_set_bytes_observed_between_candidates": peak_working_set_bytes,
        },
    }
    result = {
        "status": summary["status"],
        "target_session": target.isoformat(),
        "candidate_contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "discovery": pool.catalog,
        "filters": {"market": args.market, "board": args.board},
        "denominators": {
            "source_candidate_total": candidate_total,
            "full_source_scan_evaluated": scan_report.evaluated,
            "target_bar_present": scan_report.target_present,
            "target_bar_missing": scan_report.target_missing,
            "read_failed": scan_report.read_failed,
            "not_evaluated": scan_report.not_evaluated,
            "core_profile_produced": produced_candidates,
            "core_profile_not_produced": nonproduced_candidates,
            "consumer_failed": consumer_failed,
            "source_partition_complete": summary["source_partition_complete"],
            "core_field_partitions_complete": field_partition_complete,
            "profile_rule_partitions_complete": profile_partition_complete,
        },
        "source_target_quality": {
            "candidate_total": scan_report.candidate_total,
            "evaluated": scan_report.evaluated,
            "target_present": scan_report.target_present,
            "target_missing": scan_report.target_missing,
            "read_failed": scan_report.read_failed,
            "not_evaluated": scan_report.not_evaluated,
            "by_market": scan_report.by_market,
            "by_board": scan_report.by_board,
            "field_quality": scan_report.field_quality,
            "not_evaluated_reason_counts": {
                reason: sum(
                    item.status == "NOT_EVALUATED" and item.reason == reason
                    for item in scan_report.candidates
                )
                for reason in sorted(
                    {
                        item.reason
                        for item in scan_report.candidates
                        if item.status == "NOT_EVALUATED" and item.reason
                    }
                )
            },
        },
        "full_source_core_profile": summary,
        "page": {
            "offset": args.offset,
            "limit": args.limit,
            "symbols": [item["symbol"] for item in requested_page],
            "results_are_full_source_samples": True,
        },
        "source_package": {
            "package_id": scan_report.package_id,
            "file_size": package.file_size,
            "zip_crc": package.zip_crc,
            "zip_entry_count": package.zip_entry_count,
            "official_package_url": "https://data.tdx.com.cn/vipdoc/hsjday.zip",
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "membership_and_listing_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
        },
        "performance": {
            "package_integrity_validation_seconds": None,
            "candidate_pool_metadata_seconds": None,
            "full_source_scan_seconds": round(scan_wall, 6),
            "entry_read_seconds": scan_report.entry_read_elapsed_seconds,
            "core_profile_consumer_seconds": round(core_profile_elapsed, 6),
            "source_bytes": scan_report.bytes_read,
            "source_records": scan_report.records_read,
            "callback_retains_full_histories": False,
        },
    }
    return result, scan_report, scan_wall, core_profile_elapsed
