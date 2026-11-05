"""Stock A研究universe职责模块。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
import time
from typing import Any, Iterable, Mapping, Sequence
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.sources.tdx.vipdata import VipdataEntryCapture, VipdataTargetQualityReport

from stock_a.research.entry_core.comparison import research_compare
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.windows import (
    CompareBudget,
    UniverseDiscoveryBudget,
)
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_context.source_discovery import (
    _aggregate_availability,
    _discover_source_candidates,
)
from stock_a.research.entry_context.windows import (
    _DEFAULT_UNIVERSE_PAGE_SIZE,
    _MAX_COMPARE_SYMBOLS,
    _MAX_UNIVERSE_PAGE_SIZE,
)


def research_universe_preview(
    *,
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    offset: int = 0,
    limit: int = _DEFAULT_UNIVERSE_PAGE_SIZE,
    market_filter: str = "ALL",
    board_filter: str = "ALL",
    discovery_budget: UniverseDiscoveryBudget | None = None,
    budget: CompareBudget | None = None,
    profile_contract: dict[str, Any] | None = None,
    source_candidate_pool: Sequence[Mapping[str, Any]] | None = None,
    source_candidate_catalog: Mapping[str, Any] | None = None,
    source_captures: Mapping[str, VipdataEntryCapture] | None = None,
    source_target_quality_report: VipdataTargetQualityReport | None = None,
) -> dict[str, Any]:
    """只读发现TDX来源代码候选并对单页复用既有Core和累计预算。"""
    started = time.monotonic()
    target = _parse_target(target_session)
    _load_calendar(target)
    if type(offset) is not int or offset < 0:
        raise ResearchEntryError("UNIVERSE_PAGE_INVALID", "分页offset必须为非负整数")
    if type(limit) is not int or not 1 <= limit <= _MAX_UNIVERSE_PAGE_SIZE:
        raise ResearchEntryError(
            "UNIVERSE_PAGE_INVALID", f"每页limit必须为1至{_MAX_UNIVERSE_PAGE_SIZE}只"
        )
    if not isinstance(market_filter, str) or market_filter.upper() not in {
        "ALL",
        "SH",
        "SZ",
    }:
        raise ResearchEntryError(
            "UNIVERSE_FILTER_INVALID", "market过滤只接受ALL、SH或SZ"
        )
    if not isinstance(board_filter, str) or board_filter.upper() not in {
        "ALL",
        "MAIN_BOARD",
        "GEM",
    }:
        raise ResearchEntryError(
            "UNIVERSE_FILTER_INVALID", "board过滤只接受ALL、MAIN_BOARD或GEM"
        )
    market_filter = market_filter.upper()
    board_filter = board_filter.upper()
    discovery_budget = discovery_budget or UniverseDiscoveryBudget()
    budget = budget or CompareBudget()
    if not isinstance(budget, CompareBudget):
        raise ResearchEntryError("COMPARE_BUDGET_INVALID", "缺少有效的全批累计预算")

    if isinstance(source_roots, (str, bytes, os.PathLike)):
        raise ResearchEntryError("SOURCE_PATH_INVALID", "source_roots必须是根路径序列")
    try:
        roots = tuple(source_roots)
    except TypeError as error:
        raise ResearchEntryError(
            "SOURCE_PATH_INVALID", "source_roots必须是根路径序列"
        ) from error
    if source_target_quality_report is not None:
        if not isinstance(source_target_quality_report, VipdataTargetQualityReport):
            raise ResearchEntryError(
                "VIPDATA_QUALITY_REPORT_INVALID", "目标日来源质量报告类型无效"
            )

    if source_candidate_pool is None:
        if (
            source_candidate_catalog is not None
            or source_captures is not None
            or source_target_quality_report is not None
        ):
            raise ResearchEntryError(
                "UNIVERSE_CANDIDATE_OVERRIDE_INVALID",
                "候选目录摘要必须与候选池同时提供",
            )
        candidates, catalog = _discover_source_candidates(roots, discovery_budget)
    else:
        if not isinstance(source_candidate_catalog, Mapping) or (
            not roots
            and source_captures is None
            and source_target_quality_report is None
        ):
            raise ResearchEntryError(
                "UNIVERSE_CANDIDATE_OVERRIDE_INVALID",
                "显式候选池必须绑定来源根和目录摘要",
            )
        try:
            candidates = tuple(dict(item) for item in source_candidate_pool)
        except (TypeError, ValueError) as error:
            raise ResearchEntryError(
                "UNIVERSE_CANDIDATE_OVERRIDE_INVALID", "显式候选池格式无效"
            ) from error
        if (
            source_candidate_catalog.get("complete") is not True
            or source_candidate_catalog.get("candidate_count") != len(candidates)
            or any(
                not isinstance(item.get("symbol"), str)
                or item.get("market") not in {"SH", "SZ"}
                or item.get("board") not in {"MAIN_BOARD", "GEM"}
                for item in candidates
            )
            or len({item["symbol"] for item in candidates}) != len(candidates)
        ):
            raise ResearchEntryError(
                "UNIVERSE_CANDIDATE_OVERRIDE_INVALID", "显式候选池与目录摘要不一致"
            )
        candidates = tuple(sorted(candidates, key=lambda item: item["symbol"]))
        catalog = dict(source_candidate_catalog)
    selected = tuple(
        candidate
        for candidate in candidates
        if (market_filter == "ALL" or candidate["market"] == market_filter)
        and (board_filter == "ALL" or candidate["board"] == board_filter)
    )
    candidate_total = len(selected)
    page_candidates = selected[offset : offset + limit]
    candidate_by_symbol = {item["symbol"]: item for item in page_candidates}
    quality_by_symbol: dict[str, Any] = {}
    if source_target_quality_report is not None:
        report = source_target_quality_report
        report_rows = tuple(report.candidates)
        report_symbols = [item.symbol for item in report_rows]
        source_symbols = {item["symbol"] for item in candidates}
        report_counts = {
            status: sum(item.status == status for item in report_rows)
            for status in (
                "TARGET_PRESENT",
                "TARGET_MISSING",
                "READ_FAILED",
                "NOT_EVALUATED",
            )
        }
        if (
            report.target_session != target
            or report.package_sha256 != catalog.get("source_package_sha256")
            or report.package_id != catalog.get("source_package_id")
            or report.candidate_total != len(candidates)
            or len(report_rows) != len(candidates)
            or set(report_symbols) != source_symbols
            or len(report_symbols) != len(set(report_symbols))
            or report_counts["TARGET_PRESENT"] != report.target_present
            or report_counts["TARGET_MISSING"] != report.target_missing
            or report_counts["READ_FAILED"] != report.read_failed
            or report_counts["NOT_EVALUATED"] != report.not_evaluated
            or report.evaluated
            != report.target_present + report.target_missing + report.read_failed
            or report.candidate_total != report.evaluated + report.not_evaluated
        ):
            raise ResearchEntryError(
                "VIPDATA_QUALITY_REPORT_MISMATCH",
                "目标日来源质量报告与候选池/目标日/分母不一致",
            )
        quality_by_symbol = {item.symbol: item for item in report_rows}
        source_candidate_by_symbol = {item["symbol"]: item for item in candidates}
        if any(
            symbol not in source_symbols
            or quality_by_symbol[symbol].status
            not in {"TARGET_PRESENT", "TARGET_MISSING"}
            or source_candidate_by_symbol[symbol].get("empty_placeholder") is True
            for symbol in report.consumer_captures
        ):
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH",
                "来源质量报告保留了不属于可读取候选的Capture",
            )
        if source_captures is None:
            page_symbols = {item["symbol"] for item in page_candidates}
            source_captures = {
                symbol: capture
                for symbol, capture in report.consumer_captures.items()
                if symbol in page_symbols
            }
        expected_captures = {
            item["symbol"]
            for item in page_candidates
            if item.get("empty_placeholder") is not True
            and quality_by_symbol[item["symbol"]].status
            in {"TARGET_PRESENT", "TARGET_MISSING"}
        }
        if set(source_captures) != expected_captures:
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH",
                "同一扫描交给分页消费者的Capture未精确覆盖可读取本页候选",
            )

    scannable_candidates = tuple(
        item
        for item in page_candidates
        if item.get("empty_placeholder") is not True
        and (
            not quality_by_symbol
            or quality_by_symbol[item["symbol"]].status
            in {"TARGET_PRESENT", "TARGET_MISSING"}
        )
    )
    if source_captures is not None:
        if not isinstance(source_captures, Mapping):
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获必须是按代码索引的映射"
            )
        if set(source_captures) != {item["symbol"] for item in scannable_candidates}:
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获未精确覆盖本页非空条目"
            )
        for candidate in scannable_candidates:
            symbol = candidate["symbol"]
            capture = source_captures.get(symbol)
            source_ref = (
                capture.capture.source_package_entry
                if isinstance(capture, VipdataEntryCapture)
                else None
            )
            if (
                not isinstance(capture, VipdataEntryCapture)
                or capture.target_session != target
                or capture.source_symbol != symbol
                or source_ref is None
                or source_ref.package_sha256 != catalog.get("source_package_sha256")
                or source_ref.zip_entry_path != candidate.get("zip_entry_path")
            ):
                raise ResearchEntryError(
                    "VIPDATA_CAPTURE_MISMATCH",
                    "ZIP候选页与Capture的包/条目/目标日身份不一致",
                )
    page_results: list[dict[str, Any]] = []
    total_bytes = 0
    total_records = 0
    stop_code: str | None = None
    next_position = 0
    for candidate in page_candidates:
        if candidate.get("empty_placeholder") is True:
            page_results.append(
                {
                    "status": "FAILED",
                    "requested_symbol": candidate["symbol"],
                    "error_code": "ZIP_DAY_ENTRY_EMPTY",
                    "message": "官方ZIP将此证券作为零字节占位；未向Core提供行情",
                    "read_usage": {"bytes": 0, "records": 0},
                    "source_candidate": {
                        **candidate,
                        "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
                    },
                }
            )
        elif quality_by_symbol:
            quality = quality_by_symbol[candidate["symbol"]]
            if quality.status in {"READ_FAILED", "NOT_EVALUATED"}:
                page_results.append(
                    {
                        "status": "FAILED"
                        if quality.status == "READ_FAILED"
                        else "NOT_EVALUATED",
                        "requested_symbol": candidate["symbol"],
                        "target_bar_present": None,
                        "source_bar_status": quality.status,
                        "source_bar_reason": quality.reason,
                        "error_code": quality.reason,
                        "message": (
                            "VIPDATA条目读取失败；未向Core提供行情"
                            if quality.status == "READ_FAILED"
                            else "来源候选保留在全市场分母中，但本次目标日预算未评价"
                        ),
                        "read_usage": {
                            "bytes": quality.bytes_read,
                            "records": quality.record_count,
                        },
                        "source_candidate": {
                            **candidate,
                            "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
                        },
                    }
                )

    def mark_page_not_evaluated(symbols: Iterable[str], code: str) -> None:
        for symbol in symbols:
            candidate = candidate_by_symbol[symbol]
            page_results.append(
                {
                    "status": "NOT_EVALUATED",
                    "requested_symbol": symbol,
                    "error_code": code,
                    "message": "本页累计读取预算耗尽；对象保留在分页分母且未扫描",
                    "read_usage": {"bytes": 0, "records": 0},
                    "source_candidate": {
                        **candidate,
                        "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
                    },
                }
            )

    while next_position < len(scannable_candidates):
        elapsed = time.monotonic() - started
        remaining_seconds = budget.max_total_seconds - elapsed
        remaining_bytes = budget.max_total_bytes - total_bytes
        remaining_records = budget.max_total_records - total_records
        if remaining_seconds <= 0:
            stop_code = "MAX_TOTAL_SECONDS_EXCEEDED"
        elif remaining_bytes <= 0:
            stop_code = "MAX_TOTAL_BYTES_EXCEEDED"
        elif remaining_records <= 0:
            stop_code = "MAX_TOTAL_RECORDS_EXCEEDED"
        if stop_code is not None:
            mark_page_not_evaluated(
                (item["symbol"] for item in scannable_candidates[next_position:]),
                stop_code,
            )
            break

        chunk_candidates = scannable_candidates[
            next_position : next_position + _MAX_COMPARE_SYMBOLS
        ]
        chunk = research_compare(
            symbols=tuple(item["symbol"] for item in chunk_candidates),
            target_session=target.isoformat(),
            source_roots=roots,
            units=units,
            limits=limits,
            budget=CompareBudget(remaining_bytes, remaining_records, remaining_seconds),
            profile_contract=profile_contract,
            source_captures=(
                {
                    item["symbol"]: source_captures[item["symbol"]]
                    for item in chunk_candidates
                }
                if source_captures is not None
                else None
            ),
        )
        total_bytes += chunk["budget"]["used_bytes"]
        total_records += chunk["budget"]["used_records"]
        for result in chunk["results"]:
            candidate = candidate_by_symbol[result["requested_symbol"]]
            if (
                candidate.get("empty_placeholder") is True
                and result.get("status") == "FAILED"
            ):
                result["error_code"] = "ZIP_DAY_ENTRY_EMPTY"
                result["message"] = "官方ZIP将此证券作为零字节占位；未向Core提供行情"
            result["source_candidate"] = {
                **candidate,
                "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
            }
            page_results.append(result)
        next_position += len(chunk["results"])
        stop_code = chunk["budget"]["exhausted_by"]
        if stop_code is not None and next_position < len(scannable_candidates):
            mark_page_not_evaluated(
                (item["symbol"] for item in scannable_candidates[next_position:]),
                stop_code,
            )
            break

    for item in page_results:
        quality = quality_by_symbol.get(item.get("requested_symbol"))
        if quality is not None:
            item["source_bar_status"] = quality.status
            item["source_bar_reason"] = quality.reason
            item["source_bar_quality"] = asdict(quality)

    page_order = {item["symbol"]: index for index, item in enumerate(page_candidates)}
    page_results.sort(key=lambda item: page_order[item["requested_symbol"]])

    available = sum(item.get("status") == "AVAILABLE" for item in page_results)
    failed = sum(item.get("status") == "FAILED" for item in page_results)
    partial_unknown = sum(
        item.get("status") == "PARTIAL_UNKNOWN" for item in page_results
    )
    not_evaluated = sum(item.get("status") == "NOT_EVALUATED" for item in page_results)
    classified = available + failed + partial_unknown + not_evaluated
    if classified != len(page_candidates):
        raise ResearchEntryError(
            "UNIVERSE_CLASSIFICATION_INCOMPLETE",
            "本页来源候选未按可用、失败、部分未知和未评价完整分区",
            context={"page_requested": len(page_candidates), "classified": classified},
        )

    profile_preview_denominators: dict[str, Any] | None = None
    if profile_contract is not None:
        preview_results = [
            item
            for item in page_results
            if isinstance(item.get("profile_preview"), dict)
        ]
        component_counts = {
            "total": 0,
            "evaluated": 0,
            "observed": 0,
            "unknown_data": 0,
            "not_implemented": 0,
            "contract_unbound": 0,
        }
        for item in preview_results:
            preview = item["profile_preview"]
            for name in component_counts:
                component_counts[name] += preview["counts"][name]
        profile_preview_denominators = {
            "page_requested": len(page_candidates),
            "objects_with_profile_preview": len(preview_results),
            "objects_available": available,
            "objects_partial_unknown": partial_unknown,
            "objects_failed": failed,
            "objects_not_evaluated": not_evaluated,
            "objects_without_profile_preview": failed + not_evaluated,
            "object_partition_complete": len(preview_results) + failed + not_evaluated
            == len(page_candidates),
            "components": component_counts,
        }

    short_summary: list[dict[str, Any]] = []
    for item in page_results:
        candidate = item["source_candidate"]
        observation = item.get("observation_summary", {})
        fields = observation.get("fields", {})
        positions = fields.get("moving_average_position", {})
        detail_arguments = [
            "research-stock",
            candidate["symbol"],
            "--target-session",
            target.isoformat(),
            "--unit-version",
            units.version,
        ]
        if units.amount_to_cny is not None:
            detail_arguments.extend(("--amount-to-cny", str(units.amount_to_cny)))
        if units.volume_to_shares is not None:
            detail_arguments.extend(("--volume-to-shares", str(units.volume_to_shares)))
        detail_arguments.extend(
            (
                "--max-bytes",
                str(limits.max_bytes),
                "--max-records",
                str(limits.max_records),
                "--chunk-records",
                str(limits.chunk_records),
                "--max-seconds",
                str(limits.max_seconds),
                "--format",
                "summary",
                "--include-profile-preview",
            )
        )
        summary_item = {
            "symbol": candidate["symbol"],
            "market": candidate["market"],
            "board": candidate["board"],
            "target_session": target.isoformat(),
            "status": item["status"],
            "target_bar_present": item.get("target_bar_present"),
            "source_bar_status": item.get("source_bar_status"),
            "source_bar_reason": item.get("source_bar_reason"),
            "source_last_session": item.get("source_last_session"),
            "raw_close": fields.get("raw_close"),
            "ma5": fields.get("ma5"),
            "ma20": fields.get("ma20"),
            "ma60": fields.get("ma60"),
            "atr20": fields.get("atr20"),
            "ret5": fields.get("ret5"),
            "ret20": fields.get("ret20"),
            "amount": fields.get("amount"),
            "volume": fields.get("volume"),
            "close_vs_ma20": positions.get("close_vs_ma20"),
            "raw_availability": item.get("raw_availability"),
            "core_availability": item.get("core_availability"),
            "source_bar_quality": asdict(quality_by_symbol[candidate["symbol"]])
            if candidate["symbol"] in quality_by_symbol
            else None,
            "basic_fact_dimensions": item.get("basic_fact_dimensions"),
            "availability_explanation": item.get("availability_explanation"),
            "detail_query": {
                "arguments": detail_arguments,
                "contract": "同一来源代码、目标日、单位和单文件预算；详情在同一次Raw/Core读取上追加非PIT画像预览",
            },
            "error_code": item.get("error_code"),
        }
        if isinstance(item.get("profile_preview"), dict):
            preview = item["profile_preview"]
            summary_item["profile_preview"] = {
                "contract": preview["contract"],
                "scope": preview["scope"],
                "target_session": preview["target_session"],
                "source_last_session": preview["source_last_session"],
                "counts": preview["counts"],
                "components": [
                    {
                        key: component[key]
                        for key in (
                            "field_id",
                            "rule_id",
                            "expression_ref",
                            "status",
                            "quality",
                            "value",
                            "reason",
                            "missing_inputs",
                            "input_fields",
                            "production_eligibility",
                        )
                        if key in component
                    }
                    for component in preview["components"]
                ],
            }
        short_summary.append(summary_item)

    page_remaining = max(
        0, candidate_total - min(offset + len(page_candidates), candidate_total)
    )
    evaluated_page = available + failed + partial_unknown
    status = (
        "COMPLETE_WITH_DISCLOSURES"
        if failed == 0 and partial_unknown == 0 and not_evaluated == 0
        else "PARTIAL"
    )
    completed_reads = [
        item
        for item in page_results
        if isinstance(item.get("source_last_session"), str)
    ]
    source_last_session_counts: dict[str, int] = {}
    for item in completed_reads:
        last_session = item["source_last_session"]
        source_last_session_counts[last_session] = (
            source_last_session_counts.get(last_session, 0) + 1
        )
    latest_source_session = max(source_last_session_counts, default=None)
    page_coverage_by_market: dict[str, dict[str, int]] = {}
    page_coverage_by_board: dict[str, dict[str, int]] = {}
    for dimension, key in (
        (page_coverage_by_market, "market"),
        (page_coverage_by_board, "board"),
    ):
        for value in sorted({item["source_candidate"][key] for item in page_results}):
            matching = [
                item for item in page_results if item["source_candidate"][key] == value
            ]
            dimension[value] = {
                "candidate_count": len(matching),
                "evaluated_count": sum(
                    item.get("status") != "NOT_EVALUATED" for item in matching
                ),
                "not_evaluated_count": sum(
                    item.get("status") == "NOT_EVALUATED" for item in matching
                ),
                "actual_bar_on_target_count": sum(
                    item.get("target_bar_present") is True for item in matching
                ),
                "missing_target_bar_count": sum(
                    item.get("target_bar_present") is False for item in matching
                ),
                "source_read_failed_count": sum(
                    item.get("status") == "FAILED" for item in matching
                ),
            }
    coverage_summary = {
        "candidate_contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "candidate_total_in_filtered_pool": candidate_total,
        "page_candidate_count": len(page_candidates),
        "evaluated_count": evaluated_page,
        "not_evaluated_count": not_evaluated,
        "actual_bar_on_target_count": sum(
            item.get("target_bar_present") is True for item in page_results
        ),
        "missing_target_bar_count": sum(
            item.get("target_bar_present") is False for item in page_results
        ),
        "source_read_failed_count": failed,
        "latest_source_session": latest_source_session,
        "latest_source_session_scope": "本页已完整读到EOF并核验句柄的来源文件末条日期最大值；不是全市场正式可得时钟",
        "source_last_session_counts": dict(sorted(source_last_session_counts.items())),
        "by_market": page_coverage_by_market,
        "by_board": page_coverage_by_board,
        "page_partition_complete": classified == len(page_candidates),
        "filtered_pool_complete": offset == 0
        and page_remaining == 0
        and not_evaluated == 0,
        "pool_completion_note": "本入口只知道当前页；只有offset=0且单页覆盖完整过滤池时才标true。多页汇总须由调用方确认每一计划页均已实际完成；不代表官方上市总体",
    }
    if quality_by_symbol:
        page_quality_counts = {
            status: sum(
                quality_by_symbol[item["symbol"]].status == status
                for item in page_candidates
            )
            for status in (
                "TARGET_PRESENT",
                "TARGET_MISSING",
                "READ_FAILED",
                "NOT_EVALUATED",
            )
        }
        coverage_summary["source_target_quality"] = {
            "candidate_total": source_target_quality_report.candidate_total,
            "candidate_partition_complete": source_target_quality_report.candidate_total
            == source_target_quality_report.evaluated
            + source_target_quality_report.not_evaluated,
            "page_status_counts": page_quality_counts,
        }
    result = {
        "status": status,
        "status_explanation": "COMPLETE_WITH_DISCLOSURES仅表示当前页请求已处理并附带数据披露；不表示全候选池已读取，也不表示所有Raw/Core字段已知。请检查本页、未评价分母和逐字段quality/reason。",
        "candidate_contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "target_session": target.isoformat(),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "discovery": catalog,
        "filters": {"market": market_filter, "board": board_filter},
        "denominators": {
            "source_candidate_total": catalog["candidate_count"],
            "filtered_candidate_total": candidate_total,
            "page_requested": len(page_candidates),
            "page_evaluated": evaluated_page,
            "page_available": available,
            "page_failed": failed,
            "page_partial_unknown": partial_unknown,
            "page_not_evaluated": not_evaluated,
            "page_classified": classified,
            "page_partition_complete": classified == len(page_candidates),
            "unselected_by_this_page": candidate_total - len(page_candidates),
            "unselected_before_offset": min(offset, candidate_total),
            "remaining_after_page": page_remaining,
            "not_evaluated_in_filtered_candidate_set": max(
                0, candidate_total - evaluated_page
            ),
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "returned": len(page_candidates),
            "next_offset": offset + len(page_candidates) if page_remaining else None,
            "remaining_after_page": page_remaining,
            "symbols": [item["symbol"] for item in page_candidates],
        },
        "raw_availability": _aggregate_availability(page_results, "raw_availability"),
        "core_availability": _aggregate_availability(page_results, "core_availability"),
        "coverage_summary": coverage_summary,
        "budget": {
            "max_total_bytes": budget.max_total_bytes,
            "max_total_records": budget.max_total_records,
            "max_total_seconds": budget.max_total_seconds,
            "used_bytes": total_bytes,
            "used_records": total_records,
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "discovery_elapsed_seconds": catalog["elapsed_seconds"],
            "exhausted_by": stop_code,
            "enforcement": "ONE_SHARED_PAGE_BUDGET_ACROSS_RESEARCH_COMPARE_SUBBATCHES_OF_20",
        },
        "summary": short_summary,
        "results": page_results,
    }
    if source_target_quality_report is not None:
        report = source_target_quality_report
        fast_page_only = any(
            item.status == "NOT_EVALUATED"
            and item.reason == "NOT_EVALUATED_FOR_FAST_PAGE"
            for item in report.candidates
        )
        reported_quality_candidates = (
            tuple(
                item for item in report.candidates if item.symbol in candidate_by_symbol
            )
            if fast_page_only
            else tuple(report.candidates)
        )
        not_evaluated_reason_counts: dict[str, int] = {}
        for item in report.candidates:
            if item.status == "NOT_EVALUATED" and item.reason is not None:
                not_evaluated_reason_counts[item.reason] = (
                    not_evaluated_reason_counts.get(item.reason, 0) + 1
                )
        result["source_target_quality"] = {
            "contract": "TDX_VIPDATA_TARGET_SESSION_SOURCE_QUALITY_NON_PIT_R1",
            "target_session": report.target_session.isoformat(),
            "package_id": report.package_id,
            "package_sha256": report.package_sha256,
            "candidate_total": report.candidate_total,
            "evaluated": report.evaluated,
            "target_present": report.target_present,
            "target_missing": report.target_missing,
            "read_failed": report.read_failed,
            "not_evaluated": report.not_evaluated,
            "candidate_partition_complete": report.candidate_total
            == report.evaluated + report.not_evaluated,
            "evaluation_partition_complete": report.evaluated
            == report.target_present + report.target_missing + report.read_failed,
            "evaluation_mode": "FAST_PAGE_ONLY"
            if fast_page_only
            else "FULL_TARGET_QUALITY",
            "candidate_rows_scope": "REQUESTED_PAGE"
            if fast_page_only
            else "FULL_CANDIDATE_POOL",
            "candidate_rows_returned": len(reported_quality_candidates),
            "candidate_rows_omitted": report.candidate_total
            - len(reported_quality_candidates),
            "not_evaluated_reason_counts": dict(
                sorted(not_evaluated_reason_counts.items())
            ),
            "official_valid_listing_denominator": "UNKNOWN_NOT_PROVED_BY_ZIP_FILENAMES",
            "by_market": {key: dict(value) for key, value in report.by_market.items()},
            "by_board": {key: dict(value) for key, value in report.by_board.items()},
            "field_quality": {
                key: dict(value) for key, value in report.field_quality.items()
            },
            "pages": [asdict(item) for item in report.pages],
            "candidates": [asdict(item) for item in reported_quality_candidates],
            "read_usage": {
                "bytes": report.bytes_read,
                "records": report.records_read,
                "elapsed_seconds": report.elapsed_seconds,
                "entry_read_elapsed_seconds": report.entry_read_elapsed_seconds,
                "consumer_capture_read_elapsed_seconds": report.consumer_capture_read_elapsed_seconds,
                "budget": asdict(report.budget),
                "zip_sha_recalculated_per_candidate": False,
            },
        }
    if profile_preview_denominators is not None:
        result["profile_preview_denominators"] = profile_preview_denominators
    return result
