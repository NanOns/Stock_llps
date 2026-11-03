"""Stock A研究comparison职责模块。"""

from __future__ import annotations

import time
from typing import Any, Iterable, Mapping, Sequence
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.sources.tdx.vipdata import VipdataEntryCapture, VipdataTargetQualityReport

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.windows import CompareBudget
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_core.single_stock import research_stock
from stock_a.research.entry_context.source_discovery import (
    _aggregate_availability,
    _normalize_symbol,
)
from stock_a.research.entry_context.windows import _MAX_COMPARE_SYMBOLS
from stock_a.research.entry_core.profile_preview import _COMPARE_FIELDS


def research_compare(
    *,
    symbols: Iterable[str],
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    budget: CompareBudget | None = None,
    profile_contract: dict[str, Any] | None = None,
    foundation_contract: dict[str, Any] | None = None,
    source_captures: Mapping[str, VipdataEntryCapture] | None = None,
) -> dict[str, Any]:
    """同一日期逐代码只读计算，并对整批请求共享字节、记录和耗时预算。"""
    budget = budget or CompareBudget()
    if not isinstance(budget, CompareBudget):
        raise ResearchEntryError("COMPARE_BUDGET_INVALID", "缺少有效的全批累计预算")
    requested = tuple(symbols)
    if not requested or len(requested) > _MAX_COMPARE_SYMBOLS:
        raise ResearchEntryError(
            "COMPARE_SIZE_INVALID", f"对比代码数必须为1至{_MAX_COMPARE_SYMBOLS}只"
        )
    normalized = tuple(_normalize_symbol(item)[0] for item in requested)
    if len(set(normalized)) != len(normalized):
        raise ResearchEntryError("DUPLICATE_SYMBOL", "对比代码重复；拒绝静默去重")
    if source_captures is not None:
        if (
            not isinstance(source_captures, Mapping)
            or any(
                not isinstance(key, str) or not isinstance(value, VipdataEntryCapture)
                for key, value in source_captures.items()
            )
            or not set(normalized).issubset(source_captures)
        ):
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获没有覆盖本次对比请求"
            )
        if any(
            source_captures[symbol].target_session.isoformat() != target_session
            for symbol in normalized
        ):
            raise ResearchEntryError(
                "VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获目标日与本次对比不一致"
            )
    roots = tuple(source_roots)
    target = _parse_target(target_session)
    started = time.monotonic()
    _load_calendar(target)
    results: list[dict[str, Any]] = []
    total_bytes = 0
    total_records = 0
    stop_code: str | None = None

    def mark_not_evaluated(items: Iterable[str], code: str) -> None:
        for symbol in items:
            results.append(
                {
                    "status": "NOT_EVALUATED",
                    "requested_symbol": symbol,
                    "error_code": code,
                    "message": "全批读取预算已耗尽；该请求保留在分母中且未读取",
                    "read_usage": {"bytes": 0, "records": 0},
                }
            )

    def usage_from_error(error: ResearchEntryError | DayReadError) -> tuple[int, int]:
        context = getattr(error, "context", {})
        byte_count = context.get("byte_count", 0)
        record_count = context.get("records_attempted", context.get("record_count", 0))
        if type(byte_count) is not int or byte_count < 0:
            byte_count = 0
        if type(record_count) is not int or record_count < 0:
            record_count = 0
        return byte_count, record_count

    def exhausted_code() -> str | None:
        if time.monotonic() - started >= budget.max_total_seconds:
            return "MAX_TOTAL_SECONDS_EXCEEDED"
        if total_bytes >= budget.max_total_bytes:
            return "MAX_TOTAL_BYTES_EXCEEDED"
        if total_records >= budget.max_total_records:
            return "MAX_TOTAL_RECORDS_EXCEEDED"
        return None

    for item in normalized:
        stop_code = exhausted_code()
        if stop_code:
            mark_not_evaluated((item, *normalized[len(results) + 1 :]), stop_code)
            break
        remaining_bytes = budget.max_total_bytes - total_bytes
        remaining_records = budget.max_total_records - total_records
        remaining_seconds = budget.max_total_seconds - (time.monotonic() - started)
        if remaining_seconds <= 0:
            stop_code = "MAX_TOTAL_SECONDS_EXCEEDED"
            mark_not_evaluated((item, *normalized[len(results) + 1 :]), stop_code)
            break
        per_symbol_limits = ReadLimits(
            min(limits.max_bytes, remaining_bytes),
            min(limits.max_records, remaining_records),
            limits.chunk_records,
            min(float(limits.max_seconds), remaining_seconds),
        )
        try:
            result = research_stock(
                symbol=item,
                target_session=target_session,
                source_roots=roots,
                units=units,
                limits=per_symbol_limits,
                profile_contract=profile_contract,
                foundation_contract=foundation_contract,
                vipdata_capture=source_captures.get(item)
                if source_captures is not None
                else None,
            )
            item_bytes = result["source"]["byte_count"]
            item_records = result["source"]["record_count"]
            total_bytes += item_bytes
            total_records += item_records
            result["read_usage"] = {"bytes": item_bytes, "records": item_records}
            if time.monotonic() - started > budget.max_total_seconds:
                result = {
                    "status": "FAILED",
                    "requested_symbol": item,
                    "error_code": "MAX_TOTAL_SECONDS_EXCEEDED",
                    "message": "完成当前扫描时已超过全批耗时预算；结果不纳入已评价输出",
                    "read_usage": {"bytes": item_bytes, "records": item_records},
                }
                stop_code = "MAX_TOTAL_SECONDS_EXCEEDED"
            results.append(result)
        except (ResearchEntryError, DayReadError) as error:
            code = error.code if isinstance(error, ResearchEntryError) else error.code
            item_bytes, item_records = usage_from_error(error)
            total_bytes += item_bytes
            total_records += item_records
            global_limit_code = None
            if isinstance(error, DayReadError) and error.code == "READ_LIMIT_EXCEEDED":
                source_size = error.context.get("source_size_bytes", 0)
                source_record_limit = error.context.get("source_record_limit_bytes", 0)
                if per_symbol_limits.max_bytes < limits.max_bytes and (
                    item_bytes >= per_symbol_limits.max_bytes
                    or (
                        type(source_size) is int
                        and source_size > per_symbol_limits.max_bytes
                    )
                ):
                    global_limit_code = "MAX_TOTAL_BYTES_EXCEEDED"
                elif per_symbol_limits.max_records < limits.max_records and (
                    item_records >= per_symbol_limits.max_records
                    or (
                        type(source_record_limit) is int
                        and type(source_size) is int
                        and source_size > source_record_limit
                    )
                ):
                    global_limit_code = "MAX_TOTAL_RECORDS_EXCEEDED"
            elif isinstance(error, DayReadError) and error.code == "READ_TIMEOUT":
                if per_symbol_limits.max_seconds < float(limits.max_seconds):
                    global_limit_code = "MAX_TOTAL_SECONDS_EXCEEDED"
            result = {
                "status": "FAILED",
                "requested_symbol": item,
                "error_code": global_limit_code or code,
                "message": str(error),
                "read_usage": {"bytes": item_bytes, "records": item_records},
            }
            if global_limit_code:
                result["source_error_code"] = code
                stop_code = global_limit_code
            results.append(result)
        if stop_code:
            remaining = normalized[len(results) :]
            mark_not_evaluated(remaining, stop_code)
            break
    available = sum(item["status"] == "AVAILABLE" for item in results)
    failed = sum(item["status"] == "FAILED" for item in results)
    unknown_symbols = sum(item["status"] == "PARTIAL_UNKNOWN" for item in results)
    not_evaluated = sum(item["status"] == "NOT_EVALUATED" for item in results)
    classified = available + failed + unknown_symbols + not_evaluated
    if classified != len(requested):
        raise ResearchEntryError(
            "COMPARE_CLASSIFICATION_INCOMPLETE",
            "多标的结果未能按可用、失败、部分未知和未评价完整分区",
        )
    unknown_identity = sum(
        item.get("status") not in ("FAILED", "NOT_EVALUATED")
        and item.get("identity", {}).get("status") != "RESOLVED"
        for item in results
    )
    unknown_adjustment = sum(
        item.get("status") not in ("FAILED", "NOT_EVALUATED")
        and item.get("adjustment", {}).get("status") != "APPLIED"
        for item in results
    )
    unknown_core_fields = sum(
        item.get("coverage", {}).get("unknown_core_fields", 0) for item in results
    )
    unknown_raw_fields = sum(
        item.get("coverage", {}).get("unknown_target_raw_fields", 0) for item in results
    )
    elapsed = time.monotonic() - started
    complete = failed == 0 and unknown_symbols == 0 and not_evaluated == 0
    return {
        "status": "COMPLETE_WITH_DISCLOSURES" if complete else "PARTIAL",
        "status_explanation": "COMPLETE_WITH_DISCLOSURES仅表示请求对象已处理且披露已附上；不表示所有Raw/Core字段均已知。请分别检查raw_availability、core_availability及逐字段quality/reason。",
        "target_session": target_session,
        "denominators": {
            "requested": len(requested),
            "evaluated": len(requested) - not_evaluated,
            "available": available,
            "failed": failed,
            "not_evaluated": not_evaluated,
            "unknown": sum(
                item.get("status") == "PARTIAL_UNKNOWN"
                or (
                    item.get("status") not in ("FAILED", "NOT_EVALUATED")
                    and item.get("identity", {}).get("status") != "RESOLVED"
                )
                for item in results
            ),
            "partial_unknown": unknown_symbols,
            "unknown_identity": unknown_identity,
            "unknown_adjustment": unknown_adjustment,
            "unknown_core_fields": unknown_core_fields,
            "unknown_raw_fields": unknown_raw_fields,
        },
        "classification": {
            "available": available,
            "failed": failed,
            "partial_unknown": unknown_symbols,
            "not_evaluated": not_evaluated,
            "classified": classified,
            "requested": len(requested),
            "partition_complete": True,
        },
        "raw_availability": _aggregate_availability(results, "raw_availability"),
        "core_availability": _aggregate_availability(results, "core_availability"),
        "budget": {
            "max_total_bytes": budget.max_total_bytes,
            "max_total_records": budget.max_total_records,
            "max_total_seconds": budget.max_total_seconds,
            "byte_scope": "TDX_SOURCE_BYTES_ONLY",
            "record_scope": "TDX_DAY_RECORDS_ONLY",
            "used_bytes": total_bytes,
            "used_records": total_records,
            "elapsed_seconds": round(elapsed, 6),
            "enforcement": "PER_FILE_SCANNER_CHECKS_AND_BETWEEN_SYMBOLS",
            "exhausted_by": stop_code,
        },
        "comparison_fields": {
            name: {
                item["requested_symbol"]: item.get("core_fields", {}).get(
                    name,
                    {
                        "value": None,
                        "quality": "UNKNOWN_DATA",
                        "reason": item.get("error_code", "NOT_AVAILABLE"),
                    },
                )
                for item in results
            }
            for name in _COMPARE_FIELDS
        },
        "results": results,
    }
