"""显式本地TDX只读诊断入口；不连接数据库或参与股票价格生产。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import time
from collections import Counter
from typing import Any, Iterable

from stock_a.domain import unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.entry_context.windows import CompareBudget, _MAX_COMPARE_SYMBOLS
from stock_a.research.entry_core.fields import (
    _apply_calendar_coverage_to_core,
    _render_field,
    _render_raw,
)
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits


def _source_file(
    source_roots: Iterable[str], *, directory: str, code: str
) -> tuple[Path, Path]:
    if isinstance(source_roots, (str, bytes, Path)):
        raise ResearchEntryError(
            "SOURCE_PATH_INVALID", "source_roots必须是显式根路径序列"
        )
    try:
        roots = tuple(source_roots)
    except TypeError as error:
        raise ResearchEntryError(
            "SOURCE_PATH_INVALID", "source_roots必须是显式根路径序列"
        ) from error
    if not roots:
        raise ResearchEntryError("SOURCE_NOT_CONFIGURED", "没有绑定本地TDX只读来源根")

    candidates: list[tuple[Path, Path]] = []
    for value in roots:
        try:
            root = Path(value).resolve(strict=True)
        except PermissionError as error:
            raise ResearchEntryError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝核验本地TDX来源根"
            ) from error
        except (OSError, RuntimeError, TypeError, ValueError) as error:
            raise ResearchEntryError(
                "SOURCE_ROOT_INVALID", "本地TDX来源根无法核验"
            ) from error
        if not root.is_dir():
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "本地TDX来源根不是目录")
        candidate = root / "vipdoc" / directory / "lday" / f"{directory}{code}.day"
        try:
            resolved = candidate.resolve(strict=True)
        except FileNotFoundError:
            continue
        except PermissionError as error:
            raise ResearchEntryError(
                "SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX日线最终路径"
            ) from error
        except (OSError, RuntimeError, ValueError) as error:
            raise ResearchEntryError(
                "SOURCE_IO_ERROR", "TDX日线最终路径核验发生I/O错误"
            ) from error
        try:
            resolved.relative_to(root)
        except ValueError as error:
            raise ResearchEntryError(
                "SOURCE_PATH_OUTSIDE_ROOT", "TDX日线最终路径离开配置只读根"
            ) from error
        candidates.append((root, resolved))
    if not candidates:
        raise ResearchEntryError("SOURCE_NOT_FOUND", "配置根中没有该代码的TDX日线文件")
    if len(candidates) != 1:
        raise ResearchEntryError(
            "SOURCE_ROOT_AMBIGUOUS", "多个配置根含同一代码文件；拒绝自行选择"
        )
    return candidates[0]


def _availability(fields: dict[str, dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(field.get("quality", "UNKNOWN") for field in fields.values())
    reasons = Counter(
        field.get("reason") or "QUALITY_UNKNOWN"
        for field in fields.values()
        if field.get("quality", "UNKNOWN") != "OBSERVED"
    )
    observed = counts.get("OBSERVED", 0)
    return {
        "status": "AVAILABLE" if observed == len(fields) else "PARTIAL_UNKNOWN",
        "observed_fields": observed,
        "total_fields": len(fields),
        "unknown_reasons": dict(reasons),
    }


def research_local_tdx_stock_diagnostic(
    *,
    symbol: str,
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    max_history_sessions: int = 60,
) -> dict[str, Any]:
    """对一个显式来源代码生成RAW离线观察和Core诊断。"""
    target = _parse_target(target_session)
    normalized, directory, board = _normalize_symbol(symbol)
    if type(max_history_sessions) is not int or not 1 <= max_history_sessions <= 75:
        raise ResearchEntryError(
            "LOCAL_TDX_SESSION_LIMIT_INVALID", "本地TDX历史会话上限须为1至75"
        )
    root, source_path = _source_file(
        source_roots, directory=directory, code=normalized.split(".")[1]
    )
    calendar_payload, metadata, calendar_revision, official_sessions = _load_calendar(
        target
    )
    session_dates = tuple(day for day in official_sessions if day <= target)[
        -max_history_sessions:
    ]
    if target not in session_dates:
        raise ResearchEntryError(
            "TARGET_SESSION_OUTSIDE_LOCAL_WINDOW", "目标会话不在本地诊断窗口内"
        )
    calendar_years = {item["year"] for item in metadata}
    official_set = set(official_sessions)
    selected: dict[date, Any] = {}
    future_rows_excluded = 0
    try:
        with open_day_file(source_path, (root,), units, limits) as scan:
            for located in scan:
                day = located.decoded.session_date
                if day.year in calendar_years and day not in official_set:
                    raise ResearchEntryError(
                        "SOURCE_SESSION_NOT_IN_CALENDAR",
                        f"TDX来源在已核验年度包含非交易会话：{day.isoformat()}",
                        context={
                            "byte_count": scan.progress[0],
                            "record_count": scan.progress[1],
                        },
                    )
                if day > target:
                    future_rows_excluded += 1
                elif day in session_dates:
                    selected[day] = located.decoded
            file_summary = scan.summary
            summary = file_summary.scan
    except DayReadError:
        raise
    except PermissionError as error:
        raise ResearchEntryError(
            "SOURCE_ACCESS_DENIED", "操作系统拒绝读取本地TDX日线文件"
        ) from error
    except OSError as error:
        raise ResearchEntryError(
            "SOURCE_IO_ERROR", "读取本地TDX日线文件失败"
        ) from error

    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    history = tuple(
        CoreObservation(
            session=day,
            state="ACTUAL" if day in selected else "UNKNOWN",
            coordinate_id=coordinate_id if day in selected else None,
            open=selected[day].raw_open if day in selected else unknown("NO_TDX_BAR"),
            high=selected[day].raw_high if day in selected else unknown("NO_TDX_BAR"),
            low=selected[day].raw_low if day in selected else unknown("NO_TDX_BAR"),
            close=selected[day].raw_close if day in selected else unknown("NO_TDX_BAR"),
            amount=selected[day].amount if day in selected else unknown("NO_TDX_BAR"),
            volume=selected[day].volume if day in selected else unknown("NO_TDX_BAR"),
        )
        for day in session_dates
    )
    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_revision,
        sessions=session_dates,
    )
    core = calculate_core(history, official_calendar=bounded_calendar)
    _apply_calendar_coverage_to_core(core, history, metadata[-1]["coverage_context"])
    raw = _render_raw(selected.get(target), units)
    rendered_core = {name: _render_field(value) for name, value in core.items()}
    target_raw_fields = {
        name: raw[name] for name in ("open", "high", "low", "close", "amount", "volume")
    }
    raw_availability = _availability(target_raw_fields)
    core_availability = _availability(rendered_core)
    read_started = summary.read_started_at.isoformat()
    read_completed = summary.read_completed_at.isoformat()
    source = {
        "provider": "TDX_LOCAL_EXPLICIT_OFFLINE_DIAGNOSTIC",
        "configured_root": str(root),
        "final_path": str(source_path),
        "byte_count": summary.byte_count,
        "record_count": summary.record_count,
        "complete_read": summary.complete,
        "read_started_at": read_started,
        "read_completed_at": read_completed,
        "clock_role": "LOCAL_FILE_READ_TIME_ONLY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "production_price_source": False,
        "budgets": {
            "max_bytes": limits.max_bytes,
            "max_records": limits.max_records,
            "chunk_records": limits.chunk_records,
            "max_seconds": limits.max_seconds,
            "max_history_sessions": max_history_sessions,
        },
    }
    target_bar = selected.get(target)
    status = (
        "AVAILABLE"
        if target_bar is not None and target_bar.prices.quality == "OBSERVED"
        else "PARTIAL_UNKNOWN"
    )
    return {
        "status": status,
        "status_explanation": "显式本地TDX离线RAW诊断；不代表BaoStock权威、历史PIT或生产价格能力。",
        "requested_symbol": normalized,
        "board_scope": board,
        "target_session": target.isoformat(),
        "source_first_session": summary.first_date.isoformat(),
        "source_last_session": summary.last_date.isoformat(),
        "read_time": {"started_at": read_started, "completed_at": read_completed},
        "analysis_scope": "LOCAL_TDX_RAW_OFFLINE_DIAGNOSTIC",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "adjustment": {"coordinate": "RAW_UNADJUSTED", "status": "NOT_APPLIED"},
        "raw": raw,
        "core": rendered_core,
        "raw_availability": raw_availability,
        "core_availability": core_availability,
        "history_window": {
            "calendar_session_count": len(session_dates),
            "actual_price_bars": sum(row.state == "ACTUAL" for row in history),
            "missing_bar_sessions": sum(row.state == "UNKNOWN" for row in history),
            "future_rows_used": False,
            "calendar_revision": calendar_revision,
        },
        "canonical_selection": {
            "selection_status": "LOCAL_TDX_OFFLINE_WINDOW_SELECTED",
            "target_bar_status": "PRESENT"
            if target_bar is not None
            else "TARGET_MISSING",
            "target_session": target.isoformat(),
            "calendar_revision": calendar_revision,
            "source": source,
            "fields": target_raw_fields,
            "future_rows_excluded": future_rows_excluded,
        },
        "source": source,
        "source_file_read": True,
        "network_access": False,
        "database_read": False,
        "database_write": False,
    }


def research_local_tdx_compare_diagnostic(
    *,
    symbols: Iterable[str],
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    budget: CompareBudget,
) -> dict[str, Any]:
    """按稳定全批预算依次读取显式本地TDX来源文件。"""
    requested = tuple(symbols)
    if not requested or len(requested) > _MAX_COMPARE_SYMBOLS:
        raise ResearchEntryError(
            "COMPARE_SIZE_INVALID", f"对比代码数必须为1至{_MAX_COMPARE_SYMBOLS}只"
        )
    normalized = tuple(_normalize_symbol(item)[0] for item in requested)
    if len(set(normalized)) != len(normalized):
        raise ResearchEntryError("DUPLICATE_SYMBOL", "对比代码重复；拒绝静默去重")
    if not isinstance(budget, CompareBudget):
        raise ResearchEntryError("COMPARE_BUDGET_INVALID", "全批读取预算无效")
    roots = tuple(source_roots)
    started = time.monotonic()
    results: list[dict[str, Any]] = []
    used_bytes = 0
    used_records = 0
    for index, symbol in enumerate(normalized):
        remaining_seconds = budget.max_total_seconds - (time.monotonic() - started)
        remaining_bytes = budget.max_total_bytes - used_bytes
        remaining_records = budget.max_total_records - used_records
        if remaining_seconds <= 0 or remaining_bytes <= 0 or remaining_records <= 0:
            results.extend(
                {
                    "status": "NOT_EVALUATED",
                    "requested_symbol": item,
                    "error_code": "COMPARE_TOTAL_BUDGET_EXHAUSTED",
                    "read_usage": {"bytes": 0, "records": 0},
                }
                for item in normalized[index:]
            )
            break
        per_symbol = ReadLimits(
            min(limits.max_bytes, remaining_bytes),
            min(limits.max_records, remaining_records),
            limits.chunk_records,
            min(float(limits.max_seconds), remaining_seconds),
        )
        try:
            item = research_local_tdx_stock_diagnostic(
                symbol=symbol,
                target_session=target_session,
                source_roots=roots,
                units=units,
                limits=per_symbol,
            )
            read_usage = {
                "bytes": item["source"]["byte_count"],
                "records": item["source"]["record_count"],
            }
            item["read_usage"] = read_usage
            used_bytes += read_usage["bytes"]
            used_records += read_usage["records"]
            results.append(item)
        except (ResearchEntryError, DayReadError) as error:
            context = getattr(error, "context", {})
            byte_count = context.get("byte_count", 0)
            record_count = context.get(
                "records_attempted", context.get("record_count", 0)
            )
            if type(byte_count) is not int or byte_count < 0:
                byte_count = 0
            if type(record_count) is not int or record_count < 0:
                record_count = 0
            used_bytes += byte_count
            used_records += record_count
            results.append(
                {
                    "status": "FAILED",
                    "requested_symbol": symbol,
                    "error_code": error.code,
                    "message": str(error),
                    "read_usage": {"bytes": byte_count, "records": record_count},
                }
            )
    counts = {
        status: sum(item["status"] == status for item in results)
        for status in ("AVAILABLE", "PARTIAL_UNKNOWN", "FAILED", "NOT_EVALUATED")
    }
    summary = []
    for item in results:
        raw_symbol = item.get("requested_symbol")
        if not isinstance(raw_symbol, str):
            raise ResearchEntryError(
                "COMPARE_RESULT_SYMBOL_MISSING",
                "多代码离线诊断结果缺少来源代码，拒绝生成不完整行。",
            )
        symbol, market_directory, board = _normalize_symbol(raw_symbol)
        raw = item.get("raw", {})
        core = item.get("core", {})
        summary.append(
            {
                "symbol": symbol,
                "market": "SH" if market_directory == "sh" else "SZ",
                "board": board,
                "status": item["status"],
                "error_code": item.get("error_code"),
                "source_last_session": item.get("source_last_session"),
                "source_bar_status": item.get("canonical_selection", {}).get(
                    "target_bar_status"
                ),
                "source_bar_quality": {
                    "price_quality": raw.get("open", {}).get("quality"),
                    "amount_quality": raw.get("amount", {}).get("quality"),
                    "volume_quality": raw.get("volume", {}).get("quality"),
                },
                "raw_close": raw.get("close"),
                "ma5": core.get("ma5"),
                "ma20": core.get("ma20"),
                "ma60": core.get("ma60"),
                "atr20": core.get("atr20"),
                "ret5": core.get("ret5"),
                "ret20": core.get("ret20"),
                "close_vs_ma20": None,
                "amount": raw.get("amount"),
                "volume": raw.get("volume"),
                "raw_availability": item.get("raw_availability"),
                "core_availability": item.get("core_availability"),
            }
        )
    complete = (
        counts["FAILED"] == 0
        and counts["PARTIAL_UNKNOWN"] == 0
        and counts["NOT_EVALUATED"] == 0
    )
    return {
        "status": "COMPLETE_WITH_DISCLOSURES" if complete else "PARTIAL",
        "status_explanation": "显式本地TDX离线RAW诊断；不代表BaoStock权威、历史PIT或生产价格能力。",
        "target_session": _parse_target(target_session).isoformat(),
        "requested_symbols": list(normalized),
        "counts": counts,
        "budget": {
            "used_bytes": used_bytes,
            "used_records": used_records,
            "max_total_bytes": budget.max_total_bytes,
            "max_total_records": budget.max_total_records,
            "elapsed_seconds": round(time.monotonic() - started, 6),
        },
        "summary": summary,
        "database_read": False,
        "database_write": False,
        "network_access": False,
    }
