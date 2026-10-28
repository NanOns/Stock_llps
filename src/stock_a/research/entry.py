"""只读TDX研究入口；复用日线解码和Core，不写数据库或业务文件。"""
from __future__ import annotations

from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
from importlib.resources import files
import json
from pathlib import Path
import re
from typing import Any, Iterable

from stock_a.domain import FieldValue, unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.sources.tdx.day import DecodedDay, UnitContract
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits


_CALENDAR_RESOURCE = "cn_equity_sessions_2026.json"
_SYMBOL_PATTERN = re.compile(r"^(SH|SZ)[.]?([0-9]{6})$", re.IGNORECASE)
_MAX_COMPARE_SYMBOLS = 20
_RAW_FIELDS = ("open", "high", "low", "close", "amount", "volume")
_COMPARE_FIELDS = (
    "core.ma5", "core.ma10", "core.ma20", "core.ma60",
    "core.atr5", "core.atr20", "core.ret1", "core.ret5", "core.ret20",
    "core.amount_ratio5", "core.amount_ratio20",
    "core.volume_ratio5", "core.volume_ratio20",
)


class ResearchEntryError(ValueError):
    """研究入口输入或已声明能力不可用。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _calendar_resource() -> tuple[dict[str, Any], str]:
    try:
        raw = files("stock_a.research.calendars").joinpath(_CALENDAR_RESOURCE).read_bytes()
    except OSError as error:
        raise ResearchEntryError("CALENDAR_RESOURCE_UNAVAILABLE", "随包官方日历资源不可读取") from error
    canonical_raw = raw.replace(b"\r\n", b"\n")
    try:
        payload = json.loads(canonical_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历资源无法解析") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("sessions"), list):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历结构无效")
    return payload, hashlib.sha256(canonical_raw).hexdigest()


def _parse_target(value: str) -> date:
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ResearchEntryError("TARGET_DATE_INVALID", "目标会话必须使用YYYY-MM-DD有效日期") from error
    if result.isoformat() != value:
        raise ResearchEntryError("TARGET_DATE_INVALID", "目标会话必须使用规范YYYY-MM-DD格式")
    return result


def _load_calendar(target: date) -> tuple[dict[str, Any], str, tuple[date, ...]]:
    payload, digest = _calendar_resource()
    try:
        first = date.fromisoformat(payload["valid_from"])
        last = date.fromisoformat(payload["valid_through"])
        sessions = tuple(date.fromisoformat(day) for day in payload["sessions"])
    except (KeyError, TypeError, ValueError) as error:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历日期内容无效") from error
    if not first <= target <= last:
        raise ResearchEntryError("CALENDAR_NOT_COVERED", "官方日历资源未覆盖目标日期；本次不推断交易日")
    if target not in sessions:
        raise ResearchEntryError("TARGET_NOT_TRADING_SESSION", "目标日期不是资源中已核验的交易会话")
    if not sessions or tuple(sorted(set(sessions))) != sessions:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历必须唯一且按日期递增")
    return payload, digest, sessions


def _normalize_symbol(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise ResearchEntryError("SYMBOL_INVALID", "证券代码必须带SH或SZ市场前缀")
    match = _SYMBOL_PATTERN.fullmatch(value.strip())
    if not match:
        raise ResearchEntryError("SYMBOL_INVALID", "证券代码格式必须是SH.六位代码或SZ.六位代码")
    market, code = match.groups()
    market = market.upper()
    if market == "SH":
        board = "MAIN_BOARD" if code.startswith(("600", "601", "603", "605")) else None
        directory = "sh"
    else:
        if code.startswith(("000", "001", "002", "003")):
            board = "MAIN_BOARD"
        elif code.startswith(("300", "301")):
            board = "GEM"
        else:
            board = None
        directory = "sz"
    if board is None:
        raise ResearchEntryError("SYMBOL_OUT_OF_SCOPE", "代码类别不属于本入口的沪深主板/创业板范围")
    symbol = f"{market}.{code}"
    return symbol, directory, board


def _render_value(value: Any) -> Any:
    if isinstance(value, Fraction):
        with localcontext() as context:
            context.prec = 24
            decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        return {"exact": f"{value.numerator}/{value.denominator}", "decimal": format(decimal_value, "f")}
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, tuple):
        return [_render_value(item) for item in value]
    return str(value)


def _render_field(value: FieldValue) -> dict[str, Any]:
    if value.quality == "OBSERVED":
        return {"value": _render_value(value.value), "quality": value.quality, "reason": None}
    return {"value": None, "quality": value.quality, "reason": value.reason}


def _render_raw(decoded: DecodedDay | None) -> dict[str, Any]:
    if decoded is None:
        return {name: _render_field(unknown("TARGET_BAR_MISSING")) for name in _RAW_FIELDS}
    return {
        "open": _render_field(decoded.raw_open),
        "high": _render_field(decoded.raw_high),
        "low": _render_field(decoded.raw_low),
        "close": _render_field(decoded.raw_close),
        "amount": _render_field(decoded.amount),
        "volume": _render_field(decoded.volume),
    }


def _source_path(roots: Iterable[str], directory: str, code: str) -> tuple[Path, Path]:
    resolved_roots: list[Path] = []
    candidates: list[tuple[Path, Path]] = []
    for value in roots:
        try:
            root = Path(value).resolve(strict=True)
        except FileNotFoundError as error:
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根不存在") from error
        except PermissionError as error:
            raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝访问配置的TDX来源根") from error
        except OSError as error:
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根无法核验") from error
        if not root.is_dir():
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根不是目录")
        resolved_roots.append(root)
        candidate = root / "vipdoc" / directory / "lday" / f"{directory}{code}.day"
        try:
            source = candidate.resolve(strict=True)
        except FileNotFoundError:
            continue
        except PermissionError as error:
            raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX日线最终路径") from error
        except OSError as error:
            raise ResearchEntryError("SOURCE_IO_ERROR", "TDX日线最终路径核验发生I/O错误") from error
        try:
            source.relative_to(root)
        except ValueError as error:
            raise ResearchEntryError("SOURCE_PATH_OUTSIDE_ROOT", "TDX日线最终路径离开配置保护根") from error
        candidates.append((root, source))
    if not resolved_roots:
        raise ResearchEntryError("SOURCE_NOT_CONFIGURED", "没有配置只读TDX保护根")
    if not candidates:
        raise ResearchEntryError("SOURCE_NOT_FOUND", "配置保护根中没有该代码的TDX日线文件")
    if len(candidates) != 1:
        raise ResearchEntryError("SOURCE_ROOT_AMBIGUOUS", "多个配置来源根均含同一代码文件，拒绝自行选择")
    return candidates[0]


def _unknown_observation(session: date, reason: str) -> CoreObservation:
    values = {name: unknown(reason) for name in _RAW_FIELDS}
    return CoreObservation(
        session=session,
        state="UNKNOWN",
        coordinate_id=None,
        open=values["open"],
        high=values["high"],
        low=values["low"],
        close=values["close"],
        amount=values["amount"],
        volume=values["volume"],
    )


def _observation(session: date, symbol: str, decoded: DecodedDay | None) -> CoreObservation:
    if decoded is None:
        return _unknown_observation(session, "NO_TDX_BAR_FOR_OFFICIAL_SESSION")
    if decoded.prices.quality != "OBSERVED":
        return _unknown_observation(session, decoded.prices.reason or "PRICE_QUALITY_UNKNOWN")
    return CoreObservation(
        session=session,
        state="ACTUAL",
        coordinate_id=f"RAW:TDX_SOURCE_SYMBOL:{symbol}",
        open=decoded.raw_open,
        high=decoded.raw_high,
        low=decoded.raw_low,
        close=decoded.raw_close,
        amount=decoded.amount,
        volume=decoded.volume,
    )


def research_stock(
    *,
    symbol: str,
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
) -> dict[str, Any]:
    """读取一条来源代码序列并在显式的2026官方日历上调用现有Core。"""
    target = _parse_target(target_session)
    calendar_payload, calendar_digest, calendar_sessions = _load_calendar(target)
    normalized, market_directory, board = _normalize_symbol(symbol)
    code = normalized.split(".")[1]
    root, path = _source_path(source_roots, market_directory, code)
    selected: dict[date, DecodedDay] = {}
    try:
        with open_day_file(path, (root,), units, limits) as scan:
            for located in scan:
                day = located.decoded.session_date
                if calendar_sessions[0] <= day <= target:
                    selected[day] = located.decoded
            file_summary = scan.summary
    except DayReadError:
        raise
    except PermissionError as error:
        raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝读取配置的TDX日线文件") from error
    except OSError as error:
        raise ResearchEntryError("SOURCE_IO_ERROR", "只读TDX扫描遇到操作系统I/O错误") from error

    first_observed = min(selected) if selected else target
    session_slice = tuple(day for day in calendar_sessions if first_observed <= day <= target)
    if not session_slice:
        session_slice = (target,)
    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_payload["revision"],
        sessions=session_slice,
    )
    history = tuple(_observation(day, normalized, selected.get(day)) for day in session_slice)
    core = calculate_core(history, official_calendar=bounded_calendar)
    target_bar = selected.get(target)
    target_observation = history[-1]
    unknown_core = sum(field.quality != "OBSERVED" for field in core.values())
    raw_fields = _render_raw(target_bar)
    unknown_raw = sum(field["quality"] != "OBSERVED" for field in raw_fields.values())
    return {
        "status": "AVAILABLE" if target_observation.state == "ACTUAL" else "PARTIAL_UNKNOWN",
        "requested_symbol": normalized,
        "board_scope": board,
        "analysis_scope": "TDX_SOURCE_SYMBOL_RAW_SERIES_ONLY",
        "target_session": target.isoformat(),
        "source_last_session": file_summary.scan.last_date.isoformat(),
        "source_first_session": file_summary.scan.first_date.isoformat(),
        "source": {
            "configured_root": str(root),
            "final_path": file_summary.source.handle_path,
            "format_contract": "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
            "source_contract_status": "RESEARCH_LAYOUT_ONLY_AUTHORITY_AND_CAPTURE_CLOCK_UNRESOLVED",
            "raw_byte_sha256": file_summary.scan.raw_byte_sha256,
            "record_count": file_summary.scan.record_count,
            "byte_count": file_summary.scan.byte_count,
            "read_started_at": file_summary.scan.read_started_at.isoformat(),
            "read_completed_at": file_summary.scan.read_completed_at.isoformat(),
            "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
            "complete_read": file_summary.complete,
            "budgets": {
                "max_bytes": limits.max_bytes,
                "max_records": limits.max_records,
                "chunk_records": limits.chunk_records,
                "max_seconds": limits.max_seconds,
            },
        },
        "unit_contract": {
            "version": units.version,
            "amount_to_cny": str(units.amount_to_cny),
            "volume_to_shares": units.volume_to_shares,
            "binding_status": "CALLER_DECLARED_RESEARCH_ASSUMPTION",
        },
        "calendar": {
            "calendar_id": calendar_payload["calendar_id"],
            "revision": calendar_payload["revision"],
            "resource_sha256": calendar_digest,
            "source_urls": calendar_payload["source_urls"],
            "coverage": [calendar_payload["valid_from"], calendar_payload["valid_through"]],
            "session_count_in_calculation": len(session_slice),
            "missing_bar_session_count": sum(day not in selected for day in session_slice),
        },
        "identity": {
            "status": "SOURCE_SYMBOL_ONLY",
            "security_id": None,
            "reason": "OFFICIAL_STABLE_SECURITY_ID_SOURCE_NOT_BOUND",
        },
        "adjustment": {
            "coordinate": "RAW_UNADJUSTED",
            "status": "NOT_APPLIED_IDENTITY_AND_EVENT_TIMING_UNRESOLVED",
        },
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "target_bar_present": target_bar is not None,
        "target_prices_usable": target_observation.state == "ACTUAL",
        "target_raw_fields": raw_fields,
        "core_fields": {name: _render_field(value) for name, value in sorted(core.items())},
        "coverage": {
            "calendar_sessions": len(session_slice),
            "actual_price_bars": sum(row.state == "ACTUAL" for row in history),
            "unknown_sessions": sum(row.state == "UNKNOWN" for row in history),
            "unknown_core_fields": unknown_core,
            "unknown_target_raw_fields": unknown_raw,
        },
    }


def research_compare(
    *,
    symbols: Iterable[str],
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
) -> dict[str, Any]:
    """同一日期逐代码只读计算，保留请求、可用、失败与未知分母。"""
    requested = tuple(symbols)
    if not requested or len(requested) > _MAX_COMPARE_SYMBOLS:
        raise ResearchEntryError("COMPARE_SIZE_INVALID", f"对比代码数必须为1至{_MAX_COMPARE_SYMBOLS}只")
    normalized = tuple(_normalize_symbol(item)[0] for item in requested)
    if len(set(normalized)) != len(normalized):
        raise ResearchEntryError("DUPLICATE_SYMBOL", "对比代码重复；拒绝静默去重")
    target = _parse_target(target_session)
    _load_calendar(target)
    results: list[dict[str, Any]] = []
    for item in normalized:
        try:
            results.append(research_stock(
                symbol=item,
                target_session=target_session,
                source_roots=source_roots,
                units=units,
                limits=limits,
            ))
        except (ResearchEntryError, DayReadError) as error:
            code = error.code if isinstance(error, ResearchEntryError) else error.code
            results.append({
                "status": "FAILED",
                "requested_symbol": item,
                "error_code": code,
                "message": str(error),
            })
    available = sum(item["status"] == "AVAILABLE" for item in results)
    failed = sum(item["status"] == "FAILED" for item in results)
    unknown_symbols = sum(item["status"] == "PARTIAL_UNKNOWN" for item in results)
    unknown_identity = sum(
        item.get("status") != "FAILED" and item.get("identity", {}).get("status") != "RESOLVED"
        for item in results
    )
    unknown_adjustment = sum(
        item.get("status") != "FAILED" and item.get("adjustment", {}).get("status") != "APPLIED"
        for item in results
    )
    unknown_core_fields = sum(
        item.get("coverage", {}).get("unknown_core_fields", 0) for item in results
    )
    unknown_raw_fields = sum(
        item.get("coverage", {}).get("unknown_target_raw_fields", 0) for item in results
    )
    return {
        "status": "COMPLETE_WITH_DISCLOSURES" if failed == 0 and unknown_symbols == 0 else "PARTIAL",
        "target_session": target_session,
        "denominators": {
            "requested": len(requested),
            "available": available,
            "failed": failed,
            "unknown": sum(
                item.get("status") == "PARTIAL_UNKNOWN"
                or (item.get("status") != "FAILED" and item.get("identity", {}).get("status") != "RESOLVED")
                for item in results
            ),
            "partial_unknown": unknown_symbols,
            "unknown_identity": unknown_identity,
            "unknown_adjustment": unknown_adjustment,
            "unknown_core_fields": unknown_core_fields,
            "unknown_raw_fields": unknown_raw_fields,
        },
        "comparison_fields": {
            name: {
                item["requested_symbol"]: item.get("core_fields", {}).get(
                    name, {"value": None, "quality": "UNKNOWN_DATA", "reason": item.get("error_code", "NOT_AVAILABLE")}
                )
                for item in results
            }
            for name in _COMPARE_FIELDS
        },
        "results": results,
    }
