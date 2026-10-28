"""只读TDX研究入口；复用日线解码和Core，不写数据库或业务文件。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
from importlib.resources import files
import json
import math
from pathlib import Path
import re
import time
from typing import Any, Iterable

from stock_a.domain import FieldValue, exact_number, unknown
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits


_CALENDAR_RESOURCE_PATTERN = re.compile(r"^cn_equity_sessions_([0-9]{4})\.json$")
_SYMBOL_PATTERN = re.compile(r"^(SH|SZ)[.]?([0-9]{6})$", re.IGNORECASE)
_MAX_COMPARE_SYMBOLS = 20
_DEFAULT_MAX_TOTAL_BYTES = 335544320
_DEFAULT_MAX_TOTAL_RECORDS = 4000000
_DEFAULT_MAX_TOTAL_SECONDS = 120.0
_CALENDAR_EVIDENCE_VERIFIED = "VERIFIED_EXCHANGE_NOTICES_CROSSCHECKED"
_RAW_FIELDS = ("open", "high", "low", "close", "amount", "volume")
_COMPARE_FIELDS = (
    "core.ma5", "core.ma10", "core.ma20", "core.ma60",
    "core.atr5", "core.atr20", "core.ret1", "core.ret5", "core.ret20",
    "core.amount_ratio5", "core.amount_ratio20",
    "core.volume_ratio5", "core.volume_ratio20",
)


class ResearchEntryError(ValueError):
    """研究入口输入或已声明能力不可用。"""

    def __init__(self, code: str, message: str, *, context: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.context = dict(context or {})


@dataclass(frozen=True)
class CompareBudget:
    """多股票单次调用的累计读取预算。"""

    max_total_bytes: int = _DEFAULT_MAX_TOTAL_BYTES
    max_total_records: int = _DEFAULT_MAX_TOTAL_RECORDS
    max_total_seconds: float = _DEFAULT_MAX_TOTAL_SECONDS

    def __post_init__(self) -> None:
        if type(self.max_total_bytes) is not int or self.max_total_bytes <= 0:
            raise ResearchEntryError("COMPARE_BUDGET_INVALID", "全批最大字节数必须为正整数")
        if type(self.max_total_records) is not int or self.max_total_records <= 0:
            raise ResearchEntryError("COMPARE_BUDGET_INVALID", "全批最大记录数必须为正整数")
        if isinstance(self.max_total_seconds, bool) or not isinstance(self.max_total_seconds, (int, float)):
            raise ResearchEntryError("COMPARE_BUDGET_INVALID", "全批最大耗时必须为有限正数")
        try:
            seconds = float(self.max_total_seconds)
        except (OverflowError, TypeError, ValueError) as error:
            raise ResearchEntryError("COMPARE_BUDGET_INVALID", "全批最大耗时必须可表示为有限正数") from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ResearchEntryError("COMPARE_BUDGET_INVALID", "全批最大耗时必须为有限正数")
        object.__setattr__(self, "max_total_seconds", seconds)


def _calendar_payload(resource_name: str, year: int) -> tuple[dict[str, Any], str, tuple[date, ...]]:
    try:
        raw = files("stock_a.research.calendars").joinpath(resource_name).read_bytes()
    except OSError as error:
        raise ResearchEntryError("CALENDAR_RESOURCE_UNAVAILABLE", "随包日历资源不可读取") from error
    canonical_raw = raw.replace(b"\r\n", b"\n")
    try:
        payload = json.loads(canonical_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历资源无法解析") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("sessions"), list):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历结构无效")
    if payload.get("evidence_status") != _CALENDAR_EVIDENCE_VERIFIED:
        raise ResearchEntryError("CALENDAR_UNVERIFIED", f"{year}年度日历证据未核验，已阻止计算")
    if payload.get("weekday_policy") != "WEEKDAYS_EXCEPT_LISTED_OFFICIAL_CLOSURES":
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", f"{year}年度日历缺少已登记的工作日规则")
    if payload.get("weekend_policy") != "周六周日休市；不因补班推断周末开市":
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", f"{year}年度日历缺少周末休市合同")
    revision = payload.get("revision")
    if not isinstance(revision, str) or not revision:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", f"{year}年度日历缺少版本标识")
    try:
        first = date.fromisoformat(payload["valid_from"])
        last = date.fromisoformat(payload["valid_through"])
        evidence_reviewed_at = date.fromisoformat(payload["evidence_reviewed_at"])
        source_snapshot_as_of = date.fromisoformat(payload["source_snapshot_as_of"])
        sessions = tuple(date.fromisoformat(day) for day in payload["sessions"])
        closures = payload["closed_weekday_ranges"]
        source_urls = payload["source_urls"]
    except (KeyError, TypeError, ValueError) as error:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历日期内容无效") from error
    if first != date(year, 1, 1) or last != date(year, 12, 31):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "年度日历覆盖边界必须完整且与文件年份一致")
    if not isinstance(source_urls, list) or not source_urls or any(
        not isinstance(url, str) or not url.startswith("https://") for url in source_urls
    ):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "年度日历必须列出可追溯的HTTPS证据来源")
    closed: set[date] = set()
    previous_end: date | None = None
    if not isinstance(closures, list):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "年度休市区间必须是有序列表")
    for item in closures:
        if not isinstance(item, list) or len(item) != 2:
            raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "年度休市区间必须包含起止日期")
        try:
            start, end = (date.fromisoformat(value) for value in item)
        except (TypeError, ValueError) as error:
            raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "年度休市区间日期无效") from error
        if start > end or start < first or end > last or (previous_end is not None and start <= previous_end):
            raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "年度休市区间越界、重叠或错序")
        closed.update(start + timedelta(days=offset) for offset in range((end - start).days + 1))
        previous_end = end
    if tuple(sorted(set(sessions))) != sessions:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历会话必须唯一且按日期递增")
    expected = tuple(
        first + timedelta(days=offset)
        for offset in range((last - first).days + 1)
        if (first + timedelta(days=offset)).weekday() < 5
        and (first + timedelta(days=offset)) not in closed
    )
    if sessions != expected:
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置会话与官方休市区间不闭合")
    if payload.get("calendar_id") != "CN_SSE_SZSE_EQUITY":
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历市场身份不匹配")
    return payload, hashlib.sha256(canonical_raw).hexdigest(), sessions


def _parse_target(value: str) -> date:
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ResearchEntryError("TARGET_DATE_INVALID", "目标会话必须使用YYYY-MM-DD有效日期") from error
    if result.isoformat() != value:
        raise ResearchEntryError("TARGET_DATE_INVALID", "目标会话必须使用规范YYYY-MM-DD格式")
    return result


def _load_calendar(target: date) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], str, tuple[date, ...]]:
    calendar_package = files("stock_a.research.calendars")
    resources: dict[int, str] = {}
    for item in calendar_package.iterdir():
        match = _CALENDAR_RESOURCE_PATTERN.fullmatch(item.name)
        if match:
            resources[int(match.group(1))] = item.name
    if target.year not in resources:
        raise ResearchEntryError("CALENDAR_NOT_COVERED", "没有已核验的年度资源覆盖目标日期；本次不推断交易日")
    available_years = sorted(year for year in resources if year <= target.year)
    selected_years_descending: list[int] = []
    year = target.year
    while year in resources:
        selected_years_descending.append(year)
        year -= 1
    selected_years = tuple(reversed(selected_years_descending))
    loaded: list[tuple[int, dict[str, Any], str, tuple[date, ...]]] = []
    for year in selected_years:
        payload, digest, sessions = _calendar_payload(resources[year], year)
        loaded.append((year, payload, digest, sessions))
    target_item = next(item for item in loaded if item[0] == target.year)
    target_sessions = target_item[3]
    if target not in target_sessions:
        raise ResearchEntryError("TARGET_NOT_TRADING_SESSION", "目标日期不是资源中已核验的交易会话")
    sessions = tuple(day for _, _, _, year_sessions in loaded for day in year_sessions)
    if any(left >= right for left, right in zip(sessions, sessions[1:])):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "跨年度日历资源重叠或错序")
    current_year_sessions_to_target = sum(
        day.year == target.year and day <= target for day in target_sessions
    )
    missing_intermediate_years = tuple(
        missing_year for missing_year in range(available_years[0], target.year)
        if missing_year not in resources
    )
    prior_calendar_available = target.year - 1 in selected_years
    core_lookback_complete = (
        current_year_sessions_to_target >= CORE_MAX_ACTUAL_LOOKBACK
        or prior_calendar_available
    )
    coverage_context = {
        "known_resource_years": available_years,
        "calculation_years": selected_years,
        "missing_intermediate_years": missing_intermediate_years,
        "resource_continuity_status": "GAP_DETECTED" if missing_intermediate_years else "CONTIGUOUS",
        "required_actual_sessions": CORE_MAX_ACTUAL_LOOKBACK,
        "current_year_sessions_to_target": current_year_sessions_to_target,
        "available_calendar_sessions_to_target": sum(day <= target for day in sessions),
        "prior_year_resource_available": prior_calendar_available,
        "core_lookback_complete": core_lookback_complete,
        "core_lookback_reason": None if core_lookback_complete else "CALENDAR_LOOKBACK_NOT_COVERED",
    }
    metadata = tuple({
        "calendar_id": payload["calendar_id"],
        "revision": payload["revision"],
        "year": year,
        "evidence_status": payload["evidence_status"],
        "evidence_reviewed_at": payload["evidence_reviewed_at"],
        "source_snapshot_as_of": payload["source_snapshot_as_of"],
        "coverage": [payload["valid_from"], payload["valid_through"]],
        "session_count": len(year_sessions),
        "resource_sha256": digest,
        "source_urls": payload["source_urls"],
    } for year, payload, digest, year_sessions in loaded)
    metadata = (*metadata[:-1], {**metadata[-1], "coverage_context": coverage_context})
    revisions = "+".join(f"{item['year']}:{item['revision']}" for item in metadata)
    return target_item[1], metadata, revisions, sessions


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


def _render_raw(decoded: DecodedDay | None, units: UnitContract) -> dict[str, Any]:
    if decoded is None:
        fields = {name: _render_field(unknown("TARGET_BAR_MISSING")) for name in _RAW_FIELDS}
    else:
        fields = {
            "open": _render_field(decoded.raw_open),
            "high": _render_field(decoded.raw_high),
            "low": _render_field(decoded.raw_low),
            "close": _render_field(decoded.raw_close),
            "amount": _render_field(decoded.amount),
            "volume": _render_field(decoded.volume),
        }
    fields["amount"]["unit_trust"] = {
        "status": "CALLER_DECLARED_ASSUMPTION",
        "unit_version": units.version,
        "amount_to_cny": str(units.amount_to_cny),
    }
    fields["volume"]["unit_trust"] = {
        "status": "CALLER_DECLARED_ASSUMPTION",
        "unit_version": units.version,
        "volume_to_shares": units.volume_to_shares,
    }
    return fields


def _field_relation(left: FieldValue, right: FieldValue) -> FieldValue:
    if left.quality != "OBSERVED":
        return unknown(left.reason or "UPSTREAM_UNKNOWN")
    if right.quality != "OBSERVED":
        return unknown(right.reason or "UPSTREAM_UNKNOWN")
    try:
        left_value = exact_number(left.value)
        right_value = exact_number(right.value)
    except (TypeError, ValueError, ZeroDivisionError):
        return unknown("INVALID_OBSERVATION_INPUT")
    if left_value > right_value:
        return FieldValue("ABOVE")
    if left_value < right_value:
        return FieldValue("BELOW")
    return FieldValue("EQUAL")


def _field_greater_than(left: FieldValue, right: FieldValue) -> FieldValue:
    relation = _field_relation(left, right)
    if relation.quality != "OBSERVED":
        return unknown(relation.reason or "UPSTREAM_UNKNOWN")
    return FieldValue(relation.value == "ABOVE")


def _apply_calendar_coverage_to_core(
    core: dict[str, FieldValue],
    history: tuple[CoreObservation, ...],
    coverage_context: dict[str, Any],
) -> None:
    if coverage_context.get("core_lookback_complete") or not history:
        return
    if any(row.state != "ACTUAL" for row in history):
        return
    for name, field in tuple(core.items()):
        if field.quality != "OBSERVED" and field.reason in {"SHORT_HISTORY", "SHORT_SESSION_HISTORY"}:
            core[name] = unknown("CALENDAR_LOOKBACK_NOT_COVERED")


def _observation_summary(
    *,
    symbol: str,
    target_session: date,
    source_last_session: date,
    raw_fields: dict[str, Any],
    raw_close: FieldValue,
    core: dict[str, FieldValue],
) -> dict[str, Any]:
    return {
        "contract": "RESEARCH_PREVIEW_NON_PIT",
        "scope": "OBJECTIVE_OBSERVATIONS_ONLY_NO_PROFILE_SEED_PRIORITY_OR_TRADE_SIGNAL",
        "requested_symbol": symbol,
        "target_session": target_session.isoformat(),
        "source_last_session": source_last_session.isoformat(),
        "price_coordinate": "RAW_UNADJUSTED",
        "fields": {
            "raw_close": raw_fields["close"],
            "ma5": _render_field(core["core.ma5"]),
            "ma20": _render_field(core["core.ma20"]),
            "ma60": _render_field(core["core.ma60"]),
            "atr20": _render_field(core["core.atr20"]),
            "ret5": _render_field(core["core.ret5"]),
            "ret20": _render_field(core["core.ret20"]),
            "amount": raw_fields["amount"],
            "volume": raw_fields["volume"],
            "close_above_ma20": _render_field(
                _field_greater_than(raw_close, core["core.ma20"])
            ),
            "moving_average_position": {
                "close_vs_ma5": _render_field(
                    _field_relation(raw_close, core["core.ma5"])
                ),
                "close_vs_ma20": _render_field(
                    _field_relation(raw_close, core["core.ma20"])
                ),
                "close_vs_ma60": _render_field(
                    _field_relation(raw_close, core["core.ma60"])
                ),
                "ma5_vs_ma20": _render_field(_field_relation(core["core.ma5"], core["core.ma20"])),
                "ma20_vs_ma60": _render_field(_field_relation(core["core.ma20"], core["core.ma60"])),
            },
        },
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
    """验证目标会话后保留可读Raw；仅在可用前态上调用现有Core。"""
    target = _parse_target(target_session)
    calendar_payload, calendar_metadata, calendar_revision, calendar_sessions = _load_calendar(target)
    coverage_context = calendar_metadata[-1]["coverage_context"]
    calendar_years = {item["year"] for item in calendar_metadata}
    session_set = set(calendar_sessions)
    normalized, market_directory, board = _normalize_symbol(symbol)
    code = normalized.split(".")[1]
    root, path = _source_path(source_roots, market_directory, code)
    selected: dict[date, DecodedDay] = {}
    try:
        with open_day_file(path, (root,), units, limits) as scan:
            for located in scan:
                day = located.decoded.session_date
                if day.year in calendar_years and day not in session_set:
                    raise ResearchEntryError(
                        "SOURCE_SESSION_NOT_IN_CALENDAR",
                        f"TDX来源在已核验年度包含非交易会话记录：{day.isoformat()}",
                        context={"byte_count": scan.progress[0], "record_count": scan.progress[1]},
                    )
                if day in session_set and day <= target:
                    selected[day] = located.decoded
            file_summary = scan.summary
    except DayReadError:
        raise
    except PermissionError as error:
        raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝读取配置的TDX日线文件") from error
    except OSError as error:
        raise ResearchEntryError("SOURCE_IO_ERROR", "只读TDX扫描遇到操作系统I/O错误") from error

    session_slice = tuple(day for day in calendar_sessions if day <= target)
    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_revision,
        sessions=session_slice,
    )
    history = tuple(_observation(day, normalized, selected.get(day)) for day in session_slice)
    core = calculate_core(history, official_calendar=bounded_calendar)
    _apply_calendar_coverage_to_core(core, history, coverage_context)
    target_bar = selected.get(target)
    unknown_core = sum(field.quality != "OBSERVED" for field in core.values())
    raw_fields = _render_raw(target_bar, units)
    raw_close = target_bar.raw_close if target_bar is not None else unknown("TARGET_BAR_MISSING")
    observation_summary = _observation_summary(
        symbol=normalized,
        target_session=target,
        source_last_session=file_summary.scan.last_date,
        raw_fields=raw_fields,
        raw_close=raw_close,
        core=core,
    )
    unknown_raw = sum(raw_fields[name]["quality"] != "OBSERVED" for name in _RAW_FIELDS)
    unit_dependent_core = {
        "core.amount_ratio5": {
            "unit_trust": "CALLER_DECLARED_ASSUMPTION",
            "interpretation": "同一窗口内的金额比率；金额单位倍率需在窗口内保持一致",
        },
        "core.amount_ratio20": {
            "unit_trust": "CALLER_DECLARED_ASSUMPTION",
            "interpretation": "同一窗口内的金额比率；金额单位倍率需在窗口内保持一致",
        },
        "core.liquidity_mean20": {
            "unit_trust": "CALLER_DECLARED_ASSUMPTION",
            "interpretation": "绝对金额均值使用调用方声明的amount_to_cny倍率",
        },
        "core.volume_ratio5": {
            "unit_trust": "CALLER_DECLARED_ASSUMPTION",
            "interpretation": "同一窗口内的数量比率；数量单位倍率需在窗口内保持一致",
        },
        "core.volume_ratio20": {
            "unit_trust": "CALLER_DECLARED_ASSUMPTION",
            "interpretation": "同一窗口内的数量比率；数量单位倍率需在窗口内保持一致",
        },
    }
    target_prices_usable = target_bar is not None and target_bar.prices.quality == "OBSERVED"
    return {
        "status": "AVAILABLE" if target_prices_usable else "PARTIAL_UNKNOWN",
        "requested_symbol": normalized,
        "board_scope": board,
        "analysis_scope": "TDX_SOURCE_SYMBOL_RAW_SERIES_ONLY",
        "target_session": target.isoformat(),
        "source_last_session": file_summary.scan.last_date.isoformat(),
        "source_first_session": file_summary.scan.first_date.isoformat(),
        "read_time": {
            "started_at": file_summary.scan.read_started_at.isoformat(),
            "completed_at": file_summary.scan.read_completed_at.isoformat(),
            "clock_role": "LOCAL_FILE_READ_TIME_ONLY",
        },
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
            "amount_quality_is_unit_authority": False,
            "volume_quality_is_unit_authority": False,
        },
        "calendar": {
            "calendar_id": calendar_payload["calendar_id"],
            "revision": calendar_revision,
            "resources": list(calendar_metadata),
            "coverage": [calendar_metadata[0]["coverage"][0], target.isoformat()],
            "evidence_status": _CALENDAR_EVIDENCE_VERIFIED,
            "resource_continuity_status": coverage_context["resource_continuity_status"],
            "known_resource_years": list(coverage_context["known_resource_years"]),
            "calculation_years": list(coverage_context["calculation_years"]),
            "missing_intermediate_years": list(coverage_context["missing_intermediate_years"]),
            "core_lookback_complete": coverage_context["core_lookback_complete"],
            "core_lookback_reason": coverage_context["core_lookback_reason"],
            "required_actual_sessions": coverage_context["required_actual_sessions"],
            "available_calendar_sessions_to_target": coverage_context["available_calendar_sessions_to_target"],
            "session_count_in_calculation": len(session_slice),
            "calculation_first_session": session_slice[0].isoformat(),
            "missing_bar_session_count": sum(day not in selected for day in session_slice),
            "missing_bar_state": "UNKNOWN; 不推断为停牌或上市前",
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
        "target_prices_usable": target_prices_usable,
        "target_raw_fields": raw_fields,
        "core_fields": {name: _render_field(value) for name, value in sorted(core.items())},
        "observation_summary": observation_summary,
        "core_field_disclosures": {
            "price_coordinate": "RAW_UNADJUSTED；身份和公司行为时钟未绑定",
            "unit_assumptions": unit_dependent_core,
        },
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
    budget: CompareBudget | None = None,
) -> dict[str, Any]:
    """同一日期逐代码只读计算，并对整批请求共享字节、记录和耗时预算。"""
    budget = budget or CompareBudget()
    if not isinstance(budget, CompareBudget):
        raise ResearchEntryError("COMPARE_BUDGET_INVALID", "缺少有效的全批累计预算")
    requested = tuple(symbols)
    if not requested or len(requested) > _MAX_COMPARE_SYMBOLS:
        raise ResearchEntryError("COMPARE_SIZE_INVALID", f"对比代码数必须为1至{_MAX_COMPARE_SYMBOLS}只")
    normalized = tuple(_normalize_symbol(item)[0] for item in requested)
    if len(set(normalized)) != len(normalized):
        raise ResearchEntryError("DUPLICATE_SYMBOL", "对比代码重复；拒绝静默去重")
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
            results.append({
                "status": "NOT_EVALUATED",
                "requested_symbol": symbol,
                "error_code": code,
                "message": "全批读取预算已耗尽；该请求保留在分母中且未读取",
                "read_usage": {"bytes": 0, "records": 0},
            })

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
            mark_not_evaluated((item, *normalized[len(results) + 1:]), stop_code)
            break
        remaining_bytes = budget.max_total_bytes - total_bytes
        remaining_records = budget.max_total_records - total_records
        remaining_seconds = budget.max_total_seconds - (time.monotonic() - started)
        if remaining_seconds <= 0:
            stop_code = "MAX_TOTAL_SECONDS_EXCEEDED"
            mark_not_evaluated((item, *normalized[len(results) + 1:]), stop_code)
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
                    or (type(source_size) is int and source_size > per_symbol_limits.max_bytes)
                ):
                    global_limit_code = "MAX_TOTAL_BYTES_EXCEEDED"
                elif per_symbol_limits.max_records < limits.max_records and (
                    item_records >= per_symbol_limits.max_records
                    or (type(source_record_limit) is int and type(source_size) is int
                        and source_size > source_record_limit)
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
            remaining = normalized[len(results):]
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
        "target_session": target_session,
        "denominators": {
            "requested": len(requested),
            "evaluated": len(requested) - not_evaluated,
            "available": available,
            "failed": failed,
            "not_evaluated": not_evaluated,
            "unknown": sum(
                item.get("status") == "PARTIAL_UNKNOWN"
                or (item.get("status") not in ("FAILED", "NOT_EVALUATED")
                    and item.get("identity", {}).get("status") != "RESOLVED")
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
                    name, {"value": None, "quality": "UNKNOWN_DATA", "reason": item.get("error_code", "NOT_AVAILABLE")}
                )
                for item in results
            }
            for name in _COMPARE_FIELDS
        },
        "results": results,
    }
