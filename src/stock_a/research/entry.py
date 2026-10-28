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
import os
from pathlib import Path
import re
import stat
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
_DAY_FILENAME_PATTERN = re.compile(r"^(SH|SZ)([0-9]{6})[.]DAY$", re.IGNORECASE)
_MAX_COMPARE_SYMBOLS = 20
_MAX_UNIVERSE_PAGE_SIZE = 100
_DEFAULT_UNIVERSE_PAGE_SIZE = 20
_DEFAULT_MAX_DIRECTORY_ENTRIES = 50000
_DEFAULT_MAX_DISCOVERY_SECONDS = 30.0
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


@dataclass(frozen=True)
class UniverseDiscoveryBudget:
    """发现来源候选目录时的总目录项与耗时上限。"""

    max_directory_entries: int = _DEFAULT_MAX_DIRECTORY_ENTRIES
    max_seconds: float = _DEFAULT_MAX_DISCOVERY_SECONDS

    def __post_init__(self) -> None:
        if type(self.max_directory_entries) is not int or self.max_directory_entries <= 0:
            raise ResearchEntryError("UNIVERSE_BUDGET_INVALID", "来源目录项上限必须为正整数")
        if isinstance(self.max_seconds, bool) or not isinstance(self.max_seconds, (int, float)):
            raise ResearchEntryError("UNIVERSE_BUDGET_INVALID", "来源发现耗时必须为有限正数")
        try:
            seconds = float(self.max_seconds)
        except (OverflowError, TypeError, ValueError) as error:
            raise ResearchEntryError("UNIVERSE_BUDGET_INVALID", "来源发现耗时必须可表示为有限正数") from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ResearchEntryError("UNIVERSE_BUDGET_INVALID", "来源发现耗时必须为有限正数")
        object.__setattr__(self, "max_seconds", seconds)


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


def _availability_record(fields: dict[str, Any], *, subject: str) -> dict[str, Any]:
    total = len(fields)
    observed = sum(item.get("quality") == "OBSERVED" for item in fields.values())
    unknown_reasons: dict[str, int] = {}
    for item in fields.values():
        if item.get("quality") != "OBSERVED":
            reason = item.get("reason") or "UNKNOWN_REASON_UNSPECIFIED"
            unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
    if observed == total:
        status = "AVAILABLE"
        message = f"{subject}字段全部可用"
    elif observed == 0:
        status = "UNAVAILABLE"
        message = f"{subject}字段均不可用；逐项原因见unknown_reasons"
    else:
        status = "PARTIAL_UNKNOWN"
        message = f"{subject}部分字段可用；本状态不表示全部指标已知"
    return {
        "status": status,
        "observed_fields": observed,
        "unknown_fields": total - observed,
        "total_fields": total,
        "unknown_reasons": dict(sorted(unknown_reasons.items())),
        "message": message,
    }


def _aggregate_availability(results: list[dict[str, Any]], field_name: str) -> dict[str, Any]:
    counts = {
        "AVAILABLE": 0,
        "PARTIAL_UNKNOWN": 0,
        "UNAVAILABLE": 0,
        "FAILED": 0,
        "NOT_EVALUATED": 0,
    }
    for item in results:
        item_status = item.get("status")
        if item_status == "FAILED":
            counts["FAILED"] += 1
        elif item_status == "NOT_EVALUATED":
            counts["NOT_EVALUATED"] += 1
        else:
            availability = item.get(field_name, {})
            status = availability.get("status")
            if status in counts:
                counts[status] += 1
            else:
                counts["UNAVAILABLE"] += 1
    return {
        "counts": counts,
        "requested": len(results),
        "message": "仅汇总本次已选页对象；AVAILABLE不代表另一组Raw/Core字段全部已知",
    }


def _discover_source_candidates(
    source_roots: Iterable[str],
    budget: UniverseDiscoveryBudget,
) -> tuple[tuple[dict[str, str], ...], dict[str, Any]]:
    if not isinstance(budget, UniverseDiscoveryBudget):
        raise ResearchEntryError("UNIVERSE_BUDGET_INVALID", "缺少有效的来源发现预算")
    if isinstance(source_roots, (str, bytes, os.PathLike)):
        raise ResearchEntryError("SOURCE_PATH_INVALID", "source_roots必须是根路径序列")
    try:
        configured_roots = tuple(source_roots)
    except TypeError as error:
        raise ResearchEntryError("SOURCE_PATH_INVALID", "source_roots必须是根路径序列") from error
    if not configured_roots:
        raise ResearchEntryError("SOURCE_NOT_CONFIGURED", "没有配置只读TDX保护根")

    started = time.monotonic()
    roots: list[Path] = []
    for value in configured_roots:
        try:
            root = Path(value).resolve(strict=True)
        except FileNotFoundError as error:
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根不存在") from error
        except PermissionError as error:
            raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验配置的TDX来源根") from error
        except (RuntimeError, RecursionError, ValueError) as error:
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根无法安全解析") from error
        except (OSError, TypeError) as error:
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根无法核验") from error
        if not root.is_dir():
            raise ResearchEntryError("SOURCE_ROOT_INVALID", "配置的TDX来源根不是目录")
        roots.append(root)

    candidates_by_symbol: dict[str, dict[str, str]] = {}
    candidate_sources: dict[str, str] = {}
    directory_summaries: list[dict[str, Any]] = []
    directory_entries_scanned = 0
    day_file_entries = 0
    excluded_out_of_scope = 0
    invalid_day_filenames = 0
    invalid_day_filename_samples: list[str] = []
    by_market = {"SH": 0, "SZ": 0}
    by_board = {"MAIN_BOARD": 0, "GEM": 0}
    for root in roots:
        for market, directory in (("SH", "sh"), ("SZ", "sz")):
            logical_directory = root / "vipdoc" / directory / "lday"
            try:
                resolved_directory = logical_directory.resolve(strict=True)
            except FileNotFoundError as error:
                raise ResearchEntryError(
                    "SOURCE_DIRECTORY_UNAVAILABLE",
                    f"缺少必需的TDX来源目录vipdoc/{directory}/lday；无法给出完整候选分母",
                    context={"directory": f"vipdoc/{directory}/lday", "discovery_complete": False},
                ) from error
            except PermissionError as error:
                raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX来源目录") from error
            except (RuntimeError, RecursionError, ValueError) as error:
                raise ResearchEntryError(
                    "SOURCE_PATH_INVALID",
                    f"TDX来源目录vipdoc/{directory}/lday无法安全解析",
                    context={"directory": f"vipdoc/{directory}/lday", "discovery_complete": False},
                ) from error
            except OSError as error:
                raise ResearchEntryError("SOURCE_IO_ERROR", "核验TDX来源目录时发生I/O错误") from error
            try:
                resolved_directory.relative_to(root)
            except ValueError as error:
                raise ResearchEntryError(
                    "SOURCE_PATH_OUTSIDE_ROOT",
                    "TDX行情目录最终路径离开已配置来源保护根",
                    context={"directory": f"vipdoc/{directory}/lday", "discovery_complete": False},
                ) from error
            try:
                directory_stat_before = resolved_directory.stat()
            except PermissionError as error:
                raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝读取TDX来源目录") from error
            except OSError as error:
                raise ResearchEntryError("SOURCE_IO_ERROR", "读取TDX来源目录状态时发生I/O错误") from error
            if not stat.S_ISDIR(directory_stat_before.st_mode):
                raise ResearchEntryError(
                    "SOURCE_DIRECTORY_UNAVAILABLE",
                    f"TDX来源路径vipdoc/{directory}/lday不是目录",
                    context={"directory": f"vipdoc/{directory}/lday", "discovery_complete": False},
                )

            entry_count = 0
            day_count = 0
            directory_candidates = 0
            directory_excluded = 0
            directory_invalid_names = 0
            try:
                with os.scandir(resolved_directory) as iterator:
                    for entry in iterator:
                        entry_count += 1
                        directory_entries_scanned += 1
                        if directory_entries_scanned > budget.max_directory_entries:
                            raise ResearchEntryError(
                                "UNIVERSE_DISCOVERY_BUDGET_EXCEEDED",
                                "来源候选目录项超过发现预算；已停止且不声明完整候选总数",
                                context={
                                    "discovery_complete": False,
                                    "directory_entries_scanned": directory_entries_scanned,
                                    "day_file_entries_seen": day_file_entries,
                                    "max_directory_entries": budget.max_directory_entries,
                                },
                            )
                        if entry_count % 256 == 0 and time.monotonic() - started >= budget.max_seconds:
                            raise ResearchEntryError(
                                "UNIVERSE_DISCOVERY_BUDGET_EXCEEDED",
                                "来源候选发现超过耗时预算；已停止且不声明完整候选总数",
                                context={
                                    "discovery_complete": False,
                                    "directory_entries_scanned": directory_entries_scanned,
                                    "day_file_entries_seen": day_file_entries,
                                    "max_seconds": budget.max_seconds,
                                },
                            )
                        if not entry.name.casefold().endswith(".day"):
                            continue
                        day_file_entries += 1
                        day_count += 1
                        entry_path = Path(entry.path)
                        try:
                            resolved_entry = entry_path.resolve(strict=True)
                            entry_stat = resolved_entry.stat()
                        except FileNotFoundError as error:
                            raise ResearchEntryError(
                                "SOURCE_CANDIDATE_CHANGED",
                                "枚举期间TDX日线来源文件消失；候选总分母不完整",
                                context={"directory": f"vipdoc/{directory}/lday", "discovery_complete": False},
                            ) from error
                        except PermissionError as error:
                            raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝核验TDX日线文件") from error
                        except (RuntimeError, RecursionError, ValueError) as error:
                            raise ResearchEntryError(
                                "SOURCE_PATH_INVALID",
                                "TDX日线候选最终路径无法安全解析",
                                context={"filename": entry.name, "discovery_complete": False},
                            ) from error
                        except OSError as error:
                            raise ResearchEntryError("SOURCE_IO_ERROR", "核验TDX日线文件时发生I/O错误") from error
                        try:
                            resolved_entry.relative_to(root)
                        except ValueError as error:
                            raise ResearchEntryError(
                                "SOURCE_PATH_OUTSIDE_ROOT",
                                "TDX日线候选最终路径离开已配置来源保护根",
                                context={"filename": entry.name, "discovery_complete": False},
                            ) from error
                        if not stat.S_ISREG(entry_stat.st_mode):
                            raise ResearchEntryError(
                                "SOURCE_CANDIDATE_INVALID",
                                "后缀为.day的来源候选不是普通文件",
                                context={"filename": entry.name, "discovery_complete": False},
                            )
                        match = _DAY_FILENAME_PATTERN.fullmatch(entry.name)
                        if match is None or match.group(1).upper() != market:
                            invalid_day_filenames += 1
                            directory_invalid_names += 1
                            if len(invalid_day_filename_samples) < 100:
                                invalid_day_filename_samples.append(
                                    f"vipdoc/{directory}/lday/{entry.name}"
                                )
                            continue
                        code = match.group(2)
                        try:
                            symbol, _resolved_market_dir, board = _normalize_symbol(f"{market}.{code}")
                        except ResearchEntryError as error:
                            if error.code != "SYMBOL_OUT_OF_SCOPE":
                                raise
                            excluded_out_of_scope += 1
                            directory_excluded += 1
                            continue
                        if symbol in candidates_by_symbol:
                            raise ResearchEntryError(
                                "UNIVERSE_DUPLICATE_SYMBOL",
                                "多个TDX目录项归一为同一来源代码；拒绝静默去重",
                                context={
                                    "symbol": symbol,
                                    "first_source": candidate_sources[symbol],
                                    "duplicate_source": f"vipdoc/{directory}/lday/{entry.name}",
                                    "discovery_complete": False,
                                },
                            )
                        candidates_by_symbol[symbol] = {
                            "symbol": symbol,
                            "market": market,
                            "board": board,
                        }
                        candidate_sources[symbol] = f"vipdoc/{directory}/lday/{entry.name}"
                        directory_candidates += 1
                        by_market[market] += 1
                        by_board[board] += 1
            except ResearchEntryError:
                raise
            except PermissionError as error:
                raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝枚举TDX来源目录") from error
            except OSError as error:
                raise ResearchEntryError("SOURCE_IO_ERROR", "枚举TDX来源目录时发生I/O错误") from error
            try:
                directory_stat_after = resolved_directory.stat()
            except PermissionError as error:
                raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝复核TDX来源目录") from error
            except OSError as error:
                raise ResearchEntryError("SOURCE_IO_ERROR", "复核TDX来源目录时发生I/O错误") from error
            identity_before = (directory_stat_before.st_dev, directory_stat_before.st_ino, directory_stat_before.st_mtime_ns)
            identity_after = (directory_stat_after.st_dev, directory_stat_after.st_ino, directory_stat_after.st_mtime_ns)
            if identity_before != identity_after:
                raise ResearchEntryError(
                    "SOURCE_DIRECTORY_CHANGED",
                    "候选发现期间TDX目录发生变化；拒绝声明稳定分母",
                    context={"directory": f"vipdoc/{directory}/lday", "discovery_complete": False},
                )
            directory_summaries.append({
                "market": market,
                "directory": f"vipdoc/{directory}/lday",
                "directory_entries": entry_count,
                "day_file_entries": day_count,
                "in_scope_candidates": directory_candidates,
                "excluded_out_of_scope": directory_excluded,
                "invalid_day_filenames": directory_invalid_names,
            })
            if time.monotonic() - started >= budget.max_seconds:
                raise ResearchEntryError(
                    "UNIVERSE_DISCOVERY_BUDGET_EXCEEDED",
                    "来源候选发现超过耗时预算；已停止且不声明完整候选总数",
                    context={
                        "discovery_complete": False,
                        "directory_entries_scanned": directory_entries_scanned,
                        "day_file_entries_seen": day_file_entries,
                        "max_seconds": budget.max_seconds,
                    },
                )

    candidates = tuple(candidates_by_symbol[name] for name in sorted(candidates_by_symbol))
    return candidates, {
        "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "complete": True,
        "configured_root_count": len(roots),
        "directories": directory_summaries,
        "directory_entries_scanned": directory_entries_scanned,
        "day_file_entries": day_file_entries,
        "candidate_count": len(candidates),
        "excluded_out_of_scope": excluded_out_of_scope,
        "invalid_day_filenames": invalid_day_filenames,
        "invalid_day_filename_samples": sorted(invalid_day_filename_samples),
        "invalid_day_filename_sample_limit": 100,
        "candidate_counts_by_market": by_market,
        "candidate_counts_by_board": by_board,
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "identity_disclosure": "文件名仅形成TDX来源代码候选；不证明稳定实体、上市、当日可交易、ST状态或来源更新时点",
    }


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
    rendered_core_fields = {name: _render_field(value) for name, value in sorted(core.items())}
    raw_availability = _availability_record(raw_fields, subject="Raw")
    core_availability = _availability_record(rendered_core_fields, subject="Core")
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
        "status_explanation": "兼容状态仅按目标日原始价格组判定；Raw/Core整体可用性及字段原因分别见raw_availability、core_availability和逐字段quality/reason。",
        "raw_availability": raw_availability,
        "core_availability": core_availability,
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
        "core_fields": rendered_core_fields,
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
                    name, {"value": None, "quality": "UNKNOWN_DATA", "reason": item.get("error_code", "NOT_AVAILABLE")}
                )
                for item in results
            }
            for name in _COMPARE_FIELDS
        },
        "results": results,
    }


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
) -> dict[str, Any]:
    """只读发现TDX来源代码候选并对单页复用既有Core和累计预算。"""
    started = time.monotonic()
    target = _parse_target(target_session)
    _load_calendar(target)
    if type(offset) is not int or offset < 0:
        raise ResearchEntryError("UNIVERSE_PAGE_INVALID", "分页offset必须为非负整数")
    if type(limit) is not int or not 1 <= limit <= _MAX_UNIVERSE_PAGE_SIZE:
        raise ResearchEntryError("UNIVERSE_PAGE_INVALID", f"每页limit必须为1至{_MAX_UNIVERSE_PAGE_SIZE}只")
    if not isinstance(market_filter, str) or market_filter.upper() not in {"ALL", "SH", "SZ"}:
        raise ResearchEntryError("UNIVERSE_FILTER_INVALID", "market过滤只接受ALL、SH或SZ")
    if not isinstance(board_filter, str) or board_filter.upper() not in {"ALL", "MAIN_BOARD", "GEM"}:
        raise ResearchEntryError("UNIVERSE_FILTER_INVALID", "board过滤只接受ALL、MAIN_BOARD或GEM")
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
        raise ResearchEntryError("SOURCE_PATH_INVALID", "source_roots必须是根路径序列") from error
    candidates, catalog = _discover_source_candidates(roots, discovery_budget)
    selected = tuple(
        candidate for candidate in candidates
        if (market_filter == "ALL" or candidate["market"] == market_filter)
        and (board_filter == "ALL" or candidate["board"] == board_filter)
    )
    candidate_total = len(selected)
    page_candidates = selected[offset:offset + limit]
    candidate_by_symbol = {item["symbol"]: item for item in page_candidates}
    page_results: list[dict[str, Any]] = []
    total_bytes = 0
    total_records = 0
    stop_code: str | None = None
    next_position = 0

    def mark_page_not_evaluated(symbols: Iterable[str], code: str) -> None:
        for symbol in symbols:
            candidate = candidate_by_symbol[symbol]
            page_results.append({
                "status": "NOT_EVALUATED",
                "requested_symbol": symbol,
                "error_code": code,
                "message": "本页累计读取预算耗尽；对象保留在分页分母且未扫描",
                "read_usage": {"bytes": 0, "records": 0},
                "source_candidate": {
                    **candidate,
                    "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
                },
            })

    while next_position < len(page_candidates):
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
                (item["symbol"] for item in page_candidates[next_position:]), stop_code
            )
            break

        chunk_candidates = page_candidates[next_position:next_position + _MAX_COMPARE_SYMBOLS]
        chunk = research_compare(
            symbols=tuple(item["symbol"] for item in chunk_candidates),
            target_session=target.isoformat(),
            source_roots=roots,
            units=units,
            limits=limits,
            budget=CompareBudget(remaining_bytes, remaining_records, remaining_seconds),
        )
        total_bytes += chunk["budget"]["used_bytes"]
        total_records += chunk["budget"]["used_records"]
        for result in chunk["results"]:
            candidate = candidate_by_symbol[result["requested_symbol"]]
            result["source_candidate"] = {
                **candidate,
                "contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
            }
            page_results.append(result)
        next_position += len(chunk["results"])
        stop_code = chunk["budget"]["exhausted_by"]
        if stop_code is not None and next_position < len(page_candidates):
            mark_page_not_evaluated(
                (item["symbol"] for item in page_candidates[next_position:]), stop_code
            )
            break

    available = sum(item.get("status") == "AVAILABLE" for item in page_results)
    failed = sum(item.get("status") == "FAILED" for item in page_results)
    partial_unknown = sum(item.get("status") == "PARTIAL_UNKNOWN" for item in page_results)
    not_evaluated = sum(item.get("status") == "NOT_EVALUATED" for item in page_results)
    classified = available + failed + partial_unknown + not_evaluated
    if classified != len(page_candidates):
        raise ResearchEntryError(
            "UNIVERSE_CLASSIFICATION_INCOMPLETE",
            "本页来源候选未按可用、失败、部分未知和未评价完整分区",
            context={"page_requested": len(page_candidates), "classified": classified},
        )

    short_summary: list[dict[str, Any]] = []
    for item in page_results:
        candidate = item["source_candidate"]
        observation = item.get("observation_summary", {})
        fields = observation.get("fields", {})
        positions = fields.get("moving_average_position", {})
        short_summary.append({
            "symbol": candidate["symbol"],
            "market": candidate["market"],
            "board": candidate["board"],
            "target_session": target.isoformat(),
            "status": item["status"],
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
            "error_code": item.get("error_code"),
        })

    page_remaining = max(0, candidate_total - min(offset + len(page_candidates), candidate_total))
    evaluated_page = available + failed + partial_unknown
    status = "COMPLETE_WITH_DISCLOSURES" if failed == 0 and partial_unknown == 0 and not_evaluated == 0 else "PARTIAL"
    return {
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
            "not_evaluated_in_filtered_candidate_set": max(0, candidate_total - evaluated_page),
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
