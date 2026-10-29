"""只读TDX研究入口；复用日线解码和Core，不写数据库或业务文件。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction
from hashlib import sha256
import hashlib
from importlib.resources import files
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from typing import Any, Iterable, Mapping, Sequence
from uuid import uuid4

from stock_a.canonical.selection import CanonicalReadSelection, build_tdx_read_selection
from stock_a.domain import FieldValue, exact_number, unknown
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.research.profile import (
    ProfileHandoff,
    ProfileInput,
    RuntimeProducerBinding,
    WindowIdentity,
    evaluate_profile_kernel,
)
from stock_a.sources.baostock.identity import (
    IdentityLifecycleProjection,
    reconcile_identity_bar_status,
)
from stock_a.sources.tdx.day import DAY_STRUCT, DecodedDay, UnitContract
from stock_a.sources.tdx.capture import TdxCaptureRequest, build_tdx_capture_envelope
from stock_a.sources.tdx.day_source import open_day_file
from stock_a.sources.tdx.day_stream import DayReadError, ReadLimits
from stock_a.sources.tdx.vipdata import VipdataEntryCapture


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
_PROFILE_CORE_WINDOW_SPECS = {
    "core.ma5": (5, False),
    "core.ma10": (10, False),
    "core.ma20": (20, False),
    "core.ma60": (60, False),
    "core.hhv60": (60, False),
    "core.prior_high20": (20, True),
    "core.atr20": (21, False),
    "core.ma_slope20_atr": (25, False),
    "core.ma_slope60_atr": (70, False),
    "core.hh_progress": (10, False),
    "core.ll_progress": (10, False),
    "core.price_damage": (21, False),
    "core.bias20_atr": (21, False),
    "core.pos60": (60, False),
    "core.range_ratio": (20, False),
    "core.atr_ratio": (21, False),
    "core.vol_ratio": (21, False),
    "core.amount_ratio20": (21, False),
    "core.liquidity_mean20": (20, True),
    "core.ret1": (2, False),
    "core.ret5": (6, False),
    "core.clv": (1, False),
}
_PROFILE_NOT_IMPLEMENTED = {
    "profile.closed_week_trend": {
        "status": "NOT_IMPLEMENTED",
        "reason": "缺少经官方周历闭合、状态完整且与目标日同坐标的period.closed_week生产输入；本只读TDX日线入口未接入周线聚合。",
        "missing_inputs": ("period.closed_week",),
    },
    "profile.closed_month_trend": {
        "status": "NOT_IMPLEMENTED",
        "reason": "缺少经官方月历闭合、状态完整且与目标日同坐标的period.closed_month生产输入；本只读TDX日线入口未接入月线聚合。",
        "missing_inputs": ("period.closed_month",),
    },
    "profile.relative": {
        "status": "CONTRACT_UNBOUND",
        "reason": "跨市场RPS需要每个端点日完整、合法且有实体身份的Universe分母；相对市场比较需要同端点指数序列。候选分页不是Universe分母，本入口没有这些输入。",
        "missing_inputs": ("cross.rps5_delta3_pp", "cross.rps20", "cross.rel_market_1"),
    },
}
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
    result = {
        "status": status,
        "observed_fields": observed,
        "unknown_fields": total - observed,
        "total_fields": total,
        "unknown_reasons": dict(sorted(unknown_reasons.items())),
        "message": message,
    }
    return result


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


def _unit_trust_status(scale: object) -> str:
    return "CALLER_DECLARED_ASSUMPTION" if scale is not None else "UNKNOWN_UNIT_AUTHORITY"


def _render_raw(decoded: DecodedDay | None, units: UnitContract) -> dict[str, Any]:
    if decoded is None:
        fields = {name: _render_field(unknown("TARGET_BAR_MISSING")) for name in _RAW_FIELDS}
        raw_amount = unknown("TARGET_BAR_MISSING")
        raw_volume = unknown("TARGET_BAR_MISSING")
    else:
        fields = {
            "open": _render_field(decoded.raw_open),
            "high": _render_field(decoded.raw_high),
            "low": _render_field(decoded.raw_low),
            "close": _render_field(decoded.raw_close),
            "amount": _render_field(decoded.amount),
            "volume": _render_field(decoded.volume),
        }
        raw_amount = decoded.raw_amount or unknown("RAW_AMOUNT_NOT_RETAINED")
        raw_volume = decoded.raw_volume or unknown("RAW_VOLUME_NOT_RETAINED")
    fields["amount"]["raw_stored_value"] = {
        **_render_field(raw_amount),
        "storage_type": "TDX_FLOAT32",
        "unit": "UNVERIFIED_RAW_STORAGE_COORDINATE",
    }
    fields["volume"]["raw_stored_value"] = {
        **_render_field(raw_volume),
        "storage_type": "TDX_UINT32",
        "unit": "UNVERIFIED_RAW_STORAGE_COORDINATE",
    }
    fields["amount"]["unit_trust"] = {
        "status": "CALLER_DECLARED_ASSUMPTION" if units.amount_to_cny is not None else "UNKNOWN",
        "unit_version": units.version,
        "amount_to_cny": str(units.amount_to_cny) if units.amount_to_cny is not None else None,
    }
    fields["volume"]["unit_trust"] = {
        "status": "CALLER_DECLARED_ASSUMPTION" if units.volume_to_shares is not None else "UNKNOWN",
        "unit_version": units.version,
        "volume_to_shares": units.volume_to_shares,
    }
    return fields


def _core_history_from_selection(selection: CanonicalReadSelection) -> tuple[CoreObservation, ...]:
    """让既有Core直接消费本次规范选择的typed字段行。"""
    return tuple(
        CoreObservation(
            session=row.session,
            state=row.state,
            coordinate_id=row.coordinate_id,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            amount=row.amount,
            volume=row.volume,
        )
        for row in selection.rows
    )


def _render_canonical_selection(selection: CanonicalReadSelection) -> dict[str, Any]:
    capture = selection.capture
    package_entry = capture.source_package_entry
    source_identity = capture.source_identity
    target_fields: dict[str, Any] = {}
    for fact in selection.target_fields:
        payload = _render_field(fact.value)
        payload.update({
            "field_id": fact.field_id,
            "target_session": fact.target_session.isoformat(),
            "source_session": fact.source_session.isoformat() if fact.source_session else None,
            "unit": fact.unit,
            "unit_basis": fact.unit_basis,
            "source_sha256": fact.source_sha256,
            "field_digest": None,
            "field_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
        })
        target_fields[fact.field_id.removeprefix("raw.")] = payload
    actual_rows = sum(row.state == "ACTUAL" for row in selection.rows)
    missing_rows = len(selection.rows) - actual_rows
    target_present = any(fact.source_session == selection.target_session for fact in selection.target_fields)
    return {
        "contract": "STOCK-A-TDX-CANONICAL-READ-SELECTION/R1",
        "selection_status": "EPHEMERAL_READ_ONLY_SELECTION_COMPLETE",
        "acceptance_status": "NOT_ACCEPTED_NOT_PERSISTED",
        "target_session": selection.target_session.isoformat(),
        "target_bar_status": "PRESENT" if target_present else "UNKNOWN_NO_TDX_BAR",
        "source_candidate_scope": {
            "kind": "ONE_EXPLICIT_SOURCE_SYMBOL_QUERY",
            "candidate_count": 1,
            "selected_count": 1,
            "official_universe_denominator": "UNKNOWN",
            "exclusion_reason": "来源代码不是官方成员或稳定证券身份证明",
        },
        "source": {
            "dataset_id": capture.dataset_id,
            "dataset_version": capture.dataset_version,
            "capture_id": capture.capture_id,
            "capture_id_role": "EPHEMERAL_CURRENT_READ_ONLY_INVOCATION",
            "capture_business_digest_sha256": capture.business_digest_sha256,
            "raw_byte_sha256": capture.content_sha256,
            "record_count": capture.record_count,
            "byte_count": capture.byte_count,
            "record_date_range": [capture.first_date.isoformat(), capture.last_date.isoformat()],
            "complete_read": True,
            "source_authority": (
                "VIPDATA_OFFICIAL_ENDPOINT_LOCAL_PACKAGE_AND_ENTRY_BYTES_REVALIDATED"
                if package_entry is not None else "UNVERIFIED_LOCAL_TDX_INSTALLATION"
            ),
            "source_identity": ({
                "kind": "VERIFIED_TDX_ZIP_ENTRY",
                "package_id": package_entry.package_id,
                "package_sha256": package_entry.package_sha256,
                "official_package_url": package_entry.official_package_url,
                "package_file_size": package_entry.package_file_size,
                "zip_entry_count": package_entry.zip_entry_count,
                "zip_crc": package_entry.zip_crc,
                "package_verified_at": package_entry.package_verified_at.isoformat(),
                "zip_entry_path": package_entry.zip_entry_path,
                "entry_sha256": package_entry.entry_sha256,
                "entry_byte_count": package_entry.entry_byte_count,
                "source_symbol": package_entry.source_symbol,
                "target_session": package_entry.target_session.isoformat(),
                "source_clock_role": package_entry.source_clock_role,
                "published_at": "UNKNOWN",
                "first_available_at": "UNKNOWN",
            } if package_entry is not None else {
                "kind": "READONLY_LOCAL_FILE_HANDLE",
                "requested_access": source_identity.requested_access,
                "path_binding_verified": source_identity.path_binding_verified,
                "handle_path_verified": source_identity.handle_path_verified,
                "size_stable_during_read": source_identity.size_before == source_identity.size_after,
                "mtime_stable_during_read": source_identity.mtime_ns_before == source_identity.mtime_ns_after,
            }),
            "observed_started_at": capture.observed_started_at.isoformat(),
            "observed_completed_at": capture.observed_completed_at.isoformat(),
            "observation_clock_role": (
                package_entry.source_clock_role if package_entry is not None else "LOCAL_FILE_READ_TIME_ONLY"
            ),
            "first_available_at": "UNKNOWN",
            "published_at": "UNKNOWN",
        },
        "identity": (
            {**selection.identity_projection.as_dict(),
             "status": selection.identity_projection.identity_status,
             "security_entity_id": selection.identity_projection.entity_id,
             "membership_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
             "listing_lifecycle_status": selection.identity_projection.classification}
            if selection.identity_projection is not None else {
                "source_symbol": selection.source_symbol,
                "status": "SOURCE_SYMBOL_ONLY",
                "security_entity_id": None,
                "membership_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
                "listing_lifecycle_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
            }
        ),
        "calendar": {
            "calendar_id": selection.calendar_id,
            "revision": selection.calendar_revision,
            "evidence_status": selection.calendar_evidence_status,
            "target_session_verified": True,
            "missing_bar_semantics": "UNKNOWN; 不推断停牌、退市或上市前",
        },
        "coordinate": {
            "price": "RAW_UNADJUSTED",
            "adjustment_status": "NOT_APPLIED_IDENTITY_AND_EVENT_AVAILABILITY_UNRESOLVED",
        },
        "time_availability": {
            "mode": "NON_PIT_RETROSPECTIVE",
            "observed_at_role": "LOCAL_FILE_READ_TIME_ONLY",
            "available_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "durable_at": "NOT_CREATED",
            "accepted_at": "NOT_CREATED",
            "availability_digest_status": "NOT_COMPUTED_NO_DURABLE_ACCEPTANCE_OR_SOURCE_AVAILABILITY_CLOCK",
        },
        "history_selection": {
            "calendar_session_count": len(selection.rows),
            "actual_source_rows_selected": actual_rows,
            "missing_bar_sessions_unknown": missing_rows,
            "maximum_selected_session": selection.rows[-1].session.isoformat() if selection.rows else None,
            "future_source_rows_excluded": selection.future_source_rows_excluded,
            "future_rows_used_by_core": False,
            "field_window_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
        },
        "field_identity": {
            "field_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
            "window_digest_status": "BLOCKED_STABLE_SECURITY_ID_UNBOUND",
            "reason": "不得以来源代码冒充稳定security_entity_id",
        },
        "unit_contract": {
            "version": selection.units.version,
            "amount_to_cny": str(selection.units.amount_to_cny)
            if selection.units.amount_to_cny is not None else None,
            "volume_to_shares": selection.units.volume_to_shares,
            "amount_authority": _unit_trust_status(selection.units.amount_to_cny),
            "volume_authority": _unit_trust_status(selection.units.volume_to_shares),
            "binding_status": "CALLER_DECLARED_RESEARCH_ASSUMPTION"
            if selection.units.amount_to_cny is not None or selection.units.volume_to_shares is not None
            else "UNKNOWN_UNIT_AUTHORITY",
        },
        "fields": target_fields,
        "production_admission": "DENY_SOURCE_AUTHORITY_IDENTITY_AND_PIT_UNBOUND",
        "profile_admission": "DENY_TRUSTED_FOUNDATION_SELECTOR_REQUIRED",
    }


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


def _annotated_fact(
    *,
    field_id: str,
    value: dict[str, Any],
    formula_source: str,
    coordinate_role: str,
    coordinate_id: str | None,
) -> dict[str, Any]:
    """为可读摘要标明值的现役生产者和坐标，不重新计算字段。"""
    return {
        "field_id": field_id,
        "value": value,
        "formula_source": formula_source,
        "coordinate_role": coordinate_role,
        "coordinate_id": coordinate_id,
    }


def _basic_fact_dimensions(
    *,
    symbol: str,
    target_session: date,
    source_last_session: date,
    read_time: dict[str, Any],
    source: dict[str, Any],
    raw_fields: dict[str, Any],
    core: dict[str, FieldValue],
    observation_summary: dict[str, Any],
    raw_availability: dict[str, Any],
    core_availability: dict[str, Any],
    window_context: dict[str, Any],
    target_bar_present: bool,
    target_prices_usable: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """把既有Raw/Core字段分组展示；不增加指标或策略阈值。"""
    raw_coordinate = f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
    raw_specs = {
        "raw.close": ("close", "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00", "RAW_UNADJUSTED"),
        "raw.amount": ("amount", "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00", "SOURCE_RAW_ENCODING"),
        "raw.volume": ("volume", "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00", "SOURCE_RAW_ENCODING"),
    }
    core_specs = {
        "core.ma5": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.ma20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.ma60": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.ma_slope20_atr": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.ret5": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.ret20": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.atr20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.pos60": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.prior_high20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.prior_low20": ("stock_a.research.core:calculate_core", "RAW_UNADJUSTED"),
        "core.bias20_atr": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.dist_high20_atr": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.price_damage": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.amount_ratio20": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.volume_ratio20": ("stock_a.research.core:calculate_core", "DIMENSIONLESS"),
        "core.liquidity_mean20": ("stock_a.research.core:calculate_core", "SOURCE_AMOUNT_UNIT_ASSUMPTION"),
    }
    comparison_specs = {
        "close_vs_ma20": "core.ma20",
        "close_vs_ma60": "core.ma60",
    }
    old_fields = observation_summary["fields"]

    trend: dict[str, Any] = {}
    position: dict[str, Any] = {}
    participation: dict[str, Any] = {}
    for field_id, (raw_name, formula, coordinate) in raw_specs.items():
        target = trend if field_id == "raw.close" else participation
        target[field_id] = _annotated_fact(
            field_id=field_id,
            value=raw_fields[raw_name],
            formula_source=formula,
            coordinate_role=coordinate,
            coordinate_id=raw_coordinate if coordinate == "RAW_UNADJUSTED" else None,
        )
    for field_id, (formula, coordinate) in core_specs.items():
        target = trend if field_id in {
            "core.ma5", "core.ma20", "core.ma60", "core.ma_slope20_atr", "core.ret5", "core.ret20"
        } else position if field_id in {
            "core.atr20", "core.pos60", "core.prior_high20", "core.prior_low20",
            "core.bias20_atr", "core.dist_high20_atr", "core.price_damage",
        } else participation
        fact = _annotated_fact(
            field_id=field_id,
            value=_render_field(core[field_id]),
            formula_source=formula,
            coordinate_role=coordinate,
            coordinate_id=raw_coordinate if coordinate == "RAW_UNADJUSTED" else None,
        )
        if field_id in {"core.amount_ratio20", "core.volume_ratio20", "core.liquidity_mean20"}:
            fact["unit_trust"] = {
                "status": "CALLER_DECLARED_ASSUMPTION",
                "interpretation": (
                    "金额比率要求窗口内金额倍率一致" if field_id == "core.amount_ratio20"
                    else "数量比率要求窗口内数量倍率一致" if field_id == "core.volume_ratio20"
                    else "绝对金额均值依赖调用方声明的金额倍率"
                ),
            }
        target[field_id] = fact
    for label, core_id in comparison_specs.items():
        item = old_fields["moving_average_position"][label]
        trend[label] = _annotated_fact(
            field_id=f"research.{label}",
            value=item,
            formula_source=f"stock_a.research.entry._field_relation(raw.close, {core_id}); 精确比较，不增加阈值",
            coordinate_role="DIMENSIONLESS_RELATION",
            coordinate_id=None,
        )

    all_facts = {**trend, **position, **participation}
    unknown_facts = {
        group: {
            field_id: fact["value"].get("reason") or fact["value"].get("quality")
            for field_id, fact in fields.items()
            if fact["value"].get("quality") != "OBSERVED"
        }
        for group, fields in (
            ("price_trend", trend), ("position_volatility", position), ("participation", participation)
        )
    }
    dimensions = {
        "contract": "STOCK-A-BASIC-FACT-DIMENSIONS/R1",
        "scope": "只分组呈现本次查询的既有Raw/Core事实；不代表Profile或策略结论",
        "target_session": target_session.isoformat(),
        "source_last_session": source_last_session.isoformat(),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity": {"status": "SOURCE_SYMBOL_ONLY", "symbol": symbol},
        "dimensions": {
            "price_trend": trend,
            "position_volatility": position,
            "participation": participation,
        },
        "source_quality": {
            "target_bar_present": target_bar_present,
            "target_prices_usable": target_prices_usable,
            "read_time": read_time,
            "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
            "source_read_budget": source.get("budgets", {}),
            "window_context": window_context,
            "raw_availability": raw_availability,
            "core_availability": core_availability,
            "unknown_facts_by_dimension": unknown_facts,
            "field_count": len(all_facts),
        },
    }
    availability = {
        "contract": "STOCK-A-BASIC-FACT-AVAILABILITY/R1",
        "compatibility_status": "AVAILABLE" if target_prices_usable else "PARTIAL_UNKNOWN",
        "compatibility_status_scope": "仅按目标日原始价格组判断；不代表所有Core字段已知",
        "target_bar_present": target_bar_present,
        "raw": raw_availability,
        "core": core_availability,
        "unknown_facts_by_dimension": unknown_facts,
        "missing_target_bar_explanation": (
            None if target_bar_present else "目标日没有可用bar；不据此推断停牌、上市前或退市。"
        ),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
    }
    return dimensions, availability


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


def _profile_row_payload(row: CoreObservation) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for name in _RAW_FIELDS:
        value = getattr(row, name)
        values[name] = _render_field(value) if isinstance(value, FieldValue) else None
    return {
        "session": row.session.isoformat(),
        "state": row.state,
        "coordinate_id": row.coordinate_id,
        "fields": values,
    }


def _profile_window(
    field_id: str,
    history: tuple[CoreObservation, ...],
    *,
    window_contract: str,
    calendar_revision: str,
    quality: str,
) -> tuple[WindowIdentity, dict[str, Any]]:
    spec = _PROFILE_CORE_WINDOW_SPECS.get(field_id)
    if spec is None:
        raise ContractError("Profile预览未登记Core窗口：" + field_id)
    count, exclude_current = spec
    end = len(history) - (1 if exclude_current else 0)
    selected = history[max(0, end - count):end] if end > 0 else ()
    payload = [_profile_row_payload(row) for row in selected]
    digest = sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    if selected:
        first_endpoint: date | None = selected[0].session
        last_endpoint: date | None = selected[-1].session
    else:
        first_endpoint = last_endpoint = None
    identity = WindowIdentity(
        contract=window_contract,
        first_endpoint=first_endpoint,
        last_endpoint=last_endpoint,
        selected_rows_digest=digest,
        calendar_revision=calendar_revision,
        status_revision="TDX_NO_BAR_UNKNOWN_STATUS_R1",
    )
    return identity, {
        "contract": window_contract,
        "first_endpoint": first_endpoint.isoformat() if first_endpoint else None,
        "last_endpoint": last_endpoint.isoformat() if last_endpoint else None,
        "requested_sessions": count,
        "selected_sessions": len(selected),
        "window_complete": len(selected) == count and all(row.state == "ACTUAL" for row in selected),
        "unknown_session_dates": [row.session.isoformat() for row in selected if row.state == "UNKNOWN"],
        "selected_rows_digest": digest,
        "calendar_revision": calendar_revision,
        "status_revision": "TDX_NO_BAR_UNKNOWN_STATUS_R1",
        "input_quality": quality,
    }


def _profile_preview_source_digest(path: Path, label: str) -> str:
    try:
        return sha256(path.read_bytes()).hexdigest()
    except OSError as error:
        raise ResearchEntryError(
            "PROFILE_PREVIEW_PROVENANCE_UNAVAILABLE",
            f"无法读取Profile预览生产者源码身份：{label}",
        ) from error


def _build_profile_preview(
    *,
    symbol: str,
    target_session: date,
    source_last_session: date,
    source_sha256: str,
    target_bar: DecodedDay | None,
    history: tuple[CoreObservation, ...],
    core: dict[str, FieldValue],
    calendar_revision: str,
    contract: dict[str, Any],
) -> dict[str, Any]:
    """把本次真实Raw/Core读取接到既有规则；显式限于未复权NON_PIT预览。"""
    if not isinstance(contract, dict):
        raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_REQUIRED", "Profile预览必须使用固定配置槽位装配的day-chain合同")
    if len(source_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in source_sha256):
        raise ResearchEntryError("PROFILE_PREVIEW_SOURCE_ID_INVALID", "Profile预览缺少本次完整TDX字节摘要")
    try:
        scope = contract["scope_bindings"]["FOUNDATION_EOD"]
        rules = scope["profile_expression_registry"]
        canonical_registry = scope["field_registry"]
    except (KeyError, TypeError) as error:
        raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_INVALID", "Profile预览合同缺少FOUNDATION规则或字段登记") from error
    if not isinstance(rules, dict) or len(rules) != 11 or not isinstance(canonical_registry, list):
        raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_INVALID", "Profile预览必须保留唯一11条规则和字段登记")

    registry_by_id = {item["field_id"]: item for item in canonical_registry if isinstance(item, dict) and isinstance(item.get("field_id"), str)}
    if len(registry_by_id) != len(canonical_registry):
        raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_INVALID", "Profile字段登记存在重复或非法field_id")

    package_root = Path(__file__).resolve().parent.parent
    core_sha = _profile_preview_source_digest(Path(__file__).with_name("core.py"), "calculate_core")
    decoder_sha = _profile_preview_source_digest(package_root / "sources" / "tdx" / "day.py", "decode_day_record")
    entry_sha = _profile_preview_source_digest(Path(__file__), "_build_profile_preview")
    normalized = _normalize_symbol(symbol)[0]
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    snapshot_id = "TDX_LOCAL_FILE_SNAPSHOT_SHA256:" + source_sha256
    target_close = target_bar.raw_close if target_bar is not None else unknown("TARGET_BAR_MISSING")

    components: list[dict[str, Any]] = []
    for rule_id, rule in rules.items():
        field_id = rule.get("field_id") if isinstance(rule, dict) else None
        if not isinstance(field_id, str):
            raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_INVALID", "Profile规则缺少field_id")
        unavailable = _PROFILE_NOT_IMPLEMENTED.get(field_id)
        if unavailable is not None:
            missing_inputs = []
            for missing_id in unavailable["missing_inputs"]:
                field_spec = registry_by_id.get(missing_id, {})
                missing_inputs.append({
                    "field_id": missing_id,
                    "producer_stage": field_spec.get("producer_stage"),
                    "producer_symbol": field_spec.get("producer_symbol"),
                    "reason": unavailable["reason"],
                })
            components.append({
                "field_id": field_id,
                "rule_id": rule_id,
                "expression_ref": rule.get("expression_ref"),
                "status": unavailable["status"],
                "production_eligibility": "NOT_ELIGIBLE",
                "reason": unavailable["reason"],
                "missing_inputs": missing_inputs,
                "input_fields": list(rule.get("input_fields", [])),
            })
            continue

        input_fields = rule.get("input_fields")
        if not isinstance(input_fields, list) or not input_fields:
            raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_INVALID", "Profile规则没有明确输入字段：" + field_id)
        inputs: dict[str, ProfileInput] = {}
        expected_context: dict[str, dict[str, Any]] = {}
        expected_windows: dict[str, WindowIdentity] = {}
        runtime_producers: dict[str, RuntimeProducerBinding] = {}
        used_registry: list[dict[str, Any]] = []
        input_provenance: list[dict[str, Any]] = []

        for input_id in input_fields:
            registered = registry_by_id.get(input_id)
            if registered is None:
                raise ResearchEntryError("PROFILE_PREVIEW_CONTRACT_INVALID", "Profile输入未登记：" + str(input_id))
            field_registry = dict(registered)
            if input_id == "adjusted.close":
                # 仅本预览局部把同目标、同Raw坐标的source close映射到规则输入名。
                # 不改正式合同，也不宣称执行了P06复权生产者。
                field_registry.update({
                    "unit": "CNY",
                    "producer_stage": "RESEARCH_PREVIEW",
                    "producer_symbol": "stock_a.research.entry:_build_profile_preview",
                    "producer_source_sha256": entry_sha,
                    "expression_ref": "TDX_RAW_CLOSE_PREVIEW_ALIAS_R1",
                    "algorithm_version": None,
                    "parameter_version": "TDX_RAW_UNADJUSTED_PREVIEW_R1",
                    "context_dependencies": ["context.target_session", "context.source_cutoff"],
                    "window_contract": "NONE",
                    "input_fields": ["raw.close"],
                })
                source_field_id = "raw.close"
                source_producer = "stock_a.sources.tdx.day:decode_day_record"
                source_code_sha = decoder_sha
                input_value = target_close
                input_window = {
                    "contract": "TARGET_SESSION_R1",
                    "first_endpoint": target_session.isoformat() if target_bar is not None else None,
                    "last_endpoint": target_session.isoformat() if target_bar is not None else None,
                    "requested_sessions": 1,
                    "selected_sessions": 1 if target_bar is not None else 0,
                    "selected_rows_digest": sha256(
                        json.dumps(
                            [_profile_row_payload(history[-1])] if target_bar is not None else [],
                            ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                        ).encode("utf-8")
                    ).hexdigest(),
                }
            else:
                source_field_id = input_id
                source_producer = "stock_a.research.core:calculate_core"
                source_code_sha = core_sha
                if input_id not in core:
                    raise ResearchEntryError("PROFILE_PREVIEW_CORE_FIELD_MISSING", "本次Core结果缺少画像输入：" + input_id)
                input_value = core[input_id]
                window_contract = field_registry.get("window_contract", "NONE")
                if window_contract != "NONE":
                    window_identity, input_window = _profile_window(
                        input_id, history,
                        window_contract=window_contract,
                        calendar_revision=calendar_revision,
                        quality=input_value.quality,
                    )
                    expected_windows[input_id] = window_identity
                else:
                    input_window = None

            producer_symbol = field_registry.get("producer_symbol")
            if not isinstance(producer_symbol, str) or not producer_symbol:
                raise ResearchEntryError("PROFILE_PREVIEW_PRODUCER_UNBOUND", "画像输入缺少实际生产者：" + input_id)
            field_registry["producer_source_sha256"] = source_code_sha
            used_registry.append(field_registry)

            contexts: dict[str, Any] = {}
            for dependency in field_registry.get("context_dependencies", []):
                if dependency == "context.target_session":
                    contexts[dependency] = target_session
                elif dependency == "context.source_cutoff":
                    contexts[dependency] = snapshot_id
                elif dependency == "context.calendar_ref":
                    contexts[dependency] = calendar_revision
                else:
                    raise ResearchEntryError(
                        "PROFILE_PREVIEW_CONTEXT_UNBOUND",
                        f"当前只读预览没有该输入上下文生产者：{input_id}/{dependency}",
                    )
            expected_context[input_id] = dict(contexts)
            producer = RuntimeProducerBinding(
                symbol=producer_symbol,
                source_sha256=source_code_sha,
                algorithm_version=field_registry.get("algorithm_version"),
            )
            runtime_producers[producer_symbol] = producer
            window_identity = expected_windows.get(input_id)
            inputs[input_id] = ProfileInput(
                field_id=input_id,
                field=input_value,
                unit=field_registry.get("unit"),
                producer_stage=field_registry.get("producer_stage"),
                producer_symbol=producer_symbol,
                producer_source_sha256=source_code_sha,
                expression_ref=field_registry.get("expression_ref"),
                algorithm_version=field_registry.get("algorithm_version"),
                parameter_version=field_registry.get("parameter_version"),
                window=window_identity,
                coordinate_role=field_registry.get("coordinate_role"),
                coordinate_id=coordinate_id,
                context=contexts,
            )
            input_provenance.append({
                "rule_input_field": input_id,
                "source_field_id": source_field_id,
                "value": _render_field(input_value),
                "unit": field_registry.get("unit"),
                "producer_symbol": producer_symbol,
                "source_producer": source_producer,
                "producer_source_sha256": source_code_sha,
                "expression_ref": field_registry.get("expression_ref"),
                "parameter_version": field_registry.get("parameter_version"),
                "window": input_window,
                "coordinate_role": field_registry.get("coordinate_role"),
                "coordinate_id": coordinate_id,
                "context": {key: value.isoformat() if isinstance(value, date) else value for key, value in contexts.items()},
                "unit_trust": "CALLER_DECLARED_ASSUMPTION" if input_id in {
                    "core.amount_ratio20", "core.liquidity_mean20"
                } else None,
            })

        handoff = ProfileHandoff(
            inputs=inputs,
            target_session=target_session,
            source_cutoff=snapshot_id,
            target_coordinate_id=coordinate_id,
            expected_windows=expected_windows,
            expected_context=expected_context,
            runtime_producers=runtime_producers,
        )
        result = evaluate_profile_kernel(field_id, handoff, rules, used_registry)
        components.append({
            "field_id": field_id,
            "rule_id": rule_id,
            "expression_ref": rule.get("expression_ref"),
            "status": "EVALUATED",
            "quality": result.quality,
            "value": _render_field(result)["value"],
            "reason": result.reason,
            "production_eligibility": "PREVIEW_ONLY_RAW_UNADJUSTED_NON_PIT",
            "calculation_basis": {
                "input_fields": list(rule["input_fields"]),
                "operands": rule.get("operands", {}),
                "parameters": rule.get("parameters", {}),
                "ordered_branches": rule.get("ordered_branches", []),
            },
            "inputs": input_provenance,
        })

    return {
        "contract": "STOCK-A-PROFILE-COMPONENT-PREVIEW/R1",
        "scope": "REAL_TDX_RAW_CORE_READONLY_COMPONENT_PREVIEW",
        "target_session": target_session.isoformat(),
        "source_last_session": source_last_session.isoformat(),
        "identity": {"status": "SOURCE_SYMBOL_ONLY", "symbol": normalized, "security_id": None},
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "source": {
            "raw_byte_sha256": source_sha256,
            "source_cutoff_id": snapshot_id,
            "source_cutoff_role": "LOCAL_FILE_SNAPSHOT_ID_NOT_CAPTURED_AVAILABLE_AT",
            "local_read_clock_is_historical_availability": False,
        },
        "production_profile_entry": {
            "status": "BLOCKED",
            "reason": "profile()正式入口仍要求可信FOUNDATION选择器；本预览不放开生产准入。",
        },
        "components": components,
        "counts": {
            "total": len(components),
            "evaluated": sum(item["status"] == "EVALUATED" for item in components),
            "observed": sum(item.get("quality") == "OBSERVED" for item in components),
            "unknown_data": sum(item.get("quality") == "UNKNOWN_DATA" for item in components),
            "not_implemented": sum(item["status"] == "NOT_IMPLEMENTED" for item in components),
            "contract_unbound": sum(item["status"] == "CONTRACT_UNBOUND" for item in components),
        },
    }


def apply_identity_observation(
    result: dict[str, Any],
    *,
    identity_projection: IdentityLifecycleProjection | None,
    identity_query_status: str,
) -> dict[str, Any]:
    """将一次Bao观察附加到已完成的TDX/Core结果，不触发重读或重算。"""
    if not isinstance(result, dict) or not isinstance(identity_query_status, str) or not identity_query_status:
        raise ResearchEntryError("IDENTITY_QUERY_STATUS_INVALID", "Bao身份结果或来源状态无效")
    if identity_projection is not None and (
            identity_projection.source_symbol != result.get("requested_symbol")
            or identity_projection.target_session.isoformat() != result.get("target_session")):
        raise ResearchEntryError("IDENTITY_PROJECTION_MISMATCH", "BaoStock身份投影与已计算TDX/Core结果不匹配")
    if identity_projection is None:
        result.setdefault("identity", {}).update({
            "status": "UNAVAILABLE",
            "reason": f"BAOSTOCK_IDENTITY_SOURCE_{identity_query_status}",
        })
    else:
        result["identity"] = {
            **identity_projection.as_dict(),
            "security_id": None,
            "stable_security_id_issued": False,
        }
    selection = result.get("canonical_selection")
    target_bar_status = selection.get("target_bar_status") if isinstance(selection, dict) else None
    if target_bar_status not in {"PRESENT", "TARGET_MISSING", "READ_FAILED", "NOT_EVALUATED"}:
        target_bar_status = "PRESENT" if result.get("target_bar_present") is True else "TARGET_MISSING"
    result["identity_reconciliation"] = reconcile_identity_bar_status(
        identity_projection,
        target_bar_status=target_bar_status,
        source_status="RESPONSE_RECEIVED" if identity_projection is not None else identity_query_status,
    )
    return result


def research_stock(
    *,
    symbol: str,
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    profile_contract: dict[str, Any] | None = None,
    include_baostock_evidence: bool = False,
    vipdata_capture: VipdataEntryCapture | None = None,
    identity_projection: IdentityLifecycleProjection | None = None,
    identity_query_status: str = "NOT_REQUESTED",
    timing_sink: dict[str, float] | None = None,
) -> dict[str, Any]:
    """验证目标会话后保留可读Raw；仅在可用前态上调用现有Core。"""
    target = _parse_target(target_session)
    calendar_payload, calendar_metadata, calendar_revision, calendar_sessions = _load_calendar(target)
    coverage_context = calendar_metadata[-1]["coverage_context"]
    calendar_years = {item["year"] for item in calendar_metadata}
    session_set = set(calendar_sessions)
    normalized, market_directory, board = _normalize_symbol(symbol)
    if identity_projection is not None and (
            not isinstance(identity_projection, IdentityLifecycleProjection)
            or identity_projection.source_symbol != normalized
            or identity_projection.target_session != target):
        raise ResearchEntryError("IDENTITY_PROJECTION_MISMATCH", "BaoStock身份投影与研究代码或目标日期不匹配")
    if not isinstance(identity_query_status, str) or not identity_query_status:
        raise ResearchEntryError("IDENTITY_QUERY_STATUS_INVALID", "Bao身份查询状态必须为非空文本")
    code = normalized.split(".")[1]
    selected: dict[date, DecodedDay] = {}
    future_source_rows_excluded = 0
    file_summary = None
    root: Path | None = None
    path: Path | None = None
    if vipdata_capture is not None:
        if (not isinstance(vipdata_capture, VipdataEntryCapture)
                or vipdata_capture.source_symbol != normalized
                or vipdata_capture.target_session != target
                or vipdata_capture.capture.source_package_entry is None):
            raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获与请求代码或目标日不匹配")
        scan_summary = vipdata_capture.scan_summary
        if not scan_summary.complete:
            raise ResearchEntryError("VIPDATA_CAPTURE_INCOMPLETE", "官网ZIP条目尚未完成只读扫描")
        for decoded in vipdata_capture.rows:
            day = decoded.session_date
            if day.year in calendar_years and day not in session_set:
                raise ResearchEntryError(
                    "SOURCE_SESSION_NOT_IN_CALENDAR",
                    f"TDX来源在已核验年度包含非交易会话记录：{day.isoformat()}",
                )
            if day > target:
                future_source_rows_excluded += 1
            elif day in session_set:
                selected[day] = decoded
        if future_source_rows_excluded != vipdata_capture.future_source_rows_excluded:
            raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "来源捕获的未来行计数与读取行不一致")
        capture_envelope = vipdata_capture.capture
    else:
        root, path = _source_path(source_roots, market_directory, code)
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
                    if day > target:
                        future_source_rows_excluded += 1
                    elif day in session_set:
                        selected[day] = located.decoded
                file_summary = scan.summary
        except DayReadError:
            raise
        except PermissionError as error:
            raise ResearchEntryError("SOURCE_ACCESS_DENIED", "操作系统拒绝读取配置的TDX日线文件") from error
        except OSError as error:
            raise ResearchEntryError("SOURCE_IO_ERROR", "只读TDX扫描遇到操作系统I/O错误") from error
        scan_summary = file_summary.scan

    session_slice = tuple(day for day in calendar_sessions if day <= target)
    if vipdata_capture is None:
        capture_request = TdxCaptureRequest(
            capture_id=str(uuid4()),
            dataset_id="TDX_DAILY_BARS",
            dataset_version="TDX_DAY_RECORD_32B_CANDIDATE",
            query_scope={
                "source_symbol": normalized,
                "target_session": target.isoformat(),
                "selection_mode": "OFFICIAL_SESSIONS_NOT_AFTER_TARGET",
                "research_mode": "NON_PIT_RETROSPECTIVE",
            },
            query_start=min(scan_summary.first_date, target),
            query_end=max(scan_summary.last_date, target),
            parser_version="stock_a.sources.tdx.day.decode_day_record",
            validation_contract="TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
        )
        capture_envelope = build_tdx_capture_envelope(file_summary, capture_request)
    canonical_selection = build_tdx_read_selection(
        source_symbol=normalized,
        target_session=target,
        official_sessions=session_slice,
        selected_rows=selected,
        capture=capture_envelope,
        calendar_id=calendar_payload["calendar_id"],
        calendar_revision=calendar_revision,
        calendar_evidence_status=_CALENDAR_EVIDENCE_VERIFIED,
        units=units,
        future_source_rows_excluded=future_source_rows_excluded,
        identity_projection=identity_projection,
    )
    bounded_calendar = MarketSessionCalendar(
        calendar_id=calendar_payload["calendar_id"],
        revision=calendar_revision,
        sessions=session_slice,
    )
    history_started = time.perf_counter()
    history = _core_history_from_selection(canonical_selection)
    if timing_sink is not None:
        timing_sink["core_history_build_elapsed_ms"] = round((time.perf_counter() - history_started) * 1000, 3)
    core_started = time.perf_counter()
    core = calculate_core(history, official_calendar=bounded_calendar)
    if timing_sink is not None:
        timing_sink["core_calculation_elapsed_ms"] = round((time.perf_counter() - core_started) * 1000, 3)
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
        source_last_session=scan_summary.last_date,
        raw_fields=raw_fields,
        raw_close=raw_close,
        core=core,
    )
    unknown_raw = sum(raw_fields[name]["quality"] != "OBSERVED" for name in _RAW_FIELDS)
    unit_dependent_core = {
        "core.amount_ratio5": {
            "unit_trust": _unit_trust_status(units.amount_to_cny),
            "interpretation": "同一窗口内的金额比率；金额单位倍率需在窗口内保持一致",
        },
        "core.amount_ratio20": {
            "unit_trust": _unit_trust_status(units.amount_to_cny),
            "interpretation": "同一窗口内的金额比率；金额单位倍率需在窗口内保持一致",
        },
        "core.liquidity_mean20": {
            "unit_trust": _unit_trust_status(units.amount_to_cny),
            "interpretation": "绝对金额均值使用调用方声明的amount_to_cny倍率",
        },
        "core.volume_ratio5": {
            "unit_trust": _unit_trust_status(units.volume_to_shares),
            "interpretation": "同一窗口内的数量比率；数量单位倍率需在窗口内保持一致",
        },
        "core.volume_ratio20": {
            "unit_trust": _unit_trust_status(units.volume_to_shares),
            "interpretation": "同一窗口内的数量比率；数量单位倍率需在窗口内保持一致",
        },
    }
    target_prices_usable = target_bar is not None and target_bar.prices.quality == "OBSERVED"
    basic_fact_dimensions, availability_explanation = _basic_fact_dimensions(
        symbol=normalized,
        target_session=target,
        source_last_session=scan_summary.last_date,
        read_time={
            "started_at": scan_summary.read_started_at.isoformat(),
            "completed_at": scan_summary.read_completed_at.isoformat(),
            "clock_role": (
                vipdata_capture.capture.source_package_entry.source_clock_role
                if vipdata_capture is not None else "LOCAL_FILE_READ_TIME_ONLY"
            ),
        },
        source={"budgets": {
            "max_bytes": limits.max_bytes,
            "max_records": limits.max_records,
            "chunk_records": limits.chunk_records,
            "max_seconds": limits.max_seconds,
        }},
        raw_fields=raw_fields,
        core=core,
        observation_summary=observation_summary,
        raw_availability=raw_availability,
        core_availability=core_availability,
        window_context={
            "calendar_revision": calendar_revision,
            "calendar_session_count": len(session_slice),
            "actual_price_bars": sum(row.state == "ACTUAL" for row in history),
            "missing_bar_sessions": sum(row.state == "UNKNOWN" for row in history),
            "lookback_complete": coverage_context["core_lookback_complete"],
            "lookback_reason": coverage_context["core_lookback_reason"],
            "future_rows_used": False,
        },
        target_bar_present=target_bar is not None,
        target_prices_usable=target_prices_usable,
    )
    result = {
        "status": "AVAILABLE" if target_prices_usable else "PARTIAL_UNKNOWN",
        "status_explanation": "兼容状态仅按目标日原始价格组判定；Raw/Core整体可用性及字段原因分别见raw_availability、core_availability和逐字段quality/reason。",
        "raw_availability": raw_availability,
        "core_availability": core_availability,
        "requested_symbol": normalized,
        "board_scope": board,
        "analysis_scope": "TDX_SOURCE_SYMBOL_RAW_SERIES_ONLY",
        "target_session": target.isoformat(),
        "source_last_session": scan_summary.last_date.isoformat(),
        "source_first_session": scan_summary.first_date.isoformat(),
        "read_time": {
            "started_at": scan_summary.read_started_at.isoformat(),
            "completed_at": scan_summary.read_completed_at.isoformat(),
            "clock_role": (
                vipdata_capture.capture.source_package_entry.source_clock_role
                if vipdata_capture is not None else "LOCAL_FILE_READ_TIME_ONLY"
            ),
        },
        "source": ({
            "configured_root": None,
            "final_path": None,
            "format_contract": "TDX_HSJDAY_ZIP_ENTRY_32_BYTE_LAYOUT_R1",
            "source_contract_status": "OFFICIAL_PACKAGE_BYTES_REVALIDATED; MEMBERSHIP_PIT_AND_UNIT_AUTHORITY_UNBOUND",
            "package_id": vipdata_capture.capture.source_package_entry.package_id,
            "package_sha256": vipdata_capture.capture.source_package_entry.package_sha256,
            "official_package_url": vipdata_capture.capture.source_package_entry.official_package_url,
            "package_verified_at": vipdata_capture.capture.source_package_entry.package_verified_at.isoformat(),
            "zip_entry_path": vipdata_capture.capture.source_package_entry.zip_entry_path,
            "zip_entry_sha256": vipdata_capture.capture.source_package_entry.entry_sha256,
            "raw_byte_sha256": scan_summary.raw_byte_sha256,
            "record_count": scan_summary.record_count,
            "byte_count": scan_summary.byte_count,
            "read_started_at": scan_summary.read_started_at.isoformat(),
            "read_completed_at": scan_summary.read_completed_at.isoformat(),
            "read_clock_role": vipdata_capture.capture.source_package_entry.source_clock_role,
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "complete_read": scan_summary.complete,
            "budgets": {
                "max_bytes": limits.max_bytes,
                "max_records": limits.max_records,
                "chunk_records": limits.chunk_records,
                "max_seconds": limits.max_seconds,
            },
        } if vipdata_capture is not None else {
            "configured_root": str(root),
            "final_path": file_summary.source.handle_path,
            "format_contract": "TDX_LOCAL_LDAY_32_BYTE_LAYOUT_R00",
            "source_contract_status": "RESEARCH_LAYOUT_ONLY_AUTHORITY_AND_CAPTURE_CLOCK_UNRESOLVED",
            "raw_byte_sha256": scan_summary.raw_byte_sha256,
            "record_count": scan_summary.record_count,
            "byte_count": scan_summary.byte_count,
            "read_started_at": scan_summary.read_started_at.isoformat(),
            "read_completed_at": scan_summary.read_completed_at.isoformat(),
            "read_clock_role": "LOCAL_FILE_READ_TIME_ONLY",
            "complete_read": scan_summary.complete,
            "budgets": {
                "max_bytes": limits.max_bytes,
                "max_records": limits.max_records,
                "chunk_records": limits.chunk_records,
                "max_seconds": limits.max_seconds,
            },
        }),
        "unit_contract": {
            "version": units.version,
            "amount_to_cny": str(units.amount_to_cny) if units.amount_to_cny is not None else None,
            "volume_to_shares": units.volume_to_shares,
            "amount_authority": _unit_trust_status(units.amount_to_cny),
            "volume_authority": _unit_trust_status(units.volume_to_shares),
            "binding_status": "CALLER_DECLARED_RESEARCH_ASSUMPTION"
            if units.amount_to_cny is not None or units.volume_to_shares is not None
            else "UNKNOWN_UNIT_AUTHORITY",
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
            **(identity_projection.as_dict() if identity_projection is not None else {
                "status": "SOURCE_SYMBOL_ONLY",
                "security_id": None,
                "reason": "OFFICIAL_STABLE_SECURITY_ID_SOURCE_NOT_BOUND",
            }),
            "security_id": None,
            "stable_security_id_issued": False,
        },
        "canonical_selection": _render_canonical_selection(canonical_selection),
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
        "basic_fact_dimensions": basic_fact_dimensions,
        "availability_explanation": availability_explanation,
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
    if identity_projection is not None or identity_query_status != "NOT_REQUESTED":
        apply_identity_observation(
            result,
            identity_projection=identity_projection,
            identity_query_status=identity_query_status,
        )
    if profile_contract is not None:
        result["profile_preview"] = _build_profile_preview(
            symbol=normalized,
            target_session=target,
            source_last_session=scan_summary.last_date,
            source_sha256=scan_summary.raw_byte_sha256,
            target_bar=target_bar,
            history=history,
            core=core,
            calendar_revision=calendar_revision,
            contract=profile_contract,
        )
    if vipdata_capture is not None:
        source_ref = vipdata_capture.capture.source_package_entry
        result["source_package"] = {
            "package_id": source_ref.package_id,
            "sha256": source_ref.package_sha256,
            "official_package_url": source_ref.official_package_url,
            "file_size": source_ref.package_file_size,
            "zip_entry_count": source_ref.zip_entry_count,
            "zip_crc": source_ref.zip_crc,
            "verified_at": source_ref.package_verified_at.isoformat(),
            "zip_entry_path": source_ref.zip_entry_path,
            "zip_entry_sha256": source_ref.entry_sha256,
            "zip_entry_bytes": source_ref.entry_byte_count,
            "target_session": source_ref.target_session.isoformat(),
            "entry_observed_started_at": source_ref.observed_started_at.isoformat(),
            "entry_observed_completed_at": source_ref.observed_completed_at.isoformat(),
            "published_at": "UNKNOWN",
            "first_available_at": "UNKNOWN",
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "membership_and_listing_status": "UNKNOWN_NOT_OFFICIALLY_BOUND",
            "price_coordinate": "RAW_UNADJUSTED",
            "amount_unit_authority": _unit_trust_status(units.amount_to_cny),
            "volume_unit_authority": _unit_trust_status(units.volume_to_shares),
            "acceptance_status": "NOT_ACCEPTED_NOT_PERSISTED",
            "profile_admission": "DENY_TRUSTED_FOUNDATION_SELECTOR_REQUIRED",
        }
    if type(include_baostock_evidence) is not bool:
        raise ResearchEntryError("BAOSTOCK_OPTION_INVALID", "BaoStock证据开关必须为布尔值")
    if include_baostock_evidence:
        # 可选SDK仅在用户显式请求research-stock补充证据时延迟导入。
        from stock_a.sources.baostock.client import query_daily_rows
        from stock_a.sources.baostock.evidence import project_baostock_evidence

        query = query_daily_rows(normalized, target.isoformat())
        result["baostock_evidence"] = project_baostock_evidence(
            query=query,
            requested_symbol=normalized,
            target_session=target.isoformat(),
            tdx_result=result,
        )
    return result


def research_compare(
    *,
    symbols: Iterable[str],
    target_session: str,
    source_roots: Iterable[str],
    units: UnitContract,
    limits: ReadLimits,
    budget: CompareBudget | None = None,
    profile_contract: dict[str, Any] | None = None,
    source_captures: Mapping[str, VipdataEntryCapture] | None = None,
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
    if source_captures is not None:
        if (not isinstance(source_captures, Mapping)
                or any(not isinstance(key, str) or not isinstance(value, VipdataEntryCapture)
                       for key, value in source_captures.items())
                or not set(normalized).issubset(source_captures)):
            raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获没有覆盖本次对比请求")
        if any(source_captures[symbol].target_session.isoformat() != target_session
               for symbol in normalized):
            raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获目标日与本次对比不一致")
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
                profile_contract=profile_contract,
                vipdata_capture=source_captures.get(item) if source_captures is not None else None,
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
    profile_contract: dict[str, Any] | None = None,
    source_candidate_pool: Sequence[Mapping[str, Any]] | None = None,
    source_candidate_catalog: Mapping[str, Any] | None = None,
    source_captures: Mapping[str, VipdataEntryCapture] | None = None,
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
    if source_candidate_pool is None:
        if source_candidate_catalog is not None or source_captures is not None:
            raise ResearchEntryError("UNIVERSE_CANDIDATE_OVERRIDE_INVALID", "候选目录摘要必须与候选池同时提供")
        candidates, catalog = _discover_source_candidates(roots, discovery_budget)
    else:
        if (not isinstance(source_candidate_catalog, Mapping)
                or (not roots and source_captures is None)):
            raise ResearchEntryError("UNIVERSE_CANDIDATE_OVERRIDE_INVALID", "显式候选池必须绑定来源根和目录摘要")
        try:
            candidates = tuple(dict(item) for item in source_candidate_pool)
        except (TypeError, ValueError) as error:
            raise ResearchEntryError("UNIVERSE_CANDIDATE_OVERRIDE_INVALID", "显式候选池格式无效") from error
        if (source_candidate_catalog.get("complete") is not True
                or source_candidate_catalog.get("candidate_count") != len(candidates)
                or any(not isinstance(item.get("symbol"), str)
                       or item.get("market") not in {"SH", "SZ"}
                       or item.get("board") not in {"MAIN_BOARD", "GEM"}
                       for item in candidates)
                or len({item["symbol"] for item in candidates}) != len(candidates)):
            raise ResearchEntryError("UNIVERSE_CANDIDATE_OVERRIDE_INVALID", "显式候选池与目录摘要不一致")
        candidates = tuple(sorted(candidates, key=lambda item: item["symbol"]))
        catalog = dict(source_candidate_catalog)
    selected = tuple(
        candidate for candidate in candidates
        if (market_filter == "ALL" or candidate["market"] == market_filter)
        and (board_filter == "ALL" or candidate["board"] == board_filter)
    )
    candidate_total = len(selected)
    page_candidates = selected[offset:offset + limit]
    candidate_by_symbol = {item["symbol"]: item for item in page_candidates}
    scannable_candidates = tuple(item for item in page_candidates if item.get("empty_placeholder") is not True)
    if source_captures is not None:
        if not isinstance(source_captures, Mapping):
            raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获必须是按代码索引的映射")
        if set(source_captures) != {item["symbol"] for item in scannable_candidates}:
            raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "官网ZIP来源捕获未精确覆盖本页非空条目")
        for candidate in scannable_candidates:
            symbol = candidate["symbol"]
            capture = source_captures.get(symbol)
            source_ref = capture.capture.source_package_entry if isinstance(capture, VipdataEntryCapture) else None
            if (not isinstance(capture, VipdataEntryCapture)
                    or capture.target_session != target
                    or capture.source_symbol != symbol
                    or source_ref is None
                    or source_ref.package_sha256 != catalog.get("source_package_sha256")
                    or source_ref.zip_entry_path != candidate.get("zip_entry_path")):
                raise ResearchEntryError("VIPDATA_CAPTURE_MISMATCH", "ZIP候选页与Capture的包/条目/目标日身份不一致")
    page_results: list[dict[str, Any]] = []
    total_bytes = 0
    total_records = 0
    stop_code: str | None = None
    next_position = 0
    for candidate in page_candidates:
        if candidate.get("empty_placeholder") is True:
            page_results.append({
                "status": "FAILED",
                "requested_symbol": candidate["symbol"],
                "error_code": "ZIP_DAY_ENTRY_EMPTY",
                "message": "官方ZIP将此证券作为零字节占位；未向Core提供行情",
                "read_usage": {"bytes": 0, "records": 0},
                "source_candidate": {**candidate, "contract": "TDX_SOURCE_CANDIDATES_NON_PIT"},
            })

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
                (item["symbol"] for item in scannable_candidates[next_position:]), stop_code
            )
            break

        chunk_candidates = scannable_candidates[next_position:next_position + _MAX_COMPARE_SYMBOLS]
        chunk = research_compare(
            symbols=tuple(item["symbol"] for item in chunk_candidates),
            target_session=target.isoformat(),
            source_roots=roots,
            units=units,
            limits=limits,
            budget=CompareBudget(remaining_bytes, remaining_records, remaining_seconds),
            profile_contract=profile_contract,
            source_captures=(
                {item["symbol"]: source_captures[item["symbol"]] for item in chunk_candidates}
                if source_captures is not None else None
            ),
        )
        total_bytes += chunk["budget"]["used_bytes"]
        total_records += chunk["budget"]["used_records"]
        for result in chunk["results"]:
            candidate = candidate_by_symbol[result["requested_symbol"]]
            if candidate.get("empty_placeholder") is True and result.get("status") == "FAILED":
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
                (item["symbol"] for item in scannable_candidates[next_position:]), stop_code
            )
            break

    page_order = {item["symbol"]: index for index, item in enumerate(page_candidates)}
    page_results.sort(key=lambda item: page_order[item["requested_symbol"]])

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

    profile_preview_denominators: dict[str, Any] | None = None
    if profile_contract is not None:
        preview_results = [item for item in page_results if isinstance(item.get("profile_preview"), dict)]
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
            "object_partition_complete": len(preview_results) + failed + not_evaluated == len(page_candidates),
            "components": component_counts,
        }

    short_summary: list[dict[str, Any]] = []
    for item in page_results:
        candidate = item["source_candidate"]
        observation = item.get("observation_summary", {})
        fields = observation.get("fields", {})
        positions = fields.get("moving_average_position", {})
        summary_item = {
            "symbol": candidate["symbol"],
            "market": candidate["market"],
            "board": candidate["board"],
            "target_session": target.isoformat(),
            "status": item["status"],
            "target_bar_present": item.get("target_bar_present"),
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
            "basic_fact_dimensions": item.get("basic_fact_dimensions"),
            "availability_explanation": item.get("availability_explanation"),
            "detail_query": {
                "arguments": [
                    "research-stock", candidate["symbol"],
                    "--target-session", target.isoformat(),
                    "--unit-version", units.version,
                    "--amount-to-cny", str(units.amount_to_cny),
                    "--volume-to-shares", str(units.volume_to_shares),
                    "--max-bytes", str(limits.max_bytes),
                    "--max-records", str(limits.max_records),
                    "--chunk-records", str(limits.chunk_records),
                    "--max-seconds", str(limits.max_seconds),
                    "--format", "summary",
                    "--include-profile-preview",
                ],
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
                            "field_id", "rule_id", "expression_ref", "status", "quality",
                            "value", "reason", "missing_inputs", "input_fields",
                            "production_eligibility",
                        )
                        if key in component
                    }
                    for component in preview["components"]
                ],
            }
        short_summary.append(summary_item)

    page_remaining = max(0, candidate_total - min(offset + len(page_candidates), candidate_total))
    evaluated_page = available + failed + partial_unknown
    status = "COMPLETE_WITH_DISCLOSURES" if failed == 0 and partial_unknown == 0 and not_evaluated == 0 else "PARTIAL"
    completed_reads = [item for item in page_results if isinstance(item.get("source_last_session"), str)]
    source_last_session_counts: dict[str, int] = {}
    for item in completed_reads:
        last_session = item["source_last_session"]
        source_last_session_counts[last_session] = source_last_session_counts.get(last_session, 0) + 1
    latest_source_session = max(source_last_session_counts, default=None)
    page_coverage_by_market: dict[str, dict[str, int]] = {}
    page_coverage_by_board: dict[str, dict[str, int]] = {}
    for dimension, key in ((page_coverage_by_market, "market"), (page_coverage_by_board, "board")):
        for value in sorted({item["source_candidate"][key] for item in page_results}):
            matching = [item for item in page_results if item["source_candidate"][key] == value]
            dimension[value] = {
                "candidate_count": len(matching),
                "evaluated_count": sum(item.get("status") != "NOT_EVALUATED" for item in matching),
                "not_evaluated_count": sum(item.get("status") == "NOT_EVALUATED" for item in matching),
                "actual_bar_on_target_count": sum(item.get("target_bar_present") is True for item in matching),
                "missing_target_bar_count": sum(item.get("target_bar_present") is False for item in matching),
                "source_read_failed_count": sum(item.get("status") == "FAILED" for item in matching),
            }
    coverage_summary = {
        "candidate_contract": "TDX_SOURCE_CANDIDATES_NON_PIT",
        "candidate_total_in_filtered_pool": candidate_total,
        "page_candidate_count": len(page_candidates),
        "evaluated_count": evaluated_page,
        "not_evaluated_count": not_evaluated,
        "actual_bar_on_target_count": sum(item.get("target_bar_present") is True for item in page_results),
        "missing_target_bar_count": sum(item.get("target_bar_present") is False for item in page_results),
        "source_read_failed_count": failed,
        "latest_source_session": latest_source_session,
        "latest_source_session_scope": "本页已完整读到EOF并核验句柄的来源文件末条日期最大值；不是全市场正式可得时钟",
        "source_last_session_counts": dict(sorted(source_last_session_counts.items())),
        "by_market": page_coverage_by_market,
        "by_board": page_coverage_by_board,
        "page_partition_complete": classified == len(page_candidates),
        "filtered_pool_complete": offset == 0 and page_remaining == 0 and not_evaluated == 0,
        "pool_completion_note": "本入口只知道当前页；只有offset=0且单页覆盖完整过滤池时才标true。多页汇总须由调用方确认每一计划页均已实际完成；不代表官方上市总体",
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
    if profile_preview_denominators is not None:
        result["profile_preview_denominators"] = profile_preview_denominators
    return result
