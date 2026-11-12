"""Stock A研究sessions职责模块。"""

from __future__ import annotations

from datetime import date, timedelta
from importlib.resources import files
import json
import re
from typing import Any, Iterable, Mapping, Sequence
from stock_a.research.core import (
    CORE_MAX_ACTUAL_LOOKBACK,
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)

from stock_a.research.entry_context.errors import ResearchEntryError

_CALENDAR_RESOURCE_PATTERN = re.compile(r"^cn_equity_sessions_([0-9]{4})\.json$")
_CALENDAR_EVIDENCE_VERIFIED = "VERIFIED_EXCHANGE_NOTICES_CROSSCHECKED"


def _calendar_payload(
    resource_name: str, year: int
) -> tuple[dict[str, Any], tuple[date, ...]]:
    try:
        raw = files("stock_a.research.calendars").joinpath(resource_name).read_bytes()
    except OSError as error:
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_UNAVAILABLE", "随包日历资源不可读取"
        ) from error
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "内置日历资源无法解析"
        ) from error
    if not isinstance(payload, dict) or not isinstance(payload.get("sessions"), list):
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历结构无效")
    if payload.get("evidence_status") != _CALENDAR_EVIDENCE_VERIFIED:
        raise ResearchEntryError(
            "CALENDAR_UNVERIFIED", f"{year}年度日历证据未核验，已阻止计算"
        )
    if payload.get("weekday_policy") != "WEEKDAYS_EXCEPT_LISTED_OFFICIAL_CLOSURES":
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", f"{year}年度日历缺少已登记的工作日规则"
        )
    if payload.get("weekend_policy") != "周六周日休市；不因补班推断周末开市":
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", f"{year}年度日历缺少周末休市合同"
        )
    revision = payload.get("revision")
    if not isinstance(revision, str) or not revision:
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", f"{year}年度日历缺少版本标识"
        )
    try:
        first = date.fromisoformat(payload["valid_from"])
        last = date.fromisoformat(payload["valid_through"])
        evidence_reviewed_at = date.fromisoformat(payload["evidence_reviewed_at"])
        source_snapshot_as_of = date.fromisoformat(payload["source_snapshot_as_of"])
        sessions = tuple(date.fromisoformat(day) for day in payload["sessions"])
        closures = payload["closed_weekday_ranges"]
        source_urls = payload["source_urls"]
    except (KeyError, TypeError, ValueError) as error:
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "内置日历日期内容无效"
        ) from error
    if first != date(year, 1, 1) or last != date(year, 12, 31):
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "年度日历覆盖边界必须完整且与文件年份一致"
        )
    if (
        not isinstance(source_urls, list)
        or not source_urls
        or any(
            not isinstance(url, str) or not url.startswith("https://")
            for url in source_urls
        )
    ):
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "年度日历必须列出可追溯的HTTPS证据来源"
        )
    closed: set[date] = set()
    previous_end: date | None = None
    if not isinstance(closures, list):
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "年度休市区间必须是有序列表"
        )
    for item in closures:
        if not isinstance(item, list) or len(item) != 2:
            raise ResearchEntryError(
                "CALENDAR_RESOURCE_INVALID", "年度休市区间必须包含起止日期"
            )
        try:
            start, end = (date.fromisoformat(value) for value in item)
        except (TypeError, ValueError) as error:
            raise ResearchEntryError(
                "CALENDAR_RESOURCE_INVALID", "年度休市区间日期无效"
            ) from error
        if (
            start > end
            or start < first
            or end > last
            or (previous_end is not None and start <= previous_end)
        ):
            raise ResearchEntryError(
                "CALENDAR_RESOURCE_INVALID", "年度休市区间越界、重叠或错序"
            )
        closed.update(
            start + timedelta(days=offset) for offset in range((end - start).days + 1)
        )
        previous_end = end
    if tuple(sorted(set(sessions))) != sessions:
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "内置日历会话必须唯一且按日期递增"
        )
    expected = tuple(
        first + timedelta(days=offset)
        for offset in range((last - first).days + 1)
        if (first + timedelta(days=offset)).weekday() < 5
        and (first + timedelta(days=offset)) not in closed
    )
    if sessions != expected:
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "内置会话与官方休市区间不闭合"
        )
    if payload.get("calendar_id") != "CN_SSE_SZSE_EQUITY":
        raise ResearchEntryError("CALENDAR_RESOURCE_INVALID", "内置日历市场身份不匹配")
    return payload, sessions


def _parse_target(value: str) -> date:
    try:
        result = date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ResearchEntryError(
            "TARGET_DATE_INVALID", "目标会话必须使用YYYY-MM-DD有效日期"
        ) from error
    if result.isoformat() != value:
        raise ResearchEntryError(
            "TARGET_DATE_INVALID", "目标会话必须使用规范YYYY-MM-DD格式"
        )
    return result


def _load_calendar(
    target: date,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...], str, tuple[date, ...]]:
    calendar_package = files("stock_a.research.calendars")
    resources: dict[int, str] = {}
    for item in calendar_package.iterdir():
        match = _CALENDAR_RESOURCE_PATTERN.fullmatch(item.name)
        if match:
            resources[int(match.group(1))] = item.name
    if target.year not in resources:
        raise ResearchEntryError(
            "CALENDAR_NOT_COVERED", "没有已核验的年度资源覆盖目标日期；本次不推断交易日"
        )
    available_years = sorted(year for year in resources if year <= target.year)
    selected_years_descending: list[int] = []
    year = target.year
    while year in resources:
        selected_years_descending.append(year)
        year -= 1
    selected_years = tuple(reversed(selected_years_descending))
    loaded: list[tuple[int, dict[str, Any], tuple[date, ...]]] = []
    for year in selected_years:
        payload, sessions = _calendar_payload(resources[year], year)
        loaded.append((year, payload, sessions))
    target_item = next(item for item in loaded if item[0] == target.year)
    target_sessions = target_item[2]
    if target not in target_sessions:
        raise ResearchEntryError(
            "TARGET_NOT_TRADING_SESSION", "目标日期不是资源中已核验的交易会话"
        )
    sessions = tuple(day for _, _, year_sessions in loaded for day in year_sessions)
    if any(left >= right for left, right in zip(sessions, sessions[1:])):
        raise ResearchEntryError(
            "CALENDAR_RESOURCE_INVALID", "跨年度日历资源重叠或错序"
        )
    current_year_sessions_to_target = sum(
        day.year == target.year and day <= target for day in target_sessions
    )
    missing_intermediate_years = tuple(
        missing_year
        for missing_year in range(available_years[0], target.year)
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
        "resource_continuity_status": "GAP_DETECTED"
        if missing_intermediate_years
        else "CONTIGUOUS",
        "required_actual_sessions": CORE_MAX_ACTUAL_LOOKBACK,
        "current_year_sessions_to_target": current_year_sessions_to_target,
        "available_calendar_sessions_to_target": sum(day <= target for day in sessions),
        "prior_year_resource_available": prior_calendar_available,
        "core_lookback_complete": core_lookback_complete,
        "core_lookback_reason": None
        if core_lookback_complete
        else "CALENDAR_LOOKBACK_NOT_COVERED",
    }
    metadata = tuple(
        {
            "calendar_id": payload["calendar_id"],
            "revision": payload["revision"],
            "year": year,
            "evidence_status": payload["evidence_status"],
            "evidence_reviewed_at": payload["evidence_reviewed_at"],
            "source_snapshot_as_of": payload["source_snapshot_as_of"],
            "coverage": [payload["valid_from"], payload["valid_through"]],
            "session_count": len(year_sessions),
            "source_urls": payload["source_urls"],
        }
        for year, payload, year_sessions in loaded
    )
    metadata = (*metadata[:-1], {**metadata[-1], "coverage_context": coverage_context})
    revisions = "+".join(f"{item['year']}:{item['revision']}" for item in metadata)
    return target_item[1], metadata, revisions, sessions
