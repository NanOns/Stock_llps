"""P03来源集合候选的官方日历校验与应用用例。"""

from __future__ import annotations

from datetime import date
import hashlib
from importlib.resources import files
import json
from typing import Any, Mapping
from urllib.parse import urlparse

from stock_a.storage.postgres.source_set_candidate import (
    persist_source_set_candidate as persist_pg_candidate,
    prepare_source_set_candidate as prepare_pg_candidate,
    read_source_set_candidate as read_pg_candidate,
)


class SourceSetCandidateApplicationError(ValueError):
    """候选目标交易日或版本化官方日历不满足合同。"""


def _official_calendar(target_session: date) -> dict[str, Any]:
    if type(target_session) is not date:
        raise SourceSetCandidateApplicationError("必须显式提供目标交易会话日期")
    filename = f"cn_equity_sessions_{target_session.year}.json"
    try:
        resource = files("stock_a.research.calendars").joinpath(filename)
        raw = resource.read_bytes()
    except (FileNotFoundError, OSError, ModuleNotFoundError) as error:
        raise SourceSetCandidateApplicationError(
            "缺少对应年份的版本化官方交易日历资源"
        ) from error
    if not raw or len(raw) > 64 * 1024:
        raise SourceSetCandidateApplicationError(
            "官方交易日历资源为空或超过固定读取上限"
        )
    try:
        calendar = json.loads(
            raw.decode("utf-8"),
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise SourceSetCandidateApplicationError(
            "官方交易日历不是有效UTF-8 JSON"
        ) from error
    if not isinstance(calendar, Mapping):
        raise SourceSetCandidateApplicationError("官方交易日历顶层必须是对象")
    sessions = calendar.get("sessions")
    source_urls = calendar.get("source_urls")
    if (
        calendar.get("contract") != "STOCK-A-OFFICIAL-EXCHANGE-CALENDAR-DATA/R1"
        or calendar.get("calendar_id") != "CN_SSE_SZSE_EQUITY"
        or calendar.get("market_scope") != ["SSE", "SZSE"]
        or calendar.get("evidence_status") != "VERIFIED_EXCHANGE_NOTICES_CROSSCHECKED"
        or not isinstance(calendar.get("revision"), str)
        or not calendar["revision"]
        or not isinstance(sessions, list)
        or not isinstance(source_urls, list)
        or not source_urls
    ):
        raise SourceSetCandidateApplicationError("官方交易日历身份或交叉核验状态无效")
    if any(not isinstance(item, str) for item in sessions) or sessions != sorted(
        set(sessions)
    ):
        raise SourceSetCandidateApplicationError(
            "官方交易日历会话列表不是唯一、稳定排序的日期集合"
        )
    try:
        date.fromisoformat(str(calendar["valid_from"]))
        date.fromisoformat(str(calendar["valid_through"]))
        reviewed = date.fromisoformat(str(calendar["source_snapshot_as_of"]))
    except (KeyError, ValueError) as error:
        raise SourceSetCandidateApplicationError(
            "官方交易日历有效范围或审阅日期无效"
        ) from error
    if (
        not calendar["valid_from"]
        <= target_session.isoformat()
        <= calendar["valid_through"]
    ):
        raise SourceSetCandidateApplicationError(
            "目标会话超出版本化官方交易日历有效范围"
        )
    if reviewed > target_session:
        raise SourceSetCandidateApplicationError("日历证据审阅日期晚于目标会话")
    official_hosts = {
        urlparse(value).hostname for value in source_urls if isinstance(value, str)
    }
    if not {"www.sse.com.cn", "www.szse.cn"}.issubset(official_hosts):
        raise SourceSetCandidateApplicationError("官方日历缺少沪深交易所公告交叉来源")
    if target_session.isoformat() not in sessions:
        raise SourceSetCandidateApplicationError("目标日期不是版本化官方交易会话")
    return {
        "calendar_id": calendar["calendar_id"],
        "calendar_revision": calendar["revision"],
        "calendar_sha256": hashlib.sha256(raw).hexdigest(),
        "sessions": tuple(date.fromisoformat(item) for item in sessions),
    }


def prepare_source_set_candidate(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
) -> dict[str, Any]:
    """校验官方会话并只读计算临时来源集合候选。"""
    calendar = _official_calendar(target_session)
    return prepare_pg_candidate(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        calendar=calendar,
    )


def read_source_set_candidate(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str
) -> dict[str, Any]:
    """按精确日期与包SHA只读回已保存的候选。"""
    return read_pg_candidate(
        config, target_session=target_session, package_sha256=package_sha256
    )


def persist_source_set_candidate(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str
) -> dict[str, Any]:
    """经本次P00/P02核算后，幂等保存精确NON_PIT来源候选。"""
    from stock_a.application.source_ingress import build_source_ingress_plan

    calendar = _official_calendar(target_session)
    ingress = build_source_ingress_plan(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
    )
    if (
        ingress.get("scope") != "SOURCE_INGRESS"
        or ingress.get("target_session") != target_session.isoformat()
        or ingress.get("package_sha256") != package_sha256
        or not isinstance(ingress.get("plan", {}).get("plan_sha256"), str)
    ):
        raise SourceSetCandidateApplicationError("P00计划没有绑定精确候选身份")
    result = persist_pg_candidate(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        calendar=calendar,
        bao_capability=ingress["p02_baostock"],
    )
    return {
        **result,
        "source_ingress_plan": ingress["plan"],
        "environment_gate": ingress["environment_gate"],
        "p02_baostock": ingress["p02_baostock"],
        "formal_p03_complete": False,
    }
