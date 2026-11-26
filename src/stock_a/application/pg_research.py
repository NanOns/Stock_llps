"""将明确BaoStock价格捕获绑定到PostgreSQL价格事实。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres.baostock_price_facts import (
    BaoStockPriceFactsError,
    read_baostock_price_catalog,
    read_baostock_capture_roster,
)


class PgResearchBindingError(Exception):
    """价格捕获与用户请求的PG事实无法精确闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _is_target_session(value: Any, target_session: date) -> bool:
    if isinstance(value, date):
        return value == target_session
    return value == target_session.isoformat()


def bind_source_context_to_raw_rows(
    source_context: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    target_raw_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """核对所选paired捕获与目标日BaoStock价格行。"""
    if (
        source_context.get("status") != "AVAILABLE"
        or source_context.get("target_session") != target_session.isoformat()
        or source_context.get("source_capture_id") != source_capture_id
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_SOURCE_VERSION_MISMATCH",
            "来源捕获上下文与请求日期或source_capture_id不一致。",
        )
    target_rows = [
        row
        for row in target_raw_rows
        if _is_target_session(row.get("session_date"), target_session)
    ]
    if not target_rows:
        raise PgResearchBindingError(
            "PG_RESEARCH_TARGET_PRICE_MISSING",
            "BaoStock价格查询没有返回精确目标日行。",
        )
    if any(
        row.get("source_capture_id") != source_capture_id
        or row.get("source_symbol") is None
        for row in target_rows
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_PRICE_REFERENCE_MISMATCH",
            "目标日价格行没有绑定同一BaoStock捕获与来源代码。",
        )
    bound_context = dict(source_context)
    bound_context["target_price_binding_count"] = len(target_rows)
    bound_context["target_price_binding_verified"] = True
    return bound_context


def read_source_version_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int | None,
    research_cutoff: date | None = None,
    target_raw_rows: Sequence[Mapping[str, Any]] = (),
    expected_raw_row_count: int | None = None,
    price_coordinate: str | None = None,
) -> dict[str, Any]:
    """读取所需BaoStock坐标上下文；捕获身份只用于溯源，不替代事实键。"""
    if type(target_session) is not date:
        raise PgResearchBindingError(
            "PG_RESEARCH_TARGET_SESSION_INVALID", "研究来源上下文必须绑定官方交易日"
        )
    cutoff = target_session if research_cutoff is None else research_cutoff
    if type(cutoff) is not date or cutoff < target_session:
        raise PgResearchBindingError(
            "PG_RESEARCH_CUTOFF_INVALID", "来源上下文截止日必须不早于目标会话"
        )
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_CAPTURE_ID_INVALID", "source_capture_id必须为正整数或空值"
        )
    try:
        coordinate_scoped = price_coordinate is not None
        catalog = read_baostock_price_catalog(
            config,
            cutoff=cutoff,
            # 指定坐标时先读该坐标的完整当前有效事实总体，不能用任务捕获
            # 截掉由其它合法请求捕获写入的证券行。
            source_capture_id=source_capture_id,
        )
    except BaoStockPriceFactsError as error:
        raise PgResearchBindingError(error.code, str(error)) from error
    versions = [
        item
        for item in catalog.get("price_coordinate_versions", [])
        if item.get("session_date") == target_session.isoformat()
        and (
            source_capture_id is None
            or item.get("source_capture_id") == source_capture_id
        )
        and (
            price_coordinate is None or item.get("price_coordinate") == price_coordinate
        )
    ]
    capture_ids = {item.get("source_capture_id") for item in versions}
    if not versions:
        raise PgResearchBindingError(
            "PG_RESEARCH_SOURCE_VERSION_UNAVAILABLE",
            "该日期没有可读的paired BaoStock价格捕获。",
        )
    if not coordinate_scoped and (len(capture_ids) != 1 or None in capture_ids):
        raise PgResearchBindingError(
            "PG_RESEARCH_SOURCE_VERSION_AMBIGUOUS",
            "该目标日对应多个BaoStock捕获，必须明确指定source_capture_id。",
        )
    capture_id = next(iter(capture_ids)) if len(capture_ids) == 1 else None
    if coordinate_scoped and source_capture_id is not None and capture_id is not None:
        if source_capture_id != capture_id:
            raise PgResearchBindingError(
                "PG_RESEARCH_COORDINATE_CAPTURE_MISMATCH",
                "指定捕获与目标价格坐标的唯一真实来源捕获不一致。",
            )
    roster = None
    if capture_id is not None:
        try:
            roster = read_baostock_capture_roster(
                config,
                source_capture_id=int(capture_id),
                cutoff=cutoff,
            )
        except BaoStockPriceFactsError as error:
            if not coordinate_scoped:
                raise PgResearchBindingError(error.code, str(error)) from error
    if not coordinate_scoped and (
        not isinstance(roster, Mapping)
        or roster.get("status") != "AVAILABLE"
        or roster.get("source_capture_id") != capture_id
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_CAPTURE_CLOCK_UNAVAILABLE",
            "BaoStock capture roster缺少已存的真实read_started_at/validated_at。",
        )
    by_coordinate = {item.get("price_coordinate"): item for item in versions}
    raw = by_coordinate.get("RAW_UNADJUSTED")
    qfq = by_coordinate.get("FORWARD_ADJUSTED")
    if (not coordinate_scoped and (raw is None or qfq is None)) or (
        coordinate_scoped and price_coordinate not in by_coordinate
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_PAIRED_COORDINATE_MISSING",
            "目标日paired捕获缺少RAW_UNADJUSTED或FORWARD_ADJUSTED目录事实。",
        )
    selected = by_coordinate.get(price_coordinate) if coordinate_scoped else raw
    raw_rows = selected.get("row_count") if selected is not None else None
    expected_rows = selected.get("expected_row_count") if selected is not None else None
    if (
        expected_raw_row_count is not None
        and price_coordinate in (None, "RAW_UNADJUSTED")
        and (
            type(expected_raw_row_count) is not int
            or expected_raw_row_count != raw_rows
        )
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_RAW_COUNT_MISMATCH",
            "目标日RAW行数与BaoStock捕获目录不一致。",
        )
    if price_coordinate is not None and price_coordinate not in {
        "RAW_UNADJUSTED",
        "FORWARD_ADJUSTED",
    }:
        raise PgResearchBindingError(
            "PG_RESEARCH_PRICE_COORDINATE_INVALID", "价格坐标不在固定BaoStock坐标枚举中"
        )
    if target_raw_rows and capture_id is not None:
        target_rows = [
            row
            for row in target_raw_rows
            if _is_target_session(row.get("session_date"), target_session)
        ]
        if not target_rows or any(
            row.get("source_capture_id") != capture_id for row in target_rows
        ):
            raise PgResearchBindingError(
                "PG_RESEARCH_PRICE_REFERENCE_MISMATCH",
                "读取的目标价格行与paired捕获身份不一致。",
            )
    from stock_a.sources.baostock.identity import eligible_research_board

    target_iso = target_session.isoformat()
    research_expected_rows = (
        sum(
            1
            for item in roster.get("symbols", [])
            if eligible_research_board(item.get("source_symbol")) is not None
            and item.get("eligible_start_session")
            and item.get("eligible_end_session")
            and item["eligible_start_session"]
            <= target_iso
            <= item["eligible_end_session"]
        )
        if roster is not None
        else None
    )
    complete = (
        selected.get("coverage_status") == "COMPLETE"
        if coordinate_scoped and selected is not None
        else raw is not None
        and qfq is not None
        and raw.get("coverage_status") == "COMPLETE"
        and qfq.get("coverage_status") == "COMPLETE"
    )
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "source_capture_id": capture_id,
        "capture_read_started_at": (roster or {}).get("captured_at_start"),
        "capture_validated_at": (roster or {}).get("captured_at_validated"),
        "capture_started_at": (roster or {}).get("captured_at_start"),
        "capture_verified_at": (roster or {}).get("captured_at_validated"),
        "provider": "BAOSTOCK",
        "source_time_quality": (roster or {}).get("source_time_quality") or "UNKNOWN",
        "source_clock_role": (roster or {}).get("source_clock_role") or "UNKNOWN",
        "source_available_at": None,
        "price_coordinate": price_coordinate,
        "price_coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
        "raw_row_count": raw.get("row_count") if raw is not None else None,
        "qfq_row_count": qfq.get("row_count") if qfq is not None else None,
        "capture_expected_row_count": expected_rows,
        "research_expected_row_count": research_expected_rows,
        "raw_coverage_status": raw.get("coverage_status")
        if raw is not None
        else "NOT_REQUESTED",
        "qfq_coverage_status": qfq.get("coverage_status")
        if qfq is not None
        else "NOT_REQUESTED",
        "version_status": "COMPLETE" if complete else "INCOMPLETE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "listing_status": "LISTING_STATUS_UNKNOWN",
        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
        "official_listing_universe_status": "UNKNOWN",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "database_write": False,
        "ddl_executed": False,
        "source_io_attempted": False,
        "candidate_fallback": False,
    }
