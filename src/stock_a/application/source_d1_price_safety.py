"""为已分页的D1板块成员装配来源级价格损伤事实。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from fractions import Fraction
from time import perf_counter
from typing import Any, Mapping, Sequence

from stock_a.application.source_set_candidate import _official_calendar
from stock_a.domain import FieldValue, unknown
from stock_a.research.core import (
    CoreObservation,
    MarketSessionCalendar,
    calculate_core,
)
from stock_a.research.d0_price_facts import calculate_d0_first_day_damage
from stock_a.research.entry import ResearchEntryError, _load_calendar
from stock_a.storage.postgres.source_raw import read_raw_daily_history_batch


class PriceSafetyFactError(ValueError):
    """价格事实装配无法安全绑定到当前D1返回成员。"""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def attach_source_d1_price_safety_facts(
    result: dict[str, Any],
    *,
    config: Mapping[str, Any],
    discovery: Mapping[str, Any],
    target_session: date,
    research_cutoff: date,
) -> dict[str, Any]:
    """为当前显示的成员页及变化个股页一次性读取RAW事实。"""
    started = perf_counter()
    detail = result.get("detail_board")
    detail_members: list[dict[str, Any]] | None = None
    if isinstance(detail, dict):
        page = detail.get("member_page")
        members = page.get("members") if isinstance(page, dict) else None
        if not isinstance(members, list) or any(
            not isinstance(member, dict) for member in members
        ):
            raise PriceSafetyFactError(
                "PRICE_SAFETY_MEMBER_PAGE_INVALID", "当前返回缺少精确板块成员分页。"
            )
        detail_members = members
    elif detail is not None:
        raise PriceSafetyFactError(
            "PRICE_SAFETY_MEMBER_PAGE_INVALID", "当前板块明细不是有效对象。"
        )

    changed_members: list[dict[str, Any]] | None = None
    if "changed_stock_page" in result:
        changed_page = result.get("changed_stock_page")
        changed_rows = (
            changed_page.get("stocks") if isinstance(changed_page, dict) else None
        )
        page_meta = changed_page.get("page") if isinstance(changed_page, dict) else None
        if (
            not isinstance(changed_rows, list)
            or any(not isinstance(member, dict) for member in changed_rows)
            or not isinstance(page_meta, dict)
            or type(page_meta.get("limit")) is not int
            or not 1 <= page_meta["limit"] <= 30
            or len(changed_rows) > page_meta["limit"]
        ):
            raise PriceSafetyFactError(
                "PRICE_SAFETY_CHANGED_STOCK_PAGE_INVALID",
                "全局变化个股分页形状或上限无效。",
            )
        changed_symbols = [member.get("source_symbol") for member in changed_rows]
        if (
            any(not isinstance(symbol, str) for symbol in changed_symbols)
            or len(changed_symbols) != len(set(changed_symbols))
            or any(member.get("change_state") != "CHANGED" for member in changed_rows)
        ):
            raise PriceSafetyFactError(
                "PRICE_SAFETY_CHANGED_STOCK_PAGE_INVALID",
                "全局变化个股分页含重复代码或非CHANGED对象。",
            )
        changed_members = changed_rows

    if detail_members is None and changed_members is None:
        result["price_safety_execution"] = _no_page_execution(
            started, "NO_DISPLAYED_MEMBER_PAGE"
        )
        return result
    detail_symbols = _page_symbols(detail_members, "PRICE_SAFETY_MEMBER_SYMBOL_INVALID")
    changed_symbols = _page_symbols(
        changed_members, "PRICE_SAFETY_MEMBER_SYMBOL_INVALID"
    )
    if detail_members and changed_members:
        if len(detail_members) > 30 or len(changed_members) > 30:
            raise PriceSafetyFactError(
                "PRICE_SAFETY_DISPLAY_PAGE_LIMIT_INVALID",
                "两页同时读取价格事实时，每页最多允许显示30个对象。",
            )
    displayed_symbols = tuple(sorted(set(detail_symbols) | set(changed_symbols)))
    if not displayed_symbols:
        empty_status = (
            "EMPTY_DISPLAYED_MEMBER_PAGE"
            if detail_members is not None
            else "EMPTY_DISPLAYED_CHANGED_STOCK_PAGE"
        )
        result["price_safety_execution"] = _no_page_execution(started, empty_status)
        return result

    source = discovery.get("source")
    if not isinstance(source, Mapping):
        raise PriceSafetyFactError(
            "PRICE_SAFETY_SOURCE_IDENTITY_MISSING", "D1未返回SEALED来源身份。"
        )
    _validate_source_binding(
        result=result,
        source=source,
        target_session=target_session,
        research_cutoff=research_cutoff,
    )
    try:
        target_calendar, calendar_metadata, calendar_revision, all_sessions = (
            _load_calendar(target_session)
        )
    except ResearchEntryError:
        raise
    target_calendar_metadata = next(
        (item for item in calendar_metadata if item.get("year") == target_session.year),
        None,
    )
    official_calendar = discovery.get("official_calendar")
    try:
        target_official_calendar = _official_calendar(target_session)
    except ValueError as error:
        raise PriceSafetyFactError(
            "PRICE_SAFETY_CALENDAR_MISMATCH", "无法读取已核验的目标官方交易日历。"
        ) from error
    _validate_calendar_binding(
        target_calendar=target_calendar,
        target_calendar_metadata=target_calendar_metadata,
        official_calendar=official_calendar,
        target_official_calendar=target_official_calendar,
    )

    eligible_sessions = tuple(
        session for session in all_sessions if session <= target_session
    )
    history_sessions = eligible_sessions[-21:]
    if not history_sessions or history_sessions[-1] != target_session:
        raise PriceSafetyFactError(
            "PRICE_SAFETY_TARGET_SESSION_MISSING",
            "精确官方日历窗口没有以目标会话结束。",
        )

    symbols = displayed_symbols

    batch = read_raw_daily_history_batch(
        config,
        source_symbols=symbols,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=source["package_sha256"],
        sessions=history_sessions,
        expected_namespace_id=str(source["namespace_id"]),
        expected_source_content_id=str(source["source_content_id"]),
        expected_target_capture_id=str(source["source_capture_id"]),
    )
    calendar = MarketSessionCalendar(
        calendar_id=str(target_calendar["calendar_id"]),
        revision=calendar_revision,
        sessions=history_sessions,
    )
    year_metadata = {
        item["year"]: item
        for item in calendar_metadata
        if item.get("year") in {session.year for session in history_sessions}
    }
    window_calendar = [
        {
            "year": year,
            "calendar_revision": year_metadata[year]["revision"],
            "canonical_lf_resource_sha256": year_metadata[year]["resource_sha256"],
        }
        for year in sorted(year_metadata)
    ]
    facts_by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        rows = batch["history_rows_by_symbol"].get(symbol, [])
        by_session = {row["session_date"]: row for row in rows}
        observations = _core_observations(symbol, history_sessions, by_session)
        core_fields = calculate_core(observations, official_calendar=calendar)
        d0_fact = calculate_d0_first_day_damage(
            target_session=target_session,
            official_sessions=history_sessions,
            history_rows_by_session=by_session,
            source_symbol=symbol,
            package_sha256=source["package_sha256"],
            namespace_id=str(source["namespace_id"]),
            source_content_id=str(source["source_content_id"]),
            target_capture_id=str(source["source_capture_id"]),
        )
        facts_by_symbol[symbol] = _facts_payload(
            symbol=symbol,
            source=source,
            research_cutoff=research_cutoff,
            official_calendar=official_calendar,
            window_calendar=window_calendar,
            history_sessions=history_sessions,
            rows_by_session=by_session,
            core_fields=core_fields,
            d0_fact=d0_fact,
        )

    for members in (detail_members, changed_members):
        if members is None:
            continue
        for member in members:
            member["price_safety_facts"] = facts_by_symbol[member["source_symbol"]]

    if detail_members and changed_members:
        scope = "DISPLAYED_CHANGED_STOCK_AND_SECTOR_MEMBER_PAGE_UNION"
    elif changed_members:
        scope = "DISPLAYED_CHANGED_STOCK_PAGE_ONLY"
    else:
        scope = "DISPLAYED_SECTOR_MEMBER_PAGE_ONLY"
    execution = {
        "status": "COMPLETED",
        "scope": scope,
        "database_transaction": batch["transaction"],
        "database_reads": batch["raw_history_batch_reads"],
        "raw_history_selects": batch["raw_history_batch_reads"],
        "unique_source_symbols_requested": len(symbols),
        "displayed_member_rows": (
            len(detail_members or []) + len(changed_members or [])
            if scope == "DISPLAYED_CHANGED_STOCK_AND_SECTOR_MEMBER_PAGE_UNION"
            else len(detail_members or [])
            if scope == "DISPLAYED_SECTOR_MEMBER_PAGE_ONLY"
            else len(changed_members or [])
        ),
        "raw_history_rows_read": batch["raw_history_rows_read"],
        "official_core_input_sessions": len(history_sessions),
        "core_calculations": len(facts_by_symbol),
        "d0_first_day_damage_calculations": len(facts_by_symbol),
        "database_write_attempted": batch["database_write_attempted"],
        "ddl_executed": batch["ddl_executed"],
        "dml_executed": batch["dml_executed"],
        "source_zip_opened": batch["source_zip_opened"],
        "elapsed_ms": round((perf_counter() - started) * 1000, 3),
    }
    if scope == "DISPLAYED_CHANGED_STOCK_AND_SECTOR_MEMBER_PAGE_UNION":
        execution["displayed_page_rows"] = {
            "changed_stock_page": len(changed_members or []),
            "sector_member_page": len(detail_members or []),
            "unique_source_symbols": len(symbols),
        }
    result["price_safety_execution"] = execution
    return result


def _page_symbols(members: list[dict[str, Any]] | None, error_code: str) -> list[str]:
    if members is None:
        return []
    symbols = [member.get("source_symbol") for member in members]
    if any(not isinstance(symbol, str) for symbol in symbols):
        raise PriceSafetyFactError(error_code, "返回分页含缺失的来源代码。")
    return symbols


def _validate_source_binding(
    *,
    result: Mapping[str, Any],
    source: Mapping[str, Any],
    target_session: date,
    research_cutoff: date,
) -> None:
    expected = {
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "package_sha256": result.get("package_sha256"),
        "source_set_id": result.get("source_set_id"),
        "namespace_id": result.get("namespace_id"),
    }
    actual = {
        "target_session": source.get("target_session"),
        "research_cutoff": result.get("research_cutoff"),
        "package_sha256": source.get("package_sha256"),
        "source_set_id": source.get("source_set_id"),
        "namespace_id": source.get("namespace_id"),
    }
    if (
        actual != expected
        or source.get("seal_status") != "SEALED"
        or source.get("price_coordinate") != "RAW_UNADJUSTED"
        or source.get("pit_status") != "NON_PIT_RETROSPECTIVE"
        or source.get("database_write") is not False
        or source.get("ddl_executed") is not False
    ):
        raise PriceSafetyFactError(
            "PRICE_SAFETY_SOURCE_BINDING_MISMATCH",
            "价格事实要求与当前目标日、SEALED包、source set、namespace及RAW坐标完全一致。",
        )
    if (
        source.get("revision_id") is None
        or source.get("source_capture_id") is None
        or source.get("source_content_id") is None
    ):
        raise PriceSafetyFactError(
            "PRICE_SAFETY_SOURCE_IDENTITY_INCOMPLETE", "SEALED来源修订或捕获身份缺失。"
        )


def _validate_calendar_binding(
    *,
    target_calendar: Mapping[str, Any],
    target_calendar_metadata: Mapping[str, Any] | None,
    official_calendar: Mapping[str, Any] | None,
    target_official_calendar: Mapping[str, Any],
) -> None:
    if (
        not isinstance(official_calendar, Mapping)
        or target_calendar.get("calendar_id") != official_calendar.get("calendar_id")
        or target_calendar.get("revision") != official_calendar.get("calendar_revision")
        or target_official_calendar.get("calendar_id")
        != official_calendar.get("calendar_id")
        or target_official_calendar.get("calendar_revision")
        != official_calendar.get("calendar_revision")
        or target_official_calendar.get("calendar_sha256")
        != official_calendar.get("calendar_sha256")
        or not isinstance(target_calendar_metadata, Mapping)
        or target_calendar_metadata.get("revision")
        != official_calendar.get("calendar_revision")
    ):
        raise PriceSafetyFactError(
            "PRICE_SAFETY_CALENDAR_MISMATCH",
            "价格窗口官方日历与SEALED D1来源绑定不一致。",
        )


def _core_observations(
    symbol: str,
    sessions: Sequence[date],
    rows_by_session: Mapping[date, Mapping[str, Any]],
) -> list[CoreObservation]:
    result = []
    for session in sessions:
        row = rows_by_session.get(session)
        if row is None or row.get("bar_status") != "TARGET_PRESENT":
            result.append(
                CoreObservation(
                    session=session,
                    state="UNKNOWN",
                    coordinate_id=f"RAW:TDX_SOURCE_SYMBOL:{symbol}",
                    open=unknown("RAW_BAR_MISSING_FOR_OFFICIAL_SESSION"),
                    high=unknown("RAW_BAR_MISSING_FOR_OFFICIAL_SESSION"),
                    low=unknown("RAW_BAR_MISSING_FOR_OFFICIAL_SESSION"),
                    close=unknown("RAW_BAR_MISSING_FOR_OFFICIAL_SESSION"),
                    amount=unknown("NOT_USED_FOR_PRICE_SAFETY_FACTS"),
                    volume=unknown("NOT_USED_FOR_PRICE_SAFETY_FACTS"),
                )
            )
            continue
        reason = row.get("price_reason") or "RAW_PRICE_QUALITY_NOT_OBSERVED"
        quality = row.get("price_quality")
        result.append(
            CoreObservation(
                session=session,
                state="ACTUAL",
                coordinate_id=f"RAW:TDX_SOURCE_SYMBOL:{symbol}",
                open=_price_field(row.get("raw_open"), quality, reason),
                high=_price_field(row.get("raw_high"), quality, reason),
                low=_price_field(row.get("raw_low"), quality, reason),
                close=_price_field(row.get("raw_close"), quality, reason),
                amount=unknown("NOT_USED_FOR_PRICE_SAFETY_FACTS"),
                volume=unknown("NOT_USED_FOR_PRICE_SAFETY_FACTS"),
            )
        )
    return result


def _price_field(value: Any, quality: Any, reason: Any) -> FieldValue:
    if quality != "OBSERVED" or value is None:
        return unknown(str(reason or "RAW_PRICE_QUALITY_NOT_OBSERVED"))
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, Fraction)):
        return unknown("INVALID_PRICE_FIELD_TYPE")
    if isinstance(value, Decimal) and not value.is_finite():
        return unknown("INVALID_PRICE_FIELD_VALUE")
    return FieldValue(value)


def _facts_payload(
    *,
    symbol: str,
    source: Mapping[str, Any],
    research_cutoff: date,
    official_calendar: Mapping[str, Any],
    window_calendar: list[dict[str, Any]],
    history_sessions: Sequence[date],
    rows_by_session: Mapping[date, Mapping[str, Any]],
    core_fields: Mapping[str, FieldValue],
    d0_fact: Any,
) -> dict[str, Any]:
    raw_history = [
        _raw_session_evidence(session, rows_by_session.get(session))
        for session in history_sessions
    ]
    current_row = rows_by_session.get(history_sessions[-1])
    core_damage = core_fields["core.price_damage"]
    core_state = _field_state(core_damage)
    core_reason_code = (
        f"DAMAGE_PREDICATE_{core_state}"
        if core_state != "UNKNOWN"
        else core_damage.reason
    )
    core_reason = {
        "TRUE": "Core价格损伤公式成立：收盘价严格低于prior_low20减半个ATR20，且ret1为负。",
        "FALSE": "Core价格损伤公式已计算，但严格价格阈值与负ret1条件未同时成立。",
    }.get(core_state) or "Core价格损伤依赖质量不足，原因见reason_code及目标RAW质量。"
    core_window = {
        "role": "CORE_PRICE_DAMAGE_RAW_SESSION_WINDOW",
        "start_session": history_sessions[0].isoformat(),
        "end_session": history_sessions[-1].isoformat(),
        "official_session_count": len(history_sessions),
        "observed_bar_count": sum(
            row.get("bar_status") == "TARGET_PRESENT"
            and row.get("price_quality") == "OBSERVED"
            for row in rows_by_session.values()
        ),
        "missing_sessions": [
            session.isoformat()
            for session in history_sessions
            if session not in rows_by_session
            or rows_by_session[session].get("bar_status") != "TARGET_PRESENT"
        ],
        "official_calendar": {
            "calendar_id": official_calendar["calendar_id"],
            "target_calendar_revision": official_calendar["calendar_revision"],
            "target_calendar_sha256": official_calendar["calendar_sha256"],
            "window_calendars": window_calendar,
        },
        "raw_session_evidence": raw_history,
    }
    d0_window = {
        "role": "D0_SESSION_R2_STRICT_20_OFFICIAL_SESSIONS_INCLUDING_TARGET",
        "start_session": d0_fact.official_sessions[0].isoformat()
        if d0_fact.official_sessions
        else None,
        "end_session": d0_fact.official_sessions[-1].isoformat()
        if d0_fact.official_sessions
        else None,
        "official_session_count": len(d0_fact.official_sessions),
        "expected_official_session_count": 20,
        "observed_bar_count": sum(
            bool(
                rows_by_session.get(session)
                and rows_by_session[session].get("bar_status") == "TARGET_PRESENT"
                and rows_by_session[session].get("price_quality") == "OBSERVED"
            )
            for session in d0_fact.official_sessions
        ),
        "missing_sessions": [
            session.isoformat() for session in d0_fact.missing_sessions
        ],
        "official_calendar": {
            "calendar_id": official_calendar["calendar_id"],
            "target_calendar_revision": official_calendar["calendar_revision"],
            "target_calendar_sha256": official_calendar["calendar_sha256"],
            "window_calendars": window_calendar,
        },
        "raw_session_evidence": [
            item
            for item in raw_history
            if item["session_date"]
            in {session.isoformat() for session in d0_fact.official_sessions}
        ],
    }
    raw_source = {
        "source_symbol": symbol,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "target_session": source["target_session"],
        "research_cutoff": research_cutoff.isoformat(),
        "namespace_id": str(source["namespace_id"]),
        "source_set_id": str(source["source_set_id"]),
        "source_revision_id": source["revision_id"],
        "target_source_capture_id": str(source["source_capture_id"]),
        "source_content_id": str(source["source_content_id"]),
        "package_sha256": source["package_sha256"],
        "price_coordinate": "RAW_UNADJUSTED",
        "target_raw_bar_status": current_row.get("bar_status")
        if current_row
        else "NO_RAW_ROW",
        "target_raw_price_quality": current_row.get("price_quality")
        if current_row
        else "UNKNOWN",
        "target_raw_price_reason": current_row.get("price_reason")
        or current_row.get("bar_reason")
        if current_row
        else "TARGET_RAW_ROW_MISSING",
        "history_capture_ids": sorted(
            {row["source_capture_id"] for row in rows_by_session.values()}
        ),
    }
    return {
        "scope": "SOURCE_SYMBOL_NON_PIT_PRICE_FACTS_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "source": raw_source,
        "core.price_damage": {
            "state": core_state,
            "quality": "OBSERVED" if core_state != "UNKNOWN" else "UNKNOWN",
            "reason_code": core_reason_code,
            "reason": core_reason,
            "window": core_window,
            "dependencies": {
                field: _field_payload(core_fields[field])
                for field in (
                    "core.prior_low20",
                    "core.atr20",
                    "core.ret1",
                )
            },
            "target_close": _fraction_payload(
                _decimal_fraction(current_row.get("raw_close"))
                if current_row and current_row.get("price_quality") == "OBSERVED"
                else None
            ),
        },
        "d0.first_day_damage": {
            "state": d0_fact.state,
            "quality": d0_fact.quality,
            "reason_code": d0_fact.reason_code,
            "reason": d0_fact.reason,
            "window": d0_window,
            "target_close": _fraction_payload(d0_fact.current_close),
            "ma20": _fraction_payload(d0_fact.ma20),
            "close_to_ma20": _fraction_payload(d0_fact.close_to_ma20),
        },
    }


def _raw_session_evidence(
    session: date, row: Mapping[str, Any] | None
) -> dict[str, Any]:
    if row is None:
        return {
            "session_date": session.isoformat(),
            "status": "MISSING_RAW_ROW",
            "capture_id": None,
            "source_entry_sha256": None,
            "bar_status": "NO_RAW_ROW",
            "price_quality": "UNKNOWN",
            "price_reason": "RAW_BAR_MISSING_FOR_OFFICIAL_SESSION",
        }
    return {
        "session_date": session.isoformat(),
        "status": "RAW_ROW_READ",
        "capture_id": row["source_capture_id"],
        "source_entry_sha256": row["source_entry_sha256"],
        "bar_status": row["bar_status"],
        "price_quality": row["price_quality"],
        "price_reason": row["price_reason"],
    }


def _field_state(value: FieldValue) -> str:
    if value.quality != "OBSERVED":
        return "UNKNOWN"
    return "TRUE" if value.value is True else "FALSE"


def _field_payload(value: FieldValue) -> dict[str, Any]:
    if value.quality != "OBSERVED":
        return {
            "quality": "UNKNOWN",
            "value": None,
            "reason_code": value.reason,
        }
    return {
        "quality": "OBSERVED",
        "value": _fraction_payload(value.value)
        if isinstance(value.value, (Fraction, Decimal, int))
        and not isinstance(value.value, bool)
        else value.value,
        "reason_code": None,
    }


def _fraction_payload(value: Any) -> dict[str, int] | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal) and not value.is_finite():
        return None
    try:
        exact = Fraction(value)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return {"numerator": exact.numerator, "denominator": exact.denominator}


def _decimal_fraction(value: Any) -> Fraction | None:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int, Fraction)):
        return None
    if isinstance(value, Decimal) and not value.is_finite():
        return None
    exact = Fraction(value)
    return exact if exact > 0 else None


def _no_page_execution(started: float, status: str) -> dict[str, Any]:
    return {
        "status": status,
        "scope": "DISPLAYED_SECTOR_MEMBER_PAGE_ONLY",
        "database_transaction": "NOT_OPENED",
        "database_reads": 0,
        "raw_history_selects": 0,
        "unique_source_symbols_requested": 0,
        "displayed_member_rows": 0,
        "raw_history_rows_read": 0,
        "core_calculations": 0,
        "d0_first_day_damage_calculations": 0,
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "source_zip_opened": False,
        "elapsed_ms": round((perf_counter() - started) * 1000, 3),
    }


__all__ = ["PriceSafetyFactError", "attach_source_d1_price_safety_facts"]
