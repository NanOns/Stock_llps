"""Stock A研究single_stock职责模块。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence
from stock_a.sources.baostock.identity import (
    IdentityLifecycleProjection,
    reconcile_identity_bar_status,
)

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.entry_context.sessions import _parse_target


def apply_identity_observation(
    result: dict[str, Any],
    *,
    identity_projection: IdentityLifecycleProjection | None,
    identity_query_status: str,
) -> dict[str, Any]:
    """将BaoStock身份观察附加到已完成的PG价格/Core结果。"""
    if (
        not isinstance(result, dict)
        or not isinstance(identity_query_status, str)
        or not identity_query_status
    ):
        raise ResearchEntryError(
            "IDENTITY_QUERY_STATUS_INVALID", "Bao身份结果或来源状态无效"
        )
    if identity_projection is not None and (
        identity_projection.source_symbol != result.get("requested_symbol")
        or identity_projection.target_session.isoformat()
        != result.get("target_session")
    ):
        raise ResearchEntryError(
            "IDENTITY_PROJECTION_MISMATCH", "BaoStock身份投影与PG价格/Core预览不匹配"
        )
    if identity_projection is None:
        result.setdefault("identity", {}).update(
            {
                "status": "UNAVAILABLE",
                "reason": f"BAOSTOCK_IDENTITY_SOURCE_{identity_query_status}",
            }
        )
    else:
        result["identity"] = {
            **identity_projection.as_dict(),
            "security_id": None,
            "stable_security_id_issued": False,
        }
    selection = result.get("canonical_selection")
    target_bar_status = (
        selection.get("target_bar_status") if isinstance(selection, dict) else None
    )
    if target_bar_status is None:
        target_bar_status = {
            "BAOSTOCK_TARGET_BAR_PRESENT": "PRESENT",
            "BAOSTOCK_TARGET_BAR_MISSING": "TARGET_MISSING",
            "BAOSTOCK_READ_FAILED": "READ_FAILED",
            "HISTORY_ROW_NOT_STORED": "TARGET_MISSING",
        }.get(result.get("target_bar_status"))
    if target_bar_status not in {
        "PRESENT",
        "TARGET_MISSING",
        "READ_FAILED",
        "NOT_EVALUATED",
    }:
        target_bar_status = (
            "PRESENT" if result.get("target_bar_present") is True else "TARGET_MISSING"
        )
    result["identity_reconciliation"] = reconcile_identity_bar_status(
        identity_projection,
        target_bar_status=target_bar_status,
        source_status="RESPONSE_RECEIVED"
        if identity_projection is not None
        else identity_query_status,
    )
    return result


def research_stock(
    *,
    symbol: str,
    target_session: str,
    source_capture_id: int | None = None,
    price_coordinate: str | None = None,
    session_dates: Sequence[date] | None = None,
    history_rows: Sequence[Mapping[str, Any]] | None = None,
    profile_contract: dict[str, Any] | None = None,
    identity_projection: IdentityLifecycleProjection | None = None,
    identity_query_status: str = "NOT_REQUESTED",
    timing_sink: dict[str, float] | None = None,
    **legacy_source_inputs: Any,
) -> dict[str, Any]:
    """只消费显式BaoStock paired capture的PG历史行；旧TDX入口不再回退。"""
    target = _parse_target(target_session)
    normalized, _market_directory, board = _normalize_symbol(symbol)
    valid_capture_id = (
        source_capture_id
        if type(source_capture_id) is int and source_capture_id > 0
        else None
    )

    def not_computed(reason_code: str, message: str) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": "NOT_COMPUTED",
            "reason_code": reason_code,
            "reason": message,
            "requested_symbol": normalized,
            "board_scope": board,
            "target_session": target.isoformat(),
            "price_coordinate": price_coordinate,
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "identity": {
                "status": "SOURCE_SYMBOL_ONLY",
                "source_symbol": normalized,
                "security_id": None,
                "stable_security_id_issued": False,
            },
            "source": {
                "provider": "BAOSTOCK",
                "source_capture_id": valid_capture_id,
                "clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
            },
            "raw_availability": {"status": "NOT_COMPUTED", "reason": message},
            "core_availability": {"status": "NOT_COMPUTED", "reason": message},
            "profile_preview": None,
            "database_write": False,
            "source_file_read": False,
            "network_access": False,
        }
        return result

    if valid_capture_id is None:
        return not_computed(
            "PG_BAOSTOCK_CAPTURE_REQUIRED",
            "当前调用没有paired BaoStock source_capture_id；不读取本地TDX或VIPDATA回退。",
        )
    if price_coordinate != "FORWARD_ADJUSTED":
        return not_computed(
            "CORE_REQUIRES_QFQ_COORDINATE",
            "Core和跨日技术画像必须绑定BaoStock前复权坐标。",
        )
    if (
        not isinstance(session_dates, Sequence)
        or isinstance(session_dates, (str, bytes))
        or not session_dates
        or not isinstance(history_rows, Sequence)
        or isinstance(history_rows, (str, bytes))
        or profile_contract is None
    ):
        return not_computed(
            "PG_BAOSTOCK_HISTORY_INPUT_UNAVAILABLE",
            "缺少显式官方会话、PG BaoStock历史行或R2价格画像合同。",
        )
    sessions = tuple(session_dates)
    rows = tuple(history_rows)
    if (
        any(type(day) is not date for day in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] != target
        or any(not isinstance(row, Mapping) for row in rows)
        or any(
            row.get("source_symbol") != normalized
            or row.get("source_capture_id") != source_capture_id
            for row in rows
        )
    ):
        return not_computed(
            "PG_BAOSTOCK_HISTORY_BINDING_INVALID",
            "PG历史行未绑定同一来源代码、paired捕获和截止目标会话。",
        )

    from stock_a.research.entry_history.history_preview import (
        research_stock_pg_history_preview,
    )

    missing_sessions = tuple(
        day for day in sessions if day not in {row["session_date"] for row in rows}
    )
    result = research_stock_pg_history_preview(
        symbol=normalized,
        target_session=target.isoformat(),
        source_capture_id=valid_capture_id,
        price_coordinate=price_coordinate,
        session_dates=sessions,
        history_rows=rows,
        missing_history_sessions=missing_sessions,
        profile_contract=profile_contract,
    )
    result["requested_symbol"] = normalized
    result["board_scope"] = board
    result["source_capture_id"] = valid_capture_id
    result["price_coordinate"] = price_coordinate
    result["research_mode"] = "NON_PIT_RETROSPECTIVE"
    result["source_file_read"] = False
    result["network_access"] = False
    result["database_write"] = False
    if identity_projection is not None or identity_query_status != "NOT_REQUESTED":
        apply_identity_observation(
            result,
            identity_projection=identity_projection,
            identity_query_status=identity_query_status,
        )
    return result
