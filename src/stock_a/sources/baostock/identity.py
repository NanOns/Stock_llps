"""BaoStock身份与目标日状态的只读投影；不把证券代码提升为稳定实体ID。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import re
from typing import Any, Mapping


_SYMBOL = re.compile(r"^(SH|SZ)\.([0-9]{6})$")
_REQUIRED_BASIC_FIELDS = {"code", "code_name", "ipoDate", "outDate", "type", "status"}
_REQUIRED_SESSION_FIELDS = {"code", "tradeStatus", "code_name"}


def normalize_baostock_symbol(value: object) -> str | None:
    """把BaoStock小写市场前缀转成项目代码格式，只接受沪深六位代码。"""
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized if _SYMBOL.fullmatch(normalized) else None


def eligible_research_board(symbol: object) -> str | None:
    """返回本阶段正式范围内的板别；科创板、北交所和非沪深代码不进入分母。"""
    normalized = normalize_baostock_symbol(symbol)
    if normalized is None:
        return None
    market, code = normalized.split(".", 1)
    if market == "SH" and code.startswith(("600", "601", "603", "605")):
        return "MAIN_BOARD"
    if market == "SZ" and code.startswith(("000", "001", "002", "003")):
        return "MAIN_BOARD"
    if market == "SZ" and code.startswith(("300", "301")):
        return "GEM"
    return None


def _valid_response(snapshot: object, required_fields: set[str], *, target_session: str | None = None) -> bool:
    if not isinstance(snapshot, Mapping) or snapshot.get("status") != "RESPONSE_RECEIVED":
        return False
    if snapshot.get("query_error_code") != "0" or snapshot.get("complete") is not True:
        return False
    fields = snapshot.get("response_fields")
    rows = snapshot.get("rows")
    if (not isinstance(fields, list) or not required_fields.issubset(set(fields))
            or not isinstance(rows, list)
            or snapshot.get("rows_seen") != len(rows)):
        return False
    if target_session is not None and snapshot.get("target_session") != target_session:
        return False
    try:
        started = datetime.fromisoformat(snapshot["request_started_at"])
        completed = datetime.fromisoformat(snapshot["request_completed_at"])
    except (KeyError, TypeError, ValueError):
        return False
    return (
        started.tzinfo is not None and completed.tzinfo is not None
        and started <= completed
        and snapshot.get("capture_clock_role") == "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE"
    )


def _parse_optional_date(value: object) -> date | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ValueError("日期字段必须是文本或空值")
    return date.fromisoformat(value)


def _parse_enum(value: object, allowed: set[int]) -> int | None:
    if not isinstance(value, str) or not value.isdigit():
        return None
    parsed = int(value)
    return parsed if parsed in allowed else None


@dataclass(frozen=True)
class IdentityLifecycleProjection:
    """证券代码在一次回溯观测中的生命周期说明，不提供OFFICIAL_STABLE实体ID。"""

    source_symbol: str
    target_session: date
    board: str | None
    identity_status: str
    entity_id: None
    security_name: str | None
    security_type: int | None
    current_listing_status: int | None
    ipo_date: date | None
    out_date: date | None
    target_trade_status: int | None
    classification: str
    observed_at: datetime | None
    published_at: None = None
    available_at: None = None
    time_role: str = "NON_PIT_RETROSPECTIVE"
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_symbol": self.source_symbol,
            "target_session": self.target_session.isoformat(),
            "board": self.board,
            "identity_status": self.identity_status,
            "entity_id": None,
            "security_name": self.security_name,
            "security_type": self.security_type,
            "current_listing_status": self.current_listing_status,
            "ipo_date": self.ipo_date.isoformat() if self.ipo_date else None,
            "out_date": self.out_date.isoformat() if self.out_date else None,
            "target_trade_status": self.target_trade_status,
            "classification": self.classification,
            "observed_at": self.observed_at.isoformat() if self.observed_at else None,
            "published_at": None,
            "available_at": None,
            "time_role": self.time_role,
            "reason": self.reason,
        }


def project_identity_lifecycle(
    *,
    source_symbol: str,
    target_session: date,
    basic_snapshot: Mapping[str, Any],
    session_snapshot: Mapping[str, Any],
) -> IdentityLifecycleProjection:
    """按真实响应字段投影当前基本资料和指定日状态，保留后验采集边界。"""
    normalized = normalize_baostock_symbol(source_symbol)
    if normalized is None or type(target_session) is not date:
        raise ValueError("身份投影必须绑定沪深代码和明确目标日")
    if not _valid_response(basic_snapshot, _REQUIRED_BASIC_FIELDS):
        return IdentityLifecycleProjection(
            normalized, target_session, eligible_research_board(normalized), "UNMAPPED", None,
            None, None, None, None, None, None, "UNKNOWN_NO_BAR", None,
            reason="BAOSTOCK_BASIC_SOURCE_UNAVAILABLE_OR_SCHEMA_INVALID",
        )
    session_text = target_session.isoformat()
    if not _valid_response(session_snapshot, _REQUIRED_SESSION_FIELDS, target_session=session_text):
        return IdentityLifecycleProjection(
            normalized, target_session, eligible_research_board(normalized), "UNMAPPED", None,
            None, None, None, None, None, None, "UNKNOWN_NO_BAR", None,
            reason="BAOSTOCK_TARGET_DAY_LIST_UNAVAILABLE_OR_SCHEMA_INVALID",
        )

    def rows_by_symbol(snapshot: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        return [row for row in snapshot["rows"] if isinstance(row, Mapping)
                and normalize_baostock_symbol(row.get("code")) == normalized]

    basic_rows = rows_by_symbol(basic_snapshot)
    day_rows = rows_by_symbol(session_snapshot)
    basic = basic_rows[0] if len(basic_rows) == 1 else None
    day = day_rows[0] if len(day_rows) == 1 else None
    if len(basic_rows) > 1 or len(day_rows) > 1:
        return IdentityLifecycleProjection(
            normalized, target_session, eligible_research_board(normalized), "UNMAPPED", None,
            None, None, None, None, None, None, "UNKNOWN_NO_BAR", None,
            reason="BAOSTOCK_DUPLICATE_SYMBOL_ROWS",
        )

    observed_values = [basic_snapshot.get("request_completed_at"), session_snapshot.get("request_completed_at")]
    try:
        observed_at = max(datetime.fromisoformat(value) for value in observed_values if isinstance(value, str))
    except (TypeError, ValueError):
        observed_at = None
    if basic is None:
        return IdentityLifecycleProjection(
            normalized, target_session, eligible_research_board(normalized), "UNMAPPED", None,
            None, None, None, None, None, "UNKNOWN_NO_BAR", observed_at,
            reason="BAOSTOCK_BASIC_ROW_NOT_FOUND",
        )

    date_error = False
    try:
        ipo_date = _parse_optional_date(basic.get("ipoDate"))
        out_date = _parse_optional_date(basic.get("outDate"))
    except ValueError:
        ipo_date = out_date = None
        date_error = True
    security_type = _parse_enum(basic.get("type"), {1, 2, 3, 4, 5})
    listing_status = _parse_enum(basic.get("status"), {0, 1})
    trade_status = _parse_enum(day.get("tradeStatus"), {0, 1}) if day is not None else None
    name = basic.get("code_name") if isinstance(basic.get("code_name"), str) else None

    if eligible_research_board(normalized) is None:
        classification = "OUT_OF_FORMAL_RESEARCH_MARKET_OR_BOARD"
    elif security_type is not None and security_type != 1:
        classification = "NON_STOCK_SECURITY_TYPE"
    elif ipo_date is not None and target_session < ipo_date:
        classification = "PRELISTING_NON_PIT_RETROSPECTIVE"
    elif out_date is not None and out_date <= target_session:
        classification = "DELISTED_BY_TARGET_NON_PIT_RETROSPECTIVE"
    elif trade_status == 0:
        classification = "SUSPENDED_AT_TARGET_NON_PIT_RETROSPECTIVE"
    elif trade_status == 1:
        classification = "BAO_REPORTED_TRADING_TDX_BAR_MISSING"
    else:
        classification = "UNKNOWN_NO_BAR"

    reason = "BAOSTOCK_HAS_NO_STABLE_SECURITY_ID"
    if date_error:
        reason = "BAOSTOCK_LISTING_DATE_INVALID"
    elif basic_rows and security_type is None:
        reason = "BAOSTOCK_SECURITY_TYPE_INVALID_OR_UNKNOWN"
    return IdentityLifecycleProjection(
        normalized, target_session, eligible_research_board(normalized), "PROVISIONAL", None,
        name, security_type, listing_status, ipo_date, out_date, trade_status,
        classification, observed_at, reason=reason,
    )


def classify_missing_bar_candidates(
    *,
    source_symbols: tuple[str, ...],
    target_session: date,
    basic_snapshot: Mapping[str, Any],
    session_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """用Bao实际响应解释TDX缺Bar候选；任何未匹配项都保留UNKNOWN。"""
    if type(target_session) is not date or not isinstance(source_symbols, tuple):
        raise ValueError("缺Bar分类必须绑定明确目标日和有序来源代码元组")
    normalized = [normalize_baostock_symbol(item) for item in source_symbols]
    if any(item is None for item in normalized):
        raise ValueError("缺Bar候选含非沪深证券代码")
    if len(set(normalized)) != len(normalized):
        raise ValueError("缺Bar候选含重复代码，拒绝制造互斥分类分母")
    basic_ok = _valid_response(basic_snapshot, _REQUIRED_BASIC_FIELDS)
    session_ok = _valid_response(session_snapshot, _REQUIRED_SESSION_FIELDS,
                                 target_session=target_session.isoformat())
    projections: list[IdentityLifecycleProjection] = []
    for symbol in normalized:
        projections.append(project_identity_lifecycle(
            source_symbol=symbol, target_session=target_session,
            basic_snapshot=basic_snapshot, session_snapshot=session_snapshot,
        ))
    by_class: dict[str, int] = {}
    for projection in projections:
        by_class[projection.classification] = by_class.get(projection.classification, 0) + 1
    if sum(by_class.values()) != len(normalized):
        raise AssertionError("缺Bar分类计数未闭合")
    return {
        "target_session": target_session.isoformat(),
        "source_symbol_denominator": len(normalized),
        "evidence_status": "REAL_CAPTURED_NON_PIT" if basic_ok and session_ok else "PARTIAL_OR_UNAVAILABLE",
        "capture_clock_role": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
        "basic_capture_completed_at": basic_snapshot.get("request_completed_at") if basic_ok else None,
        "session_capture_completed_at": session_snapshot.get("request_completed_at") if session_ok else None,
        "strict_pit": False,
        "stable_security_id_issued": False,
        "classification_counts": (dict(sorted(by_class.items())) if basic_ok and session_ok
                                   else ({"NOT_EVALUATED": len(normalized)} if normalized else {})),
        "classified_with_evidence": (len(normalized) - by_class.get("UNKNOWN_NO_BAR", 0))
        if basic_ok and session_ok else 0,
        "unknown": by_class.get("UNKNOWN_NO_BAR", 0) if basic_ok and session_ok else 0,
        "not_evaluated": 0 if basic_ok and session_ok else len(normalized),
        "rows": [projection.as_dict() for projection in projections],
    }
