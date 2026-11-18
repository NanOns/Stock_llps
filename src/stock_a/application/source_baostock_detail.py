"""读取已保存的BaoStock单股补充观察；不采集来源或替代TDX价格。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping

from stock_a.sources.baostock.identity import normalize_baostock_symbol
from stock_a.storage.postgres.baostock import (
    read_baostock_daily_capture,
    resolve_baostock_daily_capture,
)


class BaoStockDetailError(ValueError):
    """单股BaoStock补充Reader的请求或PG事实错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_FIELD_UNITS = {
    "open": "人民币元/股",
    "high": "人民币元/股",
    "low": "人民币元/股",
    "close": "人民币元/股",
    "volume": "股",
    "amount": "人民币元",
    "turn": "比例值",
    "tradestatus": "枚举0或1",
    "is_st": "枚举0或1",
}
_FIELD_NAMES = tuple(_FIELD_UNITS)


def _unknown_fields(reason: str) -> dict[str, dict[str, Any]]:
    return {
        name: {
            "value": None,
            "quality": "UNKNOWN_DATA",
            "reason": reason,
            "unit": _FIELD_UNITS[name],
        }
        for name in _FIELD_NAMES
    }


def _field_value(value: Any) -> str | None:
    return None if value is None else str(value)


def read_baostock_detail(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    research_cutoff: date,
) -> dict[str, Any]:
    """读取单股单日已保存补充事实，绑定唯一捕获并保留逐字段质量。"""
    symbol = normalize_baostock_symbol(source_symbol)
    if symbol is None:
        raise BaoStockDetailError(
            "SOURCE_SYMBOL_INVALID", "BaoStock详情读取需要规范沪深来源证券代码。"
        )
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise BaoStockDetailError(
            "RESEARCH_CUTOFF_EXCEEDED",
            "BaoStock详情目标交易日必须是不晚于研究截止日的明确官方会话。",
        )
    try:
        selected = resolve_baostock_daily_capture(
            config,
            source_symbol=symbol,
            target_session=target_session,
            cutoff=research_cutoff,
        )
    except Exception as error:
        raise BaoStockDetailError(
            getattr(error, "code", type(error).__name__),
            "BaoStock补充来源读取不可用；TDX核心详情保持独立。",
        ) from error

    if selected.get("status") == "NOT_AVAILABLE":
        return {
            "status": "NOT_AVAILABLE",
            "reason_code": selected.get("reason_code"),
            "source": "BAOSTOCK",
            "source_symbol": symbol,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "price_coordinate": "RAW_UNADJUSTED",
            "pit_status": "NON_PIT",
            "fields": _unknown_fields("该来源没有目标会话的已保存捕获。"),
            "source_network_called": False,
            "database_write_attempted": False,
        }
    if selected.get("status") == "AMBIGUOUS":
        return {
            "status": "UNKNOWN",
            "reason_code": selected.get("reason_code"),
            "source": "BAOSTOCK",
            "source_symbol": symbol,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "capture_candidates": selected.get("candidates", []),
            "price_coordinate": "RAW_UNADJUSTED",
            "pit_status": "NON_PIT",
            "fields": _unknown_fields("最新捕获时刻并列，无法唯一绑定来源版本。"),
            "source_network_called": False,
            "database_write_attempted": False,
        }

    capture_id = selected["capture_id"]
    captured_at = selected["captured_at"]
    try:
        readback = read_baostock_daily_capture(
            config,
            capture_id=capture_id,
            source_symbols=(symbol,),
            start_session=target_session,
            end_session=target_session,
            cutoff=research_cutoff,
            limit=1,
            offset=0,
        )
    except Exception as error:
        raise BaoStockDetailError(
            getattr(error, "code", type(error).__name__),
            "BaoStock精确捕获读回失败；TDX核心详情保持独立。",
        ) from error

    rows = readback.get("rows", ())
    if (
        readback.get("status") != "AVAILABLE"
        or readback.get("page", {}).get("total") != 1
        or len(rows) != 1
    ):
        return {
            "status": "INCOMPLETE",
            "reason_code": "EXACT_CAPTURE_READBACK_MISMATCH",
            "source": "BAOSTOCK",
            "source_symbol": symbol,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "capture_id": str(capture_id),
            "fields": _unknown_fields("精确捕获读回没有得到唯一目标行。"),
            "source_network_called": False,
            "database_write_attempted": False,
        }

    row = rows[0]
    if (
        row.get("capture_id") != str(capture_id)
        or row.get("source_symbol") != symbol
        or row.get("session_date") != target_session.isoformat()
        or row.get("adjustment_flag") != "3"
        or row.get("price_coordinate") != "RAW_UNADJUSTED"
        or row.get("unit_contract_version") != selected.get("unit_contract_version")
        or row.get("capture_clock_role")
        != "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE"
        or row.get("captured_at") != captured_at
    ):
        return {
            "status": "INCOMPLETE",
            "reason_code": "EXACT_CAPTURE_IDENTITY_MISMATCH",
            "source": "BAOSTOCK",
            "source_symbol": symbol,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "capture_id": str(capture_id),
            "fields": _unknown_fields("捕获身份、日期、坐标或单位合同与候选不一致。"),
            "source_network_called": False,
            "database_write_attempted": False,
        }

    fields = {}
    source_fields = row.get("fields", {})
    for name, unit in _FIELD_UNITS.items():
        value = source_fields.get(name)
        fields[name] = {
            "value": _field_value(getattr(value, "value", None)),
            "quality": getattr(value, "quality", "UNKNOWN_DATA"),
            "reason": getattr(value, "reason", "来源未提供该字段质量说明"),
            "unit": unit,
        }
        if fields[name]["quality"] == "OBSERVED":
            fields[name]["reason"] = None

    contract = {
        "version": row.get("unit_contract_version"),
        "close_input_unit": str(row.get("close_input_unit")),
        "close_to_cny": str(row.get("close_to_cny")),
        "amount_input_unit": str(row.get("amount_input_unit")),
        "amount_to_cny": str(row.get("amount_to_cny")),
        "volume_input_unit": str(row.get("volume_input_unit")),
        "volume_to_shares": str(row.get("volume_to_shares")),
        "turn_input_unit": str(row.get("turn_input_unit")),
    }
    captured_at_iso = (
        captured_at.isoformat()
        if isinstance(captured_at, datetime)
        else str(captured_at)
    )
    return {
        "status": "AVAILABLE",
        "reason_code": None,
        "source": "BAOSTOCK",
        "source_symbol": symbol,
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "capture_id": str(capture_id),
        "captured_at": captured_at_iso,
        "capture_clock_role": selected["capture_clock_role"],
        "source_time_quality": "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
        "pit_status": "NON_PIT",
        "adjustment_flag": "3",
        "price_coordinate": "RAW_UNADJUSTED",
        "unit_contract": contract,
        "returned_fields": row.get("returned_fields", []),
        "fields": fields,
        "normalized_sha256": str(row.get("normalized_sha256", "")).strip(),
        "source_network_called": False,
        "database_write_attempted": False,
    }


__all__ = ["BaoStockDetailError", "read_baostock_detail"]
