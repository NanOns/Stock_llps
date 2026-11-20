"""基于paired BaoStock捕获的多代码Core/Profile读取预览。"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _parse_target
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.entry_context.windows import CompareBudget, _MAX_COMPARE_SYMBOLS
from stock_a.research.entry_core.single_stock import research_stock


_COMPARE_FIELDS = (
    "core.ma5",
    "core.ma10",
    "core.ma20",
    "core.ma60",
    "core.atr5",
    "core.atr20",
    "core.ret1",
    "core.ret5",
    "core.ret20",
    "core.amount_ratio5",
    "core.amount_ratio20",
    "core.volume_ratio5",
    "core.volume_ratio20",
)


def research_compare(
    *,
    symbols: Iterable[str],
    target_session: str,
    source_capture_id: int | None = None,
    price_coordinate: str | None = None,
    session_dates: Sequence[date] | None = None,
    history_rows_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
    profile_contract: dict[str, Any] | None = None,
    budget: CompareBudget | None = None,
    **legacy_source_inputs: Any,
) -> dict[str, Any]:
    """只对绑定paired capture的PG历史输入求Core/Profile；旧文件请求保持未计算。"""
    requested = tuple(symbols)
    if not requested or len(requested) > _MAX_COMPARE_SYMBOLS:
        raise ResearchEntryError(
            "COMPARE_SIZE_INVALID", f"对比代码数必须为1至{_MAX_COMPARE_SYMBOLS}只"
        )
    normalized = tuple(_normalize_symbol(item)[0] for item in requested)
    if len(set(normalized)) != len(normalized):
        raise ResearchEntryError("DUPLICATE_SYMBOL", "对比代码重复；拒绝静默去重")
    target = _parse_target(target_session)
    if budget is not None and not isinstance(budget, CompareBudget):
        raise ResearchEntryError("COMPARE_BUDGET_INVALID", "对比预算参数无效")
    valid_capture_id = (
        source_capture_id
        if type(source_capture_id) is int and source_capture_id > 0
        else None
    )

    def result_not_computed(symbol: str, code: str, message: str) -> dict[str, Any]:
        return {
            "status": "NOT_COMPUTED",
            "requested_symbol": symbol,
            "target_session": target.isoformat(),
            "source_capture_id": valid_capture_id,
            "price_coordinate": price_coordinate,
            "time_role": "NON_PIT_RETROSPECTIVE",
            "error_code": code,
            "message": message,
            "database_write": False,
        }

    if type(source_capture_id) is not int or source_capture_id <= 0:
        results = [
            result_not_computed(
                symbol,
                "PG_BAOSTOCK_CAPTURE_REQUIRED",
                "多代码读取必须绑定paired BaoStock source_capture_id；不会读取本地TDX/VIPDATA。",
            )
            for symbol in normalized
        ]
    elif price_coordinate != "FORWARD_ADJUSTED":
        results = [
            result_not_computed(
                symbol,
                "COMPARE_REQUIRES_QFQ_COORDINATE",
                "跨日Core/Profile对比必须绑定BaoStock QFQ坐标。",
            )
            for symbol in normalized
        ]
    elif (
        not isinstance(session_dates, Sequence)
        or isinstance(session_dates, (str, bytes))
        or not session_dates
        or len(session_dates) > 250
        or any(type(day) is not date for day in session_dates)
        or tuple(sorted(set(session_dates))) != tuple(session_dates)
        or session_dates[-1] != target
        or not isinstance(history_rows_by_symbol, Mapping)
        or profile_contract is None
    ):
        results = [
            result_not_computed(
                symbol,
                "PG_BAOSTOCK_HISTORY_INPUT_UNAVAILABLE",
                "缺少显式官方会话、PG双坐标历史行或R2价格画像合同。",
            )
            for symbol in normalized
        ]
    else:
        results = []
        for symbol in normalized:
            symbol_rows = history_rows_by_symbol.get(symbol)
            if not isinstance(symbol_rows, Sequence) or isinstance(
                symbol_rows, (str, bytes)
            ):
                results.append(
                    result_not_computed(
                        symbol,
                        "PG_BAOSTOCK_SYMBOL_HISTORY_UNAVAILABLE",
                        "该来源代码缺少显式capture内的PG历史行；不回退其它代码或坐标。",
                    )
                )
                continue
            try:
                item = research_stock(
                    symbol=symbol,
                    target_session=target.isoformat(),
                    source_capture_id=source_capture_id,
                    price_coordinate=price_coordinate,
                    session_dates=session_dates,
                    history_rows=symbol_rows,
                    profile_contract=profile_contract,
                )
            except ResearchEntryError as error:
                item = result_not_computed(
                    symbol,
                    error.code,
                    str(error),
                )
            item["requested_symbol"] = symbol
            item["source_capture_id"] = source_capture_id
            item["price_coordinate"] = price_coordinate
            item["time_role"] = "NON_PIT_RETROSPECTIVE"
            results.append(item)

    for item in results:
        qfq_fields = item.get("qfq")
        if isinstance(qfq_fields, Mapping):
            qfq_ohlc = {
                name: qfq_fields.get(
                    name,
                    {
                        "value": None,
                        "quality": "UNKNOWN",
                        "reason": "QFQ_FIELD_NOT_RETURNED",
                    },
                )
                for name in ("open", "high", "low", "close")
            }
        else:
            unavailable = {
                "value": None,
                "quality": "UNKNOWN",
                "reason": item.get("message") or "QFQ_PRICE_NOT_COMPUTED",
            }
            qfq_ohlc = {
                name: dict(unavailable) for name in ("open", "high", "low", "close")
            }
        qfq_qualities = [
            value.get("quality")
            for value in qfq_ohlc.values()
            if isinstance(value, Mapping)
        ]
        qfq_quality = (
            "OBSERVED"
            if len(qfq_qualities) == 4
            and all(value == "OBSERVED" for value in qfq_qualities)
            else "UNKNOWN"
        )
        qfq_reason = next(
            (
                value.get("reason") or value.get("quality")
                for value in qfq_ohlc.values()
                if isinstance(value, Mapping) and value.get("quality") != "OBSERVED"
            ),
            None,
        )
        item["qfq_ohlc"] = qfq_ohlc | {
            "quality": qfq_quality,
            "reason": qfq_reason,
        }
        item["qfq_close"] = qfq_ohlc["close"]
        item["selected_qfq_close"] = qfq_ohlc["close"]

    not_computed = sum(item.get("status") == "NOT_COMPUTED" for item in results)
    evaluated = len(results) - not_computed
    comparison_fields = {
        field_id: {
            item["requested_symbol"]: item.get("core", {})
            .get("fields", {})
            .get(
                field_id,
                {
                    "value": None,
                    "quality": "UNKNOWN_DATA",
                    "reason": item.get("reason_code", "NOT_COMPUTED"),
                },
            )
            for item in results
        }
        for field_id in _COMPARE_FIELDS
    }
    return {
        "contract": "STOCK_A_PG_BAOSTOCK_CAPTURE_COMPARISON_R2",
        "status": (
            "NOT_COMPUTED"
            if not_computed == len(results)
            else "AVAILABLE"
            if not not_computed
            else "PARTIAL_NOT_COMPUTED"
        ),
        "target_session": target.isoformat(),
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "time_role": "NON_PIT_RETROSPECTIVE",
        "denominators": {
            "requested": len(normalized),
            "evaluated": evaluated,
            "not_computed": not_computed,
        },
        "results": results,
        "comparison_fields": comparison_fields,
        "database_write": False,
        "source_file_read": False,
        "network_access": False,
    }
