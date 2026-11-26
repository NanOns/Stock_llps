"""将PG Raw历史窗口接入来源代码价格画像批处理。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.canonical.adjustment_periods import PERIOD_AGGREGATION_VERSION
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_source_version_context,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.research.entry import (
    ResearchEntryError,
    _load_calendar,
    research_universe_pg_history_preview,
)
from stock_a.storage.postgres.price_profiles import (
    PriceProfilePgError,
    list_profile_snapshots,
    persist_profile_records,
    read_source_profile_observability,
    read_profile_breadth,
    read_profile_breadth_members,
    read_profile_change_page,
    read_profile_page,
    read_profile_pair,
    read_profile_relation_screen,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
)
from stock_a.storage.postgres.baostock_price_facts import (
    BaoStockPriceFactsError,
    read_baostock_price_history_batch,
)
from stock_a.storage.postgres.baostock_universe import resolve_date_effective_universe


SOURCE_SYMBOL_PROFILE_SCOPE = "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE"
PG_PROFILE_MAX_HISTORY_SESSIONS = (
    32767  # smallint上限；实际窗口由官方日历与来源覆盖约束。
)
_PRODUCER_CONTRACT = "STOCK_A_PG_SOURCE_SYMBOL_CORE_PROFILE_PREVIEW_R1"
_PRODUCER_SEMANTICS_VERSION = "CORE43_PROFILE11_SOURCE_SYMBOL_R6_CONSUMED_CORE_WIRED"
_PROFILE_REVISION = "PROFILE_R08"
_CORE_FIELDS = {
    "core.ma5": "ma5",
    "core.ma10": "ma10",
    "core.ma20": "ma20",
    "core.ma60": "ma60",
    "core.atr20": "atr20",
    "core.clv": "clv",
    "core.prior_high20": "prior_high20",
    "core.ret20": "ret20",
    "core.ret1": "ret1",
    "core.ret5": "ret5",
    "core.liquidity_mean20": "liquidity_mean20",
    "core.pos60": "pos60",
    "core.bias20_atr": "bias20_atr",
    "core.ma_slope20_atr": "ma_slope20_atr",
    "core.amount_ratio5": "amount_ratio5",
    "core.amount_ratio20": "amount_ratio20",
    "core.volume_ratio5": "volume_ratio5",
    "core.volume_ratio20": "volume_ratio20",
}
_CORE_BOOLEAN_FIELDS = {"core.price_damage": "price_damage"}


class PgPriceProfileError(Exception):
    """来源代码级价格画像批处理未闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _validate_profile_semantics(profile_contract: Mapping[str, Any]) -> None:
    """核验当前Core/Profile字段合同完整，并用显式修订号标识计算语义。"""
    registry = profile_contract.get("field_registry", ())
    rules = profile_contract.get("profile_expression_registry", {})
    if not isinstance(registry, (tuple, list)) or not isinstance(rules, Mapping):
        raise PgPriceProfileError(
            "PROFILE_SEMANTICS_UNBOUND", "Core/Profile字段合同缺失"
        )
    definitions = {
        item.get("field_id"): item
        for item in registry
        if isinstance(item, Mapping) and isinstance(item.get("field_id"), str)
    }
    if len(definitions) != len(registry) or len(rules) != 11:
        raise PgPriceProfileError(
            "PROFILE_SEMANTICS_UNBOUND", "Core字段或Profile规则登记重复、缺项"
        )
    rule_semantics: dict[str, Any] = {}
    pending: set[str] = set()
    for rule_id, rule in rules.items():
        if not isinstance(rule_id, str) or not isinstance(rule, Mapping):
            raise PgPriceProfileError(
                "PROFILE_SEMANTICS_UNBOUND", "Profile规则标识或表达式非法"
            )
        rule_semantics[rule_id] = rule
        input_fields = rule.get("input_fields", ())
        if not isinstance(input_fields, (tuple, list)) or any(
            not isinstance(item, str) for item in input_fields
        ):
            raise PgPriceProfileError(
                "PROFILE_SEMANTICS_UNBOUND", "Profile输入字段登记非法：" + rule_id
            )
        pending.update(input_fields)

    consumed_fields: set[str] = set()
    while pending:
        field_id = pending.pop()
        if field_id in consumed_fields:
            continue
        consumed_fields.add(field_id)
        definition = definitions.get(field_id)
        if definition is None:
            continue
        dependencies = definition.get("input_fields", ())
        if isinstance(dependencies, (tuple, list)):
            pending.update(item for item in dependencies if isinstance(item, str))

    missing_fields = sorted(consumed_fields - definitions.keys())
    if missing_fields:
        raise PgPriceProfileError(
            "PROFILE_SEMANTICS_UNBOUND",
            "画像计算使用了未登记字段：" + ", ".join(missing_fields),
        )
    if set(rule_semantics) != set(
        profile_contract.get("profile_expression_registry", {})
    ):
        raise PgPriceProfileError("PROFILE_SEMANTICS_UNBOUND", "Profile规则登记不完整")


def _resolve_suspension_status(target_row: Mapping[str, Any]) -> str:
    """根据同捕获同日 BaoStock raw_tradestatus 字段推导目标日停牌状态。"""
    if target_row.get("raw_tradestatus_quality") == "OBSERVED":
        value = target_row.get("raw_tradestatus_value")
        if value == "1":
            return "SUSPENSION_STATUS_TRADING"
        if value == "0":
            return "SUSPENSION_STATUS_SUSPENDED"
    return "SUSPENSION_STATUS_UNKNOWN"


def _records_for_page(
    *,
    page: Mapping[str, Any],
    preview: Mapping[str, Any],
    target_session: date,
    source_capture_id: int,
    source_context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates = {item["source_symbol"]: item for item in page.get("candidates", ())}
    records: list[dict[str, Any]] = []
    for result in preview.get("results", ()):
        symbol = result["symbol"]
        candidate = candidates.get(symbol)
        if candidate is None:
            raise PgPriceProfileError(
                "PROFILE_RESULT_SOURCE_MISMATCH", "Core/Profile结果无法绑定原始分页候选"
            )
        rows = candidate["history_rows"]
        target_row = next(
            (row for row in rows if row.get("session_date") == target_session), None
        )
        if target_row is None:
            raise PgPriceProfileError(
                "PROFILE_TARGET_RAW_MISSING", "来源分页没有精确目标日Raw行"
            )
        window = result.get("window") or {}
        core_fields = result.get("core_price_fields") or {}
        profile_components = sorted(
            result.get("profile_components") or (), key=lambda item: item["field_id"]
        )
        if len(profile_components) != 11:
            raise PgPriceProfileError(
                "PROFILE_COMPONENT_COUNT_INVALID", "现役Profile11分项没有完整返回"
            )
        source_time_quality = target_row.get("source_time_quality") or "UNKNOWN"
        source_available_at = target_row.get("capture_available_at")
        if isinstance(source_available_at, str):
            source_available_at = datetime.fromisoformat(source_available_at)
        record: dict[str, Any] = {
            "source_raw_id": int(target_row["id"]),
            "source_capture_id": int(target_row["source_capture_id"]),
            "source_symbol": symbol,
            "market": candidate["market"],
            "board": candidate["board"],
            "target_session": target_session,
            "calculation_scope": SOURCE_SYMBOL_PROFILE_SCOPE,
            "producer_contract": _PRODUCER_CONTRACT,
            "producer_version": _PRODUCER_SEMANTICS_VERSION,
            "price_coordinate": "FORWARD_ADJUSTED",
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
            "listing_status": "LISTING_STATUS_UNKNOWN",
            "suspension_status": _resolve_suspension_status(target_row),
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "source_time_quality": source_time_quality,
            "source_available_at": source_available_at,
            "history_calendar_id": window.get("calendar_id") or "",
            "history_calendar_revision": window.get("calendar_revision") or "",
            "history_window_start": date.fromisoformat(
                window.get("start_session") or page["session_start"]
            ),
            "history_window_end": date.fromisoformat(
                window.get("end_session") or page["session_end"]
            ),
            "history_requested_session_count": int(
                window.get("requested_sessions") or page["session_count"]
            ),
            "history_stored_session_count": int(
                window.get("stored_sessions", len(rows))
            ),
            "history_records_complete": bool(result["history_records_complete"]),
            "history_records_status": result["history_records_status"],
            "history_actual_price_bar_count": int(window.get("real_bar_count", 0)),
            "target_bar_status": result["target_bar_status"],
            "target_price_quality": target_row.get(
                "qfq_close_quality", target_row.get("qfq_price_quality", "UNKNOWN")
            ),
            "ma20_window_valid_bar_count": int(result["ma20_window_valid_bar_count"]),
            "ma20_window_expected_bar_count": int(
                result["ma20_window_expected_bar_count"]
            ),
            "atr20_window_valid_bar_count": int(result["price_window_valid_bar_count"]),
            "atr20_window_expected_bar_count": int(
                result["price_window_expected_bar_count"]
            ),
            "price_window_usable": bool(result["price_window_usable"]),
            "price_window_status": result["price_window_status"],
            "price_window_reason": result["price_window_reason"],
            "amount_unit_status": target_row["amount_unit_status"],
            "volume_unit_status": target_row["volume_unit_status"],
        }
        for field_id, column in _CORE_FIELDS.items():
            field = core_fields.get(field_id)
            if not isinstance(field, Mapping):
                if result.get("consumer_status") != "CONSUMER_FAILED":
                    raise PgPriceProfileError(
                        "PROFILE_CORE_FIELD_OMITTED",
                        f"画像生产者未返回已登记字段{field_id}，拒绝伪造未知值",
                    )
                field = {
                    "value": None,
                    "quality": "UNKNOWN",
                    "reason": result.get("consumer_error_code") or "CORE_FIELD_OMITTED",
                }
            value = field.get("value")
            if isinstance(value, Mapping):
                decimal_value = value.get("decimal")
                exact_value = value.get("exact")
                if exact_value is not None:
                    try:
                        exact_fraction = Fraction(str(exact_value))
                    except (ValueError, ZeroDivisionError) as error:
                        raise PgPriceProfileError(
                            "PROFILE_CORE_VALUE_INVALID",
                            f"{field_id}的精确分数表示无效",
                        ) from error
                elif decimal_value is not None:
                    exact_fraction = Fraction(Decimal(str(decimal_value)))
                else:
                    exact_fraction = None
                value = decimal_value
            elif value is not None:
                exact_fraction = Fraction(Decimal(str(value)))
            else:
                exact_fraction = None
            if value is not None and not isinstance(value, (str, int, Decimal)):
                raise PgPriceProfileError(
                    "PROFILE_CORE_VALUE_INVALID",
                    f"{field_id}的Core数值表示不符合有界NUMERIC合同",
                )
            record[column] = Decimal(str(value)) if value is not None else None
            record[f"{column}_exact_numerator"] = (
                exact_fraction.numerator if exact_fraction is not None else None
            )
            record[f"{column}_exact_denominator"] = (
                exact_fraction.denominator if exact_fraction is not None else None
            )
            quality = field.get("quality") or "UNKNOWN"
            if quality == "UNKNOWN_DATA":
                quality = "UNKNOWN"
            record[f"{column}_quality"] = quality
            record[f"{column}_reason"] = field.get("reason")
        for field_id, column in _CORE_BOOLEAN_FIELDS.items():
            field = core_fields.get(field_id)
            if not isinstance(field, Mapping):
                if result.get("consumer_status") != "CONSUMER_FAILED":
                    raise PgPriceProfileError(
                        "PROFILE_CORE_FIELD_OMITTED",
                        f"画像生产者未返回已登记字段{field_id}，拒绝伪造未知值",
                    )
                field = {
                    "value": None,
                    "quality": "UNKNOWN",
                    "reason": result.get("consumer_error_code") or "CORE_FIELD_OMITTED",
                }
            value = field.get("value")
            quality = field.get("quality") or "UNKNOWN"
            if quality == "UNKNOWN_DATA":
                quality = "UNKNOWN"
            if quality == "OBSERVED":
                if type(value) is not bool or field.get("reason") is not None:
                    raise PgPriceProfileError(
                        "PROFILE_CORE_VALUE_INVALID",
                        f"{field_id}必须保存原生布尔值且不含未知原因",
                    )
                record[column] = value
                record[f"{column}_reason"] = None
            elif quality == "UNKNOWN":
                if value is not None:
                    raise PgPriceProfileError(
                        "PROFILE_CORE_VALUE_INVALID",
                        f"{field_id}未知时不得保存布尔值",
                    )
                record[column] = None
                record[f"{column}_reason"] = field.get("reason") or "CORE_FIELD_UNKNOWN"
            else:
                raise PgPriceProfileError(
                    "PROFILE_CORE_QUALITY_INVALID",
                    f"{field_id}质量必须为OBSERVED或UNKNOWN",
                )
            record[f"{column}_quality"] = quality
        record["profile_field_ids"] = [item["field_id"] for item in profile_components]
        record["profile_statuses"] = [
            item.get("status") or "UNKNOWN" for item in profile_components
        ]
        record["profile_qualities"] = [
            item.get("quality") or item.get("status") or "UNKNOWN"
            for item in profile_components
        ]
        record["profile_values"] = [
            ""
            if item.get("value") is None
            or (item.get("quality") or item.get("status")) != "OBSERVED"
            else str(item.get("value"))
            for item in profile_components
        ]
        record["profile_reasons"] = [
            item.get("reason") or "" for item in profile_components
        ]
        records.append(record)
    return records


def _read_baostock_profile_page(
    config: Mapping[str, Any],
    *,
    source_symbols: tuple[str, ...],
    source_capture_id: int,
    sessions: tuple[date, ...],
    cutoff: date,
) -> dict[str, Any]:
    histories: dict[str, list[dict[str, Any]]] = {
        symbol: [] for symbol in source_symbols
    }
    offset = 0
    expected_total: int | None = None
    while True:
        batch = read_baostock_price_history_batch(
            config,
            source_symbols=source_symbols,
            sessions=sessions,
            # 短业务键下历史行按证券/日期读取；source_capture_id仅作溯源，
            # 不能截断实际窗口。Reader逐行返回各自QFQ来源身份。
            source_capture_id=None,
            price_coordinate="FORWARD_ADJUSTED",
            cutoff=cutoff,
            offset=offset,
            limit=5000,
        )
        page_info = batch.get("page", {})
        total = page_info.get("total")
        next_offset = page_info.get("next_offset")
        if batch.get("status") != "AVAILABLE" or type(total) is not int:
            raise PgPriceProfileError(
                "PROFILE_SOURCE_PAGE_UNAVAILABLE", "BaoStock QFQ分页未返回完整PG读结果"
            )
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise PgPriceProfileError(
                "PROFILE_SOURCE_PARTITION_CHANGED", "BaoStock QFQ分页期间分母发生变化"
            )
        if any(
            item.get("coverage_status") != "COMPLETE"
            or item.get("row_count") != item.get("expected_symbol_count")
            or item.get("completed_request_count") != item.get("expected_symbol_count")
            for item in batch.get("coverage", ())
        ):
            raise PgPriceProfileError(
                "PROFILE_SOURCE_PARTITION_OPEN",
                "BaoStock QFQ来源请求或逐会话读回不完整",
            )
        for row in batch.get("rows", ()):
            symbol = row.get("source_symbol")
            if symbol not in histories:
                raise PgPriceProfileError(
                    "PROFILE_SOURCE_SYMBOL_MISMATCH", "BaoStock QFQ分页包含范围外证券"
                )
            histories[symbol].append(row)
        if next_offset is None:
            break
        if type(next_offset) is not int or next_offset <= offset:
            raise PgPriceProfileError(
                "PROFILE_SOURCE_PAGE_INVALID", "BaoStock QFQ分页游标无效"
            )
        offset = next_offset

    actual_total = sum(len(rows) for rows in histories.values())
    if expected_total is None or actual_total != expected_total:
        raise PgPriceProfileError(
            "PROFILE_SOURCE_ROW_COUNT_MISMATCH", "BaoStock QFQ历史分页行数没有闭合"
        )
    candidates = []
    target_session = sessions[-1]
    target_status_counts: Counter[str] = Counter()
    for symbol in source_symbols:
        rows = sorted(histories[symbol], key=lambda item: item["session_date"])
        qfq_captures = {
            int(item["source_capture_id"])
            for item in rows
            if item.get("bar_status") == "TARGET_PRESENT"
            and item.get("source_capture_id") is not None
        }
        if len(qfq_captures) > 1 or any(
            item.get("bar_status") == "TARGET_PRESENT"
            and item.get("source_capture_id") is None
            for item in rows
        ):
            # BaoStock QFQ为后验坐标；不同请求捕获间没有可用的换基连续性证明。
            # 只将本画像窗口的QFQ输入降为未知，RAW/量额/限价事实不受影响。
            for item in rows:
                if item.get("bar_status") == "TARGET_PRESENT":
                    item["bar_status"] = "UNKNOWN"
                    item["qfq_bar_status"] = "UNKNOWN"
                    item["qfq_close_quality"] = "UNKNOWN"
                    item["qfq_close_reason"] = "QFQ_CROSS_CAPTURE_CONTINUITY_UNPROVEN"
                    for field in ("open", "high", "low", "close"):
                        item[f"qfq_{field}_quality"] = "UNKNOWN"
                        item[f"qfq_{field}_reason"] = (
                            "QFQ_CROSS_CAPTURE_CONTINUITY_UNPROVEN"
                        )
                    for field in (
                        "selected_open",
                        "selected_high",
                        "selected_low",
                        "selected_close",
                    ):
                        item[field] = None
                    for field in (
                        "selected_open_quality",
                        "selected_high_quality",
                        "selected_low_quality",
                        "selected_close_quality",
                    ):
                        item[field] = "UNKNOWN"
        row_dates = {item["session_date"] for item in rows}
        target_row = next(
            (item for item in rows if item["session_date"] == target_session), None
        )
        if target_row is None:
            raise PgPriceProfileError(
                "PROFILE_TARGET_RAW_MISSING", f"BaoStock QFQ分页缺少目标日{symbol}"
            )
        target_status_counts[str(target_row.get("bar_status") or "READ_FAILED")] += 1
        candidates.append(
            {
                "source_symbol": symbol,
                "market": target_row["market"],
                "board": target_row["board"],
                "history_rows": rows,
                "missing_sessions": [
                    session.isoformat()
                    for session in sessions
                    if session not in row_dates
                ],
            }
        )
    if len(candidates) != len(source_symbols):
        raise PgPriceProfileError(
            "PROFILE_BATCH_DENOMINATOR_MISMATCH", "BaoStock QFQ来源代码分页没有闭合"
        )
    return {
        "status": "AVAILABLE",
        "total_count": len(candidates),
        "version_status": "CLOSED",
        "version_row_count": len(candidates),
        "version_expected_row_count": len(candidates),
        "partition_closed": True,
        "market_filter": "ALL",
        "board_filter": "ALL",
        "session_start": sessions[0].isoformat(),
        "session_end": sessions[-1].isoformat(),
        "session_count": len(sessions),
        "offset": 0,
        "limit": len(candidates),
        "page_count": len(candidates),
        "next_offset": None,
        "bar_status_counts": dict(sorted(target_status_counts.items())),
        "candidates": candidates,
    }


def run_source_symbol_price_profile(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    research_cutoff: date,
    scope: str,
    session_count: int,
    page_size: int,
    profile_contract: dict[str, Any],
    cross_section_universe_version: str | None = None,
    cross_section_universe_capture_id: int | None = None,
) -> dict[str, Any]:
    """按精确Raw包和捕获读取价格，复用Core43/Profile11并保存typed画像。"""
    if scope != SOURCE_SYMBOL_PROFILE_SCOPE:
        raise PgPriceProfileError(
            "PROFILE_SCOPE_INVALID",
            "画像计算范围必须显式为SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE",
        )
    if (
        type(session_count) is not int
        or not 1 <= session_count <= PG_PROFILE_MAX_HISTORY_SESSIONS
    ):
        raise PgPriceProfileError(
            "PROFILE_WINDOW_COUNT_INVALID",
            "画像历史会话数必须为正整数且不超过存储字段范围",
        )
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise PgPriceProfileError(
            "PROFILE_PAGE_SIZE_INVALID", "来源分页批大小必须在1至100之间"
        )
    field_count = sum(
        isinstance(item, dict)
        and isinstance(item.get("field_id"), str)
        and item["field_id"].startswith("core.")
        for item in profile_contract.get("field_registry", ())
    )
    profile_count = sum(
        isinstance(item, dict) and isinstance(item.get("field_id"), str)
        for item in profile_contract.get("profile_expression_registry", {}).values()
    )
    if (field_count, profile_count) != (43, 11):
        raise PgPriceProfileError(
            "PROFILE_CONTRACT_INVALID", "现役画像字段合同不是Core43/Profile11"
        )
    _validate_profile_semantics(profile_contract)
    try:
        if type(research_cutoff) is not date or research_cutoff < target_session:
            raise PgPriceProfileError(
                "PROFILE_RESEARCH_CUTOFF_INVALID", "画像研究截止日必须不早于目标会话"
            )
        resolved_target, session_dates, calendar_revision = resolve_pg_history_window(
            target_session.isoformat(), session_count
        )
        if resolved_target != target_session:
            raise PgPriceProfileError(
                "PROFILE_TARGET_NOT_TRADING_SESSION", "输入日期不是已登记官方交易会话"
            )
        source_context = read_source_version_context(
            config,
            target_session=target_session,
            # 画像按QFQ字段真实来源读取；任务级capture仅作溯源，不能替代
            # 同一业务日期现存价格事实的逐坐标来源身份。
            source_capture_id=None,
            research_cutoff=research_cutoff,
            price_coordinate="FORWARD_ADJUSTED",
        )
        target_universe = resolve_date_effective_universe(
            config,
            session_date=target_session,
            directory_capture_id=cross_section_universe_capture_id,
            research_eligible_only=True,
        )
        if target_universe.get("status") != "AVAILABLE":
            raise PgPriceProfileError(
                "PROFILE_TARGET_UNIVERSE_UNAVAILABLE",
                "目标日有效名录不可读，不能以来源捕获代码池代替研究总体",
            )
        source_symbols = tuple(sorted(target_universe.get("members", ())))
        raw_total = len(source_symbols)
        if raw_total <= 0:
            raise PgPriceProfileError(
                "PROFILE_TARGET_UNIVERSE_EMPTY",
                "目标日有效研究总体为空",
            )
        from stock_a.storage.postgres.cross_section import (
            read_cross_section_day_bundle,
        )

        cross_section_bundle = read_cross_section_day_bundle(
            config,
            target_session=target_session,
            price_capture_id=None,
            universe_capture_id=cross_section_universe_capture_id,
            calendar_revision=calendar_revision,
        )
        explicit_cross_section_scope = cross_section_universe_capture_id is not None
        if cross_section_bundle.get("status") == "INCOMPLETE":
            raise PgPriceProfileError(
                "PROFILE_CROSS_SECTION_SCOPE_AMBIGUOUS_OR_INCOMPLETE",
                "目标日横截面有多个候选或完整性不足；画像拒绝静默套用其他总体",
            )
        if (
            explicit_cross_section_scope
            and cross_section_bundle.get("status") != "AVAILABLE"
        ):
            raise PgPriceProfileError(
                "PROFILE_CROSS_SECTION_SCOPE_NOT_FOUND",
                "指定横截面没有唯一完整事实",
            )
        if cross_section_bundle.get("status") != "AVAILABLE":
            cross_section_bundle = None

        records: list[dict[str, Any]] = []
        consumer_failures = 0
        source_status_counts: Counter[str] = Counter()
        for start in range(0, len(source_symbols), page_size):
            symbols_page = source_symbols[start : start + page_size]
            page = _read_baostock_profile_page(
                config,
                source_symbols=symbols_page,
                source_capture_id=None,
                sessions=tuple(session_dates),
                cutoff=research_cutoff,
            )
            if page["total_count"] != len(symbols_page):
                raise PgPriceProfileError(
                    "PROFILE_SOURCE_PARTITION_CHANGED", "BaoStock QFQ来源分母发生变化"
                )
            source_status_counts.update(page["bar_status_counts"])
            preview = research_universe_pg_history_preview(
                target_session=target_session.isoformat(),
                source_capture_id=source_capture_id,
                session_dates=session_dates,
                page=page,
                profile_contract=profile_contract,
                cross_section_bundle=cross_section_bundle,
            )
            page_records = _records_for_page(
                page=page,
                preview=preview,
                target_session=target_session,
                source_capture_id=source_capture_id,
                source_context=source_context,
            )
            consumer_failures += sum(
                item.get("consumer_status") == "CONSUMER_FAILED"
                for item in preview.get("results", ())
            )
            records.extend(page_records)
        if (
            len(records) != raw_total
            or len({item["source_symbol"] for item in records}) != raw_total
        ):
            raise PgPriceProfileError(
                "PROFILE_BATCH_DENOMINATOR_MISMATCH", "全来源代码画像未一对一闭合"
            )
        calendar_id = records[0]["history_calendar_id"] if records else ""
        if not calendar_id:
            raise PgPriceProfileError(
                "PROFILE_CALENDAR_ID_MISSING", "现役Core窗口未返回正式日历身份"
            )
        result = persist_profile_records(config, records)
        result.update(
            {
                "status": "COMPLETE_WITH_SYMBOL_FAILURES"
                if consumer_failures
                else result["status"],
                "source_candidate_count": raw_total,
                "consumer_failure_count": consumer_failures,
                "source_status_counts": dict(sorted(source_status_counts.items())),
                "scope": scope,
                "session_count": session_count,
                "session_start": session_dates[0].isoformat(),
                "session_end": session_dates[-1].isoformat(),
                "calendar_revision": calendar_revision,
                "source_context": source_context,
                "source_zip_opened": False,
                "baostock_called": False,
                "formal_universe_status": "UNKNOWN_NOT_PROVED_BY_SOURCE_SYMBOLS",
                "pit_status": "NON_PIT_RETROSPECTIVE",
            }
        )
        return result
    except (
        RawSourcePgError,
        BaoStockPriceFactsError,
        PgResearchBindingError,
        PriceProfilePgError,
        ResearchEntryError,
    ) as error:
        code = getattr(error, "code", "PG_PRICE_PROFILE_FAILED")
        cause = getattr(error, "__cause__", None)
        column_name = getattr(getattr(cause, "diag", None), "column_name", None)
        detail = f"（数据库列{column_name}）" if column_name else ""
        raise PgPriceProfileError(code, f"{error}{detail}") from error


def source_symbol_price_profile_snapshots(
    config: Mapping[str, Any], *, max_target_session: date
) -> dict[str, Any]:
    try:
        return list_profile_snapshots(config, max_target_session=max_target_session)
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_READ_FAILED"), str(error)
        ) from error


def query_source_symbol_price_profiles(
    config: Mapping[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        return read_profile_page(config, **kwargs)
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_READ_FAILED"), str(error)
        ) from error


def read_source_symbol_price_profile_observation(
    config: Mapping[str, Any],
    profile_contract: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    research_cutoff: date | None = None,
    history_session_count: int | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    """按Raw捕获、来源包和现存画像修订投影已保存Core/Profile画像及质量。"""
    if type(source_capture_id) is not int or source_capture_id <= 0:
        return {
            "status": "NOT_COMPUTED",
            "reason_code": "RAW_SOURCE_VERSION_UNAVAILABLE",
            "target_session": target_session.isoformat(),
            "source_capture_id": source_capture_id,
            "database_write": False,
            "ddl_executed": False,
        }
    try:
        _validate_profile_semantics(profile_contract)
        observation = read_source_profile_observability(
            config,
            target_session=target_session,
            source_capture_id=source_capture_id,
            research_cutoff=research_cutoff,
            current_profile_revision=_PROFILE_REVISION,
            current_producer_version=_PRODUCER_SEMANTICS_VERSION,
            history_session_count=history_session_count,
            requested_profile_revision=profile_revision,
        )
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_OBSERVATION_READ_FAILED"), str(error)
        ) from error

    core_registry = profile_contract.get("field_registry", ())
    profile_registry = profile_contract.get("profile_expression_registry", {})
    observation["contract_field_counts"] = {
        "core43_registered_fields": sum(
            1
            for item in core_registry
            if isinstance(item, Mapping)
            and isinstance(item.get("field_id"), str)
            and item["field_id"].startswith("core.")
        )
        if isinstance(core_registry, (list, tuple))
        else 0,
        "stored_core_quality_fields": len(_CORE_FIELDS) + len(_CORE_BOOLEAN_FIELDS),
        "profile11_registered_fields": len(profile_registry)
        if isinstance(profile_registry, Mapping)
        else 0,
    }
    observation["contract_field_counts"]["core43_all_fields_persisted"] = (
        observation["contract_field_counts"]["core43_registered_fields"]
        == observation["contract_field_counts"]["stored_core_quality_fields"]
    )
    selected = observation.get("selected_revision")
    selected_rows = (
        int(selected.get("stored_rows", 0)) if isinstance(selected, Mapping) else 0
    )
    core_totals: dict[str, int] = {}
    for row in observation.get("core_quality_rows", ()):
        field_id = str(row["field_id"])
        if row.get("quality") in {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE"}:
            core_totals[field_id] = core_totals.get(field_id, 0) + int(row["row_count"])
    profile_totals: dict[str, int] = {}
    for row in observation.get("profile_quality_rows", ()):
        field_id = str(row["field_id"])
        profile_totals[field_id] = profile_totals.get(field_id, 0) + int(
            row["row_count"]
        )
    expected_profile_ids = (
        {
            item["field_id"]
            for item in profile_registry.values()
            if isinstance(item, Mapping) and isinstance(item.get("field_id"), str)
        }
        if isinstance(profile_registry, Mapping)
        else set()
    )
    observation["quality_field_denominators"] = {
        "core_field_rows": core_totals,
        "profile_field_rows": profile_totals,
        "all_stored_fields_closed": bool(
            selected_rows
            and all(
                core_totals.get(field_id) == selected_rows
                for field_id in (*_CORE_FIELDS, *_CORE_BOOLEAN_FIELDS)
            )
            and expected_profile_ids == set(profile_totals)
            and all(
                profile_totals.get(field_id) == selected_rows
                for field_id in expected_profile_ids
            )
        ),
    }
    if isinstance(selected, Mapping):
        observation["history_window"] = {
            "records_complete_rows": selected["history_records_complete_rows"],
            "eligible_24_session_rows": selected["history_24_session_eligible_rows"],
            "requested_24_plus_rows": selected["history_requested_24_plus_rows"],
            "requested_sessions_min": selected["min_history_requested_sessions"],
            "requested_sessions_max": selected["max_history_requested_sessions"],
            "stored_sessions_min": selected["min_history_stored_sessions"],
            "stored_sessions_max": selected["max_history_stored_sessions"],
            "price_window_usable_rows": selected["price_window_usable_rows"],
        }
    else:
        observation["history_window"] = {
            "records_complete_rows": 0,
            "eligible_24_session_rows": 0,
            "requested_24_plus_rows": 0,
            "requested_sessions_min": None,
            "requested_sessions_max": None,
            "stored_sessions_min": None,
            "stored_sessions_max": None,
            "price_window_usable_rows": 0,
        }
    return observation


def _profile_session_relation(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> dict[str, Any]:
    before_session = before["target_session"]
    after_session = after["target_session"]
    before_date = (
        before_session
        if isinstance(before_session, date)
        else date.fromisoformat(str(before_session))
    )
    after_date = (
        after_session
        if isinstance(after_session, date)
        else date.fromisoformat(str(after_session))
    )
    relation = {
        "status": "UNKNOWN",
        "before_session": before_session.isoformat()
        if isinstance(before_session, date)
        else str(before_session),
        "after_session": after_session.isoformat()
        if isinstance(after_session, date)
        else str(after_session),
        "calendar_id": None,
        "calendar_revision": None,
        "reason": "OFFICIAL_SESSION_ADJACENCY_NOT_PROVEN",
    }
    if not _profile_calendar_revisions_compatible(before, after):
        return relation
    try:
        payload, _metadata, revision, sessions = _load_calendar(after_date)
    except ResearchEntryError:
        return relation
    if (
        payload.get("calendar_id") != before.get("history_calendar_id")
        or not _snapshot_calendar_matches_revision_chain(before, revision)
        or not _snapshot_calendar_matches_revision_chain(after, revision)
        or not _profile_window_matches_sessions(before, sessions, before_date)
        or not _profile_window_matches_sessions(after, sessions, after_date)
    ):
        relation["reason"] = "PROFILE_CALENDAR_OR_WINDOW_MISMATCH"
        return relation
    relation.update(
        {
            "calendar_id": payload["calendar_id"],
            "calendar_revision": revision,
            "reason": None,
        }
    )
    before_index = next(
        (
            index
            for index, day in enumerate(sessions)
            if day == before_date or day.isoformat() == before_date.isoformat()
        ),
        None,
    )
    after_index = next(
        (
            index
            for index, day in enumerate(sessions)
            if day == after_date or day.isoformat() == after_date.isoformat()
        ),
        None,
    )
    if before_index is None or after_index is None:
        relation["reason"] = "SESSION_NOT_IN_PROFILE_CALENDAR"
    else:
        relation["status"] = (
            "CONSECUTIVE_OFFICIAL_SESSIONS"
            if after_index == before_index + 1
            else "NONCONSECUTIVE_OFFICIAL_SESSIONS"
        )
    return relation


def _calendar_revision_map(value: Any) -> dict[int, str] | None:
    if not isinstance(value, str) or not value:
        return None
    revisions: dict[int, str] = {}
    for component in value.split("+"):
        year, separator, revision = component.partition(":")
        if (
            not separator
            or re.fullmatch(r"[0-9]{4}", year) is None
            or not revision
            or int(year) in revisions
        ):
            return None
        revisions[int(year)] = revision
    return revisions


def _snapshot_calendar_window_years(
    snapshot: Mapping[str, Any],
) -> set[int] | None:
    start = snapshot.get("window_start") or snapshot.get("history_window_start")
    end = snapshot.get("window_end") or snapshot.get("history_window_end")
    try:
        start_date = (
            start.date()
            if isinstance(start, datetime)
            else start
            if isinstance(start, date)
            else date.fromisoformat(str(start))
        )
        end_date = (
            end.date()
            if isinstance(end, datetime)
            else end
            if isinstance(end, date)
            else date.fromisoformat(str(end))
        )
    except (TypeError, ValueError):
        return None
    if start_date > end_date:
        return None
    return set(range(start_date.year, end_date.year + 1))


def _snapshot_calendar_matches_revision_chain(
    snapshot: Mapping[str, Any], revision_chain: Any
) -> bool:
    snapshot_years = _snapshot_calendar_window_years(snapshot)
    snapshot_revisions = _calendar_revision_map(
        snapshot.get("history_calendar_revision")
    )
    expected_revisions = _calendar_revision_map(revision_chain)
    return bool(
        snapshot_years
        and snapshot_revisions
        and expected_revisions
        and all(
            year in snapshot_revisions
            and year in expected_revisions
            and snapshot_revisions[year] == expected_revisions[year]
            for year in snapshot_years
        )
    )


def _profile_calendar_revisions_compatible(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> bool:
    calendar_id = before.get("history_calendar_id")
    before_count = before.get("history_session_count")
    after_count = after.get("history_session_count")
    if (
        not isinstance(calendar_id, str)
        or not calendar_id
        or calendar_id != after.get("history_calendar_id")
        or type(before_count) is not int
        or before_count <= 0
        or before_count != after_count
    ):
        return False
    before_years = _snapshot_calendar_window_years(before)
    after_years = _snapshot_calendar_window_years(after)
    before_revisions = _calendar_revision_map(before.get("history_calendar_revision"))
    after_revisions = _calendar_revision_map(after.get("history_calendar_revision"))
    if (
        not before_years
        or not after_years
        or not before_revisions
        or not after_revisions
    ):
        return False
    if not before_years.issubset(before_revisions) or not after_years.issubset(
        after_revisions
    ):
        return False
    shared_years = before_years & after_years
    return bool(shared_years) and all(
        before_revisions[year] == after_revisions[year] for year in shared_years
    )


def _profile_window_matches_sessions(
    snapshot: Mapping[str, Any],
    sessions: Sequence[date],
    target_session: date,
) -> bool:
    try:
        start_value = snapshot.get("window_start")
        end_value = snapshot.get("window_end")
        start = (
            start_value.date()
            if isinstance(start_value, datetime)
            else start_value
            if isinstance(start_value, date)
            else date.fromisoformat(str(start_value))
        )
        end = (
            end_value.date()
            if isinstance(end_value, datetime)
            else end_value
            if isinstance(end_value, date)
            else date.fromisoformat(str(end_value))
        )
    except (TypeError, ValueError):
        return False
    if end != target_session:
        return False
    selected = tuple(day for day in sessions if start <= day <= end)
    return bool(
        selected
        and selected[0] == start
        and selected[-1] == end
        and len(selected) == snapshot.get("history_session_count")
    )


def query_source_symbol_price_profile_pair(
    config: Mapping[str, Any], **kwargs: Any
) -> dict[str, Any]:
    try:
        result = read_profile_pair(config, **kwargs)
        result["session_relation"] = _profile_session_relation(
            result["before_snapshot"], result["after_snapshot"]
        )
        return result
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_PAIR_READ_FAILED"), str(error)
        ) from error


def query_source_symbol_price_profile_breadth(
    config: Mapping[str, Any], **kwargs: Any
) -> dict[str, Any]:
    try:
        result = read_profile_breadth(config, **kwargs)
        result["session_relation"] = _profile_session_relation(
            result["before_snapshot"], result["after_snapshot"]
        )
        return result
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_BREADTH_READ_FAILED"), str(error)
        ) from error


def query_source_symbol_price_profile_breadth_members(
    config: Mapping[str, Any], **kwargs: Any
) -> dict[str, Any]:
    try:
        result = read_profile_breadth_members(config, **kwargs)
        result["session_relation"] = _profile_session_relation(
            result["before_snapshot"], result["after_snapshot"]
        )
        return result
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_BREADTH_MEMBERS_READ_FAILED"), str(error)
        ) from error


def query_source_symbol_price_profile_changes(
    config: Mapping[str, Any], **kwargs: Any
) -> dict[str, Any]:
    try:
        result = read_profile_change_page(config, **kwargs)
        before = result["before_snapshot"]
        after = result["after_snapshot"]
        result["session_relation"] = _profile_session_relation(before, after)
        return result
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_CHANGE_READ_FAILED"), str(error)
        ) from error


def query_source_symbol_price_profile_relation_screen(
    config: Mapping[str, Any], **kwargs: Any
) -> dict[str, Any]:
    try:
        result = read_profile_relation_screen(config, **kwargs)
        result["session_relation"] = _profile_session_relation(
            result["before_snapshot"], result["after_snapshot"]
        )
        result["research_mode"] = "SOURCE_SYMBOL_NON_PIT_QFQ_PRICE_RELATION_SCREEN"
        result["formal_listing_universe"] = "UNKNOWN_NOT_CONNECTED"
        return result
    except (RawSourcePgError, PriceProfilePgError) as error:
        raise PgPriceProfileError(
            getattr(error, "code", "PROFILE_RELATION_SCREEN_READ_FAILED"), str(error)
        ) from error
