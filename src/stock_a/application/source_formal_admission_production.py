"""从唯一PG已存D0/Core/Profile事实生产LAUNCH正式准入。"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
import inspect
from typing import Any

from stock_a.application.source_d0_raw import _official_calendar
from stock_a.application.source_formal_admission import (
    prepare_source_d0_launch_formal_admission,
)
from stock_a.application.source_pool_seed import (
    resolve_ordinary_candidate_gates,
    resolve_pg_history_window,
)
from stock_a.domain import FieldValue, unknown
from stock_a.research.d0_price_facts import (
    _has_qfq_comparable_evidence,
    _resolve_target_qfq_capture_id,
    calculate_d0_first_day_damage,
    calculate_d0_severe_drop,
)
from stock_a.research.entry_history.support import trade_status_state_from_pg_row
from stock_a.application.pg_research import read_source_version_context
from stock_a.storage.postgres.baostock_price_facts import (
    read_baostock_price_history_batch,
)
from stock_a.storage.postgres.baostock_universe import resolve_date_effective_universe
from stock_a.storage.postgres.formal_admission import (
    list_saved_launch_sessions,
    persist_source_formal_admission_rows,
    read_saved_source_fact_ids,
    read_source_formal_admission_day,
)
from stock_a.storage.postgres.static_research_facts import (
    list_saved_d0_sessions,
    read_price_limit_facts_by_symbol,
    read_source_d0_scenario_day,
    read_source_pool_seed_facts,
    read_source_pool_seed_forward_scope,
)


class SourceFormalAdmissionProductionError(ValueError):
    """正式准入生产输入缺失、不唯一或与冻结合同不符。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_QFQ_COORDINATE = "FORWARD_ADJUSTED"
_RAW_COORDINATE = "RAW_UNADJUSTED"
_RAW_AMOUNT_UNIT_CONTRACT_VERSION = (
    "BAOSTOCK_API_DOCUMENTED_UNITS_NOT_INDEPENDENTLY_BOUND_R1"
)
_D0_LIQ20_THRESHOLD_CNY = Fraction(20_000_000)


def _fraction(value: object) -> Fraction | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Fraction):
        return value
    if isinstance(value, Decimal):
        return Fraction(value) if value.is_finite() else None
    if isinstance(value, int):
        return Fraction(value)
    return None


def _same_session(value: object, expected: date) -> bool:
    return value == expected or value == expected.isoformat()


def _same_source_code(value: object, expected: str) -> bool:
    """BaoStock响应代码大小写不敏感；证券交易所与代码仍须完全匹配。"""
    return isinstance(value, str) and value.upper() == expected


def _qfq_window_row_state(
    row: Mapping[str, Any] | None,
    *,
    symbol: str,
    session: date,
    source_capture_id: int,
    target_row: Mapping[str, Any] | None = None,
) -> tuple[str, str | None]:
    """核对单日QFQ坐标和响应身份；支持同basis跨捕获放行，停牌不误判为未知缺行。"""
    if row is None:
        return "UNKNOWN", "QFQ_HISTORY_ROW_NOT_STORED"
    if row.get("source_symbol") != symbol or not _same_session(
        row.get("session_date"), session
    ):
        return "FALSE", "QFQ_HISTORY_SOURCE_IDENTITY_MISMATCH"
    row_qfq_cap = row.get("qfq_source_capture_id")
    if type(row_qfq_cap) is not int or row_qfq_cap <= 0:
        return "UNKNOWN", "QFQ_SOURCE_CAPTURE_MISSING"
    target_qfq_cap = _resolve_target_qfq_capture_id(target_row, source_capture_id)
    if target_qfq_cap is None:
        return "UNKNOWN", "QFQ_TARGET_CAPTURE_MISSING"
    if row_qfq_cap != target_qfq_cap and not _has_qfq_comparable_evidence(
        row, target_row
    ):
        return "UNKNOWN", "MIXED_PRICE_COORDINATE"
    if (
        row.get("price_coordinate") != _QFQ_COORDINATE
        or row.get("qfq_price_coordinate") != _QFQ_COORDINATE
        or row.get("qfq_adjustflag") not in (2, "2")
    ):
        return "FALSE", "QFQ_HISTORY_COORDINATE_MISMATCH"
    if not _same_session(
        row.get("qfq_response_session_date"), session
    ) or not _same_source_code(row.get("qfq_response_source_code"), symbol):
        return "FALSE", "QFQ_RESPONSE_IDENTITY_MISMATCH"
    required_fields = {"date", "code", "open", "high", "low", "close"}
    response_fields = row.get("qfq_response_fields")
    if not isinstance(response_fields, (list, tuple, set, frozenset)) or not (
        required_fields <= set(response_fields)
    ):
        return "UNKNOWN", "QFQ_RESPONSE_FIELDS_NOT_CLOSED"
    trade_state, trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate=_QFQ_COORDINATE
    )
    if trade_state == "CONFIRMED_SUSPENSION":
        return "UNKNOWN", "CONFIRMED_SUSPENSION_NO_ACTUAL_BAR"
    if row.get("qfq_bar_status") != "TARGET_PRESENT":
        return "UNKNOWN", str(row.get("qfq_bar_reason") or "QFQ_TARGET_BAR_NOT_PRESENT")
    if trade_state != "ACTUAL":
        return "UNKNOWN", trade_reason or "QFQ_TRADE_STATUS_UNKNOWN"
    if row.get("qfq_price_quality") != "OBSERVED":
        return "UNKNOWN", str(
            row.get("qfq_price_reason") or "QFQ_PRICE_QUALITY_NOT_OBSERVED"
        )
    prices = {
        name: _fraction(row.get(f"qfq_{name}"))
        for name in ("open", "high", "low", "close")
    }
    if any(
        row.get(f"qfq_{name}_quality") != "OBSERVED" or prices[name] is None
        for name in prices
    ):
        return "UNKNOWN", "QFQ_OHLC_FIELD_QUALITY_NOT_OBSERVED"
    opening, high, low, close = (
        prices[name] for name in ("open", "high", "low", "close")
    )
    if (
        any(value <= 0 for value in prices.values() if value is not None)
        or low > min(opening, close)
        or high < max(opening, close)
    ):
        return "UNKNOWN", "QFQ_OHLC_VALUE_INVALID"
    return "TRUE", None


def _coordinate_window_gate(
    *,
    symbol: str,
    source_capture_id: int,
    sessions: tuple[date, ...],
    history: Mapping[date, Mapping[str, Any]],
) -> FieldValue:
    """要求21个官方会话逐行绑定合法QFQ坐标，结果仍是NON_PIT。"""
    if len(sessions) != 21 or len(set(sessions)) != len(sessions):
        return unknown("QFQ_WINDOW_SESSION_SET_INVALID")
    target_row = history.get(sessions[-1])
    states = [
        _qfq_window_row_state(
            history.get(session),
            symbol=symbol,
            session=session,
            source_capture_id=source_capture_id,
            target_row=target_row,
        )
        for session in sessions
    ]
    mismatch = next((reason for state, reason in states if state == "FALSE"), None)
    if mismatch:
        return FieldValue(False)
    unknown_reason = next(
        (reason for state, reason in states if state == "UNKNOWN"), None
    )
    if unknown_reason:
        return unknown(unknown_reason)
    return FieldValue(True)


def _raw_amount_for_d0_liquidity(
    row: Mapping[str, Any] | None,
    *,
    symbol: str,
    session: date,
    source_capture_id: int,
) -> tuple[Fraction | None, str | None]:
    """独立按raw_source_capture_id读取RAW金额；仅接受CNY单位合同与实际成交，不牵连QFQ捕获。"""
    if row is None:
        return None, "RAW_LIQ20_ROW_NOT_STORED"
    if row.get("source_symbol") != symbol or not _same_session(
        row.get("session_date"), session
    ):
        return None, "RAW_LIQ20_SOURCE_IDENTITY_MISMATCH"
    row_raw_cap = row.get("raw_source_capture_id")
    if row_raw_cap is None:
        row_raw_cap = row.get("source_capture_id")
    if type(row_raw_cap) is not int or row_raw_cap <= 0:
        return None, "RAW_LIQ20_SOURCE_IDENTITY_MISMATCH"
    if row.get("raw_adjustflag") not in (3, "3"):
        return None, "RAW_LIQ20_COORDINATE_NOT_UNADJUSTED"
    if not _same_session(
        row.get("raw_response_session_date"), session
    ) or not _same_source_code(row.get("raw_response_source_code"), symbol):
        return None, "RAW_RESPONSE_IDENTITY_MISMATCH"
    response_fields = row.get("raw_response_fields")
    if not isinstance(response_fields, (list, tuple, set, frozenset)) or not {
        "date",
        "code",
        "amount",
        "tradestatus",
    } <= set(response_fields):
        return None, "RAW_AMOUNT_RESPONSE_FIELDS_NOT_CLOSED"
    trade_state, trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate=_RAW_COORDINATE
    )
    if trade_state == "CONFIRMED_SUSPENSION":
        return None, "CONFIRMED_SUSPENSION_NO_ACTUAL_BAR"
    if row.get("bar_status") != "TARGET_PRESENT":
        return None, str(row.get("bar_reason") or "RAW_AMOUNT_BAR_NOT_PRESENT")
    if trade_state != "ACTUAL":
        return None, trade_reason or "RAW_AMOUNT_TRADE_STATUS_NOT_ACTUAL"
    if row.get("raw_amount_quality") != "RAW_SOURCE_VALUE_OBSERVED":
        return None, str(
            row.get("raw_amount_reason") or "RAW_AMOUNT_QUALITY_NOT_OBSERVED"
        )
    if (
        row.get("amount_unit_status") != "CNY_VERIFIED"
        or row.get("unit_contract_version") != _RAW_AMOUNT_UNIT_CONTRACT_VERSION
        or row.get("amount_input_unit") != "CNY_DOCUMENTED"
        or _fraction(row.get("amount_to_cny")) != Fraction(1)
    ):
        return None, "RAW_AMOUNT_DOCUMENTED_UNIT_CONTRACT_NOT_BOUND"
    amount = _fraction(row.get("raw_amount"))
    if amount is None or amount < 0:
        return None, "RAW_AMOUNT_VALUE_INVALID"
    return amount, None


def _d0_liquidity_gate(
    *,
    symbol: str,
    source_capture_id: int,
    sessions: tuple[date, ...],
    history: Mapping[date, Mapping[str, Any]],
) -> FieldValue:
    """以严格前20会话的RAW CNY金额中位数评价D0 liq20三态。"""
    if len(sessions) != 21 or len(set(sessions)) != len(sessions):
        return unknown("RAW_LIQ20_SESSION_SET_INVALID")
    prior_amounts: list[Fraction] = []
    for session in sessions[:-1]:
        amount, reason = _raw_amount_for_d0_liquidity(
            history.get(session),
            symbol=symbol,
            session=session,
            source_capture_id=source_capture_id,
        )
        if reason:
            return unknown(reason)
        if amount is None:
            return unknown("RAW_LIQ20_AMOUNT_NOT_OBSERVED")
        prior_amounts.append(amount)
    if len(prior_amounts) != 20:
        return unknown("RAW_LIQ20_WINDOW_NOT_CLOSED")
    ordered = sorted(prior_amounts)
    median = (ordered[9] + ordered[10]) / 2
    return FieldValue(median >= _D0_LIQ20_THRESHOLD_CNY)


def _date(value: object) -> date:
    if type(value) is date:
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise SourceFormalAdmissionProductionError(
        "FORMAL_ADMISSION_DATE_INVALID", "已存D0日期字段不是有效日期"
    )


def _aware_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise SourceFormalAdmissionProductionError(
                "FORMAL_ADMISSION_CAPTURE_TIME_INVALID", "来源捕获时间缺少时区"
            )
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise SourceFormalAdmissionProductionError(
                "FORMAL_ADMISSION_CAPTURE_TIME_INVALID", "来源捕获时间缺少时区"
            )
        return parsed
    raise SourceFormalAdmissionProductionError(
        "FORMAL_ADMISSION_CAPTURE_TIME_INVALID", "来源捕获时间类型无效"
    )


def _seed_actual_quote_gate(seed: Mapping[str, Any]) -> FieldValue:
    """优先读取Seed actual_bar三态，兼容早期仅保存布尔投影的行。"""
    predicate_ids = seed.get("predicate_ids")
    predicate_states = seed.get("predicate_states")
    predicate_reasons = seed.get("predicate_reasons")
    if (
        isinstance(predicate_ids, (list, tuple))
        and isinstance(predicate_states, (list, tuple))
        and isinstance(predicate_reasons, (list, tuple))
        and len(predicate_ids) == len(predicate_states) == len(predicate_reasons)
        and "actual_bar" in predicate_ids
    ):
        index = predicate_ids.index("actual_bar")
        state = predicate_states[index]
        if state == "TRUE":
            return FieldValue(True)
        if state == "FALSE":
            return FieldValue(False)
        if state == "UNKNOWN":
            return unknown(str(predicate_reasons[index] or "TRADE_STATUS_UNKNOWN"))
    value = seed.get("target_actual_quote")
    if type(value) is bool:
        return FieldValue(value)
    return unknown("TARGET_ACTUAL_QUOTE_NOT_COMPUTED")


def _negated_bool_gate(
    value: object,
    quality: object,
    reason: object,
    *,
    unknown_reason: str,
) -> FieldValue:
    if quality != "OBSERVED" or type(value) is not bool:
        return unknown(str(reason or unknown_reason))
    return FieldValue(not value)


def _raw_history_for_candidates(
    *,
    config,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    candidates: list[str],
    sessions: tuple[date, ...],
) -> dict[str, dict[date, dict[str, Any]]]:
    histories: dict[str, dict[date, dict[str, Any]]] = {}
    for start in range(0, len(candidates), 100):
        symbols = candidates[start : start + 100]
        offset = 0
        total = None
        batch_rows = 0
        while True:
            batch = read_baostock_price_history_batch(
                config,
                source_symbols=symbols,
                sessions=sessions,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                cutoff=research_cutoff,
                offset=offset,
                limit=1000,
            )
            if (
                batch.get("source_capture_id") != source_capture_id
                or batch.get("price_coordinate") != "FORWARD_ADJUSTED"
            ):
                raise SourceFormalAdmissionProductionError(
                    "FORMAL_ADMISSION_QFQ_HISTORY_IDENTITY_INVALID",
                    "QFQ历史批次没有绑定请求的paired capture。",
                )
            page = batch.get("page", {})
            if total is None:
                total = int(page.get("total", -1))
            elif int(page.get("total", -1)) != total:
                raise SourceFormalAdmissionProductionError(
                    "FORMAL_ADMISSION_QFQ_HISTORY_DENOMINATOR_CHANGED",
                    "正式准入QFQ历史分页期间分母发生变化。",
                )
            if any(
                item.get("coverage_status") != "COMPLETE"
                for item in batch.get("coverage", ())
            ):
                raise SourceFormalAdmissionProductionError(
                    "FORMAL_ADMISSION_QFQ_HISTORY_INCOMPLETE",
                    "正式准入QFQ历史会话没有完整请求与行覆盖。",
                )
            for row in batch.get("rows", ()):
                history = histories.setdefault(row["source_symbol"], {})
                if row["session_date"] in history:
                    raise SourceFormalAdmissionProductionError(
                        "FORMAL_ADMISSION_QFQ_HISTORY_DUPLICATE",
                        "正式准入QFQ历史会话重复。",
                    )
                history[row["session_date"]] = {
                    key: row.get(key)
                    for key in (
                        "source_code",
                        "source_symbol",
                        "session_date",
                        "source_capture_id",
                        "raw_source_capture_id",
                        "qfq_source_capture_id",
                        "qfq_basis_comparable",
                        "qfq_comparable",
                        "qfq_basis_id",
                        "qfq_coordinate_basis",
                        "qfq_price_basis",
                        "price_coordinate",
                        "qfq_price_coordinate",
                        "qfq_response_session_date",
                        "qfq_response_source_code",
                        "qfq_adjustflag",
                        "qfq_response_fields",
                        "bar_status",
                        "bar_reason",
                        "qfq_bar_status",
                        "qfq_bar_reason",
                        "qfq_price_quality",
                        "qfq_price_reason",
                        "qfq_open_quality",
                        "qfq_high_quality",
                        "qfq_low_quality",
                        "qfq_close_quality",
                        "qfq_tradestatus_value",
                        "qfq_tradestatus_quality",
                        "qfq_tradestatus_reason",
                        "qfq_open",
                        "qfq_high",
                        "qfq_low",
                        "qfq_close",
                        "raw_response_session_date",
                        "raw_response_source_code",
                        "raw_adjustflag",
                        "raw_response_fields",
                        "raw_amount",
                        "raw_amount_quality",
                        "raw_amount_reason",
                        "amount_quality",
                        "amount_unit_status",
                        "unit_contract_version",
                        "amount_input_unit",
                        "amount_to_cny",
                        "raw_tradestatus_value",
                        "raw_tradestatus_quality",
                        "raw_tradestatus_reason",
                    )
                }
                batch_rows += 1
            next_offset = page.get("next_offset")
            if next_offset is None:
                break
            offset = int(next_offset)
        expected_rows = sum(
            int(item.get("expected_symbol_count", 0))
            for item in batch.get("coverage", ())
        )
        if total is None or batch_rows != total or total != expected_rows:
            coverage = batch.get("coverage", ())
            coverage_gaps = [
                {
                    "session_date": item.get("session_date"),
                    "expected": item.get("expected_symbol_count"),
                    "rows": item.get("row_count"),
                    "symbols": item.get("symbol_count"),
                    "quality_rows": item.get("quality_row_count"),
                }
                for item in coverage
                if item.get("coverage_status") != "COMPLETE"
                or item.get("expected_symbol_count") != item.get("quality_row_count")
            ][:5]
            raise SourceFormalAdmissionProductionError(
                "FORMAL_ADMISSION_QFQ_HISTORY_INCOMPLETE",
                "正式准入QFQ历史分页未闭合："
                f"候选数={len(symbols)}，批次起始索引={start}，"
                f"分页数={offset // 1000 + 1}，最后offset={offset}，"
                f"total={total}，累计行={batch_rows}，"
                f"覆盖期望行={expected_rows}，缺口样例={coverage_gaps}。",
            )
    return histories


def _line_inputs(
    *,
    symbol: str,
    target_session: date,
    source_capture_id: int,
    sessions: tuple[date, ...],
    history: dict[date, dict[str, Any]],
) -> tuple[FieldValue, FieldValue, Any]:
    close = None
    phc20 = None
    current = history.get(target_session)
    current_coordinate_state, current_coordinate_reason = _qfq_window_row_state(
        current,
        symbol=symbol,
        session=target_session,
        source_capture_id=source_capture_id,
        target_row=current,
    )
    current_trade_state, current_trade_reason = trade_status_state_from_pg_row(
        current, price_coordinate="FORWARD_ADJUSTED"
    )
    if current_coordinate_state == "TRUE" and current_trade_state == "ACTUAL":
        close = _fraction(current.get("qfq_close"))
    if close is None:
        close_field = unknown(
            current_trade_reason
            if current_trade_state != "ACTUAL"
            else current_coordinate_reason
            or "T0_QFQ_CLOSE_NOT_OBSERVED_IN_PAIRED_CAPTURE"
        )
    else:
        close_field = FieldValue(close)

    prior_closes: list[Fraction] = []
    prior_reason = None
    for session in sessions[:-1]:
        row = history.get(session)
        row_state, row_reason = _qfq_window_row_state(
            row,
            symbol=symbol,
            session=session,
            source_capture_id=source_capture_id,
            target_row=current,
        )
        trade_state, trade_reason = trade_status_state_from_pg_row(
            row, price_coordinate="FORWARD_ADJUSTED"
        )
        if row_state != "TRUE":
            prior_reason = row_reason or "STRICT_PRIOR_QFQ_CLOSES_NOT_COMPLETE"
            prior_closes = []
            break
        if trade_state != "ACTUAL":
            prior_reason = trade_reason or "STRICT_PRIOR_QFQ_TRADE_STATUS_NOT_ACTUAL"
            prior_closes = []
            break
        value = _fraction(row.get("qfq_close"))
        if value is None or value <= 0:
            prior_reason = "STRICT_PRIOR_QFQ_CLOSE_INVALID"
            prior_closes = []
            break
        prior_closes.append(value)
    phc20 = max(prior_closes) if len(prior_closes) == 20 else None
    phc_field = (
        FieldValue(phc20)
        if phc20 is not None
        else unknown(prior_reason or "STRICT_PRIOR_20_CLOSES_NOT_COMPLETE")
    )
    by_session = {session: history.get(session) for session in sessions}
    damage = calculate_d0_first_day_damage(
        target_session=target_session,
        official_sessions=sessions,
        history_rows_by_session=by_session,
        source_symbol=symbol,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    return close_field, phc_field, damage


def _research_universe_gate_with_reason(
    symbol: str,
    universe_info: Mapping[str, Any],
    *,
    price_limit_fact: Mapping[str, Any] | None = None,
) -> tuple[FieldValue, str | None]:
    """按日期有效Research Universe与真实候选制度(NORMAL+LIMITED)判定F01三态门。"""
    formal_universe, normal_listing, limited_price_rule = (
        resolve_ordinary_candidate_gates(
            source_symbol=symbol,
            universe_result=universe_info,
            target_row=None,
            price_limit_fact=price_limit_fact,
        )
    )
    for node in (formal_universe, normal_listing, limited_price_rule):
        if node["state"] == "FALSE":
            return FieldValue(False), str(
                node.get("reason") or "OUTSIDE_RESEARCH_UNIVERSE"
            )
    for node in (formal_universe, normal_listing, limited_price_rule):
        if node["state"] == "UNKNOWN":
            return unknown(
                str(node.get("reason") or "OFFICIAL_RESEARCH_UNIVERSE_NOT_STORED")
            ), None
    return FieldValue(True), None


def _research_universe_gate(
    symbol: str,
    universe_info: Mapping[str, Any],
    *,
    price_limit_fact: Mapping[str, Any] | None = None,
) -> FieldValue:
    """按日期有效Research Universe与真实候选制度判定F01三态门。"""
    return _research_universe_gate_with_reason(
        symbol, universe_info, price_limit_fact=price_limit_fact
    )[0]


def _recovery_trigger_inputs(
    fact: Mapping[str, Any],
) -> tuple[FieldValue, FieldValue, FieldValue, FieldValue]:
    """从已存RECOVERY_TURN事实提取R5/R20/MA5/MA20；R20未知时绝不降取MA5。"""
    detail = fact.get("trigger_detail") or {}
    r5_str = detail.get("r5")
    r20_str = detail.get("r20")
    r5_field = (
        FieldValue(True)
        if r5_str == "TRUE"
        else FieldValue(False)
        if r5_str == "FALSE"
        else unknown("RECOVERY_R5_UNKNOWN")
    )
    r20_field = (
        FieldValue(True)
        if r20_str == "TRUE"
        else FieldValue(False)
        if r20_str == "FALSE"
        else unknown("RECOVERY_R20_UNKNOWN")
    )

    def _frac_field(payload: object, reason: str) -> FieldValue:
        if isinstance(payload, Mapping):
            num = payload.get("numerator")
            den = payload.get("denominator")
            if num is not None and den is not None and int(den) > 0:
                return FieldValue(Fraction(int(num), int(den)))
        return unknown(reason)

    ma20_field = _frac_field(detail.get("ma20"), "RECOVERY_MA20_UNKNOWN")
    if r20_str == "UNKNOWN":
        ma5_field = unknown("RECOVERY_R20_UNKNOWN_NO_MA5_FALLBACK")
    else:
        ma5_field = _frac_field(detail.get("ma5"), "RECOVERY_MA5_UNKNOWN")
    return ma5_field, ma20_field, r5_field, r20_field


def _lightweight_common_gates(
    *,
    symbol: str,
    universe_info: Mapping[str, Any],
    price_limit_fact: Mapping[str, Any] | None,
    seed: Mapping[str, Any] | None,
    seed_scope_reason: str,
) -> tuple[dict[str, FieldValue], str | None]:
    """计算不依赖21日历史窗的共同硬门(research_universe/actual_quote/core_damage/extension)。"""
    ru_gate, ru_false_reason = _research_universe_gate_with_reason(
        symbol, universe_info, price_limit_fact=price_limit_fact
    )
    if seed is None:
        actual_gate = unknown(seed_scope_reason)
        core_gate = unknown(seed_scope_reason)
        extension_gate = unknown(seed_scope_reason)
    else:
        actual_gate = _seed_actual_quote_gate(seed)
        core_gate = _negated_bool_gate(
            seed.get("core_price_damage"),
            seed.get("core_price_damage_quality"),
            seed.get("core_price_damage_reason"),
            unknown_reason="CORE_DAMAGE_NOT_COMPUTED",
        )
        extension_value = seed.get("profile_extension_value")
        extension_quality = seed.get("profile_extension_quality")
        if extension_quality != "OBSERVED" or extension_value not in {
            "LOW",
            "MEDIUM",
            "HIGH",
            "EXTREME",
        }:
            extension_gate = unknown(
                str(
                    seed.get("profile_extension_reason")
                    or "PROFILE_EXTENSION_NOT_COMPUTED"
                )
            )
        else:
            extension_gate = FieldValue(extension_value != "EXTREME")
    return (
        {
            "research_universe": ru_gate,
            "actual_quote": actual_gate,
            "non_core_damage": core_gate,
            "non_extreme_extension": extension_gate,
        },
        ru_false_reason,
    )


def run_source_formal_admission_from_saved(
    config,
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str | None = None,
    persist: bool,
    scenario: str = "LAUNCH_CONFIRM",
    prior_episode_id: str | None = None,
    prior_episode_valid: FieldValue | None = None,
    support_anchor_id: str | None = None,
    support_lower: FieldValue | None = None,
    support_valid: FieldValue | None = None,
) -> dict[str, Any]:
    """处理一个目标日；只消费该日已闭合Raw、D0、Core和Profile事实。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or persist is not True
        or scenario not in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_ARGUMENT_INVALID",
            "准入生产必须显式绑定截止内的paired BaoStock capture、合法场景并持久化",
        )

    source = read_source_version_context(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    if (
        source.get("status") != "AVAILABLE"
        or source.get("version_status") != "COMPLETE"
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_RAW_SOURCE_NOT_CLOSED",
            "目标日paired BaoStock双坐标来源分区未闭合，不生成准入分母",
        )
    source_capture_id = source["source_capture_id"]
    expected_count = source["research_expected_row_count"]
    universe_info = resolve_date_effective_universe(
        config,
        session_date=target_session,
        research_eligible_only=True,
    )
    d0 = read_source_d0_scenario_day(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        scenario=scenario,
        price_coordinate="FORWARD_ADJUSTED",
        research_cutoff=research_cutoff,
    )
    expected_history_session_count = 25 if scenario == "RECOVERY_TURN" else 21
    if (
        d0["source_pool"]["source_count"] != expected_count
        or d0["source_version"]["source_capture_id"] != source_capture_id
        or d0["session_contract"] != "D0_SESSION_R2"
        or d0["history_session_count"] != expected_history_session_count
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_D0_SCOPE_MISMATCH",
            "已存D0原始事实与目标Raw捕获、完整分母或严格会话窗口不一致",
        )

    calendar = _official_calendar(target_session)
    official_sessions = tuple(calendar["sessions"])
    if target_session not in official_sessions:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_TARGET_NOT_OFFICIAL_SESSION",
            "D0目标日期不在现役官方交易日历内",
        )
    target_ordinal = official_sessions.index(target_session)
    if target_ordinal < expected_history_session_count - 1:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_WINDOW_LEFT_TRUNCATED",
            "目标日没有足够的严格前序官方交易会话",
        )
    d0_sessions = official_sessions[
        target_ordinal - (expected_history_session_count - 1) : target_ordinal + 1
    ]
    if (
        len(d0_sessions) != expected_history_session_count
        or d0_sessions[-1] != target_session
        or d0_sessions[-1] > research_cutoff
        or d0_sessions[0] != _date(d0["history_window_start"])
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_WINDOW_MISMATCH",
            "官方日历与已存D0严格会话窗口不匹配",
        )
    sessions = d0_sessions[-21:]
    _, _, price_limit_calendar_revision = resolve_pg_history_window(
        target_session.isoformat(), expected_history_session_count
    )
    price_limit_by_symbol = read_price_limit_facts_by_symbol(
        config,
        target_session=target_session,
        calendar_revision=price_limit_calendar_revision,
    )
    next_session = (
        official_sessions[target_ordinal + 1]
        if target_ordinal + 1 < len(official_sessions)
        else None
    )

    seed_scope: dict[str, Any] | None = None
    seed_by_symbol: dict[str, dict[str, Any]] = {}
    seed_scope_reason = "SEED_FACT_NOT_COMPUTED_FOR_EXACT_INPUT"
    try:
        seed_scope = read_source_pool_seed_forward_scope(
            config,
            target_session=target_session,
            source_capture_id=source_capture_id,
            expected_source_count=expected_count,
            profile_revision=profile_revision,
        )
        if (
            seed_scope["source_symbols"]
            != tuple(sorted(item["source_symbol"] for item in d0["results"]))
            or seed_scope["calendar_id"] != d0["official_calendar"]["calendar_id"]
            or seed_scope["calendar_revision"]
            != d0["official_calendar"]["calendar_revision"]
        ):
            seed_scope = None
            seed_scope_reason = "SEED_CALENDAR_OR_SYMBOL_SCOPE_MISMATCH"
        else:
            seed_rows = read_source_pool_seed_facts(
                config,
                target_session=target_session,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                profile_revision=seed_scope.get("profile_revision"),
                producer_version=seed_scope["producer_version"],
            )
            seed_by_symbol = {row["source_symbol"]: row for row in seed_rows}
            if len(seed_by_symbol) != expected_count:
                seed_scope = None
                seed_by_symbol = {}
                seed_scope_reason = "SEED_SOURCE_PARTITION_NOT_CLOSED"
    except (ValueError, KeyError) as error:
        seed_scope = None
        seed_by_symbol = {}
        seed_scope_reason = getattr(error, "code", "SEED_SCOPE_UNAVAILABLE")

    profile_revision = seed_scope.get("profile_revision") if seed_scope else None
    seed_version = seed_scope["producer_version"] if seed_scope else None
    read_ids_kwargs: dict[str, Any] = {
        "target_session": target_session,
        "source_capture_id": source_capture_id,
        "producer_version": d0["producer_version"],
        "algorithm_contract": d0["algorithm_contract"],
        "session_contract": d0["session_contract"],
        "calendar_revision": d0["official_calendar"]["calendar_revision"],
        "profile_revision": profile_revision,
        "seed_producer_version": seed_version,
        "expected_source_count": expected_count,
    }
    if "scenario" in inspect.signature(read_saved_source_fact_ids).parameters:
        read_ids_kwargs["scenario"] = scenario
    input_refs = read_saved_source_fact_ids(config, **read_ids_kwargs)
    if set(input_refs) != {item["source_symbol"] for item in d0["results"]}:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_RAW_SYMBOL_SET_MISMATCH",
            "原始D0事实行编号与已闭合来源代码集合不一致",
        )

    lightweight_gates_by_symbol: dict[
        str, tuple[dict[str, FieldValue], str | None]
    ] = {}
    history_candidates: list[str] = []
    for fact in d0["results"]:
        sym = fact["source_symbol"]
        lw_gates, ru_false_reason = _lightweight_common_gates(
            symbol=sym,
            universe_info=universe_info,
            price_limit_fact=price_limit_by_symbol.get(sym),
            seed=seed_by_symbol.get(sym),
            seed_scope_reason=seed_scope_reason,
        )
        lightweight_gates_by_symbol[sym] = (lw_gates, ru_false_reason)
        if fact["scenario_raw"] == "TRUE" or (
            fact["scenario_raw"] == "FALSE"
            and all(
                not (g.quality == "OBSERVED" and g.value is False)
                for g in lw_gates.values()
            )
        ):
            history_candidates.append(sym)
    history_candidates.sort()
    histories = (
        _raw_history_for_candidates(
            config=config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            candidates=history_candidates,
            sessions=sessions,
        )
        if history_candidates
        else {}
    )

    window_ok = (
        d0["history_session_count"] == expected_history_session_count
        and d0["official_calendar"]["calendar_id"] == calendar["calendar_id"]
        and d0["official_calendar"]["calendar_revision"]
        == calendar["calendar_revision"]
        and d0["source_version"]["source_capture_id"] == source_capture_id
        and source["price_coordinate"] == "FORWARD_ADJUSTED"
    )
    rows: list[dict[str, Any]] = []
    raw_counts: Counter[str] = Counter()
    gate_unknown_counts: Counter[str] = Counter()
    gate_blocked_counts: Counter[str] = Counter()
    for fact in d0["results"]:
        symbol = fact["source_symbol"]
        raw_state = fact["scenario_raw"]
        raw_counts[raw_state] += 1
        lw_gates, ru_false_reason = lightweight_gates_by_symbol[symbol]
        if raw_state in {"TRUE", "FALSE"} and symbol in histories:
            history = histories[symbol]
            close, phc20, damage = _line_inputs(
                symbol=symbol,
                target_session=target_session,
                source_capture_id=source_capture_id,
                sessions=sessions,
                history=history,
            )
            if not window_ok:
                coordinate_gate = unknown(
                    "D0_CALENDAR_COORDINATE_OR_CAPTURE_NOT_PROVED"
                )
                liquidity_gate = unknown("D0_CALENDAR_OR_CAPTURE_NOT_PROVED_FOR_LIQ20")
            else:
                coordinate_gate = _coordinate_window_gate(
                    symbol=symbol,
                    source_capture_id=source_capture_id,
                    sessions=sessions,
                    history=history,
                )
                liquidity_gate = _d0_liquidity_gate(
                    symbol=symbol,
                    source_capture_id=source_capture_id,
                    sessions=sessions,
                    history=history,
                )
            if damage.state == "UNKNOWN":
                first_day_gate = unknown(damage.reason_code)
            else:
                first_day_gate = FieldValue(damage.state == "FALSE")
            severe_sessions = (
                official_sessions[max(0, target_ordinal - 21) : target_ordinal + 1]
                if target_ordinal >= 21
                else sessions
            )
            severe_res = calculate_d0_severe_drop(
                target_session=target_session,
                official_sessions=severe_sessions,
                history_rows_by_session=history,
                source_symbol=symbol,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
            )
            if severe_res.state == "UNKNOWN":
                non_severe_drop_gate = unknown(severe_res.reason_code)
            else:
                non_severe_drop_gate = FieldValue(severe_res.state == "FALSE")
            gates = {
                **lw_gates,
                "coordinate_window": coordinate_gate,
                "d0_liquidity": liquidity_gate,
                "non_first_day_damage": first_day_gate,
                "non_severe_drop": non_severe_drop_gate,
            }
        elif raw_state == "FALSE":
            close = unknown("RAW_NOT_TRUE_NO_T0_CLOSE_BINDING")
            phc20 = unknown("RAW_NOT_TRUE_NO_T0_PHC20_FREEZE")
            gates = {
                **lw_gates,
                "coordinate_window": unknown(
                    "RAW_AND_LIGHTWEIGHT_GATE_FALSE_HISTORY_SKIPPED"
                ),
                "d0_liquidity": unknown(
                    "RAW_AND_LIGHTWEIGHT_GATE_FALSE_HISTORY_SKIPPED"
                ),
                "non_first_day_damage": unknown(
                    "RAW_AND_LIGHTWEIGHT_GATE_FALSE_HISTORY_SKIPPED"
                ),
                "non_severe_drop": unknown(
                    "RAW_AND_LIGHTWEIGHT_GATE_FALSE_HISTORY_SKIPPED"
                ),
            }
        else:
            close = unknown("RAW_NOT_TRUE_NO_T0_CLOSE_BINDING")
            phc20 = unknown("RAW_NOT_TRUE_NO_T0_PHC20_FREEZE")
            gates = {
                **lw_gates,
                "coordinate_window": unknown("RAW_UNKNOWN_GATES_NOT_EVALUATED"),
                "d0_liquidity": unknown("RAW_UNKNOWN_GATES_NOT_EVALUATED"),
                "non_first_day_damage": unknown("RAW_UNKNOWN_GATES_NOT_EVALUATED"),
                "non_severe_drop": unknown("RAW_UNKNOWN_GATES_NOT_EVALUATED"),
            }
        gate_reason_labels = {
            "research_universe": {
                "TRUE": "OFFICIAL_RESEARCH_UNIVERSE_PROVED",
                "FALSE": ru_false_reason or "OUTSIDE_RESEARCH_UNIVERSE",
                "UNKNOWN": "OFFICIAL_RESEARCH_UNIVERSE_NOT_STORED",
            },
            "actual_quote": {
                "TRUE": "TARGET_ACTUAL_QUOTE_TRUE",
                "FALSE": "TARGET_ACTUAL_QUOTE_FALSE",
                "UNKNOWN": "TARGET_ACTUAL_QUOTE_NOT_COMPUTED",
            },
            "coordinate_window": {
                "TRUE": "D0_QFQ_WINDOW_CAPTURE_BOUND_NON_PIT",
                "FALSE": "D0_COORDINATE_WINDOW_INVALID",
                "UNKNOWN": "D0_QFQ_WINDOW_NOT_PROVED",
            },
            "d0_liquidity": {
                "TRUE": "D0_LIQ20_THRESHOLD_PROVED_FROM_SAVED_RAW_NON_PIT",
                "FALSE": "D0_LIQ20_BELOW_THRESHOLD",
                "UNKNOWN": "D0_LIQ20_INPUT_OR_DOCUMENTED_UNIT_CONTRACT_UNKNOWN",
            },
            "non_core_damage": {
                "TRUE": "CORE_DAMAGE_FALSE",
                "FALSE": "CORE_DAMAGE_TRUE",
                "UNKNOWN": "CORE_DAMAGE_NOT_COMPUTED",
            },
            "non_extreme_extension": {
                "TRUE": "PROFILE_EXTENSION_NOT_EXTREME",
                "FALSE": "PROFILE_EXTENSION_EXTREME_PROVED",
                "UNKNOWN": "PROFILE_EXTENSION_NOT_COMPUTED",
            },
            "non_first_day_damage": {
                "TRUE": "FIRST_DAY_DAMAGE_FALSE",
                "FALSE": "FIRST_DAY_DAMAGE_TRUE",
                "UNKNOWN": "FIRST_DAY_DAMAGE_NOT_COMPUTED",
            },
            "non_severe_drop": {
                "TRUE": "SEVERE_DROP_EXCLUDED_BY_POSITIVE_RET1",
                "FALSE": "SEVERE_DROP_PROVED",
                "UNKNOWN": "SEVERE_DROP_NOT_COMPUTED",
            },
        }
        for name, gate in gates.items():
            if raw_state == "TRUE":
                if gate.quality != "OBSERVED":
                    gate_unknown_counts[gate.reason or name] += 1
                elif gate.value is False:
                    gate_blocked_counts[gate_reason_labels[name]["FALSE"]] += 1
        refs = input_refs[symbol]
        evidence_ref = (
            f"PG:{target_session.isoformat()}:{source_capture_id}:"
            f"{source_capture_id}:{symbol}:D0_FACT:{refs['raw_fact_id']}"
        )
        ma5_field = ma20_field = r5_field = r20_field = None
        if scenario == "RECOVERY_TURN":
            ma5_field, ma20_field, r5_field, r20_field = _recovery_trigger_inputs(fact)
        prepared = prepare_source_d0_launch_formal_admission(
            entity_id=symbol,
            target_session=target_session,
            source_capture_id=source_capture_id,
            raw_fact_id=refs["raw_fact_id"],
            raw_state=raw_state,
            raw_reason=fact["main_reason"],
            raw_role="FORMAL",
            gates=gates,
            gate_reasons=gate_reason_labels,
            close=close,
            phc20=phc20,
            session_ordinal=target_ordinal,
            next_session=next_session,
            window_start=d0_sessions[0],
            evidence_ref=evidence_ref,
            scenario=scenario,
            ma5=ma5_field,
            ma20=ma20_field,
            r5=r5_field,
            r20=r20_field,
            prior_episode_id=prior_episode_id,
            prior_episode_valid=prior_episode_valid,
            support_anchor_id=support_anchor_id,
            support_lower=support_lower,
            support_valid=support_valid,
        )
        line = prepared["frozen_line"]
        row = {
            "raw_fact_id": refs["raw_fact_id"],
            "seed_fact_id": refs["seed_fact_id"],
            "source_capture_id": source_capture_id,
            "target_session": target_session,
            "source_symbol": symbol,
            "scenario": scenario,
            "raw_role": prepared["raw_role"],
            "raw_state": prepared["raw_state"],
            "raw_reason": prepared["raw_reason"],
            "raw_producer_version": d0["producer_version"],
            "algorithm_contract": d0["algorithm_contract"],
            "session_contract": d0["session_contract"],
            "history_window_start": d0_sessions[0],
            "history_session_count": expected_history_session_count,
            "source_count": expected_count,
            "calendar_id": d0["official_calendar"]["calendar_id"],
            "calendar_revision": d0["official_calendar"]["calendar_revision"],
            "source_time_quality": d0["source_version"]["source_time_quality"],
            "source_clock_role": d0["source_version"]["source_clock_role"],
            "source_available_at": _aware_datetime(
                d0["source_version"]["source_available_at"]
            ),
            "capture_started_at": _aware_datetime(source["capture_started_at"]),
            "capture_validated_at": _aware_datetime(source["capture_validated_at"]),
            "profile_revision": refs["profile_revision"],
            "seed_producer_version": refs["seed_producer_version"],
            "identity_scope": "SOURCE_SYMBOL_ONLY",
            "research_mode": "NON_PIT_EXPLORATORY",
            "price_coordinate": "FORWARD_ADJUSTED",
            "admission_contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
            "admission_status": prepared["admission_status"],
            "admission_reason": prepared["admission_reason"],
            "research_universe_state": prepared["gates"]["research_universe"]["state"],
            "research_universe_reason": prepared["gates"]["research_universe"][
                "reason"
            ],
            "actual_quote_state": prepared["gates"]["actual_quote"]["state"],
            "actual_quote_reason": prepared["gates"]["actual_quote"]["reason"],
            "coordinate_window_state": prepared["gates"]["coordinate_window"]["state"],
            "coordinate_window_reason": prepared["gates"]["coordinate_window"][
                "reason"
            ],
            "d0_liquidity_state": prepared["gates"]["d0_liquidity"]["state"],
            "d0_liquidity_reason": prepared["gates"]["d0_liquidity"]["reason"],
            "non_core_damage_state": prepared["gates"]["non_core_damage"]["state"],
            "non_core_damage_reason": prepared["gates"]["non_core_damage"]["reason"],
            "non_extreme_extension_state": prepared["gates"]["non_extreme_extension"][
                "state"
            ],
            "non_extreme_extension_reason": prepared["gates"]["non_extreme_extension"][
                "reason"
            ],
            "non_first_day_damage_state": prepared["gates"]["non_first_day_damage"][
                "state"
            ],
            "non_first_day_damage_reason": prepared["gates"]["non_first_day_damage"][
                "reason"
            ],
            "non_severe_drop_state": prepared["gates"]["non_severe_drop"]["state"],
            "non_severe_drop_reason": prepared["gates"]["non_severe_drop"]["reason"],
            "reason_codes": prepared["reason_codes"],
            "event_id": prepared["event_id"],
            "failure_level_numerator": line["numerator"] if line else None,
            "failure_level_denominator": line["denominator"] if line else None,
            "failure_coordinate": line["coordinate"] if line else None,
            "failure_source": line["source"] if line else None,
            "failure_window_start": (
                sessions[-int(line["source_window_session_count"])]
                if line and int(line.get("source_window_session_count") or 0) in (5, 20)
                else sessions[0]
                if line
                else None
            ),
            "failure_window_session_count": line["source_window_session_count"]
            if line
            else None,
            "failure_source_event_ref": line["source_event_ref"] if line else None,
            "effective_from_session": _date(line["effective_from_session"])
            if line
            else None,
        }
        rows.append(row)

    result = persist_source_formal_admission_rows(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        expected_source_count=expected_count,
        rows=rows,
    )
    readback_kwargs: dict[str, Any] = {
        "target_session": target_session,
        "source_capture_id": source_capture_id,
        "expected_source_count": expected_count,
        "algorithm_contract": rows[0]["algorithm_contract"],
        "session_contract": rows[0]["session_contract"],
        "calendar_revision": rows[0]["calendar_revision"],
        "seed_producer_version": rows[0]["seed_producer_version"],
        "profile_history_session_count": (
            seed_scope["history_session_count"] if seed_scope else None
        ),
        "profile_revision": profile_revision,
    }
    if "scenario" in inspect.signature(read_source_formal_admission_day).parameters:
        readback_kwargs["scenario"] = scenario
    readback = read_source_formal_admission_day(config, **readback_kwargs)
    if readback.get("status") != "AVAILABLE" or readback.get("counts") != {
        "READY": result["ready_count"],
        "BLOCKED": result["blocked_count"],
        "NOT_READY": result["not_ready_count"],
        "NOT_REQUIRED": result["not_required_count"],
    }:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_INDEPENDENT_READBACK_MISMATCH",
            "正式准入事实提交后的独立只读分区或状态计数不匹配",
        )
    return {
        **result,
        "input_status": "SAVED_D0_AND_RAW_SCOPE_CLOSED",
        "source_capture_id": source_capture_id,
        "raw_producer_version": d0["producer_version"],
        "producer_version": d0["producer_version"],
        "calendar_revision": d0["official_calendar"]["calendar_revision"],
        "expected_source_count": expected_count,
        "verified_rows": expected_count,
        "raw_counts": dict(raw_counts),
        "gate_blocked_reason_counts": dict(sorted(gate_blocked_counts.items())),
        "gate_unknown_reason_counts": dict(sorted(gate_unknown_counts.items())),
        "identity_scope": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "source_time_quality": source["source_time_quality"],
        "source_clock_role": source["source_clock_role"],
        "source_available_at": source["source_available_at"],
        "profile_revision": profile_revision,
        "seed_scope_status": "AVAILABLE" if seed_scope else "UNKNOWN",
        "seed_scope_reason": None if seed_scope else seed_scope_reason,
        "independent_readback": {
            "status": readback["status"],
            "counts": readback["counts"],
            "transaction": readback["transaction"],
            "database_write_attempted": False,
        },
        "raw_history_rows_read": sum(len(rows) for rows in histories.values()),
        "raw_history_batches": (len(history_candidates) + 99) // 100,
        "future_quote_rows_read": 0,
        "research_cutoff": research_cutoff.isoformat(),
    }


def run_source_formal_admission_range_from_saved(
    config,
    *,
    start_session: date,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revisions_by_session: Mapping[str, str] | None = None,
    scenario: str | None = None,
    scenarios: Sequence[str] | None = None,
) -> dict[str, Any]:
    """生产指定范围内PG已经完成的正式D0场景分区(默认覆盖LAUNCH_CONFIRM与RECOVERY_TURN)。"""
    valid_scenarios = ("LAUNCH_CONFIRM", "RECOVERY_TURN")
    if scenarios is not None:
        selected_scenarios = tuple(scenarios)
    elif scenario is not None:
        selected_scenarios = (scenario,)
    else:
        selected_scenarios = valid_scenarios
    if not selected_scenarios or any(
        sc not in valid_scenarios for sc in selected_scenarios
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_ARGUMENT_INVALID",
            "范围正式准入仅支持LAUNCH_CONFIRM与RECOVERY_TURN场景",
        )

    reports: list[dict[str, Any]] = []
    sessions_by_scenario: dict[str, list[str]] = {}
    for sc in selected_scenarios:
        sc_sessions = list_saved_d0_sessions(
            config,
            scenario=sc,
            start_session=start_session,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
        )
        if not sc_sessions and sc == "LAUNCH_CONFIRM":
            sc_sessions = list_saved_launch_sessions(
                config,
                start_session=start_session,
                target_session=target_session,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
            )
        sessions_by_scenario[sc] = [day.isoformat() for day in sc_sessions]
        for day in sc_sessions:
            reports.append(
                run_source_formal_admission_from_saved(
                    config,
                    target_session=day,
                    research_cutoff=research_cutoff,
                    source_capture_id=source_capture_id,
                    profile_revision=(
                        profile_revisions_by_session.get(day.isoformat())
                        if isinstance(profile_revisions_by_session, Mapping)
                        else None
                    ),
                    persist=True,
                    scenario=sc,
                )
            )
    if not reports:
        return {
            "status": "NOT_COMPUTED",
            "reason_code": "SAVED_D0_NOT_FOUND_FOR_RANGE",
            "start_session": start_session.isoformat(),
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "scenarios": list(selected_scenarios),
            "completed_day_count": 0,
            "database_write_attempted": False,
        }
    return {
        "status": "PERSISTED",
        "contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
        "start_session": start_session.isoformat(),
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "scenarios": list(selected_scenarios),
        "sessions_by_scenario": sessions_by_scenario,
        "completed_day_count": len(reports),
        "reports": reports,
        "database_write_attempted": True,
        "ddl_executed": False,
        "dml_executed": True,
    }


__all__ = [
    "SourceFormalAdmissionProductionError",
    "run_source_formal_admission_from_saved",
    "run_source_formal_admission_range_from_saved",
]
