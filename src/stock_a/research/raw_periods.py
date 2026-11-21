"""来源代码级BaoStock坐标自然周期价格观察；历史回看不构成PIT结论。"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence

from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.profile import (
    ClosedPrices,
    ProfileHandoff,
    ProfileInput,
    RuntimeProducerBinding,
    WindowIdentity,
    evaluate_profile_kernel,
)
from stock_a.research.entry_history.support import trade_status_state_from_pg_row


RAW_PERIOD_ALGORITHM_ID = "R02_SOURCE_SYMBOL_BAOSTOCK_COORDINATE_PERIOD_OHLC_R2"
_BAR_STATUSES = {"TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED"}
_PRICE_FIELDS_BY_COORDINATE = {
    "RAW_UNADJUSTED": ("raw_open", "raw_high", "raw_low", "raw_close"),
    "FORWARD_ADJUSTED": ("qfq_open", "qfq_high", "qfq_low", "qfq_close"),
}
_QUALITY_FIELDS_BY_COORDINATE = {
    "RAW_UNADJUSTED": ("price_quality", "price_reason"),
    "FORWARD_ADJUSTED": ("qfq_price_quality", "qfq_price_reason"),
}
_COORDINATE_FLAGS = {"RAW_UNADJUSTED": "3", "FORWARD_ADJUSTED": "2"}
_TREND_RULES = {
    "WEEK": ("PROFILE_CLOSED_WEEK_TREND_R1", "profile.closed_week_trend"),
    "MONTH": ("PROFILE_CLOSED_MONTH_TREND_R1", "profile.closed_month_trend"),
}
_TREND_PRODUCER = "stock_a.research.raw_periods:build_source_symbol_raw_periods"


@dataclass(frozen=True)
class RawDailyBar:
    """单个官方会话在paired BaoStock捕获和指定坐标中的价格观察。"""

    session: date
    price_coordinate: str
    bar_status: str
    bar_reason: str | None
    price_quality: str
    price_reason: str | None
    open: Fraction | None
    high: Fraction | None
    low: Fraction | None
    close: Fraction | None
    valid_price_bar: bool
    source_capture_id: int | None
    raw_volume: Fraction | None
    raw_volume_quality: str
    raw_volume_reason: str | None
    volume_unit_status: str | None
    raw_amount: Fraction | None
    raw_amount_quality: str
    raw_amount_reason: str | None
    amount_unit_status: str | None
    trade_status_state: str = "UNKNOWN"
    trade_status_reason: str | None = None


@dataclass(frozen=True)
class RawPeriodBar:
    """一个自然周/月的BaoStock来源代码价格摘要与覆盖状态。"""

    kind: str
    price_coordinate: str
    source_capture_id: int | None
    time_role: str
    key: str
    calendar_ordinal: int
    calendar_start_session: date
    calendar_end_session: date
    calendar_expected_session_count: int
    elapsed_expected_session_count: int
    stored_raw_row_count: int
    valid_price_bar_count: int
    target_missing_count: int
    read_failed_count: int
    invalid_price_count: int
    missing_history_count: int
    window_truncated_session_count: int
    calendar_closed: bool
    coverage_status: str
    reason_codes: tuple[str, ...]
    open: Fraction | None
    high: Fraction | None
    low: Fraction | None
    close: Fraction | None
    open_session: date | None
    high_session: date | None
    low_session: date | None
    close_session: date | None
    raw_volume: Fraction | None
    raw_volume_quality: str
    raw_volume_reason: str | None
    volume_unit_status: str | None
    raw_amount: Fraction | None
    raw_amount_quality: str
    raw_amount_reason: str | None
    amount_unit_status: str | None
    daily_rows: tuple[RawDailyBar, ...]
    suspended_session_count: int = 0
    unknown_trade_status_count: int = 0


@dataclass(frozen=True)
class RawPeriodTrend:
    """按现役闭合周/月规则求值的NON_PIT价格坐标诊断结果。"""

    kind: str
    price_coordinate: str
    source_capture_id: int | None
    time_role: str
    rule_id: str
    status: str
    value: str | None
    reason: str | None
    required_period_count: int
    complete_period_count: int
    input_period_keys: tuple[str, ...]


def _session(value: Any) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as error:
            raise ContractError("Raw周期输入含非法会话日期") from error
        if parsed.isoformat() == value:
            return parsed
    raise ContractError("Raw周期输入含非法会话日期")


def _fraction(value: Any) -> Fraction | None:
    if value is None:
        return None
    try:
        return exact_number(value)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _valid_ohlc(values: tuple[Fraction | None, ...]) -> bool:
    if any(value is None for value in values):
        return False
    open_value, high_value, low_value, close_value = values
    assert open_value is not None
    assert high_value is not None
    assert low_value is not None
    assert close_value is not None
    return (
        open_value > 0
        and high_value > 0
        and low_value > 0
        and close_value > 0
        and high_value >= max(open_value, low_value, close_value)
        and low_value <= min(open_value, high_value, close_value)
    )


def _daily_bar(
    session: date,
    row: Mapping[str, Any] | None,
    *,
    price_coordinate: str,
) -> RawDailyBar:
    if row is None:
        return RawDailyBar(
            session=session,
            price_coordinate=price_coordinate,
            bar_status="HISTORY_ROW_NOT_STORED",
            bar_reason="PG_HISTORY_SESSION_MISSING",
            price_quality="UNKNOWN",
            price_reason="PG_HISTORY_SESSION_MISSING",
            open=None,
            high=None,
            low=None,
            close=None,
            valid_price_bar=False,
            source_capture_id=None,
            raw_volume=None,
            raw_volume_quality="UNKNOWN",
            raw_volume_reason="PG_HISTORY_SESSION_MISSING",
            volume_unit_status=None,
            raw_amount=None,
            raw_amount_quality="UNKNOWN",
            raw_amount_reason="PG_HISTORY_SESSION_MISSING",
            amount_unit_status=None,
        )
    if _session(row.get("session_date")) != session:
        raise ContractError("Raw周期输入行日期错配")
    bar_status_column = (
        "bar_status" if price_coordinate == "RAW_UNADJUSTED" else "qfq_bar_status"
    )
    bar_reason_column = (
        "bar_reason" if price_coordinate == "RAW_UNADJUSTED" else "qfq_bar_reason"
    )
    bar_status = row.get(bar_status_column)
    if bar_status not in _BAR_STATUSES:
        raise ContractError("Raw周期输入含未登记来源状态")
    if price_coordinate not in _PRICE_FIELDS_BY_COORDINATE:
        raise ContractError("Raw周期价格坐标只支持RAW_UNADJUSTED或FORWARD_ADJUSTED")
    row_coordinate = row.get("price_coordinate")
    if row_coordinate is not None and row_coordinate != price_coordinate:
        raise ContractError("Raw周期输入价格坐标与请求不一致")
    price_fields = _PRICE_FIELDS_BY_COORDINATE[price_coordinate]
    quality_field, reason_field = _QUALITY_FIELDS_BY_COORDINATE[price_coordinate]
    trade_status_state, trade_status_reason = trade_status_state_from_pg_row(
        row, price_coordinate=price_coordinate
    )
    values = tuple(_fraction(row.get(name)) for name in price_fields)
    quality = row.get(quality_field)
    source_capture_id = row.get("source_capture_id")
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise ContractError("Raw周期来源捕获ID必须是正整数或空值")
    valid = (
        bar_status == "TARGET_PRESENT"
        and trade_status_state == "ACTUAL"
        and quality == "OBSERVED"
        and _valid_ohlc(values)
    )
    raw_volume, raw_volume_quality, raw_volume_reason = _raw_activity_field(
        row,
        value_field="raw_volume",
        quality_field="volume_quality",
        reason_field="volume_reason",
        unit_field="volume_unit_status",
        expected_unit="SHARES_VERIFIED",
    )
    raw_amount, raw_amount_quality, raw_amount_reason = _raw_activity_field(
        row,
        value_field="raw_amount",
        quality_field="amount_quality",
        reason_field="amount_reason",
        unit_field="amount_unit_status",
        expected_unit="CNY_VERIFIED",
    )
    return RawDailyBar(
        session=session,
        price_coordinate=price_coordinate,
        bar_status=bar_status,
        bar_reason=row.get(bar_reason_column),
        price_quality=str(quality or "UNKNOWN"),
        price_reason=row.get(reason_field),
        open=values[0],
        high=values[1],
        low=values[2],
        close=values[3],
        valid_price_bar=valid,
        source_capture_id=source_capture_id,
        raw_volume=raw_volume,
        raw_volume_quality=raw_volume_quality,
        raw_volume_reason=raw_volume_reason,
        volume_unit_status=row.get("volume_unit_status"),
        raw_amount=raw_amount,
        raw_amount_quality=raw_amount_quality,
        raw_amount_reason=raw_amount_reason,
        amount_unit_status=row.get("amount_unit_status"),
        trade_status_state=trade_status_state,
        trade_status_reason=trade_status_reason,
    )


def _raw_activity_field(
    row: Mapping[str, Any],
    *,
    value_field: str,
    quality_field: str,
    reason_field: str,
    unit_field: str,
    expected_unit: str,
) -> tuple[Fraction | None, str, str | None]:
    quality = row.get(quality_field)
    reason = row.get(reason_field)
    value = _fraction(row.get(value_field))
    if quality == "OBSERVED":
        if value is None or value < 0:
            return None, "BAD_VALUE", reason or f"{value_field.upper()}_INVALID"
        if row.get(unit_field) != expected_unit:
            return None, "UNKNOWN", f"{value_field.upper()}_UNIT_UNVERIFIED"
        return value, "OBSERVED", None
    if quality in {"UNKNOWN", "NOT_APPLICABLE", "BAD_VALUE"}:
        return None, str(quality), reason or f"{value_field.upper()}_{quality}"
    return None, "UNKNOWN", reason or f"{value_field.upper()}_QUALITY_UNKNOWN"


def _aggregate_raw_activity(
    rows: Sequence[RawDailyBar],
    *,
    field: str,
    unit_status_field: str,
    expected_unit: str,
    calendar_closed: bool,
    window_truncated_session_count: int,
) -> tuple[Fraction | None, str, str | None, str | None]:
    unit_statuses = {getattr(row, unit_status_field) for row in rows}
    unit_status = expected_unit if unit_statuses == {expected_unit} else "UNKNOWN"
    if not calendar_closed:
        return None, "NOT_COMPUTED", "CALENDAR_PERIOD_NOT_CLOSED", unit_status
    if window_truncated_session_count:
        return None, "UNKNOWN", "WINDOW_START_TRUNCATED", unit_status
    values = [getattr(row, field) for row in rows]
    qualities = [getattr(row, f"{field}_quality") for row in rows]
    if (
        rows
        and unit_status == expected_unit
        and all(quality == "OBSERVED" for quality in qualities)
        and all(value is not None and value >= 0 for value in values)
    ):
        return sum(values, Fraction(0)), "OBSERVED", None, expected_unit
    reasons = list(
        dict.fromkeys(
            getattr(row, f"{field}_reason")
            for row in rows
            if getattr(row, f"{field}_reason")
        )
    )
    reason = "|".join(reasons) or f"{field.upper()}_PERIOD_INCOMPLETE"
    return None, "UNKNOWN", reason, unit_status


def _period_identity(kind: str, session: date) -> tuple[str, int]:
    if kind == "WEEK":
        iso = session.isocalendar()
        week_start = date.fromisocalendar(iso.year, iso.week, 1)
        return f"{iso.year}-W{iso.week:02d}", week_start.toordinal() // 7
    if kind == "MONTH":
        return f"{session.year}-{session.month:02d}", session.year * 12 + session.month
    raise ContractError("Raw周期类型只支持自然周或自然月")


def _aggregate_period(
    *,
    kind: str,
    price_coordinate: str,
    source_capture_id: int | None,
    key: str,
    ordinal: int,
    expected_sessions: Sequence[date],
    window_start: date,
    target_session: date,
    rows_by_session: Mapping[date, RawDailyBar],
) -> RawPeriodBar:
    elapsed_sessions = tuple(
        day for day in expected_sessions if window_start <= day <= target_session
    )
    daily_rows = tuple(rows_by_session[day] for day in elapsed_sessions)
    valid_rows = tuple(row for row in daily_rows if row.valid_price_bar)
    suspended_session_count = sum(
        row.trade_status_state == "CONFIRMED_SUSPENSION" for row in daily_rows
    )
    unknown_trade_status_count = sum(
        row.bar_status in _BAR_STATUSES and row.trade_status_state == "UNKNOWN"
        for row in daily_rows
    )
    missing_history_count = sum(
        row.bar_status == "HISTORY_ROW_NOT_STORED" for row in daily_rows
    )
    target_missing_count = sum(row.bar_status == "TARGET_MISSING" for row in daily_rows)
    read_failed_count = sum(row.bar_status == "READ_FAILED" for row in daily_rows)
    invalid_price_count = sum(
        row.bar_status == "TARGET_PRESENT"
        and row.trade_status_state == "ACTUAL"
        and not row.valid_price_bar
        for row in daily_rows
    )
    calendar_closed = expected_sessions[-1] <= target_session
    truncated_count = sum(day < window_start for day in expected_sessions)
    blocking_reasons: list[str] = []
    reasons: list[str] = []
    if truncated_count:
        reasons.append("WINDOW_START_TRUNCATED")
    if missing_history_count:
        reasons.append("HISTORY_ROW_NOT_STORED")
    if target_missing_count:
        reasons.append("TARGET_BAR_MISSING_STATUS_UNKNOWN")
    if read_failed_count:
        reasons.append("SOURCE_READ_FAILED")
    if invalid_price_count:
        reasons.append("PRICE_BAR_INVALID_OR_UNKNOWN")
    if suspended_session_count:
        reasons.append("CONFIRMED_SUSPENSION_SKIPPED_FROM_PERIOD_OHLC")
    if unknown_trade_status_count:
        reasons.append("TRADE_STATUS_UNKNOWN")
    blocking_reasons.extend(
        reason
        for reason in reasons
        if reason != "CONFIRMED_SUSPENSION_SKIPPED_FROM_PERIOD_OHLC"
    )
    if not calendar_closed:
        coverage_status = "UNFINISHED"
        reasons.append("CALENDAR_PERIOD_NOT_CLOSED_AS_OF_TARGET")
    elif (
        not blocking_reasons
        and len(daily_rows) == len(expected_sessions)
        and len(valid_rows) + suspended_session_count == len(expected_sessions)
    ):
        coverage_status = "COMPLETE"
    else:
        coverage_status = "PARTIAL"
    if not valid_rows:
        reasons.append("NO_VALID_PRICE_COORDINATE_BAR")

    raw_volume, raw_volume_quality, raw_volume_reason, volume_unit_status = (
        _aggregate_raw_activity(
            daily_rows,
            field="raw_volume",
            unit_status_field="volume_unit_status",
            expected_unit="SHARES_VERIFIED",
            calendar_closed=calendar_closed,
            window_truncated_session_count=truncated_count,
        )
    )
    raw_amount, raw_amount_quality, raw_amount_reason, amount_unit_status = (
        _aggregate_raw_activity(
            daily_rows,
            field="raw_amount",
            unit_status_field="amount_unit_status",
            expected_unit="CNY_VERIFIED",
            calendar_closed=calendar_closed,
            window_truncated_session_count=truncated_count,
        )
    )

    first = valid_rows[0] if valid_rows else None
    last = valid_rows[-1] if valid_rows else None
    high_row = max(valid_rows, key=lambda row: row.high) if valid_rows else None
    low_row = min(valid_rows, key=lambda row: row.low) if valid_rows else None
    expose_prices = calendar_closed
    return RawPeriodBar(
        kind=kind,
        price_coordinate=price_coordinate,
        source_capture_id=source_capture_id,
        time_role="NON_PIT_RETROSPECTIVE",
        key=key,
        calendar_ordinal=ordinal,
        calendar_start_session=expected_sessions[0],
        calendar_end_session=expected_sessions[-1],
        calendar_expected_session_count=len(expected_sessions),
        elapsed_expected_session_count=len(elapsed_sessions),
        stored_raw_row_count=sum(
            row.bar_status != "HISTORY_ROW_NOT_STORED" for row in daily_rows
        ),
        valid_price_bar_count=len(valid_rows),
        target_missing_count=target_missing_count,
        read_failed_count=read_failed_count,
        invalid_price_count=invalid_price_count,
        missing_history_count=missing_history_count,
        window_truncated_session_count=truncated_count,
        calendar_closed=calendar_closed,
        coverage_status=coverage_status,
        reason_codes=tuple(dict.fromkeys(reasons)),
        open=first.open if expose_prices and first else None,
        high=high_row.high if expose_prices and high_row else None,
        low=low_row.low if expose_prices and low_row else None,
        close=last.close if expose_prices and last else None,
        open_session=first.session if expose_prices and first else None,
        high_session=high_row.session if expose_prices and high_row else None,
        low_session=low_row.session if expose_prices and low_row else None,
        close_session=last.session if expose_prices and last else None,
        raw_volume=raw_volume,
        raw_volume_quality=raw_volume_quality,
        raw_volume_reason=raw_volume_reason,
        volume_unit_status=volume_unit_status,
        raw_amount=raw_amount,
        raw_amount_quality=raw_amount_quality,
        raw_amount_reason=raw_amount_reason,
        amount_unit_status=amount_unit_status,
        daily_rows=daily_rows if calendar_closed else (),
        suspended_session_count=suspended_session_count,
        unknown_trade_status_count=unknown_trade_status_count,
    )


def build_source_symbol_raw_periods(
    *,
    target_session: date,
    session_dates: Sequence[date],
    calendar_sessions: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    price_coordinate: str,
) -> dict[str, tuple[RawPeriodBar, ...]]:
    """按显式价格坐标聚合BaoStock日线；缺Bar不合成、不推断停牌。"""
    if (
        type(target_session) is not date
        or price_coordinate not in _PRICE_FIELDS_BY_COORDINATE
        or not isinstance(session_dates, (tuple, list))
        or not session_dates
        or any(type(day) is not date for day in session_dates)
        or not isinstance(calendar_sessions, (tuple, list))
        or any(type(day) is not date for day in calendar_sessions)
        or any(left >= right for left, right in zip(session_dates, session_dates[1:]))
        or any(
            left >= right
            for left, right in zip(calendar_sessions, calendar_sessions[1:])
        )
        or session_dates[-1] != target_session
        or target_session not in calendar_sessions
    ):
        raise ContractError("Raw周期窗口必须绑定递增官方会话与目标日")
    eligible_sessions = tuple(day for day in calendar_sessions if day <= target_session)
    if len(eligible_sessions) < len(session_dates) or tuple(
        eligible_sessions[-len(session_dates) :]
    ) != tuple(session_dates):
        raise ContractError("Raw周期窗口必须是截止目标日的连续官方会话后缀")
    rows_by_session: dict[date, Mapping[str, Any]] = {}
    for row in history_rows:
        if not isinstance(row, Mapping):
            raise ContractError("Raw周期历史行必须是映射")
        day = _session(row.get("session_date"))
        if day not in session_dates or day in rows_by_session:
            raise ContractError("Raw周期历史行超出窗口或会话重复")
        rows_by_session[day] = row
    capture_ids = {
        row.get("source_capture_id")
        for row in rows_by_session.values()
        if row.get("source_capture_id") is not None
    }
    if any(row.get("source_capture_id") is None for row in rows_by_session.values()):
        raise ContractError("Raw周期历史行缺少paired BaoStock source_capture_id")
    if any(
        type(capture_id) is not int or capture_id <= 0 for capture_id in capture_ids
    ):
        raise ContractError("Raw周期输入来源捕获ID必须是正整数")
    if len(capture_ids) > 1:
        raise ContractError("Raw周期窗口混合了多个paired BaoStock source_capture_id")
    source_capture_id = next(iter(capture_ids)) if capture_ids else None
    daily = {
        day: _daily_bar(
            day,
            rows_by_session.get(day),
            price_coordinate=price_coordinate,
        )
        for day in session_dates
    }
    results: dict[str, tuple[RawPeriodBar, ...]] = {}
    for kind in ("WEEK", "MONTH"):
        grouped: dict[str, list[date]] = {}
        ordinals: dict[str, int] = {}
        for day in calendar_sessions:
            key, ordinal = _period_identity(kind, day)
            grouped.setdefault(key, []).append(day)
            ordinals[key] = ordinal
        periods = []
        for key, expected in grouped.items():
            if expected[-1] < session_dates[0] or expected[0] > target_session:
                continue
            periods.append(
                _aggregate_period(
                    kind=kind,
                    price_coordinate=price_coordinate,
                    source_capture_id=source_capture_id,
                    key=key,
                    ordinal=ordinals[key],
                    expected_sessions=expected,
                    window_start=session_dates[0],
                    target_session=target_session,
                    rows_by_session=daily,
                )
            )
        results[kind] = tuple(periods)
    return results


def summarize_raw_history_rows(
    *,
    session_dates: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    price_coordinate: str = "FORWARD_ADJUSTED",
) -> dict[str, Any]:
    """按选定价格坐标及逐会话状态统计，不使用固定Bar分母。"""
    if (
        price_coordinate not in _PRICE_FIELDS_BY_COORDINATE
        or not isinstance(session_dates, (tuple, list))
        or not session_dates
        or any(type(day) is not date for day in session_dates)
        or any(left >= right for left, right in zip(session_dates, session_dates[1:]))
    ):
        raise ContractError("Raw历史统计需要递增官方会话")
    rows_by_session: dict[date, Mapping[str, Any]] = {}
    for row in history_rows:
        if not isinstance(row, Mapping):
            raise ContractError("Raw周期历史行必须是映射")
        day = _session(row.get("session_date"))
        if day not in session_dates or day in rows_by_session:
            raise ContractError("Raw历史统计行超出窗口或会话重复")
        rows_by_session[day] = row
    daily = tuple(
        _daily_bar(
            day,
            rows_by_session.get(day),
            price_coordinate=price_coordinate,
        )
        for day in session_dates
    )
    capture_ids = {
        row.source_capture_id for row in daily if row.source_capture_id is not None
    }
    if any(
        row.bar_status != "HISTORY_ROW_NOT_STORED" and row.source_capture_id is None
        for row in daily
    ):
        raise ContractError("Raw历史统计行缺少paired BaoStock source_capture_id")
    if len(capture_ids) > 1:
        raise ContractError(
            "Raw周期统计窗口混合了多个paired BaoStock source_capture_id"
        )
    return {
        "price_coordinate": price_coordinate,
        "source_capture_id": next(iter(capture_ids)) if capture_ids else None,
        "time_role": "NON_PIT_RETROSPECTIVE",
        "official_session_count": len(session_dates),
        "stored_raw_row_count": sum(
            row.bar_status != "HISTORY_ROW_NOT_STORED" for row in daily
        ),
        "valid_price_bar_count": sum(row.valid_price_bar for row in daily),
        "suspended_session_count": sum(
            row.trade_status_state == "CONFIRMED_SUSPENSION" for row in daily
        ),
        "unknown_trade_status_count": sum(
            row.bar_status in _BAR_STATUSES and row.trade_status_state == "UNKNOWN"
            for row in daily
        ),
        "target_missing_count": sum(
            row.bar_status == "TARGET_MISSING" for row in daily
        ),
        "read_failed_count": sum(row.bar_status == "READ_FAILED" for row in daily),
        "invalid_price_count": sum(
            row.bar_status == "TARGET_PRESENT"
            and row.trade_status_state == "ACTUAL"
            and not row.valid_price_bar
            for row in daily
        ),
        "missing_history_count": sum(
            row.bar_status == "HISTORY_ROW_NOT_STORED" for row in daily
        ),
    }


def _unknown_trend(
    kind: str,
    rule_id: str,
    minimum: int,
    complete_count: int,
    reason: str,
    *,
    price_coordinate: str,
    source_capture_id: int | None,
) -> RawPeriodTrend:
    return RawPeriodTrend(
        kind=kind,
        price_coordinate=price_coordinate,
        source_capture_id=source_capture_id,
        time_role="NON_PIT_RETROSPECTIVE",
        rule_id=rule_id,
        status="UNKNOWN",
        value=None,
        reason=reason,
        required_period_count=minimum,
        complete_period_count=complete_count,
        input_period_keys=(),
    )


def evaluate_raw_period_trend(
    *,
    kind: str,
    price_coordinate: str,
    periods: Sequence[RawPeriodBar],
    target_session: date,
    calendar_id: str,
    calendar_revision: str,
    source_cutoff: str,
    profile_rules: Mapping[str, Mapping[str, Any]],
    field_registry: Sequence[Mapping[str, Any]],
) -> RawPeriodTrend:
    """按显式BaoStock坐标复用闭合周/月公式，输出NON_PIT诊断结果。"""
    if kind not in _TREND_RULES or price_coordinate not in _PRICE_FIELDS_BY_COORDINATE:
        raise ContractError("Raw周期趋势只支持自然周或自然月")
    if (
        not isinstance(profile_rules, Mapping)
        or not isinstance(field_registry, (tuple, list))
        or type(target_session) is not date
        or not isinstance(source_cutoff, str)
        or not source_cutoff.strip()
        or source_cutoff.casefold() == "latest"
    ):
        raise ContractError("Raw周期趋势缺少显式目标、来源截止或合同")
    rule_id, formal_output_id = _TREND_RULES[kind]
    source_rule = profile_rules.get(rule_id)
    if (
        not isinstance(source_rule, Mapping)
        or source_rule.get("field_id") != formal_output_id
    ):
        raise ContractError("现役闭合周期趋势规则缺失或身份不符")
    formal_input_id = f"period.closed_{kind.lower()}"
    minimum = (
        source_rule.get("input_domains", {})
        .get(formal_input_id, {})
        .get("minimum_count")
    )
    if type(minimum) is not int or minimum < 2:
        raise ContractError("现役闭合周期趋势窗口合同无效")
    if any(period.price_coordinate != price_coordinate for period in periods):
        raise ContractError("Raw周期趋势输入混合了不同price_coordinate")
    source_capture_ids = {
        period.source_capture_id
        for period in periods
        if period.source_capture_id is not None
    }
    if len(source_capture_ids) > 1:
        return _unknown_trend(
            kind,
            rule_id,
            minimum,
            0,
            "MIXED_PAIRED_SOURCE_CAPTURE_IDS",
            price_coordinate=price_coordinate,
            source_capture_id=None,
        )
    source_capture_id = next(iter(source_capture_ids)) if source_capture_ids else None
    closed_periods = tuple(
        period
        for period in periods
        if period.calendar_closed and period.calendar_end_session <= target_session
    )
    trailing_complete: list[RawPeriodBar] = []
    for period in reversed(closed_periods):
        if period.coverage_status != "COMPLETE" or period.close is None:
            break
        trailing_complete.append(period)
    trailing_complete.reverse()
    if trailing_complete and source_capture_id is None:
        return _unknown_trend(
            kind,
            rule_id,
            minimum,
            0,
            "SOURCE_CAPTURE_ID_UNBOUND",
            price_coordinate=price_coordinate,
            source_capture_id=None,
        )
    if len(trailing_complete) < minimum:
        return _unknown_trend(
            kind,
            rule_id,
            minimum,
            len(trailing_complete),
            f"INSUFFICIENT_COMPLETE_CLOSED_PERIODS:{len(trailing_complete)}/{minimum}",
            price_coordinate=price_coordinate,
            source_capture_id=source_capture_id,
        )
    selected = tuple(trailing_complete[-minimum:])
    ordinals = tuple(period.calendar_ordinal for period in selected)
    if ordinals != tuple(range(ordinals[0], ordinals[0] + minimum)):
        return _unknown_trend(
            kind,
            rule_id,
            minimum,
            len(trailing_complete),
            "CLOSED_PERIOD_CALENDAR_SEQUENCE_GAP",
            price_coordinate=price_coordinate,
            source_capture_id=source_capture_id,
        )

    definition = next(
        (item for item in field_registry if item.get("field_id") == formal_input_id),
        None,
    )
    if not isinstance(definition, Mapping):
        raise ContractError("闭合周期输入未登记")
    diagnostic_input_id = f"research.raw_{kind.lower()}_prices"
    diagnostic_output_id = f"research.raw_{kind.lower()}_trend"
    diagnostic_producer = _TREND_PRODUCER
    diagnostic_definition = deepcopy(dict(definition))
    diagnostic_definition.update(
        {
            "field_id": diagnostic_input_id,
            "producer_symbol": diagnostic_producer,
            "value_type": "closed_prices",
            "unit": f"{price_coordinate}_OHLC",
            "coordinate_role": price_coordinate,
            "expression_ref": RAW_PERIOD_ALGORITHM_ID,
            "algorithm_version": RAW_PERIOD_ALGORITHM_ID,
            "parameter_version": RAW_PERIOD_ALGORITHM_ID,
            "context_dependencies": [
                "context.target_session",
                "context.source_cutoff",
                "context.calendar_ref",
                "context.source_capture_id",
                "context.price_coordinate_contract",
            ],
        }
    )
    diagnostic_rule = deepcopy(dict(source_rule))
    diagnostic_rule["field_id"] = diagnostic_output_id
    diagnostic_rule["input_fields"] = [diagnostic_input_id]
    diagnostic_rule["input_domains"] = {
        diagnostic_input_id: deepcopy(source_rule["input_domains"][formal_input_id])
    }
    diagnostic_rule["operands"] = {
        name: {
            **dict(operand),
            **(
                {"field_id": diagnostic_input_id}
                if operand.get("field_id") == formal_input_id
                else {}
            ),
        }
        for name, operand in source_rule["operands"].items()
    }
    window = WindowIdentity(
        contract=diagnostic_definition.get("window_contract", "CLOSED_PERIOD_R2"),
        first_endpoint=selected[0].calendar_start_session,
        last_endpoint=selected[-1].calendar_end_session,
        selected_sessions=tuple(
            row.session for period in selected for row in period.daily_rows
        ),
        calendar_revision=calendar_revision,
        status_revision="PRICE_COORDINATE_PERIOD_STATUS_R2",
    )
    adjustflag = _COORDINATE_FLAGS[price_coordinate]
    coordinate_id = (
        f"BAOSTOCK_CAPTURE_{source_capture_id}_ADJUSTFLAG_{adjustflag}_"
        f"{price_coordinate}_NON_PIT"
    )
    context = {
        "context.target_session": target_session,
        "context.source_cutoff": source_cutoff,
        "context.calendar_ref": f"{calendar_id}:{calendar_revision}",
        "context.source_capture_id": source_capture_id,
        "context.price_coordinate_contract": (
            f"BAOSTOCK_ADJUSTFLAG_{adjustflag}_{price_coordinate}_NON_PIT_R2"
        ),
    }
    field_value = ClosedPrices(
        closes=tuple(FieldValue(period.close) for period in selected if period.close),
        calendar_ordinals=ordinals,
        last_sessions=tuple(period.calendar_end_session for period in selected),
        complete=tuple(True for _ in selected),
        target_session=target_session,
        coordinate_id=coordinate_id,
    )
    supplied = ProfileInput(
        field_id=diagnostic_input_id,
        field=FieldValue(field_value),
        unit=diagnostic_definition["unit"],
        producer_symbol=diagnostic_producer,
        expression_ref=diagnostic_definition["expression_ref"],
        algorithm_version=diagnostic_definition.get("algorithm_version"),
        parameter_version=diagnostic_definition["parameter_version"],
        window=window,
        coordinate_role=diagnostic_definition["coordinate_role"],
        coordinate_id=coordinate_id,
        context=context,
    )
    handoff = ProfileHandoff(
        inputs={diagnostic_input_id: supplied},
        target_session=target_session,
        source_cutoff=source_cutoff,
        target_coordinate_id=coordinate_id,
        expected_windows={diagnostic_input_id: window},
        expected_context={diagnostic_input_id: context},
        runtime_producers={
            diagnostic_producer: RuntimeProducerBinding(
                symbol=diagnostic_producer,
                algorithm_version=diagnostic_definition.get("algorithm_version"),
            )
        },
    )
    result = evaluate_profile_kernel(
        diagnostic_output_id,
        handoff,
        {rule_id: diagnostic_rule},
        [diagnostic_definition],
    )
    return RawPeriodTrend(
        kind=kind,
        price_coordinate=price_coordinate,
        source_capture_id=source_capture_id,
        time_role="NON_PIT_RETROSPECTIVE",
        rule_id=rule_id,
        status=result.quality,
        value=str(result.value) if result.quality == "OBSERVED" else None,
        reason=result.reason,
        required_period_count=minimum,
        complete_period_count=len(trailing_complete),
        input_period_keys=tuple(period.key for period in selected),
    )


def _fraction_payload(value: Fraction | None) -> dict[str, str] | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 28
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
    exact = (
        str(value.numerator)
        if value.denominator == 1
        else f"{value.numerator}/{value.denominator}"
    )
    return {"exact": exact, "decimal": format(decimal_value, "f")}


def raw_period_payload(
    period: RawPeriodBar, *, include_daily_prices: bool = True
) -> dict[str, Any]:
    """将精确Fraction价格投影为可读且保留分数的JSON对象。"""
    if any(
        row.price_coordinate != period.price_coordinate for row in period.daily_rows
    ):
        raise ContractError("Raw周期响应混合了不同价格坐标")
    high_row = next(
        (row for row in period.daily_rows if row.session == period.high_session), None
    )
    low_row = next(
        (row for row in period.daily_rows if row.session == period.low_session), None
    )
    return {
        "kind": period.kind,
        "price_coordinate": period.price_coordinate,
        "source_capture_id": period.source_capture_id,
        "time_role": period.time_role,
        "period_key": period.key,
        "calendar_ordinal": period.calendar_ordinal,
        "calendar_start_session": period.calendar_start_session.isoformat(),
        "calendar_end_session": period.calendar_end_session.isoformat(),
        "calendar_expected_session_count": period.calendar_expected_session_count,
        "elapsed_expected_session_count": period.elapsed_expected_session_count,
        "stored_raw_row_count": period.stored_raw_row_count,
        "valid_price_bar_count": period.valid_price_bar_count,
        "suspended_session_count": period.suspended_session_count,
        "unknown_trade_status_count": period.unknown_trade_status_count,
        "target_missing_count": period.target_missing_count,
        "read_failed_count": period.read_failed_count,
        "invalid_price_count": period.invalid_price_count,
        "missing_history_count": period.missing_history_count,
        "window_truncated_session_count": period.window_truncated_session_count,
        "calendar_closed": period.calendar_closed,
        "coverage_status": period.coverage_status,
        "reason_codes": list(period.reason_codes),
        "ohlc": {
            "open": _fraction_payload(period.open),
            "high": _fraction_payload(period.high),
            "low": _fraction_payload(period.low),
            "close": _fraction_payload(period.close),
            "open_session": period.open_session.isoformat()
            if period.open_session
            else None,
            "high_session": period.high_session.isoformat()
            if period.high_session
            else None,
            "low_session": period.low_session.isoformat()
            if period.low_session
            else None,
            "close_session": period.close_session.isoformat()
            if period.close_session
            else None,
            "high_source_capture_id": high_row.source_capture_id if high_row else None,
            "low_source_capture_id": low_row.source_capture_id if low_row else None,
        },
        "raw_activity": {
            "price_coordinate": "RAW_UNADJUSTED",
            "volume": {
                "value": _fraction_payload(period.raw_volume),
                "quality": period.raw_volume_quality,
                "reason": period.raw_volume_reason,
                "unit": "SHARES",
                "unit_status": period.volume_unit_status,
            },
            "amount": {
                "value": _fraction_payload(period.raw_amount),
                "quality": period.raw_amount_quality,
                "reason": period.raw_amount_reason,
                "unit": "CNY",
                "unit_status": period.amount_unit_status,
            },
        },
        "daily_rows": [
            {
                "session": row.session.isoformat(),
                "price_coordinate": row.price_coordinate,
                "bar_status": row.bar_status,
                "bar_reason": row.bar_reason,
                "trade_status_state": row.trade_status_state,
                "trade_status_reason": row.trade_status_reason,
                "price_quality": row.price_quality,
                "price_reason": row.price_reason,
                "valid_price_bar": row.valid_price_bar,
                "open": _fraction_payload(row.open) if include_daily_prices else None,
                "high": _fraction_payload(row.high) if include_daily_prices else None,
                "low": _fraction_payload(row.low) if include_daily_prices else None,
                "close": _fraction_payload(row.close) if include_daily_prices else None,
                "source_capture_id": row.source_capture_id,
                "raw_activity": {
                    "price_coordinate": "RAW_UNADJUSTED",
                    "volume": {
                        "value": _fraction_payload(row.raw_volume),
                        "quality": row.raw_volume_quality,
                        "reason": row.raw_volume_reason,
                        "unit": "SHARES",
                        "unit_status": row.volume_unit_status,
                    },
                    "amount": {
                        "value": _fraction_payload(row.raw_amount),
                        "quality": row.raw_amount_quality,
                        "reason": row.raw_amount_reason,
                        "unit": "CNY",
                        "unit_status": row.amount_unit_status,
                    },
                },
            }
            for row in period.daily_rows
        ],
    }
