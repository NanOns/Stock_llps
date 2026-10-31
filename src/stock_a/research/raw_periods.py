"""来源代码级RAW自然周期价格观察；不构成正式P06周期画像。"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
from hashlib import sha256
import json
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


RAW_PERIOD_ALGORITHM_ID = "R02_SOURCE_SYMBOL_RAW_PERIOD_OHLC_R1"
_BAR_STATUSES = {"TARGET_PRESENT", "TARGET_MISSING", "READ_FAILED"}
_PRICE_FIELDS = ("raw_open", "raw_high", "raw_low", "raw_close")
_TREND_RULES = {
    "WEEK": ("PROFILE_CLOSED_WEEK_TREND_R1", "profile.closed_week_trend"),
    "MONTH": ("PROFILE_CLOSED_MONTH_TREND_R1", "profile.closed_month_trend"),
}
_TREND_PRODUCER = "stock_a.research.raw_periods:build_source_symbol_raw_periods"


@dataclass(frozen=True)
class RawDailyBar:
    """单个官方交易会话在一个精确来源包版本中的价格观察。"""

    session: date
    bar_status: str
    bar_reason: str | None
    price_quality: str
    price_reason: str | None
    open: Fraction | None
    high: Fraction | None
    low: Fraction | None
    close: Fraction | None
    valid_price_bar: bool
    package_sha256: str | None
    source_capture_id: str | None
    source_content_id: str | None
    source_entry_sha256: str | None


@dataclass(frozen=True)
class RawPeriodBar:
    """一个自然周/月的来源代码级RAW价格摘要与覆盖状态。"""

    kind: str
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
    high_bar_entry_sha256: str | None
    low_bar_entry_sha256: str | None
    daily_rows: tuple[RawDailyBar, ...]


@dataclass(frozen=True)
class RawPeriodTrend:
    """按现役闭合周/月规则求值的NON_PIT_RAW_DIAGNOSTIC结果。"""

    kind: str
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


def _daily_bar(session: date, row: Mapping[str, Any] | None) -> RawDailyBar:
    if row is None:
        return RawDailyBar(
            session,
            "HISTORY_ROW_NOT_STORED",
            "PG_HISTORY_SESSION_MISSING",
            "UNKNOWN",
            "PG_HISTORY_SESSION_MISSING",
            None,
            None,
            None,
            None,
            False,
            None,
            None,
            None,
            None,
        )
    if _session(row.get("session_date")) != session:
        raise ContractError("Raw周期输入行日期错配")
    bar_status = row.get("bar_status")
    if bar_status not in _BAR_STATUSES:
        raise ContractError("Raw周期输入含未登记来源状态")
    values = tuple(_fraction(row.get(name)) for name in _PRICE_FIELDS)
    quality = row.get("price_quality")
    valid = (
        bar_status == "TARGET_PRESENT" and quality == "OBSERVED" and _valid_ohlc(values)
    )
    return RawDailyBar(
        session=session,
        bar_status=bar_status,
        bar_reason=row.get("bar_reason"),
        price_quality=str(quality or "UNKNOWN"),
        price_reason=row.get("price_reason"),
        open=values[0],
        high=values[1],
        low=values[2],
        close=values[3],
        valid_price_bar=valid,
        package_sha256=row.get("package_sha256"),
        source_capture_id=(
            str(row["source_capture_id"])
            if row.get("source_capture_id") is not None
            else None
        ),
        source_content_id=(
            str(row["source_content_id"])
            if row.get("source_content_id") is not None
            else None
        ),
        source_entry_sha256=row.get("source_entry_sha256"),
    )


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
    missing_history_count = sum(
        row.bar_status == "HISTORY_ROW_NOT_STORED" for row in daily_rows
    )
    target_missing_count = sum(row.bar_status == "TARGET_MISSING" for row in daily_rows)
    read_failed_count = sum(row.bar_status == "READ_FAILED" for row in daily_rows)
    invalid_price_count = sum(
        row.bar_status == "TARGET_PRESENT" and not row.valid_price_bar
        for row in daily_rows
    )
    calendar_closed = expected_sessions[-1] <= target_session
    truncated_count = sum(day < window_start for day in expected_sessions)
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
    if not calendar_closed:
        coverage_status = "UNFINISHED"
        reasons.append("CALENDAR_PERIOD_NOT_CLOSED_AS_OF_TARGET")
    elif (
        not reasons
        and len(daily_rows) == len(expected_sessions)
        and len(valid_rows) == len(expected_sessions)
    ):
        coverage_status = "COMPLETE"
    else:
        coverage_status = "PARTIAL"
    if not valid_rows:
        reasons.append("NO_VALID_RAW_PRICE_BAR")

    first = valid_rows[0] if valid_rows else None
    last = valid_rows[-1] if valid_rows else None
    high_row = max(valid_rows, key=lambda row: row.high) if valid_rows else None
    low_row = min(valid_rows, key=lambda row: row.low) if valid_rows else None
    expose_prices = calendar_closed
    return RawPeriodBar(
        kind=kind,
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
        high_bar_entry_sha256=high_row.source_entry_sha256 if high_row else None,
        low_bar_entry_sha256=low_row.source_entry_sha256 if low_row else None,
        daily_rows=daily_rows if calendar_closed else (),
    )


def build_source_symbol_raw_periods(
    *,
    target_session: date,
    session_dates: Sequence[date],
    calendar_sessions: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
) -> dict[str, tuple[RawPeriodBar, ...]]:
    """按自然周/月聚合显式PG Raw行，缺Bar不会被压缩或推断成停牌。"""
    if (
        type(target_session) is not date
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
    daily = {day: _daily_bar(day, rows_by_session.get(day)) for day in session_dates}
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
    *, session_dates: Sequence[date], history_rows: Sequence[Mapping[str, Any]]
) -> dict[str, int]:
    """按Raw逐会话状态与OHLC合同动态统计，不使用固定Bar分母。"""
    if (
        not isinstance(session_dates, (tuple, list))
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
    daily = tuple(_daily_bar(day, rows_by_session.get(day)) for day in session_dates)
    return {
        "official_session_count": len(session_dates),
        "stored_raw_row_count": sum(
            row.bar_status != "HISTORY_ROW_NOT_STORED" for row in daily
        ),
        "valid_price_bar_count": sum(row.valid_price_bar for row in daily),
        "target_missing_count": sum(
            row.bar_status == "TARGET_MISSING" for row in daily
        ),
        "read_failed_count": sum(row.bar_status == "READ_FAILED" for row in daily),
        "invalid_price_count": sum(
            row.bar_status == "TARGET_PRESENT" and not row.valid_price_bar
            for row in daily
        ),
        "missing_history_count": sum(
            row.bar_status == "HISTORY_ROW_NOT_STORED" for row in daily
        ),
    }


def _unknown_trend(
    kind: str, rule_id: str, minimum: int, complete_count: int, reason: str
) -> RawPeriodTrend:
    return RawPeriodTrend(
        kind, rule_id, "UNKNOWN", None, reason, minimum, complete_count, ()
    )


def evaluate_raw_period_trend(
    *,
    kind: str,
    periods: Sequence[RawPeriodBar],
    target_session: date,
    calendar_id: str,
    calendar_revision: str,
    producer_source_sha256: str,
    profile_rules: Mapping[str, Mapping[str, Any]],
    field_registry: Sequence[Mapping[str, Any]],
) -> RawPeriodTrend:
    """复用Profile闭合周/月现役规则，输出来源代码NON_PIT诊断层结果。"""
    if kind not in _TREND_RULES:
        raise ContractError("Raw周期趋势只支持自然周或自然月")
    if (
        not isinstance(profile_rules, Mapping)
        or not isinstance(field_registry, (tuple, list))
        or type(target_session) is not date
        or len(producer_source_sha256) != 64
    ):
        raise ContractError("Raw周期趋势缺少显式目标、合同或生产者摘要")
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
    if len(trailing_complete) < minimum:
        return _unknown_trend(
            kind,
            rule_id,
            minimum,
            len(trailing_complete),
            f"INSUFFICIENT_COMPLETE_CLOSED_PERIODS:{len(trailing_complete)}/{minimum}",
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
            "producer_stage": "R02_NON_PIT_RAW_RESEARCH",
            "producer_symbol": diagnostic_producer,
            "value_type": "closed_prices",
            "unit": "RAW_UNADJUSTED_OHLC",
            "expression_ref": RAW_PERIOD_ALGORITHM_ID,
            "algorithm_version": RAW_PERIOD_ALGORITHM_ID,
            "parameter_version": RAW_PERIOD_ALGORITHM_ID,
            "context_dependencies": [
                "context.target_session",
                "context.source_cutoff",
                "context.calendar_ref",
                "context.sealed_actions_revision",
                "context.coordinate_contract_version",
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
    close_rows = [
        {
            "period_key": period.key,
            "close": _fraction_payload(period.close),
            "calendar_ordinal": period.calendar_ordinal,
            "close_session": period.close_session.isoformat()
            if period.close_session
            else None,
            "daily_rows": [
                {
                    "session": row.session.isoformat(),
                    "source_entry_sha256": row.source_entry_sha256,
                    "source_capture_id": row.source_capture_id,
                    "package_sha256": row.package_sha256,
                }
                for row in period.daily_rows
            ],
        }
        for period in selected
    ]
    selected_digest = sha256(
        json.dumps(
            close_rows,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    status_revision = sha256(
        json.dumps(
            [
                [
                    row.session.isoformat(),
                    row.bar_status,
                    row.price_quality,
                    row.price_reason,
                ]
                for period in selected
                for row in period.daily_rows
            ],
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()
    window = WindowIdentity(
        contract=diagnostic_definition.get("window_contract", "CLOSED_PERIOD_R1"),
        first_endpoint=selected[0].calendar_start_session,
        last_endpoint=selected[-1].calendar_end_session,
        selected_rows_digest=selected_digest,
        calendar_revision=calendar_revision,
        status_revision=status_revision,
    )
    coordinate_id = "TDX_RAW_UNADJUSTED_SOURCE_SYMBOL_NON_PIT"
    context = {
        "context.target_session": target_session,
        "context.source_cutoff": target_session.isoformat(),
        "context.calendar_ref": f"{calendar_id}:{calendar_revision}",
        "context.sealed_actions_revision": "UNKNOWN_ACTION_SELECTION_NOT_BOUND",
        "context.coordinate_contract_version": "RAW_UNADJUSTED_NON_PIT_DIAGNOSTIC_R1",
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
        producer_stage=diagnostic_definition["producer_stage"],
        producer_symbol=diagnostic_producer,
        producer_source_sha256=producer_source_sha256,
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
        source_cutoff=target_session.isoformat(),
        target_coordinate_id=coordinate_id,
        expected_windows={diagnostic_input_id: window},
        expected_context={diagnostic_input_id: context},
        runtime_producers={
            diagnostic_producer: RuntimeProducerBinding(
                symbol=diagnostic_producer,
                source_sha256=producer_source_sha256,
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
    high_row = next(
        (row for row in period.daily_rows if row.session == period.high_session), None
    )
    low_row = next(
        (row for row in period.daily_rows if row.session == period.low_session), None
    )
    return {
        "kind": period.kind,
        "period_key": period.key,
        "calendar_ordinal": period.calendar_ordinal,
        "calendar_start_session": period.calendar_start_session.isoformat(),
        "calendar_end_session": period.calendar_end_session.isoformat(),
        "calendar_expected_session_count": period.calendar_expected_session_count,
        "elapsed_expected_session_count": period.elapsed_expected_session_count,
        "stored_raw_row_count": period.stored_raw_row_count,
        "valid_price_bar_count": period.valid_price_bar_count,
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
            "high_source_entry_sha256": period.high_bar_entry_sha256,
            "low_source_entry_sha256": period.low_bar_entry_sha256,
            "high_source_capture_id": high_row.source_capture_id if high_row else None,
            "low_source_capture_id": low_row.source_capture_id if low_row else None,
        },
        "source_package_sha256s": sorted(
            {row.package_sha256 for row in period.daily_rows if row.package_sha256}
        ),
        "daily_rows": [
            {
                "session": row.session.isoformat(),
                "bar_status": row.bar_status,
                "bar_reason": row.bar_reason,
                "price_quality": row.price_quality,
                "price_reason": row.price_reason,
                "valid_price_bar": row.valid_price_bar,
                "open": _fraction_payload(row.open) if include_daily_prices else None,
                "high": _fraction_payload(row.high) if include_daily_prices else None,
                "low": _fraction_payload(row.low) if include_daily_prices else None,
                "close": _fraction_payload(row.close) if include_daily_prices else None,
                "package_sha256": row.package_sha256,
                "source_capture_id": row.source_capture_id,
                "source_content_id": row.source_content_id,
                "source_entry_sha256": row.source_entry_sha256,
            }
            for row in period.daily_rows
        ],
    }
