"""按官方交易日历构造闭合周/月周期Bar的纯计算内核。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from fractions import Fraction
from zoneinfo import ZoneInfo

from stock_a.canonical.identity import semantic_digest
from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.core import CoreObservation, MarketSessionCalendar

PERIOD_AGGREGATION_VERSION = "PERIOD_CLOSED_BAR_AGG_R1"
_PERIODS = {"W", "M"}
_STATES = {"ACTUAL", "CONFIRMED_SUSPENSION", "PRE_LISTING", "UNKNOWN", "IDENTITY_UNKNOWN", "ADJUSTMENT_UNKNOWN"}
_PRICE_FIELDS = ("open", "high", "low", "close")
_SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class CalendarCoverageProof:
    """上游封存的官方日历自然日期覆盖证明；纯内核只校验其自洽性。"""

    calendar_id: str
    calendar_revision: str
    coverage_start: date
    coverage_end: date
    available_at: datetime
    seal_basis: str
    proof_ref: str

    def __post_init__(self) -> None:
        if not all(isinstance(value, str) and value.strip() for value in
                   (self.calendar_id, self.calendar_revision, self.seal_basis, self.proof_ref)):
            raise ContractError("日历覆盖证明必须绑定日历版本、封口依据及稳定引用")
        if type(self.coverage_start) is not date or type(self.coverage_end) is not date or self.coverage_start > self.coverage_end:
            raise ContractError("日历覆盖证明日期区间非法")
        if (not isinstance(self.available_at, datetime) or self.available_at.tzinfo is None
                or self.available_at.utcoffset() is None):
            raise ContractError("日历覆盖证明可得时间必须带时区")


@dataclass(frozen=True)
class PeriodSessionStatus:
    """一个官方会话的身份/上市状态及当时可知的正式收盘时刻。"""

    session: date
    state: str
    listing_eligible: bool | None


@dataclass(frozen=True)
class ClosedPeriodBar:
    """一个自然日历槽的闭合OHLCV/Amount及独立质量。"""

    period_kind: str
    period_id: str
    calendar_ordinal: int
    period_start: date
    period_end: date
    planned_last_session: date
    first_actual_session: date | None
    last_actual_session: date | None
    actual_count: int
    suspended_count: int
    unknown_count: int
    pre_listing_count: int
    calendar_closed: bool
    data_coverage_complete: bool
    full_listing_period: bool
    eligible_closed_period: bool
    state: str
    open: FieldValue
    high: FieldValue
    low: FieldValue
    close: FieldValue
    amount: FieldValue
    volume: FieldValue
    target_session: date
    source_cutoff: datetime
    coordinate_id: str
    calendar_id: str
    calendar_revision: str
    status_revision: str
    action_revision: str
    aggregation_version: str
    input_digest: str
    calendar_coverage_proven: bool
    fact_identity_trusted: bool
    period_fact_digest: str
    selection_context_digest: str


def _period_key(kind: str, session: date) -> tuple[str, date, date, int]:
    if kind == "W":
        iso = session.isocalendar()
        start = date.fromisocalendar(iso.year, iso.week, 1)
        end = date.fromisocalendar(iso.year, iso.week, 7)
        ordinal = (start.toordinal() - 1) // 7
        return f"{iso.year:04d}-W{iso.week:02d}", start, end, ordinal
    start = date(session.year, session.month, 1)
    if session.month == 12:
        end = date(session.year, 12, 31)
    else:
        next_month = date(session.year, session.month + 1, 1)
        end = date.fromordinal(next_month.toordinal() - 1)
    return f"{session.year:04d}-{session.month:02d}", start, end, session.year * 12 + session.month - 1


def _field_number(field: FieldValue, name: str, *, positive: bool = False) -> Fraction | None:
    if not isinstance(field, FieldValue) or field.quality != "OBSERVED":
        return None
    try:
        value = exact_number(field.value)
    except ValueError:
        return None
    if positive and value <= 0 or not positive and value < 0:
        return None
    return value


def _unknown_fields(reason: str) -> tuple[FieldValue, ...]:
    return tuple(unknown(reason) for _ in range(6))


def build_closed_period_bars(
    period: str,
    official_calendar: MarketSessionCalendar,
    session_states: Sequence[PeriodSessionStatus],
    canonical_daily: Sequence[CoreObservation],
    official_session_closes: Mapping[date, datetime],
    *,
    target_coordinate_id: str,
    target_session: date,
    source_cutoff: datetime,
    status_revision: str,
    action_revision: str,
    calendar_coverage_proof: CalendarCoverageProof | None = None,
) -> tuple[ClosedPeriodBar, ...]:
    """以显式官方会话、证券状态和同目标坐标日线构造W/M闭合周期槽。

    调用方须先按source_cutoff封存并选择session_states与canonical_daily；
    本纯函数校验其覆盖、排序、身份及本地周期闭合时钟，不执行任何IO。
    """
    if not isinstance(period, str) or period not in _PERIODS:
        raise ContractError("period仅支持W或M")
    if not isinstance(official_calendar, MarketSessionCalendar):
        raise ContractError("必须提供带版本的官方市场日历")
    if calendar_coverage_proof is not None and not isinstance(calendar_coverage_proof, CalendarCoverageProof):
        raise ContractError("calendar_coverage_proof类型非法")
    if type(target_session) is not date:
        raise ContractError("target_session必须是date")
    if not isinstance(source_cutoff, datetime) or source_cutoff.tzinfo is None or source_cutoff.utcoffset() is None:
        raise ContractError("source_cutoff必须是带时区datetime")
    if not isinstance(target_coordinate_id, str) or not target_coordinate_id.strip():
        raise ContractError("目标价格坐标身份不能为空")
    if not all(isinstance(x, str) and x for x in (status_revision, action_revision)):
        raise ContractError("状态和公司行为版本必须显式提供")
    if not isinstance(session_states, Sequence) or isinstance(session_states, (str, bytes)):
        raise ContractError("session_states必须是有序序列")
    if not isinstance(canonical_daily, Sequence) or isinstance(canonical_daily, (str, bytes)):
        raise ContractError("canonical_daily必须是有序序列")
    state_rows = tuple(session_states)
    daily_rows = tuple(canonical_daily)
    if any(not isinstance(row, PeriodSessionStatus) or type(row.session) is not date for row in state_rows):
        raise ContractError("会话状态含非法行")
    if any(row.state not in _STATES or type(row.listing_eligible) not in (bool, type(None)) for row in state_rows):
        raise ContractError("会话状态或上市资格枚举非法")
    if not isinstance(official_session_closes, Mapping) or set(official_session_closes) != set(official_calendar.sessions):
        raise ContractError("官方会话收盘时刻须与版本化日历逐日完全对应")
    if any(not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None
           for value in official_session_closes.values()):
        raise ContractError("官方会话收盘时刻必须带时区")
    if any(value.astimezone(_SHANGHAI).date() != session
           for session, value in official_session_closes.items()):
        raise ContractError("官方会话收盘时刻须落在对应的A股本地会话日期")
    if any(a.session >= b.session for a, b in zip(state_rows, state_rows[1:])):
        raise ContractError("会话状态必须唯一并按日期递增")
    calendar_sessions = official_calendar.sessions
    if any(row.session not in calendar_sessions for row in state_rows):
        raise ContractError("状态包含非官方市场会话")
    if any(not isinstance(row, CoreObservation) or type(row.session) is not date for row in daily_rows):
        raise ContractError("规范日线含非法行")
    if any(a.session >= b.session for a, b in zip(daily_rows, daily_rows[1:])):
        raise ContractError("规范日线必须唯一并按日期递增")
    state_by_day = {row.session: row for row in state_rows}
    daily_by_day = {row.session: row for row in daily_rows}
    if any(day not in calendar_sessions for day in daily_by_day):
        raise ContractError("规范日线含非官方会话")
    for row in daily_rows:
        if row.state != "ACTUAL":
            raise ContractError("canonical_daily仅可包含ACTUAL日线")
        if row.coordinate_id != target_coordinate_id:
            raise ContractError("周期聚合禁止混用目标价格坐标")
    if set(daily_by_day) - {day for day, status in state_by_day.items() if status.state == "ACTUAL"}:
        raise ContractError("ACTUAL日线与会话状态不一致")

    start_date = min(calendar_sessions)
    _, first_start, _, _ = _period_key(period, start_date)
    _, _, target_period_end, _ = _period_key(period, target_session)
    proof_matches = (
        calendar_coverage_proof is not None
        and calendar_coverage_proof.calendar_id == official_calendar.calendar_id
        and calendar_coverage_proof.calendar_revision == official_calendar.revision
        and calendar_coverage_proof.available_at <= source_cutoff
    )
    if period == "W":
        cursor = first_start
        step = 7
    else:
        cursor = first_start
        step = 0
    result: list[ClosedPeriodBar] = []
    while cursor <= target_period_end:
        if period == "W":
            p_start = cursor
            p_end = date.fromordinal(cursor.toordinal() + 6)
            iso = p_start.isocalendar()
            period_id = f"{iso.year:04d}-W{iso.week:02d}"
            ordinal = (p_start.toordinal() - 1) // 7
            cursor = date.fromordinal(cursor.toordinal() + step)
        else:
            p_start = cursor
            if cursor.month == 12:
                p_end = date(cursor.year, 12, 31)
                cursor = date(cursor.year + 1, 1, 1)
            else:
                next_month = date(cursor.year, cursor.month + 1, 1)
                p_end = date.fromordinal(next_month.toordinal() - 1)
                cursor = next_month
            period_id = f"{p_start.year:04d}-{p_start.month:02d}"
            ordinal = p_start.year * 12 + p_start.month - 1
        scheduled = tuple(day for day in calendar_sessions if p_start <= day <= p_end)
        proof_covers_period = bool(
            proof_matches and calendar_coverage_proof.coverage_start <= p_start
            and calendar_coverage_proof.coverage_end >= p_end
        )
        states = [state_by_day.get(day) for day in scheduled]
        missing_days = [day for day, state in zip(scheduled, states, strict=True) if state is None]
        unknown_days = [day for day, state in zip(scheduled, states, strict=True)
                        if state is not None and (state.state in {"UNKNOWN", "IDENTITY_UNKNOWN", "ADJUSTMENT_UNKNOWN"}
                                                  or state.listing_eligible is None)]
        prelisting = [state for state in states if state is not None and (state.state == "PRE_LISTING" or state.listing_eligible is False)]
        actual = [daily_by_day[day] for day, state in zip(scheduled, states, strict=True)
                  if state is not None and state.state == "ACTUAL" and day in daily_by_day]
        missing_actual = [day for day, state in zip(scheduled, states, strict=True)
                          if state is not None and state.state == "ACTUAL" and day not in daily_by_day]
        if any(row.state == "ACTUAL" and row.listing_eligible is not True for row in states if row is not None):
            prelisting = list(prelisting) or [row for row in states if row is not None and row.listing_eligible is not True]
        coverage = not missing_days and not unknown_days and not missing_actual
        listing_complete = coverage and not prelisting and all(row is not None and row.listing_eligible is True for row in states)
        all_suspended = bool(scheduled) and coverage and not actual and len([x for x in states if x and x.state == "CONFIRMED_SUSPENSION"]) == len(scheduled)
        if scheduled:
            planned_last = scheduled[-1]
            calendar_closed = (proof_covers_period and planned_last <= target_session
                               and official_session_closes[planned_last] <= source_cutoff)
        else:
            planned_last = p_end
            next_midnight = datetime.combine(p_end + timedelta(days=1), time.min, _SHANGHAI)
            cutoff_shanghai = source_cutoff.astimezone(_SHANGHAI)
            calendar_closed = (proof_covers_period and target_session > p_end
                               and cutoff_shanghai >= next_midnight)
        # 未证明的已结束自然槽必须留下未知槽，不能被Profile跳过后补取更早历史。
        period_ended = target_session > p_end and source_cutoff.astimezone(_SHANGHAI) >= datetime.combine(
            p_end + timedelta(days=1), time.min, _SHANGHAI)
        if not calendar_closed and (proof_covers_period or not period_ended):
            continue
        if not proof_covers_period:
            state_name = "CALENDAR_COVERAGE_UNPROVEN"
            fields = _unknown_fields("CALENDAR_COVERAGE_UNPROVEN")
        elif not scheduled:
            state_name = "NO_MARKET_SESSIONS"
            fields = tuple(FieldValue(None, "NOT_APPLICABLE", "NO_MARKET_SESSIONS") for _ in range(6))
        elif missing_days or unknown_days or missing_actual:
            state_name = "CLOSED_COVERAGE_UNKNOWN"
            why = "SESSION_STATE_UNKNOWN" if unknown_days or missing_days else "ACTUAL_BAR_MISSING"
            fields = _unknown_fields(why)
        elif all_suspended:
            state_name = "CLOSED_ALL_SUSPENDED"
            fields = tuple(FieldValue(None, "NOT_APPLICABLE", "FULL_PERIOD_SUSPENDED") for _ in range(6))
        else:
            state_name = "NOT_FULL_LISTING_PERIOD" if not listing_complete else "CLOSED_WITH_ACTUAL"
            fields = _aggregate_actual(actual)
        actual_sessions = [row.session for row in actual]
        state_identity = tuple(None if row is None else
                               (row.session, row.state, row.listing_eligible,
                                official_session_closes[row.session]) for row in states)
        daily_identity = tuple((row.session, row.state, row.coordinate_id, row.open, row.high,
                                row.low, row.close, row.amount, row.volume) for row in actual)
        proof_identity = None if calendar_coverage_proof is None else (
            calendar_coverage_proof.proof_ref, calendar_coverage_proof.coverage_start,
            calendar_coverage_proof.coverage_end, calendar_coverage_proof.available_at,
            calendar_coverage_proof.seal_basis,
        )
        # 实体/逐行来源封口由P04/P05→P06适配层提供；纯内核不得把调用者标签当作可信身份。
        fact_identity_trusted = False
        period_fact_digest = semantic_digest((PERIOD_AGGREGATION_VERSION, "ENTITY_SOURCE_BINDING_UNVERIFIED",
                                              period, period_id, ordinal,
                                              state_name, coverage, listing_complete, proof_covers_period,
                                              official_calendar.calendar_id, official_calendar.revision,
                                              proof_identity, target_coordinate_id, state_identity,
                                              daily_identity, fields))
        selection_context_digest = semantic_digest((period_fact_digest, target_session, source_cutoff,
                                                   official_calendar.calendar_id, official_calendar.revision,
                                                   status_revision, action_revision, tuple(scheduled)))
        input_digest = period_fact_digest
        result.append(ClosedPeriodBar(
            period, period_id, ordinal, p_start, p_end, planned_last,
            actual_sessions[0] if actual_sessions else None, actual_sessions[-1] if actual_sessions else None,
            len(actual), sum(1 for row in states if row and row.state == "CONFIRMED_SUSPENSION"),
            len(missing_days) + len(unknown_days) + len(missing_actual), len(prelisting), calendar_closed,
            coverage, listing_complete, calendar_closed and coverage and listing_complete, state_name,
            *fields, target_session, source_cutoff, target_coordinate_id, official_calendar.calendar_id,
            official_calendar.revision, status_revision, action_revision, PERIOD_AGGREGATION_VERSION, input_digest,
            proof_covers_period, fact_identity_trusted, period_fact_digest, selection_context_digest))
    return tuple(result)


def _aggregate_actual(rows: Sequence[CoreObservation]) -> tuple[FieldValue, ...]:
    if not rows:
        return tuple(FieldValue(None, "NOT_APPLICABLE", "NO_ACTUAL_BAR") for _ in range(6))
    price_rows = [{name: getattr(row, name) for name in _PRICE_FIELDS} for row in rows]
    invalid_envelope = False
    for fields in price_rows:
        vals = tuple(_observed_price(fields[name]) for name in _PRICE_FIELDS)
        if any(invalid for _, invalid in vals):
            invalid_envelope = True
        values = tuple(value for value, _ in vals)
        o, h, low, close = values
        if h is not None and any(value is not None and value > h for value in (o, low, close)):
            invalid_envelope = True
        if low is not None and any(value is not None and value < low for value in (o, h, close)):
            invalid_envelope = True
    if invalid_envelope:
        prices = tuple(unknown("DATA_INVALID") for _ in range(4))
    else:
        first, last = rows[0], rows[-1]
        open_value = _field_number(first.open, "open", positive=True)
        high_values = [_field_number(row.high, "high", positive=True) for row in rows]
        low_values = [_field_number(row.low, "low", positive=True) for row in rows]
        close_value = _field_number(last.close, "close", positive=True)
        prices = (
            FieldValue(open_value) if open_value is not None else unknown("OPEN_UNAVAILABLE"),
            FieldValue(max(high_values)) if high_values and all(x is not None for x in high_values) else unknown("HIGH_UNAVAILABLE"),
            FieldValue(min(low_values)) if low_values and all(x is not None for x in low_values) else unknown("LOW_UNAVAILABLE"),
            FieldValue(close_value) if close_value is not None else unknown("CLOSE_UNAVAILABLE"),
        )
    amounts = [_field_number(row.amount, "amount") for row in rows]
    volumes = [_field_number(row.volume, "volume") for row in rows]
    amount = FieldValue(sum(amounts, Fraction())) if all(x is not None for x in amounts) else unknown("AMOUNT_UNAVAILABLE")
    volume = FieldValue(sum(volumes, Fraction())) if all(x is not None for x in volumes) else unknown("VOLUME_UNAVAILABLE")
    return (*prices, amount, volume)


def _observed_price(field: FieldValue) -> tuple[Fraction | None, bool]:
    """返回已观察价格和非法标记；上游UNKNOWN只影响其自身聚合依赖。"""
    if not isinstance(field, FieldValue) or field.quality != "OBSERVED":
        return None, False
    try:
        value = exact_number(field.value)
    except (ValueError, TypeError, ZeroDivisionError):
        return None, True
    return value, value <= 0
