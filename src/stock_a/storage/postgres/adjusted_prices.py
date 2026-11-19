"""公司行为事实和仿射调整日线的固定PG读写。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.sources.corporate_actions import CorporateActionRecord

from .source_raw import _transaction


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EVENT_COLUMNS = (
    "event_ref",
    "semantic_revision",
    "source_market",
    "source_symbol",
    "source_notice_id",
    "source_ref",
    "effective_session",
    "published_date",
    "available_at",
    "available_at_quality",
    "captured_at",
    "action_kind",
    "cash_per_10",
    "rights_price",
    "bonus_per_10",
    "rights_per_10",
    "declared_bonus_per_10",
    "share_change_ratio",
    "source_quality",
)
_PRICE_COLUMNS = (
    "source_package_id",
    "source_capture_id",
    "target_capture_id",
    "source_symbol",
    "source_session",
    "target_session",
    "source_coordinate_id",
    "target_coordinate_id",
    "event_set_revision",
    "event_refs",
    "applied_event_refs",
    "alpha_numerator",
    "alpha_denominator",
    "beta_numerator",
    "beta_denominator",
    "price_tick",
    "adjusted_open",
    "adjusted_high",
    "adjusted_low",
    "adjusted_close",
    "price_quality",
    "adjustment_status",
    "quality_reason",
    "action_coverage_status",
    "research_mode",
    "calendar_id",
    "calendar_revision",
    "parameter_version",
    "algorithm_version",
    "window_start_session",
    "window_session_count",
    "input_digest",
)


class AdjustedPricePgError(RuntimeError):
    """固定PG公司行为/调整日线操作失败。"""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class AdjustedPriceFact:
    """一个来源捕获、源会话和目标会话绑定的调整OHLC事实。"""

    source_package_id: int
    source_capture_id: int
    target_capture_id: int
    source_symbol: str
    source_session: date
    target_session: date
    source_coordinate_id: str
    target_coordinate_id: str
    event_set_revision: str
    event_refs: tuple[str, ...]
    applied_event_refs: tuple[str, ...]
    alpha: Fraction
    beta: Fraction
    price_tick: Decimal
    adjusted_ohlc: tuple[Decimal, Decimal, Decimal, Decimal] | None
    price_quality: str
    adjustment_status: str
    quality_reason: str | None
    action_coverage_status: str
    research_mode: str
    calendar_id: str
    calendar_revision: str
    parameter_version: str
    algorithm_version: str
    window_start_session: date
    window_session_count: int
    input_digest: str

    def __post_init__(self) -> None:
        if type(self.source_package_id) is not int or self.source_package_id <= 0:
            raise AdjustedPricePgError("ADJUSTED_PRICE_KEY_INVALID", "来源包键非法")
        if any(
            type(value) is not int or value <= 0
            for value in (self.source_capture_id, self.target_capture_id)
        ):
            raise AdjustedPricePgError("ADJUSTED_PRICE_KEY_INVALID", "来源捕获键非法")
        if not isinstance(self.source_symbol, str) or not _SYMBOL.fullmatch(
            self.source_symbol
        ):
            raise AdjustedPricePgError("ADJUSTED_PRICE_KEY_INVALID", "来源代码非法")
        if (
            type(self.source_session) is not date
            or type(self.target_session) is not date
        ):
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_KEY_INVALID", "交易会话必须是date"
            )
        if self.source_session > self.target_session:
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_KEY_INVALID", "源会话晚于目标会话"
            )
        if not self.source_coordinate_id.startswith(
            "RAW:"
        ) or not self.target_coordinate_id.startswith("ADJ:"):
            raise AdjustedPricePgError("ADJUSTED_PRICE_KEY_INVALID", "价格坐标身份非法")
        if not _SHA256.fullmatch(self.event_set_revision) or not _SHA256.fullmatch(
            self.input_digest
        ):
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_KEY_INVALID", "事件或输入修订摘要非法"
            )
        if not isinstance(self.alpha, Fraction) or self.alpha <= 0:
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_VALUE_INVALID", "alpha须为正有理数"
            )
        if not isinstance(self.beta, Fraction):
            raise AdjustedPricePgError("ADJUSTED_PRICE_VALUE_INVALID", "beta须为有理数")
        if not isinstance(self.price_tick, Decimal) or self.price_tick <= 0:
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_VALUE_INVALID", "价格刻度须为正数"
            )
        if type(self.window_session_count) is not int or self.window_session_count < 1:
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_KEY_INVALID", "输入窗口会话数非法"
            )
        if self.price_quality == "OBSERVED":
            if (
                self.adjusted_ohlc is None
                or len(self.adjusted_ohlc) != 4
                or any(
                    not isinstance(value, Decimal) or value <= 0
                    for value in self.adjusted_ohlc
                )
                or self.quality_reason is not None
                or self.adjustment_status not in {"APPLIED", "IDENTITY_VERIFIED"}
            ):
                raise AdjustedPricePgError(
                    "ADJUSTED_PRICE_VALUE_INVALID", "有效调整OHLC合同不完整"
                )
        elif (
            self.price_quality != "UNKNOWN"
            or self.adjusted_ohlc is not None
            or not self.quality_reason
            or self.adjustment_status not in {"UNKNOWN", "NOT_COMPUTED"}
        ):
            raise AdjustedPricePgError(
                "ADJUSTED_PRICE_VALUE_INVALID", "未知调整事实必须保留原因且价格为空"
            )


def persist_corporate_action_event(
    config: Mapping[str, Any], record: CorporateActionRecord
) -> dict[str, Any]:
    """按真实公告引用和语义修订幂等保存一个已解析事件。"""

    if not isinstance(record, CorporateActionRecord):
        raise AdjustedPricePgError(
            "CORPORATE_ACTION_RECORD_INVALID", "事件类型不受支持"
        )
    values = tuple(getattr(record, column) for column in _EVENT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_EVENT_COLUMNS))
    columns = ", ".join(_EVENT_COLUMNS)
    with _transaction(config, write=True) as (_connection, cursor, _):
        cursor.execute(
            f"INSERT INTO source_corporate_action_events ({columns}) VALUES ({placeholders}) "
            "ON CONFLICT (event_ref, semantic_revision) DO NOTHING",
            values,
        )
        inserted = cursor.rowcount == 1
        cursor.execute(
            f"SELECT {columns} FROM source_corporate_action_events "
            "WHERE event_ref=%s AND semantic_revision=%s",
            (record.event_ref, record.semantic_revision),
        )
        stored = cursor.fetchone()
        captured_at_index = _EVENT_COLUMNS.index("captured_at")
        comparable_stored = tuple(
            value
            for index, value in enumerate(stored or ())
            if index != captured_at_index
        )
        comparable_input = tuple(
            value for index, value in enumerate(values) if index != captured_at_index
        )
        if stored is None or comparable_stored != comparable_input:
            raise AdjustedPricePgError(
                "CORPORATE_ACTION_IDEMPOTENCY_CONFLICT",
                "相同事件修订的PG读回与来源输入不一致",
            )
    return {
        "status": "INSERTED" if inserted else "ALREADY_PRESENT",
        "event_ref": record.event_ref,
        "semantic_revision": record.semantic_revision,
    }


def read_corporate_action_events(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    through_session: date,
) -> tuple[dict[str, Any], ...]:
    """读取来源代码在目标会话内及以前的事件，不触及后续事件。"""

    if not isinstance(source_symbol, str) or not _SYMBOL.fullmatch(source_symbol):
        raise AdjustedPricePgError("CORPORATE_ACTION_SYMBOL_INVALID", "来源代码非法")
    if type(through_session) is not date:
        raise AdjustedPricePgError(
            "CORPORATE_ACTION_SESSION_INVALID", "目标会话必须是date"
        )
    columns = ", ".join(_EVENT_COLUMNS)
    with _transaction(config, write=False) as (_connection, cursor, _):
        cursor.execute(
            f"SELECT {columns} FROM source_corporate_action_events "
            "WHERE source_symbol=%s AND effective_session<=%s "
            "ORDER BY effective_session, event_ref, semantic_revision",
            (source_symbol, through_session),
        )
        rows = cursor.fetchall()
    return tuple(dict(zip(_EVENT_COLUMNS, row, strict=True)) for row in rows)


def read_verified_action_sessions(
    config: Mapping[str, Any], *, through_session: date
) -> tuple[date, ...]:
    """读取截止会话内已解析、可用于目标事件窗的真实生效会话。"""

    if type(through_session) is not date:
        raise AdjustedPricePgError(
            "CORPORATE_ACTION_SESSION_INVALID", "截止会话必须是date"
        )
    with _transaction(config, write=False) as (_connection, cursor, _):
        cursor.execute(
            """SELECT DISTINCT effective_session
               FROM source_corporate_action_events
               WHERE effective_session<=%s
                 AND source_quality='VERIFIED_OFFICIAL_NOTICE'
                 AND action_kind='CAPITAL_RESERVE_TO_STOCK'
               ORDER BY effective_session""",
            (through_session,),
        )
        return tuple(row[0] for row in cursor.fetchall())


def read_verified_action_symbols_for_session(
    config: Mapping[str, Any], *, target_session: date
) -> tuple[str, ...]:
    """读取目标生效会话上有已验证公告的来源代码，不读取其他会话事件。"""

    if type(target_session) is not date:
        raise AdjustedPricePgError(
            "CORPORATE_ACTION_SESSION_INVALID", "目标会话必须是date"
        )
    with _transaction(config, write=False) as (_connection, cursor, _):
        cursor.execute(
            """SELECT DISTINCT source_symbol
               FROM source_corporate_action_events
               WHERE effective_session=%s
                 AND source_quality='VERIFIED_OFFICIAL_NOTICE'
                 AND action_kind='CAPITAL_RESERVE_TO_STOCK'
               ORDER BY source_symbol""",
            (target_session,),
        )
        return tuple(row[0] for row in cursor.fetchall())


def read_adjustment_raw_window(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    source_package_id: int,
    target_session: date,
    session_count: int,
) -> tuple[dict[str, Any], ...]:
    """读取目标日内、同代码、最新N个已存官方Raw会话。"""

    if not isinstance(source_symbol, str) or not _SYMBOL.fullmatch(source_symbol):
        raise AdjustedPricePgError("ADJUSTED_PRICE_SYMBOL_INVALID", "来源代码非法")
    if type(source_package_id) is not int or source_package_id <= 0:
        raise AdjustedPricePgError("ADJUSTED_PRICE_PACKAGE_INVALID", "来源包键非法")
    if type(target_session) is not date:
        raise AdjustedPricePgError(
            "ADJUSTED_PRICE_SESSION_INVALID", "目标会话必须是date"
        )
    if type(session_count) is not int or not 1 <= session_count <= 500:
        raise AdjustedPricePgError(
            "ADJUSTED_PRICE_WINDOW_INVALID", "调整输入必须是1至500个官方会话"
        )
    with _transaction(config, write=False) as (_connection, cursor, _):
        cursor.execute(
            """SELECT source_package_id, source_capture_id, source_symbol,
                      session_date, market, bar_status, price_quality,
                      raw_open, raw_high, raw_low, raw_close, price_reason
               FROM source_raw_daily
               WHERE source_symbol=%s AND source_package_id=%s AND session_date<=%s
               ORDER BY session_date DESC
               LIMIT %s""",
            (source_symbol, source_package_id, target_session, session_count),
        )
        rows = cursor.fetchall()
    columns = (
        "source_package_id",
        "source_capture_id",
        "source_symbol",
        "session_date",
        "market",
        "bar_status",
        "price_quality",
        "raw_open",
        "raw_high",
        "raw_low",
        "raw_close",
        "price_reason",
    )
    return tuple(dict(zip(columns, row, strict=True)) for row in reversed(rows))


def persist_adjusted_price_facts(
    config: Mapping[str, Any], facts: Sequence[AdjustedPriceFact]
) -> dict[str, Any]:
    """单事务幂等写入并逐行读回调整价格，不更新Raw父行。"""

    if not isinstance(facts, Sequence) or isinstance(facts, (str, bytes)) or not facts:
        raise AdjustedPricePgError("ADJUSTED_PRICE_FACTS_EMPTY", "调整事实集合不能为空")
    if any(not isinstance(fact, AdjustedPriceFact) for fact in facts):
        raise AdjustedPricePgError(
            "ADJUSTED_PRICE_FACT_INVALID", "存在未验证的调整事实类型"
        )
    business_keys = [
        (
            fact.source_capture_id,
            fact.source_symbol,
            fact.source_session,
            fact.target_capture_id,
            fact.target_session,
            fact.target_coordinate_id,
            fact.event_set_revision,
            fact.parameter_version,
            fact.algorithm_version,
            fact.calendar_revision,
        )
        for fact in facts
    ]
    if len(set(business_keys)) != len(business_keys):
        raise AdjustedPricePgError(
            "ADJUSTED_PRICE_DUPLICATE_KEY", "同次输入包含重复调整业务键"
        )
    sql_columns = ", ".join(_PRICE_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_PRICE_COLUMNS))
    insert_sql = (
        f"INSERT INTO source_adjusted_daily_prices ({sql_columns}) VALUES ({placeholders}) "
        "ON CONFLICT (source_capture_id, source_symbol, source_session, target_capture_id, "
        "target_session, target_coordinate_id, event_set_revision, parameter_version, "
        "algorithm_version, calendar_revision) DO NOTHING"
    )
    inserted = 0
    with _transaction(config, write=True, statement_timeout_ms=20000) as (
        _connection,
        cursor,
        _,
    ):
        for fact in facts:
            ohlc = fact.adjusted_ohlc or (None, None, None, None)
            row = (
                fact.source_package_id,
                fact.source_capture_id,
                fact.target_capture_id,
                fact.source_symbol,
                fact.source_session,
                fact.target_session,
                fact.source_coordinate_id,
                fact.target_coordinate_id,
                fact.event_set_revision,
                list(fact.event_refs),
                list(fact.applied_event_refs),
                fact.alpha.numerator,
                fact.alpha.denominator,
                fact.beta.numerator,
                fact.beta.denominator,
                fact.price_tick,
                *ohlc,
                fact.price_quality,
                fact.adjustment_status,
                fact.quality_reason,
                fact.action_coverage_status,
                fact.research_mode,
                fact.calendar_id,
                fact.calendar_revision,
                fact.parameter_version,
                fact.algorithm_version,
                fact.window_start_session,
                fact.window_session_count,
                fact.input_digest,
            )
            cursor.execute(insert_sql, row)
            inserted += int(cursor.rowcount == 1)
        for fact in facts:
            cursor.execute(
                """SELECT source_package_id, source_capture_id, target_capture_id,
                          source_symbol, source_session, target_session,
                          source_coordinate_id, target_coordinate_id, event_set_revision,
                          event_refs, applied_event_refs, alpha_numerator, alpha_denominator,
                          beta_numerator, beta_denominator, price_tick,
                          adjusted_open, adjusted_high, adjusted_low, adjusted_close,
                          price_quality, adjustment_status, quality_reason,
                          action_coverage_status, research_mode, calendar_id,
                          calendar_revision, parameter_version, algorithm_version,
                          window_start_session, window_session_count, input_digest
                   FROM source_adjusted_daily_prices
                   WHERE source_capture_id=%s AND source_symbol=%s AND source_session=%s
                     AND target_capture_id=%s AND target_session=%s
                     AND target_coordinate_id=%s AND event_set_revision=%s
                     AND parameter_version=%s AND algorithm_version=%s
                     AND calendar_revision=%s""",
                (
                    fact.source_capture_id,
                    fact.source_symbol,
                    fact.source_session,
                    fact.target_capture_id,
                    fact.target_session,
                    fact.target_coordinate_id,
                    fact.event_set_revision,
                    fact.parameter_version,
                    fact.algorithm_version,
                    fact.calendar_revision,
                ),
            )
            stored = cursor.fetchone()
            expected = (
                fact.source_package_id,
                fact.source_capture_id,
                fact.target_capture_id,
                fact.source_symbol,
                fact.source_session,
                fact.target_session,
                fact.source_coordinate_id,
                fact.target_coordinate_id,
                fact.event_set_revision,
                list(fact.event_refs),
                list(fact.applied_event_refs),
                fact.alpha.numerator,
                fact.alpha.denominator,
                fact.beta.numerator,
                fact.beta.denominator,
                fact.price_tick,
                *(fact.adjusted_ohlc or (None, None, None, None)),
                fact.price_quality,
                fact.adjustment_status,
                fact.quality_reason,
                fact.action_coverage_status,
                fact.research_mode,
                fact.calendar_id,
                fact.calendar_revision,
                fact.parameter_version,
                fact.algorithm_version,
                fact.window_start_session,
                fact.window_session_count,
                fact.input_digest,
            )
            if stored != expected:
                raise AdjustedPricePgError(
                    "ADJUSTED_PRICE_READBACK_MISMATCH",
                    "调整价格写后独立读回不一致，事务回滚",
                )
    return {
        "status": "COMMITTED",
        "expected_rows": len(facts),
        "inserted_rows": inserted,
        "idempotent_rows": len(facts) - inserted,
        "target_sessions": sorted({fact.target_session.isoformat() for fact in facts}),
    }


def _period_key(day: date, period: str) -> str:
    if period == "week":
        iso = day.isocalendar()
        return f"{iso.year:04d}-W{iso.week:02d}"
    return f"{day.year:04d}-{day.month:02d}"


def _aggregate_incomplete_periods(
    points: Sequence[Mapping[str, Any]], period: str
) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for point in points:
        groups.setdefault(_period_key(point["session_date"], period), []).append(point)
    result = []
    for key, rows in groups.items():
        rows = sorted(rows, key=lambda item: item["session_date"])
        result.append(
            {
                "period_key": key,
                "ohlc": None,
                "coverage_status": "INCOMPLETE",
                "reason_codes": ["OFFICIAL_PERIOD_SESSION_COVERAGE_NOT_PROVEN"],
                "actual_session_count": len(rows),
            }
        )
    return result


def read_adjusted_history(
    config: Mapping[str, Any], source_symbol: str, target_session: date
) -> dict[str, Any]:
    """个股GET只读读取已提交目标坐标日线及其小范围周期投影。"""

    if not isinstance(source_symbol, str) or not _SYMBOL.fullmatch(source_symbol):
        raise AdjustedPricePgError("ADJUSTED_PRICE_SYMBOL_INVALID", "来源代码非法")
    if type(target_session) is not date:
        raise AdjustedPricePgError(
            "ADJUSTED_PRICE_SESSION_INVALID", "目标会话必须是date"
        )
    with _transaction(config, write=False) as (_connection, cursor, _):
        cursor.execute(
            """SELECT adjusted.adjusted_price_id, adjusted.source_package_id,
                      adjusted.source_capture_id, adjusted.target_capture_id,
                      adjusted.source_symbol, adjusted.source_session,
                      adjusted.target_session, adjusted.source_coordinate_id,
                      adjusted.target_coordinate_id, adjusted.event_set_revision,
                      adjusted.event_refs, adjusted.applied_event_refs,
                      adjusted.alpha_numerator, adjusted.alpha_denominator,
                      adjusted.beta_numerator, adjusted.beta_denominator,
                      adjusted.price_tick, adjusted.adjusted_open,
                      adjusted.adjusted_high, adjusted.adjusted_low,
                      adjusted.adjusted_close, adjusted.price_quality,
                      adjusted.adjustment_status, adjusted.quality_reason,
                      adjusted.action_coverage_status, adjusted.research_mode,
                      adjusted.calendar_id, adjusted.calendar_revision,
                      adjusted.parameter_version, adjusted.algorithm_version,
                      adjusted.window_start_session, adjusted.window_session_count,
                      adjusted.input_digest, adjusted.created_at,
                      raw.raw_open, raw.raw_high, raw.raw_low, raw.raw_close
               FROM source_adjusted_daily_prices adjusted
               JOIN source_raw_daily raw
                 ON raw.source_capture_id=adjusted.source_capture_id
                AND raw.source_symbol=adjusted.source_symbol
                AND raw.session_date=adjusted.source_session
               WHERE adjusted.source_symbol=%s AND adjusted.target_session=%s
                 AND adjusted.parameter_version='TDX_XRXD_UNIT_R1'
                 AND adjusted.algorithm_version='STOCK_A_AFFINE_R1'
               ORDER BY adjusted.source_session, adjusted.created_at""",
            (source_symbol, target_session),
        )
        rows = cursor.fetchall()
        event_refs = sorted({ref for row in rows for ref in (row[10] or ())})
        events: list[dict[str, Any]] = []
        if event_refs:
            cursor.execute(
                """SELECT event_ref, semantic_revision, source_ref, source_notice_id,
                          effective_session, published_date, available_at,
                          available_at_quality, action_kind, source_quality
                   FROM source_corporate_action_events
                   WHERE source_symbol=%s AND event_ref = ANY(%s)
                     AND effective_session<=%s
                   ORDER BY effective_session, event_ref, semantic_revision""",
                (source_symbol, event_refs, target_session),
            )
            events = [
                {
                    "event_ref": row[0],
                    "semantic_revision": row[1].strip(),
                    "source_ref": row[2],
                    "source_notice_id": row[3],
                    "effective_session": row[4],
                    "published_date": row[5],
                    "available_at": row[6],
                    "available_at_quality": row[7],
                    "action_kind": row[8],
                    "source_quality": row[9],
                }
                for row in cursor.fetchall()
            ]
    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "source_symbol": source_symbol,
            "target_session": target_session.isoformat(),
            "price_coordinate": "ADJUSTED",
            "research_mode": "RETROSPECTIVE_NON_PIT",
            "reason": "该代码和目标会话没有已提交的精确版本调整价格事实。",
            "source": "TDX VIPDATA Raw + 已登记公司行为事件",
            "points": [],
            "periods": {"week": [], "month": []},
            "event_refs": [],
            "coverage": {"actual_session_count": 0, "status": "NOT_COMPUTED"},
        }
    coordinate_ids = {row[8] for row in rows}
    revisions = {row[9] for row in rows}
    calendars = {(row[26], row[27]) for row in rows}
    windows = {(row[30], row[31]) for row in rows}
    packages = {row[1] for row in rows}
    source_capture_keys = {(row[2], row[5]) for row in rows}
    source_sessions = {row[5] for row in rows}
    target_captures = {row[3] for row in rows}
    if (
        any(
            len(values) != 1
            for values in (
                coordinate_ids,
                revisions,
                calendars,
                windows,
                packages,
                target_captures,
            )
        )
        or len(source_capture_keys) != len(rows)
        or len(source_sessions) != len(rows)
    ):
        return {
            "status": "INCOMPLETE",
            "source_symbol": source_symbol,
            "target_session": target_session.isoformat(),
            "price_coordinate": "ADJUSTED",
            "research_mode": "RETROSPECTIVE_NON_PIT",
            "reason": "PG存在多个坐标、事件、日历或窗口版本，未自动回退选择。",
            "source": "TDX VIPDATA Raw + 已登记公司行为事件",
            "points": [],
            "periods": {"week": [], "month": []},
            "event_refs": event_refs,
            "coverage": {"actual_session_count": len(rows), "status": "INCOMPLETE"},
        }
    points = []
    for row in rows:
        raw_ohlc = {
            "open": row[34],
            "high": row[35],
            "low": row[36],
            "close": row[37],
        }
        adjusted_ohlc = {
            "open": row[17],
            "high": row[18],
            "low": row[19],
            "close": row[20],
        }
        points.append(
            {
                "session_date": row[5],
                "raw_ohlc": raw_ohlc,
                "ohlc": adjusted_ohlc,
                "price_quality": row[21],
                "adjustment_status": row[22],
                "reason": row[23],
                "source_capture_id": row[2],
                "source_package_id": row[1],
                "event_refs": list(row[10] or ()),
                "applied_event_refs": list(row[11] or ()),
                "action_coverage_status": row[24],
                "coordinate_id": row[8],
            }
        )
    quality_status = (
        "OBSERVED"
        if all(point["price_quality"] == "OBSERVED" for point in points)
        else "UNKNOWN"
    )
    statuses = {row[24] for row in rows}
    coverage_status = "COMPLETE" if len(windows) == 1 else "INCOMPLETE"
    coverage_status = "INCOMPLETE" if statuses != {"COMPLETE"} else coverage_status
    window_start, window_count = next(iter(windows))
    actual_count = len(points)
    if (
        actual_count != window_count
        or points[0]["session_date"] != window_start
        or points[-1]["session_date"] != target_session
    ):
        coverage_status = "INCOMPLETE"
    return {
        "status": "READY" if quality_status == "OBSERVED" else "INCOMPLETE",
        "source_symbol": source_symbol,
        "target_session": target_session.isoformat(),
        "price_coordinate": "ADJUSTED",
        "research_mode": "RETROSPECTIVE_NON_PIT",
        "algorithm_version": rows[0][29],
        "source": "TDX VIPDATA Raw + 深圳证券交易所实施公告",
        "quality": quality_status,
        "reason": None
        if quality_status == "OBSERVED"
        else "调整窗口包含非有效Raw价格。",
        "event_refs": event_refs,
        "event_sources": events,
        "coverage": {
            "status": coverage_status,
            "expected_session_count": window_count,
            "actual_session_count": actual_count,
            "window_start_session": window_start.isoformat(),
            "window_end_session": target_session.isoformat(),
            "action_coverage_status": sorted(statuses),
        },
        "points": points,
        "periods": {
            "week": _aggregate_incomplete_periods(points, "week"),
            "month": _aggregate_incomplete_periods(points, "month"),
        },
    }
