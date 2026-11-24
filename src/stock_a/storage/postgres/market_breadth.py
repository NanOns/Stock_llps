"""现役固定来源篮市场宽度的typed日事实持久化。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from fractions import Fraction
import random
import time
from typing import Any, Mapping

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_DAY_TABLE = "source_market_breadth_days"
_SYMBOL_TABLE = "source_market_breadth_symbols"
_CHANGE_TABLE = "source_market_breadth_change_days"
_DAY_COLUMNS = (
    "target_session",
    "source_capture_id",
    "price_coordinate",
    "source_time_quality",
    "source_available_at",
    "capture_read_started_at",
    "capture_validated_at",
    "source_clock_role",
    "calendar_id",
    "calendar_revision",
    "session_t4",
    "session_t3",
    "session_t2",
    "session_t1",
    "contract_version",
    "source_count",
    "eligible_count",
    "target_present_count",
    "target_missing_count",
    "read_failed_count",
    "history_row_count",
)
_KEY = (
    "target_session",
    "contract_version",
    "calendar_revision",
    "source_capture_id",
    "price_coordinate",
)
_SYMBOL_COLUMNS = (
    "breadth_day_id",
    *_KEY,
    "source_symbol",
    "ret_t_numerator",
    "ret_t_denominator",
    "ret_t3_numerator",
    "ret_t3_denominator",
    "positive_t",
    "positive_t3",
    "delta_indicator",
    "classification",
    "primary_unknown_reason",
    "unknown_reason_detail",
)
_SCOPE = " AND ".join(f"{name}=%s" for name in _KEY)
_CHANGE_COLUMNS = (
    "before_session",
    "target_session",
    "before_source_capture_id",
    "target_source_capture_id",
    "before_calendar_id",
    "target_calendar_id",
    "before_calendar_revision",
    "target_calendar_revision",
    "before_contract_version",
    "target_contract_version",
    "before_price_coordinate",
    "target_price_coordinate",
    "comparison_status",
    "reason_code",
    "source_sets_equal",
    "eligible_sets_equal",
    "unknown_sets_equal",
    "unknown_reason_mismatch_count",
    "source_count_before",
    "source_count_target",
    "common_source_count",
    "eligible_count_before",
    "eligible_count_target",
    "common_eligible_count",
    "unknown_count_before",
    "unknown_count_target",
    "breadth_before_numerator",
    "breadth_before_denominator",
    "breadth_target_numerator",
    "breadth_target_denominator",
    "breadth_delta_numerator",
    "breadth_delta_denominator",
    "breadth_direction",
    "improved_symbol_count",
    "deteriorated_symbol_count",
    "unchanged_symbol_count",
    "unknown_symbol_count",
)
_CHANGE_KEY = (
    "before_session",
    "target_session",
    "target_source_capture_id",
    "target_calendar_revision",
    "target_contract_version",
)


class MarketBreadthPgError(Exception):
    """市场宽度业务事实未闭合或与既存事实冲突。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _clock(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _fraction(value: Any) -> tuple[int | None, int | None]:
    return (None, None) if value is None else (value.numerator, value.denominator)


def _scope(day: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(day[name] for name in _KEY)


def persist_market_breadth(
    config: Mapping[str, Any],
    market: Any,
    *,
    breadth_change: Mapping[str, Any],
) -> dict[str, Any]:
    """仅对序列化冲突重试完整单日事务。"""
    for attempt in range(5):
        try:
            return _persist_market_breadth_once(
                config, market, breadth_change=breadth_change
            )
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(random.uniform(0.1, 0.3) * (2**attempt))
    raise AssertionError("不可达的市场宽度事务重试状态")


def _persist_market_breadth_once(
    config: Mapping[str, Any], market: Any, *, breadth_change: Mapping[str, Any]
) -> dict[str, Any]:
    """一个SERIALIZABLE事务核验捕获并保存完整逐股市场宽度。"""
    cohort = market.cohort
    result = market.calculation
    target_session = market.target_session
    capture_id = cohort.source_capture_id
    if (
        type(target_session) is not date
        or target_session > market.research_cutoff
        or type(capture_id) is not int
        or capture_id <= 0
        or cohort.price_coordinate != "FORWARD_ADJUSTED"
        or result.n_source != len(result.symbols)
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_PARTITION_OPEN",
            "宽度日期、BaoStock捕获身份或来源分母未闭合",
        )
    if (
        breadth_change.get("target_session") != target_session
        or breadth_change.get("target_source_capture_id") != capture_id
        or breadth_change.get("target_calendar_id") != cohort.calendar_id
        or breadth_change.get("target_calendar_revision") != cohort.calendar_revision
        or breadth_change.get("target_contract_version") != result.contract_version
        or breadth_change.get("target_price_coordinate") != cohort.price_coordinate
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_TARGET_MISMATCH",
            "跨日宽度比较与本次目标日、捕获、日历、合同或坐标不一致",
        )
    if (
        result.n_source != cohort.price_row_count
        or len({item.source_symbol for item in result.symbols}) != result.n_source
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_SYMBOL_SET_INVALID", "宽度来源代码分母不唯一"
        )
    if (
        sum(result.counts.values()) != result.n_source
        or result.n_eligible != result.n_source - result.counts["UNKNOWN"]
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CLASS_PARTITION_OPEN", "宽度分类分母不闭合"
        )
    actual_counts = Counter(item.classification for item in result.symbols)
    actual_reasons = Counter(
        item.primary_unknown_reason
        for item in result.symbols
        if item.classification == "UNKNOWN"
    )
    if (
        any(
            actual_counts.get(name, 0) != count for name, count in result.counts.items()
        )
        or dict(actual_reasons) != result.unknown_reason_counts
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CLASS_PARTITION_OPEN", "逐股分类或未知原因未闭合到汇总"
        )
    window = result.session_window
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT provider,range_end_session,price_coordinates,
                      source_time_quality,source_available_at,read_started_at,
                      validated_at,source_clock_role
               FROM stock.source_captures WHERE capture_id=%s FOR SHARE""",
            (capture_id,),
        )
        capture = cursor.fetchone()
        if (
            capture is None
            or capture[0] != "BAOSTOCK"
            or capture[1] > market.research_cutoff
            or set(capture[2] or ()) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        ):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_CAPTURE_INVALID",
                "市场宽度必须绑定截止内BaoStock双坐标捕获",
            )
        day = (
            target_session,
            capture_id,
            cohort.price_coordinate,
            capture[3],
            _clock(capture[4]),
            _clock(capture[5]),
            _clock(capture[6]),
            capture[7],
            cohort.calendar_id,
            cohort.calendar_revision,
            *window[:4],
            result.contract_version,
            result.n_source,
            result.n_eligible,
            cohort.target_present_count,
            cohort.target_missing_count,
            cohort.read_failed_count,
            market.pg_result["history_row_count"],
        )
        key = tuple(day[_DAY_COLUMNS.index(name)] for name in _KEY)
        rows = [
            (
                *key,
                item.source_symbol,
                *_fraction(item.ret1_t),
                *_fraction(item.ret1_t_minus_3),
                item.positive_t,
                item.positive_t_minus_3,
                item.delta_indicator,
                item.classification,
                item.primary_unknown_reason,
                item.unknown_reason_detail,
            )
            for item in sorted(result.symbols, key=lambda value: value.source_symbol)
        ]
        cursor.execute(
            """SELECT source_symbol,source_capture_id FROM stock.source_raw_daily
               WHERE session_date=%s AND source_capture_id=%s
                 AND board IN ('MAIN_BOARD', 'GEM')
                 AND price_quality IS NOT NULL AND qfq_price_quality IS NOT NULL
               ORDER BY source_symbol""",
            (target_session, capture_id),
        )
        raw_rows = [
            {"source_symbol": row[0], "source_capture_id": row[1]}
            for row in cursor.fetchall()
        ]
        if (
            len(raw_rows) != result.n_source
            or {row["source_symbol"] for row in raw_rows}
            != {item.source_symbol for item in result.symbols}
            or {row["source_capture_id"] for row in raw_rows} != {capture_id}
        ):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_RAW_MISMATCH",
                "宽度事实与唯一BaoStock捕获、证券集合或完整分母不一致",
            )
        cursor.execute(
            f"SELECT id,{', '.join(_DAY_COLUMNS)} FROM {_DAY_TABLE} WHERE {_SCOPE}",
            key,
        )
        existing_day = cursor.fetchall()
        day_existed = bool(existing_day)
        if not existing_day:
            _insert_rows(cursor, _DAY_TABLE, _DAY_COLUMNS, _KEY, [day])
            cursor.execute(
                f"SELECT id,{', '.join(_DAY_COLUMNS)} FROM {_DAY_TABLE} WHERE {_SCOPE}",
                key,
            )
            existing_day = cursor.fetchall()
        if len(existing_day) != 1 or not _row_matches(day, existing_day[0][1:]):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_CONTENT_CONFLICT", "市场宽度父记录缺失或同键内容不一致"
            )
        parent_id = existing_day[0][0]
        rows = [(parent_id, *row) for row in rows]
        cursor.execute(
            f"SELECT {', '.join(_SYMBOL_COLUMNS)} FROM {_SYMBOL_TABLE} WHERE {_SCOPE} ORDER BY source_symbol",
            key,
        )
        existing_rows = cursor.fetchall()
        if day_existed and not existing_rows:
            raise MarketBreadthPgError(
                "MARKET_BREADTH_CONTENT_CONFLICT", "既存宽度父记录缺少逐股事实"
            )
        if existing_rows:
            if len(existing_rows) != len(rows) or any(
                not _row_matches(expected, actual)
                for expected, actual in zip(rows, existing_rows, strict=True)
            ):
                raise MarketBreadthPgError(
                    "MARKET_BREADTH_CONTENT_CONFLICT",
                    "既存宽度范围不完整或同一捕获业务键对应异内容",
                )
            inserted = 0
        else:
            inserted = _insert_rows(
                cursor,
                _SYMBOL_TABLE,
                _SYMBOL_COLUMNS,
                (*_KEY, "source_symbol"),
                rows,
            )
        cursor.execute(
            f"SELECT {', '.join(_SYMBOL_COLUMNS)} FROM {_SYMBOL_TABLE} WHERE {_SCOPE} ORDER BY source_symbol",
            key,
        )
        persisted = cursor.fetchall()
        if len(persisted) != len(rows) or any(
            not _row_matches(expected, actual)
            for expected, actual in zip(rows, persisted, strict=True)
        ):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_READBACK_MISMATCH", "宽度逐股事务内读回不一致"
            )
        change_persistence = _persist_market_breadth_change(
            cursor, breadth_change=breadth_change
        )
    return {
        "status": "WRITTEN" if inserted else "IDEMPOTENT_ALREADY_PRESENT",
        "target_session": target_session.isoformat(),
        "source_capture_id": capture_id,
        "rows": len(rows),
        "inserted_rows": inserted,
        "breadth_change": change_persistence,
        "transaction": "SERIALIZABLE_ATOMIC",
    }


def _change_row(
    value: Mapping[str, Any],
) -> tuple[Any, ...]:
    """把已校验的相邻日比较映射为固定的PG typed列。"""

    def ratio_parts(name: str) -> tuple[int | None, int | None]:
        ratio = value.get(name)
        if ratio is None:
            return None, None
        if not isinstance(ratio, Fraction):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_CHANGE_RATIO_INVALID",
                "宽度跨日变化必须使用精确Fraction",
            )
        return ratio.numerator, ratio.denominator

    status = value.get("comparison_status")
    reason = value.get("reason_code")
    before_capture = value.get("before_source_capture_id")
    target_capture = value.get("target_source_capture_id")
    if (
        status
        not in {"COMPARABLE", "NOT_COMPARABLE", "NOT_COMPUTED", "INCOMPLETE", "UNKNOWN"}
        or type(value.get("before_session")) is not date
        or type(value.get("target_session")) is not date
        or value["before_session"] >= value["target_session"]
        or (
            before_capture is not None
            and (type(before_capture) is not int or before_capture <= 0)
        )
        or type(target_capture) is not int
        or target_capture <= 0
        or (
            status == "COMPARABLE"
            and (reason is not None or before_capture != target_capture)
        )
        or (status != "COMPARABLE" and (not isinstance(reason, str) or not reason))
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_INPUT_INVALID",
            "市场宽度变化范围、状态或父事实引用不合法",
        )
    before_num, before_den = ratio_parts("breadth_before")
    target_num, target_den = ratio_parts("breadth_target")
    delta_num, delta_den = ratio_parts("breadth_delta")
    return (
        value["before_session"],
        value["target_session"],
        before_capture,
        target_capture,
        value.get("before_calendar_id"),
        value.get("target_calendar_id"),
        value.get("before_calendar_revision"),
        value.get("target_calendar_revision"),
        value.get("before_contract_version"),
        value.get("target_contract_version"),
        value.get("before_price_coordinate"),
        value.get("target_price_coordinate"),
        status,
        reason,
        value.get("source_sets_equal") is True,
        value.get("eligible_sets_equal") is True,
        value.get("unknown_sets_equal") is True,
        int(value.get("unknown_reason_mismatch_count", 0)),
        int(value.get("source_count_before", 0)),
        int(value["source_count_target"]),
        int(value.get("common_source_count", 0)),
        int(value.get("eligible_count_before", 0)),
        int(value["eligible_count_target"]),
        int(value.get("common_eligible_count", 0)),
        int(value.get("unknown_count_before", 0)),
        int(value["unknown_count_target"]),
        before_num,
        before_den,
        target_num,
        target_den,
        delta_num,
        delta_den,
        value.get("breadth_direction"),
        int(value.get("improved_symbol_count", 0)),
        int(value.get("deteriorated_symbol_count", 0)),
        int(value.get("unchanged_symbol_count", 0)),
        int(value.get("unknown_symbol_count", 0)),
    )


def _persist_market_breadth_change(
    cursor: Any,
    *,
    breadth_change: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(breadth_change, Mapping):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_INPUT_INVALID", "市场宽度变化缺少typed比较结果"
        )
    cursor.execute(
        """SELECT count(*) FROM source_market_breadth_days
           WHERE target_session=%s AND source_capture_id=%s AND calendar_id=%s
             AND calendar_revision=%s AND contract_version=%s
             AND price_coordinate=%s""",
        (
            breadth_change.get("target_session"),
            breadth_change.get("target_source_capture_id"),
            breadth_change.get("target_calendar_id"),
            breadth_change.get("target_calendar_revision"),
            breadth_change.get("target_contract_version"),
            breadth_change.get("target_price_coordinate"),
        ),
    )
    if cursor.fetchone() != (1,):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_TARGET_READBACK_MISMATCH",
            "相邻变化没有引用本事务内精确目标宽度父事实",
        )
    if breadth_change.get("comparison_status") == "COMPARABLE":
        cursor.execute(
            """SELECT count(*) FROM source_market_breadth_days
               WHERE target_session=%s AND source_capture_id=%s
                 AND calendar_id=%s AND calendar_revision=%s
                 AND contract_version=%s AND price_coordinate=%s""",
            (
                breadth_change["before_session"],
                breadth_change["before_source_capture_id"],
                breadth_change["before_calendar_id"],
                breadth_change.get("before_calendar_revision"),
                breadth_change.get("before_contract_version"),
                breadth_change.get("before_price_coordinate"),
            ),
        )
        if cursor.fetchone() != (1,):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_CHANGE_BEFORE_MISSING",
                "可比较变化缺少唯一精确前态宽度父事实",
            )
    row = _change_row(breadth_change)
    key = tuple(row[_CHANGE_COLUMNS.index(name)] for name in _CHANGE_KEY)
    predicate = " AND ".join(f"{name}=%s" for name in _CHANGE_KEY)
    cursor.execute(
        f"SELECT {', '.join(_CHANGE_COLUMNS)} FROM {_CHANGE_TABLE} WHERE {predicate}",
        key,
    )
    existing = cursor.fetchall()
    if not existing:
        _insert_rows(cursor, _CHANGE_TABLE, _CHANGE_COLUMNS, _CHANGE_KEY, [row])
    elif len(existing) != 1 or not _row_matches(row, existing[0]):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_CONTENT_CONFLICT",
            "相同相邻日宽度比较业务键已有不同或重复内容",
        )
    cursor.execute(
        f"SELECT {', '.join(_CHANGE_COLUMNS)} FROM {_CHANGE_TABLE} WHERE {predicate}",
        key,
    )
    readback = cursor.fetchall()
    if len(readback) != 1 or not _row_matches(row, readback[0]):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_READBACK_MISMATCH",
            "市场宽度变化事实事务内读回不一致",
        )
    return {
        "status": "WRITTEN" if not existing else "IDEMPOTENT_ALREADY_PRESENT",
        "comparison_status": breadth_change["comparison_status"],
        "before_session": breadth_change["before_session"].isoformat(),
        "target_session": breadth_change["target_session"].isoformat(),
        "rows": 1,
        "inserted_rows": int(not existing),
    }


def read_market_breadth(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    calendar_revision: str,
    contract_version: str,
    include_symbols: bool = False,
) -> dict[str, Any]:
    """只读完整持久化范围；不按其他版本回退或重算。"""
    if (
        type(target_session) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not calendar_revision
        or not contract_version
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_READ_ARGUMENT_INVALID",
            "市场宽度读取需要日期、来源捕获ID和显式算法修订",
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            "SELECT to_regclass('source_market_breadth_days') IS NOT NULL, "
            "to_regclass('source_market_breadth_symbols') IS NOT NULL"
        )
        if cursor.fetchone() != (True, True):
            return {
                "storage_status": "NOT_COMPUTED",
                "reason": "市场宽度事实迁移尚未应用",
            }
        cursor.execute(
            f"SELECT {', '.join(_DAY_COLUMNS)} FROM {_DAY_TABLE} "
            "WHERE target_session=%s AND source_capture_id=%s "
            "AND calendar_revision=%s AND contract_version=%s ORDER BY source_capture_id",
            (target_session, source_capture_id, calendar_revision, contract_version),
        )
        days = cursor.fetchall()
        if not days:
            return {
                "storage_status": "NOT_COMPUTED",
                "reason": "精确来源的市场宽度尚未计算",
            }
        if len(days) != 1:
            return {
                "storage_status": "INCOMPLETE",
                "reason": "市场宽度对应多个来源捕获或算法版本",
            }
        day = dict(zip(_DAY_COLUMNS, days[0], strict=True))
        key = _scope(day)
        cursor.execute(
            f"SELECT classification, count(*) FROM {_SYMBOL_TABLE} WHERE {_SCOPE} GROUP BY classification",
            key,
        )
        counts = {
            name: 0
            for name in (
                "COMPARABLE_GAIN",
                "COMPARABLE_LOSS",
                "COMPARABLE_BOTH_POSITIVE",
                "COMPARABLE_BOTH_NONPOSITIVE",
                "UNKNOWN",
            )
        }
        counts.update(dict(cursor.fetchall()))
        cursor.execute(
            f"SELECT count(*),count(DISTINCT source_symbol) FROM {_SYMBOL_TABLE} WHERE {_SCOPE}",
            key,
        )
        total, distinct = cursor.fetchone()
        if (
            total != day["source_count"]
            or distinct != total
            or total - counts["UNKNOWN"] != day["eligible_count"]
            or sum(counts.values()) != total
        ):
            return {
                "storage_status": "INCOMPLETE",
                "reason": "市场宽度逐股分类或来源分母不闭合",
            }
        cursor.execute(
            f"SELECT primary_unknown_reason,count(*) FROM {_SYMBOL_TABLE} WHERE {_SCOPE} AND classification='UNKNOWN' GROUP BY primary_unknown_reason ORDER BY primary_unknown_reason",
            key,
        )
        reasons = dict(cursor.fetchall())
        symbols = []
        if include_symbols:
            cursor.execute(
                f"SELECT {', '.join(_SYMBOL_COLUMNS)} FROM {_SYMBOL_TABLE} WHERE {_SCOPE} ORDER BY source_symbol",
                key,
            )
            symbols = [
                dict(zip(_SYMBOL_COLUMNS, row, strict=True))
                for row in cursor.fetchall()
            ]
        return {
            "storage_status": "SAVED",
            "day": day,
            "counts": counts,
            "unknown_reasons": reasons,
            "symbols": symbols,
        }


def read_market_breadth_change(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    calendar_revision: str,
    contract_version: str,
) -> dict[str, Any]:
    """只读目标会话已持久化的相邻宽度比较；不计算或回退其他版本。"""
    if (
        type(target_session) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or not isinstance(contract_version, str)
        or not contract_version
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_CHANGE_READ_ARGUMENT_INVALID",
            "宽度变化读取需要目标日、捕获、日历修订和算法合同",
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (_CHANGE_TABLE,))
        if cursor.fetchone() != (True,):
            return {
                "storage_status": "NOT_COMPUTED",
                "reason_code": "MARKET_BREADTH_CHANGE_MIGRATION_NOT_APPLIED",
            }
        cursor.execute(
            f"SELECT {', '.join(_CHANGE_COLUMNS)} FROM {_CHANGE_TABLE} "
            "WHERE target_session=%s AND target_source_capture_id=%s "
            "AND target_calendar_revision=%s AND target_contract_version=%s "
            "ORDER BY before_session",
            (target_session, source_capture_id, calendar_revision, contract_version),
        )
        rows = cursor.fetchall()
        parent_scopes_complete = None
        if len(rows) == 1:
            change_row = dict(zip(_CHANGE_COLUMNS, rows[0], strict=True))
            cursor.execute(
                """SELECT count(*) FROM source_market_breadth_days
                   WHERE target_session=%s AND source_capture_id=%s
                     AND calendar_id=%s AND calendar_revision=%s
                     AND contract_version=%s AND price_coordinate='FORWARD_ADJUSTED'
                     AND source_count=%s AND eligible_count=%s""",
                (
                    target_session,
                    source_capture_id,
                    change_row["target_calendar_id"],
                    change_row["target_calendar_revision"],
                    change_row["target_contract_version"],
                    change_row["source_count_target"],
                    change_row["eligible_count_target"],
                ),
            )
            target_parent_exists = cursor.fetchone()[0] == 1
            before_parent_exists = True
            if change_row["comparison_status"] == "COMPARABLE":
                cursor.execute(
                    """SELECT count(*) FROM source_market_breadth_days
                       WHERE target_session=%s AND source_capture_id=%s
                         AND calendar_id=%s AND calendar_revision=%s
                         AND contract_version=%s AND price_coordinate='FORWARD_ADJUSTED'
                         AND source_count=%s AND eligible_count=%s""",
                    (
                        change_row["before_session"],
                        change_row["before_source_capture_id"],
                        change_row["before_calendar_id"],
                        change_row["before_calendar_revision"],
                        change_row["before_contract_version"],
                        change_row["source_count_before"],
                        change_row["eligible_count_before"],
                    ),
                )
                before_parent_exists = cursor.fetchone()[0] == 1
            parent_scopes_complete = target_parent_exists and before_parent_exists
    if not rows:
        return {
            "storage_status": "NOT_COMPUTED",
            "reason_code": "MARKET_BREADTH_CHANGE_NOT_SAVED_FOR_EXACT_TARGET",
        }
    if len(rows) != 1:
        return {
            "storage_status": "INCOMPLETE",
            "reason_code": "MARKET_BREADTH_CHANGE_SCOPE_AMBIGUOUS",
        }
    change = dict(zip(_CHANGE_COLUMNS, rows[0], strict=True))
    if (
        change["target_session"] != target_session
        or change["target_source_capture_id"] != source_capture_id
        or change["target_calendar_revision"] != calendar_revision
        or change["target_contract_version"] != contract_version
    ):
        return {
            "storage_status": "INCOMPLETE",
            "reason_code": "MARKET_BREADTH_CHANGE_IDENTITY_MISMATCH",
        }
    status = change["comparison_status"]
    if parent_scopes_complete is not True:
        return {
            "storage_status": "INCOMPLETE",
            "reason_code": "MARKET_BREADTH_CHANGE_PARENT_SCOPE_MISSING",
        }
    if status == "COMPARABLE":
        before_num = int(change["breadth_before_numerator"])
        before_den = int(change["breadth_before_denominator"])
        target_num = int(change["breadth_target_numerator"])
        target_den = int(change["breadth_target_denominator"])
        delta_num = int(change["breadth_delta_numerator"])
        delta_den = int(change["breadth_delta_denominator"])
        if (
            change["before_source_capture_id"] != source_capture_id
            or change["before_calendar_id"] != change["target_calendar_id"]
            or change["before_calendar_revision"] != calendar_revision
            or change["before_contract_version"] != contract_version
            or change["before_price_coordinate"] != "FORWARD_ADJUSTED"
            or change["target_price_coordinate"] != "FORWARD_ADJUSTED"
            or change["source_sets_equal"] is not True
            or change["eligible_sets_equal"] is not True
            or change["unknown_sets_equal"] is not True
            or change["unknown_reason_mismatch_count"] != 0
            or change["source_count_before"] != change["source_count_target"]
            or change["common_source_count"] != change["source_count_target"]
            or change["eligible_count_before"] != change["eligible_count_target"]
            or change["common_eligible_count"] != change["eligible_count_target"]
            or change["improved_symbol_count"]
            + change["deteriorated_symbol_count"]
            + change["unchanged_symbol_count"]
            != change["eligible_count_target"]
            or change["unknown_symbol_count"] != change["unknown_count_target"]
            or Fraction(target_num, target_den) - Fraction(before_num, before_den)
            != Fraction(delta_num, delta_den)
            or change["breadth_direction"]
            != ("UP" if delta_num > 0 else "DOWN" if delta_num < 0 else "UNCHANGED")
            or change["reason_code"] is not None
        ):
            return {
                "storage_status": "INCOMPLETE",
                "reason_code": "MARKET_BREADTH_CHANGE_PARTITION_INVALID",
            }
    elif status in {"NOT_COMPARABLE", "NOT_COMPUTED", "INCOMPLETE", "UNKNOWN"}:
        if not isinstance(change.get("reason_code"), str) or not change["reason_code"]:
            return {
                "storage_status": "INCOMPLETE",
                "reason_code": "MARKET_BREADTH_CHANGE_REASON_MISSING",
            }
        if (
            change["breadth_delta_numerator"] is not None
            or change["breadth_delta_denominator"] is not None
        ):
            return {
                "storage_status": "INCOMPLETE",
                "reason_code": "MARKET_BREADTH_CHANGE_UNCOMPARABLE_HAS_DELTA",
            }
    else:
        return {
            "storage_status": "INCOMPLETE",
            "reason_code": "MARKET_BREADTH_CHANGE_STATUS_INVALID",
        }
    return {"storage_status": "SAVED", "change": change}
