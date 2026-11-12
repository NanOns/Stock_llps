"""现役固定来源篮市场宽度的typed日事实持久化。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
import random
import time
from typing import Any, Mapping

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_DAY_TABLE = "source_market_breadth_days"
_SYMBOL_TABLE = "source_market_breadth_symbols"
_DAY_COLUMNS = (
    "target_session",
    "source_package_id",
    "source_capture_id",
    "source_dataset_id",
    "source_byte_count",
    "parser_contract",
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
    "source_package_id",
    "target_session",
    "contract_version",
    "calendar_revision",
    "source_capture_id",
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


def persist_market_breadth(config: Mapping[str, Any], market: Any) -> dict[str, Any]:
    """仅对序列化冲突重试完整单日事务。"""
    for attempt in range(5):
        try:
            return _persist_market_breadth_once(config, market)
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(random.uniform(0.1, 0.3) * (2**attempt))
    raise AssertionError("不可达的市场宽度事务重试状态")


def _persist_market_breadth_once(
    config: Mapping[str, Any], market: Any
) -> dict[str, Any]:
    """一个可重试事务保存日范围及完整逐股分类，同键异内容回滚。"""
    cohort = market.cohort
    result = market.calculation
    target_session = market.target_session
    package_id = cohort.source_package_id
    capture_id = cohort.source_capture_id
    if (
        type(target_session) is not date
        or target_session > market.research_cutoff
        or type(package_id) is not int
        or package_id <= 0
        or type(capture_id) is not int
        or capture_id <= 0
        or result.n_source != len(result.symbols)
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_PARTITION_OPEN", "宽度日期、数字来源身份或来源分母不闭合"
        )
    if (
        result.n_source != cohort.raw_row_count
        or len({x.source_symbol for x in result.symbols}) != result.n_source
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
    day = (
        target_session,
        package_id,
        capture_id,
        cohort.source_dataset_id,
        cohort.source_byte_count,
        cohort.parser_contract,
        cohort.source_time_quality,
        _clock(cohort.source_available_at),
        _clock(cohort.capture_read_started_at),
        _clock(cohort.capture_validated_at),
        cohort.source_clock_role,
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
        for item in sorted(result.symbols, key=lambda x: x.source_symbol)
    ]
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        raw_rows = source_raw._read_raw_rows(cursor, target_session, package_id)
        if (
            len(raw_rows) != result.n_source
            or {row["source_symbol"] for row in raw_rows}
            != {item.source_symbol for item in result.symbols}
            or {row["source_capture_id"] for row in raw_rows} != {capture_id}
        ):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_RAW_MISMATCH",
                "宽度事实与数字来源包/捕获、证券集合或完整分母不一致",
            )
        cursor.execute(
            f"SELECT id, {', '.join(_DAY_COLUMNS)} FROM {_DAY_TABLE} WHERE {_SCOPE}",
            key,
        )
        existing_day = cursor.fetchall()
        day_existed = bool(existing_day)
        if not existing_day:
            _insert_rows(cursor, _DAY_TABLE, _DAY_COLUMNS, _KEY, [day])
            cursor.execute(
                f"SELECT id, {', '.join(_DAY_COLUMNS)} FROM {_DAY_TABLE} WHERE {_SCOPE}",
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
                    "既存宽度范围不完整或同一数字业务键对应异内容",
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
    return {
        "status": "WRITTEN" if inserted else "IDEMPOTENT_ALREADY_PRESENT",
        "target_session": target_session.isoformat(),
        "source_package_id": package_id,
        "source_capture_id": capture_id,
        "rows": len(rows),
        "inserted_rows": inserted,
        "transaction": "SERIALIZABLE_ATOMIC",
    }


def read_market_breadth(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    calendar_revision: str,
    contract_version: str,
    include_symbols: bool = False,
) -> dict[str, Any]:
    """只读完整持久化范围；不按其他版本回退或重算。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or not calendar_revision
        or not contract_version
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_READ_ARGUMENT_INVALID",
            "市场宽度读取需要日期、数字包ID和显式算法修订",
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
            "WHERE target_session=%s AND source_package_id=%s "
            "AND calendar_revision=%s AND contract_version=%s ORDER BY source_capture_id",
            (target_session, source_package_id, calendar_revision, contract_version),
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
