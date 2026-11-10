"""现役固定来源篮市场宽度的typed日事实持久化。"""

from __future__ import annotations

from datetime import datetime
from collections import Counter
import random
import time
from typing import Any, Mapping

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_DAY_COLUMNS = (
    "namespace_id",
    "target_session",
    "package_sha256",
    "source_capture_id",
    "source_content_id",
    "source_dataset_id",
    "source_byte_count",
    "parser_contract",
    "source_time_quality",
    "source_available_at",
    "capture_read_started_at",
    "capture_validated_at",
    "source_clock_role",
    "capture_digest",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
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
    "namespace_id",
    "target_session",
    "package_sha256",
    "source_capture_id",
    "calendar_revision",
    "calendar_sha256",
    "contract_version",
)
_SYMBOL_COLUMNS = (
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
    if market.target_session > market.research_cutoff or result.n_source != len(
        result.symbols
    ):
        raise MarketBreadthPgError(
            "MARKET_BREADTH_PARTITION_OPEN", "宽度日期或来源分母不闭合"
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
    w = result.session_window
    day = (
        cohort.namespace_id,
        market.target_session,
        cohort.package_sha256,
        cohort.source_capture_id,
        cohort.source_content_id,
        cohort.source_dataset_id,
        cohort.source_byte_count,
        cohort.parser_contract,
        cohort.source_time_quality,
        _clock(cohort.source_available_at),
        _clock(cohort.capture_read_started_at),
        _clock(cohort.capture_validated_at),
        cohort.source_clock_role,
        cohort.capture_digest,
        cohort.calendar_id,
        cohort.calendar_revision,
        cohort.calendar_sha256,
        *w[:4],
        result.contract_version,
        result.n_source,
        result.n_eligible,
        cohort.target_present_count,
        cohort.target_missing_count,
        cohort.read_failed_count,
        market.pg_result["history_row_count"],
    )
    key = tuple(day[_DAY_COLUMNS.index(name)] for name in _KEY)
    rows = []
    for item in sorted(result.symbols, key=lambda x: x.source_symbol):
        rows.append(
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
        )
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        namespace_id,
    ):
        if namespace_id != cohort.namespace_id:
            raise MarketBreadthPgError(
                "MARKET_BREADTH_NAMESPACE_MISMATCH", "宽度事实命名空间不匹配"
            )
        raw_rows = source_raw._read_raw_rows(
            cursor, namespace_id, market.target_session, cohort.package_sha256
        )
        if (
            len(raw_rows) != result.n_source
            or source_raw._row_digest(raw_rows) != cohort.capture_digest
            or {row["source_symbol"] for row in raw_rows}
            != {x.source_symbol for x in result.symbols}
            or {str(row["source_capture_id"]) for row in raw_rows}
            != {cohort.source_capture_id}
        ):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_RAW_MISMATCH", "宽度事实与目标Raw捕获或分母不一致"
            )
        cursor.execute(
            f"SELECT {', '.join(_DAY_COLUMNS)} FROM stock.source_market_breadth_days WHERE {_SCOPE}",
            key,
        )
        existing_day = cursor.fetchall()
        cursor.execute(
            f"SELECT {', '.join(_SYMBOL_COLUMNS)} FROM stock.source_market_breadth_symbols WHERE {_SCOPE} ORDER BY source_symbol",
            key,
        )
        existing_rows = cursor.fetchall()
        if existing_day or existing_rows:
            if (
                len(existing_day) != 1
                or not _row_matches(day, existing_day[0])
                or len(existing_rows) != len(rows)
                or any(
                    not _row_matches(a, b)
                    for a, b in zip(rows, existing_rows, strict=True)
                )
            ):
                raise MarketBreadthPgError(
                    "MARKET_BREADTH_CONTENT_CONFLICT",
                    "既存宽度范围不完整或同键内容不同",
                )
            inserted = 0
        else:
            _insert_rows(
                cursor, "stock.source_market_breadth_days", _DAY_COLUMNS, _KEY, [day]
            )
            inserted = _insert_rows(
                cursor,
                "stock.source_market_breadth_symbols",
                _SYMBOL_COLUMNS,
                (*_KEY, "source_symbol"),
                rows,
            )
        cursor.execute(
            f"SELECT {', '.join(_SYMBOL_COLUMNS)} FROM stock.source_market_breadth_symbols WHERE {_SCOPE} ORDER BY source_symbol",
            key,
        )
        actual = cursor.fetchall()
        if len(actual) != len(rows) or any(
            not _row_matches(a, b) for a, b in zip(rows, actual, strict=True)
        ):
            raise MarketBreadthPgError(
                "MARKET_BREADTH_READBACK_MISMATCH", "宽度逐股事务内读回不一致"
            )
    return {
        "status": "WRITTEN" if inserted else "IDEMPOTENT_ALREADY_PRESENT",
        "target_session": market.target_session.isoformat(),
        "rows": len(rows),
        "inserted_rows": inserted,
        "transaction": "SERIALIZABLE_ATOMIC",
    }


def read_market_breadth(
    config: Mapping[str, Any],
    *,
    target_session: Any,
    package_sha256: str,
    calendar_revision: str,
    calendar_sha256: str,
    contract_version: str,
    include_symbols: bool = False,
) -> dict[str, Any]:
    """只读完整持久化范围；不按其他版本回退或重算。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        namespace_id,
    ):
        cursor.execute(
            "SELECT to_regclass('stock.source_market_breadth_days') IS NOT NULL, to_regclass('stock.source_market_breadth_symbols') IS NOT NULL"
        )
        if cursor.fetchone() != (True, True):
            return {
                "storage_status": "NOT_COMPUTED",
                "reason": "市场宽度事实迁移尚未应用",
            }
        cursor.execute(
            f"SELECT {', '.join(_DAY_COLUMNS)} FROM stock.source_market_breadth_days WHERE namespace_id=%s AND target_session=%s AND package_sha256=%s AND calendar_revision=%s AND calendar_sha256=%s AND contract_version=%s ORDER BY source_capture_id",
            (
                namespace_id,
                target_session,
                package_sha256,
                calendar_revision,
                calendar_sha256,
                contract_version,
            ),
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
                "reason": "市场宽度对应多个来源或算法版本",
            }
        day = dict(zip(_DAY_COLUMNS, days[0], strict=True))
        key = tuple(day[name] for name in _KEY)
        cursor.execute(
            f"SELECT classification, count(*) FROM stock.source_market_breadth_symbols WHERE {_SCOPE} GROUP BY classification",
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
            f"SELECT count(*),count(DISTINCT source_symbol) FROM stock.source_market_breadth_symbols WHERE {_SCOPE}",
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
            f"SELECT primary_unknown_reason,count(*) FROM stock.source_market_breadth_symbols WHERE {_SCOPE} AND classification='UNKNOWN' GROUP BY primary_unknown_reason ORDER BY primary_unknown_reason",
            key,
        )
        reasons = dict(cursor.fetchall())
        symbols = []
        if include_symbols:
            cursor.execute(
                f"SELECT {', '.join(_SYMBOL_COLUMNS)} FROM stock.source_market_breadth_symbols WHERE {_SCOPE} ORDER BY source_symbol",
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
