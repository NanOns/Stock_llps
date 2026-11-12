"""本机中文显示标签的当前PG投影与有界读取。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import re
from typing import Any

from stock_a.storage.postgres import source_raw


_TABLE = "source_symbol_display_names"
_COLUMNS = (
    "source_symbol",
    "market",
    "display_name",
    "source_file",
    "parser_contract",
    "source_authority",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "captured_at",
    "identity_status",
    "pit_status",
)
_SYMBOL = re.compile(r"(SH|SZ|BJ)\.[0-9]{6}\Z")
_SOURCE_FILES = {"SH": "shs.tnf", "SZ": "szs.tnf", "BJ": "bjs.tnf"}
_PARSER_CONTRACT = "TDX_TNF_DISPLAY_NAME_R1"
_SOURCE_AUTHORITY = "UNVERIFIED_LOCAL_TDX_INSTALLATION"
_SOURCE_TIME_QUALITY = "NON_PIT_OBSERVED"
_SOURCE_CLOCK_ROLE = "OBSERVED_CAPTURED"
_IDENTITY_STATUS = "SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY"


class SourceSymbolDisplayNameError(ValueError):
    """名称写入或读取未满足精确标签合同。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _rows(snapshot) -> list[tuple[Any, ...]]:
    rows = []
    for item in snapshot.rows:
        match = _SYMBOL.fullmatch(item.source_symbol)
        if (
            match is None
            or item.market != match.group(1)
            or _SOURCE_FILES.get(item.market) != item.source_file
            or not isinstance(item.display_name, str)
            or not item.display_name.strip()
            or len(item.display_name) > 40
        ):
            raise SourceSymbolDisplayNameError(
                "DISPLAY_NAME_ROW_INVALID", "来源显示标签行不符合业务键或值域合同"
            )
        rows.append(
            (
                item.source_symbol,
                item.market,
                item.display_name,
                item.source_file,
                _PARSER_CONTRACT,
                _SOURCE_AUTHORITY,
                _SOURCE_TIME_QUALITY,
                _SOURCE_CLOCK_ROLE,
                None,
                snapshot.captured_at,
                _IDENTITY_STATUS,
                "NON_PIT",
            )
        )
    rows.sort(key=lambda row: row[0])
    if not rows or len(rows) > 100000 or len({row[0] for row in rows}) != len(rows):
        raise SourceSymbolDisplayNameError(
            "DISPLAY_NAME_PARTITION_INVALID",
            "名称快照为空、超过10万条或含重复source_symbol",
        )
    return rows


def persist_source_symbol_display_names(
    config: Mapping[str, Any], snapshot
) -> dict[str, Any]:
    """整体替换唯一当前显示标签快照，并在同一事务中完整读回。"""
    rows = _rows(snapshot)
    expected = [(row[0], row[2], row[3]) for row in rows]
    previous_count = 0
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"stock.{_TABLE}",))
        if cursor.fetchone() != (True,):
            raise SourceSymbolDisplayNameError(
                "DISPLAY_NAME_MIGRATION_MISSING", "0027名称标签表尚未迁移"
            )
        cursor.execute(f"SELECT count(*) FROM {_TABLE}")
        previous_count = cursor.fetchone()[0]
        cursor.execute(f"DELETE FROM {_TABLE}")
        for start in range(0, len(rows), 500):
            batch = rows[start : start + 500]
            row_placeholders = "(" + ",".join("%s" for _ in _COLUMNS) + ")"
            values_sql = ",".join(row_placeholders for _ in batch)
            parameters = tuple(value for row in batch for value in row)
            cursor.execute(
                f"INSERT INTO {_TABLE} ({', '.join(_COLUMNS)}) VALUES {values_sql}",
                parameters,
            )
        cursor.execute(
            f"SELECT source_symbol, display_name, source_file FROM {_TABLE} "
            "ORDER BY source_symbol"
        )
        actual = cursor.fetchall()
        if actual != expected:
            raise SourceSymbolDisplayNameError(
                "DISPLAY_NAME_READBACK_MISMATCH",
                "名称标签表事务内完整读回与本次来源观察不一致",
            )
    return {
        "status": "WRITTEN_AND_FULL_READBACK_VERIFIED",
        "previous_rows_replaced": previous_count,
        "rows_written": len(rows),
        "rows_readback_verified": len(expected),
        "captured_at": snapshot.captured_at.isoformat(),
        "source_file_sizes": snapshot.source_file_sizes,
        "parsed_record_counts": snapshot.parsed_record_counts,
        "rejected_truncated_name_counts": snapshot.rejected_truncated_name_counts,
        "source_time_quality": _SOURCE_TIME_QUALITY,
        "source_clock_role": _SOURCE_CLOCK_ROLE,
        "source_available_at": None,
        "source_authority": _SOURCE_AUTHORITY,
        "identity_status": _IDENTITY_STATUS,
        "pit_status": "NON_PIT",
        "transaction": "SERIALIZABLE_ATOMIC_CURRENT_SNAPSHOT_REPLACE",
        "ddl_executed": False,
    }


def read_source_symbol_display_names(
    config: Mapping[str, Any], *, source_symbols: Sequence[str]
) -> dict[str, Any]:
    """按明确来源代码读取最新标签；不推导证券实体、日期或上市状态。"""
    if (
        isinstance(source_symbols, (str, bytes))
        or not isinstance(source_symbols, Sequence)
        or len(source_symbols) > 10000
        or any(
            not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None
            for symbol in source_symbols
        )
    ):
        raise SourceSymbolDisplayNameError(
            "DISPLAY_NAME_READ_SCOPE_INVALID", "名称读取须绑定不超过10000个来源代码"
        )
    requested = tuple(dict.fromkeys(source_symbols))
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            "SELECT count(*), min(captured_at), max(captured_at), "
            "min(parser_contract), min(source_authority), min(source_time_quality), "
            "min(source_clock_role), min(identity_status), min(pit_status), "
            "count(DISTINCT captured_at) "
            f"FROM {_TABLE}"
        )
        (
            row_count,
            first_capture,
            last_capture,
            parser_contract,
            source_authority,
            time_quality,
            clock_role,
            identity_status,
            pit_status,
            capture_count,
        ) = cursor.fetchone()
        if row_count and capture_count != 1:
            raise SourceSymbolDisplayNameError(
                "DISPLAY_NAME_SNAPSHOT_AMBIGUOUS", "当前标签表不是单一原子观察快照"
            )
        if requested:
            cursor.execute(
                f"SELECT source_symbol, display_name FROM {_TABLE} "
                "WHERE source_symbol = ANY(%s) ORDER BY source_symbol",
                (list(requested),),
            )
            names = dict(cursor.fetchall())
        else:
            names = {}
    return {
        "status": "AVAILABLE" if row_count else "NOT_COMPUTED",
        "names": names,
        "requested_count": len(requested),
        "matched_count": len(names),
        "stored_name_count": row_count,
        "captured_at": last_capture.isoformat() if last_capture else None,
        "source_time_quality": time_quality or _SOURCE_TIME_QUALITY,
        "source_clock_role": clock_role or _SOURCE_CLOCK_ROLE,
        "source_available_at": None,
        "source_authority": source_authority or _SOURCE_AUTHORITY,
        "identity_status": identity_status or _IDENTITY_STATUS,
        "pit_status": pit_status or "NON_PIT",
        "parser_contract": parser_contract or _PARSER_CONTRACT,
        "snapshot_start": first_capture.isoformat() if first_capture else None,
    }


def decorate_source_symbol_display_names(
    config: Mapping[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    """给实际API结果的来源代码附加中文显示名；缺失继续明确为UNKNOWN。"""
    from copy import deepcopy

    result = deepcopy(dict(payload))
    symbols: set[str] = set()

    def collect(value: Any) -> None:
        if isinstance(value, Mapping):
            for key in ("source_symbol", "symbol"):
                candidate = value.get(key)
                if isinstance(candidate, str) and _SYMBOL.fullmatch(candidate):
                    symbols.add(candidate)
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(result)
    context = read_source_symbol_display_names(
        config, source_symbols=tuple(sorted(symbols))
    )

    def decorate(value: Any) -> None:
        if isinstance(value, dict):
            symbol = value.get("source_symbol")
            if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
                symbol = value.get("symbol")
            if isinstance(symbol, str) and _SYMBOL.fullmatch(symbol):
                value["display_name"] = context["names"].get(symbol)
                value["display_name_status"] = (
                    "AVAILABLE" if symbol in context["names"] else "UNKNOWN"
                )
            for child in value.values():
                decorate(child)
        elif isinstance(value, list):
            for child in value:
                decorate(child)

    decorate(result)
    result["display_name_context"] = {
        key: context[key]
        for key in (
            "status",
            "captured_at",
            "stored_name_count",
            "source_time_quality",
            "source_clock_role",
            "source_available_at",
            "source_authority",
            "identity_status",
            "pit_status",
            "parser_contract",
        )
    }
    return result


__all__ = [
    "SourceSymbolDisplayNameError",
    "decorate_source_symbol_display_names",
    "persist_source_symbol_display_names",
    "read_source_symbol_display_names",
]
