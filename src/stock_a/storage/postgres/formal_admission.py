"""LAUNCH正式事件准入结果的PG读写边界。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
import re
from typing import Any

from stock_a.contracts.profile_revision import (
    PROFILE_REVISION_PATTERN,
    profile_revision_history_session_count,
)
from . import source_raw


_TABLE = "source_d0_formal_admission_day_facts"
_D0_LAUNCH_PRODUCER_VERSION = "STOCK_A_D0_LAUNCH_BAOSTOCK_READER_R5"
_SEED_PRODUCER_VERSION = "SOURCE_POOL_SEED_CORE_PROFILE_R2"
_ADMISSION_CONTRACT = "STOCK_A_D0_EVENT_ADMISSION_R1"
_STATES = ("READY", "BLOCKED", "NOT_READY", "NOT_REQUIRED")
_GATE_COLUMNS = {
    "research_universe": ("research_universe_state", "research_universe_reason"),
    "actual_quote": ("actual_quote_state", "actual_quote_reason"),
    "coordinate_window": ("coordinate_window_state", "coordinate_window_reason"),
    "d0_liquidity": ("d0_liquidity_state", "d0_liquidity_reason"),
    "non_core_damage": ("non_core_damage_state", "non_core_damage_reason"),
    "non_extreme_extension": (
        "non_extreme_extension_state",
        "non_extreme_extension_reason",
    ),
    "non_first_day_damage": (
        "non_first_day_damage_state",
        "non_first_day_damage_reason",
    ),
    "non_severe_drop": ("non_severe_drop_state", "non_severe_drop_reason"),
}
_ROW_COLUMNS = (
    "raw_fact_id",
    "seed_fact_id",
    "source_capture_id",
    "target_session",
    "source_symbol",
    "scenario",
    "raw_role",
    "raw_state",
    "raw_reason",
    "raw_producer_version",
    "algorithm_contract",
    "session_contract",
    "history_window_start",
    "history_session_count",
    "source_count",
    "calendar_id",
    "calendar_revision",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "capture_started_at",
    "capture_validated_at",
    "profile_revision",
    "seed_producer_version",
    "identity_scope",
    "research_mode",
    "price_coordinate",
    "admission_contract",
    "admission_status",
    "admission_reason",
    "research_universe_state",
    "research_universe_reason",
    "actual_quote_state",
    "actual_quote_reason",
    "coordinate_window_state",
    "coordinate_window_reason",
    "d0_liquidity_state",
    "d0_liquidity_reason",
    "non_core_damage_state",
    "non_core_damage_reason",
    "non_extreme_extension_state",
    "non_extreme_extension_reason",
    "non_first_day_damage_state",
    "non_first_day_damage_reason",
    "non_severe_drop_state",
    "non_severe_drop_reason",
    "reason_codes",
    "event_id",
    "failure_level_numerator",
    "failure_level_denominator",
    "failure_coordinate",
    "failure_source",
    "failure_window_start",
    "failure_window_session_count",
    "failure_source_event_ref",
    "effective_from_session",
)
_UNIQUE_COLUMNS = (
    "target_session",
    "source_capture_id",
    "source_symbol",
    "scenario",
    "raw_producer_version",
    "algorithm_contract",
    "session_contract",
    "calendar_revision",
)


class FormalAdmissionPgError(ValueError):
    """正式准入PG输入不完整、含冲突或读取失败。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def list_saved_launch_sessions(
    config: Mapping[str, Any],
    *,
    start_session: date,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
) -> list[date]:
    """只枚举截止日内已有的LAUNCH D0分区，不扩展或回填日期。"""
    if (
        type(start_session) is not date
        or type(target_session) is not date
        or type(research_cutoff) is not date
        or start_session > target_session
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise ValueError("正式准入日期范围、截止或来源包编号无效")
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT DISTINCT target_session
               FROM source_d0_scenario_day_facts
               WHERE scenario='LAUNCH_CONFIRM' AND source_capture_id=%s
                 AND producer_version=%s
                 AND target_session >= %s AND target_session <= %s
                 AND target_session <= %s
               ORDER BY target_session""",
            (
                source_capture_id,
                _D0_LAUNCH_PRODUCER_VERSION,
                start_session,
                target_session,
                research_cutoff,
            ),
        )
        return [row[0] for row in cursor.fetchall()]


def read_saved_source_fact_ids(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    producer_version: str,
    algorithm_contract: str,
    session_contract: str,
    calendar_revision: str,
    profile_revision: str | None,
    seed_producer_version: str | None,
    expected_source_count: int,
) -> dict[str, dict[str, Any]]:
    """为完整的D0与Core/Profile输入绑定真实PG事实行编号。"""
    if producer_version != _D0_LAUNCH_PRODUCER_VERSION or seed_producer_version not in {
        None,
        _SEED_PRODUCER_VERSION,
    }:
        raise FormalAdmissionPgError(
            "FORMAL_ADMISSION_PRODUCER_VERSION_UNSUPPORTED",
            "正式准入读取只接受现役D0 Launch R3与Seed R2生产版本",
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT d.id, d.source_symbol,
                      s.id, s.profile_revision, s.producer_version
               FROM source_d0_scenario_day_facts d
               LEFT JOIN source_pool_seed_day_facts s
                 ON s.target_session=d.target_session
                AND s.source_symbol=d.source_symbol
                AND s.source_capture_id=d.source_capture_id
                AND s.profile_revision=%s
                AND s.producer_version=%s
               WHERE d.target_session=%s AND d.source_capture_id=%s AND d.scenario='LAUNCH_CONFIRM'
                 AND d.producer_version=%s AND d.algorithm_contract=%s
                 AND d.session_contract=%s AND d.calendar_revision=%s
               ORDER BY d.source_symbol""",
            (
                profile_revision,
                seed_producer_version,
                target_session,
                source_capture_id,
                producer_version,
                algorithm_contract,
                session_contract,
                calendar_revision,
            ),
        )
        rows = cursor.fetchall()
    if len(rows) != expected_source_count:
        raise FormalAdmissionPgError(
            "FORMAL_ADMISSION_RAW_PARTITION_INCOMPLETE",
            "精确LAUNCH原始事实行未闭合到来源池分母",
        )
    result: dict[str, dict[str, Any]] = {}
    for raw_id, symbol, seed_id, profile, seed_version in rows:
        if symbol in result:
            raise FormalAdmissionPgError(
                "FORMAL_ADMISSION_RAW_SYMBOL_DUPLICATED",
                "精确LAUNCH输入中来源代码重复",
            )
        result[symbol] = {
            "raw_fact_id": int(raw_id),
            "seed_fact_id": int(seed_id) if seed_id is not None else None,
            "profile_revision": profile,
            "seed_producer_version": seed_version,
        }
    return result


def _validate_row(row: Mapping[str, Any]) -> None:
    if set(row) != set(_ROW_COLUMNS):
        missing = sorted(set(_ROW_COLUMNS) - set(row))
        extra = sorted(set(row) - set(_ROW_COLUMNS))
        raise ValueError(
            f"正式准入PG行字段集合不符合typed合同：缺少{missing}，多余{extra}"
        )
    if row["admission_status"] not in _STATES:
        raise ValueError("正式准入状态不在冻结枚举内")
    if row["raw_producer_version"] != _D0_LAUNCH_PRODUCER_VERSION:
        raise ValueError("正式准入只接受当前D0 Launch R3原始事实")
    if row["seed_producer_version"] not in {None, _SEED_PRODUCER_VERSION}:
        raise ValueError("正式准入只接受当前Seed R2画像事实")
    if row["raw_role"] not in {"FORMAL", "DIAGNOSTIC"}:
        raise ValueError("LAUNCH原始事实角色必须显式为FORMAL或DIAGNOSTIC")
    if row["raw_state"] not in {"TRUE", "FALSE", "UNKNOWN"}:
        raise ValueError("LAUNCH原始事实必须保留三态")
    if row["raw_state"] == "FALSE" and row["admission_status"] != "NOT_REQUIRED":
        raise ValueError("原始FALSE必须保存为NOT_REQUIRED")
    if row["raw_state"] != "FALSE" and row["admission_status"] == "NOT_REQUIRED":
        raise ValueError("非FALSE原始事实不能保存为NOT_REQUIRED")


def persist_source_formal_admission_rows(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    expected_source_count: int,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """事务内幂等写入完整来源池；同键异内容拒绝并回滚。"""
    if (
        type(target_session) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or len(rows) != expected_source_count
    ):
        raise ValueError("正式准入事务必须绑定完整单日来源池分母")
    for row in rows:
        _validate_row(row)
        if (
            row["target_session"] != target_session
            or row["source_capture_id"] != source_capture_id
            or row["source_count"] != expected_source_count
        ):
            raise ValueError("正式准入PG行与完整单日输入键不一致")
    if len({row["source_symbol"] for row in rows}) != expected_source_count:
        raise ValueError("正式准入来源池代码集合重复或未闭合")

    columns_sql = ", ".join(_ROW_COLUMNS)
    placeholders = ", ".join("%s" for _ in _ROW_COLUMNS)
    values = [tuple(row[column] for column in _ROW_COLUMNS) for row in rows]
    expected_by_symbol = {
        row["source_symbol"]: tuple(row[c] for c in _ROW_COLUMNS) for row in rows
    }
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        exact_scope = (
            target_session,
            source_capture_id,
            "LAUNCH_CONFIRM",
            rows[0]["raw_producer_version"],
            rows[0]["algorithm_contract"],
            rows[0]["session_contract"],
            rows[0]["calendar_revision"],
        )
        cursor.execute(
            f"SELECT count(*)::bigint FROM {_TABLE} "
            "WHERE target_session=%s AND source_capture_id=%s "
            "AND scenario=%s AND raw_producer_version=%s AND algorithm_contract=%s "
            "AND session_contract=%s AND calendar_revision=%s",
            exact_scope,
        )
        before_count = int(cursor.fetchone()[0])
        cursor.executemany(
            f"INSERT INTO {_TABLE} ({columns_sql}) VALUES ({placeholders}) "
            "ON CONFLICT ON CONSTRAINT source_d0_formal_admission_unique_input DO NOTHING",
            values,
        )
        cursor.execute(
            f"SELECT {columns_sql} FROM {_TABLE} "
            "WHERE target_session=%s AND source_capture_id=%s "
            "AND scenario='LAUNCH_CONFIRM' AND raw_producer_version=%s "
            "AND algorithm_contract=%s AND session_contract=%s "
            "AND calendar_revision=%s ORDER BY source_symbol",
            (*exact_scope[:2], *exact_scope[3:]),
        )
        existing = cursor.fetchall()
        if len(existing) != expected_source_count:
            raise FormalAdmissionPgError(
                "FORMAL_ADMISSION_PERSISTED_PARTITION_INCOMPLETE",
                "准入结果行数未闭合到输入分母，事务将回滚",
            )
        symbol_index = _ROW_COLUMNS.index("source_symbol")
        for stored in existing:
            symbol = stored[symbol_index]
            expected = expected_by_symbol.get(symbol)
            if expected is None or tuple(stored) != expected:
                raise FormalAdmissionPgError(
                    "FORMAL_ADMISSION_SAME_KEY_CONFLICT",
                    "同一LAUNCH业务键已有不同准入事实；拒绝覆盖并回滚",
                )
        cursor.execute(
            f"SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint, "
            f"count(*) FILTER (WHERE admission_status='READY')::bigint, "
            f"count(*) FILTER (WHERE admission_status='BLOCKED')::bigint, "
            f"count(*) FILTER (WHERE admission_status='NOT_READY')::bigint, "
            f"count(*) FILTER (WHERE admission_status='NOT_REQUIRED')::bigint "
            f"FROM {_TABLE} WHERE target_session=%s AND source_capture_id=%s "
            "AND scenario='LAUNCH_CONFIRM' "
            "AND raw_producer_version=%s AND algorithm_contract=%s "
            "AND session_contract=%s AND calendar_revision=%s",
            (*exact_scope[:2], *exact_scope[3:]),
        )
        stored, distinct_symbols, ready, blocked, not_ready, not_required = (
            cursor.fetchone()
        )
        if (
            int(stored) != expected_source_count
            or int(distinct_symbols) != expected_source_count
        ):
            raise FormalAdmissionPgError(
                "FORMAL_ADMISSION_PARTITION_NOT_CLOSED",
                "准入状态分区未闭合到唯一来源代码分母，事务将回滚",
            )
    inserted_rows = int(stored) - before_count
    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "source_capture_id": source_capture_id,
        "source_count": int(stored),
        "distinct_source_count": int(distinct_symbols),
        "ready_count": int(ready),
        "blocked_count": int(blocked),
        "not_ready_count": int(not_ready),
        "not_required_count": int(not_required),
        "inserted_rows": inserted_rows,
        "idempotent_rerun": inserted_rows == 0,
        "transaction": "SERIALIZABLE",
        "database_write_attempted": True,
        "ddl_executed": False,
        "dml_executed": True,
    }


def read_source_formal_admission_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    expected_source_count: int,
    algorithm_contract: str,
    session_contract: str,
    calendar_revision: str,
    seed_producer_version: str | None,
    source_symbols: Sequence[str] | None = None,
    profile_history_session_count: int | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    """只读已完成准入事实；不计算Raw、不修复缺失分区。"""
    if (
        type(target_session) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or not isinstance(algorithm_contract, str)
        or not algorithm_contract
        or not isinstance(session_contract, str)
        or not session_contract
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or seed_producer_version not in {None, _SEED_PRODUCER_VERSION}
        or profile_history_session_count is not None
        and (
            type(profile_history_session_count) is not int
            or not 1 <= profile_history_session_count <= 250
        )
        or profile_revision is not None
        and (
            not isinstance(profile_revision, str)
            or re.fullmatch(PROFILE_REVISION_PATTERN, profile_revision) is None
        )
        or profile_history_session_count is not None
        and profile_revision is None
        or profile_revision is not None
        and profile_history_session_count is not None
        and profile_revision_history_session_count(profile_revision)
        != profile_history_session_count
    ):
        raise ValueError("准入Reader必须绑定日期、来源版本和闭合分母")
    selected = None if source_symbols is None else tuple(source_symbols)
    if selected is not None and (
        len(selected) > 100
        or tuple(sorted(set(selected))) != selected
        or any(not isinstance(symbol, str) for symbol in selected)
    ):
        raise ValueError("准入Reader来源代码必须去重排序且最多100只")
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        scope_predicate = (
            "target_session=%s AND source_capture_id=%s "
            "AND scenario='LAUNCH_CONFIRM' AND raw_producer_version=%s "
            "AND algorithm_contract=%s AND session_contract=%s "
            "AND calendar_revision=%s AND admission_contract=%s "
            "AND seed_producer_version IS NOT DISTINCT FROM %s"
        )
        scope_parameters: tuple[Any, ...] = (
            target_session,
            source_capture_id,
            _D0_LAUNCH_PRODUCER_VERSION,
            algorithm_contract,
            session_contract,
            calendar_revision,
            _ADMISSION_CONTRACT,
            seed_producer_version,
        )
        if profile_revision is not None:
            scope_predicate += " AND profile_revision=%s"
            scope_parameters += (profile_revision,)
        cursor.execute(
            f"SELECT DISTINCT history_session_count,profile_revision FROM {_TABLE} "
            f"WHERE {scope_predicate} "
            "ORDER BY history_session_count,profile_revision LIMIT 2",
            scope_parameters,
        )
        scopes = cursor.fetchall()
        if not scopes:
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "FORMAL_ADMISSION_NOT_COMPUTED_FOR_EXACT_INPUT",
                "target_session": target_session.isoformat(),
                "source_capture_id": source_capture_id,
                "algorithm_contract": algorithm_contract,
                "session_contract": session_contract,
                "calendar_revision": calendar_revision,
                "seed_producer_version": seed_producer_version,
                "source_count": expected_source_count,
                "profile_history_session_count": profile_history_session_count,
                "profile_revision": profile_revision,
                "counts": None,
                "by_symbol": {},
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
            }
        if len(scopes) > 1:
            return {
                "status": "AMBIGUOUS",
                "reason_code": "FORMAL_ADMISSION_SCOPE_AMBIGUOUS",
                "target_session": target_session.isoformat(),
                "source_capture_id": source_capture_id,
                "algorithm_contract": algorithm_contract,
                "session_contract": session_contract,
                "calendar_revision": calendar_revision,
                "seed_producer_version": seed_producer_version,
                "source_count": expected_source_count,
                "profile_history_session_count": profile_history_session_count,
                "profile_revision": profile_revision,
                "counts": None,
                "by_symbol": {},
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
            }
        selected_d0_history_session_count, selected_profile_revision = scopes[0]
        selected_profile_history_session_count = (
            profile_revision_history_session_count(selected_profile_revision)
            if selected_profile_revision is not None
            else None
        )
        if (
            profile_history_session_count is not None
            and selected_profile_history_session_count != profile_history_session_count
        ):
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "FORMAL_ADMISSION_PROFILE_REVISION_MISMATCH",
                "target_session": target_session.isoformat(),
                "source_capture_id": source_capture_id,
                "algorithm_contract": algorithm_contract,
                "session_contract": session_contract,
                "calendar_revision": calendar_revision,
                "seed_producer_version": seed_producer_version,
                "source_count": expected_source_count,
                "profile_history_session_count": profile_history_session_count,
                "profile_revision": selected_profile_revision,
                "counts": None,
                "by_symbol": {},
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
            }
        cursor.execute(
            f"""SELECT count(*)::bigint, count(DISTINCT source_symbol)::bigint,
                       min(source_count), max(source_count),
                       count(*) FILTER (WHERE admission_status='READY')::bigint,
                       count(*) FILTER (WHERE admission_status='BLOCKED')::bigint,
                       count(*) FILTER (WHERE admission_status='NOT_READY')::bigint,
                       count(*) FILTER (WHERE admission_status='NOT_REQUIRED')::bigint
                FROM {_TABLE}
                WHERE target_session=%s AND source_capture_id=%s
                  AND scenario='LAUNCH_CONFIRM'
                  AND raw_producer_version=%s
                  AND algorithm_contract=%s AND session_contract=%s
                  AND calendar_revision=%s
                  AND admission_contract=%s
                  AND seed_producer_version IS NOT DISTINCT FROM %s
                  AND history_session_count IS NOT DISTINCT FROM %s
                  AND profile_revision IS NOT DISTINCT FROM %s""",
            (
                target_session,
                source_capture_id,
                _D0_LAUNCH_PRODUCER_VERSION,
                algorithm_contract,
                session_contract,
                calendar_revision,
                _ADMISSION_CONTRACT,
                seed_producer_version,
                selected_d0_history_session_count,
                selected_profile_revision,
            ),
        )
        row_count, distinct_count, min_source_count, max_source_count, *counts = (
            cursor.fetchone()
        )
        if not row_count:
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "FORMAL_ADMISSION_NOT_COMPUTED_FOR_EXACT_INPUT",
                "target_session": target_session.isoformat(),
                "source_capture_id": source_capture_id,
                "algorithm_contract": algorithm_contract,
                "session_contract": session_contract,
                "calendar_revision": calendar_revision,
                "seed_producer_version": seed_producer_version,
                "source_count": expected_source_count,
                "history_session_count": selected_d0_history_session_count,
                "profile_history_session_count": selected_profile_history_session_count,
                "profile_revision": selected_profile_revision,
                "counts": None,
                "by_symbol": {},
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
            }
        if (
            int(row_count) != expected_source_count
            or int(distinct_count) != expected_source_count
            or int(min_source_count) != expected_source_count
            or int(max_source_count) != expected_source_count
        ):
            return {
                "status": "INCOMPLETE",
                "reason_code": "FORMAL_ADMISSION_PARTITION_NOT_CLOSED",
                "target_session": target_session.isoformat(),
                "source_capture_id": source_capture_id,
                "algorithm_contract": algorithm_contract,
                "session_contract": session_contract,
                "calendar_revision": calendar_revision,
                "seed_producer_version": seed_producer_version,
                "source_count": expected_source_count,
                "history_session_count": selected_d0_history_session_count,
                "profile_history_session_count": selected_profile_history_session_count,
                "profile_revision": selected_profile_revision,
                "stored_rows": int(row_count),
                "distinct_source_count": int(distinct_count),
                "counts": None,
                "by_symbol": {},
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
            }
        counts_by_status = dict(zip(_STATES, map(int, counts), strict=True))
        by_symbol: dict[str, dict[str, Any]] = {}
        if selected:
            columns = (
                "source_symbol",
                "raw_role",
                "raw_fact_id",
                "seed_fact_id",
                "raw_state",
                "raw_reason",
                "admission_status",
                "admission_reason",
                "reason_codes",
                "identity_scope",
                "research_mode",
                "price_coordinate",
                "source_capture_id",
                "source_time_quality",
                "source_clock_role",
                "source_available_at",
                "capture_started_at",
                "capture_validated_at",
                "raw_producer_version",
                "algorithm_contract",
                "session_contract",
                "history_window_start",
                "history_session_count",
                "profile_revision",
                "seed_producer_version",
                "calendar_id",
                "calendar_revision",
                "research_universe_state",
                "research_universe_reason",
                "actual_quote_state",
                "actual_quote_reason",
                "coordinate_window_state",
                "coordinate_window_reason",
                "d0_liquidity_state",
                "d0_liquidity_reason",
                "non_core_damage_state",
                "non_core_damage_reason",
                "non_extreme_extension_state",
                "non_extreme_extension_reason",
                "non_first_day_damage_state",
                "non_first_day_damage_reason",
                "non_severe_drop_state",
                "non_severe_drop_reason",
                "event_id",
                "failure_level_numerator",
                "failure_level_denominator",
                "failure_coordinate",
                "failure_source",
                "failure_window_start",
                "failure_window_session_count",
                "failure_source_event_ref",
                "effective_from_session",
            )
            cursor.execute(
                f"SELECT {', '.join(columns)} FROM {_TABLE} "
                "WHERE target_session=%s AND source_capture_id=%s "
                "AND scenario='LAUNCH_CONFIRM' "
                "AND raw_producer_version=%s "
                "AND algorithm_contract=%s AND session_contract=%s "
                "AND calendar_revision=%s AND admission_contract=%s "
                "AND seed_producer_version IS NOT DISTINCT FROM %s "
                "AND history_session_count IS NOT DISTINCT FROM %s "
                "AND profile_revision IS NOT DISTINCT FROM %s "
                "AND source_symbol=ANY(%s) ORDER BY source_symbol",
                (
                    target_session,
                    source_capture_id,
                    _D0_LAUNCH_PRODUCER_VERSION,
                    algorithm_contract,
                    session_contract,
                    calendar_revision,
                    _ADMISSION_CONTRACT,
                    seed_producer_version,
                    selected_d0_history_session_count,
                    selected_profile_revision,
                    list(selected),
                ),
            )
            for values in cursor.fetchall():
                item = dict(zip(columns, values, strict=True))
                symbol = item.pop("source_symbol")
                item["raw_fact_id"] = int(item["raw_fact_id"])
                item["seed_fact_id"] = (
                    int(item["seed_fact_id"])
                    if item["seed_fact_id"] is not None
                    else None
                )
                item["history_window_start"] = item["history_window_start"].isoformat()
                item["source_available_at"] = (
                    item["source_available_at"].isoformat()
                    if item["source_available_at"] is not None
                    else None
                )
                item["capture_started_at"] = item["capture_started_at"].isoformat()
                item["capture_validated_at"] = item["capture_validated_at"].isoformat()
                line_num = item.pop("failure_level_numerator")
                line_den = item.pop("failure_level_denominator")
                line = (
                    {"numerator": str(line_num), "denominator": str(line_den)}
                    if line_num is not None and line_den is not None
                    else None
                )
                item["frozen_line"] = (
                    {
                        "value": line,
                        "coordinate": item["failure_coordinate"],
                        "source": item["failure_source"],
                        "source_window_start": item["failure_window_start"].isoformat()
                        if item["failure_window_start"]
                        else None,
                        "source_window_session_count": item[
                            "failure_window_session_count"
                        ],
                        "source_event_ref": item["failure_source_event_ref"],
                        "effective_from_session": item[
                            "effective_from_session"
                        ].isoformat()
                        if item["effective_from_session"]
                        else None,
                    }
                    if line is not None
                    else None
                )
                item["event_status"] = (
                    "ADMITTED" if item["admission_status"] == "READY" else "NOT_CREATED"
                )
                item["forward_line_check_status"] = (
                    "NOT_DUE" if item["admission_status"] == "READY" else "NOT_COMPUTED"
                )
                item["scenario_role"] = item.pop("raw_role")
                item["admission_status"] = item["admission_status"]
                item["reason_codes"] = list(item["reason_codes"])
                by_symbol[symbol] = item
        return {
            "status": "AVAILABLE",
            "contract": _ADMISSION_CONTRACT,
            "target_session": target_session.isoformat(),
            "source_capture_id": source_capture_id,
            "algorithm_contract": algorithm_contract,
            "session_contract": session_contract,
            "calendar_revision": calendar_revision,
            "seed_producer_version": seed_producer_version,
            "history_session_count": selected_d0_history_session_count,
            "profile_history_session_count": selected_profile_history_session_count,
            "profile_revision": selected_profile_revision,
            "identity_scope": "SOURCE_SYMBOL_ONLY",
            "research_mode": "NON_PIT_EXPLORATORY",
            "price_coordinate": "FORWARD_ADJUSTED",
            "source_count": expected_source_count,
            "stored_rows": int(row_count),
            "distinct_source_count": int(distinct_count),
            "counts": counts_by_status,
            "by_symbol": by_symbol,
            "transaction": "REPEATABLE READ READ ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
        }


__all__ = [
    "FormalAdmissionPgError",
    "list_saved_launch_sessions",
    "persist_source_formal_admission_rows",
    "read_saved_source_fact_ids",
    "read_source_formal_admission_day",
]
