"""现役来源D0场景与Seed事实的类型化PG读写。"""

from __future__ import annotations

from stock_a.contracts.profile_revision import PROFILE_REVISION_PATTERN

from datetime import date
from decimal import Decimal
from fractions import Fraction
import random
import re
import time
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.research_day_facts import _insert_rows, _row_matches

_D0_PREDICATES = {
    "LAUNCH_CONFIRM": (
        "close_above_phc20",
        "positive_ret1",
        "clv_at_least_060",
        "amr20_mean_prior_at_least_120",
        "not_phh20_shadow_rejection",
    ),
    "RECOVERY_TURN": (
        "trigger",
        "positive_ret1",
        "clv",
        "rps5_delta3",
        "amr",
        "close_ma20",
    ),
}
_SEED_PREDICATES = (
    "research_universe",
    "actual_bar",
    "non_st",
    "normal_listing_phase",
    "limited_price_rule",
    "coordinate_ready",
    "liquidity_mean20",
    "no_damage",
    "no_severe_extension",
    "bias_below_3",
    "compression",
    "rps5_delta3_at_least_3",
    "rps5_delta3_at_least_10",
    "bull_transition",
    "previous_ma20_cross",
)
_CORE_MEASURES = (
    "bias20_atr",
    "liquidity_mean20",
    "ma20",
    "ret1",
    "ret5",
    "ret20",
    "range_ratio",
    "atr_ratio",
    "vol_ratio",
)
_SEED_COLUMNS = (
    "target_session",
    "source_symbol",
    "market",
    "board",
    "source_package_id",
    "source_capture_id",
    "profile_revision",
    "producer_contract",
    "producer_version",
    "history_window_start",
    "history_window_end",
    "history_session_count",
    "calendar_id",
    "calendar_revision",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "research_mode",
    "price_coordinate",
    *(
        f"core_{name}_{part}"
        for name in _CORE_MEASURES
        for part in ("numerator", "denominator", "quality", "reason")
    ),
    "core_price_damage",
    "core_price_damage_quality",
    "core_price_damage_reason",
    "target_actual_quote",
    "target_close_numerator",
    "target_close_denominator",
    "target_close_quality",
    "target_close_reason",
    "profile_compression_value",
    "profile_compression_quality",
    "profile_compression_reason",
    "profile_extension_value",
    "profile_extension_quality",
    "profile_extension_reason",
    "profile_ma_structure_value",
    "profile_ma_structure_quality",
    "profile_ma_structure_reason",
    *(
        f"{name}_{part}"
        for name in ("rps5", "rps5_delta3", "rps20", "rps20_delta3")
        for part in ("numerator", "denominator", "quality", "reason")
    ),
    "previous_ma20_cross_state",
    "previous_ma20_cross_reason",
    "predicate_ids",
    "predicate_states",
    "predicate_reasons",
    "safety_state",
    "safety_reasons",
    "s1_state",
    "s1_reasons",
    "s2_state",
    "s2_reasons",
    "source_seed_state",
    "source_seed_reasons",
    "raw_prewatch_state",
    "raw_prewatch_reasons",
)
_D0_COLUMNS = (
    "target_session",
    "source_symbol",
    "source_package_id",
    "source_capture_id",
    "parser_contract",
    "scenario",
    "producer_version",
    "algorithm_contract",
    "session_contract",
    "history_window_start",
    "history_session_count",
    "source_count",
    "scenario_state",
    "scenario_reason",
    "predicate_ids",
    "predicate_states",
    "predicate_qualities",
    "predicate_reason_codes",
    "predicate_value_numerators",
    "predicate_value_denominators",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "calendar_id",
    "calendar_revision",
    "research_mode",
    "price_coordinate",
)
_D0_KEY = (
    "target_session",
    "source_symbol",
    "source_package_id",
    "source_capture_id",
    "scenario",
    "producer_version",
    "calendar_revision",
)
_SEED_KEY = (
    "target_session",
    "source_symbol",
    "source_package_id",
    "source_capture_id",
    "profile_revision",
    "producer_version",
)
_D0_TABLE = "source_d0_scenario_day_facts"
_SEED_TABLE = "source_pool_seed_day_facts"
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")


class StaticResearchPgError(ValueError):
    """静态研究事实缺失、不完整或与既存PG事实冲突。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _fraction(value: Any) -> Fraction | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Mapping):
        if "numerator" in value and "denominator" in value:
            return Fraction(int(value["numerator"]), int(value["denominator"]))
        value = value.get("exact", value.get("value"))
    if value is None or isinstance(value, bool):
        return None
    try:
        return Fraction(str(value))
    except (ValueError, ZeroDivisionError):
        return None


def _fraction_parts(value: Any) -> tuple[int | None, int | None]:
    result = _fraction(value)
    return (None, None) if result is None else (result.numerator, result.denominator)


def _reasons(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        value = value.get("reasons") or value.get("reason")
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, (tuple, list)):
        return list(
            dict.fromkeys(item for item in value if isinstance(item, str) and item)
        )
    return []


def _d0_rows(
    scenarios: Mapping[str, Mapping[str, Any]], *, require_complete: bool = True
) -> list[tuple[Any, ...]]:
    rows = []
    for scenario, page in scenarios.items():
        ids = _D0_PREDICATES.get(scenario)
        if ids is None:
            raise StaticResearchPgError("STATIC_D0_SCENARIO_INVALID", "D0场景未登记")
        source = page["source_version"]
        pool_count = page["source_pool"]["source_count"]
        result_rows = page["results"]
        if (
            page.get("scenario") != scenario
            or page.get("source_pool", {}).get("scenario_state_partition_closed")
            is not True
            or page.get("selection", {}).get("matched_count") != pool_count
            or page.get("selection", {}).get("next_offset") is not None
            or len(result_rows) != pool_count
        ):
            raise StaticResearchPgError(
                "STATIC_D0_PARTITION_OPEN", "D0场景结果未闭合到来源分母"
            )
        expected_calendar = page["official_calendar"]
        sessions = page["strict_official_sessions"]
        for item in result_rows:
            predicates = item.get("predicates")
            if not isinstance(predicates, Mapping) or tuple(predicates) != ids:
                raise StaticResearchPgError(
                    "STATIC_D0_PREDICATES_INVALID", "D0具名谓词与现役顺序不一致"
                )
            numerators, denominators, states, qualities, reasons = [], [], [], [], []
            for name in ids:
                predicate = predicates[name]
                state = predicate.get("state")
                quality = predicate.get("quality")
                if state not in {"TRUE", "FALSE", "UNKNOWN"} or quality not in {
                    "OBSERVED",
                    "UNKNOWN",
                }:
                    raise StaticResearchPgError(
                        "STATIC_D0_PREDICATE_STATE_INVALID", "D0谓词三态或质量无效"
                    )
                numerator, denominator = _fraction_parts(predicate.get("value"))
                states.append(state)
                qualities.append(quality)
                reasons.append(
                    str(
                        predicate.get("reason_code")
                        or predicate.get("reason")
                        or "UNKNOWN_REASON_UNAVAILABLE"
                    )
                )
                numerators.append(numerator)
                denominators.append(denominator)
            symbol = item.get("source_symbol")
            if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
                raise StaticResearchPgError(
                    "STATIC_D0_SOURCE_KEY_INVALID", "D0来源代码无效"
                )
            rows.append(
                (
                    date.fromisoformat(page["target_session"]),
                    symbol,
                    source["source_package_id"],
                    source["source_capture_id"],
                    source["parser_contract"],
                    scenario,
                    page["producer_version"],
                    page["algorithm_contract"],
                    page["session_contract"],
                    date.fromisoformat(sessions[0]),
                    len(sessions),
                    pool_count,
                    item["scenario_raw"],
                    item["main_reason"],
                    list(ids),
                    states,
                    qualities,
                    reasons,
                    numerators,
                    denominators,
                    source["source_time_quality"],
                    source["source_clock_role"],
                    source["source_available_at"],
                    expected_calendar["calendar_id"],
                    expected_calendar["calendar_revision"],
                    "NON_PIT_RETROSPECTIVE",
                    "RAW_UNADJUSTED",
                )
            )
    if not scenarios or (require_complete and set(scenarios) != set(_D0_PREDICATES)):
        raise StaticResearchPgError(
            "STATIC_D0_SCENARIO_SET_INCOMPLETE", "D0需同时闭合启动与恢复场景"
        )
    return rows


def persist_static_d0_reference_scenario(
    config: Mapping[str, Any], *, scenario: str, page: Mapping[str, Any]
) -> dict[str, Any]:
    """只保存跨日消费者明确需要的一类完整D0参考事实。"""
    if scenario not in _D0_PREDICATES or page.get("scenario") != scenario:
        raise StaticResearchPgError(
            "STATIC_D0_SCENARIO_INVALID", "D0参考事实场景与计算结果不一致"
        )
    rows = _d0_rows({scenario: page}, require_complete=False)
    first = rows[0]
    predicate, params = _scope_predicate(_D0_TABLE, _D0_COLUMNS, first)
    for attempt in range(5):
        try:
            inserted = {}
            with source_raw._transaction(
                config, write=True, statement_timeout_ms=30000
            ) as (_, cursor, _connection_identity):
                cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (_D0_TABLE,))
                if cursor.fetchone() != (True,):
                    raise StaticResearchPgError(
                        "STATIC_RESEARCH_MIGRATION_MISSING",
                        "0018静态研究D0表未完成迁移",
                    )
                scoped = f"{predicate} AND scenario=%s"
                scoped_params = (*params, scenario)
                cursor.execute(
                    f"SELECT {', '.join(_D0_COLUMNS)} FROM {_D0_TABLE} "
                    f"WHERE {scoped} ORDER BY source_symbol, producer_version, calendar_revision",
                    scoped_params,
                )
                existing = cursor.fetchall()
                if existing and (
                    len(existing) != len(rows)
                    or any(
                        not _row_matches(expected, actual)
                        for expected, actual in zip(rows, existing, strict=True)
                    )
                ):
                    raise StaticResearchPgError(
                        "STATIC_D0_REFERENCE_CONTENT_CONFLICT",
                        "相同D0参考事实业务键已有不同或不完整内容",
                    )
                if not existing:
                    inserted[_D0_TABLE] = _insert_rows(
                        cursor, _D0_TABLE, _D0_COLUMNS, _D0_KEY, rows
                    )
                cursor.execute(
                    f"SELECT {', '.join(_D0_COLUMNS)} FROM {_D0_TABLE} "
                    f"WHERE {scoped} ORDER BY source_symbol, producer_version, calendar_revision",
                    scoped_params,
                )
                actual = cursor.fetchall()
                if len(actual) != len(rows) or any(
                    not _row_matches(expected, persisted)
                    for expected, persisted in zip(rows, actual, strict=True)
                ):
                    raise StaticResearchPgError(
                        "STATIC_D0_REFERENCE_READBACK_MISMATCH",
                        "D0参考事实事务内独立读回不一致",
                    )
            return {
                "status": "IDEMPOTENT_ALREADY_PRESENT"
                if existing
                else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
                "scenario": scenario,
                "rows_expected": len(rows),
                "rows_inserted": inserted,
                "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
                if existing
                else None,
                "transaction": "SERIALIZABLE_ATOMIC_SINGLE_D0_REFERENCE_SCENARIO",
                "database_write_attempted": not bool(existing),
                "ddl_executed": False,
                "dml_executed": not bool(existing),
            }
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(random.uniform(0.1, 0.3) * (2**attempt))
    raise AssertionError("不可达的D0参考事实事务重试状态")


def _field(facts: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = facts.get(name)
    return value if isinstance(value, Mapping) else {}


def _state(item: Any) -> str:
    state = item.get("state") if isinstance(item, Mapping) else item
    return state if state in {"TRUE", "FALSE", "UNKNOWN"} else "UNKNOWN"


def _seed_rows(
    seed: Mapping[str, Any],
    *,
    scenarios: Mapping[str, Mapping[str, Any]],
    history_window: Sequence[date],
    profile_revision: str,
) -> list[tuple[Any, ...]]:
    launch = scenarios["LAUNCH_CONFIRM"]
    source = launch["source_version"]
    calendar = launch["official_calendar"]
    market_by_symbol = {row["source_symbol"]: row for row in launch["results"]}
    seed_facts = seed.get("facts")
    if (
        not isinstance(seed_facts, Mapping)
        or set(seed_facts) != set(market_by_symbol)
        or seed.get("source_pool_total") != len(market_by_symbol)
        or seed.get("stored_profile_status") != "AVAILABLE"
        or not isinstance(profile_revision, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, profile_revision) is None
    ):
        raise StaticResearchPgError(
            "STATIC_SEED_PARTITION_OPEN", "Seed或精确R6画像未覆盖完整来源池"
        )
    rows = []
    for symbol in sorted(seed_facts):
        fact = seed_facts[symbol]
        raw = market_by_symbol[symbol]
        core = fact.get("core", {})
        native = fact.get("native_price", {})
        profile = fact.get("profile", {})
        predicates = fact.get("predicates", {})
        values: dict[str, Any] = {
            "target_session": date.fromisoformat(launch["target_session"]),
            "source_symbol": symbol,
            "market": raw["market"],
            "board": raw["board"],
            "source_package_id": source["source_package_id"],
            "source_capture_id": source["source_capture_id"],
            "profile_revision": profile_revision,
            "producer_contract": "UNIVERSE_ELIGIBILITY_R1+ALG-19/20",
            "producer_version": "SOURCE_POOL_SEED_CORE_PROFILE_R1",
            "history_window_start": history_window[0],
            "history_window_end": history_window[-1],
            "history_session_count": len(history_window),
            "calendar_id": calendar["calendar_id"],
            "calendar_revision": calendar["calendar_revision"],
            "source_time_quality": source["source_time_quality"],
            "source_clock_role": source["source_clock_role"],
            "source_available_at": source["source_available_at"],
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "price_coordinate": "RAW_UNADJUSTED",
        }
        for name in _CORE_MEASURES:
            field = _field(
                native.get("fields", {}) if name in {"ret1", "ret5", "ret20"} else core,
                f"core.{name}",
            )
            numerator, denominator = _fraction_parts(field.get("value"))
            quality = field.get("quality", "UNKNOWN")
            if name in {"ret1", "ret5", "ret20"} and quality == "UNKNOWN_DATA":
                quality = "UNKNOWN"
            if name in {"ret1", "ret5", "ret20"} and (
                quality not in {"OBSERVED", "UNKNOWN", "NOT_APPLICABLE"}
                or (
                    quality == "OBSERVED"
                    and (
                        numerator is None or denominator is None or field.get("reason")
                    )
                )
                or (
                    quality != "OBSERVED"
                    and (numerator is not None or not field.get("reason"))
                )
            ):
                raise StaticResearchPgError(
                    "STATIC_SEED_RETURN_INCOMPLETE", "Seed收益字段缺少精确值或未知原因"
                )
            values.update(
                {
                    f"core_{name}_numerator": numerator,
                    f"core_{name}_denominator": denominator,
                    f"core_{name}_quality": quality,
                    f"core_{name}_reason": field.get("reason"),
                }
            )
        damage = _field(core, "core.price_damage")
        damage_value = damage.get("value")
        values.update(
            {
                "core_price_damage": damage_value
                if isinstance(damage_value, bool)
                else None,
                "core_price_damage_quality": damage.get("quality", "UNKNOWN"),
                "core_price_damage_reason": damage.get("reason"),
            }
        )
        close = _fraction(native.get("close"))
        values.update(
            {
                "target_actual_quote": native.get("actual_quote") is True,
                "target_close_numerator": close.numerator
                if close is not None
                else None,
                "target_close_denominator": close.denominator
                if close is not None
                else None,
                "target_close_quality": "OBSERVED" if close is not None else "UNKNOWN",
                "target_close_reason": None
                if close is not None
                else "TARGET_CLOSE_UNKNOWN",
            }
        )
        for name, field_id in (
            ("compression", "profile.compression"),
            ("extension", "profile.extension"),
            ("ma_structure", "profile.ma_structure"),
        ):
            field = profile.get(field_id, {})
            value = field.get("value")
            values.update(
                {
                    f"profile_{name}_value": None if value is None else str(value),
                    f"profile_{name}_quality": field.get("quality", "UNKNOWN"),
                    f"profile_{name}_reason": field.get("reason"),
                }
            )
        rps = fact.get("rps", {})
        for target_name, source_name in (
            ("rps5", "rps5"),
            ("rps5_delta3", "rps5_delta3_pp"),
            ("rps20", "rps20"),
            ("rps20_delta3", "rps20_delta3_pp"),
        ):
            field = rps.get(source_name, {})
            numerator, denominator = _fraction_parts(field.get("value"))
            values.update(
                {
                    f"{target_name}_numerator": numerator,
                    f"{target_name}_denominator": denominator,
                    f"{target_name}_quality": field.get("quality", "UNKNOWN"),
                    f"{target_name}_reason": field.get("reason"),
                }
            )
        cross = _field(predicates, "previous_ma20_cross")
        values.update(
            {
                "previous_ma20_cross_state": _state(cross),
                "previous_ma20_cross_reason": cross.get("reason"),
                "predicate_ids": list(_SEED_PREDICATES),
                "predicate_states": [
                    _state(_field(predicates, name)) for name in _SEED_PREDICATES
                ],
                "predicate_reasons": [
                    ";".join(_reasons(_field(predicates, name)))
                    for name in _SEED_PREDICATES
                ],
            }
        )
        for key, prefix in (
            ("safety", "safety"),
            ("s1", "s1"),
            ("s2", "s2"),
            ("seed", "source_seed"),
            ("raw_prewatch", "raw_prewatch"),
        ):
            node = (
                (fact.get("qualification") or {}).get("source_seed_shape", {})
                if key == "seed"
                else fact.get(key, {})
            )
            values[f"{prefix}_state"] = _state(node)
            values[f"{prefix}_reasons"] = _reasons(node)
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
            raise StaticResearchPgError(
                "STATIC_SEED_SOURCE_KEY_INVALID", "Seed来源代码无效"
            )
        rows.append(tuple(values[name] for name in _SEED_COLUMNS))
    return rows


def _scope_predicate(
    table: str, columns: Sequence[str], row: Sequence[Any]
) -> tuple[str, tuple[Any, ...]]:
    names = dict(zip(columns, row, strict=True))
    scope = (
        (
            "target_session",
            "source_package_id",
            "source_capture_id",
        )
        if table == _D0_TABLE
        else (
            "target_session",
            "source_package_id",
            "source_capture_id",
            "profile_revision",
            "producer_version",
        )
    )
    return " AND ".join(f"{name}=%s" for name in scope), tuple(
        names[name] for name in scope
    )


def _existing_rows_match_expected_subset(
    *,
    columns: Sequence[str],
    key: Sequence[str],
    expected_rows: Sequence[Sequence[Any]],
    existing_rows: Sequence[Sequence[Any]],
) -> bool:
    """仅接受与预期逐键相等的既存子集，供事务内补齐缺失事实。"""
    key_positions = tuple(columns.index(name) for name in key)
    expected_by_key = {
        tuple(row[index] for index in key_positions): row for row in expected_rows
    }
    if len(expected_by_key) != len(expected_rows) or len(existing_rows) > len(
        expected_rows
    ):
        return False
    existing_keys = set()
    for actual in existing_rows:
        row_key = tuple(actual[index] for index in key_positions)
        expected = expected_by_key.get(row_key)
        if (
            row_key in existing_keys
            or expected is None
            or not _row_matches(expected, actual)
        ):
            return False
        existing_keys.add(row_key)
    return True


def persist_static_research_day(
    config: Mapping[str, Any],
    *,
    scenarios: Mapping[str, Mapping[str, Any]],
    seed: Mapping[str, Any],
    history_window: Sequence[date],
    profile_revision: str,
) -> dict[str, Any]:
    """以一个可重试串行化事务保存两类全池事实；相同键异内容回滚。"""
    d0_rows = _d0_rows(scenarios)
    seed_rows = _seed_rows(
        seed,
        scenarios=scenarios,
        history_window=history_window,
        profile_revision=profile_revision,
    )
    groups = (
        (_D0_TABLE, _D0_COLUMNS, _D0_KEY, d0_rows),
        (_SEED_TABLE, _SEED_COLUMNS, _SEED_KEY, seed_rows),
    )
    for attempt in range(5):
        try:
            return _persist_once(config, scenarios, groups)
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(random.uniform(0.1, 0.3) * (2**attempt))
    raise AssertionError("不可达的静态研究事务重试状态")


def _persist_once(config, scenarios, groups) -> dict[str, Any]:
    inserted: dict[str, int] = {}
    scopes = []
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        first = scenarios["LAUNCH_CONFIRM"]
        cursor.execute(
            "SELECT to_regclass(%s) IS NOT NULL, to_regclass(%s) IS NOT NULL",
            (_D0_TABLE, _SEED_TABLE),
        )
        if cursor.fetchone() != (True, True):
            raise StaticResearchPgError(
                "STATIC_RESEARCH_MIGRATION_MISSING", "0018静态研究表未完成迁移"
            )
        for table, columns, key, rows in groups:
            rows = sorted(
                rows, key=lambda row: tuple(row[columns.index(name)] for name in key)
            )
            predicate, params = _scope_predicate(table, columns, rows[0])
            cursor.execute(
                f"SELECT {', '.join(columns)} FROM {table} WHERE {predicate} ORDER BY {', '.join(key)}",
                params,
            )
            existing = cursor.fetchall()
            if not _existing_rows_match_expected_subset(
                columns=columns,
                key=key,
                expected_rows=rows,
                existing_rows=existing,
            ):
                raise StaticResearchPgError(
                    "STATIC_RESEARCH_CONTENT_CONFLICT",
                    "相同静态事实业务键已有不同内容，事务回滚",
                )
            scopes.append(
                (
                    table,
                    columns,
                    key,
                    rows,
                    predicate,
                    params,
                    len(existing) == len(rows),
                )
            )
        if not all(item[-1] for item in scopes):
            for table, columns, key, rows, *_ in scopes:
                inserted[table.rsplit(".", 1)[-1]] = _insert_rows(
                    cursor, table, columns, key, rows
                )
        for table, columns, key, rows, predicate, params, _ in scopes:
            cursor.execute(
                f"SELECT {', '.join(columns)} FROM {table} WHERE {predicate} ORDER BY {', '.join(key)}",
                params,
            )
            actual = cursor.fetchall()
            if len(actual) != len(rows) or any(
                not _row_matches(expected, persisted)
                for expected, persisted in zip(rows, actual, strict=True)
            ):
                raise StaticResearchPgError(
                    "STATIC_RESEARCH_READBACK_MISMATCH",
                    "事务内静态事实读回与生产结果不一致",
                )
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if all(item[-1] for item in scopes)
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": {
            table.rsplit(".", 1)[-1]: len(rows) for table, _, _, rows, *_ in scopes
        },
        "rows_inserted": inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if all(item[-1] for item in scopes)
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_D0_SCENARIOS_AND_SEED",
        "database_write_attempted": not all(item[-1] for item in scopes),
        "ddl_executed": False,
        "dml_executed": not all(item[-1] for item in scopes),
    }


def read_source_d0_scenario_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    scenario: str,
) -> dict[str, Any]:
    """以完整单日分母读取已保存D0场景，不回退Raw重算。"""
    if scenario not in _D0_PREDICATES:
        raise StaticResearchPgError("STATIC_D0_SCENARIO_INVALID", "D0场景未登记")
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            f"SELECT {', '.join(_D0_COLUMNS)} FROM {_D0_TABLE} WHERE target_session=%s AND source_package_id=%s AND scenario=%s ORDER BY source_symbol, producer_version, calendar_revision",
            (target_session, source_package_id, scenario),
        )
        rows = cursor.fetchall()
    if not rows:
        raise StaticResearchPgError(
            "STATIC_D0_NOT_COMPUTED", "该日D0场景没有已保存事实"
        )
    first = dict(zip(_D0_COLUMNS, rows[0], strict=True))
    if (
        len(
            {
                tuple(
                    dict(zip(_D0_COLUMNS, row, strict=True))[name]
                    for name in (
                        "source_package_id",
                        "source_capture_id",
                        "parser_contract",
                        "producer_version",
                        "algorithm_contract",
                        "session_contract",
                        "calendar_revision",
                    )
                )
                for row in rows
            }
        )
        != 1
    ):
        raise StaticResearchPgError("STATIC_D0_SCOPE_AMBIGUOUS", "该日D0保存修订不唯一")
    symbol_index = _D0_COLUMNS.index("source_symbol")
    if len(rows) != first["source_count"] or len(
        {row[symbol_index] for row in rows}
    ) != len(rows):
        raise StaticResearchPgError(
            "STATIC_D0_PARTITION_OPEN", "PG D0事实没有闭合到来源分母"
        )
    results = []
    for row in rows:
        item = dict(zip(_D0_COLUMNS, row, strict=True))
        if tuple(item["predicate_ids"]) != _D0_PREDICATES[scenario]:
            raise StaticResearchPgError(
                "STATIC_D0_PREDICATES_INVALID", "PG D0具名谓词顺序与现役合同不一致"
            )
        predicates = {}
        for name, state, quality, reason, numerator, denominator in zip(
            item["predicate_ids"],
            item["predicate_states"],
            item["predicate_qualities"],
            item["predicate_reason_codes"],
            item["predicate_value_numerators"],
            item["predicate_value_denominators"],
            strict=True,
        ):
            value = (
                None
                if numerator is None or denominator is None
                else {
                    "numerator": str(int(numerator)),
                    "denominator": str(int(denominator)),
                }
            )
            predicates[name] = {
                "state": state,
                "quality": quality,
                "reason_code": reason,
                "value": value,
            }
        expected_state = (
            "FALSE"
            if any(value["state"] == "FALSE" for value in predicates.values())
            else "UNKNOWN"
            if any(value["state"] == "UNKNOWN" for value in predicates.values())
            else "TRUE"
        )
        if expected_state != item["scenario_state"]:
            raise StaticResearchPgError(
                "STATIC_D0_SCENARIO_STATE_CONFLICT",
                "PG D0场景主状态与具名谓词归约不一致",
            )
        results.append(
            {
                "source_symbol": item["source_symbol"],
                "target_session": target_session.isoformat(),
                "scenario": scenario,
                "scenario_raw": item["scenario_state"],
                "quality": (
                    "UNKNOWN" if item["scenario_state"] == "UNKNOWN" else "OBSERVED"
                ),
                "main_reason": item["scenario_reason"],
                "predicates": predicates,
            }
        )
    calendar = {
        "calendar_id": first["calendar_id"],
        "calendar_revision": first["calendar_revision"],
    }
    return {
        "scenario": scenario,
        "target_session": target_session.isoformat(),
        "history_window_start": first["history_window_start"],
        "history_session_count": first["history_session_count"],
        "producer_version": first["producer_version"],
        "algorithm_contract": first["algorithm_contract"],
        "session_contract": first["session_contract"],
        "official_calendar": calendar,
        "source_version": {
            "source_package_id": first["source_package_id"],
            "source_capture_id": first["source_capture_id"],
            "parser_contract": first["parser_contract"],
            "source_time_quality": first["source_time_quality"],
            "source_clock_role": first["source_clock_role"],
            "source_available_at": first["source_available_at"],
        },
        "source_pool": {
            "source_count": len(rows),
            "scenario_state_partition_closed": True,
        },
        "selection": {"matched_count": len(rows), "next_offset": None},
        "results": results,
    }


def read_source_pool_seed_facts(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    profile_revision: str,
    producer_version: str = "SOURCE_POOL_SEED_CORE_PROFILE_R1",
) -> list[dict[str, Any]]:
    """按精确R6语义读取Seed事实全池行。"""
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            f"SELECT {', '.join(_SEED_COLUMNS)} FROM {_SEED_TABLE} WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s AND profile_revision=%s AND producer_version=%s ORDER BY source_symbol",
            (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        )
        rows = cursor.fetchall()
    if not rows:
        raise StaticResearchPgError(
            "STATIC_SEED_NOT_COMPUTED", "该日Seed没有精确语义的已保存事实"
        )
    result = [dict(zip(_SEED_COLUMNS, row, strict=True)) for row in rows]
    if len({row["source_symbol"] for row in result}) != len(result):
        raise StaticResearchPgError(
            "STATIC_SEED_DUPLICATE_SOURCE", "PG Seed来源代码重复"
        )
    return result


def read_source_pool_seed_forward_scope(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    expected_source_count: int,
) -> dict[str, Any]:
    """读取唯一闭合Seed范围，供已存T0来源池的后继生产复用。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(expected_source_count) is not int
        or expected_source_count <= 0
    ):
        raise StaticResearchPgError(
            "STATIC_SEED_FORWARD_KEY_INVALID", "后继生产Seed键或完整分母无效"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            f"""SELECT profile_revision, producer_contract, producer_version,
                       history_session_count, calendar_id, calendar_revision,
                       research_mode, price_coordinate, count(*)::bigint,
                       count(DISTINCT source_symbol)::bigint,
                       count(*) FILTER (WHERE s2_state='TRUE')::bigint,
                       count(*) FILTER (WHERE s2_state='FALSE')::bigint,
                       count(*) FILTER (WHERE s2_state='UNKNOWN')::bigint
                FROM {_SEED_TABLE}
                WHERE target_session=%s AND source_package_id=%s
                  AND source_capture_id=%s
                GROUP BY profile_revision, producer_contract, producer_version,
                         history_session_count, calendar_id, calendar_revision,
                         research_mode, price_coordinate
                ORDER BY profile_revision, producer_version""",
            (target_session, source_package_id, source_capture_id),
        )
        scopes = cursor.fetchall()
        if not scopes:
            raise StaticResearchPgError(
                "STATIC_SEED_NOT_COMPUTED", "T0没有已存Seed事实，拒绝重算替代"
            )
        if len(scopes) != 1:
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_AMBIGUOUS", "T0存在多个Seed修订范围，拒绝猜选"
            )
        scope = scopes[0]
        if scope[8] != expected_source_count or scope[9] != expected_source_count:
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_INCOMPLETE", "T0已存Seed与闭合Raw来源分母不一致"
            )
        if (
            scope[2] != "SOURCE_POOL_SEED_CORE_PROFILE_R1"
            or scope[6] != "NON_PIT_RETROSPECTIVE"
            or scope[7] != "RAW_UNADJUSTED"
        ):
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_UNSUPPORTED",
                "T0 Seed生产合同、时钟或价格坐标不受支持",
            )
        cursor.execute(
            f"""SELECT source_symbol FROM {_SEED_TABLE}
                WHERE target_session=%s AND source_package_id=%s
                  AND source_capture_id=%s AND profile_revision=%s
                  AND producer_version=%s
                ORDER BY source_symbol""",
            (
                target_session,
                source_package_id,
                source_capture_id,
                scope[0],
                scope[2],
            ),
        )
        symbols = tuple(row[0] for row in cursor.fetchall())
    if len(symbols) != expected_source_count or len(set(symbols)) != len(symbols):
        raise StaticResearchPgError(
            "STATIC_SEED_SOURCE_SET_INVALID", "T0 Seed来源代码集合未闭合或重复"
        )
    return {
        "profile_revision": scope[0],
        "producer_contract": scope[1],
        "producer_version": scope[2],
        "history_session_count": scope[3],
        "calendar_id": scope[4],
        "calendar_revision": scope[5],
        "research_mode": scope[6],
        "price_coordinate": scope[7],
        "source_count": scope[8],
        "s2_state_counts": {
            "TRUE": scope[10],
            "FALSE": scope[11],
            "UNKNOWN": scope[12],
        },
        "source_symbols": symbols,
    }


def read_source_stock_seed_fact(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    profile_revision: str,
    source_symbol: str,
    expected_source_count: int,
    producer_version: str = "SOURCE_POOL_SEED_CORE_PROFILE_R1",
) -> dict[str, Any]:
    """按完整来源身份核验分区后，只读取单股行；分母须来自已闭合的Raw版本目录。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(profile_revision, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, profile_revision) is None
        or not isinstance(source_symbol, str)
        or _SYMBOL.fullmatch(source_symbol) is None
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or producer_version != "SOURCE_POOL_SEED_CORE_PROFILE_R1"
    ):
        raise StaticResearchPgError(
            "STATIC_SEED_SINGLE_KEY_INVALID", "单股Seed读取必须绑定合法完整业务键"
        )

    with source_raw._transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            f"""SELECT count(*)::bigint AS stored_rows,
       count(DISTINCT source_package_id)::bigint AS content_count,
       min(source_package_id) AS source_package_id
FROM {_SEED_TABLE}
WHERE target_session=%s::date
  AND source_package_id=%s AND source_capture_id=%s
  AND profile_revision=%s AND producer_version=%s""",
            (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        )
        selected_rows, selected_contents, selected_content = cursor.fetchone()
        selected_rows = int(selected_rows)
        if selected_rows == 0:
            cursor.execute(
                f"""SELECT EXISTS (
    SELECT 1 FROM {_SEED_TABLE}
    WHERE target_session=%s::date
      AND source_package_id=%s AND source_capture_id=%s
      AND profile_revision<>%s AND producer_version=%s
    LIMIT 1
)""",
                (
                    target_session,
                    source_package_id,
                    source_capture_id,
                    profile_revision,
                    producer_version,
                ),
            )
            another_revision_exists = cursor.fetchone()[0]
            if another_revision_exists:
                return {
                    "status": "NOT_COMPARABLE",
                    "reason_code": "SEED_PROFILE_REVISION_NOT_STORED",
                    "message": "Seed事实只存在于其他画像修订，拒绝跨修订读取。",
                    "partition": {
                        "stored_rows": 0,
                        "expected_source_count": expected_source_count,
                        "closed": False,
                    },
                }
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "SEED_NOT_COMPUTED",
                "message": "该目标日和精确Raw捕获没有已保存Seed事实。",
                "partition": {
                    "stored_rows": 0,
                    "expected_source_count": expected_source_count,
                    "closed": False,
                },
            }
        if (
            selected_rows != expected_source_count
            or selected_contents != 1
            or int(selected_content) != source_package_id
        ):
            return {
                "status": "INCOMPLETE",
                "reason_code": "SEED_PARTITION_NOT_CLOSED",
                "message": "精确画像修订的Seed行数、来源分母或内容摘要未闭合。",
                "partition": {
                    "stored_rows": selected_rows,
                    "expected_source_count": expected_source_count,
                    "closed": False,
                },
            }

        cursor.execute(
            f"""SELECT {", ".join("s." + name for name in _SEED_COLUMNS)}
FROM {_SEED_TABLE} s
JOIN source_symbol_price_profiles p
  ON p.target_session=s.target_session
 AND p.source_symbol=s.source_symbol AND p.source_package_id=s.source_package_id
 AND p.source_capture_id=s.source_capture_id
 AND p.profile_revision=s.profile_revision
 AND p.history_window_start=s.history_window_start
 AND p.history_window_end=s.history_window_end
 AND p.history_requested_session_count=s.history_session_count
 AND p.history_calendar_id=s.calendar_id
 AND (
    p.history_calendar_revision=s.calendar_revision
    OR strpos('+' || p.history_calendar_revision || '+',
              '+' || to_char(s.target_session, 'YYYY') || ':' || s.calendar_revision || '+') > 0
 )
 AND p.price_coordinate=s.price_coordinate
WHERE s.target_session=%s::date
  AND s.source_symbol=%s AND s.source_package_id=%s
  AND s.source_capture_id=%s AND s.profile_revision=%s
  AND s.producer_version=%s
LIMIT 2""",
            (
                target_session,
                source_symbol,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        )
        rows = cursor.fetchall()

    if len(rows) != 1:
        return {
            "status": "INCOMPLETE",
            "reason_code": (
                "SEED_SOURCE_SYMBOL_WINDOW_MISMATCH"
                if not rows
                else "SEED_SOURCE_SYMBOL_DUPLICATE"
            ),
            "message": (
                "该股票缺少同画像修订、同捕获和同窗口的Seed行。"
                if not rows
                else "精确单股Seed业务键出现重复行。"
            ),
            "partition": {
                "stored_rows": selected_rows,
                "expected_source_count": expected_source_count,
                "closed": True,
            },
        }

    row = dict(zip(_SEED_COLUMNS, rows[0], strict=True))
    valid_states = {"TRUE", "FALSE", "UNKNOWN"}
    predicates = row["predicate_ids"]
    states = row["predicate_states"]
    reasons = row["predicate_reasons"]
    quality_fields = (
        "core_bias20_atr",
        "core_liquidity_mean20",
        "core_price_damage",
        "profile_compression",
        "profile_extension",
        "profile_ma_structure",
        "rps5_delta3",
        "target_close",
    )
    if (
        tuple(predicates) != _SEED_PREDICATES
        or len(states) != len(_SEED_PREDICATES)
        or len(reasons) != len(_SEED_PREDICATES)
        or any(state not in valid_states for state in states)
        or any(
            state != "TRUE" and not reason
            for state, reason in zip(states, reasons, strict=True)
        )
        or any(
            row[f"{name}_state"] not in valid_states
            or row[f"{name}_state"] != "TRUE"
            and not row[f"{name}_reasons"]
            for name in ("safety", "s1", "s2", "source_seed")
        )
        or any(
            row[f"{name}_quality"] == "UNKNOWN" and not row[f"{name}_reason"]
            for name in quality_fields
        )
        or row["research_mode"] != "NON_PIT_RETROSPECTIVE"
        or row["price_coordinate"] != "RAW_UNADJUSTED"
        or row["source_capture_id"] != source_capture_id
        or row["source_package_id"] != source_package_id
        or row["history_window_end"] != target_session
        or not row["history_window_start"] <= target_session
        or not 1 <= row["history_session_count"] <= 75
    ):
        return {
            "status": "INCOMPLETE",
            "reason_code": "SEED_TYPED_FACT_INVALID",
            "message": "Seed三态、具名原因、窗口或来源质量字段不符合已存事实合同。",
            "partition": {
                "stored_rows": selected_rows,
                "expected_source_count": expected_source_count,
                "closed": True,
            },
        }

    return {
        "status": "SAVED",
        "row": row,
        "partition": {
            "stored_rows": selected_rows,
            "expected_source_count": expected_source_count,
            "closed": True,
        },
    }


def read_seed_profile_returns(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    profile_revision: str,
    producer_version: str = "SOURCE_POOL_SEED_CORE_PROFILE_R1",
) -> dict[str, dict[str, Any]]:
    """按完整身份连接旧Seed与同窗口画像，返回来源明确的收益字段。"""
    columns = tuple(
        f"{name}_{part}"
        for name in ("ret1", "ret5", "ret20")
        for part in ("exact_numerator", "exact_denominator", "quality", "reason")
    )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            f"""SELECT s.source_symbol, {", ".join("p." + name for name in columns)}
FROM {_SEED_TABLE} s
JOIN source_symbol_price_profiles p
  ON p.target_session=s.target_session
 AND p.source_symbol=s.source_symbol AND p.source_package_id=s.source_package_id
 AND p.profile_revision=s.profile_revision AND p.source_capture_id=s.source_capture_id
 AND p.history_window_start=s.history_window_start AND p.history_window_end=s.history_window_end
 AND p.history_requested_session_count=s.history_session_count
 AND p.history_calendar_id=s.calendar_id
 AND p.price_coordinate=s.price_coordinate
WHERE s.target_session=%s AND s.source_package_id=%s AND s.source_capture_id=%s
  AND s.profile_revision=%s AND s.producer_version=%s ORDER BY s.source_symbol""",
            (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        )
        rows = cursor.fetchall()
    result = {row[0]: dict(zip(columns, row[1:], strict=True)) for row in rows}
    if len(result) != len(rows):
        raise StaticResearchPgError(
            "STATIC_SEED_PROFILE_DUPLICATE", "Seed与画像连接出现重复证券"
        )
    return result


__all__ = [
    "StaticResearchPgError",
    "persist_static_research_day",
    "read_source_d0_scenario_day",
    "read_source_pool_seed_facts",
    "read_seed_profile_returns",
]
