"""现役来源D0场景与Seed事实的类型化PG读写。"""

from __future__ import annotations

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
    "source_capture_id",
    "producer_contract",
    "producer_version",
    "ranking_universe_count",
    "evaluation_universe_count",
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
    "formal_universe_state",
    "formal_universe_reason",
    "formal_seed_state",
    "formal_seed_reasons",
    "formal_prewatch_state",
    "formal_prewatch_reasons",
)
_D0_COLUMNS = (
    "target_session",
    "source_symbol",
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
    "scenario",
)
_SEED_KEY = (
    "target_session",
    "source_symbol",
)
_SEED_SCOPE_KEY = ("target_session",)
_SEED_UNIVERSE_MATCH_STATUSES = frozenset(
    {
        "EXACT_MATCH",
        "EVALUATION_SUBSET_OF_RANKING",
        "FALLBACK_UNIVERSE_MISMATCH",
        "FALLBACK_CROSS_SECTION_UNAVAILABLE",
        "FALLBACK_CROSS_SECTION_INCOMPLETE",
    }
)
_D0_TABLE = "source_d0_scenario_day_facts"
_SEED_TABLE = "source_pool_seed_day_facts"
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_D0_PRODUCER_VERSIONS = {
    "LAUNCH_CONFIRM": "STOCK_A_D0_LAUNCH_BAOSTOCK_READER_R5",
    "RECOVERY_TURN": "STOCK_A_D0_RECOVERY_BAOSTOCK_READER_R5",
}


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


def _seed_qfq_target_close(native_price: Mapping[str, Any]) -> Fraction | None:
    quality = native_price.get("qfq_close_quality")
    if quality is None and "qfq_close_quality" not in native_price:
        quality = native_price.get("qfq_price_quality")
    value = _fraction(native_price.get("qfq_close"))
    if quality == "OBSERVED":
        if value is None or value <= 0:
            raise StaticResearchPgError(
                "SEED_TARGET_CLOSE_QUALITY_VALUE_MISMATCH",
                "QFQ目标收盘质量为OBSERVED但数值缺失或非正",
            )
        return value
    if quality not in {"UNKNOWN", "UNKNOWN_DATA", "NOT_APPLICABLE"}:
        raise StaticResearchPgError(
            "SEED_TARGET_CLOSE_QUALITY_INVALID", "QFQ目标收盘质量枚举无效"
        )
    if value is not None:
        raise StaticResearchPgError(
            "SEED_TARGET_CLOSE_QUALITY_VALUE_MISMATCH",
            "非OBSERVED的QFQ目标收盘质量不能携带数值",
        )
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


def _encode_recovery_trigger_predicate(
    predicate: Mapping[str, Any],
    trigger_detail: Mapping[str, Any] | None,
) -> tuple[str, int | None, int | None]:
    """将RECOVERY_TURN触发明细(R5/R20与对应冻结MA)无损编码到typed谓词槽位。"""
    base_reason = str(
        predicate.get("reason_code")
        or predicate.get("reason")
        or "UNKNOWN_REASON_UNAVAILABLE"
    )
    numerator, denominator = _fraction_parts(predicate.get("value"))
    if not isinstance(trigger_detail, Mapping):
        return base_reason, numerator, denominator
    r5 = str(trigger_detail.get("r5") or "UNKNOWN")
    r20 = str(trigger_detail.get("r20") or "UNKNOWN")
    if r5 not in {"TRUE", "FALSE", "UNKNOWN"}:
        r5 = "UNKNOWN"
    if r20 not in {"TRUE", "FALSE", "UNKNOWN"}:
        r20 = "UNKNOWN"
    encoded_reason = f"{base_reason.split('__R20_', 1)[0]}__R20_{r20}__R5_{r5}"
    if r20 == "TRUE":
        ma_num, ma_den = _fraction_parts(trigger_detail.get("ma20"))
        return encoded_reason, ma_num, ma_den
    if r20 == "FALSE" and r5 == "TRUE":
        ma_num, ma_den = _fraction_parts(trigger_detail.get("ma5"))
        return encoded_reason, ma_num, ma_den
    return encoded_reason, None, None


_RECOVERY_TRIGGER_REASON_RE = re.compile(
    r"__R20_(TRUE|FALSE|UNKNOWN)__R5_(TRUE|FALSE|UNKNOWN)$"
)


def _decode_recovery_trigger_detail(
    trigger_predicate: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """从已存RECOVERY_TURN的trigger typed谓词无损还原R5/R20及对应冻结MA。"""
    if not isinstance(trigger_predicate, Mapping):
        return {"r5": "UNKNOWN", "r20": "UNKNOWN", "ma5": None, "ma20": None}
    reason_code = str(trigger_predicate.get("reason_code") or "")
    match = _RECOVERY_TRIGGER_REASON_RE.search(reason_code)
    if match is None:
        return {"r5": "UNKNOWN", "r20": "UNKNOWN", "ma5": None, "ma20": None}
    r20, r5 = match.group(1), match.group(2)
    value = trigger_predicate.get("value")
    ma20 = value if r20 == "TRUE" and isinstance(value, Mapping) else None
    ma5 = (
        value
        if r20 == "FALSE" and r5 == "TRUE" and isinstance(value, Mapping)
        else None
    )
    return {"r5": r5, "r20": r20, "ma5": ma5, "ma20": ma20}


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
                if scenario == "RECOVERY_TURN" and name == "trigger":
                    reason_text, numerator, denominator = (
                        _encode_recovery_trigger_predicate(
                            predicate, item.get("trigger_detail")
                        )
                    )
                else:
                    numerator, denominator = _fraction_parts(predicate.get("value"))
                    reason_text = str(
                        predicate.get("reason_code")
                        or predicate.get("reason")
                        or "UNKNOWN_REASON_UNAVAILABLE"
                    )
                states.append(state)
                qualities.append(quality)
                reasons.append(reason_text)
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
                    "FORWARD_ADJUSTED",
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
    predicate, params = _scope_predicate(_D0_TABLE, _D0_COLUMNS, rows)
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
                matches = len(existing) == len(rows) and all(
                    _row_matches(expected, actual)
                    for expected, actual in zip(rows, existing, strict=True)
                )
                if not matches:
                    if existing:
                        cursor.execute(
                            f"DELETE FROM {_D0_TABLE} WHERE {scoped}", scoped_params
                        )
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
                if matches
                else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
                "scenario": scenario,
                "rows_expected": len(rows),
                "rows_inserted": inserted,
                "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
                if matches
                else None,
                "transaction": "SERIALIZABLE_ATOMIC_SINGLE_D0_REFERENCE_SCENARIO",
                "database_write_attempted": not matches,
                "ddl_executed": False,
                "dml_executed": not matches,
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
    profile_revision: str | None = None,
) -> list[tuple[Any, ...]]:
    launch = scenarios["LAUNCH_CONFIRM"]
    source = launch["source_version"]
    calendar = launch["official_calendar"]
    market_by_symbol = {row["source_symbol"]: row for row in launch["results"]}
    seed_facts = seed.get("facts")
    ranking_count = seed.get("ranking_universe_count")
    evaluation_count = seed.get("evaluation_universe_count")
    if (
        not isinstance(seed_facts, Mapping)
        or set(seed_facts) != set(market_by_symbol)
        or seed.get("source_pool_total") != len(market_by_symbol)
        or seed.get("stored_profile_status") != "AVAILABLE"
        or type(evaluation_count) is not int
        or evaluation_count <= 0
        or (
            ranking_count is not None
            and (type(ranking_count) is not int or ranking_count < 0)
        )
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
            "source_capture_id": source["source_capture_id"],
            "producer_contract": "UNIVERSE_ELIGIBILITY_R1+ALG-19/20",
            "producer_version": "SOURCE_POOL_SEED_CORE_PROFILE_R2",
            "ranking_universe_count": seed.get("ranking_universe_count"),
            "evaluation_universe_count": seed.get("evaluation_universe_count"),
            "history_window_start": history_window[0],
            "history_window_end": history_window[-1],
            "history_session_count": len(history_window),
            "calendar_id": calendar["calendar_id"],
            "calendar_revision": calendar["calendar_revision"],
            "source_time_quality": source["source_time_quality"],
            "source_clock_role": source["source_clock_role"],
            "source_available_at": source["source_available_at"],
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "price_coordinate": "FORWARD_ADJUSTED",
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
        close = _seed_qfq_target_close(native)
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
                else str(native.get("qfq_close_reason") or "TARGET_CLOSE_UNKNOWN"),
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
        ru_node = _field(predicates, "research_universe")
        values["formal_universe_state"] = _state(ru_node)
        values["formal_universe_reason"] = ru_node.get("reason")
        fseed_node = fact.get("formal_seed") or {}
        values["formal_seed_state"] = _state(fseed_node) if fseed_node else "UNKNOWN"
        values["formal_seed_reasons"] = _reasons(fseed_node) if fseed_node else []
        fpw_node = fact.get("formal_prewatch") or {}
        values["formal_prewatch_state"] = _state(fpw_node) if fpw_node else "UNKNOWN"
        values["formal_prewatch_reasons"] = _reasons(fpw_node) if fpw_node else []
        if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
            raise StaticResearchPgError(
                "STATIC_SEED_SOURCE_KEY_INVALID", "Seed来源代码无效"
            )
        rows.append(tuple(values[name] for name in _SEED_COLUMNS))
    return rows


def _scope_predicate(
    table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
) -> tuple[str, tuple[Any, ...]]:
    if not rows or any(len(columns) != len(row) for row in rows):
        raise StaticResearchPgError(
            "STATIC_ROW_SHAPE_MISMATCH",
            f"静态研究表{table}行宽与列合同{len(columns)}不一致",
        )
    row = rows[0]
    names = dict(zip(columns, row, strict=True))
    if table == _D0_TABLE:
        scenarios = sorted({str(item[columns.index("scenario")]) for item in rows})
        return (
            "target_session=%s AND scenario=ANY(%s)",
            (
                names["target_session"],
                scenarios,
            ),
        )
    scope = _SEED_SCOPE_KEY
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
    profile_revision: str | None = None,
) -> dict[str, Any]:
    """以一个可重试串行化事务保存或替换两类全池事实。"""
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
            return _persist_once(config, scenarios, groups, profile_revision)
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(random.uniform(0.1, 0.3) * (2**attempt))
    raise AssertionError("不可达的静态研究事务重试状态")


def _persist_once(config, scenarios, groups, profile_revision=None) -> dict[str, Any]:
    inserted: dict[str, int] = {}
    scopes = []
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
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
            predicate, params = _scope_predicate(table, columns, rows)
            cursor.execute(
                f"SELECT {', '.join(columns)} FROM {table} WHERE {predicate} ORDER BY {', '.join(key)}",
                params,
            )
            existing = cursor.fetchall()
            matches = len(existing) == len(rows) and all(
                _row_matches(expected, actual)
                for expected, actual in zip(rows, existing, strict=True)
            )
            scopes.append(
                (
                    table,
                    columns,
                    key,
                    rows,
                    predicate,
                    params,
                    matches,
                    bool(existing),
                )
            )
        if not all(item[6] for item in scopes):
            for (
                table,
                columns,
                key,
                rows,
                predicate,
                params,
                matches,
                has_existing,
            ) in scopes:
                if matches:
                    continue
                if has_existing:
                    cursor.execute(f"DELETE FROM {table} WHERE {predicate}", params)
                inserted[table.rsplit(".", 1)[-1]] = _insert_rows(
                    cursor, table, columns, key, rows
                )
        for table, columns, key, rows, predicate, params, _, _ in scopes:
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
        if all(item[6] for item in scopes)
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": {
            table.rsplit(".", 1)[-1]: len(rows) for table, _, _, rows, *_ in scopes
        },
        "producer_versions": {
            table.rsplit(".", 1)[-1]: sorted(
                {row[columns.index("producer_version")] for row in rows}
            )
            for table, columns, _key, rows, *_ in scopes
        },
        "calendar_revisions": {
            table.rsplit(".", 1)[-1]: sorted(
                {row[columns.index("calendar_revision")] for row in rows}
            )
            for table, columns, _key, rows, *_ in scopes
        },
        "rows_inserted": inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if all(item[6] for item in scopes)
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_D0_SCENARIOS_AND_SEED",
        "database_write_attempted": not all(item[6] for item in scopes),
        "ddl_executed": False,
        "dml_executed": not all(item[6] for item in scopes),
    }


def read_source_d0_scenario_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    scenario: str,
    price_coordinate: str = "FORWARD_ADJUSTED",
    research_cutoff: date | None = None,
) -> dict[str, Any]:
    """以完整单日分母读取已保存D0场景，不回退Raw重算。"""
    cutoff = research_cutoff or target_session
    if (
        scenario not in _D0_PREDICATES
        or type(target_session) is not date
        or type(cutoff) is not date
        or target_session > cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or price_coordinate != "FORWARD_ADJUSTED"
    ):
        raise StaticResearchPgError("STATIC_D0_SCENARIO_INVALID", "D0场景未登记")
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            f"""SELECT d.{", d.".join(_D0_COLUMNS)}
                FROM {_D0_TABLE} d
                JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
                WHERE d.target_session=%s AND d.target_session<=%s
                  AND d.source_capture_id=%s AND d.scenario=%s
                  AND d.producer_version=%s
                  AND d.price_coordinate=%s AND c.provider='BAOSTOCK'
                  AND c.range_start_session<=d.target_session
                  AND d.target_session<=c.range_end_session
                  AND c.range_end_session<=%s
                  AND c.price_coordinates @> ARRAY[
                      'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                  ]::text[]
                ORDER BY d.source_symbol,d.producer_version,d.calendar_revision""",
            (
                target_session,
                cutoff,
                source_capture_id,
                scenario,
                _D0_PRODUCER_VERSIONS[scenario],
                price_coordinate,
                cutoff,
            ),
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
        row_payload = {
            "source_symbol": item["source_symbol"],
            "target_session": target_session.isoformat(),
            "scenario": scenario,
            "price_coordinate": price_coordinate,
            "scenario_raw": item["scenario_state"],
            "quality": (
                "UNKNOWN" if item["scenario_state"] == "UNKNOWN" else "OBSERVED"
            ),
            "main_reason": item["scenario_reason"],
            "predicates": predicates,
        }
        if scenario == "RECOVERY_TURN":
            row_payload["trigger_detail"] = _decode_recovery_trigger_detail(
                predicates.get("trigger")
            )
        results.append(row_payload)
    calendar = {
        "calendar_id": first["calendar_id"],
        "calendar_revision": first["calendar_revision"],
    }
    return {
        "scenario": scenario,
        "price_coordinate": price_coordinate,
        "target_session": target_session.isoformat(),
        "history_window_start": first["history_window_start"],
        "history_session_count": first["history_session_count"],
        "producer_version": first["producer_version"],
        "algorithm_contract": first["algorithm_contract"],
        "session_contract": first["session_contract"],
        "official_calendar": calendar,
        "source_version": {
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
    research_cutoff: date,
    source_capture_id: int | None = None,
    price_coordinate: str = "FORWARD_ADJUSTED",
    profile_revision: str | None = None,
    producer_version: str = "SOURCE_POOL_SEED_CORE_PROFILE_R2",
    cross_section_universe_capture_id: int | None = None,
    cross_section_universe_version: str | None = None,
    cross_section_contract_version: str | None = None,
    universe_match_status: str | None = None,
) -> list[dict[str, Any]]:
    """按目标日、QFQ坐标及显式截止读取Seed事实全池行。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or price_coordinate != "FORWARD_ADJUSTED"
    ):
        raise StaticResearchPgError(
            "STATIC_SEED_READ_SCOPE_INVALID",
            "Seed读取必须绑定截止内目标日和FORWARD_ADJUSTED坐标",
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            f"""SELECT {", ".join("s." + name for name in _SEED_COLUMNS)}
                FROM {_SEED_TABLE} s
                JOIN stock.source_captures c ON c.capture_id=s.source_capture_id
                WHERE s.target_session=%s AND s.target_session<=%s
                  AND s.producer_version=%s AND s.price_coordinate=%s
                  AND c.provider='BAOSTOCK' AND c.range_start_session<=s.target_session
                  AND s.target_session<=c.range_end_session
                  AND c.range_end_session<=%s
                  AND c.price_coordinates @> ARRAY[
                      'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                  ]::text[]
                ORDER BY s.source_symbol""",
            (
                target_session,
                research_cutoff,
                producer_version,
                price_coordinate,
                research_cutoff,
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
    source_capture_id: int | None,
    expected_source_count: int | None,
    profile_revision: str | None = None,
    history_session_count: int | None = None,
    cross_section_universe_capture_id: int | None = None,
    cross_section_universe_version: str | None = None,
    cross_section_contract_version: str | None = None,
    universe_match_status: str | None = None,
) -> dict[str, Any]:
    """读取唯一闭合Seed范围，供已存T0来源池的后继生产复用。"""
    if (
        type(target_session) is not date
        or source_capture_id is not None
        and (type(source_capture_id) is not int or source_capture_id <= 0)
        or expected_source_count is not None
        and (type(expected_source_count) is not int or expected_source_count <= 0)
        or history_session_count is not None
        and (
            type(history_session_count) is not int
            or not 1 <= history_session_count <= 250
        )
    ):
        raise StaticResearchPgError(
            "STATIC_SEED_FORWARD_KEY_INVALID", "后继生产Seed键或完整分母无效"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        scope_predicate = "target_session=%s AND producer_version=%s"
        scope_parameters: tuple[Any, ...] = (
            target_session,
            "SOURCE_POOL_SEED_CORE_PROFILE_R2",
        )
        if history_session_count is not None:
            scope_predicate += " AND history_session_count=%s"
            scope_parameters = (*scope_parameters, history_session_count)
        if expected_source_count is None:
            cursor.execute(
                f"""SELECT count(*)::bigint,count(DISTINCT source_symbol)::bigint
                    FROM {_SEED_TABLE} WHERE {scope_predicate}""",
                scope_parameters,
            )
            total_rows, distinct_symbols = (
                int(value or 0) for value in cursor.fetchone()
            )
            if total_rows <= 0 or total_rows != distinct_symbols:
                raise StaticResearchPgError(
                    "STATIC_SEED_SCOPE_INCOMPLETE",
                    "T0当前Seed总体为空或存在重复证券，不能推定来源分母",
                )
            expected_source_count = total_rows
        cursor.execute(
            f"""SELECT DISTINCT ranking_universe_count, evaluation_universe_count
                FROM {_SEED_TABLE} WHERE {scope_predicate}""",
            scope_parameters,
        )
        seed_scope_rows = cursor.fetchall()
        if not seed_scope_rows:
            raise StaticResearchPgError(
                "STATIC_SEED_NOT_COMPUTED", "T0没有已存Seed事实，拒绝重算替代"
            )
        if len(seed_scope_rows) != 1:
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_AMBIGUOUS",
                "T0存在多个排名/评价范围",
            )
        selected_cross_scope = seed_scope_rows[0]
        cursor.execute(
            f"""SELECT producer_contract, producer_version,
                       history_session_count, calendar_id, calendar_revision,
                       research_mode, price_coordinate, count(*)::bigint,
                       count(DISTINCT source_symbol)::bigint,
                       count(*) FILTER (WHERE EXISTS (
                           SELECT 1 FROM source_raw_daily r
                           WHERE r.session_date=s.target_session
                             AND r.source_symbol=s.source_symbol
                       ))::bigint,
                       count(*) FILTER (WHERE EXISTS (
                           SELECT 1 FROM source_symbol_price_profiles p
                           WHERE p.target_session=s.target_session
                             AND p.source_symbol=s.source_symbol
                             AND p.history_window_start=s.history_window_start
                             AND p.history_window_end=s.history_window_end
                             AND p.history_requested_session_count=s.history_session_count
                             AND p.history_calendar_id=s.calendar_id
                             AND (p.history_calendar_revision=s.calendar_revision OR
                               strpos('+' || p.history_calendar_revision || '+', '+' ||
                                 to_char(s.target_session, 'YYYY') || ':' || s.calendar_revision || '+') > 0)
                             AND p.price_coordinate=s.price_coordinate
                       ))::bigint,
                       count(*) FILTER (WHERE s2_state='TRUE')::bigint,
                       count(*) FILTER (WHERE s2_state='FALSE')::bigint,
                       count(*) FILTER (WHERE s2_state='UNKNOWN')::bigint
                FROM {_SEED_TABLE} s
                WHERE {scope_predicate}
                GROUP BY s.producer_contract, s.producer_version,
                         s.history_session_count, s.calendar_id, s.calendar_revision,
                         s.research_mode, s.price_coordinate
                ORDER BY s.producer_version""",
            scope_parameters,
        )
        scopes = cursor.fetchall()
        if not scopes:
            raise StaticResearchPgError(
                "STATIC_SEED_NOT_COMPUTED", "T0没有已存Seed事实，拒绝重算替代"
            )
        complete_scopes = [
            item
            for item in scopes
            if int(item[7]) == expected_source_count
            and int(item[8]) == expected_source_count
            and int(item[9]) == expected_source_count
            and int(item[10]) == expected_source_count
        ]
        if len(complete_scopes) > 1:
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_AMBIGUOUS", "T0存在多个Seed修订范围，拒绝猜选"
            )
        if not complete_scopes:
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_INCOMPLETE",
                "T0已存Seed/Profile与闭合Raw来源分母不一致",
            )
        scope = complete_scopes[0]
        if (
            scope[1] != "SOURCE_POOL_SEED_CORE_PROFILE_R2"
            or scope[5] != "NON_PIT_RETROSPECTIVE"
            or scope[6] != "FORWARD_ADJUSTED"
        ):
            raise StaticResearchPgError(
                "STATIC_SEED_SCOPE_UNSUPPORTED",
                "T0 Seed生产合同、时钟或价格坐标不受支持",
            )
        cursor.execute(
            f"""SELECT source_symbol FROM {_SEED_TABLE}
                WHERE target_session=%s
                ORDER BY source_symbol""",
            (target_session,),
        )
        symbols = tuple(row[0] for row in cursor.fetchall())
    if len(symbols) != expected_source_count or len(set(symbols)) != len(symbols):
        raise StaticResearchPgError(
            "STATIC_SEED_SOURCE_SET_INVALID", "T0 Seed来源代码集合未闭合或重复"
        )
    return {
        "producer_contract": scope[0],
        "producer_version": scope[1],
        "history_session_count": scope[2],
        "calendar_id": scope[3],
        "calendar_revision": scope[4],
        "research_mode": scope[5],
        "price_coordinate": scope[6],
        "ranking_universe_count": selected_cross_scope[0],
        "evaluation_universe_count": selected_cross_scope[1],
        "source_count": scope[7],
        "s2_state_counts": {
            "TRUE": scope[11],
            "FALSE": scope[12],
            "UNKNOWN": scope[13],
        },
        "source_symbols": symbols,
    }


def read_source_stock_seed_fact(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_capture_id: int,
    profile_revision: str | None = None,
    source_symbol: str,
    expected_source_count: int,
    producer_version: str = "SOURCE_POOL_SEED_CORE_PROFILE_R2",
    cross_section_universe_capture_id: int | None = None,
    cross_section_universe_version: str | None = None,
    cross_section_contract_version: str | None = None,
    universe_match_status: str | None = None,
) -> dict[str, Any]:
    """按目标日和来源证券读取单股Seed行；分母须来自已闭合的Raw版本目录。"""
    if (
        type(target_session) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(source_symbol, str)
        or _SYMBOL.fullmatch(source_symbol) is None
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or producer_version != "SOURCE_POOL_SEED_CORE_PROFILE_R2"
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
       count(DISTINCT source_capture_id)::bigint AS content_count,
       min(source_capture_id) AS source_capture_id
FROM {_SEED_TABLE}
WHERE target_session=%s::date
  AND producer_version=%s""",
            (
                target_session,
                producer_version,
            ),
        )
        selected_rows, selected_contents, selected_content = cursor.fetchone()
        selected_rows = int(selected_rows)
        if selected_rows == 0:
            return {
                "status": "NOT_COMPUTED",
                "reason_code": "SEED_NOT_COMPUTED",
                "message": "该目标日没有已保存Seed事实。",
                "partition": {
                    "stored_rows": 0,
                    "expected_source_count": expected_source_count,
                    "closed": False,
                },
            }
        if (
            selected_rows != expected_source_count
            or selected_contents != 1
            or int(selected_content) != source_capture_id
        ):
            return {
                "status": "INCOMPLETE",
                "reason_code": "SEED_PARTITION_NOT_CLOSED",
                "message": "Seed行数、来源分母或内容摘要未闭合。",
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
 AND p.source_symbol=s.source_symbol
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
  AND s.source_symbol=%s
  AND s.producer_version=%s
LIMIT 2""",
            (
                target_session,
                source_symbol,
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
        or row["price_coordinate"] != "FORWARD_ADJUSTED"
        or row["source_capture_id"] != source_capture_id
        or row["history_window_end"] != target_session
        or not row["history_window_start"] <= target_session
        or not 1 <= row["history_session_count"] <= 32767
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
    source_capture_id: int | None = None,
    profile_revision: str | None = None,
    producer_version: str = "SOURCE_POOL_SEED_CORE_PROFILE_R2",
) -> dict[str, dict[str, Any]]:
    """按唯一目标日与证券连接Seed与同窗口画像，返回来源明确的收益字段。"""
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
 AND p.source_symbol=s.source_symbol
 AND p.history_window_start=s.history_window_start AND p.history_window_end=s.history_window_end
 AND p.history_requested_session_count=s.history_session_count
 AND p.history_calendar_id=s.calendar_id
 AND p.price_coordinate=s.price_coordinate
WHERE s.target_session=%s
  AND s.producer_version=%s ORDER BY s.source_symbol""",
            (
                target_session,
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


def read_price_limit_facts_by_symbol(
    config: Mapping[str, Any],
    *,
    target_session: date,
    calendar_revision: str | None = None,
) -> dict[str, dict[str, Any]]:
    """按目标日批量只读涨跌停限价日事实，供Seed与正式准入候选制度门消费。"""
    if type(target_session) is not date:
        raise ValueError("INVALID_TARGET_SESSION")
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            "SELECT to_regclass('stock.source_price_limit_day_facts') IS NOT NULL"
        )
        if cursor.fetchone() != (True,):
            return {}
        if calendar_revision is not None:
            cursor.execute(
                """SELECT source_symbol, rule_id, limit_status, quality, reason
                   FROM stock.source_price_limit_day_facts
                   WHERE target_session = %s
                     AND contract_version = 'STOCK_A_PRICE_LIMIT_ENGINE_R1'
                     AND calendar_revision = %s""",
                (target_session, calendar_revision),
            )
        else:
            cursor.execute(
                """SELECT source_symbol, rule_id, limit_status, quality, reason
                   FROM stock.source_price_limit_day_facts
                   WHERE target_session = %s
                     AND contract_version = 'STOCK_A_PRICE_LIMIT_ENGINE_R1'""",
                (target_session,),
            )
        rows = cursor.fetchall()
    return {
        str(sym): {
            "rule_id": str(rule_id) if rule_id is not None else None,
            "limit_status": str(limit_status)
            if limit_status is not None
            else "UNKNOWN",
            "quality": str(quality) if quality is not None else "UNKNOWN",
            "reason": str(reason) if reason is not None else None,
        }
        for sym, rule_id, limit_status, quality, reason in rows
    }


def list_saved_d0_sessions(
    config: Mapping[str, Any],
    *,
    scenario: str,
    start_session: date,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
) -> tuple[date, ...]:
    """按场景列出指定范围内PG已闭合到来源分母的D0交易日。"""
    if scenario not in _D0_PREDICATES:
        raise ValueError("STATIC_D0_SCENARIO_INVALID")
    effective_end = min(target_session, research_cutoff)
    if effective_end < start_session:
        return ()
    with source_raw._transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            f"""SELECT target_session
                FROM {_D0_TABLE}
                WHERE scenario = %s
                  AND source_capture_id = %s
                  AND price_coordinate = 'FORWARD_ADJUSTED'
                  AND target_session >= %s
                  AND target_session <= %s
                GROUP BY target_session, source_count
                HAVING COUNT(DISTINCT source_symbol) = source_count
                ORDER BY target_session ASC""",
            (scenario, source_capture_id, start_session, effective_end),
        )
        return tuple(row[0] for row in cursor.fetchall())


__all__ = [
    "StaticResearchPgError",
    "list_saved_d0_sessions",
    "persist_static_research_day",
    "read_price_limit_facts_by_symbol",
    "read_source_d0_scenario_day",
    "read_source_pool_seed_facts",
    "read_seed_profile_returns",
]
