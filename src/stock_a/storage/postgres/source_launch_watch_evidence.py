"""读取首页 D0 变化对象对应的已保存 Seed、D1 与规则事实。"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
import re
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres import source_raw


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_RULE_IDS = (
    "LAUNCH_CONFIRM",
    "RECOVERY_TURN",
    "s1",
    "s2",
    "seed",
    "D1_T0",
    "D1_ANCHOR_WINDOW",
    "STRONG_PULLBACK_DIAGNOSTIC",
    "TREND_CONTINUE_DIAGNOSTIC",
)
_D1_STRUCTURE_TYPES = (
    "PIVOT_LOW",
    "PIVOT_HIGH",
    "RANGE_FLOOR",
    "RANGE_CEILING",
    "GAP_ZONE",
)
_VALID_STATES = frozenset({"TRUE", "FALSE", "UNKNOWN"})


def _scope_status(rows: Sequence[tuple[Any, ...]], expected_count: int) -> str:
    if not rows:
        return "NOT_COMPUTED"
    if len(rows) != 1:
        return "AMBIGUOUS"
    row_count, symbol_count = int(rows[0][-2]), int(rows[0][-1])
    return "AVAILABLE" if row_count >= symbol_count == expected_count else "INCOMPLETE"


def _state(value: Any) -> str:
    return value if isinstance(value, str) and value in _VALID_STATES else "UNKNOWN"


def _reasons(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [item for item in value if isinstance(item, str) and item]


def read_source_launch_watch_evidence(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    expected_source_count: int,
    source_symbols: Sequence[str],
    d1_history_session_count: int | None = None,
) -> dict[str, Any]:
    """按同一目标日、来源包及捕获读取候选代码的已存研究证据。"""
    symbols = tuple(sorted(set(source_symbols)))
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(expected_source_count) is not int
        or expected_source_count <= 0
        or any(
            not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None
            for symbol in symbols
        )
        or len(symbols) != len(source_symbols)
        or len(symbols) > expected_source_count
        or (
            d1_history_session_count is not None
            and (
                type(d1_history_session_count) is not int
                or not 1 <= d1_history_session_count <= 32767
            )
        )
    ):
        raise ValueError("D0首页研究证据读取键无效")
    if not symbols:
        return {
            "status": "AVAILABLE",
            "expected_source_count": expected_source_count,
            "candidate_count": 0,
            "seed_status": "NOT_COMPUTED",
            "d1_status": "NOT_COMPUTED",
            "rules_status": "NOT_COMPUTED",
            "by_symbol": {},
            "execution": {
                "transaction": "REPEATABLE READ READ ONLY",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
        }

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            """SELECT profile_revision, producer_contract, producer_version,
                      history_session_count, calendar_id, calendar_revision,
                      research_mode, price_coordinate,
                      count(*)::bigint, count(DISTINCT source_symbol)::bigint
               FROM source_pool_seed_day_facts
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
               GROUP BY profile_revision, producer_contract, producer_version,
                        history_session_count, calendar_id, calendar_revision,
                        research_mode, price_coordinate
               ORDER BY profile_revision, producer_version""",
            (target_session, source_package_id, source_capture_id),
        )
        seed_scopes = cursor.fetchall()
        seed_scope_status = _scope_status(seed_scopes, expected_source_count)
        seed_scope = seed_scopes[0] if len(seed_scopes) == 1 else None

        cursor.execute(
            """SELECT rule_version, profile_revision, history_session_count,
                      d1_history_session_count, event_window_start, calendar_revision,
                      scope, count(*)::bigint, count(DISTINCT source_symbol)::bigint,
                      count(DISTINCT rule_id)::bigint
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
               GROUP BY rule_version, profile_revision, history_session_count,
                        d1_history_session_count, event_window_start,
                        calendar_revision, scope
               ORDER BY profile_revision, rule_version, event_window_start""",
            (target_session, source_package_id, source_capture_id),
        )
        rule_scopes = cursor.fetchall()
        if not rule_scopes:
            rule_scope_status = "NOT_COMPUTED"
        elif len(rule_scopes) != 1:
            rule_scope_status = "AMBIGUOUS"
        else:
            rule_scope_status = (
                "AVAILABLE"
                if int(rule_scopes[0][7]) == expected_source_count * len(_RULE_IDS)
                and int(rule_scopes[0][8]) == expected_source_count
                and int(rule_scopes[0][9]) == len(_RULE_IDS)
                else "INCOMPLETE"
            )
        rule_scope = rule_scopes[0] if len(rule_scopes) == 1 else None

        d1_scope_params: tuple[Any, ...] = (
            target_session,
            source_package_id,
            source_capture_id,
        )
        d1_count_clause = ""
        if d1_history_session_count is not None:
            d1_count_clause = " AND d1_history_session_count=%s"
            d1_scope_params += (d1_history_session_count,)
        cursor.execute(
            """SELECT d1_history_session_count, calendar_revision,
                      producer_contract, producer_version, material_change_contract,
                      research_mode, price_coordinate, count(*)::bigint,
                      count(DISTINCT source_symbol)::bigint
               FROM source_d1_structure_day_facts
               WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s"""
            + d1_count_clause
            + " GROUP BY d1_history_session_count, calendar_revision, producer_contract,"
            " producer_version, material_change_contract, research_mode, price_coordinate"
            " ORDER BY d1_history_session_count, producer_version",
            d1_scope_params,
        )
        d1_scopes = cursor.fetchall()
        d1_scope_status = _scope_status(d1_scopes, expected_source_count)
        d1_scope = d1_scopes[0] if len(d1_scopes) == 1 else None

        seed_rows: list[tuple[Any, ...]] = []
        if seed_scope is not None:
            cursor.execute(
                """SELECT source_symbol, s1_state, s1_reasons, s2_state, s2_reasons,
                          source_seed_state, source_seed_reasons, safety_state,
                          safety_reasons, target_actual_quote, target_close_quality,
                          target_close_reason, core_ma20_quality, core_ma20_reason
                   FROM source_pool_seed_day_facts
                   WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
                     AND profile_revision=%s AND producer_contract=%s AND producer_version=%s
                     AND history_session_count=%s AND calendar_id=%s AND calendar_revision=%s
                     AND research_mode=%s AND price_coordinate=%s
                     AND source_symbol=ANY(%s)
                   ORDER BY source_symbol""",
                (
                    target_session,
                    source_package_id,
                    source_capture_id,
                    *seed_scope[:8],
                    list(symbols),
                ),
            )
            seed_rows = cursor.fetchall()

        rules_semantics_match = bool(
            seed_scope is not None
            and rule_scope is not None
            and d1_scope is not None
            and rule_scope[1] == seed_scope[0]
            and rule_scope[2] == seed_scope[3]
            and rule_scope[6] == "NON_PIT_EXPLORATORY"
            and rule_scope[3] == d1_scope[0]
            and rule_scope[5] == d1_scope[1]
            and seed_scope[6] == d1_scope[5] == "NON_PIT_RETROSPECTIVE"
            and seed_scope[7] == d1_scope[6] == "RAW_UNADJUSTED"
            and rule_scope[2] > 0
            and rule_scope[3] > 0
        )
        rule_rows: list[tuple[Any, ...]] = []
        if rules_semantics_match and rule_scope is not None:
            cursor.execute(
                """SELECT source_symbol, rule_id, state, reasons, event_count
                   FROM source_rule_evaluations
                   WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
                     AND rule_version=%s AND profile_revision=%s
                     AND history_session_count=%s AND d1_history_session_count=%s
                     AND event_window_start=%s AND calendar_revision=%s
                     AND scope=%s AND source_symbol=ANY(%s) AND rule_id=ANY(%s)
                   ORDER BY source_symbol, rule_id""",
                (
                    target_session,
                    source_package_id,
                    source_capture_id,
                    *rule_scope[:7],
                    list(symbols),
                    list(_RULE_IDS),
                ),
            )
            rule_rows = cursor.fetchall()

        d1_rows: list[tuple[Any, ...]] = []
        if d1_scope is not None:
            cursor.execute(
                """SELECT source_symbol, structure_type, target_price_quality,
                          target_price_reason, generation_state, generation_reason,
                          material_change_anchor_count, persisted_detail_count
                   FROM source_d1_structure_day_facts
                   WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s
                     AND d1_history_session_count=%s AND calendar_revision=%s
                     AND producer_contract=%s AND producer_version=%s
                     AND material_change_contract=%s AND research_mode=%s
                     AND price_coordinate=%s AND source_symbol=ANY(%s)
                   ORDER BY source_symbol, structure_type""",
                (
                    target_session,
                    source_package_id,
                    source_capture_id,
                    *d1_scope[:7],
                    list(symbols),
                ),
            )
            d1_rows = cursor.fetchall()

    seed_by_symbol = {row[0]: row for row in seed_rows}
    rule_by_symbol: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for symbol, rule_id, state, reasons, event_count in rule_rows:
        if rule_id in _RULE_IDS:
            rule_by_symbol[symbol][rule_id] = {
                "state": _state(state),
                "reasons": _reasons(reasons),
                "event_count": int(event_count or 0),
                "scope": "NON_PIT_EXPLORATORY",
            }

    d1_by_symbol: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in d1_rows:
        d1_by_symbol[row[0]].append(row[1:])

    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        seed = seed_by_symbol.get(symbol)
        if seed is None:
            seed_fact: dict[str, Any] = {
                "status": "UNKNOWN",
                "reason_codes": ["SEED_FACT_NOT_SAVED_FOR_EXACT_INPUT"],
            }
        else:
            seed_fact = {
                "status": "AVAILABLE"
                if seed_scope_status == "AVAILABLE"
                else "PARTIAL",
                "s1": {"state": _state(seed[1]), "reasons": _reasons(seed[2])},
                "s2": {"state": _state(seed[3]), "reasons": _reasons(seed[4])},
                "source_seed": {
                    "state": _state(seed[5]),
                    "reasons": _reasons(seed[6]),
                },
                "safety": {"state": _state(seed[7]), "reasons": _reasons(seed[8])},
                "target_actual_quote": bool(seed[9]),
                "target_close_quality": seed[10] or "UNKNOWN",
                "target_close_reason": seed[11],
                "core_ma20_quality": seed[12] or "UNKNOWN",
                "core_ma20_reason": seed[13],
                "reason_codes": []
                if seed_scope_status == "AVAILABLE"
                else ["SEED_SCOPE_PARTITION_INCOMPLETE"],
            }

        rules = rule_by_symbol.get(symbol, {})
        missing_rule_ids = [rule_id for rule_id in _RULE_IDS if rule_id not in rules]
        complete_rules = {
            rule_id: rules.get(
                rule_id,
                {
                    "state": "UNKNOWN",
                    "reasons": ["RULE_FACT_NOT_SAVED_FOR_EXACT_INPUT"],
                    "event_count": 0,
                    "scope": "NON_PIT_EXPLORATORY",
                },
            )
            for rule_id in _RULE_IDS
        }
        known_rule_count = sum(
            item["state"] in {"TRUE", "FALSE"} for item in complete_rules.values()
        )
        if not rules_semantics_match:
            rules_status = "UNKNOWN"
            rule_reason_codes = ["RULE_SCOPE_NOT_MATCHED_TO_SEED"]
        elif missing_rule_ids:
            rules_status = "PARTIAL"
            rule_reason_codes = ["RULE_FACTS_INCOMPLETE_FOR_SOURCE_SYMBOL"]
        else:
            rules_status = "AVAILABLE"
            rule_reason_codes = []

        structures = d1_by_symbol.get(symbol, [])
        if not structures:
            d1_fact: dict[str, Any] = {
                "status": "UNKNOWN",
                "reason_codes": ["D1_STRUCTURE_FACT_NOT_SAVED_FOR_EXACT_INPUT"],
                "structure_fact_count": 0,
                "expected_structure_fact_count": len(_D1_STRUCTURE_TYPES),
                "missing_structure_types": list(_D1_STRUCTURE_TYPES),
                "target_price_quality": "UNKNOWN",
                "target_price_reasons": [],
                "generation_state_counts": {},
                "generation_reason_counts": {},
                "material_change_anchor_count": 0,
                "persisted_detail_count": 0,
            }
        else:
            structure_types = {str(row[0]) for row in structures}
            missing_structure_types = sorted(set(_D1_STRUCTURE_TYPES) - structure_types)
            price_qualities = {str(row[1] or "UNKNOWN") for row in structures}
            target_price_quality = (
                "OBSERVED"
                if price_qualities == {"OBSERVED"}
                else "UNKNOWN"
                if "UNKNOWN" in price_qualities
                else sorted(price_qualities)[0]
            )
            generation_states = Counter(str(row[3] or "UNKNOWN") for row in structures)
            generation_reasons = Counter(
                str(row[4] or "NO_REASON_RECORDED") for row in structures
            )
            d1_fact = {
                "status": "AVAILABLE"
                if d1_scope_status == "AVAILABLE" and not missing_structure_types
                else "PARTIAL",
                "structure_fact_count": len(structures),
                "expected_structure_fact_count": len(_D1_STRUCTURE_TYPES),
                "missing_structure_types": missing_structure_types,
                "target_price_quality": target_price_quality,
                "target_price_reasons": sorted(
                    {str(row[2]) for row in structures if row[2]}
                ),
                "generation_state_counts": dict(sorted(generation_states.items())),
                "generation_reason_counts": dict(sorted(generation_reasons.items())),
                "material_change_anchor_count": sum(
                    int(row[5] or 0) for row in structures
                ),
                "persisted_detail_count": sum(int(row[6] or 0) for row in structures),
                "reason_codes": []
                if d1_scope_status == "AVAILABLE" and not missing_structure_types
                else [
                    "D1_SCOPE_PARTITION_INCOMPLETE"
                    if d1_scope_status != "AVAILABLE"
                    else "D1_STRUCTURE_TYPES_INCOMPLETE_FOR_SOURCE_SYMBOL"
                ],
            }

        fact_statuses = (seed_fact.get("status"), d1_fact.get("status"), rules_status)
        symbol_status = (
            "AVAILABLE"
            if fact_statuses == ("AVAILABLE", "AVAILABLE", "AVAILABLE")
            else "PARTIAL"
            if any(status in {"AVAILABLE", "PARTIAL"} for status in fact_statuses)
            else "UNKNOWN"
        )
        by_symbol[symbol] = {
            "status": symbol_status,
            "seed": seed_fact,
            "d1": d1_fact,
            "rules": {
                "status": rules_status,
                "facts": complete_rules,
                "known_state_count": known_rule_count,
                "expected_rule_count": len(_RULE_IDS),
                "missing_rule_ids": missing_rule_ids,
                "reason_codes": rule_reason_codes,
            },
        }

    scope_consistent = bool(
        seed_scope is not None
        and rule_scope is not None
        and d1_scope is not None
        and rules_semantics_match
        and rule_scope[5] == d1_scope[1]
        and seed_scope[3] == rule_scope[2]
        and rule_scope[3] == d1_scope[0]
        and (
            d1_history_session_count is None or d1_scope[0] == d1_history_session_count
        )
    )
    statuses = (seed_scope_status, d1_scope_status, rule_scope_status)
    overall_status = (
        "AVAILABLE"
        if scope_consistent and statuses == ("AVAILABLE", "AVAILABLE", "AVAILABLE")
        else "PARTIAL"
        if any(status == "AVAILABLE" for status in statuses)
        else "UNKNOWN"
    )
    return {
        "status": overall_status,
        "expected_source_count": expected_source_count,
        "candidate_count": len(symbols),
        "seed_status": seed_scope_status,
        "seed_profile_revision": seed_scope[0] if seed_scope else None,
        "seed_scope": {
            "profile_revision": seed_scope[0],
            "producer_contract": seed_scope[1],
            "producer_version": seed_scope[2],
            "history_session_count": seed_scope[3],
            "calendar_id": seed_scope[4],
            "calendar_revision": seed_scope[5],
            "research_mode": seed_scope[6],
            "price_coordinate": seed_scope[7],
            "stored_rows": int(seed_scope[8]),
            "stored_source_symbols": int(seed_scope[9]),
        }
        if seed_scope
        else None,
        "d1_status": d1_scope_status,
        "d1_history_session_count": d1_scope[0] if d1_scope else None,
        "rules_status": rule_scope_status
        if rules_semantics_match
        else "SCOPE_MISMATCH",
        "rule_scope": {
            "profile_revision": rule_scope[1],
            "history_session_count": rule_scope[2],
            "d1_history_session_count": rule_scope[3],
            "event_window_start": rule_scope[4].isoformat()
            if isinstance(rule_scope[4], date)
            else str(rule_scope[4]),
            "calendar_revision": rule_scope[5],
            "scope": rule_scope[6],
            "stored_rows": int(rule_scope[7]),
            "stored_source_symbols": int(rule_scope[8]),
            "stored_rule_ids": int(rule_scope[9]),
        }
        if rule_scope
        else None,
        "d1_scope": {
            "history_session_count": d1_scope[0],
            "calendar_revision": d1_scope[1],
            "producer_contract": d1_scope[2],
            "producer_version": d1_scope[3],
            "material_change_contract": d1_scope[4],
            "research_mode": d1_scope[5],
            "price_coordinate": d1_scope[6],
            "stored_rows": int(d1_scope[7]),
            "stored_source_symbols": int(d1_scope[8]),
        }
        if d1_scope
        else None,
        "by_symbol": by_symbol,
        "execution": {
            "transaction": "REPEATABLE READ READ ONLY",
            "tables_read": [
                "source_pool_seed_day_facts",
                "source_d1_structure_day_facts",
                "source_rule_evaluations",
            ],
            "candidate_symbol_count": len(symbols),
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "raw_history_recalculated": False,
            "static_research_recalculated": False,
        },
    }


__all__ = ["read_source_launch_watch_evidence"]
