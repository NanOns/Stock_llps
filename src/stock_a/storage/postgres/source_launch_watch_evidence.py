"""读取首页 D0 变化对象对应的已保存 Seed、D1 与规则事实。"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
import re
import time
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
    single_symbol_only: bool = False,
    expected_before_session: date | None = None,
    selected_profile_revision: str | None = None,
    membership_snapshot_ids: Mapping[str, int | None] | None = None,
    _scope_symbol: str | None = None,
) -> dict[str, Any]:
    """按同一目标日、来源包及捕获读取候选代码的已存研究证据。"""
    reader_started = time.perf_counter()
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
        or type(single_symbol_only) is not bool
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

    if single_symbol_only:
        if len(symbols) != 1:
            raise ValueError("单股研究证据读取必须只包含一个来源代码")
        if expected_before_session is not None and (
            type(expected_before_session) is not date
            or expected_before_session >= target_session
        ):
            raise ValueError("单股D0前态必须是目标日前的官方会话")
        if selected_profile_revision is not None and not isinstance(
            selected_profile_revision, str
        ):
            raise ValueError("单股研究证据画像修订键无效")
        if membership_snapshot_ids is not None and (
            not isinstance(membership_snapshot_ids, Mapping)
            or set(membership_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
            or any(
                value is not None and (type(value) is not int or value <= 0)
                for value in membership_snapshot_ids.values()
            )
        ):
            raise ValueError("单股成员快照必须显式提供行业与概念两个键")
        return read_source_launch_watch_evidence(
            config,
            target_session=target_session,
            source_package_id=source_package_id,
            source_capture_id=source_capture_id,
            expected_source_count=expected_source_count,
            source_symbols=symbols,
            d1_history_session_count=d1_history_session_count,
            expected_before_session=expected_before_session,
            selected_profile_revision=selected_profile_revision,
            membership_snapshot_ids=membership_snapshot_ids,
            _scope_symbol=symbols[0],
        )

    if _scope_symbol is not None and (len(symbols) != 1 or symbols[0] != _scope_symbol):
        raise ValueError("有界研究证据范围必须只含绑定的单一来源代码")
    scoped_source_count = 1 if _scope_symbol is not None else expected_source_count

    with source_raw._transaction(
        config,
        write=False,
        statement_timeout_ms=15000 if _scope_symbol is not None else 30000,
    ) as (
        _,
        cursor,
        _connection_identity,
    ):
        seed_scope_filter = " AND source_symbol=%s" if _scope_symbol else ""
        if selected_profile_revision is not None:
            seed_scope_filter += " AND profile_revision=%s"
        seed_scope_params: tuple[Any, ...] = (
            target_session,
            source_package_id,
            source_capture_id,
        )
        if _scope_symbol:
            seed_scope_params += (_scope_symbol,)
        if selected_profile_revision is not None:
            seed_scope_params += (selected_profile_revision,)
        cursor.execute(
            """SELECT profile_revision, producer_contract, producer_version,
                      history_session_count, calendar_id, calendar_revision,
                      research_mode, price_coordinate,
                      count(*)::bigint, count(DISTINCT source_symbol)::bigint
               FROM source_pool_seed_day_facts
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s"""
            + seed_scope_filter
            + """
               GROUP BY profile_revision, producer_contract, producer_version,
                        history_session_count, calendar_id, calendar_revision,
                        research_mode, price_coordinate
               ORDER BY profile_revision, producer_version""",
            seed_scope_params,
        )
        seed_scopes = cursor.fetchall()
        seed_scope_status = _scope_status(seed_scopes, scoped_source_count)
        seed_scope = seed_scopes[0] if len(seed_scopes) == 1 else None

        rule_scope_filter = " AND source_symbol=%s" if _scope_symbol else ""
        if selected_profile_revision is not None:
            rule_scope_filter += " AND profile_revision=%s"
        rule_scope_params: tuple[Any, ...] = (
            target_session,
            source_package_id,
            source_capture_id,
        )
        if _scope_symbol:
            rule_scope_params += (_scope_symbol,)
        if selected_profile_revision is not None:
            rule_scope_params += (selected_profile_revision,)
        cursor.execute(
            """SELECT rule_version, profile_revision, history_session_count,
                      d1_history_session_count, event_window_start, calendar_revision,
                      scope, count(*)::bigint, count(DISTINCT source_symbol)::bigint,
                      count(DISTINCT rule_id)::bigint
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s"""
            + rule_scope_filter
            + """
               GROUP BY rule_version, profile_revision, history_session_count,
                        d1_history_session_count, event_window_start,
                        calendar_revision, scope
               ORDER BY profile_revision, rule_version, event_window_start""",
            rule_scope_params,
        )
        rule_scopes = cursor.fetchall()
        if not rule_scopes:
            rule_scope_status = "NOT_COMPUTED"
        elif len(rule_scopes) != 1:
            rule_scope_status = "AMBIGUOUS"
        else:
            rule_scope_status = (
                "AVAILABLE"
                if int(rule_scopes[0][7]) == scoped_source_count * len(_RULE_IDS)
                and int(rule_scopes[0][8]) == scoped_source_count
                and int(rule_scopes[0][9]) == len(_RULE_IDS)
                else "INCOMPLETE"
            )
        rule_scope = rule_scopes[0] if len(rule_scopes) == 1 else None

        d1_scope_params: tuple[Any, ...] = (
            target_session,
            source_package_id,
            source_capture_id,
        )
        d1_symbol_clause = " AND source_symbol=%s" if _scope_symbol else ""
        if _scope_symbol:
            d1_scope_params += (_scope_symbol,)
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
            + d1_symbol_clause
            + d1_count_clause
            + " GROUP BY d1_history_session_count, calendar_revision, producer_contract,"
            " producer_version, material_change_contract, research_mode, price_coordinate"
            " ORDER BY d1_history_session_count, producer_version",
            d1_scope_params,
        )
        d1_scopes = cursor.fetchall()
        d1_scope_status = _scope_status(d1_scopes, scoped_source_count)
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
                """SELECT source_symbol, rule_id, state, reasons, event_count,
                          frozen_line_numerator, frozen_line_denominator, frozen_line_source
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

        single_context: dict[str, Any] | None = None
        if _scope_symbol is not None:
            single_context = _read_single_symbol_context(
                cursor,
                target_session=target_session,
                source_package_id=source_package_id,
                source_capture_id=source_capture_id,
                expected_source_count=expected_source_count,
                source_symbol=_scope_symbol,
                expected_before_session=expected_before_session,
                selected_profile_revision=selected_profile_revision,
                membership_snapshot_ids=membership_snapshot_ids,
                d1_scope=d1_scope,
            )

    seed_by_symbol = {row[0]: row for row in seed_rows}
    rule_by_symbol: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for (
        symbol,
        rule_id,
        state,
        reasons,
        event_count,
        frozen_line_numerator,
        frozen_line_denominator,
        frozen_line_source,
    ) in rule_rows:
        if rule_id in _RULE_IDS:
            fact = {
                "state": _state(state),
                "reasons": _reasons(reasons),
                "event_count": int(event_count or 0),
                "scope": "NON_PIT_EXPLORATORY",
            }
            if (
                fact["state"] == "TRUE"
                and frozen_line_numerator is not None
                and frozen_line_denominator is not None
                and int(frozen_line_denominator) > 0
                and isinstance(frozen_line_source, str)
                and frozen_line_source
            ):
                fact["failure_line_candidate"] = {
                    "value": {
                        "numerator": str(frozen_line_numerator),
                        "denominator": str(frozen_line_denominator),
                    },
                    "source": frozen_line_source,
                    "price_coordinate": "RAW_UNADJUSTED",
                    "scope": "NON_PIT_EXPLORATORY",
                }
            rule_by_symbol[symbol][rule_id] = fact

    d1_by_symbol: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in d1_rows:
        d1_by_symbol[row[0]].append(row[1:])

    by_symbol: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        seed = seed_by_symbol.get(symbol)
        if seed is None:
            seed_fact: dict[str, Any] = {
                "status": "NOT_COMPUTED"
                if seed_scope_status == "NOT_COMPUTED"
                else "UNKNOWN",
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
        if _scope_symbol is not None and seed_scope is not None:
            seed_fact["scope"] = {
                "profile_revision": seed_scope[0],
                "producer_contract": seed_scope[1],
                "producer_version": seed_scope[2],
                "history_session_count": int(seed_scope[3]),
                "calendar_id": seed_scope[4],
                "calendar_revision": seed_scope[5],
                "research_mode": seed_scope[6],
                "price_coordinate": seed_scope[7],
                "verification_scope": "SINGLE_SOURCE_SYMBOL_ROW_ONLY",
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
        if rule_scope_status == "NOT_COMPUTED":
            rules_status = "NOT_COMPUTED"
            rule_reason_codes = ["RULE_FACTS_NOT_COMPUTED_FOR_EXACT_INPUT"]
        elif not rules_semantics_match:
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
                "status": "NOT_COMPUTED"
                if d1_scope_status == "NOT_COMPUTED"
                else "UNKNOWN",
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

        if _scope_symbol is not None and d1_scope is not None:
            d1_fact["scope"] = {
                "history_session_count": int(d1_scope[0]),
                "calendar_revision": d1_scope[1],
                "producer_contract": d1_scope[2],
                "producer_version": d1_scope[3],
                "material_change_contract": d1_scope[4],
                "research_mode": d1_scope[5],
                "price_coordinate": d1_scope[6],
                "history_window_start": single_context["d1_structures"][0][
                    "history_window_start"
                ]
                if single_context["d1_structures"]
                else None,
                "history_window_end": single_context["d1_structures"][0][
                    "history_window_end"
                ]
                if single_context["d1_structures"]
                else None,
                "verification_scope": "SINGLE_SOURCE_SYMBOL_ROW_ONLY",
            }
            d1_fact["structures"] = single_context["d1_structures"]

        fact_statuses = (seed_fact.get("status"), d1_fact.get("status"), rules_status)
        symbol_status = (
            "AVAILABLE"
            if fact_statuses == ("AVAILABLE", "AVAILABLE", "AVAILABLE")
            else "PARTIAL"
            if any(
                status in {"AVAILABLE", "AVAILABLE_EMPTY", "PARTIAL"}
                for status in fact_statuses
            )
            else "UNKNOWN"
            if "UNKNOWN" in fact_statuses
            else "NOT_COMPUTED"
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
                **(
                    {
                        "scope": {
                            "rule_version": rule_scope[0],
                            "profile_revision": rule_scope[1],
                            "history_session_count": int(rule_scope[2]),
                            "d1_history_session_count": int(rule_scope[3]),
                            "event_window_start": rule_scope[4].isoformat()
                            if isinstance(rule_scope[4], date)
                            else str(rule_scope[4]),
                            "calendar_revision": rule_scope[5],
                            "scope": rule_scope[6],
                        }
                    }
                    if _scope_symbol is not None and rule_scope is not None
                    else {}
                ),
            },
        }
        if _scope_symbol is not None and single_context is not None:
            by_symbol[symbol].update(
                {
                    "d0": single_context["d0"],
                    "sector_context": single_context["sector_context"],
                }
            )
            evidence_statuses = (
                by_symbol[symbol]["seed"]["status"],
                by_symbol[symbol]["d1"]["status"],
                by_symbol[symbol]["rules"]["status"],
                by_symbol[symbol]["d0"]["status"],
                by_symbol[symbol]["sector_context"]["status"],
            )
            by_symbol[symbol]["status"] = (
                "AVAILABLE"
                if all(status == "AVAILABLE" for status in evidence_statuses)
                else "PARTIAL"
                if any(
                    status in {"AVAILABLE", "AVAILABLE_EMPTY", "PARTIAL"}
                    for status in evidence_statuses
                )
                else "UNKNOWN"
                if "UNKNOWN" in evidence_statuses
                else "NOT_COMPUTED"
            )

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
        if any(status in {"AVAILABLE", "PARTIAL"} for status in statuses)
        else "UNKNOWN"
        if "UNKNOWN" in statuses
        else "NOT_COMPUTED"
    )
    if _scope_symbol is not None and by_symbol:
        overall_status = by_symbol[_scope_symbol]["status"]
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
            "rule_version": rule_scope[0],
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
            "whole_universe_scanned": _scope_symbol is None,
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "raw_history_recalculated": False,
            "static_research_recalculated": False,
            "elapsed_seconds": round(time.perf_counter() - reader_started, 6),
            "tables_read": [
                "source_pool_seed_day_facts",
                "source_d1_structure_day_facts",
                "source_rule_evaluations",
            ]
            + (
                [
                    "source_d0_scenario_day_facts",
                    "source_d0_launch_day_facts",
                    "tdx_sector_source_snapshots",
                    "tdx_sector_catalog",
                    "tdx_sector_members",
                    "source_sector_cross_period_pulse_facts",
                ]
                if _scope_symbol is not None
                else []
            ),
        },
    }


def _read_single_symbol_context(
    cursor: Any,
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    expected_source_count: int,
    source_symbol: str,
    expected_before_session: date | None,
    selected_profile_revision: str | None,
    membership_snapshot_ids: Mapping[str, int | None] | None,
    d1_scope: tuple[Any, ...] | None,
) -> dict[str, Any]:
    """在现役研究 Reader 的只读快照内读取单股 D0、D1 明细及板块 PULSE。"""
    scenarios: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    cursor.execute(
        """SELECT scenario, scenario_state, scenario_reason, predicate_ids,
                  predicate_states, predicate_qualities, predicate_reason_codes,
                  predicate_value_numerators, predicate_value_denominators,
                  history_window_start, history_session_count, source_count,
                  producer_version, calendar_revision, research_mode,
                  price_coordinate
           FROM source_d0_scenario_day_facts
           WHERE target_session=%s AND source_package_id=%s
             AND source_capture_id=%s AND source_symbol=%s
             AND scenario=ANY(%s)
           ORDER BY scenario, producer_version, calendar_revision""",
        (
            target_session,
            source_package_id,
            source_capture_id,
            source_symbol,
            ["LAUNCH_CONFIRM", "RECOVERY_TURN"],
        ),
    )
    for row in cursor.fetchall():
        scenarios[row[0]].append(row)

    scenario_facts: dict[str, dict[str, Any]] = {}
    scenario_scopes: dict[str, tuple[Any, ...]] = {}
    for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
        rows = scenarios.get(scenario, [])
        if len(rows) != 1:
            scenario_facts[scenario] = {
                "status": "NOT_COMPUTED" if not rows else "AMBIGUOUS",
                "state": "UNKNOWN",
                "reasons": [
                    "D0_SCENARIO_FACT_NOT_SAVED_FOR_EXACT_INPUT"
                    if not rows
                    else "D0_SCENARIO_SCOPE_AMBIGUOUS_FOR_SOURCE_SYMBOL"
                ],
                "predicate_details": [],
            }
            continue
        row = rows[0]
        scenario_scopes[scenario] = (row[12], row[13], row[14], row[15])
        identity_ok = (
            row[11] == expected_source_count
            and row[9] <= target_session
            and row[14] == "NON_PIT_RETROSPECTIVE"
            and row[15] == "RAW_UNADJUSTED"
        )
        scenario_facts[scenario] = {
            "status": "AVAILABLE" if identity_ok else "NOT_COMPARABLE",
            "state": _state(row[1]),
            "reasons": [row[2]] if row[2] else [],
            "reason_codes": [] if identity_ok else ["D0_SCENARIO_SCOPE_MISMATCH"],
            "predicate_details": [
                {
                    "predicate_id": values[0],
                    "state": _state(values[1]),
                    "quality": values[2],
                    "reason_code": values[3],
                    "value_numerator": str(values[4])
                    if values[4] is not None
                    else None,
                    "value_denominator": str(values[5])
                    if values[5] is not None
                    else None,
                }
                for values in zip(
                    row[3], row[4], row[5], row[6], row[7], row[8], strict=True
                )
            ],
            "history_window_start": row[9].isoformat(),
            "history_session_count": int(row[10]),
            "source_count": int(row[11]),
            "producer_version": row[12],
            "calendar_revision": row[13],
            "research_mode": row[14],
            "price_coordinate": row[15],
        }

    launch_params: tuple[Any, ...] = (
        target_session,
        source_package_id,
        source_capture_id,
        source_symbol,
    )
    before_clause = ""
    if expected_before_session is not None:
        before_clause = " AND before_session=%s"
        launch_params += (expected_before_session,)
    cursor.execute(
        """SELECT before_session, before_capture_id, target_capture_id,
                  producer_version, algorithm_contract, session_contract,
                  before_window_start, target_window_start, before_source_count,
                  target_source_count, union_source_count, before_state,
                  before_reason, after_state, after_reason, transition,
                  calendar_revision, research_mode, price_coordinate
           FROM source_d0_launch_day_facts
           WHERE target_session=%s AND source_package_id=%s
             AND target_capture_id=%s AND source_symbol=%s"""
        + before_clause
        + " ORDER BY before_session, producer_version, calendar_revision",
        launch_params,
    )
    launch_rows = cursor.fetchall()
    # 相邻日转移事实可使用跨年日历修订；逐日情景按所属年度日历保存。
    launch_matches = [
        row
        for row in launch_rows
        if (
            "LAUNCH_CONFIRM" not in scenario_scopes
            or (row[3], row[17], row[18])
            == (
                scenario_scopes["LAUNCH_CONFIRM"][0],
                scenario_scopes["LAUNCH_CONFIRM"][2],
                scenario_scopes["LAUNCH_CONFIRM"][3],
            )
        )
    ]
    if not launch_rows:
        launch = {
            "status": "NOT_COMPUTED",
            "transition": "NOT_COMPUTED",
            "transition_kind": "NOT_COMPUTED",
            "reason_codes": ["D0_ADJACENT_LAUNCH_FACT_NOT_SAVED_FOR_EXACT_INPUT"],
        }
    elif len(launch_matches) != 1:
        launch = {
            "status": "NOT_COMPARABLE" if not launch_matches else "AMBIGUOUS",
            "transition": "UNKNOWN",
            "transition_kind": "UNKNOWN",
            "reason_codes": [
                "D0_PAIR_SCOPE_NOT_MATCHED_TO_TARGET_SCENARIO"
                if not launch_matches
                else "D0_PAIR_SCOPE_AMBIGUOUS_FOR_SOURCE_SYMBOL"
            ],
        }
    else:
        row = launch_matches[0]
        before_state = _state(row[11]) if row[11] is not None else "UNKNOWN"
        after_state = _state(row[13]) if row[13] is not None else "UNKNOWN"
        cursor.execute(
            """SELECT source_package_id, target_session, normalized_count
               FROM source_captures WHERE capture_id=%s""",
            (row[1],),
        )
        before_capture_rows = cursor.fetchall()
        cursor.execute(
            """SELECT source_package_id, source_capture_id, scenario_state,
                      scenario_reason, source_count, producer_version, research_mode,
                      price_coordinate, history_window_start, history_session_count
               FROM source_d0_scenario_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND source_symbol=%s AND scenario='LAUNCH_CONFIRM'
               ORDER BY producer_version, calendar_revision""",
            (row[0], row[1], source_symbol),
        )
        before_scenario_rows = cursor.fetchall()
        before_scenario = (
            before_scenario_rows[0] if len(before_scenario_rows) == 1 else None
        )
        before_capture_ok = (
            len(before_capture_rows) == 1
            and before_capture_rows[0][1] == row[0]
            and int(before_capture_rows[0][2]) == int(row[8])
        )
        before_scenario_ok = (
            before_scenario is not None
            and before_scenario[2] is not None
            and _state(before_scenario[2]) == before_state
            and int(before_scenario[4]) == int(row[8])
            and before_scenario[5] == row[3]
            and before_scenario[6] == "NON_PIT_RETROSPECTIVE"
            and before_scenario[7] == "RAW_UNADJUSTED"
            and before_scenario[0] == before_capture_rows[0][0]
            and before_scenario[8] == row[6]
            and int(before_scenario[9]) > 0
            and before_scenario[1] == row[1]
        )
        pair_ok = (
            row[0] == expected_before_session
            and row[2] == source_capture_id
            and row[9] == expected_source_count
            and row[17] == "NON_PIT_RETROSPECTIVE"
            and row[18] == "RAW_UNADJUSTED"
            and row[13] is not None
            and before_capture_ok
            and before_scenario_ok
            and scenario_facts["LAUNCH_CONFIRM"]["state"] == after_state
            and scenario_facts["LAUNCH_CONFIRM"]["status"] == "AVAILABLE"
        )
        transition = f"{before_state}_TO_{after_state}"
        transition_ok = row[15] == transition
        transition_kind = (
            "D0_NEW"
            if before_state == "FALSE" and after_state == "TRUE"
            else "CONTINUING"
            if before_state == after_state == "TRUE"
            else "NOT_NEW"
            if before_state in {"FALSE", "TRUE"} and after_state in {"FALSE", "TRUE"}
            else "UNKNOWN"
        )
        if not pair_ok or not transition_ok:
            transition_kind = "UNKNOWN"
        launch = {
            "status": "AVAILABLE" if pair_ok and transition_ok else "UNKNOWN",
            "before": {
                "target_session": row[0].isoformat(),
                "state": _state(before_scenario[2])
                if before_scenario is not None
                else "UNKNOWN",
                "reason": before_scenario[3] if before_scenario is not None else None,
                "source_capture_id": int(row[1]),
                "window_start": row[6].isoformat(),
                "source_count": int(row[8]),
            },
            "after": {
                "target_session": target_session.isoformat(),
                "state": after_state,
                "reason": row[14],
                "source_capture_id": int(row[2]),
                "window_start": row[7].isoformat(),
                "source_count": int(row[9]),
            },
            "transition": row[15] if transition_ok else "UNKNOWN",
            "transition_kind": transition_kind,
            "union_source_count": int(row[10]),
            "producer_version": row[3],
            "algorithm_contract": row[4],
            "session_contract": row[5],
            "calendar_revision": row[16],
            "research_mode": row[17],
            "price_coordinate": row[18],
            "reason_codes": []
            if pair_ok and transition_ok
            else ["D0_PAIR_OR_TARGET_SCENARIO_IDENTITY_MISMATCH"],
        }

    d0 = {
        "status": launch["status"],
        "before": launch.get("before"),
        "after": launch.get("after"),
        "transition": launch.get("transition", "NOT_COMPUTED"),
        "transition_kind": launch.get("transition_kind", "UNKNOWN"),
        "reason_codes": launch.get("reason_codes", []),
        "scenario_status": "AVAILABLE"
        if all(
            scenario_facts[item]["status"] == "AVAILABLE"
            for item in ("LAUNCH_CONFIRM", "RECOVERY_TURN")
        )
        else "PARTIAL"
        if any(
            scenario_facts[item]["status"] == "AVAILABLE"
            for item in ("LAUNCH_CONFIRM", "RECOVERY_TURN")
        )
        else "NOT_COMPUTED",
        "scenarios": scenario_facts,
        "formal_selection_status": "NOT_EVALUATED",
    }

    d1_structures: list[dict[str, Any]] = []
    if d1_scope is not None:
        cursor.execute(
            """SELECT structure_type, history_window_start, history_window_end,
                      target_bar_status, target_bar_reason, target_price_quality,
                      target_price_reason, generation_state, generation_reason,
                      anchor_created_session, anchor_available_from_session,
                      anchor_test_from_session, relation, material_change_anchor_count,
                      persisted_detail_count
               FROM source_d1_structure_day_facts
               WHERE target_session=%s AND source_package_id=%s
                 AND target_capture_id=%s AND source_symbol=%s
                 AND d1_history_session_count=%s AND calendar_revision=%s
                 AND producer_contract=%s AND producer_version=%s
                 AND material_change_contract=%s AND research_mode=%s
                 AND price_coordinate=%s
               ORDER BY structure_type""",
            (
                target_session,
                source_package_id,
                source_capture_id,
                source_symbol,
                *d1_scope[:7],
            ),
        )
        for row in cursor.fetchall():
            d1_structures.append(
                {
                    "structure_type": row[0],
                    "history_window_start": row[1].isoformat(),
                    "history_window_end": row[2].isoformat(),
                    "target_bar_status": row[3],
                    "target_bar_reason": row[4],
                    "target_price_quality": row[5],
                    "target_price_reason": row[6],
                    "generation_state": row[7],
                    "generation_reason": row[8],
                    "anchor_created_session": row[9].isoformat() if row[9] else None,
                    "anchor_available_from_session": row[10].isoformat()
                    if row[10]
                    else None,
                    "anchor_test_from_session": row[11].isoformat()
                    if row[11]
                    else None,
                    "relation": row[12],
                    "material_change_anchor_count": int(row[13] or 0),
                    "persisted_detail_count": int(row[14] or 0),
                }
            )

    snapshot_ids = dict(membership_snapshot_ids or {})
    expected_sources = {
        "INDUSTRY": "TDX_TDXHY_CFG",
        "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
    }
    requested_ids = [
        snapshot_ids[kind]
        for kind in ("INDUSTRY", "CONCEPT")
        if snapshot_ids.get(kind) is not None
    ]
    snapshots: dict[int, tuple[Any, ...]] = {}
    if requested_ids:
        cursor.execute(
            """SELECT snapshot_id, source_kind, observed_at, source_valid_on,
                      research_mode, source_quality
               FROM tdx_sector_source_snapshots
               WHERE snapshot_id=ANY(%s)
               ORDER BY snapshot_id""",
            (requested_ids,),
        )
        snapshots = {int(row[0]): row for row in cursor.fetchall()}
    valid_ids = [
        snapshot_ids[kind]
        for kind, expected_source in expected_sources.items()
        if snapshot_ids.get(kind) in snapshots
        and snapshots[snapshot_ids[kind]][1] == expected_source
        and snapshots[snapshot_ids[kind]][4] == "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
    ]
    memberships: list[dict[str, Any]] = []
    if valid_ids:
        cursor.execute(
            """SELECT catalog.snapshot_id, catalog.board_kind,
                      catalog.board_native_id, catalog.board_name,
                      member.membership_quality, snapshot.observed_at,
                      snapshot.source_valid_on, pulse.id, pulse.profile_revision,
                      pulse.pulse_state, pulse.pulse_reason_codes,
                      pulse.dq5_pp_numerator, pulse.dq5_pp_denominator,
                      pulse.breadth_delta1_numerator,
                      pulse.breadth_delta1_denominator, pulse.member_count,
                      pulse.mapped_count, pulse.unmapped_count,
                      pulse.ret5_denominator, pulse.breadth_denominator,
                      pulse.snapshot_source_quality, pulse.calculation_status,
                      pulse.research_mode, pulse.membership_mode,
                      pulse.price_coordinate, pulse.formal_rotation_state,
                      pulse.source_pool_count, pulse.algorithm_contract,
                      pulse.catalog_id
               FROM tdx_sector_catalog AS catalog
               JOIN tdx_sector_source_snapshots AS snapshot
                 ON snapshot.snapshot_id=catalog.snapshot_id
               JOIN tdx_sector_members AS member
                 ON member.catalog_id=catalog.id AND member.source_symbol=%s
               LEFT JOIN source_sector_cross_period_pulse_facts AS pulse
                 ON pulse.catalog_id=catalog.id AND pulse.target_session=%s
                AND pulse.source_package_id=%s AND pulse.source_capture_id=%s
                AND pulse.algorithm_contract=%s
               WHERE catalog.snapshot_id=ANY(%s)
               ORDER BY catalog.board_kind, catalog.board_native_id,
                        pulse.profile_revision""",
            (
                source_symbol,
                target_session,
                source_package_id,
                source_capture_id,
                "SOURCE_CROSS_PERIOD_PULSE_R1",
                valid_ids,
            ),
        )
        membership_rows = cursor.fetchall()
        try:
            from stock_a.storage.postgres.sector_pulse_facts import _fraction_payload
        except ImportError:  # pragma: no cover
            _fraction_payload = None
        for row in membership_rows:
            pulse_rows = [item for item in membership_rows if item[:3] == row[:3]]
            if any(
                item["snapshot_id"] == int(row[0]) and item["board_native_id"] == row[2]
                for item in memberships
            ):
                continue
            exact = [
                item
                for item in pulse_rows
                if item[7] is not None
                and selected_profile_revision is not None
                and item[8] == selected_profile_revision
            ]
            all_pulses = [item for item in pulse_rows if item[7] is not None]
            if len(exact) > 1:
                pulse_status = "AMBIGUOUS"
                pulse_row = None
                pulse_reasons = ["PULSE_SCOPE_AMBIGUOUS_FOR_MEMBERSHIP"]
            elif not exact and all_pulses:
                pulse_status = "NOT_COMPARABLE"
                pulse_row = None
                pulse_reasons = ["PULSE_PROFILE_REVISION_MISMATCH"]
            elif not exact:
                pulse_status = "NOT_COMPUTED"
                pulse_row = None
                pulse_reasons = ["PULSE_FACTS_NOT_SAVED_FOR_EXACT_INPUT"]
            else:
                pulse_row = exact[0]
                pulse_ok = (
                    pulse_row[21] == "COMPLETE"
                    and pulse_row[22] == "NON_PIT_RETROSPECTIVE"
                    and pulse_row[23] == "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    and pulse_row[24] == "RAW_UNADJUSTED"
                    and pulse_row[25] == "NOT_EVALUATED"
                    and int(pulse_row[18]) == int(pulse_row[15])
                    and int(pulse_row[19]) == int(pulse_row[15])
                    and int(pulse_row[26]) == expected_source_count
                    and pulse_row[27] == "SOURCE_CROSS_PERIOD_PULSE_R1"
                )
                pulse_status = "AVAILABLE" if pulse_ok else "INCOMPLETE"
                pulse_reasons = (
                    _reasons(pulse_row[10])
                    if pulse_ok
                    else ["PULSE_FACT_QUALITY_OR_DENOMINATOR_MISMATCH"]
                )
            pulse_payload: dict[str, Any] = {
                "status": pulse_status,
                "state": _state(pulse_row[9]) if pulse_row else "UNKNOWN",
                "reason_codes": pulse_reasons,
                "transition_status": "NOT_COMPUTED",
                "transition_state": "UNKNOWN",
                "transition_reason_codes": [
                    "PULSE_ADJACENT_TRANSITION_NOT_READ_BY_SINGLE_SYMBOL_SCOPE"
                ],
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            }
            if pulse_row and pulse_status == "AVAILABLE":
                metrics = {
                    "dq5_pp_numerator": pulse_row[11],
                    "dq5_pp_denominator": pulse_row[12],
                    "breadth_delta1_numerator": pulse_row[13],
                    "breadth_delta1_denominator": pulse_row[14],
                }
                if _fraction_payload:
                    pulse_payload["dq5_pp"] = _fraction_payload(metrics, "dq5_pp")
                    pulse_payload["breadth_delta1"] = _fraction_payload(
                        metrics, "breadth_delta1"
                    )
                pulse_payload.update(
                    {
                        "member_count": int(pulse_row[15]),
                        "mapped_count": int(pulse_row[16]),
                        "unmapped_count": int(pulse_row[17]),
                        "ret5_denominator": int(pulse_row[18]),
                        "breadth_denominator": int(pulse_row[19]),
                        "snapshot_source_quality": pulse_row[20],
                        "pulse_fact_id": int(pulse_row[7]),
                        "profile_revision": pulse_row[8],
                        "algorithm_contract": pulse_row[27],
                        "catalog_id": int(pulse_row[28]),
                        "research_mode": pulse_row[22],
                        "price_coordinate": pulse_row[24],
                        "formal_rotation_state": pulse_row[25],
                    }
                )
            memberships.append(
                {
                    "snapshot_id": int(row[0]),
                    "board_kind": row[1],
                    "board_native_id": row[2],
                    "board_name": row[3],
                    "membership_quality": row[4],
                    "observed_at": row[5].isoformat() if row[5] else None,
                    "source_valid_on": row[6].isoformat() if row[6] else None,
                    "pulse_context": pulse_payload,
                }
            )
    missing_kinds = [
        kind
        for kind, expected_source in expected_sources.items()
        if snapshot_ids.get(kind) not in snapshots
        or snapshots[snapshot_ids[kind]][1] != expected_source
        or snapshots[snapshot_ids[kind]][4] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT"
    ]
    sector_fact = {
        "status": "NOT_COMPUTED"
        if membership_snapshot_ids is None
        else "PARTIAL_MEMBERSHIP_UNKNOWN"
        if missing_kinds
        else "AVAILABLE"
        if memberships
        else "AVAILABLE_EMPTY",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "membership_count": len(memberships),
        "memberships": memberships,
        "missing_membership_kinds": missing_kinds,
        "pulse_available_count": sum(
            item["pulse_context"]["status"] == "AVAILABLE" for item in memberships
        ),
        "pulse_not_computed_count": sum(
            item["pulse_context"]["status"] == "NOT_COMPUTED" for item in memberships
        ),
        "pulse_status": "AVAILABLE"
        if any(item["pulse_context"]["status"] == "AVAILABLE" for item in memberships)
        else "NOT_COMPUTED"
        if not memberships
        or any(
            item["pulse_context"]["status"] == "NOT_COMPUTED" for item in memberships
        )
        else "PARTIAL",
    }
    return {
        "d0": d0,
        "sector_context": sector_fact,
        "d1_structures": d1_structures,
    }


def read_source_launch_watch_page(
    config: Mapping[str, Any],
    *,
    before_session: date,
    target_session: date,
    source_package_id: int,
    before_capture_id: int,
    target_capture_id: int,
    expected_source_count: int,
    watch_group: str,
    offset: int,
    limit: int,
    d1_history_session_count: int | None = None,
    board_kind: str | None = None,
    board_native_id: str | None = None,
    board_snapshot_id: int | None = None,
) -> dict[str, Any]:
    """从现有PG研究事实按组聚合并分页，不在HTTP进程整理全来源池。"""
    from stock_a.storage.postgres import source_raw

    groups = {
        "D0_NEW",
        "SEED_RULE",
        "D1_MATERIAL",
        "UNKNOWN",
        "KNOWN_NO_SIGNAL",
    }
    invalid_fields = []
    if type(before_session) is not date or type(target_session) is not date:
        invalid_fields.append("日期类型")
    elif before_session >= target_session:
        invalid_fields.append("相邻日期顺序")
    if type(source_package_id) is not int or source_package_id <= 0:
        invalid_fields.append("来源包ID")
    if type(before_capture_id) is not int or before_capture_id <= 0:
        invalid_fields.append("前日捕获ID")
    if type(target_capture_id) is not int or target_capture_id <= 0:
        invalid_fields.append("目标日捕获ID")
    if type(expected_source_count) is not int or expected_source_count <= 0:
        invalid_fields.append("来源池分母")
    if watch_group not in groups:
        invalid_fields.append("观察分组")
    if type(offset) is not int or offset < 0:
        invalid_fields.append("页偏移")
    if type(limit) is not int or not 1 <= limit <= 30:
        invalid_fields.append("页大小")
    if d1_history_session_count is not None and (
        type(d1_history_session_count) is not int
        or not 1 <= d1_history_session_count <= 75
    ):
        invalid_fields.append("D1历史会话数")
    if (board_kind is None) != (board_native_id is None):
        invalid_fields.append("板块类型与原生ID配对")
    if (board_kind is None) != (board_snapshot_id is None):
        invalid_fields.append("板块快照配对")
    if board_kind is not None and board_kind not in {"INDUSTRY", "CONCEPT"}:
        invalid_fields.append("板块类型")
    if board_native_id is not None and (
        not board_native_id or len(board_native_id) > 64
    ):
        invalid_fields.append("板块原生ID")
    if board_snapshot_id is not None and (
        type(board_snapshot_id) is not int or board_snapshot_id <= 0
    ):
        invalid_fields.append("板块快照ID")
    if invalid_fields:
        raise ValueError("来源观察PG分页键无效：" + "、".join(invalid_fields))

    started = time.perf_counter()
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """WITH params AS (
    SELECT %s::date AS before_session, %s::date AS target_session,
           %s::bigint AS source_package_id, %s::bigint AS before_capture_id,
           %s::bigint AS target_capture_id, %s::bigint AS expected_source_count,
           %s::smallint AS d1_history_session_count, %s::text[] AS rule_ids,
           %s::text AS watch_group, %s::text AS board_kind,
           %s::text AS board_native_id, %s::bigint AS board_snapshot_id,
           %s::integer AS page_offset, %s::integer AS page_limit
), target_pool AS MATERIALIZED (
    SELECT r.source_symbol, r.market
    FROM source_raw_daily r CROSS JOIN params p
    WHERE r.session_date=p.target_session
      AND r.source_package_id=p.source_package_id
      AND r.source_capture_id=p.target_capture_id
), d0_scope_candidates AS MATERIALIZED (
    SELECT d.producer_version, d.algorithm_contract, d.session_contract,
           d.calendar_revision, count(*)::bigint AS fact_count,
           count(DISTINCT d.source_symbol)::bigint AS symbol_count,
           min(d.target_source_count)::bigint AS target_source_count,
           max(d.target_source_count)::bigint AS target_source_count_max,
           min(d.union_source_count)::bigint AS union_source_count,
           max(d.union_source_count)::bigint AS union_source_count_max
    FROM source_d0_launch_day_facts d CROSS JOIN params p
    WHERE d.before_session=p.before_session AND d.target_session=p.target_session
      AND d.source_package_id=p.source_package_id
      AND d.before_capture_id=p.before_capture_id
      AND d.target_capture_id=p.target_capture_id
    GROUP BY d.producer_version, d.algorithm_contract, d.session_contract,
             d.calendar_revision
), d0_scope_status AS (
    SELECT CASE
        WHEN count(*)=0 THEN 'NOT_COMPUTED'
        WHEN count(*)>1 THEN 'AMBIGUOUS'
        WHEN bool_and(fact_count=union_source_count
                      AND symbol_count=union_source_count
                      AND target_source_count=target_source_count_max
                      AND union_source_count=union_source_count_max
                      AND target_source_count=(SELECT expected_source_count FROM params)
                      AND union_source_count>=target_source_count)
            THEN 'AVAILABLE'
        ELSE 'INCOMPLETE'
    END AS status
    FROM d0_scope_candidates
), d0_scope AS (
    SELECT c.* FROM d0_scope_candidates c
    WHERE (SELECT status FROM d0_scope_status)='AVAILABLE'
), d0_by_symbol AS (
    SELECT d.source_symbol, count(*)::integer AS fact_count,
           min(d.transition) AS transition,
           min(d.before_state) AS before_state,
           min(d.after_state) AS after_state
    FROM source_d0_launch_day_facts d
    JOIN d0_scope s ON s.producer_version=d.producer_version
                   AND s.algorithm_contract=d.algorithm_contract
                   AND s.session_contract=d.session_contract
                   AND s.calendar_revision=d.calendar_revision
    CROSS JOIN params p
    WHERE d.before_session=p.before_session AND d.target_session=p.target_session
      AND d.source_package_id=p.source_package_id
      AND d.before_capture_id=p.before_capture_id
      AND d.target_capture_id=p.target_capture_id
    GROUP BY d.source_symbol
), seed_scope_candidates AS MATERIALIZED (
    SELECT s.profile_revision, s.producer_contract, s.producer_version,
           s.history_session_count, s.calendar_id, s.calendar_revision,
           s.research_mode, s.price_coordinate, count(*)::bigint AS fact_count,
           count(DISTINCT s.source_symbol)::bigint AS symbol_count
    FROM source_pool_seed_day_facts s CROSS JOIN params p
    WHERE s.target_session=p.target_session
      AND s.source_package_id=p.source_package_id
      AND s.source_capture_id=p.target_capture_id
    GROUP BY s.profile_revision, s.producer_contract, s.producer_version,
             s.history_session_count, s.calendar_id, s.calendar_revision,
             s.research_mode, s.price_coordinate
), seed_scope_status AS (
    SELECT CASE
        WHEN count(*)=0 THEN 'NOT_COMPUTED'
        WHEN count(*)>1 THEN 'AMBIGUOUS'
        WHEN bool_and(fact_count=(SELECT expected_source_count FROM params)
                      AND symbol_count=(SELECT expected_source_count FROM params))
            THEN 'AVAILABLE'
        ELSE 'INCOMPLETE'
    END AS status
    FROM seed_scope_candidates
), seed_scope AS (
    SELECT c.* FROM seed_scope_candidates c
    WHERE (SELECT status FROM seed_scope_status)='AVAILABLE'
), seed_by_symbol AS (
    SELECT s.source_symbol, count(*)::integer AS fact_count,
           min(s.source_seed_state) AS source_seed_state,
           min(s.s1_state) AS s1_state, min(s.s2_state) AS s2_state,
           min(s.safety_state) AS safety_state
    FROM source_pool_seed_day_facts s JOIN seed_scope k
      ON k.profile_revision=s.profile_revision
     AND k.producer_contract=s.producer_contract
     AND k.producer_version=s.producer_version
     AND k.history_session_count=s.history_session_count
     AND k.calendar_id=s.calendar_id
     AND k.calendar_revision=s.calendar_revision
     AND k.research_mode=s.research_mode
     AND k.price_coordinate=s.price_coordinate
    CROSS JOIN params p
    WHERE s.target_session=p.target_session
      AND s.source_package_id=p.source_package_id
      AND s.source_capture_id=p.target_capture_id
    GROUP BY s.source_symbol
), rule_scope_candidates AS MATERIALIZED (
    SELECT r.rule_version, r.profile_revision, r.history_session_count,
           r.d1_history_session_count, r.event_window_start,
           r.calendar_revision, r.scope, count(*)::bigint AS fact_count,
           count(DISTINCT r.source_symbol)::bigint AS symbol_count,
           count(DISTINCT r.rule_id)::bigint AS rule_count
    FROM source_rule_evaluations r CROSS JOIN params p
    WHERE r.target_session=p.target_session
      AND r.source_package_id=p.source_package_id
      AND r.source_capture_id=p.target_capture_id
      AND r.rule_id=ANY(p.rule_ids)
    GROUP BY r.rule_version, r.profile_revision, r.history_session_count,
             r.d1_history_session_count, r.event_window_start,
             r.calendar_revision, r.scope
), rule_scope_status AS (
    SELECT CASE
        WHEN count(*)=0 THEN 'NOT_COMPUTED'
        WHEN count(*)>1 THEN 'AMBIGUOUS'
        WHEN bool_and(fact_count=(SELECT expected_source_count * cardinality(rule_ids) FROM params)
                      AND symbol_count=(SELECT expected_source_count FROM params)
                      AND rule_count=(SELECT cardinality(rule_ids) FROM params))
            THEN 'AVAILABLE'
        ELSE 'INCOMPLETE'
    END AS status
    FROM rule_scope_candidates
), rule_scope AS (
    SELECT c.* FROM rule_scope_candidates c
    WHERE (SELECT status FROM rule_scope_status)='AVAILABLE'
), rule_by_symbol AS (
    SELECT r.source_symbol, count(DISTINCT r.rule_id)::integer AS fact_count,
           bool_or(r.state='TRUE') AS has_true,
           bool_or(r.state='UNKNOWN') AS has_unknown
    FROM source_rule_evaluations r JOIN rule_scope k
      ON k.rule_version=r.rule_version
     AND k.profile_revision=r.profile_revision
     AND k.history_session_count=r.history_session_count
     AND k.d1_history_session_count=r.d1_history_session_count
     AND k.event_window_start=r.event_window_start
     AND k.calendar_revision=r.calendar_revision
     AND k.scope=r.scope
    CROSS JOIN params p
    WHERE r.target_session=p.target_session
      AND r.source_package_id=p.source_package_id
      AND r.source_capture_id=p.target_capture_id
      AND r.rule_id=ANY(p.rule_ids)
    GROUP BY r.source_symbol
), d1_scope_candidates AS MATERIALIZED (
    SELECT d.d1_history_session_count, d.calendar_revision,
           d.producer_contract, d.producer_version,
           d.material_change_contract, d.research_mode, d.price_coordinate,
           count(*)::bigint AS fact_count,
           count(DISTINCT d.source_symbol)::bigint AS symbol_count
    FROM source_d1_structure_day_facts d CROSS JOIN params p
    WHERE d.target_session=p.target_session
      AND d.source_package_id=p.source_package_id
      AND d.target_capture_id=p.target_capture_id
      AND (p.d1_history_session_count IS NULL
           OR d.d1_history_session_count=p.d1_history_session_count)
    GROUP BY d.d1_history_session_count, d.calendar_revision,
             d.producer_contract, d.producer_version,
             d.material_change_contract, d.research_mode, d.price_coordinate
), d1_scope_status AS (
    SELECT CASE
        WHEN count(*)=0 THEN 'NOT_COMPUTED'
        WHEN count(*)>1 THEN 'AMBIGUOUS'
        WHEN bool_and(fact_count>=symbol_count
                      AND symbol_count=(SELECT expected_source_count FROM params))
            THEN 'AVAILABLE'
        ELSE 'INCOMPLETE'
    END AS status
    FROM d1_scope_candidates
), d1_scope AS (
    SELECT c.* FROM d1_scope_candidates c
    WHERE (SELECT status FROM d1_scope_status)='AVAILABLE'
), d1_by_symbol AS (
    SELECT d.source_symbol, count(DISTINCT d.structure_type)::integer AS structure_count,
           sum(d.material_change_anchor_count)::integer AS material_anchor_count,
           bool_or(d.target_price_quality='UNKNOWN') AS unknown_price
    FROM source_d1_structure_day_facts d JOIN d1_scope k
      ON k.d1_history_session_count=d.d1_history_session_count
     AND k.calendar_revision=d.calendar_revision
     AND k.producer_contract=d.producer_contract
     AND k.producer_version=d.producer_version
     AND k.material_change_contract=d.material_change_contract
     AND k.research_mode=d.research_mode
     AND k.price_coordinate=d.price_coordinate
    CROSS JOIN params p
    WHERE d.target_session=p.target_session
      AND d.source_package_id=p.source_package_id
      AND d.target_capture_id=p.target_capture_id
    GROUP BY d.source_symbol
), scope_status AS (
    SELECT (SELECT status FROM d0_scope_status) AS d0,
           (SELECT status FROM seed_scope_status) AS seed,
           (SELECT status FROM rule_scope_status) AS rules,
           (SELECT status FROM d1_scope_status) AS d1,
           CASE WHEN (SELECT status FROM seed_scope_status)='AVAILABLE'
                  AND (SELECT status FROM rule_scope_status)='AVAILABLE'
                  AND (SELECT status FROM d1_scope_status)='AVAILABLE'
                THEN (SELECT r.profile_revision=s.profile_revision
                             AND r.history_session_count=s.history_session_count
                             AND r.d1_history_session_count=d.d1_history_session_count
                             AND r.calendar_revision=d.calendar_revision
                             AND r.scope='NON_PIT_EXPLORATORY'
                             AND s.research_mode='NON_PIT_RETROSPECTIVE'
                             AND d.research_mode='NON_PIT_RETROSPECTIVE'
                             AND s.price_coordinate=d.price_coordinate
                             AND s.price_coordinate='RAW_UNADJUSTED'
                      FROM rule_scope r CROSS JOIN seed_scope s CROSS JOIN d1_scope d)
                ELSE FALSE END AS rules_match_seed
), base AS (
    SELECT p.source_symbol, p.market, d.transition, d.before_state, d.after_state,
           d.fact_count AS d0_fact_count, s.fact_count AS seed_fact_count,
           s.source_seed_state, s.s1_state, s.s2_state, s.safety_state,
           r.fact_count AS rule_fact_count, r.has_true AS rules_have_true,
           r.has_unknown AS rules_have_unknown,
           x.structure_count AS d1_structure_count,
           x.material_anchor_count AS d1_material_anchor_count,
           x.unknown_price AS d1_unknown_price,
           g.d0 AS d0_scope_status, g.seed AS seed_scope_status,
           g.rules AS rule_scope_status, g.d1 AS d1_scope_status,
           g.rules_match_seed,
           COALESCE(s.source_seed_state='TRUE' OR s.s1_state='TRUE'
                    OR s.s2_state='TRUE' OR r.has_true, FALSE) AS has_seed_rule,
           COALESCE(x.material_anchor_count, 0)>0 AS has_d1_material,
           (g.d0<>'AVAILABLE' OR d.fact_count IS DISTINCT FROM 1
             OR d.transition NOT IN ('FALSE_TO_TRUE','FALSE_TO_FALSE','FALSE_TO_UNKNOWN',
                                     'TRUE_TO_TRUE','TRUE_TO_FALSE','TRUE_TO_UNKNOWN',
                                     'UNKNOWN_TO_TRUE','UNKNOWN_TO_FALSE','UNKNOWN_TO_UNKNOWN',
                                     'AFTER_ONLY')
             OR position('UNKNOWN' in COALESCE(d.transition,''))>0
             OR g.seed<>'AVAILABLE' OR s.fact_count IS DISTINCT FROM 1
             OR s.source_seed_state IS NULL OR s.source_seed_state NOT IN ('TRUE','FALSE')
             OR s.s1_state IS NULL OR s.s1_state NOT IN ('TRUE','FALSE')
             OR s.s2_state IS NULL OR s.s2_state NOT IN ('TRUE','FALSE')
             OR s.safety_state IS NULL OR s.safety_state NOT IN ('TRUE','FALSE')
             OR g.rules<>'AVAILABLE' OR NOT g.rules_match_seed
             OR r.fact_count IS DISTINCT FROM (SELECT cardinality(rule_ids) FROM params)
             OR COALESCE(r.has_unknown, TRUE)
             OR g.d1<>'AVAILABLE' OR x.structure_count IS DISTINCT FROM 5
             OR COALESCE(x.unknown_price, TRUE)) AS has_unknown_evidence
    FROM target_pool p
    LEFT JOIN d0_by_symbol d ON d.source_symbol=p.source_symbol
    LEFT JOIN seed_by_symbol s ON s.source_symbol=p.source_symbol
    LEFT JOIN rule_by_symbol r ON r.source_symbol=p.source_symbol
    LEFT JOIN d1_by_symbol x ON x.source_symbol=p.source_symbol
    CROSS JOIN scope_status g
), classified AS MATERIALIZED (
    SELECT b.*,
           CASE WHEN transition='FALSE_TO_TRUE' THEN 'D0_NEW'
                WHEN has_seed_rule THEN 'SEED_RULE'
                WHEN has_d1_material THEN 'D1_MATERIAL'
                WHEN has_unknown_evidence THEN 'UNKNOWN'
                ELSE 'KNOWN_NO_SIGNAL' END AS watch_group,
           array_remove(ARRAY[
               CASE WHEN d0_scope_status<>'AVAILABLE' OR d0_fact_count IS DISTINCT FROM 1
                    THEN 'D0_PAIR_FACT_NOT_SAVED_FOR_EXACT_INPUT' END,
               CASE WHEN position('UNKNOWN' in COALESCE(transition,''))>0
                    THEN 'D0_ENDPOINT_STATE_UNKNOWN' END,
               CASE WHEN seed_scope_status<>'AVAILABLE' OR seed_fact_count IS DISTINCT FROM 1
                    THEN 'SEED_SCOPE_OR_SYMBOL_FACT_INCOMPLETE' END,
               CASE WHEN rule_scope_status<>'AVAILABLE' OR NOT rules_match_seed
                         OR rule_fact_count IS DISTINCT FROM (SELECT cardinality(rule_ids) FROM params)
                    THEN 'RULE_SCOPE_OR_SYMBOL_FACT_INCOMPLETE' END,
               CASE WHEN d1_scope_status<>'AVAILABLE' OR d1_structure_count IS DISTINCT FROM 5
                    THEN 'D1_SCOPE_OR_STRUCTURE_FACT_INCOMPLETE' END,
               CASE WHEN d1_unknown_price THEN 'D1_TARGET_PRICE_UNKNOWN' END
           ]::text[], NULL) AS unknown_reason_codes
    FROM base b
), group_counts AS (
    SELECT watch_group, count(*)::integer AS row_count
    FROM classified GROUP BY watch_group
), filtered AS MATERIALIZED (
    SELECT c.* FROM classified c CROSS JOIN params p
    WHERE c.watch_group=p.watch_group
      AND (p.board_kind IS NULL OR EXISTS (
          SELECT 1 FROM tdx_sector_members m
          JOIN tdx_sector_catalog catalog ON catalog.id=m.catalog_id
          WHERE catalog.snapshot_id=p.board_snapshot_id
            AND m.board_kind=p.board_kind
            AND m.board_native_id=p.board_native_id
            AND m.source_symbol=c.source_symbol
      ))
), page AS MATERIALIZED (
    SELECT f.* FROM filtered f CROSS JOIN params p
    ORDER BY f.source_symbol COLLATE "C"
    OFFSET (SELECT page_offset FROM params)
    LIMIT (SELECT page_limit FROM params)
)
SELECT COALESCE((SELECT jsonb_object_agg(watch_group,row_count) FROM group_counts), '{}'::jsonb),
       (SELECT count(*)::integer FROM target_pool),
       (SELECT count(DISTINCT source_symbol)::integer FROM target_pool),
       (SELECT count(*)::integer FROM filtered),
       COALESCE((SELECT jsonb_agg(jsonb_build_object(
           'source_symbol',source_symbol,'market',market,'transition',transition,
           'before_state',before_state,'after_state',after_state,
           'watch_group',watch_group,'unknown_reason_codes',unknown_reason_codes,
           'd0_scope_status',d0_scope_status,'seed_scope_status',seed_scope_status,
           'rule_scope_status',rule_scope_status,'d1_scope_status',d1_scope_status,
           'rules_match_seed',rules_match_seed
       ) ORDER BY source_symbol COLLATE "C") FROM page), '[]'::jsonb),
       (SELECT d0 FROM scope_status), (SELECT seed FROM scope_status),
       (SELECT rules FROM scope_status), (SELECT d1 FROM scope_status),
       (SELECT rules_match_seed FROM scope_status),
       (SELECT jsonb_build_object(
           'profile_revision',profile_revision,'producer_contract',producer_contract,
           'producer_version',producer_version,'history_session_count',history_session_count,
           'calendar_id',calendar_id,'calendar_revision',calendar_revision,
           'research_mode',research_mode,'price_coordinate',price_coordinate
       ) FROM seed_scope)""",
            (
                before_session,
                target_session,
                source_package_id,
                before_capture_id,
                target_capture_id,
                expected_source_count,
                d1_history_session_count,
                list(_RULE_IDS),
                watch_group,
                board_kind,
                board_native_id,
                board_snapshot_id,
                offset,
                limit,
            ),
        )
        row = cursor.fetchone()

    (
        counts_raw,
        pool_rows,
        distinct_pool,
        filtered_count,
        rows_raw,
        d0_status,
        seed_status,
        rules_status,
        d1_status,
        rules_match_seed,
        seed_scope,
    ) = row
    counts = {group: int((counts_raw or {}).get(group, 0)) for group in sorted(groups)}
    results = rows_raw or []
    if not isinstance(results, list) or any(
        not isinstance(item, dict) for item in results
    ):
        raise ValueError("来源观察PG分页结果格式无效")
    pool_rows = int(pool_rows or 0)
    distinct_pool = int(distinct_pool or 0)
    return {
        "status": "AVAILABLE"
        if pool_rows == expected_source_count == distinct_pool
        else "INCOMPLETE",
        "target_source_count": pool_rows,
        "target_distinct_source_count": distinct_pool,
        "expected_source_count": expected_source_count,
        "group_counts": counts,
        "partition_closed": sum(counts.values()) == pool_rows,
        "filtered_count": int(filtered_count or 0),
        "results": results,
        "scope_status": {
            "d0": d0_status,
            "seed": seed_status,
            "rules": rules_status,
            "d1": d1_status,
            "rules_match_seed": bool(rules_match_seed),
        },
        "seed_scope": seed_scope,
        "execution": {
            "transaction": "REPEATABLE READ READ ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "d0_pair_fact_read": True,
            "source_pool_python_rows_materialized": len(results),
            "source_rule_rows_returned": 0,
            "source_rule_rows_expected_for_full_scope": expected_source_count
            * len(_RULE_IDS),
            "pg_page_query_seconds": time.perf_counter() - started,
        },
    }


def read_source_launch_watch_board_rps_comparisons(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    board_symbol_contexts: Sequence[tuple[int, str, str, str]],
    seed_scope: Mapping[str, Any],
) -> dict[tuple[int, str, str, str], dict[str, Any]]:
    """一次读取同一目标来源池，精确比较多个板块内RPS5/RPS20并保留并列。"""
    from stock_a.storage.postgres import source_raw

    scope_fields = (
        "profile_revision",
        "producer_contract",
        "producer_version",
        "history_session_count",
        "calendar_id",
        "calendar_revision",
        "research_mode",
        "price_coordinate",
    )
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or isinstance(board_symbol_contexts, (str, bytes))
        or not isinstance(board_symbol_contexts, Sequence)
        or not isinstance(seed_scope, Mapping)
        or any(not seed_scope.get(name) for name in scope_fields)
        or type(seed_scope.get("history_session_count")) is not int
        or not 1 <= seed_scope["history_session_count"] <= 75
    ):
        raise ValueError("多板块板内RPS对比输入无效")

    normalized: list[tuple[int, str, str, str]] = []
    for item in board_symbol_contexts:
        if not isinstance(item, (tuple, list)) or len(item) != 4:
            raise ValueError("板块与来源代码上下文键必须是四元组")
        snapshot_id, board_kind, board_native_id, symbol = item
        if (
            type(snapshot_id) is not int
            or snapshot_id <= 0
            or board_kind not in {"INDUSTRY", "CONCEPT"}
            or not isinstance(board_native_id, str)
            or not board_native_id
            or len(board_native_id) > 64
            or not isinstance(symbol, str)
            or _SYMBOL.fullmatch(symbol) is None
        ):
            raise ValueError("多板块板内RPS对比含无效板块或来源代码键")
        normalized.append((snapshot_id, board_kind, board_native_id, symbol))
    contexts = tuple(sorted(set(normalized)))
    if len(contexts) != len(normalized) or len(contexts) > 50000:
        raise ValueError("多板块板内RPS上下文重复或超过有界比较预算")
    if not contexts:
        return {}
    if len({context[:3] for context in contexts}) > 2000:
        raise ValueError("多板块板内RPS板块数量超过有界比较预算")

    # 对正整数分母<=N，两个不同有理数至少相差1/N²；SQL只在
    # NUMERIC除法精度满足10^scale>2N²时以窗口排序，否则拒绝近似名次。
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """WITH params AS (
    SELECT %s::date AS target_session,%s::bigint AS source_package_id,
           %s::bigint AS source_capture_id,%s::text AS profile_revision,
           %s::text AS producer_contract,%s::text AS producer_version,
           %s::smallint AS history_session_count,%s::text AS calendar_id,
           %s::text AS calendar_revision,%s::text AS research_mode,
           %s::text AS price_coordinate
), requested_contexts AS MATERIALIZED (
    SELECT * FROM unnest(%s::bigint[],%s::text[],%s::text[],%s::text[])
      AS requested(snapshot_id,board_kind,board_native_id,source_symbol)
), requested_boards AS MATERIALIZED (
    SELECT DISTINCT snapshot_id,board_kind,board_native_id FROM requested_contexts
), source_scope AS MATERIALIZED (
    SELECT r.source_symbol
    FROM source_raw_daily r CROSS JOIN params p
    WHERE r.session_date=p.target_session
      AND r.source_package_id=p.source_package_id
      AND r.source_capture_id=p.source_capture_id
), source_count AS (
    SELECT count(*)::numeric AS expected_source_count FROM source_scope
), seed AS MATERIALIZED (
    SELECT s.source_symbol,s.rps5_numerator,s.rps5_denominator,
           s.rps5_quality,s.rps5_reason,s.rps20_numerator,s.rps20_denominator,
           s.rps20_quality,s.rps20_reason
    FROM source_pool_seed_day_facts s CROSS JOIN params p
    WHERE s.target_session=p.target_session
      AND s.source_package_id=p.source_package_id
      AND s.source_capture_id=p.source_capture_id
      AND s.profile_revision=p.profile_revision
      AND s.producer_contract=p.producer_contract
      AND s.producer_version=p.producer_version
      AND s.history_session_count=p.history_session_count
      AND s.calendar_id=p.calendar_id
      AND s.calendar_revision=p.calendar_revision
      AND s.research_mode=p.research_mode
      AND s.price_coordinate=p.price_coordinate
), selected_metrics AS MATERIALIZED (
    SELECT r.snapshot_id,r.board_kind,r.board_native_id,r.source_symbol,
           v.metric,v.numerator,v.denominator,v.quality,v.reason
    FROM requested_contexts r LEFT JOIN seed s USING (source_symbol)
    CROSS JOIN LATERAL (VALUES
      ('RPS5'::text,s.rps5_numerator,s.rps5_denominator,s.rps5_quality,s.rps5_reason),
      ('RPS20'::text,s.rps20_numerator,s.rps20_denominator,s.rps20_quality,s.rps20_reason)
    ) AS v(metric,numerator,denominator,quality,reason)
), snapshot_members AS MATERIALIZED (
    SELECT DISTINCT c.snapshot_id,m.board_kind,m.board_native_id,m.source_symbol
    FROM requested_boards requested
    JOIN tdx_sector_catalog c ON c.snapshot_id=requested.snapshot_id
      AND c.board_kind=requested.board_kind
      AND c.board_native_id=requested.board_native_id
    JOIN tdx_sector_members m ON m.catalog_id=c.id
      AND m.board_kind=requested.board_kind
      AND m.board_native_id=requested.board_native_id
), board_members AS MATERIALIZED (
    SELECT member.snapshot_id,member.board_kind,member.board_native_id,
           member.source_symbol
    FROM snapshot_members member
    JOIN source_scope raw USING (source_symbol)
), snapshot_counts AS (
    SELECT snapshot_id,board_kind,board_native_id,count(*)::integer AS snapshot_member_count
    FROM snapshot_members GROUP BY snapshot_id,board_kind,board_native_id
), peer_metrics AS (
    SELECT member.snapshot_id,member.board_kind,member.board_native_id,
           member.source_symbol,v.metric,v.numerator,v.denominator,v.quality
    FROM board_members member LEFT JOIN seed s USING (source_symbol)
    CROSS JOIN LATERAL (VALUES
      ('RPS5'::text,s.rps5_numerator,s.rps5_denominator,s.rps5_quality),
      ('RPS20'::text,s.rps20_numerator,s.rps20_denominator,s.rps20_quality)
    ) AS v(metric,numerator,denominator,quality)
), valid_peer_metrics AS MATERIALIZED (
    SELECT peer.snapshot_id,peer.board_kind,peer.board_native_id,
           peer.source_symbol,peer.metric,peer.numerator,peer.denominator,
           peer.numerator/peer.denominator AS order_value
    FROM peer_metrics peer
    WHERE peer.quality='OBSERVED' AND peer.numerator IS NOT NULL
      AND peer.denominator>0
), ratio_order_contract AS (
    SELECT COALESCE(bool_and(
        peer.numerator=trunc(peer.numerator)
        AND peer.denominator=trunc(peer.denominator)
        AND peer.denominator<=source_count.expected_source_count
        AND power(10::numeric,scale(peer.order_value))
            > 2*source_count.expected_source_count*source_count.expected_source_count
    ),true) AS proven_exact_order
    FROM valid_peer_metrics peer CROSS JOIN source_count
), ranked_peer_metrics AS (
    SELECT peer.snapshot_id,peer.board_kind,peer.board_native_id,
           peer.source_symbol,peer.metric,
           rank() OVER (
             PARTITION BY peer.snapshot_id,peer.board_kind,peer.board_native_id,peer.metric
             ORDER BY peer.order_value DESC
           )::integer AS rank_first,
           count(*) OVER (
             PARTITION BY peer.snapshot_id,peer.board_kind,peer.board_native_id,
                          peer.metric,peer.order_value
           )::integer AS tied_member_count,
           count(*) OVER (
             PARTITION BY peer.snapshot_id,peer.board_kind,peer.board_native_id,
                          peer.metric
           )::integer AS valid_member_count
    FROM valid_peer_metrics peer
), metric_counts AS (
    SELECT peer.snapshot_id,peer.board_kind,peer.board_native_id,peer.metric,
           count(DISTINCT peer.source_symbol)::integer AS source_pool_member_count,
           count(DISTINCT peer.source_symbol) FILTER (
             WHERE peer.quality='OBSERVED' AND peer.numerator IS NOT NULL
               AND peer.denominator>0
           )::integer AS valid_member_count
    FROM peer_metrics peer
    GROUP BY peer.snapshot_id,peer.board_kind,peer.board_native_id,peer.metric
)
SELECT selected.snapshot_id,selected.board_kind,selected.board_native_id,
       selected.source_symbol,selected.metric,selected.numerator::text,
       selected.denominator::text,selected.quality,selected.reason,
       ranked.rank_first-1 AS higher_member_count,
       ranked.tied_member_count,
       COALESCE(counts.valid_member_count,0) AS valid_member_count,
       COALESCE(counts.source_pool_member_count,0) AS source_pool_member_count,
       COALESCE(snapshot.snapshot_member_count,0) AS snapshot_member_count,
       ratio_order_contract.proven_exact_order
FROM selected_metrics selected
LEFT JOIN ranked_peer_metrics ranked
  ON ranked.snapshot_id=selected.snapshot_id
 AND ranked.board_kind=selected.board_kind
 AND ranked.board_native_id=selected.board_native_id
 AND ranked.source_symbol=selected.source_symbol
 AND ranked.metric=selected.metric
LEFT JOIN metric_counts counts
  ON counts.snapshot_id=selected.snapshot_id
 AND counts.board_kind=selected.board_kind
 AND counts.board_native_id=selected.board_native_id
 AND counts.metric=selected.metric
LEFT JOIN snapshot_counts snapshot
  ON snapshot.snapshot_id=selected.snapshot_id
 AND snapshot.board_kind=selected.board_kind
 AND snapshot.board_native_id=selected.board_native_id
CROSS JOIN ratio_order_contract
ORDER BY selected.snapshot_id,selected.board_kind,
         selected.board_native_id COLLATE "C",selected.source_symbol COLLATE "C",
         selected.metric""",
            (
                target_session,
                source_package_id,
                source_capture_id,
                seed_scope["profile_revision"],
                seed_scope["producer_contract"],
                seed_scope["producer_version"],
                seed_scope["history_session_count"],
                seed_scope["calendar_id"],
                seed_scope["calendar_revision"],
                seed_scope["research_mode"],
                seed_scope["price_coordinate"],
                [item[0] for item in contexts],
                [item[1] for item in contexts],
                [item[2] for item in contexts],
                [item[3] for item in contexts],
            ),
        )
        rows = cursor.fetchall()

    result: dict[tuple[int, str, str, str], dict[str, Any]] = {
        context: {} for context in contexts
    }
    for (
        snapshot_id,
        board_kind,
        board_native_id,
        symbol,
        metric,
        numerator,
        denominator,
        quality,
        reason,
        higher,
        tied,
        valid,
        source_pool_member_count,
        snapshot_member_count,
        proven_exact_order,
    ) in rows:
        if proven_exact_order is not True:
            raise ValueError("RPS分母/数值精度不符合板内精确排序合同；拒绝返回近似名次")
        key = (int(snapshot_id), board_kind, board_native_id, symbol)
        has_value = (
            quality == "OBSERVED"
            and numerator is not None
            and denominator is not None
            and int(denominator) > 0
        )
        valid_count = int(valid or 0)
        result[key][metric] = {
            "status": "AVAILABLE" if has_value and valid_count > 0 else "UNKNOWN",
            "numerator": numerator if has_value else None,
            "denominator": denominator if has_value else None,
            "quality": quality or "UNKNOWN",
            "reason": reason,
            "higher_member_count": int(higher or 0) if has_value else None,
            "tied_member_count": int(tied or 0) if has_value else None,
            "valid_member_count": valid_count,
            "rank_with_ties": int(higher or 0) + 1 if has_value else None,
            "rank_first": int(higher or 0) + 1 if has_value else None,
            "rank_last": int(higher or 0) + int(tied or 0) if has_value else None,
            "source_pool_member_count": int(source_pool_member_count or 0),
            "unknown_member_count": max(
                0, int(source_pool_member_count or 0) - valid_count
            ),
            "snapshot_member_count": int(snapshot_member_count or 0),
            "out_of_source_pool_member_count": max(
                0,
                int(snapshot_member_count or 0) - int(source_pool_member_count or 0),
            ),
            "rank_scope": "CURRENT_SELECTED_BOARD_NON_PIT",
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        }
    return result


def read_source_launch_watch_board_rps_comparison(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    source_symbols: Sequence[str],
    seed_scope: Mapping[str, Any],
    board_kind: str,
    board_native_id: str,
    board_snapshot_id: int,
) -> dict[str, dict[str, Any]]:
    """在用户选定的单板块内复用多板块比较内核并保留并列。"""
    symbols = tuple(sorted(set(source_symbols)))
    if (
        isinstance(source_symbols, (str, bytes))
        or len(symbols) != len(source_symbols)
        or len(symbols) > 30
        or board_kind not in {"INDUSTRY", "CONCEPT"}
        or not isinstance(board_native_id, str)
        or not board_native_id
        or len(board_native_id) > 64
        or type(board_snapshot_id) is not int
        or board_snapshot_id <= 0
        or any(
            not isinstance(item, str) or _SYMBOL.fullmatch(item) is None
            for item in symbols
        )
    ):
        raise ValueError("板内RPS对比输入无效")
    if not symbols:
        return {}
    contexts = [
        (board_snapshot_id, board_kind, board_native_id, symbol) for symbol in symbols
    ]
    compared = read_source_launch_watch_board_rps_comparisons(
        config,
        target_session=target_session,
        source_package_id=source_package_id,
        source_capture_id=source_capture_id,
        board_symbol_contexts=contexts,
        seed_scope=seed_scope,
    )
    return {
        symbol: compared[(board_snapshot_id, board_kind, board_native_id, symbol)]
        for symbol in symbols
    }


__all__ = [
    "read_source_launch_watch_board_rps_comparisons",
    "read_source_launch_watch_board_rps_comparison",
    "read_source_launch_watch_evidence",
    "read_source_launch_watch_page",
]
