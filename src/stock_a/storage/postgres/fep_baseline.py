"""FEP描述性Baseline的PG原始事实读取、typed写入与只读读取。"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.storage.postgres.research_facts import PATH_VERSION, ResearchFactConflict
from stock_a.storage.postgres.source_raw import _transaction

_TABLE = "source_fep_baseline_facts"
_COLUMNS = (
    "research_cutoff",
    "source_package_id",
    "source_capture_id",
    "target_session",
    "period_start_session",
    "period_end_session",
    "rule_id",
    "rule_version",
    "profile_family",
    "profile_revision",
    "history_session_count",
    "d1_history_session_count",
    "event_window_start",
    "calendar_revision",
    "path_rule_version",
    "source_time_quality",
    "parser_contract",
    "lane",
    "horizon",
    "summary_scope",
    "group_code",
    "metric_code",
    "condition_code",
    "market_condition_code",
    "status",
    "capability_code",
    "entity_code",
    "target_code",
    "scope",
    "price_coordinate",
    "source_pool_count",
    "t0_match_count",
    "t0_no_match_count",
    "t0_unknown_count",
    "entry_count",
    "daily_landmark_count",
    "reentry_count",
    "left_censored_count",
    "member_count",
    "due_count",
    "not_due_count",
    "observed_count",
    "unknown_count",
    "not_computed_count",
    "positive_count",
    "non_positive_count",
    "p25_numerator",
    "p25_denominator",
    "p50_numerator",
    "p50_denominator",
    "p75_numerator",
    "p75_denominator",
    "median_difference_numerator",
    "median_difference_denominator",
    "positive_fraction_difference_numerator",
    "positive_fraction_difference_denominator",
    "requested_date_count",
    "condition_date_count",
    "valid_date_count",
    "not_due_date_count",
    "unknown_date_count",
    "not_computed_date_count",
    "equal_date_mean_median_difference_numerator",
    "equal_date_mean_median_difference_denominator",
    "equal_date_mean_positive_fraction_difference_numerator",
    "equal_date_mean_positive_fraction_difference_denominator",
    "positive_direction_date_count",
    "negative_direction_date_count",
    "zero_direction_date_count",
    "market_metric_numerator",
    "market_metric_denominator",
    "market_source_count",
    "market_eligible_count",
    "market_time_quality",
    "reason_codes",
    "reason_counts",
)


def read_fep_source_contexts(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_package_id: int,
    rule_ids: tuple[str, ...],
    horizons: tuple[int, ...],
) -> list[dict[str, Any]]:
    """从截止内Raw、三态、登记、Forward及市场宽度事实读取完整输入。"""
    if (
        type(research_cutoff) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or not rule_ids
        or len(set(rule_ids)) != len(rule_ids)
        or any(not isinstance(rule, str) or not rule.strip() for rule in rule_ids)
        or not horizons
        or tuple(sorted(set(horizons))) != horizons
        or any(horizon not in {1, 3, 5} for horizon in horizons)
    ):
        raise ValueError("FEP来源读取的截止、来源包、规则或期限无效")

    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT r.id,r.target_session,r.source_symbol,r.source_package_id,
                      r.source_capture_id,r.rule_id,r.rule_version,r.profile_revision,
                      r.history_session_count,r.d1_history_session_count,
                      r.event_window_start,r.calendar_revision,r.state,r.reasons,r.scope,
                      c.source_time_quality,c.parser_contract,c.source_clock_role,
                      c.source_available_at,c.target_session
               FROM source_rule_evaluations r
               JOIN source_captures c ON c.capture_id=r.source_capture_id
                                      AND c.source_package_id=r.source_package_id
                                      AND c.target_session=r.target_session
               WHERE r.target_session<=%s AND r.source_package_id=%s
                 AND r.rule_id=ANY(%s) AND r.scope='NON_PIT_EXPLORATORY'
                 AND c.target_session<=%s
               ORDER BY r.target_session,r.rule_id,r.source_capture_id,
                        r.profile_revision,r.source_symbol""",
            (research_cutoff, source_package_id, list(rule_ids), research_cutoff),
        )
        rows = cursor.fetchall()

        grouped: dict[tuple[Any, ...], list[tuple[Any, ...]]] = {}
        for row in rows:
            key = (
                row[1],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                row[11],
                row[15],
                row[16],
                row[17],
            )
            grouped.setdefault(key, []).append(row)

        contexts = []
        for key, scope_rows in sorted(grouped.items(), key=lambda item: item[0]):
            (
                target,
                package,
                capture,
                rule,
                rule_version,
                profile_revision,
                history_count,
                d1_history_count,
                event_window_start,
                calendar_revision,
                source_time_quality,
                parser_contract,
                source_clock_role,
            ) = key
            rule_facts = {}
            fact_ids = []
            for row in scope_rows:
                fact_id, symbol, state, reasons, scope = (
                    row[0],
                    row[2],
                    row[12],
                    row[13],
                    row[14],
                )
                if scope != "NON_PIT_EXPLORATORY" or symbol in rule_facts:
                    raise ResearchFactConflict("FEP T0规则事实有重复代码或范围错误")
                if state not in {"TRUE", "FALSE", "UNKNOWN"}:
                    raise ResearchFactConflict("FEP T0规则三态无效")
                fact_ids.append(fact_id)
                rule_facts[symbol] = {
                    "id": fact_id,
                    "state": state,
                    "reasons": list(reasons or ()),
                }

            cursor.execute(
                """SELECT source_symbol FROM source_raw_daily
                   WHERE session_date=%s AND source_package_id=%s
                     AND source_capture_id=%s ORDER BY source_symbol""",
                (target, package, capture),
            )
            raw_symbols = [row[0] for row in cursor.fetchall()]
            raw_symbol_set = set(raw_symbols)
            if len(raw_symbols) != len(raw_symbol_set):
                raise ResearchFactConflict("FEP T0 Raw来源代码重复")

            true_fact_ids = [
                fact["id"] for fact in rule_facts.values() if fact["state"] == "TRUE"
            ]
            if true_fact_ids:
                cursor.execute(
                    """SELECT rule_fact_id,enrollment_kind
                       FROM source_research_enrollments
                       WHERE rule_fact_id=ANY(%s) ORDER BY rule_fact_id""",
                    (true_fact_ids,),
                )
                enrollment_rows = cursor.fetchall()
            else:
                enrollment_rows = []
            enrollments_by_fact: dict[int, list[str]] = {}
            for fact_id, kind in enrollment_rows:
                enrollments_by_fact.setdefault(fact_id, []).append(kind)
            enrollment_kinds = {}
            enrollment_complete = set(enrollments_by_fact) == set(
                true_fact_ids
            ) and all(len(kinds) == 1 for kinds in enrollments_by_fact.values())
            for symbol, fact in rule_facts.items():
                if fact["state"] == "TRUE":
                    kinds = enrollments_by_fact.get(fact["id"], ())
                    if len(kinds) == 1:
                        enrollment_kinds[symbol] = kinds[0]

            cursor.execute(
                """SELECT source_symbol,horizon,endpoint_session,maturity,
                          endpoint_quality,endpoint_reason,endpoint_numerator,
                          endpoint_denominator,price_coordinate,scope,
                          path_rule_version,calendar_revision
                   FROM source_forward_path_facts
                   WHERE target_session=%s AND source_package_id=%s
                     AND source_capture_id=%s AND horizon=ANY(%s)
                     AND path_rule_version=%s AND calendar_revision=%s
                     AND price_coordinate='RAW_UNADJUSTED'
                     AND scope='NON_PIT_EXPLORATORY'
                     AND target_session<=%s AND endpoint_session<=%s
                   ORDER BY horizon,source_symbol""",
                (
                    target,
                    package,
                    capture,
                    list(horizons),
                    PATH_VERSION,
                    calendar_revision,
                    research_cutoff,
                    research_cutoff,
                ),
            )
            forward_by_horizon: dict[int, dict[str, dict[str, Any]]] = {
                horizon: {} for horizon in horizons
            }
            forward_rows = cursor.fetchall()
            for row in forward_rows:
                (
                    symbol,
                    horizon,
                    endpoint_session,
                    maturity,
                    endpoint_quality,
                    endpoint_reason,
                    endpoint_numerator,
                    endpoint_denominator,
                    coordinate,
                    scope,
                    path_version,
                    forward_calendar_revision,
                ) = row
                if symbol in forward_by_horizon[horizon]:
                    raise ResearchFactConflict("FEP同T0期限存在重复Forward来源代码")
                forward_by_horizon[horizon][symbol] = {
                    "endpoint_session": endpoint_session,
                    "maturity": maturity,
                    "endpoint_quality": endpoint_quality,
                    "endpoint_reason": endpoint_reason,
                    "endpoint_numerator": endpoint_numerator,
                    "endpoint_denominator": endpoint_denominator,
                    "valid_forward_identity": (
                        coordinate == "RAW_UNADJUSTED"
                        and scope == "NON_PIT_EXPLORATORY"
                        and path_version == PATH_VERSION
                        and forward_calendar_revision == calendar_revision
                        and endpoint_session <= research_cutoff
                    ),
                }

            cursor.execute(
                """SELECT id,contract_version,calendar_revision,source_count,
                          eligible_count,source_time_quality,source_clock_role
                   FROM source_market_breadth_days
                   WHERE target_session=%s AND source_package_id=%s
                     AND source_capture_id=%s AND target_session<=%s
                   ORDER BY contract_version,calendar_revision""",
                (target, package, capture, research_cutoff),
            )
            market_days = cursor.fetchall()
            market = None
            if len(market_days) == 1:
                (
                    breadth_day_id,
                    contract_version,
                    breadth_calendar_revision,
                    market_source_count,
                    market_eligible_count,
                    market_time_quality,
                    market_clock_role,
                ) = market_days[0]
                cursor.execute(
                    """SELECT source_symbol,delta_indicator,classification,
                              primary_unknown_reason
                       FROM source_market_breadth_symbols
                       WHERE breadth_day_id=%s AND target_session=%s
                         AND source_package_id=%s AND source_capture_id=%s
                         AND target_session<=%s ORDER BY source_symbol""",
                    (
                        breadth_day_id,
                        target,
                        package,
                        capture,
                        research_cutoff,
                    ),
                )
                market_rows = cursor.fetchall()
                deltas = [row[1] for row in market_rows if row[1] is not None]
                reasons = Counter(
                    row[3] for row in market_rows if row[1] is None and row[3]
                )
                market = {
                    "day_id": breadth_day_id,
                    "contract_version": contract_version,
                    "calendar_revision": breadth_calendar_revision,
                    "source_count": market_source_count,
                    "eligible_count": market_eligible_count,
                    "time_quality": market_time_quality,
                    "clock_role": market_clock_role,
                    "row_count": len(market_rows),
                    "symbols": {row[0] for row in market_rows},
                    "eligible_row_count": len(deltas),
                    "metric": Fraction(sum(deltas), len(deltas)) if deltas else None,
                    "reason_counts": dict(reasons),
                    "complete_for_condition": (
                        breadth_calendar_revision == calendar_revision
                        and market_source_count == len(rule_facts)
                        and len(market_rows) == market_source_count
                        and {row[0] for row in market_rows} == set(rule_facts)
                        and market_eligible_count == market_source_count
                        and len(deltas) == market_eligible_count
                        and market_time_quality != "UNKNOWN"
                    ),
                }

            contexts.append(
                {
                    "target_session": target,
                    "source_package_id": package,
                    "source_capture_id": capture,
                    "rule_id": rule,
                    "rule_version": rule_version,
                    "profile_revision": profile_revision,
                    "profile_family": profile_revision.split("|", 1)[0],
                    "history_session_count": history_count,
                    "d1_history_session_count": d1_history_count,
                    "event_window_start": event_window_start,
                    "calendar_revision": calendar_revision,
                    "source_time_quality": source_time_quality,
                    "parser_contract": parser_contract,
                    "source_clock_role": source_clock_role,
                    "source_available_at": scope_rows[0][18],
                    "rule_facts": rule_facts,
                    "raw_symbols": raw_symbol_set,
                    "raw_count": len(raw_symbols),
                    "enrollments": enrollment_kinds,
                    "enrollment_complete": enrollment_complete,
                    "forward_by_horizon": forward_by_horizon,
                    "market": market,
                }
            )
    return contexts


def _ratio(numerator: Any, denominator: Any) -> str | None:
    if numerator is None or denominator is None:
        return None
    if int(denominator) <= 0:
        raise ResearchFactConflict("FEP持久化分母必须为正")
    return str(Fraction(int(numerator), int(denominator)))


def _row_values(row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in _COLUMNS)


def persist_fep_baseline_rows(
    config: Mapping[str, Any], rows: list[Mapping[str, Any]]
) -> dict[str, Any]:
    """以一个串行化事务幂等写入；同业务键异内容冲突并回滚。"""
    if not rows:
        raise ValueError("FEP没有可持久化的typed结果")
    values = [_row_values(row) for row in rows]
    if any(len(value) != len(_COLUMNS) for value in values):
        raise ValueError("FEP typed行列结构不完整")
    insert_sql = (
        f"INSERT INTO {_TABLE} ({','.join(_COLUMNS)}) "
        f"VALUES ({','.join(['%s'] * len(_COLUMNS))}) ON CONFLICT DO NOTHING"
    )
    rules = sorted({row[6] for row in values})
    packages = sorted({row[1] for row in values})
    cutoffs = sorted({row[0] for row in values})
    horizons = sorted({row[18] for row in values})
    lanes = sorted({row[17] for row in values})
    if len(packages) != 1 or len(cutoffs) != 1:
        raise ValueError("单次FEP写入必须绑定唯一截止与来源包")

    def business_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
        return row[:23]

    expected_by_key = {}
    for value in values:
        key = business_key(value)
        if key in expected_by_key and expected_by_key[key] != value:
            raise ResearchFactConflict("FEP输入批次同业务键包含异内容")
        expected_by_key[key] = value

    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.executemany(insert_sql, values)
        inserted = cursor.rowcount
        cursor.execute(
            f"""SELECT {",".join(_COLUMNS)} FROM {_TABLE}
                WHERE research_cutoff=%s AND source_package_id=%s
                  AND rule_id=ANY(%s) AND horizon=ANY(%s) AND lane=ANY(%s)
                ORDER BY summary_scope,target_session,period_start_session,
                         rule_id,lane,horizon,condition_code,group_code,metric_code""",
            (cutoffs[0], packages[0], rules, horizons, lanes),
        )
        stored = cursor.fetchall()
        stored_by_key = {business_key(tuple(row)): tuple(row) for row in stored}
        for key, expected in expected_by_key.items():
            actual = stored_by_key.get(key)
            if actual != expected:
                raise ResearchFactConflict(
                    "同一FEP业务键的已存typed内容不同；事务已回滚"
                )
    return {
        "inserted_row_count": inserted,
        "verified_row_count": len(expected_by_key),
        "status": "PERSISTED_AND_IDEMPOTENT",
        "database_write_attempted": True,
        "transaction": "SERIALIZABLE_SINGLE_BATCH",
    }


def read_fep_baseline(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_package_id: int,
    rule_id: str,
    horizon: int,
    lane: str,
) -> dict[str, Any]:
    """只读读取已经完成的T0组统计及等权日期汇总，不触碰来源表。"""
    if (
        type(research_cutoff) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or not isinstance(rule_id, str)
        or not rule_id.strip()
        or horizon not in {1, 3, 5}
        or lane not in {"ENTRY", "DAILY_LANDMARK"}
    ):
        raise ValueError("FEP只读查询参数无效")
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            f"""SELECT {",".join(_COLUMNS)} FROM {_TABLE}
                WHERE research_cutoff=%s AND source_package_id=%s
                  AND rule_id=%s AND horizon=%s AND lane=%s
                ORDER BY summary_scope,target_session,condition_code,group_code,metric_code""",
            (research_cutoff, source_package_id, rule_id, horizon, lane),
        )
        rows = cursor.fetchall()
    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "reason_codes": ["NO_PERSISTED_FEP_BASELINE"],
            "research_cutoff": research_cutoff.isoformat(),
            "source_package_id": source_package_id,
            "rule_id": rule_id,
            "horizon": horizon,
            "lane": lane,
            "capability": "DESCRIPTIVE_ONLY",
            "model_capability": "NOT_ENABLED",
        }
    records = []
    for row in rows:
        record = dict(zip(_COLUMNS, row))
        for code in (
            "p25",
            "p50",
            "p75",
            "median_difference",
            "positive_fraction_difference",
            "equal_date_mean_median_difference",
            "equal_date_mean_positive_fraction_difference",
            "market_metric",
        ):
            numerator = record.pop(f"{code}_numerator")
            denominator = record.pop(f"{code}_denominator")
            record[code] = _ratio(numerator, denominator)
        record["target_session"] = (
            record["target_session"].isoformat() if record["target_session"] else None
        )
        record["period_start_session"] = (
            record["period_start_session"].isoformat()
            if record["period_start_session"]
            else None
        )
        record["period_end_session"] = (
            record["period_end_session"].isoformat()
            if record["period_end_session"]
            else None
        )
        record["event_window_start"] = (
            record["event_window_start"].isoformat()
            if record["event_window_start"]
            else None
        )
        record["research_cutoff"] = record["research_cutoff"].isoformat()
        records.append(record)
    partitions = {
        (row[6], row[7], row[8], row[13], row[14], row[15], row[16]) for row in rows
    }
    return {
        "status": "AVAILABLE" if len(partitions) == 1 else "INCOMPLETE",
        "research_cutoff": research_cutoff.isoformat(),
        "source_package_id": source_package_id,
        "rule_id": rule_id,
        "rule_version": rows[0][7],
        "horizon": horizon,
        "lane": lane,
        "profile_family": rows[0][8],
        "calendar_revision": rows[0][13],
        "path_rule_version": rows[0][14],
        "entity": "SOURCE_STOCK",
        "target": "RAW_CLOSE_RETURN",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "RAW_UNADJUSTED",
        "capability": "DESCRIPTIVE_ONLY",
        "model_capability": "NOT_ENABLED",
        "rows": records,
    }
