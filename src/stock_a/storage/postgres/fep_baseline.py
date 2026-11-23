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
_RESTATABLE_COLUMNS = (
    "status",
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
    "reason_codes",
    "reason_counts",
)


def _forward_calendar_matches_rule_scope(
    *,
    rule_revision: str,
    forward_revision: str,
    target_session: date,
    endpoint_session: date,
) -> bool:
    """按T0至端点覆盖年度核对规则链与Forward路径修订。"""
    from stock_a.research.entry_context.sessions import _load_calendar
    from stock_a.research.source_forward_path import (
        forward_calendar_revision_compatible,
    )

    try:
        official_revision_chain = _load_calendar(endpoint_session)[2]
    except ValueError:
        return False
    return forward_calendar_revision_compatible(
        target_session,
        endpoint_session,
        rule_revision=rule_revision,
        path_revision=forward_revision,
        official_revision_chain=official_revision_chain,
    )


def read_fep_source_contexts(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_capture_id: int,
    rule_ids: tuple[str, ...],
    horizons: tuple[int, ...],
) -> list[dict[str, Any]]:
    """从截止内Raw、三态、登记、Forward及市场宽度事实读取完整输入。"""
    if (
        type(research_cutoff) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not rule_ids
        or len(set(rule_ids)) != len(rule_ids)
        or any(not isinstance(rule, str) or not rule.strip() for rule in rule_ids)
        or not horizons
        or tuple(sorted(set(horizons))) != horizons
        or any(horizon not in {1, 3, 5} for horizon in horizons)
    ):
        raise ValueError("FEP来源读取的截止、来源捕获、规则或期限无效")

    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT r.id,r.target_session,r.source_symbol,r.source_capture_id,
                      r.rule_id,r.rule_version,r.profile_revision,
                      r.history_session_count,r.d1_history_session_count,
                      r.event_window_start,r.calendar_revision,r.state,r.reasons,r.scope,
                      c.source_time_quality,c.parser_contract,c.source_clock_role,
                      c.source_available_at
               FROM source_rule_evaluations r
               JOIN source_captures c ON c.capture_id=r.source_capture_id
                                      AND c.provider='BAOSTOCK'
                                      AND c.range_start_session<=r.target_session
                                      AND c.range_end_session>=r.target_session
               WHERE r.target_session<=%s AND r.source_capture_id=%s
                 AND r.rule_id=ANY(%s) AND r.scope='NON_PIT_EXPLORATORY'
                 AND c.range_end_session<=%s
               ORDER BY r.target_session,r.rule_id,r.source_capture_id,
                        r.profile_revision,r.source_symbol""",
            (research_cutoff, source_capture_id, list(rule_ids), research_cutoff),
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
                row[14],
                row[15],
                row[16],
            )
            grouped.setdefault(key, []).append(row)

        contexts = []
        for key, scope_rows in sorted(grouped.items(), key=lambda item: item[0]):
            (
                target,
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
                    row[11],
                    row[12],
                    row[13],
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
                   WHERE session_date=%s AND source_capture_id=%s
                   ORDER BY source_symbol""",
                (target, capture),
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
                          endpoint_denominator,path_quality,path_reasons,
                          mfe_numerator,mfe_denominator,mae_numerator,mae_denominator,
                          mdd_numerator,mdd_denominator,price_coordinate,scope,
                          path_rule_version,calendar_revision
                   FROM source_forward_path_facts
                   WHERE target_session=%s AND source_capture_id=%s
                     AND horizon=ANY(%s)
                     AND path_rule_version=%s
                     AND price_coordinate='FORWARD_ADJUSTED'
                     AND scope='NON_PIT_EXPLORATORY'
                     AND target_session<=%s AND endpoint_session<=%s
                   ORDER BY horizon,calendar_revision,source_symbol""",
                (
                    target,
                    capture,
                    list(horizons),
                    PATH_VERSION,
                    research_cutoff,
                    research_cutoff,
                ),
            )
            forward_by_horizon: dict[int, dict[str, dict[str, Any]]] = {
                horizon: {} for horizon in horizons
            }
            forward_path_scopes: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
            forward_rows = cursor.fetchall()
            calendar_compatibility: dict[tuple[Any, ...], bool] = {}
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
                    path_quality,
                    path_reasons,
                    mfe_numerator,
                    mfe_denominator,
                    mae_numerator,
                    mae_denominator,
                    mdd_numerator,
                    mdd_denominator,
                    coordinate,
                    scope,
                    path_version,
                    forward_calendar_revision,
                ) = row
                compatibility_key = (
                    calendar_revision,
                    forward_calendar_revision,
                    target,
                    endpoint_session,
                )
                if compatibility_key not in calendar_compatibility:
                    calendar_compatibility[compatibility_key] = (
                        _forward_calendar_matches_rule_scope(
                            rule_revision=calendar_revision,
                            forward_revision=forward_calendar_revision,
                            target_session=target,
                            endpoint_session=endpoint_session,
                        )
                    )
                compatible_calendar = calendar_compatibility[compatibility_key]
                scoped_rows = forward_path_scopes.setdefault(
                    (forward_calendar_revision, horizon), {}
                )
                if symbol in scoped_rows:
                    raise ResearchFactConflict("FEP同T0期限存在重复Forward来源代码")
                forward = {
                    "endpoint_session": endpoint_session,
                    "maturity": maturity,
                    "endpoint_quality": endpoint_quality,
                    "endpoint_reason": endpoint_reason,
                    "endpoint_numerator": endpoint_numerator,
                    "endpoint_denominator": endpoint_denominator,
                    "path_quality": path_quality,
                    "path_reasons": list(path_reasons or ()),
                    "mfe_numerator": mfe_numerator,
                    "mfe_denominator": mfe_denominator,
                    "mae_numerator": mae_numerator,
                    "mae_denominator": mae_denominator,
                    "mdd_numerator": mdd_numerator,
                    "mdd_denominator": mdd_denominator,
                    "valid_forward_identity": (
                        coordinate == "FORWARD_ADJUSTED"
                        and scope == "NON_PIT_EXPLORATORY"
                        and path_version == PATH_VERSION
                        and compatible_calendar
                        and endpoint_session <= research_cutoff
                    ),
                }
                scoped_rows[symbol] = forward
                if compatible_calendar:
                    if symbol in forward_by_horizon[horizon]:
                        raise ResearchFactConflict(
                            "FEP同T0期限命中多个兼容Forward日历版本"
                        )
                    forward_by_horizon[horizon][symbol] = forward

            cursor.execute(
                """SELECT id,contract_version,calendar_revision,source_count,
                          eligible_count,source_time_quality,source_clock_role
                   FROM source_market_breadth_days
                   WHERE target_session=%s AND source_capture_id=%s
                     AND target_session<=%s
                     AND price_coordinate='FORWARD_ADJUSTED'
                   ORDER BY contract_version,calendar_revision""",
                (target, capture, research_cutoff),
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
                         AND source_capture_id=%s
                         AND target_session<=%s
                         AND price_coordinate='FORWARD_ADJUSTED'
                       ORDER BY source_symbol""",
                    (
                        breadth_day_id,
                        target,
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
                    "source_available_at": scope_rows[0][17],
                    "rule_facts": rule_facts,
                    "raw_symbols": raw_symbol_set,
                    "raw_count": len(raw_symbols),
                    "enrollments": enrollment_kinds,
                    "enrollment_complete": enrollment_complete,
                    "forward_by_horizon": forward_by_horizon,
                    "forward_path_scopes": forward_path_scopes,
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
    config: Mapping[str, Any],
    rows: list[Mapping[str, Any]],
    *,
    allow_statistical_restatement: bool = False,
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
    rules = sorted({row[5] for row in values})
    capture_ids = sorted({row[1] for row in values if row[1] is not None})
    cutoffs = sorted({row[0] for row in values})
    horizons = sorted({row[17] for row in values})
    lanes = sorted({row[16] for row in values})
    if (
        len(capture_ids) != 1
        or any(row[1] != capture_ids[0] for row in values)
        or len(cutoffs) != 1
    ):
        raise ValueError("单次FEP写入必须绑定唯一截止与BaoStock来源捕获")

    def business_key(row: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(row[_COLUMNS.index(name)] for name in _COLUMNS[:23])

    expected_by_key = {}
    for value in values:
        key = business_key(value)
        if key in expected_by_key and expected_by_key[key] != value:
            raise ResearchFactConflict("FEP输入批次同业务键包含异内容")
        expected_by_key[key] = value

    restatable_indexes = {_COLUMNS.index(name) for name in _RESTATABLE_COLUMNS}
    key_filter = " AND ".join(
        f"{column} IS NOT DISTINCT FROM %s" for column in _COLUMNS[:23]
    )
    update_sql = (
        f"UPDATE {_TABLE} SET "
        + ",".join(f"{column}=%s" for column in _RESTATABLE_COLUMNS)
        + f" WHERE {key_filter}"
    )

    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.executemany(insert_sql, values)
        inserted = cursor.rowcount
        cursor.execute(
            f"""SELECT {",".join(_COLUMNS)} FROM {_TABLE}
                WHERE research_cutoff=%s
                  AND source_capture_id=%s
                  AND rule_id=ANY(%s) AND horizon=ANY(%s) AND lane=ANY(%s)
                ORDER BY summary_scope,target_session,period_start_session,
                         rule_id,lane,horizon,condition_code,group_code,metric_code""",
            (cutoffs[0], capture_ids[0], rules, horizons, lanes),
        )
        stored = cursor.fetchall()
        stored_by_key = {business_key(tuple(row)): tuple(row) for row in stored}
        if len(stored_by_key) != len(stored):
            raise ResearchFactConflict("FEP已存范围存在重复业务键；事务已回滚")
        restated = 0
        for key, expected in expected_by_key.items():
            actual = stored_by_key.get(key)
            if actual is None:
                raise ResearchFactConflict("FEP业务键写入后未读回")
            if actual != expected and allow_statistical_restatement:
                non_restatable = {
                    index
                    for index in range(len(_COLUMNS))
                    if index not in restatable_indexes
                }
                if any(actual[index] != expected[index] for index in non_restatable):
                    raise ResearchFactConflict(
                        "FEP纠错影响业务身份或冻结分母；拒绝覆盖并回滚"
                    )
                cursor.execute(
                    update_sql,
                    tuple(
                        expected[_COLUMNS.index(name)] for name in _RESTATABLE_COLUMNS
                    )
                    + tuple(expected[_COLUMNS.index(name)] for name in _COLUMNS[:23]),
                )
                if cursor.rowcount != 1:
                    raise ResearchFactConflict("FEP同键定点修订未更新唯一业务行")
                restated += 1
            elif actual != expected:
                raise ResearchFactConflict(
                    "同一FEP业务键的已存typed内容不同；事务已回滚"
                )
        if restated:
            cursor.execute(
                f"""SELECT {",".join(_COLUMNS)} FROM {_TABLE}
                    WHERE research_cutoff=%s
                      AND source_capture_id=%s
                      AND rule_id=ANY(%s) AND horizon=ANY(%s) AND lane=ANY(%s)
                    ORDER BY summary_scope,target_session,period_start_session,
                             rule_id,lane,horizon,condition_code,group_code,metric_code""",
                (cutoffs[0], capture_ids[0], rules, horizons, lanes),
            )
            stored_after = {
                business_key(tuple(row)): tuple(row) for row in cursor.fetchall()
            }
            if any(
                stored_after.get(key) != value for key, value in expected_by_key.items()
            ):
                raise ResearchFactConflict("FEP受控修订后事务内读回不一致；事务已回滚")
    return {
        "inserted_row_count": inserted,
        "verified_row_count": len(expected_by_key),
        "restated_row_count": restated,
        "status": "PERSISTED_AND_IDEMPOTENT",
        "database_write_attempted": True,
        "transaction": "SERIALIZABLE_SINGLE_BATCH",
    }


def _validate_fep_baseline_closure(rows: list[tuple[Any, ...]]) -> list[str]:
    """证明每个T0三组、比较行及四个条件日期分区都完整且唯一。"""
    reasons = []
    keys = [tuple(row[:23]) for row in rows]
    if len(keys) != len(set(keys)):
        reasons.append("FEP_READER_DUPLICATE_BUSINESS_SCOPE")
    index = {name: _COLUMNS.index(name) for name in _COLUMNS}
    t0_rows = [row for row in rows if row[index["summary_scope"]] == "T0"]
    equal_rows = [row for row in rows if row[index["summary_scope"]] == "EQUAL_DATE"]
    if len(t0_rows) + len(equal_rows) != len(rows):
        reasons.append("FEP_READER_UNKNOWN_SUMMARY_SCOPE")
    expected_t0 = {
        (group, metric, "ALL")
        for group in ("MATCH", "NO_MATCH", "UNKNOWN")
        for metric in ("RAW_CLOSE_RETURN", "RAW_MFE", "RAW_MAE", "RAW_MDD_CLOSE")
    } | {("MATCH_VS_NO_MATCH", "RAW_CLOSE_RETURN", "ALL")}
    targets = sorted({row[index["target_session"]] for row in t0_rows})
    if not targets:
        reasons.append("FEP_READER_T0_ROWS_MISSING")
    for target in targets:
        day_rows = [row for row in t0_rows if row[index["target_session"]] == target]
        by_scope = {
            (
                row[index["group_code"]],
                row[index["metric_code"]],
                row[index["condition_code"]],
            ): row
            for row in day_rows
        }
        if len(by_scope) != len(day_rows) or set(by_scope) != expected_t0:
            reasons.append("FEP_READER_T0_GROUP_METRIC_INCOMPLETE")
            continue
        match_row = by_scope[("MATCH", "RAW_CLOSE_RETURN", "ALL")]
        source_count = match_row[index["source_pool_count"]]
        true_count = match_row[index["t0_match_count"]]
        false_count = match_row[index["t0_no_match_count"]]
        unknown_state_count = match_row[index["t0_unknown_count"]]
        state_denominators = {
            tuple(
                row[index[name]]
                for name in (
                    "source_pool_count",
                    "t0_match_count",
                    "t0_no_match_count",
                    "t0_unknown_count",
                )
            )
            for row in day_rows
        }
        if (
            None in (source_count, true_count, false_count, unknown_state_count)
            or true_count + false_count + unknown_state_count != source_count
            or state_denominators
            != {(source_count, true_count, false_count, unknown_state_count)}
        ):
            reasons.append("FEP_READER_T0_STATE_DENOMINATOR_OPEN")
        lane_member_column = {
            "ENTRY": "entry_count",
            "DAILY_LANDMARK": "daily_landmark_count",
            "REENTRY": "reentry_count",
        }.get(rows[0][index["lane"]])
        if lane_member_column is None:
            reasons.append("FEP_READER_LANE_INVALID")
            continue
        expected_members = {
            "MATCH": match_row[index[lane_member_column]],
            "NO_MATCH": false_count,
            "UNKNOWN": unknown_state_count,
        }
        for group, expected_member_count in expected_members.items():
            group_rows = [
                by_scope[(group, metric, "ALL")]
                for metric in (
                    "RAW_CLOSE_RETURN",
                    "RAW_MFE",
                    "RAW_MAE",
                    "RAW_MDD_CLOSE",
                )
            ]
            if any(
                row[index["member_count"]] != expected_member_count
                for row in group_rows
            ):
                reasons.append("FEP_READER_T0_GROUP_DENOMINATOR_MISMATCH")
            path_counts = {
                tuple(
                    row[index[name]]
                    for name in (
                        "status",
                        "member_count",
                        "due_count",
                        "not_due_count",
                        "observed_count",
                        "unknown_count",
                        "not_computed_count",
                    )
                )
                for row in group_rows[1:]
            }
            if len(path_counts) != 1:
                reasons.append("FEP_READER_T0_PATH_METRICS_PARTIAL")
        if by_scope.get(("MATCH_VS_NO_MATCH", "RAW_CLOSE_RETURN", "ALL")) is None:
            reasons.append("FEP_READER_T0_COMPARISON_MISSING")

    expected_conditions = {
        ("MATCH_VS_NO_MATCH", "RAW_CLOSE_RETURN", condition)
        for condition in (
            "ALL",
            "B_FIXED_POSITIVE",
            "B_FIXED_NON_POSITIVE",
            "B_FIXED_UNKNOWN",
        )
    }
    equal_scopes = [
        (
            row[index["group_code"]],
            row[index["metric_code"]],
            row[index["condition_code"]],
        )
        for row in equal_rows
    ]
    if (
        len(equal_scopes) != len(set(equal_scopes))
        or set(equal_scopes) != expected_conditions
    ):
        reasons.append("FEP_READER_EQUAL_DATE_CONDITIONS_INCOMPLETE")
    if equal_rows:
        expected_start = min(targets) if targets else None
        expected_end = max(targets) if targets else None
        requested = len(targets)
        for row in equal_rows:
            if (
                row[index["period_start_session"]] != expected_start
                or row[index["period_end_session"]] != expected_end
                or row[index["requested_date_count"]] != requested
            ):
                reasons.append("FEP_READER_EQUAL_DATE_RANGE_MISMATCH")
                break
    return sorted(set(reasons))


def _comparison_group(row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    index = {name: _COLUMNS.index(name) for name in _COLUMNS}
    return {
        "status": row[index["status"]],
        "member_count": row[index["member_count"]],
        "due_count": row[index["due_count"]],
        "not_due_count": row[index["not_due_count"]],
        "observed_count": row[index["observed_count"]],
        "unknown_count": row[index["unknown_count"]],
        "not_computed_count": row[index["not_computed_count"]],
        "p25": _ratio(row[index["p25_numerator"]], row[index["p25_denominator"]]),
        "p50": _ratio(row[index["p50_numerator"]], row[index["p50_denominator"]]),
        "p75": _ratio(row[index["p75_numerator"]], row[index["p75_denominator"]]),
    }


def _same_t0_lane_comparison(
    rows: list[tuple[Any, ...]], *, counterpart_closure_reasons: list[str]
) -> dict[str, Any]:
    """从同一typed表的T0 MATCH行生成只读、日期等权lane对照。"""
    index = {name: _COLUMNS.index(name) for name in _COLUMNS}
    identity_fields = (
        "source_capture_id",
        "source_capture_id",
        "target_session",
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
        "horizon",
        "summary_scope",
        "group_code",
        "condition_code",
        "market_condition_code",
        "market_time_quality",
        "entity_code",
        "target_code",
        "scope",
        "price_coordinate",
    )
    identity_indexes = tuple(index[name] for name in identity_fields)
    groups: dict[tuple[str, date, str], tuple[Any, ...] | None] = {}
    dates = sorted(
        {
            row[index["target_session"]]
            for row in rows
            if row[index["summary_scope"]] == "T0"
            and row[index["group_code"]] == "MATCH"
        }
    )
    duplicate_keys: set[tuple[str, date, str]] = set()
    for row in rows:
        if (
            row[index["summary_scope"]] != "T0"
            or row[index["group_code"]] != "MATCH"
            or row[index["condition_code"]] != "ALL"
        ):
            continue
        key = (
            row[index["lane"]],
            row[index["target_session"]],
            row[index["metric_code"]],
        )
        if key in groups:
            duplicate_keys.add(key)
        else:
            groups[key] = row

    metrics = []
    for metric_code in (
        "RAW_CLOSE_RETURN",
        "RAW_MFE",
        "RAW_MAE",
        "RAW_MDD_CLOSE",
    ):
        date_rows = []
        differences = []
        status_counts = Counter()
        for target in dates:
            entry_key = ("ENTRY", target, metric_code)
            reentry_key = ("REENTRY", target, metric_code)
            entry_row = groups.get(entry_key)
            reentry_row = groups.get(reentry_key)
            entry = _comparison_group(entry_row)
            reentry = _comparison_group(reentry_row)
            pair_status = "NO_COMPARABLE_PAIR"
            difference = None
            identity_matches = bool(
                entry_row is not None
                and reentry_row is not None
                and tuple(entry_row[i] for i in identity_indexes)
                == tuple(reentry_row[i] for i in identity_indexes)
            )
            if entry_key in duplicate_keys or reentry_key in duplicate_keys:
                pair_status = "INCOMPLETE"
            elif entry_row is not None and reentry_row is not None:
                if not identity_matches:
                    pair_status = "INPUT_IDENTITY_MISMATCH"
                elif (
                    entry["status"] == "OBSERVED"
                    and reentry["status"] == "OBSERVED"
                    and entry["observed_count"] > 0
                    and reentry["observed_count"] > 0
                    and entry["p50"] is not None
                    and reentry["p50"] is not None
                ):
                    pair_status = "OBSERVED"
                    difference = Fraction(reentry["p50"]) - Fraction(entry["p50"])
                    differences.append(difference)
                else:
                    statuses = {entry["status"], reentry["status"]}
                    pair_status = next(
                        (
                            status
                            for status in (
                                "INCOMPLETE",
                                "UNKNOWN",
                                "NOT_COMPUTED",
                                "NOT_DUE",
                                "NOT_EVALUABLE",
                            )
                            if status in statuses
                        ),
                        "NO_COMPARABLE_PAIR",
                    )
            status_counts[pair_status] += 1
            date_rows.append(
                {
                    "target_session": target.isoformat(),
                    "status": pair_status,
                    "entry": entry,
                    "reentry": reentry,
                    "reentry_minus_entry_p50": str(difference)
                    if difference is not None
                    else None,
                }
            )

        paired_dates = len(differences)
        status = (
            "NO_COMPARABLE_PAIR"
            if paired_dates == 0
            else "INSUFFICIENT_INDEPENDENT_DATES"
            if paired_dates == 1
            else "DESCRIPTIVE_ONLY"
        )
        metrics.append(
            {
                "metric_code": metric_code,
                "status": status,
                "requested_t0_date_count": len(dates),
                "paired_t0_date_count": paired_dates,
                "no_comparable_pair_date_count": status_counts["NO_COMPARABLE_PAIR"],
                "not_due_date_count": status_counts["NOT_DUE"],
                "unknown_date_count": status_counts["UNKNOWN"],
                "not_computed_date_count": status_counts["NOT_COMPUTED"],
                "incomplete_date_count": status_counts["INCOMPLETE"]
                + status_counts["INPUT_IDENTITY_MISMATCH"],
                "not_evaluable_date_count": status_counts["NOT_EVALUABLE"],
                "equal_date_mean_p50_difference": str(
                    sum(differences, Fraction()) / paired_dates
                )
                if paired_dates
                else None,
                "positive_direction_date_count": sum(
                    value > 0 for value in differences
                ),
                "negative_direction_date_count": sum(
                    value < 0 for value in differences
                ),
                "zero_direction_date_count": sum(value == 0 for value in differences),
                "dates": date_rows,
            }
        )

    overall = (
        "INCOMPLETE"
        if counterpart_closure_reasons
        else "DESCRIPTIVE_ONLY"
        if any(item["paired_t0_date_count"] > 1 for item in metrics)
        else "INSUFFICIENT_INDEPENDENT_DATES"
        if any(item["paired_t0_date_count"] == 1 for item in metrics)
        else "NO_COMPARABLE_PAIR"
    )
    return {
        "status": overall,
        "direction": "REENTRY_MINUS_ENTRY",
        "comparison_source": "PERSISTED_TYPED_T0_MATCH_ROWS",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "capability": "DESCRIPTIVE_ONLY",
        "causal_or_strategy_claim": False,
        "reason_codes": counterpart_closure_reasons,
        "requested_t0_date_count": len(dates),
        "metrics": metrics,
    }


def read_fep_baseline(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_capture_id: int,
    rule_id: str,
    horizon: int,
    lane: str,
) -> dict[str, Any]:
    """只读读取已经完成的T0组统计及等权日期汇总，不触碰来源表。"""
    if (
        type(research_cutoff) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(rule_id, str)
        or not rule_id.strip()
        or horizon not in {1, 3, 5}
        or lane not in {"ENTRY", "DAILY_LANDMARK", "REENTRY"}
    ):
        raise ValueError("FEP只读查询参数无效")
    query_lanes = ("ENTRY", "REENTRY") if lane in {"ENTRY", "REENTRY"} else (lane,)
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            f"""SELECT {",".join(_COLUMNS)} FROM {_TABLE}
                WHERE research_cutoff=%s
                  AND source_capture_id=%s
                  AND rule_id=%s AND horizon=%s AND lane=ANY(%s)
                ORDER BY lane,summary_scope,target_session,condition_code,group_code,metric_code""",
            (research_cutoff, source_capture_id, rule_id, horizon, list(query_lanes)),
        )
        stored_rows = cursor.fetchall()
    index = {name: _COLUMNS.index(name) for name in _COLUMNS}
    t0_rows = [row for row in stored_rows if row[index["summary_scope"]] == "T0"]
    target_sessions = {row[index["target_session"]] for row in t0_rows}
    equal_rows = []
    if target_sessions:
        period_start = min(target_sessions)
        period_end = max(target_sessions)
        context_columns = (
            "rule_version",
            "profile_family",
            "calendar_revision",
            "path_rule_version",
            "source_time_quality",
            "parser_contract",
            "lane",
            "horizon",
        )
        t0_contexts = {
            tuple(row[index[name]] for name in context_columns) for row in t0_rows
        }
        equal_rows = [
            row
            for row in stored_rows
            if row[index["summary_scope"]] == "EQUAL_DATE"
            and row[index["source_capture_id"]] == source_capture_id
            and row[index["period_start_session"]] == period_start
            and row[index["period_end_session"]] == period_end
            and tuple(row[index[name]] for name in context_columns) in t0_contexts
        ]
    comparison_rows = tuple((*t0_rows, *equal_rows))
    rows = [row for row in comparison_rows if row[index["lane"]] == lane]
    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "reason_codes": ["NO_PERSISTED_FEP_BASELINE"],
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
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
    partition_columns = (
        "rule_id",
        "rule_version",
        "profile_family",
        "calendar_revision",
        "path_rule_version",
        "source_time_quality",
        "parser_contract",
    )
    partitions = {
        tuple(row[_COLUMNS.index(name)] for name in partition_columns) for row in rows
    }
    closure_reasons = _validate_fep_baseline_closure(rows)
    if len(partitions) != 1:
        closure_reasons.append("FEP_READER_MULTIPLE_INPUT_PARTITIONS")
    result = {
        "status": "AVAILABLE" if not closure_reasons else "INCOMPLETE",
        "reason_codes": sorted(set(closure_reasons)),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "rule_id": rule_id,
        "rule_version": rows[0][_COLUMNS.index("rule_version")],
        "horizon": horizon,
        "lane": lane,
        "profile_family": rows[0][_COLUMNS.index("profile_family")],
        "calendar_revision": rows[0][_COLUMNS.index("calendar_revision")],
        "path_rule_version": rows[0][_COLUMNS.index("path_rule_version")],
        "entity": "SOURCE_STOCK",
        "target": "RAW_CLOSE_RETURN",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "capability": "DESCRIPTIVE_ONLY",
        "model_capability": "NOT_ENABLED",
        "rows": records,
    }
    if lane in {"ENTRY", "REENTRY"}:
        counterpart = "REENTRY" if lane == "ENTRY" else "ENTRY"
        counterpart_rows = [
            row for row in comparison_rows if row[_COLUMNS.index("lane")] == counterpart
        ]
        counterpart_reasons = (
            _validate_fep_baseline_closure(counterpart_rows) if counterpart_rows else []
        )
        if (
            counterpart_rows
            and len(
                {
                    (
                        row[_COLUMNS.index("rule_id")],
                        row[_COLUMNS.index("rule_version")],
                        row[_COLUMNS.index("profile_family")],
                        row[_COLUMNS.index("calendar_revision")],
                        row[_COLUMNS.index("path_rule_version")],
                        row[_COLUMNS.index("source_time_quality")],
                        row[_COLUMNS.index("parser_contract")],
                    )
                    for row in counterpart_rows
                }
            )
            != 1
        ):
            counterpart_reasons.append("FEP_READER_MULTIPLE_INPUT_PARTITIONS")
        result["same_t0_comparison"] = _same_t0_lane_comparison(
            comparison_rows,
            counterpart_closure_reasons=sorted(set(counterpart_reasons)),
        )
    return result
