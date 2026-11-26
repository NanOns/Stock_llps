"""来源级规则与到期路径事实的原子写入及独立读取。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence

from psycopg import sql

from stock_a.storage.postgres.source_raw import _transaction

RULE_VERSION = "SOURCE_COHORT_RULES_R1"
PATH_VERSION = "SOURCE_RAW_FORWARD_PATH_R1"
SCOPE = "NON_PIT_EXPLORATORY"
_SEED_PRODUCER_VERSION = "SOURCE_POOL_SEED_CORE_PROFILE_R2"
_MULTISESSION_KEY_MIGRATION_ID = 66


class ResearchFactNotFound(ValueError):
    """精确研究键没有已保存的规则事实。"""

    preserve_transaction_error = True


class ResearchFactConflict(ValueError):
    """同一业务键对应不同冻结事实，事务必须完整回滚。"""

    preserve_transaction_error = True


def _require_multisession_key_migration(cursor: Any) -> None:
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM migration_ledger "
        "WHERE migration_id=%s AND lineage='STOCK_A_INITIAL_R1')",
        (_MULTISESSION_KEY_MIGRATION_ID,),
    )
    if cursor.fetchone() != (True,):
        raise ResearchFactConflict(
            "0066目标会话唯一键迁移未应用，拒绝写入Forward或规则事实"
        )


RULES = frozenset(
    {
        "LAUNCH_CONFIRM",
        "RECOVERY_TURN",
        "s1",
        "s2",
        "seed",
        "D1_T0",
        "D1_ANCHOR_WINDOW",
        "STRONG_PULLBACK_DIAGNOSTIC",
        "TREND_CONTINUE_DIAGNOSTIC",
    }
)

RULE_COLUMNS = (
    "target_session",
    "source_symbol",
    "source_capture_id",
    "rule_id",
    "rule_version",
    "profile_revision",
    "history_session_count",
    "d1_history_session_count",
    "event_window_start",
    "calendar_revision",
    "state",
    "reasons",
    "event_count",
    "frozen_line_numerator",
    "frozen_line_denominator",
    "frozen_line_source",
    "scope",
)
PATH_COLUMNS = (
    "target_session",
    "source_symbol",
    "source_capture_id",
    "horizon",
    "path_rule_version",
    "calendar_revision",
    "endpoint_session",
    "maturity",
    "endpoint_quality",
    "endpoint_reason",
    "endpoint_numerator",
    "endpoint_denominator",
    "path_quality",
    "path_reasons",
    "mfe_numerator",
    "mfe_denominator",
    "mae_numerator",
    "mae_denominator",
    "mdd_numerator",
    "mdd_denominator",
    "price_coordinate",
    "scope",
)
RULE_KEY = (
    "target_session",
    "source_symbol",
    "rule_id",
)
PATH_KEY = (
    "target_session",
    "source_symbol",
    "horizon",
)


def _fraction_pair(value: Any) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    if isinstance(value, Mapping):
        value = value["exact"]
    fraction = Fraction(value)
    return fraction.numerator, fraction.denominator


def prepare_research_rows(
    *,
    target,
    source,
    calendar_revision,
    session_versions,
    frozen,
    outcomes,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
):
    """只转换实际生产者结果；未到期路径不构成持久结算事实。"""
    if not frozen or set(frozen) != set(outcomes):
        raise ValueError("冻结规则与后继路径的来源池不一致")
    capture = source["source_capture_id"]
    if (
        type(capture) is not int
        or capture <= 0
        or source.get("source_capture_id") != source_capture_id
    ):
        raise ValueError("研究事实缺少真实BaoStock来源捕获数字ID")
    rules, paths = [], []
    expected_rules = None
    for symbol in sorted(frozen):
        item = frozen[symbol]
        names = set(item["rules"])
        if expected_rules is None:
            expected_rules = names
        if names != expected_rules or names != RULES:
            raise ValueError("每个来源代码必须具有一致的九个规则")
        prefix = (target, symbol, source_capture_id)
        for rule, fact in sorted(item["rules"].items()):
            state = fact["state"]
            reasons = fact.get("reasons") or []
            if state not in {"TRUE", "FALSE", "UNKNOWN"} or any(
                not isinstance(reason, str) for reason in reasons
            ):
                raise ValueError("规则状态或原因无效")
            count = 0
            if rule == "D1_ANCHOR_WINDOW" and state == "TRUE":
                count = len(item["anchors"])
            elif rule == "STRONG_PULLBACK_DIAGNOSTIC" and state == "TRUE":
                count = item["diagnostic_shape_summary"]["matched_parent_count"]
            line = fact.get("frozen_line") or {}
            line_value = line.get("value")
            line_pair = (
                (int(line_value["numerator"]), int(line_value["denominator"]))
                if line_value is not None
                else (None, None)
            )
            rules.append(
                prefix
                + (
                    rule,
                    RULE_VERSION,
                    profile_revision,
                    history_session_count,
                    d1_history_session_count,
                    event_window_start,
                    calendar_revision,
                    state,
                    list(reasons),
                    count,
                    *line_pair,
                    line.get("source"),
                    SCOPE,
                )
            )
        for horizon, fact in sorted(outcomes[symbol].items()):
            if not fact["due"]:
                continue
            endpoint_day = date.fromisoformat(fact["target_session"])
            if endpoint_day.isoformat() not in session_versions:
                raise ValueError("到期路径缺少来源会话版本")
            endpoint = fact["endpoint"]["qfq_close_relative_change"]
            ep_num, ep_den = _fraction_pair(endpoint.get("value"))
            values = fact.get("path_values") or {}
            mfe = _fraction_pair(values.get("mfe"))
            mae = _fraction_pair(values.get("mae"))
            mdd = _fraction_pair(values.get("mdd"))
            paths.append(
                prefix
                + (
                    horizon,
                    PATH_VERSION,
                    calendar_revision,
                    endpoint_day,
                    fact["maturity"],
                    endpoint["quality"],
                    endpoint.get("reason"),
                    ep_num,
                    ep_den,
                    fact["path_quality"],
                    list(fact["path_reasons"]),
                    *mfe,
                    *mae,
                    *mdd,
                    "FORWARD_ADJUSTED",
                    SCOPE,
                )
            )
    return rules, paths


def prepare_forward_path_rows(
    *,
    target,
    source,
    calendar_revision,
    session_versions,
    outcomes,
    source_capture_id,
    horizon,
    source_capture_ids_by_symbol: Mapping[str, int] | None = None,
):
    """将现役原始价格路径算法的已到期结果转为共享typed行。"""
    if (
        type(target) is not date
        or type(horizon) is not int
        or horizon not in {1, 3, 5, 10, 20}
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or not isinstance(outcomes, Mapping)
        or not outcomes
    ):
        raise ValueError("后继路径日期、期限、日历或结果集合无效")
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise ValueError("后继路径缺少真实BaoStock任务捕获溯源")
    if source_capture_ids_by_symbol is not None and (
        not isinstance(source_capture_ids_by_symbol, Mapping)
        or set(source_capture_ids_by_symbol) != set(outcomes)
        or any(
            type(capture_id) is not int or capture_id <= 0
            for capture_id in source_capture_ids_by_symbol.values()
        )
    ):
        raise ValueError("T0证券QFQ坐标捕获映射未按来源代码精确闭合")

    rows = []
    for symbol in sorted(outcomes):
        fact = outcomes[symbol]
        if fact.get("due") is not True:
            continue
        endpoint_day = date.fromisoformat(fact["target_session"])
        version = session_versions.get(endpoint_day.isoformat())
        if (
            endpoint_day <= target
            or not isinstance(version, Mapping)
            or version.get("status") != "CLOSED"
            or version.get("row_count") != len(outcomes)
        ):
            raise ValueError("已到期后继路径没有完整且同一日历的Raw来源分区")
        endpoint = fact["endpoint"]["qfq_close_relative_change"]
        endpoint_num, endpoint_den = _fraction_pair(endpoint.get("value"))
        values = fact.get("path_values") or {}
        mfe = _fraction_pair(values.get("mfe"))
        mae = _fraction_pair(values.get("mae"))
        mdd = _fraction_pair(values.get("mdd"))
        rows.append(
            (
                target,
                symbol,
                (
                    source_capture_ids_by_symbol[symbol]
                    if source_capture_ids_by_symbol is not None
                    else source_capture_id
                ),
                horizon,
                PATH_VERSION,
                calendar_revision,
                endpoint_day,
                fact["maturity"],
                endpoint["quality"],
                endpoint.get("reason"),
                endpoint_num,
                endpoint_den,
                fact["path_quality"],
                list(fact["path_reasons"]),
                *mfe,
                *mae,
                *mdd,
                "FORWARD_ADJUSTED",
                SCOPE,
            )
        )
    if len(rows) != len(outcomes):
        raise ValueError("本批T+1结果未全部成熟，拒绝保存不完整路径行")
    return rows


def _invalidate_downstream_stats_in_cursor(
    cursor: Any,
    *,
    target_session: date,
    horizon: int | None = None,
    rule_ids: Sequence[str] | None = None,
    invalidate_s2_t1: bool = False,
) -> None:
    """在当前写事务内清除受影响Cohort/S2-T1/FEP统计并失效对应任务完成进度。"""
    affected_cutoffs: set[date] = set()
    if rule_ids:
        cursor.execute(
            """DELETE FROM source_cohort_group_stats
               WHERE target_session=%s AND rule_id=ANY(%s)""",
            (target_session, list(rule_ids)),
        )
    elif horizon is not None:
        cursor.execute(
            """DELETE FROM source_cohort_group_stats
               WHERE target_session=%s AND horizon=%s""",
            (target_session, horizon),
        )
    else:
        cursor.execute(
            """DELETE FROM source_cohort_group_stats
               WHERE target_session=%s""",
            (target_session,),
        )

    if invalidate_s2_t1:
        cursor.execute(
            """DELETE FROM source_s2_t1_cohort_stats
               WHERE target_session=%s""",
            (target_session,),
        )

    fep_filters = ["summary_scope='T0'", "target_session=%s"]
    fep_params: list[Any] = [target_session]
    if horizon is not None:
        fep_filters.append("horizon=%s")
        fep_params.append(horizon)
    if rule_ids:
        fep_filters.append("rule_id=ANY(%s)")
        fep_params.append(list(rule_ids))
    where_fep = " AND ".join(fep_filters)
    cursor.execute(
        f"""WITH affected_t0 AS (
                SELECT id, research_cutoff, rule_id, rule_version, profile_family,
                       calendar_revision, path_rule_version, source_time_quality,
                       parser_contract, lane, horizon, group_code, metric_code, condition_code
                FROM source_fep_baseline_facts
                WHERE {where_fep}
            ), affected_all AS (
                SELECT id, research_cutoff FROM affected_t0
                UNION
                SELECT f.id, f.research_cutoff
                FROM source_fep_baseline_facts f
                JOIN affected_t0 t0
                  ON f.research_cutoff=t0.research_cutoff
                 AND f.rule_id=t0.rule_id AND f.rule_version=t0.rule_version
                 AND f.profile_family=t0.profile_family
                 AND f.calendar_revision=t0.calendar_revision
                 AND f.path_rule_version=t0.path_rule_version
                 AND f.source_time_quality=t0.source_time_quality
                 AND f.parser_contract=t0.parser_contract
                 AND f.lane=t0.lane AND f.horizon=t0.horizon
                 AND f.group_code=t0.group_code
                 AND f.metric_code=t0.metric_code
                 AND f.condition_code=t0.condition_code
                WHERE f.summary_scope='EQUAL_DATE'
                  AND f.source_capture_id IS NULL
                  AND f.period_start_session<=%s
                  AND %s<=f.period_end_session
            )
            DELETE FROM source_fep_baseline_facts f
            USING affected_all a
            WHERE f.id=a.id
            RETURNING a.research_cutoff""",
        (*fep_params, target_session, target_session),
    )
    affected_cutoffs.update(row[0] for row in cursor.fetchall() if row[0] is not None)

    invalidated_job_ids: set[Any] = set()
    cursor.execute(
        """UPDATE stock.data_job_progress
           SET status='NOT_COMPUTED', completed_units=0, expected_rows=NULL,
               verified_rows=NULL, inserted_rows=0, updated_rows=0,
               quality_status=NULL, error_code='DERIVED_RESULT_CLEARED',
               error_message='上游Forward或规则事实已修订，对应Cohort统计失效。',
               updated_at=clock_timestamp(), completed_at=NULL, verified_at=NULL
           WHERE target_session=%s
             AND business_table='stock.source_cohort_group_stats'
             AND status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
           RETURNING job_id""",
        (target_session,),
    )
    invalidated_job_ids.update(row[0] for row in cursor.fetchall())

    if affected_cutoffs:
        cursor.execute(
            """UPDATE stock.data_job_progress p
               SET status='NOT_COMPUTED', completed_units=0, expected_rows=NULL,
                   verified_rows=NULL, inserted_rows=0, updated_rows=0,
                   quality_status=NULL, error_code='DERIVED_RESULT_CLEARED',
                   error_message='上游Forward或规则事实已修订，对应FEP统计失效。',
                   updated_at=clock_timestamp(), completed_at=NULL, verified_at=NULL
               FROM stock.data_jobs j
               WHERE p.job_id=j.job_id
                 AND p.business_table='stock.source_fep_baseline_facts'
                 AND j.research_cutoff=ANY(%s)
                 AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
               RETURNING p.job_id""",
            (list(sorted(affected_cutoffs)),),
        )
        invalidated_job_ids.update(row[0] for row in cursor.fetchall())

    for jid in invalidated_job_ids:
        cursor.execute(
            """UPDATE stock.data_jobs j
               SET status='INCOMPLETE',
                   completed_units=(
                       SELECT coalesce(sum(p.completed_units),0)
                       FROM stock.data_job_progress p
                       WHERE p.job_id=j.job_id
                   ),
                   last_error_code='DERIVED_RESULT_CLEARED',
                   last_error_message='上游Forward或规则事实已修订，下游统计进度失效。',
                   updated_at=clock_timestamp(), completed_at=clock_timestamp()
               WHERE job_id=%s AND status IN ('COMPLETED','NO_CHANGE')""",
            (jid,),
        )


def write_forward_path_rows(
    config,
    *,
    paths,
    source_symbols,
    endpoint_source_capture_id,
    research_cutoff,
    endpoint_source_capture_ids_by_symbol: Mapping[str, int | None] | None = None,
):
    """按T0/期限保存QFQ后继事实；来源capture分别校验且只作溯源元数据。"""
    symbols = tuple(sorted(set(source_symbols)))
    if (
        not paths
        or not symbols
        or len(symbols) != len(source_symbols)
        or len(paths) != len(symbols)
        or endpoint_source_capture_id is not None
        and (
            type(endpoint_source_capture_id) is not int
            or endpoint_source_capture_id <= 0
        )
        or type(research_cutoff) is not date
    ):
        raise ValueError("后继路径写入必须绑定非空来源池、截止日及capture身份")
    first = paths[0]
    target, _symbol, _first_capture_id = first[:3]
    horizon, path_version, calendar_revision, endpoint_session = first[3:7]
    source_capture_ids_by_symbol = {
        row[1]: row[2] for row in paths if len(row) == len(PATH_COLUMNS)
    }
    target_capture_ids = set(source_capture_ids_by_symbol.values())
    if endpoint_source_capture_ids_by_symbol is not None and (
        not isinstance(endpoint_source_capture_ids_by_symbol, Mapping)
        or set(endpoint_source_capture_ids_by_symbol) != set(symbols)
        or any(
            capture_id is not None and (type(capture_id) is not int or capture_id <= 0)
            for capture_id in endpoint_source_capture_ids_by_symbol.values()
        )
    ):
        raise ValueError("T+1证券QFQ捕获映射与来源代码集合不一致")
    endpoint_capture_ids = (
        set(endpoint_source_capture_ids_by_symbol.values())
        if endpoint_source_capture_ids_by_symbol is not None
        else {endpoint_source_capture_id}
    )
    endpoint_capture_scope_complete = None not in endpoint_capture_ids
    endpoint_capture_ids.discard(None)
    uniform_target_capture_id = (
        next(iter(target_capture_ids)) if len(target_capture_ids) == 1 else None
    )
    uniform_endpoint_capture_id = (
        next(iter(endpoint_capture_ids)) if len(endpoint_capture_ids) == 1 else None
    )
    if (
        type(target) is not date
        or type(horizon) is not int
        or horizon not in (1, 3, 5, 10, 20)
        or path_version != PATH_VERSION
        or type(endpoint_session) is not date
        or target >= endpoint_session
        or endpoint_session > research_cutoff
        or not calendar_revision
        or len(source_capture_ids_by_symbol) != len(symbols)
        or any(
            type(capture_id) is not int or capture_id <= 0
            for capture_id in target_capture_ids
        )
        or tuple(sorted(row[1] for row in paths)) != symbols
        or any(
            row[0] != target
            or row[3:7] != (horizon, path_version, calendar_revision, endpoint_session)
            or row[-2:] != ("FORWARD_ADJUSTED", SCOPE)
            for row in paths
        )
    ):
        raise ValueError("T+1 QFQ路径行存在日期、capture、坐标、期限或键混用")

    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        _require_multisession_key_migration(cursor)
        for capture_id in sorted(target_capture_ids):
            cursor.execute(
                """SELECT capture_uuid,provider,range_start_session,range_end_session,
                          price_coordinates,source_time_quality,source_clock_role,
                          source_available_at,unit_contract_version
                   FROM source_captures WHERE capture_id=%s""",
                (capture_id,),
            )
            capture = cursor.fetchone()
            if (
                capture is None
                or capture[0] is None
                or capture[1] != "BAOSTOCK"
                or capture[2] is None
                or capture[3] is None
                or not capture[2] <= target <= capture[3] <= research_cutoff
                or "FORWARD_ADJUSTED" not in set(capture[4] or ())
                or capture[5] != "NON_PIT_OBSERVED"
                or capture[6] != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
                or capture[7] is not None
                or not capture[8]
            ):
                raise ValueError("T0路径证券没有截止内真实QFQ BaoStock NON_PIT捕获")

        for endpoint_capture_id in sorted(endpoint_capture_ids):
            cursor.execute(
                """SELECT capture_uuid,provider,range_start_session,range_end_session,
                          price_coordinates,source_time_quality,source_clock_role,
                          source_available_at,unit_contract_version
                   FROM source_captures WHERE capture_id=%s""",
                (endpoint_capture_id,),
            )
            endpoint_capture = cursor.fetchone()
            if (
                endpoint_capture is None
                or endpoint_capture[0] is None
                or endpoint_capture[1] != "BAOSTOCK"
                or endpoint_capture[2] is None
                or endpoint_capture[3] is None
                or not endpoint_capture[2]
                <= endpoint_session
                <= endpoint_capture[3]
                <= research_cutoff
                or "FORWARD_ADJUSTED" not in set(endpoint_capture[4] or ())
                or endpoint_capture[5] != "NON_PIT_OBSERVED"
                or endpoint_capture[6] != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
                or endpoint_capture[7] is not None
                or not endpoint_capture[8]
            ):
                raise ValueError("T+1终点没有绑定截止内真实QFQ BaoStock NON_PIT捕获")

        for label, session in (("T0", target), ("T+1", endpoint_session)):
            cursor.execute(
                """SELECT source_symbol,qfq_source_capture_id,qfq_price_quality
                   FROM source_raw_daily
                   WHERE session_date=%s
                     AND source_symbol=ANY(%s::text[])
                     AND board IN ('MAIN_BOARD', 'GEM')
                   ORDER BY source_symbol""",
                (session, list(symbols)),
            )
            stored_rows = cursor.fetchall()
            stored_symbols = tuple(row[0] for row in stored_rows)
            if label == "T0":
                expected_capture_by_symbol = source_capture_ids_by_symbol
            elif endpoint_source_capture_ids_by_symbol is not None:
                expected_capture_by_symbol = endpoint_source_capture_ids_by_symbol
            elif endpoint_source_capture_id is not None:
                expected_capture_by_symbol = {
                    symbol: endpoint_source_capture_id for symbol in symbols
                }
            else:
                expected_capture_by_symbol = {}
            actual_capture_by_symbol = {row[0]: row[1] for row in stored_rows}
            actual_quality_by_symbol = {row[0]: row[2] for row in stored_rows}
            if (
                stored_symbols != tuple(sorted(symbols))
                or any(row[2] is None for row in stored_rows)
                or any(row[2] == "OBSERVED" and row[1] is None for row in stored_rows)
                or any(
                    expected_capture is not None
                    and actual_capture_by_symbol.get(symbol) != expected_capture
                    for symbol, expected_capture in expected_capture_by_symbol.items()
                )
                or any(
                    actual_quality_by_symbol.get(symbol) == "OBSERVED"
                    and expected_capture_by_symbol.get(symbol) is None
                    and endpoint_source_capture_ids_by_symbol is not None
                    for symbol in symbols
                )
            ):
                raise ValueError(f"{label} paired QFQ/RAW请求未闭合到固定来源集合")

        cursor.execute(
            """SELECT NULL::text AS profile_revision,producer_contract,producer_version,
                      history_session_count,calendar_id,calendar_revision,
                      research_mode,price_coordinate,count(*),count(DISTINCT source_symbol)
               FROM source_pool_seed_day_facts
               WHERE target_session=%s AND producer_version=%s
               GROUP BY producer_contract,producer_version,
                        history_session_count,calendar_id,calendar_revision,
                        research_mode,price_coordinate""",
            (target, _SEED_PRODUCER_VERSION),
        )
        seed_scopes = cursor.fetchall()
        if (
            len(seed_scopes) != 1
            or seed_scopes[0][5] != calendar_revision
            or seed_scopes[0][6] != "NON_PIT_RETROSPECTIVE"
            or seed_scopes[0][7] != "FORWARD_ADJUSTED"
            or int(seed_scopes[0][8]) != len(symbols)
            or int(seed_scopes[0][9]) != len(symbols)
        ):
            raise ValueError("T0 Seed不是单一完整的QFQ来源范围")

        cursor.execute(
            """SELECT source_symbol,endpoint_session FROM source_forward_path_facts
               WHERE target_session=%s AND horizon=%s
               ORDER BY source_symbol""",
            (target, horizon),
        )
        existing_rows = cursor.fetchall()
        stale_symbols = {row[0] for row in existing_rows} - set(symbols)
        deleted = 0
        if stale_symbols:
            cursor.execute(
                """DELETE FROM source_forward_path_facts
                   WHERE target_session=%s AND horizon=%s
                     AND source_symbol=ANY(%s::text[])""",
                (target, horizon, sorted(stale_symbols)),
            )
            deleted = cursor.rowcount
        selection = "target_session=%s AND horizon=%s"
        args = (target, horizon)
        inserted = _insert_and_compare(
            cursor,
            "source_forward_path_facts",
            PATH_COLUMNS,
            paths,
            PATH_KEY,
            selection,
            args,
        )
        cursor.execute(
            """SELECT count(*),count(DISTINCT source_symbol) FROM source_forward_path_facts
               WHERE target_session=%s AND horizon=%s""",
            args,
        )
        if cursor.fetchone() != (len(symbols), len(symbols)):
            raise ResearchFactConflict("T+1typed结果写入后未闭合T0来源池分母")
        if horizon == 1:
            from stock_a.storage.postgres.source_s2_t1_cohort import (
                persist_source_s2_t1_cohort_in_transaction,
            )

            cohort_result = persist_source_s2_t1_cohort_in_transaction(
                cursor,
                target_session=target,
                endpoint_session=endpoint_session,
                research_cutoff=research_cutoff,
                source_capture_id=uniform_target_capture_id,
                endpoint_source_capture_id=uniform_endpoint_capture_id,
                source_symbols=symbols,
                path_rule_version=path_version,
                calendar_revision=calendar_revision,
                insert_and_compare=_insert_and_compare,
            )
        else:
            cohort_result = {
                "inserted_rows": 0,
                "expected_rows": 0,
                "cohort_version": None,
            }
        if inserted or deleted:
            _invalidate_downstream_stats_in_cursor(
                cursor,
                target_session=target,
                horizon=horizon,
                invalidate_s2_t1=False,
            )
    return {
        "target_session": target.isoformat(),
        "endpoint_session": endpoint_session.isoformat(),
        "source_capture_id": uniform_target_capture_id,
        "endpoint_source_capture_id": uniform_endpoint_capture_id,
        "expected_paths": len(paths),
        "inserted_paths": inserted,
        "deleted_obsolete_paths": deleted,
        "idempotent": inserted == 0 and deleted == 0,
        "path_rule_version": path_version,
        "calendar_revision": calendar_revision,
        "s2_t1_cohort_stats_inserted": cohort_result.get("inserted_rows", 0),
        "s2_t1_cohort_version": cohort_result.get("cohort_version"),
        "_s2_t1_cohort_expected_rows": cohort_result["expected_rows"],
        "transaction": "SERIALIZABLE",
    }


def read_forward_path_scope(
    config,
    *,
    paths,
    source_symbols,
):
    """在新只读事务中逐行核对已提交的完整后继路径范围。"""
    symbols = tuple(sorted(set(source_symbols)))
    expected = tuple(sorted(paths, key=lambda row: row[1]))
    if (
        not symbols
        or len(symbols) != len(source_symbols)
        or len(expected) != len(symbols)
        or tuple(row[1] for row in expected) != symbols
    ):
        raise ValueError("T+1异事务读回必须绑定唯一完整来源代码集合")
    first = expected[0]
    target = first[0]
    horizon, path_version, calendar_revision, endpoint_session = first[3:7]
    if any(
        row[0] != target
        or type(row[2]) is not int
        or row[2] <= 0
        or row[3:7] != (horizon, path_version, calendar_revision, endpoint_session)
        for row in expected
    ):
        raise ValueError("T+1异事务读回键跨越多个业务范围或捕获元数据无效")
    columns = sql.SQL(", ").join(map(sql.Identifier, PATH_COLUMNS))
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            sql.SQL("SELECT {} FROM source_forward_path_facts WHERE ").format(columns)
            + sql.SQL("target_session=%s AND horizon=%s ORDER BY source_symbol"),
            (target, horizon),
        )
        stored = tuple(tuple(row) for row in cursor.fetchall())
    if len(stored) != len(expected) or tuple(row[1] for row in stored) != symbols:
        raise ResearchFactConflict("T+1异事务读回未闭合完整来源代码集合")
    comparable_columns = tuple(
        index for index, name in enumerate(PATH_COLUMNS) if name != "source_capture_id"
    )
    if any(
        tuple(row[index] for index in comparable_columns)
        != tuple(expected_row[index] for index in comparable_columns)
        for row, expected_row in zip(stored, expected, strict=True)
    ):
        raise ResearchFactConflict("T+1异事务读回与本次生产typed值不一致")
    endpoint_counts = {"OBSERVED": 0, "UNKNOWN": 0}
    path_counts = {"OBSERVED": 0, "UNKNOWN": 0}
    for row in stored:
        endpoint_counts[row[8]] = endpoint_counts.get(row[8], 0) + 1
        path_counts[row[12]] = path_counts.get(row[12], 0) + 1
    return {
        "status": "READ_BACK_MATCHED",
        "transaction": "REPEATABLE READ READ ONLY",
        "expected_paths": len(expected),
        "read_back_paths": len(stored),
        "endpoint_quality_counts": endpoint_counts,
        "path_quality_counts": path_counts,
    }


def _insert_and_compare(cursor, table, columns, rows, key_columns, selection, args):
    if not rows:
        return 0
    indexes = tuple(columns.index(name) for name in key_columns)
    if table in (
        "source_rule_evaluations",
        "source_forward_path_facts",
        "source_s2_t1_cohort_stats",
    ):
        update_cols = [col for col in columns if col not in key_columns]
        comparison_cols = [
            col
            for col in update_cols
            if col not in {"source_capture_id", "endpoint_source_capture_id"}
        ]
        distinct_predicate = sql.SQL(" OR ").join(
            sql.SQL("{} IS DISTINCT FROM EXCLUDED.{}").format(
                sql.Identifier(col), sql.Identifier(col)
            )
            for col in comparison_cols
        )
        statement = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {} WHERE {}"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            sql.SQL(", ").join(map(sql.Identifier, key_columns)),
            sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(col), sql.Identifier(col)
                )
                for col in update_cols
            ),
            distinct_predicate,
        )
    else:
        statement = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )
    cursor.executemany(statement, rows)
    inserted = cursor.rowcount
    cursor.execute(
        sql.SQL("SELECT {} FROM {} WHERE ").format(
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.Identifier(table),
        )
        + sql.SQL(selection),
        args,
    )
    stored = {tuple(row[i] for i in indexes): tuple(row) for row in cursor.fetchall()}
    comparable_indexes = tuple(
        index
        for index, column in enumerate(columns)
        if column not in {"source_capture_id", "endpoint_source_capture_id"}
    )
    for row in rows:
        actual = stored.get(tuple(row[i] for i in indexes))
        if actual is None or tuple(actual[i] for i in comparable_indexes) != tuple(
            row[i] for i in comparable_indexes
        ):
            raise ResearchFactConflict(f"{table}业务键已存在不同事实，整事务回滚")
    return inserted


def write_computed_rule_rows(
    config,
    *,
    rules: Sequence[Sequence[Any]],
    rule_ids: Sequence[str],
    expected_symbols_by_rule: Mapping[str, Sequence[str]],
    enrollment_rule_ids: Sequence[str],
    previous_session: date,
) -> dict[str, Any]:
    """原子保存已计算规则范围，并为完整规则总体建立真实相邻登记。"""
    selected_rules = tuple(sorted(set(rule_ids)))
    enrollment_rules = tuple(sorted(set(enrollment_rule_ids)))
    if (
        not rules
        or not selected_rules
        or not set(selected_rules).issubset(RULES)
        or len(selected_rules) != len(rule_ids)
        or not isinstance(expected_symbols_by_rule, Mapping)
        or set(expected_symbols_by_rule) != set(selected_rules)
        or not set(enrollment_rules).issubset(selected_rules)
        or type(previous_session) is not date
        or any(len(row) != len(RULE_COLUMNS) for row in rules)
    ):
        raise ValueError("已计算规则事实的写入范围无效")

    first = tuple(rules[0])
    target = first[RULE_COLUMNS.index("target_session")]
    source_capture_id = first[RULE_COLUMNS.index("source_capture_id")]
    metadata_columns = (
        "target_session",
        "source_capture_id",
        "rule_version",
        "profile_revision",
        "history_session_count",
        "d1_history_session_count",
        "event_window_start",
        "calendar_revision",
        "scope",
    )
    metadata = tuple(first[RULE_COLUMNS.index(name)] for name in metadata_columns)
    if (
        type(target) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(metadata[4]) is not int
        or type(metadata[5]) is not int
        or metadata[2] != RULE_VERSION
        or metadata[4] <= 0
        or metadata[5] <= 0
        or type(metadata[6]) is not date
        or (metadata[3] is not None and not isinstance(metadata[3], str))
        or not isinstance(metadata[7], str)
        or not metadata[7]
        or metadata[8] != SCOPE
        or previous_session >= target
    ):
        raise ValueError("规则事实缺少精确日期、Profile、D1窗口或NON_PIT身份")

    expected_by_rule: dict[str, set[str]] = {rule: set() for rule in selected_rules}
    keys = set()
    for row_value in rules:
        row = tuple(row_value)
        row_metadata = tuple(row[RULE_COLUMNS.index(name)] for name in metadata_columns)
        rule = row[RULE_COLUMNS.index("rule_id")]
        symbol = row[RULE_COLUMNS.index("source_symbol")]
        state = row[RULE_COLUMNS.index("state")]
        reasons = row[RULE_COLUMNS.index("reasons")]
        event_count = row[RULE_COLUMNS.index("event_count")]
        numerator = row[RULE_COLUMNS.index("frozen_line_numerator")]
        denominator = row[RULE_COLUMNS.index("frozen_line_denominator")]
        line_source = row[RULE_COLUMNS.index("frozen_line_source")]
        if (
            row_metadata != metadata
            or rule not in expected_by_rule
            or not isinstance(symbol, str)
            or state not in {"TRUE", "FALSE", "UNKNOWN"}
            or not isinstance(reasons, (list, tuple))
            or any(not isinstance(reason, str) for reason in reasons)
            or type(event_count) is not int
            or event_count < 0
            or (numerator is None) != (denominator is None)
            or denominator is not None
            and (type(denominator) is not int or denominator <= 0)
            or numerator is not None
            and type(numerator) is not int
            or line_source is not None
            and not isinstance(line_source, str)
            or row[RULE_COLUMNS.index("scope")] != SCOPE
        ):
            raise ValueError("规则事实包含无效三态、原因、计数或冻结线")
        key = tuple(row[RULE_COLUMNS.index(name)] for name in RULE_KEY)
        if key in keys or symbol in expected_by_rule[rule]:
            raise ValueError("规则事实存在重复真实业务键或代码")
        keys.add(key)
        expected_by_rule[rule].add(symbol)

    declared_symbols = {
        rule: set(expected_symbols_by_rule[rule]) for rule in selected_rules
    }
    if any(
        not symbols or symbols != expected_by_rule[rule]
        for rule, symbols in declared_symbols.items()
    ):
        raise ValueError("每条可计算规则实际写入行数与声明业务范围不一致")

    from stock_a.storage.postgres.source_raw import _transaction

    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        _require_multisession_key_migration(cursor)
        cursor.execute(
            "SELECT source_symbol,raw_source_capture_id FROM source_raw_daily "
            "WHERE session_date=%s AND board IN ('MAIN_BOARD', 'GEM') "
            "ORDER BY source_symbol",
            (target,),
        )
        raw_rows = cursor.fetchall()
        raw_symbols = {row[0] for row in raw_rows}
        if (
            not raw_symbols
            or len(raw_rows) != len(raw_symbols)
            or any(row[1] is None or int(row[1]) <= 0 for row in raw_rows)
            or any(
                not symbols.issubset(raw_symbols)
                for symbols in expected_by_rule.values()
            )
            or any(expected_by_rule[rule] != raw_symbols for rule in enrollment_rules)
        ):
            raise ValueError("规则事实或相邻登记与同捕获Raw来源池不一致")

        inserted = _insert_and_compare(
            cursor,
            "source_rule_evaluations",
            RULE_COLUMNS,
            [tuple(row) for row in rules],
            RULE_KEY,
            "target_session=%s",
            (target,),
        )
        if inserted:
            _invalidate_downstream_stats_in_cursor(
                cursor,
                target_session=target,
                rule_ids=list(selected_rules),
                invalidate_s2_t1=False,
            )
        from stock_a.storage.postgres.research_enrollment import (
            insert_rule_enrollments,
        )

        enrollment = None
        if enrollment_rules:
            enrollment = insert_rule_enrollments(
                cursor,
                target=target,
                before=previous_session,
                source_capture_id=source_capture_id,
                rule_version=metadata[2],
                profile_revision=metadata[3],
                history_count=metadata[4],
                d1_history_count=metadata[5],
                event_window_start=metadata[6],
                calendar_revision=metadata[7],
                rule_ids=enrollment_rules,
            )
    return {
        "status": "PERSISTED"
        if inserted or (enrollment and enrollment.get("inserted"))
        else "IDEMPOTENT",
        "target_session": target.isoformat(),
        "source_capture_id": source_capture_id,
        "rule_version": metadata[2],
        "profile_revision": metadata[3],
        "history_session_count": metadata[4],
        "d1_history_session_count": metadata[5],
        "event_window_start": metadata[6].isoformat(),
        "calendar_revision": metadata[7],
        "rule_ids": list(selected_rules),
        "enrollment_rule_ids": list(enrollment_rules),
        "source_symbol_count": len(raw_symbols),
        "expected_rows": len(rules),
        "inserted_rows": int(inserted),
        "research_enrollments": enrollment,
        "transaction": "SERIALIZABLE_RULE_SUBSET_AND_ENROLLMENTS",
        "ddl_executed": False,
    }


def restate_forward_path_metrics(
    config,
    *,
    corrections,
    research_cutoff,
):
    """按FEP真实依赖对已存Forward路径字段做受控同键纠错。"""
    if type(research_cutoff) is not date or not corrections:
        raise ValueError("Forward路径纠错须绑定截止日和非空业务行")
    grouped = {}
    for expected, desired in corrections:
        if (
            len(expected) != len(PATH_COLUMNS)
            or len(desired) != 8
            or expected[0] > research_cutoff
            or expected[6] > research_cutoff
            or expected[20:] != ("FORWARD_ADJUSTED", SCOPE)
            or expected[4] != PATH_VERSION
            or expected[3] not in {1, 3, 5}
        ):
            raise ValueError("Forward路径纠错越过截止、坐标、版本或期限范围")
        scope = (
            expected[0],
            expected[3],
            expected[4],
            expected[5],
            expected[6],
        )
        grouped.setdefault(scope, {})
        symbol = expected[1]
        if symbol in grouped[scope]:
            raise ResearchFactConflict("Forward纠错批次存在重复T0业务键")
        grouped[scope][symbol] = (tuple(expected), tuple(desired))

    columns = sql.SQL(", ").join(map(sql.Identifier, PATH_COLUMNS))
    select_scope = sql.SQL(
        "SELECT {} FROM source_forward_path_facts WHERE target_session=%s "
        "AND horizon=%s "
        "AND path_rule_version=%s AND calendar_revision=%s AND endpoint_session=%s "
        "AND target_session<=%s AND endpoint_session<=%s "
        "AND price_coordinate='FORWARD_ADJUSTED' AND scope='NON_PIT_EXPLORATORY' "
        "ORDER BY source_symbol FOR UPDATE"
    ).format(columns)
    select_scope_read = sql.SQL(
        "SELECT {} FROM source_forward_path_facts WHERE target_session=%s "
        "AND horizon=%s "
        "AND path_rule_version=%s AND calendar_revision=%s AND endpoint_session=%s "
        "AND target_session<=%s AND endpoint_session<=%s "
        "AND price_coordinate='FORWARD_ADJUSTED' AND scope='NON_PIT_EXPLORATORY' "
        "ORDER BY source_symbol"
    ).format(columns)
    update_sql = sql.SQL(
        "UPDATE source_forward_path_facts SET path_quality=%s,path_reasons=%s,"
        "mfe_numerator=%s,mfe_denominator=%s,mae_numerator=%s,mae_denominator=%s,"
        "mdd_numerator=%s,mdd_denominator=%s WHERE target_session=%s "
        "AND source_symbol=%s "
        "AND horizon=%s AND path_rule_version=%s AND calendar_revision=%s "
        "AND endpoint_session=%s"
    )
    updated_count = 0
    verified_count = 0
    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        for scope, expected_by_symbol in grouped.items():
            target, horizon, version, calendar, endpoint = scope
            cursor.execute(
                select_scope,
                (
                    target,
                    horizon,
                    version,
                    calendar,
                    endpoint,
                    research_cutoff,
                    research_cutoff,
                ),
            )
            stored = [tuple(row) for row in cursor.fetchall()]
            expected_symbols = tuple(sorted(expected_by_symbol))
            if (
                len(stored) != len(expected_symbols)
                or tuple(row[1] for row in stored) != expected_symbols
            ):
                raise ResearchFactConflict("Forward路径纠错范围未闭合完整来源代码集合")
            after = []
            updates = []
            for actual in stored:
                expected, desired = expected_by_symbol[actual[1]]
                comparable_columns = tuple(
                    index
                    for index, name in enumerate(PATH_COLUMNS)
                    if name != "source_capture_id"
                )
                if tuple(actual[index] for index in comparable_columns) != tuple(
                    expected[index] for index in comparable_columns
                ):
                    raise ResearchFactConflict(
                        "Forward路径纠错前业务行与已读PG事实不一致；事务已回滚"
                    )
                corrected = actual[:12] + desired + actual[20:]
                after.append(corrected)
                if corrected != actual:
                    updates.append(
                        desired
                        + (
                            target,
                            actual[1],
                            horizon,
                            version,
                            calendar,
                            endpoint,
                        )
                    )
            if updates:
                cursor.executemany(update_sql, updates)
            cursor.execute(
                select_scope,
                (
                    target,
                    horizon,
                    version,
                    calendar,
                    endpoint,
                    research_cutoff,
                    research_cutoff,
                ),
            )
            actual_after = [tuple(row) for row in cursor.fetchall()]
            if actual_after != after:
                raise ResearchFactConflict("Forward路径同事务读回不匹配；事务已回滚")
            updated_count += len(updates)
            verified_count += len(after)

    read_back_count = 0
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        for scope, expected_by_symbol in grouped.items():
            target, horizon, version, calendar, endpoint = scope
            cursor.execute(
                select_scope_read,
                (
                    target,
                    horizon,
                    version,
                    calendar,
                    endpoint,
                    research_cutoff,
                    research_cutoff,
                ),
            )
            stored = [tuple(row) for row in cursor.fetchall()]
            expected_after = []
            for actual in stored:
                _expected, desired = expected_by_symbol[actual[1]]
                expected_after.append(actual[:12] + desired + actual[20:])
            if stored != expected_after:
                raise ResearchFactConflict("Forward路径异事务只读读回不匹配")
            read_back_count += len(stored)
    return {
        "status": "CORRECTED_AND_READ_BACK",
        "target_scope_count": len(grouped),
        "verified_before_count": verified_count,
        "updated_path_row_count": updated_count,
        "unchanged_path_row_count": verified_count - updated_count,
        "independent_read_back_count": read_back_count,
        "transaction": "SERIALIZABLE_CORRECTION_PLUS_REPEATABLE_READ_READ_ONLY_READBACK",
    }


def write_research_rows(config, *, rules, paths, previous_session, events=None):
    """单T0整体事务；同键相同内容幂等，异值拒绝并回滚。"""
    if not rules:
        raise ValueError("不允许写入空规则来源池")
    first = rules[0]
    target, source_capture_id = first[0], first[2]
    if any((row[0], row[2]) != (target, source_capture_id) for row in rules + paths):
        raise ValueError("一个事务只能写入一个T0和来源捕获")
    rule_keys = [
        tuple(row[RULE_COLUMNS.index(name)] for name in RULE_KEY) for row in rules
    ]
    path_keys = [
        tuple(row[PATH_COLUMNS.index(name)] for name in PATH_KEY) for row in paths
    ]
    if len(rule_keys) != len(set(rule_keys)) or len(path_keys) != len(set(path_keys)):
        raise ValueError("同一研究批次内存在重复真实业务键")
    with _transaction(config, write=True) as (_, cursor, _):
        _require_multisession_key_migration(cursor)
        cursor.execute(
            """SELECT source_symbol,raw_source_capture_id FROM source_raw_daily
               WHERE session_date=%s""",
            (target,),
        )
        raw = cursor.fetchall()
        expected = {row[1] for row in rules}
        if (
            len(raw) != len(expected)
            or {row[0] for row in raw} != expected
            or any(row[1] is None for row in raw)
            or len(rules) != len(expected) * len(RULES)
            or {row[4] for row in rules} != RULES
            or not {row[1] for row in paths}.issubset(expected)
        ):
            raise ValueError("规则或路径与T0完整来源池和捕获身份不一致")
        count_rules = _insert_and_compare(
            cursor,
            "source_rule_evaluations",
            RULE_COLUMNS,
            rules,
            RULE_KEY,
            "target_session=%s",
            (target,),
        )
        count_paths = 0
        paths_by_horizon: dict[int, list[Sequence[Any]]] = {}
        for row in paths:
            paths_by_horizon.setdefault(int(row[3]), []).append(row)
        for horizon, horizon_rows in sorted(paths_by_horizon.items()):
            count_paths += _insert_and_compare(
                cursor,
                "source_forward_path_facts",
                PATH_COLUMNS,
                horizon_rows,
                PATH_KEY,
                "target_session=%s AND horizon=%s",
                (target, horizon),
            )
        if count_rules or count_paths:
            # 新捕获或路径重算会使旧统计失效，同事务清除下游统计与完成进度。
            _invalidate_downstream_stats_in_cursor(
                cursor,
                target_session=target,
                invalidate_s2_t1=bool(count_paths),
            )
        event_result = None
        if events is not None:
            from stock_a.storage.postgres.research_events import insert_source_events

            event_result = insert_source_events(
                cursor,
                rows=events,
                target=target,
                source_capture_id=source_capture_id,
                target_capture_id=source_capture_id,
                profile_revision=first[5],
                history_session_count=first[6],
                d1_history_session_count=first[7],
                event_window_start=first[8],
                calendar_revision=first[9],
            )
        from stock_a.storage.postgres.research_enrollment import insert_rule_enrollments

        enrollment = insert_rule_enrollments(
            cursor,
            target=target,
            before=previous_session,
            source_capture_id=source_capture_id,
            rule_version=first[4],
            profile_revision=first[5],
            history_count=first[6],
            d1_history_count=first[7],
            event_window_start=first[8],
            calendar_revision=first[9],
        )
    return {
        "inserted_rules": count_rules,
        "inserted_paths": count_paths,
        "events": event_result,
        "research_enrollments": enrollment,
    }


def read_research_rows(
    config,
    *,
    target,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    calendar_revision,
    rule,
    horizon,
    research_cutoff,
):
    """从已保存事实读取完整三态分母及共享路径，不扫描Raw。"""
    if horizon not in {1, 3, 5, 10, 20}:
        raise ValueError("后继期限必须为1、3、5、10或20")
    with _transaction(config, write=False) as (_, cursor, _):
        return _read_research_rows_cursor(
            cursor,
            target=target,
            source_capture_id=source_capture_id,
            profile_revision=profile_revision,
            history_session_count=history_session_count,
            d1_history_session_count=d1_history_session_count,
            event_window_start=event_window_start,
            calendar_revision=calendar_revision,
            rule=rule,
            horizon=horizon,
            research_cutoff=research_cutoff,
        )


def _read_research_rows_cursor(
    cursor,
    *,
    target,
    source_capture_id,
    profile_revision,
    history_session_count,
    d1_history_session_count,
    event_window_start,
    calendar_revision,
    rule,
    horizon,
    research_cutoff,
    symbol=None,
    offset=0,
    limit=None,
):
    """从已保存事实读取完整三态分母及共享路径，不扫描Raw。"""
    from stock_a.research.entry_context.sessions import _load_calendar
    from stock_a.research.source_forward_path import (
        forward_calendar_revision_compatible,
    )

    if horizon not in {1, 3, 5, 10, 20}:
        raise ValueError("后继期限必须为1、3、5、10或20")
    if offset < 0 or (limit is not None and not 1 <= limit <= 100):
        raise ValueError("规则读取分页参数无效")
    cursor.execute(
        """SELECT r.source_symbol, r.state, r.reasons, r.event_count,
                  p.endpoint_session, p.maturity, p.endpoint_quality,
                  p.endpoint_reason, p.endpoint_numerator, p.endpoint_denominator,
                  p.path_quality, p.path_reasons,
                  p.mfe_numerator, p.mfe_denominator, p.mae_numerator,
                  p.mae_denominator, p.mdd_numerator, p.mdd_denominator,
                  r.frozen_line_numerator, r.frozen_line_denominator,
                  r.frozen_line_source, r.source_capture_id,
                  p.calendar_revision,p.price_coordinate,p.scope
           FROM source_rule_evaluations r
           LEFT JOIN source_forward_path_facts p
             ON p.target_session=r.target_session
            AND p.source_symbol=r.source_symbol
            AND p.horizon=%s
            AND p.path_rule_version=%s
            AND p.price_coordinate='FORWARD_ADJUSTED'
            AND p.scope='NON_PIT_EXPLORATORY'
            AND p.endpoint_session <= %s
           WHERE r.target_session=%s
             AND r.rule_id=%s AND r.rule_version=%s AND r.profile_revision=%s
             AND r.history_session_count=%s AND r.d1_history_session_count=%s
             AND r.event_window_start=%s AND r.calendar_revision=%s
             AND (%s::text IS NULL OR r.source_symbol=%s)
           ORDER BY r.source_symbol LIMIT %s OFFSET %s""",
        (
            horizon,
            PATH_VERSION,
            research_cutoff,
            target,
            rule,
            RULE_VERSION,
            profile_revision,
            history_session_count,
            d1_history_session_count,
            event_window_start,
            calendar_revision,
            symbol,
            symbol,
            limit,
            offset,
        ),
    )
    rows = cursor.fetchall()
    if not rows and symbol is None and limit is None:
        raise ResearchFactNotFound("未找到精确输入版本的持久规则事实")
    official_revision_chain = _load_calendar(research_cutoff)[2]
    rows_by_symbol: dict[str, list[tuple[Any, ...]]] = {}
    for row in rows:
        rows_by_symbol.setdefault(row[0], []).append(row)
    selected_rows = []
    for source_symbol, candidates in sorted(rows_by_symbol.items()):
        compatible = [
            row
            for row in candidates
            if row[4] is not None
            and row[22] is not None
            and row[23] == "FORWARD_ADJUSTED"
            and row[24] == "NON_PIT_EXPLORATORY"
            and forward_calendar_revision_compatible(
                target,
                row[4],
                rule_revision=calendar_revision,
                path_revision=row[22],
                official_revision_chain=official_revision_chain,
            )
        ]
        if len(compatible) > 1:
            raise ResearchFactConflict("同一规则事实命中多个兼容Forward日历版本")
        if compatible:
            selected_rows.append(tuple(compatible[0][:22]))
            continue
        selected = list(candidates[0][:22])
        selected[4:18] = [None] * 14
        if any(row[4] is not None for row in candidates):
            selected[10] = "UNKNOWN"
            selected[11] = ["FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH"]
        selected_rows.append(tuple(selected))
    if len({row[0] for row in selected_rows}) != len(selected_rows):
        raise ResearchFactConflict("精确规则版本存在重复证券行")
    return selected_rows


def _read_board_members_cursor(cursor, *, industry_snapshot_id, concept_snapshot_id):
    """从调用方已打开的快照读取板块成员及快照身份。"""
    cursor.execute(
        """SELECT snapshot_id, source_kind, observed_at, research_mode
           FROM tdx_sector_source_snapshots WHERE snapshot_id IN (%s,%s)""",
        (industry_snapshot_id, concept_snapshot_id),
    )
    snapshots = cursor.fetchall()
    kinds = {row[0]: row[1] for row in snapshots}
    if kinds != {
        industry_snapshot_id: "TDX_TDXHY_CFG",
        concept_snapshot_id: "TDX_INFOHARBOR_BLOCK_DAT",
    } or any(row[3] != "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT" for row in snapshots):
        raise ValueError("行业或概念快照身份和后验模式不匹配")
    cursor.execute(
        """SELECT m.board_kind,m.board_native_id,c.board_name,m.source_symbol
           FROM tdx_sector_members m JOIN tdx_sector_catalog c
             ON c.id=m.catalog_id
           WHERE c.snapshot_id IN (%s,%s)
           ORDER BY m.board_kind,m.board_native_id,m.source_symbol""",
        (industry_snapshot_id, concept_snapshot_id),
    )
    return snapshots, cursor.fetchall()


def read_board_members(config, *, industry_snapshot_id, concept_snapshot_id):
    """读取明确指定的后验行业及概念成员，不推断历史有效期。"""
    with _transaction(config, write=False) as (_, cursor, _):
        return _read_board_members_cursor(
            cursor,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
        )


def read_research_keys(config, *, target, source_capture_id, calendar_revision):
    """列出与现行读取器规则版本和日历修订一致的冻结键。"""
    with _transaction(config, write=False) as (_, cursor, _):
        cursor.execute(
            """SELECT rule_id, history_session_count, d1_history_session_count,
                      event_window_start, profile_revision, count(*),
                      count(DISTINCT source_capture_id), min(source_capture_id)
               FROM source_rule_evaluations
               WHERE target_session=%s
                 AND rule_version=%s AND calendar_revision=%s
               GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5 LIMIT 101""",
            (target, RULE_VERSION, calendar_revision),
        )
        return [
            {
                "rule": row[0],
                "history": row[1],
                "d1_history": row[2],
                "window": row[3].isoformat(),
                "profile_revision": row[4],
                "count": row[5],
                "capture_count": row[6],
                "source_capture_id": int(row[7]) if row[6] == 1 else None,
                "rule_version": RULE_VERSION,
                "calendar_revision": calendar_revision,
            }
            for row in cursor.fetchall()
        ]
