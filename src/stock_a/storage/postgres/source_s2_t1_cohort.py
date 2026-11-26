"""S2固定T0组的T+1 typed统计写入与有界只读读取。"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation, localcontext
from typing import Any

from stock_a.domain.source_s2_t1_cohort import (
    COHORT_COLUMNS,
    COHORT_KEY,
    COHORT_VERSION,
    S2_STATES,
    T1_STATES,
    SourceS2T1CohortError,
    prepare_source_s2_t1_cohort_rows,
)
from stock_a.storage.postgres.source_raw import _transaction

_SEED_SCOPE_COLUMNS = (
    "producer_contract",
    "producer_version",
    "history_session_count",
    "calendar_id",
    "calendar_revision",
    "research_mode",
    "price_coordinate",
    "source_capture_id",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "source_count",
    "distinct_source_count",
)
_SCOPE_SELECT = """SELECT producer_contract, producer_version,
       history_session_count, calendar_id, calendar_revision, research_mode,
       price_coordinate, source_capture_id, source_time_quality,
       source_clock_role, source_available_at, count(*)::bigint,
       count(DISTINCT source_symbol)::bigint
    FROM source_pool_seed_day_facts
    WHERE target_session=%s AND source_capture_id=%s
      AND producer_version='SOURCE_POOL_SEED_CORE_PROFILE_R2'
    GROUP BY producer_contract, producer_version,
             history_session_count, calendar_id, calendar_revision,
             research_mode, price_coordinate, source_capture_id,
             source_time_quality, source_clock_role, source_available_at
    ORDER BY producer_version"""


class SourceS2T1CohortReadError(ValueError):
    """已保存S2分组统计的精确范围不能唯一读回。"""


def _integer_numeric(value: Any) -> int:
    try:
        numeric = Decimal(str(value))
    except InvalidOperation as error:
        raise SourceS2T1CohortError("T+1收益分子或分母不是有效数值") from error
    if not numeric.is_finite() or numeric != numeric.to_integral_value():
        raise SourceS2T1CohortError("T+1收益分子或分母必须是精确整数")
    return int(numeric)


def _seed_scope(cursor: Any, *, session: date, source_capture_id: int):
    cursor.execute(_SCOPE_SELECT, (session, source_capture_id))
    scopes = cursor.fetchall()
    if len(scopes) != 1:
        raise SourceS2T1CohortError("T0或T+1 Seed scope缺失或存在多个有效分区")
    scope = dict(zip(_SEED_SCOPE_COLUMNS, scopes[0], strict=True))
    scope["profile_revision"] = ""
    return scope


def persist_source_s2_t1_cohort_in_transaction(
    cursor: Any,
    *,
    target_session: date,
    endpoint_session: date,
    research_cutoff: date,
    source_capture_id: int,
    endpoint_source_capture_id: int,
    source_symbols: Sequence[str],
    path_rule_version: str,
    calendar_revision: str,
    insert_and_compare: Callable[..., int],
) -> dict[str, Any]:
    """在T+1路径写事务中闭合Seed、端点并保存12行S2统计。"""
    expected_count = len(source_symbols)
    if (
        not source_symbols
        or len(set(source_symbols)) != expected_count
        or type(target_session) is not date
        or type(endpoint_session) is not date
        or type(research_cutoff) is not date
        or not target_session < endpoint_session <= research_cutoff
    ):
        raise SourceS2T1CohortError("S2/T+1持久化范围或来源代码集合无效")

    t0_scope = _seed_scope(
        cursor,
        session=target_session,
        source_capture_id=source_capture_id,
    )
    endpoint_scope = _seed_scope(
        cursor,
        session=endpoint_session,
        source_capture_id=endpoint_source_capture_id,
    )
    for label, scope in (("T0", t0_scope), ("T+1", endpoint_scope)):
        if (
            scope["source_count"] != expected_count
            or scope["distinct_source_count"] != expected_count
            or scope["research_mode"] != "NON_PIT_RETROSPECTIVE"
            or scope["price_coordinate"] != "FORWARD_ADJUSTED"
            or scope["source_time_quality"] != "NON_PIT_OBSERVED"
            or scope["source_clock_role"] != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
            or scope["source_available_at"] is not None
        ):
            raise SourceS2T1CohortError(f"{label} Seed不是完整且合法的NON_PIT范围")

    cursor.execute(
        """SELECT t0.source_symbol, t0.s2_state, t1.s2_state, t1.s2_reasons,
                  p.endpoint_quality, p.endpoint_reason,
                  p.endpoint_numerator, p.endpoint_denominator,
                  p.price_coordinate, p.scope
           FROM source_pool_seed_day_facts t0
           JOIN source_pool_seed_day_facts t1
             ON t1.source_symbol=t0.source_symbol
            AND t1.target_session=%s
            AND t1.source_capture_id=%s
            AND t1.producer_contract=%s
            AND t1.producer_version=%s
            AND t1.history_session_count=%s
            AND t1.calendar_id=%s
            AND t1.calendar_revision=%s
            AND t1.research_mode=%s
            AND t1.price_coordinate=%s
           LEFT JOIN source_forward_path_facts p
             ON p.target_session=t0.target_session
            AND p.source_symbol=t0.source_symbol
            AND p.source_capture_id=t0.source_capture_id
            AND p.endpoint_session=%s
            AND p.horizon=1
            AND p.path_rule_version=%s
            AND p.calendar_revision=%s
            AND p.price_coordinate='FORWARD_ADJUSTED'
            AND p.scope='NON_PIT_EXPLORATORY'
           WHERE t0.target_session=%s
             AND t0.source_capture_id=%s
             AND t0.producer_contract=%s
             AND t0.producer_version=%s
             AND t0.history_session_count=%s
             AND t0.calendar_id=%s
             AND t0.calendar_revision=%s
             AND t0.research_mode=%s
             AND t0.price_coordinate=%s
           ORDER BY t0.source_symbol""",
        (
            endpoint_session,
            endpoint_source_capture_id,
            endpoint_scope["producer_contract"],
            endpoint_scope["producer_version"],
            endpoint_scope["history_session_count"],
            endpoint_scope["calendar_id"],
            endpoint_scope["calendar_revision"],
            endpoint_scope["research_mode"],
            endpoint_scope["price_coordinate"],
            endpoint_session,
            path_rule_version,
            calendar_revision,
            target_session,
            source_capture_id,
            t0_scope["producer_contract"],
            t0_scope["producer_version"],
            t0_scope["history_session_count"],
            t0_scope["calendar_id"],
            t0_scope["calendar_revision"],
            t0_scope["research_mode"],
            t0_scope["price_coordinate"],
        ),
    )
    joined_rows = cursor.fetchall()
    if len(joined_rows) != expected_count or {row[0] for row in joined_rows} != set(
        source_symbols
    ):
        raise SourceS2T1CohortError("T0 S2、T+1 S2与T+1价格事实未逐代码闭合")

    outcomes = []
    for row in joined_rows:
        (
            symbol,
            t0_state,
            t1_state,
            t1_reasons,
            endpoint_quality,
            endpoint_reason,
            numerator,
            denominator,
            price_coordinate,
            scope,
        ) = row
        if scope != "NON_PIT_EXPLORATORY" or price_coordinate != "FORWARD_ADJUSTED":
            raise SourceS2T1CohortError("T+1路径不是NON_PIT QFQ事实")
        outcomes.append(
            {
                "source_symbol": symbol,
                "t0_s2_state": t0_state,
                "t1_s2_state": t1_state,
                "t1_s2_reasons": list(t1_reasons or ()),
                "endpoint_quality": endpoint_quality,
                "endpoint_reason": endpoint_reason,
                "endpoint_numerator": _integer_numeric(numerator)
                if numerator is not None
                else None,
                "endpoint_denominator": _integer_numeric(denominator)
                if denominator is not None
                else None,
                "price_coordinate": price_coordinate,
            }
        )

    rows = prepare_source_s2_t1_cohort_rows(
        target_session=target_session,
        endpoint_session=endpoint_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        endpoint_source_capture_id=endpoint_source_capture_id,
        t0_scope=t0_scope,
        endpoint_scope=endpoint_scope,
        calendar_revision=calendar_revision,
        path_rule_version=path_rule_version,
        outcomes=outcomes,
        expected_source_count=expected_count,
    )
    selection = "target_session=%s AND endpoint_session=%s AND research_cutoff=%s"
    args = (
        target_session,
        endpoint_session,
        research_cutoff,
    )
    inserted = insert_and_compare(
        cursor,
        "source_s2_t1_cohort_stats",
        COHORT_COLUMNS,
        rows,
        COHORT_KEY,
        selection,
        args,
    )
    cursor.execute(
        """SELECT count(*)::bigint,
                  count(DISTINCT (t0_s2_state,t1_s2_state))::bigint
           FROM source_s2_t1_cohort_stats
           WHERE target_session=%s AND endpoint_session=%s
             AND research_cutoff=%s""",
        args,
    )
    if cursor.fetchone() != (12, 12):
        raise SourceS2T1CohortError("PG写入后S2/T+1三态统计矩阵未闭合")
    return {
        "inserted_rows": inserted,
        "expected_rows": rows,
        "source_pool_count": expected_count,
        "t0_profile_revision": t0_scope["profile_revision"],
        "endpoint_profile_revision": endpoint_scope["profile_revision"],
        "calendar_revision": calendar_revision,
        "cohort_version": COHORT_VERSION,
    }


def _pair_payload(record: Mapping[str, Any], prefix: str) -> dict[str, str] | None:
    numerator = record[f"{prefix}_numerator"]
    denominator = record[f"{prefix}_denominator"]
    if numerator is None or denominator is None:
        return None
    numerator_text = str(numerator)
    denominator_text = str(denominator)
    try:
        with localcontext() as context:
            context.prec = 40
            percent = (
                Decimal(numerator_text) / Decimal(denominator_text) * Decimal(100)
            ).quantize(Decimal("0.0001"))
    except (InvalidOperation, ZeroDivisionError) as error:
        raise SourceS2T1CohortReadError("PG分组收益有理数无法显示") from error
    return {
        "numerator": numerator_text,
        "denominator": denominator_text,
        "percent": format(percent, ".4f"),
    }


def _summary_payload(rows: Sequence[tuple[Any, ...]]) -> dict[str, Any]:
    if len(rows) != len(S2_STATES) * len(T1_STATES):
        return {
            "status": "INCOMPLETE",
            "reason_codes": ["S2_T1_STATS_ROWCOUNT_MISMATCH"],
        }
    records = [dict(zip(COHORT_COLUMNS, row, strict=True)) for row in rows]
    dimensions = {(item["t0_s2_state"], item["t1_s2_state"]) for item in records}
    expected_dimensions = {
        (t0_state, t1_state) for t0_state in S2_STATES for t1_state in T1_STATES
    }
    if dimensions != expected_dimensions:
        return {
            "status": "INCOMPLETE",
            "reason_codes": ["S2_T1_STATS_DIMENSIONS_INCOMPLETE"],
        }
    base_context = tuple(records[0][column] for column in COHORT_COLUMNS[:17])
    if any(
        tuple(record[column] for column in COHORT_COLUMNS[:17]) != base_context
        for record in records[1:]
    ):
        return {"status": "AMBIGUOUS", "reason_codes": ["S2_T1_STATS_CONTEXT_MIXED"]}
    if any(
        item["endpoint_observed_count"]
        + item["endpoint_unknown_count"]
        + item["endpoint_other_count"]
        != item["member_count"]
        or item["positive_count"] + item["non_positive_count"]
        != item["endpoint_observed_count"]
        for item in records
    ):
        return {
            "status": "INCOMPLETE",
            "reason_codes": ["S2_T1_STATS_DENOMINATOR_MISMATCH"],
        }

    by_key = {(item["t0_s2_state"], item["t1_s2_state"]): item for item in records}
    pool_count = records[0]["source_pool_count"]
    if (
        any(item["source_pool_count"] != pool_count for item in records)
        or sum(by_key[(state, "ALL")]["member_count"] for state in S2_STATES)
        != pool_count
        or any(
            sum(by_key[(state, part)]["member_count"] for part in S2_STATES)
            != by_key[(state, "ALL")]["member_count"]
            for state in S2_STATES
        )
    ):
        return {
            "status": "INCOMPLETE",
            "reason_codes": ["S2_T1_STATS_GROUPS_NOT_CLOSED"],
        }

    def expose(record: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "t0_s2_state": record["t0_s2_state"],
            "t1_s2_state": record["t1_s2_state"],
            "source_pool_count": record["source_pool_count"],
            "member_count": record["member_count"],
            "endpoint_observed_count": record["endpoint_observed_count"],
            "endpoint_unknown_count": record["endpoint_unknown_count"],
            "endpoint_other_count": record["endpoint_other_count"],
            "coverage_numerator": record["endpoint_observed_count"],
            "coverage_denominator": record["member_count"],
            "mean_return": _pair_payload(record, "mean_return"),
            "median_return": _pair_payload(record, "median_return"),
            "min_return": _pair_payload(record, "min_return"),
            "max_return": _pair_payload(record, "max_return"),
            "positive_count": record["positive_count"],
            "non_positive_count": record["non_positive_count"],
            "failure_reason_subject_count": record["failure_reason_subject_count"],
            "failure_reasons": [
                {"code": code, "stock_count": count}
                for code, count in zip(
                    record["failure_reason_codes"],
                    record["failure_reason_counts"],
                    strict=True,
                )
            ],
            "endpoint_unknown_reasons": [
                {"code": code, "stock_count": count}
                for code, count in zip(
                    record["endpoint_unknown_reason_codes"],
                    record["endpoint_unknown_reason_counts"],
                    strict=True,
                )
            ],
        }

    first = records[0]
    return {
        "status": "AVAILABLE",
        "target_session": first["target_session"].isoformat(),
        "endpoint_session": first["endpoint_session"].isoformat(),
        "research_cutoff": first["research_cutoff"].isoformat(),
        "source_capture_id": first["source_capture_id"],
        "endpoint_source_capture_id": first["endpoint_source_capture_id"],
        "t0_profile_revision": first["t0_profile_revision"],
        "endpoint_profile_revision": first["endpoint_profile_revision"],
        "profile_revisions": [
            first["t0_profile_revision"],
            first["endpoint_profile_revision"],
        ],
        "calendar_id": first["calendar_id"],
        "calendar_revision": first["calendar_revision"],
        "path_rule_version": first["path_rule_version"],
        "cohort_version": first["cohort_version"],
        "producer_contract": first["producer_contract"],
        "producer_version": first["producer_version"],
        "history_session_counts": [
            first["t0_history_session_count"],
            first["endpoint_history_session_count"],
        ],
        "research_mode": first["research_mode"],
        "price_coordinate": first["price_coordinate"],
        "source_pool_count": pool_count,
        "groups": [expose(by_key[(state, "ALL")]) for state in S2_STATES],
        "t0_true_t1_states": [expose(by_key[("TRUE", state)]) for state in S2_STATES],
        "group_closure": "CLOSED",
        "interpretation": "T0_FIXED_GROUP_T1_STATE_IS_POSTERIOR_ONLY",
    }


def _capture_session_count(
    cursor: Any,
    *,
    session: date,
    source_capture_id: int,
    research_cutoff: date,
) -> int | None:
    """返回仅在捕获请求、日线和双坐标分母都闭合时的逐日证券数。"""
    cursor.execute(
        """SELECT provider,range_start_session,range_end_session,
                  price_coordinates,source_time_quality,source_clock_role,
                  source_available_at
           FROM source_captures WHERE capture_id=%s""",
        (source_capture_id,),
    )
    capture = cursor.fetchone()
    if (
        capture is None
        or capture[0] != "BAOSTOCK"
        or capture[1] is None
        or capture[2] is None
        or not capture[1] <= session <= capture[2] <= research_cutoff
        or set(capture[3] or ()) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or capture[4] != "NON_PIT_OBSERVED"
        or capture[5] != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
        or capture[6] is not None
    ):
        return None
    cursor.execute(
        """WITH request_codes AS (
               SELECT source_code,
                      count(*)=2 AND count(DISTINCT adjustment_flag)=2
                        AND count(DISTINCT eligible_start_session)=1
                        AND count(DISTINCT eligible_end_session)=1
                        AND bool_and(
                            request_status IN ('COMPLETED','NO_DATA')
                            AND (request_status='NO_DATA' OR
                                 completed_through_session>=range_end_session)
                        ) AS complete,
                      min(eligible_start_session) AS eligible_start,
                      max(eligible_end_session) AS eligible_end
               FROM source_capture_requests
               WHERE capture_id=%s AND range_end_session<=%s
               GROUP BY source_code
           )
           SELECT count(*) FILTER (
                      WHERE %s BETWEEN eligible_start AND eligible_end
                  )::bigint,
                  count(*) FILTER (
                      WHERE %s BETWEEN eligible_start AND eligible_end AND complete
                  )::bigint
           FROM request_codes""",
        (source_capture_id, research_cutoff, session, session),
    )
    expected_count, complete_count = (int(value or 0) for value in cursor.fetchone())
    cursor.execute(
        """SELECT count(*)::bigint,count(DISTINCT source_symbol)::bigint,
                  count(*) FILTER (WHERE price_quality IS NOT NULL)::bigint,
                  count(*) FILTER (WHERE qfq_price_quality IS NOT NULL)::bigint,
                  count(*) FILTER (WHERE board IN ('MAIN_BOARD', 'GEM'))::bigint
           FROM source_raw_daily
           WHERE session_date=%s AND source_capture_id=%s AND session_date<=%s""",
        (session, source_capture_id, research_cutoff),
    )
    row_count, symbol_count, raw_count, qfq_count, research_count = (
        int(value or 0) for value in cursor.fetchone()
    )
    if (
        expected_count <= 0
        or expected_count != complete_count
        or row_count != expected_count
        or symbol_count != row_count
        or raw_count != row_count
        or qfq_count != row_count
    ):
        return None
    return research_count


def _read_exact_scope(
    config: Mapping[str, Any],
    *,
    target_session: date,
    endpoint_session: date,
    research_cutoff: date,
    source_capture_id: int,
    endpoint_source_capture_id: int | None,
    expected_rows: Sequence[tuple[Any, ...]] | None = None,
) -> dict[str, Any]:
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        target_capture_count = _capture_session_count(
            cursor,
            session=target_session,
            source_capture_id=source_capture_id,
            research_cutoff=research_cutoff,
        )
        if target_capture_count is None:
            return {
                "status": "INCOMPLETE",
                "reason_codes": ["T0_CAPTURE_PARTITION_NOT_CLOSED"],
            }
        if endpoint_source_capture_id is None:
            cursor.execute(
                """SELECT DISTINCT c.capture_id
                   FROM source_raw_daily r
                   JOIN source_captures c ON c.capture_id=r.source_capture_id
                   WHERE r.session_date=%s AND c.provider='BAOSTOCK'
                     AND c.range_start_session<=%s AND c.range_end_session<=%s
                     AND c.price_coordinates @> ARRAY[
                         'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                     ]::text[]
                   ORDER BY c.capture_id""",
                (endpoint_session, endpoint_session, research_cutoff),
            )
            endpoint_ids = [int(row[0]) for row in cursor.fetchall()]
            if len(endpoint_ids) != 1:
                return {
                    "status": "NOT_COMPUTED" if not endpoint_ids else "AMBIGUOUS",
                    "target_session": target_session.isoformat(),
                    "endpoint_session": endpoint_session.isoformat(),
                    "research_cutoff": research_cutoff.isoformat(),
                    "reason_codes": ["T1_CAPTURE_NOT_UNIQUE"],
                }
            endpoint_source_capture_id = endpoint_ids[0]
        endpoint_capture_count = _capture_session_count(
            cursor,
            session=endpoint_session,
            source_capture_id=endpoint_source_capture_id,
            research_cutoff=research_cutoff,
        )
        if endpoint_capture_count is None:
            return {
                "status": "INCOMPLETE",
                "reason_codes": ["T1_CAPTURE_PARTITION_NOT_CLOSED"],
            }
        columns = ", ".join(f'"{column}"' for column in COHORT_COLUMNS)
        cursor.execute(
            f"""SELECT {columns} FROM source_s2_t1_cohort_stats
                WHERE target_session=%s AND endpoint_session=%s
                  AND research_cutoff=%s
                ORDER BY t0_s2_state, t1_s2_state""",
            (
                target_session,
                endpoint_session,
                research_cutoff,
            ),
        )
        rows = tuple(cursor.fetchall())
        forward_distinct_symbols: int | None = None
        if rows:
            cursor.execute(
                """SELECT count(DISTINCT source_symbol)
                   FROM source_forward_path_facts
                   WHERE target_session=%s
                     AND horizon=1
                     AND path_rule_version=%s
                     AND calendar_revision=%s""",
                (
                    target_session,
                    rows[0][COHORT_COLUMNS.index("path_rule_version")],
                    rows[0][COHORT_COLUMNS.index("calendar_revision")],
                ),
            )
            forward_distinct_symbols = int(cursor.fetchone()[0])
    if not rows:
        return {
            "status": "NOT_COMPUTED",
            "target_session": target_session.isoformat(),
            "endpoint_session": endpoint_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "reason_codes": ["S2_T1_COHORT_STATS_NOT_SAVED"],
        }
    if (
        expected_rows is not None
        and tuple(
            sorted(
                expected_rows,
                key=lambda row: (
                    row[COHORT_COLUMNS.index("t0_s2_state")],
                    row[COHORT_COLUMNS.index("t1_s2_state")],
                ),
            )
        )
        != rows
    ):
        raise SourceS2T1CohortReadError("S2/T+1统计提交后异事务读回与事务输入不一致")
    payload = _summary_payload(rows)
    if payload.get("status") == "AVAILABLE":
        if forward_distinct_symbols is None or forward_distinct_symbols != payload.get(
            "source_pool_count"
        ):
            return {
                "status": "INCOMPLETE",
                "reason_codes": ["T1_FORWARD_PATH_MISSING_OR_INCOMPLETE"],
            }
        if payload.get("source_pool_count") != endpoint_capture_count or (
            payload.get("source_pool_count") != target_capture_count
        ):
            return {
                "status": "INCOMPLETE",
                "reason_codes": ["CAPTURE_AND_STATS_DENOMINATOR_MISMATCH"],
            }
        for key, value in (
            ("target_session", target_session.isoformat()),
            ("endpoint_session", endpoint_session.isoformat()),
            ("research_cutoff", research_cutoff.isoformat()),
        ):
            if payload.get(key) != value:
                raise SourceS2T1CohortReadError("S2/T+1统计读回与请求业务键不一致")
    payload["rows_read_back"] = len(rows)
    return payload


def read_source_s2_t1_cohort_scope(
    config: Mapping[str, Any],
    *,
    target_session: date,
    endpoint_session: date,
    research_cutoff: date,
    source_capture_id: int,
    endpoint_source_capture_id: int,
    expected_rows: Sequence[tuple[Any, ...]] | None = None,
) -> dict[str, Any]:
    """在新只读事务中读取已提交的S2统计并核验可比较分母闭合。"""
    return _read_exact_scope(
        config,
        target_session=target_session,
        endpoint_session=endpoint_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        endpoint_source_capture_id=endpoint_source_capture_id,
        expected_rows=expected_rows,
    )


def read_source_s2_t1_cohort_for_focus(
    config: Mapping[str, Any],
    *,
    target_session: date,
    endpoint_session: date | None,
    research_cutoff: date,
    source_capture_id: int,
    endpoint_source_capture_id: int | None = None,
) -> dict[str, Any]:
    """Focus只读精确T0/T+1摘要；未到期或缺少数据时保留状态。"""
    if endpoint_session is None:
        return {
            "status": "NOT_DUE",
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "reason_codes": ["WAITING_FOR_NEXT_STORED_SESSION"],
        }
    if (
        type(target_session) is not date
        or type(endpoint_session) is not date
        or type(research_cutoff) is not date
        or not target_session < endpoint_session <= research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise SourceS2T1CohortReadError("Focus S2/T+1读取键无效")
    result = _read_exact_scope(
        config,
        target_session=target_session,
        endpoint_session=endpoint_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        endpoint_source_capture_id=endpoint_source_capture_id,
    )
    return result
