"""RPS位次变化事实的离线保存及Focus/Cohort有限范围读取。"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from fractions import Fraction
from typing import Any, Sequence

from stock_a.domain.source_rps_pulse import classify_rank_change

from . import source_raw


_CALCULATION_VERSION = "RPS_PULSE_T0_GROUPED_FORWARD_R1"
_SEED_PRODUCER_VERSION = "SOURCE_POOL_SEED_CORE_PROFILE_R2"
_MAX_RELATION_ROWS = 20000
_MAX_OUTCOME_ROWS = 60000
_HORIZONS = (1, 3, 5)
_SCENARIO_SQL = {
    "DOUBLE_IMPROVED_PULSE_FALSE": (
        "c.rps5_change_state='IMPROVED' AND c.rps20_change_state='IMPROVED' "
        "AND o.pulse_status='AVAILABLE' AND o.pulse_state='FALSE'"
    ),
    "DOUBLE_IMPROVED_PULSE_TRUE": (
        "c.rps5_change_state='IMPROVED' AND c.rps20_change_state='IMPROVED' "
        "AND o.pulse_status='AVAILABLE' AND o.pulse_state='TRUE'"
    ),
    "RPS5_UP_RPS20_DOWN_PULSE_TRUE": (
        "c.rps5_change_state='IMPROVED' AND c.rps20_change_state='WORSENED' "
        "AND o.pulse_status='AVAILABLE' AND o.pulse_state='TRUE'"
    ),
    "DOUBLE_WORSENED_PULSE_TRUE": (
        "c.rps5_change_state='WORSENED' AND c.rps20_change_state='WORSENED' "
        "AND o.pulse_status='AVAILABLE' AND o.pulse_state='TRUE'"
    ),
    "QUALITY_UNKNOWN": (
        "(c.rps5_change_state IS NULL OR c.rps5_change_state NOT IN "
        "('IMPROVED','WORSENED','UNCHANGED')) OR "
        "(c.rps20_change_state IS NULL OR c.rps20_change_state NOT IN "
        "('IMPROVED','WORSENED','UNCHANGED')) OR "
        "o.pulse_status<>'AVAILABLE' OR o.pulse_state NOT IN ('TRUE','FALSE')"
    ),
}
_SCENARIO_LABELS = {
    "DOUBLE_IMPROVED_PULSE_FALSE": "RPS5/RPS20双前移 · PULSE FALSE",
    "DOUBLE_IMPROVED_PULSE_TRUE": "RPS5/RPS20双前移 · PULSE TRUE",
    "RPS5_UP_RPS20_DOWN_PULSE_TRUE": "RPS5前移、RPS20后退 · PULSE TRUE",
    "DOUBLE_WORSENED_PULSE_TRUE": "RPS5/RPS20双后退 · PULSE TRUE",
    "QUALITY_UNKNOWN": "不可比 / UNKNOWN / NOT_COMPUTED 质量分母",
}

_INSERT_COLUMNS = (
    "source_t0_fact_id",
    "target_session",
    "previous_session",
    "source_capture_id",
    "previous_source_capture_id",
    "enrollment_id",
    "source_symbol",
    "board_kind",
    "board_native_id",
    "membership_snapshot_id",
    "catalog_id",
    "rule_fact_id",
    "rule_id",
    "rule_version",
    "rule_state",
    "calculation_version",
    "source_pool_status",
    "source_pool_reason_codes",
    "previous_seed_fact_id",
    "rps5_change_state",
    "rps5_reason_codes",
    "rps5_previous_rank_first",
    "rps5_previous_rank_last",
    "rps5_previous_valid_member_count",
    "rps20_change_state",
    "rps20_reason_codes",
    "rps20_previous_rank_first",
    "rps20_previous_rank_last",
    "rps20_previous_valid_member_count",
    "membership_mode",
    "research_mode",
    "price_coordinate",
)


def _seed_history_windows_are_contiguous(
    *,
    previous_count: int,
    previous_start: date,
    previous_end: date,
    current_count: int,
    current_start: date,
    current_end: date,
    previous_session: date,
    target_session: date,
    official_sessions: Sequence[date],
) -> bool:
    """校验相邻官方会话的画像窗：先扩容，再在上限滚动。"""
    if (
        type(previous_count) is not int
        or type(current_count) is not int
        or previous_count < 1
        or current_count < 1
        or any(
            type(value) is not date
            for value in (
                previous_start,
                previous_end,
                current_start,
                current_end,
                previous_session,
                target_session,
            )
        )
        or not isinstance(official_sessions, (list, tuple))
        or any(type(value) is not date for value in official_sessions)
    ):
        return False
    if len(set(official_sessions)) != len(official_sessions) or any(
        left >= right for left, right in zip(official_sessions, official_sessions[1:])
    ):
        return False
    positions = {session: index for index, session in enumerate(official_sessions)}
    required = (
        previous_start,
        previous_end,
        current_start,
        current_end,
        previous_session,
        target_session,
    )
    if any(session not in positions for session in required):
        return False
    previous_index = positions[previous_session]
    target_index = positions[target_session]
    previous_start_index = positions[previous_start]
    previous_end_index = positions[previous_end]
    current_start_index = positions[current_start]
    current_end_index = positions[current_end]
    if (
        target_index != previous_index + 1
        or previous_end != previous_session
        or current_end != target_session
        or previous_start_index > previous_end_index
        or current_start_index > current_end_index
        or previous_end_index - previous_start_index + 1 != previous_count
        or current_end_index - current_start_index + 1 != current_count
    ):
        return False
    grew_with_fixed_start = (
        current_count == previous_count + 1
        and current_start_index == previous_start_index
    )
    rolled_at_same_size = (
        current_count == previous_count
        and current_start_index == previous_start_index + 1
    )
    return grew_with_fixed_start or rolled_at_same_size


def _select_adjacent_seed_scope_pair(
    previous_scopes: Sequence[Sequence[Any]],
    target_scopes: Sequence[Sequence[Any]],
    *,
    previous_session: date,
    target_session: date,
    official_sessions: Sequence[date],
    expected_source_count: int,
    history_session_count: int | None = None,
    profile_revision: str | None = None,
) -> tuple[str, Sequence[Any] | None, Sequence[Any] | None, str | None]:
    """选择唯一闭合Seed前后态；优先等长窗口，否则接受首次扩容。"""

    def complete(scope: Sequence[Any]) -> bool:
        return (
            len(scope) >= 18
            and scope[4] == _SEED_PRODUCER_VERSION
            and int(scope[14]) == expected_source_count
            and int(scope[15]) == expected_source_count
            and int(scope[16]) == expected_source_count
            and int(scope[17]) == expected_source_count
        )

    target_matches = [
        scope
        for scope in target_scopes
        if len(scope) >= 18
        and scope[4] == _SEED_PRODUCER_VERSION
        and (history_session_count is None or scope[5] == history_session_count)
    ]
    current_candidates = [scope for scope in target_matches if complete(scope)]
    if not current_candidates:
        reason = (
            "ADJACENT_SEED_SCOPE_INCOMPLETE"
            if target_matches
            else "ADJACENT_SEED_SCOPE_NOT_COMPUTED"
        )
        return "NOT_COMPUTED", None, None, reason

    if not previous_scopes:
        return (
            "NOT_COMPUTED",
            None,
            None,
            "ADJACENT_SEED_SCOPE_NOT_COMPUTED",
        )
    previous_candidates = [scope for scope in previous_scopes if complete(scope)]
    if not previous_candidates:
        return (
            "NOT_COMPUTED",
            None,
            None,
            "ADJACENT_SEED_SCOPE_INCOMPLETE"
            if previous_scopes
            else "ADJACENT_SEED_SCOPE_NOT_COMPUTED",
        )

    same_fields = (1, 3, 4, 6, 7, 8, 9, 12, 13)
    pairs: list[tuple[Sequence[Any], Sequence[Any]]] = []
    had_same_context = False
    had_invalid_window = False
    for current in current_candidates:
        same_context = [
            before
            for before in previous_candidates
            if all(before[index] == current[index] for index in same_fields)
        ]
        had_same_context = had_same_context or bool(same_context)
        valid_same_n = [
            before
            for before in same_context
            if before[5] == current[5]
            and _seed_history_windows_are_contiguous(
                previous_count=before[5],
                previous_start=before[10],
                previous_end=before[11],
                current_count=current[5],
                current_start=current[10],
                current_end=current[11],
                previous_session=previous_session,
                target_session=target_session,
                official_sessions=official_sessions,
            )
        ]
        if valid_same_n:
            pairs.extend((before, current) for before in valid_same_n)
            continue
        valid_growth = [
            before
            for before in same_context
            if current[5] == before[5] + 1
            and _seed_history_windows_are_contiguous(
                previous_count=before[5],
                previous_start=before[10],
                previous_end=before[11],
                current_count=current[5],
                current_start=current[10],
                current_end=current[11],
                previous_session=previous_session,
                target_session=target_session,
                official_sessions=official_sessions,
            )
        ]
        if valid_growth:
            pairs.extend((before, current) for before in valid_growth)
        elif same_context:
            had_invalid_window = True

    if len(pairs) == 1:
        before, current = pairs[0]
        return "AVAILABLE", before, current, None
    if len(pairs) > 1:
        return (
            "NOT_COMPARABLE",
            None,
            None,
            "ADJACENT_SEED_SCOPE_AMBIGUOUS",
        )
    if had_same_context and had_invalid_window:
        return (
            "NOT_COMPARABLE",
            None,
            None,
            "HISTORY_WINDOW_NOT_CONTIGUOUS",
        )
    return "NOT_COMPARABLE", None, None, "ADJACENT_SEED_SCOPE_CHANGED"


def _read_latest_mapped_t0(
    cursor: Any, *, target: date, source_capture_id: int
) -> list[tuple[Any, ...]]:
    cursor.execute(
        f"""WITH latest AS (
               SELECT DISTINCT ON (o.enrollment_id,o.board_context_key) o.*
               FROM source_t0_early_observation_facts o
               WHERE o.target_session=%s AND o.source_capture_id=%s
               ORDER BY o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           )
           SELECT o.id,o.target_session,o.source_capture_id,
                  o.enrollment_id,o.source_symbol,o.board_kind,o.board_native_id,
                  o.membership_snapshot_id,o.catalog_id,o.rule_fact_id,
                  COALESCE(r.rule_id,'UNSCOPED'),
                  COALESCE(r.rule_version,'UNSCOPED'),
                  COALESCE(r.state,'NOT_APPLICABLE'),
                  o.pulse_status,o.pulse_state,o.membership_mode,
                  o.research_mode,o.price_coordinate,
                  o.rps5_quality,o.rps5_reason,o.rps5_rank_first,
                  o.rps5_rank_last,o.rps5_valid_member_count,
                  o.rps20_quality,o.rps20_reason,o.rps20_rank_first,
                  o.rps20_rank_last,o.rps20_valid_member_count
           FROM latest o
           LEFT JOIN source_rule_evaluations r ON r.id=o.rule_fact_id
           WHERE o.membership_status='MAPPED'
             AND o.board_kind IN ('INDUSTRY','CONCEPT')
             AND o.membership_snapshot_id IS NOT NULL
             AND o.catalog_id IS NOT NULL AND o.board_native_id IS NOT NULL
           ORDER BY o.source_capture_id,o.id""",
        (target, source_capture_id),
    )
    rows = cursor.fetchall()
    if len(rows) > _MAX_RELATION_ROWS:
        raise ValueError("单日登记×板块关系超过离线生产上限")
    return rows


def _read_source_context(
    cursor: Any,
    *,
    target: date,
    previous: date,
    source_capture_id: int,
    research_cutoff: date,
    official_sessions: Sequence[date],
    history_session_count: int | None = None,
    profile_revision: str | None = None,
):
    """核对单个覆盖多个会话的BaoStock paired capture及逐日请求分母。"""
    cursor.execute(
        """SELECT provider,range_start_session,range_end_session,
                  price_coordinates,source_time_quality,source_clock_role,
                  source_available_at
           FROM source_captures
           WHERE capture_id=%s AND range_end_session<=%s""",
        (source_capture_id, research_cutoff),
    )
    reasons: list[str] = []
    capture = cursor.fetchone()
    if capture is None:
        return {
            "previous_source_capture_id": None,
            "status": "NOT_COMPUTED",
            "reason_codes": ["PAIRED_CAPTURE_NOT_FOUND"],
            "previous_scope": None,
            "current_scope": None,
        }
    (
        provider,
        range_start,
        range_end,
        price_coordinates,
        source_time_quality,
        source_clock_role,
        source_available_at,
    ) = capture
    if (
        provider != "BAOSTOCK"
        or range_start is None
        or range_end is None
        or range_start > previous
        or range_end < target
        or range_end > research_cutoff
        or set(price_coordinates or ()) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or source_available_at is not None
    ):
        reasons.append("PAIRED_CAPTURE_SCOPE_INVALID")
    if source_clock_role != "BAOSTOCK_QUERY_CAPTURED_NON_PIT":
        reasons.append("SOURCE_CAPTURE_CLOCK_ROLE_INVALID")
    if source_time_quality in {"UNKNOWN", "NOT_COMPUTED"}:
        reasons.append("SOURCE_TIME_QUALITY_UNKNOWN")

    # 60-session capture identity spans both endpoints; requests and eligible
    # windows determine the per-session denominator instead of target_session
    # on the capture header.
    cursor.execute(
        """SELECT source_code,adjustment_flag,eligible_start_session,
                  eligible_end_session,request_status,completed_through_session
           FROM source_capture_requests
           WHERE capture_id=%s AND range_end_session<=%s
           ORDER BY source_code,adjustment_flag""",
        (source_capture_id, research_cutoff),
    )
    request_rows = cursor.fetchall()
    requests_by_code: dict[str, dict[str, tuple]] = defaultdict(dict)
    duplicate_request_keys: set[str] = set()
    for row in request_rows:
        code = str(row[0]).upper()
        flag = str(row[1])
        if flag in requests_by_code[code]:
            duplicate_request_keys.add(code)
        requests_by_code[code][flag] = row

    def eligible_codes(session: date) -> set[str]:
        result: set[str] = set()
        for code, coordinate_rows in requests_by_code.items():
            rows = [row for flag, row in coordinate_rows.items() if flag in {"2", "3"}]
            if len(rows) != 2 or any(
                row[2] is None or row[3] is None or row[2] > session or row[3] < session
                for row in rows
            ):
                continue
            result.add(code)
        return result

    def incomplete_request_codes(session: date) -> set[str]:
        incomplete = set(duplicate_request_keys)
        for code in eligible_codes(session):
            coordinate_rows = requests_by_code[code]
            if set(coordinate_rows) != {"2", "3"}:
                incomplete.add(code)
                continue
            if any(
                row[4] not in {"COMPLETED", "NO_DATA"}
                or row[4] == "COMPLETED"
                and (row[5] is None or row[5] < row[3])
                for row in coordinate_rows.values()
            ):
                incomplete.add(code)
        return incomplete

    expected_symbols_by_day = {
        session: eligible_codes(session) for session in (previous, target)
    }
    for session, codes in expected_symbols_by_day.items():
        if not codes:
            reasons.append(
                "PREVIOUS_REQUEST_ROSTER_EMPTY"
                if session == previous
                else "T0_REQUEST_ROSTER_EMPTY"
            )
        if incomplete_request_codes(session):
            reasons.append(
                "PREVIOUS_RAW_REQUESTS_INCOMPLETE"
                if session == previous
                else "T0_RAW_REQUESTS_INCOMPLETE"
            )

    raw_by_key: dict[date, tuple] = {}
    if any(expected_symbols_by_day.values()):
        cursor.execute(
            """SELECT session_date,count(*)::integer,
                      count(DISTINCT source_symbol)::integer,
                      array_agg(source_symbol ORDER BY source_symbol)
               FROM source_raw_daily
               WHERE source_capture_id=%s AND session_date=ANY(%s::date[])
               GROUP BY session_date""",
            (source_capture_id, [previous, target]),
        )
        raw_by_key = {row[0]: row[1:] for row in cursor.fetchall()}

    source_symbols_by_day: dict[date, tuple[str, ...]] = {}
    for session, expected_symbols in expected_symbols_by_day.items():
        raw = raw_by_key.get(session)
        if (
            raw is None
            or int(raw[0]) != len(expected_symbols)
            or int(raw[1]) != len(expected_symbols)
            or set(raw[2]) != expected_symbols
        ):
            reasons.append(
                "T0_RAW_REQUEST_PARTITION_NOT_CLOSED"
                if session == target
                else "PREVIOUS_RAW_REQUEST_PARTITION_NOT_CLOSED"
            )
        else:
            source_symbols_by_day[session] = tuple(raw[2])

    scopes_by_day: dict[date, list[tuple]] = defaultdict(list)
    if not reasons and source_symbols_by_day:
        cursor.execute(
            """SELECT s.target_session,s.source_capture_id,
                      ''::text,s.producer_contract,s.producer_version,
                      s.history_session_count,s.calendar_id,s.calendar_revision,
                      s.research_mode,s.price_coordinate,s.history_window_start,
                      s.history_window_end,s.source_time_quality,s.source_clock_role,
                      count(*)::bigint,
                      count(DISTINCT s.source_symbol)::bigint,
                      count(*) FILTER (WHERE EXISTS (
                          SELECT 1 FROM source_raw_daily r
                          WHERE r.session_date=s.target_session
                            AND r.source_capture_id=s.source_capture_id
                            AND r.source_symbol=s.source_symbol
                      ))::bigint,
                      count(*) FILTER (WHERE EXISTS (
                          SELECT 1 FROM source_symbol_price_profiles p
                          WHERE p.target_session=s.target_session
                            AND p.source_capture_id=s.source_capture_id
                            AND p.source_symbol=s.source_symbol
                            AND p.history_window_start=s.history_window_start
                            AND p.history_window_end=s.history_window_end
                            AND p.history_requested_session_count=s.history_session_count
                            AND p.history_calendar_id=s.calendar_id
                            AND (p.history_calendar_revision=s.calendar_revision OR
                              strpos('+' || p.history_calendar_revision || '+', '+' ||
                                to_char(s.target_session, 'YYYY') || ':' || s.calendar_revision || '+') > 0)
                            AND p.price_coordinate=s.price_coordinate
                      ))::bigint
               FROM source_pool_seed_day_facts s
               WHERE s.source_capture_id=%s AND s.target_session=ANY(%s::date[])
                 AND s.producer_version=%s
               GROUP BY s.target_session,s.source_capture_id,
                        s.producer_contract,s.producer_version,
                        s.history_session_count,s.calendar_id,s.calendar_revision,
                        s.research_mode,s.price_coordinate,s.history_window_start,
                        s.history_window_end,s.source_time_quality,s.source_clock_role
               ORDER BY s.target_session,s.source_capture_id""",
            (
                source_capture_id,
                [previous, target],
                _SEED_PRODUCER_VERSION,
            ),
        )
        for row in cursor.fetchall():
            scopes_by_day[row[0]].append(row)

    scope_status, before, current, scope_reason = _select_adjacent_seed_scope_pair(
        scopes_by_day.get(previous, []),
        scopes_by_day.get(target, []),
        previous_session=previous,
        target_session=target,
        official_sessions=official_sessions,
        expected_source_count=len(source_symbols_by_day.get(target, ())),
        history_session_count=history_session_count,
        profile_revision=profile_revision,
    )
    if scope_status != "AVAILABLE" or before is None or current is None:
        reasons.append(scope_reason or "ADJACENT_SEED_SCOPE_NOT_COMPUTED")
    else:
        if current[8] != "NON_PIT_RETROSPECTIVE":
            reasons.append("RESEARCH_MODE_UNSUPPORTED")
        if current[9] != "FORWARD_ADJUSTED":
            reasons.append("PRICE_COORDINATE_UNSUPPORTED")
        if before[11] != previous or current[11] != target:
            reasons.append("HISTORY_WINDOW_END_MISMATCH")

    if source_symbols_by_day and source_symbols_by_day.get(
        previous
    ) != source_symbols_by_day.get(target):
        reasons.append("SOURCE_POOL_MEMBERS_CHANGED")

    not_computed_reasons = {
        "PREVIOUS_SOURCE_CAPTURE_NOT_COMPUTED",
        "ADJACENT_SEED_SCOPE_NOT_COMPUTED",
    }
    unknown_reasons = {"SOURCE_TIME_QUALITY_UNKNOWN"}
    if not reasons:
        status = "AVAILABLE"
    elif any(
        reason not in not_computed_reasons | unknown_reasons for reason in reasons
    ):
        status = "NOT_COMPARABLE"
    elif any(reason in not_computed_reasons for reason in reasons):
        status = "NOT_COMPUTED"
    else:
        status = "UNKNOWN"

    return {
        "previous_source_capture_id": source_capture_id
        if source_symbols_by_day.get(previous)
        else None,
        "status": status,
        "reason_codes": sorted(set(reasons)),
        "previous_scope": before,
        "current_scope": current,
    }


def _read_ranked_relation_metrics(
    cursor: Any,
    *,
    target: date,
    previous: date,
    source_capture_id: int,
    research_cutoff: date,
    previous_scope: tuple,
    current_scope: tuple,
):
    specs = []
    for scope in (previous_scope, current_scope):
        specs.extend(
            (
                scope[0],
                scope[1],
                scope[2],
                scope[3],
                scope[4],
                scope[5],
                scope[6],
                scope[7],
                scope[8],
                scope[9],
                scope[12],
                scope[13],
            )
        )
    cursor.execute(
        """WITH latest AS (
               SELECT DISTINCT ON (o.enrollment_id,o.board_context_key) o.*
               FROM source_t0_early_observation_facts o
               WHERE o.target_session=%s AND o.source_capture_id=%s
               ORDER BY o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           ), selected AS MATERIALIZED (
               SELECT o.* FROM latest o
               WHERE o.membership_status='MAPPED'
                 AND o.board_kind IN ('INDUSTRY','CONCEPT')
                 AND o.membership_snapshot_id IS NOT NULL
                 AND o.catalog_id IS NOT NULL AND o.board_native_id IS NOT NULL
           ), boards AS (
               SELECT DISTINCT membership_snapshot_id,catalog_id,
                      board_kind,board_native_id
               FROM selected
           ), session_specs AS (
               SELECT %s::date AS session_date,%s::bigint AS capture_id,
                      'PREVIOUS'::text AS role,%s::text AS profile_revision,
                      %s::text AS producer_contract,%s::text AS producer_version,
                      %s::smallint AS history_session_count,%s::text AS calendar_id,
                      %s::text AS calendar_revision,%s::text AS research_mode,
                      %s::text AS price_coordinate,%s::text AS source_time_quality,
                      %s::text AS source_clock_role
               UNION ALL
               SELECT %s::date,%s::bigint,'T0',%s::text,%s::text,%s::text,
                      %s::smallint,%s::text,%s::text,%s::text,%s::text,
                      %s::text,%s::text
           ), raw_counts AS MATERIALIZED (
               SELECT session_date,count(*)::integer AS expected_source_count
               FROM source_raw_daily
               WHERE source_capture_id=%s AND session_date=ANY(%s::date[])
               GROUP BY session_date
           ), peer_metrics AS MATERIALIZED (
               SELECT b.membership_snapshot_id,b.catalog_id,b.board_kind,
                      b.board_native_id,ss.role,ss.session_date,
                      m.source_symbol,s.id AS seed_fact_id,
                      v.metric,v.numerator,v.denominator,v.quality,v.reason,
                      raw_counts.expected_source_count
               FROM boards b
               JOIN tdx_sector_catalog catalog
                 ON catalog.id=b.catalog_id
                AND catalog.snapshot_id=b.membership_snapshot_id
                AND catalog.board_kind=b.board_kind
                AND catalog.board_native_id=b.board_native_id
               JOIN tdx_sector_members m ON m.catalog_id=catalog.id
                 AND m.board_kind=b.board_kind
                 AND m.board_native_id=b.board_native_id
               CROSS JOIN session_specs ss
               JOIN source_captures c ON c.capture_id=ss.capture_id
                 AND c.provider='BAOSTOCK'
                 AND c.range_end_session<=%s
                 AND c.price_coordinates @> ARRAY[
                     'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                 ]::text[]
               JOIN raw_counts ON raw_counts.session_date=ss.session_date
               JOIN source_raw_daily raw ON raw.session_date=ss.session_date
                 AND raw.source_capture_id=ss.capture_id
                 AND raw.source_symbol=m.source_symbol
               LEFT JOIN source_pool_seed_day_facts s
                 ON s.target_session=ss.session_date
                AND s.source_capture_id=ss.capture_id
                AND s.source_symbol=m.source_symbol
                AND s.producer_contract=ss.producer_contract
                AND s.producer_version=ss.producer_version
                AND s.history_session_count=ss.history_session_count
                AND s.calendar_id=ss.calendar_id
                AND s.calendar_revision=ss.calendar_revision
                AND s.research_mode=ss.research_mode
                AND s.price_coordinate=ss.price_coordinate
                AND s.source_time_quality=ss.source_time_quality
                AND s.source_clock_role=ss.source_clock_role
               CROSS JOIN LATERAL (VALUES
                   ('RPS5'::text,s.rps5_numerator,s.rps5_denominator,
                    COALESCE(s.rps5_quality,'NOT_COMPUTED'),s.rps5_reason),
                   ('RPS20'::text,s.rps20_numerator,s.rps20_denominator,
                    COALESCE(s.rps20_quality,'NOT_COMPUTED'),s.rps20_reason)
               ) AS v(metric,numerator,denominator,quality,reason)
           ), valid_peer_metrics AS MATERIALIZED (
               SELECT p.*,(p.numerator/p.denominator) AS order_value
               FROM peer_metrics p
               WHERE p.quality='OBSERVED' AND p.numerator IS NOT NULL
                 AND p.denominator>0
           ), ranked AS MATERIALIZED (
               SELECT p.membership_snapshot_id,p.catalog_id,p.board_kind,
                      p.board_native_id,p.role,p.session_date,p.source_symbol,
                      p.seed_fact_id,p.metric,p.quality,p.reason,
                      rank() OVER (
                          PARTITION BY p.membership_snapshot_id,p.catalog_id,
                              p.board_kind,p.board_native_id,p.role,p.metric
                          ORDER BY p.order_value DESC
                      )::integer AS rank_first,
                      count(*) OVER (
                          PARTITION BY p.membership_snapshot_id,p.catalog_id,
                              p.board_kind,p.board_native_id,p.role,p.metric,
                              p.order_value
                      )::integer AS tied_member_count,
                      count(*) OVER (
                          PARTITION BY p.membership_snapshot_id,p.catalog_id,
                              p.board_kind,p.board_native_id,p.role,p.metric
                      )::integer AS valid_member_count
               FROM valid_peer_metrics p
           ), member_sets AS MATERIALIZED (
               SELECT p.membership_snapshot_id,p.catalog_id,p.board_kind,
                      p.board_native_id,p.role,p.metric,
                      COALESCE(
                          array_agg(p.source_symbol ORDER BY p.source_symbol)
                              FILTER (WHERE p.quality='OBSERVED'
                                  AND p.numerator IS NOT NULL AND p.denominator>0),
                          ARRAY[]::text[]
                      ) AS valid_symbols
               FROM peer_metrics p
               GROUP BY p.membership_snapshot_id,p.catalog_id,p.board_kind,
                        p.board_native_id,p.role,p.metric
           ), order_proof AS MATERIALIZED (
               SELECT p.membership_snapshot_id,p.catalog_id,p.board_kind,
                      p.board_native_id,p.role,p.metric,
                      COALESCE(bool_and(
                          p.numerator=trunc(p.numerator)
                          AND p.denominator=trunc(p.denominator)
                          AND p.denominator<=p.expected_source_count
                          AND power(10::numeric,scale(p.order_value))
                              > 2*p.expected_source_count*p.expected_source_count
                      ),true) AS proven_exact
               FROM valid_peer_metrics p
               GROUP BY p.membership_snapshot_id,p.catalog_id,p.board_kind,
                        p.board_native_id,p.role,p.metric
           ), metric_context AS (
               SELECT before.membership_snapshot_id,before.catalog_id,
                      before.board_kind,before.board_native_id,before.metric,
                      before.valid_symbols=current.valid_symbols AS member_sets_equal,
                      COALESCE(before_proof.proven_exact,true)
                          AND COALESCE(current_proof.proven_exact,true)
                          AS order_exact
               FROM member_sets before
               JOIN member_sets current
                 ON current.membership_snapshot_id=before.membership_snapshot_id
                AND current.catalog_id=before.catalog_id
                AND current.board_kind=before.board_kind
                AND current.board_native_id=before.board_native_id
                AND current.metric=before.metric AND current.role='T0'
               LEFT JOIN order_proof before_proof
                 ON before_proof.membership_snapshot_id=before.membership_snapshot_id
                AND before_proof.catalog_id=before.catalog_id
                AND before_proof.board_kind=before.board_kind
                AND before_proof.board_native_id=before.board_native_id
                AND before_proof.metric=before.metric
                AND before_proof.role='PREVIOUS'
               LEFT JOIN order_proof current_proof
                 ON current_proof.membership_snapshot_id=current.membership_snapshot_id
                AND current_proof.catalog_id=current.catalog_id
                AND current_proof.board_kind=current.board_kind
                AND current_proof.board_native_id=current.board_native_id
                AND current_proof.metric=current.metric
                AND current_proof.role='T0'
               WHERE before.role='PREVIOUS'
           ), relation_metrics AS (
               SELECT o.id AS source_t0_fact_id,o.target_session,
                      o.source_capture_id,o.enrollment_id,
                      o.source_symbol,o.board_kind,o.board_native_id,
                      o.membership_snapshot_id,o.catalog_id,o.rule_fact_id,
                      o.pulse_status,o.pulse_state,o.membership_mode,
                      o.research_mode,o.price_coordinate,
                      v.metric,v.current_quality,v.current_reason,
                      v.current_rank_first,v.current_rank_last,
                      v.current_valid_member_count,
                      previous_peer.seed_fact_id AS previous_seed_fact_id,
                      previous_peer.quality AS previous_quality,
                      previous_peer.reason AS previous_reason,
                      before.rank_first AS previous_rank_first,
                      CASE WHEN before.rank_first IS NOT NULL
                           THEN before.rank_first+before.tied_member_count-1 END
                          AS previous_rank_last,
                      before.valid_member_count AS previous_valid_member_count,
                      COALESCE(context.member_sets_equal,false) AS member_sets_equal,
                      COALESCE(context.order_exact,false) AS order_exact,
                      CASE v.metric
                          WHEN 'RPS5' THEN
                              (current.rank_first IS NOT DISTINCT FROM o.rps5_rank_first
                               AND CASE WHEN current.rank_first IS NOT NULL
                                    THEN current.rank_first+current.tied_member_count-1 END
                                   IS NOT DISTINCT FROM o.rps5_rank_last
                               AND COALESCE(current.valid_member_count,0)=o.rps5_valid_member_count)
                          ELSE
                              (current.rank_first IS NOT DISTINCT FROM o.rps20_rank_first
                               AND CASE WHEN current.rank_first IS NOT NULL
                                    THEN current.rank_first+current.tied_member_count-1 END
                                   IS NOT DISTINCT FROM o.rps20_rank_last
                               AND COALESCE(current.valid_member_count,0)=o.rps20_valid_member_count)
                      END AS current_rank_matches
               FROM selected o
               CROSS JOIN LATERAL (VALUES
                   ('RPS5'::text,o.rps5_quality,o.rps5_reason,
                    o.rps5_rank_first,o.rps5_rank_last,o.rps5_valid_member_count),
                   ('RPS20'::text,o.rps20_quality,o.rps20_reason,
                    o.rps20_rank_first,o.rps20_rank_last,o.rps20_valid_member_count)
               ) AS v(metric,current_quality,current_reason,current_rank_first,
                      current_rank_last,current_valid_member_count)
               LEFT JOIN ranked before
                 ON before.membership_snapshot_id=o.membership_snapshot_id
                AND before.catalog_id=o.catalog_id AND before.board_kind=o.board_kind
                AND before.board_native_id=o.board_native_id
                AND before.source_symbol=o.source_symbol
                AND before.metric=v.metric AND before.role='PREVIOUS'
               LEFT JOIN peer_metrics previous_peer
                 ON previous_peer.membership_snapshot_id=o.membership_snapshot_id
                AND previous_peer.catalog_id=o.catalog_id
                AND previous_peer.board_kind=o.board_kind
                AND previous_peer.board_native_id=o.board_native_id
                AND previous_peer.source_symbol=o.source_symbol
                AND previous_peer.metric=v.metric
                AND previous_peer.role='PREVIOUS'
               LEFT JOIN ranked current
                 ON current.membership_snapshot_id=o.membership_snapshot_id
                AND current.catalog_id=o.catalog_id AND current.board_kind=o.board_kind
                AND current.board_native_id=o.board_native_id
                AND current.source_symbol=o.source_symbol
                AND current.metric=v.metric AND current.role='T0'
               LEFT JOIN metric_context context
                 ON context.membership_snapshot_id=o.membership_snapshot_id
                AND context.catalog_id=o.catalog_id AND context.board_kind=o.board_kind
                AND context.board_native_id=o.board_native_id
                AND context.metric=v.metric
           )
           SELECT source_t0_fact_id,target_session,source_capture_id,
                  enrollment_id,source_symbol,board_kind,
                  board_native_id,membership_snapshot_id,catalog_id,rule_fact_id,
                  pulse_status,pulse_state,membership_mode,research_mode,
                  price_coordinate,metric,current_quality,current_reason,
                  current_rank_first,current_rank_last,current_valid_member_count,
                  previous_seed_fact_id,previous_quality,previous_reason,
                  previous_rank_first,previous_rank_last,
                  previous_valid_member_count,member_sets_equal,order_exact,
                  current_rank_matches
           FROM relation_metrics
           ORDER BY source_t0_fact_id,metric""",
        (
            target,
            source_capture_id,
            *specs,
            source_capture_id,
            [previous, target],
            research_cutoff,
        ),
    )
    rows = cursor.fetchall()
    if len(rows) > _MAX_RELATION_ROWS * 2:
        raise ValueError("单日RPS关系结果超过离线生产上限")
    return rows


def _read_latest_unmapped_count(
    cursor: Any, *, target: date, source_capture_id: int
) -> int:
    cursor.execute(
        """WITH latest AS (
               SELECT DISTINCT ON (o.enrollment_id,o.board_context_key) o.*
               FROM source_t0_early_observation_facts o
               WHERE o.target_session=%s AND o.source_capture_id=%s
               ORDER BY o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           )
           SELECT count(*)::integer
           FROM latest WHERE membership_status<>'MAPPED'""",
        (target, source_capture_id),
    )
    return int(cursor.fetchone()[0])


def persist_source_rps_pulse_t0_change_facts(
    config,
    *,
    target_session: date,
    previous_session: date,
    research_cutoff: date,
    source_capture_id: int,
    official_sessions: Sequence[date],
    history_session_count: int | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    """单日事务内读取已存前态/Seed/T0并幂等保存关系级RPS变化事实。"""
    if (
        type(target_session) is not date
        or type(previous_session) is not date
        or type(research_cutoff) is not date
        or not previous_session < target_session <= research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(official_sessions, (list, tuple))
        or any(type(session) is not date for session in official_sessions)
        or history_session_count is not None
        and (
            type(history_session_count) is not int
            or not 1 <= history_session_count <= 250
        )
        or profile_revision is not None
        and not isinstance(profile_revision, str)
    ):
        raise ValueError("RPS×PULSE离线生产的T0、前态、截止或捕获ID无效")
    official_positions = {
        session: index for index, session in enumerate(official_sessions)
    }
    if (
        previous_session not in official_positions
        or target_session not in official_positions
        or official_positions[target_session]
        != official_positions[previous_session] + 1
    ):
        raise ValueError("RPS×PULSE前态与目标日必须是相邻官方交易会话")

    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        latest_rows = _read_latest_mapped_t0(
            cursor, target=target_session, source_capture_id=source_capture_id
        )
        unmapped_rows = _read_latest_unmapped_count(
            cursor, target=target_session, source_capture_id=source_capture_id
        )
        if not latest_rows:
            return {
                "status": "NO_STORED_T0_RELATIONS",
                "target_session": target_session.isoformat(),
                "rows_expected": 0,
                "rows_inserted_or_updated": 0,
                "unmapped_t0_rows": unmapped_rows,
                "transaction": "SERIALIZABLE",
            }

        by_capture: dict[int, list[tuple]] = defaultdict(list)
        for row in latest_rows:
            by_capture[int(row[2])].append(row)

        expected_rows: list[tuple] = []
        for current_capture, relation_rows in sorted(by_capture.items()):
            source_context = _read_source_context(
                cursor,
                target=target_session,
                previous=previous_session,
                source_capture_id=source_capture_id,
                research_cutoff=research_cutoff,
                official_sessions=official_sessions,
                history_session_count=history_session_count,
                profile_revision=profile_revision,
            )
            metrics_by_fact: dict[int, dict[str, tuple]] = defaultdict(dict)
            if (
                source_context["status"] == "AVAILABLE"
                and source_context["previous_source_capture_id"] is not None
                and source_context["previous_scope"] is not None
                and source_context["current_scope"] is not None
            ):
                ranked_rows = _read_ranked_relation_metrics(
                    cursor,
                    target=target_session,
                    previous=previous_session,
                    source_capture_id=source_capture_id,
                    research_cutoff=research_cutoff,
                    previous_scope=source_context["previous_scope"],
                    current_scope=source_context["current_scope"],
                )
                for row in ranked_rows:
                    fact_id = int(row[0])
                    metric = row[15]
                    if (
                        metric not in {"RPS5", "RPS20"}
                        or metric in metrics_by_fact[fact_id]
                    ):
                        raise ValueError("RPS排名查询返回重复或未知指标")
                    metrics_by_fact[fact_id][metric] = row
                if set(metrics_by_fact) != {int(row[0]) for row in relation_rows}:
                    raise ValueError("已存T0关系与批量RPS排名结果未逐键闭合")
                if any(
                    set(items) != {"RPS5", "RPS20"}
                    for items in metrics_by_fact.values()
                ):
                    raise ValueError("RPS5/RPS20独立向量未完整返回")

            for relation in relation_rows:
                (
                    fact_id,
                    target,
                    capture,
                    enrollment_id,
                    symbol,
                    board_kind,
                    board_id,
                    snapshot_id,
                    catalog_id,
                    rule_fact_id,
                    rule_id,
                    rule_version,
                    rule_state,
                    pulse_status,
                    pulse_state,
                    membership_mode,
                    research_mode,
                    price_coordinate,
                    current5_quality,
                    current5_reason,
                    current5_first,
                    current5_last,
                    current5_count,
                    current20_quality,
                    current20_reason,
                    current20_first,
                    current20_last,
                    current20_count,
                ) = relation
                base = {
                    "source_t0_fact_id": fact_id,
                    "target_session": target,
                    "previous_session": previous_session,
                    "source_capture_id": capture,
                    "previous_source_capture_id": source_context[
                        "previous_source_capture_id"
                    ],
                    "enrollment_id": enrollment_id,
                    "source_symbol": symbol,
                    "board_kind": board_kind,
                    "board_native_id": board_id,
                    "membership_snapshot_id": snapshot_id,
                    "catalog_id": catalog_id,
                    "rule_fact_id": rule_fact_id,
                    "rule_id": rule_id,
                    "rule_version": rule_version,
                    "rule_state": rule_state,
                    "calculation_version": _CALCULATION_VERSION,
                    "source_pool_status": source_context["status"],
                    "source_pool_reason_codes": source_context["reason_codes"],
                    "membership_mode": membership_mode,
                    "research_mode": research_mode,
                    "price_coordinate": price_coordinate,
                }
                metric_outputs = {}
                for name, current_values in (
                    (
                        "RPS5",
                        (
                            current5_quality,
                            current5_first,
                            current5_last,
                            current5_count,
                            current5_reason,
                        ),
                    ),
                    (
                        "RPS20",
                        (
                            current20_quality,
                            current20_first,
                            current20_last,
                            current20_count,
                            current20_reason,
                        ),
                    ),
                ):
                    metric = metrics_by_fact.get(int(fact_id), {}).get(name)
                    if metric is None:
                        previous_seed_id = None
                        previous_quality = None
                        previous_first = previous_last = previous_count = None
                        equal = False
                        order_exact = False
                        current_match = False
                    else:
                        previous_seed_id = metric[21]
                        previous_quality = metric[22]
                        previous_first = metric[24] if metric[28] else None
                        previous_last = metric[25] if metric[28] else None
                        previous_count = metric[26] if metric[28] else None
                        equal = bool(metric[27])
                        order_exact = bool(metric[28])
                        current_match = bool(metric[29])
                    state, reasons = classify_rank_change(
                        source_pool_status=source_context["status"],
                        member_sets_equal=equal,
                        order_exact=order_exact,
                        previous_quality=previous_quality,
                        previous_rank_first=previous_first,
                        previous_rank_last=previous_last,
                        current_quality=current_values[0],
                        current_rank_first=current_values[1],
                        current_rank_last=current_values[2],
                        current_rank_matches=current_match,
                    )
                    if state in {"UNKNOWN", "NOT_COMPARABLE", "NOT_COMPUTED"}:
                        if metric is not None and metric[23]:
                            reasons.append(f"PREVIOUS_SEED_REASON:{metric[23]}")
                        if current_values[4]:
                            reasons.append(f"T0_SEED_REASON:{current_values[4]}")
                    metric_outputs[name] = {
                        "state": state,
                        "reasons": reasons,
                        "previous_rank_first": previous_first,
                        "previous_rank_last": previous_last,
                        "previous_valid_member_count": previous_count,
                        "previous_seed_fact_id": previous_seed_id,
                    }
                if (
                    metric_outputs["RPS5"]["previous_seed_fact_id"]
                    != metric_outputs["RPS20"]["previous_seed_fact_id"]
                ):
                    raise ValueError("RPS5与RPS20前态Seed来源不一致")
                row = {
                    **base,
                    "previous_seed_fact_id": metric_outputs["RPS5"][
                        "previous_seed_fact_id"
                    ],
                    "rps5_change_state": metric_outputs["RPS5"]["state"],
                    "rps5_reason_codes": metric_outputs["RPS5"]["reasons"],
                    "rps5_previous_rank_first": metric_outputs["RPS5"][
                        "previous_rank_first"
                    ],
                    "rps5_previous_rank_last": metric_outputs["RPS5"][
                        "previous_rank_last"
                    ],
                    "rps5_previous_valid_member_count": metric_outputs["RPS5"][
                        "previous_valid_member_count"
                    ],
                    "rps20_change_state": metric_outputs["RPS20"]["state"],
                    "rps20_reason_codes": metric_outputs["RPS20"]["reasons"],
                    "rps20_previous_rank_first": metric_outputs["RPS20"][
                        "previous_rank_first"
                    ],
                    "rps20_previous_rank_last": metric_outputs["RPS20"][
                        "previous_rank_last"
                    ],
                    "rps20_previous_valid_member_count": metric_outputs["RPS20"][
                        "previous_valid_member_count"
                    ],
                }
                expected_rows.append(tuple(row[column] for column in _INSERT_COLUMNS))

        if len(expected_rows) != len(latest_rows):
            raise ValueError("RPS×PULSE离线事实没有覆盖全部最新映射关系")

        placeholders = ",".join(["%s"] * len(_INSERT_COLUMNS))
        update_columns = [
            column for column in _INSERT_COLUMNS if column != "source_t0_fact_id"
        ]
        assignments = ",".join(
            f"{column}=EXCLUDED.{column}" for column in update_columns
        )
        comparable_columns = ",".join(
            f"source_rps_pulse_t0_change_facts.{column}" for column in update_columns
        )
        excluded_columns = ",".join(f"EXCLUDED.{column}" for column in update_columns)
        cursor.executemany(
            f"""INSERT INTO source_rps_pulse_t0_change_facts
                   ({",".join(_INSERT_COLUMNS)})
               VALUES ({placeholders})
               ON CONFLICT (source_t0_fact_id) DO UPDATE
               SET {assignments},recorded_at=clock_timestamp()
               WHERE ROW({comparable_columns}) IS DISTINCT FROM ROW({excluded_columns})""",
            expected_rows,
        )
        inserted_or_updated = int(cursor.rowcount)
        fact_ids = [row[0] for row in expected_rows]
        cursor.execute(
            """SELECT source_t0_fact_id,target_session,previous_session,
                      source_capture_id,previous_source_capture_id,
                      enrollment_id,source_symbol,board_kind,board_native_id,
                      membership_snapshot_id,catalog_id,rule_fact_id,rule_id,
                      rule_version,rule_state,calculation_version,source_pool_status,
                      source_pool_reason_codes,previous_seed_fact_id,
                      rps5_change_state,rps5_reason_codes,rps5_previous_rank_first,
                      rps5_previous_rank_last,rps5_previous_valid_member_count,
                      rps20_change_state,rps20_reason_codes,
                      rps20_previous_rank_first,rps20_previous_rank_last,
                      rps20_previous_valid_member_count,membership_mode,
                      research_mode,price_coordinate
               FROM source_rps_pulse_t0_change_facts
               WHERE source_t0_fact_id=ANY(%s)
               ORDER BY source_t0_fact_id""",
            (fact_ids,),
        )
        readback = cursor.fetchall()
        expected_sorted = sorted(expected_rows, key=lambda row: row[0])
        if readback != expected_sorted:
            raise ValueError("RPS×PULSE关系事实异事务前同事务读回不一致，事务回滚")

    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "previous_session": previous_session.isoformat(),
        "source_capture_id": source_capture_id,
        "rows_expected": len(expected_rows),
        "rows_inserted_or_updated": inserted_or_updated,
        "readback_rows": len(readback),
        "unmapped_t0_rows": unmapped_rows,
        "source_status_counts": dict(Counter(row[16] for row in expected_rows)),
        "rps5_change_counts": dict(Counter(row[19] for row in expected_rows)),
        "rps20_change_counts": dict(Counter(row[24] for row in expected_rows)),
        "transaction": "SERIALIZABLE",
        "database_write_attempted": True,
        "ddl_executed": False,
        "dml_executed": True,
    }


def _median_fraction(values: list[Fraction]) -> Fraction | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _read_saved_rps_pulse_t0_date_counts_cursor(
    cursor: Any,
    *,
    research_cutoff: date,
    source_capture_id: int,
    rule_id: str,
    sessions: list[date],
):
    """统计同一已存RPS×PULSE组拥有多少个已到期独立T0日期。"""
    from stock_a.storage.postgres.cohort_stats import (
        _MINIMUM_INDEPENDENT_T0_DATES,
    )
    from stock_a.storage.postgres.research_facts import PATH_VERSION, RULE_VERSION

    cursor.execute(
        """WITH cohort_scope_candidates AS MATERIALIZED (
               SELECT s.target_session,s.source_capture_id,
                      s.history_session_count,s.d1_history_session_count,
                      s.event_window_start,s.calendar_revision
               FROM source_cohort_group_stats s
               WHERE s.research_cutoff=%s AND s.source_capture_id=%s
                 AND s.rule_id=%s AND s.rule_version=%s
                 AND s.path_rule_version=%s AND s.horizon=%s
                 AND s.target_session<=%s
               GROUP BY s.target_session,s.source_capture_id,
                        s.history_session_count,s.d1_history_session_count,
                        s.event_window_start,s.calendar_revision
               HAVING count(*)=6
                  AND count(DISTINCT (s.state,s.weighting))=6
                  AND count(DISTINCT s.source_pool_count)=1
           ), cohort_scopes AS MATERIALIZED (
               SELECT target_session,source_capture_id,
                      history_session_count,d1_history_session_count,
                      event_window_start,calendar_revision
               FROM (
                   SELECT c.*,
                          count(*) OVER (PARTITION BY c.target_session) AS scope_count
                   FROM cohort_scope_candidates c
               ) scoped
               WHERE scope_count=1
           ), latest AS MATERIALIZED (
               SELECT DISTINCT ON (
                          o.target_session,o.enrollment_id,o.board_context_key
                      )
                      o.id,o.target_session,o.source_capture_id,
                      o.enrollment_id,o.board_context_key,
                      o.rule_fact_id,o.membership_status,o.board_kind,
                      o.pulse_status,o.pulse_state
               FROM source_t0_early_observation_facts o
               WHERE o.source_capture_id=%s
                 AND o.target_session IN (
                     SELECT target_session FROM cohort_scopes
                 )
               ORDER BY o.target_session,o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           )
           SELECT o.target_session,o.board_kind,c.rps5_change_state,
                  c.rps20_change_state,o.pulse_status,o.pulse_state,
                  r.state,r.rule_id,r.rule_version,count(*)::integer
           FROM latest o
           JOIN cohort_scopes s
             ON s.target_session=o.target_session
            AND s.source_capture_id=o.source_capture_id
           JOIN source_rule_evaluations r
             ON r.id=o.rule_fact_id
            AND r.target_session=o.target_session
            AND r.source_capture_id=o.source_capture_id
            AND r.rule_version=%s AND r.rule_id=%s
            AND r.history_session_count=s.history_session_count
            AND r.d1_history_session_count=s.d1_history_session_count
            AND r.event_window_start=s.event_window_start
            AND r.calendar_revision=s.calendar_revision
           JOIN source_rps_pulse_t0_change_facts c
             ON c.source_t0_fact_id=o.id
            AND c.target_session=o.target_session
            AND c.source_capture_id=o.source_capture_id
            AND c.rule_fact_id=r.id
            AND c.rule_id=r.rule_id AND c.rule_version=r.rule_version
            AND c.rule_state=r.state
            AND c.calculation_version=%s
           WHERE o.membership_status='MAPPED'
             AND o.board_kind IN ('INDUSTRY','CONCEPT')
           GROUP BY o.target_session,o.board_kind,c.rps5_change_state,
                    c.rps20_change_state,o.pulse_status,o.pulse_state,
                    r.state,r.rule_id,r.rule_version""",
        (
            research_cutoff,
            source_capture_id,
            rule_id,
            RULE_VERSION,
            PATH_VERSION,
            5,
            research_cutoff,
            source_capture_id,
            RULE_VERSION,
            rule_id,
            _CALCULATION_VERSION,
        ),
    )
    session_indexes = {session: index for index, session in enumerate(sessions)}
    counts: dict[tuple, dict[str, int | str]] = {}
    for (
        target_session,
        board_kind,
        rps5_state,
        rps20_state,
        pulse_status,
        pulse_state,
        rule_state,
        stored_rule_id,
        stored_rule_version,
        _relation_count,
    ) in cursor.fetchall():
        key = (
            board_kind,
            rps5_state,
            rps20_state,
            pulse_status,
            pulse_state,
            rule_state,
            stored_rule_id,
            stored_rule_version,
        )
        current = counts.setdefault(
            key,
            {
                "saved_t0_date_count": 0,
                "due_t0_date_count": 0,
                "not_due_t0_date_count": 0,
            },
        )
        current["saved_t0_date_count"] += 1
        session_index = session_indexes.get(target_session)
        endpoint_index = (
            session_index + 5 if session_index is not None else len(sessions)
        )
        if (
            endpoint_index < len(sessions)
            and sessions[endpoint_index] <= research_cutoff
        ):
            current["due_t0_date_count"] += 1
        else:
            current["not_due_t0_date_count"] += 1

    for values in counts.values():
        values["date_status"] = (
            "INSUFFICIENT_INDEPENDENT_DATES"
            if values["due_t0_date_count"] < _MINIMUM_INDEPENDENT_T0_DATES
            else "DESCRIPTIVE_BY_T0_DATE"
        )
        values["date_scope"] = "SAVED_SAME_RPS_PULSE_GROUP_T0_DATES"
    return counts


def _select_rps_pulse_forward_rows(
    rows: Sequence[Sequence[Any]],
    *,
    target: date,
    expected_endpoints: dict[int, date | None],
    calendar_revision: str,
    official_revision_chain: str,
) -> list[tuple[Any, ...]]:
    """每个T0事实/期限只选一条年度日历兼容路径。"""
    from stock_a.research.source_forward_path import (
        forward_calendar_revision_compatible,
    )
    from stock_a.storage.postgres.research_facts import ResearchFactConflict

    grouped: dict[tuple[int, int], list[Sequence[Any]]] = defaultdict(list)
    for row in rows:
        if len(row) != 36:
            raise ResearchFactConflict("RPS×PULSE查询行缺少Forward日历修订字段")
        grouped[(int(row[0]), int(row[25]))].append(row)

    selected_rows: list[tuple[Any, ...]] = []
    for identity, candidates in sorted(grouped.items()):
        horizon = identity[1]
        due_session = expected_endpoints[horizon]
        base_rows = {tuple(row[:26]) + (row[34],) for row in candidates}
        if len(base_rows) != 1:
            raise ResearchFactConflict("同一T0/期限Forward候选绑定了不同关系事实")

        paths = [
            row for row in candidates if row[35] is not None or row[26] is not None
        ]
        compatible = [
            row
            for row in paths
            if due_session is not None
            and row[26] == due_session
            and forward_calendar_revision_compatible(
                target,
                due_session,
                rule_revision=calendar_revision,
                path_revision=row[35],
                official_revision_chain=official_revision_chain,
            )
        ]
        if len(compatible) > 1:
            raise ResearchFactConflict("同一RPS×PULSE关系命中多个兼容Forward日历版本")
        if compatible:
            selected_rows.append(tuple(compatible[0][:35]))
            continue

        if due_session is not None and any(row[26] == due_session for row in paths):
            if len(paths) > 1:
                raise ResearchFactConflict("同一RPS×PULSE关系存在多个不兼容Forward候选")
            selected_rows.append(
                _forward_path_unknown_row(
                    paths[0], "FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH"
                )
            )
            continue

        if len(paths) > 1:
            raise ResearchFactConflict("同一RPS×PULSE关系命中多个Forward候选")
        if paths:
            reason = (
                "FORWARD_PATH_STORED_BEFORE_OFFICIAL_ENDPOINT_DUE"
                if due_session is None
                else "FORWARD_PATH_ENDPOINT_SESSION_MISMATCH"
            )
            selected_rows.append(_forward_path_unknown_row(paths[0], reason))
        else:
            selected_rows.append(tuple(candidates[0][:35]))
    return selected_rows


def _forward_path_unknown_row(row: Sequence[Any], reason: str) -> tuple[Any, ...]:
    """保留错误路径身份但清除其数值，避免误计为Observed收益。"""
    unknown = list(row[:35])
    unknown[28] = "UNKNOWN"
    unknown[29] = reason
    unknown[30] = None
    unknown[31] = None
    unknown[32] = "UNKNOWN"
    unknown[33] = [reason]
    return tuple(unknown)


def read_rps_pulse_forward_groups_cursor(
    cursor: Any,
    *,
    target: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str | None = None,
    history_session_count: int,
    d1_history_session_count: int,
    event_window_start: date,
    calendar_revision: str,
    rule_id: str,
    board_kind: str,
    scenario_id: str | None,
    rule_state_filter: str,
):
    """按单个已选T0/冻结规则键聚合PG内已存关系标签和Forward路径。"""
    if board_kind not in {"ALL", "INDUSTRY", "CONCEPT"}:
        raise ValueError("RPS×PULSE读取的板块类型无效")
    if scenario_id not in {None, "ALL", *_SCENARIO_SQL}:
        raise ValueError("RPS×PULSE研究分组无效")
    if rule_state_filter not in {"ALL", "TRUE", "FALSE", "UNKNOWN"}:
        raise ValueError("RPS×PULSE原规则状态筛选无效")

    from stock_a.research.entry_context.sessions import _load_calendar
    from stock_a.storage.postgres.research_facts import PATH_VERSION, RULE_VERSION

    _, _, official_revision_chain, sessions = _load_calendar(research_cutoff)
    if calendar_revision != official_revision_chain:
        raise ValueError("Cohort日历修订与现行官方日历不一致")
    if target not in sessions:
        raise ValueError("RPS×PULSE T0不在官方日历中")
    target_index = sessions.index(target)
    expected_endpoints = {
        horizon: (
            sessions[target_index + horizon]
            if target_index + horizon < len(sessions)
            and sessions[target_index + horizon] <= research_cutoff
            else None
        )
        for horizon in _HORIZONS
    }

    board_clause = "" if board_kind == "ALL" else "AND o.board_kind=%s"
    rule_state_clause = "" if rule_state_filter == "ALL" else "AND r.state=%s"

    cursor.execute(
        f"""WITH latest AS (
               SELECT DISTINCT ON (o.enrollment_id,o.board_context_key) o.*
               FROM source_t0_early_observation_facts o
               WHERE o.target_session=%s AND o.source_capture_id=%s
               ORDER BY o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           )
           SELECT COALESCE(c.rps5_change_state,'NOT_COMPUTED'),
                  COALESCE(c.rps20_change_state,'NOT_COMPUTED'),
                  o.pulse_status,o.pulse_state,r.state,
                  count(*)::integer,
                  count(*) FILTER (WHERE c.source_t0_fact_id IS NULL)::integer
           FROM latest o
           JOIN source_rule_evaluations r ON r.id=o.rule_fact_id
           LEFT JOIN source_rps_pulse_t0_change_facts c
             ON c.source_t0_fact_id=o.id
            AND c.calculation_version=%s
           WHERE o.target_session=%s AND o.source_capture_id=%s
             AND r.target_session=o.target_session
             AND r.source_capture_id=o.source_capture_id
             AND r.rule_version=%s AND r.rule_id=%s
             AND r.history_session_count=%s
             AND r.d1_history_session_count=%s AND r.event_window_start=%s
             AND r.calendar_revision=%s AND o.membership_status='MAPPED'
             AND o.board_kind IN ('INDUSTRY','CONCEPT')
             {board_clause} {rule_state_clause}
           GROUP BY 1,2,3,4,5""",
        (
            target,
            source_capture_id,
            _CALCULATION_VERSION,
            target,
            source_capture_id,
            RULE_VERSION,
            rule_id,
            history_session_count,
            d1_history_session_count,
            event_window_start,
            calendar_revision,
            *([board_kind] if board_kind != "ALL" else []),
            *([rule_state_filter] if rule_state_filter != "ALL" else []),
        ),
    )
    distribution_rows = cursor.fetchall()
    quality_distribution = {
        "rps5": {},
        "rps20": {},
        "pulse": {},
    }
    scenario_counts = Counter()
    missing_change_fact_count = 0
    grouped_relation_population = 0
    for (
        rps5_state,
        rps20_state,
        pulse_status,
        pulse_state,
        rule_state,
        count,
        missing_count,
    ) in distribution_rows:
        count = int(count)
        grouped_relation_population += count
        missing_change_fact_count += int(missing_count or 0)
        quality_distribution["rps5"][rps5_state] = (
            quality_distribution["rps5"].get(rps5_state, 0) + count
        )
        quality_distribution["rps20"][rps20_state] = (
            quality_distribution["rps20"].get(rps20_state, 0) + count
        )
        pulse_key = f"{pulse_status}:{pulse_state}"
        quality_distribution["pulse"][pulse_key] = (
            quality_distribution["pulse"].get(pulse_key, 0) + count
        )
        for available_id in _SCENARIO_SQL:
            if available_id == "DOUBLE_IMPROVED_PULSE_FALSE":
                matches = (
                    rps5_state == "IMPROVED"
                    and rps20_state == "IMPROVED"
                    and pulse_status == "AVAILABLE"
                    and pulse_state == "FALSE"
                )
            elif available_id == "DOUBLE_IMPROVED_PULSE_TRUE":
                matches = (
                    rps5_state == "IMPROVED"
                    and rps20_state == "IMPROVED"
                    and pulse_status == "AVAILABLE"
                    and pulse_state == "TRUE"
                )
            elif available_id == "RPS5_UP_RPS20_DOWN_PULSE_TRUE":
                matches = (
                    rps5_state == "IMPROVED"
                    and rps20_state == "WORSENED"
                    and pulse_status == "AVAILABLE"
                    and pulse_state == "TRUE"
                )
            elif available_id == "DOUBLE_WORSENED_PULSE_TRUE":
                matches = (
                    rps5_state == "WORSENED"
                    and rps20_state == "WORSENED"
                    and pulse_status == "AVAILABLE"
                    and pulse_state == "TRUE"
                )
            else:
                matches = (
                    rps5_state not in {"IMPROVED", "WORSENED", "UNCHANGED"}
                    or rps20_state not in {"IMPROVED", "WORSENED", "UNCHANGED"}
                    or pulse_status != "AVAILABLE"
                    or pulse_state not in {"TRUE", "FALSE"}
                )
            if matches:
                scenario_counts[available_id] += count

    available_scenarios = [
        {
            "id": scenario_id,
            "label": _SCENARIO_LABELS[scenario_id],
            "relation_count": scenario_counts[scenario_id],
        }
        for scenario_id in _SCENARIO_SQL
        if scenario_counts[scenario_id] > 0
    ]
    if scenario_id in {None, "ALL"}:
        selected_scenario_id = "ALL"
    else:
        selected_scenario_id = scenario_id

    scenario_clause = _SCENARIO_SQL.get(selected_scenario_id, "TRUE")
    selected_scenario_relation_count = (
        grouped_relation_population
        if selected_scenario_id == "ALL"
        else int(scenario_counts.get(selected_scenario_id, 0))
    )
    distribution_missing_change_count = sum(
        int(row[6] or 0) for row in distribution_rows
    )
    cursor.execute(
        f"""WITH latest AS (
               SELECT DISTINCT ON (o.enrollment_id,o.board_context_key) o.*
               FROM source_t0_early_observation_facts o
               WHERE o.target_session=%s AND o.source_capture_id=%s
               ORDER BY o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           )
           SELECT o.id,o.enrollment_id,o.source_symbol,o.board_kind,
                  o.board_native_id,o.membership_snapshot_id,o.catalog_id,
                  o.rule_fact_id,o.pulse_status,o.pulse_state,
                  c.source_pool_status,c.source_pool_reason_codes,
                  c.rps5_change_state,c.rps5_reason_codes,
                  c.rps5_previous_rank_first,c.rps5_previous_rank_last,
                  c.rps5_previous_valid_member_count,
                  c.rps20_change_state,c.rps20_reason_codes,
                  c.rps20_previous_rank_first,c.rps20_previous_rank_last,
                  c.rps20_previous_valid_member_count,
                  r.state AS rule_state,r.rule_id,r.rule_version,
                  h.horizon,p.endpoint_session,p.maturity,p.endpoint_quality,
                  p.endpoint_reason,p.endpoint_numerator,p.endpoint_denominator,
                  p.path_quality,p.path_reasons,
                  (c.source_t0_fact_id IS NOT NULL) AS change_fact_present,
                  p.calendar_revision
           FROM latest o
           JOIN source_rule_evaluations r ON r.id=o.rule_fact_id
           LEFT JOIN source_rps_pulse_t0_change_facts c
             ON c.source_t0_fact_id=o.id
            AND c.calculation_version=%s
           CROSS JOIN (VALUES (1::smallint),(3::smallint),(5::smallint)) h(horizon)
           LEFT JOIN source_forward_path_facts p
             ON p.target_session=o.target_session
            AND p.source_capture_id=o.source_capture_id
            AND p.source_symbol=o.source_symbol
            AND p.horizon=h.horizon
            AND p.path_rule_version=%s
            AND p.price_coordinate='FORWARD_ADJUSTED'
            AND p.scope='NON_PIT_EXPLORATORY'
            AND p.endpoint_session<=%s
           WHERE o.target_session=%s AND o.source_capture_id=%s
             AND o.rule_fact_id IS NOT NULL
             AND r.target_session=o.target_session
             AND r.source_capture_id=o.source_capture_id
             AND r.rule_version=%s AND r.rule_id=%s
             AND r.history_session_count=%s
             AND r.d1_history_session_count=%s
             AND r.event_window_start=%s AND r.calendar_revision=%s
             AND o.membership_status='MAPPED'
             AND o.board_kind IN ('INDUSTRY','CONCEPT')
             {board_clause} AND ({scenario_clause}) {rule_state_clause}
           ORDER BY o.id,h.horizon""",
        (
            target,
            source_capture_id,
            _CALCULATION_VERSION,
            PATH_VERSION,
            research_cutoff,
            target,
            source_capture_id,
            RULE_VERSION,
            rule_id,
            history_session_count,
            d1_history_session_count,
            event_window_start,
            calendar_revision,
            *([board_kind] if board_kind != "ALL" else []),
            *([rule_state_filter] if rule_state_filter != "ALL" else []),
        ),
    )
    raw_rows = cursor.fetchall()
    if len(raw_rows) > _MAX_OUTCOME_ROWS:
        raise ValueError("所选T0/冻结规则的Forward候选超过轻量读取上限")
    rows = _select_rps_pulse_forward_rows(
        raw_rows,
        target=target,
        expected_endpoints=expected_endpoints,
        calendar_revision=calendar_revision,
        official_revision_chain=official_revision_chain,
    )
    if len(rows) > _MAX_OUTCOME_ROWS:
        raise ValueError("所选T0/冻结规则的板块关系超过轻量读取上限")

    # 精确规则分母及未映射登记单独核对，不用有价Forward行反推总体。
    cursor.execute(
        f"""WITH latest AS MATERIALIZED (
               SELECT DISTINCT ON (o.enrollment_id,o.board_context_key) o.*
               FROM source_t0_early_observation_facts o
               WHERE o.target_session=%s AND o.source_capture_id=%s
               ORDER BY o.enrollment_id,o.board_context_key,
                        o.recorded_at DESC,o.id DESC
           )
           SELECT count(DISTINCT e.id)::integer,
                  count(o.id)::integer,
                  count(o.id) FILTER (WHERE o.membership_status='MAPPED')::integer,
                  count(o.id) FILTER (WHERE o.membership_status='UNKNOWN')::integer,
                  count(o.id) FILTER (WHERE o.membership_status='NO_STORED_FACT')::integer,
                  count(DISTINCT e.id) FILTER (WHERE o.id IS NULL)::integer,
                  count(o.id) FILTER (
                      WHERE o.membership_status='MAPPED'
                        AND (%s='ALL' OR o.board_kind=%s)
                  )::integer
           FROM source_rule_evaluations r
           JOIN source_research_enrollments e ON e.rule_fact_id=r.id
           LEFT JOIN latest o ON o.enrollment_id=e.id
             AND o.rule_fact_id=r.id
             AND o.source_capture_id=r.source_capture_id
           WHERE r.target_session=%s AND r.source_capture_id=%s
             AND r.rule_version=%s AND r.rule_id=%s
             AND r.history_session_count=%s
             AND r.d1_history_session_count=%s AND r.event_window_start=%s
             AND r.calendar_revision=%s {rule_state_clause}""",
        (
            target,
            source_capture_id,
            board_kind,
            board_kind,
            target,
            source_capture_id,
            RULE_VERSION,
            rule_id,
            history_session_count,
            d1_history_session_count,
            event_window_start,
            calendar_revision,
            *([rule_state_filter] if rule_state_filter != "ALL" else []),
        ),
    )
    root_counts = cursor.fetchone()
    independent_t0_date_counts = _read_saved_rps_pulse_t0_date_counts_cursor(
        cursor,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule_id=rule_id,
        sessions=sessions,
    )

    groups: dict[tuple, dict[str, Any]] = {}
    for row in rows:
        (
            t0_fact_id,
            enrollment_id,
            symbol,
            relation_board_kind,
            board_native_id,
            snapshot_id,
            catalog_id,
            _rule_fact_id,
            pulse_status,
            pulse_state,
            source_pool_status,
            source_reasons,
            rps5_state,
            rps5_reasons,
            rps5_prev_first,
            rps5_prev_last,
            rps5_prev_count,
            rps20_state,
            rps20_reasons,
            rps20_prev_first,
            rps20_prev_last,
            rps20_prev_count,
            rule_state,
            stored_rule_id,
            stored_rule_version,
            horizon,
            endpoint_session,
            maturity,
            endpoint_quality,
            endpoint_reason,
            numerator,
            denominator,
            path_quality,
            path_reasons,
            change_fact_present,
        ) = row
        key = (
            relation_board_kind,
            rps5_state if change_fact_present else "NOT_COMPUTED",
            rps20_state if change_fact_present else "NOT_COMPUTED",
            pulse_status,
            pulse_state,
            rule_state,
            stored_rule_id,
            stored_rule_version,
        )
        endpoint_due = expected_endpoints[horizon]
        actual_path = endpoint_session is not None
        if endpoint_reason in {
            "FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH",
            "FORWARD_PATH_ENDPOINT_SESSION_MISMATCH",
            "FORWARD_PATH_STORED_BEFORE_OFFICIAL_ENDPOINT_DUE",
        }:
            outcome_state = "UNKNOWN"
        elif actual_path and endpoint_session != endpoint_due:
            outcome_state = "INCOMPLETE"
        elif endpoint_due is None:
            outcome_state = "NOT_DUE"
        elif not actual_path:
            outcome_state = "NOT_COMPUTED"
        elif endpoint_quality == "OBSERVED" and numerator is not None and denominator:
            outcome_state = "OBSERVED"
        else:
            outcome_state = "UNKNOWN"

        group = groups.setdefault(
            key,
            {
                "group_id": (
                    f"BOARD={key[0]};RPS5={key[1]};RPS20={key[2]};"
                    f"PULSE={key[3]}:{key[4]};RULE={key[5]};"
                    f"RULE_ID={key[6]};RULE_VERSION={key[7]}"
                ),
                "board_kind": relation_board_kind,
                "rps5_change_state": key[1],
                "rps20_change_state": key[2],
                "pulse_status": pulse_status,
                "pulse_state": pulse_state,
                "rule_state": rule_state,
                "rule_id": stored_rule_id,
                "rule_version": stored_rule_version,
                "relation_count": 0,
                "unique_stock_board_t0_count": 0,
                "independent_security_count": 0,
                "rps5_reasons": Counter(),
                "rps20_reasons": Counter(),
                "source_pool_reasons": Counter(),
                "outcomes": {},
                "_relation_keys": set(),
                "_symbols": set(),
            },
        )
        # 三个期限分别出现同一关系；只在第一个期限累加完整分母。
        if horizon == 1:
            group["relation_count"] += 1
            group["_relation_keys"].add(
                (target, symbol, relation_board_kind, board_native_id)
            )
            group["_symbols"].add(symbol)
            for reason in source_reasons or []:
                group["source_pool_reasons"][reason] += 1
            for reason in rps5_reasons or []:
                group["rps5_reasons"][reason] += 1
            for reason in rps20_reasons or []:
                group["rps20_reasons"][reason] += 1
        outcome = group["outcomes"].setdefault(
            horizon,
            {
                "horizon": horizon,
                "due": endpoint_due is not None,
                "endpoint_session": endpoint_due.isoformat()
                if endpoint_due is not None
                else None,
                "relation_denominator": 0,
                "not_due_count": 0,
                "path_stored_count": 0,
                "path_missing_count": 0,
                "endpoint_observed_count": 0,
                "endpoint_unknown_count": 0,
                "incomplete_count": 0,
                "positive_count": 0,
                "non_positive_count": 0,
                "returns": [],
                "maturity_counts": Counter(),
                "endpoint_quality_counts": Counter(),
                "path_quality_counts": Counter(),
                "reason_counts": Counter(),
                "examples": [],
                "failure_examples": [],
            },
        )
        if horizon == 1:
            outcome["relation_denominator"] = group["relation_count"]
        if outcome_state == "NOT_DUE":
            outcome["not_due_count"] += 1
            outcome["reason_counts"]["NOT_DUE_AS_OF_RESEARCH_CUTOFF"] += 1
            continue
        if outcome_state == "NOT_COMPUTED":
            outcome["path_missing_count"] += 1
            outcome["reason_counts"]["FORWARD_PATH_NOT_STORED"] += 1
            if len(outcome["failure_examples"]) < 3:
                outcome["failure_examples"].append(
                    {
                        "source_symbol": symbol,
                        "board_native_id": board_native_id,
                        "outcome_state": outcome_state,
                        "endpoint_reason": "FORWARD_PATH_NOT_STORED",
                        "path_reasons": [],
                    }
                )
            continue
        outcome["path_stored_count"] += 1
        if maturity:
            outcome["maturity_counts"][maturity] += 1
        if endpoint_quality:
            outcome["endpoint_quality_counts"][endpoint_quality] += 1
        if path_quality:
            outcome["path_quality_counts"][path_quality] += 1
        for reason in path_reasons or []:
            outcome["reason_counts"][reason] += 1
        if outcome_state == "INCOMPLETE":
            outcome["incomplete_count"] += 1
            outcome["reason_counts"]["FORWARD_ENDPOINT_SESSION_MISMATCH"] += 1
            if len(outcome["failure_examples"]) < 3:
                outcome["failure_examples"].append(
                    {
                        "source_symbol": symbol,
                        "board_native_id": board_native_id,
                        "outcome_state": outcome_state,
                        "endpoint_reason": "FORWARD_ENDPOINT_SESSION_MISMATCH",
                        "path_reasons": list(path_reasons or []),
                    }
                )
            continue
        if outcome_state == "UNKNOWN":
            outcome["endpoint_unknown_count"] += 1
            if endpoint_reason:
                outcome["reason_counts"][endpoint_reason] += 1
            if not endpoint_reason and not path_reasons:
                outcome["reason_counts"]["ENDPOINT_QUALITY_UNKNOWN_NO_REASON"] += 1
            if len(outcome["failure_examples"]) < 3:
                outcome["failure_examples"].append(
                    {
                        "source_symbol": symbol,
                        "board_native_id": board_native_id,
                        "outcome_state": outcome_state,
                        "endpoint_reason": endpoint_reason,
                        "path_reasons": list(path_reasons or []),
                    }
                )
            continue

        outcome["endpoint_observed_count"] += 1
        return_value = Fraction(int(numerator), int(denominator))
        outcome["returns"].append(return_value)
        if return_value > 0:
            outcome["positive_count"] += 1
        else:
            outcome["non_positive_count"] += 1
        if return_value < 0:
            outcome["examples"].append(
                {
                    "source_symbol": symbol,
                    "board_native_id": board_native_id,
                    "return_numerator": str(numerator),
                    "return_denominator": str(denominator),
                    "return_fraction": str(return_value),
                    "endpoint_session": endpoint_session.isoformat(),
                }
            )
            outcome["examples"] = sorted(
                outcome["examples"],
                key=lambda item: Fraction(
                    int(item["return_numerator"]), int(item["return_denominator"])
                ),
            )[:3]

    formatted_groups = []
    for key, group in sorted(groups.items()):
        group["unique_stock_board_t0_count"] = len(group.pop("_relation_keys"))
        group["independent_security_count"] = len(group.pop("_symbols"))
        date_counts = independent_t0_date_counts.get(key)
        group["independent_t0_date_count"] = (
            date_counts["due_t0_date_count"] if date_counts else None
        )
        group["independent_t0_saved_date_count"] = (
            date_counts["saved_t0_date_count"] if date_counts else None
        )
        group["independent_t0_not_due_date_count"] = (
            date_counts["not_due_t0_date_count"] if date_counts else None
        )
        group["independent_t0_date_status"] = (
            date_counts["date_status"] if date_counts else "NOT_COMPUTED"
        )
        group["independent_t0_date_scope"] = (
            date_counts["date_scope"] if date_counts else "NO_SAVED_SAME_GROUP_T0_DATE"
        )
        group["rps5_reasons"] = dict(group["rps5_reasons"])
        group["rps20_reasons"] = dict(group["rps20_reasons"])
        group["source_pool_reasons"] = dict(group["source_pool_reasons"])
        for horizon, outcome in sorted(group["outcomes"].items()):
            if horizon == 5:
                outcome["independent_t0_date_count"] = group[
                    "independent_t0_date_count"
                ]
                outcome["independent_t0_saved_date_count"] = group[
                    "independent_t0_saved_date_count"
                ]
                outcome["independent_t0_not_due_date_count"] = group[
                    "independent_t0_not_due_date_count"
                ]
                outcome["independent_t0_date_status"] = group[
                    "independent_t0_date_status"
                ]
                outcome["independent_t0_date_scope"] = group[
                    "independent_t0_date_scope"
                ]
            outcome["median_return"] = (
                str(_median_fraction(outcome.pop("returns")))
                if outcome["returns"]
                else None
            )
            outcome["maturity_counts"] = dict(outcome["maturity_counts"])
            outcome["endpoint_quality_counts"] = dict(
                outcome["endpoint_quality_counts"]
            )
            outcome["path_quality_counts"] = dict(outcome["path_quality_counts"])
            outcome["reason_counts"] = dict(outcome["reason_counts"])
            relation_count = group["relation_count"]
            outcome["relation_denominator"] = relation_count
            outcome["due_relation_count"] = max(
                0, relation_count - outcome["not_due_count"]
            )
            outcome["coverage_count"] = outcome["endpoint_observed_count"]
            outcome["coverage_denominator"] = relation_count
            outcome["coverage_fraction"] = (
                f"{outcome['endpoint_observed_count']}/{relation_count}"
                if relation_count
                else None
            )
            outcome["coverage_of_due"] = (
                f"{outcome['endpoint_observed_count']}/{outcome['due_relation_count']}"
                if outcome["due_relation_count"]
                else None
            )
            outcome["examples"] = sorted(
                outcome["examples"],
                key=lambda item: Fraction(
                    int(item["return_numerator"]), int(item["return_denominator"])
                ),
            )
        formatted_groups.append(group)

    return {
        "status": (
            "INCOMPLETE"
            if distribution_missing_change_count
            or len(rows) != selected_scenario_relation_count * len(_HORIZONS)
            else "AVAILABLE"
            if formatted_groups
            else "NO_NONEMPTY_GROUPS"
        ),
        "source_capture_id": source_capture_id,
        "target_session": target.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_registration_count": int(root_counts[0] or 0),
        "t0_observation_relation_rows": int(root_counts[1] or 0),
        "mapped_board_relation_count": int(root_counts[2] or 0),
        "unknown_membership_relation_count": int(root_counts[3] or 0),
        "no_stored_membership_relation_count": int(root_counts[4] or 0),
        "registration_without_t0_observation_count": int(root_counts[5] or 0),
        "selected_board_relation_count": int(root_counts[6] or 0),
        "selected_scenario_relation_count": selected_scenario_relation_count,
        "selected_board_kind": board_kind,
        "selected_scenario_id": selected_scenario_id,
        "available_scenarios": available_scenarios,
        "quality_distribution": quality_distribution,
        "missing_change_fact_count": distribution_missing_change_count,
        "change_fact_status": (
            "INCOMPLETE"
            if distribution_missing_change_count
            or len(rows) != selected_scenario_relation_count * len(_HORIZONS)
            else "AVAILABLE"
        ),
        "groups": formatted_groups,
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "research_mode": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "independence_note": "板块关系、同股多板及跨日登记可重叠；证券和T0日期分母分别列示。",
    }
