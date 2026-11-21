"""读取单板块有界多会话PULSE、转移、后继及市场事实。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP, localcontext
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.contracts.profile_revision import PROFILE_REVISION_PATTERN
from stock_a.research.source_market_breadth import SOURCE_MARKET_BREADTH_CONTRACT
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.sector_pulse_facts import (
    _ALGORITHM,
    _KINDS,
    _TABLE,
    _pulse_payload,
    SectorPulsePgError,
)


def _read_market_context(
    cursor: Any,
    *,
    target_session: date,
    research_cutoff: date,
    source_identity: Mapping[str, Any] | None,
    source_pool_count: int | None,
    calendar_id: str,
    calendar_revision: str,
) -> dict[str, Any]:
    """独立读取同日精确来源的宽度与参与事实，不依赖PULSE是否存在。"""
    if not isinstance(source_identity, Mapping):
        reason = ["MARKET_FACT_SOURCE_IDENTITY_UNAVAILABLE"]
        return {
            "breadth": {"status": "NOT_COMPUTED", "reason_codes": reason},
            "participation": {"status": "NOT_COMPUTED", "reason_codes": reason},
        }
    capture_id = source_identity.get("source_capture_id")
    profile_revision = source_identity.get("profile_revision")
    if type(capture_id) is not int or capture_id <= 0:
        reason = ["MARKET_FACT_SOURCE_IDENTITY_INVALID"]
        return {
            "breadth": {"status": "INCOMPLETE", "reason_codes": reason},
            "participation": {"status": "INCOMPLETE", "reason_codes": reason},
        }

    cursor.execute(
        """SELECT id,contract_version,calendar_id,calendar_revision,
       source_count,eligible_count,target_present_count,target_missing_count,
       read_failed_count
FROM source_market_breadth_days
WHERE target_session=%s AND target_session<=%s AND source_capture_id=%s
  AND price_coordinate='FORWARD_ADJUSTED'
  AND calendar_id=%s AND calendar_revision=%s AND contract_version=%s
ORDER BY id LIMIT 2""",
        (
            target_session,
            research_cutoff,
            capture_id,
            calendar_id,
            calendar_revision,
            SOURCE_MARKET_BREADTH_CONTRACT,
        ),
    )
    breadth_days = cursor.fetchall()
    if not breadth_days:
        breadth = {
            "status": "NOT_COMPUTED",
            "reason_codes": ["MARKET_BREADTH_FACT_NOT_SAVED_FOR_EXACT_SOURCE"],
        }
    elif len(breadth_days) != 1:
        breadth = {
            "status": "INCOMPLETE",
            "reason_codes": ["MARKET_BREADTH_SCOPE_AMBIGUOUS"],
        }
    else:
        (
            breadth_day_id,
            breadth_contract,
            breadth_calendar_id,
            breadth_calendar_revision,
            source_count,
            eligible_count,
            present_count,
            missing_count,
            read_failed_count,
        ) = breadth_days[0]
        cursor.execute(
            """SELECT classification,count(*)::int
FROM source_market_breadth_symbols WHERE breadth_day_id=%s GROUP BY classification""",
            (breadth_day_id,),
        )
        breadth_counts = dict(cursor.fetchall())
        cursor.execute(
            """SELECT count(*)::int,count(DISTINCT source_symbol)::int
FROM source_market_breadth_symbols WHERE breadth_day_id=%s""",
            (breadth_day_id,),
        )
        symbol_count, distinct_symbol_count = cursor.fetchone()
        unknown_count = int(breadth_counts.get("UNKNOWN", 0))
        valid_breadth = (
            breadth_calendar_id == calendar_id
            and breadth_calendar_revision == calendar_revision
            and symbol_count == int(source_count)
            and distinct_symbol_count == symbol_count
            and int(source_count) - unknown_count == int(eligible_count)
            and (source_pool_count is None or int(source_count) == source_pool_count)
        )
        breadth = {
            "status": "AVAILABLE" if valid_breadth else "INCOMPLETE",
            "contract_version": breadth_contract,
            "source_count": int(source_count),
            "eligible_count": int(eligible_count),
            "target_present_count": int(present_count),
            "target_missing_count": int(missing_count),
            "read_failed_count": int(read_failed_count),
            "classification_counts": breadth_counts,
            "reason_codes": []
            if valid_breadth
            else ["MARKET_BREADTH_PARTITION_NOT_CLOSED"],
        }
        if unknown_count:
            cursor.execute(
                """SELECT COALESCE(primary_unknown_reason,'UNKNOWN'),count(*)::int
FROM source_market_breadth_symbols
WHERE breadth_day_id=%s AND classification='UNKNOWN'
GROUP BY primary_unknown_reason ORDER BY primary_unknown_reason""",
                (breadth_day_id,),
            )
            breadth["unknown_reason_counts"] = dict(cursor.fetchall())

    participation_sql = """SELECT id,profile_revision,producer_version,price_coordinate,
       pit_status,source_pool_count,profile_count,assessable_count,
       profile_unknown_count,profile_missing_count,median_numerator,
       median_denominator,classification,status
FROM source_market_participation_days
WHERE target_session=%s AND target_session<=%s AND source_capture_id=%s
  AND contract_version='MARKET_PARTICIPATION_R1'
  AND price_coordinate='FORWARD_ADJUSTED'
  AND calendar_id=%s AND calendar_revision=%s"""
    participation_params: tuple[Any, ...] = (
        target_session,
        research_cutoff,
        capture_id,
        calendar_id,
        calendar_revision,
    )
    if profile_revision is not None:
        participation_sql += " AND profile_revision=%s"
        participation_params += (profile_revision,)
    cursor.execute(participation_sql + " ORDER BY id LIMIT 2", participation_params)
    participation_rows = cursor.fetchall()
    if not participation_rows:
        participation = {
            "status": "NOT_COMPUTED",
            "reason_codes": ["MARKET_PARTICIPATION_FACT_NOT_SAVED_FOR_EXACT_SOURCE"],
        }
    elif len(participation_rows) != 1:
        participation = {
            "status": "INCOMPLETE",
            "reason_codes": ["MARKET_PARTICIPATION_SCOPE_AMBIGUOUS"],
        }
    else:
        (
            _participation_id,
            stored_profile_revision,
            producer_version,
            price_coordinate,
            pit_status,
            stored_source_count,
            profile_count,
            assessable_count,
            profile_unknown_count,
            profile_missing_count,
            median_numerator,
            median_denominator,
            classification,
            participation_status,
        ) = participation_rows[0]
        valid_participation = (
            price_coordinate == "FORWARD_ADJUSTED"
            and pit_status == "NON_PIT_RETROSPECTIVE"
            and int(stored_source_count)
            == int(profile_count) + int(profile_missing_count)
            and int(profile_count) == int(assessable_count) + int(profile_unknown_count)
            and (
                source_pool_count is None
                or int(stored_source_count) == source_pool_count
            )
            and (
                profile_revision is None
                or stored_profile_revision.strip() == profile_revision
            )
        )
        median_value = None
        if median_numerator is not None and median_denominator is not None:
            if int(median_denominator) > 0:
                median_value = str(
                    Fraction(int(median_numerator), int(median_denominator))
                )
            else:
                valid_participation = False
        participation = {
            "status": participation_status if valid_participation else "INCOMPLETE",
            "profile_revision": stored_profile_revision.strip(),
            "producer_version": producer_version,
            "classification": classification,
            "price_coordinate": price_coordinate,
            "pit_status": pit_status,
            "source_pool_count": int(stored_source_count),
            "profile_count": int(profile_count),
            "assessable_count": int(assessable_count),
            "profile_unknown_count": int(profile_unknown_count),
            "profile_missing_count": int(profile_missing_count),
            "median": median_value,
            "reason_codes": []
            if valid_participation
            else ["MARKET_PARTICIPATION_PARTITION_NOT_CLOSED"],
        }
    return {"breadth": breadth, "participation": participation}


def read_source_sector_pulse_history(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    target_source_capture_id: int,
    profile_revision: str,
    snapshot_id: int,
    board_kind: str,
    board_native_id: str,
    calendar_id: str,
    calendar_revision: str,
    calendar_sessions: Sequence[date],
    history_sessions: Sequence[date],
) -> dict[str, Any]:
    """读取一个板块最多七个官方会话的既存PULSE、转移、后继与市场背景。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(target_source_capture_id) is not int
        or target_source_capture_id <= 0
        or not isinstance(profile_revision, str)
        or re.fullmatch(PROFILE_REVISION_PATTERN, profile_revision) is None
        or type(snapshot_id) is not int
        or snapshot_id <= 0
        or board_kind not in _KINDS
        or not isinstance(board_native_id, str)
        or not board_native_id
        or len(board_native_id) > 64
        or not isinstance(calendar_id, str)
        or not calendar_id
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or isinstance(calendar_sessions, (str, bytes))
        or not isinstance(calendar_sessions, Sequence)
        or not 1 <= len(calendar_sessions) <= 12
        or any(type(session) is not date for session in calendar_sessions)
        or tuple(sorted(set(calendar_sessions))) != tuple(calendar_sessions)
        or target_session not in calendar_sessions
        or isinstance(history_sessions, (str, bytes))
        or not isinstance(history_sessions, Sequence)
        or not 1 <= len(history_sessions) <= 7
        or any(type(session) is not date for session in history_sessions)
        or tuple(sorted(set(history_sessions))) != tuple(history_sessions)
        or history_sessions[-1] != target_session
        or any(
            session > target_session or session > research_cutoff
            for session in history_sessions
        )
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_HISTORY_ARGUMENT_INVALID", "板块历史读取键或官方会话范围无效"
        )
    calendar_positions = {
        session: index for index, session in enumerate(calendar_sessions)
    }
    history_positions = [
        calendar_positions.get(session) for session in history_sessions
    ]
    if any(position is None for position in history_positions) or any(
        right != left + 1
        for left, right in zip(history_positions, history_positions[1:])
    ):
        raise SectorPulsePgError(
            "SECTOR_PULSE_HISTORY_NOT_ADJACENT",
            "板块历史范围必须是严格相邻的官方交易会话",
        )

    output_rows: list[dict[str, Any]] = []
    with source_raw._transaction(config, write=False, statement_timeout_ms=20000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            f"""SELECT p.id,p.target_session,p.source_capture_id,
       p.profile_revision,p.catalog_id,c.snapshot_id,p.board_kind,p.board_native_id,
       p.board_name,p.algorithm_contract,p.pulse_state,p.pulse_reason_codes,
       p.calculation_status,p.formal_rotation_state,p.research_mode,p.membership_mode,
       p.price_coordinate,p.snapshot_source_quality,p.source_pool_count,
       p.source_time_quality,p.source_clock_role,p.source_available_at,
       p.snapshot_board_count,p.member_count,p.mapped_count,p.unmapped_count,
       p.dq5_pp_numerator,p.dq5_pp_denominator,p.ret5_target_numerator,
       p.ret5_target_denominator,p.ret5_prior_numerator,p.ret5_prior_denominator,
       p.ret5_common_count,p.ret5_denominator,p.ret5_target_peer_count,
       p.ret5_prior_peer_count,p.ret5_reason_codes,p.breadth_target_numerator,
       p.breadth_target_denominator,p.breadth_prior_numerator,
       p.breadth_prior_denominator,p.breadth_delta1_numerator,
       p.breadth_delta1_denominator,p.breadth_common_count,p.breadth_denominator,
       p.breadth_reason_codes
FROM {_TABLE} p
JOIN tdx_sector_catalog c ON c.id=p.catalog_id
WHERE p.target_session=ANY(%s) AND p.target_session<=%s
  AND p.source_capture_id=%s
  AND p.board_kind=%s AND p.board_native_id=%s
  AND c.snapshot_id=%s AND p.algorithm_contract=%s
ORDER BY p.target_session,p.id""",
            (
                list(history_sessions),
                research_cutoff,
                target_source_capture_id,
                board_kind,
                board_native_id,
                snapshot_id,
                _ALGORITHM,
            ),
        )
        pulse_columns = [column.name for column in cursor.description]
        pulse_by_session: dict[date, list[dict[str, Any]]] = {}
        for values in cursor.fetchall():
            row = dict(zip(pulse_columns, values, strict=True))
            pulse_by_session.setdefault(row["target_session"], []).append(row)

        cursor.execute(
            """WITH capture AS (
    SELECT capture_id,provider,range_start_session,range_end_session,
           price_coordinates,source_time_quality,source_clock_role,source_available_at
    FROM source_captures WHERE capture_id=%s
), request_codes AS (
    SELECT source_code,
           count(*)=2 AND count(DISTINCT adjustment_flag)=2
             AND count(DISTINCT eligible_start_session)=1
             AND count(DISTINCT eligible_end_session)=1
             AND bool_and(request_status IN ('COMPLETED','NO_DATA')
               AND (request_status='NO_DATA' OR completed_through_session>=range_end_session))
               AS complete,
           min(eligible_start_session) AS eligible_start_session,
           max(eligible_end_session) AS eligible_end_session
    FROM source_capture_requests WHERE capture_id=%s GROUP BY source_code
), requested(session_date) AS (
    SELECT unnest(%s::date[])
), expected AS (
    SELECT requested.session_date,
           count(*) FILTER (WHERE requested.session_date BETWEEN eligible_start_session
                                              AND eligible_end_session)::bigint AS expected_count,
           count(*) FILTER (WHERE requested.session_date BETWEEN eligible_start_session
                                              AND eligible_end_session AND complete)::bigint AS complete_count
    FROM requested CROSS JOIN request_codes
    GROUP BY requested.session_date
), raw AS (
    SELECT session_date,count(*)::bigint AS row_count,
           count(DISTINCT source_symbol)::bigint AS symbol_count,
           count(*) FILTER (WHERE price_quality IS NOT NULL)::bigint AS raw_quality_count,
           count(*) FILTER (WHERE qfq_price_quality IS NOT NULL)::bigint AS qfq_quality_count
    FROM source_raw_daily
    WHERE source_capture_id=%s AND session_date=ANY(%s::date[])
    GROUP BY session_date
)
SELECT requested.session_date,capture.capture_id,capture.provider,
       capture.range_start_session,capture.range_end_session,capture.price_coordinates,
       capture.source_time_quality,capture.source_clock_role,capture.source_available_at,
       expected.expected_count,expected.complete_count,raw.row_count,raw.symbol_count,
       raw.raw_quality_count,raw.qfq_quality_count
FROM requested CROSS JOIN capture
JOIN expected USING (session_date)
LEFT JOIN raw USING (session_date)
WHERE capture.provider='BAOSTOCK'
  AND capture.range_start_session<=%s AND capture.range_end_session>=%s
  AND capture.range_end_session<=%s
  AND capture.price_coordinates @> ARRAY['RAW_UNADJUSTED','FORWARD_ADJUSTED']::text[]
ORDER BY requested.session_date""",
            (
                target_source_capture_id,
                target_source_capture_id,
                list(history_sessions),
                target_source_capture_id,
                list(history_sessions),
                history_sessions[0],
                history_sessions[-1],
                research_cutoff,
            ),
        )
        capture_contexts: dict[date, list[dict[str, Any]]] = {}
        for values in cursor.fetchall():
            (
                session,
                capture_id,
                provider,
                range_start,
                range_end,
                coordinates,
                time_quality,
                clock_role,
                available_at,
                expected_count,
                complete_count,
                row_count,
                symbol_count,
                raw_quality_count,
                qfq_quality_count,
            ) = values
            if (
                provider != "BAOSTOCK"
                or time_quality != "NON_PIT_OBSERVED"
                or clock_role != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
                or available_at is not None
                or set(coordinates or ()) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
                or expected_count is None
                or int(expected_count) <= 0
                or int(expected_count) != int(complete_count or 0)
                or int(expected_count) != int(row_count or 0)
                or int(expected_count) != int(symbol_count or 0)
                or int(expected_count) != int(raw_quality_count or 0)
                or int(expected_count) != int(qfq_quality_count or 0)
            ):
                continue
            capture_contexts.setdefault(session, []).append(
                {
                    "source_capture_id": int(capture_id),
                    "source_pool_count": int(expected_count),
                    "source_time_quality": time_quality,
                    "source_clock_role": clock_role,
                    "source_available_at": available_at,
                    "range_start_session": range_start,
                    "range_end_session": range_end,
                }
            )

        def unique_capture(session: date) -> dict[str, Any] | None:
            candidates = capture_contexts.get(session, [])
            if len(candidates) != 1 or candidates[0]["source_pool_count"] <= 0:
                return None
            return candidates[0]

        for session in history_sessions:
            candidates = pulse_by_session.get(session, [])
            if not candidates:
                capture = unique_capture(session)
                output_rows.append(
                    {
                        "target_session": session.isoformat(),
                        "status": "NOT_COMPUTED",
                        "reason_codes": [
                            "PULSE_FACT_NOT_SAVED_FOR_EXACT_SNAPSHOT_AND_PROFILE"
                        ],
                        "identity": (
                            {
                                "source_capture_id": capture["source_capture_id"],
                                "profile_revision": None,
                            }
                            if capture
                            else None
                        ),
                        "source_pool_count": capture["source_pool_count"]
                        if capture
                        else None,
                        "market": None,
                    }
                )
                continue
            if len(candidates) != 1:
                capture = unique_capture(session)
                output_rows.append(
                    {
                        "target_session": session.isoformat(),
                        "status": "INCOMPLETE",
                        "reason_codes": ["PULSE_FACT_SCOPE_AMBIGUOUS"],
                        "identity": (
                            {
                                "source_capture_id": capture["source_capture_id"],
                                "profile_revision": None,
                            }
                            if capture
                            else None
                        ),
                        "source_pool_count": capture["source_pool_count"]
                        if capture
                        else None,
                        "market": None,
                    }
                )
                continue
            row = candidates[0]
            reasons = []
            capture_candidates = capture_contexts.get(session, [])
            capture = unique_capture(session)
            if (
                row["target_session"] != session
                or int(row["snapshot_id"]) != snapshot_id
                or not isinstance(row["profile_revision"], str)
                or re.fullmatch(
                    PROFILE_REVISION_PATTERN, row["profile_revision"].strip()
                )
                is None
                or row["board_kind"] != board_kind
                or row["board_native_id"] != board_native_id
                or row["algorithm_contract"] != _ALGORITHM
                or row["pulse_state"] not in {"TRUE", "FALSE", "UNKNOWN"}
                or row["calculation_status"] != "COMPLETE"
                or row["formal_rotation_state"] != "NOT_EVALUATED"
                or row["research_mode"] != "NON_PIT_RETROSPECTIVE"
                or row["membership_mode"] != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                or row["price_coordinate"] != "FORWARD_ADJUSTED"
                or int(row["source_pool_count"]) <= 0
                or int(row["snapshot_board_count"]) <= 0
                or int(row["member_count"])
                != int(row["mapped_count"]) + int(row["unmapped_count"])
            ):
                reasons.append("PULSE_TYPED_FACT_CONTRACT_MISMATCH")
            if (
                session == target_session
                and int(row["source_capture_id"]) != target_source_capture_id
            ):
                reasons.append("PULSE_TARGET_SOURCE_CAPTURE_MISMATCH")
            if (
                session == target_session
                and row["profile_revision"].strip() != profile_revision
            ):
                reasons.append("PULSE_TARGET_PROFILE_REVISION_MISMATCH")
            if (
                capture is None
                or capture["source_capture_id"] != int(row["source_capture_id"])
                or capture["source_pool_count"] != int(row["source_pool_count"])
                or row["source_time_quality"] != capture["source_time_quality"]
                or row["source_clock_role"] != capture["source_clock_role"]
                or row["source_available_at"] != capture["source_available_at"]
                or len(capture_candidates) != 1
            ):
                reasons.append("PULSE_SOURCE_CAPTURE_CONTEXT_NOT_CLOSED")
            output_rows.append(
                {
                    "target_session": session.isoformat(),
                    "status": "AVAILABLE" if not reasons else "INCOMPLETE",
                    "reason_codes": reasons,
                    "identity": {
                        "pulse_fact_id": int(row["id"]),
                        "catalog_id": int(row["catalog_id"]),
                        "snapshot_id": int(row["snapshot_id"]),
                        "source_capture_id": int(row["source_capture_id"]),
                        "profile_revision": row["profile_revision"].strip(),
                    },
                    "pulse": _pulse_payload(row),
                    "transition": None,
                    "followups": [],
                    "market": None,
                    "source_pool_count": int(row["source_pool_count"]),
                }
            )

        available_by_session = {
            date.fromisoformat(row["target_session"]): row
            for row in output_rows
            if row["status"] == "AVAILABLE"
        }
        ordered_pairs = [
            (previous, current)
            for previous, current in zip(history_sessions, history_sessions[1:])
            if previous in available_by_session and current in available_by_session
        ]
        transitions_by_pair: dict[tuple[int, int], list[dict[str, Any]]] = {}
        if ordered_pairs:
            previous_ids = [
                available_by_session[previous]["identity"]["pulse_fact_id"]
                for previous, _ in ordered_pairs
            ]
            target_ids = [
                available_by_session[current]["identity"]["pulse_fact_id"]
                for _, current in ordered_pairs
            ]
            cursor.execute(
                """SELECT previous_pulse_id,target_pulse_id,calendar_id,
       calendar_revision,previous_pulse_state,target_pulse_state,transition_state,
       previous_reason_codes,target_reason_codes,calculation_status,research_mode,
       price_coordinate,formal_rotation_state
FROM source_sector_pulse_adjacent_transitions
WHERE previous_pulse_id=ANY(%s) AND target_pulse_id=ANY(%s)
  AND transition_contract=%s AND calendar_id=%s AND calendar_revision=%s
ORDER BY previous_pulse_id,target_pulse_id,id""",
                (
                    previous_ids,
                    target_ids,
                    "SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1",
                    calendar_id,
                    calendar_revision,
                ),
            )
            transition_columns = [column.name for column in cursor.description]
            for values in cursor.fetchall():
                transition = dict(zip(transition_columns, values, strict=True))
                pair = (
                    int(transition["previous_pulse_id"]),
                    int(transition["target_pulse_id"]),
                )
                transitions_by_pair.setdefault(pair, []).append(transition)

        for previous, current in zip(history_sessions, history_sessions[1:]):
            target_row = next(
                row
                for row in output_rows
                if row["target_session"] == current.isoformat()
            )
            previous_row = available_by_session.get(previous)
            current_row = available_by_session.get(current)
            if previous_row is None or current_row is None:
                target_row["transition"] = {
                    "status": "NOT_COMPUTED",
                    "reason_codes": ["PULSE_TRANSITION_PARENT_FACT_NOT_AVAILABLE"],
                }
                continue
            pair = (
                previous_row["identity"]["pulse_fact_id"],
                current_row["identity"]["pulse_fact_id"],
            )
            matches = transitions_by_pair.get(pair, [])
            if not matches:
                target_row["transition"] = {
                    "status": "NOT_COMPUTED",
                    "reason_codes": [
                        "PULSE_TRANSITION_FACT_NOT_SAVED_FOR_EXACT_PARENTS"
                    ],
                }
                continue
            if len(matches) != 1:
                target_row["transition"] = {
                    "status": "INCOMPLETE",
                    "reason_codes": ["PULSE_TRANSITION_PARENT_KEY_AMBIGUOUS"],
                }
                continue
            transition = matches[0]
            valid_transition = (
                transition["calendar_id"] == calendar_id
                and transition["calendar_revision"] == calendar_revision
                and transition["previous_pulse_state"] == previous_row["pulse"]["state"]
                and transition["target_pulse_state"] == current_row["pulse"]["state"]
                and transition["calculation_status"] == "COMPLETE"
                and transition["research_mode"] == "NON_PIT_RETROSPECTIVE"
                and transition["price_coordinate"] == "FORWARD_ADJUSTED"
                and transition["formal_rotation_state"] == "NOT_EVALUATED"
            )
            target_row["transition"] = {
                "status": "AVAILABLE" if valid_transition else "INCOMPLETE",
                "transition_state": transition["transition_state"]
                if valid_transition
                else None,
                "previous_state": transition["previous_pulse_state"],
                "target_state": transition["target_pulse_state"],
                "reason_codes": sorted(
                    set(transition["previous_reason_codes"] or [])
                    | set(transition["target_reason_codes"] or [])
                )
                if valid_transition
                else ["PULSE_TRANSITION_TYPED_FACT_CONTRACT_MISMATCH"],
                "formal_rotation_state": transition["formal_rotation_state"],
            }

        available_ids = [
            row["identity"]["pulse_fact_id"]
            for row in output_rows
            if row["status"] == "AVAILABLE"
        ]
        followups_by_key: dict[tuple[int, int], list[dict[str, Any]]] = {}
        if available_ids:
            cursor.execute(
                """SELECT pulse_fact_id,horizon_sessions,target_session,
       target_source_capture_id,fixed_member_denominator,
       observed_member_count,unknown_member_count,equal_weight_return_numerator,
       equal_weight_return_denominator,outcome_status,reason_codes,research_mode,
       membership_mode,price_coordinate,calendar_id,calendar_revision
FROM source_sector_pulse_followup_facts
WHERE pulse_fact_id=ANY(%s) AND target_session<=%s
  AND horizon_sessions=ANY(%s) AND followup_contract=%s
  AND calendar_id=%s AND calendar_revision=%s
ORDER BY pulse_fact_id,horizon_sessions,target_session,id""",
                (
                    available_ids,
                    research_cutoff,
                    [1, 3, 5],
                    "SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1",
                    calendar_id,
                    calendar_revision,
                ),
            )
            followup_columns = [column.name for column in cursor.description]
            for values in cursor.fetchall():
                followup = dict(zip(followup_columns, values, strict=True))
                key = (
                    int(followup["pulse_fact_id"]),
                    int(followup["horizon_sessions"]),
                )
                followups_by_key.setdefault(key, []).append(followup)

        for row in output_rows:
            if row["status"] != "AVAILABLE":
                continue
            session = date.fromisoformat(row["target_session"])
            session_index = calendar_positions[session]
            horizon_results = []
            for horizon in (1, 3, 5):
                future_index = session_index + horizon
                due_session = (
                    calendar_sessions[future_index]
                    if future_index < len(calendar_sessions)
                    else None
                )
                if due_session is None:
                    horizon_results.append(
                        {
                            "horizon": horizon,
                            "status": "NOT_COMPUTED",
                            "reason_codes": ["OFFICIAL_HORIZON_SESSION_UNAVAILABLE"],
                        }
                    )
                    continue
                if due_session > research_cutoff:
                    horizon_results.append(
                        {
                            "horizon": horizon,
                            "status": "NOT_DUE",
                            "reason_codes": ["WAITING_FOR_FUTURE_OFFICIAL_SESSION"],
                        }
                    )
                    continue
                pulse_id = row["identity"]["pulse_fact_id"]
                candidates = followups_by_key.get((pulse_id, horizon), [])
                exact = [
                    candidate
                    for candidate in candidates
                    if candidate["target_session"] == due_session
                ]
                if not exact:
                    horizon_results.append(
                        {
                            "horizon": horizon,
                            "target_session": due_session.isoformat(),
                            "status": "INCOMPLETE" if candidates else "NOT_COMPUTED",
                            "reason_codes": [
                                "FOLLOWUP_FACT_TARGET_SESSION_MISMATCH"
                                if candidates
                                else "FOLLOWUP_FACT_NOT_SAVED_FOR_EXACT_HORIZON"
                            ],
                        }
                    )
                    continue
                if len(exact) != 1:
                    horizon_results.append(
                        {
                            "horizon": horizon,
                            "target_session": due_session.isoformat(),
                            "status": "INCOMPLETE",
                            "reason_codes": ["FOLLOWUP_FACT_KEY_AMBIGUOUS"],
                        }
                    )
                    continue
                fact = exact[0]
                denominator = (
                    int(fact["equal_weight_return_denominator"])
                    if fact["equal_weight_return_denominator"] is not None
                    else None
                )
                numerator = (
                    int(fact["equal_weight_return_numerator"])
                    if fact["equal_weight_return_numerator"] is not None
                    else None
                )
                expected_member_count = row["pulse"]["member_count"]
                reasons = list(fact["reason_codes"] or [])
                valid = (
                    fact["calendar_id"] == calendar_id
                    and fact["calendar_revision"] == calendar_revision
                    and fact["research_mode"] == "NON_PIT_RETROSPECTIVE"
                    and fact["membership_mode"]
                    == "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
                    and fact["price_coordinate"] == "FORWARD_ADJUSTED"
                    and int(fact["fixed_member_denominator"]) == expected_member_count
                    and int(fact["observed_member_count"])
                    + int(fact["unknown_member_count"])
                    == int(fact["fixed_member_denominator"])
                    and (denominator is None or denominator > 0)
                )
                outcome_status = fact["outcome_status"] if valid else "INCOMPLETE"
                if not valid:
                    reasons = ["PULSE_FOLLOWUP_TYPED_FACT_CONTRACT_MISMATCH"]
                return_fraction = (
                    Fraction(numerator, denominator)
                    if numerator is not None
                    and denominator is not None
                    and denominator > 0
                    else None
                )
                return_percent = None
                if return_fraction is not None:
                    with localcontext() as decimal_context:
                        decimal_context.prec = 50
                        return_percent = str(
                            (
                                Decimal(return_fraction.numerator)
                                * Decimal(100)
                                / Decimal(return_fraction.denominator)
                            ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                        )
                horizon_results.append(
                    {
                        "horizon": horizon,
                        "target_session": due_session.isoformat(),
                        "status": outcome_status,
                        "fixed_member_denominator": int(
                            fact["fixed_member_denominator"]
                        ),
                        "observed_member_count": int(fact["observed_member_count"]),
                        "unknown_member_count": int(fact["unknown_member_count"]),
                        "equal_weight_return": str(return_fraction)
                        if return_fraction is not None
                        else None,
                        "return_percent_4dp": return_percent,
                        "target_source_capture_id": int(
                            fact["target_source_capture_id"]
                        ),
                        "reason_codes": reasons,
                    }
                )
            row["followups"] = horizon_results

        for row in output_rows:
            row["market"] = _read_market_context(
                cursor,
                target_session=date.fromisoformat(row["target_session"]),
                research_cutoff=research_cutoff,
                source_identity=row.get("identity"),
                source_pool_count=row.get("source_pool_count"),
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
            )

    complete_rows = sum(row["status"] == "AVAILABLE" for row in output_rows)
    overall_status = (
        "AVAILABLE"
        if complete_rows == len(output_rows)
        else "PARTIAL"
        if complete_rows
        else "NOT_COMPUTED"
    )
    return {
        "status": overall_status,
        "reason_codes": []
        if overall_status == "AVAILABLE"
        else ["PULSE_HISTORY_SESSION_FACTS_PARTIAL"],
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "board": {
            "board_kind": board_kind,
            "board_native_id": board_native_id,
            "snapshot_id": snapshot_id,
            "profile_revision": profile_revision,
            "algorithm_contract": _ALGORITHM,
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            "price_coordinate": "FORWARD_ADJUSTED",
            "formal_rotation_state": "NOT_EVALUATED",
        },
        "sessions": output_rows,
        "execution": {
            "transaction": "REPEATABLE_READ_READ_ONLY",
            "database_write_attempted": False,
            "dml_executed": False,
            "ddl_executed": False,
            "raw_rows_read": False,
            "history_session_count": len(history_sessions),
            "whole_market_recalculated": False,
        },
    }


__all__ = ["read_source_sector_pulse_history"]
