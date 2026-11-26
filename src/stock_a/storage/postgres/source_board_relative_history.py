"""读取单股、单板块的有界跨会话RPS输入。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.research.source_market_breadth import SOURCE_MARKET_BREADTH_CONTRACT
from stock_a.storage.postgres.baostock_price_facts import (
    read_baostock_capture_roster,
)
from stock_a.storage.postgres import source_raw


class SourceBoardRelativeHistoryPgError(ValueError):
    """板内相对位置历史读取拒绝。"""


def _symbols(cursor: Any, sql: str, params: tuple[Any, ...]) -> list[str]:
    cursor.execute(sql, params)
    return [str(row[0]) for row in cursor.fetchall()]


def read_source_board_relative_history_inputs(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    source_symbol: str,
    membership_snapshot_id: int,
    board_kind: str,
    board_native_id: str,
    producer_version: str,
    sessions: Sequence[date],
) -> dict[str, Any]:
    """读取固定成员快照内最多七个官方会话的RPS与来源上下文。"""
    session_days = tuple(sessions)
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(membership_snapshot_id) is not int
        or membership_snapshot_id <= 0
        or not isinstance(source_symbol, str)
        or len(source_symbol) != 9
        or source_symbol[:3] not in {"SH.", "SZ."}
        or not source_symbol[3:].isdigit()
        or not isinstance(board_kind, str)
        or board_kind not in {"INDUSTRY", "CONCEPT"}
        or not isinstance(board_native_id, str)
        or not board_native_id
        or not isinstance(producer_version, str)
        or not producer_version
        or len(session_days) < 1
        or len(session_days) > 7
        or any(type(item) is not date for item in session_days)
        or any(item > research_cutoff for item in session_days)
        or tuple(sorted(set(session_days))) != session_days
    ):
        raise SourceBoardRelativeHistoryPgError("板内RPS历史读取键无效")
    target_index = (
        session_days.index(target_session) if target_session in session_days else -1
    )
    if target_index not in (0, 1):
        raise SourceBoardRelativeHistoryPgError(
            "T0日期须位于单股板内历史窗口首位或紧随一个官方前态"
        )

    capture = read_baostock_capture_roster(
        config,
        source_capture_id=source_capture_id,
        cutoff=research_cutoff,
    )
    if capture.get("status") != "AVAILABLE":
        return {
            "status": capture.get("status", "INCOMPLETE"),
            "reason": "所选BaoStock捕获目录没有闭合",
            "days": [],
            "execution": _execution(len(session_days)),
        }
    capture_start = capture.get("range_start_session")
    capture_end = capture.get("range_end_session")
    capture_symbols = {
        row.get("source_symbol"): row for row in capture.get("symbols", [])
    }
    symbol_capture = capture_symbols.get(source_symbol)
    qfq_request = (
        symbol_capture.get("coordinates", {}).get("FORWARD_ADJUSTED")
        if isinstance(symbol_capture, Mapping)
        else None
    )
    qfq_start = (
        qfq_request.get("eligible_start_session")
        if isinstance(qfq_request, Mapping)
        else None
    )
    qfq_end = (
        qfq_request.get("eligible_end_session")
        if isinstance(qfq_request, Mapping)
        else None
    )
    qfq_completed = (
        qfq_request.get("completed_through_session")
        if isinstance(qfq_request, Mapping)
        else None
    )
    if (
        capture.get("provider") != "BAOSTOCK"
        or capture.get("source_capture_id") != source_capture_id
        or capture.get("source_time_quality") != "NON_PIT_OBSERVED"
        or set(capture.get("price_coordinates") or ())
        != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or not isinstance(capture_start, str)
        or capture_start > session_days[0].isoformat()
        or not isinstance(capture_end, str)
        or capture_end < session_days[-1].isoformat()
        or not isinstance(symbol_capture, Mapping)
        or symbol_capture.get("request_complete") is not True
        or not isinstance(qfq_request, Mapping)
        or qfq_request.get("price_coordinate") != "FORWARD_ADJUSTED"
        or qfq_request.get("request_status") not in {"COMPLETED", "NO_DATA"}
        or not isinstance(qfq_start, str)
        or qfq_start > session_days[0].isoformat()
        or not isinstance(qfq_end, str)
        or qfq_end < session_days[-1].isoformat()
        or not isinstance(qfq_completed, str)
        or qfq_completed < session_days[-1].isoformat()
    ):
        return {
            "status": "NOT_COMPARABLE",
            "reason": "所选证券缺少覆盖历史窗口的同捕获BaoStock QFQ请求",
            "days": [],
            "membership_snapshot_id": membership_snapshot_id,
            "execution": _execution(len(session_days)),
        }
    source_versions = {
        session: {
            "status": "AVAILABLE",
            "source_capture_id": source_capture_id,
            "price_coordinate": "FORWARD_ADJUSTED",
            "capture_uuid": capture.get("capture_uuid"),
        }
        for session in session_days
    }

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT DISTINCT membership_snapshot_id,catalog_id,
                      membership_mode,research_mode,price_coordinate,
                      hypothesis_version
               FROM source_t0_early_observation_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND source_symbol=%s
                 AND board_kind=%s AND board_native_id=%s
                 AND membership_snapshot_id=%s
               ORDER BY membership_snapshot_id,catalog_id""",
            (
                target_session,
                source_capture_id,
                source_symbol,
                board_kind,
                board_native_id,
                membership_snapshot_id,
            ),
        )
        selected_contexts = cursor.fetchall()
        if len(selected_contexts) != 1:
            return {
                "status": "NO_STORED_FACT" if not selected_contexts else "AMBIGUOUS",
                "reason": "所选T0没有唯一的板块成员快照和算法上下文",
                "days": [],
                "execution": _execution(len(session_days)),
            }
        (
            membership_snapshot_id,
            catalog_id,
            membership_mode,
            research_mode,
            price_coordinate,
            hypothesis_version,
        ) = selected_contexts[0]
        if (
            type(membership_snapshot_id) is not int
            or type(catalog_id) is not int
            or membership_mode != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
            or research_mode != "NON_PIT_EXPLORATORY"
            or price_coordinate != "FORWARD_ADJUSTED"
        ):
            return {
                "status": "NOT_COMPARABLE",
                "reason": "板块成员、研究模式或价格坐标不符合描述性比较合同",
                "days": [],
                "membership_snapshot_id": membership_snapshot_id,
                "catalog_id": catalog_id,
                "execution": _execution(len(session_days)),
            }

        cursor.execute(
            """SELECT DISTINCT m.source_symbol
               FROM tdx_sector_catalog c
               JOIN tdx_sector_members m ON m.catalog_id=c.id
                 AND m.board_kind=c.board_kind
                 AND m.board_native_id=c.board_native_id
               WHERE c.id=%s AND c.snapshot_id=%s AND c.board_kind=%s
                 AND c.board_native_id=%s
               ORDER BY m.source_symbol """,
            (catalog_id, membership_snapshot_id, board_kind, board_native_id),
        )
        snapshot_members = [str(row[0]) for row in cursor.fetchall()]
        if not snapshot_members or source_symbol not in snapshot_members:
            return {
                "status": "NOT_COMPARABLE",
                "reason": "目标股票不在所选固定成员快照中",
                "days": [],
                "membership_snapshot_id": membership_snapshot_id,
                "catalog_id": catalog_id,
                "execution": _execution(len(session_days)),
            }

        days = []
        for session in session_days:
            source = source_versions[session]
            source_capture = source.get("source_capture_id")
            day = {
                "target_session": session,
                "source_status": source.get("status", "UNKNOWN"),
                "source_capture_id": source_capture,
                "source_raw_row_count": source.get("raw_row_count"),
                "source_expected_row_count": source.get("capture_expected_row_count"),
                "membership_snapshot_id": membership_snapshot_id,
                "catalog_id": catalog_id,
                "membership_mode": membership_mode,
                "research_mode": research_mode,
                "price_coordinate": price_coordinate,
                "hypothesis_version": hypothesis_version,
                "snapshot_member_count": len(snapshot_members),
                "snapshot_members": snapshot_members,
                "source_pool_symbols": [],
                "rps5_member_symbols": [],
                "rps20_member_symbols": [],
                "seed_scope": None,
                "t0_observation_status": "NO_STORED_FACT",
                "t0_registration_count": 0,
                "t0_cases": [],
                "pulse_status": "NOT_COMPUTED",
                "pulse_state": "NOT_COMPUTED",
                "transition_status": "NOT_COMPUTED",
                "transition_state": "NOT_COMPUTED",
                "rule_status": "NOT_COMPUTED",
                "rule_states": {},
                "rule_details": {},
                "market_context": {
                    "breadth_status": "NOT_COMPUTED",
                    "participation_status": "NOT_COMPUTED",
                },
            }
            if (
                source.get("status") != "AVAILABLE"
                or type(source_capture) is not int
                or source_capture != source_capture_id
            ):
                day["reason"] = "该会话没有唯一且闭合的已存Raw捕获"
                days.append(day)
                continue

            capture_id = source_capture_id
            cursor.execute(
                """SELECT DISTINCT producer_contract,
                          producer_version,history_session_count,calendar_id,
                          calendar_revision,research_mode,price_coordinate,
                          history_window_start,history_window_end,
                          source_time_quality,source_clock_role
                   FROM source_pool_seed_day_facts
                   WHERE target_session=%s AND source_capture_id=%s
                     AND source_symbol=%s
                     AND producer_version=%s
                   ORDER BY producer_contract,producer_version""",
                (session, capture_id, source_symbol, producer_version),
            )
            scope_rows = cursor.fetchall()
            if len(scope_rows) == 1:
                names = [item.name for item in cursor.description]
                day["seed_scope"] = dict(zip(names, scope_rows[0], strict=True))
            elif len(scope_rows) > 1:
                day["seed_scope_status"] = "AMBIGUOUS"
                day["reason"] = "同日目标股票存在多个Seed生产版本"

            day["source_pool_symbols"] = _symbols(
                cursor,
                """SELECT source_symbol FROM source_raw_daily
                   WHERE session_date=%s AND source_capture_id=%s
                   ORDER BY source_symbol """,
                (session, capture_id),
            )

            scope = day["seed_scope"]
            if scope is not None:
                cursor.execute(
                    """SELECT m.source_symbol,
                              s.rps5_numerator,s.rps5_denominator,s.rps5_quality,
                              s.rps20_numerator,s.rps20_denominator,s.rps20_quality
                       FROM tdx_sector_members m
                       JOIN source_raw_daily r ON r.source_symbol=m.source_symbol
                         AND r.session_date=%s AND r.source_capture_id=%s
                       LEFT JOIN source_pool_seed_day_facts s
                         ON s.source_symbol=m.source_symbol
                        AND s.target_session=%s
                        AND s.source_capture_id=%s
                        AND s.producer_contract=%s
                        AND s.producer_version=%s
                        AND s.history_session_count=%s
                        AND s.calendar_id=%s
                        AND s.calendar_revision=%s
                        AND s.research_mode=%s
                        AND s.price_coordinate=%s
                       WHERE m.catalog_id=%s AND m.board_kind=%s
                         AND m.board_native_id=%s
                       ORDER BY m.source_symbol """,
                    (
                        session,
                        capture_id,
                        session,
                        capture_id,
                        scope["producer_contract"],
                        scope["producer_version"],
                        scope["history_session_count"],
                        scope["calendar_id"],
                        scope["calendar_revision"],
                        scope["research_mode"],
                        scope["price_coordinate"],
                        catalog_id,
                        board_kind,
                        board_native_id,
                    ),
                )
                peer_rows = cursor.fetchall()
                day["rps5_member_symbols"] = [
                    str(row[0])
                    for row in peer_rows
                    if row[3] == "OBSERVED"
                    and row[1] is not None
                    and row[2] is not None
                    and row[2] > 0
                ]
                day["rps20_member_symbols"] = [
                    str(row[0])
                    for row in peer_rows
                    if row[6] == "OBSERVED"
                    and row[4] is not None
                    and row[5] is not None
                    and row[5] > 0
                ]

            cursor.execute(
                """SELECT count(DISTINCT enrollment_id),
                          COALESCE(array_agg(DISTINCT research_case
                              ORDER BY research_case)
                              FILTER (WHERE research_case IS NOT NULL), ARRAY[]::text[])
                          ,COALESCE(array_agg(DISTINCT alignment_state
                              ORDER BY alignment_state)
                              FILTER (WHERE alignment_state IS NOT NULL), ARRAY[]::text[])
                   FROM source_t0_early_observation_facts
                   WHERE target_session=%s AND source_capture_id=%s
                     AND source_symbol=%s
                     AND board_kind=%s AND board_native_id=%s""",
                (
                    session,
                    capture_id,
                    source_symbol,
                    board_kind,
                    board_native_id,
                ),
            )
            registration_count, cases, alignments = cursor.fetchone()
            day["t0_registration_count"] = int(registration_count or 0)
            day["t0_observation_status"] = (
                "AVAILABLE" if day["t0_registration_count"] else "NO_STORED_FACT"
            )
            day["t0_cases"] = list(cases or [])
            day["t0_alignment_states"] = list(alignments or [])

            cursor.execute(
                """SELECT p.pulse_state,p.calculation_status,p.research_mode,
                          p.membership_mode,p.price_coordinate,p.source_time_quality,
                          p.source_clock_role,t.transition_state,
                          t.calculation_status
                   FROM source_sector_cross_period_pulse_facts p
                   LEFT JOIN source_sector_pulse_adjacent_transitions t
                     ON t.target_pulse_id=p.id
                   WHERE p.target_session=%s AND p.board_kind=%s
                     AND p.board_native_id=%s AND p.catalog_id=%s
                     AND p.source_capture_id=%s
                   ORDER BY p.id""",
                (
                    session,
                    board_kind,
                    board_native_id,
                    catalog_id,
                    capture_id,
                ),
            )
            pulse_rows = cursor.fetchall()
            if len(pulse_rows) == 1:
                pulse = pulse_rows[0]
                day["pulse_status"] = pulse[1]
                day["pulse_state"] = pulse[0]
                day["transition_status"] = pulse[8] or "NOT_COMPUTED"
                day["transition_state"] = pulse[7] or "NOT_COMPUTED"
                day["pulse_context"] = {
                    "research_mode": pulse[2],
                    "membership_mode": pulse[3],
                    "price_coordinate": pulse[4],
                    "source_time_quality": pulse[5],
                    "source_clock_role": pulse[6],
                    "profile_revision": None,
                }
            elif len(pulse_rows) > 1:
                day["pulse_status"] = "AMBIGUOUS"
                day["pulse_state"] = "UNKNOWN"
                day["transition_status"] = "AMBIGUOUS"
                day["transition_state"] = "UNKNOWN"

            cursor.execute(
                """SELECT rule_id,state,rule_version,scope,
                          frozen_line_numerator::text,
                          frozen_line_denominator::text,frozen_line_source
                   FROM source_rule_evaluations
                   WHERE target_session=%s AND source_capture_id=%s
                     AND source_symbol=%s
                     AND rule_id IN ('LAUNCH_CONFIRM','RECOVERY_TURN')
                   ORDER BY rule_id,rule_version,scope""",
                (session, capture_id, source_symbol),
            )
            rule_rows = cursor.fetchall()
            rule_details: dict[str, dict[str, Any]] = {}
            for rule_row in rule_rows:
                rule_id = str(rule_row[0])
                if rule_id in rule_details:
                    rule_details[rule_id] = {"state": "AMBIGUOUS"}
                    continue
                rule_details[rule_id] = {
                    "state": str(rule_row[1]),
                    "rule_version": str(rule_row[2]),
                    "scope": str(rule_row[3]),
                    "frozen_line_numerator": rule_row[4],
                    "frozen_line_denominator": rule_row[5],
                    "frozen_line_source": rule_row[6],
                }
            day["rule_details"] = rule_details
            day["rule_states"] = {
                rule_id: detail["state"] for rule_id, detail in rule_details.items()
            }
            day["rule_status"] = (
                "AVAILABLE"
                if set(rule_details) == {"LAUNCH_CONFIRM", "RECOVERY_TURN"}
                and all(
                    detail.get("state") != "AMBIGUOUS"
                    for detail in rule_details.values()
                )
                else "INCOMPLETE"
                if rule_details
                else "NOT_COMPUTED"
            )

            cursor.execute(
                """SELECT id,calendar_revision,contract_version,source_count,
                          eligible_count,target_present_count,target_missing_count,
                          read_failed_count,history_row_count,session_t1,session_t3
                   FROM source_market_breadth_days
                   WHERE target_session=%s AND source_capture_id=%s
                     AND contract_version=%s
                   ORDER BY id""",
                (session, capture_id, SOURCE_MARKET_BREADTH_CONTRACT),
            )
            breadth_rows = cursor.fetchall()
            if len(breadth_rows) == 1:
                (
                    breadth_id,
                    breadth_calendar_revision,
                    breadth_contract,
                    breadth_source_count,
                    breadth_eligible_count,
                    breadth_target_present_count,
                    breadth_target_missing_count,
                    breadth_read_failed_count,
                    breadth_history_row_count,
                    breadth_session_t1,
                    breadth_session_t3,
                ) = breadth_rows[0]
                cursor.execute(
                    """SELECT classification,count(*)::integer
                       FROM source_market_breadth_symbols
                       WHERE breadth_day_id=%s
                       GROUP BY classification ORDER BY classification""",
                    (breadth_id,),
                )
                breadth_counts = {
                    str(name): int(count) for name, count in cursor.fetchall()
                }
                positive_count = breadth_counts.get("COMPARABLE_GAIN", 0) + (
                    breadth_counts.get("COMPARABLE_BOTH_POSITIVE", 0)
                )
                nonpositive_count = breadth_counts.get("COMPARABLE_LOSS", 0) + (
                    breadth_counts.get("COMPARABLE_BOTH_NONPOSITIVE", 0)
                )
                day["market_context"] = {
                    "breadth_status": "AVAILABLE",
                    "breadth_source_count": int(breadth_source_count),
                    "breadth_eligible_count": int(breadth_eligible_count),
                    "breadth_target_present_count": int(breadth_target_present_count),
                    "breadth_target_missing_count": int(breadth_target_missing_count),
                    "breadth_read_failed_count": int(breadth_read_failed_count),
                    "breadth_history_row_count": int(breadth_history_row_count),
                    "breadth_positive_count": positive_count,
                    "breadth_nonpositive_count": nonpositive_count,
                    "breadth_unknown_count": breadth_counts.get("UNKNOWN", 0),
                    "breadth_classification_counts": breadth_counts,
                    "breadth_contract": breadth_contract,
                    "calendar_revision": breadth_calendar_revision,
                    "comparison_session_1": breadth_session_t1.isoformat()
                    if breadth_session_t1
                    else None,
                    "comparison_session_3": breadth_session_t3.isoformat()
                    if breadth_session_t3
                    else None,
                    "membership_mode": membership_mode,
                    "research_mode": "NON_PIT_EXPLORATORY",
                    "price_coordinate": "FORWARD_ADJUSTED",
                }
            elif len(breadth_rows) > 1:
                day["market_context"] = {
                    "breadth_status": "AMBIGUOUS",
                    "participation_status": "NOT_COMPUTED",
                }

            cursor.execute(
                """SELECT status,classification,amount_unit_status,source_pool_count,
                          assessable_count,profile_unknown_count,profile_missing_count,
                          median_numerator::text,median_denominator::text,
                          median_value::text,calendar_revision,price_coordinate,pit_status
                   FROM source_market_participation_days
                   WHERE target_session=%s AND source_capture_id=%s
                   ORDER BY id""",
                (session, capture_id),
            )
            participation_rows = cursor.fetchall()
            if len(participation_rows) == 1:
                participation = participation_rows[0]
                day["market_context"]["participation_status"] = participation[0]
                day["market_context"]["participation"] = {
                    "classification": participation[1],
                    "amount_unit_status": participation[2],
                    "source_pool_count": participation[3],
                    "assessable_count": participation[4],
                    "profile_unknown_count": participation[5],
                    "profile_missing_count": participation[6],
                    "median_numerator": participation[7],
                    "median_denominator": participation[8],
                    "median_value": participation[9],
                    "calendar_revision": participation[10],
                    "price_coordinate": participation[11],
                    "pit_status": participation[12],
                }
            elif len(participation_rows) > 1:
                day["market_context"]["participation_status"] = "AMBIGUOUS"
            days.append(day)

    return {
        "status": "AVAILABLE",
        "membership_snapshot_id": membership_snapshot_id,
        "catalog_id": catalog_id,
        "membership_mode": membership_mode,
        "research_mode": research_mode,
        "price_coordinate": price_coordinate,
        "hypothesis_version": hypothesis_version,
        "snapshot_member_count": len(snapshot_members),
        "days": days,
        "execution": _execution(len(session_days)),
    }


def _execution(session_count: int) -> dict[str, Any]:
    return {
        "transaction": "只读；BaoStock捕获目录与每日板内RPS由现役Reader读取",
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "official_sessions_requested": session_count,
        "single_symbol_only": True,
        "single_board_only": True,
    }
