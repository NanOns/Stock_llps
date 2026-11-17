"""来源级观察段的离线PG生产者与只读分页。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.domain.source_research_observation import Observation, advance_observation
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.research_enrollment import (
    _contextual_rule_observation,
    _read_rule_day_contexts,
    _rule_context_comparable,
    _enrollment_forward_paths,
)
from stock_a.storage.postgres.source_raw import _transaction


PRODUCER_CONTRACT = "SOURCE_RESEARCH_OBSERVATION_SEGMENTS_R1"
PRODUCER_VERSION = "R1"
PRICE_COORDINATE = "RAW_UNADJUSTED"
IDENTITY_STATUS = "SOURCE_SYMBOL_ONLY"
RESEARCH_SCOPE = "NON_PIT_EXPLORATORY"

_SEGMENT_COLUMNS = (
    "source_enrollment_id",
    "source_package_id",
    "source_symbol",
    "rule_id",
    "rule_version",
    "t0_session",
    "anchor_rule_fact_id",
    "anchor_capture_id",
    "enrollment_kind",
    "parent_enrollment_id",
    "observation_session",
    "previous_session",
    "target_capture_id",
    "evidence_rule_fact_id",
    "linked_enrollment_id",
    "observation_state",
    "lifecycle",
    "event_code",
    "quality_status",
    "comparability_status",
    "reason_code",
    "profile_revision",
    "history_session_count",
    "d1_history_session_count",
    "event_window_start",
    "calendar_revision",
    "source_time_quality",
    "source_clock_role",
    "scope",
    "price_coordinate",
    "identity_status",
    "producer_contract",
    "producer_version",
)


class ObservationSegmentConflict(ValueError):
    """来源登记或同一观察业务键存在冲突，拒绝覆盖。"""

    preserve_transaction_error = True


def _validate_producer_arguments(
    *, research_cutoff: date, source_package_id: int, rule_ids: Sequence[str] | None
) -> None:
    if (
        type(research_cutoff) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or (
            rule_ids is not None
            and (
                not rule_ids
                or len(set(rule_ids)) != len(rule_ids)
                or any(
                    not isinstance(rule, str) or not rule.strip() for rule in rule_ids
                )
            )
        )
    ):
        raise ValueError("来源观察段生产参数无效")


def _anchor_rows(
    cursor: Any, *, source_package_id: int, cutoff: date, rule_ids: Sequence[str] | None
) -> list[tuple[Any, ...]]:
    rule_filter = "AND r.rule_id = ANY(%s)" if rule_ids else ""
    params: tuple[Any, ...] = (
        (source_package_id, cutoff, list(rule_ids))
        if rule_ids
        else (
            source_package_id,
            cutoff,
        )
    )
    cursor.execute(
        f"""SELECT e.id,r.target_session,r.id,r.source_symbol,r.rule_id,r.rule_version,
                  r.profile_revision,r.history_session_count,r.d1_history_session_count,
                  r.event_window_start,r.calendar_revision,r.scope,r.state,
                  r.source_capture_id,r.reasons,e.enrollment_kind,e.reason_code,
                  e.parent_enrollment_id
           FROM source_research_enrollments e
           JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
           WHERE r.source_package_id=%s AND r.target_session<=%s
             AND e.enrollment_kind IN
                 ('ENTRY','FIRST_OBSERVED_LEFT_CENSORED','REENTRY')
             {rule_filter}
           ORDER BY r.rule_id,r.target_session,r.source_symbol,e.id""",
        params,
    )
    return list(cursor.fetchall())


def _fact_groups(
    cursor: Any,
    *,
    source_package_id: int,
    sessions: Sequence[date],
    symbols: Sequence[str],
    rule_ids: Sequence[str],
) -> dict[tuple[str, str], dict[date, list[tuple[Any, ...]]]]:
    if not sessions or not symbols or not rule_ids:
        return {}
    cursor.execute(
        """SELECT target_session,id,source_symbol,rule_id,rule_version,state,
                  profile_revision,history_session_count,d1_history_session_count,
                  event_window_start,calendar_revision,scope,source_capture_id,reasons
           FROM source_rule_evaluations
           WHERE source_package_id=%s AND target_session=ANY(%s)
             AND source_symbol=ANY(%s) AND rule_id=ANY(%s)
           ORDER BY source_symbol,rule_id,target_session,id""",
        (source_package_id, list(sessions), list(symbols), list(rule_ids)),
    )
    grouped: dict[tuple[str, str], dict[date, list[tuple[Any, ...]]]] = {}
    for row in cursor.fetchall():
        grouped.setdefault((row[2], row[3]), {}).setdefault(row[0], []).append(row)
    return grouped


def _context_rule_values(context: Mapping[str, Any] | None) -> dict[str, Any]:
    rule = (context or {}).get("rule") or {}
    return {
        "profile_revision": rule.get("profile_revision"),
        "history_session_count": rule.get("history_count"),
        "d1_history_session_count": rule.get("d1_history_count"),
        "event_window_start": rule.get("event_window_start"),
        "calendar_revision": rule.get("calendar_revision"),
        "capture": (context or {}).get("capture"),
        "source_time_quality": (context or {}).get("source_time_quality"),
        "source_clock_role": (context or {}).get("source_clock_role"),
        "scope": rule.get("scope"),
    }


def _rule_observation_row(
    *,
    enrollment_id: int,
    package: int,
    symbol: str,
    rule_id: str,
    rule_version: str,
    t0_session: date,
    anchor_fact_id: int,
    anchor_capture_id: int,
    enrollment_kind: str,
    parent_enrollment_id: int | None,
    observation_session: date,
    previous_session: date | None,
    target_capture_id: int | None,
    evidence_fact_id: int | None,
    linked_enrollment_id: int | None,
    observation_state: str | None,
    lifecycle: str,
    event_code: str,
    quality_status: str,
    comparability_status: str,
    reason_code: str,
    context_values: Mapping[str, Any],
    scope: str,
) -> tuple[Any, ...]:
    return (
        enrollment_id,
        package,
        symbol,
        rule_id,
        rule_version,
        t0_session,
        anchor_fact_id,
        anchor_capture_id,
        enrollment_kind,
        parent_enrollment_id,
        observation_session,
        previous_session,
        target_capture_id,
        evidence_fact_id,
        linked_enrollment_id,
        observation_state,
        lifecycle,
        event_code,
        quality_status,
        comparability_status,
        reason_code,
        context_values.get("profile_revision"),
        context_values.get("history_session_count"),
        context_values.get("d1_history_session_count"),
        context_values.get("event_window_start"),
        context_values.get("calendar_revision"),
        context_values.get("source_time_quality"),
        context_values.get("source_clock_role"),
        scope,
        PRICE_COORDINATE,
        IDENTITY_STATUS,
        PRODUCER_CONTRACT,
        PRODUCER_VERSION,
    )


def _compute_segment_rows(
    *,
    anchors: Sequence[tuple[Any, ...]],
    contexts: Mapping[date, Mapping[str, Any]],
    facts_by_pair: Mapping[tuple[str, str], Mapping[date, Sequence[tuple[Any, ...]]]],
    official_sessions: Sequence[date],
    source_package_id: int,
    research_cutoff: date,
) -> list[tuple[Any, ...]]:
    by_fact_id = {
        row[2]: {
            "enrollment_id": row[0],
            "parent_enrollment_id": row[17],
            "enrollment_kind": row[15],
        }
        for row in anchors
    }
    rows: list[tuple[Any, ...]] = []
    for anchor in anchors:
        (
            enrollment_id,
            t0_session,
            anchor_fact_id,
            symbol,
            rule_id,
            rule_version,
            profile_revision,
            history_count,
            d1_history_count,
            event_window_start,
            calendar_revision,
            scope,
            anchor_state,
            anchor_capture_id,
            _anchor_reasons,
            enrollment_kind,
            enrollment_reason,
            parent_enrollment_id,
        ) = anchor
        if anchor_state != "TRUE" or t0_session not in official_sessions:
            raise ObservationSegmentConflict(
                "来源级登记不是官方会话上的已证TRUE规则事实"
            )
        pair = (symbol, rule_id)
        candidates = facts_by_pair.get(pair, {}).get(t0_session, ())
        anchor_context = contexts.get(t0_session)
        anchor_observation = _contextual_rule_observation(
            candidates,
            anchor_context,
            rule_id=rule_id,
            rule_version=rule_version,
        )
        anchor_matches = bool(
            anchor_observation
            and anchor_observation.evidence_id == anchor_fact_id
            and anchor_observation.state == "TRUE"
            and _context_rule_values(anchor_context).get("capture") == anchor_capture_id
            and _context_rule_values(anchor_context).get("profile_revision")
            == profile_revision
            and _context_rule_values(anchor_context).get("history_session_count")
            == history_count
            and _context_rule_values(anchor_context).get("d1_history_session_count")
            == d1_history_count
            and _context_rule_values(anchor_context).get("event_window_start")
            == event_window_start
            and _context_rule_values(anchor_context).get("calendar_revision")
            == calendar_revision
        )
        anchor_context_values = _context_rule_values(anchor_context)
        initial_quality = "OBSERVED" if anchor_matches else "INCOMPLETE"
        initial_reason = (
            enrollment_reason if anchor_matches else "ANCHOR_RULE_CONTEXT_NOT_CLOSED"
        )
        rows.append(
            _rule_observation_row(
                enrollment_id=enrollment_id,
                package=source_package_id,
                symbol=symbol,
                rule_id=rule_id,
                rule_version=rule_version,
                t0_session=t0_session,
                anchor_fact_id=anchor_fact_id,
                anchor_capture_id=anchor_capture_id,
                enrollment_kind=enrollment_kind,
                parent_enrollment_id=parent_enrollment_id,
                observation_session=t0_session,
                previous_session=None,
                target_capture_id=anchor_context_values.get("capture"),
                evidence_fact_id=anchor_fact_id,
                linked_enrollment_id=None,
                observation_state=anchor_state,
                lifecycle="ACTIVE",
                event_code=enrollment_kind,
                quality_status=initial_quality,
                comparability_status="INITIAL",
                reason_code=initial_reason,
                context_values=anchor_context_values,
                scope=scope,
            )
        )
        lifecycle = "ACTIVE"
        previous_session = t0_session
        previous_context_available = anchor_matches
        sessions = tuple(
            session
            for session in official_sessions
            if t0_session < session <= research_cutoff
        )
        for session in sessions:
            previous_context = contexts.get(previous_session)
            target_context = contexts.get(session)
            pair_comparable = bool(
                previous_context_available
                and _rule_context_comparable(
                    previous_context,
                    target_context,
                    before=previous_session,
                    target=session,
                )
            )
            day_candidates = facts_by_pair.get(pair, {}).get(session, ())
            successor_rule = (target_context or {}).get("rule") or {}
            valid = [
                row
                for row in day_candidates
                if pair_comparable
                and row[3] == rule_id
                and row[4] == rule_version
                and row[4] == successor_rule.get("version")
                and row[6] == successor_rule.get("profile_revision")
                and row[7] == successor_rule.get("history_count")
                and row[8] == successor_rule.get("d1_history_count")
                and row[9] == successor_rule.get("event_window_start")
                and row[10] == successor_rule.get("calendar_revision")
                and row[11] == successor_rule.get("scope")
                and row[12] == (target_context or {}).get("capture")
            ]
            current = (
                Observation(session, valid[0][5], valid[0][1])
                if len(day_candidates) == len(valid) == 1
                and valid[0][5] in {"TRUE", "FALSE", "UNKNOWN"}
                else None
            )
            invalid_reason = None
            if current is None:
                if day_candidates and not valid:
                    invalid_reason = "RULE_CONTEXT_NOT_COMPARABLE"
                elif day_candidates:
                    invalid_reason = "AMBIGUOUS_OR_NONADJACENT_FACT"
                elif (
                    target_context is not None
                    and target_context.get("rules_present")
                    and not target_context.get("rules_complete")
                ):
                    invalid_reason = "RULE_CONTEXT_NOT_COMPARABLE"
            decision = advance_observation(
                lifecycle=lifecycle,
                previous_session=previous_session,
                expected_session=session,
                current=current,
                research_cutoff=research_cutoff,
            )
            if decision.lifecycle is None:
                raise ObservationSegmentConflict("后继状态机返回了无生命周期结果")
            lifecycle = decision.lifecycle
            context_values = _context_rule_values(target_context)
            quality_status = (
                "OBSERVED"
                if current is not None and current.state != "UNKNOWN"
                else "UNKNOWN"
                if current is not None
                else "INCOMPLETE"
                if invalid_reason
                or not target_context
                or not target_context.get("inputs_complete")
                or not target_context.get("rules_complete")
                else "UNKNOWN"
            )
            comparability_status = (
                "COMPARABLE"
                if pair_comparable
                else "INCOMPARABLE"
                if previous_context
                and target_context
                and previous_context.get("inputs_complete")
                and previous_context.get("rules_complete")
                and target_context.get("inputs_complete")
                and target_context.get("rules_complete")
                else "INCOMPLETE"
            )
            event_code = decision.event
            reason_code = invalid_reason or decision.reason
            linked_enrollment_id = None
            if decision.event == "REENTRY" and current is not None:
                child = by_fact_id.get(current.evidence_id)
                if (
                    child
                    and child["enrollment_kind"] == "REENTRY"
                    and child["parent_enrollment_id"] == enrollment_id
                ):
                    linked_enrollment_id = child["enrollment_id"]
                else:
                    event_code = "REENTRY_NOT_LINKED"
                    quality_status = "INCOMPLETE"
                    reason_code = "REENTRY_PARENT_NOT_PROVED"
            rows.append(
                _rule_observation_row(
                    enrollment_id=enrollment_id,
                    package=source_package_id,
                    symbol=symbol,
                    rule_id=rule_id,
                    rule_version=rule_version,
                    t0_session=t0_session,
                    anchor_fact_id=anchor_fact_id,
                    anchor_capture_id=anchor_capture_id,
                    enrollment_kind=enrollment_kind,
                    parent_enrollment_id=parent_enrollment_id,
                    observation_session=session,
                    previous_session=previous_session,
                    target_capture_id=context_values.get("capture"),
                    evidence_fact_id=current.evidence_id if current else None,
                    linked_enrollment_id=linked_enrollment_id,
                    observation_state=current.state if current else None,
                    lifecycle=lifecycle,
                    event_code=event_code,
                    quality_status=quality_status,
                    comparability_status=comparability_status,
                    reason_code=reason_code,
                    context_values=context_values,
                    scope=scope,
                )
            )
            if event_code == "REENTRY_NOT_LINKED":
                break
            previous_context_available = current is not None
            previous_session = session
    return rows


def persist_source_observation_segments(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_package_id: int,
    rule_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """从已存来源规则登记离线生成逐会话观察段；一次事务整体提交。"""
    selected_rules = tuple(rule_ids) if rule_ids is not None else None
    _validate_producer_arguments(
        research_cutoff=research_cutoff,
        source_package_id=source_package_id,
        rule_ids=selected_rules,
    )
    calendar_sessions = tuple(
        session
        for session in _load_calendar(research_cutoff)[3]
        if session <= research_cutoff
    )
    with _transaction(config, write=True, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        anchors = _anchor_rows(
            cursor,
            source_package_id=source_package_id,
            cutoff=research_cutoff,
            rule_ids=selected_rules,
        )
        if not anchors:
            return {
                "status": "NO_OBSERVED_SAMPLE",
                "source_package_id": source_package_id,
                "research_cutoff": research_cutoff.isoformat(),
                "enrollment_count": 0,
                "observation_count": 0,
                "inserted_count": 0,
                "rule_counts": {},
                "event_counts": {},
                "quality_counts": {},
            }
        first_session = min(row[1] for row in anchors)
        sessions = tuple(
            session
            for session in calendar_sessions
            if first_session <= session <= research_cutoff
        )
        contexts = _read_rule_day_contexts(
            cursor, sessions=sessions, package=source_package_id
        )
        facts = _fact_groups(
            cursor,
            source_package_id=source_package_id,
            sessions=sessions,
            symbols=tuple(sorted({row[3] for row in anchors})),
            rule_ids=tuple(sorted({row[4] for row in anchors})),
        )
        rows = _compute_segment_rows(
            anchors=anchors,
            contexts=contexts,
            facts_by_pair=facts,
            official_sessions=calendar_sessions,
            source_package_id=source_package_id,
            research_cutoff=research_cutoff,
        )
        column_sql = ", ".join(_SEGMENT_COLUMNS)
        placeholders = ", ".join(["%s"] * len(_SEGMENT_COLUMNS))
        cursor.executemany(
            f"""INSERT INTO source_research_observation_segment_days
                ({column_sql}) VALUES ({placeholders})
                ON CONFLICT (source_enrollment_id, observation_session) DO NOTHING""",
            rows,
        )
        inserted_count = max(0, cursor.rowcount)
        enrollment_ids = sorted({row[0] for row in anchors})
        cursor.execute(
            f"""SELECT {column_sql}
                FROM source_research_observation_segment_days
                WHERE source_enrollment_id=ANY(%s) AND observation_session<=%s
                ORDER BY source_enrollment_id,observation_session""",
            (enrollment_ids, research_cutoff),
        )
        actual = list(cursor.fetchall())
        if len(actual) != len(rows) or set(actual) != set(rows):
            raise ObservationSegmentConflict(
                "同一观察业务键已有不同内容或批次读回不完整，事务已回滚"
            )
        event_counts = Counter(row[17] for row in rows)
        quality_counts = Counter(row[18] for row in rows)
        lifecycle_counts = Counter(row[16] for row in rows)
        rule_counts = Counter(row[4] for row in anchors)
        return {
            "status": "PERSISTED",
            "source_package_id": source_package_id,
            "research_cutoff": research_cutoff.isoformat(),
            "enrollment_count": len(anchors),
            "source_symbol_count": len({row[3] for row in anchors}),
            "observation_count": len(rows),
            "inserted_count": inserted_count,
            "rule_counts": dict(sorted(rule_counts.items())),
            "event_counts": dict(sorted(event_counts.items())),
            "quality_counts": dict(sorted(quality_counts.items())),
            "lifecycle_counts": dict(sorted(lifecycle_counts.items())),
            "transaction": "SERIALIZABLE 单批原子事务",
            "readback": "同事务精确业务键/typed内容一致",
            "scope": RESEARCH_SCOPE,
            "identity_status": IDENTITY_STATUS,
            "price_coordinate": PRICE_COORDINATE,
        }


def _validate_read_arguments(
    *,
    as_of: date,
    research_cutoff: date,
    source_package_id: int,
    rule_id: str | None,
    status: str | None,
    offset: int,
    limit: int,
) -> None:
    allowed_statuses = {
        "ACTIVE",
        "PAUSED",
        "EXITED",
        "EXITED_UNKNOWN",
        "UNKNOWN",
        "INCOMPLETE",
        "NOT_COMPUTED",
    }
    if (
        type(as_of) is not date
        or type(research_cutoff) is not date
        or as_of > research_cutoff
        or type(source_package_id) is not int
        or source_package_id <= 0
        or (rule_id is not None and (not isinstance(rule_id, str) or not rule_id))
        or (status is not None and status not in allowed_statuses)
        or type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise ValueError("来源观察段只读分页参数无效")


def read_source_observation_segments(
    config: Mapping[str, Any],
    *,
    as_of: date,
    research_cutoff: date,
    source_package_id: int,
    rule_id: str | None = None,
    status: str | None = None,
    offset: int = 0,
    limit: int = 20,
) -> dict[str, Any]:
    """同一只读快照内读取计数、稳定分页和已保存的逐会话历史。"""
    _validate_read_arguments(
        as_of=as_of,
        research_cutoff=research_cutoff,
        source_package_id=source_package_id,
        rule_id=rule_id,
        status=status,
        offset=offset,
        limit=limit,
    )
    official_sessions = _load_calendar(research_cutoff)[3]
    next_official_session = next(
        (session for session in official_sessions if session > research_cutoff), None
    )
    base = """WITH segments AS (
        SELECT e.id AS enrollment_id,r.source_package_id,r.source_symbol,r.rule_id,
               r.rule_version,r.target_session AS t0_session,r.id AS anchor_rule_fact_id,
               r.source_capture_id AS anchor_capture_id,e.enrollment_kind,
               e.reason_code AS enrollment_reason,e.parent_enrollment_id,
               r.profile_revision AS anchor_profile_revision,
               r.history_session_count AS anchor_history_session_count,
               r.d1_history_session_count AS anchor_d1_history_session_count,
               r.event_window_start AS anchor_event_window_start,
               r.calendar_revision AS anchor_calendar_revision,r.scope,
               latest.observation_session AS latest_observation_session,
               latest.observation_state AS latest_observation_state,
               latest.lifecycle AS latest_lifecycle,latest.event_code AS latest_event_code,
               latest.quality_status AS latest_quality_status,
               latest.reason_code AS latest_reason_code,
               latest.evidence_rule_fact_id AS latest_evidence_rule_fact_id,
               latest.target_capture_id AS latest_capture_id,
               latest.linked_enrollment_id AS latest_linked_enrollment_id,
               CASE
                 WHEN latest.id IS NULL THEN 'NOT_COMPUTED'
                 WHEN latest.observation_session < %s THEN 'INCOMPLETE'
                 WHEN latest.quality_status = 'INCOMPLETE' THEN 'INCOMPLETE'
                 WHEN latest.quality_status = 'NOT_COMPUTED' THEN 'NOT_COMPUTED'
                 WHEN latest.lifecycle = 'PAUSED' THEN 'PAUSED'
                 WHEN latest.lifecycle = 'EXITED_UNKNOWN' THEN 'EXITED_UNKNOWN'
                 WHEN latest.quality_status = 'UNKNOWN' THEN 'UNKNOWN'
                 ELSE latest.lifecycle
               END AS current_status
        FROM source_research_enrollments e
        JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
        LEFT JOIN LATERAL (
          SELECT o.id,o.observation_session,o.observation_state,o.lifecycle,
                 o.event_code,o.quality_status,o.reason_code,o.evidence_rule_fact_id,
                 o.target_capture_id,o.linked_enrollment_id
          FROM source_research_observation_segment_days o
          WHERE o.source_enrollment_id=e.id AND o.observation_session<=%s
          ORDER BY o.observation_session DESC,o.id DESC LIMIT 1
        ) latest ON TRUE
        WHERE r.source_package_id=%s AND r.target_session<=%s
          AND e.enrollment_kind IN
              ('ENTRY','FIRST_OBSERVED_LEFT_CENSORED','REENTRY')
    )"""
    params = (as_of, as_of, source_package_id, as_of)
    rule_clause = " AND rule_id=%s" if rule_id else ""
    status_clause = " AND current_status=%s" if status else ""
    selected_params = params + ((rule_id,) if rule_id else ())
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT r.rule_id,count(*) AS enrollment_count,
                      count(DISTINCT r.source_symbol) AS source_symbol_count
               FROM source_research_enrollments e
               JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE r.source_package_id=%s AND r.target_session<=%s
                 AND e.enrollment_kind IN
                     ('ENTRY','FIRST_OBSERVED_LEFT_CENSORED','REENTRY')
               GROUP BY r.rule_id ORDER BY r.rule_id""",
            (source_package_id, as_of),
        )
        available_rules = [
            {
                "rule_id": row[0],
                "enrollment_count": row[1],
                "source_symbol_count": row[2],
            }
            for row in cursor.fetchall()
        ]
        cursor.execute(
            f"""{base}
                SELECT current_status,count(*) AS enrollment_count,
                       count(DISTINCT source_symbol) AS source_symbol_count,
                       count(*) FILTER (WHERE latest_observation_session IS NULL)
                         AS not_computed_count,
                       count(*) FILTER (WHERE latest_quality_status='INCOMPLETE')
                         AS incomplete_count,
                       count(*) FILTER (WHERE latest_quality_status='UNKNOWN')
                         AS unknown_count,
                       count(*) FILTER (WHERE latest_quality_status='OBSERVED')
                         AS observed_segment_count
                FROM segments WHERE 1=1 {rule_clause}
                GROUP BY current_status ORDER BY current_status""",
            selected_params,
        )
        status_counts = {
            row[0]: {
                "enrollment_count": row[1],
                "source_symbol_count": row[2],
                "not_computed_count": row[3],
                "incomplete_count": row[4],
                "unknown_count": row[5],
                "observed_segment_count": row[6],
            }
            for row in cursor.fetchall()
        }
        cursor.execute(
            f"""{base}
                SELECT count(*),count(DISTINCT source_symbol)
                FROM segments WHERE 1=1 {rule_clause} {status_clause}""",
            selected_params + ((status,) if status else ()),
        )
        total_count, source_symbol_count = cursor.fetchone()
        cursor.execute(
            f"""{base}
                SELECT enrollment_id,source_package_id,source_symbol,rule_id,rule_version,
                       t0_session,anchor_rule_fact_id,anchor_capture_id,enrollment_kind,
                       enrollment_reason,parent_enrollment_id,anchor_profile_revision,
                       anchor_history_session_count,anchor_d1_history_session_count,
                       anchor_event_window_start,anchor_calendar_revision,scope,
                       latest_observation_session,latest_observation_state,latest_lifecycle,
                       latest_event_code,latest_quality_status,latest_reason_code,
                       latest_evidence_rule_fact_id,latest_capture_id,
                       latest_linked_enrollment_id,current_status
                FROM segments WHERE 1=1 {rule_clause} {status_clause}
                ORDER BY t0_session DESC,rule_id,source_symbol,enrollment_id
                LIMIT %s OFFSET %s""",
            selected_params + ((status,) if status else ()) + (limit, offset),
        )
        page_rows = cursor.fetchall()
        enrollment_ids = [row[0] for row in page_rows]
        history_by_id: dict[int, list[dict[str, Any]]] = {
            enrollment_id: [] for enrollment_id in enrollment_ids
        }
        if enrollment_ids:
            cursor.execute(
                """SELECT source_enrollment_id,observation_session,previous_session,
                          target_capture_id,evidence_rule_fact_id,linked_enrollment_id,
                          observation_state,lifecycle,event_code,quality_status,
                          comparability_status,reason_code,profile_revision,
                          history_session_count,d1_history_session_count,
                          event_window_start,calendar_revision,scope,price_coordinate,
                          identity_status
                   FROM source_research_observation_segment_days
                   WHERE source_enrollment_id=ANY(%s) AND observation_session<=%s
                   ORDER BY source_enrollment_id,observation_session,id""",
                (enrollment_ids, as_of),
            )
            for row in cursor.fetchall():
                history_by_id[row[0]].append(
                    {
                        "session": row[1].isoformat(),
                        "previous_session": row[2].isoformat() if row[2] else None,
                        "source_capture_id": row[3],
                        "fact_id": row[4],
                        "linked_enrollment_id": row[5],
                        "state": row[6],
                        "lifecycle": row[7],
                        "event": row[8],
                        "status": row[9],
                        "comparability_status": row[10],
                        "reason": row[11],
                        "profile_revision": row[12],
                        "history_session_count": row[13],
                        "d1_history_session_count": row[14],
                        "event_window_start": row[15].isoformat() if row[15] else None,
                        "calendar_revision": row[16],
                        "scope": row[17],
                        "price_coordinate": row[18],
                        "identity_status": row[19],
                    }
                )
        items = []
        for row in page_rows:
            history = history_by_id[row[0]]
            current = history[-1] if history else None
            status_value = row[26]
            if current and current["session"] != as_of.isoformat():
                status_value = "INCOMPLETE"
            next_status = None
            if (
                as_of == research_cutoff
                and row[17] == as_of
                and row[19]
                and next_official_session is not None
            ):
                next_status = advance_observation(
                    lifecycle=row[19],
                    previous_session=as_of,
                    expected_session=next_official_session,
                    current=None,
                    research_cutoff=research_cutoff,
                ).event
            items.append(
                {
                    "enrollment_id": row[0],
                    "source_package_id": row[1],
                    "source_symbol": row[2],
                    "rule_id": row[3],
                    "rule_version": row[4],
                    "t0_session": row[5].isoformat(),
                    "anchor_rule_fact_id": row[6],
                    "anchor_capture_id": row[7],
                    "enrollment_kind": row[8],
                    "enrollment_reason": row[9],
                    "parent_enrollment_id": row[10],
                    "profile_revision": row[11],
                    "history_session_count": row[12],
                    "d1_history_session_count": row[13],
                    "event_window_start": row[14].isoformat() if row[14] else None,
                    "calendar_revision": row[15],
                    "scope": row[16],
                    "latest_observation_session": (
                        row[17].isoformat() if row[17] else None
                    ),
                    "latest_state": row[18],
                    "latest_lifecycle": row[19],
                    "latest_event": row[20],
                    "latest_quality_status": row[21],
                    "latest_reason": row[22],
                    "latest_evidence_fact_id": row[23],
                    "latest_capture_id": row[24],
                    "latest_linked_enrollment_id": row[25],
                    "status": status_value,
                    "next_official_session_status": next_status,
                    "history": history,
                }
            )
        return {
            "status": "AVAILABLE" if total_count else "NO_OBSERVED_SAMPLE",
            "as_of": as_of.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "next_official_session_status": (
                "NOT_DUE"
                if as_of == research_cutoff and next_official_session is not None
                else "NOT_COMPUTED"
            ),
            "source_package_id": source_package_id,
            "rule_id": rule_id or "ALL_REGISTERED_RULES",
            "available_rules": available_rules,
            "status_counts": status_counts,
            "denominator": {
                "kind": "PERSISTED_SOURCE_ENROLLMENT_SEGMENTS",
                "enrollment_count": total_count,
                "independent_source_symbol_count": source_symbol_count,
            },
            "pagination": {
                "offset": offset,
                "limit": limit,
                "total_count": total_count,
                "next_offset": offset + len(items)
                if offset + len(items) < total_count
                else None,
            },
            "results": items,
            "scope": RESEARCH_SCOPE,
            "identity_status": IDENTITY_STATUS,
            "price_coordinate": PRICE_COORDINATE,
            "reader": "PG只读已持久化观察段；未触发生命周期计算",
        }


def read_source_observation_segment_followups(
    config: Mapping[str, Any],
    *,
    target: date,
    cutoff: date,
    enrollment_ids: Sequence[int],
    horizon_due_sessions: Sequence[date] = (),
) -> dict[int, dict[str, Any]]:
    """按现存T0登记ID读取已存规则状态与同T0的既存价格后继。"""
    if (
        type(target) is not date
        or type(cutoff) is not date
        or target > cutoff
        or len(enrollment_ids) > 500
        or len(set(enrollment_ids)) != len(enrollment_ids)
        or any(type(item) is not int or item <= 0 for item in enrollment_ids)
        or any(type(day) is not date or day <= target for day in horizon_due_sessions)
        or tuple(sorted(set(horizon_due_sessions))) != tuple(horizon_due_sessions)
    ):
        raise ValueError("来源观察段后继读取范围无效")
    if not enrollment_ids:
        return {}
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT e.id,r.target_session,r.id,r.source_package_id,r.source_capture_id,
                      r.source_symbol,r.rule_id,r.rule_version,r.profile_revision,
                      r.history_session_count,r.d1_history_session_count,
                      r.event_window_start,r.calendar_revision,r.scope,
                      e.parent_enrollment_id,e.enrollment_kind
               FROM source_research_enrollments e
               JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE e.id=ANY(%s)""",
            (list(enrollment_ids),),
        )
        anchors = {row[0]: row for row in cursor.fetchall()}
        if len(anchors) != len(enrollment_ids) or any(
            row[1] != target for row in anchors.values()
        ):
            raise ValueError("来源观察段登记ID与请求T0不匹配")
        timeline_by_requested = {
            enrollment_id: enrollment_id
            for enrollment_id, anchor in anchors.items()
            if anchor[15] in {"ENTRY", "FIRST_OBSERVED_LEFT_CENSORED", "REENTRY"}
        }
        daily_ids = [
            enrollment_id
            for enrollment_id, anchor in anchors.items()
            if anchor[15] == "DAILY_LANDMARK"
        ]
        daily_candidates: dict[int, set[int]] = {
            enrollment_id: set() for enrollment_id in daily_ids
        }
        if daily_ids:
            cursor.execute(
                """SELECT e.id,o.source_enrollment_id
                   FROM source_research_enrollments e
                   JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
                   JOIN source_research_observation_segment_days o
                     ON o.evidence_rule_fact_id=r.id
                    AND o.observation_session=r.target_session
                    AND o.source_package_id=r.source_package_id
                    AND o.source_symbol=r.source_symbol
                    AND o.rule_id=r.rule_id
                   WHERE e.id=ANY(%s) AND o.quality_status='OBSERVED'
                     AND o.event_code IN ('CONTINUES','LEFT_CENSORED')
                     AND o.source_enrollment_id<>e.id
                   ORDER BY e.id,o.source_enrollment_id""",
                (daily_ids,),
            )
            for daily_id, segment_id in cursor.fetchall():
                daily_candidates[daily_id].add(segment_id)
        for daily_id, segment_ids in daily_candidates.items():
            if len(segment_ids) == 1:
                timeline_by_requested[daily_id] = next(iter(segment_ids))
        timeline_ids = sorted(set(timeline_by_requested.values()))
        stored_by_id: dict[int, list[tuple[Any, ...]]] = {
            enrollment_id: [] for enrollment_id in timeline_ids
        }
        initial_rows: set[int] = set()
        if timeline_ids:
            cursor.execute(
                """SELECT source_enrollment_id
                   FROM source_research_observation_segment_days
                   WHERE source_enrollment_id=ANY(%s) AND observation_session=%s
                   ORDER BY source_enrollment_id""",
                (timeline_ids, target),
            )
            initial_rows = {row[0] for row in cursor.fetchall()}
            cursor.execute(
                """SELECT source_enrollment_id,observation_session,previous_session,
                          target_capture_id,evidence_rule_fact_id,linked_enrollment_id,
                          observation_state,lifecycle,event_code,quality_status,
                          comparability_status,reason_code,profile_revision,
                          history_session_count,d1_history_session_count,event_window_start,
                          calendar_revision,scope,price_coordinate,identity_status
                   FROM source_research_observation_segment_days
                   WHERE source_enrollment_id=ANY(%s) AND observation_session>%s
                     AND observation_session<=%s
                   ORDER BY source_enrollment_id,observation_session,id""",
                (timeline_ids, target, cutoff),
            )
            for row in cursor.fetchall():
                stored_by_id[row[0]].append(row)
        due_by_horizon = {
            horizon: (
                horizon_due_sessions[horizon - 1]
                if len(horizon_due_sessions) >= horizon
                else None
            )
            for horizon in (1, 3, 5)
        }
        due_horizons = [
            horizon
            for horizon, endpoint in due_by_horizon.items()
            if endpoint is not None and endpoint <= cutoff
        ]
        forward_rows: dict[tuple[int, str, int], list[tuple[Any, ...]]] = {}
        packages = {row[3] for row in anchors.values()}
        for package in packages:
            package_anchors = [
                (enrollment_id, row)
                for enrollment_id, row in anchors.items()
                if row[3] == package
            ]
            captures = sorted({row[4] for _, row in package_anchors})
            symbols = sorted({row[5] for _, row in package_anchors})
            if not due_horizons:
                continue
            cursor.execute(
                """SELECT target_session,source_package_id,source_capture_id,
                          source_symbol,horizon,path_rule_version,calendar_revision,
                          endpoint_session,maturity,endpoint_quality,endpoint_reason,
                          endpoint_numerator,endpoint_denominator,path_quality,
                          path_reasons,price_coordinate,scope
                   FROM source_forward_path_facts
                   WHERE target_session=%s AND source_package_id=%s
                     AND source_capture_id=ANY(%s) AND source_symbol=ANY(%s)
                     AND horizon=ANY(%s) AND endpoint_session<=%s
                   ORDER BY source_capture_id,source_symbol,horizon,
                            path_rule_version,calendar_revision""",
                (target, package, captures, symbols, due_horizons, cutoff),
            )
            stored = cursor.fetchall()
            for enrollment_id, anchor in package_anchors:
                forward_rows[(enrollment_id, anchor[5], anchor[4])] = [
                    row
                    for row in stored
                    if row[1] == package and row[2] == anchor[4] and row[3] == anchor[5]
                ]
        result: dict[int, dict[str, Any]] = {}
        for enrollment_id, anchor in anchors.items():
            timeline_id = timeline_by_requested.get(enrollment_id)
            entries = stored_by_id.get(timeline_id, []) if timeline_id else []
            history = [
                {
                    "session": row[1].isoformat(),
                    "previous_session": row[2].isoformat() if row[2] else None,
                    "source_capture_id": row[3],
                    "fact_id": row[4],
                    "linked_enrollment_id": row[5],
                    "state": row[6],
                    "lifecycle": row[7],
                    "status": row[8],
                    "quality_status": row[9],
                    "comparability_status": row[10],
                    "reason": row[11],
                    "profile_revision": row[12],
                    "history_session_count": row[13],
                    "d1_history_session_count": row[14],
                    "event_window_start": row[15].isoformat() if row[15] else None,
                    "calendar_revision": row[16],
                    "scope": row[17],
                    "price_coordinate": row[18],
                    "identity_status": row[19],
                }
                for row in entries
            ]
            if entries:
                latest = entries[-1]
                current_status = (
                    "INCOMPLETE" if latest[9] == "INCOMPLETE" else latest[8]
                )
                reason = latest[11]
                if latest[1] != cutoff:
                    current_status = "INCOMPLETE"
                    reason = "OBSERVATION_SEGMENT_NOT_COMPLETE_THROUGH_CUTOFF"
            elif timeline_id and timeline_id in initial_rows:
                current_status = "NOT_DUE" if target == cutoff else "INCOMPLETE"
                reason = (
                    "NO_SUCCESSOR_WITHIN_RESEARCH_CUTOFF"
                    if target == cutoff
                    else "OBSERVATION_SEGMENT_SUCCESSORS_NOT_PERSISTED"
                )
            elif (
                enrollment_id in daily_candidates
                and len(daily_candidates[enrollment_id]) > 1
            ):
                current_status = "INCOMPLETE"
                reason = "DAILY_LANDMARK_SEGMENT_AMBIGUOUS"
            else:
                current_status = "NOT_COMPUTED"
                reason = (
                    "DAILY_LANDMARK_SEGMENT_NOT_LINKED"
                    if enrollment_id in daily_candidates
                    else "SOURCE_OBSERVATION_SEGMENTS_NOT_PERSISTED"
                )
            latest = entries[-1] if entries else None
            forward_anchor = {
                "package": anchor[3],
                "capture": anchor[4],
                "source_symbol": anchor[5],
                "calendar_revision": anchor[12],
                "scope": anchor[13],
            }
            result[enrollment_id] = {
                "status": current_status,
                "reason": reason,
                "history": history,
                "history_truncated": False,
                "segment_enrollment_id": timeline_id,
                "fact_id": latest[4] if latest else anchor[2],
                "state": latest[6] if latest else "TRUE",
                "source_capture_id": latest[3] if latest else anchor[4],
                "base_fact_id": anchor[2],
                "base_capture_id": anchor[4],
                "rule_id": anchor[6],
                "rule_version": anchor[7],
                "profile_revision": latest[12] if latest else anchor[8],
                "history_session_count": latest[13] if latest else anchor[9],
                "d1_history_session_count": latest[14] if latest else anchor[10],
                "event_window_start": (anchor[11].isoformat() if anchor[11] else None),
                "calendar_revision": anchor[12],
                "scope": anchor[13],
                "parent_enrollment_id": anchor[14],
                "enrollment_kind": anchor[15],
                "forward_paths": _enrollment_forward_paths(
                    forward_anchor,
                    forward_rows.get((enrollment_id, anchor[5], anchor[4]), []),
                    due_by_horizon,
                    cutoff=cutoff,
                ),
                "reader": "PG只读已持久化来源规则观察段",
            }
        return result
