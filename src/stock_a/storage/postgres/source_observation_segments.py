"""来源级观察段的离线PG生产者与只读分页。"""

from __future__ import annotations

from collections import Counter, defaultdict
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
from stock_a.storage.postgres.source_observation_outcomes import (
    read_source_observation_outcomes_cursor,
)
from stock_a.storage.postgres.source_raw import _transaction


PRODUCER_CONTRACT = "SOURCE_RESEARCH_OBSERVATION_SEGMENTS_R1"
PRODUCER_VERSION = "R1"
PRICE_COORDINATE = "FORWARD_ADJUSTED"
IDENTITY_STATUS = "SOURCE_SYMBOL_ONLY"
RESEARCH_SCOPE = "NON_PIT_EXPLORATORY"

_EVENT_FILTERS = {
    "ENTRY",
    "FIRST_OBSERVED_LEFT_CENSORED",
    "CONTINUES",
    "EXIT_PROVED",
    "PAUSED_UNKNOWN",
    "REENTRY",
    "LEFT_CENSORED",
    "NO_MATCH",
    "UNKNOWN",
    "REENTRY_NOT_LINKED",
    "INCOMPLETE",
    "NOT_COMPUTED",
}

_SEGMENT_COLUMNS = (
    "source_enrollment_id",
    "source_capture_id",
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
    *, research_cutoff: date, source_capture_id: int, rule_ids: Sequence[str] | None
) -> None:
    if (
        type(research_cutoff) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
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
    cursor: Any, *, source_capture_id: int, cutoff: date, rule_ids: Sequence[str] | None
) -> list[tuple[Any, ...]]:
    rule_filter = "AND r.rule_id = ANY(%s)" if rule_ids else ""
    params: tuple[Any, ...] = (
        (source_capture_id, cutoff, list(rule_ids))
        if rule_ids
        else (
            source_capture_id,
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
           WHERE r.source_capture_id=%s AND r.target_session<=%s
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
    source_capture_id: int,
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
           WHERE source_capture_id=%s AND target_session=ANY(%s)
             AND source_symbol=ANY(%s) AND rule_id=ANY(%s)
           ORDER BY source_symbol,rule_id,target_session,id""",
        (source_capture_id, list(sessions), list(symbols), list(rule_ids)),
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
    source_capture_id: int,
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
    children_by_parent_session: dict[tuple[int, date], list[dict[str, Any]]] = (
        defaultdict(list)
    )
    for row in anchors:
        if row[15] == "REENTRY" and row[17] is not None:
            children_by_parent_session[(row[17], row[1])].append(
                {
                    "enrollment_id": row[0],
                    "fact_id": row[2],
                    "parent_enrollment_id": row[17],
                }
            )
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
                package=source_capture_id,
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
            children_on_session = children_by_parent_session.get(
                (enrollment_id, session), []
            )
            if decision.event == "REENTRY" and current is not None:
                child = by_fact_id.get(current.evidence_id)
                if (
                    child
                    and child["enrollment_kind"] == "REENTRY"
                    and child["parent_enrollment_id"] == enrollment_id
                    and len(children_on_session) == 1
                    and children_on_session[0]["enrollment_id"]
                    == child["enrollment_id"]
                    and children_on_session[0]["fact_id"] == current.evidence_id
                ):
                    linked_enrollment_id = child["enrollment_id"]
                else:
                    event_code = "REENTRY_NOT_LINKED"
                    quality_status = "INCOMPLETE"
                    reason_code = "REENTRY_PARENT_NOT_PROVED"
            elif children_on_session:
                event_code = "REENTRY_NOT_LINKED"
                quality_status = "INCOMPLETE"
                reason_code = "REENTRY_PARENT_NOT_PROVED"
            if linked_enrollment_id is not None:
                # 重入登记自身是唯一的新段事件；旧父段保留此前的EXIT_PROVED终态。
                # 父子关系由子登记的parent_enrollment_id表达，避免重入日生成第二条事件。
                break
            rows.append(
                _rule_observation_row(
                    enrollment_id=enrollment_id,
                    package=source_capture_id,
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
    source_capture_id: int,
    rule_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """从已存来源规则登记离线生成逐会话观察段；一次事务整体提交。"""
    selected_rules = tuple(rule_ids) if rule_ids is not None else None
    _validate_producer_arguments(
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
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
            source_capture_id=source_capture_id,
            cutoff=research_cutoff,
            rule_ids=selected_rules,
        )
        if not anchors:
            return {
                "status": "NO_OBSERVED_SAMPLE",
                "source_capture_id": source_capture_id,
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
            cursor, sessions=sessions, source_capture_id=source_capture_id
        )
        facts = _fact_groups(
            cursor,
            source_capture_id=source_capture_id,
            sessions=sessions,
            symbols=tuple(sorted({row[3] for row in anchors})),
            rule_ids=tuple(sorted({row[4] for row in anchors})),
        )
        rows = _compute_segment_rows(
            anchors=anchors,
            contexts=contexts,
            facts_by_pair=facts,
            official_sessions=calendar_sessions,
            source_capture_id=source_capture_id,
            research_cutoff=research_cutoff,
        )
        expected_by_key = {(row[0], row[10]): row for row in rows}
        if len(expected_by_key) != len(rows):
            raise ObservationSegmentConflict("观察段生产结果包含重复业务键")
        column_sql = ", ".join(_SEGMENT_COLUMNS)
        enrollment_ids = sorted({row[0] for row in anchors})
        cursor.execute(
            f"""SELECT {column_sql}
                FROM source_research_observation_segment_days
                WHERE source_enrollment_id=ANY(%s) AND observation_session<=%s
                ORDER BY source_enrollment_id,observation_session""",
            (enrollment_ids, research_cutoff),
        )
        existing_rows = list(cursor.fetchall())
        existing_by_key = {(row[0], row[10]): row for row in existing_rows}
        if len(existing_by_key) != len(existing_rows):
            raise ObservationSegmentConflict("现有观察日事实存在重复业务键")

        children_by_parent: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
        for anchor in anchors:
            if anchor[15] == "REENTRY" and anchor[17] is not None:
                children_by_parent[anchor[17]].append(anchor)
        repair_boundaries: dict[int, date] = {}
        for parent_id, children in children_by_parent.items():
            if len(children) != 1:
                raise ObservationSegmentConflict(
                    "一个父观察段关联多个直接REENTRY子段，拒绝自动纠正"
                )
            child = children[0]
            child_id, child_session, child_fact_id = child[0], child[1], child[2]
            parent_tail_keys = [
                key
                for key in existing_by_key
                if key[0] == parent_id and key[1] >= child_session
            ]
            if not parent_tail_keys:
                continue
            link_row = existing_by_key.get((parent_id, child_session))
            proved_old_link = bool(
                link_row
                and link_row[1] == source_capture_id
                and link_row[2] == child[3]
                and link_row[3] == child[4]
                and link_row[4] == child[5]
                and link_row[10] == child_session
                and link_row[12] == child[13]
                and link_row[13] == child_fact_id
                and link_row[14] == child_id
                and link_row[15] == "TRUE"
                and link_row[17] == "REENTRY"
                and link_row[18] == "OBSERVED"
            )
            if not proved_old_link:
                raise ObservationSegmentConflict(
                    "父段重入后事实缺少同键已证REENTRY链接，拒绝删除"
                )
            repair_boundaries[parent_id] = child_session

        stale_keys = set(existing_by_key) - set(expected_by_key)
        unproved_stale_keys = {
            key
            for key in stale_keys
            if key[0] not in repair_boundaries or key[1] < repair_boundaries[key[0]]
        }
        if unproved_stale_keys:
            raise ObservationSegmentConflict(
                "现有观察日事实含无法由已证REENTRY边界解释的额外业务键"
            )
        for key, actual_row in existing_by_key.items():
            expected_row = expected_by_key.get(key)
            if expected_row is not None and actual_row != expected_row:
                raise ObservationSegmentConflict(
                    "现有观察日事实与修正生产结果同键异内容，拒绝覆盖"
                )
        corrected_deleted_count = 0
        if stale_keys:
            cursor.executemany(
                """DELETE FROM source_research_observation_segment_days
                   WHERE source_enrollment_id=%s AND observation_session=%s""",
                sorted(stale_keys),
            )
            corrected_deleted_count = max(0, cursor.rowcount)
            if corrected_deleted_count != len(stale_keys):
                raise ObservationSegmentConflict(
                    "父段边界纠正删除数量与预核验业务键不一致"
                )
        placeholders = ", ".join(["%s"] * len(_SEGMENT_COLUMNS))
        cursor.executemany(
            f"""INSERT INTO source_research_observation_segment_days
                ({column_sql}) VALUES ({placeholders})
                ON CONFLICT (source_enrollment_id, observation_session) DO NOTHING""",
            rows,
        )
        inserted_count = max(0, cursor.rowcount)
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
            "source_capture_id": source_capture_id,
            "research_cutoff": research_cutoff.isoformat(),
            "enrollment_count": len(anchors),
            "source_symbol_count": len({row[3] for row in anchors}),
            "observation_count": len(rows),
            "inserted_count": inserted_count,
            "corrected_deleted_count": corrected_deleted_count,
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
    source_capture_id: int,
    rule_id: str | None,
    status: str | None,
    event_code: str | None,
    offset: int,
    limit: int,
    event_offset: int,
    event_limit: int,
    outcome_horizon: int,
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
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or (rule_id is not None and (not isinstance(rule_id, str) or not rule_id))
        or (status is not None and status not in allowed_statuses)
        or (event_code is not None and event_code not in _EVENT_FILTERS)
        or type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
        or type(event_offset) is not int
        or event_offset < 0
        or type(event_limit) is not int
        or not 1 <= event_limit <= 100
        or type(outcome_horizon) is not int
        or outcome_horizon not in {1, 3, 5}
    ):
        raise ValueError("来源观察段只读分页参数无效")


def read_source_observation_segments(
    config: Mapping[str, Any],
    *,
    as_of: date,
    research_cutoff: date,
    source_capture_id: int,
    rule_id: str | None = None,
    status: str | None = None,
    event_code: str | None = None,
    offset: int = 0,
    limit: int = 20,
    event_offset: int = 0,
    event_limit: int = 20,
    outcome_horizon: int = 1,
) -> dict[str, Any]:
    """同一只读快照内读取状态、日期事件、历史与原T0后继。"""
    _validate_read_arguments(
        as_of=as_of,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule_id=rule_id,
        status=status,
        event_code=event_code,
        offset=offset,
        limit=limit,
        event_offset=event_offset,
        event_limit=event_limit,
        outcome_horizon=outcome_horizon,
    )
    official_sessions = _load_calendar(research_cutoff)[3]
    if as_of not in official_sessions:
        raise ValueError("来源观察段查询日期必须是官方交易会话")
    next_official_session = next(
        (session for session in official_sessions if session > research_cutoff), None
    )
    base = """WITH segments AS (
        SELECT e.id AS enrollment_id,r.source_capture_id,r.source_symbol,r.rule_id,
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
                 WHEN latest.quality_status = 'INCOMPLETE' THEN 'INCOMPLETE'
                 WHEN latest.quality_status = 'NOT_COMPUTED' THEN 'NOT_COMPUTED'
                 WHEN latest.lifecycle = 'PAUSED' THEN 'PAUSED'
                 WHEN latest.lifecycle = 'EXITED_UNKNOWN' THEN 'EXITED_UNKNOWN'
                 WHEN latest.quality_status = 'UNKNOWN' THEN 'UNKNOWN'
                 WHEN latest.lifecycle = 'EXITED' THEN 'EXITED'
                 WHEN latest.observation_session < %s THEN 'INCOMPLETE'
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
        WHERE r.source_capture_id=%s AND r.target_session<=%s
          AND e.enrollment_kind IN
              ('ENTRY','FIRST_OBSERVED_LEFT_CENSORED','REENTRY')
    )"""
    params = (as_of, as_of, source_capture_id, as_of)
    rule_clause = " AND rule_id=%s" if rule_id else ""
    status_clause = " AND current_status=%s" if status else ""
    selected_params = params + ((rule_id,) if rule_id else ())
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        outcome_summary, outcome_by_enrollment = (
            read_source_observation_outcomes_cursor(
                cursor,
                source_capture_id=source_capture_id,
                t0_session=as_of,
                research_cutoff=research_cutoff,
                rule_id=rule_id,
                horizon=outcome_horizon,
                official_sessions=official_sessions,
            )
        )
        cursor.execute(
            """SELECT r.rule_id,count(*) AS enrollment_count,
                      count(DISTINCT r.source_symbol) AS source_symbol_count
               FROM source_research_enrollments e
               JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE r.source_capture_id=%s AND r.target_session<=%s
                 AND e.enrollment_kind IN
                     ('ENTRY','FIRST_OBSERVED_LEFT_CENSORED','REENTRY')
               GROUP BY r.rule_id ORDER BY r.rule_id""",
            (source_capture_id, as_of),
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
                SELECT enrollment_id,source_capture_id,source_symbol,rule_id,rule_version,
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
        event_rule_clause = " AND s.rule_id=%s" if rule_id else ""
        event_base = f"""{base}, actual AS (
          SELECT o.source_enrollment_id AS enrollment_id,s.source_symbol,s.rule_id,
                 o.event_code AS event_type,o.id AS event_fact_id,
                 o.evidence_rule_fact_id AS event_evidence_fact_id,
                 o.quality_status AS event_quality_status,o.reason_code AS event_reason,
                 o.target_capture_id AS event_capture_id,
                 o.observation_state AS event_state,o.lifecycle AS event_lifecycle,
                 o.comparability_status AS event_comparability,
                 o.previous_session AS event_previous_session
          FROM segments s
          JOIN source_research_observation_segment_days o
            ON o.source_enrollment_id=s.enrollment_id
          WHERE o.source_capture_id=%s AND o.observation_session=%s
            AND NOT (o.event_code='REENTRY' AND o.linked_enrollment_id IS NOT NULL
                     AND s.enrollment_kind<>'REENTRY') {event_rule_clause}
        ), expected AS (
          SELECT s.enrollment_id,s.source_symbol,s.rule_id,s.t0_session,
                 previous.id AS previous_fact_id,
                 previous.observation_session AS previous_session,
                 previous.observation_state AS previous_state,
                 previous.lifecycle AS previous_lifecycle
          FROM segments s
          LEFT JOIN LATERAL (
            SELECT o.id,o.observation_session,o.observation_state,o.lifecycle
            FROM source_research_observation_segment_days o
            WHERE o.source_enrollment_id=s.enrollment_id
              AND o.observation_session<%s
            ORDER BY o.observation_session DESC,o.id DESC LIMIT 1
          ) previous ON TRUE
          WHERE (s.t0_session=%s OR previous.id IS NULL
                 OR previous.lifecycle<>'EXITED') {event_rule_clause}
        ), missing AS (
          SELECT e.enrollment_id,e.source_symbol,e.rule_id,
                 CASE WHEN e.previous_fact_id IS NULL
                      THEN 'NOT_COMPUTED' ELSE 'INCOMPLETE' END AS event_type,
                 NULL::bigint AS event_fact_id,
                 NULL::bigint AS event_evidence_fact_id,
                 CASE WHEN e.previous_fact_id IS NULL
                      THEN 'NOT_COMPUTED' ELSE 'INCOMPLETE' END
                      AS event_quality_status,
                 CASE WHEN e.previous_fact_id IS NULL
                      THEN 'OBSERVATION_SEGMENT_NOT_COMPUTED'
                      ELSE 'DUE_OBSERVATION_FACT_MISSING' END AS event_reason,
                 NULL::bigint AS event_capture_id,e.previous_state AS event_state,
                 e.previous_lifecycle AS event_lifecycle,
                 'INCOMPLETE'::text AS event_comparability,
                 e.previous_session AS event_previous_session
          FROM expected e
          WHERE NOT EXISTS (
            SELECT 1 FROM actual a WHERE a.enrollment_id=e.enrollment_id
          )
        ), event_rows AS (
          SELECT * FROM actual
          UNION ALL
          SELECT * FROM missing
        )"""
        event_base_params = params + (source_capture_id, as_of)
        event_base_params += (rule_id,) if rule_id else ()
        event_base_params += (as_of, as_of)
        event_base_params += (rule_id,) if rule_id else ()
        cursor.execute(
            f"""{event_base}
                SELECT event_type,count(DISTINCT enrollment_id),
                       count(DISTINCT source_symbol)
                FROM event_rows
                GROUP BY event_type ORDER BY event_type""",
            event_base_params,
        )
        daily_event_counts = {
            row[0]: {"segment_count": row[1], "source_symbol_count": row[2]}
            for row in cursor.fetchall()
        }
        cursor.execute(
            f"""{event_base}
                SELECT (SELECT count(*) FROM actual),
                       (SELECT count(DISTINCT source_symbol) FROM actual),
                       (SELECT count(*) FROM expected),
                       (SELECT count(*) FROM missing),
                       (SELECT count(*) FROM actual
                         WHERE event_quality_status='UNKNOWN'),
                       (SELECT count(*) FROM actual
                         WHERE event_quality_status='INCOMPLETE'),
                       (SELECT count(*) FROM missing
                         WHERE event_type='NOT_COMPUTED')""",
            event_base_params,
        )
        (
            observed_day_segment_count,
            observed_day_symbol_count,
            due_segment_count,
            missing_due_segment_count,
            unknown_day_count,
            incomplete_day_count,
            not_computed_day_count,
        ) = cursor.fetchone()
        cursor.execute(
            """SELECT count(*),count(DISTINCT source_symbol)
               FROM source_rule_evaluations
               WHERE source_capture_id=%s AND target_session=%s
                 AND (%s::text IS NULL OR rule_id=%s)""",
            (source_capture_id, as_of, rule_id, rule_id),
        )
        input_rule_fact_count, input_rule_symbol_count = cursor.fetchone()
        cursor.execute(
            f"""{base}
                SELECT count(*),count(DISTINCT source_symbol)
                FROM segments WHERE current_status IN ('ACTIVE','PAUSED')
                  {rule_clause}""",
            selected_params,
        )
        risk_set_segment_count, risk_set_symbol_count = cursor.fetchone()
        event_filter_conditions = []
        event_filter_params: list[Any] = []
        if event_code == "INCOMPLETE":
            event_filter_conditions.append(
                "(event_type='INCOMPLETE' OR event_quality_status='INCOMPLETE')"
            )
        elif event_code is not None:
            event_filter_conditions.append("event_type=%s")
            event_filter_params.append(event_code)
        if status is not None:
            event_filter_conditions.append("s.current_status=%s")
            event_filter_params.append(status)
        event_filter_clause = (
            "WHERE " + " AND ".join(event_filter_conditions)
            if event_filter_conditions
            else ""
        )
        cursor.execute(
            f"""{event_base}
                SELECT count(*)
                FROM event_rows er JOIN segments s ON s.enrollment_id=er.enrollment_id
                {event_filter_clause}""",
            event_base_params + tuple(event_filter_params),
        )
        total_event_count = cursor.fetchone()[0]
        cursor.execute(
            f"""{event_base}
                SELECT s.enrollment_id,s.source_capture_id,s.source_symbol,s.rule_id,
                       s.rule_version,s.t0_session,s.anchor_rule_fact_id,
                       s.anchor_capture_id,s.enrollment_kind,s.enrollment_reason,
                       s.parent_enrollment_id,s.anchor_profile_revision,
                       s.anchor_history_session_count,s.anchor_d1_history_session_count,
                       s.anchor_event_window_start,s.anchor_calendar_revision,s.scope,
                       s.latest_observation_session,s.latest_observation_state,
                       s.latest_lifecycle,s.latest_event_code,s.latest_quality_status,
                       s.latest_reason_code,s.latest_evidence_rule_fact_id,
                       s.latest_capture_id,s.latest_linked_enrollment_id,s.current_status,
                       er.event_type,er.event_fact_id,er.event_evidence_fact_id,
                       er.event_quality_status,er.event_reason,er.event_capture_id,
                       er.event_state,er.event_lifecycle,er.event_comparability,
                       er.event_previous_session
                FROM event_rows er JOIN segments s ON s.enrollment_id=er.enrollment_id
                {event_filter_clause}
                ORDER BY er.event_type,s.source_symbol,s.enrollment_id
                LIMIT %s OFFSET %s""",
            event_base_params
            + tuple(event_filter_params)
            + (event_limit, event_offset),
        )
        event_page_rows = cursor.fetchall()
        related_parent_ids = {
            row[10] for row in page_rows + event_page_rows if row[10] is not None
        }
        enrollment_ids = sorted(
            {row[0] for row in page_rows}
            | {row[0] for row in event_page_rows}
            | related_parent_ids
        )
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

        def build_item(row: tuple[Any, ...]) -> dict[str, Any]:
            history = history_by_id[row[0]]
            current = history[-1] if history else None
            status_value = row[26]
            if (
                current
                and current["session"] != as_of.isoformat()
                and current["lifecycle"] != "EXITED"
            ):
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
            return {
                "enrollment_id": row[0],
                "source_capture_id": row[1],
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
                "horizon_outcome": outcome_by_enrollment.get(row[0]),
                "status": status_value,
                "next_official_session_status": next_status,
                "history": history,
            }

        items = [build_item(row) for row in page_rows]
        event_items = []
        for row in event_page_rows:
            item = build_item(row[:27])
            item.update(
                {
                    "daily_event_code": row[27],
                    "daily_event_fact_id": row[28],
                    "daily_event_evidence_fact_id": row[29],
                    "daily_event_quality_status": row[30],
                    "daily_event_reason": row[31],
                    "daily_event_capture_id": row[32],
                    "daily_event_state": row[33],
                    "daily_event_lifecycle": row[34],
                    "daily_event_comparability_status": row[35],
                    "daily_event_previous_session": (
                        row[36].isoformat() if row[36] else None
                    ),
                    "daily_event_session": as_of.isoformat(),
                }
            )
            event_items.append(item)

        displayed_items_by_id = {
            item["enrollment_id"]: item for item in items + event_items
        }
        if displayed_items_by_id:
            cursor.execute(
                """SELECT c.parent_enrollment_id,c.id,cr.target_session,cr.id,
                          cr.source_capture_id,cr.source_symbol,
                          cr.rule_id,cr.rule_version
                   FROM source_research_enrollments c
                   JOIN source_rule_evaluations cr ON cr.id=c.rule_fact_id
                   JOIN source_research_enrollments p ON p.id=c.parent_enrollment_id
                   JOIN source_rule_evaluations pr ON pr.id=p.rule_fact_id
                   WHERE c.parent_enrollment_id=ANY(%s)
                     AND c.enrollment_kind='REENTRY' AND cr.target_session<=%s
                     AND cr.source_capture_id=pr.source_capture_id
                     AND cr.source_symbol=pr.source_symbol
                     AND cr.rule_id=pr.rule_id AND cr.rule_version=pr.rule_version
                   ORDER BY c.parent_enrollment_id,cr.target_session,c.id""",
                (sorted(displayed_items_by_id), as_of),
            )
            for row in cursor.fetchall():
                parent_item = displayed_items_by_id.get(row[0])
                if parent_item is not None:
                    parent_item.setdefault("child_enrollments", []).append(
                        {
                            "enrollment_id": row[1],
                            "t0_session": row[2].isoformat(),
                            "rule_fact_id": row[3],
                            "capture_id": row[4],
                        }
                    )
                child_item = displayed_items_by_id.get(row[1])
                if child_item is not None:
                    child_item["child_t0_session"] = row[2].isoformat()

        for item in displayed_items_by_id.values():
            if item.get("parent_enrollment_id") in history_by_id:
                item["parent_history"] = history_by_id[item["parent_enrollment_id"]]

        if displayed_items_by_id:
            displayed_ids = sorted(displayed_items_by_id)
            cursor.execute(
                """SELECT e.id,f.target_session,f.source_capture_id,
                          f.source_symbol,f.horizon,
                          f.path_rule_version,f.calendar_revision,f.endpoint_session,
                          f.maturity,f.endpoint_quality,f.endpoint_reason,
                          f.endpoint_numerator,f.endpoint_denominator,f.path_quality,
                          f.path_reasons,f.price_coordinate,f.scope,
                          f.mfe_numerator,f.mfe_denominator,f.mae_numerator,
                          f.mae_denominator,f.mdd_numerator,f.mdd_denominator
                   FROM source_research_enrollments e
                   JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
                   JOIN source_forward_path_facts f
                     ON f.target_session=r.target_session
                    AND f.source_capture_id=r.source_capture_id
                    AND f.source_symbol=r.source_symbol
                   WHERE e.id=ANY(%s) AND r.target_session<=%s
                     AND f.horizon IN (1,3,5) AND f.endpoint_session<=%s
                   ORDER BY e.id,f.horizon,f.path_rule_version,f.calendar_revision""",
                (displayed_ids, as_of, research_cutoff),
            )
            forward_rows_by_id: dict[int, list[tuple[Any, ...]]] = defaultdict(list)
            for row in cursor.fetchall():
                forward_rows_by_id[row[0]].append(row[1:])
            calendar_sessions = tuple(official_sessions)
            for enrollment_id, item in displayed_items_by_id.items():
                target = date.fromisoformat(item["t0_session"])
                try:
                    target_index = calendar_sessions.index(target)
                except ValueError:
                    due_by_horizon = {horizon: None for horizon in (1, 3, 5)}
                else:
                    due_by_horizon = {
                        horizon: (
                            calendar_sessions[target_index + horizon]
                            if target_index + horizon < len(calendar_sessions)
                            else None
                        )
                        for horizon in (1, 3, 5)
                    }
                item["forward_paths"] = _enrollment_forward_paths(
                    {
                        "package": item["source_capture_id"],
                        "capture": item["anchor_capture_id"],
                        "source_symbol": item["source_symbol"],
                        "calendar_revision": item["calendar_revision"],
                        "scope": item["scope"],
                    },
                    forward_rows_by_id.get(enrollment_id, []),
                    due_by_horizon,
                    cutoff=research_cutoff,
                )
                item["forward_as_of"] = research_cutoff.isoformat()
                item["forward_classification"] = "RETROSPECTIVE_DESCRIPTIVE_NON_PIT"
        for item in items + event_items:
            details = displayed_items_by_id.get(item["enrollment_id"], {})
            for key in (
                "child_enrollments",
                "parent_history",
                "child_t0_session",
                "forward_paths",
                "forward_as_of",
                "forward_classification",
            ):
                if key in details:
                    item[key] = details[key]
        daily_event_status = (
            "NOT_COMPUTED"
            if input_rule_fact_count == 0
            else "INCOMPLETE"
            if missing_due_segment_count or incomplete_day_count
            else "AVAILABLE"
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
            "source_capture_id": source_capture_id,
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
            "observation_outcomes": outcome_summary,
            "daily_events": {
                "status": daily_event_status,
                "as_of": as_of.isoformat(),
                "event_filter": event_code or "ALL",
                "status_filter": status or "ALL",
                "event_counts": daily_event_counts,
                "quality_counts": {
                    "UNKNOWN": unknown_day_count,
                    "INCOMPLETE": incomplete_day_count
                    + missing_due_segment_count
                    - not_computed_day_count,
                    "NOT_COMPUTED": not_computed_day_count,
                },
                "denominator": {
                    "kind": "PERSISTED_SOURCE_OBSERVATION_DAY_FACTS",
                    "observed_segment_count": observed_day_segment_count,
                    "independent_source_symbol_count": observed_day_symbol_count,
                    "due_segment_count": due_segment_count,
                    "due_observed_segment_count": (
                        due_segment_count - missing_due_segment_count
                    ),
                    "missing_due_segment_count": missing_due_segment_count,
                    "input_rule_fact_count": input_rule_fact_count,
                    "input_rule_source_symbol_count": input_rule_symbol_count,
                    "current_risk_set_segment_count": risk_set_segment_count,
                    "current_risk_set_source_symbol_count": risk_set_symbol_count,
                },
                "pagination": {
                    "offset": event_offset,
                    "limit": event_limit,
                    "total_count": total_event_count,
                    "next_offset": event_offset + len(event_items)
                    if event_offset + len(event_items) < total_event_count
                    else None,
                },
                "results": event_items,
                "classification": "SOURCE_SYMBOL_ONLY / FORWARD_ADJUSTED / NON_PIT_EXPLORATORY",
            },
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
            """SELECT e.id,r.target_session,r.id,r.source_capture_id,
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
            if anchor[14] in {"ENTRY", "FIRST_OBSERVED_LEFT_CENSORED", "REENTRY"}
        }
        daily_ids = [
            enrollment_id
            for enrollment_id, anchor in anchors.items()
            if anchor[14] == "DAILY_LANDMARK"
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
                    AND o.source_capture_id=r.source_capture_id
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
        source_capture_ids = {row[3] for row in anchors.values()}
        for source_capture_id in source_capture_ids:
            capture_anchors = [
                (enrollment_id, row)
                for enrollment_id, row in anchors.items()
                if row[3] == source_capture_id
            ]
            symbols = sorted({row[4] for _, row in capture_anchors})
            if not due_horizons:
                continue
            cursor.execute(
                """SELECT target_session,source_capture_id,source_symbol,
                          horizon,path_rule_version,calendar_revision,
                          endpoint_session,maturity,endpoint_quality,endpoint_reason,
                          endpoint_numerator,endpoint_denominator,path_quality,
                          path_reasons,price_coordinate,scope,mfe_numerator,
                          mfe_denominator,mae_numerator,mae_denominator,
                          mdd_numerator,mdd_denominator
                   FROM source_forward_path_facts
                   WHERE target_session=%s AND source_capture_id=%s
                     AND source_symbol=ANY(%s)
                     AND horizon=ANY(%s) AND endpoint_session<=%s
                   ORDER BY source_capture_id,source_symbol,horizon,
                            path_rule_version,calendar_revision""",
                (target, source_capture_id, symbols, due_horizons, cutoff),
            )
            stored = cursor.fetchall()
            for enrollment_id, anchor in capture_anchors:
                forward_rows[(enrollment_id, anchor[4], anchor[3])] = [
                    row for row in stored if row[1] == anchor[3] and row[2] == anchor[4]
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
                if latest[9] == "INCOMPLETE":
                    current_status = "INCOMPLETE"
                elif latest[7] == "EXITED":
                    current_status = "EXITED"
                elif latest[1] != cutoff:
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
                "capture": anchor[3],
                "source_symbol": anchor[4],
                "calendar_revision": anchor[11],
                "scope": anchor[12],
            }
            result[enrollment_id] = {
                "status": current_status,
                "reason": reason,
                "history": history,
                "history_truncated": False,
                "segment_enrollment_id": timeline_id,
                "fact_id": latest[4] if latest else anchor[2],
                "state": latest[6] if latest else "TRUE",
                "source_capture_id": latest[3] if latest else anchor[3],
                "base_fact_id": anchor[2],
                "base_capture_id": anchor[3],
                "rule_id": anchor[5],
                "rule_version": anchor[6],
                "profile_revision": latest[12] if latest else anchor[7],
                "history_session_count": latest[13] if latest else anchor[8],
                "d1_history_session_count": latest[14] if latest else anchor[9],
                "event_window_start": (anchor[10].isoformat() if anchor[10] else None),
                "calendar_revision": anchor[11],
                "scope": anchor[12],
                "parent_enrollment_id": anchor[13],
                "enrollment_kind": anchor[14],
                "forward_paths": _enrollment_forward_paths(
                    forward_anchor,
                    forward_rows.get((enrollment_id, anchor[4], anchor[3]), []),
                    due_by_horizon,
                    cutoff=cutoff,
                ),
                "reader": "PG只读已持久化来源规则观察段",
            }
        return result
