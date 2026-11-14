"""从精确已存来源事实生成和读取来源级研究登记。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping

from stock_a.domain.source_research_observation import (
    Observation,
    advance_observation,
    initial_observation,
)
from stock_a.storage.postgres.source_raw import _transaction


class EnrollmentConflict(ValueError):
    """登记业务键已有不同内容，拒绝静默覆盖。"""

    preserve_transaction_error = True


def _insert_enrollments(cursor: Any, rows: list[tuple[Any, ...]]) -> int:
    """批量写入并对照同事务读回；父事实是不可变的业务证据。"""
    if not rows:
        return 0
    cursor.executemany(
        """INSERT INTO source_research_enrollments
           (d0_fact_id, rule_fact_id, previous_rule_fact_id, enrollment_kind, reason_code,
            adjacent_d0_fact_id)
           VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        rows,
    )
    inserted = cursor.rowcount
    d0_ids = [row[0] for row in rows if row[0] is not None]
    rule_ids = [row[1] for row in rows if row[1] is not None]
    cursor.execute(
        """SELECT d0_fact_id, rule_fact_id, previous_rule_fact_id, enrollment_kind, reason_code,
                  adjacent_d0_fact_id
           FROM source_research_enrollments
           WHERE d0_fact_id = ANY(%s) OR rule_fact_id = ANY(%s)""",
        (d0_ids, rule_ids),
    )
    actual = set(cursor.fetchall())
    if actual != set(rows):
        raise EnrollmentConflict("同一来源事实已有不同研究登记，事务已回滚")
    return inserted


def insert_d0_enrollments(
    cursor: Any,
    *,
    target: date,
    package: int,
    capture: int,
    before: date,
    before_capture: int,
    producer_version: str,
    algorithm_contract: str,
    session_contract: str,
    calendar_revision: str,
) -> dict[str, int]:
    """在D0生产事务内引用该目标分区的已证TRUE事实。"""
    cursor.execute(
        """SELECT id, before_session, before_state, after_state
           FROM source_d0_launch_day_facts
           WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s
             AND before_capture_id=%s AND producer_version=%s AND algorithm_contract=%s
             AND session_contract=%s AND calendar_revision=%s
             AND after_state='TRUE' ORDER BY id""",
        (
            target,
            package,
            capture,
            before_capture,
            producer_version,
            algorithm_contract,
            session_contract,
            calendar_revision,
        ),
    )
    rows = []
    for fact_id, actual_before, before_state, after_state in cursor.fetchall():
        if actual_before != before:
            raise ValueError("D0前态不是指定的相邻官方会话")
        previous = (
            Observation(before, before_state, fact_id)
            if before_state in {"TRUE", "FALSE", "UNKNOWN"}
            else None
        )
        decision = initial_observation(
            Observation(target, after_state, fact_id),
            previous=previous,
            previous_official_session=before,
        )
        rows.append((fact_id, None, None, decision.event, decision.reason, None))
    return {"expected": len(rows), "inserted": _insert_enrollments(cursor, rows)}


def insert_rule_enrollments(
    cursor: Any,
    *,
    target: date,
    before: date,
    package: int,
    capture: int,
    rule_version: str,
    profile_revision: str,
    history_count: int,
    d1_history_count: int,
    event_window_start: date,
    calendar_revision: str,
) -> dict[str, int]:
    """按同规则和同来源坐标关联相邻前态；不唯一或缺失时按左截断处理。"""
    cursor.execute(
        """SELECT r.id, count(DISTINCT p.id), min(p.id), min(p.state),
                  count(DISTINCT d.id), min(d.id), min(d.before_state)
           FROM source_rule_evaluations r
           LEFT JOIN source_rule_evaluations p
             ON p.target_session=%s AND p.source_package_id=r.source_package_id
            AND p.source_symbol=r.source_symbol AND p.rule_id=r.rule_id
            AND p.rule_version=r.rule_version AND p.profile_revision=r.profile_revision
            AND p.history_session_count=r.history_session_count
            AND p.d1_history_session_count=r.d1_history_session_count
            AND p.calendar_revision=r.calendar_revision AND p.scope=r.scope
           LEFT JOIN source_d0_launch_day_facts d
             ON r.rule_id='LAUNCH_CONFIRM' AND d.target_session=r.target_session
            AND d.before_session=%s AND d.source_package_id=r.source_package_id
            AND d.target_capture_id=r.source_capture_id AND d.source_symbol=r.source_symbol
            AND d.calendar_revision=r.calendar_revision AND d.after_state=r.state
            AND d.price_coordinate='RAW_UNADJUSTED'
           WHERE r.target_session=%s AND r.source_package_id=%s AND r.source_capture_id=%s
             AND r.rule_version=%s AND r.profile_revision=%s AND r.history_session_count=%s
             AND r.d1_history_session_count=%s AND r.event_window_start=%s
             AND r.calendar_revision=%s AND r.state='TRUE'
           GROUP BY r.id ORDER BY r.id""",
        (
            before,
            before,
            target,
            package,
            capture,
            rule_version,
            profile_revision,
            history_count,
            d1_history_count,
            event_window_start,
            calendar_revision,
        ),
    )
    rows = []
    for (
        fact_id,
        previous_count,
        previous_id,
        previous_state,
        d0_count,
        d0_id,
        d0_before_state,
    ) in cursor.fetchall():
        if previous_count == 1 and d0_count == 1 and previous_state != d0_before_state:
            raise EnrollmentConflict("规则前态与同谓词D0相邻证据不一致")
        previous = (
            Observation(before, d0_before_state, d0_id)
            if d0_count == 1 and d0_before_state in {"TRUE", "FALSE", "UNKNOWN"}
            else Observation(before, previous_state, previous_id)
            if previous_count == 1
            else None
        )
        decision = initial_observation(
            Observation(target, "TRUE", fact_id),
            previous=previous,
            previous_official_session=before,
        )
        rows.append(
            (
                None,
                fact_id,
                previous_id if previous_count == 1 else None,
                decision.event,
                decision.reason,
                d0_id if d0_count == 1 else None,
            )
        )
    return {"expected": len(rows), "inserted": _insert_enrollments(cursor, rows)}


def persist_existing_enrollments(
    config: Mapping[str, Any],
    *,
    target: date,
    before: date,
    package: int,
    d1_history_count: int,
) -> dict[str, Any]:
    """在一个单日事务中引用既存完整D0和规则事实，不读取价格历史。"""
    if (
        type(target) is not date
        or type(before) is not date
        or before >= target
        or type(package) is not int
        or package <= 0
        or type(d1_history_count) is not int
        or d1_history_count <= 0
    ):
        raise ValueError("研究登记目标范围无效")
    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """SELECT count(*), count(DISTINCT source_symbol), count(DISTINCT source_capture_id),
                      min(source_capture_id) FROM source_raw_daily
               WHERE session_date=%s AND source_package_id=%s""",
            (target, package),
        )
        raw_count, raw_symbols, captures, capture = cursor.fetchone()
        if not raw_count or raw_count != raw_symbols or captures != 1:
            raise ValueError("目标日Raw来源池不唯一或不完整")
        cursor.execute(
            """SELECT normalized_count FROM source_captures
               WHERE capture_id=%s AND source_package_id=%s AND target_session=%s""",
            (capture, package, target),
        )
        if cursor.fetchone() != (raw_count,):
            raise ValueError("目标来源捕获的分母与Raw行数不一致")
        cursor.execute(
            """SELECT before_session,before_capture_id,producer_version,algorithm_contract,
                      session_contract,calendar_revision,count(*),count(DISTINCT source_symbol),
                      min(target_source_count),max(target_source_count),
                      min(union_source_count),max(union_source_count)
               FROM source_d0_launch_day_facts
               WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s
               GROUP BY 1,2,3,4,5,6""",
            (target, package, capture),
        )
        d0_scopes = cursor.fetchall()
        if (
            len(d0_scopes) != 1
            or d0_scopes[0][0] != before
            or d0_scopes[0][6] != d0_scopes[0][7]
            or d0_scopes[0][8] != raw_count
            or d0_scopes[0][9] != raw_count
            or d0_scopes[0][10] != d0_scopes[0][11]
            or d0_scopes[0][6] != d0_scopes[0][10]
        ):
            raise ValueError("D0事实不是唯一完整的相邻会话范围")
        _, before_capture, producer, algorithm, session_contract, d0_calendar, *_ = (
            d0_scopes[0]
        )
        cursor.execute(
            """SELECT count(*) FROM source_raw_daily raw
               LEFT JOIN source_d0_launch_day_facts d
                 ON d.target_session=raw.session_date AND d.source_package_id=raw.source_package_id
                AND d.target_capture_id=raw.source_capture_id AND d.source_symbol=raw.source_symbol
                AND d.before_session=%s AND d.before_capture_id=%s AND d.producer_version=%s
                AND d.algorithm_contract=%s AND d.session_contract=%s AND d.calendar_revision=%s
               WHERE raw.session_date=%s AND raw.source_package_id=%s AND raw.source_capture_id=%s
                 AND d.id IS NULL""",
            (
                before,
                before_capture,
                producer,
                algorithm,
                session_contract,
                d0_calendar,
                target,
                package,
                capture,
            ),
        )
        if cursor.fetchone()[0]:
            raise ValueError("D0来源代码集合未覆盖目标Raw分区")
        cursor.execute(
            """SELECT rule_version,profile_revision,history_session_count,
                      d1_history_session_count,event_window_start,calendar_revision,
                      count(*),count(DISTINCT source_symbol),count(DISTINCT rule_id),
                      count(*) FILTER (WHERE rule_id = ANY(%s))
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
                 AND d1_history_session_count=%s
               GROUP BY 1,2,3,4,5,6""",
            (
                [
                    "LAUNCH_CONFIRM",
                    "RECOVERY_TURN",
                    "s1",
                    "s2",
                    "seed",
                    "D1_T0",
                    "D1_ANCHOR_WINDOW",
                    "STRONG_PULLBACK_DIAGNOSTIC",
                    "TREND_CONTINUE_DIAGNOSTIC",
                ],
                target,
                package,
                capture,
                d1_history_count,
            ),
        )
        scopes = cursor.fetchall()
        if len(scopes) != 1 or tuple(scopes[0][-4:]) != (
            raw_count * 9,
            raw_count,
            9,
            raw_count * 9,
        ):
            raise ValueError("规则事实不是唯一完整的九规则来源池范围")
        version, profile, history_count, d1_count, window_start, calendar_revision = (
            scopes[0][:6]
        )
        cursor.execute(
            """SELECT count(*),count(DISTINCT source_symbol) FROM source_pool_seed_day_facts
               WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s
                 AND profile_revision=%s AND history_session_count=%s""",
            (target, package, capture, profile, history_count),
        )
        if cursor.fetchone() != (raw_count, raw_count):
            raise ValueError("Seed与规则的来源、画像或完整分母不一致")
        cursor.execute(
            """SELECT count(*),count(DISTINCT source_symbol),count(DISTINCT structure_type)
               FROM source_d1_structure_day_facts
               WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s
                 AND d1_history_session_count=%s AND calendar_revision=%s""",
            (target, package, capture, d1_count, calendar_revision),
        )
        if cursor.fetchone() != (raw_count * 5, raw_count, 5):
            raise ValueError("D1结构与规则的日历或完整分母不一致")
        if d0_calendar != calendar_revision:
            raise ValueError("D0与规则日历修订不一致")
        d0 = insert_d0_enrollments(
            cursor,
            target=target,
            package=package,
            capture=capture,
            before=before,
            before_capture=before_capture,
            producer_version=producer,
            algorithm_contract=algorithm,
            session_contract=session_contract,
            calendar_revision=d0_calendar,
        )
        rules = insert_rule_enrollments(
            cursor,
            target=target,
            before=before,
            package=package,
            capture=capture,
            rule_version=version,
            profile_revision=profile,
            history_count=history_count,
            d1_history_count=d1_count,
            event_window_start=window_start,
            calendar_revision=calendar_revision,
        )
    return {
        "status": "PERSISTED",
        "target_session": target.isoformat(),
        "source_package_id": package,
        "source_capture_id": capture,
        "source_count": raw_count,
        "d0": d0,
        "rules": rules,
    }


def read_enrollments_for_symbols(
    config: Mapping[str, Any],
    *,
    target: date,
    package: int,
    capture: int,
    before: date,
    before_capture: int,
    include_d0: bool,
    symbols: list[str],
    rule_scope: Mapping[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    """仅按当前页来源键读取，所有证据详情仍由原typed事实提供。"""
    if (
        type(target) is not date
        or type(package) is not int
        or package <= 0
        or type(capture) is not int
        or capture <= 0
        or type(before) is not date
        or before >= target
        or type(before_capture) is not int
        or before_capture <= 0
        or type(include_d0) is not bool
        or len(symbols) > 100
        or len(set(symbols)) != len(symbols)
    ):
        raise ValueError("研究登记读取范围无效")
    if not symbols:
        return {}
    with _transaction(config, write=False) as (_, cursor, _):
        if include_d0:
            cursor.execute(
                """SELECT d.source_symbol,e.id,e.enrollment_kind,e.reason_code,
                      'D0' AS fact_type,d.id,d.algorithm_contract,d.producer_version,
                      d.source_time_quality,d.source_clock_role,d.source_available_at,
                      d.research_mode,d.price_coordinate
               FROM source_d0_launch_day_facts d
               JOIN source_research_enrollments e ON e.d0_fact_id=d.id
               WHERE d.target_session=%s AND d.source_package_id=%s
                 AND d.target_capture_id=%s AND d.before_session=%s
                 AND d.before_capture_id=%s AND d.source_symbol=ANY(%s)
               ORDER BY d.source_symbol,e.id""",
                (target, package, capture, before, before_capture, symbols),
            )
            d0_rows = cursor.fetchall()
        else:
            d0_rows = []
        result: dict[str, list[dict[str, Any]]] = {}
        for row in d0_rows:
            (
                symbol,
                enrollment_id,
                kind,
                reason,
                fact_type,
                fact_id,
                contract,
                version,
                quality,
                clock,
                available,
                mode,
                coordinate,
            ) = row
            result.setdefault(symbol, []).append(
                {
                    "id": enrollment_id,
                    "kind": kind,
                    "reason": reason,
                    "fact_type": fact_type,
                    "fact_id": fact_id,
                    "rule_contract": contract,
                    "rule_version": version,
                    "source_time_quality": quality,
                    "source_clock_role": clock,
                    "source_available_at": available.isoformat() if available else None,
                    "scope": mode,
                    "price_coordinate": coordinate,
                }
            )
        if rule_scope is not None:
            cursor.execute(
                """SELECT r.source_symbol,e.id,e.enrollment_kind,e.reason_code,r.id,
                          r.rule_id,r.rule_version,r.profile_revision,
                          c.source_time_quality,c.source_clock_role,c.source_available_at,r.scope,
                          e.adjacent_d0_fact_id
                   FROM source_rule_evaluations r
                   JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                   JOIN source_captures c ON c.capture_id=r.source_capture_id
                   WHERE r.target_session=%s AND r.source_package_id=%s
                     AND r.source_capture_id=%s AND r.source_symbol=ANY(%s)
                     AND r.rule_version=%s AND r.profile_revision=%s
                     AND r.history_session_count=%s AND r.d1_history_session_count=%s
                     AND r.event_window_start=%s AND r.calendar_revision=%s
                   ORDER BY r.source_symbol,e.id""",
                (
                    target,
                    package,
                    capture,
                    symbols,
                    rule_scope["rule_version"],
                    rule_scope["profile_revision"],
                    rule_scope["history_session_count"],
                    rule_scope["d1_history_session_count"],
                    rule_scope["event_window_start"],
                    rule_scope["calendar_revision"],
                ),
            )
            for (
                symbol,
                enrollment_id,
                kind,
                reason,
                fact_id,
                rule_id,
                version,
                profile,
                quality,
                clock,
                available,
                scope,
                adjacent_d0_fact_id,
            ) in cursor.fetchall():
                result.setdefault(symbol, []).append(
                    {
                        "id": enrollment_id,
                        "kind": kind,
                        "reason": reason,
                        "fact_type": "RULE",
                        "fact_id": fact_id,
                        "rule_id": rule_id,
                        "adjacent_d0_fact_id": adjacent_d0_fact_id,
                        "rule_version": version,
                        "profile_revision": profile,
                        "source_time_quality": quality,
                        "source_clock_role": clock,
                        "source_available_at": available.isoformat()
                        if available
                        else None,
                        "scope": scope,
                        "price_coordinate": "RAW_UNADJUSTED",
                    }
                )
    return result


def read_enrollment_followups(
    config: Mapping[str, Any],
    *,
    target: date,
    cutoff: date,
    enrollment_ids: list[int],
    successor_sessions: tuple[date, ...],
) -> dict[int, dict[str, Any]]:
    """校验登记锚定目标后，按至多20个官方后继会话读取同规则事实。"""
    if (
        type(target) is not date
        or type(cutoff) is not date
        or target > cutoff
        or len(enrollment_ids) > 300
        or len(set(enrollment_ids)) != len(enrollment_ids)
        or any(type(item) is not int or item <= 0 for item in enrollment_ids)
        or any(
            type(day) is not date or not target < day <= cutoff
            for day in successor_sessions
        )
        or tuple(sorted(set(successor_sessions))) != successor_sessions
    ):
        raise ValueError("研究登记后继读取范围无效")
    if not enrollment_ids:
        return {}
    selected = successor_sessions[:20]
    rows: list[tuple[Any, ...]] = []
    invalid_anchor_scope = False
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT e.id,
                      CASE WHEN e.d0_fact_id IS NOT NULL THEN d.target_session
                           ELSE r.target_session END,
                      e.d0_fact_id IS NOT NULL,e.rule_fact_id IS NOT NULL
               FROM source_research_enrollments e
               LEFT JOIN source_d0_launch_day_facts d ON d.id=e.d0_fact_id
               LEFT JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE e.id=ANY(%s)""",
            (enrollment_ids,),
        )
        anchors = cursor.fetchall()
        invalid_anchor_scope = (
            len(anchors) != len(enrollment_ids)
            or {row[0] for row in anchors} != set(enrollment_ids)
            or any(row[1] != target or row[2] == row[3] for row in anchors)
        )
        if not invalid_anchor_scope and successor_sessions:
            cursor.execute(
                """SELECT e.id,n.target_session,n.id,n.after_state,n.before_session,
                      n.before_state,n.before_capture_id,n.target_capture_id,b.target_capture_id
               FROM source_research_enrollments e
               JOIN source_d0_launch_day_facts b ON b.id=e.d0_fact_id
               JOIN source_d0_launch_day_facts n ON n.source_symbol=b.source_symbol
                 AND n.source_package_id=b.source_package_id
                 AND n.producer_version=b.producer_version
                 AND n.algorithm_contract=b.algorithm_contract
                 AND n.session_contract=b.session_contract
                 AND n.calendar_revision=b.calendar_revision
                 AND n.price_coordinate=b.price_coordinate AND n.research_mode=b.research_mode
               WHERE e.id=ANY(%s) AND n.target_session=ANY(%s)""",
                (enrollment_ids, list(selected)),
            )
            rows = list(cursor.fetchall())
            cursor.execute(
                """SELECT e.id,n.target_session,n.id,n.state,NULL::date,
                      NULL::text,NULL::bigint,NULL::bigint,NULL::bigint
               FROM source_research_enrollments e
               JOIN source_rule_evaluations b ON b.id=e.rule_fact_id
               JOIN source_rule_evaluations n ON n.source_symbol=b.source_symbol
                 AND n.source_package_id=b.source_package_id AND n.rule_id=b.rule_id
                 AND n.rule_version=b.rule_version AND n.profile_revision=b.profile_revision
                 AND n.history_session_count=b.history_session_count
                 AND n.d1_history_session_count=b.d1_history_session_count
                 AND n.calendar_revision=b.calendar_revision AND n.scope=b.scope
               WHERE e.id=ANY(%s) AND n.target_session=ANY(%s)""",
                (enrollment_ids, list(selected)),
            )
            rows.extend(cursor.fetchall())
    if invalid_anchor_scope:
        raise ValueError("研究登记父事实与请求目标会话不一致，拒绝跨日期附着")
    if not successor_sessions:
        return {
            item: {"status": "NOT_DUE", "reason": "WAITING_FOR_FUTURE_SESSION"}
            for item in enrollment_ids
        }
    by_key: dict[tuple[int, date], list[tuple[Any, ...]]] = {}
    base_captures = {}
    for (
        enrollment_id,
        session,
        fact_id,
        state,
        before,
        before_state,
        before_capture,
        capture,
        base_capture,
    ) in rows:
        by_key.setdefault((enrollment_id, session), []).append(
            (fact_id, state, before, before_state, before_capture, capture)
        )
        if base_capture is not None:
            base_captures[enrollment_id] = base_capture
    output = {}
    for enrollment_id in enrollment_ids:
        lifecycle = "ACTIVE"
        previous = target
        previous_state = "TRUE"
        previous_capture = base_captures.get(enrollment_id)
        last_exit = None
        detail = None
        for session in selected:
            candidates = by_key.get((enrollment_id, session), [])
            valid = [
                row
                for row in candidates
                if row[2] is None
                or (
                    row[2] == previous
                    and (previous_capture is None or row[4] == previous_capture)
                    and (
                        previous_state not in {"TRUE", "FALSE"}
                        or row[3] == previous_state
                    )
                )
            ]
            current = (
                Observation(session, valid[0][1], valid[0][0])
                if len(candidates) == len(valid) == 1
                and valid[0][1] in {"TRUE", "FALSE", "UNKNOWN"}
                else None
            )
            decision = advance_observation(
                lifecycle=lifecycle,
                previous_session=previous,
                expected_session=session,
                current=current,
                research_cutoff=cutoff,
            )
            lifecycle = decision.lifecycle
            if decision.event == "EXIT_PROVED":
                last_exit = session.isoformat()
            detail = {
                "status": decision.event,
                "reason": decision.reason,
                "session": session.isoformat(),
                "fact_id": current.evidence_id if current else None,
                "last_proved_exit_session": last_exit,
            }
            if current is None and candidates:
                detail["reason"] = "AMBIGUOUS_OR_NONADJACENT_FACT"
            previous_state = current.state if current else None
            previous_capture = (
                valid[0][5] if current and valid[0][2] is not None else None
            )
            previous = session
        assert detail is not None
        if len(successor_sessions) > len(selected):
            detail = {
                **detail,
                "status": "INCOMPLETE",
                "reason": "FOLLOWUP_WINDOW_LIMIT",
                "last_evaluated_status": detail["status"],
            }
        output[enrollment_id] = detail
    return output
