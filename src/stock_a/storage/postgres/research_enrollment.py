"""从精确已存来源事实生成和读取来源级研究登记。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence
from urllib.parse import unquote

from stock_a.domain.source_research_observation import (
    Observation,
    advance_observation,
    initial_observation,
)
from stock_a.storage.postgres.source_raw import _transaction


class EnrollmentConflict(ValueError):
    """登记业务键已有不同内容，拒绝静默覆盖。"""

    preserve_transaction_error = True


def _profile_revision_context(
    revision: str, history_count: int
) -> tuple[str, str, str] | None:
    """解析含明确窗口和日历身份的PROFILE修订，不接受短键。"""
    if not isinstance(revision, str):
        return None
    parts = revision.split("|")
    if len(parts) != 4 or not parts[1].isdecimal() or int(parts[1]) != history_count:
        return None
    return parts[0], unquote(parts[2]), unquote(parts[3])


def _read_rule_day_contexts(
    cursor: Any, *, sessions: Sequence[date], package: int
) -> dict[date, dict[str, Any]]:
    """读取每个目标日各自闭合的Raw、Profile、Seed、D1和规则范围。"""
    from stock_a.storage.postgres.research_facts import RULES, RULE_VERSION, SCOPE

    selected = tuple(sorted(set(sessions)))
    if not selected:
        return {}
    cursor.execute(
        """SELECT c.target_session,c.capture_id,c.parser_contract,c.source_time_quality,
                  c.source_clock_role,c.normalized_count,count(r.id),
                  count(DISTINCT r.source_symbol)
           FROM source_captures c
           LEFT JOIN source_raw_daily r
             ON r.session_date=c.target_session
            AND r.source_package_id=c.source_package_id
            AND r.source_capture_id=c.capture_id
           WHERE c.target_session=ANY(%s) AND c.source_package_id=%s
           GROUP BY c.target_session,c.capture_id,c.parser_contract,c.source_time_quality,
                    c.source_clock_role,c.normalized_count
           ORDER BY c.target_session,c.capture_id""",
        (list(selected), package),
    )
    capture_rows = cursor.fetchall()
    cursor.execute(
        """SELECT target_session,source_capture_id,profile_revision,
                  history_requested_session_count,history_stored_session_count,
                  history_calendar_id,history_calendar_revision,history_window_start,
                  history_window_end,price_coordinate,count(*),count(DISTINCT source_symbol)
           FROM source_symbol_price_profiles
           WHERE target_session=ANY(%s) AND source_package_id=%s
           GROUP BY 1,2,3,4,5,6,7,8,9,10 ORDER BY 1,2,3""",
        (list(selected), package),
    )
    profile_rows = cursor.fetchall()
    cursor.execute(
        """SELECT target_session,source_capture_id,profile_revision,history_session_count,
                  history_window_start,history_window_end,calendar_revision,research_mode,
                  price_coordinate,producer_contract,producer_version,count(*),
                  count(DISTINCT source_symbol)
           FROM source_pool_seed_day_facts
           WHERE target_session=ANY(%s) AND source_package_id=%s
           GROUP BY 1,2,3,4,5,6,7,8,9,10,11 ORDER BY 1,2,3""",
        (list(selected), package),
    )
    seed_rows = cursor.fetchall()
    cursor.execute(
        """SELECT target_session,target_capture_id,d1_history_session_count,
                  history_window_start,history_window_end,calendar_revision,
                  producer_contract,producer_version,research_mode,price_coordinate,
                  count(*),count(DISTINCT source_symbol),count(DISTINCT structure_type)
           FROM source_d1_structure_day_facts
           WHERE target_session=ANY(%s) AND source_package_id=%s
           GROUP BY 1,2,3,4,5,6,7,8,9,10 ORDER BY 1,2,3""",
        (list(selected), package),
    )
    d1_rows = cursor.fetchall()
    cursor.execute(
        """SELECT target_session,source_capture_id,rule_version,profile_revision,
                  history_session_count,d1_history_session_count,event_window_start,
                  calendar_revision,scope,count(*),count(DISTINCT source_symbol),
                  count(DISTINCT rule_id)
           FROM source_rule_evaluations
           WHERE target_session=ANY(%s) AND source_package_id=%s
           GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY 1,2,3,4""",
        (list(selected), package),
    )
    rule_rows = cursor.fetchall()

    by_table: dict[str, dict[date, list[tuple[Any, ...]]]] = {
        name: {session: [] for session in selected}
        for name in ("captures", "profiles", "seeds", "d1", "rules")
    }
    for row, key in (
        *((row, "captures") for row in capture_rows),
        *((row, "profiles") for row in profile_rows),
        *((row, "seeds") for row in seed_rows),
        *((row, "d1") for row in d1_rows),
        *((row, "rules") for row in rule_rows),
    ):
        by_table[key][row[0]].append(row[1:])

    contexts: dict[date, dict[str, Any]] = {}
    for session in selected:
        captures = by_table["captures"][session]
        context: dict[str, Any] = {
            "session": session,
            "package": package,
            "capture": None,
            "source_count": 0,
            "inputs_complete": False,
            "rules_present": bool(by_table["rules"][session]),
            "rules_complete": False,
            "reason": "DAY_SCOPE_MISSING_OR_AMBIGUOUS",
        }
        if len(captures) != 1:
            contexts[session] = context
            continue
        (
            capture,
            parser_contract,
            source_time_quality,
            source_clock_role,
            normalized_count,
            raw_count,
            raw_symbols,
        ) = captures[0]
        context.update(
            capture=capture,
            parser_contract=parser_contract,
            source_time_quality=source_time_quality,
            source_clock_role=source_clock_role,
            normalized_count=normalized_count,
            source_count=raw_count,
        )
        profile_scopes = by_table["profiles"][session]
        seed_scopes = by_table["seeds"][session]
        d1_scopes = by_table["d1"][session]
        if not (raw_count > 0 and raw_count == raw_symbols == normalized_count):
            context["reason"] = "RAW_PARTITION_NOT_CLOSED"
            contexts[session] = context
            continue
        if not (len(profile_scopes) == len(seed_scopes) == len(d1_scopes) == 1):
            context["reason"] = "PROFILE_SEED_OR_D1_SCOPE_AMBIGUOUS"
            contexts[session] = context
            continue
        profile = profile_scopes[0]
        seed = seed_scopes[0]
        d1 = d1_scopes[0]
        (
            profile_capture,
            profile_revision,
            requested_count,
            stored_count,
            history_calendar_id,
            history_calendar_revision,
            profile_start,
            profile_end,
            profile_coordinate,
            profile_count,
            profile_symbols,
        ) = profile
        (
            seed_capture,
            seed_revision,
            history_count,
            seed_start,
            seed_end,
            seed_calendar_revision,
            research_mode,
            seed_coordinate,
            seed_contract,
            seed_version,
            seed_count,
            seed_symbols,
        ) = seed
        (
            d1_capture,
            d1_history_count,
            d1_start,
            d1_end,
            d1_calendar_revision,
            d1_contract,
            d1_version,
            d1_research_mode,
            d1_coordinate,
            d1_count,
            d1_symbols,
            structure_count,
        ) = d1
        revision_context = _profile_revision_context(profile_revision, history_count)
        inputs_complete = (
            profile_capture == seed_capture == d1_capture == capture
            and profile_count == profile_symbols == raw_count
            and seed_count == seed_symbols == raw_count
            and d1_count == raw_count * 5
            and d1_symbols == raw_count
            and structure_count == 5
            and requested_count == stored_count == history_count
            and profile_start == seed_start
            and profile_end == seed_end == session
            and profile_end == session
            and history_calendar_revision == d1_calendar_revision
            and bool(seed_calendar_revision)
            and profile_coordinate
            == seed_coordinate
            == d1_coordinate
            == "RAW_UNADJUSTED"
            and research_mode == d1_research_mode == "NON_PIT_RETROSPECTIVE"
            and revision_context
            == (revision_context[0], history_calendar_id, history_calendar_revision)
            if revision_context is not None
            else False
        )
        if not inputs_complete:
            context["reason"] = "PROFILE_SEED_D1_CONTEXT_MISMATCH"
            contexts[session] = context
            continue
        context.update(
            inputs_complete=True,
            reason=None,
            profile={
                "revision": profile_revision,
                "revision_context": revision_context,
                "history_count": history_count,
                "window_start": profile_start,
                "window_end": profile_end,
                "calendar_id": history_calendar_id,
                "calendar_revision": history_calendar_revision,
                "price_coordinate": profile_coordinate,
            },
            seed={
                "contract": seed_contract,
                "version": seed_version,
                "calendar_revision": seed_calendar_revision,
                "research_mode": research_mode,
                "price_coordinate": seed_coordinate,
            },
            d1={
                "history_count": d1_history_count,
                "window_start": d1_start,
                "window_end": d1_end,
                "calendar_revision": d1_calendar_revision,
                "contract": d1_contract,
                "version": d1_version,
                "research_mode": d1_research_mode,
                "price_coordinate": d1_coordinate,
            },
        )
        rule_scopes = by_table["rules"][session]
        if len(rule_scopes) != 1:
            context["rules_present"] = bool(rule_scopes)
            context["rules_reason"] = "RULE_SCOPE_MISSING_OR_AMBIGUOUS"
            contexts[session] = context
            continue
        rule_scope = rule_scopes[0]
        (
            rule_capture,
            rule_version,
            rule_profile,
            rule_history_count,
            rule_d1_count,
            event_window_start,
            rule_calendar_revision,
            scope,
            rule_count,
            rule_symbols,
            rule_id_count,
        ) = rule_scope
        rules_complete = (
            rule_capture == capture
            and rule_version == RULE_VERSION
            and rule_profile == profile_revision == seed_revision
            and rule_history_count == history_count
            and rule_d1_count == d1_history_count
            and event_window_start == d1_start
            and rule_calendar_revision == history_calendar_revision
            and scope == SCOPE
            and rule_count == raw_count * len(RULES)
            and rule_symbols == raw_count
            and rule_id_count == len(RULES)
            and d1_end == session
        )
        context.update(
            rules_complete=rules_complete,
            rules_reason=None if rules_complete else "RULE_PARTITION_NOT_CLOSED",
            rule={
                "capture": rule_capture,
                "version": rule_version,
                "profile_revision": rule_profile,
                "history_count": rule_history_count,
                "d1_history_count": rule_d1_count,
                "event_window_start": event_window_start,
                "calendar_revision": rule_calendar_revision,
                "scope": scope,
            },
        )
        contexts[session] = context
    return contexts


def _rule_context_comparable(
    before_context: Mapping[str, Any] | None,
    target_context: Mapping[str, Any] | None,
    *,
    before: date,
    target: date,
) -> bool:
    """仅允许完整同语义规则范围的一会话窗口增长或固定长度滚动。"""
    if not before_context or not target_context:
        return False
    if not all(
        item.get("inputs_complete") and item.get("rules_complete")
        for item in (before_context, target_context)
    ):
        return False
    from stock_a.research.entry_context.sessions import _load_calendar

    _, _, _, official_sessions = _load_calendar(target)
    try:
        before_index = official_sessions.index(before)
        target_index = official_sessions.index(target)
        before_profile_start = official_sessions.index(
            before_context["profile"]["window_start"]
        )
        target_profile_start = official_sessions.index(
            target_context["profile"]["window_start"]
        )
        before_d1_start = official_sessions.index(before_context["d1"]["window_start"])
        target_d1_start = official_sessions.index(target_context["d1"]["window_start"])
    except ValueError:
        return False
    if target_index != before_index + 1:
        return False
    before_profile_count = before_context["profile"]["history_count"]
    target_profile_count = target_context["profile"]["history_count"]
    profile_window_grew = (
        target_profile_count == before_profile_count + 1
        and target_profile_start == before_profile_start
    )
    profile_window_rolled = (
        target_profile_count == before_profile_count
        and target_profile_start == before_profile_start + 1
    )
    if not (profile_window_grew or profile_window_rolled):
        return False
    if (
        before_index - before_profile_start + 1 != before_profile_count
        or target_index - target_profile_start + 1 != target_profile_count
        or before_index - before_d1_start + 1 != before_context["d1"]["history_count"]
        or target_index - target_d1_start + 1 != target_context["d1"]["history_count"]
        or target_d1_start != before_d1_start + 1
    ):
        return False
    before_profile = before_context["profile"]
    target_profile = target_context["profile"]
    before_rule = before_context["rule"]
    target_rule = target_context["rule"]
    return all(
        (
            before_context["package"] == target_context["package"],
            before_context["source_time_quality"]
            == target_context["source_time_quality"],
            before_context["source_clock_role"] == target_context["source_clock_role"],
            before_context["parser_contract"] == target_context["parser_contract"],
            before_profile["revision_context"] == target_profile["revision_context"],
            before_profile["window_end"] == before,
            target_profile["window_end"] == target,
            before_context["seed"]["contract"] == target_context["seed"]["contract"],
            before_context["seed"]["version"] == target_context["seed"]["version"],
            before_context["seed"]["calendar_revision"]
            == target_context["seed"]["calendar_revision"],
            before_context["seed"]["research_mode"]
            == target_context["seed"]["research_mode"],
            before_context["seed"]["price_coordinate"]
            == target_context["seed"]["price_coordinate"],
            before_context["d1"]["history_count"]
            == target_context["d1"]["history_count"],
            before_context["d1"]["contract"] == target_context["d1"]["contract"],
            before_context["d1"]["version"] == target_context["d1"]["version"],
            before_context["d1"]["window_end"] == before,
            target_context["d1"]["window_end"] == target,
            before_context["d1"]["research_mode"]
            == target_context["d1"]["research_mode"],
            before_context["d1"]["price_coordinate"]
            == target_context["d1"]["price_coordinate"],
            before_rule["version"] == target_rule["version"],
            before_rule["capture"] == before_context["capture"],
            target_rule["capture"] == target_context["capture"],
            before_rule["profile_revision"] == before_profile["revision"],
            target_rule["profile_revision"] == target_profile["revision"],
            before_rule["d1_history_count"] == target_rule["d1_history_count"],
            before_rule["calendar_revision"] == target_rule["calendar_revision"],
            before_rule["scope"] == target_rule["scope"],
            before_rule["event_window_start"] == before_context["d1"]["window_start"],
            target_rule["event_window_start"] == target_context["d1"]["window_start"],
        )
    )


def _insert_rule_enrollments(
    cursor: Any, rows: list[tuple[Any, ...]]
) -> tuple[int, int]:
    """幂等插入并仅纠正缺前态时形成的精确规则登记。"""
    if not rows:
        return 0, 0
    rule_ids = [row[1] for row in rows]
    cursor.execute(
        """SELECT d0_fact_id,rule_fact_id,previous_rule_fact_id,enrollment_kind,
                  reason_code,adjacent_d0_fact_id
           FROM source_research_enrollments WHERE rule_fact_id=ANY(%s)""",
        (rule_ids,),
    )
    existing = {row[1]: row for row in cursor.fetchall()}
    corrections = []
    for row in rows:
        old = existing.get(row[1])
        if old is None or old == row:
            continue
        same_parent = old[0] is None and old[1] == row[1] and old[5] == row[5]
        left_censored_correction = (
            same_parent
            and old[3] == "FIRST_OBSERVED_LEFT_CENSORED"
            and old[4] == "NO_PROVED_ADJACENT_ENTRY"
            and row[3] in {"ENTRY", "DAILY_LANDMARK", "FIRST_OBSERVED_LEFT_CENSORED"}
        )
        link_correction = (
            same_parent
            and old[3:5] == row[3:5]
            and old[2] is None
            and row[2] is not None
        )
        if not (left_censored_correction or link_correction):
            raise EnrollmentConflict("既存规则登记不符合精确前态纠错边界")
        corrections.append(row)
    for row in corrections:
        cursor.execute(
            """UPDATE source_research_enrollments
               SET previous_rule_fact_id=%s,enrollment_kind=%s,reason_code=%s
               WHERE rule_fact_id=%s AND d0_fact_id IS NULL""",
            (row[2], row[3], row[4], row[1]),
        )
        if cursor.rowcount != 1:
            raise EnrollmentConflict("规则登记纠错目标不再唯一，事务已回滚")
    inserted = _insert_enrollments(cursor, rows)
    return inserted, len(corrections)


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
    """在同规则、同来源和合法自然窗口下关联相邻前态。"""
    contexts = _read_rule_day_contexts(
        cursor, sessions=(before, target), package=package
    )
    current_context = contexts.get(target)
    current_rule = (current_context or {}).get("rule") or {}
    if (
        not current_context
        or not current_context.get("inputs_complete")
        or not current_context.get("rules_complete")
        or current_context.get("capture") != capture
        or current_rule.get("version") != rule_version
        or current_rule.get("profile_revision") != profile_revision
        or current_rule.get("history_count") != history_count
        or current_rule.get("d1_history_count") != d1_history_count
        or current_rule.get("event_window_start") != event_window_start
        or current_rule.get("calendar_revision") != calendar_revision
    ):
        raise EnrollmentConflict("目标规则分区与自身Raw、Profile、Seed或D1范围不闭合")
    previous_context = contexts.get(before)
    previous_is_comparable = _rule_context_comparable(
        previous_context, current_context, before=before, target=target
    )
    previous_capture_id = (
        previous_context["capture"] if previous_is_comparable else None
    )
    previous_profile_revision = (
        previous_context["profile"]["revision"] if previous_is_comparable else None
    )
    previous_history_count = (
        previous_context["profile"]["history_count"] if previous_is_comparable else None
    )
    previous_event_window_start = (
        previous_context["rule"]["event_window_start"]
        if previous_is_comparable
        else None
    )
    current_event_window_start = current_context["rule"]["event_window_start"]
    cursor.execute(
        """SELECT r.id, count(DISTINCT p.id), min(p.id), min(p.state),
                  count(DISTINCT d.id), min(d.id), min(d.before_state)
           FROM source_rule_evaluations r
           LEFT JOIN source_rule_evaluations p
             ON p.target_session=%s AND p.source_package_id=r.source_package_id
            AND p.source_symbol=r.source_symbol AND p.rule_id=r.rule_id
            AND p.source_capture_id=%s AND %s::boolean
            AND p.rule_version=r.rule_version AND p.profile_revision=%s
            AND p.history_session_count=%s
            AND p.d1_history_session_count=r.d1_history_session_count
            AND p.event_window_start=%s AND r.event_window_start=%s
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
            previous_capture_id,
            previous_is_comparable,
            previous_profile_revision,
            previous_history_count,
            previous_event_window_start,
            current_event_window_start,
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
    rule_rows = cursor.fetchall()
    rows = []
    for (
        fact_id,
        previous_count,
        previous_id,
        previous_state,
        d0_count,
        d0_id,
        d0_before_state,
    ) in rule_rows:
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
        reason = decision.reason
        if (
            previous_context
            and previous_context.get("rules_present")
            and not previous_is_comparable
            and previous_count == 0
            and d0_count != 1
        ):
            reason = "PREVIOUS_RULE_CONTEXT_NOT_COMPARABLE"
        rows.append(
            (
                None,
                fact_id,
                previous_id if previous_count == 1 else None,
                decision.event,
                reason,
                d0_id if d0_count == 1 else None,
            )
        )
    inserted, corrected = _insert_rule_enrollments(cursor, rows)
    return {
        "expected": len(rows),
        "inserted": inserted,
        "corrected": corrected,
    }


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
                          r.history_session_count,r.d1_history_session_count,
                          r.event_window_start,r.calendar_revision,r.reasons,
                          r.source_capture_id,
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
                history_count,
                d1_history_count,
                event_window_start,
                calendar_revision,
                rule_reasons,
                rule_capture_id,
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
                        "history_session_count": history_count,
                        "d1_history_session_count": d1_history_count,
                        "event_window_start": event_window_start.isoformat(),
                        "calendar_revision": calendar_revision,
                        "rule_reasons": list(rule_reasons or []),
                        "source_capture_id": rule_capture_id,
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
    d0_rows: list[tuple[Any, ...]] = []
    rule_rows: list[tuple[Any, ...]] = []
    invalid_anchor_scope = False
    anchors_by_id: dict[int, dict[str, Any]] = {}
    with _transaction(config, write=False, statement_timeout_ms=15000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT e.id,
                      CASE WHEN e.d0_fact_id IS NOT NULL THEN d.target_session
                           ELSE r.target_session END,
                      e.d0_fact_id IS NOT NULL,e.rule_fact_id IS NOT NULL,
                      COALESCE(d.source_package_id,r.source_package_id),
                      COALESCE(d.target_capture_id,r.source_capture_id),
                      r.id,r.source_symbol,r.rule_id,r.state,r.rule_version,
                      r.profile_revision,r.history_session_count,
                      r.d1_history_session_count,r.event_window_start,
                      r.calendar_revision,r.scope
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
        for row in anchors:
            anchors_by_id[row[0]] = {
                "package": row[4],
                "capture": row[5],
                "rule_fact_id": row[6],
                "source_symbol": row[7],
                "rule_id": row[8],
                "state": row[9],
                "rule_version": row[10],
                "profile_revision": row[11],
                "history_session_count": row[12],
                "d1_history_session_count": row[13],
                "event_window_start": row[14],
                "calendar_revision": row[15],
                "scope": row[16],
                "is_rule": row[3],
            }
        rule_contexts: dict[int, dict[date, dict[str, Any]]] = {}
        if not invalid_anchor_scope and selected:
            rule_packages = {
                anchor["package"]
                for anchor in anchors_by_id.values()
                if anchor["is_rule"]
            }
            for package in rule_packages:
                rule_contexts[package] = _read_rule_day_contexts(
                    cursor, sessions=(target, *selected), package=package
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
            d0_rows = list(cursor.fetchall())
            cursor.execute(
                """SELECT e.id,n.target_session,n.id,n.state,n.source_capture_id,
                          n.rule_id,n.rule_version,n.profile_revision,
                          n.history_session_count,n.d1_history_session_count,
                          n.event_window_start,n.calendar_revision,n.scope,n.reasons
               FROM source_research_enrollments e
               JOIN source_rule_evaluations b ON b.id=e.rule_fact_id
               JOIN source_rule_evaluations n ON n.source_symbol=b.source_symbol
                 AND n.source_package_id=b.source_package_id AND n.rule_id=b.rule_id
               WHERE e.id=ANY(%s) AND n.target_session=ANY(%s)""",
                (enrollment_ids, list(selected)),
            )
            rule_rows = list(cursor.fetchall())
    if invalid_anchor_scope:
        raise ValueError("研究登记父事实与请求目标会话不一致，拒绝跨日期附着")
    if not successor_sessions:
        return {
            item: {
                "status": "NOT_DUE",
                "reason": "WAITING_FOR_FUTURE_SESSION",
                "history": [],
                "history_truncated": False,
                "base_fact_id": anchors_by_id[item]["rule_fact_id"],
                "base_capture_id": anchors_by_id[item]["capture"],
                "rule_id": anchors_by_id[item]["rule_id"],
                "rule_version": anchors_by_id[item]["rule_version"],
                "profile_revision": anchors_by_id[item]["profile_revision"],
                "history_session_count": anchors_by_id[item]["history_session_count"],
                "d1_history_session_count": anchors_by_id[item][
                    "d1_history_session_count"
                ],
                "event_window_start": (
                    anchors_by_id[item]["event_window_start"].isoformat()
                    if anchors_by_id[item]["event_window_start"]
                    else None
                ),
                "calendar_revision": anchors_by_id[item]["calendar_revision"],
                "scope": anchors_by_id[item]["scope"],
            }
            for item in enrollment_ids
        }
    rule_pair_comparable: dict[tuple[int, date, date], bool] = {}
    for package, contexts in rule_contexts.items():
        sequence = (target, *selected)
        for previous_session, next_session in zip(sequence, sequence[1:]):
            rule_pair_comparable[(package, previous_session, next_session)] = (
                _rule_context_comparable(
                    contexts.get(previous_session),
                    contexts.get(next_session),
                    before=previous_session,
                    target=next_session,
                )
            )
    by_key: dict[tuple[int, date], list[tuple[Any, ...]]] = {}
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
    ) in d0_rows:
        by_key.setdefault((enrollment_id, session), []).append(
            (fact_id, state, before, before_state, before_capture, capture)
        )
        if base_capture != anchors_by_id[enrollment_id]["capture"]:
            raise ValueError("D0后继来源捕获与登记锚点不一致")

    rule_candidates: dict[tuple[int, date], list[dict[str, Any]]] = {}
    for row in rule_rows:
        (
            enrollment_id,
            session,
            fact_id,
            state,
            capture,
            rule_id,
            rule_version,
            profile_revision,
            history_count,
            d1_history_count,
            event_window_start,
            calendar_revision,
            scope,
            reasons,
        ) = row
        rule_candidates.setdefault((enrollment_id, session), []).append(
            {
                "fact_id": fact_id,
                "state": state,
                "capture": capture,
                "rule_id": rule_id,
                "rule_version": rule_version,
                "profile_revision": profile_revision,
                "history_session_count": history_count,
                "d1_history_session_count": d1_history_count,
                "event_window_start": event_window_start,
                "calendar_revision": calendar_revision,
                "scope": scope,
                "reasons": list(reasons or []),
            }
        )
    output = {}
    for enrollment_id in enrollment_ids:
        anchor = anchors_by_id[enrollment_id]
        anchor_context = rule_contexts.get(anchor["package"], {}).get(target)
        anchor_rule = (anchor_context or {}).get("rule") or {}
        rule_context_available = bool(
            anchor["is_rule"]
            and anchor["state"] == "TRUE"
            and anchor_context
            and anchor_context.get("inputs_complete")
            and anchor_context.get("rules_complete")
            and anchor["rule_id"] is not None
            and anchor["rule_version"] == anchor_rule.get("version")
            and anchor["profile_revision"] == anchor_rule.get("profile_revision")
            and anchor["history_session_count"] == anchor_rule.get("history_count")
            and anchor["d1_history_session_count"]
            == anchor_rule.get("d1_history_count")
            and anchor["event_window_start"] == anchor_rule.get("event_window_start")
            and anchor["calendar_revision"] == anchor_rule.get("calendar_revision")
            and anchor["scope"] == anchor_rule.get("scope")
        )
        lifecycle = "ACTIVE"
        previous = target
        previous_state = "TRUE"
        previous_capture = anchor["capture"]
        last_exit = None
        detail = None
        history = []
        for session in selected:
            candidate_detail: dict[str, Any] | None = None
            if anchor["is_rule"]:
                candidates = rule_candidates.get((enrollment_id, session), [])
                successor_context = rule_contexts.get(anchor["package"], {}).get(
                    session
                )
                successor_rule = (successor_context or {}).get("rule") or {}
                pair_comparable = rule_context_available and rule_pair_comparable.get(
                    (anchor["package"], previous, session), False
                )
                valid = [
                    item
                    for item in candidates
                    if pair_comparable
                    and item["rule_id"] == anchor["rule_id"]
                    and item["rule_version"] == anchor["rule_version"]
                    and item["rule_version"] == successor_rule.get("version")
                    and item["profile_revision"]
                    == successor_rule.get("profile_revision")
                    and item["history_session_count"]
                    == successor_rule.get("history_count")
                    and item["d1_history_session_count"]
                    == successor_rule.get("d1_history_count")
                    and item["event_window_start"]
                    == successor_rule.get("event_window_start")
                    and item["calendar_revision"]
                    == successor_rule.get("calendar_revision")
                    and item["scope"] == successor_rule.get("scope")
                    and item["capture"] == successor_context.get("capture")
                ]
                current = (
                    Observation(session, valid[0]["state"], valid[0]["fact_id"])
                    if len(candidates) == len(valid) == 1
                    and valid[0]["state"] in {"TRUE", "FALSE", "UNKNOWN"}
                    else None
                )
                candidate_detail = valid[0] if current else None
                invalid_reason = None
                if current is None:
                    if candidates and not valid:
                        invalid_reason = "RULE_CONTEXT_NOT_COMPARABLE"
                    elif candidates:
                        invalid_reason = "AMBIGUOUS_OR_NONADJACENT_FACT"
                    elif (
                        successor_context is not None
                        and successor_context.get("rules_present")
                        and not successor_context.get("rules_complete")
                    ):
                        invalid_reason = "RULE_CONTEXT_NOT_COMPARABLE"
            else:
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
                invalid_reason = (
                    "AMBIGUOUS_OR_NONADJACENT_FACT"
                    if current is None and candidates
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
            if invalid_reason:
                detail["reason"] = invalid_reason
            if candidate_detail:
                detail.update(
                    {
                        "state": candidate_detail["state"],
                        "source_capture_id": candidate_detail["capture"],
                        "rule_id": candidate_detail["rule_id"],
                        "rule_version": candidate_detail["rule_version"],
                        "profile_revision": candidate_detail["profile_revision"],
                        "history_session_count": candidate_detail[
                            "history_session_count"
                        ],
                        "d1_history_session_count": candidate_detail[
                            "d1_history_session_count"
                        ],
                        "event_window_start": candidate_detail[
                            "event_window_start"
                        ].isoformat(),
                        "calendar_revision": candidate_detail["calendar_revision"],
                        "scope": candidate_detail["scope"],
                        "price_coordinate": "RAW_UNADJUSTED",
                        "rule_reasons": candidate_detail["reasons"],
                    }
                )
            history.append(dict(detail))
            previous_state = current.state if current else None
            if anchor["is_rule"]:
                previous_capture = (
                    candidate_detail["capture"] if candidate_detail else None
                )
                rule_context_available = current is not None
            else:
                previous_capture = (
                    valid[0][5] if current and valid[0][2] is not None else None
                )
            previous = session
        assert detail is not None
        history_truncated = len(successor_sessions) > len(selected)
        detail = {
            **detail,
            "history": history,
            "history_truncated": history_truncated,
        }
        if history_truncated:
            detail = {
                **detail,
                "status": "INCOMPLETE",
                "reason": "FOLLOWUP_WINDOW_LIMIT",
                "last_evaluated_status": detail["status"],
            }
        output[enrollment_id] = detail
    return output
