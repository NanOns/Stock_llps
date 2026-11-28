"""从精确已存来源事实生成和读取来源级研究登记。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Any, Mapping, Sequence
from urllib.parse import unquote

from stock_a.domain.source_research_observation import (
    Observation,
    advance_observation,
    initial_observation,
)
from stock_a.storage.postgres.source_raw import _transaction

_SEED_PRODUCER_VERSION = "SOURCE_POOL_SEED_CORE_PROFILE_R2"
_D1_PRODUCER_VERSION = "STOCK_A_D1_BAOSTOCK_QFQ_R3"
_D1_SUPPORTED_PRODUCER_VERSIONS = (
    "STOCK_A_D1_BAOSTOCK_QFQ_R3",
    "STOCK_A_D1_BAOSTOCK_QFQ_R4",
    "STOCK_A_D1_BAOSTOCK_QFQ_R5",
)
_D0_LAUNCH_PRODUCER_VERSION = "STOCK_A_D0_LAUNCH_BAOSTOCK_READER_R5"
_POOL_RULE_IDS = (
    "LAUNCH_CONFIRM",
    "RECOVERY_TURN",
    "s1",
    "s2",
    "seed",
)


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
    cursor: Any,
    *,
    sessions: Sequence[date],
    source_capture_id: int | None = None,
    required_rule_ids: Sequence[str] | None = None,
) -> dict[date, dict[str, Any]]:
    """读取每个目标日各自闭合的Raw、Profile、Seed、D1和规则范围。"""
    from stock_a.storage.postgres.research_facts import RULES, RULE_VERSION, SCOPE

    required_rules = frozenset(required_rule_ids or _POOL_RULE_IDS)
    if not required_rules or not required_rules.issubset(RULES):
        raise ValueError("规则登记范围必须是现行冻结规则的非空子集")
    selected = tuple(sorted(set(sessions)))
    if not selected:
        return {}
    capture_filter = "AND c.capture_id=%s" if source_capture_id is not None else ""
    req_filter = "WHERE capture_id=%s" if source_capture_id is not None else ""
    req_params: tuple[Any, ...] = (
        (source_capture_id,) if source_capture_id is not None else ()
    )
    cap_params: tuple[Any, ...] = (
        (source_capture_id,) if source_capture_id is not None else ()
    )
    cursor.execute(
        f"""WITH request_codes AS (
               SELECT capture_id,source_code,
                      count(*)=2 AND count(DISTINCT adjustment_flag)=2
                        AND count(DISTINCT eligible_start_session)=1
                        AND count(DISTINCT eligible_end_session)=1
                        AND bool_and(
                            request_status IN ('COMPLETED','NO_DATA')
                            AND (request_status='NO_DATA'
                                 OR completed_through_session>=range_end_session)
                        ) AS complete,
                      min(eligible_start_session) AS eligible_start,
                      max(eligible_end_session) AS eligible_end
               FROM source_capture_requests
               {req_filter}
               GROUP BY capture_id,source_code
           ), selected_sessions AS (
               SELECT unnest(%s::date[]) AS target_session
           ), day_captures AS (
               SELECT DISTINCT r.session_date AS target_session,r.source_capture_id AS capture_id
               FROM source_raw_daily r
               JOIN selected_sessions s ON s.target_session=r.session_date
           )
           SELECT s.target_session,c.capture_id,c.parser_contract,
                  c.source_time_quality,c.source_clock_role,
                  count(r.id)::bigint AS raw_count,
                  count(DISTINCT r.source_symbol)::bigint AS raw_symbols,
                  count(r.id) FILTER (WHERE r.board IN ('MAIN_BOARD', 'GEM'))::bigint
                      AS research_raw_count,
                  count(q.source_code)::bigint AS expected_count,
                  count(q.source_code) FILTER (WHERE q.complete)::bigint
                      AS complete_requests
           FROM selected_sessions s
           JOIN day_captures dc ON dc.target_session=s.target_session
           JOIN source_captures c
             ON c.capture_id=dc.capture_id AND c.provider='BAOSTOCK'
            AND c.range_start_session<=s.target_session
            AND s.target_session<=c.range_end_session
            {capture_filter}
           LEFT JOIN request_codes q
             ON q.capture_id=c.capture_id
            AND s.target_session BETWEEN q.eligible_start AND q.eligible_end
           LEFT JOIN source_raw_daily r
             ON r.session_date=s.target_session
            AND r.source_capture_id=c.capture_id
            AND lower(r.source_symbol)=q.source_code
           GROUP BY s.target_session,c.capture_id,c.parser_contract,
                    c.source_time_quality,c.source_clock_role
           ORDER BY s.target_session,c.capture_id""",
        (*req_params, list(selected), *cap_params),
    )
    capture_rows = cursor.fetchall()
    p_cap_filter = "AND p.source_capture_id=%s" if source_capture_id is not None else ""
    cursor.execute(
        f"""SELECT p.target_session,p.source_capture_id,NULL::text AS profile_revision,
                  p.history_requested_session_count,
                  history_calendar_id,history_calendar_revision,history_window_start,
                  history_window_end,price_coordinate,
                  min(history_stored_session_count),max(history_stored_session_count),
                  count(*),count(DISTINCT p.source_symbol),
                  NULL::bigint AS cross_section_universe_capture_id,
                  NULL::text AS cross_section_universe_version,
                  NULL::text AS cross_section_contract_version
           FROM source_symbol_price_profiles p
           WHERE p.target_session=ANY(%s) {p_cap_filter}
             AND p.price_coordinate='FORWARD_ADJUSTED'
             AND EXISTS (
                 SELECT 1 FROM source_pool_seed_day_facts s
                 WHERE s.target_session=p.target_session
                   AND s.source_capture_id=p.source_capture_id
                   AND s.producer_version=%s
                   AND s.price_coordinate='FORWARD_ADJUSTED'
             )
           GROUP BY 1,2,3,4,5,6,7,8,9,14,15,16 ORDER BY 1,2,3""",
        (list(selected), *cap_params, _SEED_PRODUCER_VERSION),
    )
    profile_rows = cursor.fetchall()
    s_cap_filter = "AND source_capture_id=%s" if source_capture_id is not None else ""
    cursor.execute(
        f"""SELECT target_session,source_capture_id,NULL::text AS profile_revision,history_session_count,
                  history_window_start,history_window_end,calendar_revision,research_mode,
                  price_coordinate,producer_contract,producer_version,count(*),
                  count(DISTINCT source_symbol),NULL::bigint AS cross_section_universe_capture_id,
                  NULL::text AS cross_section_universe_version,NULL::text AS cross_section_contract_version,
                  NULL::text AS universe_match_status,ranking_universe_count,evaluation_universe_count
           FROM source_pool_seed_day_facts
           WHERE target_session=ANY(%s) {s_cap_filter}
             AND producer_version=%s
           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,14,15,16,17,18,19
           ORDER BY 1,2,3""",
        (list(selected), *cap_params, _SEED_PRODUCER_VERSION),
    )
    seed_rows = cursor.fetchall()
    cursor.execute(
        f"""SELECT target_session,target_capture_id,d1_history_session_count,
                  history_window_start,history_window_end,calendar_id,calendar_revision,
                  producer_contract,producer_version,material_change_contract,
                  research_mode,price_coordinate,count(*),
                  count(DISTINCT source_symbol),count(DISTINCT structure_type),
                  array_agg(DISTINCT structure_type ORDER BY structure_type),
                  NULL::bigint AS cross_section_universe_capture_id,
                  NULL::text AS cross_section_universe_version,
                  NULL::text AS cross_section_contract_version
           FROM source_d1_structure_day_facts
           WHERE target_session=ANY(%s) {s_cap_filter}
             AND producer_version=ANY(%s)
           GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,17,18,19 ORDER BY 1,2,3""",
        (list(selected), *cap_params, list(_D1_SUPPORTED_PRODUCER_VERSIONS)),
    )
    d1_rows = cursor.fetchall()
    cursor.execute(
        f"""SELECT target_session,source_capture_id,rule_version,profile_revision,
                  history_session_count,d1_history_session_count,event_window_start,
                  calendar_revision,scope,count(*),count(DISTINCT source_symbol),
                  count(DISTINCT rule_id),array_agg(DISTINCT rule_id ORDER BY rule_id)
           FROM source_rule_evaluations
           WHERE target_session=ANY(%s) {s_cap_filter}
             AND rule_id=ANY(%s)
           GROUP BY 1,2,3,4,5,6,7,8,9 ORDER BY 1,2,3,4""",
        (list(selected), *cap_params, list(sorted(required_rules))),
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
    from stock_a.storage.postgres.research_day_facts import (
        _D1_PRODUCER_VERSION_7,
        _expected_d1_structure_types,
        _select_preferred_d1_scopes,
    )

    for session in selected:
        captures = by_table["captures"][session]
        context: dict[str, Any] = {
            "session": session,
            "source_capture_id": source_capture_id,
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
            raw_count,
            raw_symbols,
            research_raw_count,
            expected_count,
            complete_requests,
        ) = captures[0]
        context.update(
            source_capture_id=capture,
            capture=capture,
            parser_contract=parser_contract,
            source_time_quality=source_time_quality,
            source_clock_role=source_clock_role,
            normalized_count=raw_count,
            source_count=research_raw_count,
        )
        profile_scopes = [
            row for row in by_table["profiles"][session] if row[0] == capture
        ]
        seed_scopes = [row for row in by_table["seeds"][session] if row[0] == capture]
        d1_scopes = [row for row in by_table["d1"][session] if row[0] == capture]
        d1_scopes = _select_preferred_d1_scopes(
            d1_scopes,
            version_index=7,
            comparable_indices=(0, 1, 2, 3, 4, 5, 6, 8, 9, 10),
        )
        if not (
            raw_count > 0
            and raw_count == raw_symbols == expected_count == complete_requests
        ):
            context["reason"] = "RAW_CAPTURE_REQUEST_PARTITION_NOT_CLOSED"
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
            history_calendar_id,
            history_calendar_revision,
            profile_start,
            profile_end,
            profile_coordinate,
            minimum_stored_count,
            maximum_stored_count,
            profile_count,
            profile_symbols,
            profile_cross_capture_id,
            profile_cross_version,
            profile_cross_contract,
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
            seed_cross_capture_id,
            seed_cross_version,
            seed_cross_contract,
            seed_match_status,
            seed_ranking_count,
            seed_evaluation_count,
        ) = seed
        (
            d1_capture,
            d1_history_count,
            d1_start,
            d1_end,
            d1_calendar_id,
            d1_calendar_revision,
            d1_contract,
            d1_version,
            d1_material_change_contract,
            d1_research_mode,
            d1_coordinate,
            d1_count,
            d1_symbols,
            structure_count,
            stored_structure_types,
            d1_cross_capture_id,
            d1_cross_version,
            d1_cross_contract,
        ) = d1
        expected_d1_types = _expected_d1_structure_types(d1_version)
        expected_d1_type_count = len(expected_d1_types)
        revision_context = (None, history_calendar_id, history_calendar_revision)
        inputs_complete = (
            profile_capture == seed_capture == d1_capture == capture
            and profile_count == profile_symbols == raw_count
            and seed_count == seed_symbols == research_raw_count
            and d1_count == research_raw_count * expected_d1_type_count
            and d1_symbols == research_raw_count
            and structure_count == expected_d1_type_count
            and frozenset(stored_structure_types or ()) == frozenset(expected_d1_types)
            and requested_count == history_count
            and 0 <= minimum_stored_count <= maximum_stored_count <= requested_count
            and profile_start == seed_start
            and profile_end == seed_end == session
            and profile_end == session
            and history_calendar_id == d1_calendar_id
            and history_calendar_revision == d1_calendar_revision
            and bool(seed_calendar_revision)
            and profile_coordinate
            == seed_coordinate
            == d1_coordinate
            == "FORWARD_ADJUSTED"
            and research_mode == d1_research_mode == "NON_PIT_RETROSPECTIVE"
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
                "cross_section_universe_capture_id": profile_cross_capture_id,
                "cross_section_universe_version": profile_cross_version,
                "cross_section_contract_version": profile_cross_contract,
            },
            seed={
                "contract": seed_contract,
                "version": seed_version,
                "calendar_revision": seed_calendar_revision,
                "research_mode": research_mode,
                "price_coordinate": seed_coordinate,
                "cross_section_universe_capture_id": seed_cross_capture_id,
                "cross_section_universe_version": seed_cross_version,
                "cross_section_contract_version": seed_cross_contract,
                "universe_match_status": seed_match_status,
                "ranking_universe_count": seed_ranking_count,
                "evaluation_universe_count": seed_evaluation_count,
            },
            d1={
                "history_count": d1_history_count,
                "window_start": d1_start,
                "window_end": d1_end,
                "calendar_id": d1_calendar_id,
                "calendar_revision": d1_calendar_revision,
                "contract": d1_contract,
                "material_change_contract": d1_material_change_contract,
                "version": d1_version,
                "research_mode": d1_research_mode,
                "price_coordinate": d1_coordinate,
                "cross_section_universe_capture_id": d1_cross_capture_id,
                "cross_section_universe_version": d1_cross_version,
                "cross_section_contract_version": d1_cross_contract,
            },
        )
        rule_scopes = [row for row in by_table["rules"][session] if row[0] == capture]
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
            stored_rule_ids,
        ) = rule_scope
        rules_complete = (
            rule_capture == capture
            and rule_version == RULE_VERSION
            and rule_history_count == history_count
            and rule_d1_count == d1_history_count
            and event_window_start == d1_start
            and rule_calendar_revision == history_calendar_revision
            and scope == SCOPE
            and rule_count == research_raw_count * len(required_rules)
            and rule_symbols == research_raw_count
            and rule_id_count == len(required_rules)
            and frozenset(stored_rule_ids) == required_rules
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
                "rule_ids": tuple(sorted(stored_rule_ids)),
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
            before_context["source_time_quality"]
            == target_context["source_time_quality"],
            before_context["source_clock_role"] == target_context["source_clock_role"],
            before_context["parser_contract"] == target_context["parser_contract"],
            before_profile["revision_context"] == target_profile["revision_context"],
            before_profile["cross_section_universe_capture_id"]
            == target_profile["cross_section_universe_capture_id"],
            before_profile["cross_section_universe_version"]
            == target_profile["cross_section_universe_version"],
            before_profile["cross_section_contract_version"]
            == target_profile["cross_section_contract_version"],
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
            before_context["seed"]["cross_section_universe_capture_id"]
            == target_context["seed"]["cross_section_universe_capture_id"],
            before_context["seed"]["cross_section_universe_version"]
            == target_context["seed"]["cross_section_universe_version"],
            before_context["seed"]["cross_section_contract_version"]
            == target_context["seed"]["cross_section_contract_version"],
            before_context["seed"]["universe_match_status"]
            == target_context["seed"]["universe_match_status"],
            before_context["d1"]["history_count"]
            == target_context["d1"]["history_count"],
            before_context["d1"]["calendar_id"] == target_context["d1"]["calendar_id"],
            before_context["d1"]["contract"] == target_context["d1"]["contract"],
            before_context["d1"]["material_change_contract"]
            == target_context["d1"]["material_change_contract"],
            before_context["d1"]["version"] == target_context["d1"]["version"],
            before_context["d1"]["cross_section_universe_capture_id"]
            == target_context["d1"]["cross_section_universe_capture_id"],
            before_context["d1"]["cross_section_universe_version"]
            == target_context["d1"]["cross_section_universe_version"],
            before_context["d1"]["cross_section_contract_version"]
            == target_context["d1"]["cross_section_contract_version"],
            before_context["d1"]["window_end"] == before,
            target_context["d1"]["window_end"] == target,
            before_context["d1"]["research_mode"]
            == target_context["d1"]["research_mode"],
            before_context["d1"]["price_coordinate"]
            == target_context["d1"]["price_coordinate"],
            before_rule["version"] == target_rule["version"],
            before_rule["capture"] == before_context["capture"],
            target_rule["capture"] == target_context["capture"],
            before_rule["d1_history_count"] == target_rule["d1_history_count"],
            before_rule["calendar_revision"] == target_rule["calendar_revision"],
            before_rule["scope"] == target_rule["scope"],
            before_rule["rule_ids"] == target_rule["rule_ids"],
            before_rule["event_window_start"] == before_context["d1"]["window_start"],
            target_rule["event_window_start"] == target_context["d1"]["window_start"],
        )
    )


def _contextual_rule_observation(
    candidates: Sequence[tuple[Any, ...]],
    context: Mapping[str, Any] | None,
    *,
    rule_id: str,
    rule_version: str,
) -> Observation | None:
    """仅将唯一、完整且与当日冻结范围一致的规则行转为观察。"""
    rule = (context or {}).get("rule") or {}
    if (
        not context
        or not context.get("inputs_complete")
        or not context.get("rules_complete")
        or rule.get("version") != rule_version
    ):
        return None
    valid = [
        row
        for row in candidates
        if row[3] == rule_id
        and row[4] == rule_version
        and row[5] in {"TRUE", "FALSE", "UNKNOWN"}
        and row[6] == rule.get("profile_revision")
        and row[7] == rule.get("history_count")
        and row[8] == rule.get("d1_history_count")
        and row[9] == rule.get("event_window_start")
        and row[10] == rule.get("calendar_revision")
        and row[11] == rule.get("scope")
        and row[12] == context.get("capture")
    ]
    if len(candidates) != 1 or len(valid) != 1:
        return None
    return Observation(valid[0][0], valid[0][5], valid[0][1])


def _reentry_parent_ids(
    cursor: Any,
    *,
    target: date,
    before: date,
    source_capture_id: int,
    rule_version: str,
    candidate_rows: Sequence[tuple[int, str, str]],
) -> dict[int, int]:
    """只把有可比较历史且已推进至EXITED的最近来源观察段作为父段。"""
    if not candidate_rows:
        return {}
    symbols = sorted({row[1] for row in candidate_rows})
    rule_ids = sorted({row[2] for row in candidate_rows})
    candidate_pairs = {(row[1], row[2]) for row in candidate_rows}
    from stock_a.research.entry_context.sessions import _load_calendar

    official_sessions = tuple(
        session for session in _load_calendar(target)[3] if session <= target
    )
    cursor.execute(
        """SELECT min(target_session) FROM source_rule_evaluations
           WHERE target_session<=%s""",
        (target,),
    )
    first_rule_session = cursor.fetchone()[0]
    if first_rule_session is None:
        return {}
    sessions = tuple(
        session for session in official_sessions if session >= first_rule_session
    )[-60:]
    if not sessions or before not in sessions or target not in sessions:
        return {}
    contexts = _read_rule_day_contexts(cursor, sessions=sessions)
    cursor.execute(
        """SELECT target_session,id,source_symbol,rule_id,rule_version,state,
                  profile_revision,history_session_count,d1_history_session_count,
                  event_window_start,calendar_revision,scope,source_capture_id
           FROM source_rule_evaluations
           WHERE target_session=ANY(%s)
             AND source_symbol=ANY(%s) AND rule_id=ANY(%s) AND rule_version=%s
           ORDER BY source_symbol,rule_id,target_session,id""",
        (list(sessions), symbols, rule_ids, rule_version),
    )
    facts_by_pair: dict[tuple[str, str], dict[date, list[tuple[Any, ...]]]] = {}
    for row in cursor.fetchall():
        pair = (row[2], row[3])
        if pair in candidate_pairs:
            facts_by_pair.setdefault(pair, {}).setdefault(row[0], []).append(row)
    cursor.execute(
        """SELECT e.id,r.id,r.target_session,r.source_symbol,r.rule_id
           FROM source_research_enrollments e
           JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
           WHERE r.target_session=ANY(%s)
             AND r.target_session<%s AND r.source_symbol=ANY(%s)
             AND r.rule_id=ANY(%s) AND r.rule_version=%s AND r.state='TRUE'
             AND e.enrollment_kind IN
                 ('ENTRY','FIRST_OBSERVED_LEFT_CENSORED','REENTRY')
           ORDER BY r.source_symbol,r.rule_id,r.target_session DESC,e.id DESC""",
        (list(sessions), before, symbols, rule_ids, rule_version),
    )
    roots_by_pair: dict[tuple[str, str], list[tuple[int, int, date]]] = {}
    for enrollment_id, fact_id, session, symbol, rule_id in cursor.fetchall():
        pair = (symbol, rule_id)
        if pair in candidate_pairs:
            roots_by_pair.setdefault(pair, []).append((enrollment_id, fact_id, session))

    parents: dict[int, int] = {}
    for current_fact_id, symbol, rule_id in candidate_rows:
        pair = (symbol, rule_id)
        roots = roots_by_pair.get(pair, [])
        if not roots:
            continue
        latest_session = roots[0][2]
        latest_roots = [row for row in roots if row[2] == latest_session]
        if len(latest_roots) != 1:
            continue
        parent_id, root_fact_id, root_session = latest_roots[0]
        root_context = contexts.get(root_session)
        root_observation = _contextual_rule_observation(
            facts_by_pair.get(pair, {}).get(root_session, []),
            root_context,
            rule_id=rule_id,
            rule_version=rule_version,
        )
        if (
            root_observation is None
            or root_observation.evidence_id != root_fact_id
            or root_observation.state != "TRUE"
        ):
            continue
        lifecycle = "ACTIVE"
        previous_session = root_session
        previous_context_available = True
        parent_proved_exited = False
        for session in sessions:
            if not root_session < session <= before:
                continue
            current_context = contexts.get(session)
            comparable = previous_context_available and _rule_context_comparable(
                contexts.get(previous_session),
                current_context,
                before=previous_session,
                target=session,
            )
            current = (
                _contextual_rule_observation(
                    facts_by_pair.get(pair, {}).get(session, []),
                    current_context,
                    rule_id=rule_id,
                    rule_version=rule_version,
                )
                if comparable
                else None
            )
            decision = advance_observation(
                lifecycle=lifecycle,
                previous_session=previous_session,
                expected_session=session,
                current=current,
                research_cutoff=before,
            )
            lifecycle = decision.lifecycle
            if decision.event == "EXIT_PROVED":
                parent_proved_exited = True
                break
            previous_context_available = current is not None
            previous_session = session
        if parent_proved_exited and lifecycle == "EXITED":
            parents[current_fact_id] = parent_id
    return parents


def _insert_rule_enrollments(
    cursor: Any, rows: list[tuple[Any, ...]]
) -> tuple[int, int]:
    """幂等插入并仅纠正缺前态时形成的精确规则登记。"""
    if not rows:
        return 0, 0
    rule_ids = [row[1] for row in rows]
    cursor.execute(
        """SELECT d0_fact_id,rule_fact_id,previous_rule_fact_id,enrollment_kind,
                  reason_code,adjacent_d0_fact_id,parent_enrollment_id
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
        if not same_parent:
            raise EnrollmentConflict("既存规则登记父事实键冲突，事务已回滚")
        corrections.append(row)
    for row in corrections:
        cursor.execute(
            """UPDATE source_research_enrollments
               SET previous_rule_fact_id=%s,enrollment_kind=%s,reason_code=%s,
                   parent_enrollment_id=%s
               WHERE rule_fact_id=%s AND d0_fact_id IS NULL""",
            (row[2], row[3], row[4], row[6], row[1]),
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
            adjacent_d0_fact_id, parent_enrollment_id)
           VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
        rows,
    )
    inserted = cursor.rowcount
    d0_ids = [row[0] for row in rows if row[0] is not None]
    rule_ids = [row[1] for row in rows if row[1] is not None]
    cursor.execute(
        """SELECT d0_fact_id, rule_fact_id, previous_rule_fact_id, enrollment_kind, reason_code,
                  adjacent_d0_fact_id, parent_enrollment_id
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
    source_capture_id: int,
    target_capture_id: int,
    before: date,
    before_capture_id: int,
    producer_version: str,
    algorithm_contract: str,
    session_contract: str,
    calendar_revision: str,
) -> dict[str, int]:
    """在D0生产事务内引用该目标分区的已证TRUE事实。"""
    cursor.execute(
        """SELECT id, before_session, before_state, after_state
           FROM source_d0_launch_day_facts
           WHERE target_session=%s AND source_capture_id=%s AND target_capture_id=%s
             AND before_capture_id=%s AND producer_version=%s AND algorithm_contract=%s
             AND session_contract=%s AND calendar_revision=%s
             AND after_state='TRUE' ORDER BY id""",
        (
            target,
            source_capture_id,
            target_capture_id,
            before_capture_id,
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
        rows.append((fact_id, None, None, decision.event, decision.reason, None, None))
    return {"expected": len(rows), "inserted": _insert_enrollments(cursor, rows)}


def insert_rule_enrollments(
    cursor: Any,
    *,
    target: date,
    before: date,
    source_capture_id: int,
    rule_version: str,
    profile_revision: str,
    history_count: int,
    d1_history_count: int,
    event_window_start: date,
    calendar_revision: str,
    rule_ids: Sequence[str] | None = None,
) -> dict[str, int]:
    """在同规则、同来源和合法自然窗口下关联相邻前态。"""
    from stock_a.storage.postgres.research_facts import RULES

    selected_rules = tuple(sorted(set(rule_ids or _POOL_RULE_IDS)))
    if not selected_rules or not set(selected_rules).issubset(RULES):
        raise ValueError("规则登记范围必须是现行冻结规则的非空子集")
    contexts = _read_rule_day_contexts(
        cursor,
        sessions=(before, target),
        required_rule_ids=selected_rules,
    )
    current_context = contexts.get(target)
    current_rule = (current_context or {}).get("rule") or {}
    if (
        not current_context
        or not current_context.get("inputs_complete")
        or not current_context.get("rules_complete")
        or current_context.get("capture") != source_capture_id
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
        """SELECT r.id,r.source_symbol,r.rule_id,
                  count(DISTINCT p.id), min(p.id), min(p.state),
                  count(DISTINCT d.id), min(d.id), min(d.before_state)
           FROM source_rule_evaluations r
           LEFT JOIN source_rule_evaluations p
             ON p.target_session=%s
            AND p.source_symbol=r.source_symbol AND p.rule_id=r.rule_id
            AND p.source_capture_id=%s AND %s::boolean
            AND p.rule_version=r.rule_version
            AND (%s::text IS NULL OR p.profile_revision=%s)
            AND p.history_session_count=%s
            AND p.d1_history_session_count=r.d1_history_session_count
            AND p.event_window_start=%s AND r.event_window_start=%s
            AND p.calendar_revision=r.calendar_revision AND p.scope=r.scope
           LEFT JOIN source_d0_launch_day_facts d
             ON r.rule_id='LAUNCH_CONFIRM' AND d.target_session=r.target_session
            AND d.before_session=%s AND d.source_capture_id=r.source_capture_id
            AND d.target_capture_id=r.source_capture_id AND d.source_symbol=r.source_symbol
            AND d.producer_version=%s
            AND d.calendar_revision=r.calendar_revision AND d.after_state=r.state
            AND d.price_coordinate='FORWARD_ADJUSTED'
           WHERE r.target_session=%s AND r.source_capture_id=%s
             AND r.rule_version=%s AND r.profile_revision=%s AND r.history_session_count=%s
             AND r.d1_history_session_count=%s AND r.event_window_start=%s
             AND r.calendar_revision=%s AND r.state='TRUE'
             AND r.rule_id=ANY(%s)
           GROUP BY r.id,r.source_symbol,r.rule_id ORDER BY r.id""",
        (
            before,
            previous_capture_id,
            previous_is_comparable,
            previous_profile_revision,
            previous_profile_revision,
            previous_history_count,
            previous_event_window_start,
            current_event_window_start,
            before,
            _D0_LAUNCH_PRODUCER_VERSION,
            target,
            source_capture_id,
            rule_version,
            profile_revision,
            history_count,
            d1_history_count,
            event_window_start,
            calendar_revision,
            list(selected_rules),
        ),
    )
    rule_rows = cursor.fetchall()
    rows = []
    possible_reentries = []
    for (
        fact_id,
        source_symbol,
        rule_id,
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
        if (
            decision.event == "ENTRY"
            and previous_count == 1
            and previous_state == "FALSE"
            and previous_is_comparable
        ):
            possible_reentries.append((fact_id, source_symbol, rule_id))
        rows.append(
            (
                None,
                fact_id,
                previous_id if previous_count == 1 else None,
                decision.event,
                reason,
                d0_id if d0_count == 1 else None,
                None,
            )
        )
    parents = _reentry_parent_ids(
        cursor,
        target=target,
        before=before,
        source_capture_id=source_capture_id,
        rule_version=rule_version,
        candidate_rows=possible_reentries,
    )
    if parents:
        rows = [
            (
                row[0],
                row[1],
                row[2],
                "REENTRY" if row[1] in parents else row[3],
                "MATCH_AFTER_PROVED_EXIT" if row[1] in parents else row[4],
                row[5],
                parents.get(row[1]),
            )
            for row in rows
        ]
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
    research_cutoff: date,
    source_capture_id: int,
    d1_history_count: int,
) -> dict[str, Any]:
    """在一个单日事务中引用既存完整D0和规则事实，不读取价格历史。"""
    if (
        type(target) is not date
        or type(before) is not date
        or type(research_cutoff) is not date
        or target > research_cutoff
        or before >= target
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(d1_history_count) is not int
        or d1_history_count <= 0
    ):
        raise ValueError("研究登记目标范围无效")
    with _transaction(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """SELECT count(*),count(DISTINCT source_symbol),
                      count(DISTINCT source_capture_id)
               FROM source_raw_daily
               WHERE session_date=%s AND source_capture_id=%s
                 AND price_quality IS NOT NULL AND qfq_price_quality IS NOT NULL""",
            (target, source_capture_id),
        )
        raw_count, raw_symbols, captures = cursor.fetchone()
        if not raw_count or raw_count != raw_symbols or captures != 1:
            raise ValueError("目标日Raw来源池不唯一或不完整")
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
            or not capture[1] <= target <= capture[2]
            or capture[2] > research_cutoff
            or set(capture[3] or ()) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
            or capture[4] != "NON_PIT_OBSERVED"
            or capture[5] != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
            or capture[6] is not None
        ):
            raise ValueError("目标来源捕获的BaoStock双坐标或NON_PIT身份不合法")
        cursor.execute(
            """WITH request_codes AS (
                   SELECT source_code,
                          count(*)=2 AND count(DISTINCT adjustment_flag)=2
                            AND count(DISTINCT eligible_start_session)=1
                            AND count(DISTINCT eligible_end_session)=1
                            AND bool_and(request_status IN ('COMPLETED','NO_DATA')
                              AND (request_status='NO_DATA' OR
                                   completed_through_session>=range_end_session)) AS complete,
                          min(eligible_start_session) AS eligible_start,
                          max(eligible_end_session) AS eligible_end
                   FROM source_capture_requests WHERE capture_id=%s GROUP BY source_code
               )
               SELECT count(*) FILTER (
                          WHERE %s BETWEEN eligible_start AND eligible_end
                      )::bigint,
                      count(*) FILTER (
                          WHERE %s BETWEEN eligible_start AND eligible_end AND complete
                      )::bigint
               FROM request_codes""",
            (source_capture_id, target, target),
        )
        expected_rows, complete_requests = cursor.fetchone()
        if int(expected_rows) != raw_count or int(complete_requests) != raw_count:
            raise ValueError("目标日BaoStock代码有效分母或RAW/QFQ请求未闭合")
        cursor.execute(
            """SELECT before_session,before_capture_id,producer_version,algorithm_contract,
                      session_contract,calendar_revision,count(*),count(DISTINCT source_symbol),
                      min(target_source_count),max(target_source_count),
                      min(union_source_count),max(union_source_count)
               FROM source_d0_launch_day_facts
               WHERE target_session=%s AND source_capture_id=%s AND target_capture_id=%s
               GROUP BY 1,2,3,4,5,6""",
            (target, source_capture_id, source_capture_id),
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
                 ON d.target_session=raw.session_date AND d.source_capture_id=raw.source_capture_id
                AND d.target_capture_id=raw.source_capture_id AND d.source_symbol=raw.source_symbol
                AND d.before_session=%s AND d.before_capture_id=%s AND d.producer_version=%s
                AND d.algorithm_contract=%s AND d.session_contract=%s AND d.calendar_revision=%s
               WHERE raw.session_date=%s AND raw.source_capture_id=%s
                 AND d.id IS NULL""",
            (
                before,
                before_capture,
                producer,
                algorithm,
                session_contract,
                d0_calendar,
                target,
                source_capture_id,
            ),
        )
        if cursor.fetchone()[0]:
            raise ValueError("D0来源代码集合未覆盖目标Raw分区")
        cursor.execute(
            """SELECT rule_version,profile_revision,history_session_count,
                      d1_history_session_count,event_window_start,calendar_revision,
                      count(*),count(DISTINCT source_symbol),count(DISTINCT rule_id)
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_capture_id=%s
                 AND d1_history_session_count=%s AND rule_id=ANY(%s)
               GROUP BY 1,2,3,4,5,6""",
            (target, source_capture_id, d1_history_count, list(_POOL_RULE_IDS)),
        )
        scopes = cursor.fetchall()
        if len(scopes) != 1 or tuple(scopes[0][-3:]) != (
            raw_count * len(_POOL_RULE_IDS),
            raw_count,
            len(_POOL_RULE_IDS),
        ):
            raise ValueError("核心五规则事实不是唯一完整的全池范围")
        version, profile, history_count, d1_count, window_start, calendar_revision = (
            scopes[0][:6]
        )
        cursor.execute(
            """SELECT count(*),count(DISTINCT source_symbol),
                      count(DISTINCT producer_contract),count(DISTINCT producer_version),
                      count(DISTINCT calendar_id),count(DISTINCT calendar_revision),
                      count(DISTINCT research_mode),count(DISTINCT price_coordinate),
                      min(producer_version),min(calendar_id),min(calendar_revision),
                      min(research_mode),min(price_coordinate)
               FROM source_pool_seed_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND history_session_count=%s""",
            (target, source_capture_id, history_count),
        )
        seed_scope = cursor.fetchone()
        history_calendar_id = seed_scope[9]
        if not (
            seed_scope[0] == raw_count
            and seed_scope[1] == raw_count
            and seed_scope[2:8] == (1, 1, 1, 1, 1, 1)
            and seed_scope[8:]
            == (
                _SEED_PRODUCER_VERSION,
                history_calendar_id,
                calendar_revision,
                "NON_PIT_RETROSPECTIVE",
                "FORWARD_ADJUSTED",
            )
        ):
            raise ValueError("Seed与规则的来源、画像或完整分母不一致")
        from stock_a.storage.postgres.research_day_facts import (
            _expected_d1_structure_types,
            _select_preferred_d1_scopes,
        )

        cursor.execute(
            """SELECT d1_history_session_count,history_window_start,
                      history_window_end,calendar_id,calendar_revision,
                      producer_contract,producer_version,material_change_contract,research_mode,
                      price_coordinate,count(*),count(DISTINCT source_symbol),
                      count(DISTINCT structure_type),
                      array_agg(DISTINCT structure_type ORDER BY structure_type)
               FROM source_d1_structure_day_facts
               WHERE target_session=%s AND source_capture_id=%s AND target_capture_id=%s
                 AND d1_history_session_count=%s AND history_window_start=%s
                 AND history_window_end=%s AND calendar_id=%s
                 AND calendar_revision=%s
                 AND research_mode='NON_PIT_RETROSPECTIVE'
                 AND price_coordinate='FORWARD_ADJUSTED'
                 AND producer_version=ANY(%s)
               GROUP BY d1_history_session_count,history_window_start,
                        history_window_end,calendar_id,calendar_revision,producer_contract,
                        producer_version,material_change_contract,research_mode,
                        price_coordinate""",
            (
                target,
                source_capture_id,
                source_capture_id,
                d1_count,
                window_start,
                target,
                history_calendar_id,
                calendar_revision,
                list(_D1_SUPPORTED_PRODUCER_VERSIONS),
            ),
        )
        d1_version_rows = _select_preferred_d1_scopes(
            cursor.fetchall(),
            version_index=6,
            comparable_indices=(0, 1, 2, 3, 4, 5, 7, 8, 9),
        )
        if len(d1_version_rows) != 1:
            raise ValueError("D1结构目标窗口或生产范围不唯一")
        d1_scope = d1_version_rows[0]
        expected_d1_types = _expected_d1_structure_types(str(d1_scope[6]))
        if not (
            d1_scope[10] == raw_count * len(expected_d1_types)
            and d1_scope[11] == raw_count
            and d1_scope[12] == len(expected_d1_types)
            and frozenset(d1_scope[13] or ()) == frozenset(expected_d1_types)
        ):
            raise ValueError("D1结构与规则的日历或完整分母不一致")
        if d0_calendar != calendar_revision:
            raise ValueError("D0与规则日历修订不一致")
        d0 = insert_d0_enrollments(
            cursor,
            target=target,
            source_capture_id=source_capture_id,
            target_capture_id=source_capture_id,
            before=before,
            before_capture_id=before_capture,
            producer_version=producer,
            algorithm_contract=algorithm,
            session_contract=session_contract,
            calendar_revision=d0_calendar,
        )
        rules = insert_rule_enrollments(
            cursor,
            target=target,
            before=before,
            source_capture_id=source_capture_id,
            rule_version=version,
            profile_revision=profile,
            history_count=history_count,
            d1_history_count=d1_count,
            event_window_start=window_start,
            calendar_revision=calendar_revision,
            rule_ids=_POOL_RULE_IDS,
        )
    return {
        "status": "PERSISTED",
        "target_session": target.isoformat(),
        "source_capture_id": source_capture_id,
        "source_count": raw_count,
        "d0": d0,
        "rules": rules,
    }


def read_enrollments_for_symbols(
    config: Mapping[str, Any],
    *,
    target: date,
    source_capture_id: int,
    target_capture_id: int,
    before: date,
    before_capture: int,
    include_d0: bool,
    symbols: list[str],
    rule_scope: Mapping[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    """仅按当前页来源键读取，所有证据详情仍由原typed事实提供。"""
    if (
        type(target) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(target_capture_id) is not int
        or target_capture_id <= 0
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
                      e.parent_enrollment_id,
                      'D0' AS fact_type,d.id,d.algorithm_contract,d.producer_version,
                      d.source_time_quality,d.source_clock_role,d.source_available_at,
                      d.research_mode,d.price_coordinate
               FROM source_d0_launch_day_facts d
               JOIN source_research_enrollments e ON e.d0_fact_id=d.id
               WHERE d.target_session=%s AND d.source_capture_id=%s
                 AND d.target_capture_id=%s AND d.before_session=%s
                 AND d.before_capture_id=%s AND d.source_symbol=ANY(%s)
               ORDER BY d.source_symbol,e.id""",
                (
                    target,
                    source_capture_id,
                    target_capture_id,
                    before,
                    before_capture,
                    symbols,
                ),
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
                parent_enrollment_id,
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
                    "parent_enrollment_id": parent_enrollment_id,
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
                """SELECT r.source_symbol,e.id,e.enrollment_kind,e.reason_code,
                          e.parent_enrollment_id,r.id,
                          r.rule_id,r.rule_version,r.profile_revision,
                          r.history_session_count,r.d1_history_session_count,
                          r.event_window_start,r.calendar_revision,r.reasons,
                          r.source_capture_id,
                          c.source_time_quality,c.source_clock_role,c.source_available_at,r.scope,
                          e.adjacent_d0_fact_id
                   FROM source_rule_evaluations r
                   JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                   JOIN source_captures c ON c.capture_id=r.source_capture_id
                   WHERE r.target_session=%s AND r.source_capture_id=%s
                     AND r.source_symbol=ANY(%s)
                     AND r.rule_version=%s AND r.profile_revision=%s
                     AND r.history_session_count=%s AND r.d1_history_session_count=%s
                     AND r.event_window_start=%s AND r.calendar_revision=%s
                   ORDER BY r.source_symbol,e.id""",
                (
                    target,
                    source_capture_id,
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
                parent_enrollment_id,
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
                        "parent_enrollment_id": parent_enrollment_id,
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
                        "price_coordinate": "FORWARD_ADJUSTED",
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
    horizon_due_sessions: tuple[date, ...] = (),
) -> dict[int, dict[str, Any]]:
    """有界读取登记的逐会话状态和精确T+1/T+3/T+5来源路径。"""
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
        or len(horizon_due_sessions) > 20
        or any(type(day) is not date or day <= target for day in horizon_due_sessions)
        or tuple(sorted(set(horizon_due_sessions))) != horizon_due_sessions
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
                      COALESCE(d.source_capture_id,r.source_capture_id),
                      COALESCE(d.target_capture_id,r.source_capture_id),
                      r.id,COALESCE(d.source_symbol,r.source_symbol),r.rule_id,r.state,r.rule_version,
                      r.profile_revision,r.history_session_count,
                      r.d1_history_session_count,r.event_window_start,
                      COALESCE(d.calendar_revision,r.calendar_revision),r.scope,
                      e.parent_enrollment_id,e.enrollment_kind
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
                "target_session": row[1],
                "source_capture_id": row[4],
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
                "parent_enrollment_id": row[17],
                "enrollment_kind": row[18],
            }
        shared_rule_contexts: dict[date, dict[str, Any]] = {}
        if not invalid_anchor_scope and selected:
            if any(anchor["is_rule"] for anchor in anchors_by_id.values()):
                shared_rule_contexts = _read_rule_day_contexts(
                    cursor,
                    sessions=(target, *selected),
                )
        if not invalid_anchor_scope and successor_sessions:
            cursor.execute(
                """SELECT e.id,n.target_session,n.id,n.after_state,n.before_session,
                      n.before_state,n.before_capture_id,n.target_capture_id,b.target_capture_id
               FROM source_research_enrollments e
               JOIN source_d0_launch_day_facts b ON b.id=e.d0_fact_id
               JOIN source_d0_launch_day_facts n ON n.source_symbol=b.source_symbol
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
                          n.event_window_start,n.calendar_revision,n.scope,n.reasons,
                          next_e.id,next_e.parent_enrollment_id,next_e.enrollment_kind
               FROM source_research_enrollments e
               JOIN source_rule_evaluations b ON b.id=e.rule_fact_id
               JOIN source_rule_evaluations n ON n.source_symbol=b.source_symbol
                 AND n.rule_id=b.rule_id
               LEFT JOIN source_research_enrollments next_e ON next_e.rule_fact_id=n.id
               WHERE e.id=ANY(%s) AND n.target_session=ANY(%s)""",
                (enrollment_ids, list(selected)),
            )
            rule_rows = list(cursor.fetchall())

        forward_due_by_horizon = {
            horizon: (
                horizon_due_sessions[horizon - 1]
                if len(horizon_due_sessions) >= horizon
                else None
            )
            for horizon in (1, 3, 5)
        }
        forward_rows_by_key: dict[tuple[int, str, int], list[tuple[Any, ...]]] = {}
        from stock_a.storage.postgres.research_facts import PATH_VERSION

        for source_capture_id in {
            anchor["source_capture_id"]
            for anchor in anchors_by_id.values()
            if anchor["source_capture_id"] is not None
        }:
            source_capture_id_anchors = [
                (enrollment_id, anchor)
                for enrollment_id, anchor in anchors_by_id.items()
                if anchor["source_capture_id"] == source_capture_id
                and anchor["capture"] is not None
                and anchor["source_symbol"] is not None
            ]
            if not source_capture_id_anchors:
                continue
            symbols = sorted(
                {anchor["source_symbol"] for _, anchor in source_capture_id_anchors}
            )
            due_horizons = [
                horizon
                for horizon, endpoint in forward_due_by_horizon.items()
                if endpoint is not None and endpoint <= cutoff
            ]
            if not due_horizons:
                continue
            cursor.execute(
                """SELECT target_session,source_symbol,source_capture_id,
                          horizon,path_rule_version,calendar_revision,
                          endpoint_session,maturity,endpoint_quality,endpoint_reason,
                          endpoint_numerator,endpoint_denominator,path_quality,
                          path_reasons,mfe_numerator,mfe_denominator,
                          mae_numerator,mae_denominator,mdd_numerator,mdd_denominator,
                          price_coordinate,scope,
                          mfe_quality,mfe_reason,mae_quality,mae_reason,
                          mdd_quality,mdd_reason
                   FROM source_forward_path_facts
                   WHERE target_session=%s
                     AND source_symbol=ANY(%s)
                     AND horizon=ANY(%s)
                   ORDER BY source_symbol,horizon,
                            path_rule_version,calendar_revision""",
                (target, symbols, due_horizons),
            )
            stored_paths = cursor.fetchall()
            for enrollment_id, anchor in source_capture_id_anchors:
                key = (
                    enrollment_id,
                    anchor["source_symbol"],
                    anchor["capture"],
                )
                matches = [
                    row for row in stored_paths if row[1] == anchor["source_symbol"]
                ]
                forward_rows_by_key[key] = matches
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
                "parent_enrollment_id": anchors_by_id[item]["parent_enrollment_id"],
                "enrollment_kind": anchors_by_id[item]["enrollment_kind"],
                "forward_paths": _enrollment_forward_paths(
                    anchors_by_id[item],
                    forward_rows_by_key.get(
                        (
                            item,
                            anchors_by_id[item]["source_symbol"],
                            anchors_by_id[item]["capture"],
                        ),
                        [],
                    ),
                    forward_due_by_horizon,
                    cutoff=cutoff,
                ),
            }
            for item in enrollment_ids
        }
    rule_pair_comparable: dict[tuple[date, date], bool] = {}
    if shared_rule_contexts:
        sequence = (target, *selected)
        for previous_session, next_session in zip(sequence, sequence[1:]):
            rule_pair_comparable[(previous_session, next_session)] = (
                _rule_context_comparable(
                    shared_rule_contexts.get(previous_session),
                    shared_rule_contexts.get(next_session),
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
            next_enrollment_id,
            parent_enrollment_id,
            enrollment_kind,
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
                "enrollment_id": next_enrollment_id,
                "parent_enrollment_id": parent_enrollment_id,
                "enrollment_kind": enrollment_kind,
            }
        )
    output = {}
    for enrollment_id in enrollment_ids:
        anchor = anchors_by_id[enrollment_id]
        anchor_context = shared_rule_contexts.get(target)
        anchor_rule = (anchor_context or {}).get("rule") or {}
        rule_context_available = bool(
            anchor["is_rule"]
            and anchor["state"] == "TRUE"
            and anchor_context
            and anchor_context.get("inputs_complete")
            and anchor_context.get("rules_complete")
            and anchor_context.get("capture") == anchor["source_capture_id"]
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
        proved_exit_detail = None
        linked_reentry = False
        history = []
        for session in selected:
            candidate_detail: dict[str, Any] | None = None
            if anchor["is_rule"]:
                candidates = rule_candidates.get((enrollment_id, session), [])
                successor_context = shared_rule_contexts.get(session)
                successor_rule = (successor_context or {}).get("rule") or {}
                pair_comparable = rule_context_available and rule_pair_comparable.get(
                    (previous, session), False
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
                        "price_coordinate": "FORWARD_ADJUSTED",
                        "rule_reasons": candidate_detail["reasons"],
                        "enrollment_id": candidate_detail["enrollment_id"],
                        "parent_enrollment_id": candidate_detail[
                            "parent_enrollment_id"
                        ],
                        "enrollment_kind": candidate_detail["enrollment_kind"],
                    }
                )
            history.append(dict(detail))
            if decision.event == "EXIT_PROVED" and anchor["is_rule"]:
                proved_exit_detail = dict(detail)
            if decision.event == "REENTRY" and anchor["is_rule"]:
                linked_reentry = bool(
                    candidate_detail
                    and candidate_detail["enrollment_kind"] == "REENTRY"
                    and candidate_detail["parent_enrollment_id"] == enrollment_id
                )
                if not linked_reentry:
                    history[-1]["status"] = "REENTRY_NOT_LINKED"
                    history[-1]["reason"] = "REENTRY_PARENT_NOT_PROVED"
                if proved_exit_detail is not None:
                    detail = dict(proved_exit_detail)
                break
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
        if proved_exit_detail is not None and anchor["is_rule"]:
            detail = dict(proved_exit_detail)
        history_truncated = not linked_reentry and len(successor_sessions) > len(
            selected
        )
        detail = {
            **detail,
            "history": history,
            "history_truncated": history_truncated,
            "parent_enrollment_id": anchor["parent_enrollment_id"],
            "enrollment_kind": anchor["enrollment_kind"],
            "forward_paths": _enrollment_forward_paths(
                anchor,
                forward_rows_by_key.get(
                    (enrollment_id, anchor["source_symbol"], anchor["capture"]),
                    [],
                ),
                forward_due_by_horizon,
                cutoff=cutoff,
            ),
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


def _enrollment_forward_paths(
    anchor: Mapping[str, Any],
    rows: Sequence[tuple[Any, ...]],
    due_by_horizon: Mapping[int, date | None],
    *,
    cutoff: date,
) -> list[dict[str, Any]]:
    """按登记T0的来源捕获、日历及路径版本投影已到期收益。"""
    from stock_a.research.entry_context.sessions import _load_calendar
    from stock_a.research.source_forward_path import (
        forward_calendar_revision_compatible,
    )
    from stock_a.storage.postgres.research_facts import PATH_VERSION

    _, _, official_revision_chain, _ = _load_calendar(cutoff)
    result = []
    for horizon in (1, 3, 5, 10, 20):
        due_session = due_by_horizon[horizon]
        item: dict[str, Any] = {
            "horizon": horizon,
            "due_session": due_session.isoformat() if due_session else None,
            "status": "UNKNOWN",
            "reason_codes": [],
            "path_rule_version": PATH_VERSION,
            "calendar_revision": anchor.get("calendar_revision"),
            "price_coordinate": "FORWARD_ADJUSTED",
            "scope": "NON_PIT_EXPLORATORY",
            "endpoint_quality": "UNKNOWN",
            "path_quality": "UNKNOWN",
            "path_metrics_status": "UNKNOWN",
        }
        if due_session is None:
            item["reason_codes"] = ["OFFICIAL_HORIZON_SESSION_UNAVAILABLE"]
        elif due_session > cutoff:
            item.update(
                status="NOT_DUE",
                path_metrics_status="NOT_DUE",
                reason_codes=["WAITING_FOR_FUTURE_OFFICIAL_SESSION"],
            )
        else:
            candidates = [row for row in rows if row[3] == horizon]
            exact = [
                row
                for row in candidates
                if row[4] == PATH_VERSION
                and row[6] == due_session
                and row[20] == "FORWARD_ADJUSTED"
                and row[21] == "NON_PIT_EXPLORATORY"
                and forward_calendar_revision_compatible(
                    anchor["target_session"],
                    due_session,
                    rule_revision=anchor.get("calendar_revision"),
                    path_revision=row[5],
                    official_revision_chain=official_revision_chain,
                )
            ]
            if len(exact) != 1:
                if exact or candidates:
                    item["reason_codes"] = [
                        "FORWARD_PATH_KEY_AMBIGUOUS"
                        if exact
                        else "FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH"
                    ]
                else:
                    item.update(
                        status="NOT_COMPUTED",
                        path_metrics_status="NOT_COMPUTED",
                        reason_codes=["FORWARD_PATH_FACT_NOT_SAVED"],
                    )
            else:
                row = exact[0]
                (
                    _target,
                    symbol,
                    capture_id,
                    _horizon,
                    path_version,
                    calendar_revision,
                    endpoint_session,
                    maturity,
                    endpoint_quality,
                    endpoint_reason,
                    numerator,
                    denominator,
                    path_quality,
                    path_reasons,
                    mfe_numerator,
                    mfe_denominator,
                    mae_numerator,
                    mae_denominator,
                    mdd_numerator,
                    mdd_denominator,
                    price_coordinate,
                    scope,
                ) = row[:22]
                if type(capture_id) is not int or not isinstance(symbol, str):
                    raise ValueError(
                        "Forward路径行列类型与source_symbol/source_capture_id合同不符"
                    )
                valid_key = (
                    symbol == anchor.get("source_symbol")
                    and endpoint_session == due_session
                    and price_coordinate == "FORWARD_ADJUSTED"
                    and scope == "NON_PIT_EXPLORATORY"
                )
                reason_codes = list(path_reasons or [])
                if endpoint_reason:
                    reason_codes.insert(0, endpoint_reason)
                if not valid_key:
                    item["reason_codes"] = ["FORWARD_PATH_SCOPE_MISMATCH"]
                else:
                    observed = (
                        maturity == "ENDPOINT_OBSERVED_NON_PIT"
                        and endpoint_quality == "OBSERVED"
                        and numerator is not None
                        and denominator is not None
                        and denominator > 0
                    )
                    metrics = {}
                    metric_pairs = {
                        "mfe": (mfe_numerator, mfe_denominator),
                        "mae": (mae_numerator, mae_denominator),
                        "mdd_close": (mdd_numerator, mdd_denominator),
                    }
                    mfe_qual = (
                        row[22]
                        if len(row) >= 28
                        else ("OBSERVED" if mfe_numerator is not None else "UNKNOWN")
                    )
                    mfe_rsn = row[23] if len(row) >= 28 else None
                    mae_qual = (
                        row[24]
                        if len(row) >= 28
                        else ("OBSERVED" if mae_numerator is not None else "UNKNOWN")
                    )
                    mae_rsn = row[25] if len(row) >= 28 else None
                    mdd_qual = (
                        row[26]
                        if len(row) >= 28
                        else ("OBSERVED" if mdd_numerator is not None else "UNKNOWN")
                    )
                    mdd_rsn = row[27] if len(row) >= 28 else None
                    metric_qual_map = {
                        "mfe": (mfe_qual, mfe_rsn),
                        "mae": (mae_qual, mae_rsn),
                        "mdd_close": (mdd_qual, mdd_rsn),
                    }
                    for metric, (
                        metric_numerator,
                        metric_denominator,
                    ) in metric_pairs.items():
                        m_qual, m_rsn = metric_qual_map[metric]
                        valid_metric = (
                            m_qual == "OBSERVED"
                            and metric_numerator is not None
                            and metric_denominator is not None
                            and metric_denominator > 0
                        )
                        metrics[f"{metric}_numerator"] = (
                            str(metric_numerator) if valid_metric else None
                        )
                        metrics[f"{metric}_denominator"] = (
                            str(metric_denominator) if valid_metric else None
                        )
                        metrics[metric] = (
                            str(
                                Fraction(int(metric_numerator), int(metric_denominator))
                            )
                            if valid_metric
                            else None
                        )
                        metrics[f"{metric}_quality"] = m_qual
                        metrics[f"{metric}_reason"] = m_rsn
                    metrics_observed = bool(metric_pairs) and all(
                        metrics.get(metric) is not None
                        for metric in ("mfe", "mae", "mdd_close")
                    )
                    item.update(
                        status="OBSERVED" if observed else "UNKNOWN",
                        maturity=maturity,
                        endpoint_quality=endpoint_quality,
                        endpoint_reason=endpoint_reason,
                        endpoint_numerator=(
                            str(numerator) if numerator is not None else None
                        ),
                        endpoint_denominator=(
                            str(denominator) if denominator is not None else None
                        ),
                        endpoint_return=(
                            str(Fraction(int(numerator), int(denominator)))
                            if observed
                            else None
                        ),
                        path_quality=path_quality,
                        reason_codes=list(dict.fromkeys(reason_codes)),
                        path_rule_version=path_version,
                        calendar_revision=calendar_revision,
                        price_coordinate=price_coordinate,
                        scope=scope,
                        path_metrics_status=(
                            "OBSERVED"
                            if metrics_observed and path_quality == "OBSERVED"
                            else "UNKNOWN"
                        ),
                        **metrics,
                    )
                    if not observed and not item["reason_codes"]:
                        item["reason_codes"] = [
                            "FORWARD_ENDPOINT_NOT_OBSERVED"
                            if maturity == "ENDPOINT_OBSERVED_NON_PIT"
                            else "FORWARD_PATH_MATURITY_MISMATCH"
                        ]
        result.append(item)
    return result
