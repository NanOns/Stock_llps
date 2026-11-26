"""Focus/Cohort读取来源登记类别与已保存后继结果。"""

from __future__ import annotations

from collections import Counter
from contextlib import nullcontext
from datetime import date
from fractions import Fraction
from statistics import median
from typing import Any

from stock_a.storage.postgres.research_facts import (
    PATH_VERSION,
    RULES,
    RULE_VERSION,
    SCOPE,
    ResearchFactConflict,
)
from stock_a.storage.postgres.source_raw import _transaction
from stock_a.research.source_forward_path import forward_calendar_revision_compatible

_KINDS = (
    "ENTRY",
    "REENTRY",
    "FIRST_OBSERVED_LEFT_CENSORED",
    "DAILY_LANDMARK",
)
_HORIZONS = (1, 3, 5)
_ACTIVE_KINDS = (
    "ENTRY",
    "FIRST_OBSERVED_LEFT_CENSORED",
    "DAILY_LANDMARK",
    "REENTRY",
)


def read_cohort_enrollment_datewise_comparison(
    config,
    *,
    dates: list[dict[str, Any]],
    research_cutoff: date,
    source_capture_id: int,
    rule: str,
    _cursor: Any | None = None,
) -> dict[str, Any]:
    """对已保存T0范围一次批量读取ENTRY/REENTRY及其Forward事实。"""
    from stock_a.research.entry_context.sessions import _load_calendar

    if (
        type(research_cutoff) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or rule not in RULES
        or not isinstance(dates, list)
        or len(dates) > 20
    ):
        raise ValueError("跨T0登记对照的截止日、规则、来源包或日期范围无效")

    _, _, calendar_revision, calendar = _load_calendar(research_cutoff)
    sessions = tuple(session for session in calendar if session <= research_cutoff)
    session_indexes = {session: index for index, session in enumerate(sessions)}
    contexts: list[dict[str, Any]] = []
    results_by_session: dict[date, dict[str, Any]] = {}
    for item in dates:
        try:
            target = date.fromisoformat(item["target_session"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("跨T0登记对照日期无效") from error
        result = {
            "target_session": target.isoformat(),
            "status": "SCOPE_NOT_COMPARABLE",
            "reason_codes": [],
            "source_capture_id": item.get("source_capture_id"),
            "profile_revision": item.get("profile_revision"),
            "history_session_count": item.get("history_session_count"),
            "d1_history_session_count": item.get("d1_history_session_count"),
            "event_window_start": item.get("event_window_start"),
            "calendar_revision": item.get("calendar_revision"),
            "source_pool_count": item.get("source_pool_count"),
            "enrollment_counts": {},
            "excluded_from_entry_reentry": {},
            "horizons": [],
        }
        results_by_session[target] = result
        if item.get("status") not in {"AVAILABLE", "NOT_DUE"}:
            result["reason_codes"] = item.get("reason_codes") or [
                "COHORT_T0_SCOPE_NOT_CLOSED"
            ]
            continue
        if target not in session_indexes:
            result["reason_codes"] = ["T0_NOT_IN_CUTOFF_OFFICIAL_CALENDAR"]
            continue
        if item.get("calendar_revision") != calendar_revision:
            result["reason_codes"] = ["OFFICIAL_CALENDAR_REVISION_MISMATCH"]
            continue
        try:
            context = {
                "target_session": target,
                "source_capture_id": int(item["source_capture_id"]),
                "profile_revision": str(item.get("profile_revision") or ""),
                "history_session_count": int(item["history_session_count"]),
                "d1_history_session_count": int(item["d1_history_session_count"]),
                "event_window_start": date.fromisoformat(item["event_window_start"]),
                "calendar_revision": str(item["calendar_revision"]),
                "source_pool_count": int(item["source_pool_count"]),
            }
        except (KeyError, TypeError, ValueError) as error:
            raise ResearchFactConflict("跨T0登记对照的精确来源上下文缺失") from error
        if (
            context["source_capture_id"] <= 0
            or context["history_session_count"] < 0
            or context["d1_history_session_count"] < 0
            or context["source_pool_count"] <= 0
        ):
            raise ResearchFactConflict("跨T0登记对照的精确来源上下文无效")
        context["result"] = result
        contexts.append(context)

    if contexts:
        targets = [item["target_session"] for item in contexts]
        captures = [item["source_capture_id"] for item in contexts]
        profiles = [item["profile_revision"] for item in contexts]
        histories = [item["history_session_count"] for item in contexts]
        d1_histories = [item["d1_history_session_count"] for item in contexts]
        windows = [item["event_window_start"] for item in contexts]
        calendars = [item["calendar_revision"] for item in contexts]
        pool_counts = [item["source_pool_count"] for item in contexts]
        transaction = (
            _transaction(config, write=False, statement_timeout_ms=15000)
            if _cursor is None
            else nullcontext((None, _cursor, None))
        )
        with transaction as (_, cursor, _):
            cursor.execute(
                """WITH selected AS (
                       SELECT * FROM unnest(
                           %s::date[],%s::bigint[],%s::text[],%s::integer[],
                           %s::integer[],%s::date[],%s::text[],%s::bigint[]
                       ) AS s(target_session,source_capture_id,profile_revision,
                              history_session_count,d1_history_session_count,
                              event_window_start,calendar_revision,source_pool_count)
                   )
                   SELECT s.target_session,s.source_capture_id,s.profile_revision,
                          s.history_session_count,s.d1_history_session_count,
                          s.event_window_start,s.calendar_revision,s.source_pool_count,
                          r.id,r.source_symbol,r.state,r.source_capture_id,
                          e.id,e.enrollment_kind,e.reason_code,e.parent_enrollment_id,
                          e.previous_rule_fact_id,parent_e.id,parent_e.enrollment_kind,
                          parent_r.target_session,parent_r.source_symbol,parent_r.rule_id,
                          parent_r.state,parent_r.rule_version,parent_r.source_capture_id,
                          previous_r.target_session,previous_r.source_symbol,
                          previous_r.rule_id,previous_r.state,previous_r.rule_version,
                          previous_r.source_capture_id,
                          f.horizon,f.endpoint_session,f.maturity,f.endpoint_quality,
                          f.endpoint_reason,f.endpoint_numerator::text,
                          f.endpoint_denominator::text,f.path_quality,f.path_reasons,
                          f.price_coordinate,f.scope,f.calendar_revision
                   FROM selected s
                   JOIN source_rule_evaluations r
                     ON r.target_session=s.target_session
                    AND r.source_capture_id=%s AND r.source_capture_id=s.source_capture_id
                    AND r.rule_id=%s AND r.rule_version=%s
                    AND r.history_session_count=s.history_session_count
                    AND r.d1_history_session_count=s.d1_history_session_count
                    AND r.event_window_start=s.event_window_start
                    AND r.calendar_revision=s.calendar_revision AND r.scope=%s
                   JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                   LEFT JOIN source_research_enrollments parent_e
                     ON parent_e.id=e.parent_enrollment_id
                   LEFT JOIN source_rule_evaluations parent_r
                     ON parent_r.id=parent_e.rule_fact_id
                   LEFT JOIN source_rule_evaluations previous_r
                     ON previous_r.id=e.previous_rule_fact_id
                   LEFT JOIN source_forward_path_facts f
                     ON f.target_session=r.target_session
                    AND f.source_capture_id=r.source_capture_id
                    AND f.source_symbol=r.source_symbol
                    AND f.horizon=ANY(%s::smallint[])
                    AND f.path_rule_version=%s
                    AND f.price_coordinate='FORWARD_ADJUSTED' AND f.scope=%s
                    AND f.endpoint_session<=%s
                   ORDER BY s.target_session,e.enrollment_kind,r.source_symbol,
                            e.id,f.horizon
                   LIMIT 20001""",
                (
                    targets,
                    captures,
                    profiles,
                    histories,
                    d1_histories,
                    windows,
                    calendars,
                    pool_counts,
                    source_capture_id,
                    rule,
                    RULE_VERSION,
                    SCOPE,
                    [1, 3, 5],
                    PATH_VERSION,
                    SCOPE,
                    research_cutoff,
                ),
            )
            rows = cursor.fetchall()
        if len(rows) > 20000:
            raise ResearchFactConflict("跨T0登记对照超出固定已存事实行数预算")

        enrollments_by_session: dict[date, dict[str, dict[int, dict[str, Any]]]] = {
            item["target_session"]: {kind: {} for kind in _KINDS} for item in contexts
        }
        symbols_by_session: dict[date, dict[str, set[str]]] = {
            item["target_session"]: {kind: set() for kind in _KINDS}
            for item in contexts
        }
        for row in rows:
            (
                target,
                source_capture_id,
                profile_revision,
                history_session_count,
                d1_history_session_count,
                event_window_start,
                selected_calendar_revision,
                source_pool_count,
                rule_fact_id,
                symbol,
                state,
                fact_capture_id,
                enrollment_id,
                kind,
                reason_code,
                parent_id,
                previous_rule_fact_id,
                parent_record_id,
                parent_kind,
                parent_session,
                parent_symbol,
                parent_rule,
                parent_state,
                parent_version,
                parent_package,
                previous_target,
                previous_symbol,
                previous_rule,
                previous_state,
                previous_version,
                previous_package,
                horizon,
                endpoint_session,
                maturity,
                endpoint_quality,
                endpoint_reason,
                numerator_text,
                denominator_text,
                path_quality,
                path_reasons,
                coordinate,
                scope,
                forward_calendar_revision,
            ) = row
            context = next(
                item for item in contexts if item["target_session"] == target
            )
            if (
                context["source_capture_id"] != source_capture_id
                or context["profile_revision"] != profile_revision
                or context["history_session_count"] != history_session_count
                or context["d1_history_session_count"] != d1_history_session_count
                or context["event_window_start"] != event_window_start
                or context["calendar_revision"] != selected_calendar_revision
                or context["source_pool_count"] != source_pool_count
            ):
                raise ResearchFactConflict("跨T0登记查询返回了不匹配的来源上下文")
            if (
                kind not in _KINDS
                or state != "TRUE"
                or fact_capture_id != source_capture_id
            ):
                raise ResearchFactConflict(
                    "跨T0登记类别含未知类别、非TRUE事实或捕获错配"
                )
            by_kind = enrollments_by_session[target][kind]
            entry = by_kind.get(int(enrollment_id))
            if entry is None:
                if symbol in symbols_by_session[target][kind]:
                    raise ResearchFactConflict("跨T0登记包含同类重复证券代码")
                entry = {
                    "id": int(enrollment_id),
                    "symbol": symbol,
                    "reason_code": reason_code,
                    "parent_id": parent_id,
                    "previous_rule_fact_id": previous_rule_fact_id,
                    "forward": {},
                }
                by_kind[int(enrollment_id)] = entry
                symbols_by_session[target][kind].add(symbol)
            elif (
                entry["symbol"] != symbol
                or entry["reason_code"] != reason_code
                or entry["parent_id"] != parent_id
                or entry["previous_rule_fact_id"] != previous_rule_fact_id
            ):
                raise ResearchFactConflict("跨T0登记与Forward分行上下文不一致")
            if kind == "REENTRY":
                proved = (
                    parent_id is not None
                    and parent_record_id == parent_id
                    and parent_kind
                    in {"ENTRY", "FIRST_OBSERVED_LEFT_CENSORED", "REENTRY"}
                    and parent_session is not None
                    and parent_session < target
                    and parent_symbol == symbol
                    and parent_rule == rule
                    and parent_state == "TRUE"
                    and parent_version == RULE_VERSION
                    and parent_package == source_capture_id
                    and reason_code == "MATCH_AFTER_PROVED_EXIT"
                    and previous_rule_fact_id is not None
                    and previous_target is not None
                    and previous_symbol == symbol
                    and previous_rule == rule
                    and previous_state == "FALSE"
                    and previous_version == RULE_VERSION
                    and previous_package == source_capture_id
                    and parent_session < previous_target < target
                )
                if not proved:
                    raise ResearchFactConflict(
                        "跨T0 REENTRY缺少同证券、同规则的父登记或已证退出前态"
                    )
            if horizon is not None:
                if horizon not in (1, 3, 5):
                    raise ResearchFactConflict("跨T0 Forward期限超出冻结范围")
                entry["forward"].setdefault(horizon, []).append(
                    {
                        "endpoint_session": endpoint_session,
                        "maturity": maturity,
                        "endpoint_quality": endpoint_quality,
                        "endpoint_reason": endpoint_reason,
                        "numerator": numerator_text,
                        "denominator": denominator_text,
                        "path_quality": path_quality,
                        "path_reasons": path_reasons,
                        "coordinate": coordinate,
                        "scope": scope,
                        "calendar_revision": forward_calendar_revision,
                    }
                )

        for context in contexts:
            target = context["target_session"]
            result = context["result"]
            by_kind = enrollments_by_session[target]
            result["enrollment_counts"] = {kind: len(by_kind[kind]) for kind in _KINDS}
            result["excluded_from_entry_reentry"] = {
                kind: len(by_kind[kind])
                for kind in ("FIRST_OBSERVED_LEFT_CENSORED", "DAILY_LANDMARK")
            }
            result["status"] = (
                "PAIRED_RISK_SET"
                if by_kind["ENTRY"] and by_kind["REENTRY"]
                else "NO_SAME_DAY_ENTRY_REENTRY_PAIR"
            )
            result["reason_codes"] = (
                []
                if result["status"] == "PAIRED_RISK_SET"
                else ["ENTRY_AND_REENTRY_NOT_BOTH_NONEMPTY"]
            )
            for horizon in (1, 3, 5):
                index = session_indexes[target]
                endpoint = (
                    sessions[index + horizon]
                    if index + horizon < len(sessions)
                    else None
                )
                groups = []
                group_by_kind: dict[str, dict[str, Any]] = {}
                for kind in ("ENTRY", "REENTRY"):
                    entries = list(by_kind[kind].values())
                    outcome_counts = Counter()
                    maturity_counts = Counter()
                    endpoint_quality_counts = Counter()
                    path_quality_counts = Counter()
                    reason_counts = Counter()
                    values: list[tuple[Fraction, str, int]] = []
                    for entry in entries:
                        if endpoint is None:
                            outcome_counts["NOT_DUE"] += 1
                            maturity_counts["NOT_DUE"] += 1
                            reason_counts["WAITING_FOR_FUTURE_OFFICIAL_SESSION"] += 1
                            continue
                        candidates = entry["forward"].get(horizon)
                        if candidates is None:
                            outcome_counts["NOT_COMPUTED"] += 1
                            maturity_counts["MISSING_FACT"] += 1
                            reason_counts["MISSING_FORWARD_FACT"] += 1
                            continue
                        exact = [
                            fact
                            for fact in candidates
                            if fact["endpoint_session"] == endpoint
                            and fact["coordinate"] == "FORWARD_ADJUSTED"
                            and fact["scope"] == "NON_PIT_EXPLORATORY"
                            and forward_calendar_revision_compatible(
                                target,
                                fact["endpoint_session"],
                                rule_revision=context["calendar_revision"],
                                path_revision=fact["calendar_revision"],
                                official_revision_chain=calendar_revision,
                            )
                        ]
                        if len(exact) > 1:
                            raise ResearchFactConflict(
                                "同一T0登记与期限命中多个兼容Forward日历版本"
                            )
                        if not exact:
                            outcome_counts["NOT_COMPUTED"] += 1
                            maturity_counts["CALENDAR_OR_COORDINATE_MISMATCH"] += 1
                            reason_counts[
                                "FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH"
                            ] += 1
                            continue
                        fact = exact[0]
                        if fact["endpoint_session"] != endpoint:
                            raise ResearchFactConflict(
                                "跨T0 Forward端点与截止内官方交易会话错位"
                            )
                        maturity_counts[fact["maturity"] or "UNKNOWN"] += 1
                        endpoint_quality_counts[
                            fact["endpoint_quality"] or "UNKNOWN"
                        ] += 1
                        path_quality_counts[fact["path_quality"] or "UNKNOWN"] += 1
                        numerator = fact["numerator"]
                        denominator = fact["denominator"]
                        if (
                            fact["maturity"] == "ENDPOINT_OBSERVED_NON_PIT"
                            and fact["endpoint_quality"] == "OBSERVED"
                            and numerator is not None
                            and denominator is not None
                            and int(denominator) > 0
                            and fact["coordinate"] == "FORWARD_ADJUSTED"
                            and fact["scope"] == SCOPE
                        ):
                            value = Fraction(int(numerator), int(denominator))
                            values.append((value, entry["symbol"], entry["id"]))
                            outcome_counts["OBSERVED"] += 1
                        else:
                            outcome_counts["UNKNOWN"] += 1
                            reason = (
                                fact["endpoint_reason"]
                                or fact["maturity"]
                                or fact["endpoint_quality"]
                                or "UNKNOWN"
                            )
                            reason_counts[str(reason)] += 1
                            for path_reason in fact["path_reasons"] or ():
                                reason_counts[str(path_reason)] += 1

                    values.sort(key=lambda item: (item[0], item[1], item[2]))
                    positive = [value for value in values if value[0] > 0]
                    non_positive = [value for value in values if value[0] <= 0]

                    def example(item):
                        if item is None:
                            return None
                        return {
                            "source_symbol": item[1],
                            "enrollment_id": item[2],
                            "exact_return": str(item[0]),
                        }

                    risk_denominator = len(entries)
                    outcomes = {
                        state: outcome_counts[state]
                        for state in ("OBSERVED", "UNKNOWN", "NOT_COMPUTED", "NOT_DUE")
                    }
                    if sum(outcomes.values()) != risk_denominator:
                        raise ResearchFactConflict("跨T0 Forward质量状态未闭合登记分母")
                    if endpoint is None and risk_denominator:
                        status = "NOT_DUE"
                    elif risk_denominator == 0:
                        status = "EMPTY_RISK_SET"
                    elif outcomes["NOT_COMPUTED"]:
                        status = "INCOMPLETE"
                    elif outcomes["UNKNOWN"]:
                        status = "AVAILABLE_WITH_UNKNOWN"
                    else:
                        status = "AVAILABLE"
                    group = {
                        "kind": kind,
                        "status": status,
                        "risk_denominator": risk_denominator,
                        "independent_security_count": len(
                            symbols_by_session[target][kind]
                        ),
                        "outcomes": outcomes,
                        "observed_count": outcomes["OBSERVED"],
                        "unknown_count": outcomes["UNKNOWN"],
                        "not_computed_count": outcomes["NOT_COMPUTED"],
                        "not_due_count": outcomes["NOT_DUE"],
                        "positive_count": len(positive),
                        "non_positive_count": len(non_positive),
                        "coverage_exact": (
                            str(Fraction(outcomes["OBSERVED"], risk_denominator))
                            if risk_denominator
                            else None
                        ),
                        "median_exact_return": (
                            str(median([value[0] for value in values]))
                            if values
                            else None
                        ),
                        "maturity_counts": dict(sorted(maturity_counts.items())),
                        "endpoint_quality_counts": dict(
                            sorted(endpoint_quality_counts.items())
                        ),
                        "path_quality_counts": dict(
                            sorted(path_quality_counts.items())
                        ),
                        "unevaluable_reasons": dict(sorted(reason_counts.items())),
                        "examples": {
                            "positive": example(positive[-1] if positive else None),
                            "non_positive": example(
                                non_positive[0] if non_positive else None
                            ),
                        },
                    }
                    if group["risk_denominator"] != group["independent_security_count"]:
                        raise ResearchFactConflict("跨T0登记数与去重证券数不一致")
                    groups.append(group)
                    group_by_kind[kind] = group

                entry = group_by_kind["ENTRY"]
                reentry = group_by_kind["REENTRY"]
                entry_median = entry["median_exact_return"]
                reentry_median = reentry["median_exact_return"]
                if entry_median is None or reentry_median is None:
                    difference = {
                        "status": "NO_COMPARABLE_OBSERVED_MEDIANS",
                        "definition": "ENTRY中位数−REENTRY中位数",
                        "median_difference_exact": None,
                        "direction": None,
                    }
                else:
                    median_difference = Fraction(entry_median) - Fraction(
                        reentry_median
                    )
                    difference = {
                        "status": "DESCRIPTIVE_OBSERVED_SUBSETS",
                        "definition": "ENTRY中位数−REENTRY中位数",
                        "median_difference_exact": str(median_difference),
                        "direction": (
                            "ENTRY_MEDIAN_HIGHER"
                            if median_difference > 0
                            else "REENTRY_MEDIAN_HIGHER"
                            if median_difference < 0
                            else "MEDIANS_EQUAL"
                        ),
                    }
                result["horizons"].append(
                    {
                        "horizon": horizon,
                        "endpoint_session": endpoint.isoformat() if endpoint else None,
                        "status": (
                            "NOT_DUE"
                            if endpoint is None
                            else "AVAILABLE"
                            if all(
                                group["status"] in {"AVAILABLE", "EMPTY_RISK_SET"}
                                for group in groups
                            )
                            else "AVAILABLE_WITH_QUALITY_GAPS"
                        ),
                        "groups": groups,
                        "comparison": difference,
                    }
                )

    paired_t0_count = sum(
        item["status"] == "PAIRED_RISK_SET" for item in results_by_session.values()
    )
    comparable_by_horizon = {
        str(horizon): sum(
            any(
                item["horizon"] == horizon
                and item["comparison"]["status"] == "DESCRIPTIVE_OBSERVED_SUBSETS"
                for item in result["horizons"]
            )
            for result in results_by_session.values()
        )
        for horizon in (1, 3, 5)
    }
    ordered = [
        results_by_session[key] for key in sorted(results_by_session, reverse=True)
    ]
    return {
        "status": "AVAILABLE" if contexts else "NO_COMPARABLE_STORED_SCOPE",
        "rule": rule,
        "rule_version": RULE_VERSION,
        "path_rule_version": PATH_VERSION,
        "scope": SCOPE,
        "price_coordinate": "FORWARD_ADJUSTED",
        "research_cutoff": research_cutoff.isoformat(),
        "paired_t0_date_count": paired_t0_count,
        "comparable_t0_date_count_by_horizon": comparable_by_horizon,
        "aggregation": "NO_CROSS_DATE_AGGREGATE; each T0 date remains independent",
        "dates": ordered,
    }


def read_cohort_enrollment_lifecycle_groups(
    config,
    *,
    target: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str | None = None,
    history_session_count: int = 0,
    d1_history_session_count: int = 0,
    event_window_start: date | None = None,
    calendar_revision: str = "",
    rule: str = "",
    horizon_sessions: tuple[date, ...] = (),
    previous_session: date | None = None,
) -> dict[str, Any]:
    """在一个精确T0×规则内分组，GET只读PG且只读截止内会话。"""
    from stock_a.research.entry_context.sessions import _load_calendar

    if (
        type(target) is not date
        or type(research_cutoff) is not date
        or target > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or rule not in RULES
        or (profile_revision is not None and not isinstance(profile_revision, str))
        or type(history_session_count) is not int
        or type(d1_history_session_count) is not int
        or type(event_window_start) is not date
        or not isinstance(calendar_revision, str)
        or not calendar_revision
        or len(horizon_sessions) > 5
        or tuple(sorted(set(horizon_sessions))) != horizon_sessions
        or any(
            type(session) is not date or session <= target or session > research_cutoff
            for session in horizon_sessions
        )
        or (
            previous_session is not None
            and (
                type(previous_session) is not date
                or previous_session >= target
                or previous_session > research_cutoff
            )
        )
    ):
        raise ValueError("来源登记分组的T0、版本或有界后继范围无效")
    _, _, official_revision_chain, official_sessions = _load_calendar(research_cutoff)
    try:
        target_index = official_sessions.index(target)
    except ValueError as error:
        raise ValueError("登记T0不在官方日历内") from error
    expected_horizon_sessions = tuple(
        official_sessions[target_index + 1 : target_index + 1 + len(horizon_sessions)]
    )
    if expected_horizon_sessions != horizon_sessions:
        raise ValueError("登记后继会话与目标日后的官方交易日历不一致")

    key = (
        target,
        source_capture_id,
        rule,
        RULE_VERSION,
        history_session_count,
        d1_history_session_count,
        event_window_start,
        calendar_revision,
        SCOPE,
    )
    registered: dict[str, list[dict[str, Any]]] = {kind: [] for kind in _KINDS}
    with _transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT count(*)::bigint,count(DISTINCT source_symbol)::bigint,
                      count(DISTINCT source_capture_id)::bigint,
                      min(source_capture_id),max(source_capture_id)
               FROM source_rule_evaluations
               WHERE target_session=%s AND source_capture_id=%s AND rule_id=%s
                 AND rule_version=%s
                 AND history_session_count=%s AND d1_history_session_count=%s
                 AND event_window_start=%s AND calendar_revision=%s AND scope=%s""",
            key,
        )
        pool_count, symbol_count, capture_count, capture_min, capture_max = (
            cursor.fetchone()
        )
        pool_count = int(pool_count)
        if pool_count == 0:
            return {
                "status": "NO_STORED_FACT",
                "target_session": target.isoformat(),
                "rule": rule,
                "source_pool_count": 0,
                "groups": [],
                "reason_codes": ["NO_EXACT_T0_RULE_FACT"],
                "database_transaction": "READ_ONLY",
                "database_write_attempted": False,
            }
        if (
            symbol_count != pool_count
            or capture_count != 1
            or capture_min != capture_max
        ):
            raise ResearchFactConflict("同一T0规则键不是唯一捕获或来源代码分母不闭合")
        source_capture_id = int(capture_min)

        cursor.execute(
            """SELECT r.state,count(*)::bigint,count(e.id)::bigint
               FROM source_rule_evaluations r
               LEFT JOIN source_research_enrollments e ON e.rule_fact_id=r.id
               WHERE r.target_session=%s AND r.source_capture_id=%s AND r.rule_id=%s
                 AND r.rule_version=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s AND r.scope=%s
               GROUP BY r.state ORDER BY r.state""",
            key,
        )
        state_rows = cursor.fetchall()
        state_counts = {state: int(count) for state, count, _ in state_rows}
        enrolled_by_state = {state: int(enrolled) for state, _, enrolled in state_rows}
        if sum(state_counts.values()) != pool_count:
            raise ResearchFactConflict("规则三态分母与精确来源池不一致")

        cursor.execute(
            """SELECT e.id,r.source_symbol,e.enrollment_kind,e.reason_code,
                      e.parent_enrollment_id,e.previous_rule_fact_id,r.state,
                      r.source_capture_id,parent_e.id,parent_e.enrollment_kind,
                      parent_r.target_session,parent_r.source_symbol,parent_r.rule_id,
                      parent_r.state,parent_r.rule_version,parent_r.source_capture_id,
                      previous_r.target_session,previous_r.source_symbol,
                      previous_r.rule_id,previous_r.state,previous_r.rule_version,
                      previous_r.source_capture_id
               FROM source_rule_evaluations r
               JOIN source_research_enrollments e ON e.rule_fact_id=r.id
               LEFT JOIN source_research_enrollments parent_e
                 ON parent_e.id=e.parent_enrollment_id
               LEFT JOIN source_rule_evaluations parent_r
                 ON parent_r.id=parent_e.rule_fact_id
               LEFT JOIN source_rule_evaluations previous_r
                 ON previous_r.id=e.previous_rule_fact_id
               WHERE r.target_session=%s AND r.source_capture_id=%s AND r.rule_id=%s
                 AND r.rule_version=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s AND r.scope=%s
               ORDER BY e.enrollment_kind,r.source_symbol,e.id""",
            key,
        )
        enrollment_rows = cursor.fetchall()
        enrollment_ids: set[int] = set()
        symbols_by_kind: dict[str, set[str]] = {kind: set() for kind in _KINDS}
        for row in enrollment_rows:
            (
                enrollment_id,
                symbol,
                kind,
                reason_code,
                parent_id,
                previous_rule_fact_id,
                state,
                capture_id,
                parent_record_id,
                parent_kind,
                parent_session,
                parent_symbol,
                parent_rule,
                parent_state,
                parent_version,
                parent_package,
                previous_target,
                previous_symbol,
                previous_rule,
                previous_state,
                previous_version,
                previous_package,
            ) = row
            if kind not in _KINDS or state != "TRUE" or capture_id != source_capture_id:
                raise ResearchFactConflict(
                    "规则登记含非TRUE事实、未登记类别或来源捕获错配"
                )
            if enrollment_id in enrollment_ids or symbol in symbols_by_kind[kind]:
                raise ResearchFactConflict("同一T0规则登记关系出现重复来源代码")
            enrollment_ids.add(enrollment_id)
            symbols_by_kind[kind].add(symbol)
            if kind == "REENTRY":
                proved = (
                    parent_id is not None
                    and parent_record_id == parent_id
                    and parent_kind
                    in {"ENTRY", "FIRST_OBSERVED_LEFT_CENSORED", "REENTRY"}
                    and parent_session is not None
                    and parent_session < target
                    and parent_symbol == symbol
                    and parent_rule == rule
                    and parent_state == "TRUE"
                    and parent_version == RULE_VERSION
                    and parent_package == source_capture_id
                    and reason_code == "MATCH_AFTER_PROVED_EXIT"
                    and previous_rule_fact_id is not None
                    and previous_session is not None
                    and previous_target == previous_session
                    and previous_symbol == symbol
                    and previous_rule == rule
                    and previous_state == "FALSE"
                    and previous_version == RULE_VERSION
                    and previous_package == source_capture_id
                    and parent_session < previous_session < target
                )
                if not proved:
                    raise ResearchFactConflict(
                        "REENTRY缺少同证券同规则的父登记或已证FALSE退出前态"
                    )
            registered[kind].append(
                {"id": int(enrollment_id), "symbol": symbol, "parent_id": parent_id}
            )

        enrollment_counts = {kind: len(registered[kind]) for kind in _KINDS}
        if sum(enrollment_counts.values()) != sum(enrolled_by_state.values()):
            raise ResearchFactConflict("登记类别分母与规则事实关联数不一致")
        if enrolled_by_state.get("FALSE", 0) or enrolled_by_state.get("UNKNOWN", 0):
            raise ResearchFactConflict("非TRUE规则事实被持久化为来源登记")

        cursor.execute(
            """SELECT count(DISTINCT r.id)::bigint
               FROM source_rule_evaluations r
               JOIN LATERAL (
                 SELECT h.id,h.state
                 FROM source_rule_evaluations h
                 WHERE h.source_capture_id=r.source_capture_id
                   AND h.source_symbol=r.source_symbol AND h.rule_id=r.rule_id
                   AND h.rule_version=r.rule_version AND h.scope=r.scope
                   AND h.target_session<r.target_session
                   AND h.state IN ('TRUE','FALSE')
                 ORDER BY h.target_session DESC,h.id DESC LIMIT 1
               ) latest ON TRUE
               JOIN source_research_enrollments active_e
                 ON active_e.rule_fact_id=latest.id
               WHERE r.target_session=%s AND r.source_capture_id=%s
                 AND r.rule_id=%s AND r.rule_version=%s
                 AND r.history_session_count=%s
                 AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s
                 AND r.scope=%s AND r.state='UNKNOWN'
                 AND latest.state='TRUE'
                 AND active_e.enrollment_kind=ANY(%s::text[])""",
            (*key, list(_ACTIVE_KINDS)),
        )
        paused_unknown_count = int(cursor.fetchone()[0])

        risk_set = {
            "source_pool_count": pool_count,
            "unique_source_symbol_count": int(symbol_count),
            "rule_state_counts": state_counts,
            "enrollment_counts": enrollment_counts,
            "unregistered_counts": {
                "FALSE_NO_ENROLLMENT": max(
                    0,
                    state_counts.get("FALSE", 0) - enrolled_by_state.get("FALSE", 0),
                ),
                "PAUSED_UNKNOWN": paused_unknown_count,
                "UNKNOWN_LIFECYCLE_UNPROVED": max(
                    0,
                    state_counts.get("UNKNOWN", 0)
                    - enrolled_by_state.get("UNKNOWN", 0)
                    - paused_unknown_count,
                ),
                "TRUE_WITHOUT_ENROLLMENT": max(
                    0,
                    state_counts.get("TRUE", 0) - enrolled_by_state.get("TRUE", 0),
                ),
            },
            "paused_unknown_basis": "LATEST_KNOWN_TRUE_WITH_ACTIVE_ENROLLMENT",
        }

        cursor.execute(
            """SELECT e.id,r.source_symbol,e.enrollment_kind,
                      f.horizon,f.endpoint_session,f.maturity,f.endpoint_quality,
                      f.endpoint_reason,f.endpoint_numerator::text,
                      f.endpoint_denominator::text,f.path_quality,f.path_reasons,
                      f.price_coordinate,f.scope,f.calendar_revision
               FROM source_rule_evaluations r
               JOIN source_research_enrollments e ON e.rule_fact_id=r.id
               LEFT JOIN source_forward_path_facts f
                 ON f.target_session=r.target_session
                AND f.source_capture_id=r.source_capture_id
                AND f.source_symbol=r.source_symbol
                AND f.horizon=ANY(%s::smallint[])
                AND f.path_rule_version=%s
                AND f.endpoint_session<=%s
                AND f.price_coordinate='FORWARD_ADJUSTED' AND f.scope=%s
               WHERE r.target_session=%s AND r.source_capture_id=%s AND r.rule_id=%s
                 AND r.rule_version=%s
                 AND r.history_session_count=%s AND r.d1_history_session_count=%s
                 AND r.event_window_start=%s AND r.calendar_revision=%s AND r.scope=%s
               ORDER BY e.enrollment_kind,r.source_symbol,f.horizon""",
            (list(_HORIZONS), PATH_VERSION, research_cutoff, SCOPE, *key),
        )
        forward_rows = cursor.fetchall()
        forward_candidates: dict[tuple[int, int], list[tuple[Any, ...]]] = {}
        for row in forward_rows:
            if row[3] is None:
                continue
            identity = (int(row[0]), int(row[3]))
            forward_candidates.setdefault(identity, []).append(row)

        forward_by_enrollment: dict[tuple[int, int], tuple[Any, ...]] = {}
        forward_calendar_mismatch: set[tuple[int, int]] = set()
        for identity, candidates in forward_candidates.items():
            horizon = identity[1]
            due_session = (
                horizon_sessions[horizon - 1]
                if len(horizon_sessions) >= horizon
                else None
            )
            compatible = [
                row
                for row in candidates
                if due_session is not None
                and row[4] == due_session
                and row[12] == "FORWARD_ADJUSTED"
                and row[13] == SCOPE
                and forward_calendar_revision_compatible(
                    target,
                    due_session,
                    rule_revision=calendar_revision,
                    path_revision=row[14],
                    official_revision_chain=official_revision_chain,
                )
            ]
            if len(compatible) > 1:
                raise ResearchFactConflict("同一登记与期限命中多个兼容Forward事实")
            if compatible:
                forward_by_enrollment[identity] = compatible[0]
            else:
                forward_calendar_mismatch.add(identity)

        group_results = []
        for kind in _KINDS:
            entries = registered[kind]
            forward_groups = []
            for horizon in _HORIZONS:
                endpoint_session = (
                    horizon_sessions[horizon - 1]
                    if len(horizon_sessions) >= horizon
                    else None
                )
                outcomes = Counter()
                maturity_counts = Counter()
                endpoint_quality_counts = Counter()
                path_quality_counts = Counter()
                unevaluable_reasons = Counter()
                values: list[tuple[Fraction, str, int]] = []
                for entry in entries:
                    if endpoint_session is None:
                        outcomes["NOT_DUE"] += 1
                        maturity_counts["NOT_DUE"] += 1
                        unevaluable_reasons["WAITING_FOR_FUTURE_OFFICIAL_SESSION"] += 1
                        continue
                    row = forward_by_enrollment.get((entry["id"], horizon))
                    if row is None:
                        outcomes["NOT_COMPUTED"] += 1
                        identity = (entry["id"], horizon)
                        if identity in forward_calendar_mismatch:
                            maturity_counts["CALENDAR_OR_COORDINATE_MISMATCH"] += 1
                            unevaluable_reasons[
                                "FORWARD_PATH_VERSION_OR_CALENDAR_MISMATCH"
                            ] += 1
                        else:
                            maturity_counts["MISSING_FACT"] += 1
                            unevaluable_reasons["MISSING_FORWARD_FACT"] += 1
                        continue
                    (
                        _,
                        symbol,
                        _,
                        _,
                        stored_endpoint,
                        maturity,
                        endpoint_quality,
                        endpoint_reason,
                        numerator_text,
                        denominator_text,
                        path_quality,
                        path_reasons,
                        coordinate,
                        scope,
                        _path_calendar_revision,
                    ) = row
                    if stored_endpoint != endpoint_session:
                        raise ResearchFactConflict("Forward端点与官方会话期限不一致")
                    maturity_counts[maturity or "UNKNOWN"] += 1
                    endpoint_quality_counts[endpoint_quality or "UNKNOWN"] += 1
                    path_quality_counts[path_quality or "UNKNOWN"] += 1
                    if (
                        maturity == "ENDPOINT_OBSERVED_NON_PIT"
                        and endpoint_quality == "OBSERVED"
                        and numerator_text is not None
                        and denominator_text is not None
                        and int(denominator_text) > 0
                        and coordinate == "FORWARD_ADJUSTED"
                        and scope == SCOPE
                    ):
                        values.append(
                            (
                                Fraction(int(numerator_text), int(denominator_text)),
                                symbol,
                                entry["id"],
                            )
                        )
                        outcomes["OBSERVED"] += 1
                    else:
                        outcomes["UNKNOWN"] += 1
                        reason = (
                            endpoint_reason or maturity or endpoint_quality or "UNKNOWN"
                        )
                        unevaluable_reasons[str(reason)] += 1
                        for path_reason in path_reasons or ():
                            unevaluable_reasons[str(path_reason)] += 1

                values.sort(key=lambda item: (item[0], item[1]))
                positive = [value for value in values if value[0] > 0]
                non_positive = [value for value in values if value[0] <= 0]

                def example(item):
                    if item is None:
                        return None
                    return {
                        "source_symbol": item[1],
                        "enrollment_id": item[2],
                        "exact_return": str(item[0]),
                    }

                if endpoint_session is None:
                    status = "NOT_DUE"
                elif outcomes["NOT_COMPUTED"]:
                    status = "INCOMPLETE"
                elif outcomes["UNKNOWN"]:
                    status = "AVAILABLE_WITH_UNKNOWN"
                else:
                    status = "AVAILABLE"
                forward_groups.append(
                    {
                        "horizon": horizon,
                        "endpoint_session": (
                            endpoint_session.isoformat()
                            if endpoint_session is not None
                            else None
                        ),
                        "status": status,
                        "risk_denominator": len(entries),
                        "observed_count": outcomes["OBSERVED"],
                        "unknown_count": outcomes["UNKNOWN"],
                        "not_computed_count": outcomes["NOT_COMPUTED"],
                        "not_due_count": outcomes["NOT_DUE"],
                        "positive_count": len(positive),
                        "non_positive_count": len(non_positive),
                        "median_exact_return": (
                            str(median([value[0] for value in values]))
                            if values
                            else None
                        ),
                        "maturity_counts": dict(sorted(maturity_counts.items())),
                        "endpoint_quality_counts": dict(
                            sorted(endpoint_quality_counts.items())
                        ),
                        "path_quality_counts": dict(
                            sorted(path_quality_counts.items())
                        ),
                        "unevaluable_reasons": dict(
                            sorted(unevaluable_reasons.items())
                        ),
                        "examples": {
                            "positive": example(positive[-1] if positive else None),
                            "non_positive": example(
                                non_positive[0] if non_positive else None
                            ),
                        },
                    }
                )

            group_result = {
                "kind": kind,
                "enrollment_count": len(entries),
                "independent_security_count": len(symbols_by_kind[kind]),
                "parent_proved_count": len(entries) if kind == "REENTRY" else 0,
                "forward": forward_groups,
            }
            if (
                group_result["enrollment_count"]
                != group_result["independent_security_count"]
            ):
                raise ResearchFactConflict("登记关系数与去重来源证券数不一致")
            for outcome in forward_groups:
                if (
                    sum(
                        outcome[field]
                        for field in (
                            "observed_count",
                            "unknown_count",
                            "not_computed_count",
                            "not_due_count",
                        )
                    )
                    != group_result["enrollment_count"]
                ):
                    raise ResearchFactConflict("Forward质量状态未闭合登记分母")
            group_results.append(group_result)

        line_scope = (
            "SOURCE_FROZEN_FAILURE_LINE"
            if rule in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}
            else "NOT_APPLICABLE"
        )
        line_counts = {kind: {h: Counter() for h in _HORIZONS} for kind in _KINDS}
        raw_session_versions = []
        if line_scope != "NOT_APPLICABLE":
            closed_sessions = list(horizon_sessions)
            raw_session_versions = [
                {
                    "session": session.isoformat(),
                    "status": "OFFICIAL_SESSION_CAPTURE_SCOPED",
                    "source_capture_id": source_capture_id,
                    "row_count": None,
                    "expected_count": None,
                }
                for session in horizon_sessions
            ]
            cursor.execute(
                """WITH horizons AS (
                     SELECT unnest(ARRAY[1,3,5]::smallint[]) AS horizon
                   ), line_candidates AS (
                     SELECT e.id AS enrollment_id,e.enrollment_kind,r.source_symbol,
                            r.frozen_line_numerator,r.frozen_line_denominator
                     FROM source_rule_evaluations r
                     JOIN source_research_enrollments e ON e.rule_fact_id=r.id
                     WHERE r.target_session=%s AND r.source_capture_id=%s
                       AND r.rule_id=%s AND r.rule_version=%s
                       AND r.history_session_count=%s
                       AND r.d1_history_session_count=%s
                       AND r.event_window_start=%s AND r.calendar_revision=%s
                       AND r.scope=%s AND r.state='TRUE'
                       AND r.frozen_line_numerator>0
                       AND r.frozen_line_denominator>0
                       AND r.frozen_line_source IS NOT NULL
                   ), points AS (
                     SELECT l.enrollment_id,l.enrollment_kind,h.horizon,
                            s.session_date,
                            CASE
                              WHEN NOT (s.session_date=ANY(%s::date[]))
                                THEN 'UNKNOWN'
                              WHEN p.source_symbol IS NULL OR c.capture_id IS NULL
                                OR p.qfq_bar_status IS DISTINCT FROM 'TARGET_PRESENT'
                                OR p.qfq_price_quality IS DISTINCT FROM 'OBSERVED'
                                OR p.qfq_tradestatus_quality IS DISTINCT FROM 'OBSERVED'
                                OR p.qfq_tradestatus_value IS DISTINCT FROM '1'
                                OR p.qfq_close IS NULL OR p.qfq_close<=0
                                THEN 'UNKNOWN'
                              WHEN p.qfq_close*l.frozen_line_denominator
                                   <=l.frozen_line_numerator
                                THEN 'TOUCHED'
                              ELSE 'ABOVE'
                            END AS point_state
                     FROM line_candidates l
                     CROSS JOIN horizons h
                     JOIN LATERAL (
                       SELECT session_date
                       FROM unnest(%s::date[]) WITH ORDINALITY AS s(session_date,ordinal)
                       WHERE s.ordinal<=h.horizon
                     ) s ON TRUE
                     LEFT JOIN source_raw_daily p
                       ON p.source_symbol=l.source_symbol
                      AND p.session_date=s.session_date AND p.source_capture_id=%s
                     LEFT JOIN source_captures c
                       ON c.capture_id=p.source_capture_id
                      AND c.source_capture_id=p.source_capture_id
                      AND c.target_session=p.session_date
                      AND c.provider='BAOSTOCK'
                   ), touch_points AS (
                     SELECT p.*,
                            min(session_date) FILTER (WHERE point_state='TOUCHED')
                              OVER (PARTITION BY enrollment_id,horizon) AS first_touch
                     FROM points p
                   ), return_points AS (
                     SELECT p.*,
                            min(session_date) FILTER (
                              WHERE point_state='ABOVE' AND first_touch IS NOT NULL
                                AND session_date>first_touch
                            ) OVER (PARTITION BY enrollment_id,horizon) AS first_return
                     FROM touch_points p
                   ), candidate_horizons AS (
                     SELECT l.enrollment_id,l.enrollment_kind,h.horizon,
                            min(p.first_touch) AS first_touch,
                            min(p.first_return) AS first_return,
                            COALESCE(bool_or(
                              p.point_state='UNKNOWN'
                              AND (p.first_touch IS NULL OR p.session_date<p.first_touch)
                            ),FALSE) AS unknown_before_touch,
                            COALESCE(bool_or(
                              p.point_state='UNKNOWN'
                              AND (p.first_return IS NULL OR p.session_date<p.first_return)
                            ),FALSE) AS unknown_before_return
                     FROM line_candidates l
                     CROSS JOIN horizons h
                     LEFT JOIN return_points p
                       ON p.enrollment_id=l.enrollment_id AND p.horizon=h.horizon
                     GROUP BY l.enrollment_id,l.enrollment_kind,h.horizon
                   ), statuses AS (
                     SELECT enrollment_kind,horizon,
                       CASE
                         WHEN first_touch IS NOT NULL AND NOT unknown_before_touch
                           THEN 'OBSERVED'
                         WHEN unknown_before_touch THEN 'UNKNOWN'
                         WHEN horizon<=%s THEN 'NOT_OBSERVED'
                         ELSE 'NOT_DUE'
                       END AS touch_status,
                       CASE
                         WHEN first_return IS NOT NULL AND NOT unknown_before_return
                           THEN 'OBSERVED'
                         WHEN unknown_before_return THEN 'UNKNOWN'
                         WHEN horizon<=%s THEN 'NOT_OBSERVED'
                         ELSE 'NOT_DUE'
                       END AS return_status
                     FROM candidate_horizons
                   )
                   SELECT enrollment_kind,horizon,touch_status,return_status,
                          count(*)::bigint
                   FROM statuses
                   GROUP BY enrollment_kind,horizon,touch_status,return_status
                   ORDER BY enrollment_kind,horizon,touch_status,return_status""",
                (
                    *key,
                    closed_sessions,
                    list(horizon_sessions),
                    source_capture_id,
                    len(horizon_sessions),
                    len(horizon_sessions),
                ),
            )
            for kind, horizon, touch_status, return_status, count in cursor.fetchall():
                line_counts[kind][int(horizon)][touch_status] += int(count)
                line_counts[kind][int(horizon)][f"RETURN_{return_status}"] += int(count)

        for group_result in group_results:
            kind = group_result["kind"]
            followup = []
            for horizon in _HORIZONS:
                counts = line_counts[kind][horizon]
                touch = {
                    status: counts[status]
                    for status in ("OBSERVED", "UNKNOWN", "NOT_OBSERVED", "NOT_DUE")
                }
                returned = {
                    status: counts[f"RETURN_{status}"]
                    for status in ("OBSERVED", "UNKNOWN", "NOT_OBSERVED", "NOT_DUE")
                }
                followup.append(
                    {
                        "horizon": horizon,
                        "candidate_line_count": sum(touch.values()),
                        "touch_status_counts": touch,
                        "return_status_counts": returned,
                    }
                )
            group_result["line_scope"] = line_scope
            group_result["candidate_line_followup"] = followup

        return {
            "status": "AVAILABLE",
            "scope": SCOPE,
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "price_coordinate": "FORWARD_ADJUSTED",
            "target_session": target.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "rule": rule,
            "rule_version": RULE_VERSION,
            "path_rule_version": PATH_VERSION,
            "source_capture_id": source_capture_id,
            "profile_revision": profile_revision,
            "history_session_count": history_session_count,
            "d1_history_session_count": d1_history_session_count,
            "event_window_start": event_window_start.isoformat(),
            "calendar_revision": calendar_revision,
            "risk_set": risk_set,
            "groups": group_results,
            "comparison_status": (
                "COMPARABLE_DESCRIPTIVE_SINGLE_T0"
                if enrollment_counts["ENTRY"] and enrollment_counts["REENTRY"]
                else "NO_VALID_COMPARABLE_SCOPE"
            ),
            "independent_t0_date_count": 1,
            "raw_session_versions": raw_session_versions,
            "database_transaction": "REPEATABLE_READ_READ_ONLY",
            "database_write_attempted": False,
        }
