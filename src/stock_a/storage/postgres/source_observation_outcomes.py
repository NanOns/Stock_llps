"""现存来源观察日与T0 Forward事实的有界只读期限结局。"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from fractions import Fraction
from math import ceil
from typing import Any, Mapping, Sequence

from stock_a.domain.source_observation_outcomes import (
    OUTCOME_STATUSES,
    classify_observation_horizon,
)
from stock_a.storage.postgres.research_enrollment import _enrollment_forward_paths


_OBSERVATION_COLUMNS = (
    "id",
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
    "scope",
    "price_coordinate",
    "identity_status",
    "producer_contract",
    "producer_version",
    "evidence_target_session",
    "evidence_source_package_id",
    "evidence_source_symbol",
    "evidence_rule_id",
    "evidence_rule_version",
    "evidence_capture_id",
    "evidence_state",
    "evidence_profile_revision",
    "evidence_history_session_count",
    "evidence_d1_history_session_count",
    "evidence_event_window_start",
    "evidence_calendar_revision",
    "evidence_scope",
)

_FORWARD_COLUMNS = (
    "target_session",
    "source_package_id",
    "source_capture_id",
    "source_symbol",
    "horizon",
    "path_rule_version",
    "calendar_revision",
    "endpoint_session",
    "maturity",
    "endpoint_quality",
    "endpoint_reason",
    "endpoint_numerator",
    "endpoint_denominator",
    "path_quality",
    "path_reasons",
    "price_coordinate",
    "scope",
    "mfe_numerator",
    "mfe_denominator",
    "mae_numerator",
    "mae_denominator",
    "mdd_numerator",
    "mdd_denominator",
)

_METRIC_COLUMNS = {
    "RAW_CLOSE_RETURN": "endpoint_return",
    "RAW_MFE": "mfe",
    "RAW_MAE": "mae",
    "RAW_MDD_CLOSE": "mdd_close",
}


def _fraction_summary(values: Sequence[str]) -> dict[str, Any]:
    fractions = sorted(Fraction(value) for value in values)
    if not fractions:
        return {
            "evaluated_count": 0,
            "independent_t0_date_count": 0,
            "minimum": None,
            "p25_nearest_rank": None,
            "p50_nearest_rank": None,
            "p75_nearest_rank": None,
            "maximum": None,
        }

    def nearest_rank(numerator: int, denominator: int) -> Fraction:
        position = max(1, ceil(len(fractions) * numerator / denominator)) - 1
        return fractions[position]

    return {
        "evaluated_count": len(fractions),
        "independent_t0_date_count": 1,
        "minimum": str(fractions[0]),
        "p25_nearest_rank": str(nearest_rank(1, 4)),
        "p50_nearest_rank": str(nearest_rank(1, 2)),
        "p75_nearest_rank": str(nearest_rank(3, 4)),
        "maximum": str(fractions[-1]),
    }


def read_source_observation_outcomes_cursor(
    cursor: Any,
    *,
    source_package_id: int,
    t0_session: date,
    research_cutoff: date,
    rule_id: str | None,
    horizon: int,
    official_sessions: Sequence[date],
) -> tuple[dict[str, Any], dict[int, dict[str, Any]]]:
    """在调用方现有只读事务中汇总单一T0及一个期限。"""
    if (
        type(source_package_id) is not int
        or source_package_id <= 0
        or type(t0_session) is not date
        or type(research_cutoff) is not date
        or t0_session > research_cutoff
        or horizon not in {1, 3, 5}
        or t0_session not in official_sessions
    ):
        raise ValueError("来源观察期限结局参数无效")

    t0_index = official_sessions.index(t0_session)
    due_session = (
        official_sessions[t0_index + horizon]
        if t0_index + horizon < len(official_sessions)
        else None
    )
    due_by_horizon = {
        item: (
            official_sessions[t0_index + item]
            if t0_index + item < len(official_sessions)
            else None
        )
        for item in (1, 3, 5)
    }
    expected_sessions = tuple(official_sessions[t0_index + 1 : t0_index + horizon + 1])

    rule_clause = " AND r.rule_id=%s" if rule_id else ""
    rule_params = (rule_id,) if rule_id else ()
    cursor.execute(
        f"""SELECT e.id,r.source_package_id,r.source_symbol,r.rule_id,
                      r.rule_version,r.target_session,r.id,r.source_capture_id,
                      e.enrollment_kind,e.reason_code,e.parent_enrollment_id,
                      r.profile_revision,r.history_session_count,
                      r.d1_history_session_count,r.event_window_start,
                      r.calendar_revision,r.scope,r.state
               FROM source_research_enrollments e
               JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
               WHERE r.source_package_id=%s AND r.target_session=%s
                 AND e.enrollment_kind IN
                     ('ENTRY','REENTRY','FIRST_OBSERVED_LEFT_CENSORED','DAILY_LANDMARK')
                 {rule_clause}
               ORDER BY r.rule_id,e.enrollment_kind,r.source_symbol,e.id""",
        (source_package_id, t0_session, *rule_params),
    )
    anchor_rows = cursor.fetchall()
    all_by_kind: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for row in anchor_rows:
        all_by_kind[row[8]].append(row)

    eligible = [row for row in anchor_rows if row[8] in {"ENTRY", "REENTRY"}]
    anchors: dict[int, dict[str, Any]] = {}
    for row in eligible:
        anchors[row[0]] = {
            "enrollment_id": row[0],
            "source_package_id": row[1],
            "source_symbol": row[2],
            "rule_id": row[3],
            "rule_version": row[4],
            "t0_session": row[5],
            "anchor_rule_fact_id": row[6],
            "anchor_capture_id": row[7],
            "enrollment_kind": row[8],
            "enrollment_reason": row[9],
            "parent_enrollment_id": row[10],
            "profile_revision": row[11],
            "history_session_count": row[12],
            "d1_history_session_count": row[13],
            "event_window_start": row[14],
            "calendar_revision": row[15],
            "scope": row[16],
            "t0_state": row[17],
            "price_coordinate": "RAW_UNADJUSTED",
            "identity_status": "SOURCE_SYMBOL_ONLY",
        }

    observations_by_id: dict[int, list[dict[str, Any]]] = {
        enrollment_id: [] for enrollment_id in anchors
    }
    observation_end = (
        min(due_session, research_cutoff)
        if due_session is not None
        else min(expected_sessions[-1], research_cutoff)
        if expected_sessions
        else None
    )
    if anchors and observation_end is not None and observation_end >= t0_session:
        cursor.execute(
            f"""SELECT {", ".join(f"o.{column}" for column in _OBSERVATION_COLUMNS[:32])},
                          ev.target_session,ev.source_package_id,ev.source_symbol,
                          ev.rule_id,ev.rule_version,ev.source_capture_id,ev.state,
                          ev.profile_revision,ev.history_session_count,
                          ev.d1_history_session_count,ev.event_window_start,
                          ev.calendar_revision,ev.scope
                   FROM source_research_observation_segment_days o
                   LEFT JOIN source_rule_evaluations ev
                     ON ev.id=o.evidence_rule_fact_id
                   WHERE o.source_enrollment_id=ANY(%s)
                     AND o.observation_session>=%s
                     AND o.observation_session<=%s
                   ORDER BY o.source_enrollment_id,o.observation_session,o.id""",
            (list(anchors), t0_session, observation_end),
        )
        for raw in cursor.fetchall():
            row = dict(zip(_OBSERVATION_COLUMNS, raw, strict=True))
            observations_by_id[row["source_enrollment_id"]].append(row)

    details: dict[int, dict[str, Any]] = {}
    for enrollment_id, anchor in anchors.items():
        if anchor["t0_state"] != "TRUE":
            outcome = {
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": "T0_ENROLLMENT_ANCHOR_NOT_TRUE",
                "evidence_closed": False,
                "horizon_session": due_session.isoformat() if due_session else None,
                "first_exit": None,
                "unknown_sessions": [],
                "quality_reason_codes": ["T0_ENROLLMENT_ANCHOR_NOT_TRUE"],
                "observed_true_session_count": 0,
            }
        else:
            outcome = classify_observation_horizon(
                anchor=anchor,
                rows=observations_by_id[enrollment_id],
                expected_sessions=expected_sessions,
                due_session=due_session,
                research_cutoff=research_cutoff,
            )
        details[enrollment_id] = {
            **outcome,
            "enrollment_id": enrollment_id,
            "source_symbol": anchor["source_symbol"],
            "rule_id": anchor["rule_id"],
            "lane": anchor["enrollment_kind"],
            "t0_session": t0_session.isoformat(),
            "horizon": horizon,
            "parent_enrollment_id": anchor["parent_enrollment_id"],
            "source_capture_id": anchor["anchor_capture_id"],
            "calendar_revision": anchor["calendar_revision"],
        }

    forward_rows_by_id: dict[int, list[tuple[Any, ...]]] = {
        enrollment_id: [] for enrollment_id in anchors
    }
    if anchors and due_session is not None and due_session <= research_cutoff:
        cursor.execute(
            f"""SELECT e.id,{", ".join(f"f.{column}" for column in _FORWARD_COLUMNS)}
                   FROM source_research_enrollments e
                   JOIN source_rule_evaluations r ON r.id=e.rule_fact_id
                   JOIN source_forward_path_facts f
                     ON f.target_session=r.target_session
                    AND f.source_package_id=r.source_package_id
                    AND f.source_capture_id=r.source_capture_id
                    AND f.source_symbol=r.source_symbol
                   WHERE e.id=ANY(%s) AND f.horizon=%s
                     AND f.endpoint_session<=%s
                   ORDER BY e.id,f.path_rule_version,f.calendar_revision,
                            f.endpoint_session""",
            (list(anchors), horizon, research_cutoff),
        )
        for row in cursor.fetchall():
            forward_rows_by_id[row[0]].append(row[1:])

    metric_values: dict[tuple[str, str, str], dict[str, list[str]]] = defaultdict(
        lambda: {metric: [] for metric in _METRIC_COLUMNS}
    )
    forward_status_counts: Counter[tuple[str, str, str, str]] = Counter()
    path_quality_counts: Counter[tuple[str, str, str, str]] = Counter()
    observation_reason_counts: Counter[tuple[str, str, str, str]] = Counter()
    forward_reason_counts: Counter[tuple[str, str, str, str]] = Counter()
    enrollment_counts: Counter[tuple[str, str, str]] = Counter()
    pre_due_status_counts: Counter[tuple[str, str, str, str]] = Counter()
    pre_due_reason_counts: Counter[tuple[str, str, str, str]] = Counter()
    symbols_by_group: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    closed_counts: Counter[tuple[str, str, str]] = Counter()
    all_lanes: dict[tuple[str, str], list[int]] = defaultdict(list)

    from stock_a.storage.postgres.research_facts import PATH_VERSION

    for enrollment_id, anchor in anchors.items():
        outcome = details[enrollment_id]
        rule = anchor["rule_id"]
        lane = anchor["enrollment_kind"]
        status = outcome["status"]
        key = (rule, lane, status)
        enrollment_counts[key] += 1
        symbols_by_group[key].add(anchor["source_symbol"])
        all_lanes[(rule, lane)].append(enrollment_id)
        if outcome["evidence_closed"]:
            closed_counts[key] += 1
        observation_reasons = set(outcome.get("quality_reason_codes", []))
        if outcome.get("reason_code"):
            observation_reasons.add(outcome["reason_code"])
        for reason in observation_reasons:
            observation_reason_counts[key + (reason,)] += 1
        if status == "NOT_DUE":
            pre_due_status = outcome.get("pre_due_status", "NOT_DUE")
            pre_due_status_counts[key + (pre_due_status,)] += 1
            pre_due_reason = outcome.get("pre_due_reason_code")
            if pre_due_reason:
                pre_due_reason_counts[key + (pre_due_reason,)] += 1

        forward_anchor = {
            "package": anchor["source_package_id"],
            "capture": anchor["anchor_capture_id"],
            "source_symbol": anchor["source_symbol"],
            "calendar_revision": anchor["calendar_revision"],
            "scope": anchor["scope"],
        }
        paths = _enrollment_forward_paths(
            forward_anchor,
            forward_rows_by_id[enrollment_id],
            due_by_horizon,
            cutoff=research_cutoff,
        )
        path = next(item for item in paths if item["horizon"] == horizon)
        outcome["forward"] = path
        forward_status_counts[key + (path["status"],)] += 1
        if path["status"] in {"OBSERVED", "UNKNOWN"}:
            path_quality_counts[key + (path["path_quality"],)] += 1
        for reason in path.get("reason_codes", []):
            forward_reason_counts[key + (reason,)] += 1
        for metric, field in _METRIC_COLUMNS.items():
            value = path.get(field)
            if value is not None:
                metric_values[key][metric].append(value)

    kind_counts = {
        kind: {
            "enrollment_count": len(rows),
            "independent_source_symbol_count": len({row[2] for row in rows}),
        }
        for kind, rows in sorted(all_by_kind.items())
    }
    lane_groups = []
    for rule, lane in sorted(
        all_lanes,
        key=lambda item: (item[0], {"ENTRY": 0, "REENTRY": 1}.get(item[1], 2)),
    ):
        risk_ids = all_lanes[(rule, lane)]
        risk_symbols = {
            anchors[enrollment_id]["source_symbol"] for enrollment_id in risk_ids
        }
        for status in OUTCOME_STATUSES:
            key = (rule, lane, status)
            metric_summaries = {
                metric: _fraction_summary(metric_values[key][metric])
                for metric in _METRIC_COLUMNS
            }
            lane_groups.append(
                {
                    "rule_id": rule,
                    "lane": lane,
                    "outcome_status": status,
                    "enrollment_count": enrollment_counts[key],
                    "independent_source_symbol_count": len(symbols_by_group[key]),
                    "risk_set_enrollment_count": len(risk_ids),
                    "risk_set_independent_source_symbol_count": len(risk_symbols),
                    "evidence_closed_count": closed_counts[key],
                    "forward_status_counts": {
                        state: forward_status_counts[key + (state,)]
                        for state in ("OBSERVED", "UNKNOWN", "NOT_COMPUTED", "NOT_DUE")
                    },
                    "path_quality_counts": {
                        quality: path_quality_counts[key + (quality,)]
                        for quality in ("OBSERVED", "UNKNOWN")
                    },
                    "metrics": metric_summaries,
                    "observation_reason_counts": {
                        reason: count
                        for (
                            group_rule,
                            group_lane,
                            group_status,
                            reason,
                        ), count in sorted(observation_reason_counts.items())
                        if (group_rule, group_lane, group_status) == key
                    },
                    "forward_reason_counts": {
                        reason: count
                        for (
                            group_rule,
                            group_lane,
                            group_status,
                            reason,
                        ), count in sorted(forward_reason_counts.items())
                        if (group_rule, group_lane, group_status) == key
                    },
                    "pre_due_status_counts": {
                        pre_due_status: pre_due_status_counts[key + (pre_due_status,)]
                        for pre_due_status in (
                            "FIRST_EXIT_PROVED",
                            "PAUSED_UNKNOWN",
                            "CONTINUOUS_THROUGH_CUTOFF",
                            "INCOMPARABLE_OR_INCOMPLETE",
                            "NOT_COMPUTED",
                            "NOT_DUE",
                        )
                        if pre_due_status_counts[key + (pre_due_status,)]
                    },
                    "pre_due_reason_counts": {
                        reason: count
                        for (
                            group_rule,
                            group_lane,
                            group_status,
                            reason,
                        ), count in sorted(pre_due_reason_counts.items())
                        if (group_rule, group_lane, group_status) == key
                    },
                    "independent_t0_date_count": 1 if enrollment_counts[key] else 0,
                    "quantile_method": "NEAREST_RANK",
                }
            )

    risk_rows = [row for row in eligible]
    full_symbols = {row[2] for row in anchor_rows}
    risk_symbols = {row[2] for row in risk_rows}
    summary = {
        "status": "AVAILABLE" if risk_rows else "NO_OBSERVED_SAMPLE",
        "t0_session": t0_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "horizon": horizon,
        "horizon_session": due_session.isoformat() if due_session else None,
        "outcome_classification": "RETROSPECTIVE_OUTCOME_STRATIFICATION_NON_PIT",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "scope": "NON_PIT_EXPLORATORY",
        "price_coordinate": "RAW_UNADJUSTED",
        "denominator": {
            "kind": "FROZEN_SOURCE_T0_ENROLLMENTS",
            "full_t0_enrollment_count": len(anchor_rows),
            "full_t0_independent_source_symbol_count": len(full_symbols),
            "enrollment_kind_counts": kind_counts,
            "primary_entry_reentry_risk_set_count": len(risk_rows),
            "primary_entry_reentry_source_symbol_count": len(risk_symbols),
            "excluded_left_censored_and_daily_landmark_count": len(anchor_rows)
            - len(risk_rows),
        },
        "due_status": (
            "NOT_DUE"
            if due_session is None or due_session > research_cutoff
            else "DUE_BY_RESEARCH_CUTOFF"
        ),
        "groups": lane_groups,
        "supports_comparable_price": any(
            group["metrics"]["RAW_CLOSE_RETURN"]["evaluated_count"] > 0
            for group in lane_groups
        ),
        "aggregate_status": (
            "NO_COMPARABLE_SAMPLE"
            if not any(
                group["metrics"]["RAW_CLOSE_RETURN"]["evaluated_count"] > 0
                for group in lane_groups
            )
            else "DESCRIPTIVE_ONLY"
        ),
    }
    return summary, details
