"""来源观察段在官方期限内的后验描述结局。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence


OUTCOME_STATUSES = (
    "CONTINUOUS_MATCH_THROUGH_HORIZON",
    "FIRST_EXIT_PROVED",
    "PAUSED_UNKNOWN",
    "INCOMPARABLE_OR_INCOMPLETE",
    "NOT_COMPUTED",
    "NOT_DUE",
)

_ANCHOR_FIELDS = (
    ("source_package_id", "source_package_id"),
    ("source_symbol", "source_symbol"),
    ("rule_id", "rule_id"),
    ("rule_version", "rule_version"),
    ("t0_session", "t0_session"),
    ("anchor_rule_fact_id", "anchor_rule_fact_id"),
    ("anchor_capture_id", "anchor_capture_id"),
    ("enrollment_kind", "enrollment_kind"),
    ("parent_enrollment_id", "parent_enrollment_id"),
    ("calendar_revision", "calendar_revision"),
    ("scope", "scope"),
    ("price_coordinate", "price_coordinate"),
    ("identity_status", "identity_status"),
)


def classify_observation_horizon(
    *,
    anchor: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    expected_sessions: Sequence[date],
    due_session: date | None,
    research_cutoff: date,
) -> dict[str, Any]:
    """依据已存事实分类单一ENTRY/REENTRY登记，不推算缺失状态。"""
    t0 = anchor.get("t0_session")
    if type(t0) is not date or type(research_cutoff) is not date:
        raise ValueError("观察结局必须绑定官方T0和研究截止")
    if due_session is not None and type(due_session) is not date:
        raise ValueError("观察期限必须是官方日期")
    if any(type(session) is not date or session <= t0 for session in expected_sessions):
        raise ValueError("观察期限会话必须晚于T0")
    if tuple(sorted(set(expected_sessions))) != tuple(expected_sessions):
        raise ValueError("观察期限会话必须严格递增且唯一")

    base = {
        "horizon_session": due_session.isoformat() if due_session else None,
        "evidence_closed": False,
        "first_exit": None,
        "unknown_sessions": [],
        "quality_reason_codes": [],
        "observed_true_session_count": 0,
    }
    if due_session is None or due_session > research_cutoff:
        pre_due_sessions = tuple(
            session for session in expected_sessions if session <= research_cutoff
        )
        if pre_due_sessions:
            partial = classify_observation_horizon(
                anchor=anchor,
                rows=rows,
                expected_sessions=pre_due_sessions,
                due_session=pre_due_sessions[-1],
                research_cutoff=research_cutoff,
            )
            pre_due_status = (
                "CONTINUOUS_THROUGH_CUTOFF"
                if partial["status"] == "CONTINUOUS_MATCH_THROUGH_HORIZON"
                else partial["status"]
            )
            return {
                **base,
                "status": "NOT_DUE",
                "reason_code": "HORIZON_NOT_DUE_BY_RESEARCH_CUTOFF",
                "pre_due_status": pre_due_status,
                "pre_due_observed_through": pre_due_sessions[-1].isoformat(),
                "pre_due_reason_code": partial.get("reason_code"),
                "pre_due_first_exit": partial.get("first_exit"),
                "pre_due_unknown_sessions": partial.get("unknown_sessions", []),
                "pre_due_quality_reason_codes": partial.get("quality_reason_codes", []),
                "pre_due_evidence_closed_through_cutoff": partial.get(
                    "evidence_closed", False
                ),
            }
        return {
            **base,
            "status": "NOT_DUE",
            "reason_code": "HORIZON_NOT_DUE_BY_RESEARCH_CUTOFF",
            "pre_due_status": "NOT_DUE",
            "pre_due_observed_through": None,
            "pre_due_reason_code": "NO_OFFICIAL_SESSION_THROUGH_CUTOFF",
            "pre_due_first_exit": None,
            "pre_due_unknown_sessions": [],
            "pre_due_quality_reason_codes": [],
            "pre_due_evidence_closed_through_cutoff": False,
        }

    by_session: dict[date, Mapping[str, Any]] = {}
    for row in rows:
        session = row.get("observation_session")
        if type(session) is not date or session in by_session:
            return {
                **base,
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": "OBSERVATION_FACT_DATE_AMBIGUOUS",
            }
        by_session[session] = row

    reasons: list[str] = []

    def validate_identity(row: Mapping[str, Any], session: date) -> str | None:
        if row.get("observation_session") != session:
            return "OBSERVATION_FACT_SESSION_MISMATCH"
        for row_key, anchor_key in _ANCHOR_FIELDS:
            if row.get(row_key) != anchor.get(anchor_key):
                return f"OBSERVATION_ANCHOR_MISMATCH:{row_key}"
        if row.get("producer_contract") != "SOURCE_RESEARCH_OBSERVATION_SEGMENTS_R1":
            return "OBSERVATION_PRODUCER_CONTRACT_MISMATCH"
        if row.get("producer_version") != "R1":
            return "OBSERVATION_PRODUCER_VERSION_MISMATCH"
        if (
            row.get("previous_session") is not None
            and row.get("previous_session") >= session
        ):
            return "OBSERVATION_PREVIOUS_SESSION_INVALID"
        if row.get("evidence_rule_fact_id") is not None:
            evidence_fields = (
                ("evidence_target_session", session),
                ("evidence_source_package_id", anchor.get("source_package_id")),
                ("evidence_source_symbol", anchor.get("source_symbol")),
                ("evidence_rule_id", anchor.get("rule_id")),
                ("evidence_rule_version", anchor.get("rule_version")),
                ("evidence_capture_id", row.get("target_capture_id")),
                ("evidence_state", row.get("observation_state")),
                ("evidence_profile_revision", row.get("profile_revision")),
                (
                    "evidence_history_session_count",
                    row.get("history_session_count"),
                ),
                (
                    "evidence_d1_history_session_count",
                    row.get("d1_history_session_count"),
                ),
                ("evidence_event_window_start", row.get("event_window_start")),
                ("evidence_calendar_revision", row.get("calendar_revision")),
                ("evidence_scope", row.get("scope")),
            )
            if any(row.get(key) != expected for key, expected in evidence_fields):
                return "OBSERVATION_EVIDENCE_FACT_MISMATCH"
        elif row.get("quality_status") == "OBSERVED":
            return "OBSERVED_FACT_WITHOUT_RULE_EVIDENCE"
        return None

    t0_row = by_session.get(t0)
    if t0_row is None:
        return {
            **base,
            "status": "NOT_COMPUTED",
            "reason_code": "T0_OBSERVATION_SEGMENT_NOT_COMPUTED",
        }
    t0_error = validate_identity(t0_row, t0)
    if t0_error:
        return {
            **base,
            "status": "INCOMPARABLE_OR_INCOMPLETE",
            "reason_code": t0_error,
        }
    if t0_row.get("quality_status") == "NOT_COMPUTED":
        return {
            **base,
            "status": "NOT_COMPUTED",
            "reason_code": t0_row.get("reason_code") or "T0_OBSERVATION_NOT_COMPUTED",
        }
    if (
        t0_row.get("quality_status") != "OBSERVED"
        or t0_row.get("observation_state") != "TRUE"
        or t0_row.get("comparability_status") != "INITIAL"
        or t0_row.get("evidence_rule_fact_id") != anchor.get("anchor_rule_fact_id")
        or t0_row.get("target_capture_id") != anchor.get("anchor_capture_id")
        or t0_row.get("profile_revision") != anchor.get("profile_revision")
        or t0_row.get("history_session_count") != anchor.get("history_session_count")
        or t0_row.get("d1_history_session_count")
        != anchor.get("d1_history_session_count")
        or t0_row.get("event_window_start") != anchor.get("event_window_start")
        or t0_row.get("calendar_revision") != anchor.get("calendar_revision")
        or t0_row.get("scope") != anchor.get("scope")
    ):
        reason = t0_row.get("reason_code") or "T0_OBSERVATION_ANCHOR_NOT_PROVED"
        return {
            **base,
            "status": "INCOMPARABLE_OR_INCOMPLETE",
            "reason_code": reason,
        }

    expected_by_session = {
        session: index for index, session in enumerate(expected_sessions)
    }
    if due_session not in expected_by_session:
        return {
            **base,
            "status": "INCOMPARABLE_OR_INCOMPLETE",
            "reason_code": "HORIZON_SESSION_NOT_IN_OFFICIAL_SEQUENCE",
        }
    horizon_index = expected_by_session[due_session]
    relevant_sessions = expected_sessions[: horizon_index + 1]
    observed_successors = [
        session for session in relevant_sessions if session in by_session
    ]
    if not observed_successors:
        return {
            **base,
            "status": "NOT_COMPUTED",
            "reason_code": "OBSERVATION_HORIZON_NOT_COMPUTED",
        }

    unknown_sessions: list[dict[str, Any]] = []
    true_count = 0
    previous_session = t0
    for session in relevant_sessions:
        row = by_session.get(session)
        if row is None:
            reasons.append("DUE_OBSERVATION_FACT_MISSING")
            return {
                **base,
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": "DUE_OBSERVATION_FACT_MISSING",
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "unknown_sessions": unknown_sessions,
                "observed_true_session_count": true_count,
            }
        identity_error = validate_identity(row, session)
        if identity_error:
            reasons.append(identity_error)
            return {
                **base,
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": identity_error,
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "unknown_sessions": unknown_sessions,
                "observed_true_session_count": true_count,
            }
        if row.get("previous_session") != previous_session:
            reasons.append("NONADJACENT_OFFICIAL_OBSERVATION_FACT")
            return {
                **base,
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": "NONADJACENT_OFFICIAL_OBSERVATION_FACT",
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "unknown_sessions": unknown_sessions,
                "observed_true_session_count": true_count,
            }
        quality = row.get("quality_status")
        state = row.get("observation_state")
        event = row.get("event_code")
        comparability = row.get("comparability_status")
        reason = row.get("reason_code")
        if reason:
            reasons.append(reason)
        if quality == "NOT_COMPUTED":
            return {
                **base,
                "status": "NOT_COMPUTED"
                if not unknown_sessions and true_count == 0
                else "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": reason or "DUE_OBSERVATION_NOT_COMPUTED",
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "unknown_sessions": unknown_sessions,
                "observed_true_session_count": true_count,
            }
        if quality == "INCOMPLETE" or comparability != "COMPARABLE":
            return {
                **base,
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": reason or "OBSERVATION_FACT_NOT_COMPARABLE",
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "unknown_sessions": unknown_sessions,
                "observed_true_session_count": true_count,
            }
        if (
            quality == "OBSERVED"
            and state == "FALSE"
            and event == "EXIT_PROVED"
            and comparability == "COMPARABLE"
            and row.get("evidence_rule_fact_id") is not None
        ):
            exit_fact = {
                "session": session.isoformat(),
                "previous_session": previous_session.isoformat(),
                "observation_fact_id": row.get("id"),
                "evidence_rule_fact_id": row.get("evidence_rule_fact_id"),
                "capture_id": row.get("target_capture_id"),
                "reason_code": reason,
            }
            return {
                **base,
                "status": "FIRST_EXIT_PROVED",
                "reason_code": reason or "OBSERVED_COMPARABLE_FALSE",
                "first_exit": exit_fact,
                "unknown_sessions": unknown_sessions,
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "observed_true_session_count": true_count,
                "evidence_closed": True,
            }
        if (
            quality == "UNKNOWN"
            or state == "UNKNOWN"
            or event in {"PAUSED_UNKNOWN", "UNKNOWN"}
        ):
            unknown_sessions.append(
                {
                    "session": session.isoformat(),
                    "observation_fact_id": row.get("id"),
                    "evidence_rule_fact_id": row.get("evidence_rule_fact_id"),
                    "capture_id": row.get("target_capture_id"),
                    "reason_code": reason,
                }
            )
        elif quality == "OBSERVED" and state == "TRUE" and event == "LEFT_CENSORED":
            unknown_sessions.append(
                {
                    "session": session.isoformat(),
                    "observation_fact_id": row.get("id"),
                    "evidence_rule_fact_id": row.get("evidence_rule_fact_id"),
                    "capture_id": row.get("target_capture_id"),
                    "reason_code": reason or "MATCH_AFTER_UNKNOWN_INTERVAL",
                }
            )
        elif (
            quality == "OBSERVED"
            and state == "TRUE"
            and event == "CONTINUES"
            and comparability == "COMPARABLE"
        ):
            true_count += 1
        else:
            reason = reason or "OBSERVATION_STATE_NOT_CLOSED"
            reasons.append(reason)
            return {
                **base,
                "status": "INCOMPARABLE_OR_INCOMPLETE",
                "reason_code": reason,
                "quality_reason_codes": list(dict.fromkeys(reasons)),
                "unknown_sessions": unknown_sessions,
                "observed_true_session_count": true_count,
            }
        previous_session = session

    if unknown_sessions:
        return {
            **base,
            "status": "PAUSED_UNKNOWN",
            "reason_code": "UNKNOWN_OR_LEFT_CENSORED_WITHIN_HORIZON",
            "unknown_sessions": unknown_sessions,
            "quality_reason_codes": list(dict.fromkeys(reasons)),
            "observed_true_session_count": true_count,
            "evidence_closed": True,
        }
    if true_count == len(relevant_sessions):
        return {
            **base,
            "status": "CONTINUOUS_MATCH_THROUGH_HORIZON",
            "reason_code": "ALL_OFFICIAL_SUCCESSOR_SESSIONS_TRUE_AND_COMPARABLE",
            "quality_reason_codes": list(dict.fromkeys(reasons)),
            "observed_true_session_count": true_count,
            "evidence_closed": True,
        }
    return {
        **base,
        "status": "INCOMPARABLE_OR_INCOMPLETE",
        "reason_code": "OBSERVATION_HORIZON_NOT_CLOSED",
        "quality_reason_codes": list(dict.fromkeys(reasons)),
        "observed_true_session_count": true_count,
    }
