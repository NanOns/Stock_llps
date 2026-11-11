"""已存板块PULSE的离线后继Raw等权结果生产链。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from fractions import Fraction
from typing import Any, Mapping

from stock_a.application.source_d0_raw import _official_calendar
from stock_a.domain.sector_pulse_followup import (
    source_sector_pulse_followup_outcome,
)
from stock_a.storage.postgres.sector_pulse_followups import (
    persist_source_sector_pulse_followups,
    read_source_sector_pulse_followup_inputs,
    read_source_sector_pulse_followups,
)

_HORIZONS = (1, 3, 5)


class SourceSectorPulseFollowupError(ValueError):
    """板块PULSE后继研究的输入或持久化错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _horizon_targets(
    target_session: date,
    research_cutoff: date,
    sessions: tuple[date, ...],
) -> tuple[dict[int, date | None], dict[int, date]]:
    try:
        index = sessions.index(target_session)
    except ValueError as error:
        raise SourceSectorPulseFollowupError(
            "PULSE_FOLLOWUP_T0_NOT_OFFICIAL", "T0不是正式交易日历中的官方会话"
        ) from error
    all_targets = {
        horizon: sessions[index + horizon] if index + horizon < len(sessions) else None
        for horizon in _HORIZONS
    }
    due = {
        horizon: target
        for horizon, target in all_targets.items()
        if target is not None and target <= research_cutoff
    }
    return all_targets, due


def _readback_pulse_day(context: Mapping[str, Any]) -> dict[str, Any]:
    pulses = context["pulse_rows"]
    identity = pulses[0]
    by_kind: dict[str, dict[str, Any]] = {}
    for kind in ("INDUSTRY", "CONCEPT"):
        group = [row for row in pulses if row["board_kind"] == kind]
        by_kind[kind] = {
            "status": "AVAILABLE",
            "reason_codes": [],
            "snapshot_id": group[0]["snapshot_id"],
            "expected_board_count": len(group),
            "read_board_count": len(group),
            "facts": {
                row["board_native_id"]: {
                    "state": row["pulse_state"],
                    "member_count": int(row["member_count"]),
                }
                for row in group
            },
        }
    return {
        "status": "AVAILABLE",
        "target_session": identity["target_session"].isoformat(),
        "profile_revision": identity["profile_revision"],
        "source_version": {
            "namespace_id": identity["namespace_id"],
            "package_sha256": identity["package_sha256"],
            "source_capture_id": identity["source_capture_id"],
            "source_content_id": identity["source_content_id"],
            "source_raw_digest": identity["source_raw_digest"],
        },
        "by_kind": by_kind,
    }


def _summaries(outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    result = {}
    for horizon in _HORIZONS:
        scoped = [item for item in outcomes if item["horizon_sessions"] == horizon]
        by_pulse: dict[str, dict[str, int]] = {}
        for item in scoped:
            group = by_pulse.setdefault(
                item["pulse"]["pulse_state"], {"OBSERVED": 0, "UNKNOWN": 0}
            )
            group[item["outcome"]["outcome_status"]] += 1
        reason_counts = Counter(
            reason for item in scoped for reason in item["outcome"]["reason_codes"]
        )
        result[str(horizon)] = {
            "target_session": scoped[0]["target_session"].isoformat()
            if scoped
            else None,
            "board_count": len(scoped),
            "pulse_state_counts": dict(sorted(by_pulse.items())),
            "reason_code_counts": dict(sorted(reason_counts.items())),
        }
    return result


def run_source_sector_pulse_followup_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
) -> dict[str, Any]:
    """只消费PG已存PULSE，批量读取固定篮子必要Raw并原子保存后继结果。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or not isinstance(package_sha256, str)
        or len(package_sha256) != 64
        or any(character not in "0123456789abcdef" for character in package_sha256)
    ):
        raise SourceSectorPulseFollowupError(
            "PULSE_FOLLOWUP_REQUEST_INVALID", "T0、研究截止或来源包参数无效"
        )
    calendar = _official_calendar(research_cutoff)
    all_targets, due_targets = _horizon_targets(
        target_session, research_cutoff, tuple(calendar["sessions"])
    )
    context = read_source_sector_pulse_followup_inputs(
        config,
        t0_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        due_targets=due_targets,
    )
    pulse_states = Counter(row["pulse_state"] for row in context["pulse_rows"])
    outcomes: list[dict[str, Any]] = []
    incomplete_horizons = []
    for horizon, target in due_targets.items():
        status = context["target_partition_statuses"].get(horizon, {}).get("status")
        if status != "CLOSED":
            incomplete_horizons.append(
                {
                    "horizon_sessions": horizon,
                    "target_session": target.isoformat(),
                    "status": "INCOMPLETE",
                    "reason_codes": ["FOLLOWUP_RAW_SESSION_PARTITION_NOT_CLOSED"],
                }
            )
            continue
        for board_key, pulse in context["pulse_by_board"].items():
            kind, board_id = board_key
            outcome = source_sector_pulse_followup_outcome(
                member_symbols=context["members_by_board"][board_key],
                rows_by_symbol=context["rows_by_symbol"],
                t0_session=target_session,
                target_session=target,
                namespace_id=context["namespace_id"],
                package_sha256=package_sha256,
                source_content_id=pulse["source_content_id"],
            )
            outcomes.append(
                {
                    "pulse": pulse,
                    "board_kind": kind,
                    "board_native_id": board_id,
                    "horizon_sessions": horizon,
                    "target_session": target,
                    "outcome": outcome,
                }
            )
    if outcomes:
        persistence = persist_source_sector_pulse_followups(
            config,
            context=context,
            outcomes=outcomes,
            calendar_identity=calendar,
        )
        readback = read_source_sector_pulse_followups(
            config,
            pulse_day=_readback_pulse_day(context),
            research_cutoff=research_cutoff,
            horizon_targets=all_targets,
            calendar_identity=calendar,
        )
        for item in outcomes:
            fact = (
                readback.get("by_kind", {})
                .get(item["board_kind"], {})
                .get("facts", {})
                .get(item["board_native_id"], {})
            )
            stored = next(
                (
                    horizon
                    for horizon in fact.get("horizons", ())
                    if horizon.get("horizon_sessions") == item["horizon_sessions"]
                ),
                None,
            )
            expected = item["outcome"]
            expected_fraction = expected["equal_weight_mean_return"]
            if (
                not isinstance(stored, Mapping)
                or stored.get("status") != expected["outcome_status"]
                or stored.get("fixed_member_denominator")
                != expected["fixed_member_denominator"]
                or stored.get("observed_member_count")
                != expected["observed_member_count"]
                or stored.get("unknown_member_count")
                != expected["unknown_member_count"]
                or stored.get("equal_weight_return")
                != (str(expected_fraction) if expected_fraction is not None else None)
            ):
                raise SourceSectorPulseFollowupError(
                    "PULSE_FOLLOWUP_CROSS_TRANSACTION_READBACK_MISMATCH",
                    "提交后独立PG事务读回与离线生产结果不一致",
                )
    else:
        persistence = {
            "status": "NO_DUE_CLOSED_HORIZONS"
            if not due_targets
            else "NO_COMPLETE_HORIZON_TO_PERSIST",
            "rows_expected": 0,
            "rows_inserted": 0,
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
        }
        readback = None
    summary = _summaries(outcomes)
    for horizon, target in all_targets.items():
        summary.setdefault(
            str(horizon),
            {
                "target_session": target.isoformat() if target else None,
                "board_count": 0,
                "status": "NOT_DUE",
            },
        )
        if target is not None and target > research_cutoff:
            summary[str(horizon)].update(
                {
                    "target_session": target.isoformat(),
                    "board_count": 0,
                    "status": "NOT_DUE",
                }
            )
    return {
        "status": "PERSISTED" if outcomes else persistence["status"],
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "package_sha256": package_sha256,
        "pulse_state_counts": dict(sorted(pulse_states.items())),
        "pulse_board_count": len(context["pulse_rows"]),
        "followup_summaries": summary,
        "incomplete_horizons": incomplete_horizons,
        "persistence": persistence,
        "independent_transaction_readback": "PASS"
        if readback is not None
        else "NOT_RUN",
        "readback_status": readback.get("status") if readback else None,
        "readback_saved_through": readback.get("saved_through") if readback else None,
        "input": {
            "t0_source_capture_id": context["t0_source_version"]["source_capture_id"],
            "t0_source_content_id": context["t0_source_version"]["source_content_id"],
            "t0_source_raw_digest": context["t0_source_version"]["source_raw_digest"],
            "profile_revision": context["pulse_rows"][0]["profile_revision"],
            "snapshot_ids": {
                kind: next(
                    row["snapshot_id"]
                    for row in context["pulse_rows"]
                    if row["board_kind"] == kind
                )
                for kind in ("INDUSTRY", "CONCEPT")
            },
            "calendar_id": calendar["calendar_id"],
            "calendar_revision": calendar["calendar_revision"],
            "calendar_sha256": calendar["calendar_sha256"],
            "raw_sessions": [
                target_session.isoformat(),
                *[
                    context["target_source_versions"][horizon][
                        "session_date"
                    ].isoformat()
                    for horizon in sorted(context["target_source_versions"])
                ],
            ],
            "raw_history_rows_read": context["raw_history_row_count"],
            "raw_partition_statuses": {
                str(horizon): {
                    "target_session": item["target_session"].isoformat(),
                    "status": item["status"],
                    "row_count": item["row_count"],
                    "expected_count": item["expected_count"],
                }
                for horizon, item in context["target_partition_statuses"].items()
            },
            "saved_through": context["saved_through"].isoformat()
            if context["saved_through"]
            else None,
        },
        "database_write_attempted": persistence.get("database_write_attempted", False),
        "ddl_executed": False,
        "dml_executed": persistence.get("dml_executed", False),
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "price_coordinate": "RAW_UNADJUSTED",
        "formal_rotation_state": "NOT_EVALUATED",
    }


__all__ = ["SourceSectorPulseFollowupError", "run_source_sector_pulse_followup_day"]
