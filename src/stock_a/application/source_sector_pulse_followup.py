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

_HORIZONS = (1, 3, 5, 10, 20)


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
                    "pulse_fact_id": int(row["id"]),
                    "catalog_id": int(row["catalog_id"]),
                }
                for row in group
            },
        }
    return {
        "status": "AVAILABLE",
        "target_session": identity["target_session"].isoformat(),
        "source_version": context["t0_source_version"],
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
    source_capture_id: int,
    target_capture_ids: Mapping[int, int] | None = None,
) -> dict[str, Any]:
    """只消费PG已存PULSE，批量读取固定篮子QFQ并原子保存后继结果。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise SourceSectorPulseFollowupError(
            "PULSE_FOLLOWUP_REQUEST_INVALID", "T0、研究截止或paired capture无效"
        )
    calendar = _official_calendar(research_cutoff)
    calendar_identity = {
        "calendar_id": calendar["calendar_id"],
        "calendar_revision": calendar["calendar_revision"],
    }
    all_targets, due_targets = _horizon_targets(
        target_session, research_cutoff, tuple(calendar["sessions"])
    )
    resolved_target_capture_ids = {
        horizon: source_capture_id for horizon in due_targets
    }
    if target_capture_ids is not None and any(
        target_capture_ids.get(horizon) != source_capture_id
        for horizon in due_targets
        if horizon in target_capture_ids
    ):
        raise SourceSectorPulseFollowupError(
            "PULSE_FOLLOWUP_CAPTURE_MISMATCH",
            "本批后继会话必须复用覆盖完整窗口的paired capture",
        )
    context = read_source_sector_pulse_followup_inputs(
        config,
        t0_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        due_targets=due_targets,
        target_capture_ids=resolved_target_capture_ids,
    )
    t0_source = context.get("t0_source_version")
    if (
        not isinstance(t0_source, Mapping)
        or t0_source.get("source_capture_id") != source_capture_id
        or set(t0_source.get("price_coordinates", ()))
        != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or t0_source.get("source_time_quality") != "NON_PIT_OBSERVED"
        or t0_source.get("source_clock_role") != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
        or t0_source.get("source_available_at") is not None
    ):
        raise SourceSectorPulseFollowupError(
            "PULSE_FOLLOWUP_T0_CAPTURE_INVALID",
            "T0 PULSE来源不是完整、成对且NON_PIT的BaoStock捕获",
        )
    for horizon, version in context.get("target_source_versions", {}).items():
        if (
            version.get("source_capture_id") != source_capture_id
            or set(version.get("price_coordinates", ()))
            != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
            or version.get("source_time_quality") != "NON_PIT_OBSERVED"
            or version.get("source_clock_role") != "BAOSTOCK_QUERY_CAPTURED_NON_PIT"
            or version.get("source_available_at") is not None
        ):
            raise SourceSectorPulseFollowupError(
                "PULSE_FOLLOWUP_TARGET_CAPTURE_INVALID",
                f"{horizon}会话未绑定本批成对NON_PIT BaoStock捕获",
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
                    "reason_codes": ["FOLLOWUP_QFQ_SESSION_PARTITION_NOT_CLOSED"],
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
                t0_source_capture_id=context["t0_source_version"]["source_capture_id"],
                target_source_capture_id=context["target_source_versions"][horizon][
                    "source_capture_id"
                ],
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
    persistence = persist_source_sector_pulse_followups(
        config,
        context=context,
        outcomes=outcomes,
        horizon_targets=all_targets,
        calendar_identity=calendar_identity,
    )
    readback = read_source_sector_pulse_followups(
        config,
        pulse_day=_readback_pulse_day(context),
        research_cutoff=research_cutoff,
        horizon_targets=all_targets,
        calendar_identity=calendar_identity,
        target_capture_ids={
            h: v["source_capture_id"]
            for h, v in context["target_source_versions"].items()
        },
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
            or stored.get("observed_member_count") != expected["observed_member_count"]
            or stored.get("unknown_member_count") != expected["unknown_member_count"]
            or stored.get("equal_weight_return")
            != (str(expected_fraction) if expected_fraction is not None else None)
        ):
            raise SourceSectorPulseFollowupError(
                "PULSE_FOLLOWUP_CROSS_TRANSACTION_READBACK_MISMATCH",
                "PG独立事务读回与本次精确后继结果不一致",
            )
    if any(
        readback.get("by_kind", {}).get(kind, {}).get("comparison", {}).get("status")
        != "AVAILABLE"
        for kind in ("INDUSTRY", "CONCEPT")
    ):
        raise SourceSectorPulseFollowupError(
            "PULSE_COHORT_CROSS_TRANSACTION_READBACK_INCOMPLETE",
            "PG独立事务未读回完整板块PULSE分组事实",
        )
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
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "pulse_state_counts": dict(sorted(pulse_states.items())),
        "pulse_board_count": len(context["pulse_rows"]),
        "followup_summaries": summary,
        "incomplete_horizons": incomplete_horizons,
        "persistence": persistence,
        "independent_transaction_readback": "PASS",
        "readback_status": readback.get("status"),
        "readback_saved_through": readback.get("saved_through"),
        "input": {
            "t0_source_capture_id": context["t0_source_version"]["source_capture_id"],
            "snapshot_ids": {
                kind: next(
                    row["snapshot_id"]
                    for row in context["pulse_rows"]
                    if row["board_kind"] == kind
                )
                for kind in ("INDUSTRY", "CONCEPT")
            },
            "calendar_id": calendar_identity["calendar_id"],
            "calendar_revision": calendar_identity["calendar_revision"],
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
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "formal_rotation_state": "NOT_EVALUATED",
    }


__all__ = ["SourceSectorPulseFollowupError", "run_source_sector_pulse_followup_day"]
