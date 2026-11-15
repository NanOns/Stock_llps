"""从已存来源事实评估单股正式D0准入；不创建正式事件或episode。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from stock_a.domain import FieldValue, unknown
from stock_a.research.admission import SCENARIOS, admit


_STORED_SCENARIOS = ("LAUNCH_CONFIRM", "RECOVERY_TURN")


def _raw_field(state: object, reason: str) -> FieldValue:
    if state == "TRUE":
        return FieldValue(True)
    if state == "FALSE":
        return FieldValue(False)
    return unknown(reason)


def _unique(values: Sequence[object]) -> list[str]:
    return sorted({value for value in values if isinstance(value, str) and value})


def _scenario_result(
    *,
    scenario: str,
    fact: Mapping[str, Any] | None,
    evidence: Mapping[str, Any],
    entity_id: str,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    session_ordinal: int,
    next_session: date | None,
) -> dict[str, Any]:
    if scenario not in _STORED_SCENARIOS:
        reason_codes = ["SOURCE_SHAPE_DIAGNOSTIC_ONLY"]
        if scenario == "STRONG_PULLBACK":
            reason_codes.extend(
                ("FORMAL_PRIOR_EPISODE_NOT_PROVED", "FORMAL_SUPPORT_NOT_PROVED")
            )
        else:
            reason_codes.extend(
                ("FORMAL_PRIOR_EPISODE_NOT_PROVED", "FORMAL_LOO_NOT_PROVED")
            )
        return {
            "raw_status": "NOT_COMPUTED",
            "raw_state": "NOT_COMPUTED",
            "formal_state": "NOT_COMPUTED",
            "admission_status": "NOT_COMPUTED",
            "message_zh": "该正式场景没有已存原始评价，保持未计算。",
            "reason_code": "FORMAL_RAW_NOT_PRODUCED",
            "reason_codes": _unique(reason_codes),
            "event_status": "NOT_CREATED",
            "frozen_line_status": "NOT_CREATED",
            "effective_from_session": None,
        }

    fact = fact or {}
    raw_status = fact.get("status", "NOT_COMPUTED")
    raw_state = fact.get("state", "UNKNOWN")
    if raw_status != "AVAILABLE" or raw_state not in {"TRUE", "FALSE", "UNKNOWN"}:
        status = "NOT_COMPUTED" if raw_status == "NOT_COMPUTED" else "UNKNOWN"
        reason_codes = list(fact.get("reason_codes") or fact.get("reasons") or ())
        if not reason_codes:
            reason_codes = [
                "D0_SCENARIO_FACT_NOT_SAVED_FOR_EXACT_INPUT"
                if status == "NOT_COMPUTED"
                else "D0_SCENARIO_SCOPE_NOT_PROVED"
            ]
        return {
            "raw_status": status,
            "raw_state": status,
            "formal_state": status,
            "admission_status": status,
            "message_zh": "原始事实未能按精确来源输入核实，正式准入不作结论。",
            "reason_code": reason_codes[0],
            "reason_codes": _unique(reason_codes),
            "event_status": "NOT_CREATED",
            "frozen_line_status": "NOT_CREATED",
            "effective_from_session": None,
        }

    seed = evidence.get("seed") or {}
    safety = seed.get("safety") or {}
    safety_state = safety.get("state", "UNKNOWN")
    safety_reasons = list(safety.get("reasons") or ())
    blockers: list[str] = []
    if raw_state == "TRUE":
        if safety_state != "TRUE":
            blockers.extend(safety_reasons or ["SEED_SAFETY_NOT_PROVED"])
        blockers.extend(
            (
                "STABLE_SECURITY_ID_UNVERIFIED",
                "DATE_VALID_IDENTITY_LIFECYCLE_EVIDENCE_MISSING",
            )
        )
        if scenario == "LAUNCH_CONFIRM":
            blockers.append("FORMAL_PHC20_FREEZE_INPUT_NOT_BOUND")
        else:
            blockers.extend(
                (
                    "FORMAL_R5_R20_TRIGGER_PRIORITY_NOT_BOUND",
                    "FORMAL_RECOVERY_MA_FAILURE_LINE_NOT_BOUND",
                )
            )

    seed_status = seed.get("status", "NOT_COMPUTED")
    safety_field = _raw_field(
        safety_state,
        "SEED_SAFETY_NOT_PROVED"
        if seed_status == "AVAILABLE"
        else "SEED_SAFETY_FACT_NOT_COMPUTED",
    )
    raw = _raw_field(raw_state, "RAW_SCENARIO_UNKNOWN")
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{entity_id}"
    evidence_ref = (
        f"PG:{target_session.isoformat()}:{source_package_id}:"
        f"{source_capture_id}:{entity_id}:{scenario}"
    )
    window_ref = str(fact.get("history_window_start") or "SAVED_D0_WINDOW")
    decision = admit(
        entity_id=entity_id,
        ordinal=session_ordinal,
        next_ordinal=session_ordinal + 1,
        coordinate_id=coordinate_id,
        scenario=scenario,
        role="FORMAL",
        raw=raw,
        gates=(
            safety_field,
            unknown("STABLE_SECURITY_ID_UNVERIFIED"),
            unknown("DATE_VALID_IDENTITY_LIFECYCLE_EVIDENCE_MISSING"),
        ),
        close=unknown("FORMAL_CLOSE_NOT_BOUND"),
        phc20=unknown("FORMAL_PHC20_FREEZE_INPUT_NOT_BOUND"),
        ma5=unknown("FORMAL_MA5_FREEZE_INPUT_NOT_BOUND"),
        ma20=unknown("FORMAL_MA20_FREEZE_INPUT_NOT_BOUND"),
        r5=unknown("FORMAL_R5_TRIGGER_NOT_BOUND"),
        r20=unknown("FORMAL_R20_TRIGGER_NOT_BOUND"),
        prior_episode_id=None,
        prior_episode_valid=unknown("FORMAL_PRIOR_EPISODE_NOT_PROVED"),
        support_anchor_id=None,
        support_lower=unknown("FORMAL_SUPPORT_LOWER_NOT_BOUND"),
        support_valid=unknown("FORMAL_SUPPORT_NOT_PROVED"),
        evidence_ref=evidence_ref,
        window_ref=window_ref,
        capability_state="READY",
    )
    formal = decision.formal
    if isinstance(formal, FieldValue):
        formal_state = (
            "TRUE"
            if formal.quality == "OBSERVED" and formal.value is True
            else "FALSE"
            if formal.quality == "OBSERVED" and formal.value is False
            else "UNKNOWN"
        )
    else:
        formal_state = str(formal)
    if raw_state != "TRUE":
        blockers = []
    message_zh = (
        "原始场景已证FALSE，本场景正式准入为FALSE/NOT_REQUIRED。"
        if raw_state == "FALSE"
        else "原始场景已评价为UNKNOWN，正式准入保持UNKNOWN。"
        if raw_state == "UNKNOWN"
        else "来源原始条件为TRUE，但必需安全门已证FALSE；正式准入为FALSE，不创建事件。"
        if formal_state == "FALSE"
        else "来源原始条件为TRUE，但正式Safety、身份/生命周期或冻结输入缺证；不创建事件和冻结线。"
    )
    return {
        "raw_status": "AVAILABLE",
        "raw_state": raw_state,
        "raw_reason_codes": _unique(
            [
                f"{predicate.get('predicate_id')}:{predicate.get('reason_code')}"
                for predicate in fact.get("predicate_details", ())
                if isinstance(predicate, Mapping) and predicate.get("state") != "TRUE"
            ]
        ),
        "formal_state": formal_state,
        "admission_status": decision.status,
        "message_zh": message_zh,
        "reason_code": decision.reason,
        "reason_codes": _unique(blockers),
        "event_status": (
            "ADMITTED"
            if decision.event is not None
            else "NOT_REQUIRED"
            if decision.status == "NOT_REQUIRED"
            else "NOT_CREATED"
        ),
        "frozen_line_status": "FROZEN"
        if decision.event is not None
        else "NOT_REQUIRED"
        if decision.status == "NOT_REQUIRED"
        else "NOT_CREATED",
        "effective_from_session": next_session.isoformat()
        if decision.event is not None and next_session is not None
        else None,
    }


def prepare_source_formal_d0_admission(
    *,
    evidence: Mapping[str, Any],
    entity_id: str,
    target_session: date,
    source_package_id: int,
    source_capture_id: int,
    official_sessions: Sequence[date],
) -> dict[str, Any]:
    """从现存D0与Seed行计算单股准入解释，不把诊断提升为正式episode。"""
    sessions = tuple(official_sessions)
    if (
        not isinstance(evidence, Mapping)
        or not isinstance(entity_id, str)
        or not entity_id
        or type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not sessions
        or tuple(sorted(set(sessions))) != sessions
        or target_session not in sessions
    ):
        raise ValueError("正式D0准入解释缺少精确单股、PG来源或官方日历身份")
    ordinal = sessions.index(target_session)
    next_session = sessions[ordinal + 1] if ordinal + 1 < len(sessions) else None
    scenarios = (evidence.get("d0") or {}).get("scenarios") or {}
    scenario_results = {
        scenario: _scenario_result(
            scenario=scenario,
            fact=scenarios.get(scenario),
            evidence=evidence,
            entity_id=entity_id,
            target_session=target_session,
            source_package_id=source_package_id,
            source_capture_id=source_capture_id,
            session_ordinal=ordinal,
            next_session=next_session,
        )
        for scenario in SCENARIOS
    }
    computed = [
        item
        for scenario, item in scenario_results.items()
        if scenario in _STORED_SCENARIOS
    ]
    available = sum(item["raw_status"] == "AVAILABLE" for item in computed)
    return {
        "contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
        "status": "PARTIAL" if available else "NOT_COMPUTED",
        "scope": "SINGLE_SOURCE_SYMBOL_DIAGNOSTIC",
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "source_capture_id": source_capture_id,
        "source_symbol": entity_id,
        "identity_scope": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "scenarios": scenario_results,
        "formal_episode_status": "NOT_COMPUTED",
        "d2_status": "NOT_COMPUTED",
        "risk_status": "NOT_COMPUTED",
        "priority_status": "NOT_COMPUTED",
        "qualification_status": "NOT_COMPUTED",
        "reason_codes": _unique(
            [
                code
                for item in scenario_results.values()
                for code in item.get("reason_codes", ())
            ]
            + [
                "FORMAL_EPISODE_PRODUCER_NOT_CONNECTED",
                "FORMAL_D2_RISK_PRIORITY_NOT_PERSISTED",
            ]
        ),
        "database_write_attempted": False,
        "formal_event_created": False,
    }
