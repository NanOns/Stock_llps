"""LAUNCH正式准入纯投影与已存事实的用户解释。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from fractions import Fraction
from typing import Any

from stock_a.domain import FieldValue, unknown
from stock_a.research.admission import SCENARIOS, admit


def _raw_field(state: object, reason: str) -> FieldValue:
    if state == "TRUE":
        return FieldValue(True)
    if state == "FALSE":
        return FieldValue(False)
    return unknown(reason)


def _unique(values: Sequence[object]) -> list[str]:
    return sorted({value for value in values if isinstance(value, str) and value})


def prepare_source_d0_launch_formal_admission(
    *,
    entity_id: str,
    target_session: date,
    source_capture_id: int,
    raw_fact_id: int,
    raw_state: str,
    raw_reason: str,
    raw_role: str,
    gates: Mapping[str, FieldValue],
    gate_reasons: Mapping[str, Any],
    close: FieldValue,
    phc20: FieldValue,
    session_ordinal: int,
    next_session: date | None,
    window_start: date,
    evidence_ref: str,
) -> dict[str, Any]:
    """将闭合的已存D0原始事实送入R1纯核，诊断Reader语义不变。"""
    gate_order = (
        "research_universe",
        "actual_quote",
        "coordinate_window",
        "d0_liquidity",
        "non_core_damage",
        "non_extreme_extension",
        "non_first_day_damage",
        "non_severe_drop",
    )
    if (
        not entity_id
        or type(target_session) is not date
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(raw_fact_id) is not int
        or raw_fact_id <= 0
        or raw_state not in {"TRUE", "FALSE", "UNKNOWN"}
        or raw_role not in {"FORMAL", "DIAGNOSTIC"}
        or set(gates) != set(gate_order)
        or set(gate_reasons) != set(gate_order)
        or any(
            not (
                isinstance(reason, str)
                and reason
                or isinstance(reason, Mapping)
                and set(reason) == {"TRUE", "FALSE", "UNKNOWN"}
                and all(isinstance(value, str) and value for value in reason.values())
            )
            for reason in gate_reasons.values()
        )
        or type(window_start) is not date
        or window_start > target_session
        or not evidence_ref
    ):
        raise ValueError("LAUNCH正式准入必须绑定精确的原始事实、角色、窗口和来源键")

    raw = _raw_field(raw_state, raw_reason or "RAW_SCENARIO_UNKNOWN")
    effective_role = raw_role
    if raw_state in {"FALSE", "UNKNOWN"}:
        # 无事件候选时保留契约三态；诊断TRUE不会被提升为正式事件。
        effective_role = "FORMAL"
    decision = admit(
        entity_id=entity_id,
        ordinal=session_ordinal,
        next_ordinal=session_ordinal + 1,
        coordinate_id=f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{entity_id}",
        scenario="LAUNCH_CONFIRM",
        role=effective_role,
        raw=raw,
        gates=tuple(gates[key] for key in gate_order),
        close=close,
        phc20=phc20,
        ma5=unknown("LAUNCH_MA5_NOT_APPLICABLE"),
        ma20=unknown("LAUNCH_MA20_NOT_APPLICABLE"),
        r5=unknown("LAUNCH_R5_NOT_APPLICABLE"),
        r20=unknown("LAUNCH_R20_NOT_APPLICABLE"),
        prior_episode_id=None,
        prior_episode_valid=unknown("LAUNCH_PRIOR_EPISODE_NOT_REQUIRED"),
        support_anchor_id=None,
        support_lower=unknown("LAUNCH_SUPPORT_NOT_APPLICABLE"),
        support_valid=unknown("LAUNCH_SUPPORT_NOT_APPLICABLE"),
        evidence_ref=evidence_ref,
        window_ref=f"{window_start.isoformat()}..{target_session.isoformat()}:PHC20_STRICT_PRIOR_20",
        capability_state="READY",
    )
    status = decision.status
    reason = decision.reason
    if status == "DIAGNOSTIC":
        status = "NOT_READY"
        reason = "RAW_ROLE_DIAGNOSTIC_NOT_FORMAL"

    gate_result: dict[str, dict[str, str]] = {}
    reason_codes = [raw_reason] if raw_reason else []
    for key in gate_order:
        field = gates[key]
        state = (
            "TRUE"
            if field.quality == "OBSERVED" and field.value is True
            else "FALSE"
            if field.quality == "OBSERVED" and field.value is False
            else "UNKNOWN"
        )
        reason_contract = gate_reasons[key]
        gate_reason = field.reason or (
            reason_contract.get(state)
            if isinstance(reason_contract, Mapping)
            else reason_contract
        )
        if raw_state != "TRUE":
            state = "NOT_EVALUATED"
            gate_reason = "RAW_NOT_TRUE"
        gate_result[key] = {"state": state, "reason": gate_reason}
        if raw_state == "TRUE" and state != "TRUE":
            reason_codes.append(gate_reason)
    if status == "NOT_READY" and reason == "RAW_ROLE_DIAGNOSTIC_NOT_FORMAL":
        reason_codes.append(reason)
    elif status in {"BLOCKED", "NOT_READY"}:
        reason_codes.append(reason)
    if status == "NOT_REQUIRED":
        reason_codes.append("RAW_FALSE_NOT_REQUIRED")
    if status == "NOT_READY" and raw_state == "UNKNOWN":
        reason_codes.append("RAW_UNKNOWN")

    event = decision.event if status == "READY" else None
    line_value = Fraction(event.failure_line.value) if event is not None else None
    frozen_line = (
        {
            "numerator": line_value.numerator,
            "denominator": line_value.denominator,
            "coordinate": "FORWARD_ADJUSTED",
            "source": "T0_PHC20",
            "source_window_start": window_start.isoformat(),
            "source_window_session_count": 20,
            "source_event_ref": evidence_ref,
            "effective_from_session": next_session.isoformat()
            if next_session is not None
            else None,
        }
        if line_value is not None and next_session is not None
        else None
    )
    if status == "READY" and frozen_line is None:
        raise ValueError("READY事件缺少下一官方会话，不能生成出生冻结线")
    return {
        "raw_role": raw_role,
        "raw_state": raw_state,
        "raw_reason": raw_reason,
        "admission_status": status,
        "admission_reason": reason,
        "formal_state": "TRUE"
        if status == "READY"
        else "FALSE"
        if status in {"BLOCKED", "NOT_REQUIRED"}
        else "UNKNOWN",
        "gates": gate_result,
        "reason_codes": _unique(reason_codes),
        "event_id": event.event_id if event is not None else None,
        "frozen_line": frozen_line,
        "raw_fact_id": raw_fact_id,
        "effective_from_session": next_session.isoformat()
        if frozen_line is not None and next_session is not None
        else None,
        "formal_episode_status": "NOT_CREATED",
        "d2_status": "NOT_IMPLEMENTED",
        "identity_scope": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
    }


def present_source_formal_admission(
    *,
    stored_day: Mapping[str, Any],
    source_symbol: str,
    target_session: date,
    source_capture_id: int,
    d0_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """将PG完成事实或明确未计算状态投影给Focus和单股解释器。"""
    if (
        not isinstance(source_symbol, str)
        or type(target_session) is not date
        or type(source_capture_id) is not int
    ):
        raise ValueError("准入页面投影必须绑定来源代码、日期、来源包和捕获")
    stored = (stored_day.get("by_symbol") or {}).get(source_symbol)
    d0_scenarios = (d0_evidence or {}).get("scenarios") or {}
    launch_raw = d0_scenarios.get("LAUNCH_CONFIRM") or {}
    other: dict[str, Any] = {}
    for scenario in SCENARIOS:
        if scenario == "LAUNCH_CONFIRM":
            continue
        raw_fact = d0_scenarios.get(scenario) or {}
        other[scenario] = {
            "raw_status": "AVAILABLE"
            if raw_fact.get("scenario_raw") in {"TRUE", "FALSE", "UNKNOWN"}
            else "NOT_COMPUTED",
            "raw_state": raw_fact.get("scenario_raw", "NOT_COMPUTED"),
            "formal_state": "NOT_IMPLEMENTED",
            "admission_status": "NOT_IMPLEMENTED",
            "message_zh": "该场景本批未实现正式准入。",
            "reason_code": "FORMAL_SCENARIO_NOT_IMPLEMENTED_IN_SCOPE",
            "reason_codes": ["FORMAL_SCENARIO_NOT_IMPLEMENTED_IN_SCOPE"],
            "event_status": "NOT_CREATED",
            "frozen_line_status": "NOT_CREATED",
            "effective_from_session": None,
        }
    if stored is None:
        raw_state = launch_raw.get("scenario_raw", "NOT_COMPUTED")
        missing_reason = stored_day.get("reason_code") or (
            "FORMAL_ADMISSION_SOURCE_SYMBOL_NOT_IN_PARTITION"
            if stored_day.get("status") == "AVAILABLE"
            else "FORMAL_ADMISSION_NOT_COMPUTED_FOR_EXACT_INPUT"
        )
        launch = {
            "raw_status": "AVAILABLE"
            if raw_state in {"TRUE", "FALSE", "UNKNOWN"}
            else "NOT_COMPUTED",
            "raw_state": raw_state,
            "formal_state": "NOT_COMPUTED",
            "admission_status": "NOT_COMPUTED",
            "message_zh": "LAUNCH正式准入typed事实未对该精确输入完成生产。",
            "reason_code": missing_reason,
            "reason_codes": [missing_reason],
            "raw_reason_codes": [launch_raw.get("main_reason")]
            if launch_raw.get("main_reason")
            else [],
            "event_status": "NOT_CREATED",
            "frozen_line_status": "NOT_CREATED",
            "effective_from_session": None,
            "frozen_line": None,
        }
    else:
        status = stored["admission_status"]
        formal_state = (
            "TRUE"
            if status == "READY"
            else "FALSE"
            if status in {"BLOCKED", "NOT_REQUIRED"}
            else "UNKNOWN"
        )
        message = {
            "READY": "LAUNCH原始条件与公共准入门均已证实；T0的PHC20冻结线从下一官方会话生效。",
            "BLOCKED": "LAUNCH原始条件为TRUE，但至少一个公共硬门已证FALSE；未创建事件或冻结线。",
            "NOT_READY": "LAUNCH正式准入仍有必需输入未知；未创建事件或冻结线。",
            "NOT_REQUIRED": "LAUNCH原始条件已证FALSE；本场景正式准入为NOT_REQUIRED。",
        }[status]
        launch = {
            **stored,
            "raw_status": "AVAILABLE",
            "raw_state": stored["raw_state"],
            "formal_state": formal_state,
            "message_zh": message,
            "reason_code": stored["admission_reason"],
            "event_status": stored["event_status"],
            "frozen_line_status": "FROZEN"
            if status == "READY"
            else "NOT_REQUIRED"
            if status == "NOT_REQUIRED"
            else "NOT_CREATED",
            "effective_from_session": (stored.get("frozen_line") or {}).get(
                "effective_from_session"
            ),
        }
    counts = stored_day.get("counts")
    total = stored_day.get("source_count")
    return {
        "contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
        "status": stored_day.get("status", "NOT_COMPUTED"),
        "scope": "SOURCE_SYMBOL_FORMAL_ADMISSION",
        "target_session": target_session.isoformat(),
        "source_capture_id": source_capture_id,
        "source_symbol": source_symbol,
        "identity_scope": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "source_role_boundary": {
            "formal_admission_role": "FORMAL",
            "source_d0_raw_reader_role": "DIAGNOSTIC",
        },
        "source_count": total,
        "admission_counts": counts,
        "scenarios": {"LAUNCH_CONFIRM": launch, **other},
        "formal_episode_status": "NOT_IMPLEMENTED",
        "d2_status": "NOT_IMPLEMENTED",
        "risk_status": "NOT_IMPLEMENTED",
        "priority_status": "NOT_IMPLEMENTED",
        "qualification_status": "NOT_A_SECURITY_OR_TRADING_QUALIFICATION",
        "reason_codes": _unique(
            list(launch.get("reason_codes", ()))
            + [code for item in other.values() for code in item.get("reason_codes", ())]
        ),
        "database_write_attempted": False,
    }
