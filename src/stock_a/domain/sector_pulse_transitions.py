"""板块来源级PULSE相邻会话变化的纯状态归类。"""

from __future__ import annotations

_STATES = frozenset({"TRUE", "FALSE", "UNKNOWN"})


def classify_pulse_transition(previous_state: str, target_state: str) -> str:
    """按两端三态归类描述性变化；未计算状态不得作为FALSE传入。"""
    if (
        not isinstance(previous_state, str)
        or previous_state not in _STATES
        or not isinstance(target_state, str)
        or target_state not in _STATES
    ):
        raise ValueError("PULSE_TRANSITION_STATE_INVALID")
    if "UNKNOWN" in (previous_state, target_state):
        return "PULSE_TRANSITION_UNKNOWN"
    return {
        ("FALSE", "TRUE"): "PULSE_APPEARED",
        ("TRUE", "TRUE"): "PULSE_PERSISTED",
        ("TRUE", "FALSE"): "PULSE_CEASED",
        ("FALSE", "FALSE"): "PULSE_NOT_PRESENT_BOTH",
    }[(previous_state, target_state)]


__all__ = ["classify_pulse_transition"]
