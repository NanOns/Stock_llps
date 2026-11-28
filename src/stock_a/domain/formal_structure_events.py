"""原 T0 结构首事件与首退出纯算法核（E01 / §11.9）。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

_EXIT_PRIORITY = ("INVALIDATED", "EXPIRED", "EXITED")


def evaluate_structure_first_events(
    *,
    sessions: Sequence[date],
    day_events_by_session: Mapping[date, Mapping[str, Any]],
    horizon_complete: bool,
) -> dict[str, Any]:
    """按 §11.9 评估 1..N 会话内的结构首事件与首退出：

    - 同日退出优先序：INVALIDATED > EXPIRED > EXITED；
    - 升级等非退出结构事件不计入 FIRST_EXIT，但保留于 ANY 与 FIRST_STRUCTURE_EVENT；
    - 更早存在 UNKNOWN 时，FIRST_EXIT 为 UNKNOWN 且保留已知上界；
    - 窗口未满或缺路径不填 NONE。
    """
    any_events: list[str] = []
    first_struct_type: str | None = None
    first_struct_session: date | None = None
    first_exit_type: str | None = None
    first_exit_session: date | None = None
    earlier_unknown = False
    unknown_reasons: list[str] = []

    for day in sessions:
        info = day_events_by_session.get(day)
        if info is None or info.get("status") == "UNKNOWN":
            earlier_unknown = True
            rsn = info.get("reason") if isinstance(info, Mapping) else None
            unknown_reasons.append(str(rsn or "DAY_STRUCTURE_STATE_UNKNOWN"))
            continue

        day_flags = list(info.get("events") or ())
        for ev in day_flags:
            if ev not in any_events:
                any_events.append(ev)
        if first_struct_type is None and day_flags:
            # 若同日有多事件，优先取退出序或首项
            chosen_struct = next(
                (e for e in _EXIT_PRIORITY if e in day_flags), day_flags[0]
            )
            first_struct_type = chosen_struct
            first_struct_session = day

        if first_exit_type is None:
            for exit_ev in _EXIT_PRIORITY:
                if exit_ev in day_flags:
                    first_exit_type = exit_ev
                    first_exit_session = day
                    break

    if not horizon_complete:
        return {
            "first_exit_type": None,
            "first_exit_session": None,
            "first_exit_quality": "NOT_DUE",
            "known_upper_bound_session": first_exit_session,
            "known_upper_bound_type": first_exit_type,
            "first_structure_event_type": first_struct_type,
            "first_structure_event_session": first_struct_session,
            "any_events": any_events,
            "reason_codes": ["WINDOW_NOT_DUE_OR_INCOMPLETE"],
        }

    if earlier_unknown and (first_exit_session is None or True):
        # 只要在已知退出之前存在 UNKNOWN（或者没有退出但存在 UNKNOWN），first_exit 即 UNKNOWN 并保上界
        # 注：如果第一个事件发生在 UNKNOWN 之前（即 first_exit_session 确定早于任何 UNKNOWN），则为已知！
        first_unknown_day = next(
            (
                d
                for d in sessions
                if day_events_by_session.get(d) is None
                or day_events_by_session.get(d, {}).get("status") == "UNKNOWN"
            ),
            None,
        )
        if first_exit_session is not None and (
            first_unknown_day is None or first_exit_session < first_unknown_day
        ):
            return {
                "first_exit_type": first_exit_type,
                "first_exit_session": first_exit_session,
                "first_exit_quality": "OBSERVED",
                "known_upper_bound_session": first_exit_session,
                "known_upper_bound_type": first_exit_type,
                "first_structure_event_type": first_struct_type,
                "first_structure_event_session": first_struct_session,
                "any_events": any_events,
                "reason_codes": [],
            }
        return {
            "first_exit_type": "UNKNOWN",
            "first_exit_session": None,
            "first_exit_quality": "UNKNOWN",
            "known_upper_bound_session": first_exit_session,
            "known_upper_bound_type": first_exit_type,
            "first_structure_event_type": (
                first_struct_type
                if first_struct_session is not None
                and (
                    first_unknown_day is None
                    or first_struct_session < first_unknown_day
                )
                else None
            ),
            "first_structure_event_session": (
                first_struct_session
                if first_struct_session is not None
                and (
                    first_unknown_day is None
                    or first_struct_session < first_unknown_day
                )
                else None
            ),
            "any_events": any_events,
            "reason_codes": list(dict.fromkeys(unknown_reasons)),
        }

    return {
        "first_exit_type": first_exit_type or "NONE",
        "first_exit_session": first_exit_session,
        "first_exit_quality": "OBSERVED",
        "known_upper_bound_session": first_exit_session,
        "known_upper_bound_type": first_exit_type,
        "first_structure_event_type": first_struct_type,
        "first_structure_event_session": first_struct_session,
        "any_events": any_events,
        "reason_codes": [],
    }
