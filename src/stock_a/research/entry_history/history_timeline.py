"""Stock A研究history_timeline职责模块。"""

from __future__ import annotations

from datetime import date, timedelta
from fractions import Fraction
import re
from typing import Any, Iterable, Mapping, Sequence
from stock_a.domain import ContractError, FieldValue, exact_number, unknown

from stock_a.research.entry_core.fields import _render_field
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_history.history_preview import (
    research_stock_pg_history_preview,
)
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_context.source_discovery import _normalize_symbol

_PRICE_RELATION_FIELDS = (
    "core.ma5",
    "core.ma10",
    "core.ma20",
    "core.ma60",
    "core.prior_high20",
)
_PRICE_RELATION_VALUES = frozenset({"BELOW", "EQUAL", "ABOVE"})


def _timeline_relation(
    close_field: Mapping[str, Any], ma20_field: Mapping[str, Any]
) -> tuple[str | None, str | None]:
    """用现役Core输出和RAW收盘值精确判定价格相对MA20位置。"""
    for field in (close_field, ma20_field):
        if field.get("quality") != "OBSERVED":
            return None, field.get("reason") or "PRICE_FIELD_UNKNOWN"
    try:
        close_value = close_field["value"]
        ma20_value = ma20_field["value"]
        close_exact = (
            Fraction(close_value["exact"])
            if isinstance(close_value, Mapping) and "exact" in close_value
            else Fraction(str(close_value))
        )
        ma20_exact = (
            Fraction(ma20_value["exact"])
            if isinstance(ma20_value, Mapping) and "exact" in ma20_value
            else Fraction(str(ma20_value))
        )
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        return None, "PRICE_VALUE_INVALID"
    if close_exact > ma20_exact:
        return "ABOVE", None
    if close_exact < ma20_exact:
        return "BELOW", None
    return "EQUAL", None


def _timeline_raw_ohlc(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """把同一条PG Raw行的四个价格字段投影到轨迹，不推导或修补价格。"""
    field_names = {
        "open": "raw_open",
        "high": "raw_high",
        "low": "raw_low",
        "close": "raw_close",
    }
    if row is None:
        quality = "UNKNOWN"
        reason = "UNKNOWN_HISTORY_SESSION_NOT_STORED"
    elif row.get("bar_status") != "TARGET_PRESENT":
        quality = "UNKNOWN"
        reason = row.get("bar_reason") or row.get("bar_status") or "RAW_BAR_UNKNOWN"
    elif row.get("price_quality") != "OBSERVED":
        quality = str(row.get("price_quality") or "UNKNOWN")
        reason = row.get("price_reason") or "RAW_PRICE_QUALITY_UNKNOWN"
    else:
        try:
            values = {
                name: exact_number(row.get(column))
                for name, column in field_names.items()
            }
        except (TypeError, ValueError, ZeroDivisionError, OverflowError):
            values = {}
        if len(values) != len(field_names) or any(
            value <= 0 for value in values.values()
        ):
            quality = "BAD_VALUE"
            reason = row.get("price_reason") or "RAW_OHLC_INVALID_OR_INCOMPLETE"
        elif values["high"] < max(
            values["open"], values["low"], values["close"]
        ) or values["low"] > min(values["open"], values["high"], values["close"]):
            quality = "BAD_VALUE"
            reason = row.get("price_reason") or "RAW_OHLC_ENVELOPE_INVALID"
        else:
            return {
                name: _render_field(FieldValue(row[column]))
                for name, column in field_names.items()
            } | {"quality": "OBSERVED", "reason": None}
    return {
        name: {"value": None, "quality": quality, "reason": reason}
        for name in field_names
    } | {"quality": quality, "reason": reason}


def post_signal_price_facts(
    sessions: Sequence[date], rows: Sequence[Mapping[str, Any]], requested: int
) -> dict[str, Any]:
    """逐官方会话投影原始价格；缺行不借用相邻会话。"""
    by_date = {row["session_date"]: row for row in rows}
    if len(by_date) != len(rows) or any(day not in sessions for day in by_date):
        raise ResearchEntryError("POST_SIGNAL_ROWS_INVALID", "后续行情会话重复或越界")
    base_row = by_date.get(sessions[0])
    base = _timeline_raw_ohlc(base_row)
    points = []
    for index, session in enumerate(sessions[1:], 1):
        row = by_date.get(session)
        ohlc = _timeline_raw_ohlc(row)
        comparison = {
            "quality": "UNKNOWN",
            "reason": "T0_OR_ENDPOINT_BAR_UNKNOWN",
            "value": None,
        }
        if base["quality"] == ohlc["quality"] == "OBSERVED":
            if (
                base_row.get("price_coordinate") != "RAW_UNADJUSTED"
                or row.get("price_coordinate") != "RAW_UNADJUSTED"
            ):
                comparison["reason"] = "PRICE_COORDINATE_MISMATCH"
            else:
                value = (
                    exact_number(row["raw_close"]) / exact_number(base_row["raw_close"])
                    - 1
                )
                comparison = _render_field(FieldValue(value))
        points.append(
            {
                "offset": index,
                "session": session.isoformat(),
                "bar_status": row.get("bar_status")
                if row
                else "HISTORY_ROW_NOT_STORED",
                "ohlc": ohlc,
                "raw_close_relative_change": comparison,
            }
        )
    available = len(points)
    valid_bars = sum(point["ohlc"]["quality"] == "OBSERVED" for point in points)
    for index in range(available + 1, requested + 1):
        reason = "NO_FUTURE_SESSION_IN_SAVED_WINDOW"
        unavailable = {"value": None, "quality": "NOT_AVAILABLE", "reason": reason}
        points.append(
            {
                "offset": index,
                "session": None,
                "bar_status": "NOT_AVAILABLE",
                "ohlc": {
                    key: dict(unavailable) for key in ("open", "high", "low", "close")
                }
                | {"quality": "NOT_AVAILABLE", "reason": reason},
                "raw_close_relative_change": dict(unavailable),
            }
        )
    return {
        "t0": {"session": sessions[0].isoformat(), "ohlc": base},
        "points": points,
        "requested_session_count": requested,
        "available_session_count": available,
        "not_available_session_count": requested - available,
        "valid_bar_count": valid_bars,
        "unknown_bar_count": available - valid_bars,
        "status": "NO_FUTURE_SESSION_IN_SAVED_WINDOW"
        if not available
        else (
            "TRUNCATED_AT_RESEARCH_CUTOFF"
            if available < requested
            else "PARTIAL_UNKNOWN"
            if valid_bars < available
            else "AVAILABLE"
        ),
        "price_coordinate": "RAW_UNADJUSTED",
        "pit_status": "HISTORICAL_NON_PIT_EXPLORATION",
    }


def _timeline_transition(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    *,
    first_evaluable: bool,
) -> dict[str, Any]:
    """只比较紧邻官方会话；不会跨未知日寻找更早前态。"""
    current_relation = current.get("close_vs_ma20")
    previous_session = previous.get("session_date") if previous else None
    previous_relation = previous.get("close_vs_ma20") if previous else None
    if previous is not None and any(
        previous.get(key) != current.get(key)
        for key in ("coordinate_id", "package_sha256", "semantic_version")
    ):
        return {
            "status": "NOT_COMPARABLE",
            "event": "NOT_COMPARABLE",
            "previous_session": previous_session,
            "previous_relation": previous_relation,
            "relation_transition": None,
            "reason": "PRICE_COORDINATE_PACKAGE_OR_SEMANTIC_VERSION_MISMATCH",
        }
    if current_relation is None:
        return {
            "status": "NOT_COMPARABLE",
            "event": "NOT_COMPARABLE",
            "previous_session": previous_session,
            "previous_relation": previous_relation,
            "relation_transition": None,
            "reason": current.get("relation_unknown_reason")
            or "CURRENT_RELATION_UNKNOWN",
        }
    if previous is None or previous_relation is None:
        return {
            "status": "FIRST_EVALUABLE" if first_evaluable else "NOT_COMPARABLE",
            "event": "FIRST_EVALUABLE" if first_evaluable else "NOT_COMPARABLE",
            "previous_session": previous_session,
            "previous_relation": previous_relation,
            "relation_transition": None,
            "reason": "NO_EVALUABLE_IMMEDIATE_PREVIOUS_SESSION",
        }
    transition = f"{previous_relation}_TO_{current_relation}"
    if previous_relation == current_relation:
        event = f"HOLD_{current_relation}"
    elif previous_relation == "BELOW" and current_relation == "ABOVE":
        event = "CROSS_ABOVE"
    elif previous_relation == "ABOVE" and current_relation == "BELOW":
        event = "CROSS_BELOW"
    elif current_relation == "EQUAL":
        event = f"ENTER_EQUAL_FROM_{previous_relation}"
    else:
        event = f"LEAVE_EQUAL_TO_{current_relation}"
    return {
        "status": "COMPARABLE",
        "event": event,
        "previous_session": previous_session,
        "previous_relation": previous_relation,
        "relation_transition": transition,
        "reason": None,
    }


def _timeline_relation_evidence(
    point: Mapping[str, Any], field_id: str
) -> tuple[str | None, list[dict[str, str]]]:
    """从已生成point提取单字段关系，并保留字段级不可比原因。"""
    issues: list[dict[str, str]] = []
    raw_status = point.get("raw_status")
    if point.get("raw_row_present") is not True:
        code = str(raw_status or "HISTORY_ROW_NOT_STORED")
        messages = {
            "HISTORY_ROW_NOT_STORED": "该官方会话的历史Raw行未保存",
            "TARGET_MISSING": "存在PG Raw行，但来源数据中缺少目标会话Bar",
            "READ_FAILED": "存在PG Raw行，但目标会话来源读取失败",
        }
        issues.append(
            {
                "code": code,
                "message": messages.get(
                    code, f"该官方会话没有可用的历史Raw行（{code}）"
                ),
            }
        )
    elif raw_status != "TARGET_PRESENT":
        code = str(raw_status or "RAW_BAR_UNKNOWN")
        issues.append(
            {
                "code": code,
                "message": (
                    f"存在PG Raw行，但该官方会话Bar状态为{code}"
                    + (
                        f"；来源原因：{point.get('price_reason')}"
                        if point.get("price_reason")
                        else ""
                    )
                ),
            }
        )
    if point.get("price_quality") != "OBSERVED":
        code = str(point.get("price_quality") or "PRICE_QUALITY_UNKNOWN")
        issues.append(
            {
                "code": code,
                "message": (
                    f"该会话价格质量为{code}，未达到OBSERVED"
                    + (
                        f"；来源原因：{point.get('price_reason')}"
                        if point.get("price_reason")
                        else ""
                    )
                ),
            }
        )
    close_field = point.get("raw_close")
    if not isinstance(close_field, Mapping) or close_field.get("quality") != "OBSERVED":
        code = (
            str(close_field.get("quality") or "RAW_CLOSE_UNKNOWN")
            if isinstance(close_field, Mapping)
            else "RAW_CLOSE_MISSING"
        )
        issues.append(
            {
                "code": code,
                "message": (
                    f"收盘字段质量为{code}"
                    + (
                        f"；字段原因：{close_field.get('reason')}"
                        if isinstance(close_field, Mapping)
                        and close_field.get("reason")
                        else ""
                    )
                ),
            }
        )
    raw_ohlc = point.get("raw_ohlc")
    if not isinstance(raw_ohlc, Mapping) or raw_ohlc.get("quality") != "OBSERVED":
        code = (
            str(raw_ohlc.get("quality") or "RAW_OHLC_UNKNOWN")
            if isinstance(raw_ohlc, Mapping)
            else "RAW_OHLC_MISSING"
        )
        issues.append(
            {
                "code": code,
                "message": (
                    f"完整OHLC质量为{code}"
                    + (
                        f"；字段原因：{raw_ohlc.get('reason')}"
                        if isinstance(raw_ohlc, Mapping) and raw_ohlc.get("reason")
                        else ""
                    )
                ),
            }
        )

    core_fields = point.get("core_price_fields")
    threshold = core_fields.get(field_id) if isinstance(core_fields, Mapping) else None
    if not isinstance(threshold, Mapping) or threshold.get("quality") != "OBSERVED":
        code = (
            str(threshold.get("quality") or "CORE_THRESHOLD_UNKNOWN")
            if isinstance(threshold, Mapping)
            else "CORE_THRESHOLD_MISSING"
        )
        issues.append(
            {
                "code": code,
                "message": (
                    f"{field_id}Core阈值质量为{code}"
                    + (
                        f"；Core原因：{threshold.get('reason')}"
                        if isinstance(threshold, Mapping) and threshold.get("reason")
                        else ""
                    )
                ),
            }
        )
    elif threshold.get("value") is None:
        issues.append(
            {"code": "CORE_THRESHOLD_VALUE_MISSING", "message": "Core阈值没有数值"}
        )

    if field_id == "core.prior_high20":
        window = point.get("prior_high20_window")
        relation = point.get("close_vs_prior_high20")
        relation_reason = point.get("prior_high20_relation_unknown_reason")
        if point.get("prior_high20_excludes_target_session") is not True:
            issues.append(
                {
                    "code": "PRIOR_HIGH_TARGET_INCLUDED_OR_UNPROVEN",
                    "message": "prior_high20未证明严格排除目标会话",
                }
            )
        required = 20
    else:
        window = (point.get("moving_average_windows") or {}).get(field_id)
        relation_payload = (point.get("close_vs_ma") or {}).get(field_id)
        if isinstance(relation_payload, Mapping):
            relation = relation_payload.get("relation")
            relation_reason = relation_payload.get("reason")
            if relation_payload.get("quality") != "OBSERVED":
                issues.append(
                    {
                        "code": str(
                            relation_payload.get("quality") or "MA_RELATION_UNKNOWN"
                        ),
                        "message": (
                            f"{field_id}收盘关系未达到OBSERVED"
                            + (
                                f"；字段原因：{relation_reason}"
                                if relation_reason
                                else ""
                            )
                        ),
                    }
                )
        else:
            relation = None
            relation_reason = None
            issues.append(
                {
                    "code": "MA_RELATION_MISSING",
                    "message": f"没有{field_id}的收盘关系",
                }
            )
        required = int(field_id.removeprefix("core.ma"))

    if not isinstance(window, Mapping):
        issues.append(
            {
                "code": "CORE_WINDOW_MISSING",
                "message": f"没有{field_id}的实际官方会话窗口",
            }
        )
    else:
        window_count = window.get("official_session_count")
        valid_count = window.get("valid_bar_count")
        required_count = window.get("required_valid_bar_count")
        if (
            required_count != required
            or window_count != required
            or valid_count != required
        ):
            issues.append(
                {
                    "code": "CORE_WINDOW_INCOMPLETE",
                    "message": (
                        f"{field_id}實際窗口為{valid_count}/{required_count}個有效Bar、"
                        f"{window_count}個官方會話；需要完整{required}會話"
                    ),
                }
            )
    if relation not in _PRICE_RELATION_VALUES:
        code = "RELATION_VALUE_INVALID" if relation is not None else "RELATION_UNKNOWN"
        issues.append(
            {
                "code": code,
                "message": (
                    f"{field_id}收盘关系为UNKNOWN"
                    + (f"；字段原因：{relation_reason}" if relation_reason else "")
                ),
            }
        )
    return relation if relation in _PRICE_RELATION_VALUES else None, issues


def _timeline_value_evidence(field: Any) -> dict[str, Any]:
    if not isinstance(field, Mapping):
        return {"value": None, "quality": "UNKNOWN", "reason": "FIELD_MISSING"}
    return {
        "value": field.get("value"),
        "quality": field.get("quality") or "UNKNOWN",
        "reason": field.get("reason"),
    }


def _timeline_price_relation_history(
    points: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """只消费已投影的Core与关系字段，按相邻官方会话生成价格变化观察。"""
    statistics = {
        field_id: {
            "comparison_count": 0,
            "comparable_count": 0,
            "change_count": 0,
            "no_relation_change_count": 0,
            "not_comparable_count": 0,
            "reason_counts": {},
        }
        for field_id in _PRICE_RELATION_FIELDS
    }
    observations: list[dict[str, Any]] = []
    pair_statuses: list[list[str]] = []
    identity_keys = (
        "source_symbol",
        "namespace_id",
        "package_sha256",
        "coordinate_id",
        "producer",
        "semantic_version",
    )
    side_labels = {"BEFORE": "前一官方会话", "AFTER": "当日", "PAIR": "前后身份"}
    for previous, current in zip(points, points[1:], strict=False):
        pair_status: list[str] = []
        previous_session = str(previous.get("session_date") or "")
        current_session = str(current.get("session_date") or "")
        for field_id in _PRICE_RELATION_FIELDS:
            before_relation, before_issues = _timeline_relation_evidence(
                previous, field_id
            )
            after_relation, after_issues = _timeline_relation_evidence(
                current, field_id
            )
            issues: list[dict[str, str]] = []
            for key in identity_keys:
                if previous.get(key) != current.get(key):
                    issues.append(
                        {
                            "side": "PAIR",
                            "code": f"{key.upper()}_MISMATCH",
                            "message": f"前后会话的{key}不一致",
                        }
                    )
            issues.extend({"side": "BEFORE", **issue} for issue in before_issues)
            issues.extend({"side": "AFTER", **issue} for issue in after_issues)
            if issues:
                status = "NOT_COMPARABLE"
                event_kind = None
                summary = "；".join(
                    f"{side_labels[item['side']]}：{item['message']}（{item['code']}）"
                    for item in issues
                )
            elif before_relation == after_relation:
                status = "NO_RELATION_CHANGE"
                event_kind = None
                summary = None
            else:
                status = "RELATION_CHANGED"
                if "EQUAL" in (before_relation, after_relation):
                    event_kind = "EQUAL_BOUNDARY"
                elif field_id == "core.prior_high20":
                    event_kind = "RELATIVE_PRIOR_HIGH_POSITION_CHANGED"
                elif before_relation == "BELOW" and after_relation == "ABOVE":
                    event_kind = "STRICT_CROSS_UP"
                elif before_relation == "ABOVE" and after_relation == "BELOW":
                    event_kind = "STRICT_CROSS_DOWN"
                else:
                    status = "NOT_COMPARABLE"
                    event_kind = None
                    issues.append(
                        {
                            "side": "PAIR",
                            "code": "RELATION_TRANSITION_INVALID",
                            "message": "前后关系组合不属于允许的严格跨越或等值边界变化",
                        }
                    )
                    summary = "；".join(
                        f"{side_labels[item['side']]}："
                        f"{item['message']}（{item['code']}）"
                        for item in issues
                    )
                if status == "RELATION_CHANGED":
                    summary = None

            stats = statistics[field_id]
            stats["comparison_count"] += 1
            if status == "NOT_COMPARABLE":
                stats["not_comparable_count"] += 1
                for item in issues:
                    code = item["code"]
                    stats["reason_counts"][code] = (
                        stats["reason_counts"].get(code, 0) + 1
                    )
            else:
                stats["comparable_count"] += 1
                if status == "NO_RELATION_CHANGE":
                    stats["no_relation_change_count"] += 1
                else:
                    stats["change_count"] += 1

            core_fields_before = previous.get("core_price_fields") or {}
            core_fields_after = current.get("core_price_fields") or {}
            observation = {
                "observation_key": f"{current_session}|{field_id}",
                "session_date": current_session,
                "previous_official_session": previous_session,
                "field_id": field_id,
                "comparison_status": status,
                "event_kind": event_kind,
                "before_relation": before_relation,
                "after_relation": after_relation,
                "before_close": _timeline_value_evidence(previous.get("raw_close")),
                "before_threshold": _timeline_value_evidence(
                    core_fields_before.get(field_id)
                ),
                "after_close": _timeline_value_evidence(current.get("raw_close")),
                "after_threshold": _timeline_value_evidence(
                    core_fields_after.get(field_id)
                ),
                "reason_codes": [item["code"] for item in issues],
                "reason_summary": summary,
                "coordinate_id": current.get("coordinate_id"),
                "producer": current.get("producer"),
                "semantic_version": current.get("semantic_version"),
                "before_source_capture_id": previous.get("source_capture_id"),
                "after_source_capture_id": current.get("source_capture_id"),
            }
            observations.append(observation)
            pair_status.append(status)
        pair_statuses.append(pair_status)

    observations.sort(key=lambda item: item["session_date"], reverse=True)
    events = [
        {
            **observation,
            "event_key": (
                f"{observation['session_date']}|{observation['field_id']}|"
                f"{observation['event_kind']}"
            ),
        }
        for observation in observations
        if observation["comparison_status"] == "RELATION_CHANGED"
    ]
    comparison_count = sum(item["comparison_count"] for item in statistics.values())
    not_comparable_count = sum(
        item["not_comparable_count"] for item in statistics.values()
    )
    comparable_pair_count = sum(
        any(status != "NOT_COMPARABLE" for status in statuses)
        for statuses in pair_statuses
    )
    fully_comparable_pair_count = sum(
        all(status != "NOT_COMPARABLE" for status in statuses)
        for statuses in pair_statuses
    )
    return {
        "contract": "STOCK_A_PG_PRICE_RELATION_HISTORY_R1",
        "semantic_version": "PRICE_RELATION_EVENT_HISTORY_R1",
        "producer": "stock_a.research.entry:_timeline_price_relation_history",
        "official_session_count": len(points),
        "official_session_pair_count": max(0, len(points) - 1),
        "comparable_session_pair_count": comparable_pair_count,
        "fully_comparable_session_pair_count": fully_comparable_pair_count,
        "comparison_count": comparison_count,
        "comparable_comparison_count": comparison_count - not_comparable_count,
        "not_comparable_comparison_count": not_comparable_count,
        "no_relation_change_count": sum(
            item["no_relation_change_count"] for item in statistics.values()
        ),
        "event_count": len(events),
        "unknown_count": not_comparable_count,
        "field_statistics": statistics,
        "observations": observations,
        "events": events,
    }


def _timeline_ma20_direction_value(
    point: Mapping[str, Any],
) -> tuple[Fraction | None, dict[str, Any]]:
    field = point.get("core_ma20")
    if not isinstance(field, Mapping):
        return None, {
            "value": None,
            "quality": "UNKNOWN",
            "reason": "CORE_MA20_FIELD_MISSING",
            "window": point.get("ma20_window"),
        }
    evidence = {
        "value": field.get("value"),
        "quality": field.get("quality") or "UNKNOWN",
        "reason": field.get("reason"),
        "window": point.get("ma20_window"),
    }
    if evidence["quality"] != "OBSERVED":
        evidence["reason"] = evidence["reason"] or "CORE_MA20_NOT_OBSERVED"
        return None, evidence
    raw_value = evidence["value"]
    if isinstance(raw_value, Mapping):
        raw_value = raw_value.get("exact", raw_value.get("value"))
    try:
        value = Fraction(str(raw_value))
    except (TypeError, ValueError, ZeroDivisionError, OverflowError):
        evidence["quality"] = "BAD_VALUE"
        evidence["reason"] = "CORE_MA20_VALUE_INVALID"
        return None, evidence
    if value <= 0:
        evidence["quality"] = "BAD_VALUE"
        evidence["reason"] = "CORE_MA20_VALUE_NOT_POSITIVE"
        return None, evidence
    return value, evidence


def _timeline_ma20_direction_history(
    points: Sequence[Mapping[str, Any]],
    *,
    target_session: str,
    source_symbol: str,
    namespace_id: str,
    package_sha256: str,
    calendar_id: str,
    calendar_revision: str,
    coordinate_id: str,
) -> dict[str, Any]:
    """只消费同一轨迹已生成的Core MA20点，不重算Core或跨未知会话比较。"""
    identity_fields = (
        "source_symbol",
        "namespace_id",
        "package_sha256",
        "coordinate_id",
        "producer",
        "semantic_version",
    )
    windows: dict[str, Any] = {}
    for requested_count in (5, 10):
        selected = tuple(points[-requested_count:])
        counts = {"UP": 0, "SAME": 0, "DOWN": 0, "UNKNOWN": 0}
        pairs: list[dict[str, Any]] = []
        for previous, current in zip(selected, selected[1:], strict=False):
            reasons: list[dict[str, str]] = []
            for key in identity_fields:
                if previous.get(key) != current.get(key):
                    reasons.append(
                        {"side": "PAIR", "code": "MA20_PAIR_IDENTITY_MISMATCH"}
                    )
                    break
            previous_value, previous_evidence = _timeline_ma20_direction_value(previous)
            current_value, current_evidence = _timeline_ma20_direction_value(current)
            if previous_value is None:
                reasons.append(
                    {
                        "side": "FROM",
                        "code": str(previous_evidence.get("reason") or "MA20_UNKNOWN"),
                    }
                )
            if current_value is None:
                reasons.append(
                    {
                        "side": "TO",
                        "code": str(current_evidence.get("reason") or "MA20_UNKNOWN"),
                    }
                )
            if not reasons:
                if current_value > previous_value:
                    direction = "UP"
                elif current_value < previous_value:
                    direction = "DOWN"
                else:
                    direction = "SAME"
            else:
                direction = "UNKNOWN"
            counts[direction] += 1
            pairs.append(
                {
                    "from_session": previous.get("session_date"),
                    "to_session": current.get("session_date"),
                    "direction": direction,
                    "from_ma20": previous_evidence,
                    "to_ma20": current_evidence,
                    "unknown_reasons": reasons,
                }
            )
        session_count = len(selected)
        windows[str(requested_count)] = {
            "requested_session_count": requested_count,
            "actual_session_count": session_count,
            "start_session": selected[0].get("session_date") if selected else None,
            "end_session": selected[-1].get("session_date") if selected else None,
            "expected_pair_count": max(session_count - 1, 0),
            "pair_count": len(pairs),
            "counts": counts,
            "pairs": pairs,
        }
    return {
        "contract": "STOCK_A_PG_MA20_ADJACENT_SESSION_DIRECTION_R1",
        "semantic_version": "MA20_ADJACENT_OFFICIAL_SESSION_NUMERIC_ORDER_R1",
        "producer": "stock_a.research.entry:_timeline_ma20_direction_history",
        "core_producer": "stock_a.research.core:calculate_core",
        "core_semantic_version": "CORE_PRIMITIVES_FIELDSET_R2",
        "identity_ref": None,
        "binding": {
            "target_session": target_session,
            "source_symbol": source_symbol,
            "namespace_id": namespace_id,
            "package_sha256": package_sha256,
            "calendar_id": calendar_id,
            "calendar_revision": calendar_revision,
            "price_coordinate": "RAW_UNADJUSTED",
            "coordinate_id": coordinate_id,
        },
        "windows": windows,
        "future_session_rows_used": 0,
        "database_write": False,
        "ddl_executed": False,
    }


def research_stock_pg_history_timeline(
    *,
    symbol: str,
    target_session: str,
    package_sha256: str,
    session_dates: Sequence[date],
    history_rows: Sequence[Mapping[str, Any]],
    missing_history_sessions: Sequence[date],
    namespace_id: str,
    profile_contract: dict[str, Any],
) -> dict[str, Any]:
    """按实际官方会话逐日复用PG Core生产路径，形成单股价格轨迹。"""
    target = _parse_target(target_session)
    normalized = _normalize_symbol(symbol)[0]
    sessions = tuple(session_dates)
    rows = tuple(history_rows)
    if (
        not sessions
        or len(sessions) > 75
        or sessions[-1] != target
        or tuple(sorted(set(sessions))) != sessions
        or any(type(day) is not date for day in sessions)
    ):
        raise ResearchEntryError(
            "PG_TIMELINE_WINDOW_INVALID", "价格轨迹须绑定递增官方会话且最多75日"
        )
    if (
        not isinstance(package_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", package_sha256) is None
        or not isinstance(namespace_id, str)
        or not namespace_id
    ):
        raise ResearchEntryError(
            "PG_TIMELINE_BINDING_INVALID", "价格轨迹缺少完整来源包SHA或namespace"
        )
    calendar_payload, _calendar_metadata, calendar_revision, calendar_sessions = (
        _load_calendar(target)
    )
    official_window = tuple(
        day for day in calendar_sessions if sessions[0] <= day <= target
    )
    if sessions != official_window:
        raise ResearchEntryError(
            "PG_TIMELINE_CALENDAR_INVALID",
            "价格轨迹会话没有与版本化官方交易日历闭合",
        )
    row_dates = tuple(row.get("session_date") for row in rows)
    if (
        tuple(sorted(set(row_dates))) != row_dates
        or any(day not in sessions for day in row_dates)
        or any(
            row.get("source_symbol") != normalized
            or row.get("package_sha256") != package_sha256
            or row.get("namespace_id") != namespace_id
            for row in rows
        )
    ):
        raise ResearchEntryError(
            "PG_TIMELINE_HISTORY_INVALID",
            "轨迹Raw行重复、越界或混入其它代码、版本或namespace",
        )
    rows_by_session = {row["session_date"]: row for row in rows}
    missing = tuple(day for day in sessions if day not in rows_by_session)
    if tuple(missing_history_sessions) != missing:
        raise ResearchEntryError(
            "PG_TIMELINE_MISSING_BINDING_INVALID", "Raw缺失会话清单与实际行不一致"
        )

    points: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    price_quality_counts: dict[str, int] = {}
    event_counts: dict[str, int] = {}
    unknown_reason_counts: dict[str, int] = {}
    valid_price_bar_count = 0
    first_evaluable_session = None
    coordinate_id = f"RAW:TDX_SOURCE_SYMBOL:{normalized}"
    ma_field_ids = (
        "core.ma5",
        "core.ma10",
        "core.ma20",
        "core.ma60",
    )
    price_field_ids = (*ma_field_ids, "core.atr20", "core.prior_high20")
    for index, session in enumerate(sessions):
        row = rows_by_session.get(session)
        raw_status = row.get("bar_status") if row else "HISTORY_ROW_NOT_STORED"
        status_counts[raw_status] = status_counts.get(raw_status, 0) + 1
        price_quality = str(row.get("price_quality") or "UNKNOWN") if row else "UNKNOWN"
        price_quality_counts[price_quality] = (
            price_quality_counts.get(price_quality, 0) + 1
        )
        ma20_window = sessions[max(0, index - 19) : index + 1]
        prior_window = sessions[max(0, index - 20) : index]

        def valid_bar(day: date) -> bool:
            candidate = rows_by_session.get(day)
            return bool(
                candidate is not None
                and candidate.get("bar_status") == "TARGET_PRESENT"
                and candidate.get("price_quality") == "OBSERVED"
                and all(
                    candidate.get(name) is not None
                    for name in ("raw_open", "raw_high", "raw_low", "raw_close")
                )
            )

        if row is None:
            raw_close_field = {
                "value": None,
                "quality": "UNKNOWN",
                "reason": "UNKNOWN_HISTORY_SESSION_NOT_STORED",
            }
            ma20_field = {
                "value": None,
                "quality": "UNKNOWN",
                "reason": "UNKNOWN_HISTORY_SESSION_NOT_STORED",
            }
            prior_high_field = dict(ma20_field)
            amount_ratio_field = dict(ma20_field)
            volume_ratio_field = dict(ma20_field)
            core_price_fields = {
                field_id: dict(ma20_field) for field_id in price_field_ids
            }
            close_vs_ma = {
                field_id: {
                    "relation": None,
                    "quality": "UNKNOWN",
                    "reason": "UNKNOWN_HISTORY_SESSION_NOT_STORED",
                }
                for field_id in ma_field_ids
            }
            moving_average_order = {
                "quality": "UNKNOWN",
                "groups": [],
                "unknown_fields": list(ma_field_ids),
                "reason": "UNKNOWN_HISTORY_SESSION_NOT_STORED",
            }
            relation = None
            relation_reason = "UNKNOWN_HISTORY_SESSION_NOT_STORED"
            prior_high_relation = None
            prior_high_relation_reason = "UNKNOWN_HISTORY_SESSION_NOT_STORED"
        else:
            prefix_sessions = sessions[: index + 1]
            prefix_rows = tuple(
                item for item in rows if item["session_date"] <= session
            )
            prefix_missing = tuple(
                day for day in prefix_sessions if day not in rows_by_session
            )
            preview = research_stock_pg_history_preview(
                symbol=normalized,
                target_session=session.isoformat(),
                package_sha256=package_sha256,
                session_dates=prefix_sessions,
                history_rows=prefix_rows,
                missing_history_sessions=prefix_missing,
                namespace_id=namespace_id,
                profile_contract=profile_contract,
                include_profile_preview=False,
            )
            raw_close_field = preview["raw"]["close"]
            core_fields = preview["core"]["fields"]
            core_price_fields = {
                field_id: core_fields[field_id] for field_id in price_field_ids
            }
            close_vs_ma = {}
            for field_id in ma_field_ids:
                ma_relation, ma_reason = _timeline_relation(
                    raw_close_field, core_price_fields[field_id]
                )
                close_vs_ma[field_id] = {
                    "relation": ma_relation,
                    "quality": "OBSERVED" if ma_relation else "UNKNOWN",
                    "reason": ma_reason,
                }
            unknown_ma_fields = [
                field_id
                for field_id in ma_field_ids
                if core_price_fields[field_id].get("quality") != "OBSERVED"
            ]
            if unknown_ma_fields:
                reasons = [
                    core_price_fields[field_id].get("reason")
                    or core_price_fields[field_id].get("quality")
                    or "UNKNOWN_REASON_NOT_PROVIDED"
                    for field_id in unknown_ma_fields
                ]
                moving_average_order = {
                    "quality": "UNKNOWN",
                    "groups": [],
                    "unknown_fields": unknown_ma_fields,
                    "reason": ";".join(dict.fromkeys(reasons)),
                }
            else:
                exact_ma_values = []
                for field_id in ma_field_ids:
                    value = core_price_fields[field_id]["value"]
                    exact = value.get("exact") if isinstance(value, Mapping) else value
                    exact_ma_values.append((field_id, Fraction(str(exact))))
                exact_ma_values.sort(key=lambda item: item[1], reverse=True)
                ma_groups: list[list[str]] = []
                previous_value = None
                for field_id, value in exact_ma_values:
                    if previous_value is None or value != previous_value:
                        ma_groups.append([])
                    ma_groups[-1].append(field_id)
                    previous_value = value
                moving_average_order = {
                    "quality": "OBSERVED",
                    "groups": ma_groups,
                    "unknown_fields": [],
                    "reason": None,
                }
            ma20_field = core_fields["core.ma20"]
            prior_high_field = core_fields["core.prior_high20"]
            amount_ratio_field = core_fields["core.amount_ratio20"]
            volume_ratio_field = core_fields["core.volume_ratio20"]
            relation, relation_reason = _timeline_relation(raw_close_field, ma20_field)
            prior_high_relation, prior_high_relation_reason = _timeline_relation(
                raw_close_field, prior_high_field
            )
            if valid_bar(session):
                valid_price_bar_count += 1
        if relation is None:
            if row is not None and raw_status != "TARGET_PRESENT":
                relation_reason = row.get("bar_reason") or raw_status
            elif row is not None and price_quality != "OBSERVED":
                relation_reason = row.get("price_reason") or price_quality
            unknown_reason = relation_reason or "PRICE_RELATION_UNKNOWN"
            unknown_reason_counts[unknown_reason] = (
                unknown_reason_counts.get(unknown_reason, 0) + 1
            )
        else:
            unknown_reason = None
            if first_evaluable_session is None:
                first_evaluable_session = session.isoformat()
        ma_windows = {}
        for field_id, required in zip(ma_field_ids, (5, 10, 20, 60), strict=True):
            window_sessions = sessions[max(0, index - required + 1) : index + 1]
            ma_windows[field_id] = {
                "official_session_count": len(window_sessions),
                "valid_bar_count": sum(valid_bar(day) for day in window_sessions),
                "required_valid_bar_count": required,
            }
        atr20_sessions = sessions[max(0, index - 20) : index + 1]
        point = {
            "session_date": session.isoformat(),
            "source_symbol": normalized,
            "raw_row_present": row is not None,
            "raw_status": raw_status,
            "price_quality": price_quality,
            "price_reason": row.get("price_reason")
            if row
            else "UNKNOWN_HISTORY_SESSION_NOT_STORED",
            "raw_ohlc": _timeline_raw_ohlc(row),
            "raw_close": raw_close_field,
            "core_price_fields": core_price_fields,
            "close_vs_ma": close_vs_ma,
            "moving_average_order": moving_average_order,
            "core_ma20": ma20_field,
            "core_prior_high20": prior_high_field,
            "core_ma5": core_price_fields["core.ma5"],
            "core_ma10": core_price_fields["core.ma10"],
            "core_ma60": core_price_fields["core.ma60"],
            "core_atr20": core_price_fields["core.atr20"],
            "core_amount_ratio20": amount_ratio_field,
            "core_volume_ratio20": volume_ratio_field,
            "raw_amount_quality": row.get("raw_amount_quality", "UNKNOWN")
            if row
            else "UNKNOWN",
            "raw_volume_quality": row.get("raw_volume_quality", "UNKNOWN")
            if row
            else "UNKNOWN",
            "amount_unit_status": "UNIT_UNVERIFIED" if row else "UNKNOWN",
            "volume_unit_status": "UNIT_UNVERIFIED" if row else "UNKNOWN",
            "close_vs_prior_high20": prior_high_relation,
            "prior_high20_relation_unknown_reason": prior_high_relation_reason,
            "close_vs_ma20": relation,
            "relation_unknown_reason": unknown_reason,
            "ma20_window": {
                "official_session_count": len(ma20_window),
                "valid_bar_count": sum(valid_bar(day) for day in ma20_window),
                "required_valid_bar_count": 20,
            },
            "moving_average_windows": ma_windows,
            "atr20_window": {
                "official_session_count": len(atr20_sessions),
                "valid_bar_count": sum(valid_bar(day) for day in atr20_sessions),
                "required_valid_bar_count": 21,
            },
            "prior_high20_window": {
                "official_session_count": len(prior_window),
                "valid_bar_count": sum(valid_bar(day) for day in prior_window),
                "required_valid_bar_count": 20,
            },
            "prior_high20_excludes_target_session": True,
            "coordinate_id": coordinate_id,
            "namespace_id": namespace_id,
            "package_sha256": package_sha256,
            "producer": "stock_a.research.core:calculate_core",
            "semantic_version": "CORE_PRIMITIVES_FIELDSET_R2",
            "source_capture_id": row.get("source_capture_id") if row else None,
            "source_content_id": row.get("source_content_id") if row else None,
            "source_entry_sha256": row.get("source_entry_sha256") if row else None,
        }
        points.append(point)
        transition = _timeline_transition(
            points[index - 1] if index else None,
            point,
            first_evaluable=relation is not None
            and first_evaluable_session == session.isoformat(),
        )
        point["comparison"] = transition
        event = transition["event"]
        event_counts[event] = event_counts.get(event, 0) + 1

    price_relation_history = _timeline_price_relation_history(points)
    ma20_direction_history = _timeline_ma20_direction_history(
        points,
        target_session=target.isoformat(),
        source_symbol=normalized,
        namespace_id=namespace_id,
        package_sha256=package_sha256,
        calendar_id=calendar_payload["calendar_id"],
        calendar_revision=calendar_revision,
        coordinate_id=coordinate_id,
    )
    known_count = sum(point["close_vs_ma20"] is not None for point in points)
    return {
        "ok": True,
        "contract": "STOCK_A_PG_SOURCE_SYMBOL_PRICE_TIMELINE_R1",
        "status": "RESEARCH_PREVIEW_NON_PIT",
        "research_mode": "NON_PIT_RAW_DIAGNOSTIC",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "formal_selection_status": "NOT_CONNECTED",
        "symbol": normalized,
        "target_session": target.isoformat(),
        "package_sha256": package_sha256,
        "namespace_id": namespace_id,
        "session_start": sessions[0].isoformat(),
        "session_end": sessions[-1].isoformat(),
        "calendar_id": calendar_payload["calendar_id"],
        "calendar_revision": calendar_revision,
        "total_session_count": len(sessions),
        "raw_row_count": len(rows),
        "valid_price_bar_count": valid_price_bar_count,
        "known_count": known_count,
        "unknown_count": len(points) - known_count,
        "not_comparable_count": sum(
            point["comparison"]["status"] == "NOT_COMPARABLE" for point in points
        ),
        "first_evaluable_count": sum(
            point["comparison"]["status"] == "FIRST_EVALUABLE" for point in points
        ),
        "comparable_transition_count": sum(
            point["comparison"]["status"] == "COMPARABLE" for point in points
        ),
        "first_evaluable_session": first_evaluable_session,
        "raw_status_counts": dict(sorted(status_counts.items())),
        "price_quality_counts": dict(sorted(price_quality_counts.items())),
        "event_counts": dict(sorted(event_counts.items())),
        "unknown_reason_counts": dict(sorted(unknown_reason_counts.items())),
        "producer": "stock_a.research.core:calculate_core",
        "semantic_version": "CORE_PRIMITIVES_FIELDSET_R2",
        "points": points,
        "price_relation_history": price_relation_history,
        "ma20_direction_history": ma20_direction_history,
        "future_session_rows_used": 0,
        "database_write": False,
        "ddl_executed": False,
        "network_access": False,
        "event_semantics": "仅紧邻官方会话可比；严格BELOW↔ABOVE为CROSS；等值进入、离开和保持分列",
    }
