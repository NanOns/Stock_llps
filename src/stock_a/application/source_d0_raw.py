"""来源股票D0启动和恢复原始场景只读诊断。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from fractions import Fraction
import re
import time
from typing import Any, Mapping
from uuid import UUID

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.d0_launch_raw import (
    calculate_d0_launch_confirm_raw,
    calculate_d0_recovery_turn_raw,
    fraction_payload,
)
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.tdx_sector_members import _raw_window_observation
from stock_a.research.core import MarketSessionCalendar, calculate_core
from stock_a.storage.postgres.d0_raw import (
    MAX_SOURCE_SYMBOLS,
    D0RawPgError,
    read_source_d0_raw_window,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_raw_daily_history_stock,
)


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_STATUSES = {"TRUE", "FALSE", "UNKNOWN", "ALL"}
_TRANSITIONS = (
    "FALSE_TO_TRUE",
    "FALSE_TO_FALSE",
    "FALSE_TO_UNKNOWN",
    "TRUE_TO_TRUE",
    "TRUE_TO_FALSE",
    "TRUE_TO_UNKNOWN",
    "UNKNOWN_TO_TRUE",
    "UNKNOWN_TO_FALSE",
    "UNKNOWN_TO_UNKNOWN",
)
_CHANGE_SELECTIONS = {*_TRANSITIONS, "BEFORE_ONLY", "AFTER_ONLY", "ALL"}
_MAX_LIMIT = MAX_SOURCE_SYMBOLS
_MAX_CHANGE_LIMIT = 2 * MAX_SOURCE_SYMBOLS


class SourceD0RawError(ValueError):
    """D0原始场景读取的参数、合同或PG错误。"""

    def __init__(self, code: str, message: str, *, category: str = "UNAVAILABLE"):
        self.code = code
        self.category = category
        super().__init__(message)


def attach_source_d1_prior_reference(
    item: dict[str, Any], discovery: Mapping[str, Any]
) -> None:
    """给D0诊断附上前一会话已观察的来源级D1支撑，不构造正式父事件。"""
    symbol = item["source_symbol"]
    if discovery.get("target_session") != item["target_session"]:
        raise SourceD0RawError("D0_D1_SESSION_MISMATCH", "D0与D1目标会话不一致。")
    previous = discovery["previous_official_session"]
    details = [
        row
        for row in discovery.get("complete_symbol_details", ())
        if row.get("source_symbol") == symbol
    ]
    if len(details) != 1:
        raise SourceD0RawError(
            "D0_D1_SOURCE_SYMBOL_MISMATCH", "D1完整来源池缺少唯一股票。"
        )
    references = []
    for event in details[0]["anchor_events"]:
        observation = event.get("support_observation_before")
        if event["support_anchor_session"] >= item["target_session"] or not isinstance(
            observation, Mapping
        ):
            continue
        if observation.get("session") != previous:
            raise SourceD0RawError(
                "D0_D1_PRIOR_SESSION_MISMATCH", "D1支撑观察不是D0前一官方会话。"
            )
        support = observation.get("support_observation") or {}
        references.append(
            {
                "anchor_session": event["support_anchor_session"],
                "observable_session": previous,
                "state": support.get("state"),
                "quality": support.get("quality"),
                "event_key": event.get("event_key"),
                "birth_predicate": event.get("raw_d1_breakout_predicate"),
                "platform_generation": event.get("platform_generation"),
                "prior_observation": observation,
                "after_observation": event.get("support_observation_after"),
            }
        )
    usable_states = {
        "IDLE",
        "APPROACHING",
        "TESTING",
        "HELD_TENTATIVE",
        "RETESTING",
        "HELD_CONFIRMED",
        "BREACHED_SHALLOW",
        "RECLAIMED",
    }
    usable = [
        ref
        for ref in references
        if ref["quality"] == "OBSERVED" and ref["state"] in usable_states
    ]
    item["d1_prior_structure_reference"] = {
        "state": "TRUE" if usable else "UNKNOWN",
        "reason": "SOURCE_SUPPORT_OBSERVED"
        if usable
        else "NO_PROVEN_UNBROKEN_SOURCE_SUPPORT_OR_LEFT_CENSORED",
        "formal_parent": "NOT_FORMAL_PARENT",
        "references": references,
    }
    for scenario in ("STRONG_PULLBACK", "TREND_CONTINUE"):
        item["other_scenario_partial_predicates"][scenario][
            "source_prior_support_reference"
        ] = {
            "state": "TRUE" if usable else "UNKNOWN",
            "formal_parent": "NOT_FORMAL_PARENT",
        }


def attach_recovery_failure_followup(
    config: Mapping[str, Any],
    item: dict[str, Any],
    *,
    target: date,
    followup: date,
    cutoff: date,
    package_sha256: str,
    namespace_id: str,
) -> None:
    """只观察诊断线出生后有界官方会话，不生成D2状态。"""
    if type(followup) is not date or not target < followup <= cutoff:
        raise SourceD0RawError(
            "D0_FOLLOWUP_DATE_INVALID",
            "后继会话必须晚于出生且不晚于研究截止。",
            category="ARGUMENT",
        )
    calendar = _official_calendar(followup)["sessions"]
    try:
        start, end = calendar.index(target), calendar.index(followup)
    except ValueError as error:
        raise SourceD0RawError(
            "D0_FOLLOWUP_NOT_OFFICIAL_SESSION",
            "出生及后继须为官方会话。",
            category="ARGUMENT",
        ) from error
    sessions = calendar[start : end + 1]
    if not 2 <= len(sessions) <= 11:
        raise SourceD0RawError(
            "D0_FOLLOWUP_WINDOW_INVALID",
            "后继窗口须为1至10个官方会话。",
            category="ARGUMENT",
        )
    try:
        history = read_raw_daily_history_stock(
            config,
            source_symbol=item["source_symbol"],
            target_session=target,
            package_sha256=package_sha256,
            sessions=sessions,
            expected_namespace_id=namespace_id,
            starts_at_target=True,
            research_cutoff=cutoff,
        )
    except RawSourcePgError as error:
        raise SourceD0RawError(error.code, str(error)) from error
    by_date = {row["session_date"]: row for row in history["history_rows"]}
    line_data = item["failure_line_candidate"]["value"]
    line = (
        Fraction(int(line_data["numerator"]), int(line_data["denominator"]))
        if line_data
        else None
    )
    birth = by_date.get(target)
    result = []
    for session in sessions[1:]:
        row = by_date.get(session)
        reason = None
        close = None
        if line is None:
            reason = "FAILURE_LINE_CANDIDATE_UNAVAILABLE"
        elif row is None:
            saved_end = (history.get("saved_session_range") or [None, None])[1]
            reason = (
                "NOT_AVAILABLE_AFTER_SAVED_RANGE"
                if saved_end is not None and session.isoformat() > saved_end
                else "FOLLOWUP_RAW_NOT_STORED"
            )
        elif (
            birth is None
            or not birth.get("source_entry_sha256")
            or not row.get("source_entry_sha256")
            or row.get("source_entry_sha256") != birth.get("source_entry_sha256")
            or row.get("source_content_id") != birth.get("source_content_id")
            or row.get("price_coordinate") != "RAW_UNADJUSTED"
        ):
            reason = "FOLLOWUP_COORDINATE_OR_SOURCE_MISMATCH"
        elif (
            row.get("bar_status") != "TARGET_PRESENT"
            or row.get("price_quality") != "OBSERVED"
        ):
            reason = "FOLLOWUP_PRICE_UNKNOWN"
        else:
            try:
                if any(
                    isinstance(row[key], bool)
                    for key in ("raw_open", "raw_high", "raw_low", "raw_close")
                ):
                    raise ValueError("布尔值不是有效OHLC")
                values = [
                    Fraction(row[key])
                    for key in ("raw_open", "raw_high", "raw_low", "raw_close")
                ]
                opening, high, low, close = values
                if (
                    min(values) <= 0
                    or not low <= min(opening, close)
                    or not high >= max(opening, close)
                ):
                    reason, close = "FOLLOWUP_OHLC_INVALID", None
            except (TypeError, ValueError, ZeroDivisionError):
                reason = "FOLLOWUP_OHLC_INVALID"
        result.append(
            {
                "session": session.isoformat(),
                "close": fraction_payload(close),
                "close_at_or_below_line": None if reason else close <= line,
                "quality": "UNKNOWN" if reason else "OBSERVED",
                "reason": reason,
            }
        )
    item["failure_line_followup"] = {
        "role": "DIAGNOSTIC_ONLY_NOT_D2",
        "observations": result,
    }


def _fraction(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return fraction_payload(Fraction(value))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _iso(value: Any) -> str | None:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return None if value is None else str(value)


def _target_row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "target_bar_status": row.get("bar_status"),
        "target_bar_reason": row.get("bar_reason"),
        "raw_ohlc": {
            key: _fraction(row.get(column))
            for key, column in (
                ("open", "raw_open"),
                ("high", "raw_high"),
                ("low", "raw_low"),
                ("close", "raw_close"),
            )
        },
        "price_quality": row.get("price_quality"),
        "price_reason": row.get("price_reason"),
        "raw_amount": _fraction(row.get("raw_amount")),
        "raw_amount_quality": row.get("raw_amount_quality"),
        "raw_amount_reason": row.get("raw_amount_reason"),
        "amount_unit_status": row.get("amount_unit_status"),
        "raw_volume_quality": row.get("raw_volume_quality"),
        "volume_unit_status": row.get("volume_unit_status"),
        "source_capture_id": str(row.get("source_capture_id")),
        "source_content_id": row.get("source_content_id"),
        "source_entry_sha256": row.get("source_entry_sha256"),
        "source_clock_role": row.get("source_clock_role"),
        "source_available_at": _iso(row.get("source_available_at")),
        "observed_started_at": _iso(row.get("observed_started_at")),
        "observed_completed_at": _iso(row.get("observed_completed_at")),
        "first_source_date": _iso(row.get("first_source_date")),
        "last_source_date": _iso(row.get("last_source_date")),
        "future_source_rows_excluded": row.get("future_source_rows_excluded"),
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "stable_security_id_status": "UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
    }


def _attach_continuation_core(
    item,
    *,
    rows,
    sessions,
    calendar,
    namespace_id,
    source_content_id,
    target_capture_id,
    parser_contract,
):
    """将现役Core斜率和前高投影到延续场景，保留其actual窗口身份。"""
    symbol = item["source_symbol"]
    present = {
        day: row
        for day, row in rows.items()
        if isinstance(day, date) and row is not None
    }
    hashes = {
        row.get("source_entry_sha256")
        for row in present.values()
        if row.get("source_entry_sha256")
    }
    inconsistent = len(hashes) > 1 or any(
        row.get("parser_contract") != parser_contract
        or row.get("source_symbol") != symbol
        or row.get("namespace_id") != namespace_id
        or row.get("source_content_id") != source_content_id
        or row.get("reference_quality") is False
        for row in present.values()
    )
    result = item["other_scenario_partial_predicates"]["TREND_CONTINUE"]
    if inconsistent:
        for key in ("core_slope_positive", "close_above_core_prior_high20"):
            result[key] = {
                "state": "UNKNOWN",
                "quality": "UNKNOWN",
                "reason": "RAW_SOURCE_IDENTITY_MISMATCH",
            }
        return
    by_key = {(symbol, day): row for day, row in present.items()}
    counts = {
        key: {"row_count": 1, "package_count": 1, "capture_count": 1} for key in by_key
    }
    history = tuple(
        _raw_window_observation(
            source_symbol=symbol,
            session=day,
            raw_by_key=by_key,
            revision_counts=counts,
            target_session=sessions[-1],
            target_capture_id=target_capture_id,
        )[0]
        for day in sessions
    )
    core = calculate_core(
        history,
        official_calendar=MarketSessionCalendar(
            calendar["calendar_id"],
            calendar["calendar_revision_chain"],
            tuple(sessions),
        ),
    )
    damage = core["core.price_damage"]
    item["common_gate_diagnostics"]["core_price_damage"] = {
        "state": ("TRUE" if damage.value else "FALSE")
        if damage.quality == "OBSERVED"
        else "UNKNOWN",
        "reason": damage.reason,
        "window_role": "CORE_ACTUAL_WINDOW",
    }
    slope, high = core["core.ma_slope20_atr"], core["core.prior_high20"]
    close = history[-1].close
    slope_value = Fraction(slope.value) if slope.quality == "OBSERVED" else None
    high_value = Fraction(high.value) if high.quality == "OBSERVED" else None
    close_value = Fraction(close.value) if close.quality == "OBSERVED" else None
    for key, value, threshold, formula, reason in (
        (
            "core_slope_positive",
            slope_value,
            Fraction(0),
            "core.ma_slope20_atr>0",
            slope.reason,
        ),
        (
            "close_above_core_prior_high20",
            close_value,
            high_value,
            "C>core.prior_high20",
            high.reason or close.reason,
        ),
    ):
        known = value is not None and threshold is not None
        result[key] = {
            "state": "UNKNOWN"
            if not known
            else "TRUE"
            if value > threshold
            else "FALSE",
            "quality": "OBSERVED" if known else "UNKNOWN",
            "reason": None if known else reason,
            "formula": formula,
            "value": fraction_payload(value),
            "threshold": fraction_payload(threshold),
            "window_role": "CORE_ACTUAL_WINDOW",
        }


def _classify_transition(before_state: str | None, after_state: str | None) -> str:
    if before_state is None and after_state is not None:
        return "AFTER_ONLY"
    if after_state is None and before_state is not None:
        return "BEFORE_ONLY"
    if before_state is None and after_state is None:
        raise SourceD0RawError("D0_CHANGE_EMPTY_PAIR", "变化联结键不能同时缺少前后端。")
    if before_state not in {"TRUE", "FALSE", "UNKNOWN"} or after_state not in {
        "TRUE",
        "FALSE",
        "UNKNOWN",
    }:
        raise SourceD0RawError(
            "D0_CHANGE_STATE_INVALID", "变化联结端点必须是TRUE/FALSE/UNKNOWN。"
        )
    return f"{before_state}_TO_{after_state}"


def _canonical_expected_namespace(value: str | None) -> str | None:
    if value is None:
        return None
    try:
        canonical = str(UUID(value))
    except (ValueError, TypeError, AttributeError) as error:
        raise SourceD0RawError(
            "D0_NAMESPACE_ID_INVALID",
            "expected_namespace_id必须是规范小写UUID。",
            category="ARGUMENT",
        ) from error
    if canonical != value:
        raise SourceD0RawError(
            "D0_NAMESPACE_ID_INVALID",
            "expected_namespace_id必须是规范小写UUID。",
            category="ARGUMENT",
        )
    return canonical


def _official_calendar(target: date) -> dict[str, Any]:
    payload, metadata, revision_chain, sessions = _load_calendar(target)
    year_metadata = next(item for item in metadata if item["year"] == target.year)
    return {
        "calendar_id": payload["calendar_id"],
        "calendar_revision": payload["revision"],
        "calendar_revision_chain": revision_chain,
        "calendar_sha256": year_metadata["resource_sha256"],
        "sessions": sessions,
    }


def query_source_d0_raw_launch(
    config: Mapping[str, Any],
    *,
    scenario: str,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    state: str | None = None,
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
    action: str = "current",
    before_session: date | None = None,
    before_package_sha256: str | None = None,
    transition: str | None = None,
    expected_namespace_id: str | None = None,
    _history_symbol: str | None = None,
) -> dict[str, Any]:
    """读取启动或恢复原始状态，或联结启动相邻官方会话的三态变化。"""
    expected_namespace_id = _canonical_expected_namespace(expected_namespace_id)
    if _history_symbol is not None and (
        action != "current"
        or not isinstance(_history_symbol, str)
        or _SYMBOL.fullmatch(_history_symbol) is None
    ):
        raise SourceD0RawError(
            "D0_SOURCE_SYMBOL_INVALID", "内部历史窗口代码无效。", category="ARGUMENT"
        )
    if action == "changes":
        if scenario != "LAUNCH_CONFIRM":
            raise SourceD0RawError(
                "D0_CHANGES_SCENARIO_UNSUPPORTED",
                "相邻变化目前仅支持LAUNCH_CONFIRM。",
                category="ARGUMENT",
            )
        return _query_source_d0_raw_launch_changes(
            config,
            scenario=scenario,
            before_session=before_session,
            before_package_sha256=before_package_sha256,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            state=state,
            symbol=symbol,
            offset=offset,
            limit=limit,
            transition=transition,
            expected_namespace_id=expected_namespace_id,
        )
    if action != "current":
        raise SourceD0RawError(
            "D0_ACTION_INVALID",
            "动作只允许current或changes。",
            category="ARGUMENT",
        )
    if any(
        value is not None
        for value in (
            before_session,
            before_package_sha256,
            transition,
        )
    ):
        raise SourceD0RawError(
            "D0_CHANGES_ARGUMENTS_REQUIRE_CHANGES_ACTION",
            "相邻变化参数只能与--action changes一起使用。",
            category="ARGUMENT",
        )
    state = "ALL" if state is None else state
    if scenario not in {"LAUNCH_CONFIRM", "RECOVERY_TURN"}:
        raise SourceD0RawError(
            "D0_SCENARIO_NOT_IMPLEMENTED",
            "该诊断场景尚未实现。",
            category="ARGUMENT",
        )
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise SourceD0RawError(
            "D0_DATE_AFTER_CUTOFF",
            "目标官方会话必须不晚于显式research_cutoff。",
            category="ARGUMENT",
        )
    if state not in _STATUSES:
        raise SourceD0RawError(
            "D0_STATE_INVALID",
            "状态只允许TRUE/FALSE/UNKNOWN/ALL。",
            category="ARGUMENT",
        )
    if symbol is not None and _SYMBOL.fullmatch(symbol) is None:
        raise SourceD0RawError(
            "D0_SOURCE_SYMBOL_INVALID",
            "精确来源代码须为SH./SZ.加六位数字。",
            category="ARGUMENT",
        )
    if type(offset) is not int or offset < 0:
        raise SourceD0RawError(
            "D0_OFFSET_INVALID", "分页offset必须为非负整数。", category="ARGUMENT"
        )
    if type(limit) is not int or not 1 <= limit <= _MAX_LIMIT:
        raise SourceD0RawError(
            "D0_LIMIT_INVALID",
            f"分页limit必须为1至{_MAX_LIMIT}之间的整数。",
            category="ARGUMENT",
        )

    try:
        calendar = _official_calendar(target_session)
        target_index = calendar["sessions"].index(target_session)
    except (ResearchEntryError, ValueError) as error:
        raise SourceD0RawError("D0_OFFICIAL_CALENDAR_INVALID", str(error)) from error
    lookback = 24 if scenario == "RECOVERY_TURN" else 20
    sessions = calendar["sessions"][max(0, target_index - lookback) : target_index + 1]
    calendar_identity = {
        "calendar_id": calendar["calendar_id"],
        "calendar_revision": calendar["calendar_revision"],
        "calendar_sha256": calendar["calendar_sha256"],
    }

    process_started = time.perf_counter()
    cpu_started = time.process_time()
    try:
        pg_result = read_source_d0_raw_window(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            sessions=sessions,
            expected_calendar=calendar_identity,
            expected_namespace_id=expected_namespace_id,
        )
    except D0RawPgError as error:
        raise SourceD0RawError(error.code, str(error)) from error
    read_cpu_seconds = time.process_time() - cpu_started

    source_version = pg_result["source_version"]
    rps_facts = {}
    rps_input_error = None
    if scenario == "RECOVERY_TURN":
        try:
            rps = read_source_pool_rps(
                config,
                target_session=target_session,
                research_cutoff=research_cutoff,
                package_sha256=package_sha256,
                expected_namespace_id=pg_result["namespace_id"],
            )
            if set(rps["facts"]) != set(pg_result["source_symbols"]):
                rps_input_error = "D0_RPS_SOURCE_POOL_MISMATCH"
            else:
                rps_facts = rps["facts"]
        except ValueError as error:
            rps_input_error = f"RPS_INPUT_VALUE_ERROR:{error}"
    calculate_started = time.perf_counter()
    calculate_cpu_started = time.process_time()
    results = []
    unknown_reasons: Counter[str] = Counter()
    false_reasons: Counter[str] = Counter()
    state_counts: Counter[str] = Counter()
    for source_symbol in pg_result["source_symbols"]:
        arguments = dict(
            target_session=target_session,
            official_sessions=sessions,
            history_rows_by_session=pg_result["history_rows_by_symbol"][source_symbol],
            source_symbol=source_symbol,
            package_sha256=package_sha256,
            namespace_id=pg_result["namespace_id"],
            source_content_id=source_version["source_content_id"],
            target_capture_id=source_version["source_capture_id"],
            parser_contract=source_version["parser_contract"],
        )
        if scenario == "LAUNCH_CONFIRM":
            item = calculate_d0_launch_confirm_raw(
                **arguments,
                include_history_window=source_symbol in (symbol, _history_symbol),
            )
        else:
            item = calculate_d0_recovery_turn_raw(
                **arguments,
                rps5_delta3=rps_facts.get(source_symbol, {}).get("rps5_delta3_pp"),
                rps20=rps_facts.get(source_symbol, {}).get("rps20"),
                include_prior_ols=source_symbol == symbol,
            )
            item["rps_input_error"] = rps_input_error
        target_row = pg_result["target_rows"][source_symbol]
        item.update(
            {
                "market": target_row.get("market"),
                "board": target_row.get("board"),
                "target": _target_row_payload(target_row),
            }
        )
        state_counts[item["scenario_raw"]] += 1
        if item["scenario_raw"] == "UNKNOWN":
            unknown_reasons[item["main_reason"]] += 1
        elif item["scenario_raw"] == "FALSE":
            false_reasons[item["main_reason"]] += 1
        results.append(item)
    calculate_seconds = time.perf_counter() - calculate_started
    calculate_cpu_seconds = time.process_time() - calculate_cpu_started

    results.sort(key=lambda item: item["source_symbol"])
    selected = [
        item
        for item in results
        if (state == "ALL" or item["scenario_raw"] == state)
        and (symbol is None or item["source_symbol"] == symbol)
    ]
    selected_count = len(selected)
    page = selected[offset : offset + limit]
    if scenario == "RECOVERY_TURN" and symbol is not None:
        for item in page:
            _attach_continuation_core(
                item,
                rows=pg_result["history_rows_by_symbol"][item["source_symbol"]],
                sessions=sessions,
                calendar=calendar,
                namespace_id=pg_result["namespace_id"],
                source_content_id=source_version["source_content_id"],
                target_capture_id=source_version["source_capture_id"],
                parser_contract=source_version["parser_contract"],
            )
    elif scenario == "RECOVERY_TURN":
        for item in page:
            item["other_scenario_partial_predicates"]["TREND_CONTINUE"][
                "core_fields_status"
            ] = "NOT_REQUESTED_SELECT_EXACT_SOURCE_SYMBOL"
    source_count = pg_result["source_count"]
    if sum(state_counts.values()) != source_count:
        raise SourceD0RawError(
            "D0_STATE_PARTITION_OPEN", "TRUE/FALSE/UNKNOWN没有闭合到完整来源池。"
        )

    return {
        "ok": True,
        "contract": "STOCK_A_D0_LAUNCH_CONFIRM_SOURCE_SYMBOL_RAW_R1"
        if scenario == "LAUNCH_CONFIRM"
        else "STOCK_A_D0_RECOVERY_TURN_SOURCE_SYMBOL_RAW_R1",
        "producer_version": "STOCK_A_D0_LAUNCH_RAW_READER_R1"
        if scenario == "LAUNCH_CONFIRM"
        else "STOCK_A_D0_RECOVERY_RAW_READER_R1",
        "algorithm_contract": "ALG-28 + §11.6 D0_SCENARIO_FACTS_R3",
        "scenario": scenario,
        "scenario_role": "DIAGNOSTIC_ONLY",
        "scenario_raw": "PER_SOURCE_SYMBOL_TRUE_FALSE_UNKNOWN",
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "research_universe_status": "UNKNOWN",
        "absolute_liquidity_cny_status": "UNKNOWN_UNIT_UNVERIFIED",
        "turn_status": "UNKNOWN_NOT_PRODUCED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "episode_status": "NOT_ISSUED",
        "other_scenarios": {
            "RECOVERY_TURN": "AVAILABLE_VIA_SEPARATE_REQUEST"
            if scenario == "LAUNCH_CONFIRM"
            else "CURRENT",
            "STRONG_PULLBACK": "NOT_IMPLEMENTED",
            "TREND_CONTINUE": "NOT_IMPLEMENTED",
        },
        "session_contract": "D0_SESSION_R2",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "strict_official_sessions": [session.isoformat() for session in sessions],
        "official_calendar": calendar_identity,
        "namespace_id": pg_result["namespace_id"],
        "source_version": {
            "package_sha256": str(source_version["package_sha256"]).strip(),
            "source_content_id": source_version["source_content_id"],
            "source_capture_id": source_version["source_capture_id"],
            "parser_contract": source_version["parser_contract"],
            "source_raw_digest": pg_result["target_raw_digest"],
            "source_time_quality": source_version["source_time_quality"],
            "source_available_at": _iso(source_version["source_available_at"]),
            "source_clock_role": source_version["source_clock_role"],
            "source_byte_count": source_version["source_byte_count"],
            "capture_expected_row_count": source_version["capture_expected_row_count"],
            **calendar_identity,
        },
        "source_pool": {
            "source_count": source_count,
            "target_present_count": source_version["target_present_count"],
            "target_missing_count": source_version["target_missing_count"],
            "read_failed_count": source_version["read_failed_count"],
            "scenario_state_counts": {
                key: state_counts.get(key, 0) for key in ("TRUE", "FALSE", "UNKNOWN")
            },
            "scenario_state_partition_closed": sum(state_counts.values())
            == source_count,
            "unknown_main_reason_counts": dict(sorted(unknown_reasons.items())),
            "unknown_main_reason_partition_closed": sum(unknown_reasons.values())
            == state_counts.get("UNKNOWN", 0),
            "false_main_reason_counts": dict(sorted(false_reasons.items())),
            "evaluated_before_filter_and_pagination": True,
        },
        "selection": {
            "state": state,
            "symbol": symbol,
            "matched_count": selected_count,
            "offset": offset,
            "limit": limit,
            "page_count": len(page),
            "next_offset": offset + len(page)
            if offset + len(page) < selected_count
            else None,
            "ordering": "source_symbol COLLATE C ASC",
        },
        "results": page,
        "execution": {
            "transaction": pg_result["transaction"],
            "raw_history_batch_reads": pg_result["raw_history_batch_reads"],
            "raw_history_rows_read": pg_result["raw_history_rows_read"],
            "history_grid_cells": pg_result["history_grid_cells"],
            "missing_history_cells": pg_result["missing_history_cells"],
            "pg_source_version_read_seconds": pg_result[
                "pg_source_version_read_seconds"
            ],
            "pg_target_scan_seconds": pg_result["pg_target_scan_seconds"],
            "pg_history_scan_seconds": pg_result["pg_history_scan_seconds"],
            "pg_read_total_seconds": pg_result["pg_read_total_seconds"],
            "pg_and_validation_cpu_seconds": read_cpu_seconds,
            "local_calculation_cpu_seconds": calculate_cpu_seconds,
            "local_calculation_seconds": calculate_seconds,
            "total_wall_seconds": time.perf_counter() - process_started,
            "database_write_attempted": pg_result["database_write_attempted"],
            "ddl_executed": pg_result["ddl_executed"],
            "dml_executed": pg_result["dml_executed"],
            "source_zip_opened": pg_result["source_zip_opened"],
            "source_scan_performed": pg_result["source_scan_performed"],
        },
    }


def _query_source_d0_raw_launch_changes(
    config: Mapping[str, Any],
    *,
    scenario: str,
    before_session: date | None,
    before_package_sha256: str | None,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    state: str | None,
    symbol: str | None,
    offset: int,
    limit: int,
    transition: str | None,
    expected_namespace_id: str | None,
) -> dict[str, Any]:
    if scenario != "LAUNCH_CONFIRM":
        raise SourceD0RawError(
            "D0_SCENARIO_NOT_IMPLEMENTED",
            "本读者只实现LAUNCH_CONFIRM诊断raw谓词；其他场景标记NOT_IMPLEMENTED。",
            category="ARGUMENT",
        )
    if state is not None:
        raise SourceD0RawError(
            "D0_STATE_NOT_ALLOWED_FOR_CHANGES",
            "changes动作不接受单日--state；请使用--transition。",
            category="ARGUMENT",
        )
    if (
        type(before_session) is not date
        or type(target_session) is not date
        or type(research_cutoff) is not date
        or before_session >= target_session
        or target_session > research_cutoff
        or before_session > research_cutoff
    ):
        raise SourceD0RawError(
            "D0_CHANGE_DATE_INVALID",
            "changes需要截止日以内且前后有序的两个官方会话。",
            category="ARGUMENT",
        )
    if (
        not isinstance(before_package_sha256, str)
        or not isinstance(package_sha256, str)
        or before_package_sha256 != package_sha256
    ):
        raise SourceD0RawError(
            "D0_CHANGE_PACKAGE_IDENTITY_MISMATCH",
            "相邻比较两端必须显式绑定同一个完整来源包SHA。",
            category="ARGUMENT",
        )
    if transition is None:
        transition = "ALL"
    if transition not in _CHANGE_SELECTIONS:
        raise SourceD0RawError(
            "D0_TRANSITION_INVALID",
            "变化筛选须为九种三态转移、BEFORE_ONLY、AFTER_ONLY或ALL。",
            category="ARGUMENT",
        )
    if symbol is not None and _SYMBOL.fullmatch(symbol) is None:
        raise SourceD0RawError(
            "D0_SOURCE_SYMBOL_INVALID",
            "精确来源代码须为SH./SZ.加六位数字。",
            category="ARGUMENT",
        )
    if type(offset) is not int or offset < 0:
        raise SourceD0RawError(
            "D0_OFFSET_INVALID", "分页offset必须为非负整数。", category="ARGUMENT"
        )
    if type(limit) is not int or not 1 <= limit <= _MAX_CHANGE_LIMIT:
        raise SourceD0RawError(
            "D0_LIMIT_INVALID",
            f"相邻变化分页limit必须为1至{_MAX_CHANGE_LIMIT}之间的整数。",
            category="ARGUMENT",
        )

    try:
        after_calendar = _official_calendar(target_session)
        before_index = after_calendar["sessions"].index(before_session)
        after_index = after_calendar["sessions"].index(target_session)
    except (ResearchEntryError, ValueError) as error:
        raise SourceD0RawError("D0_OFFICIAL_CALENDAR_INVALID", str(error)) from error
    if after_index != before_index + 1:
        raise SourceD0RawError(
            "D0_SESSIONS_NOT_ADJACENT",
            "前后日期必须是同一版本官方交易日历中的相邻会话。",
            category="ARGUMENT",
        )
    try:
        before_calendar = _official_calendar(before_session)
    except ResearchEntryError as error:
        raise SourceD0RawError("D0_OFFICIAL_CALENDAR_INVALID", str(error)) from error
    before_identity = {
        key: before_calendar[key]
        for key in ("calendar_id", "calendar_revision", "calendar_sha256")
    }
    after_identity = {
        key: after_calendar[key]
        for key in ("calendar_id", "calendar_revision", "calendar_sha256")
    }
    if before_identity != after_identity or after_index != before_index + 1:
        raise SourceD0RawError(
            "D0_SESSIONS_NOT_ADJACENT",
            "前后日期必须是同一版本官方交易日历中的相邻会话。",
            category="ARGUMENT",
        )
    before_sessions = tuple(
        before_calendar["sessions"][max(0, before_index - 20) : before_index + 1]
    )
    after_sessions = tuple(
        after_calendar["sessions"][max(0, after_index - 20) : after_index + 1]
    )
    if len(before_sessions) != 21 or len(after_sessions) != 21:
        raise SourceD0RawError(
            "D0_CHANGE_WINDOW_LEFT_TRUNCATED",
            "相邻变化的两端都必须具备目标会话及前20个官方会话。",
        )

    started = time.perf_counter()
    try:
        before_result = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=before_session,
            research_cutoff=research_cutoff,
            package_sha256=before_package_sha256,
            state="ALL",
            offset=0,
            limit=_MAX_LIMIT,
            expected_namespace_id=expected_namespace_id,
            _history_symbol=symbol,
        )
        after_result = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            state="ALL",
            offset=0,
            limit=_MAX_LIMIT,
            expected_namespace_id=expected_namespace_id,
            _history_symbol=symbol,
        )
    except SourceD0RawError:
        raise

    before_source = before_result["source_version"]
    after_source = after_result["source_version"]
    compatible_identity = (
        before_result["namespace_id"] == after_result["namespace_id"]
        and before_source["package_sha256"] == after_source["package_sha256"]
        and before_source["source_content_id"] == after_source["source_content_id"]
        and before_result["official_calendar"] == after_result["official_calendar"]
        and before_result["session_contract"] == after_result["session_contract"]
        and before_result["algorithm_contract"] == after_result["algorithm_contract"]
        and before_result["producer_version"] == after_result["producer_version"]
    )
    if not compatible_identity:
        raise SourceD0RawError(
            "D0_PAIR_NOT_COMPARABLE",
            "两端命名空间、完整包、来源内容、日历或算法身份不兼容，拒绝计算转移。",
        )

    def indexed_rows(
        result: Mapping[str, Any],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        pool = result["source_pool"]
        rows = result["results"]
        if (
            len(rows) != pool["source_count"]
            or result["selection"]["matched_count"] != pool["source_count"]
            or result["selection"]["offset"] != 0
            or result["selection"]["limit"] < pool["source_count"]
        ):
            raise SourceD0RawError(
                "D0_CHANGE_ENDPOINT_INCOMPLETE",
                "相邻变化端点没有先计算并返回完整来源股票池。",
            )
        indexed: dict[tuple[str, str], dict[str, Any]] = {}
        for item in rows:
            source_symbol = item.get("source_symbol")
            market = item.get("market")
            if (
                not isinstance(source_symbol, str)
                or _SYMBOL.fullmatch(source_symbol) is None
                or market not in {"SH", "SZ"}
                or source_symbol[:2] != market
            ):
                raise SourceD0RawError(
                    "D0_CHANGE_SOURCE_KEY_INVALID",
                    "来源股票变化键必须包含相符的交易所与来源代码。",
                )
            key = (market, source_symbol)
            if key in indexed:
                raise SourceD0RawError(
                    "D0_CHANGE_SOURCE_KEY_DUPLICATED", "端点来源股票联结键重复。"
                )
            indexed[key] = item
        return indexed

    before_rows = indexed_rows(before_result)
    after_rows = indexed_rows(after_result)
    before_keys = set(before_rows)
    after_keys = set(after_rows)
    union_keys = before_keys | after_keys
    counts: Counter[str] = Counter()
    unknown_causes: Counter[str] = Counter()
    changes: list[dict[str, Any]] = []
    for key in sorted(union_keys, key=lambda item: (item[1], item[0])):
        before_item = before_rows.get(key)
        after_item = after_rows.get(key)
        transition_name = _classify_transition(
            None if before_item is None else before_item["scenario_raw"],
            None if after_item is None else after_item["scenario_raw"],
        )
        if before_item is not None and after_item is not None:
            if before_item["scenario_raw"] == "UNKNOWN":
                unknown_causes[
                    str(before_item.get("main_reason") or "UNKNOWN_REASON_UNAVAILABLE")
                ] += 1
            if after_item["scenario_raw"] == "UNKNOWN":
                unknown_causes[
                    str(after_item.get("main_reason") or "UNKNOWN_REASON_UNAVAILABLE")
                ] += 1
        counts[transition_name] += 1
        item = {
            "market": key[0],
            "source_symbol": key[1],
            "transition": transition_name,
            "prior_unknown": before_item is not None
            and before_item["scenario_raw"] == "UNKNOWN",
            "before": before_item,
            "after": after_item,
            "identity_status": "SOURCE_SYMBOL_ONLY",
            "price_coordinate": "RAW_UNADJUSTED",
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "scenario_role": "DIAGNOSTIC_ONLY",
            "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        }
        changes.append(item)

    selected = [
        item
        for item in changes
        if (transition == "ALL" or item["transition"] == transition)
        and (symbol is None or item["source_symbol"] == symbol)
    ]
    matched_count = len(selected)
    page = selected[offset : offset + limit]
    if sum(counts.values()) != len(union_keys):
        raise SourceD0RawError(
            "D0_CHANGE_PARTITION_OPEN", "变化分区没有闭合到来源键并集。"
        )
    comparable_count = len(before_keys & after_keys)
    transition_counts = {name: counts.get(name, 0) for name in _TRANSITIONS}
    if sum(transition_counts.values()) != comparable_count:
        raise SourceD0RawError(
            "D0_CHANGE_COMPARABLE_PARTITION_OPEN",
            "九种三态转移没有闭合到双端可比交集。",
        )

    def endpoint_summary(result: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "target_session": result["target_session"],
            "research_cutoff": result["research_cutoff"],
            "strict_official_sessions": result["strict_official_sessions"],
            "official_calendar": result["official_calendar"],
            "namespace_id": result["namespace_id"],
            "source_version": result["source_version"],
            "source_pool": result["source_pool"],
            "session_contract": result["session_contract"],
            "algorithm_contract": result["algorithm_contract"],
            "scenario_role": result["scenario_role"],
            "formal_admission_status": result["formal_admission_status"],
            "research_universe_status": result["research_universe_status"],
            "absolute_liquidity_cny_status": result["absolute_liquidity_cny_status"],
            "stable_security_id_status": result["stable_security_id_status"],
            "pit_status": result["pit_status"],
            "price_coordinate": "RAW_UNADJUSTED",
            "execution": result["execution"],
        }

    return {
        "ok": True,
        "contract": "STOCK_A_D0_LAUNCH_ADJACENT_TRANSITION_RAW_R1",
        "producer_version": "STOCK_A_D0_LAUNCH_RAW_READER_R1",
        "action": "changes",
        "scenario": scenario,
        "pair_status": "COMPARABLE",
        "transition_semantics": "THREE_VALUED_KLEENE_CROSS_SESSION",
        "scenario_role": "DIAGNOSTIC_ONLY",
        "scenario_raw": "PER_SOURCE_SYMBOL_TRUE_FALSE_UNKNOWN",
        "formal_admission_status": "NOT_AUTHORIZED_SOURCE_SYMBOL_DIAGNOSTIC",
        "research_universe_status": "UNKNOWN",
        "absolute_liquidity_cny_status": "UNKNOWN_UNIT_UNVERIFIED",
        "turn_status": "UNKNOWN_NOT_PRODUCED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "episode_status": "NOT_ISSUED",
        "session_contract": "D0_SESSION_R2",
        "research_cutoff": research_cutoff.isoformat(),
        "price_coordinate": "RAW_UNADJUSTED",
        "entity_identity": "SOURCE_SYMBOL_ONLY",
        "before": endpoint_summary(before_result),
        "after": endpoint_summary(after_result),
        "source_key_set": {
            "key": "market+source_symbol",
            "before_count": len(before_keys),
            "after_count": len(after_keys),
            "comparable_intersection_count": comparable_count,
            "source_union_count": len(union_keys),
            "before_only_count": counts.get("BEFORE_ONLY", 0),
            "after_only_count": counts.get("AFTER_ONLY", 0),
            "pair_not_comparable_count": 0,
            "union_partition_closed": len(before_keys & after_keys)
            + counts.get("BEFORE_ONLY", 0)
            + counts.get("AFTER_ONLY", 0)
            == len(union_keys),
        },
        "transition_counts": transition_counts,
        "missing_endpoint_counts": {
            "BEFORE_ONLY": counts.get("BEFORE_ONLY", 0),
            "AFTER_ONLY": counts.get("AFTER_ONLY", 0),
        },
        "unknown_endpoint_main_reason_counts": dict(sorted(unknown_causes.items())),
        "selection": {
            "transition": transition,
            "symbol": symbol,
            "matched_count": matched_count,
            "offset": offset,
            "limit": limit,
            "page_count": len(page),
            "next_offset": offset + len(page)
            if offset + len(page) < matched_count
            else None,
            "ordering": "source_symbol COLLATE C ASC, market COLLATE C ASC",
            "evaluated_before_filter_and_pagination": True,
        },
        "results": page,
        "execution": {
            "transaction": "2 x REPEATABLE READ READ ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "source_zip_opened": False,
            "source_scan_performed": False,
            "total_wall_seconds": time.perf_counter() - started,
        },
    }


__all__ = ["SourceD0RawError", "query_source_d0_raw_launch"]
