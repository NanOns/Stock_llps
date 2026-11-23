"""来源股票D0启动和恢复原始场景只读诊断。"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from datetime import date, datetime
from fractions import Fraction
import re
import time
from typing import Any, Mapping

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.d0_launch_raw import (
    calculate_d0_launch_confirm_raw,
    calculate_d0_recovery_turn_raw,
    fraction_payload,
    saved_d0_predicate_presentation,
)
from stock_a.research.d0_prior_observation import observe_prior_shapes
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.source_pool_seed import source_seed_predicates
from stock_a.application.source_d1_structures import (
    discover_source_d1_prior_support_reference,
)
from stock_a.research.core import MarketSessionCalendar, calculate_core
from stock_a.domain import FieldValue, unknown
from stock_a.research.admission import admit, line_breached
from stock_a.research.facts import Fact
from stock_a.research.risk import compose, priority_status
from stock_a.research.state import CONTRACT as STATE_CONTRACT, Rules, health
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.runtime_config import _CONFIG_ROOT
from stock_a.contracts import load_price_profile_contract
from stock_a.research.entry_history.history_preview import (
    research_stock_pg_history_preview,
)
from stock_a.research.entry_history.history_timeline import post_signal_price_facts
from stock_a.research.entry_history.support import (
    _core_observation_from_pg_price_row,
    trade_status_state_from_pg_row,
)
from stock_a.storage.postgres.d0_raw import (
    MAX_SOURCE_SYMBOLS,
    D0RawPgError,
    read_source_d0_raw_window,
)
from stock_a.storage.postgres.baostock_price_facts import read_baostock_price_history
from stock_a.storage.postgres.source_raw import RawSourcePgError
from stock_a.storage.postgres.baostock_price_facts import (
    read_baostock_price_history_batch,
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


def present_saved_d0_scenario_day(page: Mapping[str, Any]) -> dict[str, Any]:
    """为PG保存谓词补回现役公式和中文解释，不读取Raw或重算状态。"""
    scenario = page.get("scenario")
    results = []
    for source_result in page.get("results", ()):
        result = dict(source_result)
        predicates = {}
        for name, source_predicate in source_result.get("predicates", {}).items():
            predicate = dict(source_predicate)
            predicate.update(
                saved_d0_predicate_presentation(
                    scenario,
                    name,
                    predicate["state"],
                    predicate["reason_code"],
                )
            )
            predicates[name] = predicate
        result["predicates"] = predicates
        results.append(result)
    return {**page, "results": results}


_CHANGE_SELECTIONS = {*_TRANSITIONS, "BEFORE_ONLY", "AFTER_ONLY", "ALL"}
_MAX_LIMIT = MAX_SOURCE_SYMBOLS
_MAX_CHANGE_LIMIT = 2 * MAX_SOURCE_SYMBOLS


class SourceD0RawError(ValueError):
    """D0原始场景读取的参数、合同或PG错误。"""

    def __init__(self, code: str, message: str, *, category: str = "UNAVAILABLE"):
        self.code = code
        self.category = category
        super().__init__(message)


def _rps_unavailability(error: ValueError) -> str:
    """仅把精确来源池缺失视为不可评价，其余RPS错误保持可区分失败。"""
    message = str(error)
    if message == "目标日没有精确来源池":
        return "HISTORICAL_RPS_SOURCE_POOL_EMPTY"
    if message in {
        "来源池分母不完整",
        "来源池分页分母发生变化",
        "RPS端点QFQ分区或证券总体未闭合",
        "RPS端点分页期间分母发生变化",
        "RPS端点代码总体与分页分母不闭合",
        "RPS历史分页期间行数分母发生变化",
        "RPS QFQ历史会话覆盖未完整闭合",
        "RPS历史行数与PG分页分母不一致",
    }:
        raise SourceD0RawError(
            "D0_RPS_SOURCE_POOL_INCOMPLETE",
            f"RPS来源分区未闭合：{message}",
            category="DATA_PARTITION",
        ) from error
    if message == "RPS目标、截止、paired capture或分页上限无效":
        raise SourceD0RawError(
            "D0_RPS_SCOPE_INVALID", message, category="ARGUMENT"
        ) from error
    raise SourceD0RawError(
        "D0_RPS_INPUT_INVALID",
        f"RPS算法合同或来源输入失败：{message}",
        category="DATA_OR_CONTRACT",
    ) from error


def attach_continuation_observation(item):
    """归约来源续强子条件，不把诊断父观察升级为正式父事件。"""
    obs = item["prior_shape_observation"]
    parts = item["other_scenario_partial_predicates"]["TREND_CONTINUE"]
    keys = (
        "ma_order",
        "positive_ret1",
        "clv",
        "rps20_at_least_70",
        "amr_range",
        "core_slope_positive",
        "close_above_core_prior_high20",
        "loo_confirmed_raw",
    )
    states = {key: parts[key]["state"] for key in keys}
    states["diagnostic_prior"] = (
        "TRUE" if obs["diagnostic_parent_candidates"] else "UNKNOWN"
    )
    values = states.values()
    state = (
        "FALSE" if "FALSE" in values else "UNKNOWN" if "UNKNOWN" in values else "TRUE"
    )
    obs["continuation_shape"] = {
        "state": state,
        "components": states,
        "scope": "EXPLORATORY_DIAGNOSTIC_PARENT_NON_PIT",
        "formal_raw": "NOT_EVALUATED_FORMAL_PARENT_UNPROVEN",
    }


def finalize_prior_shape_observation(item):
    """以同一实现归约单股或全池的非正式回踩、续强诊断。"""
    partial = item["other_scenario_partial_predicates"]
    partial["TREND_CONTINUE"]["loo_confirmed_raw"] = {
        "state": "UNKNOWN",
        "quality": "UNKNOWN",
        "reason": "BOARD_LOO_NOT_REQUESTED_OR_FORMAL_INPUT_UNPROVED",
    }
    for candidate in item["prior_shape_observation"]["diagnostic_parent_candidates"]:
        if "predicates" not in candidate:
            continue
        pb = partial["STRONG_PULLBACK"]
        states = [value["state"] for value in candidate["predicates"].values()]
        states.extend(
            pb[key]["state"]
            for key in (
                "prior_ols_slope_positive",
                "prior_ols_r2_at_least_040",
                "close_not_below_previous",
                "clv",
                "no_shadow_rejection",
            )
        )
        alternatives = (pb["reclaim_ma5"]["state"], pb["retest_previous_ma10"]["state"])
        states.append(
            "TRUE"
            if "TRUE" in alternatives
            else "UNKNOWN"
            if "UNKNOWN" in alternatives
            else "FALSE"
        )
        candidate["source_shape_state"] = (
            "FALSE"
            if "FALSE" in states
            else "UNKNOWN"
            if "UNKNOWN" in states
            else "TRUE"
        )
        candidate["source_shape_scope"] = "NON_PIT_DIAGNOSTIC_NOT_FORMAL_RAW"
    attach_continuation_observation(item)


def read_source_shape_cohort(config, day, *, research_cutoff, source_capture_id):
    """对T0 Focus登记代码复用回踩、续强诊断；历史RPS按会话共享。"""
    target = day["session"]
    source = day["scenarios"]["RECOVERY_TURN"]["source_version"]
    source_capture_id = source["source_capture_id"]
    expected = set(day["states"])
    calendar = _official_calendar(target)
    end = calendar["sessions"].index(target)
    sessions = calendar["sessions"][max(0, end - 44) : end + 1]
    short_sessions = sessions[-25:]
    target_rps_error = None
    target_rps = day["seed"].get("rps_read")
    if target_rps is None:
        try:
            target_rps = read_source_pool_rps(
                config,
                target_session=target,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
            )
        except RawSourcePgError as error:
            if error.code != "PG_SOURCE_NO_MATCH":
                raise
            target_rps_error = error.code
        except ValueError as error:
            target_rps_error = _rps_unavailability(error)
        if target_rps_error:
            target_rps = {"facts": {symbol: {} for symbol in expected}}
    if set(target_rps["facts"]) != expected:
        target_rps_error = "D0_SHAPE_RPS_POOL_MISMATCH"
        target_rps = {"facts": {symbol: {} for symbol in expected}}
    rps_cache = {}
    rps_errors = {}
    output = {}
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {
        symbol: [] for symbol in expected
    }
    offset = 0
    expected_rows = None
    while True:
        page = read_baostock_price_history_batch(
            config,
            source_symbols=tuple(sorted(expected)),
            sessions=sessions,
            source_capture_id=source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
            cutoff=research_cutoff,
            offset=offset,
            limit=5000,
        )
        if (
            page.get("source_capture_id") != source_capture_id
            or page.get("price_coordinate") != "FORWARD_ADJUSTED"
        ):
            raise SourceD0RawError(
                "D0_SHAPE_POOL_MISMATCH", "诊断形态没有绑定配对QFQ来源捕获。"
            )
        if expected_rows is None:
            expected_rows = int(page["page"]["total"])
        elif int(page["page"]["total"]) != expected_rows:
            raise SourceD0RawError(
                "D0_SHAPE_POOL_MISMATCH", "QFQ历史分页期间行数分母发生变化。"
            )
        if any(
            item.get("coverage_status") != "COMPLETE"
            for item in page.get("coverage", ())
        ):
            raise SourceD0RawError(
                "D0_SHAPE_HISTORY_INCOMPLETE", "D0诊断历史QFQ坐标请求未闭合。"
            )
        for row in page.get("rows", ()):
            symbol = row["source_symbol"]
            rows_by_symbol.setdefault(symbol, []).append(row)
        next_offset = page["page"].get("next_offset")
        if next_offset is None:
            break
        offset = int(next_offset)

    for symbol in sorted(expected):
        rows = rows_by_symbol.get(symbol, [])
        if symbol not in expected:
            raise SourceD0RawError(
                "D0_SHAPE_POOL_MISMATCH", "QFQ历史含目标来源池外代码。"
            )
        by_date = {row["session_date"]: row for row in rows}
        birth = by_date.get(target)
        if (
            symbol not in expected
            or symbol in output
            or birth is None
            or birth.get("source_capture_id") != source["source_capture_id"]
            or birth.get("qfq_price_coordinate") != "FORWARD_ADJUSTED"
        ):
            raise SourceD0RawError(
                "D0_SHAPE_TARGET_MISMATCH", "诊断形态T0来源捕获不一致。"
            )
        args = {
            "sessions": sessions,
            "rows": rows,
            "symbol": symbol,
            "source_capture_id": source["source_capture_id"],
            "price_coordinate": "FORWARD_ADJUSTED",
        }
        shape = observe_prior_shapes(**args)
        needed = [
            date.fromisoformat(item["session"])
            for item in shape["historical_states"]
            if item.get("recovery_needs_rps")
        ]
        for prior in needed:
            if prior in rps_cache or prior in rps_errors:
                continue
            try:
                rps_cache[prior] = read_source_pool_rps(
                    config,
                    target_session=prior,
                    research_cutoff=research_cutoff,
                    source_capture_id=source_capture_id,
                    page_size=5000,
                )["facts"]
            except RawSourcePgError as error:
                if error.code != "PG_SOURCE_NO_MATCH":
                    raise
                rps_errors[prior] = error.code
            except ValueError as error:
                rps_errors[prior] = _rps_unavailability(error)
        historical_rps = {
            prior: rps_cache[prior].get(symbol, {}).get("rps5_delta3_pp")
            for prior in needed
            if prior in rps_cache
        }
        if historical_rps:
            shape = observe_prior_shapes(**args, historical_rps=historical_rps)
        rps = target_rps["facts"][symbol]
        item = calculate_d0_recovery_turn_raw(
            target_session=target,
            official_sessions=short_sessions,
            history_rows_by_session=by_date,
            source_symbol=symbol,
            source_capture_id=source["source_capture_id"],
            price_coordinate="FORWARD_ADJUSTED",
            rps5_delta3=rps.get("rps5_delta3_pp"),
            rps20=rps.get("rps20"),
            include_prior_ols=True,
        )
        _attach_continuation_core(
            item,
            rows={session: by_date.get(session) for session in short_sessions},
            sessions=short_sessions,
            calendar=calendar,
            source_capture_id=source["source_capture_id"],
            price_coordinate="FORWARD_ADJUSTED",
        )
        item["prior_shape_observation"] = shape
        finalize_prior_shape_observation(item)
        parents = shape["diagnostic_parent_candidates"]
        parent_states = [
            parent.get("source_shape_state", "UNKNOWN") for parent in parents
        ]
        history_unknown = len(shape["historical_states"]) < 20 or any(
            row["launch"] == "UNKNOWN" or row["recovery"] == "UNKNOWN"
            for row in shape["historical_states"]
        )
        partial = item["other_scenario_partial_predicates"]["STRONG_PULLBACK"]
        independent = [
            partial[key]["state"]
            for key in (
                "prior_ols_slope_positive",
                "prior_ols_r2_at_least_040",
                "close_not_below_previous",
                "clv",
                "no_shadow_rejection",
            )
        ]
        alternatives = [
            partial["reclaim_ma5"]["state"],
            partial["retest_previous_ma10"]["state"],
        ]
        independent.append(
            "TRUE"
            if "TRUE" in alternatives
            else "UNKNOWN"
            if "UNKNOWN" in alternatives
            else "FALSE"
        )
        pullback = (
            "TRUE"
            if "TRUE" in parent_states
            else "FALSE"
            if "FALSE" in independent
            else "UNKNOWN"
            if history_unknown or "UNKNOWN" in parent_states
            else "FALSE"
        )
        continuation = shape["continuation_shape"]["state"]
        shape_reasons = []
        if target_rps_error:
            shape_reasons.append(f"T0_RPS:{target_rps_error}")
        if len(shape["historical_states"]) < 20:
            shape_reasons.append("DIAGNOSTIC_PARENT_WINDOW_TRUNCATED")
        for history in shape["historical_states"]:
            if history["launch"] == "UNKNOWN" or history["recovery"] == "UNKNOWN":
                reason = (
                    history.get("reason")
                    or history.get("recovery_reason")
                    or "UNKNOWN_PARENT_STATE"
                )
                shape_reasons.append(f"{history['session']}:{reason}")
        for parent in parents:
            if parent.get("source_shape_state", "UNKNOWN") == "UNKNOWN":
                if parent.get("reason"):
                    shape_reasons.append(
                        f"{parent['birth_session']}:{parent['reason']}"
                    )
                for name, predicate in parent.get("predicates", {}).items():
                    if predicate["state"] == "UNKNOWN":
                        shape_reasons.append(
                            f"{name}:{predicate.get('reason') or 'UNKNOWN'}"
                        )
        for group in ("STRONG_PULLBACK", "TREND_CONTINUE"):
            for name, predicate in item["other_scenario_partial_predicates"][
                group
            ].items():
                if isinstance(predicate, dict) and predicate.get("state") == "UNKNOWN":
                    shape_reasons.append(
                        f"{group}.{name}:{predicate.get('reason') or predicate.get('reason_code') or 'UNKNOWN'}"
                    )
        shape_reasons = list(dict.fromkeys(shape_reasons))
        pullback_false_reasons = [
            f"PREDICATE_FALSE:{key}"
            for key in (
                "prior_ols_slope_positive",
                "prior_ols_r2_at_least_040",
                "close_not_below_previous",
                "clv",
                "no_shadow_rejection",
            )
            if partial[key]["state"] == "FALSE"
        ]
        if all(value == "FALSE" for value in alternatives):
            pullback_false_reasons.append("RECLAIM_AND_RETEST_FALSE")
        if pullback == "FALSE" and not pullback_false_reasons:
            pullback_false_reasons.append("NO_MATCHING_DIAGNOSTIC_PARENT")
        continuation_false_reasons = [
            f"PREDICATE_FALSE:{key}"
            for key, value in shape["continuation_shape"]["components"].items()
            if value == "FALSE"
        ]
        output[symbol] = {
            "STRONG_PULLBACK_DIAGNOSTIC": {
                "state": pullback,
                "reasons": shape_reasons
                if pullback == "UNKNOWN"
                else pullback_false_reasons
                if pullback == "FALSE"
                else [],
            },
            "TREND_CONTINUE_DIAGNOSTIC": {
                "state": continuation,
                "reasons": shape_reasons
                if continuation == "UNKNOWN"
                else continuation_false_reasons
                if continuation == "FALSE"
                else [],
            },
            "matched_parent_count": sum(state == "TRUE" for state in parent_states),
            "parent_count": len(parents),
            "history_unknown": history_unknown,
        }
    if set(output) != expected:
        raise SourceD0RawError("D0_SHAPE_POOL_MISMATCH", "诊断形态来源代码集合不完整。")
    return {
        "facts": output,
        "target_rps_error": target_rps_error,
        "historical_rps_days": [day.isoformat() for day in sorted(rps_cache)],
        "historical_rps_errors": {
            day.isoformat(): reason for day, reason in rps_errors.items()
        },
        "scope": "DIAGNOSTIC_SHAPE_NON_PIT_NOT_FORMAL_D0",
    }


def diagnostic_admission(result, *, target, source_capture_id):
    """应用来源诊断角色，不生成正式事件。"""
    item = result["results"][0]
    symbol = item["source_symbol"]
    source = f"BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}"
    missing = unknown("FORMAL_EPISODE_NOT_ESTABLISHED")
    raw = (
        FieldValue(item["scenario_raw"] == "TRUE")
        if item["scenario_raw"] != "UNKNOWN"
        else unknown("RAW_SCENARIO_UNKNOWN")
    )
    ordinal = _official_calendar(target)["sessions"].index(target)
    return asdict(
        admit(
            entity_id=symbol,
            ordinal=ordinal,
            next_ordinal=ordinal + 1,
            coordinate_id=f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{symbol}",
            scenario=result["scenario"],
            role="DIAGNOSTIC",
            raw=raw,
            gates=(),
            close=missing,
            phc20=missing,
            ma5=missing,
            ma20=missing,
            r5=missing,
            r20=missing,
            prior_episode_id=None,
            prior_episode_valid=missing,
            support_anchor_id=None,
            support_lower=missing,
            support_valid=missing,
            evidence_ref=source,
            window_ref=source,
            capability_state="READY",
        )
    )


def attach_stock_risk_readiness(
    config, result, *, target, cutoff, source_capture_id, rps_read=None
):
    """将同股同日PG历史事实交给现役风险归并，保留正式结构未知。"""
    item = result["results"][0]
    symbol = item["source_symbol"]
    _, sessions, _ = resolve_pg_history_window(target.isoformat(), 75)
    history = read_baostock_price_history(
        config,
        source_symbol=symbol,
        target_session=target,
        sessions=sessions,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        cutoff=cutoff,
    )
    preview = research_stock_pg_history_preview(
        symbol=symbol,
        target_session=target.isoformat(),
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        session_dates=sessions,
        history_rows=history["rows"],
        profile_contract=load_price_profile_contract(_CONFIG_ROOT).document,
    )
    damage_data = preview["core"]["fields"]["core.price_damage"]
    extension_data = next(
        x
        for x in preview["profile_preview"]["components"]
        if x["field_id"] == "profile.extension"
    )

    def field(data):
        if data.get("quality") == "OBSERVED":
            return FieldValue(data["value"])
        return unknown(data.get("reason") or "SOURCE_FIELD_UNKNOWN")

    damage, extension = field(damage_data), field(extension_data)
    missing = unknown("FORMAL_EPISODE_NOT_ESTABLISHED")
    coordinate = (
        f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{symbol}"
    )
    source = f"BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}"
    fact = lambda value: Fact(value, symbol, target, coordinate, source)
    risk = compose(
        entity_id=symbol,
        session=target,
        coordinate_id=coordinate,
        selected_episode_id=None,
        structure_applicability="UNKNOWN",
        support_applicability="UNKNOWN",
        damage=fact(damage),
        extension=fact(extension),
        invalidated=fact(missing),
        shallow_breach=fact(missing),
        capability_state="READY",
    )
    if rps_read is None:
        rps_read = read_source_pool_rps(
            config,
            target_session=target,
            research_cutoff=cutoff,
            source_capture_id=source_capture_id,
        )
    if (
        rps_read["target_session"] != target
        or rps_read["research_cutoff"] != cutoff
        or rps_read["source_capture_id"] != source_capture_id
    ):
        raise ValueError("风险输入RPS与目标来源身份不一致")
    metric_field = rps_read["facts"].get(symbol, {}).get("rps5_delta3_pp") or unknown(
        "RPS5_DELTA3_UNAVAILABLE"
    )
    prior = research_stock_pg_history_preview(
        symbol=symbol,
        target_session=sessions[-2].isoformat(),
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        session_dates=sessions[:-1],
        history_rows=[row for row in history["rows"] if row["session_date"] < target],
        profile_contract=load_price_profile_contract(_CONFIG_ROOT).document,
        include_profile_preview=False,
        allow_missing_target_row=True,
    )
    previous_close = prior["qfq"]["close"]
    previous_ma = prior["core"]["fields"]["core.ma20"]
    current_close = preview["qfq"]["close"]
    current_ma = preview["core"]["fields"]["core.ma20"]
    exact_values = tuple(
        _rendered_exact_fraction(value)
        for value in (previous_close, previous_ma, current_close, current_ma)
    )
    if all(value is not None for value in exact_values):
        (
            previous_close_value,
            previous_ma_value,
            current_close_value,
            current_ma_value,
        ) = exact_values
        crossed = (
            previous_close_value <= previous_ma_value
            and current_close_value > current_ma_value
        )
        cross = {"state": "TRUE" if crossed else "FALSE", "reason": None}
    else:
        cross = {"state": "UNKNOWN", "reason": "PREVIOUS_MA20_CROSS_INPUT_UNKNOWN"}
    seed = source_seed_predicates(preview, metric_field, cross)
    health_field = health(
        damage, extension, metric_field, Rules(STATE_CONTRACT, 2, 10, 3, 3)
    )
    eligibility = unknown("FORMAL_QUALIFICATION_NOT_EVALUATED")
    item["risk_priority_readiness"] = {
        "source": source,
        "coordinate": coordinate,
        "session": target.isoformat(),
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "source_seed": seed,
        "core_damage": damage_data,
        "profile_extension": extension_data,
        "structure_applicability": "UNKNOWN",
        "diagnostic_admission": diagnostic_admission(
            result, target=target, source_capture_id=source_capture_id
        ),
        "risk": asdict(risk),
        "health_input_rps5_delta3_pp": fraction_payload(Fraction(metric_field.value))
        if metric_field.quality == "OBSERVED"
        else None,
        "health_direction": asdict(health_field),
        "health_direction_scope": "SOURCE_INPUTS_ONLY_NOT_FORMAL_EPISODE_HEALTH",
        "formal_episode": "NOT_EVALUATED",
        "formal_eligibility": "UNKNOWN",
        "priority": priority_status(eligibility, risk, other_keys_ready=False),
        "missing": [
            "HISTORICAL_RESEARCH_UNIVERSE",
            "FORMAL_EVENT_CLOCK",
            "CORPORATE_ACTION_COORDINATE",
            "LIQUIDITY_CNY_UNIT",
            "STABLE_SECURITY_ID",
            "FORMAL_EPISODE",
            "FORMAL_ELIGIBILITY",
            "PRIORITY_KEYS",
        ],
    }


def attach_source_d1_prior_reference(
    item: dict[str, Any],
    discovery: Mapping[str, Any] | None = None,
    *,
    source_capture_id: int,
    source_prior: Mapping[str, Any] | None = None,
) -> None:
    """给D0诊断附上前一会话已观察的来源级D1支撑，不构造正式父事件。"""
    symbol = item["source_symbol"]
    if (
        discovery is not None
        and discovery.get("target_session") != item["target_session"]
    ):
        raise SourceD0RawError("D0_D1_SESSION_MISMATCH", "D0与D1目标会话不一致。")
    references = []
    source_prior_references = []
    if discovery is not None:
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
        for event in details[0]["anchor_events"]:
            observation = event.get("support_observation_before")
            if event["support_anchor_session"] >= item[
                "target_session"
            ] or not isinstance(observation, Mapping):
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
    if source_prior is not None:
        target = date.fromisoformat(item["target_session"])
        calendar_sessions = _official_calendar(target)["sessions"]
        target_index = calendar_sessions.index(target)
        expected_prior = (
            calendar_sessions[target_index - 1].isoformat()
            if target_index > 0
            else None
        )
        if source_prior.get("window", {}).get("prior_session") != expected_prior:
            raise SourceD0RawError(
                "D0_D1_PRIOR_SESSION_MISMATCH", "结构支撑参考不是D0前一官方会话。"
            )
        if source_prior.get("source_symbol", symbol) != symbol:
            raise SourceD0RawError(
                "D0_D1_SOURCE_SYMBOL_MISMATCH", "结构支撑参考来源代码与D0不一致。"
            )
        expected_coordinate = (
            f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{symbol}"
        )
        source_prior_identity_reasons = []
        if source_prior.get("identity_status") != "SOURCE_SYMBOL_ONLY":
            source_prior_identity_reasons.append(
                "SOURCE_IDENTITY_NOT_SOURCE_SYMBOL_ONLY"
            )
        if source_prior.get("research_mode") != "NON_PIT_RETROSPECTIVE":
            source_prior_identity_reasons.append("SOURCE_RESEARCH_MODE_NOT_NON_PIT")
        if source_prior.get("price_coordinate") != "FORWARD_ADJUSTED":
            source_prior_identity_reasons.append("SOURCE_PRICE_COORDINATE_NOT_QFQ")
        for identity_field in ("source_capture_id",):
            if source_prior.get(identity_field) is None:
                source_prior_identity_reasons.append(
                    f"SOURCE_{identity_field.upper()}_IDENTITY_MISSING"
                )
            elif source_prior.get(identity_field) != source_capture_id:
                source_prior_identity_reasons.append("SOURCE_PRIOR_CAPTURE_ID_MISMATCH")
        for raw_reference in source_prior.get("references", ()):
            if not isinstance(raw_reference, Mapping):
                continue
            if raw_reference.get("relation") != "SUPPORT" or raw_reference.get(
                "type"
            ) not in {"PIVOT_LOW", "RANGE_FLOOR", "GAP_ZONE"}:
                continue
            reference = dict(raw_reference)
            quality_reasons = set(reference.get("quality_reasons") or ())
            quality_reasons.update(source_prior_identity_reasons)
            if reference.get("source_symbol") != symbol:
                quality_reasons.add("REFERENCE_SOURCE_SYMBOL_MISMATCH")
            if reference.get("coordinate_id") != expected_coordinate:
                quality_reasons.add("REFERENCE_COORDINATE_MISMATCH")
            if reference.get("identity_status") != "SOURCE_SYMBOL_ONLY":
                quality_reasons.add("REFERENCE_IDENTITY_NOT_SOURCE_SYMBOL_ONLY")
            if reference.get("research_mode") != "NON_PIT_RETROSPECTIVE":
                quality_reasons.add("REFERENCE_RESEARCH_MODE_NOT_NON_PIT")
            if reference.get("price_coordinate_status") != "FORWARD_ADJUSTED":
                quality_reasons.add("REFERENCE_PRICE_COORDINATE_NOT_QFQ")
            if reference.get("observable_session") != expected_prior:
                quality_reasons.add("REFERENCE_OBSERVABLE_SESSION_MISMATCH")
            anchor_session = reference.get("anchor_session")
            try:
                anchor_day = date.fromisoformat(anchor_session)
            except (TypeError, ValueError):
                anchor_day = None
                quality_reasons.add("REFERENCE_ANCHOR_SESSION_INVALID")
            if anchor_day is not None and anchor_day >= date.fromisoformat(
                expected_prior
            ):
                reference["comparability"] = "NOT_COMPARABLE"
                quality_reasons.add("ANCHOR_NOT_BORN_BEFORE_PRIOR_SESSION")
            observation_source = reference.get("source")
            birth_source = reference.get("birth_source")
            for label, row, expected_session in (
                ("OBSERVATION", observation_source, expected_prior),
                (
                    "BIRTH",
                    birth_source,
                    anchor_day.isoformat() if anchor_day is not None else None,
                ),
            ):
                if not isinstance(row, Mapping):
                    quality_reasons.add(f"{label}_SOURCE_IDENTITY_MISSING")
                    continue
                if row.get("session") != expected_session:
                    quality_reasons.add(f"{label}_SOURCE_SESSION_MISMATCH")
                for identity_field in ("source_capture_id",):
                    expected_value = source_prior.get(identity_field)
                    if (
                        expected_value is None
                        or row.get(identity_field) != expected_value
                    ):
                        quality_reasons.add(
                            f"{label}_{identity_field.upper()}_MISMATCH"
                        )
            if quality_reasons or reference.get("state") == "UNKNOWN":
                reference["quality"] = "UNKNOWN"
                if reference.get("state") == "UNKNOWN" and not quality_reasons:
                    quality_reasons.add("SOURCE_SUPPORT_STATE_UNKNOWN")
            reference["quality_reasons"] = sorted(quality_reasons)
            source_prior_references.append(reference)
        references.extend(source_prior_references)
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
    source_prior_observed_references = (
        source_prior_references
        if source_prior is not None
        else [ref for ref in references if ref.get("quality") == "OBSERVED"]
    )
    source_prior_observed = any(
        ref.get("quality") == "OBSERVED" for ref in source_prior_observed_references
    )
    source_observed_state = "TRUE" if source_prior_observed else "UNKNOWN"
    formal_parent_status = {
        "state": "UNKNOWN",
        "implementation_status": "NOT_IMPLEMENTED",
        "reason": "FORMAL_PARENT_EPISODE_NOT_READ_BY_SOURCE_D0_RAW",
    }
    item["d1_prior_structure_reference"] = {
        "state": "TRUE" if usable else "UNKNOWN",
        "reason": "SOURCE_SUPPORT_OBSERVED"
        if usable
        else "SOURCE_SUPPORT_REFERENCES_ONLY_BROKEN_OR_UNKNOWN"
        if source_prior_observed_references and source_prior_observed
        else "SOURCE_SUPPORT_REFERENCE_QUALITY_UNKNOWN"
        if source_prior_observed_references
        else "NO_PROVEN_UNBROKEN_SOURCE_SUPPORT_OR_LEFT_CENSORED",
        "formal_parent": "NOT_FORMAL_PARENT",
        "references": references,
        "source_prior_support_observed": source_observed_state,
        "source_prior_support_reason": "OBSERVED_SOURCE_SUPPORT_REFERENCE"
        if source_prior_observed
        else "SOURCE_PRIOR_REFERENCES_NOT_QUALIFIED"
        if source_prior_observed_references
        else "NO_PRIOR_SUPPORT_PROVEN",
        "formal_parent_episode_ready": formal_parent_status,
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "source_capture_id": source_capture_id,
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "window": source_prior.get("window") if source_prior is not None else None,
    }
    for scenario in ("STRONG_PULLBACK", "TREND_CONTINUE"):
        item["other_scenario_partial_predicates"][scenario][
            "source_prior_support_reference"
        ] = {
            "state": "TRUE" if usable else "UNKNOWN",
            "formal_parent": "NOT_FORMAL_PARENT",
        }
        item["other_scenario_partial_predicates"][scenario][
            "source_prior_support_observed"
        ] = {
            "state": source_observed_state,
            "reason": item["d1_prior_structure_reference"][
                "source_prior_support_reason"
            ],
        }
        item["other_scenario_partial_predicates"][scenario][
            "formal_parent_episode_ready"
        ] = formal_parent_status


def attach_recovery_failure_followup(
    config: Mapping[str, Any],
    item: dict[str, Any],
    *,
    target: date,
    followup: date,
    cutoff: date,
    source_capture_id: int,
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
        history = read_baostock_price_history_batch(
            config,
            source_symbols=(item["source_symbol"],),
            sessions=sessions,
            source_capture_id=source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
            cutoff=cutoff,
            limit=100,
        )
    except (RawSourcePgError, ValueError) as error:
        raise SourceD0RawError(
            getattr(error, "code", "FOLLOWUP_QFQ_HISTORY_UNAVAILABLE"), str(error)
        ) from error
    history_rows = history.get("rows", ())
    by_date = {row["session_date"]: row for row in history_rows}
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
        if row is None:
            saved_end = item.get("source_version", {}).get("range_end_session")
            reason = (
                "NOT_AVAILABLE_AFTER_SAVED_RANGE"
                if saved_end is not None and session.isoformat() > saved_end
                else "FOLLOWUP_RAW_NOT_STORED"
            )
        elif (
            birth is None
            or row.get("source_capture_id") != birth.get("source_capture_id")
            or row.get("source_symbol") != item["source_symbol"]
            or row.get("qfq_price_coordinate") != "FORWARD_ADJUSTED"
        ):
            reason = "FOLLOWUP_COORDINATE_OR_SOURCE_MISMATCH"
        elif (
            row.get("qfq_adjustflag") not in (2, "2")
            or row.get("qfq_response_session_date")
            not in (
                session,
                session.isoformat(),
            )
            or str(row.get("qfq_response_source_code") or "").upper()
            != str(row.get("source_symbol") or "").upper()
            or not isinstance(
                row.get("qfq_response_fields"), (list, tuple, set, frozenset)
            )
            or not {"date", "code", "close"}.issubset(
                set(row.get("qfq_response_fields") or ())
            )
        ):
            reason = "FOLLOWUP_QFQ_RESPONSE_IDENTITY_MISMATCH"
        elif row.get("qfq_bar_status") != "TARGET_PRESENT":
            reason = "FOLLOWUP_QFQ_BAR_MISSING"
        elif row.get("qfq_close_quality") != "OBSERVED":
            reason = str(row.get("qfq_close_reason") or "FOLLOWUP_QFQ_CLOSE_UNKNOWN")
        elif (
            trade_status_state_from_pg_row(row, price_coordinate="FORWARD_ADJUSTED")[0]
            != "ACTUAL"
        ):
            trade_state, trade_reason = trade_status_state_from_pg_row(
                row, price_coordinate="FORWARD_ADJUSTED"
            )
            reason = (
                "FOLLOWUP_CONFIRMED_SUSPENSION_NO_ACTUAL_BAR"
                if trade_state == "CONFIRMED_SUSPENSION"
                else trade_reason or "FOLLOWUP_TRADE_STATUS_UNKNOWN"
            )
        else:
            try:
                close_value = row.get("qfq_close")
                if isinstance(close_value, bool) or close_value is None:
                    raise ValueError("QFQ收盘缺失或非法")
                close = Fraction(close_value)
                if close <= 0:
                    reason, close = "FOLLOWUP_QFQ_CLOSE_INVALID", None
            except (TypeError, ValueError, ZeroDivisionError):
                reason = "FOLLOWUP_QFQ_CLOSE_INVALID"
        if reason is None and line is None:
            reason = "FAILURE_LINE_CANDIDATE_UNAVAILABLE"
        result.append(
            {
                "session": session.isoformat(),
                "close": fraction_payload(close),
                "close_quality": "OBSERVED" if close is not None else "UNKNOWN",
                "close_at_or_below_line": None
                if reason
                else line_breached(
                    FieldValue(close),
                    FieldValue(line),
                    ordinal=calendar.index(session),
                    effective_ordinal=start + 1,
                ).value,
                "quality": "UNKNOWN" if reason else "OBSERVED",
                "reason": reason,
            }
        )
    item["failure_line_followup"] = {
        "role": "DIAGNOSTIC_ONLY_NOT_D2",
        "observations": result,
    }
    if "prior_shape_observation" in item:
        parent_line_observations = []
        for parent in item["prior_shape_observation"]["diagnostic_parent_candidates"]:
            candidate_line = parent.get("diagnostic_failure_line", {})
            line_value = candidate_line.get("value")
            frozen = (
                Fraction(int(line_value["numerator"]), int(line_value["denominator"]))
                if line_value is not None
                else None
            )
            comparisons = []
            for point in result:
                close_value = point["close"]
                close = (
                    Fraction(
                        int(close_value["numerator"]), int(close_value["denominator"])
                    )
                    if close_value is not None
                    else None
                )
                known = frozen is not None and close is not None
                comparisons.append(
                    {
                        "session": point["session"],
                        "state": "UNKNOWN"
                        if not known
                        else "TRUE"
                        if close <= frozen
                        else "FALSE",
                        "reason": None
                        if known
                        else "DIAGNOSTIC_LINE_OR_FOLLOWUP_CLOSE_UNAVAILABLE",
                    }
                )
            parent_line_observations.append(
                {
                    "birth_session": parent["birth_session"],
                    "birth_scenario": parent["birth_scenario"],
                    "line": candidate_line,
                    "role": "DIAGNOSTIC_ONLY_NOT_D2_INVALIDATION",
                    "observations": comparisons,
                }
            )
        item["prior_shape_observation"]["parent_line_followup"] = (
            parent_line_observations
        )
        safe_rows = []
        for session in sessions:
            row = by_date.get(session)
            if (
                row is not None
                and birth is not None
                and (
                    row.get("source_capture_id") == birth.get("source_capture_id")
                    and row.get("qfq_price_coordinate") == "FORWARD_ADJUSTED"
                )
            ):
                safe_rows.append(row)
            else:
                safe_rows.append(
                    {
                        "session_date": session,
                        "bar_status": "FOLLOWUP_COORDINATE_OR_SOURCE_MISMATCH",
                    }
                )
        item["prior_shape_observation"]["post_signal_price_facts"] = {
            "role": "NON_PIT_DESCRIPTIVE_NOT_FORMAL_FORWARD",
            "selection": "T0_STATES_COMPUTED_WITHOUT_FOLLOWUP_INPUT",
            "facts": post_signal_price_facts(sessions, safe_rows, len(sessions) - 1),
        }


def attach_shape_state_followup(
    config, result, *, target, followup, cutoff, source_capture_id
):
    """追踪T0已选诊断父；各后继会话形态与峰为滚动重算。"""
    item = result["results"][0]
    initial = item["prior_shape_observation"]
    parents = initial["diagnostic_parent_candidates"]
    calendar = _official_calendar(followup)["sessions"]
    start, end = calendar.index(target), calendar.index(followup)
    if not 1 <= end - start <= 10 or followup > cutoff:
        raise SourceD0RawError(
            "D0_SHAPE_FOLLOWUP_INVALID", "形态后继须为截止内的1至10个官方会话。"
        )
    observations = []
    for session in calendar[start + 1 : end + 1]:
        try:
            later = query_source_d0_raw_launch(
                config,
                scenario="RECOVERY_TURN",
                target_session=session,
                research_cutoff=cutoff,
                source_capture_id=source_capture_id,
                symbol=item["source_symbol"],
                limit=1,
                _include_shape_observation=True,
            )
        except SourceD0RawError as error:
            if error.code != "D0_SOURCE_VERSION_NOT_UNIQUE":
                raise
            observations.append(
                {
                    "session": session.isoformat(),
                    "state": "UNKNOWN",
                    "reason": error.code,
                    "parents": [],
                }
            )
            continue
        if (
            len(later["results"]) != 1
            or later["source_version"]["source_capture_id"]
            != result["source_version"]["source_capture_id"]
        ):
            observations.append(
                {
                    "session": session.isoformat(),
                    "state": "UNKNOWN",
                    "reason": "SOURCE_SYMBOL_OR_CONTENT_MISMATCH",
                    "parents": [],
                }
            )
            continue
        later_obs = later["results"][0]["prior_shape_observation"]
        later_launch = query_source_d0_raw_launch(
            config,
            scenario="LAUNCH_CONFIRM",
            target_session=session,
            research_cutoff=cutoff,
            source_capture_id=source_capture_id,
            symbol=item["source_symbol"],
            limit=1,
        )
        if (
            len(later_launch["results"]) != 1
            or later_launch["source_version"]["source_capture_id"]
            != result["source_version"]["source_capture_id"]
        ):
            source_states = {"LAUNCH_CONFIRM": "UNKNOWN", "RECOVERY_TURN": "UNKNOWN"}
        else:
            source_states = {
                "LAUNCH_CONFIRM": later_launch["results"][0]["scenario_raw"],
                "RECOVERY_TURN": later["results"][0]["scenario_raw"],
            }
        lookup = {
            (p["birth_session"], p["birth_scenario"]): p
            for p in later_obs["diagnostic_parent_candidates"]
        }
        parent_states = []
        for parent in parents:
            key = (parent["birth_session"], parent["birth_scenario"])
            candidate = lookup.get(key)
            parent_states.append(
                {
                    "birth_session": key[0],
                    "birth_scenario": key[1],
                    "state": candidate.get("source_shape_state", "UNKNOWN")
                    if candidate
                    else "UNKNOWN",
                    "reason": None
                    if candidate
                    else "PRIOR_DIAGNOSTIC_PARENT_NOT_REOBSERVED_OR_EXPIRED",
                    "peak_session": candidate.get("peak_session")
                    if candidate
                    else None,
                }
            )
        states = [p["state"] for p in parent_states]
        state = (
            "TRUE"
            if "TRUE" in states
            else "UNKNOWN"
            if not states or "UNKNOWN" in states
            else "FALSE"
        )
        observations.append(
            {
                "session": session.isoformat(),
                "state": state,
                "parents": parent_states,
                "source_scenario_states": source_states,
                "later_continuation_diagnostic_unbound_to_t0_parent": later_obs[
                    "continuation_shape"
                ],
            }
        )
    earlier_unknown = False
    first_false = None
    first_proved = None
    for row in observations:
        if row["state"] == "UNKNOWN":
            earlier_unknown = True
        elif row["state"] == "FALSE" and first_proved is None:
            first_proved = row["session"]
            if not earlier_unknown:
                first_false = row["session"]
    initial["shape_state_followup"] = {
        "scope": "T0_DIAGNOSTIC_PARENTS_NON_PIT_NOT_D2",
        "shape_semantics": "ROLLING_DIAGNOSTIC_PEAK_MAY_CHANGE_NOT_T0_FROZEN_INVALIDATION",
        "observations": observations,
        "first_false_session": first_false,
        "first_false_status": "OBSERVED"
        if first_false
        else "UNKNOWN"
        if earlier_unknown
        else "NOT_OBSERVED_IN_WINDOW",
        "first_proved_false_upper_bound": first_proved,
    }
    parent_summaries = []
    for parent in parents:
        unknown_earlier = False
        first = None
        upper = None
        for row in observations:
            matching = next(
                (
                    candidate
                    for candidate in row.get("parents", ())
                    if candidate["birth_session"] == parent["birth_session"]
                    and candidate["birth_scenario"] == parent["birth_scenario"]
                ),
                None,
            )
            state = matching["state"] if matching else "UNKNOWN"
            if state == "UNKNOWN":
                unknown_earlier = True
            elif state == "FALSE" and upper is None:
                upper = row["session"]
                if not unknown_earlier:
                    first = row["session"]
        parent_summaries.append(
            {
                "birth_session": parent["birth_session"],
                "birth_scenario": parent["birth_scenario"],
                "first_false_session": first,
                "first_false_status": "OBSERVED"
                if first
                else "UNKNOWN"
                if unknown_earlier
                else "NOT_OBSERVED_IN_WINDOW",
                "first_proved_false_upper_bound": upper,
            }
        )
    initial["shape_state_followup"]["parent_first_false"] = parent_summaries
    source_first_false = {}
    for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
        unknown_earlier = False
        first = upper = None
        for row in observations:
            state = row.get("source_scenario_states", {}).get(scenario, "UNKNOWN")
            if state == "UNKNOWN":
                unknown_earlier = True
            elif state == "FALSE" and upper is None:
                upper = row["session"]
                if not unknown_earlier:
                    first = row["session"]
        source_first_false[scenario] = {
            "first_false_session": first,
            "first_false_status": "OBSERVED"
            if first
            else "UNKNOWN"
            if unknown_earlier
            else "NOT_OBSERVED_IN_WINDOW",
            "first_proved_false_upper_bound": upper,
        }
    initial["shape_state_followup"]["source_scenario_first_false"] = source_first_false


def _fraction(value: Any) -> dict[str, str] | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return fraction_payload(Fraction(value))
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _rendered_exact_fraction(field: Any) -> Fraction | None:
    """严格读取现役QFQ渲染字段的exact分数，不经float或展示小数。"""
    if not isinstance(field, Mapping) or field.get("quality") != "OBSERVED":
        return None
    value = field.get("value")
    exact = value.get("exact") if isinstance(value, Mapping) else value
    if not isinstance(exact, (str, int, Fraction)) or isinstance(exact, bool):
        return None
    try:
        result = Fraction(exact)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    return result if result.denominator > 0 else None


def _iso(value: Any) -> str | None:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return None if value is None else str(value)


def _target_row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    raw_trade_state, raw_trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate="RAW_UNADJUSTED"
    )
    qfq_trade_state, qfq_trade_reason = trade_status_state_from_pg_row(
        row, price_coordinate="FORWARD_ADJUSTED"
    )
    return {
        "target_bar_status": row.get("bar_status"),
        "target_bar_reason": row.get("bar_reason"),
        "qfq_bar_status": row.get("qfq_bar_status"),
        "qfq_bar_reason": row.get("qfq_bar_reason"),
        "raw_trade_status_state": raw_trade_state,
        "raw_trade_status_reason": raw_trade_reason,
        "qfq_trade_status_state": qfq_trade_state,
        "qfq_trade_status_reason": qfq_trade_reason,
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
        "qfq_ohlc": {
            key: _fraction(row.get(column))
            for key, column in (
                ("open", "qfq_open"),
                ("high", "qfq_high"),
                ("low", "qfq_low"),
                ("close", "qfq_close"),
            )
        },
        "qfq_price_quality": row.get("qfq_price_quality"),
        "qfq_price_reason": row.get("qfq_price_reason"),
        "raw_amount": _fraction(row.get("raw_amount")),
        "raw_amount_quality": row.get("raw_amount_quality"),
        "raw_amount_reason": row.get("raw_amount_reason"),
        "amount_unit_status": row.get("amount_unit_status"),
        "raw_volume_quality": row.get("raw_volume_quality"),
        "volume_unit_status": row.get("volume_unit_status"),
        "source_capture_id": row.get("source_capture_id"),
        "source_clock_role": row.get("source_clock_role"),
        "source_available_at": _iso(row.get("source_available_at")),
        "observed_started_at": _iso(row.get("observed_started_at")),
        "observed_completed_at": _iso(row.get("observed_completed_at")),
        "first_source_date": _iso(row.get("first_source_date")),
        "last_source_date": _iso(row.get("last_source_date")),
        "future_source_rows_excluded": row.get("future_source_rows_excluded"),
        "price_coordinate": "FORWARD_ADJUSTED",
        "price_coordinates_in_capture": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
        "time_role": "NON_PIT_RETROSPECTIVE",
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
    source_capture_id,
    price_coordinate,
):
    """以QFQ跨日结构及RAW成交量额构造连续Core观察。"""
    symbol = item["source_symbol"]
    present = {
        day: row
        for day, row in rows.items()
        if isinstance(day, date) and row is not None
    }
    inconsistent = any(
        row.get("source_symbol") != symbol
        or row.get("source_capture_id") != source_capture_id
        or row.get("qfq_price_coordinate") != "FORWARD_ADJUSTED"
        or row.get("qfq_price_quality") is None
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
    history = tuple(
        _core_observation_from_pg_price_row(
            present.get(day),
            session=day,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
            coordinate_id=(
                f"FORWARD_ADJUSTED:BAOSTOCK_SOURCE_CAPTURE:{source_capture_id}:{symbol}"
            ),
        )
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


def _official_calendar(target: date) -> dict[str, Any]:
    payload, metadata, revision_chain, sessions = _load_calendar(target)
    return {
        "calendar_id": payload["calendar_id"],
        "calendar_revision": payload["revision"],
        "calendar_revision_chain": revision_chain,
        "calendar_revision_by_year": {
            int(item["year"]): item["revision"] for item in metadata
        },
        "target_year": target.year,
        "sessions": sessions,
    }


def _calendar_revision_map(identity: Mapping[str, Any]) -> dict[int, str] | None:
    raw = identity.get("calendar_revision_by_year")
    if isinstance(raw, Mapping):
        revisions: dict[int, str] = {}
        for year, revision in raw.items():
            try:
                parsed_year = int(year)
            except (TypeError, ValueError):
                return None
            if (
                not 1000 <= parsed_year <= 9999
                or not isinstance(revision, str)
                or not revision
                or parsed_year in revisions
            ):
                return None
            revisions[parsed_year] = revision
        return revisions or None

    chain = identity.get("calendar_revision_chain")
    if not isinstance(chain, str) or not chain:
        return None
    revisions = {}
    for component in chain.split("+"):
        year_text, separator, revision = component.partition(":")
        if not separator or not year_text.isdigit() or not revision:
            return None
        year = int(year_text)
        if not 1000 <= year <= 9999 or year in revisions:
            return None
        revisions[year] = revision
    return revisions or None


def _calendar_identities_compatible(
    before: Mapping[str, Any], after: Mapping[str, Any]
) -> bool:
    """允许相邻年度各自修订，同时要求共享资源逐年一致且覆盖完整。"""
    if before.get("calendar_id") != after.get("calendar_id"):
        return False
    before_revisions = _calendar_revision_map(before)
    after_revisions = _calendar_revision_map(after)
    before_year = before.get("target_year")
    after_year = after.get("target_year")
    if (
        before_revisions is None
        or after_revisions is None
        or type(before_year) is not int
        or type(after_year) is not int
        or not before_year <= after_year <= before_year + 1
        or before_year not in before_revisions
        or after_year not in after_revisions
        or before.get("calendar_revision") != before_revisions[before_year]
        or after.get("calendar_revision") != after_revisions[after_year]
    ):
        return False
    if any(
        before_revisions[year] != revision
        for year, revision in after_revisions.items()
        if year in before_revisions
    ):
        return False
    return all(year in after_revisions for year in range(before_year, after_year + 1))


def _adjacent_d0_calendar_windows(
    before_session: date, target_session: date
) -> tuple[dict[str, Any], tuple[date, ...], tuple[date, ...]]:
    """在目标端完整连续日历内定位相邻会话及两端窗口。"""
    before_calendar = _official_calendar(before_session)
    after_calendar = _official_calendar(target_session)
    if not _calendar_identities_compatible(before_calendar, after_calendar):
        raise SourceD0RawError(
            "D0_CALENDAR_REVISIONS_INCOMPATIBLE",
            "相邻日期覆盖的年度日历资源缺失或共享年度修订冲突。",
            category="DATA",
        )
    sessions = after_calendar["sessions"]
    try:
        before_index = sessions.index(before_session)
        after_index = sessions.index(target_session)
    except ValueError as error:
        raise SourceD0RawError(
            "D0_OFFICIAL_CALENDAR_INVALID",
            "相邻日期未同时出现在目标端已核验的连续官方日历中。",
            category="ARGUMENT",
        ) from error
    if after_index != before_index + 1:
        raise SourceD0RawError(
            "D0_SESSIONS_NOT_ADJACENT",
            "前后日期必须是连续日历中的相邻官方会话。",
            category="ARGUMENT",
        )
    before_window = tuple(sessions[max(0, before_index - 20) : before_index + 1])
    after_window = tuple(sessions[max(0, after_index - 20) : after_index + 1])
    if len(before_window) != 21 or len(after_window) != 21:
        raise SourceD0RawError(
            "D0_CHANGE_WINDOW_LEFT_TRUNCATED",
            "相邻变化的两端都必须具备目标会话及前20个官方会话。",
        )
    return after_calendar, before_window, after_window


def query_source_d0_raw_launch(
    config: Mapping[str, Any],
    *,
    scenario: str,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    price_coordinate: str = "FORWARD_ADJUSTED",
    state: str | None = None,
    symbol: str | None = None,
    offset: int = 0,
    limit: int = 30,
    action: str = "current",
    before_session: date | None = None,
    before_source_capture_id: int | None = None,
    transition: str | None = None,
    _history_symbol: str | None = None,
    _include_shape_observation: bool = False,
    _single_symbol_only: bool = False,
    _rps_result: Mapping[str, Any] | None = None,
    _rps_result_error: str | None = None,
) -> dict[str, Any]:
    """读取启动或恢复原始状态，或联结启动相邻官方会话的三态变化。"""
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
            before_source_capture_id=before_source_capture_id,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
            state=state,
            symbol=symbol,
            offset=offset,
            limit=limit,
            transition=transition,
            single_symbol_only=_single_symbol_only,
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
            before_source_capture_id,
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
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or price_coordinate != "FORWARD_ADJUSTED"
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
    if _single_symbol_only and symbol is None:
        raise SourceD0RawError(
            "D0_SINGLE_SYMBOL_REQUIRED",
            "单股诊断读取必须显式提供来源代码。",
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
        "calendar_revision_chain": calendar["calendar_revision_chain"],
        "calendar_revision_by_year": calendar["calendar_revision_by_year"],
        "target_year": calendar["target_year"],
    }

    process_started = time.perf_counter()
    cpu_started = time.process_time()
    source_d1_prior_history_read_seconds = 0.0
    source_d1_prior_history_rows_read = 0
    source_d1_prior_rows_decoded = 0
    source_d1_prior_session_count = 0
    try:
        pg_result = read_source_d0_raw_window(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            sessions=sessions,
            expected_calendar={
                "calendar_id": calendar_identity["calendar_id"],
                "calendar_revision": calendar_identity["calendar_revision"],
            },
            history_source_symbol=symbol if _single_symbol_only else None,
        )
    except D0RawPgError as error:
        raise SourceD0RawError(error.code, str(error)) from error
    read_cpu_seconds = time.process_time() - cpu_started

    source_version = pg_result["source_version"]
    rps_facts = {}
    rps_input_error = None
    if scenario == "RECOVERY_TURN":
        if _rps_result_error is not None:
            if _rps_result_error != "PG_SOURCE_NO_MATCH":
                raise SourceD0RawError(
                    "D0_RPS_INPUT_INVALID",
                    "RPS读取只允许将精确来源事实缺失标为不可评价。",
                    category="DATA_OR_CONTRACT",
                )
            rps_input_error = _rps_result_error
        else:
            try:
                rps = (
                    _rps_result
                    if _rps_result is not None
                    else read_source_pool_rps(
                        config,
                        target_session=target_session,
                        research_cutoff=research_cutoff,
                        source_capture_id=source_capture_id,
                    )
                )
                if (
                    rps.get("target_session") != target_session
                    or rps.get("research_cutoff") != research_cutoff
                    or rps.get("source_capture_id") != source_capture_id
                ):
                    rps_input_error = "D0_RPS_SOURCE_IDENTITY_MISMATCH"
                elif set(rps["facts"]) != set(pg_result["source_symbols"]):
                    rps_input_error = "D0_RPS_SOURCE_POOL_MISMATCH"
                else:
                    rps_facts = rps["facts"]
            except RawSourcePgError as error:
                if error.code != "PG_SOURCE_NO_MATCH":
                    raise
                rps_input_error = "PG_SOURCE_NO_MATCH"
            except ValueError as error:
                rps_input_error = _rps_unavailability(error)
    calculate_started = time.perf_counter()
    calculate_cpu_started = time.process_time()
    results = []
    unknown_reasons: Counter[str] = Counter()
    false_reasons: Counter[str] = Counter()
    state_counts: Counter[str] = Counter()
    evaluated_source_symbols = (
        (symbol,)
        if _single_symbol_only and symbol in pg_result["source_symbols"]
        else ()
        if _single_symbol_only
        else pg_result["source_symbols"]
    )
    for source_symbol in evaluated_source_symbols:
        arguments = dict(
            target_session=target_session,
            official_sessions=sessions,
            history_rows_by_session=pg_result["history_rows_by_symbol"][source_symbol],
            source_symbol=source_symbol,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
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
                source_capture_id=source_version["source_capture_id"],
                price_coordinate=price_coordinate,
            )
            if _include_shape_observation:
                extended_sessions = calendar["sessions"][
                    max(0, target_index - 59) : target_index + 1
                ]
                history_read_started = time.perf_counter()
                history = read_baostock_price_history_batch(
                    config,
                    source_symbols=(item["source_symbol"],),
                    sessions=extended_sessions,
                    source_capture_id=source_capture_id,
                    price_coordinate="FORWARD_ADJUSTED",
                    cutoff=research_cutoff,
                    limit=100,
                )
                source_d1_prior_history_read_seconds += (
                    time.perf_counter() - history_read_started
                )
                history_rows = history.get("rows", ())
                source_d1_prior_history_rows_read += len(history_rows)
                history_target = next(
                    (
                        row
                        for row in history_rows
                        if row["session_date"] == target_session
                    ),
                    None,
                )
                if (
                    history_target is None
                    or history_target.get("source_capture_id")
                    != source_version["source_capture_id"]
                    or history_target.get("qfq_price_coordinate") != "FORWARD_ADJUSTED"
                ):
                    raise SourceD0RawError(
                        "D0_PRIOR_HISTORY_TARGET_BINDING_MISMATCH",
                        "单股历史与目标日来源捕获或解析合同不一致。",
                    )
                shape_sessions = extended_sessions[-45:]
                shape_session_set = set(shape_sessions)
                shape_rows = [
                    row
                    for row in history_rows
                    if row["session_date"] in shape_session_set
                ]
                item["prior_shape_observation"] = observe_prior_shapes(
                    sessions=shape_sessions,
                    rows=shape_rows,
                    symbol=item["source_symbol"],
                    source_capture_id=source_capture_id,
                    price_coordinate="FORWARD_ADJUSTED",
                )
                historical_rps = {}
                rps_errors = {}
                rps_denominators = {}
                for prior in item["prior_shape_observation"]["historical_states"]:
                    if not prior.get("recovery_needs_rps"):
                        continue
                    prior_day = date.fromisoformat(prior["session"])
                    try:
                        prior_rps = read_source_pool_rps(
                            config,
                            target_session=prior_day,
                            research_cutoff=research_cutoff,
                            source_capture_id=source_capture_id,
                        )
                        historical_rps[prior_day] = (
                            prior_rps["facts"]
                            .get(item["source_symbol"], {})
                            .get("rps5_delta3_pp")
                        )
                        rps_denominators[prior["session"]] = prior_rps[
                            "endpoint_counts"
                        ]
                    except RawSourcePgError as error:
                        if error.code != "PG_SOURCE_NO_MATCH":
                            raise
                        rps_errors[prior["session"]] = error.code
                    except ValueError as error:
                        rps_errors[prior["session"]] = _rps_unavailability(error)
                if historical_rps:
                    item["prior_shape_observation"] = observe_prior_shapes(
                        sessions=shape_sessions,
                        rows=shape_rows,
                        symbol=item["source_symbol"],
                        source_capture_id=source_capture_id,
                        price_coordinate="FORWARD_ADJUSTED",
                        historical_rps=historical_rps,
                    )
                item["prior_shape_observation"]["historical_rps_errors"] = rps_errors
                item["prior_shape_observation"]["historical_rps_denominators"] = (
                    rps_denominators
                )
                finalize_prior_shape_observation(item)
                prior_sessions = extended_sessions[:-1]
                prior_session_set = set(prior_sessions)
                prior_rows = [
                    row
                    for row in history_rows
                    if row["session_date"] in prior_session_set
                ]
                source_prior = discover_source_d1_prior_support_reference(
                    symbol=item["source_symbol"],
                    sessions=prior_sessions,
                    history_rows=prior_rows,
                    source_capture_id=source_capture_id,
                    price_coordinate="FORWARD_ADJUSTED",
                )
                attach_source_d1_prior_reference(
                    item,
                    source_capture_id=source_capture_id,
                    source_prior=source_prior,
                )
                source_d1_prior_rows_decoded += source_prior["raw_history_rows_decoded"]
                source_d1_prior_session_count += len(prior_sessions)
    elif scenario == "RECOVERY_TURN":
        for item in page:
            item["other_scenario_partial_predicates"]["TREND_CONTINUE"][
                "core_fields_status"
            ] = "NOT_REQUESTED_SELECT_EXACT_SOURCE_SYMBOL"
    source_count = pg_result["source_count"]
    partition_closed = sum(state_counts.values()) == source_count
    if not _single_symbol_only and not partition_closed:
        raise SourceD0RawError(
            "D0_STATE_PARTITION_OPEN", "TRUE/FALSE/UNKNOWN没有闭合到完整来源池。"
        )

    return {
        "ok": True,
        "contract": "STOCK_A_D0_LAUNCH_CONFIRM_BAOSTOCK_QFQ_R2"
        if scenario == "LAUNCH_CONFIRM"
        else "STOCK_A_D0_RECOVERY_TURN_BAOSTOCK_QFQ_R2",
        "producer_version": "STOCK_A_D0_LAUNCH_BAOSTOCK_READER_R5"
        if scenario == "LAUNCH_CONFIRM"
        else "STOCK_A_D0_RECOVERY_BAOSTOCK_READER_R5",
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
            "STRONG_PULLBACK": "SOURCE_SHAPE_COMPONENTS_AVAILABLE_FORMAL_NOT_EVALUATED"
            if scenario == "RECOVERY_TURN"
            and symbol is not None
            and _include_shape_observation
            else "NOT_IMPLEMENTED",
            "TREND_CONTINUE": "SOURCE_SHAPE_COMPONENTS_AVAILABLE_FORMAL_NOT_EVALUATED"
            if scenario == "RECOVERY_TURN"
            and symbol is not None
            and _include_shape_observation
            else "NOT_IMPLEMENTED",
        },
        "session_contract": "D0_SESSION_R2",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "price_coordinate": price_coordinate,
        "time_role": "NON_PIT_RETROSPECTIVE",
        "strict_official_sessions": [session.isoformat() for session in sessions],
        "official_calendar": calendar_identity,
        "source_version": {
            "source_capture_id": source_version["source_capture_id"],
            "parser_contract": source_version["parser_contract"],
            "price_coordinate": price_coordinate,
            "price_coordinates_in_capture": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
            "range_end_session": _iso(source_version["range_end_session"]),
            "unit_contract_version": source_version["unit_contract_version"],
            "source_time_quality": source_version["source_time_quality"],
            "source_available_at": _iso(source_version["source_available_at"]),
            "source_clock_role": source_version["source_clock_role"],
            "capture_expected_row_count": source_version["capture_expected_row_count"],
            **calendar_identity,
        },
        "source_pool": {
            "source_count": source_count,
            "evaluation_scope": "EXACT_SOURCE_SYMBOL_ONLY"
            if _single_symbol_only
            else "FULL_SOURCE_POOL",
            "requested_symbol_in_pool": symbol in pg_result["source_symbols"]
            if _single_symbol_only
            else None,
            "evaluated_source_count": len(evaluated_source_symbols),
            "target_present_count": source_version["target_present_count"],
            "target_missing_count": source_version["target_missing_count"],
            "read_failed_count": source_version["read_failed_count"],
            "scenario_state_counts": {
                key: state_counts.get(key, 0) for key in ("TRUE", "FALSE", "UNKNOWN")
            },
            "scenario_state_partition_scope": "FULL_SOURCE_POOL"
            if not _single_symbol_only
            else "EXACT_SOURCE_SYMBOL_ONLY",
            "scenario_state_partition_closed": partition_closed
            if not _single_symbol_only
            else False,
            "unknown_main_reason_counts": dict(sorted(unknown_reasons.items())),
            "unknown_main_reason_partition_closed": not _single_symbol_only
            and sum(unknown_reasons.values()) == state_counts.get("UNKNOWN", 0),
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
            "source_d1_prior_history_rows_read": source_d1_prior_history_rows_read,
            "source_d1_prior_history_rows_decoded": source_d1_prior_rows_decoded,
            "source_d1_prior_official_sessions": source_d1_prior_session_count,
            "source_d1_prior_history_read_seconds": round(
                source_d1_prior_history_read_seconds, 6
            ),
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
    before_source_capture_id: int | None,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    price_coordinate: str,
    state: str | None,
    symbol: str | None,
    offset: int,
    limit: int,
    transition: str | None,
    single_symbol_only: bool,
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
        type(before_source_capture_id) is not int
        or type(source_capture_id) is not int
        or before_source_capture_id <= 0
        or source_capture_id <= 0
        or before_source_capture_id != source_capture_id
        or price_coordinate != "FORWARD_ADJUSTED"
    ):
        raise SourceD0RawError(
            "D0_CHANGE_PACKAGE_IDENTITY_MISMATCH",
            "相邻比较两端必须显式绑定同一个数字来源包ID。",
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

    _, before_sessions, after_sessions = _adjacent_d0_calendar_windows(
        before_session, target_session
    )

    started = time.perf_counter()
    try:
        before_result = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=before_session,
            research_cutoff=research_cutoff,
            source_capture_id=before_source_capture_id,
            price_coordinate=price_coordinate,
            state="ALL",
            offset=0,
            limit=_MAX_LIMIT,
            symbol=symbol if single_symbol_only else None,
            _history_symbol=symbol,
            _single_symbol_only=single_symbol_only,
        )
        after_result = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            price_coordinate=price_coordinate,
            state="ALL",
            offset=0,
            limit=_MAX_LIMIT,
            symbol=symbol if single_symbol_only else None,
            _history_symbol=symbol,
            _single_symbol_only=single_symbol_only,
        )
    except SourceD0RawError:
        raise

    before_source = before_result["source_version"]
    after_source = after_result["source_version"]
    compatible_identity = (
        before_source["source_capture_id"] == after_source["source_capture_id"]
        and _calendar_identities_compatible(
            before_result["official_calendar"], after_result["official_calendar"]
        )
        and before_result["session_contract"] == after_result["session_contract"]
        and before_result["algorithm_contract"] == after_result["algorithm_contract"]
        and before_result["producer_version"] == after_result["producer_version"]
    )
    if not compatible_identity:
        raise SourceD0RawError(
            "D0_PAIR_NOT_COMPARABLE",
            "两端来源包、日历或算法身份不兼容，拒绝计算转移。",
        )

    def indexed_rows(
        result: Mapping[str, Any],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        pool = result["source_pool"]
        rows = result["results"]
        if pool.get("evaluation_scope") == "EXACT_SOURCE_SYMBOL_ONLY":
            if (
                symbol is None
                or result["selection"].get("symbol") != symbol
                or result["selection"]["page_count"] != len(rows)
                or len(rows) > 1
                or any(row.get("source_symbol") != symbol for row in rows)
            ):
                raise SourceD0RawError(
                    "D0_CHANGE_SYMBOL_ENDPOINT_INVALID",
                    "单股相邻变化端点没有绑定到唯一请求代码。",
                )
        elif (
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
            "price_coordinate": "FORWARD_ADJUSTED",
            "time_role": "NON_PIT_RETROSPECTIVE",
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
    if not single_symbol_only and sum(transition_counts.values()) != comparable_count:
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
            "source_capture_id": result["source_version"]["source_capture_id"],
            "price_coordinate": "FORWARD_ADJUSTED",
            "time_role": "NON_PIT_RETROSPECTIVE",
            "execution": result["execution"],
        }

    return {
        "ok": True,
        "contract": "STOCK_A_D0_LAUNCH_ADJACENT_TRANSITION_BAOSTOCK_QFQ_R2",
        "producer_version": "STOCK_A_D0_LAUNCH_BAOSTOCK_READER_R5",
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
        "source_capture_id": source_capture_id,
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "entity_identity": "SOURCE_SYMBOL_ONLY",
        "before": endpoint_summary(before_result),
        "after": endpoint_summary(after_result),
        "source_key_set": {
            "key": "market+source_symbol",
            "evaluation_scope": "EXACT_SOURCE_SYMBOL_ONLY"
            if single_symbol_only
            else "FULL_SOURCE_POOL",
            "before_count": before_result["source_pool"]["source_count"]
            if single_symbol_only
            else len(before_keys),
            "after_count": after_result["source_pool"]["source_count"]
            if single_symbol_only
            else len(after_keys),
            "comparable_intersection_count": None
            if single_symbol_only
            else comparable_count,
            "source_union_count": None if single_symbol_only else len(union_keys),
            "before_only_count": None
            if single_symbol_only
            else counts.get("BEFORE_ONLY", 0),
            "after_only_count": None
            if single_symbol_only
            else counts.get("AFTER_ONLY", 0),
            "pair_not_comparable_count": None if single_symbol_only else 0,
            "union_partition_closed": None
            if single_symbol_only
            else len(before_keys & after_keys)
            + counts.get("BEFORE_ONLY", 0)
            + counts.get("AFTER_ONLY", 0)
            == len(union_keys),
        },
        "transition_counts": transition_counts,
        "transition_counts_scope": "EXACT_SOURCE_SYMBOL_ONLY"
        if single_symbol_only
        else "FULL_SOURCE_POOL",
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
