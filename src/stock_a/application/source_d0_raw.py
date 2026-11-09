"""来源股票D0启动和恢复原始场景只读诊断。"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict
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
from stock_a.research.d0_prior_observation import observe_prior_shapes
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.source_pool_seed import source_seed_predicates
from stock_a.application.source_d1_structures import (
    discover_source_d1_prior_support_reference,
)
from stock_a.application.tdx_sector_members import _raw_window_observation
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
from stock_a.storage.postgres.d0_raw import (
    MAX_SOURCE_SYMBOLS,
    D0RawPgError,
    read_source_d0_raw_window,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_raw_daily_history_page,
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


def read_source_shape_cohort(config, day, *, research_cutoff, package_sha256):
    """对完整T0来源池复用回踩、续强诊断；历史RPS按会话共享。"""
    target = day["session"]
    namespace = day["namespace_id"]
    source = day["scenarios"]["RECOVERY_TURN"]["source_version"]
    expected = set(day["states"])
    calendar = _official_calendar(target)
    end = calendar["sessions"].index(target)
    sessions = calendar["sessions"][max(0, end - 44) : end + 1]
    short_sessions = sessions[-25:]
    unavailable_messages = {
        "目标日没有精确来源池": "HISTORICAL_RPS_SOURCE_POOL_EMPTY",
        "来源池分母不完整": "RPS_SOURCE_POOL_INCOMPLETE",
        "来源池分页分母发生变化": "RPS_SOURCE_POOL_INCOMPLETE",
    }
    target_rps_error = None
    target_rps = day["seed"].get("rps_read")
    if target_rps is None:
        try:
            target_rps = read_source_pool_rps(
                config,
                target_session=target,
                research_cutoff=research_cutoff,
                package_sha256=package_sha256,
                expected_namespace_id=namespace,
            )
        except RawSourcePgError as error:
            if error.code != "PG_SOURCE_NO_MATCH":
                raise
            target_rps_error = error.code
        except ValueError as error:
            if str(error) not in unavailable_messages:
                raise
            target_rps_error = unavailable_messages[str(error)]
        if target_rps_error:
            target_rps = {"facts": {symbol: {} for symbol in expected}}
    if set(target_rps["facts"]) != expected:
        target_rps_error = "D0_SHAPE_RPS_POOL_MISMATCH"
        target_rps = {"facts": {symbol: {} for symbol in expected}}
    rps_cache = {}
    rps_errors = {}
    output = {}
    offset = 0
    while True:
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            package_sha256=package_sha256,
            sessions=sessions,
            offset=offset,
            limit=100,
            market="ALL",
            board="ALL",
            include_parser_contract=True,
        )
        if (
            page["namespace_id"] != namespace
            or page["total_count"] != len(expected)
            or page["version_status"] != "CLOSED"
            or not page["partition_closed"]
        ):
            raise SourceD0RawError(
                "D0_SHAPE_POOL_MISMATCH", "诊断形态来源池或版本不完整。"
            )
        for candidate in page["candidates"]:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            by_date = {row["session_date"]: row for row in rows}
            birth = by_date.get(target)
            if (
                symbol not in expected
                or symbol in output
                or birth is None
                or birth.get("source_capture_id") != source["source_capture_id"]
                or birth.get("source_content_id") != source["source_content_id"]
                or birth.get("parser_contract") != source["parser_contract"]
            ):
                raise SourceD0RawError(
                    "D0_SHAPE_TARGET_MISMATCH", "诊断形态T0来源捕获不一致。"
                )
            args = {
                "sessions": sessions,
                "rows": rows,
                "symbol": symbol,
                "package": package_sha256,
                "namespace": namespace,
                "content": source["source_content_id"],
                "parser": source["parser_contract"],
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
                        package_sha256=package_sha256,
                        expected_namespace_id=namespace,
                    )["facts"]
                except RawSourcePgError as error:
                    if error.code != "PG_SOURCE_NO_MATCH":
                        raise
                    rps_errors[prior] = error.code
                except ValueError as error:
                    if str(error) not in unavailable_messages:
                        raise
                    rps_errors[prior] = unavailable_messages[str(error)]
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
                package_sha256=package_sha256,
                namespace_id=namespace,
                source_content_id=source["source_content_id"],
                target_capture_id=source["source_capture_id"],
                parser_contract=source["parser_contract"],
                rps5_delta3=rps.get("rps5_delta3_pp"),
                rps20=rps.get("rps20"),
                include_prior_ols=True,
            )
            _attach_continuation_core(
                item,
                rows={session: by_date.get(session) for session in short_sessions},
                sessions=short_sessions,
                calendar=calendar,
                namespace_id=namespace,
                source_content_id=source["source_content_id"],
                target_capture_id=source["source_capture_id"],
                parser_contract=source["parser_contract"],
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
                    if (
                        isinstance(predicate, dict)
                        and predicate.get("state") == "UNKNOWN"
                    ):
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
        offset = page["next_offset"]
        if offset is None:
            break
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


def diagnostic_admission(result, *, target, package_sha256):
    """应用来源诊断角色，不生成正式事件。"""
    item = result["results"][0]
    symbol = item["source_symbol"]
    source = f"{result['namespace_id']}:{package_sha256}"
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
            coordinate_id=f"RAW:TDX_SOURCE_SYMBOL:{symbol}",
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
    config, result, *, target, cutoff, package_sha256, rps_read=None
):
    """将同股同日PG历史事实交给现役风险归并，保留正式结构未知。"""
    item = result["results"][0]
    symbol = item["source_symbol"]
    namespace = result["namespace_id"]
    _, sessions, _ = resolve_pg_history_window(target.isoformat(), 75)
    history = read_raw_daily_history_stock(
        config,
        source_symbol=symbol,
        target_session=target,
        package_sha256=package_sha256,
        sessions=sessions,
        expected_namespace_id=namespace,
        research_cutoff=cutoff,
    )
    preview = research_stock_pg_history_preview(
        symbol=symbol,
        target_session=target.isoformat(),
        package_sha256=package_sha256,
        session_dates=sessions,
        history_rows=history["history_rows"],
        namespace_id=namespace,
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
    coordinate = f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
    source = f"{namespace}:{package_sha256}"
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
            package_sha256=package_sha256,
            expected_namespace_id=namespace,
        )
    if (
        rps_read["namespace_id"] != namespace
        or rps_read["target_session"] != target
        or rps_read["research_cutoff"] != cutoff
        or rps_read["package_sha256"] != package_sha256
    ):
        raise ValueError("风险输入RPS与目标来源身份不一致")
    metric_field = rps_read["facts"].get(symbol, {}).get("rps5_delta3_pp") or unknown(
        "RPS5_DELTA3_UNAVAILABLE"
    )
    prior = research_stock_pg_history_preview(
        symbol=symbol,
        target_session=sessions[-2].isoformat(),
        package_sha256=package_sha256,
        session_dates=sessions[:-1],
        history_rows=[
            row for row in history["history_rows"] if row["session_date"] < target
        ],
        namespace_id=namespace,
        profile_contract=load_price_profile_contract(_CONFIG_ROOT).document,
        include_profile_preview=False,
        allow_missing_target_row=True,
    )
    previous_close = prior["raw"]["close"]
    previous_ma = prior["core"]["fields"]["core.ma20"]
    current_close = preview["raw"]["close"]
    current_ma = preview["core"]["fields"]["core.ma20"]
    if all(
        x.get("quality") == "OBSERVED"
        for x in (previous_close, previous_ma, current_close, current_ma)
    ):
        crossed = Fraction(previous_close["value"]) <= Fraction(
            previous_ma["value"]["exact"]
        ) and Fraction(current_close["value"]) > Fraction(current_ma["value"]["exact"])
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
            result, target=target, package_sha256=package_sha256
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
        expected_coordinate = f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
        source_prior_identity_reasons = []
        if source_prior.get("identity_status") != "SOURCE_SYMBOL_ONLY":
            source_prior_identity_reasons.append(
                "SOURCE_IDENTITY_NOT_SOURCE_SYMBOL_ONLY"
            )
        if source_prior.get("research_mode") != "NON_PIT_RETROSPECTIVE":
            source_prior_identity_reasons.append("SOURCE_RESEARCH_MODE_NOT_NON_PIT")
        if source_prior.get("price_coordinate") != "RAW_UNADJUSTED":
            source_prior_identity_reasons.append("SOURCE_PRICE_COORDINATE_NOT_RAW")
        for identity_field in (
            "namespace_id",
            "package_sha256",
            "source_content_id",
            "parser_contract",
        ):
            if source_prior.get(identity_field) is None:
                source_prior_identity_reasons.append(
                    f"SOURCE_{identity_field.upper()}_IDENTITY_MISSING"
                )
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
            if reference.get("price_coordinate_status") != "RAW_UNADJUSTED":
                quality_reasons.add("REFERENCE_PRICE_COORDINATE_NOT_RAW")
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
                for identity_field in (
                    "namespace_id",
                    "package_sha256",
                    "source_content_id",
                    "parser_contract",
                ):
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
        "price_coordinate": "RAW_UNADJUSTED",
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
        if row is None:
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
                    row.get("source_entry_sha256")
                    and row.get("source_entry_sha256")
                    == birth.get("source_entry_sha256")
                    and row.get("source_content_id") == birth.get("source_content_id")
                    and row.get("price_coordinate") == "RAW_UNADJUSTED"
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
    config, result, *, target, followup, cutoff, package_sha256
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
                package_sha256=package_sha256,
                symbol=item["source_symbol"],
                limit=1,
                expected_namespace_id=result["namespace_id"],
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
            or later["source_version"]["source_content_id"]
            != result["source_version"]["source_content_id"]
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
            package_sha256=package_sha256,
            symbol=item["source_symbol"],
            limit=1,
            expected_namespace_id=result["namespace_id"],
        )
        if (
            len(later_launch["results"]) != 1
            or later_launch["source_version"]["source_content_id"]
            != result["source_version"]["source_content_id"]
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
    _include_shape_observation: bool = False,
    _single_symbol_only: bool = False,
    _rps_result: Mapping[str, Any] | None = None,
    _rps_result_error: str | None = None,
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
        "calendar_sha256": calendar["calendar_sha256"],
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
            package_sha256=package_sha256,
            sessions=sessions,
            expected_calendar=calendar_identity,
            expected_namespace_id=expected_namespace_id,
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
            rps_input_error = f"RPS_INPUT_VALUE_ERROR:{_rps_result_error}"
        else:
            try:
                rps = (
                    _rps_result
                    if _rps_result is not None
                    else read_source_pool_rps(
                        config,
                        target_session=target_session,
                        research_cutoff=research_cutoff,
                        package_sha256=package_sha256,
                        expected_namespace_id=pg_result["namespace_id"],
                    )
                )
                if (
                    rps.get("target_session") != target_session
                    or rps.get("research_cutoff") != research_cutoff
                    or rps.get("namespace_id") != pg_result["namespace_id"]
                    or rps.get("package_sha256") != package_sha256
                ):
                    rps_input_error = "D0_RPS_SOURCE_IDENTITY_MISMATCH"
                elif set(rps["facts"]) != set(pg_result["source_symbols"]):
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
            if _include_shape_observation:
                extended_sessions = calendar["sessions"][
                    max(0, target_index - 74) : target_index + 1
                ]
                history_read_started = time.perf_counter()
                history = read_raw_daily_history_stock(
                    config,
                    source_symbol=item["source_symbol"],
                    target_session=target_session,
                    package_sha256=package_sha256,
                    sessions=extended_sessions,
                    expected_namespace_id=pg_result["namespace_id"],
                    include_parser_contract=True,
                )
                source_d1_prior_history_read_seconds += (
                    time.perf_counter() - history_read_started
                )
                source_d1_prior_history_rows_read += len(history["history_rows"])
                history_target = next(
                    (
                        row
                        for row in history["history_rows"]
                        if row["session_date"] == target_session
                    ),
                    None,
                )
                if (
                    history_target is None
                    or history_target.get("source_capture_id")
                    != source_version["source_capture_id"]
                    or history_target.get("source_content_id")
                    != source_version["source_content_id"]
                    or history_target.get("parser_contract")
                    != source_version["parser_contract"]
                ):
                    raise SourceD0RawError(
                        "D0_PRIOR_HISTORY_TARGET_BINDING_MISMATCH",
                        "单股历史与目标日来源捕获或解析合同不一致。",
                    )
                shape_sessions = extended_sessions[-45:]
                shape_session_set = set(shape_sessions)
                shape_rows = [
                    row
                    for row in history["history_rows"]
                    if row["session_date"] in shape_session_set
                ]
                item["prior_shape_observation"] = observe_prior_shapes(
                    sessions=shape_sessions,
                    rows=shape_rows,
                    symbol=item["source_symbol"],
                    package=package_sha256,
                    namespace=pg_result["namespace_id"],
                    content=source_version["source_content_id"],
                    parser=source_version["parser_contract"],
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
                            package_sha256=package_sha256,
                            expected_namespace_id=pg_result["namespace_id"],
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
                        if str(error) != "目标日没有精确来源池":
                            raise
                        rps_errors[prior["session"]] = (
                            "HISTORICAL_RPS_SOURCE_POOL_EMPTY"
                        )
                if historical_rps:
                    item["prior_shape_observation"] = observe_prior_shapes(
                        sessions=shape_sessions,
                        rows=shape_rows,
                        symbol=item["source_symbol"],
                        package=package_sha256,
                        namespace=pg_result["namespace_id"],
                        content=source_version["source_content_id"],
                        parser=source_version["parser_contract"],
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
                    for row in history["history_rows"]
                    if row["session_date"] in prior_session_set
                ]
                source_prior = discover_source_d1_prior_support_reference(
                    symbol=item["source_symbol"],
                    sessions=prior_sessions,
                    history_rows=prior_rows,
                    namespace_id=pg_result["namespace_id"],
                    package_sha256=package_sha256,
                    source_content_id=source_version["source_content_id"],
                    parser_contract=source_version["parser_contract"],
                )
                attach_source_d1_prior_reference(item, source_prior=source_prior)
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
            symbol=symbol if single_symbol_only else None,
            expected_namespace_id=expected_namespace_id,
            _history_symbol=symbol,
            _single_symbol_only=single_symbol_only,
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
            symbol=symbol if single_symbol_only else None,
            expected_namespace_id=expected_namespace_id,
            _history_symbol=symbol,
            _single_symbol_only=single_symbol_only,
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
