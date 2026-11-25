"""从现有D0、Seed、D1和QFQ研究输入生产冻结规则typed事实。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from decimal import Decimal
from fractions import Fraction
from typing import Any, Mapping
from urllib.parse import unquote

from stock_a.domain import FieldValue
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_d0_raw import read_source_shape_cohort
from stock_a.application.pg_research import read_source_version_context
from stock_a.storage.postgres.research_facts import (
    RULE_VERSION,
    SCOPE,
    RULE_COLUMNS,
    write_computed_rule_rows,
)
from stock_a.storage.postgres.source_launch_watch_evidence import (
    read_source_launch_watch_evidence,
)
from stock_a.storage.postgres.static_research_facts import (
    read_source_d0_scenario_day,
    read_source_pool_seed_facts,
)
from stock_a.storage.postgres.t0_early_observations import (
    T0EarlyObservationPgError,
    read_source_t0_early_observation_inputs,
)


_UNIMPLEMENTED_RULES = ("D1_T0", "D1_ANCHOR_WINDOW")
_SEED_PRODUCER_VERSION = "SOURCE_POOL_SEED_CORE_PROFILE_R2"
_ENROLLABLE_RULES = ("LAUNCH_CONFIRM", "RECOVERY_TURN", "s1", "s2", "seed")
_DIAGNOSTIC_RULES = (
    "STRONG_PULLBACK_DIAGNOSTIC",
    "TREND_CONTINUE_DIAGNOSTIC",
)


class SourceFrozenRulesError(ValueError):
    """冻结规则事实缺少精确现役输入或无法独立闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _configured_history_session_limit(config: Mapping[str, Any]) -> int:
    try:
        limit = config["source_policy"]["baostock_primary"]["request_budget"][
            "max_history_session_count"
        ]
    except (KeyError, TypeError) as error:
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SCOPE_INVALID", "缺少BaoStock历史会话预算"
        ) from error
    if type(limit) is not int or not 1 <= limit <= 250:
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SCOPE_INVALID", "BaoStock历史会话预算超出运行配置合同"
        )
    return limit


def _rule_fact(state: Any, reasons: Any) -> dict[str, Any]:
    if state not in {"TRUE", "FALSE", "UNKNOWN"}:
        raise SourceFrozenRulesError(
            "SOURCE_RULE_STATE_INVALID", "现有规则生产者没有返回有效三态"
        )
    clean_reasons = [
        reason for reason in (reasons or ()) if isinstance(reason, str) and reason
    ]
    if state != "TRUE" and not clean_reasons:
        clean_reasons = ["SOURCE_RULE_REASON_NOT_RECORDED"]
    return {"state": state, "reasons": clean_reasons}


def _calendar_revision_components(value: Any) -> dict[int, str] | None:
    if not isinstance(value, str) or not value:
        return None
    result: dict[int, str] = {}
    for component in value.split("+"):
        year_text, separator, revision = component.partition(":")
        if not separator or not year_text.isdigit() or not revision:
            return None
        year = int(year_text)
        if not 1000 <= year <= 9999 or year in result:
            return None
        result[year] = revision
    return result or None


def _calendar_revision_for_target_year(value: Any, year: int) -> str | None:
    components = _calendar_revision_components(value)
    if components is not None:
        return components.get(year)
    if isinstance(value, str) and value and "+" not in value and ":" not in value:
        return value
    return None


def _seed_rps_field(row: Mapping[str, Any], prefix: str) -> FieldValue:
    quality = row.get(f"{prefix}_quality")
    reason = row.get(f"{prefix}_reason")
    numerator = row.get(f"{prefix}_numerator")
    denominator = row.get(f"{prefix}_denominator")
    if quality == "OBSERVED":
        exact_numerator = (
            numerator
            if type(numerator) is int
            else int(numerator)
            if isinstance(numerator, Decimal)
            and numerator.is_finite()
            and numerator == numerator.to_integral_value()
            else None
        )
        exact_denominator = (
            denominator
            if type(denominator) is int
            else int(denominator)
            if isinstance(denominator, Decimal)
            and denominator.is_finite()
            and denominator == denominator.to_integral_value()
            else None
        )
        if (
            exact_numerator is None
            or exact_denominator is None
            or exact_denominator <= 0
            or reason is not None
        ):
            raise SourceFrozenRulesError(
                "SOURCE_RULE_SEED_RPS_INVALID", "已观测Seed RPS缺少精确有理数"
            )
        return FieldValue(Fraction(exact_numerator, exact_denominator))
    if (
        quality in {"UNKNOWN_DATA", "NOT_APPLICABLE"}
        and numerator is None
        and denominator is None
        and isinstance(reason, str)
        and reason
    ):
        return FieldValue(None, quality, reason)
    raise SourceFrozenRulesError(
        "SOURCE_RULE_SEED_RPS_INVALID", "Seed RPS质量、数值和原因不一致"
    )


def _rule_row(
    *,
    target_session: date,
    source_capture_id: int,
    source_symbol: str,
    rule_id: str,
    profile_revision: str,
    history_session_count: int,
    d1_history_session_count: int,
    event_window_start: date,
    calendar_revision: str,
    fact: Mapping[str, Any],
) -> tuple[Any, ...]:
    state = fact["state"]
    reasons = list(fact.get("reasons") or ())
    event_count = int(fact.get("event_count") or 0)
    line = fact.get("frozen_line") or {}
    line_value = line.get("value") if isinstance(line, Mapping) else None
    if isinstance(line_value, Mapping):
        numerator = int(line_value["numerator"])
        denominator = int(line_value["denominator"])
    else:
        numerator = denominator = None
    source = line.get("source") if isinstance(line, Mapping) else None
    row = (
        target_session,
        source_symbol,
        source_capture_id,
        rule_id,
        RULE_VERSION,
        profile_revision,
        history_session_count,
        d1_history_session_count,
        event_window_start,
        calendar_revision,
        state,
        reasons,
        event_count,
        numerator,
        denominator,
        source,
        SCOPE,
    )
    if len(row) != len(RULE_COLUMNS):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_TYPED_COLUMNS_INVALID", "冻结规则typed行列数无效"
        )
    return row


def persist_source_computable_rules_from_saved(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str,
    history_session_count: int,
    snapshot_ids: Mapping[str, int] | None = None,
    include_diagnostics: bool = True,
    cross_section_universe_capture_id: int | None = None,
    cross_section_universe_version: str | None = None,
    cross_section_contract_version: str | None = None,
    universe_match_status: str | None = None,
) -> dict[str, Any]:
    """只组合已保存的D0/Seed/D1输入与既有QFQ诊断，不采集或重算基础事实。"""
    history_session_limit = _configured_history_session_limit(config)
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or not isinstance(profile_revision, str)
        or not profile_revision
        or type(history_session_count) is not int
        or not 1 <= history_session_count <= history_session_limit
        or type(include_diagnostics) is not bool
        or cross_section_universe_capture_id is not None
        and (
            type(cross_section_universe_capture_id) is not int
            or cross_section_universe_capture_id <= 0
        )
        or cross_section_universe_version is not None
        and (
            not isinstance(cross_section_universe_version, str)
            or not cross_section_universe_version.strip()
        )
        or cross_section_contract_version is not None
        and (
            not isinstance(cross_section_contract_version, str)
            or not cross_section_contract_version.strip()
        )
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SCOPE_INVALID", "冻结规则目标、来源或Profile键无效"
        )

    scenario_pages = {
        scenario: read_source_d0_scenario_day(
            config,
            target_session=target_session,
            source_capture_id=source_capture_id,
            scenario=scenario,
            price_coordinate="FORWARD_ADJUSTED",
            research_cutoff=research_cutoff,
        )
        for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN")
    }
    recovery = scenario_pages["RECOVERY_TURN"]
    launch = scenario_pages["LAUNCH_CONFIRM"]
    if (
        recovery.get("producer_version") != "STOCK_A_D0_RECOVERY_BAOSTOCK_READER_R5"
        or launch.get("producer_version") != "STOCK_A_D0_LAUNCH_BAOSTOCK_READER_R5"
        or any(
            page.get("source_version", {}).get("source_capture_id") != source_capture_id
            or page.get("price_coordinate") != "FORWARD_ADJUSTED"
            or page.get("source_pool", {}).get("scenario_state_partition_closed")
            is not True
            for page in scenario_pages.values()
        )
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_D0_SCOPE_NOT_CLOSED", "冻结规则没有同捕获D0 R5完整来源池"
        )

    seed_rows = read_source_pool_seed_facts(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        profile_revision=profile_revision,
        producer_version=_SEED_PRODUCER_VERSION,
        cross_section_universe_capture_id=cross_section_universe_capture_id,
        cross_section_universe_version=cross_section_universe_version,
        cross_section_contract_version=cross_section_contract_version,
        universe_match_status=universe_match_status,
    )
    symbols = {row["source_symbol"] for row in launch["results"]}
    if (
        len(seed_rows) != len(symbols)
        or {row["source_symbol"] for row in seed_rows} != symbols
        or any(
            row.get("producer_version") != _SEED_PRODUCER_VERSION
            or row.get("profile_revision") != profile_revision
            or row.get("history_session_count") != history_session_count
            or row.get("price_coordinate") != "FORWARD_ADJUSTED"
            or row.get("research_mode") != "NON_PIT_RETROSPECTIVE"
            for row in seed_rows
        )
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SEED_SCOPE_NOT_CLOSED", "冻结规则没有唯一Seed R2 QFQ完整来源池"
        )

    seed_scope = seed_rows[0]
    seed_calendar_revision = seed_scope.get("calendar_revision")
    launch_calendar = launch.get("official_calendar")
    launch_calendar_id = (
        launch_calendar.get("calendar_id")
        if isinstance(launch_calendar, Mapping)
        else None
    )
    launch_calendar_revision = (
        launch_calendar.get("calendar_revision")
        if isinstance(launch_calendar, Mapping)
        else None
    )
    seed_calendar_id = seed_scope.get("calendar_id")
    seed_target_revision = _calendar_revision_for_target_year(
        seed_calendar_revision, target_session.year
    )
    launch_target_revision = _calendar_revision_for_target_year(
        launch_calendar_revision, target_session.year
    )
    if (
        seed_target_revision is None
        or launch_target_revision is None
        or not isinstance(launch_calendar_id, str)
        or not launch_calendar_id
        or seed_calendar_id != launch_calendar_id
        or seed_target_revision != launch_target_revision
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_CALENDAR_REVISION_MISMATCH", "D0或Seed缺少官方日历修订"
        )
    try:
        seed_target, seed_sessions, expected_seed_calendar_revision = (
            resolve_pg_history_window(target_session.isoformat(), history_session_count)
        )
    except ValueError as error:
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SEED_WINDOW_INVALID", "Seed画像窗口无法按官方会话解析"
        ) from error
    if (
        seed_target != target_session
        or len(seed_sessions) != history_session_count
        or not seed_sessions
        or seed_sessions[-1] != target_session
        or _calendar_revision_for_target_year(
            expected_seed_calendar_revision, target_session.year
        )
        != seed_target_revision
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SEED_WINDOW_INVALID",
            "Seed日历修订链未覆盖目标日和完整画像窗口",
        )
    profile_revision_parts = profile_revision.split("|")
    profile_calendar_id = (
        unquote(profile_revision_parts[2]) if len(profile_revision_parts) == 4 else None
    )
    profile_calendar_revision = (
        unquote(profile_revision_parts[3]) if len(profile_revision_parts) == 4 else None
    )
    if (
        profile_calendar_id != seed_calendar_id
        or profile_calendar_revision != expected_seed_calendar_revision
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SEED_WINDOW_INVALID",
            "Seed画像修订没有绑定其完整官方日历历史窗口",
        )

    evidence = read_source_launch_watch_evidence(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        expected_source_count=len(symbols),
        source_symbols=(min(symbols),),
        selected_profile_revision=profile_revision,
        selected_history_session_count=history_session_count,
        cross_section_universe_capture_id=cross_section_universe_capture_id,
        cross_section_universe_version=cross_section_universe_version,
        cross_section_contract_version=cross_section_contract_version,
        universe_match_status=universe_match_status,
    )
    d1_scope = evidence.get("d1_scope")
    if (
        evidence.get("d1_status") != "AVAILABLE"
        or not isinstance(d1_scope, Mapping)
        or d1_scope.get("producer_version")
        not in {"STOCK_A_D1_BAOSTOCK_QFQ_R3", "STOCK_A_D1_BAOSTOCK_QFQ_R4"}
        or d1_scope.get("research_mode") != "NON_PIT_RETROSPECTIVE"
        or d1_scope.get("price_coordinate") != "FORWARD_ADJUSTED"
        or not isinstance(d1_scope.get("calendar_revision"), str)
        or type(d1_scope.get("history_session_count")) is not int
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_D1_SCOPE_NOT_CLOSED",
            "冻结规则没有唯一完整的D1 QFQ五类或七类窗口",
        )
    d1_scope_tuple = (
        d1_scope.get("cross_section_universe_capture_id"),
        d1_scope.get("cross_section_universe_version"),
        d1_scope.get("cross_section_contract_version"),
    )
    if any(
        (
            row.get("cross_section_universe_capture_id"),
            row.get("cross_section_universe_version"),
            row.get("cross_section_contract_version"),
        )
        != d1_scope_tuple
        for row in seed_rows
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_SEED_D1_CROSS_SECTION_SCOPE_MISMATCH",
            "冻结规则Seed与D1保存事实横截面Universe范围不一致",
        )
    selected_scope_tuple = (
        cross_section_universe_capture_id,
        cross_section_universe_version,
        cross_section_contract_version,
    )
    if (
        any(value is not None for value in selected_scope_tuple)
        and d1_scope_tuple != selected_scope_tuple
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_D1_CROSS_SECTION_SCOPE_MISMATCH",
            "冻结规则D1保存事实与任务选定横截面Universe范围不一致",
        )
    rule_calendar_revision = d1_scope["calendar_revision"]
    d1_history_session_count = d1_scope["history_session_count"]
    _, d1_sessions, d1_calendar_revision = resolve_pg_history_window(
        target_session.isoformat(), d1_history_session_count
    )
    if (
        d1_calendar_revision != rule_calendar_revision
        or len(d1_sessions) != d1_history_session_count
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_D1_WINDOW_INVALID", "冻结规则D1事件窗口与官方日历不一致"
        )
    event_window_start = d1_sessions[0]

    focus_symbols: set[str] = set()
    if snapshot_ids is not None:
        source_version = read_source_version_context(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
        )
        if (
            source_version.get("status") != "AVAILABLE"
            or source_version.get("source_capture_id") != source_capture_id
            or source_version.get("price_coordinate") != "FORWARD_ADJUSTED"
            or source_version.get("version_status") != "COMPLETE"
            or source_version.get("time_role") != "NON_PIT_RETROSPECTIVE"
            or type(source_version.get("capture_expected_row_count")) is not int
        ):
            raise SourceFrozenRulesError(
                "SOURCE_RULE_SOURCE_SCOPE_NOT_CLOSED",
                "规则诊断所需的QFQ paired capture未闭合",
            )
        try:
            t0_inputs = read_source_t0_early_observation_inputs(
                config,
                target_session=target_session,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
                expected_source_count=source_version["research_expected_row_count"],
                snapshot_ids=snapshot_ids,
            )
        except T0EarlyObservationPgError as error:
            if error.code != "T0_EARLY_NO_STORED_ENROLLMENTS":
                raise SourceFrozenRulesError(error.code, str(error)) from error
        else:
            focus_symbols = {
                item["source_symbol"]
                for item in t0_inputs["registrations"]
                if item.get("d0_fact_id") is not None
            }
            if not focus_symbols.issubset(symbols):
                raise SourceFrozenRulesError(
                    "SOURCE_RULE_FOCUS_SYMBOL_SCOPE_MISMATCH",
                    "T0 Focus登记代码不在同捕获D0/Seed来源池",
                )

    if not include_diagnostics or not focus_symbols:
        base_states: dict[str, dict[str, dict[str, Any]]] = {
            rule: {} for rule in _ENROLLABLE_RULES
        }
        for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
            for item in scenario_pages[scenario]["results"]:
                base_states[scenario][item["source_symbol"]] = _rule_fact(
                    item["scenario_raw"],
                    [] if item["scenario_raw"] == "TRUE" else [item.get("main_reason")],
                )
        for row in seed_rows:
            for rule_id, state_key, reason_key in (
                ("s1", "s1_state", "s1_reasons"),
                ("s2", "s2_state", "s2_reasons"),
                ("seed", "source_seed_state", "source_seed_reasons"),
            ):
                base_states[rule_id][row["source_symbol"]] = _rule_fact(
                    row.get(state_key), row.get(reason_key)
                )
        if any(set(values) != symbols for values in base_states.values()):
            raise SourceFrozenRulesError(
                "SOURCE_RULE_COMPUTED_PARTITION_OPEN",
                "D0/Seed规则没有闭合到完整来源代码池",
            )
        base_rows = [
            _rule_row(
                target_session=target_session,
                source_capture_id=source_capture_id,
                source_symbol=symbol,
                rule_id=rule_id,
                profile_revision=profile_revision,
                history_session_count=history_session_count,
                d1_history_session_count=d1_history_session_count,
                event_window_start=event_window_start,
                calendar_revision=rule_calendar_revision,
                fact=fact,
            )
            for rule_id in _ENROLLABLE_RULES
            for symbol, fact in sorted(base_states[rule_id].items())
        ]
        resolved, adjacent, _ = resolve_pg_history_window(target_session.isoformat(), 2)
        if resolved != target_session or len(adjacent) != 2:
            raise SourceFrozenRulesError(
                "SOURCE_RULE_PREVIOUS_SESSION_INVALID",
                "规则登记缺少精确前一官方会话",
            )
        persistence = write_computed_rule_rows(
            config,
            rules=base_rows,
            rule_ids=_ENROLLABLE_RULES,
            expected_symbols_by_rule={rule: symbols for rule in _ENROLLABLE_RULES},
            enrollment_rule_ids=_ENROLLABLE_RULES,
            previous_session=adjacent[0],
        )
        counts_by_rule = {
            rule: dict(
                Counter(base_states[rule][symbol]["state"] for symbol in symbols)
            )
            for rule in _ENROLLABLE_RULES
        }
        diagnostic_reason = (
            "T0_FOCUS_DIAGNOSTIC_NOT_REQUESTED"
            if focus_symbols
            else "NO_T0_FOCUS_REGISTRATIONS"
        )
        return {
            "status": persistence["status"],
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "profile_revision": profile_revision,
            "history_session_count": history_session_count,
            "d1_history_session_count": d1_history_session_count,
            "event_window_start": event_window_start.isoformat(),
            "calendar_revision": rule_calendar_revision,
            "scope": SCOPE,
            "price_coordinate": "FORWARD_ADJUSTED",
            "source_symbol_count": len(symbols),
            "diagnostic_focus_symbol_count": len(focus_symbols),
            "supported_rule_ids": list(_ENROLLABLE_RULES),
            "rule_scope_counts": {rule: len(symbols) for rule in _ENROLLABLE_RULES},
            "skipped_rules": [
                {
                    "rule_id": rule,
                    "status": "SKIPPED_NOT_IMPLEMENTED",
                    "reason_code": "NO_CURRENT_FORMULA_PRODUCER",
                }
                for rule in _UNIMPLEMENTED_RULES
            ]
            + [
                {
                    "rule_id": rule,
                    "status": "NOT_COMPUTED",
                    "reason_code": diagnostic_reason,
                }
                for rule in _DIAGNOSTIC_RULES
            ],
            "state_counts_by_rule": counts_by_rule,
            "diagnostic_source_scope": diagnostic_reason,
            "persistence": persistence,
        }

    diagnostic_facts: dict[str, Any] = {}
    if focus_symbols:
        seed_rps = {
            row["source_symbol"]: {
                "rps5": _seed_rps_field(row, "rps5"),
                "rps5_delta3_pp": _seed_rps_field(row, "rps5_delta3"),
                "rps20": _seed_rps_field(row, "rps20"),
                "rps20_delta3_pp": _seed_rps_field(row, "rps20_delta3"),
            }
            for row in seed_rows
        }
        if set(seed_rps) != symbols:
            raise SourceFrozenRulesError(
                "SOURCE_RULE_SEED_RPS_SCOPE_NOT_CLOSED",
                "Seed RPS与完整来源池代码集合不一致",
            )
        day = {
            "session": target_session,
            "states": {symbol: {} for symbol in focus_symbols},
            "scenarios": scenario_pages,
            "seed": {
                "rps_read": {
                    "target_session": target_session,
                    "research_cutoff": research_cutoff,
                    "source_capture_id": source_capture_id,
                    "scope": "SOURCE_POOL_NON_PIT_EXPLORATORY",
                    "coordinate": "FORWARD_ADJUSTED",
                    "price_coordinate": "FORWARD_ADJUSTED",
                    "time_role": "NON_PIT_RETROSPECTIVE",
                    "corporate_action_continuity": "NON_PIT_RETROSPECTIVE",
                    "formal_research_universe": "UNKNOWN",
                    "calendar_revision": seed_calendar_revision,
                    "facts": {
                        symbol: seed_rps[symbol] for symbol in sorted(focus_symbols)
                    },
                }
            },
        }
        diagnostic = read_source_shape_cohort(
            config,
            day,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
        )
        diagnostic_facts = diagnostic.get("facts", {})
        if (
            diagnostic.get("scope") != "DIAGNOSTIC_SHAPE_NON_PIT_NOT_FORMAL_D0"
            or set(diagnostic_facts) != focus_symbols
        ):
            raise SourceFrozenRulesError(
                "SOURCE_RULE_DIAGNOSTIC_SCOPE_NOT_CLOSED",
                "回踩/续强诊断未闭合到当前T0 Focus登记代码",
            )

    state_maps: dict[str, dict[str, dict[str, Any]]] = {
        rule: {} for rule in _ENROLLABLE_RULES
    }
    for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
        rule_page = scenario_pages[scenario]
        for item in rule_page["results"]:
            state_maps[scenario][item["source_symbol"]] = _rule_fact(
                item["scenario_raw"],
                [] if item["scenario_raw"] == "TRUE" else [item.get("main_reason")],
            )
    for row in seed_rows:
        for rule_id, state_key, reason_key in (
            ("s1", "s1_state", "s1_reasons"),
            ("s2", "s2_state", "s2_reasons"),
            ("seed", "source_seed_state", "source_seed_reasons"),
        ):
            state_maps[rule_id][row["source_symbol"]] = _rule_fact(
                row.get(state_key), row.get(reason_key)
            )
    for symbol, facts in diagnostic_facts.items():
        for rule_id in _DIAGNOSTIC_RULES:
            item = facts[rule_id]
            state_maps.setdefault(rule_id, {})[symbol] = _rule_fact(
                item.get("state"), item.get("reasons")
            )
        state_maps["STRONG_PULLBACK_DIAGNOSTIC"][symbol]["event_count"] = int(
            facts.get("matched_parent_count") or 0
        )

    supported_rules = (
        (*_ENROLLABLE_RULES, *_DIAGNOSTIC_RULES) if focus_symbols else _ENROLLABLE_RULES
    )
    expected_symbols_by_rule = {
        **{rule: symbols for rule in _ENROLLABLE_RULES},
        **{rule: focus_symbols for rule in _DIAGNOSTIC_RULES if focus_symbols},
    }
    if any(
        set(state_maps.get(rule, {})) != expected_symbols_by_rule[rule]
        for rule in supported_rules
    ):
        raise SourceFrozenRulesError(
            "SOURCE_RULE_COMPUTED_PARTITION_OPEN",
            "可计算冻结规则没有闭合到声明的来源或Focus登记范围",
        )

    rows = [
        _rule_row(
            target_session=target_session,
            source_capture_id=source_capture_id,
            source_symbol=symbol,
            rule_id=rule_id,
            profile_revision=profile_revision,
            history_session_count=history_session_count,
            d1_history_session_count=d1_history_session_count,
            event_window_start=event_window_start,
            calendar_revision=rule_calendar_revision,
            fact=fact,
        )
        for rule_id in supported_rules
        for symbol, fact in sorted(state_maps[rule_id].items())
    ]
    resolved, adjacent, _ = resolve_pg_history_window(target_session.isoformat(), 2)
    if resolved != target_session or len(adjacent) != 2:
        raise SourceFrozenRulesError(
            "SOURCE_RULE_PREVIOUS_SESSION_INVALID", "规则登记缺少精确前一官方会话"
        )
    persistence = write_computed_rule_rows(
        config,
        rules=rows,
        rule_ids=supported_rules,
        expected_symbols_by_rule=expected_symbols_by_rule,
        enrollment_rule_ids=_ENROLLABLE_RULES,
        previous_session=adjacent[0],
    )
    counts_by_rule = {
        rule: dict(
            Counter(
                state_maps[rule][symbol]["state"]
                for symbol in expected_symbols_by_rule[rule]
            )
        )
        for rule in supported_rules
    }
    return {
        "status": persistence["status"],
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "profile_revision": profile_revision,
        "history_session_count": history_session_count,
        "d1_history_session_count": d1_history_session_count,
        "event_window_start": event_window_start.isoformat(),
        "calendar_revision": rule_calendar_revision,
        "scope": SCOPE,
        "price_coordinate": "FORWARD_ADJUSTED",
        "source_symbol_count": len(symbols),
        "supported_rule_ids": list(supported_rules),
        "rule_scope_counts": {
            rule: len(scope) for rule, scope in expected_symbols_by_rule.items()
        },
        "skipped_rules": [
            {
                "rule_id": rule,
                "status": "SKIPPED_NOT_IMPLEMENTED",
                "reason_code": "NO_CURRENT_FORMULA_PRODUCER",
            }
            for rule in _UNIMPLEMENTED_RULES
        ]
        + (
            []
            if focus_symbols
            else [
                {
                    "rule_id": rule,
                    "status": "NOT_COMPUTED",
                    "reason_code": "NO_T0_FOCUS_REGISTRATIONS",
                }
                for rule in _DIAGNOSTIC_RULES
            ]
        ),
        "state_counts_by_rule": counts_by_rule,
        "diagnostic_source_scope": (
            "DIAGNOSTIC_SHAPE_NON_PIT_NOT_FORMAL_D0_REGISTRATIONS"
            if focus_symbols
            else "NOT_COMPUTED_NO_T0_FOCUS_REGISTRATIONS"
        ),
        "diagnostic_focus_symbol_count": len(focus_symbols),
        "persistence": persistence,
    }


__all__ = ["SourceFrozenRulesError", "persist_source_computable_rules_from_saved"]
