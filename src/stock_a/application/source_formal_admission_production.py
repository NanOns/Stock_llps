"""从唯一PG已存D0/Core/Profile事实生产LAUNCH正式准入。"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any

from stock_a.application.source_d0_raw import _official_calendar
from stock_a.application.source_formal_admission import (
    prepare_source_d0_launch_formal_admission,
)
from stock_a.domain import FieldValue, unknown
from stock_a.research.d0_price_facts import calculate_d0_first_day_damage
from stock_a.research.entry_history.support import trade_status_state_from_pg_row
from stock_a.application.pg_research import read_source_version_context
from stock_a.storage.postgres.baostock_price_facts import (
    read_baostock_price_history_batch,
)
from stock_a.storage.postgres.formal_admission import (
    list_saved_launch_sessions,
    persist_source_formal_admission_rows,
    read_saved_source_fact_ids,
    read_source_formal_admission_day,
)
from stock_a.storage.postgres.static_research_facts import (
    read_source_d0_scenario_day,
    read_source_pool_seed_facts,
    read_source_pool_seed_forward_scope,
)


class SourceFormalAdmissionProductionError(ValueError):
    """正式准入生产输入缺失、不唯一或与冻结合同不符。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _fraction(value: object) -> Fraction | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, Fraction):
        return value
    if isinstance(value, Decimal):
        return Fraction(value) if value.is_finite() else None
    if isinstance(value, int):
        return Fraction(value)
    return None


def _date(value: object) -> date:
    if type(value) is date:
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise SourceFormalAdmissionProductionError(
        "FORMAL_ADMISSION_DATE_INVALID", "已存D0日期字段不是有效日期"
    )


def _aware_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise SourceFormalAdmissionProductionError(
                "FORMAL_ADMISSION_CAPTURE_TIME_INVALID", "来源捕获时间缺少时区"
            )
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise SourceFormalAdmissionProductionError(
                "FORMAL_ADMISSION_CAPTURE_TIME_INVALID", "来源捕获时间缺少时区"
            )
        return parsed
    raise SourceFormalAdmissionProductionError(
        "FORMAL_ADMISSION_CAPTURE_TIME_INVALID", "来源捕获时间类型无效"
    )


def _seed_actual_quote_gate(seed: Mapping[str, Any]) -> FieldValue:
    """优先读取Seed actual_bar三态，兼容早期仅保存布尔投影的行。"""
    predicate_ids = seed.get("predicate_ids")
    predicate_states = seed.get("predicate_states")
    predicate_reasons = seed.get("predicate_reasons")
    if (
        isinstance(predicate_ids, (list, tuple))
        and isinstance(predicate_states, (list, tuple))
        and isinstance(predicate_reasons, (list, tuple))
        and len(predicate_ids) == len(predicate_states) == len(predicate_reasons)
        and "actual_bar" in predicate_ids
    ):
        index = predicate_ids.index("actual_bar")
        state = predicate_states[index]
        if state == "TRUE":
            return FieldValue(True)
        if state == "FALSE":
            return FieldValue(False)
        if state == "UNKNOWN":
            return unknown(str(predicate_reasons[index] or "TRADE_STATUS_UNKNOWN"))
    value = seed.get("target_actual_quote")
    if type(value) is bool:
        return FieldValue(value)
    return unknown("TARGET_ACTUAL_QUOTE_NOT_COMPUTED")


def _negated_bool_gate(
    value: object,
    quality: object,
    reason: object,
    *,
    unknown_reason: str,
) -> FieldValue:
    if quality != "OBSERVED" or type(value) is not bool:
        return unknown(str(reason or unknown_reason))
    return FieldValue(not value)


def _raw_history_for_candidates(
    *,
    config,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    candidates: list[str],
    sessions: tuple[date, ...],
) -> dict[str, dict[date, dict[str, Any]]]:
    histories: dict[str, dict[date, dict[str, Any]]] = {}
    for start in range(0, len(candidates), 100):
        symbols = candidates[start : start + 100]
        offset = 0
        total = None
        batch_rows = 0
        while True:
            batch = read_baostock_price_history_batch(
                config,
                source_symbols=symbols,
                sessions=sessions,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                cutoff=research_cutoff,
                offset=offset,
                limit=1000,
            )
            if (
                batch.get("source_capture_id") != source_capture_id
                or batch.get("price_coordinate") != "FORWARD_ADJUSTED"
            ):
                raise SourceFormalAdmissionProductionError(
                    "FORMAL_ADMISSION_QFQ_HISTORY_IDENTITY_INVALID",
                    "QFQ历史批次没有绑定请求的paired capture。",
                )
            page = batch.get("page", {})
            if total is None:
                total = int(page.get("total", -1))
            elif int(page.get("total", -1)) != total:
                raise SourceFormalAdmissionProductionError(
                    "FORMAL_ADMISSION_QFQ_HISTORY_DENOMINATOR_CHANGED",
                    "正式准入QFQ历史分页期间分母发生变化。",
                )
            if any(
                item.get("coverage_status") != "COMPLETE"
                for item in batch.get("coverage", ())
            ):
                raise SourceFormalAdmissionProductionError(
                    "FORMAL_ADMISSION_QFQ_HISTORY_INCOMPLETE",
                    "正式准入QFQ历史会话没有完整请求与行覆盖。",
                )
            for row in batch.get("rows", ()):
                history = histories.setdefault(row["source_symbol"], {})
                if row["session_date"] in history:
                    raise SourceFormalAdmissionProductionError(
                        "FORMAL_ADMISSION_QFQ_HISTORY_DUPLICATE",
                        "正式准入QFQ历史会话重复。",
                    )
                history[row["session_date"]] = {
                    key: row.get(key)
                    for key in (
                        "source_symbol",
                        "session_date",
                        "source_capture_id",
                        "bar_status",
                        "bar_reason",
                        "qfq_bar_status",
                        "qfq_bar_reason",
                        "qfq_price_quality",
                        "qfq_price_reason",
                        "qfq_tradestatus_value",
                        "qfq_tradestatus_quality",
                        "qfq_tradestatus_reason",
                        "qfq_price_coordinate",
                        "price_coordinate",
                        "qfq_open",
                        "qfq_high",
                        "qfq_low",
                        "qfq_close",
                    )
                }
                batch_rows += 1
            next_offset = page.get("next_offset")
            if next_offset is None:
                break
            offset = int(next_offset)
        expected_rows = sum(
            int(item.get("expected_symbol_count", 0))
            for item in batch.get("coverage", ())
        )
        if total is None or batch_rows != total or total != expected_rows:
            coverage = batch.get("coverage", ())
            coverage_gaps = [
                {
                    "session_date": item.get("session_date"),
                    "expected": item.get("expected_symbol_count"),
                    "rows": item.get("row_count"),
                    "symbols": item.get("symbol_count"),
                    "quality_rows": item.get("quality_row_count"),
                }
                for item in coverage
                if item.get("coverage_status") != "COMPLETE"
                or item.get("expected_symbol_count") != item.get("quality_row_count")
            ][:5]
            raise SourceFormalAdmissionProductionError(
                "FORMAL_ADMISSION_QFQ_HISTORY_INCOMPLETE",
                "正式准入QFQ历史分页未闭合："
                f"候选数={len(symbols)}，批次起始索引={start}，"
                f"分页数={offset // 1000 + 1}，最后offset={offset}，"
                f"total={total}，累计行={batch_rows}，"
                f"覆盖期望行={expected_rows}，缺口样例={coverage_gaps}。",
            )
    return histories


def _line_inputs(
    *,
    symbol: str,
    target_session: date,
    source_capture_id: int,
    sessions: tuple[date, ...],
    history: dict[date, dict[str, Any]],
) -> tuple[FieldValue, FieldValue, Any]:
    close = None
    phc20 = None
    current = history.get(target_session)
    current_trade_state, current_trade_reason = trade_status_state_from_pg_row(
        current, price_coordinate="FORWARD_ADJUSTED"
    )
    if (
        current is not None
        and current.get("source_capture_id") == source_capture_id
        and current.get("qfq_price_quality") == "OBSERVED"
        and current_trade_state == "ACTUAL"
    ):
        close = _fraction(current.get("qfq_close"))
    if close is None:
        close_field = unknown(
            current_trade_reason or "T0_QFQ_CLOSE_NOT_OBSERVED_IN_PAIRED_CAPTURE"
        )
    else:
        close_field = FieldValue(close)

    prior_closes: list[Fraction] = []
    for session in sessions[:-1]:
        row = history.get(session)
        if (
            row is None
            or row.get("session_date") != session
            or row.get("source_symbol") != symbol
            or row.get("source_capture_id") != source_capture_id
            or row.get("qfq_bar_status") != "TARGET_PRESENT"
            or row.get("qfq_price_quality") != "OBSERVED"
            or trade_status_state_from_pg_row(row, price_coordinate="FORWARD_ADJUSTED")[
                0
            ]
            != "ACTUAL"
        ):
            prior_closes = []
            break
        value = _fraction(row.get("qfq_close"))
        if value is None or value <= 0:
            prior_closes = []
            break
        prior_closes.append(value)
    phc20 = max(prior_closes) if len(prior_closes) == 20 else None
    phc_field = (
        FieldValue(phc20)
        if phc20 is not None
        else unknown("STRICT_PRIOR_20_CLOSES_NOT_COMPLETE")
    )
    by_session = {session: history.get(session) for session in sessions}
    damage = calculate_d0_first_day_damage(
        target_session=target_session,
        official_sessions=sessions,
        history_rows_by_session=by_session,
        source_symbol=symbol,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    return close_field, phc_field, damage


def run_source_formal_admission_from_saved(
    config,
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revision: str | None = None,
    persist: bool,
) -> dict[str, Any]:
    """处理一个目标日；只消费该日已闭合Raw、D0、Core和Profile事实。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or persist is not True
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_ARGUMENT_INVALID",
            "准入生产必须显式绑定截止内的paired BaoStock capture并持久化",
        )

    source = read_source_version_context(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    if (
        source.get("status") != "AVAILABLE"
        or source.get("version_status") != "COMPLETE"
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_RAW_SOURCE_NOT_CLOSED",
            "目标日paired BaoStock双坐标来源分区未闭合，不生成准入分母",
        )
    source_capture_id = source["source_capture_id"]
    expected_count = source["capture_expected_row_count"]
    d0 = read_source_d0_scenario_day(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        scenario="LAUNCH_CONFIRM",
        price_coordinate="FORWARD_ADJUSTED",
        research_cutoff=research_cutoff,
    )
    if (
        d0["source_pool"]["source_count"] != expected_count
        or d0["source_version"]["source_capture_id"] != source_capture_id
        or d0["session_contract"] != "D0_SESSION_R2"
        or d0["history_session_count"] != 21
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_D0_SCOPE_MISMATCH",
            "已存D0原始事实与目标Raw捕获、完整分母或严格21会话窗口不一致",
        )

    calendar = _official_calendar(target_session)
    official_sessions = tuple(calendar["sessions"])
    if target_session not in official_sessions:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_TARGET_NOT_OFFICIAL_SESSION",
            "D0目标日期不在现役官方交易日历内",
        )
    target_ordinal = official_sessions.index(target_session)
    if target_ordinal < 20:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_WINDOW_LEFT_TRUNCATED",
            "目标日没有20个严格前序官方交易会话",
        )
    sessions = official_sessions[target_ordinal - 20 : target_ordinal + 1]
    if (
        len(sessions) != 21
        or sessions[-1] != target_session
        or sessions[-1] > research_cutoff
        or sessions[0] != _date(d0["history_window_start"])
    ):
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_WINDOW_MISMATCH",
            "官方日历与已存D0严格前20会话窗口不匹配",
        )
    next_session = (
        official_sessions[target_ordinal + 1]
        if target_ordinal + 1 < len(official_sessions)
        else None
    )

    seed_scope: dict[str, Any] | None = None
    seed_by_symbol: dict[str, dict[str, Any]] = {}
    seed_scope_reason = "SEED_FACT_NOT_COMPUTED_FOR_EXACT_INPUT"
    try:
        seed_scope = read_source_pool_seed_forward_scope(
            config,
            target_session=target_session,
            source_capture_id=source_capture_id,
            expected_source_count=expected_count,
            profile_revision=profile_revision,
        )
        if (
            seed_scope["source_symbols"]
            != tuple(sorted(item["source_symbol"] for item in d0["results"]))
            or seed_scope["calendar_id"] != d0["official_calendar"]["calendar_id"]
            or seed_scope["calendar_revision"]
            != d0["official_calendar"]["calendar_revision"]
        ):
            seed_scope = None
            seed_scope_reason = "SEED_CALENDAR_OR_SYMBOL_SCOPE_MISMATCH"
        else:
            seed_rows = read_source_pool_seed_facts(
                config,
                target_session=target_session,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                profile_revision=seed_scope["profile_revision"],
                producer_version=seed_scope["producer_version"],
            )
            seed_by_symbol = {row["source_symbol"]: row for row in seed_rows}
            if len(seed_by_symbol) != expected_count:
                seed_scope = None
                seed_by_symbol = {}
                seed_scope_reason = "SEED_SOURCE_PARTITION_NOT_CLOSED"
    except (ValueError, KeyError) as error:
        seed_scope = None
        seed_by_symbol = {}
        seed_scope_reason = getattr(error, "code", "SEED_SCOPE_UNAVAILABLE")

    profile_revision = seed_scope["profile_revision"] if seed_scope else None
    seed_version = seed_scope["producer_version"] if seed_scope else None
    input_refs = read_saved_source_fact_ids(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        producer_version=d0["producer_version"],
        algorithm_contract=d0["algorithm_contract"],
        session_contract=d0["session_contract"],
        calendar_revision=d0["official_calendar"]["calendar_revision"],
        profile_revision=profile_revision,
        seed_producer_version=seed_version,
        expected_source_count=expected_count,
    )
    if set(input_refs) != {item["source_symbol"] for item in d0["results"]}:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_RAW_SYMBOL_SET_MISMATCH",
            "原始D0事实行编号与已闭合来源代码集合不一致",
        )

    raw_true_symbols = sorted(
        item["source_symbol"]
        for item in d0["results"]
        if item["scenario_raw"] == "TRUE"
    )
    histories = (
        _raw_history_for_candidates(
            config=config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            candidates=raw_true_symbols,
            sessions=sessions,
        )
        if raw_true_symbols
        else {}
    )

    window_ok = (
        d0["history_session_count"] == 21
        and d0["official_calendar"]["calendar_id"] == calendar["calendar_id"]
        and d0["official_calendar"]["calendar_revision"]
        == calendar["calendar_revision"]
        and d0["source_version"]["source_capture_id"] == source_capture_id
        and source["price_coordinate"] == "FORWARD_ADJUSTED"
    )
    rows: list[dict[str, Any]] = []
    raw_counts: Counter[str] = Counter()
    gate_unknown_counts: Counter[str] = Counter()
    gate_blocked_counts: Counter[str] = Counter()
    for fact in d0["results"]:
        symbol = fact["source_symbol"]
        raw_state = fact["scenario_raw"]
        raw_counts[raw_state] += 1
        seed = seed_by_symbol.get(symbol)
        if raw_state == "TRUE":
            history = histories.get(symbol, {})
            close, phc20, damage = _line_inputs(
                symbol=symbol,
                target_session=target_session,
                source_capture_id=source_capture_id,
                sessions=sessions,
                history=history,
            )
            if not window_ok:
                coordinate_gate = unknown(
                    "D0_CALENDAR_COORDINATE_OR_CAPTURE_NOT_PROVED"
                )
            else:
                exact_history = all(
                    session in history
                    and history[session].get("qfq_price_quality") == "OBSERVED"
                    and history[session].get("source_symbol") == symbol
                    and history[session].get("source_capture_id") == source_capture_id
                    and (
                        session != target_session
                        or history[session].get("source_capture_id")
                        == source_capture_id
                    )
                    for session in sessions
                )
                coordinate_gate = unknown(
                    "QFQ_NON_PIT_COORDINATE_NOT_CLOSED"
                    if exact_history
                    else "STRICT_D0_WINDOW_NOT_REPRODUCED_FROM_SAVED_RAW"
                )
            if seed is None:
                actual_gate = unknown(seed_scope_reason)
                core_gate = unknown(seed_scope_reason)
                extension_gate = unknown(seed_scope_reason)
            else:
                actual_gate = _seed_actual_quote_gate(seed)
                core_gate = _negated_bool_gate(
                    seed.get("core_price_damage"),
                    seed.get("core_price_damage_quality"),
                    seed.get("core_price_damage_reason"),
                    unknown_reason="CORE_DAMAGE_NOT_COMPUTED",
                )
                extension_value = seed.get("profile_extension_value")
                extension_quality = seed.get("profile_extension_quality")
                if extension_quality != "OBSERVED" or extension_value not in {
                    "LOW",
                    "MEDIUM",
                    "HIGH",
                    "EXTREME",
                }:
                    extension_gate = unknown(
                        str(
                            seed.get("profile_extension_reason")
                            or "PROFILE_EXTENSION_NOT_COMPUTED"
                        )
                    )
                else:
                    extension_gate = FieldValue(extension_value != "EXTREME")
            if damage.state == "UNKNOWN":
                first_day_gate = unknown(damage.reason_code)
            else:
                first_day_gate = FieldValue(damage.state == "FALSE")
            gates = {
                "research_universe": unknown("OFFICIAL_RESEARCH_UNIVERSE_NOT_STORED"),
                "actual_quote": actual_gate,
                "coordinate_window": coordinate_gate,
                "d0_liquidity": unknown("RAW_AMOUNT_UNIT_UNVERIFIED"),
                "non_core_damage": core_gate,
                "non_extreme_extension": extension_gate,
                "non_first_day_damage": first_day_gate,
                "non_severe_drop": FieldValue(True)
                if fact["predicates"]["positive_ret1"]["state"] == "TRUE"
                else unknown("POSITIVE_RET1_NOT_PROVED_FOR_SEVERE_DROP_GATE"),
            }
        else:
            close = unknown("RAW_NOT_TRUE_NO_T0_CLOSE_BINDING")
            phc20 = unknown("RAW_NOT_TRUE_NO_T0_PHC20_FREEZE")
            gates = {
                name: unknown("RAW_NOT_TRUE_GATES_NOT_EVALUATED")
                for name in (
                    "research_universe",
                    "actual_quote",
                    "coordinate_window",
                    "d0_liquidity",
                    "non_core_damage",
                    "non_extreme_extension",
                    "non_first_day_damage",
                    "non_severe_drop",
                )
            }
        gate_reason_labels = {
            "research_universe": {
                "TRUE": "OFFICIAL_RESEARCH_UNIVERSE_PROVED",
                "FALSE": "OUTSIDE_RESEARCH_UNIVERSE",
                "UNKNOWN": "OFFICIAL_RESEARCH_UNIVERSE_NOT_STORED",
            },
            "actual_quote": {
                "TRUE": "TARGET_ACTUAL_QUOTE_TRUE",
                "FALSE": "TARGET_ACTUAL_QUOTE_FALSE",
                "UNKNOWN": "TARGET_ACTUAL_QUOTE_NOT_COMPUTED",
            },
            "coordinate_window": {
                "TRUE": "D0_COORDINATE_WINDOW_MATCHED",
                "FALSE": "D0_COORDINATE_WINDOW_INVALID",
                "UNKNOWN": "QFQ_NON_PIT_COORDINATE_NOT_CLOSED",
            },
            "d0_liquidity": {
                "TRUE": "D0_LIQ20_THRESHOLD_PROVED",
                "FALSE": "D0_LIQ20_BELOW_THRESHOLD",
                "UNKNOWN": "RAW_AMOUNT_UNIT_UNVERIFIED",
            },
            "non_core_damage": {
                "TRUE": "CORE_DAMAGE_FALSE",
                "FALSE": "CORE_DAMAGE_TRUE",
                "UNKNOWN": "CORE_DAMAGE_NOT_COMPUTED",
            },
            "non_extreme_extension": {
                "TRUE": "PROFILE_EXTENSION_NOT_EXTREME",
                "FALSE": "PROFILE_EXTENSION_EXTREME_PROVED",
                "UNKNOWN": "PROFILE_EXTENSION_NOT_COMPUTED",
            },
            "non_first_day_damage": {
                "TRUE": "FIRST_DAY_DAMAGE_FALSE",
                "FALSE": "FIRST_DAY_DAMAGE_TRUE",
                "UNKNOWN": "FIRST_DAY_DAMAGE_NOT_COMPUTED",
            },
            "non_severe_drop": {
                "TRUE": "SEVERE_DROP_EXCLUDED_BY_POSITIVE_RET1",
                "FALSE": "SEVERE_DROP_PROVED",
                "UNKNOWN": "SEVERE_DROP_NOT_COMPUTED",
            },
        }
        for name, gate in gates.items():
            if raw_state == "TRUE":
                if gate.quality != "OBSERVED":
                    gate_unknown_counts[gate.reason or name] += 1
                elif gate.value is False:
                    gate_blocked_counts[gate_reason_labels[name]["FALSE"]] += 1
        refs = input_refs[symbol]
        evidence_ref = (
            f"PG:{target_session.isoformat()}:{source_capture_id}:"
            f"{source_capture_id}:{symbol}:D0_FACT:{refs['raw_fact_id']}"
        )
        prepared = prepare_source_d0_launch_formal_admission(
            entity_id=symbol,
            target_session=target_session,
            source_capture_id=source_capture_id,
            raw_fact_id=refs["raw_fact_id"],
            raw_state=raw_state,
            raw_reason=fact["main_reason"],
            raw_role="FORMAL",
            gates=gates,
            gate_reasons=gate_reason_labels,
            close=close,
            phc20=phc20,
            session_ordinal=target_ordinal,
            next_session=next_session,
            window_start=sessions[0],
            evidence_ref=evidence_ref,
        )
        line = prepared["frozen_line"]
        row = {
            "raw_fact_id": refs["raw_fact_id"],
            "seed_fact_id": refs["seed_fact_id"],
            "source_capture_id": source_capture_id,
            "target_session": target_session,
            "source_symbol": symbol,
            "scenario": "LAUNCH_CONFIRM",
            "raw_role": prepared["raw_role"],
            "raw_state": prepared["raw_state"],
            "raw_reason": prepared["raw_reason"],
            "raw_producer_version": d0["producer_version"],
            "algorithm_contract": d0["algorithm_contract"],
            "session_contract": d0["session_contract"],
            "history_window_start": sessions[0],
            "history_session_count": 21,
            "source_count": expected_count,
            "calendar_id": d0["official_calendar"]["calendar_id"],
            "calendar_revision": d0["official_calendar"]["calendar_revision"],
            "source_time_quality": d0["source_version"]["source_time_quality"],
            "source_clock_role": d0["source_version"]["source_clock_role"],
            "source_available_at": _aware_datetime(
                d0["source_version"]["source_available_at"]
            ),
            "capture_started_at": _aware_datetime(source["capture_started_at"]),
            "capture_validated_at": _aware_datetime(source["capture_validated_at"]),
            "profile_revision": refs["profile_revision"],
            "seed_producer_version": refs["seed_producer_version"],
            "identity_scope": "SOURCE_SYMBOL_ONLY",
            "research_mode": "NON_PIT_EXPLORATORY",
            "price_coordinate": "FORWARD_ADJUSTED",
            "admission_contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
            "admission_status": prepared["admission_status"],
            "admission_reason": prepared["admission_reason"],
            "research_universe_state": prepared["gates"]["research_universe"]["state"],
            "research_universe_reason": prepared["gates"]["research_universe"][
                "reason"
            ],
            "actual_quote_state": prepared["gates"]["actual_quote"]["state"],
            "actual_quote_reason": prepared["gates"]["actual_quote"]["reason"],
            "coordinate_window_state": prepared["gates"]["coordinate_window"]["state"],
            "coordinate_window_reason": prepared["gates"]["coordinate_window"][
                "reason"
            ],
            "d0_liquidity_state": prepared["gates"]["d0_liquidity"]["state"],
            "d0_liquidity_reason": prepared["gates"]["d0_liquidity"]["reason"],
            "non_core_damage_state": prepared["gates"]["non_core_damage"]["state"],
            "non_core_damage_reason": prepared["gates"]["non_core_damage"]["reason"],
            "non_extreme_extension_state": prepared["gates"]["non_extreme_extension"][
                "state"
            ],
            "non_extreme_extension_reason": prepared["gates"]["non_extreme_extension"][
                "reason"
            ],
            "non_first_day_damage_state": prepared["gates"]["non_first_day_damage"][
                "state"
            ],
            "non_first_day_damage_reason": prepared["gates"]["non_first_day_damage"][
                "reason"
            ],
            "non_severe_drop_state": prepared["gates"]["non_severe_drop"]["state"],
            "non_severe_drop_reason": prepared["gates"]["non_severe_drop"]["reason"],
            "reason_codes": prepared["reason_codes"],
            "event_id": prepared["event_id"],
            "failure_level_numerator": line["numerator"] if line else None,
            "failure_level_denominator": line["denominator"] if line else None,
            "failure_coordinate": line["coordinate"] if line else None,
            "failure_source": line["source"] if line else None,
            "failure_window_start": sessions[0] if line else None,
            "failure_window_session_count": 20 if line else None,
            "failure_source_event_ref": line["source_event_ref"] if line else None,
            "effective_from_session": _date(line["effective_from_session"])
            if line
            else None,
        }
        rows.append(row)

    result = persist_source_formal_admission_rows(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        expected_source_count=expected_count,
        rows=rows,
    )
    readback = read_source_formal_admission_day(
        config,
        target_session=target_session,
        source_capture_id=source_capture_id,
        expected_source_count=expected_count,
        profile_history_session_count=(
            seed_scope["history_session_count"] if seed_scope else None
        ),
        profile_revision=profile_revision,
    )
    if readback.get("status") != "AVAILABLE" or readback.get("counts") != {
        "READY": result["ready_count"],
        "BLOCKED": result["blocked_count"],
        "NOT_READY": result["not_ready_count"],
        "NOT_REQUIRED": result["not_required_count"],
    }:
        raise SourceFormalAdmissionProductionError(
            "FORMAL_ADMISSION_INDEPENDENT_READBACK_MISMATCH",
            "正式准入事实提交后的独立只读分区或状态计数不匹配",
        )
    return {
        **result,
        "input_status": "SAVED_D0_AND_RAW_SCOPE_CLOSED",
        "source_capture_id": source_capture_id,
        "raw_producer_version": d0["producer_version"],
        "producer_version": d0["producer_version"],
        "calendar_revision": d0["official_calendar"]["calendar_revision"],
        "expected_source_count": expected_count,
        "verified_rows": expected_count,
        "raw_counts": dict(raw_counts),
        "gate_blocked_reason_counts": dict(sorted(gate_blocked_counts.items())),
        "gate_unknown_reason_counts": dict(sorted(gate_unknown_counts.items())),
        "identity_scope": "SOURCE_SYMBOL_ONLY",
        "research_mode": "NON_PIT_EXPLORATORY",
        "price_coordinate": "FORWARD_ADJUSTED",
        "time_role": "NON_PIT_RETROSPECTIVE",
        "source_time_quality": source["source_time_quality"],
        "source_clock_role": source["source_clock_role"],
        "source_available_at": source["source_available_at"],
        "profile_revision": profile_revision,
        "seed_scope_status": "AVAILABLE" if seed_scope else "UNKNOWN",
        "seed_scope_reason": None if seed_scope else seed_scope_reason,
        "independent_readback": {
            "status": readback["status"],
            "counts": readback["counts"],
            "transaction": readback["transaction"],
            "database_write_attempted": False,
        },
        "raw_history_rows_read": sum(len(rows) for rows in histories.values()),
        "raw_history_batches": (len(raw_true_symbols) + 99) // 100,
        "future_quote_rows_read": 0,
        "research_cutoff": research_cutoff.isoformat(),
    }


def run_source_formal_admission_range_from_saved(
    config,
    *,
    start_session: date,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    profile_revisions_by_session: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """仅生产指定范围内PG已经完成的LAUNCH D0分区。"""
    sessions = list_saved_launch_sessions(
        config,
        start_session=start_session,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
    )
    if not sessions:
        return {
            "status": "NOT_COMPUTED",
            "reason_code": "SAVED_LAUNCH_D0_NOT_FOUND_FOR_RANGE",
            "start_session": start_session.isoformat(),
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "completed_day_count": 0,
            "database_write_attempted": False,
        }
    reports = [
        run_source_formal_admission_from_saved(
            config,
            target_session=day,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            profile_revision=(
                profile_revisions_by_session.get(day.isoformat())
                if isinstance(profile_revisions_by_session, Mapping)
                else None
            ),
            persist=True,
        )
        for day in sessions
    ]
    return {
        "status": "PERSISTED",
        "contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
        "start_session": start_session.isoformat(),
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "completed_day_count": len(reports),
        "reports": reports,
        "database_write_attempted": True,
        "ddl_executed": False,
        "dml_executed": True,
    }


__all__ = [
    "SourceFormalAdmissionProductionError",
    "run_source_formal_admission_from_saved",
    "run_source_formal_admission_range_from_saved",
]
