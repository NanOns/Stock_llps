"""来源池Seed原始谓词及板块完整成员的只读研究消费。"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import date
from fractions import Fraction
from math import sqrt
from statistics import median
from typing import Any, Mapping

from stock_a.application.pg_price_profiles import (
    PgPriceProfileError,
    SOURCE_SYMBOL_PROFILE_SCOPE,
    _profile_revision,
    _profile_semantics_sha256,
    query_source_symbol_price_profiles,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.tdx_sector_members import (
    _raw_window_observation,
    read_tdx_sector_members,
)
from stock_a.research.core import MarketSessionCalendar, calculate_core
from stock_a.research.entry_history.history_preview import (
    research_stock_pg_history_preview,
)
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_raw import read_raw_daily_history_page


def _tri(value: bool | None, reason: str | None = None) -> dict[str, Any]:
    return {
        "state": "UNKNOWN" if value is None else "TRUE" if value else "FALSE",
        "reason": reason,
    }


def _and(*values: dict[str, Any]) -> dict[str, Any]:
    state = (
        "FALSE"
        if any(v["state"] == "FALSE" for v in values)
        else "UNKNOWN"
        if any(v["state"] == "UNKNOWN" for v in values)
        else "TRUE"
    )
    return {"state": state, "reasons": _reasons(values)}


def _or(*values: dict[str, Any]) -> dict[str, Any]:
    state = (
        "TRUE"
        if any(v["state"] == "TRUE" for v in values)
        else "UNKNOWN"
        if any(v["state"] == "UNKNOWN" for v in values)
        else "FALSE"
    )
    return {"state": state, "reasons": _reasons(values)}


def _tri_state(value: Mapping[str, Any] | str) -> str:
    if isinstance(value, str):
        return value
    return str(value.get("state", "UNKNOWN"))


def _state_counts(values) -> dict[str, int]:
    counts = Counter(_tri_state(value) for value in values)
    return {state: counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")}


def _reasons(values: tuple[dict[str, Any], ...]) -> list[str]:
    return list(
        dict.fromkeys(
            reason
            for value in values
            for reason in (
                [value.get("reason")]
                if value.get("reason")
                else value.get("reasons", [])
            )
            if reason
        )
    )


def _from_field(
    field: Mapping[str, Any], predicate, *, false_reason: str | None = None
) -> dict[str, Any]:
    if field.get("quality") != "OBSERVED":
        return _tri(None, str(field.get("reason") or "FIELD_UNKNOWN"))
    value = field.get("value")
    if isinstance(value, dict):
        value = Fraction(value["exact"])
    result = bool(predicate(value))
    return _tri(result, None if result else false_reason)


def source_qualification_explanation(
    predicates: Mapping[str, Mapping[str, Any]],
    *,
    s1: Mapping[str, Any],
    s2: Mapping[str, Any],
    safety: Mapping[str, Any],
    seed: Mapping[str, Any],
    formal_candidate_seed: Mapping[str, Any],
    source_seed: Mapping[str, Any],
    prewatch: Mapping[str, Any],
) -> dict[str, Any]:
    """以同一组三态谓词说明来源形态、正式门、排除与缺证。"""
    public_conditions = (
        "research_universe",
        "actual_bar",
        "non_st",
        "normal_listing_phase",
        "limited_price_rule",
        "coordinate_ready",
    )
    seed_safety_conditions = (
        "research_universe",
        "actual_bar",
        "coordinate_ready",
        "liquidity_mean20",
        "no_damage",
        "no_severe_extension",
    )
    public_gate = _and(*(dict(predicates[key]) for key in public_conditions))
    public_failures = [
        {"condition": key, "reason": predicates[key].get("reason")}
        for key in public_conditions
        if _tri_state(predicates[key]) == "FALSE"
    ]
    seed_failures = [
        {"condition": key, "reason": predicates[key].get("reason")}
        for key in seed_safety_conditions
        if _tri_state(predicates[key]) == "FALSE"
    ]
    route_failures = []
    for route_name, route in (("S1", s1), ("S2", s2)):
        if _tri_state(route) == "FALSE":
            route_failures.append(
                {
                    "route": route_name,
                    "reasons": list(route.get("reasons", [])),
                }
            )

    relevant = list(dict.fromkeys(public_conditions + seed_safety_conditions))
    unknown_gaps = [
        {"condition": key, "reason": predicates[key].get("reason")}
        for key in relevant
        if _tri_state(predicates[key]) == "UNKNOWN"
    ]
    for route_name, route in (("S1", s1), ("S2", s2)):
        if _tri_state(route) == "UNKNOWN":
            unknown_gaps.append(
                {
                    "condition": route_name,
                    "reason": list(route.get("reasons", [])),
                }
            )

    first_exclusion = next(iter(public_failures or seed_failures), None)
    if first_exclusion:
        primary = {
            "kind": "PROVED_FORMAL_GATE_EXCLUSION",
            **first_exclusion,
        }
    elif _tri_state(s1) == _tri_state(s2) == "FALSE":
        primary = {
            "kind": "NO_SEED_ROUTE_MATCHED",
            "reason": "S1_AND_S2_FALSE",
        }
    elif _tri_state(formal_candidate_seed) == "TRUE":
        primary = {"kind": "FORMAL_CANDIDATE_SEED_TRUE", "reason": None}
    else:
        primary = {
            "kind": "FORMAL_CANDIDATE_SEED_UNKNOWN",
            "reason": (formal_candidate_seed.get("reasons") or [None])[0],
        }

    exclusion_pairs = list(
        dict.fromkeys(
            (item["condition"], item.get("reason"))
            for item in public_failures + seed_failures
        )
    )
    return {
        "contract": "UNIVERSE_ELIGIBILITY_R1+ALG-19/20",
        "evidence_scope": "SOURCE_ONLY_NON_PIT",
        "data_universe": {
            "state": "TRUE",
            "evidence_status": "TRACEABLE_SOURCE_ROW",
            "identity_partition": "UNRESOLVED_SOURCE_SYMBOL",
            "reason": "RAW_SOURCE_POOL_MEMBERSHIP_ONLY",
        },
        "research_universe": dict(predicates["research_universe"]),
        "ordinary_candidate_public_gate": public_gate,
        "ordinary_candidate_state_counts_scope": "ONE_SOURCE_SYMBOL",
        "s1": dict(s1),
        "s2": dict(s2),
        "source_seed_shape": dict(source_seed),
        "seed_safety": dict(safety),
        "formal_seed": dict(seed),
        "formal_candidate_seed": dict(formal_candidate_seed),
        "raw_prewatch": {
            **dict(prewatch),
            "implementation_status": "NOT_IMPLEMENTED",
            "unbound_condition": "MANDATORY_CORE_QUALITY_CONTRACT_UNBOUND",
        },
        "known_exclusions": [
            {"condition": condition, "reason": reason}
            for condition, reason in exclusion_pairs
        ],
        "seed_routes_not_matched": route_failures,
        "unknown_gaps": unknown_gaps,
        "primary_explanation": primary,
        "counts": {
            "ordinary_candidate_public_gate": _state_counts([public_gate]),
            "formal_seed": _state_counts([seed]),
            "formal_candidate_seed": _state_counts([formal_candidate_seed]),
            "raw_prewatch": _state_counts([prewatch]),
        },
    }


def summarize_source_qualification(
    facts: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """按完整来源池分母汇总资格三态、已知排除和缺证原因。"""
    state_counts = {
        name: Counter()
        for name in (
            "data_universe",
            "research_universe",
            "ordinary_candidate_public_gate",
            "formal_seed",
            "formal_candidate_seed",
            "raw_prewatch",
        )
    }
    reasons: dict[str, Counter[str]] = {
        "known_exclusions": Counter(),
        "seed_route_failures": Counter(),
        "seed_route_reasons": Counter(),
        "unknown_gaps": Counter(),
        "primary_explanations": Counter(),
    }
    shape_counts = {name: Counter() for name in ("s1", "s2", "seed")}
    prewatch_implementation = Counter()
    for fact in facts.values():
        explanation = fact.get("qualification") or {}
        for name in state_counts:
            node = explanation.get(name) or {}
            state_counts[name][_tri_state(node)] += 1
        for name in shape_counts:
            shape_counts[name][_tri_state(fact.get(name) or {})] += 1
        prewatch_implementation[
            (explanation.get("raw_prewatch") or {}).get(
                "implementation_status", "NOT_IMPLEMENTED"
            )
        ] += 1
        for item in explanation.get("known_exclusions", []):
            reasons["known_exclusions"][item["condition"]] += 1
        for item in explanation.get("seed_routes_not_matched", []):
            reasons["seed_route_failures"][item["route"]] += 1
            reasons["seed_route_reasons"].update(item.get("reasons", []))
        for item in explanation.get("unknown_gaps", []):
            reasons["unknown_gaps"][item["condition"]] += 1
        primary = explanation.get("primary_explanation") or {}
        reasons["primary_explanations"][primary.get("kind", "NOT_EVALUATED")] += 1

    return {
        "scope": "FULL_SOURCE_POOL",
        "source_pool_total": len(facts),
        "source_shape_status": "SOURCE_ONLY_NON_PIT",
        "shape_states": {
            name: {
                state: shape_counts[name][state]
                for state in ("TRUE", "FALSE", "UNKNOWN")
            }
            for name in shape_counts
        },
        "eligibility_states": {
            name: {
                state: state_counts[name][state]
                for state in ("TRUE", "FALSE", "UNKNOWN")
            }
            for name in state_counts
        },
        "raw_prewatch_implementation": dict(prewatch_implementation),
        "reason_counts": {
            name: dict(sorted(counter.items())) for name, counter in reasons.items()
        },
    }


def unavailable_source_qualification(reason: str) -> dict[str, Any]:
    """历史输入冲突时保留完整三态形状并说明局部不可评估。"""
    predicates = {
        name: _tri(None, reason)
        for name in (
            "research_universe",
            "actual_bar",
            "non_st",
            "normal_listing_phase",
            "limited_price_rule",
            "coordinate_ready",
            "liquidity_mean20",
            "no_damage",
            "no_severe_extension",
        )
    }
    s1 = _and(_tri(None, reason), _tri(None, reason), _tri(None, reason))
    s2 = _and(
        _tri(None, reason),
        _tri(None, reason),
        _or(_tri(None, reason), _tri(None, reason)),
    )
    safety = _and(
        predicates["research_universe"],
        predicates["actual_bar"],
        predicates["coordinate_ready"],
        predicates["liquidity_mean20"],
        predicates["no_damage"],
        predicates["no_severe_extension"],
    )
    source_seed = _and(safety, _or(s1, s2))
    public_gate = _and(
        predicates["research_universe"],
        predicates["actual_bar"],
        predicates["non_st"],
        predicates["normal_listing_phase"],
        predicates["limited_price_rule"],
        predicates["coordinate_ready"],
    )
    seed = _and(safety, _or(s1, s2))
    formal_candidate_seed = _and(public_gate, seed)
    prewatch = _and(seed, _tri(None, "MANDATORY_CORE_QUALITY_CONTRACT_UNBOUND"))
    prewatch["implementation_status"] = "NOT_IMPLEMENTED"
    return source_qualification_explanation(
        predicates,
        s1=s1,
        s2=s2,
        safety=safety,
        seed=seed,
        formal_candidate_seed=formal_candidate_seed,
        source_seed=source_seed,
        prewatch=prewatch,
    )


def source_seed_predicates(
    preview: Mapping[str, Any], delta, previous_cross: dict[str, Any]
) -> dict[str, Any]:
    """按ALG-19/20归约实际字段；缺失正式Universe和单位不冒充已知。"""
    core = preview["core"]["fields"]
    components = {
        item["field_id"]: item for item in preview["profile_preview"]["components"]
    }
    bias = _from_field(
        core["core.bias20_atr"],
        lambda value: value < 3,
        false_reason="BIAS20_ATR_NOT_BELOW_3",
    )
    delta3 = (
        _tri(delta.value >= 3, None if delta.value >= 3 else "RPS5_DELTA3_BELOW_3")
        if delta.quality == "OBSERVED"
        else _tri(None, delta.reason)
    )
    delta10 = (
        _tri(delta.value >= 10, None if delta.value >= 10 else "RPS5_DELTA3_BELOW_10")
        if delta.quality == "OBSERVED"
        else _tri(None, delta.reason)
    )
    compression = _from_field(
        components["profile.compression"],
        lambda value: value in {"COMPRESSING", "COMPRESSING_STRONG"},
        false_reason="COMPRESSION_NOT_QUALIFIED",
    )
    bull = _from_field(
        components["profile.ma_structure"],
        lambda value: value == "BULL_TRANSITION",
        false_reason="MA_STRUCTURE_NOT_BULL_TRANSITION",
    )
    damage = _from_field(
        core["core.price_damage"],
        lambda value: value is False,
        false_reason="PRICE_DAMAGE_PROVED",
    )
    extension = _from_field(
        components["profile.extension"],
        lambda value: value != "EXTREME",
        false_reason="EXTENSION_EXTREME_PROVED",
    )
    # 此读取链仅持有原生金额，未取得人民币单位权威，不能比较绝对金额门槛。
    liquidity = _tri(None, "UNKNOWN_UNIT_UNVERIFIED")
    actual = (
        _tri(True)
        if preview["target_bar_status"] == "TDX_TARGET_BAR_PRESENT"
        and preview["price_research_status"] == "AVAILABLE"
        else _tri(None, "TARGET_ACTUAL_OR_PRICE_UNKNOWN")
    )
    formal_universe = _tri(None, "DATE_VALID_IDENTITY_LIFECYCLE_EVIDENCE_MISSING")
    formal_coordinate = _tri(None, "RAW_UNADJUSTED_COORDINATE_CONTINUITY_UNPROVED")
    non_st = _tri(None, "DATE_VALID_ST_STATUS_EVIDENCE_MISSING")
    normal_listing = _tri(None, "DATE_VALID_LISTING_PHASE_EVIDENCE_MISSING")
    limited_price_rule = _tri(None, "DATE_VALID_LIMIT_APPLICABILITY_EVIDENCE_MISSING")
    safety = _and(
        formal_universe, actual, formal_coordinate, liquidity, damage, extension
    )
    s1 = _and(bias, compression, delta3)
    s2 = _and(bias, delta10, _or(bull, previous_cross))
    seed = _and(safety, _or(s1, s2))
    public_gate = _and(
        formal_universe,
        actual,
        non_st,
        normal_listing,
        limited_price_rule,
        formal_coordinate,
    )
    formal_candidate_seed = _and(public_gate, seed)
    prewatch = _and(seed, _tri(None, "MANDATORY_CORE_QUALITY_CONTRACT_UNBOUND"))
    prewatch["implementation_status"] = "NOT_IMPLEMENTED"
    predicates = {
        "research_universe": formal_universe,
        "actual_bar": actual,
        "non_st": non_st,
        "normal_listing_phase": normal_listing,
        "limited_price_rule": limited_price_rule,
        "coordinate_ready": formal_coordinate,
        "liquidity_mean20": liquidity,
        "no_damage": damage,
        "no_severe_extension": extension,
        "bias_below_3": bias,
        "compression": compression,
        "rps5_delta3_at_least_3": delta3,
        "rps5_delta3_at_least_10": delta10,
        "bull_transition": bull,
        "previous_ma20_cross": previous_cross,
    }
    qualification = source_qualification_explanation(
        predicates,
        s1=s1,
        s2=s2,
        safety=safety,
        seed=seed,
        formal_candidate_seed=formal_candidate_seed,
        source_seed=seed,
        prewatch=prewatch,
    )
    return {
        "predicates": predicates,
        "safety": safety,
        "s1": s1,
        "s2": s2,
        "seed": seed,
        "raw_prewatch": prewatch,
        "formal_eligibility": "NOT_AUTHORIZED",
        "qualification": qualification,
    }


def read_source_pool_seed(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    namespace_id: str,
    profile_revision: str,
    profile_contract: dict[str, Any],
    history_session_count: int,
    selected_symbols: frozenset[str] | None = None,
    include_rps_read: bool = False,
    allow_raw_profile_recompute: bool = False,
) -> dict[str, Any]:
    """读取精确已存画像并复用现役Core/Profile计算未持久的Seed输入。"""
    if selected_symbols is not None and (
        not isinstance(selected_symbols, frozenset)
        or any(not isinstance(symbol, str) or not symbol for symbol in selected_symbols)
    ):
        raise ValueError("来源模式的指定代码集合无效")
    rps = read_source_pool_rps(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        expected_namespace_id=namespace_id,
    )
    _, sessions, revision = resolve_pg_history_window(
        target_session.isoformat(), history_session_count
    )
    profile_offset = 0
    profiles = {}
    profile_snapshot_missing = False
    while True:
        try:
            profile_page = query_source_symbol_price_profiles(
                config,
                target_session=target_session,
                max_target_session=research_cutoff,
                package_sha256=package_sha256,
                profile_revision=profile_revision,
                offset=profile_offset,
                limit=100,
            )
        except PgPriceProfileError as error:
            if (
                not allow_raw_profile_recompute
                or profile_offset != 0
                or error.code != "PROFILE_CHANGE_SNAPSHOT_MISSING"
            ):
                raise
            profile_snapshot_missing = True
            profiles = {symbol: {} for symbol in rps["facts"]}
            break
        if (
            profile_page["namespace_id"] != namespace_id
            or profile_page["source_pool_total"] != len(rps["facts"])
            or profile_page["history_window"]["requested_session_count"]
            != history_session_count
        ):
            raise ValueError("画像与精确来源池身份、窗口或分母不一致")
        for item in profile_page["rows"]:
            if item["source_symbol"] in profiles:
                raise ValueError("画像来源代码重复")
            profiles[item["source_symbol"]] = item
        profile_offset = profile_page["page"]["next_offset"]
        if profile_offset is None:
            break
    if set(profiles) != set(rps["facts"]):
        raise ValueError("画像没有覆盖完整来源池")
    calendar_id = (
        _load_calendar(target_session)[0]["calendar_id"]
        if profile_snapshot_missing
        else profile_page["rows"][0]["history_calendar_id"]
    )
    runtime_revision = _profile_revision(
        scope=SOURCE_SYMBOL_PROFILE_SCOPE,
        semantics_sha256=_profile_semantics_sha256(profile_contract),
        calendar_id=calendar_id,
        calendar_revision=revision,
        history_session_count=history_session_count,
    )
    seed_facts = {}
    offset = 0
    while True:
        page = read_raw_daily_history_page(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            sessions=sessions,
            offset=offset,
            limit=100,
            market="ALL",
            board="ALL",
        )
        if (
            page["namespace_id"] != namespace_id
            or page["total_count"] != len(profiles)
            or page["version_status"] != "CLOSED"
        ):
            raise ValueError("来源池与画像身份不一致")
        for candidate in page["candidates"]:
            symbol = candidate["source_symbol"]
            if selected_symbols is not None and symbol not in selected_symbols:
                continue
            rows = candidate["history_rows"]
            try:
                preview = research_stock_pg_history_preview(
                    symbol=symbol,
                    target_session=target_session.isoformat(),
                    package_sha256=package_sha256,
                    session_dates=sessions,
                    history_rows=rows,
                    namespace_id=namespace_id,
                    profile_contract=profile_contract,
                )
            except ResearchEntryError as error:
                if error.code != "PG_HISTORY_ENTRY_SHA_CONFLICT":
                    raise
                unavailable = {"state": "UNKNOWN", "reasons": [error.code]}
                seed_facts[symbol] = {
                    "predicates": {},
                    "safety": unavailable,
                    "s1": unavailable,
                    "s2": unavailable,
                    "seed": unavailable,
                    "raw_prewatch": unavailable,
                    "formal_eligibility": "NOT_AUTHORIZED",
                    "qualification": unavailable_source_qualification(error.code),
                    "reason": error.code,
                }
                continue
            by_key = {(symbol, row["session_date"]): row for row in rows}
            counts = {
                key: {"row_count": 1, "package_count": 1, "capture_count": 1}
                for key in by_key
            }
            target_row = by_key[(symbol, target_session)]
            observations = tuple(
                _raw_window_observation(
                    source_symbol=symbol,
                    session=day,
                    raw_by_key=by_key,
                    revision_counts=counts,
                    target_session=target_session,
                    target_capture_id=target_row["source_capture_id"],
                )[0]
                for day in sessions
            )
            previous = None
            prior_close = None
            if len(sessions) > 1:
                prior_calendar = MarketSessionCalendar(
                    preview["window"]["calendar_id"], revision, sessions[:-1]
                )
                previous = calculate_core(
                    observations[:-1], official_calendar=prior_calendar
                )["core.ma20"]
                prior_close = observations[-2].close
            current_close = observations[-1].close
            current_bar = observations[-1]
            quote_fields = (
                current_bar.open,
                current_bar.high,
                current_bar.low,
                current_bar.close,
            )
            valid_quote = current_bar.state == "ACTUAL" and all(
                value is not None and value.quality == "OBSERVED"
                for value in quote_fields
            )
            if valid_quote:
                opening, high, low, close = (
                    Fraction(value.value) for value in quote_fields
                )
                valid_quote = (
                    min(opening, high, low, close) > 0
                    and low <= min(opening, close)
                    and high >= max(opening, low, close)
                )
            current_ma = preview["core"]["fields"]["core.ma20"]
            if (
                previous is not None
                and previous.quality == "OBSERVED"
                and prior_close
                and prior_close.quality == "OBSERVED"
                and current_close
                and current_close.quality == "OBSERVED"
                and current_ma["quality"] == "OBSERVED"
            ):
                crossed = Fraction(prior_close.value) <= Fraction(
                    previous.value
                ) and Fraction(current_close.value) > Fraction(
                    current_ma["value"]["exact"]
                )
                cross = _tri(
                    crossed,
                    None if crossed else "PREVIOUS_MA20_CROSS_NOT_MET",
                )
            else:
                cross = _tri(None, "PREVIOUS_MA20_CROSS_INPUT_UNKNOWN")
            seed = source_seed_predicates(
                preview, rps["facts"][symbol]["rps5_delta3_pp"], cross
            )
            seed["core"] = {
                key: preview["core"]["fields"][key]
                for key in (
                    "core.bias20_atr",
                    "core.liquidity_mean20",
                    "core.price_damage",
                    "core.ma20",
                )
            }
            seed["native_price"] = {
                "actual_quote": valid_quote,
                "close": str(current_close.value)
                if current_close and current_close.quality == "OBSERVED"
                else None,
                "fields": {
                    key: preview["core"]["fields"][key]
                    for key in ("core.ret1", "core.ret5", "core.ret20", "core.ma20")
                },
            }
            saved = profiles[symbol]
            seed["saved_profile_core"] = {
                name: {
                    "value": str(saved[name]) if saved.get(name) is not None else None,
                    "quality": "UNKNOWN"
                    if profile_snapshot_missing
                    else saved.get(f"{name}_quality"),
                    "reason": "PROFILE_SNAPSHOT_MISSING"
                    if profile_snapshot_missing
                    else saved.get(f"{name}_reason"),
                }
                for name in ("bias20_atr", "ma20", "price_damage")
            }
            seed["profile"] = {
                item["field_id"]: {
                    key: item.get(key) for key in ("quality", "value", "reason")
                }
                for item in preview["profile_preview"]["components"]
                if item["field_id"]
                in {"profile.compression", "profile.extension", "profile.ma_structure"}
            }
            seed["rps"] = {
                key: {
                    "quality": value.quality,
                    "value": str(value.value) if value.value is not None else None,
                    "reason": value.reason,
                }
                for key, value in rps["facts"][symbol].items()
            }
            seed_facts[symbol] = seed
        offset = page["next_offset"]
        if offset is None:
            break
    expected_symbols = (
        set(profiles) if selected_symbols is None else set(profiles) & selected_symbols
    )
    if set(seed_facts) != expected_symbols:
        raise ValueError("Seed事实没有覆盖完整来源池")
    seed_counts = Counter(fact["seed"]["state"] for fact in seed_facts.values())
    result = {
        "target_session": target_session,
        "research_cutoff": research_cutoff,
        "namespace_id": namespace_id,
        "package_sha256": package_sha256,
        "profile_revision": profile_revision,
        "runtime_profile_revision": runtime_revision,
        "profile_revision_matches_runtime": runtime_revision == profile_revision,
        "profile_revision_comparison": {
            "requested_profile_revision": profile_revision,
            "stored_profile_revision": None
            if profile_snapshot_missing
            else profile_revision,
            "runtime_profile_revision": runtime_revision,
            "status": "MISSING_RAW_RECOMPUTED"
            if profile_snapshot_missing
            else "MATCH"
            if runtime_revision == profile_revision
            else "STORED_AND_RUNTIME_REVISIONS_DIFFER",
        },
        "history_session_count": history_session_count,
        "profile_input_note": (
            "该日无已存画像；以同包Raw和现役Core/Profile合同即时计算，标明实际运行修订，未生成画像快照。"
            if profile_snapshot_missing
            else "所选PG画像用于身份与已存字段读取；Seed分项由当前Core/Profile合同从同包Raw即时计算，并非声称该修订保存了这些分项数值。"
        ),
        "stored_profile_status": "MISSING_RAW_RECOMPUTED"
        if profile_snapshot_missing
        else "AVAILABLE",
        "scope": rps["scope"],
        "price_coordinate": rps["coordinate"],
        "corporate_action_continuity": rps["corporate_action_continuity"],
        "formal_research_universe": rps["formal_research_universe"],
        "endpoint_counts": rps["endpoint_counts"],
        "source_pool_total": len(profiles),
        "seed_evaluated_count": len(seed_facts),
        "seed_evaluation_scope": "FULL_SOURCE_POOL"
        if selected_symbols is None
        else "EXPLICIT_SOURCE_SYMBOLS",
        "seed_counts": {
            state: seed_counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")
        },
        "facts": seed_facts,
    }
    if include_rps_read:
        result["rps_read"] = rps
    return result


def _board_native(pool, symbols, peer_members, board_id, *, peer_cache=None):
    """以完整成员槽位计算非PIT板块事实，缺失值不缩减质量分母。"""
    facts = pool["facts"]

    def field(symbol, key):
        item = (
            facts.get(symbol, {}).get("native_price", {}).get("fields", {}).get(key, {})
        )
        value = item.get("value")
        return Fraction(value["exact"]) if item.get("quality") == "OBSERVED" else None

    def metrics(members):
        n = len(members)
        quotes = sum(
            bool(facts.get(s, {}).get("native_price", {}).get("actual_quote"))
            for s in members
        )
        quote_ok = n >= 5 and quotes >= 5 and 5 * quotes >= 4 * n
        out = {
            "member_count": n,
            "actual_quote_count": quotes,
            "actual_quote_unknown_count": n - quotes,
            "matched_source_count": sum(s in facts for s in members),
            "unmapped_source_count": sum(s not in facts for s in members),
            "quote_coverage": str(Fraction(quotes, n)) if n else None,
        }
        for period in (1, 5, 20):
            values = [
                v for s in members if (v := field(s, f"core.ret{period}")) is not None
            ]
            qualified = quote_ok and len(values) >= 5 and 5 * len(values) >= 4 * n
            out[f"ret{period}"] = {
                "known": len(values),
                "unknown": n - len(values),
                "median": str(median(values)) if qualified else None,
            }
            if period == 1:
                out["breadth"] = {
                    "known": len(values),
                    "unknown": n - len(values),
                    "positive": sum(v > 0 for v in values),
                    "value": str(Fraction(sum(v > 0 for v in values), len(values)))
                    if qualified
                    else None,
                }
        ma_pairs = []
        for s in members:
            native = facts.get(s, {}).get("native_price", {})
            ma = field(s, "core.ma20")
            if (
                native.get("actual_quote")
                and native.get("close") is not None
                and ma is not None
            ):
                ma_pairs.append(Fraction(native["close"]) > ma)
        qualified = quote_ok and len(ma_pairs) >= 5 and 5 * len(ma_pairs) >= 4 * n
        out["ma20_width"] = {
            "known": len(ma_pairs),
            "unknown": n - len(ma_pairs),
            "above": sum(ma_pairs),
            "value": str(Fraction(sum(ma_pairs), len(ma_pairs))) if qualified else None,
        }
        return out

    if peer_cache is not None and "peers" in peer_cache:
        peers = peer_cache["peers"]
    else:
        peers = {key: metrics(members) for key, members in peer_members.items()}
        if peer_cache is not None:
            peer_cache["peers"] = peers
    result = deepcopy(peers[board_id])
    for period in (5, 20):
        key = f"ret{period}"
        values = sorted(
            Fraction(v[key]["median"])
            for v in peers.values()
            if v[key]["median"] is not None
        )
        own = result[key]["median"]
        rank = None
        if own is not None and len(values) >= 2:
            value = Fraction(own)
            rank = (
                Fraction(100)
                * (
                    sum(v < value for v in values)
                    + Fraction(sum(v == value for v in values) - 1, 2)
                )
                / (len(values) - 1)
            )
        result[key]["percentile"] = str(rank) if rank is not None else None
        result[key]["peer_count"] = len(values)
        result[key]["peer_total"] = len(peers)
    patterns = Counter()
    for symbol in symbols:
        fact = facts.get(symbol)
        state = _or(fact["s1"], fact["s2"])["state"] if fact else "UNKNOWN"
        patterns[state] += 1
    known = patterns["TRUE"] + patterns["FALSE"]
    k = patterns["TRUE"]
    wilson = None
    if known:
        z = 1.96
        p = k / known
        wilson = (
            p
            + z * z / (2 * known)
            - z * sqrt(p * (1 - p) / known + z * z / (4 * known * known))
        ) / (1 + z * z / known)
    result["source_pattern_width_non_pit"] = {
        "true": k,
        "false": patterns["FALSE"],
        "unknown": patterns["UNKNOWN"],
        "known_denominator": known,
        "width": str(Fraction(k, known)) if known else None,
        "wilson_lower": wilson,
        "meaning": "SOURCE_PATTERN_WIDTH_NON_PIT",
    }
    result["formal_seed_adjusted_width"] = {
        "state": "UNKNOWN",
        "reason": "FORMAL_SEED_ELIGIBILITY_UNPROVED",
    }
    result["history_changes"] = {
        "state": "UNKNOWN",
        "reason": "FORMAL_COMMON_COORDINATE_AND_MEMBERSHIP_UNPROVED",
    }
    safety = _and(
        _tri(
            len(symbols) >= 5
            and result["actual_quote_count"] >= 5
            and 5 * result["actual_quote_count"] >= 4 * len(symbols)
        ),
        _tri(None, "FORMAL_COORDINATE_UNPROVED"),
    )

    def threshold(value, minimum):
        return (
            _tri(Fraction(value) >= minimum)
            if value is not None
            else _tri(None, "NATIVE_INPUT_UNKNOWN")
        )

    seed_width = _tri(None, "FORMAL_SEED_WIDTH_UNKNOWN")
    delta = _tri(None, "FORMAL_COMMON_MEMBERSHIP_DELTA_UNKNOWN")
    warm = _and(
        safety,
        threshold(result["ret5"]["percentile"], 60),
        threshold(result["ret20"]["percentile"], 50),
        threshold(result["breadth"]["value"], Fraction(1, 2)),
        threshold(result["ma20_width"]["value"], Fraction(1, 2)),
        seed_width,
    )
    result["raw_qualification"] = {
        "safety": safety,
        "prewatch_raw": _and(safety, delta, seed_width),
        "warm_raw": warm,
        "confirmed_raw": _and(
            warm,
            threshold(result["ret5"]["percentile"], 70),
            threshold(result["ret20"]["percentile"], 60),
            threshold(result["breadth"]["value"], Fraction(55, 100)),
            threshold(result["ma20_width"]["value"], Fraction(55, 100)),
            delta,
            _tri(None, "CNY_CONCENTRATION_UNKNOWN"),
        ),
    }
    result["formal_eligibility"] = "NOT_AUTHORIZED"
    result["scope"] = "SOURCE_POOL_NON_PIT_EXPLORATORY"
    result["price_coordinate"] = "RAW_UNADJUSTED"
    return result


def _cross_period_boards(peer_members, inputs):
    """固定成员下逐字段取双端交集，再对同类型板块全集求中秩。"""
    target, prior, previous = (
        "target",
        "three_official_sessions_before",
        "previous_official_session",
    )
    results = {}
    for board_id, members in peer_members.items():
        n = len(members)
        endpoints = {}
        for endpoint in (target, prior, previous):
            quotes = sum(
                bool(inputs.get(s, {}).get(endpoint, {}).get("actual_quote"))
                for s in members
            )
            endpoints[endpoint] = {
                "actual_quote_count": quotes,
                "unknown_quote_count": n - quotes,
                "quote_coverage": str(Fraction(quotes, n)) if n else None,
            }
        result = {
            "scope": "SOURCE_POOL_NON_PIT_EXPLORATORY",
            "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            "price_coordinate": "RAW_UNADJUSTED",
            "member_count": n,
            "mapped_count": sum(bool(inputs.get(s, {}).get("mapped")) for s in members),
            "unmapped_count": sum(not inputs.get(s, {}).get("mapped") for s in members),
            "endpoints": endpoints,
        }
        for field, earlier in (
            ("ret5", prior),
            ("ret20", prior),
            ("ret1", prior),
            ("above_ma20", prior),
            ("ret1_delta1", previous),
        ):
            raw_field = "ret1" if field == "ret1_delta1" else field
            a = {s: inputs.get(s, {}).get(target, {}).get(raw_field) for s in members}
            b = {s: inputs.get(s, {}).get(earlier, {}).get(raw_field) for s in members}
            common = [s for s in members if a[s] is not None and b[s] is not None]
            qualified = (
                n >= 5
                and len(common) >= 5
                and 5 * len(common) >= 4 * n
                and all(
                    endpoints[e]["actual_quote_count"] >= 5
                    and 5 * endpoints[e]["actual_quote_count"] >= 4 * n
                    for e in (target, earlier)
                )
            )

            def aggregate(values):
                if not qualified:
                    return None
                if raw_field == "above_ma20":
                    return Fraction(sum(values), len(values))
                numbers = [Fraction(v) for v in values]
                if raw_field == "ret1":
                    return Fraction(sum(v > 0 for v in numbers), len(numbers))
                return median(numbers)

            now = aggregate([a[s] for s in common])
            before = aggregate([b[s] for s in common])
            result[field] = {
                "target_known": sum(v is not None for v in a.values()),
                "prior_known": sum(v is not None for v in b.values()),
                "target_unknown": sum(v is None for v in a.values()),
                "prior_unknown": sum(v is None for v in b.values()),
                "common_count": len(common),
                "denominator": n,
                "target": str(now) if now is not None else None,
                "prior": str(before) if before is not None else None,
                "delta": str(now - before)
                if now is not None and before is not None
                else None,
            }
        results[board_id] = result
    for field in ("ret5", "ret20"):
        for endpoint in ("target", "prior"):
            values = [
                Fraction(row[field][endpoint])
                for row in results.values()
                if row[field][endpoint] is not None
            ]
            for row in results.values():
                own = row[field][endpoint]
                rank = None
                if own is not None and len(values) >= 2:
                    value = Fraction(own)
                    rank = (
                        100
                        * (
                            sum(v < value for v in values)
                            + Fraction(sum(v == value for v in values) - 1, 2)
                        )
                        / (len(values) - 1)
                    )
                row[field][endpoint + "_percentile"] = (
                    str(rank) if rank is not None else None
                )
                row[field][endpoint + "_peer_count"] = len(values)
        for row in results.values():
            metric = row[field]
            a, b = metric["target_percentile"], metric["prior_percentile"]
            metric["percentile_delta_pp"] = (
                str(Fraction(a) - Fraction(b))
                if a is not None and b is not None
                else None
            )
    for row in results.values():
        dq = row["ret5"]["percentile_delta_pp"]
        delta1 = row["ret1_delta1"]["delta"]
        pulse = _and(
            _tri(Fraction(dq) >= 10) if dq is not None else _tri(None, "DQ5_UNKNOWN"),
            _tri(Fraction(delta1) >= Fraction(5, 100))
            if delta1 is not None
            else _tri(None, "BREADTH_DELTA1_UNKNOWN"),
        )
        row["rotation"] = {
            "scope": "SOURCE_POOL_NON_PIT_EXPLORATORY",
            "dq5_pp": dq,
            "breadth_delta3": row["ret1"]["delta"],
            "breadth_delta1": delta1,
            "pulse": pulse,
            "formal_state": "NOT_EVALUATED",
            "reason": "FROZEN_EPISODE_AND_BASKET_UNAVAILABLE",
        }
    return results


def _loo_differences(before, after):
    """仅对两侧均已知的同口径板块结果计算差值。"""

    def difference(left, right):
        return (
            str(Fraction(right) - Fraction(left))
            if left is not None and right is not None
            else None
        )

    return {
        "ret5_median": difference(before["ret5"]["median"], after["ret5"]["median"]),
        "ret20_median": difference(before["ret20"]["median"], after["ret20"]["median"]),
        "breadth": difference(before["breadth"]["value"], after["breadth"]["value"]),
        "ma20_width": difference(
            before["ma20_width"]["value"], after["ma20_width"]["value"]
        ),
        "rs5_pp": difference(before["ret5"]["percentile"], after["ret5"]["percentile"]),
        "rs20_pp": difference(
            before["ret20"]["percentile"], after["ret20"]["percentile"]
        ),
    }


def read_board_seed_facts(
    config: dict[str, Any],
    pool: Mapping[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str,
    peer_members: Mapping[str, list[str]] | None = None,
    cross_period_inputs: Mapping[str, Any] | None = None,
    cross_period_sessions: Mapping[str, str] | None = None,
    loo_symbol: str | None = None,
) -> dict[str, Any]:
    """复用现役成员读取，按完整成员计数并保留未映射未知。"""
    offset = 0
    symbols = []
    while True:
        page = read_tdx_sector_members(
            config,
            snapshot_id=snapshot_id,
            board_kind=board_kind,
            board_native_id=board_native_id,
            target_session=pool["target_session"],
            research_cutoff=pool["research_cutoff"],
            package_sha256=pool["package_sha256"],
            profile_revision=pool["profile_revision"],
            offset=offset,
            limit=100,
        )
        if page["namespace_id"] != pool["namespace_id"]:
            raise ValueError("板块与股票来源namespace不一致")
        symbols.extend(row["source_symbol"] for row in page["rows"])
        offset += len(page["rows"])
        if offset >= page["source_member_count"]:
            break
        if not page["rows"]:
            raise ValueError("板块成员分页未闭合")
    if len(symbols) != len(set(symbols)) or len(symbols) != page["source_member_count"]:
        raise ValueError("板块成员重复或分母不闭合")
    counts = Counter(
        pool["facts"][symbol]["seed"]["state"] if symbol in pool["facts"] else "UNKNOWN"
        for symbol in symbols
    )
    result = {
        "board_kind": board_kind,
        "board_native_id": board_native_id,
        "snapshot_id": snapshot_id,
        "membership_mode": page["membership_mode"],
        "member_count": len(symbols),
        "matched_source_count": sum(symbol in pool["facts"] for symbol in symbols),
        "seed_counts": {state: counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")},
        "evaluable_count": counts["TRUE"] + counts["FALSE"],
        "members": [
            {
                "source_symbol": symbol,
                "seed": pool["facts"].get(symbol)
                or {
                    "seed": {
                        "state": "UNKNOWN",
                        "reasons": ["MEMBER_NOT_IN_EXACT_TARGET_SOURCE_POOL"],
                    }
                },
            }
            for symbol in symbols
        ],
    }
    if peer_members is not None:
        if set(peer_members.get(board_native_id, ())) != set(symbols):
            raise ValueError("板块完整成员与同类板块读取不一致")
        result["native"] = _board_native(pool, symbols, peer_members, board_native_id)
        if cross_period_inputs is not None:
            result["native"]["cross_period"] = _cross_period_boards(
                peer_members, cross_period_inputs
            )[board_native_id]
            result["native"]["cross_period"]["sessions"] = cross_period_sessions
        if loo_symbol is not None:
            known_symbol = loo_symbol in pool["facts"] or any(
                loo_symbol in members for members in peer_members.values()
            )
            if not known_symbol:
                result["loo"] = {
                    "source_symbol": loo_symbol,
                    "state": "UNKNOWN",
                    "reason": "SOURCE_SYMBOL_NOT_FOUND",
                }
            elif loo_symbol not in symbols:
                result["loo"] = {"source_symbol": loo_symbol, "state": "NOT_APPLICABLE"}
            else:
                reduced = {
                    key: [s for s in members if s != loo_symbol]
                    for key, members in peer_members.items()
                }
                after = _board_native(
                    pool, reduced[board_native_id], reduced, board_native_id
                )
                if cross_period_inputs is not None:
                    after["cross_period"] = _cross_period_boards(
                        reduced, cross_period_inputs
                    )[board_native_id]
                    after["cross_period"]["sessions"] = cross_period_sessions
                result["loo"] = {
                    "source_symbol": loo_symbol,
                    "state": "APPLIED",
                    "source_mapping": "MAPPED"
                    if loo_symbol in pool["facts"]
                    else "UNMAPPED",
                    "affected_board_count": sum(
                        loo_symbol in members for members in peer_members.values()
                    ),
                    "before": result["native"],
                    "after": after,
                    "differences": _loo_differences(result["native"], after),
                }
    return result
