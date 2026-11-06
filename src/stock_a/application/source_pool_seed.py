"""来源池Seed原始谓词及板块完整成员的只读研究消费。"""

from __future__ import annotations

from collections import Counter
from datetime import date
from fractions import Fraction
from math import sqrt
from statistics import median
from typing import Any, Mapping

from stock_a.application.pg_price_profiles import (
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


def _from_field(field: Mapping[str, Any], predicate) -> dict[str, Any]:
    if field.get("quality") != "OBSERVED":
        return _tri(None, str(field.get("reason") or "FIELD_UNKNOWN"))
    value = field.get("value")
    if isinstance(value, dict):
        value = Fraction(value["exact"])
    return _tri(bool(predicate(value)))


def source_seed_predicates(
    preview: Mapping[str, Any], delta, previous_cross: dict[str, Any]
) -> dict[str, Any]:
    """按ALG-19/20归约实际字段；缺失正式Universe和单位不冒充已知。"""
    core = preview["core"]["fields"]
    components = {
        item["field_id"]: item for item in preview["profile_preview"]["components"]
    }
    bias = _from_field(core["core.bias20_atr"], lambda value: value < 3)
    delta3 = (
        _tri(delta.value >= 3)
        if delta.quality == "OBSERVED"
        else _tri(None, delta.reason)
    )
    delta10 = (
        _tri(delta.value >= 10)
        if delta.quality == "OBSERVED"
        else _tri(None, delta.reason)
    )
    compression = _from_field(
        components["profile.compression"],
        lambda value: value in {"COMPRESSING", "COMPRESSING_STRONG"},
    )
    bull = _from_field(
        components["profile.ma_structure"], lambda value: value == "BULL_TRANSITION"
    )
    damage = _from_field(core["core.price_damage"], lambda value: value is False)
    extension = _from_field(
        components["profile.extension"], lambda value: value != "EXTREME"
    )
    # 此读取链仅持有原生金额，未取得人民币单位权威，不能比较绝对金额门槛。
    liquidity = _tri(None, "UNKNOWN_UNIT_UNVERIFIED")
    actual = (
        _tri(True)
        if preview["target_bar_status"] == "TDX_TARGET_BAR_PRESENT"
        and preview["price_research_status"] == "AVAILABLE"
        else _tri(None, "TARGET_ACTUAL_OR_PRICE_UNKNOWN")
    )
    formal_universe = _tri(None, "HISTORICAL_RESEARCH_UNIVERSE_UNPROVED")
    formal_coordinate = _tri(None, "CORPORATE_ACTION_COORDINATE_UNPROVED")
    safety = _and(
        formal_universe, actual, formal_coordinate, liquidity, damage, extension
    )
    s1 = _and(bias, compression, delta3)
    s2 = _and(bias, delta10, _or(bull, previous_cross))
    seed = _and(safety, _or(s1, s2))
    prewatch = _and(seed, _tri(None, "MANDATORY_CORE_QUALITY_CONTRACT_UNBOUND"))
    return {
        "predicates": {
            "research_universe": formal_universe,
            "actual_bar": actual,
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
        },
        "safety": safety,
        "s1": s1,
        "s2": s2,
        "seed": seed,
        "raw_prewatch": prewatch,
        "formal_eligibility": "NOT_AUTHORIZED",
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
) -> dict[str, Any]:
    """读取精确已存画像并复用现役Core/Profile计算未持久的Seed输入。"""
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
    while True:
        profile_page = query_source_symbol_price_profiles(
            config,
            target_session=target_session,
            max_target_session=research_cutoff,
            package_sha256=package_sha256,
            profile_revision=profile_revision,
            offset=profile_offset,
            limit=100,
        )
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
    runtime_revision = _profile_revision(
        scope=SOURCE_SYMBOL_PROFILE_SCOPE,
        semantics_sha256=_profile_semantics_sha256(profile_contract),
        calendar_id=profile_page["rows"][0]["history_calendar_id"],
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
                cross = _tri(
                    Fraction(prior_close.value) <= Fraction(previous.value)
                    and Fraction(current_close.value)
                    > Fraction(current_ma["value"]["exact"])
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
                    "quality": saved.get(f"{name}_quality"),
                    "reason": saved.get(f"{name}_reason"),
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
    if set(seed_facts) != set(profiles):
        raise ValueError("Seed事实没有覆盖完整来源池")
    seed_counts = Counter(fact["seed"]["state"] for fact in seed_facts.values())
    return {
        "target_session": target_session,
        "research_cutoff": research_cutoff,
        "namespace_id": namespace_id,
        "package_sha256": package_sha256,
        "profile_revision": profile_revision,
        "runtime_profile_revision": runtime_revision,
        "profile_revision_matches_runtime": runtime_revision == profile_revision,
        "history_session_count": history_session_count,
        "profile_input_note": "所选PG画像用于身份与已存字段读取；Seed分项由当前Core/Profile合同从同包Raw即时计算，并非声称该修订保存了这些分项数值。",
        "scope": rps["scope"],
        "price_coordinate": rps["coordinate"],
        "corporate_action_continuity": rps["corporate_action_continuity"],
        "formal_research_universe": rps["formal_research_universe"],
        "endpoint_counts": rps["endpoint_counts"],
        "source_pool_total": len(seed_facts),
        "seed_counts": {
            state: seed_counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")
        },
        "facts": seed_facts,
    }


def _board_native(pool, symbols, peer_members, board_id):
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

    peers = {key: metrics(members) for key, members in peer_members.items()}
    result = peers[board_id]
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


def read_board_seed_facts(
    config: dict[str, Any],
    pool: Mapping[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str,
    peer_members: Mapping[str, list[str]] | None = None,
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
    return result
