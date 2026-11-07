"""来源级市场观察研究的正式命令行消费者。"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
from fractions import Fraction
import time

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.runtime_config import (
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    load_runtime_config,
)
from stock_a.application.source_market_observation import (
    SourceObservationError,
    assemble_observation_changes,
    attach_board_strengths,
    attach_comparison_continuity,
    attach_observation_outcomes,
    attach_observation_state_followup,
    compare_source_outcomes,
    classify_source_transition,
    read_market_observation_sessions,
    read_observation_board_details,
    read_observation_d1,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    attach_stock_risk_readiness,
)
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.pg_price_profiles import PgPriceProfileError
from stock_a.application.pg_research import PgResearchBindingError
from stock_a.application.source_market_context import SourceMarketContextError
from stock_a.application.source_relative_position import (
    board_changes,
    market_changes,
    relative_position_view,
)
from stock_a.storage.postgres.source_raw import RawSourcePgError
from stock_a.storage.postgres.tdx_sector_members import TdxSectorMembersPgError
from stock_a.contracts import load_price_profile_contract
from stock_a.research.entry_context.sessions import _load_calendar


def _run_source_market_observation(args) -> int:
    started = time.perf_counter()
    try:
        start = _parse_session_date(args.start_session)
        end = _parse_session_date(args.end_session)
        cutoff = _parse_session_date(args.research_cutoff)
        event_start = _parse_session_date(args.event_window_start)
        if start > end or end > cutoff or args.offset < 0 or not 1 <= args.limit <= 100:
            raise ValueError("会话范围、截止日或分页参数无效")
        if bool(args.board_kind) != bool(args.board_id):
            raise ValueError("板块类型和编号必须同时提供")
        if bool(args.rotation_start) != bool(args.board_id):
            raise ValueError("精确板块与Rotation起始会话必须同时提供")
        if args.relative_position and not args.board_id:
            raise ValueError("相对位置研究需要精确板块")
        horizons = sorted(set(args.horizon or []))
        if not horizons and args.view == "full":
            raise ValueError("完整研究必须指定至少一个后继会话数")
        if any(not 1 <= value <= 10 for value in horizons):
            raise ValueError("后续会话数须为1至10")
        _, _, _, calendar = _load_calendar(end)
        if start not in calendar or end not in calendar:
            raise ValueError("研究范围端点必须是官方会话")
        sessions = tuple(day for day in calendar if start <= day <= end)
        config, config_path = load_runtime_config()
        contract = load_price_profile_contract(_CONFIG_ROOT).document
        days = read_market_observation_sessions(
            config,
            sessions=sessions,
            research_cutoff=cutoff,
            package_sha256=args.package_sha256,
            profile_revision=args.profile_revision,
            profile_contract=contract,
            history_session_count=args.history_session_count,
        )
        d1 = read_observation_d1(
            config,
            str(config_path),
            sessions=sessions,
            research_cutoff=cutoff,
            package_sha256=args.package_sha256,
            event_window_start=event_start,
            history_session_count=args.d1_history_session_count,
        )
        result = assemble_observation_changes(
            config,
            days,
            industry_snapshot_id=args.industry_snapshot_id,
            concept_snapshot_id=args.concept_snapshot_id,
            d1_by_session=d1,
        )
        attach_board_strengths(
            config,
            days,
            result,
            industry_snapshot_id=args.industry_snapshot_id,
            concept_snapshot_id=args.concept_snapshot_id,
            research_cutoff=cutoff,
            package_sha256=args.package_sha256,
            profile_revision=args.profile_revision,
        )
        _, _, _, cutoff_calendar = _load_calendar(cutoff)
        followup_sessions = tuple(
            day for day in cutoff_calendar if end < day <= cutoff
        )[: max(horizons, default=0)]
        followup_days = []
        if followup_sessions:
            followup_days = read_market_observation_sessions(
                config,
                sessions=followup_sessions,
                research_cutoff=cutoff,
                package_sha256=args.package_sha256,
                profile_revision=args.profile_revision,
                profile_contract=contract,
                history_session_count=args.history_session_count,
            )
            for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
                before = days[-1]["scenarios"][scenario]
                after = followup_days[0]["scenarios"][scenario]
                if (
                    before["namespace_id"] != after["namespace_id"]
                    or before["source_version"]["source_content_id"]
                    != after["source_version"]["source_content_id"]
                    or before["official_calendar"] != after["official_calendar"]
                    or before["algorithm_contract"] != after["algorithm_contract"]
                    or before["producer_version"] != after["producer_version"]
                ):
                    raise SourceObservationError("观察期与后继来源或算法身份不一致")
            d1.update(
                read_observation_d1(
                    config,
                    str(config_path),
                    sessions=followup_sessions,
                    research_cutoff=cutoff,
                    package_sha256=args.package_sha256,
                    event_window_start=event_start,
                    history_session_count=args.d1_history_session_count,
                )
            )
        if horizons:
            attach_observation_state_followup(
                days + followup_days, result, d1_by_session=d1, horizon=max(horizons)
            )
        else:
            for event in result["events"]:
                event["source_state_followup"] = "NOT_REQUESTED"
                event["first_proved_failure_session"] = "NOT_REQUESTED"
                event["first_candidate_line_touch_session"] = "NOT_REQUESTED"
        history_cache = {}
        read_metrics = {
            "comparison_pool_scans": 0,
            "comparison_page_reads": 0,
            "comparison_cache_reuses": 0,
            "comparison_missing_stock_reads": 0,
            "event_stock_reads": 0,
            "event_cache_hits": 0,
        }
        comparisons = [
            row
            for horizon in reversed(horizons)
            for row in compare_source_outcomes(
                config,
                days,
                result,
                research_cutoff=cutoff,
                package_sha256=args.package_sha256,
                horizon=horizon,
                history_cache=history_cache,
                read_metrics=read_metrics,
            )
        ]
        if horizons:
            attach_observation_outcomes(
                config,
                days,
                result,
                research_cutoff=cutoff,
                package_sha256=args.package_sha256,
                horizon=max(horizons),
                history_cache=history_cache,
                read_metrics=read_metrics,
            )
            attach_comparison_continuity(comparisons, result)
        comparisons.sort(key=lambda row: row["horizon"])
        selected = [
            event
            for event in result["events"]
            if (not args.symbol or event["source_symbol"] == args.symbol)
            and (not args.signal or event["signal"] == args.signal)
            and (
                not args.board_id
                or any(
                    membership["board_kind"] == args.board_kind
                    and membership["board_native_id"] == args.board_id
                    for membership in event["memberships"]
                )
            )
        ]
        page_limit = min(args.limit, 30) if args.view == "summary" else args.limit
        page_events = selected[args.offset : args.offset + page_limit]
        if args.view == "summary" and not (args.symbol or args.board_id or args.signal):
            page_events = []
        page_keys = {
            (event["session"], event["source_symbol"]) for event in page_events
        }
        stock_observations = {}
        for event in result["events"]:
            key = (event["session"], event["source_symbol"])
            if key not in page_keys:
                continue
            summary = stock_observations.setdefault(
                key,
                {"session": key[0], "source_symbol": key[1], "events": []},
            )
            summary["events"].append(
                {
                    "signal": event["signal"],
                    "transition": event["transition"],
                    "d1_anchor_session": event.get("d1_source_event", {}).get(
                        "support_anchor_session"
                    ),
                    "source_state_followup": event["source_state_followup"],
                    "first_proved_failure_session": event[
                        "first_proved_failure_session"
                    ],
                    "first_candidate_line_touch_session": event[
                        "first_candidate_line_touch_session"
                    ],
                }
            )
        for summary in stock_observations.values():
            summary["event_count"] = len(summary["events"])
        by_session = {day["session"]: day for day in days}
        detail_cache = {}
        rps_by_session = {}

        def stock_detail(key):
            if key not in detail_cache:
                day = by_session[key[0]]
                if key[1] not in day["states"]:
                    return {
                        "status": "UNKNOWN_DATA",
                        "reason": "SOURCE_SYMBOL_NOT_IN_POOL",
                    }
                scenario = day["scenarios"]["LAUNCH_CONFIRM"]
                item = deepcopy(
                    next(
                        row
                        for row in scenario["results"]
                        if row["source_symbol"] == key[1]
                    )
                )
                diagnostic = {
                    "namespace_id": day["namespace_id"],
                    "scenario": "LAUNCH_CONFIRM",
                    "results": [item],
                }
                if key[0] not in rps_by_session:
                    rps_by_session[key[0]] = day["seed"].get(
                        "rps_read"
                    ) or read_source_pool_rps(
                        config,
                        target_session=key[0],
                        research_cutoff=cutoff,
                        package_sha256=args.package_sha256,
                        expected_namespace_id=day["namespace_id"],
                    )
                attach_stock_risk_readiness(
                    config,
                    diagnostic,
                    target=key[0],
                    cutoff=cutoff,
                    package_sha256=args.package_sha256,
                    rps_read=rps_by_session[key[0]],
                )
                detail_cache[key] = {
                    "source_seed": day["seed"]["facts"][key[1]],
                    "d0": {
                        name: next(
                            row
                            for row in source["results"]
                            if row["source_symbol"] == key[1]
                        )
                        for name, source in day["scenarios"].items()
                    },
                    "risk_priority_readiness": item["risk_priority_readiness"],
                }
            return detail_cache[key]

        for event in page_events:
            event["source_detail"] = stock_detail(
                (event["session"], event["source_symbol"])
            )
        stock_summary = None
        if args.symbol:
            memberships = result["memberships_by_symbol"].get(args.symbol, [])
            d1_stock = {
                day["session"]: next(
                    (
                        row
                        for row in d1[day["session"]]["complete_symbol_details"]
                        if row["source_symbol"] == args.symbol
                    ),
                    {"status": "UNKNOWN_DATA", "reason": "SOURCE_SYMBOL_NOT_IN_POOL"},
                )
                for day in days
            }
            stock_summary = [
                {
                    "session": day["session"],
                    "source_symbol": args.symbol,
                    "memberships": memberships,
                    "membership_reason": None
                    if memberships
                    else "NO_PROVED_MEMBERSHIP",
                    "selected_board_relation": (
                        "MEMBER"
                        if any(
                            row["board_kind"] == args.board_kind
                            and row["board_native_id"] == args.board_id
                            for row in memberships
                        )
                        else "NOT_IN_SELECTED_CURRENT_SNAPSHOT"
                        if args.board_id
                        else "NOT_REQUESTED"
                    ),
                    "observation_selection": {
                        "event_count": sum(
                            event["session"] == day["session"]
                            and event["source_symbol"] == args.symbol
                            for event in result["events"]
                        ),
                        "reason": "DISCOVERED_EVENT"
                        if any(
                            event["session"] == day["session"]
                            and event["source_symbol"] == args.symbol
                            for event in result["events"]
                        )
                        else "NO_PROVED_SELECTED_TRANSITION_OR_D1_CHANGE"
                        if args.view == "full"
                        else "SOURCE_SYMBOL_NOT_IN_POOL"
                        if args.symbol not in day["states"]
                        else "PROVED_D0_TRUE_TO_FALSE_NOT_AUTOMATIC_EVENT"
                        if any(
                            classify_source_transition(
                                day["prior_states"].get(args.symbol, {}).get(signal),
                                day["states"][args.symbol][signal],
                            )
                            == "TRUE_TO_FALSE"
                            for signal in ("LAUNCH_CONFIRM", "RECOVERY_TURN")
                        )
                        else "UNKNOWN_DATA_NO_PROVED_CHANGE"
                        if "UNKNOWN" in day["states"][args.symbol].values()
                        or d1_stock[day["session"]].get("summary_state") == "UNKNOWN"
                        else "D1_NOT_EVALUABLE_NO_PROVED_CHANGE"
                        if d1_stock[day["session"]].get("summary_state")
                        in {"NOT_COMPARABLE", "NOT_EVALUABLE"}
                        else "CALCULATED_NO_PROVED_CHANGE",
                        "states": day["states"].get(args.symbol),
                        "state_transitions": {
                            name: classify_source_transition(
                                day["prior_states"].get(args.symbol, {}).get(name),
                                state,
                            )
                            for name, state in day["states"]
                            .get(args.symbol, {})
                            .items()
                        },
                    },
                    "detail": stock_detail((day["session"], args.symbol)),
                    "d1": d1_stock[day["session"]],
                    "formal_eligibility": "UNKNOWN",
                    "formal_eligibility_reason": "FORMAL_EPISODE_AND_QUALIFICATION_UNPROVED",
                    "formal_authorization": "FORMAL_NOT_AUTHORIZED",
                }
                for day in days
            ]
        board_details = None
        if args.board_id:
            board_details = read_observation_board_details(
                config,
                days[-1],
                snapshot_id=(
                    args.industry_snapshot_id
                    if args.board_kind == "INDUSTRY"
                    else args.concept_snapshot_id
                ),
                board_kind=args.board_kind,
                board_native_id=args.board_id,
                package_sha256=args.package_sha256,
                profile_revision=args.profile_revision,
                research_cutoff=cutoff,
                rotation_start=_parse_session_date(args.rotation_start),
                profile_contract=contract,
                loo_symbol=args.symbol,
            )
        relative_position = None
        if args.relative_position:
            relative_position = relative_position_view(
                days,
                result,
                board_kind=args.board_kind,
                board_id=args.board_id,
                symbol=args.symbol,
                d1_by_session=d1,
            )
            board_group = f"|BOARD:{args.board_kind}:{args.board_id}"
            relative_position["outcome_comparisons"] = [
                row for row in comparisons if row["group"].endswith(board_group)
            ]
            for session_view in relative_position["sessions"]:
                for stock in session_view["stocks"]:
                    key = (session_view["session"], stock["source_symbol"])
                    history = history_cache.get(key)
                    stock["price_followup"] = (
                        [history["points"][n - 1] for n in horizons]
                        if history is not None
                        else "NOT_REQUESTED"
                        if not horizons
                        else None
                    )
                    stock["risk_priority_readiness"] = (
                        stock_detail(key).get(
                            "risk_priority_readiness", stock_detail(key)
                        )
                        if args.symbol
                        else "NOT_EVALUATED_SELECT_SYMBOL"
                    )
        day_summaries = []
        for day in days:
            seed_reasons = {}
            for name in ("s1", "s2", "seed"):
                reason_counts = Counter()
                for fact in day["seed"]["facts"].values():
                    value = fact[name]
                    reasons = value.get("reasons", [])
                    if not reasons and value.get("reason"):
                        reasons = [value["reason"]]
                    reason_counts.update(reasons)
                seed_reasons[name] = dict(reason_counts)
            day_summaries.append(
                {
                    "session": day["session"],
                    "source_pool_count": len(day["states"]),
                    "observation_event_count": sum(
                        event["session"] == day["session"] for event in result["events"]
                    ),
                    "observation_event_symbol_count": len(
                        {
                            event["source_symbol"]
                            for event in result["events"]
                            if event["session"] == day["session"]
                        }
                    ),
                    "market": day["market"],
                    "d0": {
                        name: row["source_pool"]
                        for name, row in day["scenarios"].items()
                    },
                    "seed_states": {
                        name: dict(Counter(row[name] for row in day["states"].values()))
                        for name in ("s1", "s2", "seed")
                    },
                    "seed_reason": day["seed"].get("reason"),
                    "seed_reason_counts": seed_reasons,
                    "profile_revision_matches_runtime": day["seed"].get(
                        "profile_revision_matches_runtime"
                    ),
                    "runtime_profile_revision": day["seed"].get(
                        "runtime_profile_revision"
                    ),
                    "stored_profile_status": day["seed"].get("stored_profile_status"),
                    "profile_input_note": day["seed"].get("profile_input_note"),
                    "transition_counts": day["transition_counts"],
                    "before_only_symbols": day["before_only_symbols"],
                    "d1_counts": d1[day["session"]]["support_event_counts"],
                    "d1_reason_counts": d1[day["session"]][
                        "support_event_reason_counts"
                    ],
                    "d1_raw_counts_by_session": d1[day["session"]][
                        "raw_d1_counts_by_session"
                    ],
                    "formal_pb_continue_d2": "NOT_EVALUATED",
                }
            )
        # D0转FALSE是可证状态变化，但不是现役自动观察事件；只在摘要中补充，
        # 不改变事件、后继对照或冻结算法的定义。
        summary_changes = list(result["events"])
        if args.view == "summary":
            for day in days:
                for symbol, states in day["states"].items():
                    for signal in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
                        if (
                            classify_source_transition(
                                day["prior_states"].get(symbol, {}).get(signal),
                                states[signal],
                            )
                            == "TRUE_TO_FALSE"
                        ):
                            summary_changes.append(
                                {
                                    "session": day["session"],
                                    "source_symbol": symbol,
                                    "signal": signal,
                                    "transition": "TRUE_TO_FALSE",
                                    "memberships": result["memberships_by_symbol"].get(
                                        symbol, []
                                    ),
                                    "source_state_followup": "NOT_REQUESTED"
                                    if not horizons
                                    else "NOT_COMPUTED_TRANSITION_ONLY",
                                    "first_proved_failure_session": "NOT_REQUESTED"
                                    if not horizons
                                    else "NOT_COMPUTED_TRANSITION_ONLY",
                                    "first_candidate_line_touch_session": "NOT_REQUESTED"
                                    if not horizons
                                    else "NOT_COMPUTED_TRANSITION_ONLY",
                                }
                            )
        board_changes_by_key = {}
        market_change_symbols = {}
        for event in summary_changes:
            market_change_symbols.setdefault(event["session"], set()).add(
                event["source_symbol"]
            )
            for member in event["memberships"]:
                key = (
                    event["session"],
                    member["board_kind"],
                    member["board_native_id"],
                )
                group = board_changes_by_key.setdefault(
                    key, {"events": 0, "symbols": set()}
                )
                group["events"] += 1
                group["symbols"].add(event["source_symbol"])
        summary_boards = []
        for row in result["boards"]:
            key = (row["session"], row["board_kind"], row["board_native_id"])
            group = board_changes_by_key.get(key)
            if group:
                summary_boards.append(
                    {
                        **row,
                        "proved_change_count": group["events"],
                        "proved_change_symbol_count": len(group["symbols"]),
                        "market_proved_change_symbol_denominator": len(
                            market_change_symbols[row["session"]]
                        ),
                        "share_of_market_proved_change_symbols": str(
                            Fraction(
                                len(group["symbols"]),
                                len(market_change_symbols[row["session"]]),
                            )
                        ),
                    }
                )
        changed_boards = sorted(
            summary_boards,
            key=lambda row: (
                -row["proved_change_symbol_count"],
                str(row["session"]),
                row["board_kind"],
                row["board_native_id"],
            ),
        )
        changed_stocks = {}
        for event in summary_changes:
            key = (event["session"], event["source_symbol"])
            row = changed_stocks.setdefault(
                key,
                {
                    "session": key[0],
                    "source_symbol": key[1],
                    "memberships": event["memberships"],
                    "signals": [],
                },
            )
            row["signals"].append(
                {
                    "signal": event["signal"],
                    "transition": event["transition"],
                    "source_state_followup": event["source_state_followup"],
                    "first_proved_failure_session": event[
                        "first_proved_failure_session"
                    ],
                    "first_candidate_line_touch_session": event[
                        "first_candidate_line_touch_session"
                    ],
                }
            )
        stock_rows = sorted(
            changed_stocks.values(),
            key=lambda row: (str(row["session"]), row["source_symbol"]),
        )
        distinct_stocks = sorted({row["source_symbol"] for row in stock_rows})
        shown_symbols = set(distinct_stocks[:30])
        output = {
            "ok": True,
            "scope": "NON_PIT_DESCRIPTIVE_NOT_RETURN_VALIDATION",
            "configuration_path": str(config_path),
            "namespace_id": days[0]["namespace_id"],
            "package_sha256": args.package_sha256,
            "profile_revision": args.profile_revision,
            "research_cutoff": cutoff,
            "sessions": day_summaries,
            "membership_status": result["membership_status"],
            "membership_snapshots": result["snapshot_metadata"],
            "boards": changed_boards[:15]
            if args.view == "summary"
            else result["boards"],
            "board_details": board_details,
            **(
                {
                    "relative_position": relative_position,
                    "market_changes": market_changes(days),
                    "board_changes": board_changes(days, result["boards"]),
                }
                if args.relative_position
                else {}
            ),
            "stock_summary": stock_summary,
            "comparisons": comparisons,
            "observation_event_count": result["event_count"],
            "observation_distinct_symbol_count": result["distinct_symbol_count"],
            "page": {
                "offset": args.offset,
                "limit": page_limit,
                "matched_count": len(selected),
                "next_offset": args.offset + page_limit
                if args.offset + page_limit < len(selected)
                else None,
            },
            "events": page_events,
            "page_stock_observations": list(stock_observations.values()),
            "page_stock_observations_scope": "PAGE_KEYS_ALL_SAME_SESSION_SYMBOL_EVENTS",
            "formal_eligibility": "UNKNOWN",
            "formal_authorization": "FORMAL_NOT_AUTHORIZED",
            "formal_eligibility_reason": "FORMAL_EPISODE_AND_QUALIFICATION_UNPROVED",
            "database_write_attempted": False,
            "execution": {
                "elapsed_seconds": time.perf_counter() - started,
                "database_mode": "READ_ONLY",
                "followup_read_metrics": read_metrics,
            },
        }
        if args.view == "summary":
            drilldown_requested = bool(args.symbol or args.board_id or args.signal)
            if not drilldown_requested:
                output["page"] = {"status": "NOT_REQUESTED"}
            for day_summary in day_summaries:
                changes = [
                    event
                    for event in summary_changes
                    if event["session"] == day_summary["session"]
                ]
                day_summary["proved_change_count"] = len(changes)
                day_summary["proved_change_symbol_count"] = len(
                    {event["source_symbol"] for event in changes}
                )
            board_keys = {
                (row["board_kind"], row["board_native_id"]) for row in changed_boards
            }
            shown_board_keys = {
                (row["board_kind"], row["board_native_id"])
                for row in changed_boards[:15]
            }
            output["summary"] = {
                "ordering": "DESCRIPTIVE_ONLY_NOT_PRIORITY",
                "changed_board_session_count": len(changed_boards),
                "omitted_board_session_count": max(0, len(changed_boards) - 15),
                "changed_distinct_board_count": len(board_keys),
                "omitted_distinct_board_count": len(board_keys - shown_board_keys),
                "changed_distinct_symbol_count": len(distinct_stocks),
                "stock_list_scope": "DRILLDOWN_HOME_LIST_NOT_DISPLAYED"
                if drilldown_requested
                else "HOME_CHANGE_LIST",
                "omitted_distinct_symbol_count": len(distinct_stocks)
                if drilldown_requested
                else max(0, len(distinct_stocks) - 30),
                "proved_change_count": len(summary_changes),
                "automatic_observation_event_count": result["event_count"],
                "change_scope": "现役自动观察事件及D0已证TRUE转FALSE；后者不改变自动观察组和对照",
                "stocks": []
                if drilldown_requested
                else [
                    row for row in stock_rows if row["source_symbol"] in shown_symbols
                ],
                "followup": "REQUESTED" if horizons else "NOT_REQUESTED",
                "drilldown": "使用相同来源参数并指定 --symbol，或指定 --board-kind、--board-id、--rotation-start 和 --relative-position",
            }
            if horizons:
                board_group = (
                    f"|BOARD:{args.board_kind}:{args.board_id}"
                    if args.board_id
                    else None
                )
                displayed_comparisons = [
                    row
                    for row in comparisons
                    if "|BOARD:" not in row["group"]
                    or (board_group and row["group"].endswith(board_group))
                ]
                output["comparisons"] = displayed_comparisons
                output["summary"]["comparison_group_count"] = len(comparisons)
                output["summary"]["omitted_comparison_group_count"] = len(
                    comparisons
                ) - len(displayed_comparisons)
            else:
                output["comparisons"] = "NOT_REQUESTED"
        _emit(output)
        return _EXIT_OK
    except (
        RawSourcePgError,
        PgPriceProfileError,
        PgResearchBindingError,
        SourceMarketContextError,
        TdxSectorMembersPgError,
        SourceD0RawError,
        SourceObservationError,
    ) as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
            },
            error=True,
        )
        return (
            _EXIT_ARGUMENT
            if getattr(error, "category", None) == "ARGUMENT"
            else _EXIT_UNAVAILABLE
        )
    except ValueError as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "OBSERVATION_INPUT_ERROR"),
                "message": str(error),
            },
            error=True,
        )
        return _EXIT_ARGUMENT
