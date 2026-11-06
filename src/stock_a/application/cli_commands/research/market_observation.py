"""来源级市场观察研究的正式命令行消费者。"""

from __future__ import annotations

from collections import Counter
from copy import deepcopy
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
    attach_observation_outcomes,
    attach_observation_state_followup,
    compare_source_outcomes,
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
        horizons = sorted(set(args.horizon))
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
        )[: max(horizons)]
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
        attach_observation_state_followup(
            days + followup_days, result, d1_by_session=d1
        )
        attach_observation_outcomes(
            config,
            days,
            result,
            research_cutoff=cutoff,
            package_sha256=args.package_sha256,
            horizon=max(horizons),
        )
        comparisons = [
            row
            for horizon in horizons
            for row in compare_source_outcomes(
                config,
                days,
                result,
                research_cutoff=cutoff,
                package_sha256=args.package_sha256,
                horizon=horizon,
            )
        ]
        selected = [
            event
            for event in result["events"]
            if (not args.symbol or event["source_symbol"] == args.symbol)
            and (
                not args.board_id
                or any(
                    membership["board_kind"] == args.board_kind
                    and membership["board_native_id"] == args.board_id
                    for membership in event["memberships"]
                )
            )
        ]
        page_events = selected[args.offset : args.offset + args.limit]
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
                    "detail": stock_detail((day["session"], args.symbol)),
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
        _emit(
            {
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
                "boards": result["boards"],
                "board_details": board_details,
                "stock_summary": stock_summary,
                "comparisons": comparisons,
                "observation_event_count": result["event_count"],
                "observation_distinct_symbol_count": result["distinct_symbol_count"],
                "page": {
                    "offset": args.offset,
                    "limit": args.limit,
                    "matched_count": len(selected),
                    "next_offset": args.offset + args.limit
                    if args.offset + args.limit < len(selected)
                    else None,
                },
                "events": page_events,
                "formal_eligibility": "UNKNOWN",
                "formal_authorization": "FORMAL_NOT_AUTHORIZED",
                "formal_eligibility_reason": "FORMAL_EPISODE_AND_QUALIFICATION_UNPROVED",
                "database_write_attempted": False,
                "execution": {
                    "elapsed_seconds": time.perf_counter() - started,
                    "database_mode": "READ_ONLY",
                },
            }
        )
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
