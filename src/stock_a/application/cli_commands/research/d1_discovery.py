"""Stock A命令行d1_discovery职责模块。"""

from __future__ import annotations

from datetime import date
from typing import Any
from stock_a.contracts import (
    load_price_profile_contract,
)
from stock_a.storage.postgres.source_raw import (
    RAW_DAILY_APPEND_TASK_SCOPES,
    RawSourcePgError,
    import_verified_vipdata_target,
    import_verified_vipdata_history,
    read_raw_daily_catalog,
    read_raw_daily_page,
    read_raw_daily_history_page,
    read_raw_daily_history_stock,
    read_raw_daily_stock,
)
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_source_version_context,
)
from stock_a.research.entry import (
    CompareBudget,
    ResearchEntryError,
    UniverseDiscoveryBudget,
    _normalize_symbol,
    _load_calendar,
    _parse_target,
    apply_identity_observation,
    research_compare,
    research_stock,
    research_stock_pg_history_preview,
    research_stock_pg_support_followup_preview,
    classify_source_support_change,
    select_source_support_change_page,
    research_universe_preview,
    research_universe_pg_history_preview,
)
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_PERMISSION,
    _EXIT_UNAVAILABLE,
    _WORKSPACE,
    load_runtime_config as _load_runtime_config,
)


def _official_calendar(target: date) -> dict[str, Any]:
    payload, metadata, revision_chain, sessions = _load_calendar(target)
    return {
        "calendar_id": payload["calendar_id"],
        "calendar_revision": payload["revision"],
        "calendar_revision_chain": revision_chain,
        "sessions": sessions,
    }


def _discover_source_d1_true(
    config,
    config_path,
    target,
    cutoff,
    sessions,
    calendar_revision,
    source_package_id,
    offset,
    limit,
    previous_state=None,
) -> dict[str, Any]:
    """完整评价Raw来源池后，才对TRUE集合分页。"""

    if previous_state is not None and previous_state not in {
        "ALL",
        "TRUE",
        "FALSE",
        "UNKNOWN",
    }:
        raise PgResearchBindingError(
            "D1_BREAKOUT_PREVIOUS_STATE_INVALID", "前态筛选值无效"
        )
    if previous_state is not None and len(sessions) < 2:
        raise PgResearchBindingError(
            "D1_BREAKOUT_PREVIOUS_SESSION_UNAVAILABLE",
            "前态筛选至少需要目标会话及其紧邻前一官方会话",
        )
    previous_session = sessions[-2] if previous_state is not None else None
    loaded_contract = load_price_profile_contract(_CONFIG_ROOT)
    contract = loaded_contract.document
    counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
    previous_counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
    reasons: dict[str, int] = {}
    matches = []
    seen: set[str] = set()
    source_context = None
    source_offset = 0
    previous = None
    while True:
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            source_package_id=source_package_id,
            sessions=sessions,
            offset=source_offset,
            limit=100,
            market="ALL",
            board="ALL",
        )
        candidates = page["candidates"]
        target_rows = []
        for candidate in candidates:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            dates = [row["session_date"] for row in rows]
            if (
                symbol in seen
                or (previous is not None and symbol <= previous)
                or len(dates) != len(set(dates))
                or any(day not in sessions for day in dates)
            ):
                raise PgResearchBindingError(
                    "D1_BREAKOUT_POOL_DUPLICATE_OR_WINDOW_INVALID",
                    "来源代码或历史窗口不闭合",
                )
            target_matches = [row for row in rows if row["session_date"] == target]
            if len(target_matches) != 1:
                raise PgResearchBindingError(
                    "D1_BREAKOUT_TARGET_ROW_INVALID", "来源代码缺少唯一目标日Raw行"
                )
            target_rows.extend(target_matches)
            seen.add(symbol)
            previous = symbol
        context = read_source_version_context(
            config,
            target_session=target,
            source_package_id=source_package_id,
            target_raw_rows=target_rows,
            expected_raw_row_count=page["version_row_count"],
        )
        if source_context is None:
            source_context = context
        if (
            any(
                context[key] != source_context[key]
                for key in (
                    "source_package_id",
                    "source_capture_id",
                    "raw_row_count",
                )
            )
            or page["version_status"] != "CLOSED"
            or not page["partition_closed"]
            or page["total_count"] != context["raw_row_count"]
            or page["version_expected_row_count"]
            != context["capture_expected_row_count"]
            or page["target_session"] != target.isoformat()
            or page["source_package_id"] != source_package_id
            or page["session_start"] != sessions[0].isoformat()
            or page["session_end"] != target.isoformat()
            or page["session_count"] != len(sessions)
            or page["offset"] != source_offset
            or page["page_count"] != len(candidates)
            or len(target_rows) != len(candidates)
        ):
            raise PgResearchBindingError(
                "D1_BREAKOUT_SOURCE_POOL_NOT_CLOSED", "Raw分页或捕获分母不一致"
            )
        preview = research_universe_pg_history_preview(
            target_session=target.isoformat(),
            source_package_id=source_package_id,
            session_dates=sessions,
            page=page,
            profile_contract=contract,
            include_profile_preview=False,
            include_raw_breakout=True,
        )
        if len(preview["results"]) != len(candidates):
            raise PgResearchBindingError(
                "D1_BREAKOUT_CONSUMER_INCOMPLETE", "来源页计算结果数量不一致"
            )
        for candidate, row, item in zip(
            candidates, target_rows, preview["results"], strict=True
        ):
            predicate = item.get("raw_d1_breakout_predicate")
            if (
                item.get("symbol") != candidate["source_symbol"]
                or item.get("consumer_status") != "RESEARCHED"
                or not isinstance(predicate, dict)
            ):
                raise PgResearchBindingError(
                    "D1_BREAKOUT_CONSUMER_INCOMPLETE", "来源代码计算失败或错绑"
                )
            state = predicate["predicates"]["result"]["state"]
            if state not in counts:
                raise PgResearchBindingError(
                    "D1_BREAKOUT_STATE_INVALID", "原始谓词状态无效"
                )
            counts[state] += 1
            if state == "UNKNOWN":
                reason = (
                    predicate["predicates"]["result"].get("reason")
                    or "UNKNOWN_REASON_NOT_PROVIDED"
                )
                reasons[reason] = reasons.get(reason, 0) + 1
            if state == "TRUE":
                result = {
                    "symbol": item["symbol"],
                    "consumer_status": item["consumer_status"],
                    "target_bar_status": item.get("target_bar_status"),
                    "history_records_complete": item.get("history_records_complete"),
                    "price_window_status": item.get("price_window_status"),
                    "source_revision": {
                        "source_capture_id": str(row["source_capture_id"]),
                        "source_package_id": row.get("source_package_id"),
                        "source_clock_role": row.get("source_clock_role"),
                        "source_time_quality": context["source_time_quality"],
                        "source_available_at": row.get("source_available_at"),
                        "observed_started_at": row.get("observed_started_at"),
                        "observed_completed_at": row.get("observed_completed_at"),
                        "price_coordinate": "RAW_UNADJUSTED",
                        "coordinate_id": f"RAW:TDX_SOURCE_SYMBOL:{item['symbol']}",
                    },
                    "capture_revisions": item.get("source_capture_revisions"),
                    "raw_d1_breakout_predicate": predicate,
                }
                if previous_state is not None:
                    previous_window = tuple(
                        day for day in sessions if day <= previous_session
                    )
                    previous_rows = tuple(
                        history_row
                        for history_row in candidate["history_rows"]
                        if history_row["session_date"] <= previous_session
                    )
                    previous_dates = {
                        history_row["session_date"] for history_row in previous_rows
                    }
                    if any(
                        history_row.get("source_symbol") != candidate["source_symbol"]
                        or history_row.get("source_package_id") != source_package_id
                        or history_row["session_date"] > previous_session
                        for history_row in previous_rows
                    ):
                        raise PgResearchBindingError(
                            "D1_BREAKOUT_PREVIOUS_HISTORY_BINDING_INVALID",
                            "前态历史越过前一官方会话或来源身份不一致",
                        )
                    previous_preview = research_stock_pg_history_preview(
                        symbol=candidate["source_symbol"],
                        target_session=previous_session.isoformat(),
                        source_package_id=source_package_id,
                        session_dates=previous_window,
                        history_rows=previous_rows,
                        missing_history_sessions=tuple(
                            day for day in previous_window if day not in previous_dates
                        ),
                        profile_contract=contract,
                        include_profile_preview=False,
                        include_raw_breakout=True,
                        allow_missing_target_row=True,
                    )
                    previous_predicate = previous_preview.get(
                        "raw_d1_breakout_predicate"
                    )
                    if not isinstance(previous_predicate, dict):
                        raise PgResearchBindingError(
                            "D1_BREAKOUT_PREVIOUS_CONSUMER_INCOMPLETE",
                            "前一官方会话的ALG-29计算未返回谓词证据",
                        )
                    prior_state = previous_predicate["predicates"]["result"]["state"]
                    if prior_state not in previous_counts:
                        raise PgResearchBindingError(
                            "D1_BREAKOUT_PREVIOUS_STATE_INVALID",
                            "前一官方会话的原始谓词状态无效",
                        )
                    previous_counts[prior_state] += 1
                    previous_row = next(
                        (
                            history_row
                            for history_row in previous_rows
                            if history_row["session_date"] == previous_session
                        ),
                        None,
                    )
                    result["adjacent_comparison"] = {
                        "previous_official_session": previous_session.isoformat(),
                        "current_session": target.isoformat(),
                        "official_sessions_adjacent": True,
                        "previous_state": prior_state,
                        "current_state": state,
                        "transition_type": f"{prior_state}_TO_{state}",
                        "previous_bar_status": previous_preview.get(
                            "target_bar_status"
                        ),
                        "previous_history_records_complete": previous_preview.get(
                            "history_records_complete"
                        ),
                        "previous_price_window_status": previous_preview.get(
                            "price_window_status"
                        ),
                        "previous_price_window_reason": previous_preview.get(
                            "price_window_reason"
                        ),
                        "previous_source_revision": {
                            "source_capture_id": str(previous_row["source_capture_id"])
                            if previous_row
                            else None,
                            "source_package_id": previous_row.get("source_package_id")
                            if previous_row
                            else None,
                            "source_available_at": previous_row.get(
                                "source_available_at"
                            )
                            if previous_row
                            else None,
                            "observed_started_at": previous_row.get(
                                "observed_started_at"
                            )
                            if previous_row
                            else None,
                            "observed_completed_at": previous_row.get(
                                "observed_completed_at"
                            )
                            if previous_row
                            else None,
                            "price_coordinate": "RAW_UNADJUSTED",
                            "coordinate_id": f"RAW:TDX_SOURCE_SYMBOL:{item['symbol']}",
                        },
                        "raw_d1_breakout_predicate": previous_predicate,
                    }
                matches.append(result)
        next_offset = page["next_offset"]
        expected_next = source_offset + len(candidates)
        if next_offset is None:
            if expected_next != page["total_count"]:
                raise PgResearchBindingError(
                    "D1_BREAKOUT_POOL_INCOMPLETE", "来源池提前结束"
                )
            break
        if (
            not candidates
            or next_offset != expected_next
            or next_offset >= page["total_count"]
        ):
            raise PgResearchBindingError(
                "D1_BREAKOUT_PAGE_INVALID", "来源分页游标不连续"
            )
        source_offset = next_offset
    if len(seen) != source_context["raw_row_count"] or sum(counts.values()) != len(
        seen
    ):
        raise PgResearchBindingError(
            "D1_BREAKOUT_POOL_INCOMPLETE", "来源全集与互斥计数不一致"
        )
    source_context["target_raw_binding_count"] = len(seen)
    source_context["target_raw_binding_verified"] = True
    if previous_state is not None and sum(previous_counts.values()) != counts["TRUE"]:
        raise PgResearchBindingError(
            "D1_BREAKOUT_PREVIOUS_PARTITION_INCOMPLETE",
            "当日TRUE候选的前态TRUE/FALSE/UNKNOWN分区未闭合",
        )
    filtered_matches = (
        matches
        if previous_state in (None, "ALL")
        else [
            item
            for item in matches
            if item["adjacent_comparison"]["previous_state"] == previous_state
        ]
    )
    selected = filtered_matches[offset : offset + limit]
    result = {
        "ok": True,
        "status": "SOURCE_LEVEL_RAW_BREAKOUT_TRUE_DISCOVERY",
        "contract": "ALG-29_RAW_BREAKOUT_PREDICATE",
        "day_chain_contract": {
            "contract_id": contract["contract_id"],
            "version": contract["version"],
            "contract_version": loaded_contract.document["version"],
        },
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_session": target.isoformat(),
        "research_cutoff": cutoff.isoformat(),
        "calendar_revision": calendar_revision,
        "source": source_context,
        "source_capture_started_at": source_context.get("capture_read_started_at"),
        "source_capture_validated_at": source_context.get("capture_validated_at"),
        "source_pool_actual_total": len(seen),
        "predicate_counts": counts,
        "unknown_reason_counts": dict(sorted(reasons.items())),
        "predicate_partition_closed": True,
        "page": {
            "offset": offset,
            "limit": limit,
            "page_count": len(selected),
            "filtered_total": len(filtered_matches),
            "next_offset": offset + len(selected)
            if offset + len(selected) < len(filtered_matches)
            else None,
            "stable_order": "source_symbol ASC",
            "symbols": [item["symbol"] for item in selected],
        },
        "results": selected,
        "formal_downstream": {
            "d1_anchor": "NOT_EVALUATED",
            "episode": "NOT_EVALUATED",
            "d2_seed_focus": "NOT_EVALUATED",
            "trade_signal": "NOT_EVALUATED",
        },
        "execution": {
            "configuration_path": str(config_path),
            "database_transaction": "REPEATABLE_READ_READ_ONLY",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "source_zip_opened": False,
        },
    }
    if previous_state is not None:
        official_calendar = _official_calendar(target)
        result["previous_state_filter"] = previous_state
        result["previous_session"] = previous_session.isoformat()
        result["official_calendar"] = {
            "calendar_id": official_calendar["calendar_id"],
            "calendar_revision": official_calendar["calendar_revision"],
            "adjacency_verified_from_session_window": True,
        }
        result["current_true_total"] = counts["TRUE"]
        result["previous_state_counts"] = previous_counts
        result["previous_state_partition_closed"] = (
            sum(previous_counts.values()) == counts["TRUE"]
        )
        result["page"]["previous_state_filter"] = previous_state
        result["page"]["filtered_total"] = len(filtered_matches)
    return result


def _discover_source_d1_support_cohort(
    config,
    config_path,
    target,
    cutoff,
    sessions,
    calendar_revision,
    source_package_id,
    support_anchor,
    offset,
    limit,
    *,
    change_only,
) -> dict[str, Any]:
    """闭合目标日Raw来源池后，先算锚日D1，再批量观察同锚支撑变化。"""

    if (
        support_anchor not in sessions
        or target not in sessions
        or support_anchor >= target
        or target > cutoff
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_COHORT_SESSION_INVALID",
            "支撑批次锚日须是早于目标日的版本化官方会话",
        )

    target_index = sessions.index(target)
    anchor_index = sessions.index(support_anchor)
    previous_session = sessions[target_index - 1]
    post_anchor_sessions = tuple(
        day for day in sessions if support_anchor < day <= target
    )
    requested_sessions = tuple(
        day for day in sessions if support_anchor <= day <= target
    )
    anchor_sessions = tuple(day for day in sessions if day <= support_anchor)
    official_calendar = _official_calendar(target)
    loaded_contract = load_price_profile_contract(_CONFIG_ROOT)
    profile_contract = loaded_contract.document
    predicate_counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
    unknown_reasons: dict[str, int] = {}
    true_candidates: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    last_symbol = None
    source_context = None
    source_offset = 0
    source_page_count = 0

    while True:
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            source_package_id=source_package_id,
            sessions=sessions,
            offset=source_offset,
            limit=100,
            market="ALL",
            board="ALL",
        )
        candidates = page["candidates"]
        target_rows = []
        for candidate in candidates:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            dates = [row["session_date"] for row in rows]
            if (
                symbol in seen
                or (last_symbol is not None and symbol <= last_symbol)
                or dates != sorted(set(dates))
                or any(day not in sessions or day > target for day in dates)
                or any(
                    row.get("source_symbol") != symbol
                    or row.get("source_package_id") != source_package_id
                    for row in rows
                )
            ):
                raise PgResearchBindingError(
                    "D1_SUPPORT_COHORT_HISTORY_INVALID",
                    "来源代码重复、顺序错误或历史行跨代码、来源包、越过目标日",
                )
            matches = [row for row in rows if row["session_date"] == target]
            if len(matches) != 1:
                raise PgResearchBindingError(
                    "D1_SUPPORT_COHORT_TARGET_ROW_INVALID",
                    "完整目标来源池中的代码没有唯一目标日Raw行",
                )
            target_rows.extend(matches)
            seen.add(symbol)
            last_symbol = symbol

        context = read_source_version_context(
            config,
            target_session=target,
            source_package_id=source_package_id,
            target_raw_rows=target_rows,
            expected_raw_row_count=page["version_row_count"],
        )
        if source_context is None:
            source_context = context
        context_keys = (
            "target_session",
            "source_package_id",
            "source_capture_id",
            "raw_row_count",
            "capture_expected_row_count",
        )
        if (
            context.get("status") != "AVAILABLE"
            or any(context.get(key) != source_context.get(key) for key in context_keys)
            or page["version_status"] != "CLOSED"
            or not page["partition_closed"]
            or page["total_count"] != context["raw_row_count"]
            or page["version_row_count"] != context["raw_row_count"]
            or page["version_expected_row_count"]
            != context["capture_expected_row_count"]
            or page["target_session"] != target.isoformat()
            or page["source_package_id"] != source_package_id
            or page["session_start"] != sessions[0].isoformat()
            or page["session_end"] != target.isoformat()
            or page["session_count"] != len(sessions)
            or page["offset"] != source_offset
            or page["page_count"] != len(candidates)
            or len(target_rows) != len(candidates)
        ):
            raise PgResearchBindingError(
                "D1_SUPPORT_COHORT_POOL_NOT_CLOSED",
                "目标日来源页、Raw捕获分母或版本身份不闭合",
            )

        for candidate in candidates:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            prefix_rows = tuple(
                row for row in rows if row["session_date"] <= support_anchor
            )
            prefix_dates = {row["session_date"] for row in prefix_rows}
            preview = research_stock_pg_history_preview(
                symbol=symbol,
                target_session=support_anchor.isoformat(),
                source_package_id=source_package_id,
                session_dates=anchor_sessions,
                history_rows=prefix_rows,
                missing_history_sessions=tuple(
                    day for day in anchor_sessions if day not in prefix_dates
                ),
                profile_contract=profile_contract,
                include_profile_preview=False,
                include_raw_breakout=True,
                allow_missing_target_row=True,
            )
            predicate = preview.get("raw_d1_breakout_predicate")
            if (
                preview.get("symbol") != symbol
                or preview.get("target_session") != support_anchor.isoformat()
                or preview.get("identity_status") != "SOURCE_SYMBOL_ONLY"
                or preview.get("price_coordinate") != "RAW_UNADJUSTED"
                or not isinstance(predicate, dict)
                or predicate.get("target_session") != support_anchor.isoformat()
                or predicate.get("coordinate_id") != f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
            ):
                raise PgResearchBindingError(
                    "D1_SUPPORT_COHORT_ANCHOR_PREVIEW_INVALID",
                    "锚日Core/ALG-29预览缺失或身份、坐标不一致",
                )
            state = predicate.get("predicates", {}).get("result", {}).get("state")
            if state not in predicate_counts:
                raise PgResearchBindingError(
                    "D1_SUPPORT_COHORT_ANCHOR_STATE_INVALID",
                    "锚日原始D1谓词未返回TRUE/FALSE/UNKNOWN三态",
                )
            predicate_counts[state] += 1
            if state == "UNKNOWN":
                reasons = predicate["predicates"]["result"].get(
                    "dependency_reasons", []
                )
                reason = next(iter(reasons), "UNKNOWN_REASON_NOT_PROVIDED")
                unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1
            if state == "TRUE":
                anchor_rows = [
                    row for row in rows if row["session_date"] == support_anchor
                ]
                if len(anchor_rows) != 1:
                    raise PgResearchBindingError(
                        "D1_SUPPORT_COHORT_ANCHOR_ROW_INVALID",
                        "D1 TRUE缺少唯一同包锚日Raw行",
                    )
                true_candidates[symbol] = {
                    "history_rows": rows,
                    "raw_d1_breakout_predicate": predicate,
                    "anchor_row": anchor_rows[0],
                    "target_row": next(
                        row for row in rows if row["session_date"] == target
                    ),
                }

        source_page_count += 1
        expected_next = source_offset + len(candidates)
        if page["next_offset"] is None:
            if expected_next != page["total_count"]:
                raise PgResearchBindingError(
                    "D1_SUPPORT_COHORT_PAGE_INCOMPLETE", "来源分页提前结束"
                )
            break
        if (
            not candidates
            or page["next_offset"] != expected_next
            or page["next_offset"] >= page["total_count"]
        ):
            raise PgResearchBindingError(
                "D1_SUPPORT_COHORT_PAGE_INVALID", "来源分页游标不连续"
            )
        source_offset = page["next_offset"]

    if (
        source_context is None
        or len(seen) != source_context["raw_row_count"]
        or sum(predicate_counts.values()) != len(seen)
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_COHORT_POOL_INCOMPLETE",
            "目标日完整来源池与锚日原始D1三态分母不守恒",
        )

    support_counts = {
        "CHANGED": 0,
        "UNCHANGED": 0,
        "UNKNOWN": 0,
        "NOT_COMPARABLE": 0,
        "NOT_EVALUABLE": 0,
        "OTHER_EXPLAINED": 0,
    }
    support_reason_counts: dict[str, int] = {}
    cohort_results = []
    for symbol in sorted(true_candidates):
        candidate = true_candidates[symbol]
        rows = candidate["history_rows"]
        support_points = []
        for day in requested_sessions:
            bounded_rows = tuple(row for row in rows if row["session_date"] <= day)
            present = {row["session_date"] for row in bounded_rows}
            preview = research_stock_pg_history_preview(
                symbol=symbol,
                target_session=day.isoformat(),
                source_package_id=source_package_id,
                session_dates=tuple(session for session in sessions if session <= day),
                history_rows=bounded_rows,
                missing_history_sessions=tuple(
                    session
                    for session in sessions
                    if session <= day and session not in present
                ),
                profile_contract=profile_contract,
                include_profile_preview=False,
                include_raw_breakout=True,
                allow_missing_target_row=True,
            )
            day_predicate = preview.get("raw_d1_breakout_predicate")
            if (
                preview.get("symbol") != symbol
                or preview.get("target_session") != day.isoformat()
                or preview.get("identity_status") != "SOURCE_SYMBOL_ONLY"
                or preview.get("price_coordinate") != "RAW_UNADJUSTED"
                or not isinstance(day_predicate, dict)
                or day_predicate.get("target_session") != day.isoformat()
                or day_predicate.get("official_dependency_window", {}).get(
                    "end_session"
                )
                != day.isoformat()
            ):
                raise PgResearchBindingError(
                    "D1_SUPPORT_COHORT_FOLLOWUP_PREVIEW_INVALID",
                    "支撑历史Core/ALG-29前态越界或来源坐标不匹配",
                )
            row = next(
                (item for item in bounded_rows if item["session_date"] == day), None
            )
            support_points.append(
                {
                    "session": day,
                    "row": row,
                    "preview": preview,
                    "history_max_session": max(present, default=None),
                }
            )

        anchor_point = next(
            point for point in support_points if point["session"] == support_anchor
        )
        if (
            anchor_point["preview"].get("raw_d1_breakout_predicate")
            != candidate["raw_d1_breakout_predicate"]
        ):
            raise PgResearchBindingError(
                "D1_SUPPORT_COHORT_ANCHOR_RECALC_MISMATCH",
                "锚日D1完整来源池阶段与支撑阶段重算结果不一致",
            )
        support_followup = research_stock_pg_support_followup_preview(
            symbol=symbol,
            target_session=target,
            support_anchor_session=support_anchor,
            official_sessions=sessions,
            requested_sessions=requested_sessions,
            history_points=support_points,
            source_package_id=source_package_id,
            source_context=source_context,
            official_calendar={
                key: official_calendar[key]
                for key in ("calendar_id", "calendar_revision")
            },
        )
        comparison = classify_source_support_change(
            support_followup,
            previous_session=previous_session,
            target_session=target,
            post_anchor_session_count=len(post_anchor_sessions),
        )
        classification = comparison["classification"]
        support_counts[classification] += 1
        reason_code = comparison.get("reason_code")
        if reason_code:
            support_reason_counts[reason_code] = (
                support_reason_counts.get(reason_code, 0) + 1
            )
        anchor_row = candidate["anchor_row"]
        target_row = candidate["target_row"]
        cohort_results.append(
            {
                "symbol": symbol,
                "source_symbol": symbol,
                "support_anchor_session": support_anchor.isoformat(),
                "previous_official_session": previous_session.isoformat(),
                "target_session": target.isoformat(),
                "change_classification": classification,
                "reason_code": reason_code,
                "support_followup_status": support_followup.get("status"),
                "platform_generation": support_followup.get("platform_generation"),
                "raw_d1_breakout_predicate": candidate["raw_d1_breakout_predicate"],
                "support_observation_before": comparison.get("previous_observation"),
                "support_observation_after": comparison.get("current_observation"),
                "source": {
                    "source_package_id": source_package_id,
                    "price_coordinate": "RAW_UNADJUSTED",
                    "coordinate_id": f"RAW:TDX_SOURCE_SYMBOL:{symbol}",
                    "anchor_capture_id": str(anchor_row["source_capture_id"]),
                    "anchor_clock_role": anchor_row.get("source_clock_role"),
                    "anchor_source_available_at": anchor_row.get("source_available_at"),
                    "anchor_observed_started_at": anchor_row.get("observed_started_at"),
                    "anchor_observed_completed_at": anchor_row.get(
                        "observed_completed_at"
                    ),
                    "target_capture_id": str(target_row["source_capture_id"]),
                    "target_clock_role": target_row.get("source_clock_role"),
                    "target_source_available_at": target_row.get("source_available_at"),
                    "target_observed_started_at": target_row.get("observed_started_at"),
                    "target_observed_completed_at": target_row.get(
                        "observed_completed_at"
                    ),
                },
            }
        )

    if sum(support_counts.values()) != predicate_counts["TRUE"]:
        raise PgResearchBindingError(
            "D1_SUPPORT_COHORT_PARTITION_INCOMPLETE",
            "锚日D1 TRUE批次与支撑变化分类分母不守恒",
        )
    selected_page = select_source_support_change_page(
        cohort_results, offset=offset, limit=limit, change_only=change_only
    )
    return {
        "ok": True,
        "status": "SOURCE_LEVEL_RAW_BREAKOUT_SUPPORT_CHANGE_COHORT",
        "contract": "ALG-29_RAW_BREAKOUT_PREDICATE_WITH_STRUCTURE_SUPPORT_FOLLOWUP",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "scope": "TARGET_SESSION_RAW_SOURCE_POOL_HISTORICAL_BREAKOUT_COHORT",
        "target_session": target.isoformat(),
        "support_anchor_session": support_anchor.isoformat(),
        "previous_official_session": previous_session.isoformat(),
        "research_cutoff": cutoff.isoformat(),
        "calendar_revision": calendar_revision,
        "official_calendar": {
            "calendar_id": official_calendar["calendar_id"],
            "calendar_revision": official_calendar["calendar_revision"],
            "anchor_and_target_same_versioned_calendar": True,
            "previous_target_sessions_adjacent": True,
        },
        "source": source_context,
        "source_capture_started_at": source_context.get("capture_read_started_at"),
        "source_capture_validated_at": source_context.get("capture_validated_at"),
        "source_pool_actual_total": len(seen),
        "raw_d1_predicate_counts": predicate_counts,
        "raw_d1_unknown_reason_counts": dict(sorted(unknown_reasons.items())),
        "raw_d1_partition_closed": sum(predicate_counts.values()) == len(seen),
        "cohort_total": predicate_counts["TRUE"],
        "support_change_counts": support_counts,
        "support_change_reason_counts": dict(sorted(support_reason_counts.items())),
        "support_change_partition_closed": sum(support_counts.values())
        == predicate_counts["TRUE"],
        "change_only_filter": change_only,
        "page": selected_page["page"],
        "results": selected_page["results"],
        "formal_downstream": {
            "stable_security_id": "NOT_ISSUED",
            "official_listing_universe": "NOT_EVALUATED",
            "p15_anchor": "NOT_CREATED",
            "d2_seed_focus": "NOT_EVALUATED",
            "trade_signal": "NOT_EVALUATED",
        },
        "execution": {
            "configuration_path": str(config_path),
            "database_transaction": "REPEATABLE_READ_READ_ONLY_PER_SOURCE_PAGE",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "source_zip_opened": False,
            "source_pages_read": source_page_count,
            "raw_d1_candidates_evaluated": len(seen),
            "support_followups_evaluated": predicate_counts["TRUE"],
            "support_requested_sessions": len(requested_sessions),
        },
    }
