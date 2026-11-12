"""封装可由多个现役消费者复用的只读D1支撑变化发现。"""

from __future__ import annotations

import re
from typing import Any, Mapping

from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_source_version_context,
)
from stock_a.research.entry import (
    classify_source_support_change,
    research_stock_pg_support_followup_preview,
    select_source_support_change_page,
)
from stock_a.research.entry_history.history_preview import (
    prepare_source_d1_preview_context,
    research_stock_pg_d1_history_previews,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_raw import read_raw_daily_history_page


def discover_source_d1_support_changes(
    config,
    config_path,
    target,
    cutoff,
    sessions,
    calendar_revision,
    source_package_id,
    event_window_start,
    offset,
    limit,
    include_symbol_details=False,
    source_symbol=None,
    source_version_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """单遍读取Raw来源历史，发现窗口内全部D1事件的目标日支撑变化。"""
    if (
        target not in sessions
        or event_window_start not in sessions
        or event_window_start >= target
        or target > cutoff
        or len(sessions) > 75
        or tuple(sorted(set(sessions))) != tuple(sessions)
        or sessions[-1] != target
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_SESSION_INVALID",
            "目标日与事件窗口必须位于有界版本化官方会话范围内，且窗口早于目标日",
        )

    if source_symbol is not None:
        if not isinstance(source_symbol, str):
            raise PgResearchBindingError(
                "D1_SUPPORT_DISCOVERY_SYMBOL_INVALID", "单股D1解释代码无效"
            )
        normalized_symbol = source_symbol.strip().upper()
        if re.fullmatch(r"(?:SH|SZ)\.[0-9]{6}", normalized_symbol) is None:
            raise PgResearchBindingError(
                "D1_SUPPORT_DISCOVERY_SYMBOL_INVALID", "单股D1解释代码无效"
            )
        source_symbol = normalized_symbol

    event_sessions = tuple(
        day for day in sessions if event_window_start <= day < target
    )
    analysis_sessions = tuple(day for day in sessions if day >= event_window_start)
    preview_context = prepare_source_d1_preview_context(analysis_sessions)
    previous_session = sessions[sessions.index(target) - 1]
    calendar_payload, _calendar_metadata, _, _ = _load_calendar(target)
    calendar_component = f"{target.year}:{calendar_payload.get('revision')}"
    if calendar_component not in calendar_revision.split("+"):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_CALENDAR_INVALID",
            "历史窗口与请求的官方日历修订不一致",
        )
    official_calendar = {
        "calendar_id": calendar_payload["calendar_id"],
        "calendar_revision": calendar_payload["revision"],
    }

    per_session_counts = {
        day.isoformat(): {
            "TRUE": 0,
            "FALSE": 0,
            "UNKNOWN": 0,
            "unknown_reason_counts": {},
        }
        for day in event_sessions
    }
    support_counts = {
        "CHANGED": 0,
        "UNCHANGED": 0,
        "UNKNOWN": 0,
        "NOT_COMPARABLE": 0,
        "NOT_EVALUABLE": 0,
        "OTHER_EXPLAINED": 0,
    }
    support_reason_counts: dict[str, int] = {}
    changed_events: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    seen_event_keys: set[tuple[str, ...]] = set()
    last_symbol = None
    source_context = None
    source_pool_total = None
    source_query_total = None
    source_offset = 0
    source_page_count = 0
    history_preview_count = 0
    history_preview_batch_count = 0
    history_row_decode_count = 0
    raw_d1_event_count = 0
    support_requested_sessions = 0
    symbol_details: dict[str, dict[str, Any]] = {}

    if source_version_context is not None and (
        not isinstance(source_version_context, Mapping)
        or source_version_context.get("status") != "AVAILABLE"
        or source_version_context.get("target_session") != target.isoformat()
        or source_version_context.get("source_package_id") != source_package_id
        or source_version_context.get("version_status") != "COMPLETE"
        or source_version_context.get("identity_status") != "SOURCE_SYMBOL_ONLY"
        or source_version_context.get("price_coordinate") != "RAW_UNADJUSTED"
        or type(source_version_context.get("raw_row_count")) is not int
        or source_version_context.get("raw_row_count") <= 0
        or source_version_context.get("capture_expected_row_count")
        != source_version_context.get("raw_row_count")
        or not source_version_context.get("source_package_id")
        or not source_version_context.get("source_capture_id")
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_SOURCE_CONTEXT_INVALID",
            "复用的D1来源上下文必须完整绑定已闭合的目标日Raw捕获",
        )

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
            source_symbol=source_symbol,
            include_parser_contract=True,
        )
        candidates = page["candidates"]
        if source_query_total is None:
            source_query_total = page["total_count"]
        if source_pool_total is None:
            source_pool_total = page["version_row_count"]
        target_rows = []
        for candidate in candidates:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            symbol_details[symbol] = {
                "source_symbol": symbol,
                "raw_d1_state_counts": {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0},
                "raw_d1_unknown_sessions": [],
                "anchor_events": [],
            }
            dates = [row["session_date"] for row in rows]
            if (
                symbol in seen_symbols
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
                    "D1_SUPPORT_DISCOVERY_HISTORY_INVALID",
                    "来源历史重复、顺序错误或跨代码、跨包、越过目标日",
                )
            target_matches = [row for row in rows if row["session_date"] == target]
            if len(target_matches) != 1:
                raise PgResearchBindingError(
                    "D1_SUPPORT_DISCOVERY_TARGET_ROW_INVALID",
                    "目标日完整来源池中的代码没有唯一Raw行",
                )
            target_rows.extend(target_matches)
            seen_symbols.add(symbol)
            last_symbol = symbol

        context = (
            read_source_version_context(
                config,
                target_session=target,
                source_package_id=source_package_id,
                target_raw_rows=target_rows,
                expected_raw_row_count=page["version_row_count"],
            )
            if source_version_context is None
            else dict(source_version_context)
        )
        if source_version_context is not None:
            context["target_raw_binding_count"] = len(target_rows)
            context["target_raw_binding_verified"] = bool(target_rows)
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
            or (
                page["total_count"] != context["raw_row_count"]
                if source_symbol is None
                else page["total_count"] not in {0, 1}
                or page["total_count"] == 1
                and (
                    len(candidates) != 1
                    or candidates[0]["source_symbol"] != source_symbol
                )
            )
            or page["version_row_count"] != context["raw_row_count"]
            or page["version_expected_row_count"]
            != context["capture_expected_row_count"]
            or page["offset"] != source_offset
            or page["page_count"] != len(candidates)
            or len(target_rows) != len(candidates)
            or page["total_count"] != source_query_total
            or source_version_context is not None
            and (
                context.get("raw_row_count") != page["version_row_count"]
                or context.get("capture_expected_row_count")
                != page["version_expected_row_count"]
                or any(
                    row.get("source_package_id") != source_package_id
                    or row.get("source_package_id") != context.get("source_package_id")
                    or row.get("source_capture_id") != context.get("source_capture_id")
                    for row in target_rows
                )
            )
        ):
            raise PgResearchBindingError(
                "D1_SUPPORT_DISCOVERY_POOL_INCOMPLETE",
                "目标日来源页、捕获分母或来源版本身份不完整",
            )

        for candidate in candidates:
            symbol = candidate["source_symbol"]
            rows = candidate["history_rows"]
            rows_by_session = {row["session_date"]: row for row in rows}
            previews_by_session = research_stock_pg_d1_history_previews(
                symbol=symbol,
                event_sessions=event_sessions,
                followup_session=target,
                session_dates=sessions,
                source_package_id=source_package_id,
                history_rows=rows,
                context=preview_context,
            )
            history_preview_batch_count += 1
            history_preview_count += len(previews_by_session)
            history_row_decode_count += sum(
                row.get("bar_status") == "TARGET_PRESENT" for row in rows
            )
            for day in event_sessions:
                preview = previews_by_session[day]
                predicate = preview.get("raw_d1_breakout_predicate")
                if (
                    preview.get("symbol") != symbol
                    or preview.get("target_session") != day.isoformat()
                    or preview.get("identity_status") != "SOURCE_SYMBOL_ONLY"
                    or preview.get("price_coordinate") != "RAW_UNADJUSTED"
                    or not isinstance(predicate, dict)
                    or predicate.get("target_session") != day.isoformat()
                    or predicate.get("coordinate_id")
                    != f"RAW:TDX_SOURCE_SYMBOL:{symbol}"
                    or predicate.get("official_dependency_window", {}).get(
                        "end_session"
                    )
                    != day.isoformat()
                ):
                    raise PgResearchBindingError(
                        "D1_SUPPORT_DISCOVERY_PREVIEW_INVALID",
                        "历史Core/ALG-29预览缺失或身份、坐标、截止日不一致",
                    )

            symbol_true_events = []
            for day in event_sessions:
                predicate = previews_by_session[day]["raw_d1_breakout_predicate"]
                state = predicate.get("predicates", {}).get("result", {}).get("state")
                if state not in {"TRUE", "FALSE", "UNKNOWN"}:
                    raise PgResearchBindingError(
                        "D1_SUPPORT_DISCOVERY_STATE_INVALID",
                        "历史原始D1谓词没有返回TRUE/FALSE/UNKNOWN三态",
                    )
                session_counts = per_session_counts[day.isoformat()]
                session_counts[state] += 1
                symbol_detail = symbol_details[symbol]
                symbol_detail["raw_d1_state_counts"][state] += 1
                if state == "UNKNOWN":
                    reasons = predicate["predicates"]["result"].get(
                        "dependency_reasons", []
                    )
                    reason = next(iter(reasons), "UNKNOWN_REASON_NOT_PROVIDED")
                    symbol_detail["raw_d1_unknown_sessions"].append(
                        {
                            "session": day.isoformat(),
                            "dependency_reasons": list(reasons) or [reason],
                        }
                    )
                    reason_counts = session_counts["unknown_reason_counts"]
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                elif state == "TRUE":
                    anchor_rows = [row for row in rows if row["session_date"] == day]
                    if len(anchor_rows) != 1:
                        raise PgResearchBindingError(
                            "D1_SUPPORT_DISCOVERY_ANCHOR_ROW_INVALID",
                            "D1 TRUE缺少唯一同包出生日Raw行",
                        )
                    producer_symbol = predicate.get("core_producer_symbol")
                    if not isinstance(producer_symbol, str) or not predicate.get(
                        "core_field_ids"
                    ):
                        raise PgResearchBindingError(
                            "D1_SUPPORT_DISCOVERY_CONTRACT_IDENTITY_INVALID",
                            "D1事件缺少可复核的现役Core合同身份",
                        )
                    event_key = (
                        source_context["source_capture_id"],
                        source_package_id,
                        symbol,
                        day.isoformat(),
                        predicate["coordinate_id"],
                        producer_symbol,
                    )
                    if event_key in seen_event_keys:
                        raise PgResearchBindingError(
                            "D1_SUPPORT_DISCOVERY_EVENT_DUPLICATE",
                            "来源级D1事件业务键重复",
                        )
                    seen_event_keys.add(event_key)
                    symbol_true_events.append(
                        {
                            "session": day,
                            "predicate": predicate,
                            "anchor_row": anchor_rows[0],
                            "target_row": rows_by_session[target],
                        }
                    )
                    raw_d1_event_count += 1

            for event in symbol_true_events:
                anchor_session = event["session"]
                requested_sessions = tuple(
                    day for day in sessions if anchor_session <= day <= target
                )
                support_requested_sessions += len(requested_sessions)
                support_points = []
                for day in requested_sessions:
                    present_rows = tuple(
                        row for row in rows if row["session_date"] <= day
                    )
                    support_points.append(
                        {
                            "session": day,
                            "row": rows_by_session.get(day),
                            "preview": previews_by_session[day],
                            "history_max_session": max(
                                (row["session_date"] for row in present_rows),
                                default=None,
                            ),
                        }
                    )
                support_followup = research_stock_pg_support_followup_preview(
                    symbol=symbol,
                    target_session=target,
                    support_anchor_session=anchor_session,
                    official_sessions=sessions,
                    requested_sessions=requested_sessions,
                    history_points=support_points,
                    source_package_id=source_package_id,
                    source_context=source_context,
                    official_calendar=official_calendar,
                )
                comparison = classify_source_support_change(
                    support_followup,
                    previous_session=previous_session,
                    target_session=target,
                    post_anchor_session_count=len(
                        tuple(day for day in sessions if anchor_session < day <= target)
                    ),
                )
                classification = comparison["classification"]
                support_counts[classification] += 1
                reason_code = comparison.get("reason_code")
                if reason_code:
                    support_reason_counts[reason_code] = (
                        support_reason_counts.get(reason_code, 0) + 1
                    )
                predicate = event["predicate"]
                anchor_row = event["anchor_row"]
                target_row = event["target_row"]
                detail_event = {
                    "source_symbol": symbol,
                    "support_anchor_session": anchor_session.isoformat(),
                    "previous_official_session": previous_session.isoformat(),
                    "target_session": target.isoformat(),
                    "change_classification": classification,
                    "reason_code": reason_code,
                    "event_key": {
                        "target_source_capture_id": source_context["source_capture_id"],
                        "source_symbol": symbol,
                        "event_session": anchor_session.isoformat(),
                        "price_coordinate": "RAW_UNADJUSTED",
                        "coordinate_id": predicate["coordinate_id"],
                        "algorithm_contract_identity": {
                            "algorithm_family": "ALG-29",
                            "predicate": "RAW_D1_BREAKOUT",
                            "core_producer_symbol": predicate.get(
                                "core_producer_symbol"
                            ),
                            "core_field_ids": predicate.get("core_field_ids"),
                        },
                    },
                    "raw_d1_breakout_predicate": predicate,
                    "platform_generation": support_followup.get("platform_generation"),
                    "support_observation_before": comparison.get(
                        "previous_observation"
                    ),
                    "support_observation_after": comparison.get("current_observation"),
                    "support_followup_status": support_followup.get("status"),
                    "source": {
                        "price_coordinate": "RAW_UNADJUSTED",
                        "anchor_capture_id": int(anchor_row["source_capture_id"]),
                        "target_capture_id": int(target_row["source_capture_id"]),
                        "source_package_id": source_context.get("source_package_id"),
                        "source_capture_id": source_context.get("source_capture_id"),
                    },
                }
                symbol_details[symbol]["anchor_events"].append(detail_event)
                if classification == "CHANGED":
                    changed_events.append(detail_event)

        source_page_count += 1
        expected_next = source_offset + len(candidates)
        if page["next_offset"] is None:
            if expected_next != page["total_count"]:
                raise PgResearchBindingError(
                    "D1_SUPPORT_DISCOVERY_PAGE_INCOMPLETE", "来源分页提前结束"
                )
            break
        if (
            not candidates
            or page["next_offset"] != expected_next
            or page["next_offset"] >= page["total_count"]
        ):
            raise PgResearchBindingError(
                "D1_SUPPORT_DISCOVERY_PAGE_INVALID", "来源分页游标不连续"
            )
        source_offset = page["next_offset"]

    if (
        source_context is None
        or source_pool_total is None
        or len(seen_symbols) != source_query_total
        or source_pool_total != source_context["raw_row_count"]
        or (
            source_symbol is None
            and len(seen_symbols) != source_context["raw_row_count"]
        )
        or (
            source_symbol is not None
            and len(seen_symbols) == 1
            and source_symbol not in seen_symbols
        )
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_POOL_INCOMPLETE",
            "Raw来源池分页未覆盖目标日捕获分母",
        )

    for day in event_sessions:
        counts = per_session_counts[day.isoformat()]
        if sum(counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")) != len(
            seen_symbols
        ):
            raise PgResearchBindingError(
                "D1_SUPPORT_DISCOVERY_D1_PARTITION_INCOMPLETE",
                f"{day.isoformat()}来源D1三态分母不守恒",
            )
    if sum(support_counts.values()) != raw_d1_event_count:
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_SUPPORT_PARTITION_INCOMPLETE",
            "全部历史D1 TRUE事件与支撑分类分母不守恒",
        )

    complete_symbol_details = []
    for symbol in sorted(seen_symbols):
        detail = symbol_details[symbol]
        classifications = {
            event["change_classification"] for event in detail["anchor_events"]
        }
        if "CHANGED" in classifications:
            summary_state = "CHANGED"
        elif "UNKNOWN" in classifications or detail["raw_d1_state_counts"]["UNKNOWN"]:
            summary_state = "UNKNOWN"
        elif "NOT_COMPARABLE" in classifications:
            summary_state = "NOT_COMPARABLE"
        elif (
            "NOT_EVALUABLE" in classifications
            or "OTHER_EXPLAINED" in classifications
            or not classifications
        ):
            summary_state = "NOT_EVALUABLE"
        else:
            summary_state = "UNCHANGED"
        detail["summary_state"] = summary_state
        detail["anchor_events"].sort(key=lambda item: item["support_anchor_session"])
        detail["raw_d1_unknown_sessions"].sort(key=lambda item: item["session"])
        complete_symbol_details.append(detail)

    events_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for event in changed_events:
        events_by_symbol.setdefault(event["source_symbol"], []).append(event)
    changed_symbols = [
        {
            "source_symbol": symbol,
            "change_classification": "CHANGED",
            "changed_event_count": len(events),
            "change_events": events,
        }
        for symbol, events in sorted(events_by_symbol.items())
    ]
    unique_changed_symbol_count = len(events_by_symbol)
    if unique_changed_symbol_count != len(
        {event["source_symbol"] for event in changed_events}
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_SYMBOL_DEDUP_INVALID",
            "CHANGED事件集合与去重来源代码集合不一致",
        )
    selected_page = select_source_support_change_page(
        changed_symbols, offset=offset, limit=limit, change_only=True
    )
    result = {
        "ok": True,
        "status": "SOURCE_LEVEL_MULTI_SESSION_SUPPORT_CHANGE_DISCOVERY",
        "contract": "ALG-29_RAW_BREAKOUT_PREDICATE_WITH_STRUCTURE_SUPPORT_FOLLOWUP",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "scope": (
            "TARGET_SESSION_RAW_SOURCE_POOL_EVENT_WINDOW_DISCOVERY"
            if source_symbol is None
            else "SOURCE_SYMBOL_ONLY_TARGET_SESSION_EVENT_WINDOW_D1_SUPPORT_EXPLANATION"
        ),
        "target_session": target.isoformat(),
        "event_window_start": event_window_start.isoformat(),
        "event_window_end_exclusive": target.isoformat(),
        "previous_official_session": previous_session.isoformat(),
        "research_cutoff": cutoff.isoformat(),
        "calendar_revision": calendar_revision,
        "official_calendar": {
            "calendar_id": official_calendar["calendar_id"],
            "calendar_revision": official_calendar["calendar_revision"],
            "previous_target_sessions_adjacent": True,
        },
        "source": source_context,
        "source_capture_started_at": source_context.get("capture_read_started_at"),
        "source_capture_validated_at": source_context.get("capture_validated_at"),
        "source_pool_count": source_context["raw_row_count"],
        "evaluated_source_symbol_count": len(seen_symbols),
        "evaluated_source_symbol": source_symbol,
        "source_pool_partition_closed": source_symbol is None
        and len(seen_symbols) == source_context["raw_row_count"],
        "anchor_window_session_count": len(event_sessions),
        "raw_d1_counts_by_session": {
            day: {
                **{state: counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")},
                "unknown_reason_counts": dict(
                    sorted(counts["unknown_reason_counts"].items())
                ),
                "evaluated_partition_closed": sum(
                    counts[state] for state in ("TRUE", "FALSE", "UNKNOWN")
                )
                == len(seen_symbols),
                "partition_closed": source_symbol is None
                and sum(counts[state] for state in ("TRUE", "FALSE", "UNKNOWN"))
                == source_context["raw_row_count"],
            }
            for day, counts in per_session_counts.items()
        },
        "raw_d1_event_count": raw_d1_event_count if source_symbol is None else None,
        "evaluated_raw_d1_event_count": raw_d1_event_count,
        "raw_d1_evaluated_partition_closed": all(
            sum(counts[state] for state in ("TRUE", "FALSE", "UNKNOWN"))
            == len(seen_symbols)
            for counts in per_session_counts.values()
        ),
        "raw_d1_partition_closed": source_symbol is None
        and all(
            sum(counts[state] for state in ("TRUE", "FALSE", "UNKNOWN"))
            == source_context["raw_row_count"]
            for counts in per_session_counts.values()
        ),
        "support_event_counts": support_counts if source_symbol is None else None,
        "evaluated_support_event_counts": support_counts,
        "support_event_reason_counts": dict(sorted(support_reason_counts.items())),
        "support_event_partition_closed": source_symbol is None
        and sum(support_counts.values()) == raw_d1_event_count,
        "support_event_evaluated_partition_closed": sum(support_counts.values())
        == raw_d1_event_count,
        "unique_changed_symbol_count": unique_changed_symbol_count
        if source_symbol is None
        else None,
        "evaluated_unique_changed_symbol_count": unique_changed_symbol_count,
        "changed_symbol_partition_closed": source_symbol is None
        and unique_changed_symbol_count
        == len({event["source_symbol"] for event in changed_events}),
        "evaluated_changed_symbol_partition_closed": unique_changed_symbol_count
        == len({event["source_symbol"] for event in changed_events}),
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
            "raw_history_batch_reads": source_page_count,
            "raw_d1_candidates_evaluated": len(seen_symbols) * len(event_sessions),
            "cached_core_previews_evaluated": history_preview_count,
            "d1_core_preview_batches_evaluated": history_preview_batch_count,
            "full_core_preview_calls_evaluated": 0,
            "d1_core_input_evaluations": history_preview_count,
            "source_history_rows_decoded_once": history_row_decode_count,
            "preview_calendar_resources_loaded": len(preview_context.calendars_by_year),
            "support_followups_evaluated": raw_d1_event_count,
            "support_requested_sessions": support_requested_sessions,
        },
    }
    if include_symbol_details:
        result["complete_symbol_details"] = complete_symbol_details
    return result
