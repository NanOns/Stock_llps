"""Stock A研究universe_history职责模块。"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterable, Mapping, Sequence

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_history.history_preview import (
    research_stock_pg_history_preview,
)
from stock_a.research.entry_context.sessions import _load_calendar


def research_universe_pg_history_preview(
    *,
    target_session: str,
    package_sha256: str,
    session_dates: Sequence[date],
    page: Mapping[str, Any],
    profile_contract: dict[str, Any],
    closed_period_bars_by_symbol: Mapping[str, Mapping[str, Sequence[object]]]
    | None = None,
    include_profile_preview: bool = True,
    include_raw_breakout: bool = False,
) -> dict[str, Any]:
    """以PG SQL分页候选结果生成来源符号级Core/Profile质量摘要。"""
    results: list[dict[str, Any]] = []
    for candidate in page.get("candidates", ()):
        symbol = candidate["source_symbol"]
        try:
            preview = research_stock_pg_history_preview(
                symbol=symbol,
                target_session=target_session,
                package_sha256=package_sha256,
                session_dates=session_dates,
                history_rows=candidate["history_rows"],
                missing_history_sessions=tuple(
                    date.fromisoformat(item) for item in candidate["missing_sessions"]
                ),
                namespace_id=page["namespace_id"],
                profile_contract=profile_contract,
                closed_period_bars=(
                    closed_period_bars_by_symbol.get(symbol)
                    if isinstance(closed_period_bars_by_symbol, Mapping)
                    else None
                ),
                include_profile_preview=include_profile_preview,
                include_raw_breakout=include_raw_breakout,
            )
        except ResearchEntryError as error:
            field_registry = (
                profile_contract.get("scope_bindings", {})
                .get("FOUNDATION_EOD", {})
                .get("field_registry", [])
            )
            profile_registry = (
                profile_contract.get("scope_bindings", {})
                .get("FOUNDATION_EOD", {})
                .get("profile_expression_registry", {})
            )
            error_code = error.code
            target_row = next(
                (
                    row
                    for row in candidate["history_rows"]
                    if row.get("session_date") == date.fromisoformat(target_session)
                ),
                None,
            )
            target_status = (target_row or {}).get("bar_status")
            target_status = {
                "TARGET_PRESENT": "TDX_TARGET_BAR_PRESENT",
                "TARGET_MISSING": "TDX_TARGET_BAR_MISSING",
                "READ_FAILED": "TDX_READ_FAILED",
            }.get(target_status, "TDX_READ_FAILED")
            core_field_ids = sorted(
                {
                    item.get("field_id")
                    for item in field_registry
                    if isinstance(item, dict)
                    and isinstance(item.get("field_id"), str)
                    and item["field_id"].startswith("core.")
                }
            )
            component_ids = sorted(
                {
                    rule.get("field_id")
                    for rule in profile_registry.values()
                    if isinstance(rule, dict) and isinstance(rule.get("field_id"), str)
                }
            )
            if len(core_field_ids) != 43 or len(component_ids) != 11:
                raise ResearchEntryError(
                    "PROFILE_PREVIEW_CONTRACT_INVALID",
                    "逐代码研究失败时无法从现役合同恢复Core/Profile字段身份",
                ) from error
            field_names = (
                "core.ma5",
                "core.ma10",
                "core.ma20",
                "core.ma60",
                "core.atr20",
                "core.clv",
                "core.prior_high20",
                "core.ret20",
                "core.pos60",
                "core.bias20_atr",
                "core.ma_slope20_atr",
                "core.amount_ratio5",
                "core.amount_ratio20",
                "core.volume_ratio5",
                "core.volume_ratio20",
                "core.price_damage",
            )
            row_dates = {
                row.get("session_date"): row for row in candidate["history_rows"]
            }
            expected = tuple(session_dates)
            present = set(row_dates)
            (
                calendar_payload,
                _calendar_metadata,
                calendar_revision,
                _calendar_sessions,
            ) = _load_calendar(date.fromisoformat(target_session))
            stored_count = len(row_dates)
            valid_count = sum(
                row.get("bar_status") == "TARGET_PRESENT"
                and row.get("price_quality") == "OBSERVED"
                for row in candidate["history_rows"]
            )
            ma20_valid_count = sum(
                day in row_dates
                and row_dates[day].get("bar_status") == "TARGET_PRESENT"
                and row_dates[day].get("price_quality") == "OBSERVED"
                for day in expected[-20:]
            )
            atr20_valid_count = sum(
                day in row_dates
                and row_dates[day].get("bar_status") == "TARGET_PRESENT"
                and row_dates[day].get("price_quality") == "OBSERVED"
                for day in expected[-21:]
            )
            results.append(
                {
                    "symbol": symbol,
                    "status": "CONSUMER_FAILED",
                    "consumer_status": "CONSUMER_FAILED",
                    "consumer_error_code": error_code,
                    "identity_status": "SOURCE_SYMBOL_ONLY",
                    "source_object_status": "TDX_SOURCE_SYMBOL_OBSERVED",
                    "target_bar_status": target_status,
                    "price_research_status": "UNKNOWN_DATA",
                    "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
                    "listing_status": "LISTING_STATUS_UNKNOWN",
                    "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
                    "pit_status": "NON_PIT_RETROSPECTIVE",
                    "window": {
                        "calendar_id": calendar_payload["calendar_id"],
                        "calendar_revision": calendar_revision,
                        "start_session": expected[0].isoformat(),
                        "end_session": expected[-1].isoformat(),
                        "requested_sessions": len(expected),
                        "stored_sessions": stored_count,
                        "real_bar_count": valid_count,
                        "session_status_counts": {},
                    },
                    "history_status": (
                        "RECORDS_COMPLETE"
                        if stored_count == len(expected)
                        else "HISTORY_ROW_NOT_STORED"
                    ),
                    "history_records_complete": stored_count == len(expected),
                    "history_records_status": (
                        "RECORDS_COMPLETE"
                        if stored_count == len(expected)
                        else "HISTORY_ROW_NOT_STORED"
                    ),
                    "price_window_usable": False,
                    "price_window_status": "PRICE_FIELDS_UNKNOWN",
                    "price_window_reason": error_code,
                    "price_window_valid_bar_count": atr20_valid_count,
                    "price_window_expected_bar_count": min(21, len(expected)),
                    "ma20_window_valid_bar_count": ma20_valid_count,
                    "ma20_window_expected_bar_count": min(20, len(expected)),
                    "missing_history_sessions": [
                        day.isoformat() for day in expected if day not in present
                    ],
                    "price_quality_counts": {},
                    "core_price_fields": {
                        name: {
                            "value": None,
                            "quality": "UNKNOWN",
                            "reason": error_code,
                        }
                        for name in field_names
                    },
                    "core_unknown_field_count": 43,
                    "core_unknown_reason_counts": {error_code: 43},
                    "profile_rule_count": 11,
                    "profile_status_counts": {"FAILED": 11},
                    "profile_components": [
                        {
                            "field_id": field_id,
                            "status": "FAILED",
                            "quality": "UNKNOWN",
                            "reason": error_code,
                        }
                        for field_id in component_ids
                    ],
                    "profile_unavailable": [
                        {
                            "field_id": field_id,
                            "status": "FAILED",
                            "quality": "UNKNOWN",
                            "reason": error_code,
                        }
                        for field_id in component_ids
                    ],
                }
            )
            continue
        fields = preview.get("core", {}).get("fields", {})
        profile_preview = preview.get("profile_preview")
        components = (
            profile_preview.get("components", [])
            if isinstance(profile_preview, Mapping)
            else []
        )
        profile_status_counts: dict[str, int] = {}
        profile_unavailable = []
        for component in components:
            status = str(component.get("status", "UNKNOWN"))
            quality = (
                str(component.get("quality", status))
                if status == "EVALUATED"
                else status
            )
            profile_status_counts[quality] = profile_status_counts.get(quality, 0) + 1
            if quality != "OBSERVED":
                profile_unavailable.append(
                    {
                        "field_id": component.get("field_id"),
                        "status": status,
                        "quality": component.get("quality"),
                        "reason": component.get("reason"),
                    }
                )
        selected_fields = {
            name: fields[name]
            for name in (
                "core.ma5",
                "core.ma10",
                "core.ma20",
                "core.ma60",
                "core.atr20",
                "core.clv",
                "core.prior_high20",
                "core.ret20",
                "core.pos60",
                "core.bias20_atr",
                "core.ma_slope20_atr",
                "core.amount_ratio5",
                "core.amount_ratio20",
                "core.volume_ratio5",
                "core.volume_ratio20",
                "core.price_damage",
            )
            if name in fields
        }
        result_item = {
            "symbol": symbol,
            "status": preview.get("status"),
            "consumer_status": "RESEARCHED",
            "consumer_error_code": None,
            "identity_status": preview.get("identity_status"),
            "source_object_status": preview.get("source_object_status"),
            "target_bar_status": preview.get("target_bar_status"),
            "price_research_status": preview.get("price_research_status"),
            "stable_security_id_status": preview.get("stable_security_id_status"),
            "listing_status": preview.get("listing_status"),
            "suspension_status": preview.get("suspension_status"),
            "pit_status": preview.get("pit_status"),
            "window": preview.get("window"),
            "history_status": preview.get("history_status"),
            "history_records_complete": preview.get("history_records_complete"),
            "history_records_status": preview.get("history_records_status"),
            "price_window_usable": preview.get("price_window_usable"),
            "price_window_status": preview.get("price_window_status"),
            "price_window_reason": preview.get("price_window_reason"),
            "price_window_valid_bar_count": preview.get("price_window_valid_bar_count"),
            "price_window_expected_bar_count": preview.get(
                "price_window_expected_bar_count"
            ),
            "ma20_window_valid_bar_count": preview.get("ma20_window_valid_bar_count"),
            "ma20_window_expected_bar_count": preview.get(
                "ma20_window_expected_bar_count"
            ),
            "missing_history_sessions": preview.get("missing_history_sessions"),
            "price_quality_counts": preview.get("quality", {}).get(
                "price_quality_counts"
            ),
            "core_price_fields": selected_fields,
            "core_unknown_field_count": preview.get("core", {}).get(
                "unknown_field_count"
            ),
            "core_unknown_reason_counts": preview.get("core", {}).get(
                "unknown_reason_counts"
            ),
            "profile_rule_count": len(components),
            "profile_status_counts": dict(sorted(profile_status_counts.items())),
            "profile_components": [
                {
                    "field_id": item.get("field_id"),
                    "status": item.get("status"),
                    "quality": item.get("quality"),
                    "reason": item.get("reason"),
                }
                for item in components
            ],
            "profile_unavailable": profile_unavailable,
        }
        if include_raw_breakout:
            result_item["source_capture_revisions"] = preview.get("source", {}).get(
                "capture_revisions"
            )
            result_item["raw_d1_breakout_predicate"] = preview.get(
                "raw_d1_breakout_predicate"
            )
        results.append(result_item)
    return {
        "ok": True,
        "contract": "STOCK_A_PG_SOURCE_SYMBOL_UNIVERSE_CORE_PROFILE_PREVIEW_R1",
        "status": page["status"],
        "target_session": target_session,
        "package_sha256": package_sha256,
        "research_mode": "SOURCE_SYMBOL_NON_PIT_PREVIEW",
        "price_coordinate": "RAW_UNADJUSTED",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "source_object_status": "TDX_SOURCE_SYMBOL_OBSERVED",
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "listing_status": "LISTING_STATUS_UNKNOWN",
        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "official_listing_denominator": "UNKNOWN_NOT_PROVED_BY_SOURCE_CANDIDATES",
        "namespace_id": page["namespace_id"],
        "session_start": page["session_start"],
        "session_end": page["session_end"],
        "session_count": page["session_count"],
        "filters": {"market": page["market_filter"], "board": page["board_filter"]},
        "page": {
            "offset": page["offset"],
            "limit": page["limit"],
            "page_count": page["page_count"],
            "total_source_candidates": page["total_count"],
            "next_offset": page["next_offset"],
            "symbols": [item["symbol"] for item in results],
        },
        "target_day_source_status_counts": page["bar_status_counts"],
        "candidate_partition_closed": page["partition_closed"],
        "source_version_status": page["version_status"],
        "source_version_row_count": page["version_row_count"],
        "source_version_expected_row_count": page["version_expected_row_count"],
        "results": results,
        "execution": {
            "sql_pagination": True,
            "source_zip_opened": False,
            "source_scan_performed": False,
            "source_recalculated": False,
            "ddl_executed": False,
            "database_write": False,
            "network_access": False,
        },
    }
