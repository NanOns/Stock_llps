"""将完整TDX板块收益集合与来源级D1支撑变化作只读联接。"""

from __future__ import annotations

from collections import Counter
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.application.tdx_sector_members import TdxSectorMembersApplicationError


_SUPPORT_STATES = {
    "CHANGED",
    "UNCHANGED",
    "UNKNOWN",
    "NOT_COMPARABLE",
    "NOT_EVALUABLE",
    "OTHER_EXPLAINED",
}
_SUMMARY_STATES = {
    "CHANGED",
    "UNCHANGED",
    "UNKNOWN",
    "NOT_COMPARABLE",
    "NOT_EVALUABLE",
}
_MEMBER_STATE_ORDER = {
    "CHANGED": 0,
    "UNKNOWN": 1,
    "NOT_COMPARABLE": 2,
    "NOT_EVALUABLE": 3,
    "UNCHANGED": 4,
}
_SOURCE_SYMBOL_PATTERN = re.compile(r"(?:SH|SZ|BJ)\.[0-9]{6}")


def _fraction_value(value: Any) -> Fraction | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TdxSectorMembersApplicationError(
            "STRENGTH_SORT_VALUE_INVALID", "强弱来源排序值不是精确分数"
        )
    try:
        numerator = value["numerator"]
        denominator = value["denominator"]
        if type(numerator) is not int or type(denominator) is not int:
            raise ValueError
        return Fraction(numerator, denominator)
    except (KeyError, TypeError, ValueError, ZeroDivisionError) as error:
        raise TdxSectorMembersApplicationError(
            "STRENGTH_SORT_VALUE_INVALID", "强弱来源排序值缺少有效分子或分母"
        ) from error


def _d1_symbol_index(discovery: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    records = discovery.get("complete_symbol_details")
    if not isinstance(records, list):
        raise TdxSectorMembersApplicationError(
            "D1_COMPLETE_SYMBOL_SET_REQUIRED", "D1联接要求完整来源代码与锚事件集合"
        )
    indexed: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise TdxSectorMembersApplicationError(
                "D1_SYMBOL_DETAIL_INVALID", "D1完整代码集合包含无效记录"
            )
        symbol = record.get("source_symbol")
        if not isinstance(symbol, str) or not symbol or symbol in indexed:
            raise TdxSectorMembersApplicationError(
                "D1_SYMBOL_DETAIL_DUPLICATE", "D1来源代码缺失或重复"
            )
        state = record.get("summary_state")
        events = record.get("anchor_events")
        unknown_sessions = record.get("raw_d1_unknown_sessions")
        counts = record.get("raw_d1_state_counts")
        if (
            state not in _SUMMARY_STATES
            or not isinstance(events, list)
            or not isinstance(unknown_sessions, list)
            or not isinstance(counts, Mapping)
        ):
            raise TdxSectorMembersApplicationError(
                "D1_SYMBOL_DETAIL_INVALID", "D1来源代码状态、锚列表或质量原因缺失"
            )
        for event in events:
            if (
                not isinstance(event, Mapping)
                or event.get("change_classification") not in _SUPPORT_STATES
            ):
                raise TdxSectorMembersApplicationError(
                    "D1_ANCHOR_EVENT_INVALID", "D1历史锚的支撑变化状态无效"
                )
        indexed[symbol] = record
    if len(indexed) != discovery.get("source_pool_count"):
        raise TdxSectorMembersApplicationError(
            "D1_SYMBOL_PARTITION_NOT_CLOSED", "D1完整代码集合与SEALED来源池分母不一致"
        )
    return indexed


def _member_detail(symbol: str, d1_symbol: Mapping[str, Any] | None) -> dict[str, Any]:
    if d1_symbol is None:
        return {
            "source_symbol": symbol,
            "change_state": "NOT_EVALUABLE",
            "unknown_reasons": ["MEMBER_NOT_IN_EXACT_TARGET_SOURCE_POOL"],
            "raw_d1_state_counts": {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0},
            "raw_d1_unknown_sessions": [],
            "historical_anchor_sessions": [],
            "anchor_events": [],
        }

    events = d1_symbol["anchor_events"]
    unknown_sessions = d1_symbol["raw_d1_unknown_sessions"]
    reasons = sorted(
        {
            str(reason)
            for event in events
            if event.get("reason_code")
            for reason in (event["reason_code"],)
        }
        | {
            str(reason)
            for item in unknown_sessions
            for reason in item.get("dependency_reasons", [])
        }
    )
    anchors = []
    for event in events:
        predicate = event.get("raw_d1_breakout_predicate") or {}
        result = predicate.get("predicates", {}).get("result", {})
        anchors.append(
            {
                "session": event["support_anchor_session"],
                "raw_d1_state": result.get("state", "TRUE"),
                "change_state": event["change_classification"],
                "unknown_reason": event.get("reason_code"),
                "event_key": event.get("event_key"),
                "support_observation_before": event.get("support_observation_before"),
                "support_observation_after": event.get("support_observation_after"),
                "support_followup_status": event.get("support_followup_status"),
            }
        )
    return {
        "source_symbol": symbol,
        "change_state": d1_symbol["summary_state"],
        "unknown_reasons": reasons,
        "raw_d1_state_counts": dict(d1_symbol["raw_d1_state_counts"]),
        "raw_d1_unknown_sessions": list(unknown_sessions),
        "historical_anchor_sessions": [item["session"] for item in anchors],
        "anchor_events": anchors,
    }


def _changed_stock_page(
    *,
    discovery: Mapping[str, Any],
    d1_symbols: Mapping[str, Mapping[str, Any]],
    all_board_rows: Sequence[Mapping[str, Any]],
    offset: int,
    limit: int,
) -> dict[str, Any]:
    changed_symbols = sorted(
        symbol
        for symbol, detail in d1_symbols.items()
        if detail.get("summary_state") == "CHANGED"
    )
    changed_event_symbols = {
        symbol
        for symbol, detail in d1_symbols.items()
        if any(
            event.get("change_classification") == "CHANGED"
            for event in detail.get("anchor_events", [])
        )
    }
    expected_count = discovery.get("unique_changed_symbol_count")
    if (
        type(expected_count) is not int
        or expected_count != len(changed_symbols)
        or changed_symbols != sorted(changed_event_symbols)
        or discovery.get("changed_symbol_partition_closed") is not True
    ):
        raise TdxSectorMembersApplicationError(
            "D1_CHANGED_SYMBOL_COUNT_MISMATCH",
            "全局CHANGED源码集合与既有去重分母或锚事件集合不一致",
        )
    if any(
        _SOURCE_SYMBOL_PATTERN.fullmatch(symbol) is None for symbol in changed_symbols
    ):
        raise TdxSectorMembersApplicationError(
            "D1_CHANGED_SYMBOL_INVALID", "全局CHANGED集合含非法来源代码"
        )

    source = discovery["source"]
    required_source_fields = (
        "namespace_id",
        "source_set_id",
        "revision_id",
        "target_session",
        "package_sha256",
        "source_content_id",
        "source_raw_digest",
        "source_capture_id",
    )
    if any(not source.get(field) for field in required_source_fields):
        raise TdxSectorMembersApplicationError(
            "D1_SOURCE_IDENTITY_MISSING", "全局CHANGED页缺少完整SEALED来源引用"
        )
    for detail in d1_symbols.values():
        for event in detail.get("anchor_events", []):
            event_source = event.get("source")
            if (
                not isinstance(event_source, Mapping)
                or event_source.get("namespace_id") != source["namespace_id"]
                or event_source.get("source_set_id") != source["source_set_id"]
                or event_source.get("revision_id") != source["revision_id"]
                or event_source.get("package_sha256") != source["package_sha256"]
                or event_source.get("source_content_id") != source["source_content_id"]
                or event_source.get("source_raw_digest") != source["source_raw_digest"]
                or event_source.get("source_capture_id") != source["source_capture_id"]
                or event_source.get("target_capture_id") != source["source_capture_id"]
                or event.get("target_session") != discovery["target_session"]
            ):
                raise TdxSectorMembersApplicationError(
                    "D1_CHANGED_EVENT_SOURCE_MISMATCH",
                    "D1变化锚事件与精确SEALED来源包或目标捕获不一致",
                )

    sectors_by_symbol: dict[str, dict[tuple[str, str, str], dict[str, Any]]] = {
        symbol: {} for symbol in changed_symbols
    }
    for board in all_board_rows:
        symbols = board.get("_member_symbols")
        snapshot = board.get("_board_snapshot")
        kind = board.get("board_kind")
        native_id = board.get("board_native_id")
        if (
            not isinstance(symbols, list)
            or not isinstance(snapshot, Mapping)
            or kind not in {"INDUSTRY", "CONCEPT"}
            or not isinstance(native_id, str)
        ):
            raise TdxSectorMembersApplicationError(
                "R03_BOARD_MEMBER_SET_INVALID", "全局CHANGED页的成员关联输入不完整"
            )
        snapshot_id = snapshot.get("snapshot_id")
        source_kind = snapshot.get("source_kind")
        source_sha256 = snapshot.get("source_file_sha256")
        if (
            not isinstance(snapshot_id, str)
            or not snapshot_id
            or not isinstance(source_kind, str)
            or not isinstance(source_sha256, str)
        ):
            raise TdxSectorMembersApplicationError(
                "R03_SNAPSHOT_IDENTITY_INVALID", "成员关联缺少精确快照身份"
            )
        membership_key = (kind, snapshot_id, native_id)
        for symbol in symbols:
            memberships = sectors_by_symbol.get(symbol)
            if memberships is None:
                continue
            membership = {
                "board_kind": kind,
                "snapshot_id": snapshot_id,
                "board_native_id": native_id,
                "board_name": board.get("board_name"),
                "native_category": "GN" if kind == "CONCEPT" else "INDUSTRY",
                "source_kind": source_kind,
                "source_file_sha256": source_sha256,
                "source_observed_at": snapshot.get("source_observed_at"),
                "source_valid_on": snapshot.get("source_valid_on"),
                "source_known_at": snapshot.get("source_known_at"),
                "source_available_at": snapshot.get("source_available_at"),
                "source_clock_quality": snapshot.get("source_clock_quality"),
                "source_quality": snapshot.get("source_quality"),
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
            }
            previous = memberships.get(membership_key)
            if previous is not None and previous != membership:
                raise TdxSectorMembersApplicationError(
                    "R03_BOARD_MEMBERSHIP_DUPLICATE",
                    "同一快照板块身份的成员关联元数据互相冲突",
                )
            memberships[membership_key] = membership

    stock_rows = []
    for symbol in changed_symbols:
        member = _member_detail(symbol, d1_symbols[symbol])
        events = d1_symbols[symbol]["anchor_events"]
        changed_events = [
            event for event in events if event.get("change_classification") == "CHANGED"
        ]
        if not changed_events:
            raise TdxSectorMembersApplicationError(
                "D1_CHANGED_EVENT_MISSING", "CHANGED源码没有对应的变化锚事件"
            )
        member["change_state"] = "CHANGED"
        member["changed_anchor_event_count"] = len(changed_events)
        member["raw_d1_event_count"] = len(events)
        member["target_session"] = discovery["target_session"]
        member["research_cutoff"] = discovery["research_cutoff"]
        member["event_window_start"] = discovery["event_window_start"]
        member["source_evidence"] = {
            field: source[field] for field in required_source_fields
        }
        member["source_evidence"].update(
            {
                "source_digest": source.get("source_digest"),
                "seal_status": source.get("seal_status"),
                "sealed_at": discovery.get("source_sealed_at"),
                "capture_started_at": discovery.get("source_capture_started_at"),
                "capture_validated_at": discovery.get("source_capture_validated_at"),
                "official_calendar": discovery.get("official_calendar"),
                "price_coordinate": "RAW_UNADJUSTED",
                "entity_identity": "SOURCE_SYMBOL_ONLY",
                "pit_status": "NON_PIT_RETROSPECTIVE",
            }
        )
        member["membership_mode"] = "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        memberships = sectors_by_symbol[symbol]
        member["also_in_sectors"] = [memberships[key] for key in sorted(memberships)]
        member["unmapped_reason"] = (
            None
            if member["also_in_sectors"]
            else "NO_CURRENT_TDX_BOARD_MEMBERSHIP_MATCH"
        )
        history_base = [
            "stock-a",
            "source-d1-breakout-preview",
            "--target-session",
            discovery["target_session"],
            "--research-cutoff",
            discovery["research_cutoff"],
            "--source-set-id",
            source["source_set_id"],
            "--package-sha256",
            source["package_sha256"],
            "--symbol",
            symbol,
            "--history-start",
            discovery["event_window_start"],
            "--history-end",
            discovery["target_session"],
        ]
        member["drill_down"] = {
            "research_stock_argv": [
                "stock-a",
                "research-stock",
                symbol,
                "--target-session",
                discovery["target_session"],
                "--unit-version",
                "TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1",
                "--pg-source-history",
                "--pg-source-sha256",
                source["package_sha256"],
            ],
            "d1_history_argv": [
                history_base
                + [
                    "--support-anchor-session",
                    event["support_anchor_session"],
                ]
                for event in events
            ]
            or [history_base],
        }
        stock_rows.append(member)

    page_rows = stock_rows[offset : offset + limit]
    next_offset = offset + len(page_rows)
    total_count = len(stock_rows)
    return {
        "status": "AVAILABLE" if page_rows else "EMPTY",
        "total_count": total_count,
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(page_rows),
            "next_offset": next_offset if next_offset < total_count else None,
            "stable_order": "source_symbol ASC COLLATE C",
        },
        "stocks": page_rows,
    }


def combine_sector_strength_and_d1_changes(
    strength_results: Sequence[Mapping[str, Any]],
    discovery: Mapping[str, Any],
    *,
    board_kind_filter: str | None,
    detail_board_kind: str | None,
    detail_board_native_id: str | None,
    changed_only: bool,
    sort_by: str,
    direction: str,
    offset: int,
    limit: int,
    member_state: str,
    member_offset: int,
    member_limit: int,
    include_changed_stock_page: bool = False,
    stock_offset: int = 0,
    stock_limit: int = 30,
) -> dict[str, Any]:
    """先联接完整来源集合，再过滤、排序及分页板块和板内股票。"""
    if (
        not isinstance(strength_results, (list, tuple))
        or not strength_results
        or type(changed_only) is not bool
        or sort_by not in {"changed_stocks", "delta20", "delta5", "ret20", "ret5"}
        or direction not in {"STRONGER", "WEAKER"}
        or type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
        or member_state
        not in {
            "ALL",
            "CHANGED",
            "UNCHANGED",
            "UNKNOWN",
            "NOT_COMPARABLE",
            "NOT_EVALUABLE",
        }
        or type(member_offset) is not int
        or member_offset < 0
        or type(member_limit) is not int
        or not 0 <= member_limit <= 100
        or type(include_changed_stock_page) is not bool
        or type(stock_offset) is not int
        or stock_offset < 0
        or type(stock_limit) is not int
        or not 1 <= stock_limit <= 30
    ):
        raise TdxSectorMembersApplicationError(
            "SECTOR_CHANGE_PAGE_INVALID", "行业概念联动筛选、排序或分页参数无效"
        )
    if board_kind_filter not in {None, "INDUSTRY", "CONCEPT"}:
        raise TdxSectorMembersApplicationError(
            "BOARD_KIND_INVALID", "正式行业概念联动只接受INDUSTRY或CONCEPT"
        )
    if (detail_board_kind is None) != (detail_board_native_id is None):
        raise TdxSectorMembersApplicationError(
            "DETAIL_BOARD_SELECTION_INVALID", "板块明细必须同时指定类型和精确原生编号"
        )
    if detail_board_kind is not None and detail_board_kind not in {
        "INDUSTRY",
        "CONCEPT",
    }:
        raise TdxSectorMembersApplicationError(
            "BOARD_KIND_INVALID", "板块明细只接受INDUSTRY或CONCEPT"
        )

    if (
        discovery.get("ok") is not True
        or discovery.get("raw_d1_partition_closed") is not True
        or discovery.get("support_event_partition_closed") is not True
        or discovery.get("changed_symbol_partition_closed") is not True
    ):
        raise TdxSectorMembersApplicationError(
            "D1_DISCOVERY_PARTITION_NOT_CLOSED", "D1全池事件与变化分类分母未闭合"
        )
    d1_symbols = _d1_symbol_index(discovery)
    d1_source = discovery.get("source")
    if not isinstance(d1_source, Mapping):
        raise TdxSectorMembersApplicationError(
            "D1_SOURCE_IDENTITY_MISSING", "D1发现结果缺少SEALED来源身份"
        )
    expected_identity = {
        "target_session": discovery.get("target_session"),
        "research_cutoff": discovery.get("research_cutoff"),
        "source_set_id": d1_source.get("source_set_id"),
        "package_sha256": d1_source.get("package_sha256"),
        "namespace_id": d1_source.get("namespace_id"),
    }
    if (
        not expected_identity["target_session"]
        or not expected_identity["research_cutoff"]
        or not expected_identity["source_set_id"]
        or not expected_identity["package_sha256"]
        or not expected_identity["namespace_id"]
        or d1_source.get("seal_status") != "SEALED"
    ):
        raise TdxSectorMembersApplicationError(
            "D1_SOURCE_NOT_SEALED", "D1变化发现未绑定完整SEALED来源身份"
        )

    board_rows: list[dict[str, Any]] = []
    strength_source_summaries = []
    seen_kinds: set[str] = set()
    for strength in strength_results:
        kind = strength.get("filter", {}).get("board_kind")
        if kind not in {"INDUSTRY", "CONCEPT"} or kind in seen_kinds:
            raise TdxSectorMembersApplicationError(
                "BOARD_KIND_INVALID", "强弱输入含错误或重复的TDX板块类型"
            )
        seen_kinds.add(kind)
        expected_source_kind = (
            "TDX_TDXHY_CFG" if kind == "INDUSTRY" else "TDX_INFOHARBOR_BLOCK_DAT"
        )
        snapshot = strength.get("snapshot", {})
        target_source = strength.get("target_source_set", {})
        if snapshot.get("source_kind") != expected_source_kind:
            raise TdxSectorMembersApplicationError(
                "SNAPSHOT_BOARD_KIND_MISMATCH",
                "TDX成员快照原始来源类型与板块类型不匹配",
            )
        actual_identity = {
            "target_session": strength.get("target_session"),
            "research_cutoff": strength.get("research_cutoff"),
            "source_set_id": target_source.get("source_set_id"),
            "package_sha256": target_source.get("package_sha256"),
            "namespace_id": strength.get("namespace_id"),
        }
        if actual_identity != expected_identity:
            raise TdxSectorMembersApplicationError(
                "R03_R04_SOURCE_IDENTITY_MISMATCH",
                "R03强弱与R04变化不是同目标、同SEALED包、同namespace来源口径",
            )
        strength_calendar = strength.get("official_calendar")
        d1_calendar = discovery.get("official_calendar")
        strength_revision = (
            strength_calendar.get("calendar_revision")
            if isinstance(strength_calendar, Mapping)
            else None
        )
        d1_revision = (
            d1_calendar.get("calendar_revision")
            if isinstance(d1_calendar, Mapping)
            else None
        )
        strength_revision_parts = (
            {item.partition(":")[2] or item for item in strength_revision.split("+")}
            if isinstance(strength_revision, str)
            else set()
        )
        if (
            not isinstance(strength_calendar, Mapping)
            or not isinstance(d1_calendar, Mapping)
            or strength_calendar.get("calendar_id") != d1_calendar.get("calendar_id")
            or not isinstance(d1_revision, str)
            or d1_revision not in strength_revision_parts
        ):
            raise TdxSectorMembersApplicationError(
                "R03_R04_CALENDAR_IDENTITY_MISMATCH",
                "R03强弱与R04变化没有绑定同一目标官方日历修订",
            )
        summaries = strength.get("all_boards_for_linkage")
        members_by_id = strength.get("all_member_symbols_for_linkage")
        sort_values = strength.get("strength_sort_values_for_linkage")
        if (
            not isinstance(summaries, list)
            or not isinstance(members_by_id, Mapping)
            or not isinstance(sort_values, Mapping)
            or strength.get("database_write_attempted") is not False
            or strength.get("ddl_executed") is not False
            or strength.get("dml_executed") is not False
        ):
            raise TdxSectorMembersApplicationError(
                "R03_COMPLETE_READ_REQUIRED", "R03联接输入不是完整只读板块集合"
            )
        strength_source_summaries.append(
            {
                "board_kind": kind,
                "snapshot": snapshot,
                "target_source_set": target_source,
                "target_profile_snapshot": strength.get("target_profile_snapshot"),
                "official_calendar": strength.get("official_calendar"),
                "window_sessions": strength.get("window_sessions"),
                "three_official_sessions_before": strength.get(
                    "three_official_sessions_before"
                ),
                "return_provenance": strength.get("return_provenance"),
                "board_total_count": len(summaries),
                "member_relationship_count": strength.get("member_relationship_count"),
                "distinct_source_symbol_count": strength.get(
                    "distinct_source_symbol_count"
                ),
            }
        )
        for summary in summaries:
            board_id = summary.get("board_native_id")
            board_symbols = members_by_id.get(board_id)
            if (
                summary.get("board_kind") != kind
                or not isinstance(board_id, str)
                or not isinstance(board_symbols, list)
                or len(board_symbols) != len(set(board_symbols))
                or summary.get("source_member_total") != len(board_symbols)
            ):
                raise TdxSectorMembersApplicationError(
                    "R03_BOARD_MEMBER_SET_INVALID",
                    "板块目录、成员总数或来源代码集合不守恒",
                )
            d1_state_counts = Counter(
                _member_detail(symbol, d1_symbols.get(symbol))["change_state"]
                for symbol in board_symbols
            )
            changed_members = [
                symbol
                for symbol in board_symbols
                if d1_symbols.get(symbol, {}).get("summary_state") == "CHANGED"
            ]
            changed_event_count = sum(
                sum(
                    event.get("change_classification") == "CHANGED"
                    for event in d1_symbols[symbol]["anchor_events"]
                )
                for symbol in changed_members
            )
            strength_values = sort_values.get(board_id)
            if not isinstance(strength_values, Mapping):
                raise TdxSectorMembersApplicationError(
                    "STRENGTH_SORT_VALUE_INVALID", "完整强弱集合缺少精确排序值"
                )
            exact_values = {
                field: _fraction_value(strength_values.get(field))
                for field in ("delta20", "delta5", "ret20", "ret5")
            }
            board_rows.append(
                {
                    **summary,
                    "d1_link": {
                        "member_state_counts": {
                            state: d1_state_counts.get(state, 0)
                            for state in (
                                "CHANGED",
                                "UNCHANGED",
                                "UNKNOWN",
                                "NOT_COMPARABLE",
                                "NOT_EVALUABLE",
                            )
                        },
                        "changed_member_count": len(changed_members),
                        "changed_event_count": changed_event_count,
                        "cross_board_membership_is_counted_per_board": True,
                    },
                    "_exact_strength_values": exact_values,
                    "_member_symbols": board_symbols,
                    "_board_snapshot": snapshot,
                }
            )

    all_board_rows = board_rows
    all_legal_board_count = len(all_board_rows)
    source_kind_counts = {
        kind: sum(row["board_kind"] == kind for row in all_board_rows)
        for kind in ("INDUSTRY", "CONCEPT")
    }
    if board_kind_filter is not None:
        board_rows = [
            row for row in board_rows if row["board_kind"] == board_kind_filter
        ]
    if detail_board_native_id is not None:
        detail_matches = [
            row
            for row in all_board_rows
            if row["board_kind"] == detail_board_kind
            and row["board_native_id"] == detail_board_native_id
        ]
        if len(detail_matches) != 1:
            raise TdxSectorMembersApplicationError(
                "BOARD_NOT_FOUND", "指定原生板块不在精确TDX来源快照内"
            )
        detail_board = detail_matches[0]
    else:
        detail_board = None

    descending = direction == "STRONGER"

    def sort_key(row: Mapping[str, Any]) -> tuple[Any, ...]:
        if sort_by == "changed_stocks":
            value = row["d1_link"]["changed_member_count"]
        else:
            value = row["_exact_strength_values"][sort_by]
        if value is None:
            return True, Fraction(0), row["board_kind"], row["board_native_id"]
        return (
            False,
            -value if descending else value,
            row["board_kind"],
            row["board_native_id"],
        )

    ordered = sorted(board_rows, key=sort_key)
    filtered = (
        [row for row in ordered if row["d1_link"]["changed_member_count"] > 0]
        if changed_only
        else ordered
    )
    board_page = filtered[offset : offset + limit]
    board_page_payload = [
        {key: value for key, value in row.items() if not key.startswith("_")}
        for row in board_page
    ]

    detail = None
    if detail_board is not None:
        members = []
        for symbol in detail_board["_member_symbols"]:
            member = _member_detail(symbol, d1_symbols.get(symbol))
            d1_history_base = [
                "stock-a",
                "source-d1-breakout-preview",
                "--target-session",
                expected_identity["target_session"],
                "--research-cutoff",
                expected_identity["research_cutoff"],
                "--source-set-id",
                expected_identity["source_set_id"],
                "--package-sha256",
                expected_identity["package_sha256"],
                "--symbol",
                symbol,
                "--history-start",
                str(discovery["event_window_start"]),
                "--history-end",
                expected_identity["target_session"],
            ]
            d1_history_argv = [
                d1_history_base + ["--support-anchor-session", anchor]
                for anchor in member["historical_anchor_sessions"]
            ] or [d1_history_base]
            member["drill_down"] = {
                "research_stock_argv": [
                    "stock-a",
                    "research-stock",
                    symbol,
                    "--target-session",
                    expected_identity["target_session"],
                    "--unit-version",
                    "TDX_RAW_SOURCE_VALUES_UNIT_UNVERIFIED_R1",
                    "--pg-source-history",
                    "--pg-source-sha256",
                    expected_identity["package_sha256"],
                ],
                "d1_history_argv": d1_history_argv,
            }
            if member_state == "ALL" or member["change_state"] == member_state:
                members.append(member)
        members.sort(
            key=lambda item: (
                _MEMBER_STATE_ORDER[item["change_state"]],
                item["source_symbol"],
            )
        )
        member_page = members[member_offset : member_offset + member_limit]
        for member in member_page:
            if (
                member["change_state"] == "NOT_EVALUABLE"
                and not member["unknown_reasons"]
            ):
                member["unknown_reasons"] = ["NO_TRUE_D1_EVENT_IN_WINDOW"]
        detail = {
            "board_kind": detail_board["board_kind"],
            "board_native_id": detail_board["board_native_id"],
            "board_name": detail_board["board_name"],
            "strength_summary": {
                key: value
                for key, value in detail_board.items()
                if not key.startswith("_")
            },
            "snapshot_id": detail_board["_board_snapshot"]["snapshot_id"],
            "source_kind": detail_board["_board_snapshot"]["source_kind"],
            "source_file_sha256": detail_board["_board_snapshot"]["source_file_sha256"],
            "member_count": len(detail_board["_member_symbols"]),
            "d1_link": detail_board["d1_link"],
            "member_state_filter": member_state,
            "member_page": {
                "offset": member_offset,
                "limit": member_limit,
                "count": len(member_page),
                "filtered_total": len(members),
                "next_offset": (
                    member_offset + len(member_page)
                    if member_offset + len(member_page) < len(members)
                    else None
                ),
                "members": member_page,
                "summaries_computed_before_pagination": True,
            },
        }

    result = {
        "status": "AVAILABLE"
        if board_page
        else ("NO_MATCH" if changed_only else "EMPTY"),
        "analysis_scope": "NON_PIT_RETROSPECTIVE_CURRENT_TDX_MEMBER_CORE_RETURN_WITH_D1_SUPPORT_CHANGE",
        "target_session": expected_identity["target_session"],
        "research_cutoff": expected_identity["research_cutoff"],
        "source_set_id": expected_identity["source_set_id"],
        "package_sha256": expected_identity["package_sha256"],
        "namespace_id": expected_identity["namespace_id"],
        "price_coordinate": "RAW_UNADJUSTED",
        "entity_identity": "SOURCE_SYMBOL_ONLY",
        "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "formal_sector_native_qualification": "NOT_EVALUATED_BY_THIS_ACTION",
        "formal_rps_percentile_created": False,
        "rotation_state_created": False,
        "dynamic_style_exclusion": {
            "included_board_kinds": ["INDUSTRY", "CONCEPT"],
            "industry_source_kind": "TDX_TDXHY_CFG",
            "concept_source_kind": "TDX_INFOHARBOR_BLOCK_DAT",
            "concept_included_native_category": "GN",
            "concept_excluded_native_categories": {
                "FG": "STYLE",
                "ZS": "INDEX_GROUP",
            },
            "name_based_exclusion_applied": False,
        },
        "source_filtering": {
            "pg_research_board_count_by_kind": source_kind_counts,
            "pg_research_board_total_count": all_legal_board_count,
            "pg_catalog_rows_excluded_by_cli": 0,
            "raw_header_counts_for_excluded_kinds": "NOT_PERSISTED_IN_PG_SOURCE_SNAPSHOT",
            "reason": "infoharbor原始头类别在采集解析时已按GN纳入、FG与ZS排除；本只读联动只消费INDUSTRY/CONCEPT PG目录，不从TDX原件回退读取。",
            "excluded_native_categories": {
                "FG": {"meaning": "STYLE", "research_boards_admitted": False},
                "ZS": {"meaning": "INDEX_GROUP", "research_boards_admitted": False},
            },
        },
        "source_counts": {
            "legal_board_count_by_kind": source_kind_counts,
            "legal_board_total_count": all_legal_board_count,
            "selected_board_count": len(board_rows)
            if board_kind_filter is not None
            else all_legal_board_count,
            "d1_source_pool_count": discovery.get("source_pool_count"),
            "d1_raw_event_count": discovery.get("raw_d1_event_count"),
            "d1_unique_changed_source_symbol_count": discovery.get(
                "unique_changed_symbol_count"
            ),
            "d1_support_event_counts": discovery.get("support_event_counts"),
            "d1_raw_state_partition_closed": discovery.get("raw_d1_partition_closed"),
            "d1_support_partition_closed": discovery.get(
                "support_event_partition_closed"
            ),
        },
        "filter": {
            "board_kind": board_kind_filter,
            "changed_stocks_only": changed_only,
            "sort_by": sort_by,
            "direction": direction,
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "count": len(board_page),
            "filtered_total": len(filtered),
            "next_offset": (
                offset + len(board_page)
                if offset + len(board_page) < len(filtered)
                else None
            ),
            "stable_tie_order": "board_kind ASC, board_native_id ASC COLLATE C",
            "complete_source_sets_joined_before_filter_sort_pagination": True,
        },
        "boards": board_page_payload,
        "detail_board": detail,
        "strength_sources": strength_source_summaries,
        "d1_discovery": {
            key: value
            for key, value in discovery.items()
            if key not in {"complete_symbol_details", "results", "page"}
        },
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
        "dml_rows_affected": 0,
        "source_modified": False,
        "source_downloaded": False,
    }
    if include_changed_stock_page:
        result["changed_stock_page"] = _changed_stock_page(
            discovery=discovery,
            d1_symbols=d1_symbols,
            all_board_rows=all_board_rows,
            offset=stock_offset,
            limit=stock_limit,
        )
    return result


__all__ = ["combine_sector_strength_and_d1_changes"]
