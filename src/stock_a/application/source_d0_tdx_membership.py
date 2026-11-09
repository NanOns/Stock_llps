"""把完整D0来源变化集合批量联接到精确TDX行业/GN快照。"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
import math
import re
from typing import Any, Mapping, Sequence

from stock_a.application.tdx_sector_change_stocks import _d0_launch_new_linkage
from stock_a.application.tdx_sector_members import TdxSectorMembersApplicationError
from stock_a.storage.postgres.tdx_sector_members import (
    TdxSectorMembersPgError,
    query_tdx_sector_memberships_for_symbols,
)


_SOURCE_SYMBOL = re.compile(r"(?:SH|SZ)\.[0-9]{6}\Z")
_SNAPSHOT_ROLES = {
    "INDUSTRY": ("TDX_TDXHY_CFG", "INDUSTRY"),
    "CONCEPT": ("TDX_INFOHARBOR_BLOCK_DAT", "CONCEPT"),
}


def read_tdx_sector_memberships_for_symbols(
    config: dict[str, Any],
    *,
    source_symbols: Sequence[str],
    target_session: date,
    industry_snapshot_id: str | None,
    concept_snapshot_id: str | None,
    expected_namespace_id: str,
) -> dict[str, Any]:
    """一次性读取完整FT来源代码在可用行业/GN快照中的成员关系。"""
    try:
        return query_tdx_sector_memberships_for_symbols(
            config,
            source_symbols=source_symbols,
            target_session=target_session,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
            expected_namespace_id=expected_namespace_id,
        )
    except TdxSectorMembersPgError as error:
        raise TdxSectorMembersApplicationError(error.code, str(error)) from error


def build_d0_launch_watch_membership_result(
    changes: Mapping[str, Any], membership_read: Mapping[str, Any]
) -> dict[str, Any]:
    """校验完整D0九态集合后联接成员关系并计算独立股票/板块分母。"""
    if not isinstance(changes, Mapping) or not isinstance(membership_read, Mapping):
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_INPUT_INVALID", "D0变化和板块成员结果必须是对象"
        )
    before = changes.get("before")
    after = changes.get("after")
    if not isinstance(before, Mapping) or not isinstance(after, Mapping):
        raise TdxSectorMembersApplicationError(
            "D0_CHANGE_RESULT_INVALID", "D0结果缺少前后来源端点"
        )
    target_session = after.get("target_session")
    if membership_read.get("target_session") != target_session:
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_TARGET_MISMATCH", "TDX关系目标日与D0后端目标日不一致"
        )
    if membership_read.get("namespace_id") != after.get("namespace_id"):
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_NAMESPACE_MISMATCH", "TDX成员namespace与D0来源不一致"
        )

    before_version = before.get("source_version")
    after_version = after.get("source_version")
    snapshots = membership_read.get("snapshots")
    memberships = membership_read.get("memberships")
    if not isinstance(before_version, Mapping) or not isinstance(
        after_version, Mapping
    ):
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_SOURCE_IDENTITY_INVALID",
            "TDX联接需要同namespace、同包的完整D0 Raw端点及后验快照",
        )
    if (
        not isinstance(snapshots, list)
        or not isinstance(memberships, list)
        or membership_read.get("membership_mode")
        != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        or before_version.get("package_sha256") != after_version.get("package_sha256")
        or before.get("namespace_id") != after.get("namespace_id")
    ):
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_SOURCE_IDENTITY_INVALID",
            "TDX联接需要同namespace、同包的完整D0 Raw端点及后验快照",
        )

    snapshot_by_id: dict[str, Mapping[str, Any]] = {}
    expected_snapshot_ids: dict[str, str] = {}
    for snapshot in snapshots:
        if not isinstance(snapshot, Mapping):
            raise TdxSectorMembersApplicationError(
                "SECTOR_SNAPSHOT_INVALID", "TDX快照元数据记录无效"
            )
        board_kind = snapshot.get("board_kind")
        expected_source_kind, expected_board_kind = _SNAPSHOT_ROLES.get(
            board_kind, (None, None)
        )
        snapshot_id = snapshot.get("snapshot_id")
        if (
            not isinstance(snapshot_id, str)
            or snapshot_id in snapshot_by_id
            or board_kind not in _SNAPSHOT_ROLES
            or snapshot.get("source_kind") != expected_source_kind
            or snapshot.get("board_kind") != expected_board_kind
            or snapshot.get("membership_mode")
            != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
        ):
            raise TdxSectorMembersApplicationError(
                "SECTOR_SNAPSHOT_ROLE_INVALID", "行业/GN快照身份、来源或后验语义无效"
            )
        snapshot_by_id[snapshot_id] = snapshot
        expected_snapshot_ids[board_kind] = snapshot_id
    missing_kinds = set(_SNAPSHOT_ROLES) - set(expected_snapshot_ids)

    change_rows = changes.get("results")
    if not isinstance(change_rows, list):
        raise TdxSectorMembersApplicationError(
            "D0_CHANGE_RESULT_INVALID", "D0结果缺少完整变化行"
        )
    ft_symbols: set[str] = set()
    for row in change_rows:
        if not isinstance(row, Mapping) or row.get("transition") != "FALSE_TO_TRUE":
            continue
        source_symbol = row.get("source_symbol")
        if (
            not isinstance(source_symbol, str)
            or _SOURCE_SYMBOL.fullmatch(source_symbol) is None
            or row.get("market") != source_symbol[:2]
            or source_symbol in ft_symbols
        ):
            raise TdxSectorMembersApplicationError(
                "D0_SOURCE_KEY_DUPLICATED", "D0新增启动集合存在无效或重复来源代码"
            )
        ft_symbols.add(source_symbol)
    requested_symbols = membership_read.get("source_symbols_requested")
    execution = membership_read.get("execution")
    matched_snapshot_ids = {
        member.get("snapshot_id")
        for member in memberships
        if isinstance(member, Mapping)
    }
    empty_snapshot_rows = sum(
        snapshot.get("snapshot_id") not in matched_snapshot_ids
        for snapshot in snapshots
    )
    expected_select_rows = len(memberships) + empty_snapshot_rows
    if (
        not isinstance(requested_symbols, list)
        or requested_symbols != sorted(ft_symbols)
        or membership_read.get("source_symbol_request_count") != len(ft_symbols)
        or not isinstance(execution, Mapping)
        or execution.get("select_statement_count") != 1
        or execution.get("transaction") != "REPEATABLE READ READ ONLY"
        or execution.get("select_rows_returned") != expected_select_rows
        or execution.get("fetched_relation_rows") != len(memberships)
        or execution.get("membership_relation_rows_returned") != len(memberships)
        or execution.get("source_symbol_parameter_count") != len(ft_symbols)
        or execution.get("database_write_attempted") is not False
        or execution.get("ddl_executed") is not False
        or execution.get("dml_executed") is not False
        or execution.get("dml_rows_affected") != 0
        or type(execution.get("pg_membership_read_seconds")) not in {int, float}
        or not math.isfinite(execution["pg_membership_read_seconds"])
        or execution["pg_membership_read_seconds"] < 0
        or type(execution.get("transaction_wall_seconds")) not in {int, float}
        or not math.isfinite(execution["transaction_wall_seconds"])
        or execution["transaction_wall_seconds"] < 0
    ):
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_QUERY_SCOPE_MISMATCH",
            "TDX批量读取范围、完整关系行数或只读执行证据未闭合",
        )

    members_by_board: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    board_metadata: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    seen_relations: set[tuple[str, str, str, str]] = set()
    for member in memberships:
        if not isinstance(member, Mapping):
            raise TdxSectorMembersApplicationError(
                "SECTOR_MEMBERSHIP_INVALID", "TDX成员关系记录无效"
            )
        snapshot_id = member.get("snapshot_id")
        snapshot = snapshot_by_id.get(snapshot_id)
        source_symbol = member.get("source_symbol")
        board_kind = member.get("board_kind")
        board_native_id = member.get("board_native_id")
        if (
            snapshot is None
            or source_symbol not in ft_symbols
            or board_kind != snapshot.get("board_kind")
            or member.get("source_kind") != snapshot.get("source_kind")
            or member.get("membership_quality") != "OBSERVED_SOURCE_RELATION"
            or member.get("membership_mode")
            != "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"
            or not isinstance(board_native_id, str)
            or not board_native_id
            or not isinstance(member.get("board_name"), str)
            or not member["board_name"]
            or member.get("native_category")
            != ("GN" if board_kind == "CONCEPT" else "INDUSTRY")
            or member.get("observed_at") != snapshot.get("observed_at")
            or member.get("source_file_sha256") != snapshot.get("source_file_sha256")
            or member.get("source_valid_on") != snapshot.get("source_valid_on")
            or member.get("source_known_at") != snapshot.get("source_known_at")
            or member.get("source_available_at") != snapshot.get("source_available_at")
            or member.get("clock_quality") != snapshot.get("clock_quality")
            or member.get("source_quality") != snapshot.get("source_quality")
            or member.get("research_mode") != snapshot.get("research_mode")
            or member.get("temporal_relationship")
            != snapshot.get("temporal_relationship")
        ):
            raise TdxSectorMembersApplicationError(
                "SECTOR_MEMBERSHIP_ROLE_INVALID",
                "TDX关系不属于完整FT集合或快照/时间/质量身份不匹配",
            )
        relation = (snapshot_id, board_kind, board_native_id, source_symbol)
        if relation in seen_relations:
            raise TdxSectorMembersApplicationError(
                "SECTOR_MEMBERSHIP_DUPLICATED", "TDX板块与D0股票关系重复"
            )
        seen_relations.add(relation)
        key = (board_kind, snapshot_id, board_native_id)
        members_by_board[key].add(source_symbol)
        board_metadata[key] = member

    all_board_rows: list[dict[str, Any]] = []
    for (board_kind, snapshot_id, board_native_id), symbols in members_by_board.items():
        snapshot = snapshot_by_id[snapshot_id]
        all_board_rows.append(
            {
                "board_kind": board_kind,
                "board_native_id": board_native_id,
                "board_name": board_metadata[
                    (board_kind, snapshot_id, board_native_id)
                ]["board_name"],
                "_member_symbols": sorted(symbols),
                "_board_snapshot": {
                    "snapshot_id": snapshot_id,
                    "source_kind": snapshot["source_kind"],
                    "source_file_sha256": snapshot["source_file_sha256"],
                    "source_observed_at": snapshot["observed_at"],
                    "source_valid_on": snapshot["source_valid_on"],
                    "source_known_at": snapshot["source_known_at"],
                    "source_available_at": snapshot["source_available_at"],
                    "source_clock_quality": snapshot["clock_quality"],
                    "source_quality": snapshot["source_quality"],
                },
            }
        )
    all_board_rows.sort(
        key=lambda row: (
            row["board_kind"],
            row["board_native_id"],
        )
    )

    # 此共享联接器已经验证完整D0九态、端点质量及来源池分母闭合。
    summary, _stock_index, _members_by_board = _d0_launch_new_linkage(
        changes=changes,
        all_board_rows=all_board_rows,
    )
    stocks = summary.pop("stocks")
    if missing_kinds:
        for stock in stocks:
            stock["missing_membership_kinds"] = sorted(missing_kinds)
            if not stock.get("also_in_sectors"):
                stock["unmapped_reason"] = "MEMBERSHIP_SNAPSHOT_UNAVAILABLE"
    summary.pop("members_by_board")
    board_summaries = []
    for row in all_board_rows:
        snapshot = row["_board_snapshot"]
        board_summaries.append(
            {
                "board_kind": row["board_kind"],
                "native_category": "GN"
                if row["board_kind"] == "CONCEPT"
                else "INDUSTRY",
                "board_native_id": row["board_native_id"],
                "board_name": row["board_name"],
                "snapshot_id": snapshot["snapshot_id"],
                "source_kind": snapshot["source_kind"],
                "source_file_sha256": snapshot["source_file_sha256"],
                "observed_at": snapshot["source_observed_at"],
                "source_valid_on": snapshot["source_valid_on"],
                "source_known_at": snapshot["source_known_at"],
                "source_available_at": snapshot["source_available_at"],
                "clock_quality": snapshot["source_clock_quality"],
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "new_member_count": len(row["_member_symbols"]),
            }
        )
    board_summaries.sort(
        key=lambda row: (
            -row["new_member_count"],
            row["board_kind"],
            row["board_native_id"],
        )
    )

    mapped_count = sum(bool(row["also_in_sectors"]) for row in stocks)
    unmapped_count = len(stocks) - mapped_count
    relation_count = sum(len(row["also_in_sectors"]) for row in stocks)
    if (
        len(stocks) != len(ft_symbols)
        or mapped_count + unmapped_count != len(ft_symbols)
        or relation_count != len(seen_relations)
        or relation_count != membership_read.get("membership_relation_count")
        or membership_read.get("matched_source_symbol_count") != mapped_count
    ):
        raise TdxSectorMembersApplicationError(
            "D0_MEMBERSHIP_PARTITION_NOT_CLOSED",
            "全局FT股票、无映射股票与板块关系分母未闭合",
        )
    summary.update(
        {
            "target_session": target_session,
            "research_cutoff": after.get("research_cutoff"),
            "namespace_id": after.get("namespace_id"),
            "package_sha256": after_version.get("package_sha256"),
            "global_new_stock_count": len(ft_symbols),
            "global_unique_source_symbol_count": len(ft_symbols),
            "mapped_distinct_source_symbol_count": mapped_count,
            "unmapped_distinct_source_symbol_count": None
            if missing_kinds
            else unmapped_count,
            "missing_membership_kinds": sorted(missing_kinds),
            "board_membership_relation_count": relation_count,
            "membership_snapshots": list(snapshots),
            "membership_read_execution": membership_read.get("execution"),
            "boards": board_summaries,
            "stocks": stocks,
            "stock_stable_order": "source_symbol ASC COLLATE C",
            "board_stable_order": (
                "new_member_count DESC, board_kind ASC, board_native_id ASC COLLATE C"
            ),
        }
    )
    return summary


__all__ = [
    "build_d0_launch_watch_membership_result",
    "read_tdx_sector_memberships_for_symbols",
]
