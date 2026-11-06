"""复用PG行业/GN强弱与D1变化的只读应用用例。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
import uuid
from typing import Any, Mapping

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    query_source_d0_raw_launch,
)
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.application.tdx_sector_change_stocks import (
    combine_sector_strength_and_d1_changes,
)
from stock_a.application.tdx_sector_members import (
    TdxSectorMembersApplicationError,
    TdxSectorMembersPgError,
    read_tdx_sector_strength,
    tdx_sector_strength_read_context,
)
from stock_a.application.pg_research import PgResearchBindingError
from stock_a.application.source_market_context import (
    SourceMarketContextError,
    read_source_market_context,
    source_market_context_payload,
    source_market_not_evaluable_payload,
    validate_source_market_context_binding,
)
from stock_a.application.source_d1_price_safety import (
    PriceSafetyFactError,
    attach_source_d1_price_safety_facts,
)
from stock_a.research.entry import ResearchEntryError
from stock_a.storage.postgres.source_raw import RawSourcePgError

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class SourceSectorChangeInputs:
    """全池读入的一次精确来源组合，可在本机短时内存中复用。"""

    target_session: date
    research_cutoff: date
    package_sha256: str
    profile_revision: str | None
    industry_snapshot_id: str
    concept_snapshot_id: str
    event_window_start: date
    pg_history_session_count: int
    config_path: str
    strength_results: tuple[Mapping[str, Any], ...]
    discovery: Mapping[str, Any]
    d0_launch_changes: Mapping[str, Any] | None


def prepare_source_sector_change_inputs(
    config: dict[str, Any],
    config_path: str,
    *,
    target_session: date,
    research_cutoff: date,
    industry_snapshot_id: str,
    concept_snapshot_id: str,
    package_sha256: str,
    profile_revision: str | None,
    event_window_start: date,
    pg_history_session_count: int,
    include_d0_launch_new: bool = False,
    d0_before_session: date | None = None,
) -> SourceSectorChangeInputs:
    """从固定PG读取完整行业/GN集合与指定事件窗的D1历史。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or not isinstance(config_path, str)
        or not config_path
        or not isinstance(package_sha256, str)
        or _SHA256.fullmatch(package_sha256) is None
        or profile_revision is not None
        and (
            not isinstance(profile_revision, str)
            or _SHA256.fullmatch(profile_revision) is None
        )
        or type(pg_history_session_count) is not int
        or not 1 <= pg_history_session_count <= 75
        or type(include_d0_launch_new) is not bool
    ):
        raise TdxSectorMembersApplicationError(
            "SECTOR_CHANGE_IDENTITY_INVALID", "行业/GN联动请求身份或历史窗口无效"
        )
    try:
        industry_uuid = str(uuid.UUID(industry_snapshot_id))
        concept_uuid = str(uuid.UUID(concept_snapshot_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise TdxSectorMembersApplicationError(
            "SECTOR_CHANGE_SOURCE_ID_INVALID", "行业与GN快照编号必须为规范UUID"
        ) from error
    if (
        industry_uuid != industry_snapshot_id
        or concept_uuid != concept_snapshot_id
        or industry_uuid == concept_uuid
    ):
        raise TdxSectorMembersApplicationError(
            "SECTOR_CHANGE_SOURCE_ID_INVALID",
            "行业与GN快照编号必须规范且互不相同",
        )
    if include_d0_launch_new:
        if (
            type(d0_before_session) is not date
            or d0_before_session >= target_session
            or d0_before_session > research_cutoff
        ):
            raise TdxSectorMembersApplicationError(
                "D0_BEFORE_SESSION_INVALID",
                "D0前一官方会话须早于目标会话且不晚于研究截止",
            )
    elif d0_before_session is not None:
        raise TdxSectorMembersApplicationError(
            "D0_BEFORE_IDENTITY_REQUIRES_FLAG",
            "D0前一会话身份只能与D0联动同时提供",
        )

    try:
        _resolved_target, sessions, calendar_revision = resolve_pg_history_window(
            target_session.isoformat(), pg_history_session_count
        )
    except ResearchEntryError as error:
        raise TdxSectorMembersApplicationError(error.code, str(error)) from error
    if (
        type(event_window_start) is not date
        or event_window_start not in sessions
        or event_window_start >= target_session
    ):
        raise TdxSectorMembersApplicationError(
            "SECTOR_CHANGE_EVENT_WINDOW_INVALID",
            "D1事件窗起始必须是读取窗口内早于目标会话的官方交易日",
        )

    d0_launch_changes = None
    if include_d0_launch_new:
        d0_launch_changes = query_source_d0_raw_launch(
            config,
            scenario="LAUNCH_CONFIRM",
            action="changes",
            before_session=d0_before_session,
            before_package_sha256=package_sha256,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            transition="ALL",
            offset=0,
            limit=5000,
        )

    strength_results = []
    with tdx_sector_strength_read_context(config) as strength_read_context:
        for board_kind, snapshot_id in (
            ("INDUSTRY", industry_uuid),
            ("CONCEPT", concept_uuid),
        ):
            strength_results.append(
                read_tdx_sector_strength(
                    config,
                    snapshot_id=snapshot_id,
                    board_kind=board_kind,
                    board_native_id=None,
                    target_session=target_session,
                    research_cutoff=research_cutoff,
                    package_sha256=package_sha256,
                    profile_revision=profile_revision,
                    changed_only=False,
                    sort_by="delta20",
                    direction="STRONGER",
                    offset=0,
                    limit=1,
                    member_offset=0,
                    member_limit=0,
                    include_all_boards=True,
                    include_all_member_symbols=True,
                    read_context=strength_read_context,
                )
            )

    try:
        discovery = discover_source_d1_support_changes(
            config,
            config_path,
            target_session,
            research_cutoff,
            sessions,
            calendar_revision,
            package_sha256,
            event_window_start,
            0,
            1,
            include_symbol_details=True,
        )
    except PgResearchBindingError:
        raise
    return SourceSectorChangeInputs(
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        profile_revision=profile_revision,
        industry_snapshot_id=industry_uuid,
        concept_snapshot_id=concept_uuid,
        event_window_start=event_window_start,
        pg_history_session_count=pg_history_session_count,
        config_path=config_path,
        strength_results=tuple(strength_results),
        discovery=discovery,
        d0_launch_changes=d0_launch_changes,
    )


def build_source_sector_change_view(
    config: dict[str, Any],
    inputs: SourceSectorChangeInputs,
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
    include_changed_stock_page: bool,
    stock_offset: int,
    stock_limit: int,
    d0_new_only: bool = False,
    include_source_market_context: bool = False,
    include_price_safety_facts: bool = False,
    member_search: str | None = None,
    explain_symbol: str | None = None,
) -> dict[str, Any]:
    """从同一全量来源读入生成一个板块/成员/个股分页视图。"""
    result = combine_sector_strength_and_d1_changes(
        inputs.strength_results,
        inputs.discovery,
        board_kind_filter=board_kind_filter,
        detail_board_kind=detail_board_kind,
        detail_board_native_id=detail_board_native_id,
        changed_only=changed_only,
        sort_by=sort_by,
        direction=direction,
        offset=offset,
        limit=limit,
        member_state=member_state,
        member_offset=member_offset,
        member_limit=member_limit,
        include_changed_stock_page=include_changed_stock_page,
        stock_offset=stock_offset,
        stock_limit=stock_limit,
        d0_launch_changes=inputs.d0_launch_changes,
        d0_new_only=d0_new_only,
        member_search=member_search,
        explain_symbol=explain_symbol,
    )
    if include_source_market_context:
        try:
            market_read = read_source_market_context(
                config,
                target_session=inputs.target_session,
                research_cutoff=inputs.research_cutoff,
                package_sha256=inputs.package_sha256,
            )
        except SourceMarketContextError as error:
            if error.category != "NOT_EVALUABLE":
                raise
            d1_source = inputs.discovery["source"]
            result["source_market_context"] = source_market_not_evaluable_payload(
                source_identity={**d1_source},
                official_calendar=inputs.discovery["official_calendar"],
                target_session=inputs.target_session,
                research_cutoff=inputs.research_cutoff,
                reason_code=error.code,
                reason=str(error),
            )
        else:
            validate_source_market_context_binding(
                market_read,
                strength_results=inputs.strength_results,
                discovery=inputs.discovery,
            )
            result["source_market_context"] = source_market_context_payload(market_read)
    if include_price_safety_facts:
        attach_source_d1_price_safety_facts(
            result,
            config=config,
            discovery=inputs.discovery,
            target_session=inputs.target_session,
            research_cutoff=inputs.research_cutoff,
        )
    result.update(
        {
            "ok": True,
            "operation": "change-stocks",
            "configuration_path": inputs.config_path,
            "database_io_attempted": True,
            "database_read": {
                "database": "stock_a",
                "schema": "stock",
                "account": "stock_a",
                "server": "127.0.0.1:5432",
                "transactions": [
                    item["database_read"] for item in inputs.strength_results
                ],
                "d1_transaction": inputs.discovery["execution"]["database_transaction"],
                "d1_source_pages_read": inputs.discovery["execution"][
                    "source_pages_read"
                ],
                "d1_raw_history_batch_reads": inputs.discovery["execution"][
                    "raw_history_batch_reads"
                ],
            },
            "d1_execution": inputs.discovery["execution"],
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
        }
    )
    if inputs.d0_launch_changes is not None:
        result["d0_execution"] = inputs.d0_launch_changes["execution"]
        result["database_read"]["d0_transactions"] = inputs.d0_launch_changes[
            "execution"
        ]["transaction"]
    return result


def run_source_sector_change_research(
    config: dict[str, Any], config_path: str, **request: Any
) -> dict[str, Any]:
    """CLI使用的共享PG读取与行业/GN变化视图应用用例。"""
    filters = {
        key: request.pop(key)
        for key in (
            "board_kind_filter",
            "detail_board_kind",
            "detail_board_native_id",
            "changed_only",
            "sort_by",
            "direction",
            "offset",
            "limit",
            "member_state",
            "member_offset",
            "member_limit",
            "include_changed_stock_page",
            "stock_offset",
            "stock_limit",
            "d0_new_only",
            "include_source_market_context",
            "include_price_safety_facts",
        )
    }
    filters["member_search"] = request.pop("member_search", None)
    inputs = prepare_source_sector_change_inputs(config, config_path, **request)
    return build_source_sector_change_view(config, inputs, **filters)


__all__ = [
    "SourceSectorChangeInputs",
    "prepare_source_sector_change_inputs",
    "build_source_sector_change_view",
    "run_source_sector_change_research",
]
