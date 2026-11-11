"""复用PG行业/GN强弱与D1变化的只读应用用例。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from fractions import Fraction
import re
import uuid
from typing import Any, Mapping

from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.pg_price_profiles import PgPriceProfileError
from stock_a.application.source_pool_factors import read_source_pool_rps
from stock_a.application.source_pool_seed import (
    _cross_period_boards,
    read_source_pool_seed,
)
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
    _raw_window_observation,
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
from stock_a.research.core import (
    MarketSessionCalendar,
    calculate_core,
    calculate_core_return,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_raw_daily_history_page,
)
from stock_a.storage.postgres.tdx_sector_members import read_rotation_snapshot_members

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


def validate_source_d1_discovery_fact(
    discovery: Mapping[str, Any],
    *,
    source_version_context: Mapping[str, Any],
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    sessions: tuple[date, ...],
    calendar_revision: str,
    event_window_start: date,
) -> None:
    """验证可在画像或成员快照切换时复用的完整Raw D1事实。"""
    if not isinstance(source_version_context, Mapping):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_SOURCE_CONTEXT_INVALID",
            "D1事实复用必须提供已读取的Raw来源上下文",
        )
    source = discovery.get("source") if isinstance(discovery, Mapping) else None
    event_sessions = tuple(
        day for day in sessions if event_window_start <= day < target_session
    )
    row_count = source_version_context.get("raw_row_count")
    event_counts = (
        discovery.get("raw_d1_counts_by_session")
        if isinstance(discovery, Mapping)
        else None
    )
    execution = discovery.get("execution") if isinstance(discovery, Mapping) else None
    expected_event_days = {day.isoformat() for day in event_sessions}
    if (
        not isinstance(discovery, Mapping)
        or not isinstance(source, Mapping)
        or not isinstance(event_counts, Mapping)
        or not isinstance(execution, Mapping)
        or discovery.get("scope")
        != "TARGET_SESSION_RAW_SOURCE_POOL_EVENT_WINDOW_DISCOVERY"
        or discovery.get("identity_status") != "SOURCE_SYMBOL_ONLY"
        or discovery.get("price_coordinate") != "RAW_UNADJUSTED"
        or discovery.get("target_session") != target_session.isoformat()
        or discovery.get("research_cutoff") != research_cutoff.isoformat()
        or discovery.get("event_window_start") != event_window_start.isoformat()
        or discovery.get("calendar_revision") != calendar_revision
        or discovery.get("evaluated_source_symbol") is not None
        or discovery.get("source_pool_count") != row_count
        or discovery.get("evaluated_source_symbol_count") != row_count
        or discovery.get("source_pool_partition_closed") is not True
        or discovery.get("raw_d1_partition_closed") is not True
        or discovery.get("support_event_partition_closed") is not True
        or discovery.get("changed_symbol_partition_closed") is not True
        or set(event_counts) != expected_event_days
        or any(
            not isinstance(event_counts[day], Mapping) for day in expected_event_days
        )
        or any(
            type(event_counts[day].get(state)) is not int
            or event_counts[day][state] < 0
            for day in expected_event_days
            for state in ("TRUE", "FALSE", "UNKNOWN")
        )
        or any(
            sum(
                event_counts[day].get(state, -1)
                for state in ("TRUE", "FALSE", "UNKNOWN")
            )
            != row_count
            or event_counts[day].get("partition_closed") is not True
            for day in expected_event_days
        )
        or source.get("status") != "AVAILABLE"
        or source.get("version_status") != "COMPLETE"
        or source.get("namespace_id") != source_version_context.get("namespace_id")
        or source.get("target_session") != target_session.isoformat()
        or source.get("package_sha256") != package_sha256
        or source.get("source_content_id")
        != source_version_context.get("source_content_id")
        or str(source.get("source_capture_id"))
        != str(source_version_context.get("source_capture_id"))
        or source.get("source_raw_digest")
        != source_version_context.get("source_raw_digest")
        or source.get("capture_digest") != source_version_context.get("capture_digest")
        or source.get("raw_row_count") != row_count
        or source.get("capture_expected_row_count") != row_count
        or execution.get("database_write_attempted") is not False
        or execution.get("ddl_executed") is not False
        or execution.get("dml_executed") is not False
    ):
        raise PgResearchBindingError(
            "D1_SUPPORT_DISCOVERY_REUSE_IDENTITY_MISMATCH",
            "待复用D1事实未与当前Raw捕获、窗口或完整分母严格闭合",
        )


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
    cross_period_board_kind: str | None = None,
    source_d1_discovery: Mapping[str, Any] | None = None,
    source_version_context: Mapping[str, Any] | None = None,
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
                    include_cross_period_inputs=board_kind == cross_period_board_kind,
                    read_context=strength_read_context,
                )
            )

    if source_d1_discovery is None:
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
            source_version_context=source_version_context,
        )
    else:
        discovery = source_d1_discovery
    if source_version_context is not None:
        validate_source_d1_discovery_fact(
            discovery,
            source_version_context=source_version_context,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            sessions=sessions,
            calendar_revision=calendar_revision,
            event_window_start=event_window_start,
        )
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
    board_search: str | None = None,
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
        board_search=board_search,
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


def read_retrospective_rotation(
    config: Mapping[str, Any],
    *,
    snapshot_id: str,
    board_kind: str,
    board_native_id: str,
    start_session: date,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
    profile_revision: str | None = None,
    profile_contract: dict[str, Any] | None = None,
    profile_history_session_count: int = 75,
    peer_target_only: bool = False,
    peer_snapshot_ids: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """固定后验同类成员，逐会话计算pulse并追踪每个触发日的等权篮子。"""
    if (
        type(start_session) is not date
        or type(target_session) is not date
        or type(research_cutoff) is not date
        or not start_session <= target_session <= research_cutoff
    ):
        raise TdxSectorMembersApplicationError(
            "ROTATION_REQUEST_INVALID", "Rotation窗口必须绑定不晚于研究截止的官方会话"
        )
    if type(peer_target_only) is not bool or (
        peer_target_only and start_session != target_session
    ):
        raise TdxSectorMembersApplicationError(
            "ROTATION_PEER_TARGET_INVALID", "完整同类板块单日读取要求起止为同一会话"
        )
    if profile_revision is not None and (
        not isinstance(profile_contract, dict)
        or type(profile_history_session_count) is not int
        or not 1 <= profile_history_session_count <= 75
    ):
        raise TdxSectorMembersApplicationError(
            "ROTATION_PROFILE_INVALID", "Rotation来源模式须绑定画像合同和有界会话数"
        )
    calendar, _, revision, all_sessions = _load_calendar(target_session)
    try:
        start_index, end_index = (
            all_sessions.index(start_session),
            all_sessions.index(target_session),
        )
    except ValueError as error:
        raise TdxSectorMembersApplicationError(
            "ROTATION_SESSION_INVALID", "Rotation窗口端点不是官方会话"
        ) from error
    lookback = 23 if peer_target_only else 8
    if start_index < lookback or not 1 <= end_index - start_index + 1 <= 15:
        raise TdxSectorMembersApplicationError(
            "ROTATION_WINDOW_INVALID",
            "Rotation候选窗须为1至15个会话，且至少有8个前态会话",
        )
    sessions = tuple(all_sessions[start_index - lookback : end_index + 1])
    if peer_snapshot_ids is not None and (
        not peer_target_only
        or set(peer_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or peer_snapshot_ids.get(board_kind) != snapshot_id
    ):
        raise TdxSectorMembersApplicationError(
            "ROTATION_PEER_SNAPSHOTS_INVALID",
            "批量同行读取须同时绑定行业和GN快照，并与主快照一致",
        )
    snapshots_by_kind = (
        {
            kind: read_rotation_snapshot_members(
                config,
                snapshot_id=peer_snapshot_ids[kind],
                board_kind=kind,
            )
            for kind in ("INDUSTRY", "CONCEPT")
        }
        if peer_snapshot_ids is not None
        else {
            board_kind: read_rotation_snapshot_members(
                config, snapshot_id=snapshot_id, board_kind=board_kind
            )
        }
    )
    namespace_ids = {item["namespace_id"] for item in snapshots_by_kind.values()}
    if len(namespace_ids) != 1:
        raise TdxSectorMembersPgError(
            "ROTATION_PEER_NAMESPACE_MISMATCH", "同类板块快照不属于同一来源namespace"
        )
    snapshot = snapshots_by_kind[board_kind]
    boards = snapshot["boards"]
    if board_native_id not in boards or not boards[board_native_id]["members"]:
        raise TdxSectorMembersPgError(
            "ROTATION_BOARD_NOT_FOUND", "指定板块在快照中不存在或没有成员"
        )
    peer_members_by_kind = {
        kind: {key: row["members"] for key, row in item["boards"].items()}
        for kind, item in snapshots_by_kind.items()
    }
    board_names_by_kind = {
        kind: {key: row["name"] for key, row in item["boards"].items()}
        for kind, item in snapshots_by_kind.items()
    }
    wanted = {
        symbol
        for members_by_board in peer_members_by_kind.values()
        for members in members_by_board.values()
        for symbol in members
    }
    observations = {}
    raw_by_symbol = {}
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
            page["namespace_id"] != snapshot["namespace_id"]
            or page["version_status"] != "CLOSED"
            or page["total_count"] != page["version_expected_row_count"]
        ):
            raise TdxSectorMembersPgError(
                "ROTATION_SOURCE_IDENTITY_MISMATCH",
                "Rotation成员与目标Raw来源身份或分母不一致",
            )
        for candidate in page["candidates"]:
            symbol = candidate["source_symbol"]
            if symbol not in wanted:
                continue
            rows = candidate["history_rows"]
            raw = {row["session_date"]: row for row in rows}
            if (
                len(raw) != len(rows)
                or symbol in observations
                or target_session not in raw
            ):
                raise TdxSectorMembersPgError(
                    "ROTATION_HISTORY_INVALID", "Rotation来源历史重复或目标行缺失"
                )
            raw_by_symbol[symbol] = raw
            by_key = {(symbol, day): row for day, row in raw.items()}
            counts = {
                key: {"row_count": 1, "package_count": 1, "capture_count": 1}
                for key in by_key
            }
            capture = raw[target_session]["source_capture_id"]
            observations[symbol] = tuple(
                _raw_window_observation(
                    source_symbol=symbol,
                    session=day,
                    raw_by_key=by_key,
                    revision_counts=counts,
                    target_session=target_session,
                    target_capture_id=capture,
                )[0]
                for day in sessions
            )
        if page["next_offset"] is None:
            if offset + len(page["candidates"]) != page["total_count"]:
                raise TdxSectorMembersPgError(
                    "ROTATION_SOURCE_PARTITION_MISMATCH", "Rotation来源池分页没有闭合"
                )
            break
        if page["next_offset"] != offset + len(page["candidates"]):
            raise TdxSectorMembersPgError(
                "ROTATION_PAGE_INVALID", "Rotation来源池分页游标不连续"
            )
        offset = page["next_offset"]

    def quote(symbol, i):
        history = observations.get(symbol)
        if history is None or i < 0:
            return None
        bar = history[i]
        fields = (bar.open, bar.high, bar.low, bar.close)
        if bar.state != "ACTUAL" or any(
            x is None or x.quality != "OBSERVED" for x in fields
        ):
            return None
        opening, high, low, close = (Fraction(x.value) for x in fields)
        if (
            min(opening, high, low, close) <= 0
            or low > min(opening, close)
            or high < max(opening, close)
        ):
            return None
        return close

    def matching_entries(symbol, indexes):
        raw = raw_by_symbol.get(symbol, {})
        entries = [raw.get(sessions[i], {}).get("source_entry_sha256") for i in indexes]
        return all(entries) and len(set(entries)) == 1

    endpoints = {}
    endpoint_indices = (
        (len(sessions) - 4, len(sessions) - 2, len(sessions) - 1)
        if peer_target_only
        else range(5, len(sessions))
    )
    for i in endpoint_indices:
        per_symbol = {}
        for symbol in wanted:
            current = quote(symbol, i)
            previous = quote(symbol, i - 1)
            ret1 = (
                current / previous - 1
                if current is not None
                and previous is not None
                and matching_entries(symbol, (i - 1, i))
                else None
            )
            ret5 = None
            history = observations.get(symbol)
            if (
                history is not None
                and current is not None
                and matching_entries(symbol, range(i - 5, i + 1))
            ):
                window = sessions[i - 5 : i + 1]
                result = calculate_core_return(
                    history[i - 5 : i + 1],
                    periods=5,
                    official_calendar=MarketSessionCalendar(
                        str(calendar["calendar_id"]), revision, window
                    ),
                )
                if result.quality == "OBSERVED":
                    ret5 = result.value
            ret20 = None
            above_ma20 = None
            entry_versions = (
                {
                    row.get("source_entry_sha256")
                    for row in raw_by_symbol.get(symbol, {}).values()
                    if row.get("source_entry_sha256")
                }
                if peer_target_only
                else set()
            )
            if (
                peer_target_only
                and history is not None
                and current is not None
                and len(entry_versions) <= 1
                and matching_entries(symbol, range(i - 20, i + 1))
            ):
                history_window = history[i - 20 : i + 1]
                history_calendar = MarketSessionCalendar(
                    str(calendar["calendar_id"]), revision, sessions[i - 20 : i + 1]
                )
                long_return = calculate_core_return(
                    history_window, periods=20, official_calendar=history_calendar
                )
                if long_return.quality == "OBSERVED":
                    ret20 = long_return.value
                ma20 = calculate_core(
                    history_window, official_calendar=history_calendar
                )["core.ma20"]
                if ma20.quality == "OBSERVED":
                    above_ma20 = current > Fraction(ma20.value)
            per_symbol[symbol] = {
                "actual_quote": current is not None,
                "ret1": ret1,
                "ret5": ret5,
                "ret20": ret20,
                "above_ma20": above_ma20,
            }
        endpoints[i] = per_symbol

    members = boards[board_native_id]["members"]
    candidates = []
    peer_pulses = []
    rps_by_session = {}
    seed_by_session = {}

    def session_rps(index):
        if index not in rps_by_session:
            try:
                rps_by_session[index] = (
                    read_source_pool_rps(
                        config,
                        target_session=sessions[index],
                        research_cutoff=research_cutoff,
                        package_sha256=package_sha256,
                        expected_namespace_id=snapshot["namespace_id"],
                    )["facts"],
                    None,
                )
            except ValueError as error:
                rps_by_session[index] = ({}, f"RPS_INPUT_VALUE_ERROR:{error}")
        return rps_by_session[index]

    def rps_value(facts, symbol, field):
        value = facts.get(symbol, {}).get(field)
        return (
            Fraction(value.value)
            if value is not None and value.quality == "OBSERVED"
            else None
        )

    def session_seed(index):
        if profile_revision is None:
            return {}, "PROFILE_REVISION_NOT_REQUESTED", None
        if index not in seed_by_session:
            try:
                pool = read_source_pool_seed(
                    config,
                    target_session=sessions[index],
                    research_cutoff=research_cutoff,
                    package_sha256=package_sha256,
                    namespace_id=snapshot["namespace_id"],
                    profile_revision=profile_revision,
                    profile_contract=profile_contract,
                    history_session_count=profile_history_session_count,
                    selected_symbols=frozenset(members),
                )
                seed_by_session[index] = (
                    pool["facts"],
                    None,
                    pool["profile_revision_matches_runtime"],
                )
            except (ValueError, PgPriceProfileError) as error:
                seed_by_session[index] = (
                    {},
                    f"SEED_INPUT_{type(error).__name__}:{error}",
                    None,
                )
        return seed_by_session[index]

    candidate_indices = (
        (len(sessions) - 1,) if peer_target_only else range(8, len(sessions))
    )
    for i in candidate_indices:
        inputs = {
            symbol: {
                "mapped": symbol in observations,
                "target": endpoints[i][symbol],
                "three_official_sessions_before": endpoints[i - 3][symbol],
                "previous_official_session": endpoints[i - 1][symbol],
            }
            for symbol in wanted
        }
        all_rotations_by_kind = {
            kind: _cross_period_boards(members_by_board, inputs)
            for kind, members_by_board in peer_members_by_kind.items()
        }
        if peer_target_only:
            all_rotations = all_rotations_by_kind[board_kind]
            return {
                "namespace_id": snapshot["namespace_id"],
                "snapshot": snapshot["snapshot"],
                "snapshots_by_kind": {
                    kind: item["snapshot"] for kind, item in snapshots_by_kind.items()
                },
                "board_names_by_kind": board_names_by_kind,
                "target_session": target_session.isoformat(),
                "comparison_sessions": {
                    "prior_ret5": sessions[-4],
                    "prior_breadth": sessions[-2],
                },
                "board_kind": board_kind,
                "package_sha256": package_sha256,
                "membership_mode": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                "peer_target": all_rotations,
                "peer_target_by_kind": all_rotations_by_kind,
                "database_write_attempted": False,
            }
        all_rotations = all_rotations_by_kind[board_kind]
        rotation = all_rotations[board_native_id]["rotation"]
        peer_pulses.append(
            {
                "session": sessions[i].isoformat(),
                "board_native_ids": [
                    key
                    for key, row in all_rotations.items()
                    if row["rotation"]["pulse"]["state"] == "TRUE"
                ],
            }
        )
        candidate = {
            "session": sessions[i].isoformat(),
            **rotation,
            "left_boundary": "FIRST_OBSERVED_LEFT_CENSORED"
            if i == 8
            else "WITHIN_REQUEST_WINDOW",
        }
        if rotation["pulse"]["state"] == "TRUE":
            baseline_rps, baseline_rps_error = session_rps(i)
            baseline_seed, baseline_seed_error, _ = session_seed(i)
            frozen = [
                {
                    "source_symbol": symbol,
                    "base": quote(symbol, i - 1),
                    "t0": quote(symbol, i),
                }
                for symbol in members
            ]
            observations_by_day = []
            for j in range(i, len(sessions)):
                formation, post = [], []
                for member in frozen:
                    symbol, base, t0 = (
                        member["source_symbol"],
                        member["base"],
                        member["t0"],
                    )
                    close = quote(symbol, j)
                    formation.append(
                        close / base - 1
                        if close is not None
                        and base is not None
                        and matching_entries(symbol, (i - 1, i, j))
                        else None
                    )
                    post.append(
                        close / t0 - 1
                        if close is not None
                        and t0 is not None
                        and matching_entries(symbol, (i, j))
                        else None
                    )
                n = len(members)
                ret5_values = [endpoints[j][s]["ret5"] for s in members]
                current_rps, rps_error = session_rps(j)
                current_seed, seed_error, profile_matches_runtime = session_seed(j)
                rps5 = [rps_value(current_rps, s, "rps5") for s in members]
                rps20 = [rps_value(current_rps, s, "rps20") for s in members]
                initial_rps5 = [rps_value(baseline_rps, s, "rps5") for s in members]
                retained = [
                    (a, b)
                    for a, b in zip(initial_rps5, rps5)
                    if a is not None and b is not None
                ]
                patterns = {}
                for pattern in ("s1", "s2"):
                    now = [
                        current_seed.get(s, {}).get(pattern, {}).get("state", "UNKNOWN")
                        for s in members
                    ]
                    initial = [
                        baseline_seed.get(s, {})
                        .get(pattern, {})
                        .get("state", "UNKNOWN")
                        for s in members
                    ]
                    paired = [
                        (a, b)
                        for a, b in zip(initial, now)
                        if a in {"TRUE", "FALSE"} and b in {"TRUE", "FALSE"}
                    ]
                    patterns[pattern] = {
                        "true": now.count("TRUE"),
                        "false": now.count("FALSE"),
                        "unknown": now.count("UNKNOWN"),
                        "t0_true_paired": sum(a == "TRUE" for a, b in paired),
                        "t0_true_still_true": sum(
                            a == "TRUE" and b == "TRUE" for a, b in paired
                        ),
                        "pair_unknown": n - len(paired),
                    }
                observations_by_day.append(
                    {
                        "session": sessions[j].isoformat(),
                        "member_count": n,
                        "formation_R": str(sum(formation, Fraction()) / n)
                        if all(v is not None for v in formation)
                        else None,
                        "formation_unknown_count": sum(v is None for v in formation),
                        "formation_quality": "OBSERVED"
                        if all(v is not None for v in formation)
                        else "UNKNOWN",
                        "formation_reason": None
                        if all(v is not None for v in formation)
                        else "MEMBER_BASE_PRICE_OR_COORDINATE_UNAVAILABLE",
                        "post_R": str(sum(post, Fraction()) / n)
                        if all(v is not None for v in post)
                        else None,
                        "post_unknown_count": sum(v is None for v in post),
                        "post_quality": "OBSERVED"
                        if all(v is not None for v in post)
                        else "UNKNOWN",
                        "post_reason": None
                        if all(v is not None for v in post)
                        else "MEMBER_T0_PRICE_OR_COORDINATE_UNAVAILABLE",
                        "post_positive_count": sum(
                            v is not None and v > 0 for v in post
                        ),
                        "post_nonpositive_count": sum(
                            v is not None and v <= 0 for v in post
                        ),
                        "source_ret5_positive_count": sum(
                            v is not None and v > 0 for v in ret5_values
                        ),
                        "source_ret5_nonpositive_count": sum(
                            v is not None and v <= 0 for v in ret5_values
                        ),
                        "source_ret5_unknown_count": sum(
                            v is None for v in ret5_values
                        ),
                        "source_ret5_range": str(max(ret5_values) - min(ret5_values))
                        if all(v is not None for v in ret5_values)
                        else None,
                        "source_s1_s2": {
                            "scope": "NON_PIT_SOURCE_PATTERN_ONLY",
                            "profile_revision": profile_revision,
                            "profile_matches_runtime": profile_matches_runtime,
                            "profile_note": "已存画像用于来源身份；S1/S2按当前Core/Profile合同即时计算，并非读取已存模式值。",
                            "input_error": seed_error,
                            "t0_input_error": baseline_seed_error,
                            "patterns": patterns,
                            "members": [
                                {
                                    "source_symbol": symbol,
                                    **{
                                        key: current_seed.get(symbol, {}).get(key)
                                        for key in (
                                            "s1",
                                            "s2",
                                            "predicates",
                                            "core",
                                            "profile",
                                            "rps",
                                        )
                                    },
                                }
                                for symbol in members
                            ],
                            "formal_seed_retention": "NOT_EVALUATED",
                        },
                        "source_rps": {
                            "scope": "PER_SESSION_COMPLETE_SOURCE_POOL_NON_PIT",
                            "input_error": rps_error,
                            "t0_input_error": baseline_rps_error,
                            "rps5_strong_ge_70": sum(
                                v is not None and v >= 70 for v in rps5
                            ),
                            "rps5_weak_below_50": sum(
                                v is not None and v < 50 for v in rps5
                            ),
                            "rps5_unknown": sum(v is None for v in rps5),
                            "rps20_strong_ge_70": sum(
                                v is not None and v >= 70 for v in rps20
                            ),
                            "rps20_unknown": sum(v is None for v in rps20),
                            "rps5_minus_rps20_mean_pp": str(
                                sum((a - b for a, b in zip(rps5, rps20)), Fraction())
                                / n
                            )
                            if all(
                                a is not None and b is not None
                                for a, b in zip(rps5, rps20)
                            )
                            else None,
                            "t0_strong_rps5_known": sum(a >= 70 for a, b in retained),
                            "t0_strong_rps5_still_strong": sum(
                                a >= 70 and b >= 70 for a, b in retained
                            ),
                            "retention_pair_unknown": n - len(retained),
                            "formal_strong_retention": "NOT_EVALUATED",
                        },
                    }
                )
            candidate["basket"] = {
                "membership_mode": "RETROSPECTIVE_FIXED_CURRENT_MEMBERS",
                "weight": "EQUAL_FIXED_DENOMINATOR",
                "price_coordinate": "RAW_UNADJUSTED",
                "t0_minus_1": sessions[i - 1].isoformat(),
                "base_observation": {
                    "session": sessions[i - 1].isoformat(),
                    "formation_R": "0"
                    if all(row["base"] is not None for row in frozen)
                    else None,
                    "formation_unknown_count": sum(
                        row["base"] is None for row in frozen
                    ),
                    "post_R": "NOT_EVALUATED_BEFORE_T0",
                },
                "members": [
                    {
                        "source_symbol": row["source_symbol"],
                        "base_close": str(row["base"])
                        if row["base"] is not None
                        else None,
                        "t0_close": str(row["t0"]) if row["t0"] is not None else None,
                        "followup_closes": [
                            {
                                "session": sessions[j].isoformat(),
                                "close": str(quote(row["source_symbol"], j))
                                if quote(row["source_symbol"], j) is not None
                                else None,
                                "same_entry_as_t0": matching_entries(
                                    row["source_symbol"], (i, j)
                                ),
                            }
                            for j in range(i, len(sessions))
                        ],
                    }
                    for row in frozen
                ],
                "observations": observations_by_day,
                "formal_seed_retention": "NOT_EVALUATED",
                "source_s1_s2_retention": "PER_OBSERVATION_SOURCE_PATTERN_ONLY",
                "formal_rotation_state": "NOT_EVALUATED",
            }
        candidates.append(candidate)
    return {
        "contract": "SOURCE_ROTATION_RETROSPECTIVE_FIXED_BASKET_R1",
        "snapshot": snapshot["snapshot"],
        "namespace_id": snapshot["namespace_id"],
        "board_kind": board_kind,
        "board_native_id": board_native_id,
        "package_sha256": package_sha256,
        "research_cutoff": research_cutoff.isoformat(),
        "later_session_status": "NOT_AVAILABLE_AFTER_RESEARCH_CUTOFF"
        if target_session == research_cutoff
        else "NOT_REQUESTED_AFTER_TARGET",
        "official_calendar_id": str(calendar["calendar_id"]),
        "official_calendar_revision": revision,
        "official_sessions": [day.isoformat() for day in sessions],
        "membership_mode": "RETROSPECTIVE_FIXED_CURRENT_MEMBERS",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "candidate_sessions": candidates,
        "peer_pulses": peer_pulses,
        "source_pool_membership": "TARGET_SOURCE_POOL_NOT_HISTORICAL_LISTING_UNIVERSE",
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
    }


__all__ = [
    "read_retrospective_rotation",
    "SourceSectorChangeInputs",
    "prepare_source_sector_change_inputs",
    "build_source_sector_change_view",
    "run_source_sector_change_research",
]
