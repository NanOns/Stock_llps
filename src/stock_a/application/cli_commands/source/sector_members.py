"""Stock A命令行sector_members职责模块。"""

from __future__ import annotations

import argparse
from stock_a.storage.postgres.source_raw import RawSourcePgError
from stock_a.application.source_market_context import (
    SourceMarketContextError,
)
from stock_a.application.source_d1_price_safety import (
    PriceSafetyFactError,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
)
from stock_a.application.pg_research import (
    PgResearchBindingError,
)
from stock_a.research.entry import (
    ResearchEntryError,
)
from stock_a.application.runtime_config import (
    DiagnosticError,
    _CONFIG_ROOT,
    _EXIT_ARGUMENT,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    load_runtime_config as _load_runtime_config,
)
from stock_a.application.tdx_sector_members import (
    TdxSectorMembersApplicationError,
    TdxSectorMembersPgError,
    read_tdx_sector_breadth,
    read_tdx_sector_catalog,
    read_tdx_sector_members,
    read_tdx_sector_strength,
)

from stock_a.application.cli_commands.common import _emit, _parse_session_date


def _run_source_tdx_sector_members(args: argparse.Namespace) -> int:
    if args.sector_action == "change-stocks":
        return _run_source_tdx_sector_change_stocks(args)
    config, config_path = _load_runtime_config()
    try:
        if args.sector_action == "catalog":
            result = read_tdx_sector_catalog(
                config,
                snapshot_id=args.snapshot_id,
                board_kind=args.board_kind,
                search=args.search,
                offset=args.offset,
                limit=args.limit,
            )
        elif args.sector_action == "members":
            result = read_tdx_sector_members(
                config,
                snapshot_id=args.snapshot_id,
                board_kind=args.board_kind,
                board_native_id=args.board_native_id,
                target_session=_parse_session_date(args.target_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                source_capture_id=args.source_capture_id,
                profile_revision=None,
                offset=args.offset,
                limit=args.limit,
            )
        elif args.sector_action == "breadth":
            result = read_tdx_sector_breadth(
                config,
                snapshot_id=args.snapshot_id,
                board_kind=args.board_kind,
                board_native_id=args.board_native_id,
                before_session=_parse_session_date(args.before_session),
                after_session=_parse_session_date(args.after_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                before_source_capture_id=args.before_source_capture_id,
                before_profile_revision=None,
                after_source_capture_id=args.after_source_capture_id,
                after_profile_revision=None,
                offset=args.offset,
                limit=args.limit,
                member_offset=args.member_offset,
                member_limit=args.member_limit,
                include_core_transitions=args.include_core_transitions,
                core_transition_kind=args.core_transition_kind,
            )
        elif args.sector_action == "strength":
            if getattr(args, "rotation_start", None):
                if args.board_native_id is None:
                    raise DiagnosticError(
                        "ROTATION_BOARD_REQUIRED",
                        "Rotation研究必须指定精确板块编号",
                        _EXIT_ARGUMENT,
                    )
                if (
                    args.changed_only
                    or args.sort_by != "delta20"
                    or args.direction != "STRONGER"
                    or args.offset != 0
                    or args.limit != 30
                    or args.member_offset != 0
                    or args.member_limit != 30
                ):
                    raise DiagnosticError(
                        "ROTATION_FILTER_INVALID",
                        "Rotation研究不接受强弱榜单的筛选、排序或分页参数",
                        _EXIT_ARGUMENT,
                    )
                from stock_a.application.source_sector_change_research import (
                    read_retrospective_rotation,
                )
                from stock_a.contracts import load_price_profile_contract

                profile_contract = load_price_profile_contract(_CONFIG_ROOT).document

                result = read_retrospective_rotation(
                    config,
                    snapshot_id=args.snapshot_id,
                    board_kind=args.board_kind,
                    board_native_id=args.board_native_id,
                    start_session=_parse_session_date(args.rotation_start),
                    target_session=_parse_session_date(args.target_session),
                    research_cutoff=_parse_session_date(args.research_cutoff),
                    source_capture_id=args.source_capture_id,
                    profile_contract=profile_contract,
                )
            else:
                result = read_tdx_sector_strength(
                    config,
                    snapshot_id=args.snapshot_id,
                    board_kind=args.board_kind,
                    board_native_id=args.board_native_id,
                    target_session=_parse_session_date(args.target_session),
                    research_cutoff=_parse_session_date(args.research_cutoff),
                    source_capture_id=args.source_capture_id,
                    profile_revision=None,
                    changed_only=args.changed_only,
                    sort_by=args.sort_by,
                    direction=args.direction,
                    offset=args.offset,
                    limit=args.limit,
                    member_offset=args.member_offset,
                    member_limit=args.member_limit,
                )
        else:
            raise DiagnosticError(
                "SECTOR_ACTION_INVALID", "未登记的行业概念成员操作", _EXIT_ARGUMENT
            )
    except DiagnosticError:
        raise
    except (TdxSectorMembersApplicationError,) as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "operation": args.sector_action,
                "database_io_attempted": False,
                "database_write_attempted": False,
                "dml_executed": False,
                "ddl_executed": False,
                "source_modified": False,
                "source_downloaded": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    except (TdxSectorMembersPgError, RawSourcePgError) as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "operation": args.sector_action,
                "database_io_attempted": True,
                "database_write_attempted": False,
                "dml_executed": False,
                "ddl_executed": False,
                "source_modified": False,
                "source_downloaded": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": result.get("status")
            not in {"NO_MATCH", "REJECTED", "BLOCKED_REVISION_KEY"},
            "operation": args.sector_action,
            "configuration_path": str(config_path),
            "database_io_attempted": True,
            "ddl_executed": False,
            "source_zip_opened": result.get("zhb_zip") is not None,
        }
    )
    _emit(result)
    return _EXIT_OK if result["ok"] else _EXIT_UNAVAILABLE


def _run_source_tdx_sector_change_stocks(args: argparse.Namespace) -> int:
    target = _parse_session_date(args.target_session)
    cutoff = _parse_session_date(args.research_cutoff)
    if target > cutoff:
        raise DiagnosticError(
            "SECTOR_CHANGE_DATE_AFTER_CUTOFF",
            "目标交易会话晚于显式研究截止会话，拒绝读取PG",
            _EXIT_ARGUMENT,
        )
    if args.pg_history_session_count is not None and args.pg_history_session_count <= 0:
        raise DiagnosticError(
            "SECTOR_CHANGE_HISTORY_WINDOW_INVALID",
            "D1官方历史窗口长度必须是正整数",
            _EXIT_ARGUMENT,
        )
    if args.include_source_seed_board and (
        not args.detail_board_kind or not args.detail_board_native_id
    ):
        raise DiagnosticError(
            "SOURCE_SEED_BOARD_IDENTITY_REQUIRED",
            "跨期强度输入须显式选择一个板块",
            _EXIT_ARGUMENT,
        )
    if args.include_d0_launch_new and not args.d0_before_session:
        raise DiagnosticError(
            "D0_BEFORE_IDENTITY_REQUIRED",
            "启用D0新增启动联动时必须提供--d0-before-session",
            _EXIT_ARGUMENT,
        )
    d0_before_session = (
        _parse_session_date(args.d0_before_session) if args.d0_before_session else None
    )
    config, config_path = _load_runtime_config()
    try:
        from stock_a.application.source_sector_change_research import (
            run_source_sector_change_research,
        )

        result = run_source_sector_change_research(
            config,
            str(config_path),
            target_session=target,
            research_cutoff=cutoff,
            industry_snapshot_id=args.industry_snapshot_id,
            concept_snapshot_id=args.concept_snapshot_id,
            source_capture_id=args.source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
            event_window_start=_parse_session_date(args.event_window_start),
            pg_history_session_count=args.pg_history_session_count,
            include_d0_launch_new=args.include_d0_launch_new,
            d0_before_session=d0_before_session,
            cross_period_board_kind=(
                args.detail_board_kind if args.include_source_seed_board else None
            ),
            board_kind_filter=args.board_kind,
            detail_board_kind=args.detail_board_kind,
            detail_board_native_id=args.detail_board_native_id,
            changed_only=args.changed_only,
            sort_by=args.sort_by,
            direction=args.direction,
            offset=args.offset,
            limit=args.limit,
            member_state=args.member_state,
            member_offset=args.member_offset,
            member_limit=args.member_limit,
            include_changed_stock_page=args.include_changed_stock_page,
            stock_offset=args.stock_offset,
            stock_limit=args.stock_limit,
            d0_new_only=args.d0_new_only,
            include_source_market_context=args.include_source_market_context,
            include_price_safety_facts=args.include_price_safety_facts,
        )
    except (
        TdxSectorMembersApplicationError,
        TdxSectorMembersPgError,
        PgResearchBindingError,
        ResearchEntryError,
        SourceD0RawError,
        SourceMarketContextError,
        PriceSafetyFactError,
    ) as error:
        code = getattr(error, "code", "SECTOR_CHANGE_DISCOVERY_FAILED")
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": code,
                "message": str(error),
                "operation": "change-stocks",
                "database_io_attempted": True,
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if code.endswith("INVALID") else _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": True,
            "operation": "change-stocks",
            "analysis_scope": "BAOSTOCK_QFQ_TDX_MEMBER_CHANGE_STOCKS",
            "source_capture_id": args.source_capture_id,
            "price_coordinate": "FORWARD_ADJUSTED",
            "time_role": "NON_PIT_RETROSPECTIVE",
            "configuration_path": str(config_path),
        }
    )
    _emit(result)
    return _EXIT_OK
