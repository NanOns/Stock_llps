"""Stock A命令行sector_members职责模块。"""

from __future__ import annotations

import argparse
import re
import uuid
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
    read_sealed_source_market_breadth_history,
)
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.application.pg_research import (
    PgResearchBindingError,
    read_sealed_source_context,
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
from stock_a.application.research_window import (
    resolve_pg_history_window as _pg_history_window,
)
from stock_a.application.tdx_sector_members import (
    TdxSectorMembersApplicationError,
    TdxSectorMembersPgError,
    TdxSectorSourceError,
    ingest_tdx_sector_members,
    refresh_remote_tdx_sector_members,
    read_tdx_sector_breadth,
    read_tdx_sector_catalog,
    read_tdx_sector_members,
    read_tdx_sector_strength,
)
from stock_a.application.tdx_sector_change_stocks import (
    combine_sector_strength_and_d1_changes,
)
from stock_a.sources.tdx.remote_sector import TdxRemoteSectorError

from stock_a.application.cli_commands.common import _emit, _parse_source_set_date


def _run_source_raw_catalog(args: argparse.Namespace) -> int:
    max_target_session = _parse_source_set_date(args.research_cutoff)
    config, config_path = _load_runtime_config()
    try:
        result = read_raw_daily_catalog(
            config,
            max_target_session=max_target_session,
            year=args.year,
            offset=args.offset,
            limit=args.limit,
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "dml_executed": False,
                "ddl_executed": False,
                "source_zip_opened": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    result.update(
        {
            "ok": True,
            "analysis_scope": "PERSISTED_TDX_SOURCE_RAW_VERSION_CATALOG",
            "database_io_attempted": True,
            "configuration_path": str(config_path),
        }
    )
    _emit(result)
    return _EXIT_OK


def _run_source_tdx_sector_members(args: argparse.Namespace) -> int:
    if args.sector_action == "change-stocks":
        return _run_source_tdx_sector_change_stocks(args)
    config, config_path = _load_runtime_config()
    try:
        if args.sector_action == "ingest":
            if args.confirm_task_scope != "R03_TDX_REAL_SECTOR_MEMBERS_TO_PG_R1":
                raise DiagnosticError(
                    "TASK_SCOPE_REQUIRED",
                    "来源成员写入必须绑定本批R03任务范围",
                    _EXIT_ARGUMENT,
                )
            result = ingest_tdx_sector_members(config)
        elif args.sector_action == "refresh-remote":
            if args.confirm_task_scope != "R03_TDX_HEADLESS_SECTOR_SOURCE_REFRESH_R1":
                raise DiagnosticError(
                    "TASK_SCOPE_REQUIRED",
                    "远端成员刷新必须绑定本批R03任务范围",
                    _EXIT_ARGUMENT,
                )
            result = refresh_remote_tdx_sector_members(
                config,
                validate_only=args.validate_only,
                validated_source_sha256=args.validated_source_sha256,
                oracle_set_sha256=args.oracle_set_sha256,
            )
        elif args.sector_action == "catalog":
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
                target_session=_parse_source_set_date(args.target_session),
                research_cutoff=_parse_source_set_date(args.research_cutoff),
                source_set_id=args.source_set_id,
                package_sha256=args.package_sha256,
                profile_revision=args.profile_revision,
                offset=args.offset,
                limit=args.limit,
            )
        elif args.sector_action == "breadth":
            result = read_tdx_sector_breadth(
                config,
                snapshot_id=args.snapshot_id,
                board_kind=args.board_kind,
                board_native_id=args.board_native_id,
                before_session=_parse_source_set_date(args.before_session),
                after_session=_parse_source_set_date(args.after_session),
                research_cutoff=_parse_source_set_date(args.research_cutoff),
                before_source_set_id=args.before_source_set_id,
                before_package_sha256=args.before_package_sha256,
                before_profile_revision=args.before_profile_revision,
                after_source_set_id=args.after_source_set_id,
                after_package_sha256=args.after_package_sha256,
                after_profile_revision=args.after_profile_revision,
                offset=args.offset,
                limit=args.limit,
                member_offset=args.member_offset,
                member_limit=args.member_limit,
            )
        elif args.sector_action == "strength":
            result = read_tdx_sector_strength(
                config,
                snapshot_id=args.snapshot_id,
                board_kind=args.board_kind,
                board_native_id=args.board_native_id,
                target_session=_parse_source_set_date(args.target_session),
                research_cutoff=_parse_source_set_date(args.research_cutoff),
                source_set_id=args.source_set_id,
                package_sha256=args.package_sha256,
                profile_revision=args.profile_revision,
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
    except (
        TdxSectorMembersApplicationError,
        TdxSectorSourceError,
        TdxRemoteSectorError,
    ) as error:
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
                **(
                    {"details": error.details}
                    if isinstance(error, TdxRemoteSectorError) and error.details
                    else {}
                ),
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    except TdxSectorMembersPgError as error:
        write_attempted = args.sector_action == "ingest" or (
            args.sector_action == "refresh-remote" and not args.validate_only
        )
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "operation": args.sector_action,
                "database_io_attempted": True,
                "database_write_attempted": write_attempted,
                "dml_executed": None if write_attempted else False,
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
    target = _parse_source_set_date(args.target_session)
    cutoff = _parse_source_set_date(args.research_cutoff)
    if target > cutoff:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": "SECTOR_CHANGE_DATE_AFTER_CUTOFF",
                "message": "目标交易会话晚于显式研究截止会话，拒绝读取PG",
                "operation": "change-stocks",
                "database_io_attempted": False,
                "d1_discovery_attempted": False,
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
                "dml_rows_affected": 0,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    if re.fullmatch(r"[0-9a-f]{64}", args.package_sha256) is None:
        raise DiagnosticError(
            "SECTOR_CHANGE_PACKAGE_SHA_INVALID",
            "完整TDX来源包SHA必须是64位小写十六进制值",
            _EXIT_ARGUMENT,
        )
    try:
        source_set_id = str(uuid.UUID(args.source_set_id))
        industry_snapshot_id = str(uuid.UUID(args.industry_snapshot_id))
        concept_snapshot_id = str(uuid.UUID(args.concept_snapshot_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise DiagnosticError(
            "SECTOR_CHANGE_SOURCE_ID_INVALID",
            "source set与快照编号必须为规范UUID",
            _EXIT_ARGUMENT,
        ) from error
    if (
        source_set_id != args.source_set_id
        or industry_snapshot_id != args.industry_snapshot_id
        or concept_snapshot_id != args.concept_snapshot_id
    ):
        raise DiagnosticError(
            "SECTOR_CHANGE_SOURCE_ID_INVALID",
            "source set与快照编号必须为规范UUID",
            _EXIT_ARGUMENT,
        )
    if not 1 <= args.pg_history_session_count <= 75:
        raise DiagnosticError(
            "SECTOR_CHANGE_HISTORY_WINDOW_INVALID",
            "D1官方历史窗口长度必须为1至75个会话",
            _EXIT_ARGUMENT,
        )
    if (args.detail_board_kind is None) != (args.detail_board_native_id is None):
        raise DiagnosticError(
            "DETAIL_BOARD_SELECTION_INVALID",
            "板块明细必须同时提供--detail-board-kind与--detail-board-native-id",
            _EXIT_ARGUMENT,
        )
    if (
        type(args.offset) is not int
        or args.offset < 0
        or type(args.limit) is not int
        or not 1 <= args.limit <= 100
        or type(args.member_offset) is not int
        or args.member_offset < 0
        or type(args.member_limit) is not int
        or not 0 <= args.member_limit <= 100
    ):
        raise DiagnosticError(
            "SECTOR_CHANGE_PAGE_INVALID",
            "板块与成员分页偏移不得为负，板块限1至100、成员限0至100",
            _EXIT_ARGUMENT,
        )
    try:
        _resolved_target, sessions, calendar_revision = _pg_history_window(
            target.isoformat(), args.pg_history_session_count
        )
    except ResearchEntryError as error:
        raise DiagnosticError(error.code, str(error), _EXIT_ARGUMENT) from error
    event_window_start = _parse_source_set_date(args.event_window_start)
    if event_window_start not in sessions or event_window_start >= target:
        raise DiagnosticError(
            "SECTOR_CHANGE_EVENT_WINDOW_INVALID",
            "D1历史事件起始须为已加载窗口内早于目标日的官方会话",
            _EXIT_ARGUMENT,
        )

    config, config_path = _load_runtime_config()
    strength_results = []
    d1_read_attempted = False
    try:
        for kind, snapshot_id in (
            ("INDUSTRY", industry_snapshot_id),
            ("CONCEPT", concept_snapshot_id),
        ):
            strength = read_tdx_sector_strength(
                config,
                snapshot_id=snapshot_id,
                board_kind=kind,
                board_native_id=None,
                target_session=target,
                research_cutoff=cutoff,
                source_set_id=source_set_id,
                package_sha256=args.package_sha256,
                profile_revision=args.profile_revision,
                changed_only=False,
                sort_by="delta20",
                direction="STRONGER",
                offset=0,
                limit=1,
                member_offset=0,
                member_limit=0,
                include_all_boards=True,
                include_all_member_symbols=True,
            )
            strength_results.append(strength)

        d1_read_attempted = True
        discovery = discover_source_d1_support_changes(
            config,
            config_path,
            target,
            cutoff,
            sessions,
            calendar_revision,
            source_set_id,
            args.package_sha256,
            event_window_start,
            0,
            1,
            include_symbol_details=True,
        )
        result = combine_sector_strength_and_d1_changes(
            strength_results,
            discovery,
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
        )
    except (
        TdxSectorMembersApplicationError,
        TdxSectorMembersPgError,
        PgResearchBindingError,
        ResearchEntryError,
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
                "d1_discovery_attempted": d1_read_attempted,
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
                "dml_rows_affected": 0,
                "source_modified": False,
                "source_downloaded": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE

    result.update(
        {
            "ok": True,
            "operation": "change-stocks",
            "configuration_path": str(config_path),
            "database_io_attempted": True,
            "database_read": {
                "database": "stock_a",
                "schema": "stock",
                "account": "stock_a",
                "server": "127.0.0.1:5432",
                "transactions": [item["database_read"] for item in strength_results],
                "d1_transaction": discovery["execution"]["database_transaction"],
                "d1_source_pages_read": discovery["execution"]["source_pages_read"],
                "d1_raw_history_batch_reads": discovery["execution"][
                    "raw_history_batch_reads"
                ],
            },
            "d1_execution": discovery["execution"],
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
        }
    )
    _emit(result)
    return _EXIT_OK
