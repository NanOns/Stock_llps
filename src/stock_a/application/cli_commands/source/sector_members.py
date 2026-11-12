"""Stock A命令行sector_members职责模块。"""

from __future__ import annotations

import argparse
import re
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
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.application.source_d0_raw import (
    SourceD0RawError,
    attach_continuation_observation,
    attach_recovery_failure_followup,
    attach_stock_risk_readiness,
    attach_source_d1_prior_reference,
    diagnostic_admission,
    query_source_d0_raw_launch,
)
from stock_a.application.pg_research import (
    PgResearchBindingError,
)
from stock_a.application.pg_price_profiles import PgPriceProfileError
from stock_a.contracts import PriceProfileContractError
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
    tdx_sector_strength_read_context,
)
from stock_a.application.tdx_sector_change_stocks import (
    combine_sector_strength_and_d1_changes,
)
from stock_a.application.source_sector_change_research import (
    prepare_source_sector_change_inputs,
    build_source_sector_change_view,
)
from stock_a.sources.tdx.remote_sector import TdxRemoteSectorError

from stock_a.application.cli_commands.common import _emit, _parse_session_date


def _run_source_raw_catalog(args: argparse.Namespace) -> int:
    max_target_session = _parse_session_date(args.research_cutoff)
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
                target_session=_parse_session_date(args.target_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
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
                before_session=_parse_session_date(args.before_session),
                after_session=_parse_session_date(args.after_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                before_source_package_id=args.before_source_package_id,
                before_profile_revision=args.before_profile_revision,
                after_source_package_id=args.after_source_package_id,
                after_profile_revision=args.after_profile_revision,
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

                profile_contract = (
                    load_price_profile_contract(_CONFIG_ROOT).document
                    if args.profile_revision
                    else None
                )

                result = read_retrospective_rotation(
                    config,
                    snapshot_id=args.snapshot_id,
                    board_kind=args.board_kind,
                    board_native_id=args.board_native_id,
                    start_session=_parse_session_date(args.rotation_start),
                    target_session=_parse_session_date(args.target_session),
                    research_cutoff=_parse_session_date(args.research_cutoff),
                    source_package_id=args.source_package_id,
                    profile_revision=args.profile_revision,
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
                    source_package_id=args.source_package_id,
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
    except (TdxSectorMembersPgError, RawSourcePgError) as error:
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
    target = _parse_session_date(args.target_session)
    cutoff = _parse_session_date(args.research_cutoff)
    include_d0_launch_new = getattr(args, "include_d0_launch_new", False)
    include_seed = getattr(args, "include_source_seed_board", False)
    if args.loo_source_symbol and not include_seed:
        raise DiagnosticError(
            "LOO_REQUIRES_SOURCE_SEED_BOARD",
            "单股LOO须启用来源Seed板块",
            _EXIT_ARGUMENT,
        )
    if include_seed and (
        not args.profile_revision
        or not args.detail_board_kind
        or not args.detail_board_native_id
    ):
        raise DiagnosticError(
            "SOURCE_SEED_BOARD_IDENTITY_REQUIRED",
            "Seed汇总须显式选择画像修订和一个板块",
            _EXIT_ARGUMENT,
        )
    d0_new_only = getattr(args, "d0_new_only", False)
    if not include_d0_launch_new and (args.sort_by == "d0_launch_new" or d0_new_only):
        raise DiagnosticError(
            "D0_NEW_OPTIONS_REQUIRE_FLAG",
            "D0新增启动排序或筛选必须与--include-d0-launch-new一起使用",
            _EXIT_ARGUMENT,
        )
    d0_before_session = None
    if include_d0_launch_new:
        if not args.d0_before_session:
            raise DiagnosticError(
                "D0_BEFORE_IDENTITY_REQUIRED",
                "启用D0新增启动联动时必须提供--d0-before-session",
                _EXIT_ARGUMENT,
            )
        d0_before_session = _parse_session_date(args.d0_before_session)
        if d0_before_session >= target or d0_before_session > cutoff:
            raise DiagnosticError(
                "D0_BEFORE_SESSION_INVALID",
                "D0前一官方会话必须早于目标会话且不晚于显式研究截止",
                _EXIT_ARGUMENT,
            )
    elif getattr(args, "d0_before_session", None) is not None:
        raise DiagnosticError(
            "D0_BEFORE_IDENTITY_REQUIRES_FLAG",
            "D0前一会话身份参数只能与--include-d0-launch-new一起使用",
            _EXIT_ARGUMENT,
        )
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
    if type(args.source_package_id) is not int or args.source_package_id <= 0:
        raise DiagnosticError(
            "SECTOR_CHANGE_PACKAGE_ID_INVALID",
            "必须提供正数TDX来源包BIGINT数字ID",
            _EXIT_ARGUMENT,
        )
    industry_snapshot_id = args.industry_snapshot_id
    concept_snapshot_id = args.concept_snapshot_id
    if (
        type(industry_snapshot_id) is not int
        or industry_snapshot_id <= 0
        or type(concept_snapshot_id) is not int
        or concept_snapshot_id <= 0
    ):
        raise DiagnosticError(
            "SECTOR_CHANGE_SOURCE_ID_INVALID",
            "快照编号必须是正数BIGINT数字ID",
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
        or type(args.stock_offset) is not int
        or args.stock_offset < 0
        or type(args.stock_limit) is not int
        or not 1 <= args.stock_limit <= 30
    ):
        raise DiagnosticError(
            "SECTOR_CHANGE_PAGE_INVALID",
            "板块与成员分页偏移不得为负，板块限1至100、成员限0至100、变化个股限1至30",
            _EXIT_ARGUMENT,
        )
    try:
        _resolved_target, sessions, calendar_revision = _pg_history_window(
            target.isoformat(), args.pg_history_session_count
        )
    except ResearchEntryError as error:
        raise DiagnosticError(error.code, str(error), _EXIT_ARGUMENT) from error
    event_window_start = _parse_session_date(args.event_window_start)
    if event_window_start not in sessions or event_window_start >= target:
        raise DiagnosticError(
            "SECTOR_CHANGE_EVENT_WINDOW_INVALID",
            "D1历史事件起始须为已加载窗口内早于目标日的官方会话",
            _EXIT_ARGUMENT,
        )

    config, config_path = _load_runtime_config()
    d1_read_attempted = True
    d0_read_attempted = include_d0_launch_new
    try:
        inputs = prepare_source_sector_change_inputs(
            config,
            str(config_path),
            target_session=target,
            research_cutoff=cutoff,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
            source_package_id=args.source_package_id,
            profile_revision=args.profile_revision,
            event_window_start=event_window_start,
            pg_history_session_count=args.pg_history_session_count,
            include_d0_launch_new=include_d0_launch_new,
            d0_before_session=d0_before_session,
            cross_period_board_kind=args.detail_board_kind if include_seed else None,
        )
        result = build_source_sector_change_view(
            config,
            inputs,
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
            d0_new_only=d0_new_only,
            include_source_market_context=getattr(
                args, "include_source_market_context", False
            ),
            include_price_safety_facts=getattr(
                args, "include_price_safety_facts", False
            ),
        )
        if getattr(args, "d0_structure_symbol", None):
            d0 = query_source_d0_raw_launch(
                config,
                scenario="RECOVERY_TURN",
                target_session=target,
                research_cutoff=cutoff,
                source_package_id=args.source_package_id,
                symbol=args.d0_structure_symbol,
                limit=1,
                _include_shape_observation=True,
            )
            if len(d0["results"]) != 1:
                raise SourceD0RawError(
                    "D0_D1_SYMBOL_NOT_FOUND", "目标来源池不含指定股票。"
                )
            attach_source_d1_prior_reference(d0["results"][0], inputs.discovery)
            if args.d0_followup_session:
                attach_recovery_failure_followup(
                    config,
                    d0["results"][0],
                    target=target,
                    followup=_parse_session_date(args.d0_followup_session),
                    cutoff=cutoff,
                    source_package_id=args.source_package_id,
                )
            if args.d0_risk_readiness:
                attach_stock_risk_readiness(
                    config,
                    d0,
                    target=target,
                    cutoff=cutoff,
                    source_package_id=args.source_package_id,
                )
                launch = query_source_d0_raw_launch(
                    config,
                    scenario="LAUNCH_CONFIRM",
                    target_session=target,
                    research_cutoff=cutoff,
                    source_package_id=args.source_package_id,
                    symbol=args.d0_structure_symbol,
                    limit=1,
                )
                if len(launch["results"]) != 1:
                    raise SourceD0RawError(
                        "D0_D1_SYMBOL_NOT_FOUND", "目标来源池不含指定股票。"
                    )
                launch["results"][0]["d1_prior_structure_reference"] = d0["results"][0][
                    "d1_prior_structure_reference"
                ]
                launch["results"][0]["diagnostic_admission"] = diagnostic_admission(
                    launch, target=target, source_package_id=args.source_package_id
                )
                if args.d0_followup_session:
                    attach_recovery_failure_followup(
                        config,
                        launch["results"][0],
                        target=target,
                        followup=_parse_session_date(args.d0_followup_session),
                        cutoff=cutoff,
                        source_package_id=args.source_package_id,
                    )
                result["d0_launch_diagnostic"] = launch
            result["d0_d1_structure_diagnostic"] = d0
        elif args.d0_followup_session or args.d0_risk_readiness:
            raise SourceD0RawError(
                "D0_STRUCTURE_SYMBOL_REQUIRED",
                "风险或后继观察要求指定D0结构来源代码。",
                category="ARGUMENT",
            )
        if include_seed:
            from stock_a.application.runtime_config import _CONFIG_ROOT
            from stock_a.application.source_pool_seed import (
                read_board_seed_facts,
                read_source_pool_seed,
            )
            from stock_a.contracts import load_price_profile_contract

            pool = read_source_pool_seed(
                config,
                target_session=target,
                research_cutoff=cutoff,
                source_package_id=args.source_package_id,
                profile_revision=args.profile_revision,
                profile_contract=load_price_profile_contract(_CONFIG_ROOT).document,
                history_session_count=args.pg_history_session_count,
            )
            snapshot_id = (
                industry_snapshot_id
                if args.detail_board_kind == "INDUSTRY"
                else concept_snapshot_id
            )
            strength_input = next(
                item
                for item in inputs.strength_results
                if item["filter"]["board_kind"] == args.detail_board_kind
            )
            result["source_seed_board"] = read_board_seed_facts(
                config,
                pool,
                snapshot_id=snapshot_id,
                board_kind=args.detail_board_kind,
                board_native_id=args.detail_board_native_id,
                peer_members=strength_input["all_member_symbols_for_linkage"],
                cross_period_inputs=strength_input["cross_period_inputs_for_linkage"],
                cross_period_sessions={
                    "target": strength_input["target_session"],
                    "three_official_sessions_before": strength_input[
                        "three_official_sessions_before"
                    ],
                    "previous_official_session": strength_input["window_sessions"][-2],
                },
                loo_symbol=args.loo_source_symbol,
            )
            result["source_seed_pool"] = {
                key: value for key, value in pool.items() if key != "facts"
            }
            if (
                getattr(args, "d0_structure_symbol", None)
                and args.loo_source_symbol == args.d0_structure_symbol
            ):
                loo = result["source_seed_board"].get("loo", {})
                if loo.get("state") == "APPLIED":
                    qualification = loo.get("after", {}).get("raw_qualification", {})
                    confirmed = qualification.get("confirmed_raw", {})
                    state = confirmed.get("state", "UNKNOWN")
                    result["d0_d1_structure_diagnostic"]["results"][0][
                        "other_scenario_partial_predicates"
                    ]["TREND_CONTINUE"]["loo_confirmed_raw"] = {
                        "state": "TRUE" if state == "TRUE" else "UNKNOWN",
                        "quality": "OBSERVED" if state == "TRUE" else "UNKNOWN",
                        "reason": None
                        if state == "TRUE"
                        else "OTHER_MEMBER_BOARDS_NOT_EVALUATED",
                        "selected_board_state": state,
                        "selected_board": args.detail_board_native_id,
                        "board_kind": args.detail_board_kind,
                        "selected_board_qualification": confirmed,
                        "membership": "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT",
                    }
                    attach_continuation_observation(
                        result["d0_d1_structure_diagnostic"]["results"][0]
                    )
    except (
        TdxSectorMembersApplicationError,
        TdxSectorMembersPgError,
        PgResearchBindingError,
        ResearchEntryError,
        SourceD0RawError,
        RawSourcePgError,
        SourceMarketContextError,
        PriceSafetyFactError,
        PgPriceProfileError,
        PriceProfileContractError,
    ) as error:
        code = getattr(error, "code", "SECTOR_CHANGE_DISCOVERY_FAILED")
        error_payload = {
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
        }
        if d0_read_attempted:
            error_payload["d0_discovery_attempted"] = True
        _emit(error_payload, error=True)
        return _EXIT_UNAVAILABLE

    _emit(result)
    return _EXIT_OK
