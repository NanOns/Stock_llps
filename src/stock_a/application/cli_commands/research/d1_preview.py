"""Stock A命令行d1_preview职责模块。"""

from __future__ import annotations

import argparse
import re
from stock_a.contracts import (
    PriceProfileContractError,
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
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.application.source_d1_structures import (
    discover_source_d1_structures,
    evaluate_source_d1_structure_timeline,
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
from stock_a.application.research_window import (
    resolve_pg_history_window as _pg_history_window,
)

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.cli_commands.research.d1_discovery import (
    _discover_source_d1_support_cohort,
    _discover_source_d1_true,
)


def _run_source_d1_breakout_preview(args: argparse.Namespace) -> int:
    target = _parse_session_date(args.target_session)
    cutoff = _parse_session_date(args.research_cutoff)
    if target > cutoff:
        raise DiagnosticError(
            "D1_BREAKOUT_DATE_AFTER_CUTOFF",
            "目标交易会话晚于研究截止会话，拒绝读取PG",
            _EXIT_ARGUMENT,
        )
    if type(args.source_package_id) is not int or args.source_package_id <= 0:
        raise DiagnosticError(
            "D1_BREAKOUT_PACKAGE_ID_INVALID",
            "必须提供正数TDX来源包BIGINT数字ID",
            _EXIT_ARGUMENT,
        )
    history_mode = any(
        (args.history_start, args.history_end, args.history_last is not None)
    )
    discover_support_changes = args.discover_support_changes
    discover_structures = args.discover_structures
    structure_changes = args.structure_changes
    material_changes_only = args.material_changes_only
    structure_type = args.structure_type
    structure_change_status = args.structure_change_status
    include_structures = args.include_structures
    if args.structure_symbol is not None and not (
        discover_structures or structure_changes
    ):
        raise DiagnosticError(
            "D1_STRUCTURE_SYMBOL_SELECTION_INVALID",
            "--structure-symbol仅可与--discover-structures配合使用",
            _EXIT_ARGUMENT,
        )
    if structure_changes and not discover_structures:
        raise DiagnosticError(
            "D1_STRUCTURE_CHANGE_SELECTION_INVALID",
            "--structure-changes须与--discover-structures配合使用",
            _EXIT_ARGUMENT,
        )
    if material_changes_only and not structure_changes:
        raise DiagnosticError(
            "D1_MATERIAL_CHANGE_FILTER_INVALID",
            "--material-changes-only须与--structure-changes配合使用",
            _EXIT_ARGUMENT,
        )
    if (structure_type is not None or structure_change_status is not None) and not (
        structure_changes
    ):
        raise DiagnosticError(
            "D1_STRUCTURE_CHANGE_FILTER_INVALID",
            "结构类型和变化状态过滤须用于--structure-changes",
            _EXIT_ARGUMENT,
        )
    if discover_structures and (
        args.only_true
        or args.symbol is not None
        or history_mode
        or args.support_anchor_session is not None
        or args.support_cohort_anchor_session is not None
        or discover_support_changes
        or args.change_only
        or args.previous_state is not None
        or args.event_window_start is not None
        or args.offset is None
        or args.limit is None
        or include_structures
    ):
        raise DiagnosticError(
            "D1_STRUCTURE_DISCOVERY_SELECTION_INVALID",
            "全池结构发现须选择目标日分页，不能与单股历史或其他D1模式混用",
            _EXIT_ARGUMENT,
        )
    if include_structures and (
        not history_mode
        or args.symbol is None
        or args.support_anchor_session is not None
        or args.support_cohort_anchor_session is not None
        or discover_support_changes
        or discover_structures
        or args.only_true
        or args.change_only
        or args.previous_state is not None
    ):
        raise DiagnosticError(
            "D1_STRUCTURE_HISTORY_SELECTION_INVALID",
            "--include-structures须用于单股历史模式，且不能叠加其他锚观察模式",
            _EXIT_ARGUMENT,
        )
    if args.event_window_start is not None and not discover_support_changes:
        raise DiagnosticError(
            "D1_SUPPORT_DISCOVERY_SELECTION_INVALID",
            "--event-window-start仅可与--discover-support-changes配合使用",
            _EXIT_ARGUMENT,
        )
    if discover_support_changes and (
        args.only_true
        or args.symbol is not None
        or history_mode
        or args.support_anchor_session is not None
        or args.support_cohort_anchor_session is not None
        or args.change_only
        or args.previous_state is not None
        or args.event_window_start is None
        or include_structures
        or discover_structures
    ):
        raise DiagnosticError(
            "D1_SUPPORT_DISCOVERY_SELECTION_INVALID",
            "多历史变化发现须提供起始会话和全池分页，不能与其他模式筛选混用",
            _EXIT_ARGUMENT,
        )
    support_anchor = None
    support_cohort_anchor = None
    if args.support_anchor_session is not None:
        if args.only_true or args.symbol is None or not history_mode:
            raise DiagnosticError(
                "D1_BREAKOUT_SUPPORT_SELECTION_INVALID",
                "--support-anchor-session必须与--symbol及单股历史范围同时使用",
                _EXIT_ARGUMENT,
            )
        support_anchor = _parse_session_date(args.support_anchor_session)
    if args.support_cohort_anchor_session is not None:
        if (
            args.only_true
            or args.symbol is not None
            or history_mode
            or args.support_anchor_session is not None
            or args.previous_state is not None
        ):
            raise DiagnosticError(
                "D1_BREAKOUT_SUPPORT_COHORT_SELECTION_INVALID",
                "来源批次支撑模式不可与--only-true、单代码、单股历史或其他状态筛选混用",
                _EXIT_ARGUMENT,
            )
        support_cohort_anchor = _parse_session_date(args.support_cohort_anchor_session)
    if args.change_only and support_cohort_anchor is None:
        raise DiagnosticError(
            "D1_BREAKOUT_CHANGE_ONLY_SELECTION_INVALID",
            "--change-only仅可与--support-cohort-anchor-session配合使用",
            _EXIT_ARGUMENT,
        )
    if args.previous_state is not None and not args.only_true:
        raise DiagnosticError(
            "D1_BREAKOUT_PREVIOUS_STATE_SELECTION_INVALID",
            "--previous-state仅可与--only-true配合使用",
            _EXIT_ARGUMENT,
        )
    if args.previous_state is not None and args.pg_history_session_count < 2:
        raise DiagnosticError(
            "D1_BREAKOUT_PREVIOUS_WINDOW_TOO_SHORT",
            "--previous-state至少需要包含目标会话及其紧邻前一官方会话的窗口",
            _EXIT_ARGUMENT,
        )
    if args.only_true and (args.symbol is not None or history_mode):
        raise DiagnosticError(
            "D1_BREAKOUT_SELECTION_INVALID",
            "--only-true不可与单代码或历史模式同时使用",
            _EXIT_ARGUMENT,
        )
    if history_mode and (
        args.symbol is None
        or (args.history_last is not None and (args.history_start or args.history_end))
        or (args.history_last is None and not (args.history_start and args.history_end))
        or (args.history_last is not None and not 1 <= args.history_last <= 75)
    ):
        raise DiagnosticError(
            "D1_BREAKOUT_HISTORY_SELECTION_INVALID",
            "历史模式须指定单代码及起止会话，或最近1至75个会话",
            _EXIT_ARGUMENT,
        )
    if args.symbol is not None:
        if args.limit is not None:
            raise DiagnosticError(
                "D1_BREAKOUT_SELECTION_INVALID",
                "单来源代码模式不能同时提供分页limit",
                _EXIT_ARGUMENT,
            )
        offset, limit = 0, 1
        source_symbol = args.symbol
    else:
        if args.offset is None or args.limit is None:
            raise DiagnosticError(
                "D1_BREAKOUT_SELECTION_REQUIRED",
                "请提供--symbol，或同时提供分页--offset与--limit",
                _EXIT_ARGUMENT,
            )
        offset, limit = args.offset, args.limit
        source_symbol = None
    if (
        type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise DiagnosticError(
            "D1_BREAKOUT_PAGE_INVALID",
            "分页offset须为非负整数且limit须为1至100",
            _EXIT_ARGUMENT,
        )
    if discover_support_changes and limit > 30:
        raise DiagnosticError(
            "D1_SUPPORT_DISCOVERY_PAGE_INVALID",
            "多历史变化发现每页最多返回30个去重来源代码",
            _EXIT_ARGUMENT,
        )

    try:
        _resolved_target, sessions, calendar_revision = _pg_history_window(
            target.isoformat(), args.pg_history_session_count
        )
    except ResearchEntryError as error:
        if not args.only_true:
            raise
        raise DiagnosticError(error.code, str(error), _EXIT_ARGUMENT) from error
    history_sessions = ()
    event_window_start = None
    if discover_support_changes:
        if not isinstance(args.event_window_start, str):
            raise DiagnosticError(
                "D1_SUPPORT_DISCOVERY_WINDOW_INVALID",
                "多历史变化发现必须提供事件窗口起始会话",
                _EXIT_ARGUMENT,
            )
        event_window_start = _parse_session_date(args.event_window_start)
        if event_window_start not in sessions or event_window_start >= target:
            raise DiagnosticError(
                "D1_SUPPORT_DISCOVERY_WINDOW_INVALID",
                "事件窗口起始须是读取窗口内早于目标日的官方会话",
                _EXIT_ARGUMENT,
            )
    if structure_changes and (len(sessions) < 2 or sessions[-2] >= target):
        raise DiagnosticError(
            "D1_STRUCTURE_CHANGE_PREVIOUS_SESSION_UNAVAILABLE",
            "结构相邻变化至少需要窗口内的前一官方会话",
            _EXIT_ARGUMENT,
        )
    if history_mode:
        if args.history_last is not None:
            if args.history_last > len(sessions):
                raise DiagnosticError(
                    "D1_BREAKOUT_HISTORY_RANGE_INVALID",
                    "请求超出读取窗口",
                    _EXIT_ARGUMENT,
                )
            history_sessions = sessions[-args.history_last :]
        else:
            start = _parse_session_date(args.history_start)
            end = _parse_session_date(args.history_end)
            if (
                start not in sessions
                or end not in sessions
                or start > end
                or end > cutoff
            ):
                raise DiagnosticError(
                    "D1_BREAKOUT_HISTORY_RANGE_INVALID",
                    "历史起止须在读取窗口内且为官方会话",
                    _EXIT_ARGUMENT,
                )
            history_sessions = tuple(day for day in sessions if start <= day <= end)
    if include_structures and (not history_sessions or history_sessions[-1] != target):
        raise DiagnosticError(
            "D1_STRUCTURE_HISTORY_END_INVALID",
            "结构后继观察历史必须以目标官方会话结束",
            _EXIT_ARGUMENT,
        )
    if support_anchor is not None:
        if support_anchor not in sessions:
            raise DiagnosticError(
                "D1_BREAKOUT_SUPPORT_ANCHOR_NOT_OFFICIAL",
                "显式平台出生会话不在当前版本化官方会话窗口内",
                _EXIT_ARGUMENT,
            )
        if support_anchor not in history_sessions:
            raise DiagnosticError(
                "D1_BREAKOUT_SUPPORT_ANCHOR_OUT_OF_RANGE",
                "显式平台出生会话必须在请求的单股历史范围内",
                _EXIT_ARGUMENT,
            )
        if support_anchor >= target:
            raise DiagnosticError(
                "D1_BREAKOUT_SUPPORT_ANCHOR_NOT_HISTORICAL",
                "平台出生会话必须早于目标会话；出生日不参与同日自证",
                _EXIT_ARGUMENT,
            )
        if not history_sessions or history_sessions[-1] != target:
            raise DiagnosticError(
                "D1_BREAKOUT_SUPPORT_TARGET_NOT_HISTORY_END",
                "支撑观察历史范围必须以目标会话结束，避免输出范围与截止点不一致",
                _EXIT_ARGUMENT,
            )
    if support_cohort_anchor is not None and (
        support_cohort_anchor not in sessions or support_cohort_anchor >= target
    ):
        raise DiagnosticError(
            "D1_BREAKOUT_SUPPORT_COHORT_ANCHOR_INVALID",
            "来源批次支撑锚日须位于已加载版本化官方会话窗口内且早于目标日",
            _EXIT_ARGUMENT,
        )
    config, config_path = _load_runtime_config()
    try:
        if discover_support_changes:
            result = discover_source_d1_support_changes(
                config,
                config_path,
                target,
                cutoff,
                sessions,
                calendar_revision,
                args.source_package_id,
                event_window_start,
                offset,
                limit,
            )
            _emit(result)
            return _EXIT_OK
        if discover_structures:
            result = discover_source_d1_structures(
                config,
                target_session=target,
                research_cutoff=cutoff,
                sessions=sessions,
                source_package_id=args.source_package_id,
                offset=offset,
                limit=limit,
                structure_symbol=args.structure_symbol,
                structure_changes=structure_changes,
                structure_type=structure_type,
                structure_change_status=structure_change_status,
                material_changes_only=material_changes_only,
            )
            _emit(result)
            return _EXIT_OK
        if support_cohort_anchor is not None:
            result = _discover_source_d1_support_cohort(
                config,
                config_path,
                target,
                cutoff,
                sessions,
                calendar_revision,
                args.source_package_id,
                support_cohort_anchor,
                offset,
                limit,
                change_only=args.change_only,
            )
            _emit(result)
            return _EXIT_OK
        if args.only_true:
            result = _discover_source_d1_true(
                config,
                config_path,
                target,
                cutoff,
                sessions,
                calendar_revision,
                args.source_package_id,
                offset,
                limit,
                previous_state=args.previous_state,
            )
            _emit(result)
            return _EXIT_OK
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            source_package_id=args.source_package_id,
            sessions=sessions,
            offset=offset,
            limit=limit,
            market="ALL",
            board="ALL",
            source_symbol=source_symbol,
        )
        target_rows = [
            row
            for candidate in page["candidates"]
            for row in candidate["history_rows"]
            if row["session_date"] == target
        ]
        source_context = read_source_version_context(
            config,
            target_session=target,
            source_package_id=args.source_package_id,
            target_raw_rows=target_rows,
            expected_raw_row_count=page["version_row_count"],
        )
        if (
            page["version_status"] != "CLOSED"
            or not page["partition_closed"]
            or page["version_row_count"] != source_context["raw_row_count"]
            or page["version_expected_row_count"]
            != source_context["capture_expected_row_count"]
            or (source_symbol is not None and not page["candidates"])
        ):
            raise PgResearchBindingError(
                "D1_BREAKOUT_SOURCE_POOL_NOT_CLOSED",
                "目标日来源池与Raw捕获分母未闭合，拒绝生成谓词结果",
            )

        profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
        calendar_payload, _, _, _ = _load_calendar(target)
        official_calendar = {
            "calendar_id": calendar_payload["calendar_id"],
            "calendar_revision": calendar_payload["revision"],
        }
        if history_mode:
            candidate = page["candidates"][0]
            rows = candidate["history_rows"]
            points = []
            support_history_points = []
            structure_previews = {}
            counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
            calculation_sessions = sessions if include_structures else history_sessions
            history_session_set = set(history_sessions)
            for day in calculation_sessions:
                window = tuple(session for session in sessions if session <= day)
                bounded_rows = tuple(row for row in rows if row["session_date"] <= day)
                observed = research_stock_pg_history_preview(
                    symbol=source_symbol,
                    target_session=day.isoformat(),
                    source_package_id=args.source_package_id,
                    session_dates=window,
                    history_rows=bounded_rows,
                    profile_contract=profile_contract,
                    include_profile_preview=False,
                    include_raw_breakout=True,
                    allow_missing_target_row=True,
                )
                predicate = observed["raw_d1_breakout_predicate"]
                if day in history_session_set:
                    counts[predicate["predicates"]["result"]["state"]] += 1
                if include_structures:
                    structure_previews[day] = observed
                row = next(
                    (item for item in bounded_rows if item["session_date"] == day), None
                )
                point = {
                    "session": day.isoformat(),
                    "source_capture_id": str(row["source_capture_id"]) if row else None,
                    "source_available_at": row.get("source_available_at")
                    if row
                    else None,
                    "source_observed_started_at": row.get("observed_started_at")
                    if row
                    else None,
                    "source_observed_completed_at": row.get("observed_completed_at")
                    if row
                    else None,
                    "target_bar_status": observed["target_bar_status"],
                    "price_window_status": observed["price_window_status"],
                    "price_window_reason": observed["price_window_reason"],
                    "predicate": predicate,
                }
                if day in history_session_set:
                    points.append(point)
                if support_anchor is not None and day in history_session_set:
                    support_history_points.append(
                        {
                            "session": day,
                            "row": row,
                            "preview": observed,
                            "history_max_session": max(
                                (item["session_date"] for item in bounded_rows),
                                default=None,
                            ),
                        }
                    )
            support_followup = None
            if support_anchor is not None:
                if not isinstance(source_symbol, str):
                    raise DiagnosticError(
                        "D1_BREAKOUT_SUPPORT_SELECTION_INVALID",
                        "支撑跟踪必须绑定单个来源代码",
                        _EXIT_ARGUMENT,
                    )
                support_followup = research_stock_pg_support_followup_preview(
                    symbol=source_symbol,
                    target_session=target,
                    support_anchor_session=support_anchor,
                    official_sessions=sessions,
                    requested_sessions=history_sessions,
                    history_points=support_history_points,
                    source_package_id=args.source_package_id,
                    source_context=source_context,
                    official_calendar=official_calendar,
                )
            structure_timeline = None
            if include_structures:
                structure_timeline = evaluate_source_d1_structure_timeline(
                    symbol=source_symbol,
                    sessions=sessions,
                    history_rows=rows,
                    source_package_id=args.source_package_id,
                    core_previews_by_session=structure_previews,
                )
                timeline_points = {
                    item["session"]: item for item in structure_timeline["points"]
                }
                for point in points:
                    structure_point = timeline_points[point["session"]]
                    point["structures"] = structure_point["structures"]
                    point["subsequent_support_observations"] = structure_point[
                        "subsequent_support_observations"
                    ]
                structure_timeline = {
                    key: value
                    for key, value in structure_timeline.items()
                    if key != "points"
                }
            _emit(
                {
                    "ok": True,
                    "status": "SOURCE_LEVEL_RAW_BREAKOUT_HISTORY_WITH_D1_STRUCTURES"
                    if include_structures
                    else "SOURCE_LEVEL_RAW_BREAKOUT_HISTORY_WITH_SUPPORT_FOLLOWUP"
                    if support_anchor is not None
                    else "SOURCE_LEVEL_RAW_BREAKOUT_HISTORY",
                    "research_mode": "NON_PIT_RETROSPECTIVE",
                    "identity_status": "SOURCE_SYMBOL_ONLY",
                    "price_coordinate": "RAW_UNADJUSTED",
                    "symbol": source_symbol,
                    "target_session": target.isoformat(),
                    "research_cutoff": cutoff.isoformat(),
                    "calendar_revision": calendar_revision,
                    "source": source_context,
                    "source_capture_started_at": source_context.get(
                        "capture_read_started_at"
                    ),
                    "source_capture_validated_at": source_context.get(
                        "capture_validated_at"
                    ),
                    "n_total": len(points),
                    "n_true": counts["TRUE"],
                    "n_false": counts["FALSE"],
                    "n_unknown": counts["UNKNOWN"],
                    "points": points,
                    "support_followup": support_followup,
                    "structure_timeline": structure_timeline,
                    "formal_downstream": {
                        "d1_anchor": "NOT_EVALUATED",
                        "episode": "NOT_EVALUATED",
                    },
                    "execution": {
                        "database_transaction": "REPEATABLE_READ_READ_ONLY",
                        "database_write_attempted": False,
                        "ddl_executed": False,
                        "dml_executed": False,
                    },
                }
            )
            return _EXIT_OK
        preview = research_universe_pg_history_preview(
            target_session=target.isoformat(),
            source_package_id=args.source_package_id,
            session_dates=sessions,
            page=page,
            profile_contract=profile_contract,
            include_profile_preview=False,
            include_raw_breakout=True,
        )
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT if error.code.endswith("INVALID") else _EXIT_UNAVAILABLE
    except PgResearchBindingError as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    except (PriceProfileContractError, ResearchEntryError) as error:
        _emit(
            {
                "ok": False,
                "status": "REJECTED",
                "code": error.code,
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE

    candidate_by_symbol = {item["source_symbol"]: item for item in page["candidates"]}
    results = []
    predicate_counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
    consumer_failures = 0
    for item in preview["results"]:
        candidate = candidate_by_symbol[item["symbol"]]
        target_row = next(
            row for row in candidate["history_rows"] if row["session_date"] == target
        )
        predicate = item.get("raw_d1_breakout_predicate")
        if item.get("consumer_status") != "RESEARCHED" or not isinstance(
            predicate, dict
        ):
            consumer_failures += 1
        else:
            state = predicate["predicates"]["result"]["state"]
            predicate_counts[state] += 1
        results.append(
            {
                "symbol": item["symbol"],
                "consumer_status": item.get("consumer_status"),
                "consumer_error_code": item.get("consumer_error_code"),
                "target_bar_status": item.get("target_bar_status"),
                "history_records_complete": item.get("history_records_complete"),
                "price_window_status": item.get("price_window_status"),
                "source_revision": {
                    "source_capture_id": str(target_row["source_capture_id"]),
                    "source_package_id": target_row.get("source_package_id"),
                    "source_clock_role": target_row.get("source_clock_role"),
                    "source_time_quality": source_context["source_time_quality"],
                    "source_available_at": target_row.get("source_available_at"),
                    "observed_started_at": target_row.get("observed_started_at"),
                    "observed_completed_at": target_row.get("observed_completed_at"),
                    "price_coordinate": "RAW_UNADJUSTED",
                    "coordinate_id": f"RAW:TDX_SOURCE_SYMBOL:{item['symbol']}",
                },
                "capture_revisions": item.get("source_capture_revisions"),
                "raw_d1_breakout_predicate": predicate,
            }
        )

    page_is_complete = (
        consumer_failures == 0 and sum(predicate_counts.values()) == page["page_count"]
    )
    result = {
        "ok": page_is_complete,
        "status": "SOURCE_LEVEL_RAW_BREAKOUT_PREDICATE_PREVIEW"
        if page_is_complete
        else "PARTIAL_EVIDENCE",
        "research_mode": "NON_PIT_RETROSPECTIVE",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_session": target.isoformat(),
        "research_cutoff": cutoff.isoformat(),
        "calendar_revision": calendar_revision,
        "source_package_id": args.source_package_id,
        "source_time_quality": source_context["source_time_quality"],
        "source_available_at": source_context["source_available_at"],
        "price_qualities": source_context["price_qualities"],
        "amount_unit_statuses": source_context["amount_unit_statuses"],
        "volume_unit_statuses": source_context["volume_unit_statuses"],
        "source_pool_actual_total": source_context["raw_row_count"],
        "page": {
            "offset": offset,
            "limit": limit,
            "page_count": page["page_count"],
            "filtered_total": page["total_count"],
            "source_pool_actual_total": source_context["raw_row_count"],
            "next_offset": page["next_offset"],
            "symbols": [item["source_symbol"] for item in page["candidates"]],
            "stable_order": "source_symbol ASC",
        },
        "predicate_counts": predicate_counts,
        "consumer_failure_count": consumer_failures,
        "predicate_partition_closed": page_is_complete,
        "results": results,
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
            "source_recalculated": False,
        },
    }
    _emit(result, error=not page_is_complete)
    return _EXIT_OK if page_is_complete else _EXIT_UNAVAILABLE
