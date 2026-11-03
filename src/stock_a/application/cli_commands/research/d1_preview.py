"""Stock A命令行d1_preview职责模块。"""

from __future__ import annotations

import argparse
import re
import uuid
from stock_a.contracts import (
    DAY_CHAIN_MODULES,
    DayChainContractError,
    load_day_chain_contract,
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
    read_sealed_source_market_breadth_history,
)
from stock_a.application.source_d1_support_changes import (
    discover_source_d1_support_changes,
)
from stock_a.application.source_set_candidate import (
    SourceSetCandidateApplicationError,
    _official_calendar,
    persist_source_set_candidate,
    prepare_source_set_candidate,
    read_source_set_candidate,
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

from stock_a.application.cli_commands.common import _emit, _parse_source_set_date
from stock_a.application.cli_commands.research.d1_discovery import (
    _discover_source_d1_support_cohort,
    _discover_source_d1_true,
)


def _run_source_d1_breakout_preview(args: argparse.Namespace) -> int:
    target = _parse_source_set_date(args.target_session)
    cutoff = _parse_source_set_date(args.research_cutoff)
    if target > cutoff:
        raise DiagnosticError(
            "D1_BREAKOUT_DATE_AFTER_CUTOFF",
            "目标交易会话晚于研究截止会话，拒绝读取PG",
            _EXIT_ARGUMENT,
        )
    if re.fullmatch(r"[0-9a-f]{64}", args.package_sha256) is None:
        raise DiagnosticError(
            "D1_BREAKOUT_PACKAGE_SHA_INVALID",
            "必须提供完整64位小写package SHA-256",
            _EXIT_ARGUMENT,
        )
    try:
        canonical_source_set_id = str(uuid.UUID(args.source_set_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise DiagnosticError(
            "D1_BREAKOUT_SOURCE_SET_ID_INVALID",
            "source_set_id必须为规范UUID",
            _EXIT_ARGUMENT,
        ) from error
    if canonical_source_set_id != args.source_set_id:
        raise DiagnosticError(
            "D1_BREAKOUT_SOURCE_SET_ID_INVALID",
            "source_set_id必须为规范UUID",
            _EXIT_ARGUMENT,
        )

    history_mode = any(
        (args.history_start, args.history_end, args.history_last is not None)
    )
    discover_support_changes = args.discover_support_changes
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
        support_anchor = _parse_source_set_date(args.support_anchor_session)
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
        support_cohort_anchor = _parse_source_set_date(
            args.support_cohort_anchor_session
        )
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
        event_window_start = _parse_source_set_date(args.event_window_start)
        if event_window_start not in sessions or event_window_start >= target:
            raise DiagnosticError(
                "D1_SUPPORT_DISCOVERY_WINDOW_INVALID",
                "事件窗口起始须是读取窗口内早于目标日的官方会话",
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
            start = _parse_source_set_date(args.history_start)
            end = _parse_source_set_date(args.history_end)
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
                args.source_set_id,
                args.package_sha256,
                event_window_start,
                offset,
                limit,
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
                args.source_set_id,
                args.package_sha256,
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
                args.source_set_id,
                args.package_sha256,
                offset,
                limit,
                previous_state=args.previous_state,
            )
            _emit(result)
            return _EXIT_OK
        page = read_raw_daily_history_page(
            config,
            target_session=target,
            package_sha256=args.package_sha256,
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
        source_context = read_sealed_source_context(
            config,
            target_session=target,
            package_sha256=args.package_sha256,
            namespace_id=page["namespace_id"],
            target_raw_rows=target_rows,
            expected_raw_row_count=page["version_row_count"],
        )
        if source_context["source_set_id"] != args.source_set_id:
            raise PgResearchBindingError(
                "D1_BREAKOUT_SOURCE_SET_ID_MISMATCH",
                "指定source_set_id与目标日精确SEALED来源集合不一致",
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
                "目标日来源池与精确SEALED分母未闭合，拒绝生成谓词结果",
            )

        profile_contract = load_day_chain_contract(_CONFIG_ROOT).document
        if history_mode:
            from stock_a.storage.postgres.source_set_ingress import (
                read_p04_source_input,
            )

            sealed = read_p04_source_input(
                config, target_session=target, package_sha256=args.package_sha256
            )
            if (
                sealed.get("status") != "AVAILABLE"
                or sealed.get("source_set_id") != source_context["source_set_id"]
                or sealed.get("namespace_id") != source_context["namespace_id"]
                or sealed.get("seal_status") != "SEALED"
                or not sealed.get("sealed_at")
            ):
                raise PgResearchBindingError(
                    "D1_BREAKOUT_SOURCE_CLOCK_MISMATCH",
                    "来源封口时钟与精确来源集合不一致",
                )
            if support_anchor is not None:
                official_calendar = _official_calendar(target)
                if any(
                    sealed.get(key) != official_calendar[key]
                    for key in ("calendar_id", "calendar_revision", "calendar_sha256")
                ):
                    raise PgResearchBindingError(
                        "D1_BREAKOUT_SUPPORT_CALENDAR_MISMATCH",
                        "支撑历史来源封口与当前版本化官方日历身份、修订或摘要不一致",
                    )
            candidate = page["candidates"][0]
            rows = candidate["history_rows"]
            points = []
            support_history_points = []
            counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0}
            for day in history_sessions:
                window = tuple(session for session in sessions if session <= day)
                bounded_rows = tuple(row for row in rows if row["session_date"] <= day)
                observed = research_stock_pg_history_preview(
                    symbol=source_symbol,
                    target_session=day.isoformat(),
                    package_sha256=args.package_sha256,
                    session_dates=window,
                    history_rows=bounded_rows,
                    namespace_id=page["namespace_id"],
                    profile_contract=profile_contract,
                    include_profile_preview=False,
                    include_raw_breakout=True,
                    allow_missing_target_row=True,
                )
                predicate = observed["raw_d1_breakout_predicate"]
                counts[predicate["predicates"]["result"]["state"]] += 1
                row = next(
                    (item for item in bounded_rows if item["session_date"] == day), None
                )
                points.append(
                    {
                        "session": day.isoformat(),
                        "source_capture_id": str(row["source_capture_id"])
                        if row
                        else None,
                        "source_entry_sha256": row.get("source_entry_sha256")
                        if row
                        else None,
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
                )
                if support_anchor is not None:
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
                support_followup = research_stock_pg_support_followup_preview(
                    symbol=source_symbol,
                    target_session=target,
                    support_anchor_session=support_anchor,
                    official_sessions=sessions,
                    requested_sessions=history_sessions,
                    history_points=support_history_points,
                    package_sha256=args.package_sha256,
                    source_set_id=args.source_set_id,
                    source_context=source_context,
                    sealed_source=sealed,
                )
            _emit(
                {
                    "ok": True,
                    "status": "SOURCE_LEVEL_RAW_BREAKOUT_HISTORY_WITH_SUPPORT_FOLLOWUP"
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
                    "source_capture_started_at": sealed.get("capture_read_started_at"),
                    "source_capture_validated_at": sealed.get("capture_validated_at"),
                    "source_sealed_at": sealed.get("sealed_at"),
                    "n_total": len(points),
                    "n_true": counts["TRUE"],
                    "n_false": counts["FALSE"],
                    "n_unknown": counts["UNKNOWN"],
                    "points": points,
                    "support_followup": support_followup,
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
            package_sha256=args.package_sha256,
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
    except (DayChainContractError, ResearchEntryError) as error:
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
                    "source_entry_sha256": target_row.get("source_entry_sha256"),
                    "source_capture_id": str(target_row["source_capture_id"]),
                    "source_content_id": target_row.get("source_content_id"),
                    "package_sha256": target_row["package_sha256"].strip(),
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
        "package_sha256": args.package_sha256,
        "namespace_id": source_context["namespace_id"],
        "source_set_id": source_context["source_set_id"],
        "source_set_status": source_context["seal_status"],
        "source_time_quality": source_context["source_time_quality"],
        "source_available_at": source_context["source_available_at"],
        "price_unit_status": source_context["price_unit_status"],
        "amount_unit_status": source_context["amount_unit_status"],
        "volume_unit_status": source_context["volume_unit_status"],
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
