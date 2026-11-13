"""单日来源D0/D1事实计算及显式持久化CLI命令。"""

from __future__ import annotations

import argparse
from datetime import date

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    DiagnosticError,
    load_runtime_config,
)
from stock_a.application.source_research_day import (
    SourceResearchDayError,
    run_source_d0_launch_reference_day,
    run_source_research_day,
    run_source_static_research_day,
)
from stock_a.application.source_d0_raw import _official_calendar
from stock_a.storage.postgres.source_raw import read_raw_daily_catalog


def _range_sessions(config, start: date, end: date, cutoff: date, package: int):
    """按已存目录与官方日历构造范围；不猜测同日多来源版本。"""
    if start > end or end > cutoff:
        raise ValueError("日期范围必须满足起始日不晚于目标日及研究截止日")
    versions = []
    offset = 0
    while True:
        page = read_raw_daily_catalog(
            config, max_target_session=cutoff, offset=offset, limit=200
        )
        versions.extend(page["versions"])
        if page["next_offset"] is None:
            break
        offset = page["next_offset"]
    by_date = {}
    for version in versions:
        day = date.fromisoformat(version["session_date"])
        if start <= day <= end:
            by_date.setdefault(day, []).append(version)
    official = _official_calendar(end)["sessions"]
    sessions = set(official)
    available_dates = {date.fromisoformat(item["session_date"]) for item in versions}
    result = []
    for day, items in sorted(by_date.items()):
        if day not in sessions or len(items) != 1:
            raise ValueError(f"{day}不是唯一官方会话来源版本")
        version = items[0]
        if (
            version["source_package_id"] != package
            or version["version_status"] != "CLOSED"
        ):
            raise ValueError(f"{day}来源包不匹配或来源分区未闭合")
        index = official.index(day)
        has_before = index > 0 and official[index - 1] in available_dates
        result.append((day, has_before))
    if not result:
        raise ValueError("指定日期范围没有已存来源会话")
    return result


def _run_source_research_day(args: argparse.Namespace) -> int:
    try:
        if args.static_only and not args.persist:
            raise ValueError("--static-only 必须显式指定 --persist")
        snapshot_ids = {
            "INDUSTRY": args.industry_snapshot_id,
            "CONCEPT": args.concept_snapshot_id,
        }
        snapshot_ids_supplied = any(snapshot_ids.values())
        if args.include_sector_pulse and (
            not args.static_only
            or args.start_session
            or not all(snapshot_ids.values())
            or snapshot_ids["INDUSTRY"] == snapshot_ids["CONCEPT"]
        ):
            raise ValueError(
                "--include-sector-pulse 仅用于单日--static-only，并须提供不同的行业与GN数字快照ID"
            )
        if snapshot_ids_supplied and not args.include_sector_pulse:
            raise ValueError("板块数字快照ID只能与 --include-sector-pulse 同时使用")
        sector_snapshot_ids = snapshot_ids if args.include_sector_pulse else None
        config, config_path = load_runtime_config()
        if args.enrollment_from_saved:
            if (
                not args.persist
                or args.start_session
                or args.static_only
                or args.d0_launch_reference_only
                or args.include_sector_pulse
                or snapshot_ids_supplied
            ):
                raise ValueError("已存事实登记仅支持单日并须显式指定--persist")
            from stock_a.application.research_window import resolve_pg_history_window
            from stock_a.storage.postgres.research_enrollment import (
                persist_existing_enrollments,
            )

            target = _parse_session_date(args.target_session)
            cutoff = _parse_session_date(args.research_cutoff)
            if target > cutoff:
                raise ValueError("目标会话晚于研究截止日期")
            _, sessions, _ = resolve_pg_history_window(target.isoformat(), 2)
            report = persist_existing_enrollments(
                config,
                target=target,
                before=sessions[0],
                package=args.source_package_id,
                d1_history_count=args.d1_history_session_count,
            )
            report["configuration_path"] = str(config_path)
            report["ok"] = True
            _emit(report)
            return _EXIT_OK
        if args.d0_launch_reference_only:
            if (
                not args.persist
                or args.static_only
                or args.start_session
                or args.include_sector_pulse
                or snapshot_ids_supplied
            ):
                raise ValueError(
                    "--d0-launch-reference-only须单日指定--persist，且不能合并Seed、D1或PULSE生产"
                )
            report = run_source_d0_launch_reference_day(
                config,
                target_session=_parse_session_date(args.target_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
                persist=args.persist,
            )
            report["configuration_path"] = str(config_path)
            report["ok"] = True
            _emit(report)
            return _EXIT_OK
        if args.static_only and not args.start_session:
            report = run_source_static_research_day(
                config,
                target_session=_parse_session_date(args.target_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
                persist=args.persist,
                history_session_count=args.history_session_count,
                worker_count=args.workers,
                include_sector_pulse=args.include_sector_pulse,
                sector_snapshot_ids=sector_snapshot_ids,
            )
            report["configuration_path"] = str(config_path)
            report["ok"] = True
            _emit(report)
            return _EXIT_OK
        if args.start_session:
            start = _parse_session_date(args.start_session)
            end = _parse_session_date(args.target_session)
            cutoff = _parse_session_date(args.research_cutoff)
            sessions = _range_sessions(
                config, start, end, cutoff, args.source_package_id
            )
            skipped = False
            for target, has_before in sessions:
                if not has_before and not args.static_only:
                    skipped = True
                    _emit(
                        {
                            "ok": False,
                            "target_session": target.isoformat(),
                            "status": "INSUFFICIENT_HISTORY",
                            "reason": "相邻前一官方会话没有已存来源，单日D0/D1联合事实未写入",
                        }
                    )
                    continue
                if args.static_only:
                    report = run_source_static_research_day(
                        config,
                        target_session=target,
                        research_cutoff=cutoff,
                        source_package_id=args.source_package_id,
                        persist=args.persist,
                        history_session_count=args.history_session_count,
                        worker_count=args.workers,
                        include_sector_pulse=False,
                    )
                else:
                    report = run_source_research_day(
                        config,
                        target_session=target,
                        research_cutoff=cutoff,
                        source_package_id=args.source_package_id,
                        d1_history_session_count=args.d1_history_session_count,
                        persist=args.persist,
                        history_session_count=args.history_session_count,
                        worker_count=args.workers,
                    )
                report["configuration_path"] = str(config_path)
                report["ok"] = True
                _emit(report)
            return _EXIT_UNAVAILABLE if skipped else _EXIT_OK
        if args.static_only:
            report = run_source_static_research_day(
                config,
                target_session=_parse_session_date(args.target_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
                persist=args.persist,
                history_session_count=args.history_session_count,
                worker_count=args.workers,
                include_sector_pulse=args.include_sector_pulse,
                sector_snapshot_ids=sector_snapshot_ids,
            )
        else:
            report = run_source_research_day(
                config,
                target_session=_parse_session_date(args.target_session),
                research_cutoff=_parse_session_date(args.research_cutoff),
                source_package_id=args.source_package_id,
                d1_history_session_count=args.d1_history_session_count,
                persist=args.persist,
                history_session_count=args.history_session_count,
                worker_count=args.workers,
            )
    except DiagnosticError as error:
        _emit({"ok": False, "code": error.code, "message": str(error)}, error=True)
        return error.exit_code
    except (SourceResearchDayError, ValueError) as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "RESEARCH_DAY_ARGUMENT_OR_DATA_ERROR"),
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    except Exception as error:
        error_code = getattr(error, "code", None)
        cause = error.__cause__
        _emit(
            {
                "ok": False,
                "code": error_code or "RESEARCH_DAY_ENGINEERING_ERROR",
                "cause_type": type(error).__name__,
                "cause_code": getattr(cause, "code", None),
                "cause_message": str(cause) if cause is not None else None,
                "message": str(error)
                if error_code
                else f"单日研究计算失败：{type(error).__name__}",
                "database_write_attempted": bool(args.persist),
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    report["configuration_path"] = str(config_path)
    report["ok"] = True
    _emit(report)
    return (
        _EXIT_OK
        if report.get("status") in {"DRY_RUN_READY", "PERSISTED"}
        else _EXIT_UNAVAILABLE
    )


__all__ = ["_run_source_research_day"]
