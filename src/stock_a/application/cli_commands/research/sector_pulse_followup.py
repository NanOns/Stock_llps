"""显式离线生产板块PULSE真实后继表现。"""

from __future__ import annotations

import argparse

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    load_runtime_config,
)
from stock_a.application.source_sector_pulse_followup import (
    SourceSectorPulseFollowupError,
    run_source_sector_pulse_followup_day,
)
from stock_a.storage.postgres.sector_pulse_followups import (
    SectorPulseFollowupPgError,
)


def _run_source_sector_pulse_followup_day(args: argparse.Namespace) -> int:
    try:
        if args.persist is not True:
            raise ValueError("板块PULSE后继生产必须显式指定--persist")
        config, config_path = load_runtime_config()
        target_package_ids = {}
        for value in args.target_package:
            parts = value.split("=")
            if len(parts) != 2 or not all(part.isdecimal() for part in parts):
                raise ValueError("终点来源包格式必须为期限=包ID")
            horizon, package_id = map(int, parts)
            if (
                horizon not in (1, 3, 5)
                or package_id <= 0
                or horizon in target_package_ids
            ):
                raise ValueError("终点期限或包ID无效，或期限重复")
            target_package_ids[horizon] = package_id
        report = run_source_sector_pulse_followup_day(
            config,
            target_session=_parse_session_date(args.target_session),
            research_cutoff=_parse_session_date(args.research_cutoff),
            source_package_id=args.source_package_id,
            target_package_ids=target_package_ids,
        )
        report["configuration_path"] = str(config_path)
    except (SectorPulseFollowupPgError, SourceSectorPulseFollowupError) as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_write_attempted": getattr(
                    error, "database_write_attempted", False
                ),
                "transaction_rolled_back": getattr(
                    error, "transaction_rolled_back", None
                ),
                "ddl_executed": False,
            },
            error=True,
        )
        return _EXIT_UNAVAILABLE
    except ValueError as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "PULSE_FOLLOWUP_ARGUMENT_OR_DATA_ERROR"),
                "message": str(error),
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    except Exception as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "PULSE_FOLLOWUP_ENGINEERING_ERROR"),
                "cause_type": type(error).__name__,
                "cause_code": getattr(error.__cause__, "code", None),
                "message": str(error)
                if isinstance(
                    error, (SectorPulseFollowupPgError, SourceSectorPulseFollowupError)
                )
                else f"板块PULSE后继研究失败：{type(error).__name__}",
                "database_write_attempted": False,
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    report["ok"] = report.get("status") == "PERSISTED"
    _emit(report, error=not report["ok"])
    return _EXIT_OK if report["ok"] else _EXIT_UNAVAILABLE


__all__ = ["_run_source_sector_pulse_followup_day"]
