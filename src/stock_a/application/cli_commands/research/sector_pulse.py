"""板块PULSE缺失日补算及相邻变化静态PG生产命令。"""

from __future__ import annotations

import argparse

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.runtime_config import (
    _EXIT_ENGINEERING,
    _EXIT_OK,
    _EXIT_UNAVAILABLE,
    DiagnosticError,
    load_runtime_config,
)
from stock_a.application.source_sector_pulse import (
    SourceSectorPulseError,
    run_source_sector_pulse_transition_day,
)


def _run_source_sector_pulse_transition_day(args: argparse.Namespace) -> int:
    try:
        config, config_path = load_runtime_config()
        result = run_source_sector_pulse_transition_day(
            config,
            previous_session=_parse_session_date(args.previous_session),
            target_session=_parse_session_date(args.target_session),
            research_cutoff=_parse_session_date(args.research_cutoff),
            previous_package_sha256=args.previous_package_sha256,
            target_package_sha256=args.target_package_sha256,
            previous_profile_revision=args.previous_profile_revision,
            target_profile_revision=args.target_profile_revision,
            industry_snapshot_id=args.industry_snapshot_id,
            concept_snapshot_id=args.concept_snapshot_id,
        )
    except DiagnosticError as error:
        _emit({"ok": False, "code": error.code, "message": str(error)}, error=True)
        return error.exit_code
    except SourceSectorPulseError as error:
        _emit({"ok": False, "code": error.code, "message": str(error)}, error=True)
        return _EXIT_UNAVAILABLE
    except Exception as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "SECTOR_PULSE_TRANSITION_FAILED"),
                "cause_type": type(error).__name__,
                "message": f"板块PULSE相邻变化静态生产失败：{type(error).__name__}",
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    result["configuration_path"] = str(config_path)
    result["ok"] = True
    _emit(result)
    return (
        _EXIT_OK if result.get("readback_status") == "AVAILABLE" else _EXIT_UNAVAILABLE
    )


__all__ = ["_run_source_sector_pulse_transition_day"]
