"""正式CLI中的FEP离线条件统计生产入口。"""

from __future__ import annotations

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.fep_baseline import produce_fep_baseline
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    DiagnosticError,
    load_runtime_config,
)
from stock_a.storage.postgres.research_facts import ResearchFactConflict
from stock_a.storage.postgres.source_raw import RawSourcePgError


def _run_source_fep_baseline(args) -> int:
    if not args.persist:
        raise DiagnosticError(
            "FEP_PERSIST_REQUIRED",
            "FEP只能通过显式离线--persist生产typed事实",
            _EXIT_ARGUMENT,
        )
    try:
        cutoff = _parse_session_date(args.research_cutoff)
        config, config_path = load_runtime_config()
        result = produce_fep_baseline(
            config,
            research_cutoff=cutoff,
            source_capture_id=args.source_capture_id,
            rule_ids=tuple(sorted(set(args.rule_id))),
            horizons=tuple(sorted(set(args.horizon))),
            lanes=tuple(sorted(set(args.lane))),
        )
    except ResearchFactConflict as error:
        _emit(
            {
                "ok": False,
                "code": "FEP_BASELINE_FACT_CONFLICT",
                "message": str(error),
                "database_write_attempted": True,
                "transaction": "ROLLED_BACK",
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    except ValueError as error:
        _emit(
            {
                "ok": False,
                "code": getattr(error, "code", "FEP_BASELINE_INPUT_INVALID"),
                "message": str(error),
                "database_write_attempted": False,
            },
            error=True,
        )
        return _EXIT_ARGUMENT
    except RawSourcePgError as error:
        _emit(
            {
                "ok": False,
                "code": error.code,
                "message": str(error),
                "database_write_attempted": "UNKNOWN",
                "committed_rows": error.committed_rows,
            },
            error=True,
        )
        return _EXIT_ENGINEERING
    result.update(
        {
            "ok": True,
            "configuration_path": str(config_path),
            "producer": "stock_a.application.fep_baseline.produce_fep_baseline",
            "source_reads": "EXISTING_PG_TYPED_FACTS_ONLY",
            "future_market_quotes_read": False,
        }
    )
    _emit(result)
    return _EXIT_OK
