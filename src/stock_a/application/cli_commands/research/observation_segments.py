"""来源级观察段的正式离线CLI入口。"""

from __future__ import annotations

from stock_a.application.cli_commands.common import _emit, _parse_session_date
from stock_a.application.runtime_config import (
    _EXIT_ARGUMENT,
    _EXIT_ENGINEERING,
    _EXIT_OK,
    DiagnosticError,
    load_runtime_config,
)
from stock_a.application.source_observation_segments import (
    produce_source_observation_segments,
)
from stock_a.storage.postgres.source_observation_segments import (
    ObservationSegmentConflict,
)
from stock_a.storage.postgres.source_raw import RawSourcePgError


def _run_source_observation_segments(args) -> int:
    if not args.persist:
        raise DiagnosticError(
            "SOURCE_OBSERVATION_SEGMENTS_PERSIST_REQUIRED",
            "来源观察段只能由显式离线--persist生产typed事实",
            _EXIT_ARGUMENT,
        )
    try:
        cutoff = _parse_session_date(args.research_cutoff)
        config, config_path = load_runtime_config()
        rules = tuple(sorted(set(args.rule_id))) if args.rule_id else None
        result = produce_source_observation_segments(
            config,
            research_cutoff=cutoff,
            source_package_id=args.source_package_id,
            rule_ids=rules,
        )
    except ObservationSegmentConflict as error:
        _emit(
            {
                "ok": False,
                "code": "SOURCE_OBSERVATION_SEGMENT_CONFLICT",
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
                "code": getattr(
                    error, "code", "SOURCE_OBSERVATION_SEGMENTS_INPUT_INVALID"
                ),
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
            "producer": "stock_a.application.source_observation_segments.produce_source_observation_segments",
            "source_reads": "EXISTING_PG_TYPED_REGISTRATIONS_RULE_FACTS_AND_CONTEXTS_ONLY",
            "market_data_read_after_cutoff": False,
        }
    )
    _emit(result)
    return _EXIT_OK
