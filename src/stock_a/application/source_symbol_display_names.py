"""生产与读取仅用于页面的本机非PIT中文显示标签。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from stock_a.sources.tdx.display_names import (
    DISPLAY_NAME_PARSE_CONTRACT,
    TdxDisplayNameError,
    parse_tdx_display_name_snapshot,
)
from stock_a.storage.postgres.source_symbol_display_names import (
    SourceSymbolDisplayNameError,
    decorate_source_symbol_display_names,
    persist_source_symbol_display_names,
    read_source_symbol_display_names,
)


class SourceSymbolDisplayNameApplicationError(ValueError):
    """展示名称的来源根、生产输入或PG消费者边界错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def capture_source_symbol_display_names(
    config: Mapping[str, Any], *, persist: bool
) -> dict[str, Any]:
    """只读唯一配置TDX根，生成并可选保存当前展示标签快照。"""
    policy = config.get("source_policy")
    roots = (
        policy.get("configured_tdx_source_roots")
        if isinstance(policy, Mapping)
        else None
    )
    if (
        not isinstance(policy, Mapping)
        or policy.get("all_source_roots_read_only") is not True
        or not isinstance(roots, list)
        or len(roots) != 1
        or not isinstance(roots[0], str)
        or not roots[0]
    ):
        raise SourceSymbolDisplayNameApplicationError(
            "DISPLAY_NAME_SOURCE_ROOT_NOT_BOUND",
            "中文显示标签要求唯一登记的只读TDX来源根",
        )
    captured_at = datetime.now(timezone.utc)
    try:
        snapshot = parse_tdx_display_name_snapshot(roots[0], captured_at=captured_at)
    except TdxDisplayNameError as error:
        raise SourceSymbolDisplayNameApplicationError(error.code, str(error)) from error
    result = {
        "status": "PARSED_NOT_PERSISTED",
        "source_root": snapshot.source_root,
        "captured_at": snapshot.captured_at.isoformat(),
        "parser_contract": DISPLAY_NAME_PARSE_CONTRACT,
        "source_files": snapshot.source_file_sizes,
        "parsed_record_counts": snapshot.parsed_record_counts,
        "rejected_truncated_name_counts": snapshot.rejected_truncated_name_counts,
        "parsed_valid_display_names": len(snapshot.rows),
        "source_authority": "UNVERIFIED_LOCAL_TDX_INSTALLATION",
        "source_time_quality": "NON_PIT_OBSERVED",
        "source_clock_role": "OBSERVED_CAPTURED",
        "source_available_at": None,
        "identity_status": "SOURCE_SYMBOL_ONLY_NOT_STABLE_ENTITY",
        "pit_status": "NON_PIT",
        "source_downloaded": False,
        "source_modified": False,
        "page_use_only": True,
    }
    if persist:
        try:
            result["persistence"] = persist_source_symbol_display_names(
                config, snapshot
            )
        except SourceSymbolDisplayNameError as error:
            raise SourceSymbolDisplayNameApplicationError(
                error.code, str(error)
            ) from error
        result["status"] = result["persistence"]["status"]
    return result


def read_current_source_symbol_display_names(
    config: Mapping[str, Any], *, source_symbols: tuple[str, ...]
) -> dict[str, Any]:
    try:
        return read_source_symbol_display_names(config, source_symbols=source_symbols)
    except SourceSymbolDisplayNameError as error:
        raise SourceSymbolDisplayNameApplicationError(error.code, str(error)) from error


def decorate_api_source_symbols(
    config: Mapping[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        return decorate_source_symbol_display_names(config, payload)
    except SourceSymbolDisplayNameError as error:
        raise SourceSymbolDisplayNameApplicationError(error.code, str(error)) from error


__all__ = [
    "SourceSymbolDisplayNameApplicationError",
    "capture_source_symbol_display_names",
    "decorate_api_source_symbols",
    "read_current_source_symbol_display_names",
]
