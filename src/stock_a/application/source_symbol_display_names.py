"""生产与读取仅用于页面的本机非PIT中文显示标签。"""

from __future__ import annotations

from typing import Any, Mapping

from stock_a.storage.postgres.source_symbol_display_names import (
    SourceSymbolDisplayNameError,
    decorate_source_symbol_display_names,
    read_source_symbol_display_names,
)


class SourceSymbolDisplayNameApplicationError(ValueError):
    """展示名称的来源根、生产输入或PG消费者边界错误。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


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
    "decorate_api_source_symbols",
    "read_current_source_symbol_display_names",
]
