"""BaoStock研究入口使用的证券代码与板别规范化。"""

from __future__ import annotations

import re

from stock_a.research.entry_context.errors import ResearchEntryError


_SYMBOL_PATTERN = re.compile(r"^(SH|SZ)[.]?([0-9]{6})$", re.IGNORECASE)


def _normalize_symbol(value: str) -> tuple[str, str, str]:
    if not isinstance(value, str):
        raise ResearchEntryError("SYMBOL_INVALID", "证券代码必须带SH或SZ市场前缀")
    match = _SYMBOL_PATTERN.fullmatch(value.strip())
    if not match:
        raise ResearchEntryError(
            "SYMBOL_INVALID", "证券代码格式必须是SH.六位代码或SZ.六位代码"
        )
    market, code = match.groups()
    market = market.upper()
    if market == "SH":
        if code.startswith(("600", "601", "603", "605")):
            board = "MAIN_BOARD"
        elif code.startswith(("688", "689")):
            board = "STAR_BOARD"
        else:
            board = None
        directory = "sh"
    else:
        if code.startswith(("000", "001", "002", "003")):
            board = "MAIN_BOARD"
        elif code.startswith(("300", "301")):
            board = "GEM"
        else:
            board = None
        directory = "sz"
    if board is None:
        raise ResearchEntryError(
            "SYMBOL_OUT_OF_SCOPE", "代码类别不属于本入口的沪深主板、创业板或科创板范围"
        )
    symbol = f"{market}.{code}"
    return symbol, directory, board


__all__ = ["_normalize_symbol"]
