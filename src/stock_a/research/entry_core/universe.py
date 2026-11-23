"""已退役的本地TDX股票候选诊断入口。"""

from __future__ import annotations

from typing import Any

from stock_a.research.entry_context.errors import ResearchEntryError


def research_universe_preview(*args: Any, **kwargs: Any) -> dict[str, Any]:
    """拒绝恢复本地TDX股票日线候选发现与价量读取。"""
    raise ResearchEntryError(
        "TDX_STOCK_RESEARCH_RETIRED",
        "本地TDX股票日线研究入口已退役；请使用BaoStock价格事实的正式消费者。",
    )


__all__ = ["research_universe_preview"]
