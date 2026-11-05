"""TDX研究的稳定Python入口；领域实现各由下列单一研究模块负责。"""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from stock_a.research.entry_core.comparison import research_compare
from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.windows import (
    CompareBudget,
    UniverseDiscoveryBudget,
)
from stock_a.research.entry_history.history_preview import (
    research_stock_pg_history_preview,
)
from stock_a.research.entry_history.history_timeline import (
    research_stock_pg_history_timeline,
)
from stock_a.research.entry_context.sessions import _load_calendar, _parse_target
from stock_a.research.entry_core.single_stock import (
    apply_identity_observation,
    research_stock,
)
from stock_a.research.entry_context.source_discovery import _normalize_symbol
from stock_a.research.entry_history.support import (
    classify_source_support_change,
    research_stock_pg_support_followup_preview,
    select_source_support_change_page,
)
from stock_a.research.entry_core.universe import research_universe_preview
from stock_a.research.entry_history.universe_history import (
    research_universe_pg_history_preview,
)

_IMPLEMENTATION_MODULES = (
    "stock_a.research.entry_core.comparison",
    "stock_a.research.entry_core.fields",
    "stock_a.research.entry_context.errors",
    "stock_a.research.entry_context.windows",
    "stock_a.research.entry_history.history_preview",
    "stock_a.research.entry_history.history_timeline",
    "stock_a.research.entry_core.profile_preview",
    "stock_a.research.entry_context.sessions",
    "stock_a.research.entry_core.single_stock",
    "stock_a.research.entry_context.source_discovery",
    "stock_a.research.entry_history.support",
    "stock_a.research.entry_core.universe",
    "stock_a.research.entry_history.universe_history",
)


def implementation_source_sha256() -> str:
    """返回兼容研究入口及其全部研究实现模块的稳定源码摘要。"""
    digest = sha256()
    paths = [Path(__file__).resolve(strict=True)]
    for module_name in _IMPLEMENTATION_MODULES:
        module = __import__(module_name, fromlist=["__file__"])
        paths.append(Path(module.__file__).resolve(strict=True))
    for path in paths:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


__all__ = [
    "CompareBudget",
    "ResearchEntryError",
    "UniverseDiscoveryBudget",
    "_load_calendar",
    "_normalize_symbol",
    "_parse_target",
    "apply_identity_observation",
    "classify_source_support_change",
    "implementation_source_sha256",
    "research_compare",
    "research_stock",
    "research_stock_pg_history_preview",
    "research_stock_pg_history_timeline",
    "research_stock_pg_support_followup_preview",
    "research_universe_pg_history_preview",
    "research_universe_preview",
    "select_source_support_change_page",
]
