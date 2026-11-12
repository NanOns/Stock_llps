"""TDX研究的稳定Python入口；领域实现各由下列单一研究模块负责。"""

from __future__ import annotations

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

__all__ = [
    "CompareBudget",
    "ResearchEntryError",
    "UniverseDiscoveryBudget",
    "_load_calendar",
    "_normalize_symbol",
    "_parse_target",
    "apply_identity_observation",
    "classify_source_support_change",
    "research_compare",
    "research_stock",
    "research_stock_pg_history_preview",
    "research_stock_pg_history_timeline",
    "research_stock_pg_support_followup_preview",
    "research_universe_pg_history_preview",
    "research_universe_preview",
    "select_source_support_change_page",
]
