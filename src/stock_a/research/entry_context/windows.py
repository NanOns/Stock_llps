"""Stock A研究entry_windows职责模块。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math

from stock_a.research.entry_context.errors import ResearchEntryError

_MAX_COMPARE_SYMBOLS = 20
_MAX_UNIVERSE_PAGE_SIZE = 100
_DEFAULT_UNIVERSE_PAGE_SIZE = 20
_DEFAULT_MAX_DIRECTORY_ENTRIES = 50000
_DEFAULT_MAX_DISCOVERY_SECONDS = 30.0
_DEFAULT_MAX_TOTAL_BYTES = 335544320
_DEFAULT_MAX_TOTAL_RECORDS = 4000000
_DEFAULT_MAX_TOTAL_SECONDS = 120.0


@dataclass(frozen=True)
class CompareBudget:
    """多股票单次调用的累计读取预算。"""

    max_total_bytes: int = _DEFAULT_MAX_TOTAL_BYTES
    max_total_records: int = _DEFAULT_MAX_TOTAL_RECORDS
    max_total_seconds: float = _DEFAULT_MAX_TOTAL_SECONDS

    def __post_init__(self) -> None:
        if type(self.max_total_bytes) is not int or self.max_total_bytes <= 0:
            raise ResearchEntryError(
                "COMPARE_BUDGET_INVALID", "全批最大字节数必须为正整数"
            )
        if type(self.max_total_records) is not int or self.max_total_records <= 0:
            raise ResearchEntryError(
                "COMPARE_BUDGET_INVALID", "全批最大记录数必须为正整数"
            )
        if isinstance(self.max_total_seconds, bool) or not isinstance(
            self.max_total_seconds, (int, float)
        ):
            raise ResearchEntryError(
                "COMPARE_BUDGET_INVALID", "全批最大耗时必须为有限正数"
            )
        try:
            seconds = float(self.max_total_seconds)
        except (OverflowError, TypeError, ValueError) as error:
            raise ResearchEntryError(
                "COMPARE_BUDGET_INVALID", "全批最大耗时必须可表示为有限正数"
            ) from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ResearchEntryError(
                "COMPARE_BUDGET_INVALID", "全批最大耗时必须为有限正数"
            )
        object.__setattr__(self, "max_total_seconds", seconds)


@dataclass(frozen=True)
class UniverseDiscoveryBudget:
    """发现来源候选目录时的总目录项与耗时上限。"""

    max_directory_entries: int = _DEFAULT_MAX_DIRECTORY_ENTRIES
    max_seconds: float = _DEFAULT_MAX_DISCOVERY_SECONDS

    def __post_init__(self) -> None:
        if (
            type(self.max_directory_entries) is not int
            or self.max_directory_entries <= 0
        ):
            raise ResearchEntryError(
                "UNIVERSE_BUDGET_INVALID", "来源目录项上限必须为正整数"
            )
        if isinstance(self.max_seconds, bool) or not isinstance(
            self.max_seconds, (int, float)
        ):
            raise ResearchEntryError(
                "UNIVERSE_BUDGET_INVALID", "来源发现耗时必须为有限正数"
            )
        try:
            seconds = float(self.max_seconds)
        except (OverflowError, TypeError, ValueError) as error:
            raise ResearchEntryError(
                "UNIVERSE_BUDGET_INVALID", "来源发现耗时必须可表示为有限正数"
            ) from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ResearchEntryError(
                "UNIVERSE_BUDGET_INVALID", "来源发现耗时必须为有限正数"
            )
        object.__setattr__(self, "max_seconds", seconds)
