"""Stock A研究entry_errors职责模块。"""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


class ResearchEntryError(ValueError):
    """研究入口输入或已声明能力不可用。"""

    def __init__(
        self, code: str, message: str, *, context: dict[str, Any] | None = None
    ):
        super().__init__(message)
        self.code = code
        self.context = dict(context or {})
