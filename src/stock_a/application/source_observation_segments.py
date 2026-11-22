"""来源级登记跨官方会话观察段的离线应用生产链。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres.source_observation_segments import (
    persist_source_observation_segments as _persist_source_observation_segments,
)


def produce_source_observation_segments(
    config: Mapping[str, Any],
    *,
    research_cutoff: date,
    source_capture_id: int,
    rule_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """只从已存登记和规则事实生产逐官方会话typed观察段。"""
    return _persist_source_observation_segments(
        config,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        rule_ids=rule_ids,
    )
