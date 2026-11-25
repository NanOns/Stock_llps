"""R03 正式市场、板块、轮动、LOO 与 Overlap 离线协调器入口。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any

from stock_a.research.sector_native import (
    compute_sector_loo_and_support,
    compute_sector_native_day,
)
from stock_a.research.sector_overlap import compute_sector_overlap
from stock_a.storage.postgres.r03_formal_sector import (
    write_r03_loo_support_fact,
    write_r03_native_day_facts,
    write_r03_overlap_clusters,
)

R03_CONTRACT_VERSION = "R03_FORMAL_SECTOR_V1"


def run_r03_formal_sector_day(
    config: Mapping[str, Any],
    *,
    trade_date: date,
    scope_key: str,
    member_version: str,
    boards_t: Sequence[Mapping[str, Any]],
    boards_t1: Mapping[str, Mapping[str, Any]],
    boards_t3: Mapping[str, Mapping[str, Any]],
    previous_states: Mapping[str, Mapping[str, Any]] | None = None,
    previous_clusters: Sequence[Mapping[str, Any]] | None = None,
    loo_targets: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """执行单日 R03 正式 Native、资格、前态、风险、Priority、Overlap 与 LOO 离线生产并幂等落库。"""
    native_rows = compute_sector_native_day(
        boards_t=boards_t,
        boards_t1=boards_t1,
        boards_t3=boards_t3,
        previous_states=previous_states,
    )
    native_pg = write_r03_native_day_facts(
        config,
        trade_date=trade_date,
        scope_key=scope_key,
        member_version=member_version,
        contract_version=R03_CONTRACT_VERSION,
        rows=native_rows,
    )

    priority_rows = [r["priority"] for r in native_rows]
    overlap = compute_sector_overlap(
        boards=native_rows,
        priority_rows=priority_rows,
        previous_clusters=previous_clusters,
    )
    overlap_pg = write_r03_overlap_clusters(
        config,
        trade_date=trade_date,
        scope_key=scope_key,
        contract_version=R03_CONTRACT_VERSION,
        clusters=overlap["clusters"],
    )

    loo_written = 0
    for target in loo_targets or ():
        loo_res = compute_sector_loo_and_support(
            target_entity_id=str(target["entity_id"]),
            boards_t=boards_t,
            boards_t1=boards_t1,
            boards_t3=boards_t3,
            primary_industry_id=target.get("primary_industry_id"),
        )
        if write_r03_loo_support_fact(
            config,
            trade_date=trade_date,
            board_kind=str(target.get("board_kind", "CONCEPT")),
            scope_key=scope_key,
            contract_version=R03_CONTRACT_VERSION,
            loo_result=loo_res,
        ):
            loo_written += 1

    return {
        "trade_date": trade_date.isoformat(),
        "scope_key": scope_key,
        "contract_version": R03_CONTRACT_VERSION,
        "native_count": len(native_rows),
        "native_pg": native_pg,
        "overlap_clusters_count": len(overlap["clusters"]),
        "overlap_pg": overlap_pg,
        "loo_written": loo_written,
    }
