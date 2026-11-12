"""单股当前TDX行业与概念成员关系只读查询。"""

from __future__ import annotations

from datetime import date
import re
from typing import Any

from stock_a.storage.postgres.tdx_sector_members import (
    TdxSectorMembersPgError,
    query_tdx_sector_symbol_membership,
)


class SourceTdxSectorMembershipError(ValueError):
    """单股TDX成员关系应用查询拒绝无效身份或来源状态。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


_SOURCE_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")


def read_tdx_sector_symbol_membership(
    config: dict[str, Any],
    *,
    source_symbol: str,
    target_session: date,
    industry_snapshot_id: int,
    concept_snapshot_id: int,
) -> dict[str, Any]:
    """读取精确来源股票在显式行业和概念快照中的当前成员归属。"""
    if not isinstance(source_symbol, str):
        raise SourceTdxSectorMembershipError(
            "SOURCE_SYMBOL_INVALID", "必须提供精确SH/SZ来源股票代码"
        )
    match = _SOURCE_SYMBOL.fullmatch(source_symbol)
    if match is None:
        raise SourceTdxSectorMembershipError(
            "SOURCE_SYMBOL_INVALID", "来源股票代码格式必须为SH.六位代码或SZ.六位代码"
        )
    try:
        return query_tdx_sector_symbol_membership(
            config,
            exchange=match.group(1),
            source_symbol=source_symbol,
            target_session=target_session,
            industry_snapshot_id=industry_snapshot_id,
            concept_snapshot_id=concept_snapshot_id,
        )
    except TdxSectorMembersPgError as error:
        raise SourceTdxSectorMembershipError(error.code, str(error)) from error


__all__ = [
    "SourceTdxSectorMembershipError",
    "read_tdx_sector_symbol_membership",
]
