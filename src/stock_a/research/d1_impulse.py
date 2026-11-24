"""MATH:ALG-29 + §10.4 D1 中阳线支撑锚点（IMPULSE_BODY / IMPULSE_LOW）规范适配入口。

严格遵守合同：
- 使用前态 ATR20[t-1]（Aprev）在当日 QFQ 坐标下的尺度，严禁用当日 ATR20 抬高门槛；
- 要求 Aprev > 0、C - O >= Aprev、H - L >= Aprev、CLV >= 0.7、amount_ratio20 >= 1.2、rel_market_1 > 0；
- IMPULSE_BODY 区间为 [O, (O + C) / 2]，IMPULSE_LOW 区间为 [L, L]，最早于 t+1 会话测试。
"""

from __future__ import annotations

from typing import Any

from stock_a.domain.types import ContractError, FieldValue
from stock_a.research.structure import Bar, generate


def evaluate_d1_impulse_anchors(
    *,
    entity_id: str,
    ordinal: int,
    coordinate_id: str,
    source_ref: str,
    window_ref: str,
    actual: FieldValue,
    today: Bar,
    atr20_previous: FieldValue,
    clv: FieldValue,
    amount_ratio20: FieldValue,
    rel_market_1: FieldValue,
    capability_state: str = "READY",
) -> dict[str, Any]:
    """基于前态 ATR20[t-1] 与全市场 rel_market_1 复用 structure.generate 生成中阳线双支撑锚。"""
    if not isinstance(atr20_previous, FieldValue) or not isinstance(
        rel_market_1, FieldValue
    ):
        raise ContractError(
            "D1中阳锚必须显式传入FieldValue类型的前态ATR20与rel_market_1"
        )
    inputs = {
        "atr_previous": atr20_previous,
        "clv": clv,
        "amount_ratio20": amount_ratio20,
        "rel_market_1": rel_market_1,
    }
    body_res = generate(
        "IMPULSE_BODY",
        entity_id=entity_id,
        ordinal=ordinal,
        coordinate_id=coordinate_id,
        source_ref=source_ref,
        window_ref=window_ref,
        actual=actual,
        today=today,
        inputs=inputs,
        capability_state=capability_state,
    )
    low_res = generate(
        "IMPULSE_LOW",
        entity_id=entity_id,
        ordinal=ordinal,
        coordinate_id=coordinate_id,
        source_ref=source_ref,
        window_ref=window_ref,
        actual=actual,
        today=today,
        inputs=inputs,
        capability_state=capability_state,
    )
    return {
        "IMPULSE_BODY": body_res,
        "IMPULSE_LOW": low_res,
    }
