"""领域共享类型的稳定轻量导出。"""

from stock_a.domain.types import ContractError, FieldValue, exact_number, unknown
from stock_a.domain.tri_state import (
    reduce_seed_with_safety,
    reduce_source_seed_shape,
    tri_and,
    tri_or,
)

__all__ = [
    "ContractError",
    "FieldValue",
    "exact_number",
    "reduce_seed_with_safety",
    "reduce_source_seed_shape",
    "tri_and",
    "tri_or",
    "unknown",
]
