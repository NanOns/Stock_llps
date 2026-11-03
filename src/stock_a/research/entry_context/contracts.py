"""Stock A研究entry_contracts职责模块。"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class FoundationTypedFieldHandoff:
    """Foundation单字段的值、质量、单位、窗口和消费者交接。"""

    field_id: str
    value: Any
    value_type: str
    unit: str | None
    quality: str
    reason: str | None
    status: str
    producer_symbol: str | None
    intended_producer_symbol: str | None
    input_fields: tuple[str, ...]
    consumers: tuple[str, ...]
    window_contract: str | None
    window_identity_ref: str | None
    window: Mapping[str, Any]
    unit_authority: str | None = None


@dataclass(frozen=True)
class FoundationTypedHandoff:
    """同一目标日、同一来源版本下可供typed适配器审查的只读字段边界。"""

    contract: str
    source_symbol: str
    target_session: str
    source_package_id: str | None
    source_package_sha256: str | None
    source_entry_sha256: str | None
    source_byte_sha256: str | None
    source_clock_role: str
    published_at: str
    first_available_at: str
    price_coordinate: str
    research_mode: str
    trusted_foundation_selector_bound: bool
    identity_status: str
    stable_security_id_issued: bool
    target_bar_status: str
    research_price_status: str
    quality_groups: Mapping[str, Mapping[str, Any]]
    typed_pg_admission_status: str
    typed_pg_admission_reasons: tuple[str, ...]
    typed_pg_admission_reason_explanations: Mapping[str, str]
    foundation_admission_status: str
    foundation_admission_reasons: tuple[str, ...]
    foundation_admission_reason_explanations: Mapping[str, str]
    fields: tuple[FoundationTypedFieldHandoff, ...]
    consumer_edges: Mapping[str, tuple[str, ...]]
    insert_plan_generated: bool = False
    persistence_attempted: bool = False
