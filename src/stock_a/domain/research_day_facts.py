"""单日D0/D1持久化投影使用的显式业务记录。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal


D0State = Literal["TRUE", "FALSE", "UNKNOWN"]
D1GenerationState = Literal["GENERATED", "NOT_GENERATED", "UNKNOWN"]


@dataclass(frozen=True, slots=True)
class ExactRational:
    """用整数分子、正整数分母保存算法坐标，不作浮点舍入。"""

    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        if type(self.numerator) is not int or type(self.denominator) is not int:
            raise TypeError("精确坐标必须使用整数分子和分母")
        if self.denominator <= 0:
            raise ValueError("精确坐标分母必须为正")


@dataclass(frozen=True, slots=True)
class D0LaunchFact:
    """同一来源股票前后会话的现役D0启动三态与解释。"""

    source_symbol: str
    market: str
    before_state: D0State | None
    before_reason: str | None
    after_state: D0State | None
    after_reason: str | None
    transition: str


@dataclass(frozen=True, slots=True)
class D1StructureFact:
    """逐股、逐结构类型的D1生成状态与历史锚分母。"""

    source_symbol: str
    market: str
    board: str
    structure_type: str
    generation_state: D1GenerationState
    generation_reason: str | None
    history_row_count: int
    target_bar_status: str
    target_bar_reason: str | None
    target_price_quality: str
    target_price_reason: str | None
    historical_anchor_count: int
    material_change_anchor_count: int
    count_only_anchor_count: int
    unchanged_anchor_count: int
    unknown_anchor_count: int
    not_comparable_anchor_count: int
    not_evaluable_anchor_count: int
    not_applicable_anchor_count: int
    persisted_detail_count: int
    anchor_id: str | None = None
    anchor_created_session: date | None = None
    anchor_available_from_session: date | None = None
    anchor_test_from_session: date | None = None
    coordinate_id: str | None = None
    relation: str | None = None
    lower: ExactRational | None = None
    upper: ExactRational | None = None
    source_event_id: str | None = None
    source_ref: str | None = None


@dataclass(frozen=True, slots=True)
class D1MaterialAnchorFact:
    """现役D1材料变化或质量锚；只含有实际消费者的类型化字段。"""

    source_symbol: str
    structure_type: str
    anchor_id: str
    relation: str
    created_session: date
    previous_session: date
    coordinate_id: str | None
    lower: ExactRational | None
    upper: ExactRational | None
    birth_status: str
    birth_capture_id: str | None
    birth_entry_sha256: str | None
    previous_capture_id: str | None
    previous_entry_sha256: str | None
    target_entry_sha256: str | None
    official_sessions_adjacent: bool
    anchor_birth_before_previous_session: bool
    change_classification: str
    material_change_classification: str
    support_change_classification: str
    gap_fill_classification: str
    change_reasons: tuple[str, ...]
    material_change_reasons: tuple[str, ...]
    quality_reasons: tuple[str, ...]
    before_state: str | None
    before_quality: str | None
    before_acceptance: str | None
    before_freshness: str | None
    before_breach_streak: int | None
    before_acceptance_streak: int | None
    before_test_count: int | None
    before_separated_sessions: int | None
    before_retest_qualified: bool | None
    after_state: str | None
    after_quality: str | None
    after_acceptance: str | None
    after_freshness: str | None
    after_breach_streak: int | None
    after_acceptance_streak: int | None
    after_test_count: int | None
    after_separated_sessions: int | None
    after_retest_qualified: bool | None
    gap_before_state: str | None
    gap_before_quality: str | None
    gap_before_reason: str | None
    gap_after_state: str | None
    gap_after_quality: str | None
    gap_after_reason: str | None


@dataclass(frozen=True, slots=True)
class ResearchDayFacts:
    """完整的单日生产者输出和全部typed结果行。"""

    namespace_id: str
    before_session: date
    target_session: date
    package_sha256: str
    source_content_id: str
    before_capture_id: str
    target_capture_id: str
    source_time_quality: str
    source_clock_role: str
    source_available_at: str | None
    calendar_id: str
    calendar_revision: str
    calendar_sha256: str
    d0_before_window_start: date
    d0_target_window_start: date
    d0_before_source_count: int
    d0_target_source_count: int
    d0_union_source_count: int
    d1_history_session_count: int
    d1_history_window_start: date
    d1_history_window_end: date
    d1_source_count: int
    d0_producer_version: str
    d0_algorithm_contract: str
    d0_session_contract: str
    d1_producer_contract: str
    d1_producer_version: str
    d1_material_change_contract: str
    d0_facts: tuple[D0LaunchFact, ...]
    d1_structure_facts: tuple[D1StructureFact, ...]
    d1_anchor_facts: tuple[D1MaterialAnchorFact, ...]
