"""现役D0/D1单日typed研究事实的PG写入和只读读取。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
import random
import re
import time
from typing import Any, Mapping, Sequence
from uuid import UUID

from stock_a.domain.research_day_facts import (
    D0LaunchFact,
    D1MaterialAnchorFact,
    D1StructureFact,
    ResearchDayFacts,
)
from stock_a.storage.postgres import source_raw


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_STRUCTURE_TYPES = (
    "PIVOT_LOW",
    "PIVOT_HIGH",
    "RANGE_FLOOR",
    "RANGE_CEILING",
    "GAP_ZONE",
)
_STRUCTURE_TYPE_SET = frozenset(_STRUCTURE_TYPES)
_D0_COLUMNS = (
    "namespace_id",
    "before_session",
    "target_session",
    "source_symbol",
    "market",
    "package_sha256",
    "source_content_id",
    "before_capture_id",
    "target_capture_id",
    "producer_version",
    "algorithm_contract",
    "session_contract",
    "before_window_start",
    "target_window_start",
    "before_source_count",
    "target_source_count",
    "union_source_count",
    "before_state",
    "before_reason",
    "after_state",
    "after_reason",
    "transition",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
    "research_mode",
    "price_coordinate",
)
_D1_COLUMNS = (
    "namespace_id",
    "before_session",
    "target_session",
    "source_symbol",
    "market",
    "board",
    "structure_type",
    "package_sha256",
    "source_content_id",
    "target_capture_id",
    "d1_history_session_count",
    "history_window_start",
    "history_window_end",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
    "producer_contract",
    "producer_version",
    "material_change_contract",
    "source_time_quality",
    "source_clock_role",
    "source_available_at",
    "research_mode",
    "price_coordinate",
    "history_row_count",
    "target_bar_status",
    "target_bar_reason",
    "target_price_quality",
    "target_price_reason",
    "generation_state",
    "generation_reason",
    "anchor_id",
    "anchor_created_session",
    "anchor_available_from_session",
    "anchor_test_from_session",
    "coordinate_id",
    "relation",
    "lower_numerator",
    "lower_denominator",
    "upper_numerator",
    "upper_denominator",
    "source_event_id",
    "source_ref",
    "historical_anchor_count",
    "material_change_anchor_count",
    "count_only_anchor_count",
    "unchanged_anchor_count",
    "unknown_anchor_count",
    "not_comparable_anchor_count",
    "not_evaluable_anchor_count",
    "not_applicable_anchor_count",
    "persisted_detail_count",
)
_ANCHOR_COLUMNS = (
    "namespace_id",
    "before_session",
    "target_session",
    "source_symbol",
    "structure_type",
    "package_sha256",
    "target_capture_id",
    "d1_history_session_count",
    "calendar_revision",
    "calendar_sha256",
    "producer_contract",
    "producer_version",
    "material_change_contract",
    "anchor_id",
    "created_session",
    "previous_session",
    "coordinate_id",
    "relation",
    "lower_numerator",
    "lower_denominator",
    "upper_numerator",
    "upper_denominator",
    "birth_status",
    "birth_capture_id",
    "birth_entry_sha256",
    "previous_capture_id",
    "previous_entry_sha256",
    "target_entry_sha256",
    "official_sessions_adjacent",
    "anchor_birth_before_previous_session",
    "change_classification",
    "material_change_classification",
    "support_change_classification",
    "gap_fill_classification",
    "change_reasons",
    "material_change_reasons",
    "quality_reasons",
    "before_state",
    "before_quality",
    "before_acceptance",
    "before_freshness",
    "before_breach_streak",
    "before_acceptance_streak",
    "before_test_count",
    "before_separated_sessions",
    "before_retest_qualified",
    "after_state",
    "after_quality",
    "after_acceptance",
    "after_freshness",
    "after_breach_streak",
    "after_acceptance_streak",
    "after_test_count",
    "after_separated_sessions",
    "after_retest_qualified",
    "gap_before_state",
    "gap_before_quality",
    "gap_before_reason",
    "gap_after_state",
    "gap_after_quality",
    "gap_after_reason",
)
_D0_KEY = (
    "namespace_id",
    "target_session",
    "source_symbol",
    "package_sha256",
    "before_capture_id",
    "target_capture_id",
    "producer_version",
    "algorithm_contract",
    "session_contract",
    "calendar_revision",
    "calendar_sha256",
)
_D1_KEY = (
    "namespace_id",
    "target_session",
    "source_symbol",
    "structure_type",
    "package_sha256",
    "target_capture_id",
    "d1_history_session_count",
    "calendar_revision",
    "calendar_sha256",
    "producer_contract",
    "producer_version",
    "material_change_contract",
)
_ANCHOR_KEY = _D1_KEY + ("anchor_id", "created_session")
_TABLES = (
    ("stock.source_d0_launch_day_facts", _D0_COLUMNS, _D0_KEY),
    ("stock.source_d1_structure_day_facts", _D1_COLUMNS, _D1_KEY),
    ("stock.source_d1_material_anchor_day_facts", _ANCHOR_COLUMNS, _ANCHOR_KEY),
)


class ResearchDayPgError(ValueError):
    """单日研究事实存储缺迁移、来源不一致或已存在内容冲突。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _available_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as error:
        raise ResearchDayPgError(
            "SOURCE_AVAILABLE_AT_INVALID", "来源可用时间格式无效"
        ) from error
    if parsed.tzinfo is None:
        raise ResearchDayPgError(
            "SOURCE_AVAILABLE_AT_TIMEZONE_MISSING", "来源可用时间缺少时区"
        )
    return parsed


def _ratio_pair(value: Any) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    rational = Fraction(value.numerator, value.denominator)
    return rational.numerator, rational.denominator


def _d0_rows(facts: ResearchDayFacts) -> list[tuple[Any, ...]]:
    result = []
    for item in facts.d0_facts:
        result.append(
            (
                facts.namespace_id,
                facts.before_session,
                facts.target_session,
                item.source_symbol,
                item.market,
                facts.package_sha256,
                facts.source_content_id,
                facts.before_capture_id,
                facts.target_capture_id,
                facts.d0_producer_version,
                facts.d0_algorithm_contract,
                facts.d0_session_contract,
                facts.d0_before_window_start,
                facts.d0_target_window_start,
                facts.d0_before_source_count,
                facts.d0_target_source_count,
                facts.d0_union_source_count,
                item.before_state,
                item.before_reason,
                item.after_state,
                item.after_reason,
                item.transition,
                facts.source_time_quality,
                facts.source_clock_role,
                _available_at(facts.source_available_at),
                facts.calendar_id,
                facts.calendar_revision,
                facts.calendar_sha256,
                "NON_PIT_RETROSPECTIVE",
                "RAW_UNADJUSTED",
            )
        )
    return result


def _d1_rows(facts: ResearchDayFacts) -> list[tuple[Any, ...]]:
    result = []
    for item in facts.d1_structure_facts:
        lower_num, lower_den = _ratio_pair(item.lower)
        upper_num, upper_den = _ratio_pair(item.upper)
        result.append(
            (
                facts.namespace_id,
                facts.before_session,
                facts.target_session,
                item.source_symbol,
                item.market,
                item.board,
                item.structure_type,
                facts.package_sha256,
                facts.source_content_id,
                facts.target_capture_id,
                facts.d1_history_session_count,
                facts.d1_history_window_start,
                facts.d1_history_window_end,
                facts.calendar_id,
                facts.calendar_revision,
                facts.calendar_sha256,
                facts.d1_producer_contract,
                facts.d1_producer_version,
                facts.d1_material_change_contract,
                facts.source_time_quality,
                facts.source_clock_role,
                _available_at(facts.source_available_at),
                "NON_PIT_RETROSPECTIVE",
                "RAW_UNADJUSTED",
                item.history_row_count,
                item.target_bar_status,
                item.target_bar_reason,
                item.target_price_quality,
                item.target_price_reason,
                item.generation_state,
                item.generation_reason,
                item.anchor_id,
                item.anchor_created_session,
                item.anchor_available_from_session,
                item.anchor_test_from_session,
                item.coordinate_id,
                item.relation,
                lower_num,
                lower_den,
                upper_num,
                upper_den,
                item.source_event_id,
                item.source_ref,
                item.historical_anchor_count,
                item.material_change_anchor_count,
                item.count_only_anchor_count,
                item.unchanged_anchor_count,
                item.unknown_anchor_count,
                item.not_comparable_anchor_count,
                item.not_evaluable_anchor_count,
                item.not_applicable_anchor_count,
                item.persisted_detail_count,
            )
        )
    return result


def _anchor_rows(facts: ResearchDayFacts) -> list[tuple[Any, ...]]:
    result = []
    for item in facts.d1_anchor_facts:
        lower_num, lower_den = _ratio_pair(item.lower)
        upper_num, upper_den = _ratio_pair(item.upper)
        result.append(
            (
                facts.namespace_id,
                facts.before_session,
                facts.target_session,
                item.source_symbol,
                item.structure_type,
                facts.package_sha256,
                facts.target_capture_id,
                facts.d1_history_session_count,
                facts.calendar_revision,
                facts.calendar_sha256,
                facts.d1_producer_contract,
                facts.d1_producer_version,
                facts.d1_material_change_contract,
                item.anchor_id,
                item.created_session,
                item.previous_session,
                item.coordinate_id,
                item.relation,
                lower_num,
                lower_den,
                upper_num,
                upper_den,
                item.birth_status,
                item.birth_capture_id,
                item.birth_entry_sha256,
                item.previous_capture_id,
                item.previous_entry_sha256,
                item.target_entry_sha256,
                item.official_sessions_adjacent,
                item.anchor_birth_before_previous_session,
                item.change_classification,
                item.material_change_classification,
                item.support_change_classification,
                item.gap_fill_classification,
                list(item.change_reasons),
                list(item.material_change_reasons),
                list(item.quality_reasons),
                item.before_state,
                item.before_quality,
                item.before_acceptance,
                item.before_freshness,
                item.before_breach_streak,
                item.before_acceptance_streak,
                item.before_test_count,
                item.before_separated_sessions,
                item.before_retest_qualified,
                item.after_state,
                item.after_quality,
                item.after_acceptance,
                item.after_freshness,
                item.after_breach_streak,
                item.after_acceptance_streak,
                item.after_test_count,
                item.after_separated_sessions,
                item.after_retest_qualified,
                item.gap_before_state,
                item.gap_before_quality,
                item.gap_before_reason,
                item.gap_after_state,
                item.gap_after_quality,
                item.gap_after_reason,
            )
        )
    return result


def _validate_facts(facts: ResearchDayFacts) -> None:
    if not isinstance(facts, ResearchDayFacts):
        raise ResearchDayPgError("RESEARCH_DAY_FACTS_INVALID", "单日事实类型无效")
    if (
        facts.target_session <= facts.before_session
        or facts.d0_before_source_count < 0
        or facts.d0_target_source_count < 1
        or facts.d0_union_source_count != len(facts.d0_facts)
        or facts.d1_source_count < 1
        or facts.d1_history_session_count < 1
        or facts.d1_history_window_end != facts.target_session
        or facts.d1_history_window_start > facts.before_session
        or _SHA256.fullmatch(facts.package_sha256) is None
        or _SHA256.fullmatch(facts.source_content_id) is None
        or _SHA256.fullmatch(facts.calendar_sha256) is None
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_FACTS_INCOMPLETE", "单日来源、日历或窗口分母无效"
        )
    d0_by_symbol = {item.source_symbol: item for item in facts.d0_facts}
    d1_by_key = {
        (item.source_symbol, item.structure_type): item
        for item in facts.d1_structure_facts
    }
    if len(d0_by_symbol) != len(facts.d0_facts) or len(d1_by_key) != len(
        facts.d1_structure_facts
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_FACTS_DUPLICATED_KEY", "D0或D1事实包含重复业务键"
        )
    before_symbols = set()
    target_symbols = set()
    for item in facts.d0_facts:
        if (
            _SYMBOL.fullmatch(item.source_symbol) is None
            or item.market not in {"SH", "SZ"}
            or item.source_symbol[:2] != item.market
            or item.before_state not in {None, "TRUE", "FALSE", "UNKNOWN"}
            or item.after_state not in {None, "TRUE", "FALSE", "UNKNOWN"}
            or (item.before_state is None and item.after_state is None)
            or not item.transition
        ):
            raise ResearchDayPgError(
                "RESEARCH_DAY_D0_FACT_INVALID", "D0三态、来源代码或转移原因无效"
            )
        if item.before_state is not None:
            before_symbols.add(item.source_symbol)
        if item.after_state is not None:
            target_symbols.add(item.source_symbol)
    if (
        len(before_symbols) != facts.d0_before_source_count
        or len(target_symbols) != facts.d0_target_source_count
        or len(facts.d0_facts) != facts.d0_union_source_count
        or target_symbols != {item.source_symbol for item in facts.d1_structure_facts}
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_D0_PARTITION_OPEN", "D0三态事实与前后总体分母未闭合"
        )
    structures_by_symbol: dict[str, set[str]] = {}
    for item in facts.d1_structure_facts:
        key = (item.source_symbol, item.structure_type)
        structures_by_symbol.setdefault(item.source_symbol, set()).add(
            item.structure_type
        )
        status_total = sum(
            (
                item.material_change_anchor_count,
                item.count_only_anchor_count,
                item.unchanged_anchor_count,
                item.unknown_anchor_count,
                item.not_comparable_anchor_count,
                item.not_evaluable_anchor_count,
                item.not_applicable_anchor_count,
            )
        )
        if (
            _SYMBOL.fullmatch(item.source_symbol) is None
            or item.market not in {"SH", "SZ"}
            or item.source_symbol[:2] != item.market
            or item.board not in {"MAIN_BOARD", "GEM"}
            or item.structure_type not in _STRUCTURE_TYPE_SET
            or item.generation_state not in {"GENERATED", "NOT_GENERATED", "UNKNOWN"}
            or item.history_row_count < 1
            or item.historical_anchor_count < 0
            or status_total != item.historical_anchor_count
            or item.persisted_detail_count < 0
        ):
            raise ResearchDayPgError(
                "RESEARCH_DAY_D1_FACT_INVALID", "D1结构、质量或分母字段无效"
            )
    if (
        len(facts.d1_structure_facts) != facts.d1_source_count * len(_STRUCTURE_TYPES)
        or any(
            set(kinds) != _STRUCTURE_TYPE_SET for kinds in structures_by_symbol.values()
        )
        or set(structures_by_symbol) != target_symbols
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_D1_PARTITION_OPEN", "D1逐股五类结构事实未完整闭合"
        )
    detail_counts: dict[tuple[str, str], int] = {}
    anchor_keys = set()
    for item in facts.d1_anchor_facts:
        key = (
            item.source_symbol,
            item.structure_type,
            item.anchor_id,
            item.created_session,
        )
        anchor_keys.add(key)
        owner = (item.source_symbol, item.structure_type)
        detail_counts[owner] = detail_counts.get(owner, 0) + 1
        if (
            owner not in d1_by_key
            or not item.anchor_id
            or item.created_session > facts.target_session
            or item.previous_session != facts.before_session
            or item.official_sessions_adjacent is not True
            or not item.material_change_classification
        ):
            raise ResearchDayPgError(
                "RESEARCH_DAY_D1_ANCHOR_INVALID", "D1材料或质量锚明细不完整"
            )
    if len(anchor_keys) != len(facts.d1_anchor_facts) or any(
        detail_counts.get(key, 0) != item.persisted_detail_count
        for key, item in d1_by_key.items()
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_D1_DETAIL_PARTITION_OPEN",
            "D1逐股结构分母与typed锚明细不一致",
        )


def _normalize_value(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else value
    if isinstance(value, list):
        return tuple(_normalize_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_normalize_value(item) for item in value)
    return value


def _row_matches(expected: Sequence[Any], actual: Sequence[Any]) -> bool:
    return len(expected) == len(actual) and tuple(
        map(_normalize_value, expected)
    ) == tuple(map(_normalize_value, actual))


def _sort_rows(
    rows: Sequence[Sequence[Any]], columns: Sequence[str], key: Sequence[str]
) -> list[tuple[Any, ...]]:
    positions = tuple(columns.index(name) for name in key)
    return sorted(
        (tuple(row) for row in rows), key=lambda row: tuple(row[i] for i in positions)
    )


def _scope_predicate(
    table_name: str, facts: ResearchDayFacts
) -> tuple[str, tuple[Any, ...]]:
    if table_name.endswith("source_d0_launch_day_facts"):
        return (
            "namespace_id=%s AND target_session=%s AND package_sha256=%s "
            "AND before_capture_id=%s AND target_capture_id=%s AND producer_version=%s "
            "AND algorithm_contract=%s AND session_contract=%s "
            "AND calendar_revision=%s AND calendar_sha256=%s",
            (
                facts.namespace_id,
                facts.target_session,
                facts.package_sha256,
                facts.before_capture_id,
                facts.target_capture_id,
                facts.d0_producer_version,
                facts.d0_algorithm_contract,
                facts.d0_session_contract,
                facts.calendar_revision,
                facts.calendar_sha256,
            ),
        )
    if table_name.endswith("source_d1_structure_day_facts"):
        return (
            "namespace_id=%s AND target_session=%s AND package_sha256=%s "
            "AND target_capture_id=%s AND d1_history_session_count=%s "
            "AND calendar_revision=%s AND calendar_sha256=%s "
            "AND producer_contract=%s AND producer_version=%s "
            "AND material_change_contract=%s",
            (
                facts.namespace_id,
                facts.target_session,
                facts.package_sha256,
                facts.target_capture_id,
                facts.d1_history_session_count,
                facts.calendar_revision,
                facts.calendar_sha256,
                facts.d1_producer_contract,
                facts.d1_producer_version,
                facts.d1_material_change_contract,
            ),
        )
    return (
        "namespace_id=%s AND target_session=%s AND package_sha256=%s "
        "AND target_capture_id=%s AND d1_history_session_count=%s "
        "AND calendar_revision=%s AND calendar_sha256=%s "
        "AND producer_contract=%s AND producer_version=%s "
        "AND material_change_contract=%s",
        (
            facts.namespace_id,
            facts.target_session,
            facts.package_sha256,
            facts.target_capture_id,
            facts.d1_history_session_count,
            facts.calendar_revision,
            facts.calendar_sha256,
            facts.d1_producer_contract,
            facts.d1_producer_version,
            facts.d1_material_change_contract,
        ),
    )


def _verify_raw_source(cursor: Any, facts: ResearchDayFacts) -> None:
    cursor.execute(
        """
        SELECT session_date, count(*), count(DISTINCT source_symbol),
               count(DISTINCT source_capture_id), min(source_capture_id::text),
               count(DISTINCT source_content_id), min(source_content_id)
        FROM stock.source_raw_daily
        WHERE namespace_id=%s AND package_sha256=%s
          AND session_date IN (%s, %s)
        GROUP BY session_date ORDER BY session_date
        """,
        (
            facts.namespace_id,
            facts.package_sha256,
            facts.before_session,
            facts.target_session,
        ),
    )
    observed = {row[0]: row[1:] for row in cursor.fetchall()}
    target = observed.get(facts.target_session)
    before = observed.get(facts.before_session)
    if (
        target is None
        or before is None
        or target[0] != facts.d1_source_count
        or target[1] != facts.d1_source_count
        or target[2] != 1
        or target[3] != facts.target_capture_id
        or target[4] != 1
        or target[5] != facts.source_content_id
        or before[1] != facts.d0_before_source_count
        or before[2] != 1
        or before[3] != facts.before_capture_id
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_RAW_IDENTITY_MISMATCH",
            "D0/D1事实与固定PG中的前后态Raw总体、捕获或内容摘要不一致",
        )
    cursor.execute(
        """
        SELECT session_date, source_symbol FROM stock.source_raw_daily
        WHERE namespace_id=%s AND package_sha256=%s
          AND session_date IN (%s, %s)
        ORDER BY session_date, source_symbol
        """,
        (
            facts.namespace_id,
            facts.package_sha256,
            facts.before_session,
            facts.target_session,
        ),
    )
    raw_symbols_by_session = {facts.before_session: set(), facts.target_session: set()}
    for session_date, source_symbol in cursor.fetchall():
        raw_symbols_by_session[session_date].add(source_symbol)
    raw_symbols = raw_symbols_by_session[facts.target_session]
    d1_symbols = {item.source_symbol for item in facts.d1_structure_facts}
    d0_target_symbols = {
        item.source_symbol for item in facts.d0_facts if item.after_state is not None
    }
    if raw_symbols != d1_symbols or raw_symbols != d0_target_symbols:
        raise ResearchDayPgError(
            "RESEARCH_DAY_RAW_SYMBOL_PARTITION_OPEN",
            "目标Raw、D0目标态和D1逐股集合不一致",
        )
    before_symbols = {
        item.source_symbol for item in facts.d0_facts if item.before_state is not None
    }
    fact_union_symbols = {item.source_symbol for item in facts.d0_facts}
    if (
        before_symbols != raw_symbols_by_session[facts.before_session]
        or len(before_symbols) != facts.d0_before_source_count
        or fact_union_symbols
        != raw_symbols_by_session[facts.before_session] | raw_symbols
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_BEFORE_PARTITION_OPEN", "D0前态来源分母未闭合到事实行"
        )


def _ensure_tables(cursor: Any) -> None:
    cursor.execute(
        "SELECT to_regclass('stock.source_d0_launch_day_facts') IS NOT NULL, "
        "to_regclass('stock.source_d1_structure_day_facts') IS NOT NULL, "
        "to_regclass('stock.source_d1_material_anchor_day_facts') IS NOT NULL"
    )
    if cursor.fetchone() != (True, True, True):
        raise ResearchDayPgError(
            "RESEARCH_DAY_MIGRATION_NOT_APPLIED",
            "R06前向迁移尚未应用，拒绝研究事实写入或已存读取",
        )


def _select_rows(
    cursor: Any,
    table: str,
    columns: Sequence[str],
    predicate: str,
    params: Sequence[Any],
) -> list[tuple[Any, ...]]:
    cursor.execute(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {predicate} ORDER BY "
        + ", ".join(
            _D0_KEY
            if table.endswith("source_d0_launch_day_facts")
            else _D1_KEY
            if table.endswith("source_d1_structure_day_facts")
            else _ANCHOR_KEY
        ),
        tuple(params),
    )
    return cursor.fetchall()


def _insert_rows(
    cursor: Any,
    table: str,
    columns: Sequence[str],
    key: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> int:
    if not rows:
        return 0
    placeholders = ", ".join(["%s"] * len(columns))
    conflict = ", ".join(key)
    inserted = 0
    # 参数数量有界；每一批仍属于同一个单日事务，完整读回后才提交。
    batch_size = min(100, 60000 // len(columns))
    for offset in range(0, len(rows), batch_size):
        batch = rows[offset : offset + batch_size]
        values = ", ".join(f"({placeholders})" for _ in batch)
        cursor.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values} "
            f"ON CONFLICT ({conflict}) DO NOTHING",
            tuple(value for row in batch for value in row),
        )
        inserted += cursor.rowcount
    return inserted


def persist_source_research_day(
    config: Mapping[str, Any], facts: ResearchDayFacts
) -> dict[str, Any]:
    """序列化冲突时重试完整单日事务。"""
    for attempt in range(5):
        try:
            return _persist_source_research_day_once(config, facts)
        except source_raw.RawSourcePgError as error:
            if getattr(error.__cause__, "sqlstate", None) != "40001" or attempt == 4:
                raise
            time.sleep(random.uniform(0.1, 0.3) * (2**attempt))
    raise AssertionError("不可达的研究事实事务重试状态")


def _persist_source_research_day_once(
    config: Mapping[str, Any], facts: ResearchDayFacts
) -> dict[str, Any]:
    """串行化事务写三张typed事实表；同键异内容拒绝并回滚。"""
    _validate_facts(facts)
    expected_rows = tuple(
        _sort_rows(rows, columns, key)
        for (table, columns, key), rows in zip(
            _TABLES,
            (_d0_rows(facts), _d1_rows(facts), _anchor_rows(facts)),
            strict=True,
        )
    )
    inserted = {}
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        namespace_id,
    ):
        if namespace_id != facts.namespace_id:
            raise ResearchDayPgError(
                "RESEARCH_DAY_NAMESPACE_MISMATCH", "写入事实与PG DEV命名空间不一致"
            )
        _ensure_tables(cursor)
        _verify_raw_source(cursor, facts)
        existing_scopes = []
        any_existing = False
        for (table, columns, _key), rows in zip(_TABLES, expected_rows, strict=True):
            predicate, params = _scope_predicate(table, facts)
            existing = _select_rows(cursor, table, columns, predicate, params)
            any_existing = any_existing or bool(existing)
            if existing and len(existing) != len(rows):
                raise ResearchDayPgError(
                    "RESEARCH_DAY_PARTIAL_EXISTING_SCOPE",
                    "PG中存在不完整的单日事实范围，拒绝覆盖或继续写入",
                )
            if existing and any(
                not _row_matches(expected, actual)
                for expected, actual in zip(rows, existing, strict=True)
            ):
                raise ResearchDayPgError(
                    "RESEARCH_DAY_CONTENT_CONFLICT",
                    "相同D0/D1业务键已存在不同typed事实，事务回滚",
                )
            existing_scopes.append(len(existing) == len(rows))
        if any_existing and not all(existing_scopes):
            raise ResearchDayPgError(
                "RESEARCH_DAY_CROSS_TABLE_PARTIAL_SCOPE",
                "三类事实表的既存范围不完整，拒绝跨表补写",
            )
        if not all(existing_scopes):
            for (table, columns, key), rows in zip(_TABLES, expected_rows, strict=True):
                inserted[table.rsplit(".", 1)[1]] = _insert_rows(
                    cursor, table, columns, key, rows
                )
        for (table, columns, _key), rows in zip(_TABLES, expected_rows, strict=True):
            predicate, params = _scope_predicate(table, facts)
            actual = _select_rows(cursor, table, columns, predicate, params)
            if len(actual) != len(rows) or any(
                not _row_matches(expected, persisted)
                for expected, persisted in zip(rows, actual, strict=True)
            ):
                raise ResearchDayPgError(
                    "RESEARCH_DAY_TYPED_READBACK_MISMATCH",
                    "事务内typed事实读回和预期不一致，拒绝提交",
                )
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if all(existing_scopes)
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": {
            table.rsplit(".", 1)[1]: len(rows)
            for (table, _columns, _key), rows in zip(
                _TABLES, expected_rows, strict=True
            )
        },
        "rows_inserted": inserted,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if all(existing_scopes)
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_THREE_TABLE_WRITE",
        "database_write_attempted": not all(existing_scopes),
        "ddl_executed": False,
        "dml_executed": not all(existing_scopes),
    }


def _json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _as_dict(columns: Sequence[str], row: Sequence[Any]) -> dict[str, Any]:
    return {
        column: _json_value(value) for column, value in zip(columns, row, strict=True)
    }


def read_source_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    d1_history_session_count: int,
    offset: int = 0,
    limit: int = 30,
    symbol: str | None = None,
    industry_snapshot_id: str | None = None,
    concept_snapshot_id: str | None = None,
    board_kind: str | None = None,
    board_native_id: str | None = None,
    structure_type: str | None = None,
    material_change_reason: str | None = None,
    include_all_results: bool = False,
) -> dict[str, Any]:
    """只读分页读取PG事实；未迁移或未计算时返回明确状态，不重算回退。"""
    if (
        type(target_session) is not date
        or not isinstance(package_sha256, str)
        or _SHA256.fullmatch(package_sha256) is None
        or type(d1_history_session_count) is not int
        or not 1 <= d1_history_session_count <= 32767
        or type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or type(include_all_results) is not bool
        or not 1 <= limit <= 200
        and not include_all_results
        or (symbol is not None and _SYMBOL.fullmatch(symbol) is None)
        or (board_kind is not None and board_kind not in {"INDUSTRY", "CONCEPT"})
        or (
            board_native_id is not None
            and (board_kind is None or not board_native_id.strip())
        )
        or (
            board_kind is not None
            and board_native_id is not None
            and not (
                industry_snapshot_id is not None
                if board_kind == "INDUSTRY"
                else concept_snapshot_id is not None
            )
        )
        or (structure_type is not None and structure_type not in _STRUCTURE_TYPE_SET)
        or (
            material_change_reason is not None
            and (not material_change_reason or len(material_change_reason) > 128)
        )
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_QUERY_INVALID", "PG研究事实读取键或分页参数无效"
        )

    def incomplete_result(
        namespace_id: str, reason: str, **details: Any
    ) -> dict[str, Any]:
        return {
            "storage_status": "INCOMPLETE",
            "namespace_id": namespace_id,
            "target_session": target_session.isoformat(),
            "package_sha256": package_sha256,
            "d1_history_session_count": d1_history_session_count,
            "results": [],
            "reason": reason,
            "integrity_details": details,
        }

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        namespace_id,
    ):
        cursor.execute(
            "SELECT to_regclass('stock.source_d0_launch_day_facts') IS NOT NULL, "
            "to_regclass('stock.source_d1_structure_day_facts') IS NOT NULL, "
            "to_regclass('stock.source_d1_material_anchor_day_facts') IS NOT NULL"
        )
        if cursor.fetchone() != (True, True, True):
            return {
                "storage_status": "NOT_COMPUTED",
                "namespace_id": namespace_id,
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "d1_history_session_count": d1_history_session_count,
                "results": [],
                "reason": "R06事实迁移尚未应用",
            }
        cursor.execute(
            "SELECT DISTINCT target_capture_id, calendar_revision, calendar_sha256, "
            "producer_contract, producer_version, material_change_contract "
            "FROM stock.source_d1_structure_day_facts "
            "WHERE namespace_id=%s AND target_session=%s AND package_sha256=%s "
            "AND d1_history_session_count=%s ORDER BY target_capture_id, calendar_revision, "
            "calendar_sha256, producer_contract, producer_version, material_change_contract",
            (namespace_id, target_session, package_sha256, d1_history_session_count),
        )
        d1_scopes = cursor.fetchall()
        if not d1_scopes:
            return {
                "storage_status": "NOT_COMPUTED",
                "namespace_id": namespace_id,
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "d1_history_session_count": d1_history_session_count,
                "results": [],
                "reason": "该精确来源与D1窗口尚无已保存研究事实",
            }
        if len(d1_scopes) != 1:
            raise ResearchDayPgError(
                "RESEARCH_DAY_SCOPE_AMBIGUOUS", "同一读取键对应多个目标捕获或生产版本"
            )
        (
            target_capture_id,
            calendar_revision,
            calendar_sha256,
            producer_contract,
            d1_producer_version,
            material_change_contract,
        ) = d1_scopes[0]
        d1_predicate = (
            "namespace_id=%s AND target_session=%s AND package_sha256=%s "
            "AND target_capture_id=%s AND d1_history_session_count=%s "
            "AND calendar_revision=%s AND calendar_sha256=%s "
            "AND producer_contract=%s AND producer_version=%s "
            "AND material_change_contract=%s"
        )
        d1_params = (
            namespace_id,
            target_session,
            package_sha256,
            target_capture_id,
            d1_history_session_count,
            calendar_revision,
            calendar_sha256,
            producer_contract,
            d1_producer_version,
            material_change_contract,
        )
        snapshot_specs = []
        for supplied_id, kind, source_kind in (
            (industry_snapshot_id, "INDUSTRY", "TDX_TDXHY_CFG"),
            (concept_snapshot_id, "CONCEPT", "TDX_INFOHARBOR_BLOCK_DAT"),
        ):
            if supplied_id is None:
                continue
            try:
                snapshot_uuid = UUID(supplied_id)
            except (TypeError, ValueError, AttributeError) as error:
                raise ResearchDayPgError(
                    "RESEARCH_DAY_MEMBER_SNAPSHOT_INVALID", "板块成员快照不是合法UUID"
                ) from error
            if str(snapshot_uuid) != supplied_id:
                raise ResearchDayPgError(
                    "RESEARCH_DAY_MEMBER_SNAPSHOT_INVALID",
                    "板块成员快照必须是规范小写UUID",
                )
            cursor.execute(
                "SELECT source_kind, research_mode FROM stock.tdx_sector_source_snapshots "
                "WHERE snapshot_id=%s",
                (snapshot_uuid,),
            )
            snapshot_meta = cursor.fetchone()
            if snapshot_meta != (
                source_kind,
                "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
            ):
                raise ResearchDayPgError(
                    "RESEARCH_DAY_MEMBER_SNAPSHOT_NOT_AVAILABLE",
                    f"{kind}快照缺失、类型不符或未标记NON_PIT",
                )
            snapshot_specs.append((snapshot_uuid, kind))
        cursor.execute(
            f"SELECT before_session, history_window_start, history_window_end, "
            f"calendar_id, calendar_sha256, producer_contract, material_change_contract, "
            f"source_time_quality, source_clock_role, source_available_at, research_mode, "
            f"price_coordinate, min(source_content_id), count(DISTINCT source_content_id), "
            f"count(DISTINCT source_symbol), count(*) "
            f"FROM stock.source_d1_structure_day_facts WHERE {d1_predicate} "
            f"GROUP BY before_session, history_window_start, history_window_end, calendar_id, "
            f"calendar_sha256, producer_contract, material_change_contract, "
            f"source_time_quality, source_clock_role, source_available_at, research_mode, price_coordinate",
            d1_params,
        )
        contexts = cursor.fetchall()
        if not contexts:
            return incomplete_result(namespace_id, "D1单日事实缺少窗口或来源质量上下文")
        if len(contexts) != 1:
            raise ResearchDayPgError(
                "RESEARCH_DAY_D1_CONTEXT_AMBIGUOUS", "D1单日事实上下文不唯一"
            )
        d1_context = contexts[0]
        before_session = d1_context[0]
        source_content_id = d1_context[12]
        if d1_context[13] != 1:
            return incomplete_result(
                namespace_id,
                "D1逐股事实绑定多个来源内容摘要",
                distinct_source_content_count=d1_context[13],
            )
        d1_source_count = d1_context[-2]
        d1_row_count = d1_context[-1]
        if d1_row_count != d1_source_count * len(_STRUCTURE_TYPES):
            return incomplete_result(
                namespace_id,
                "D1逐股五类结构行数不闭合",
                source_symbol_count=d1_source_count,
                observed_structure_rows=d1_row_count,
                expected_structure_rows=d1_source_count * len(_STRUCTURE_TYPES),
            )
        cursor.execute(
            f"SELECT structure_type, count(*), count(DISTINCT source_symbol) "
            f"FROM stock.source_d1_structure_day_facts WHERE {d1_predicate} "
            "GROUP BY structure_type ORDER BY structure_type",
            d1_params,
        )
        type_counts = cursor.fetchall()
        if {row[0] for row in type_counts} != set(_STRUCTURE_TYPES) or any(
            row[1] != d1_source_count or row[2] != d1_source_count
            for row in type_counts
        ):
            return incomplete_result(
                namespace_id,
                "D1五类结构逐股分母不一致",
                structure_type_counts=[list(row) for row in type_counts],
                source_symbol_count=d1_source_count,
            )
        cursor.execute(
            f"SELECT count(*), sum(persisted_detail_count) "
            f"FROM stock.source_d1_structure_day_facts WHERE {d1_predicate}",
            d1_params,
        )
        d1_rows_count, expected_anchor_count = cursor.fetchone()
        anchor_predicate = (
            "namespace_id=%s AND target_session=%s AND package_sha256=%s "
            "AND target_capture_id=%s AND d1_history_session_count=%s "
            "AND calendar_revision=%s AND calendar_sha256=%s "
            "AND producer_contract=%s AND producer_version=%s "
            "AND material_change_contract=%s"
        )
        cursor.execute(
            f"SELECT count(*) FROM stock.source_d1_material_anchor_day_facts WHERE {anchor_predicate}",
            d1_params,
        )
        actual_anchor_count = cursor.fetchone()[0]
        if actual_anchor_count != expected_anchor_count:
            return incomplete_result(
                namespace_id,
                "D1锚明细与逐股分母不一致",
                expected_anchor_rows=expected_anchor_count,
                observed_anchor_rows=actual_anchor_count,
            )
        cursor.execute(
            "SELECT DISTINCT before_capture_id, producer_version, algorithm_contract, session_contract, "
            "before_source_count, target_source_count, union_source_count, before_window_start, "
            "target_window_start, calendar_id, calendar_revision, calendar_sha256, "
            "source_time_quality, source_clock_role, source_available_at, research_mode, price_coordinate, "
            "source_content_id, before_session "
            "FROM stock.source_d0_launch_day_facts WHERE namespace_id=%s AND target_session=%s "
            "AND package_sha256=%s AND target_capture_id=%s ORDER BY before_capture_id, producer_version, session_contract",
            (namespace_id, target_session, package_sha256, target_capture_id),
        )
        d0_scopes = cursor.fetchall()
        if not d0_scopes:
            return incomplete_result(namespace_id, "精确目标D0事实缺失")
        if len(d0_scopes) != 1:
            raise ResearchDayPgError(
                "RESEARCH_DAY_D0_SCOPE_MISSING_OR_AMBIGUOUS",
                "目标D0事实缺失或读取键不唯一",
            )
        d0_scope = d0_scopes[0]
        if d0_scope[17] != source_content_id:
            return incomplete_result(
                namespace_id,
                "D0与D1绑定的来源内容摘要不一致",
                d0_source_content_id=d0_scope[17],
                d1_source_content_id=source_content_id,
            )
        if (
            d0_scope[10] != calendar_revision
            or d0_scope[11] != calendar_sha256
            or d0_scope[18] != before_session
        ):
            return incomplete_result(
                namespace_id,
                "D0与D1目标日、前态或官方日历上下文不一致",
                d0_before_session=_json_value(d0_scope[18]),
                d1_before_session=_json_value(before_session),
                d0_calendar_revision=d0_scope[10],
                d1_calendar_revision=calendar_revision,
                d0_calendar_sha256=d0_scope[11],
                d1_calendar_sha256=calendar_sha256,
            )
        d0_predicate = (
            "namespace_id=%s AND target_session=%s AND package_sha256=%s "
            "AND before_capture_id=%s AND target_capture_id=%s "
            "AND producer_version=%s AND algorithm_contract=%s AND session_contract=%s "
            "AND calendar_revision=%s AND calendar_sha256=%s"
        )
        d0_params = (
            namespace_id,
            target_session,
            package_sha256,
            d0_scope[0],
            target_capture_id,
            d0_scope[1],
            d0_scope[2],
            d0_scope[3],
            d0_scope[10],
            d0_scope[11],
        )
        cursor.execute(
            "SELECT count(*), count(DISTINCT source_symbol), min(union_source_count), "
            "max(union_source_count), count(*) FILTER (WHERE after_state IS NOT NULL) "
            f"FROM stock.source_d0_launch_day_facts WHERE {d0_predicate}",
            d0_params,
        )
        d0_counts = cursor.fetchone()
        if (
            d0_counts[0] != d0_scope[6]
            or d0_counts[1] != d0_scope[6]
            or d0_counts[2] != d0_scope[6]
            or d0_counts[3] != d0_scope[6]
            or d0_counts[4] != d1_source_count
        ):
            return incomplete_result(
                namespace_id,
                "D0并集分母或目标态股票数不闭合",
                observed_d0_rows=d0_counts[0],
                distinct_d0_symbols=d0_counts[1],
                declared_union_count=d0_scope[6],
                d0_target_symbols=d0_counts[4],
                d1_target_symbols=d1_source_count,
            )
        cursor.execute(
            "SELECT EXISTS ((SELECT DISTINCT source_symbol FROM stock.source_d1_structure_day_facts "
            f"WHERE {d1_predicate}) EXCEPT (SELECT source_symbol FROM stock.source_d0_launch_day_facts "
            f"WHERE {d0_predicate} AND after_state IS NOT NULL)) "
            "OR EXISTS ((SELECT source_symbol FROM stock.source_d0_launch_day_facts "
            f"WHERE {d0_predicate} AND after_state IS NOT NULL) "
            f"EXCEPT (SELECT DISTINCT source_symbol FROM stock.source_d1_structure_day_facts WHERE {d1_predicate}))",
            d1_params + d0_params + d0_params + d1_params,
        )
        if cursor.fetchone()[0]:
            return incomplete_result(namespace_id, "D0目标态与D1逐股集合不一致")
        cursor.execute(
            "SELECT before_state, after_state, transition, count(*) FROM stock.source_d0_launch_day_facts "
            f"WHERE {d0_predicate} "
            "GROUP BY before_state, after_state, transition ORDER BY before_state NULLS FIRST, after_state NULLS FIRST, transition",
            d0_params,
        )
        d0_transitions = [
            {
                "before_state": row[0],
                "after_state": row[1],
                "transition": row[2],
                "count": row[3],
            }
            for row in cursor.fetchall()
        ]
        cursor.execute(
            "SELECT sum(material_change_anchor_count), sum(unknown_anchor_count), "
            "sum(not_comparable_anchor_count), sum(not_evaluable_anchor_count) "
            f"FROM stock.source_d1_structure_day_facts WHERE {d1_predicate}",
            d1_params,
        )
        material_count, unknown_count, not_comparable_count, not_evaluable_count = (
            cursor.fetchone()
        )
        material_count = material_count or 0
        quality_count = (
            (unknown_count or 0)
            + (not_comparable_count or 0)
            + (not_evaluable_count or 0)
        )
        result_status = (
            "HAS_MATERIAL_CHANGE"
            if material_count
            else "UNKNOWN"
            if quality_count
            else "NO_CHANGE"
        )

        d1_alias_predicate = (
            "d.namespace_id=%s AND d.target_session=%s AND d.package_sha256=%s "
            "AND d.target_capture_id=%s AND d.d1_history_session_count=%s "
            "AND d.calendar_revision=%s AND d.calendar_sha256=%s "
            "AND d.producer_contract=%s AND d.producer_version=%s "
            "AND d.material_change_contract=%s"
        )
        filters = []
        filter_params = []
        if structure_type is not None:
            filters.append(
                "EXISTS (SELECT 1 FROM stock.source_d1_structure_day_facts f "
                "WHERE f.namespace_id=d.namespace_id AND f.target_session=d.target_session "
                "AND f.source_symbol=d.source_symbol AND f.structure_type=%s "
                "AND f.package_sha256=d.package_sha256 AND f.target_capture_id=d.target_capture_id "
                "AND f.d1_history_session_count=d.d1_history_session_count "
                "AND f.calendar_revision=d.calendar_revision AND f.calendar_sha256=d.calendar_sha256 "
                "AND f.producer_contract=d.producer_contract AND f.producer_version=d.producer_version "
                "AND f.material_change_contract=d.material_change_contract "
                "AND f.material_change_anchor_count>0)"
            )
            filter_params.append(structure_type)
        if material_change_reason is not None:
            filters.append(
                "EXISTS (SELECT 1 FROM stock.source_d1_material_anchor_day_facts a "
                "WHERE a.namespace_id=d.namespace_id AND a.target_session=d.target_session "
                "AND a.source_symbol=d.source_symbol AND a.package_sha256=d.package_sha256 "
                "AND a.target_capture_id=d.target_capture_id "
                "AND a.d1_history_session_count=d.d1_history_session_count "
                "AND a.calendar_revision=d.calendar_revision AND a.calendar_sha256=d.calendar_sha256 "
                "AND a.producer_contract=d.producer_contract AND a.producer_version=d.producer_version "
                "AND a.material_change_contract=d.material_change_contract "
                "AND a.material_change_classification='MATERIAL_CHANGE' "
                + ("AND a.structure_type=%s " if structure_type is not None else "")
                + "AND %s = ANY(a.material_change_reasons))"
            )
            if structure_type is not None:
                filter_params.append(structure_type)
            filter_params.append(material_change_reason)
        if board_kind is not None and board_native_id is not None:
            snapshot_uuid = next(
                snapshot for snapshot, kind in snapshot_specs if kind == board_kind
            )
            filters.append(
                "EXISTS (SELECT 1 FROM stock.tdx_sector_members m "
                "WHERE m.snapshot_id=%s AND m.board_kind=%s AND m.board_native_id=%s "
                "AND m.source_symbol=d.source_symbol)"
            )
            filter_params.extend((snapshot_uuid, board_kind, board_native_id))
        if symbol is not None:
            filters.append("d.source_symbol=%s")
            filter_params.append(symbol)
        filter_sql = "".join(f" AND ({item})" for item in filters)
        selected_query = (
            "SELECT d.source_symbol, d.market, d.board "
            "FROM stock.source_d1_structure_day_facts d "
            f"WHERE {d1_alias_predicate}{filter_sql} "
            "GROUP BY d.source_symbol, d.market, d.board ORDER BY d.source_symbol"
        )
        cursor.execute(
            "SELECT count(*) FROM (" + selected_query + ") AS selected_stocks",
            d1_params + tuple(filter_params),
        )
        stock_total = cursor.fetchone()[0]
        if include_all_results:
            offset = 0
            limit = stock_total
        cursor.execute(
            selected_query + " OFFSET %s LIMIT %s",
            d1_params + tuple(filter_params) + (offset, limit),
        )
        stock_page = cursor.fetchall()
        page_symbols = [row[0] for row in stock_page]
        board_summaries = []
        membership_coverage = []
        for snapshot_uuid, kind in snapshot_specs:
            anchor_filters = ["material_change_classification='MATERIAL_CHANGE'"]
            anchor_params = []
            if structure_type is not None:
                anchor_filters.append("structure_type=%s")
                anchor_params.append(structure_type)
            if material_change_reason is not None:
                anchor_filters.append("%s = ANY(material_change_reasons)")
                anchor_params.append(material_change_reason)
            cursor.execute(
                "WITH source_symbols AS (SELECT DISTINCT source_symbol "
                f"FROM stock.source_d1_structure_day_facts WHERE {d1_predicate} "
                "), selected_anchors AS (SELECT source_symbol, count(*) AS material_anchor_count "
                f"FROM stock.source_d1_material_anchor_day_facts WHERE {d1_predicate} AND "
                + " AND ".join(anchor_filters)
                + " GROUP BY source_symbol), stock_summary AS (SELECT s.source_symbol, "
                "coalesce(a.material_anchor_count, 0) AS material_anchor_count "
                "FROM source_symbols s LEFT JOIN selected_anchors a USING (source_symbol)) "
                "SELECT c.board_kind, c.board_native_id, c.board_name, "
                "count(DISTINCT m.source_symbol) FILTER (WHERE s.source_symbol IS NOT NULL), "
                "count(DISTINCT m.source_symbol) FILTER (WHERE s.material_anchor_count>0), "
                "coalesce(sum(s.material_anchor_count), 0) "
                "FROM stock.tdx_sector_catalog c JOIN stock.tdx_sector_members m "
                "ON m.snapshot_id=c.snapshot_id AND m.board_kind=c.board_kind "
                "AND m.board_native_id=c.board_native_id "
                "LEFT JOIN stock_summary s ON s.source_symbol=m.source_symbol "
                "WHERE c.snapshot_id=%s AND c.board_kind=%s "
                "GROUP BY c.board_kind, c.board_native_id, c.board_name "
                "ORDER BY c.board_name, c.board_native_id OFFSET %s LIMIT %s",
                d1_params
                + d1_params
                + tuple(anchor_params)
                + (snapshot_uuid, kind, offset, limit),
            )
            board_summaries.extend(
                {
                    "board_kind": row[0],
                    "board_native_id": row[1],
                    "board_name": row[2],
                    "source_stock_count": row[3],
                    "material_change_stock_count": row[4],
                    "material_change_anchor_count": int(row[5]),
                    "snapshot_id": str(snapshot_uuid),
                    "membership_scope": "NON_PIT_CURRENT_SNAPSHOT",
                }
                for row in cursor.fetchall()
            )
            cursor.execute(
                "WITH source_symbols AS (SELECT DISTINCT source_symbol "
                f"FROM stock.source_d1_structure_day_facts WHERE {d1_predicate}) "
                "SELECT count(*), count(*) FILTER (WHERE m.source_symbol IS NULL) "
                "FROM source_symbols s LEFT JOIN (SELECT DISTINCT snapshot_id, source_symbol "
                "FROM stock.tdx_sector_members WHERE snapshot_id=%s) m "
                "ON m.source_symbol=s.source_symbol",
                d1_params + (snapshot_uuid,),
            )
            coverage_total, unmapped_count = cursor.fetchone()
            membership_coverage.append(
                {
                    "board_kind": kind,
                    "snapshot_id": str(snapshot_uuid),
                    "source_symbol_denominator": coverage_total,
                    "mapped_source_symbol_count": coverage_total - unmapped_count,
                    "unmapped_source_symbol_count": unmapped_count,
                    "membership_scope": "NON_PIT_CURRENT_SNAPSHOT",
                }
            )
        if not page_symbols:
            d1_selected, d0_selected, anchors_selected = [], [], []
        else:
            cursor.execute(
                f"SELECT {', '.join(_D1_COLUMNS)} FROM stock.source_d1_structure_day_facts "
                f"WHERE {d1_predicate} AND source_symbol = ANY(%s) "
                "ORDER BY source_symbol, structure_type",
                d1_params + (page_symbols,),
            )
            d1_selected = [_as_dict(_D1_COLUMNS, row) for row in cursor.fetchall()]
            d0_columns = _D0_COLUMNS
            cursor.execute(
                f"SELECT {', '.join(d0_columns)} FROM stock.source_d0_launch_day_facts "
                f"WHERE {d0_predicate} AND source_symbol = ANY(%s) ORDER BY source_symbol",
                d0_params + (page_symbols,),
            )
            d0_selected = [_as_dict(d0_columns, row) for row in cursor.fetchall()]
            cursor.execute(
                f"SELECT {', '.join(_ANCHOR_COLUMNS)} FROM stock.source_d1_material_anchor_day_facts "
                f"WHERE {anchor_predicate} AND source_symbol = ANY(%s) "
                "ORDER BY source_symbol, structure_type, created_session, anchor_id",
                d1_params + (page_symbols,),
            )
            anchors_selected = [
                _as_dict(_ANCHOR_COLUMNS, row) for row in cursor.fetchall()
            ]
        memberships_by_symbol: dict[str, list[dict[str, Any]]] = {}
        if snapshot_specs and page_symbols:
            snapshot_ids = [snapshot for snapshot, _kind in snapshot_specs]
            cursor.execute(
                "SELECT m.source_symbol, c.board_kind, c.board_native_id, c.board_name, "
                "m.membership_quality, m.snapshot_id "
                "FROM stock.tdx_sector_members m JOIN stock.tdx_sector_catalog c "
                "ON c.snapshot_id=m.snapshot_id AND c.board_kind=m.board_kind "
                "AND c.board_native_id=m.board_native_id "
                "WHERE m.snapshot_id = ANY(%s::uuid[]) AND m.source_symbol = ANY(%s) "
                "ORDER BY m.source_symbol, c.board_kind, c.board_name, c.board_native_id",
                (snapshot_ids, page_symbols),
            )
            for row in cursor.fetchall():
                memberships_by_symbol.setdefault(row[0], []).append(
                    {
                        "board_kind": row[1],
                        "board_native_id": row[2],
                        "board_name": row[3],
                        "membership_quality": row[4],
                        "snapshot_id": str(row[5]),
                        "scope": "NON_PIT_CURRENT_SNAPSHOT",
                    }
                )
        by_symbol = {
            row[0]: {
                "source_symbol": row[0],
                "market": row[1],
                "board": row[2],
                "memberships": memberships_by_symbol.get(row[0], []),
                "structures": [],
            }
            for row in stock_page
        }
        for item in d1_selected:
            by_symbol[item["source_symbol"]]["structures"].append(item)
        for item in d0_selected:
            by_symbol[item["source_symbol"]]["d0"] = item
        for item in anchors_selected:
            by_symbol[item["source_symbol"]].setdefault("material_anchors", []).append(
                item
            )
        return {
            "storage_status": "SAVED",
            "namespace_id": namespace_id,
            "target_session": target_session.isoformat(),
            "before_session": _json_value(before_session),
            "package_sha256": package_sha256,
            "source_content_id": source_content_id,
            "target_capture_id": str(target_capture_id),
            "d1_history_session_count": d1_history_session_count,
            "calendar_revision": calendar_revision,
            "calendar_sha256": calendar_sha256,
            "d1_producer_contract": producer_contract,
            "d1_producer_version": d1_producer_version,
            "d1_material_change_contract": material_change_contract,
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "price_coordinate": "RAW_UNADJUSTED",
            "result_status": result_status,
            "membership_snapshots": [
                {
                    "board_kind": kind,
                    "snapshot_id": str(snapshot_uuid),
                    "scope": "NON_PIT_CURRENT_SNAPSHOT",
                }
                for snapshot_uuid, kind in snapshot_specs
            ],
            "membership_coverage": membership_coverage,
            "board_summaries": board_summaries,
            "board_counts_are_non_additive": bool(snapshot_specs),
            "selected_filters": {
                "board_kind": board_kind,
                "board_native_id": board_native_id,
                "structure_type": structure_type,
                "material_change_reason": material_change_reason,
                "source_symbol": symbol,
            },
            "d0": {
                "union_source_count": d0_counts[0],
                "target_source_count": d0_counts[4],
                "algorithm_contract": d0_scope[2],
                "session_contract": d0_scope[3],
                "transitions": d0_transitions,
            },
            "d1": {
                "source_symbol_count": d1_source_count,
                "structure_type_counts": {row[0]: row[1] for row in type_counts},
                "typed_structure_rows": d1_rows_count,
                "typed_anchor_rows": actual_anchor_count,
                "material_change_anchor_count": material_count,
                "quality_anchor_count": quality_count,
            },
            "pagination": {
                "offset": offset,
                "limit": limit,
                "total_count": stock_total,
                "next_offset": offset + limit if offset + limit < stock_total else None,
            },
            "results": [by_symbol[symbol] for symbol in page_symbols],
            "execution": {
                "database_mode": "READ_ONLY",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
        }


__all__ = [
    "ResearchDayPgError",
    "persist_source_research_day",
    "read_source_research_day",
]
