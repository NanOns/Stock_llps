"""现役D0/D1单日typed研究事实的PG写入和只读读取。"""

from __future__ import annotations

from collections import Counter
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
    "before_session",
    "target_session",
    "source_symbol",
    "market",
    "source_package_id",
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
    "research_mode",
    "price_coordinate",
)
_D1_COLUMNS = (
    "before_session",
    "target_session",
    "source_symbol",
    "market",
    "board",
    "structure_type",
    "source_package_id",
    "target_capture_id",
    "d1_history_session_count",
    "history_window_start",
    "history_window_end",
    "calendar_id",
    "calendar_revision",
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
    "anchor_created_session",
    "anchor_available_from_session",
    "anchor_test_from_session",
    "coordinate_id",
    "relation",
    "lower_numerator",
    "lower_denominator",
    "upper_numerator",
    "upper_denominator",
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
    "d1_structure_id",
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
    "previous_capture_id",
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
    "source_package_id",
    "target_session",
    "source_symbol",
    "before_capture_id",
    "target_capture_id",
    "producer_version",
    "algorithm_contract",
    "session_contract",
    "calendar_revision",
)
_D1_KEY = (
    "source_package_id",
    "target_session",
    "source_symbol",
    "structure_type",
    "target_capture_id",
    "d1_history_session_count",
    "calendar_revision",
    "producer_contract",
    "producer_version",
    "material_change_contract",
)
_ANCHOR_KEY = ("d1_structure_id", "created_session")
_TABLES = (
    ("source_d0_launch_day_facts", _D0_COLUMNS, _D0_KEY),
    ("source_d1_structure_day_facts", _D1_COLUMNS, _D1_KEY),
    ("source_d1_material_anchor_day_facts", _ANCHOR_COLUMNS, _ANCHOR_KEY),
)


class ResearchDayPgError(ValueError):
    """单日研究事实存储缺迁移、来源不一致或已存在内容冲突。"""

    preserve_transaction_error = True

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
                facts.before_session,
                facts.target_session,
                item.source_symbol,
                item.market,
                facts.source_package_id,
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
                facts.before_session,
                facts.target_session,
                item.source_symbol,
                item.market,
                item.board,
                item.structure_type,
                facts.source_package_id,
                facts.target_capture_id,
                facts.d1_history_session_count,
                facts.d1_history_window_start,
                facts.d1_history_window_end,
                facts.calendar_id,
                facts.calendar_revision,
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
                item.anchor_created_session,
                item.anchor_available_from_session,
                item.anchor_test_from_session,
                item.coordinate_id,
                item.relation,
                lower_num,
                lower_den,
                upper_num,
                upper_den,
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


def _anchor_rows(
    facts: ResearchDayFacts, structure_ids: Mapping[tuple[str, str], int]
) -> list[tuple[Any, ...]]:
    result = []
    for item in facts.d1_anchor_facts:
        lower_num, lower_den = _ratio_pair(item.lower)
        upper_num, upper_den = _ratio_pair(item.upper)
        result.append(
            (
                structure_ids[(item.source_symbol, item.structure_type)],
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
                item.previous_capture_id,
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
        or type(facts.source_package_id) is not int
        or facts.source_package_id <= 0
        or type(facts.before_capture_id) is not int
        or type(facts.target_capture_id) is not int
        or facts.before_capture_id <= 0
        or facts.target_capture_id <= 0
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
            item.created_session,
        )
        anchor_keys.add(key)
        owner = (item.source_symbol, item.structure_type)
        detail_counts[owner] = detail_counts.get(owner, 0) + 1
        if (
            owner not in d1_by_key
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
    table_name: str,
    facts: ResearchDayFacts,
    structure_ids: Sequence[int] = (),
) -> tuple[str, tuple[Any, ...]]:
    if table_name.endswith("source_d0_launch_day_facts"):
        return (
            "target_session=%s AND source_package_id=%s "
            "AND before_capture_id=%s AND target_capture_id=%s AND producer_version=%s "
            "AND algorithm_contract=%s AND session_contract=%s "
            "AND calendar_revision=%s",
            (
                facts.target_session,
                facts.source_package_id,
                facts.before_capture_id,
                facts.target_capture_id,
                facts.d0_producer_version,
                facts.d0_algorithm_contract,
                facts.d0_session_contract,
                facts.calendar_revision,
            ),
        )
    if table_name.endswith("source_d1_structure_day_facts"):
        return (
            "target_session=%s AND source_package_id=%s "
            "AND target_capture_id=%s AND d1_history_session_count=%s "
            "AND calendar_revision=%s "
            "AND producer_contract=%s AND producer_version=%s "
            "AND material_change_contract=%s",
            (
                facts.target_session,
                facts.source_package_id,
                facts.target_capture_id,
                facts.d1_history_session_count,
                facts.calendar_revision,
                facts.d1_producer_contract,
                facts.d1_producer_version,
                facts.d1_material_change_contract,
            ),
        )
    return "d1_structure_id = ANY(%s)", (list(structure_ids),)


def _verify_raw_source(cursor: Any, facts: ResearchDayFacts) -> None:
    cursor.execute(
        """
        SELECT session_date, count(*), count(DISTINCT source_symbol),
               count(DISTINCT source_capture_id), min(source_capture_id),
               count(DISTINCT source_package_id), min(source_package_id)
        FROM source_raw_daily
        WHERE source_package_id=%s
          AND session_date IN (%s, %s)
        GROUP BY session_date ORDER BY session_date
        """,
        (
            facts.source_package_id,
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
        or target[5] != facts.source_package_id
        or before[1] != facts.d0_before_source_count
        or before[2] != 1
        or before[3] != facts.before_capture_id
    ):
        raise ResearchDayPgError(
            "RESEARCH_DAY_RAW_IDENTITY_MISMATCH",
            "D0/D1事实与固定PG中的前后态Raw总体、捕获或数据包编号不一致",
        )
    cursor.execute(
        """
        SELECT session_date, source_symbol FROM source_raw_daily
        WHERE source_package_id=%s
          AND session_date IN (%s, %s)
        ORDER BY session_date, source_symbol
        """,
        (
            facts.source_package_id,
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
        "SELECT to_regclass('source_d0_launch_day_facts') IS NOT NULL, "
        "to_regclass('source_d1_structure_day_facts') IS NOT NULL, "
        "to_regclass('source_d1_material_anchor_day_facts') IS NOT NULL"
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
    # 0022以捕获父键收窄唯一约束；调用方可保留更严格的范围比较。
    current_keys = {
        "source_d0_launch_day_facts": (
            "target_capture_id",
            "source_symbol",
            "before_capture_id",
            "producer_version",
            "algorithm_contract",
            "session_contract",
            "calendar_revision",
        ),
        "source_d1_structure_day_facts": (
            "target_capture_id",
            "source_symbol",
            "structure_type",
            "d1_history_session_count",
            "calendar_revision",
            "producer_contract",
            "producer_version",
            "material_change_contract",
        ),
        "source_d0_scenario_day_facts": (
            "source_capture_id",
            "source_symbol",
            "scenario",
            "producer_version",
            "calendar_revision",
        ),
        "source_pool_seed_day_facts": (
            "source_capture_id",
            "source_symbol",
            "profile_revision",
            "producer_version",
        ),
        "source_market_breadth_days": (
            "source_capture_id",
            "contract_version",
            "calendar_revision",
        ),
        "source_market_breadth_symbols": ("breadth_day_id", "source_symbol"),
    }
    conflict = ", ".join(current_keys.get(table.rsplit(".", 1)[-1], key))
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
    d0_rows = _sort_rows(_d0_rows(facts), _D0_COLUMNS, _D0_KEY)
    d1_rows = _sort_rows(_d1_rows(facts), _D1_COLUMNS, _D1_KEY)
    inserted = {}
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _connection_identity,
    ):
        _ensure_tables(cursor)
        _verify_raw_source(cursor, facts)
        # 在本事务写入之前读取范围，避免把刚插入的行误认作历史半批。
        preexisting = []
        for (table, columns, _key), rows in zip(
            _TABLES[:2], (d0_rows, d1_rows), strict=True
        ):
            predicate, params = _scope_predicate(table, facts)
            existing = _select_rows(cursor, table, columns, predicate, params)
            if existing and (
                len(existing) != len(rows)
                or any(
                    not _row_matches(expected, actual)
                    for expected, actual in zip(rows, existing, strict=True)
                )
            ):
                raise ResearchDayPgError(
                    "RESEARCH_DAY_CONTENT_CONFLICT",
                    "相同D0/D1业务键已有不同或不完整事实",
                )
            preexisting.append(existing)
        inserted["source_d0_launch_day_facts"] = _insert_rows(
            cursor,
            "source_d0_launch_day_facts",
            _D0_COLUMNS,
            _D0_KEY,
            d0_rows,
        )
        inserted["source_d1_structure_day_facts"] = _insert_rows(
            cursor,
            "source_d1_structure_day_facts",
            _D1_COLUMNS,
            _D1_KEY,
            d1_rows,
        )
        d1_predicate, d1_params = _scope_predicate(
            "source_d1_structure_day_facts", facts
        )
        cursor.execute(
            "SELECT id, source_symbol, structure_type "
            f"FROM source_d1_structure_day_facts WHERE {d1_predicate} "
            "ORDER BY source_symbol, structure_type",
            d1_params,
        )
        structure_ids = {
            (source_symbol, structure_type): structure_id
            for structure_id, source_symbol, structure_type in cursor.fetchall()
        }
        expected_d1_keys = {
            (item.source_symbol, item.structure_type)
            for item in facts.d1_structure_facts
        }
        if set(structure_ids) != expected_d1_keys:
            raise ResearchDayPgError(
                "RESEARCH_DAY_D1_PARENT_MISSING",
                "D1材料锚无法绑定唯一的数字结构事实主键",
            )
        anchor_rows = _sort_rows(
            _anchor_rows(facts, structure_ids), _ANCHOR_COLUMNS, _ANCHOR_KEY
        )
        expected_rows = (d0_rows, d1_rows, anchor_rows)
        predicate, params = _scope_predicate(
            _TABLES[2][0], facts, tuple(structure_ids.values())
        )
        preexisting.append(
            _select_rows(cursor, _TABLES[2][0], _ANCHOR_COLUMNS, predicate, params)
        )
        existing_scopes = []
        any_existing = any(preexisting)
        for existing, rows in zip(preexisting, expected_rows, strict=True):
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
            inserted["source_d1_material_anchor_day_facts"] = _insert_rows(
                cursor,
                "source_d1_material_anchor_day_facts",
                _ANCHOR_COLUMNS,
                _ANCHOR_KEY,
                anchor_rows,
            )
        for index, ((table, columns, _key), rows) in enumerate(
            zip(_TABLES, expected_rows, strict=True)
        ):
            predicate, params = (
                _scope_predicate(table, facts, tuple(structure_ids.values()))
                if index == 2
                else _scope_predicate(table, facts)
            )
            actual = _select_rows(cursor, table, columns, predicate, params)
            if len(actual) != len(rows) or any(
                not _row_matches(expected, persisted)
                for expected, persisted in zip(rows, actual, strict=True)
            ):
                raise ResearchDayPgError(
                    "RESEARCH_DAY_TYPED_READBACK_MISMATCH",
                    "事务内typed事实读回和预期不一致，拒绝提交",
                )
        from stock_a.storage.postgres.research_enrollment import insert_d0_enrollments

        enrollment = insert_d0_enrollments(
            cursor,
            target=facts.target_session,
            package=facts.source_package_id,
            capture=facts.target_capture_id,
            before=facts.before_session,
            before_capture=facts.before_capture_id,
            producer_version=facts.d0_producer_version,
            algorithm_contract=facts.d0_algorithm_contract,
            session_contract=facts.d0_session_contract,
            calendar_revision=facts.calendar_revision,
        )
    return {
        "status": "IDEMPOTENT_ALREADY_PRESENT"
        if all(existing_scopes) and enrollment["inserted"] == 0
        else "WRITTEN_AND_TYPED_READBACK_VERIFIED",
        "rows_expected": {
            table.rsplit(".", 1)[-1]: len(rows)
            for (table, _columns, _key), rows in zip(
                _TABLES, expected_rows, strict=True
            )
        },
        "rows_inserted": inserted,
        "research_enrollments": enrollment,
        "same_input_retry": "ZERO_INSERTS_TYPED_EQUALITY_VERIFIED"
        if all(existing_scopes) and enrollment["inserted"] == 0
        else None,
        "transaction": "SERIALIZABLE_ATOMIC_THREE_TABLE_WRITE",
        "database_write_attempted": any(inserted.values())
        or enrollment["inserted"] > 0,
        "ddl_executed": False,
        "dml_executed": any(inserted.values()) or enrollment["inserted"] > 0,
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


def _read_selected_board_observation(
    cursor: Any,
    *,
    target_session: date,
    source_package_id: int,
    target_capture_id: int,
    d1_history_session_count: int,
    d1_predicate: str,
    d1_params: tuple[Any, ...],
    d0_predicate: str,
    d0_params: tuple[Any, ...],
    snapshot_specs: Sequence[tuple[int, str]],
    board_kind: str,
    board_native_id: str,
    offset: int,
    limit: int,
) -> dict[str, Any]:
    """同一只读快照读取一个当前板块的成员、重叠及已保存研究事实。"""
    selected_snapshot = next(
        (snapshot_id for snapshot_id, kind in snapshot_specs if kind == board_kind),
        None,
    )
    if selected_snapshot is None:
        return {"status": "NOT_COMPUTED", "reason": "所选板块成员快照不可用"}
    cursor.execute(
        "SELECT m.source_symbol, m.membership_quality "
        "FROM tdx_sector_catalog c JOIN tdx_sector_members m "
        "ON m.catalog_id=c.id AND m.board_kind=c.board_kind "
        "AND m.board_native_id=c.board_native_id "
        "WHERE c.snapshot_id=%s AND c.board_kind=%s AND c.board_native_id=%s "
        "ORDER BY m.source_symbol",
        (selected_snapshot, board_kind, board_native_id),
    )
    member_rows = cursor.fetchall()
    symbols = [row[0] for row in member_rows]
    if not symbols:
        return {
            "status": "AVAILABLE",
            "board_kind": board_kind,
            "board_native_id": board_native_id,
            "membership_scope": "NON_PIT_CURRENT_SNAPSHOT",
            "snapshot_member_count": 0,
            "source_pool": {"inside": 0, "outside": 0},
            "d0": {
                "true": 0,
                "false": 0,
                "unknown": 0,
                "missing": 0,
                "transitions": {},
            },
            "seed": {
                "scope_status": "NOT_COMPUTED",
                "true": 0,
                "false": 0,
                "unknown": 0,
                "missing": 0,
            },
            "d1": {
                "scope_status": "AVAILABLE",
                "material_change_members": 0,
                "material_anchor_count": 0,
                "target_price_observed": 0,
                "target_price_unknown": 0,
            },
            "cross_kind_overlap": {"member_count": 0, "top_boards": []},
            "pagination": {
                "offset": 0,
                "limit": limit,
                "total_count": 0,
                "next_offset": None,
            },
            "members": [],
        }

    cursor.execute(
        "SELECT source_symbol FROM source_raw_daily "
        "WHERE session_date=%s AND source_package_id=%s AND source_capture_id=%s",
        (target_session, source_package_id, target_capture_id),
    )
    target_source_symbols = {row[0] for row in cursor.fetchall()}

    cursor.execute(
        "SELECT source_symbol, before_state, after_state, transition "
        f"FROM source_d0_launch_day_facts WHERE {d0_predicate} "
        "AND source_symbol=ANY(%s) ORDER BY source_symbol",
        d0_params + (symbols,),
    )
    d0_by_symbol = {
        row[0]: {"before_state": row[1], "after_state": row[2], "transition": row[3]}
        for row in cursor.fetchall()
    }

    # Seed画像窗与D1结构窗各自有独立业务键，不能强制使用同一会话数。
    cursor.execute(
        "SELECT profile_revision, producer_contract, producer_version, "
        "history_session_count, calendar_id, calendar_revision, research_mode, "
        "price_coordinate, count(*)::bigint, count(DISTINCT source_symbol)::bigint "
        "FROM source_pool_seed_day_facts "
        "WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s "
        "GROUP BY profile_revision, producer_contract, producer_version, "
        "history_session_count, calendar_id, calendar_revision, research_mode, price_coordinate "
        "ORDER BY profile_revision, producer_contract, producer_version, history_session_count",
        (target_session, source_package_id, target_capture_id),
    )
    seed_scopes = cursor.fetchall()
    seed_scope = None
    seed_scope_status = "NOT_COMPUTED"
    if len(seed_scopes) == 1:
        candidate = seed_scopes[0]
        if candidate[6] == "NON_PIT_RETROSPECTIVE" and candidate[7] == "RAW_UNADJUSTED":
            seed_scope = candidate
            seed_scope_status = (
                "AVAILABLE"
                if int(candidate[9]) == int(candidate[8])
                and int(candidate[9]) == len(target_source_symbols)
                else "INCOMPLETE"
            )
        else:
            seed_scope_status = "UNKNOWN"
    elif len(seed_scopes) > 1:
        seed_scope_status = "AMBIGUOUS"

    seed_by_symbol: dict[str, tuple[Any, ...]] = {}
    if seed_scope is not None:
        cursor.execute(
            "SELECT source_symbol, s1_state, s1_reasons, s2_state, s2_reasons, "
            "source_seed_state, source_seed_reasons, safety_state, safety_reasons, "
            "target_close_quality, rps5_numerator, rps5_denominator, rps5_quality, "
            "rps20_numerator, rps20_denominator, rps20_quality "
            "FROM source_pool_seed_day_facts "
            "WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s "
            "AND profile_revision=%s AND producer_contract=%s AND producer_version=%s "
            "AND history_session_count=%s AND calendar_id=%s AND calendar_revision=%s "
            "AND research_mode=%s AND price_coordinate=%s AND source_symbol=ANY(%s) "
            "ORDER BY source_symbol",
            (
                target_session,
                source_package_id,
                target_capture_id,
                *seed_scope[:8],
                symbols,
            ),
        )
        seed_by_symbol = {row[0]: row for row in cursor.fetchall()}

    cursor.execute(
        "SELECT source_symbol, count(*)::bigint, "
        "count(*) FILTER (WHERE material_change_anchor_count>0)::bigint, "
        "coalesce(sum(material_change_anchor_count),0)::bigint, "
        "count(*) FILTER (WHERE target_price_quality='OBSERVED')::bigint, "
        "count(*) FILTER (WHERE target_price_quality='UNKNOWN')::bigint, "
        "array_agg(DISTINCT target_price_quality ORDER BY target_price_quality) "
        f"FROM source_d1_structure_day_facts WHERE {d1_predicate} "
        "AND source_symbol=ANY(%s) GROUP BY source_symbol ORDER BY source_symbol",
        d1_params + (symbols,),
    )
    d1_by_symbol = {row[0]: row[1:] for row in cursor.fetchall()}

    opposite_kind = "CONCEPT" if board_kind == "INDUSTRY" else "INDUSTRY"
    opposite_snapshot = next(
        (snapshot_id for snapshot_id, kind in snapshot_specs if kind == opposite_kind),
        None,
    )
    overlap_count = 0
    overlap_boards: list[dict[str, Any]] = []
    if opposite_snapshot is not None:
        cursor.execute(
            "SELECT count(DISTINCT m.source_symbol) FROM tdx_sector_catalog c "
            "JOIN tdx_sector_members m ON m.catalog_id=c.id "
            "AND m.board_kind=c.board_kind AND m.board_native_id=c.board_native_id "
            "WHERE c.snapshot_id=%s AND c.board_kind=%s AND m.source_symbol=ANY(%s)",
            (opposite_snapshot, opposite_kind, symbols),
        )
        overlap_count = int(cursor.fetchone()[0] or 0)
        cursor.execute(
            "SELECT c.board_native_id, c.board_name, count(DISTINCT m.source_symbol)::bigint "
            "FROM tdx_sector_catalog c JOIN tdx_sector_members m "
            "ON m.catalog_id=c.id AND m.board_kind=c.board_kind "
            "AND m.board_native_id=c.board_native_id "
            "WHERE c.snapshot_id=%s AND c.board_kind=%s AND m.source_symbol=ANY(%s) "
            "GROUP BY c.board_native_id,c.board_name "
            "ORDER BY count(DISTINCT m.source_symbol) DESC,c.board_name,c.board_native_id LIMIT 5",
            (opposite_snapshot, opposite_kind, symbols),
        )
        overlap_boards = [
            {
                "board_native_id": row[0],
                "board_name": row[1],
                "overlap_member_count": int(row[2]),
            }
            for row in cursor.fetchall()
        ]

    members: list[dict[str, Any]] = []
    state_counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0, "NOT_COMPUTED": 0}
    transition_counts: dict[str, int] = {}
    seed_counts = {"TRUE": 0, "FALSE": 0, "UNKNOWN": 0, "NOT_COMPUTED": 0}
    d1_anchor_members = 0
    d1_anchor_count = 0
    d1_price_observed_members = 0
    d1_price_unknown_members = 0
    d1_price_observed_structure_rows = 0
    d1_price_unknown_structure_rows = 0
    d1_price_quality_counts: dict[str, int] = {}
    rps5_values: dict[str, Fraction] = {}
    rps20_values: dict[str, Fraction] = {}
    for symbol, membership_quality in member_rows:
        d1 = d1_by_symbol.get(symbol)
        in_target_source_pool = symbol in target_source_symbols
        d0 = d0_by_symbol.get(symbol)
        after_state = d0.get("after_state") if d0 else None
        d0_state = (
            after_state
            if after_state in {"TRUE", "FALSE", "UNKNOWN"}
            else "NOT_COMPUTED"
        )
        if in_target_source_pool:
            state_counts[d0_state] += 1
        transition = d0.get("transition") if d0 else None
        if in_target_source_pool and isinstance(transition, str):
            transition_counts[transition] = transition_counts.get(transition, 0) + 1
        seed = seed_by_symbol.get(symbol)
        if not in_target_source_pool:
            seed_metric: dict[str, Any] = {"status": "NOT_APPLICABLE"}
        else:
            seed_state = (
                seed[5]
                if seed and seed[5] in {"TRUE", "FALSE", "UNKNOWN"}
                else "NOT_COMPUTED"
                if seed_scope_status == "NOT_COMPUTED"
                else "UNKNOWN"
            )
            seed_counts[seed_state] += 1
            seed_metric = {
                "status": "NOT_COMPUTED"
                if seed_scope_status == "NOT_COMPUTED"
                else "UNKNOWN"
            }
        if seed and in_target_source_pool:
            seed_metric = {
                "status": "AVAILABLE"
                if seed_scope_status == "AVAILABLE"
                else "PARTIAL",
                "s1": seed[1] or "UNKNOWN",
                "s2": seed[3] or "UNKNOWN",
                "source_seed": seed[5] or "UNKNOWN",
                "safety": seed[7] or "UNKNOWN",
                "target_close_quality": seed[9] or "UNKNOWN",
                "rps5": None,
                "rps5_quality": seed[12] or "UNKNOWN",
                "rps5_board_rank": None,
                "rps5_board_rank_denominator": 0,
                "rps5_board_tie_count": 0,
                "rps5_value_scope": "FULL_SOURCE_POOL_RPS",
                "rps5_rank_scope": "CURRENT_BOARD_OBSERVED_MEMBERS",
                "rps20": None,
                "rps20_quality": seed[15] or "UNKNOWN",
                "rps20_board_rank": None,
                "rps20_board_rank_denominator": 0,
                "rps20_board_tie_count": 0,
                "rps20_value_scope": "FULL_SOURCE_POOL_RPS",
                "rps20_rank_scope": "CURRENT_BOARD_OBSERVED_MEMBERS",
            }
            for name, numerator, denominator, quality, destination in (
                ("rps5", seed[10], seed[11], seed[12], rps5_values),
                ("rps20", seed[13], seed[14], seed[15], rps20_values),
            ):
                if (
                    numerator is not None
                    and denominator is not None
                    and int(denominator) > 0
                ):
                    ratio = Fraction(int(numerator), int(denominator))
                    seed_metric[name] = f"{ratio.numerator}/{ratio.denominator}"
                    if in_target_source_pool and quality == "OBSERVED":
                        destination[symbol] = ratio
        if d1:
            anchors_for_member = int(d1[2] or 0)
            d1_anchor_count += anchors_for_member
            d1_anchor_members += int(int(d1[1] or 0) > 0)
            d1_price_observed_structure_rows += int(d1[3] or 0)
            d1_price_unknown_structure_rows += int(d1[4] or 0)
            quality_set = set(d1[5] or ())
            d1_price_observed_members += int("OBSERVED" in quality_set)
            d1_price_unknown_members += int("UNKNOWN" in quality_set)
            price_quality = (
                "OBSERVED"
                if quality_set == {"OBSERVED"}
                else "UNKNOWN"
                if "UNKNOWN" in quality_set
                else next(iter(quality_set))
                if len(quality_set) == 1
                else "PARTIAL"
            )
            d1_price_quality_counts[price_quality] = (
                d1_price_quality_counts.get(price_quality, 0) + 1
            )
            d1_view = {
                "status": "AVAILABLE"
                if int(d1[0]) == len(_STRUCTURE_TYPES)
                else "PARTIAL",
                "structure_rows": int(d1[0]),
                "expected_structure_rows": len(_STRUCTURE_TYPES),
                "target_price_quality": price_quality,
                "material_change_members": int(int(d1[1] or 0) > 0),
                "material_anchor_count": anchors_for_member,
            }
        else:
            d1_view = {
                "status": "NOT_COMPUTED",
                "structure_rows": 0,
                "expected_structure_rows": len(_STRUCTURE_TYPES),
                "target_price_quality": "UNKNOWN",
                "material_change_members": 0,
                "material_anchor_count": 0,
            }
        members.append(
            {
                "source_symbol": symbol,
                "membership_quality": membership_quality,
                "source_pool_status": "IN_TARGET_SOURCE_POOL"
                if in_target_source_pool
                else "OUTSIDE_TARGET_SOURCE_POOL",
                "d0": {
                    "before_state": d0.get("before_state") if d0 else None,
                    "after_state": d0_state,
                    "transition": transition or "NOT_COMPUTED",
                },
                "seed": seed_metric,
                "d1": d1_view,
            }
        )
    for metric_name, values in (("rps5", rps5_values), ("rps20", rps20_values)):
        ordered_values = sorted(set(values.values()), reverse=True)
        ranks = {value: index + 1 for index, value in enumerate(ordered_values)}
        tie_counts = Counter(values.values())
        for member in members:
            seed_metric = member["seed"]
            if member["source_symbol"] in values:
                value = values[member["source_symbol"]]
                seed_metric[f"{metric_name}_board_rank"] = ranks[value]
                seed_metric[f"{metric_name}_board_rank_denominator"] = len(values)
                seed_metric[f"{metric_name}_board_tie_count"] = tie_counts[value]

    state_order = {"TRUE": 0, "FALSE": 1, "UNKNOWN": 2, "NOT_COMPUTED": 3}
    members.sort(
        key=lambda member: (
            member["source_pool_status"] != "IN_TARGET_SOURCE_POOL",
            state_order.get(member["d0"]["after_state"], 3),
            member["source_symbol"],
        )
    )
    page = members[offset : offset + limit]
    return {
        "status": "AVAILABLE",
        "board_kind": board_kind,
        "board_native_id": board_native_id,
        "membership_scope": "NON_PIT_CURRENT_SNAPSHOT",
        "snapshot_member_count": len(member_rows),
        "source_pool": {
            "inside": sum(
                member["source_pool_status"] == "IN_TARGET_SOURCE_POOL"
                for member in members
            ),
            "outside": sum(
                member["source_pool_status"] == "OUTSIDE_TARGET_SOURCE_POOL"
                for member in members
            ),
        },
        "d0": {
            "true": state_counts["TRUE"],
            "false": state_counts["FALSE"],
            "unknown": state_counts["UNKNOWN"],
            "missing": state_counts["NOT_COMPUTED"],
            "target_source_pool_denominator": sum(
                member["source_pool_status"] == "IN_TARGET_SOURCE_POOL"
                for member in members
            ),
            "outside_target_source_pool": sum(
                member["source_pool_status"] == "OUTSIDE_TARGET_SOURCE_POOL"
                for member in members
            ),
            "transitions": transition_counts,
        },
        "seed": {
            "scope_status": seed_scope_status,
            "history_session_count": int(seed_scope[3])
            if seed_scope is not None
            else None,
            "target_source_pool_denominator": sum(
                member["source_pool_status"] == "IN_TARGET_SOURCE_POOL"
                for member in members
            ),
            "true": seed_counts["TRUE"],
            "false": seed_counts["FALSE"],
            "unknown": seed_counts["UNKNOWN"],
            "missing": seed_counts["NOT_COMPUTED"],
        },
        "d1": {
            "scope_status": "AVAILABLE"
            if len(d1_by_symbol)
            == sum(
                1
                for member in members
                if member["source_pool_status"] == "IN_TARGET_SOURCE_POOL"
            )
            and all(
                int(item[0]) == len(_STRUCTURE_TYPES) for item in d1_by_symbol.values()
            )
            else "PARTIAL",
            "material_change_members": d1_anchor_members,
            "material_anchor_count": d1_anchor_count,
            "target_price_observed": d1_price_observed_members,
            "target_price_unknown": d1_price_unknown_members,
            "target_price_observed_structure_rows": d1_price_observed_structure_rows,
            "target_price_unknown_structure_rows": d1_price_unknown_structure_rows,
            "target_price_quality_counts": d1_price_quality_counts,
        },
        "cross_kind_overlap": {
            "kind": opposite_kind,
            "member_count": overlap_count,
            "top_boards": overlap_boards,
        },
        "pagination": {
            "offset": offset,
            "limit": limit,
            "total_count": len(members),
            "next_offset": offset + limit if offset + limit < len(members) else None,
        },
        "members": page,
    }


def read_source_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    d1_history_session_count: int,
    offset: int = 0,
    limit: int = 30,
    symbol: str | None = None,
    industry_snapshot_id: int | None = None,
    concept_snapshot_id: int | None = None,
    board_kind: str | None = None,
    board_native_id: str | None = None,
    structure_type: str | None = None,
    material_change_reason: str | None = None,
    include_all_results: bool = False,
) -> dict[str, Any]:
    """只读分页读取PG事实；未迁移或未计算时返回明确状态，不重算回退。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
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

    def incomplete_result(reason: str, **details: Any) -> dict[str, Any]:
        return {
            "storage_status": "INCOMPLETE",
            "target_session": target_session.isoformat(),
            "source_package_id": source_package_id,
            "d1_history_session_count": d1_history_session_count,
            "results": [],
            "reason": reason,
            "integrity_details": details,
        }

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            "SELECT to_regclass('source_d0_launch_day_facts') IS NOT NULL, "
            "to_regclass('source_d1_structure_day_facts') IS NOT NULL, "
            "to_regclass('source_d1_material_anchor_day_facts') IS NOT NULL"
        )
        if cursor.fetchone() != (True, True, True):
            return {
                "storage_status": "NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "source_package_id": source_package_id,
                "d1_history_session_count": d1_history_session_count,
                "results": [],
                "reason": "R06事实迁移尚未应用",
            }
        cursor.execute(
            "SELECT DISTINCT target_capture_id, calendar_revision, "
            "producer_contract, producer_version, material_change_contract "
            "FROM source_d1_structure_day_facts "
            "WHERE target_session=%s AND source_package_id=%s "
            "AND d1_history_session_count=%s ORDER BY target_capture_id, calendar_revision, "
            "producer_contract, producer_version, material_change_contract",
            (target_session, source_package_id, d1_history_session_count),
        )
        d1_scopes = cursor.fetchall()
        if not d1_scopes:
            return {
                "storage_status": "NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "source_package_id": source_package_id,
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
            producer_contract,
            d1_producer_version,
            material_change_contract,
        ) = d1_scopes[0]
        d1_predicate = (
            "target_session=%s AND source_package_id=%s "
            "AND target_capture_id=%s AND d1_history_session_count=%s "
            "AND calendar_revision=%s "
            "AND producer_contract=%s AND producer_version=%s "
            "AND material_change_contract=%s"
        )
        d1_params = (
            target_session,
            source_package_id,
            target_capture_id,
            d1_history_session_count,
            calendar_revision,
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
            if type(supplied_id) is not int or supplied_id <= 0:
                raise ResearchDayPgError(
                    "RESEARCH_DAY_MEMBER_SNAPSHOT_INVALID",
                    "板块成员快照必须是正整数ID",
                )
            cursor.execute(
                "SELECT source_kind, research_mode FROM tdx_sector_source_snapshots "
                "WHERE snapshot_id=%s",
                (supplied_id,),
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
            snapshot_specs.append((supplied_id, kind))
        cursor.execute(
            f"SELECT before_session, history_window_start, history_window_end, "
            f"calendar_id, calendar_revision, producer_contract, material_change_contract, "
            f"source_time_quality, source_clock_role, source_available_at, research_mode, "
            f"price_coordinate, min(source_package_id), count(DISTINCT source_package_id), "
            f"count(DISTINCT source_symbol), count(*) "
            f"FROM source_d1_structure_day_facts WHERE {d1_predicate} "
            f"GROUP BY before_session, history_window_start, history_window_end, calendar_id, "
            f"calendar_revision, producer_contract, material_change_contract, "
            f"source_time_quality, source_clock_role, source_available_at, research_mode, price_coordinate",
            d1_params,
        )
        contexts = cursor.fetchall()
        if not contexts:
            return incomplete_result("D1单日事实缺少窗口或来源质量上下文")
        if len(contexts) != 1:
            raise ResearchDayPgError(
                "RESEARCH_DAY_D1_CONTEXT_AMBIGUOUS", "D1单日事实上下文不唯一"
            )
        d1_context = contexts[0]
        before_session = d1_context[0]
        source_package_id = d1_context[12]
        if d1_context[13] != 1:
            return incomplete_result(
                "D1逐股事实绑定多个来源数据包",
                distinct_source_package_count=d1_context[13],
            )
        d1_source_count = d1_context[-2]
        d1_row_count = d1_context[-1]
        if d1_row_count != d1_source_count * len(_STRUCTURE_TYPES):
            return incomplete_result(
                "D1逐股五类结构行数不闭合",
                source_symbol_count=d1_source_count,
                observed_structure_rows=d1_row_count,
                expected_structure_rows=d1_source_count * len(_STRUCTURE_TYPES),
            )
        cursor.execute(
            f"SELECT structure_type, count(*), count(DISTINCT source_symbol) "
            f"FROM source_d1_structure_day_facts WHERE {d1_predicate} "
            "GROUP BY structure_type ORDER BY structure_type",
            d1_params,
        )
        type_counts = cursor.fetchall()
        if {row[0] for row in type_counts} != set(_STRUCTURE_TYPES) or any(
            row[1] != d1_source_count or row[2] != d1_source_count
            for row in type_counts
        ):
            return incomplete_result(
                "D1五类结构逐股分母不一致",
                structure_type_counts=[list(row) for row in type_counts],
                source_symbol_count=d1_source_count,
            )
        cursor.execute(
            f"SELECT count(*), sum(persisted_detail_count) "
            f"FROM source_d1_structure_day_facts WHERE {d1_predicate}",
            d1_params,
        )
        d1_rows_count, expected_anchor_count = cursor.fetchone()
        cursor.execute(
            "SELECT count(*) FROM source_d1_material_anchor_day_facts "
            "WHERE d1_structure_id = ANY (SELECT id FROM source_d1_structure_day_facts "
            f"WHERE {d1_predicate})",
            d1_params,
        )
        actual_anchor_count = cursor.fetchone()[0]
        if actual_anchor_count != expected_anchor_count:
            return incomplete_result(
                "D1锚明细与逐股分母不一致",
                expected_anchor_rows=expected_anchor_count,
                observed_anchor_rows=actual_anchor_count,
            )
        cursor.execute(
            "SELECT DISTINCT before_capture_id, producer_version, algorithm_contract, session_contract, "
            "before_source_count, target_source_count, union_source_count, before_window_start, "
            "target_window_start, calendar_id, calendar_revision, "
            "source_time_quality, source_clock_role, source_available_at, research_mode, price_coordinate, "
            "source_package_id, before_session "
            "FROM source_d0_launch_day_facts WHERE target_session=%s "
            "AND source_package_id=%s AND target_capture_id=%s ORDER BY before_capture_id, producer_version, session_contract",
            (target_session, source_package_id, target_capture_id),
        )
        d0_scopes = cursor.fetchall()
        if not d0_scopes:
            return incomplete_result("精确目标D0事实缺失")
        if len(d0_scopes) != 1:
            raise ResearchDayPgError(
                "RESEARCH_DAY_D0_SCOPE_MISSING_OR_AMBIGUOUS",
                "目标D0事实缺失或读取键不唯一",
            )
        d0_scope = d0_scopes[0]
        if d0_scope[16] != source_package_id:
            return incomplete_result(
                "D0与D1绑定的来源数据包编号不一致",
                d0_source_package_id=d0_scope[16],
                d1_source_package_id=source_package_id,
            )
        if d0_scope[10] != calendar_revision or d0_scope[17] != before_session:
            return incomplete_result(
                "D0与D1目标日、前态或官方日历上下文不一致",
                d0_before_session=_json_value(d0_scope[17]),
                d1_before_session=_json_value(before_session),
                d0_calendar_revision=d0_scope[10],
                d1_calendar_revision=calendar_revision,
            )
        d0_predicate = (
            "target_session=%s AND source_package_id=%s "
            "AND before_capture_id=%s AND target_capture_id=%s "
            "AND producer_version=%s AND algorithm_contract=%s AND session_contract=%s "
            "AND calendar_revision=%s"
        )
        d0_params = (
            target_session,
            source_package_id,
            d0_scope[0],
            target_capture_id,
            d0_scope[1],
            d0_scope[2],
            d0_scope[3],
            d0_scope[10],
        )
        cursor.execute(
            "SELECT count(*), count(DISTINCT source_symbol), min(union_source_count), "
            "max(union_source_count), count(*) FILTER (WHERE after_state IS NOT NULL) "
            f"FROM source_d0_launch_day_facts WHERE {d0_predicate}",
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
                "D0并集分母或目标态股票数不闭合",
                observed_d0_rows=d0_counts[0],
                distinct_d0_symbols=d0_counts[1],
                declared_union_count=d0_scope[6],
                d0_target_symbols=d0_counts[4],
                d1_target_symbols=d1_source_count,
            )
        cursor.execute(
            "SELECT EXISTS ((SELECT DISTINCT source_symbol FROM source_d1_structure_day_facts "
            f"WHERE {d1_predicate}) EXCEPT (SELECT source_symbol FROM source_d0_launch_day_facts "
            f"WHERE {d0_predicate} AND after_state IS NOT NULL)) "
            "OR EXISTS ((SELECT source_symbol FROM source_d0_launch_day_facts "
            f"WHERE {d0_predicate} AND after_state IS NOT NULL) "
            f"EXCEPT (SELECT DISTINCT source_symbol FROM source_d1_structure_day_facts WHERE {d1_predicate}))",
            d1_params + d0_params + d0_params + d1_params,
        )
        if cursor.fetchone()[0]:
            return incomplete_result("D0目标态与D1逐股集合不一致")
        cursor.execute(
            "SELECT before_state, after_state, transition, count(*) FROM source_d0_launch_day_facts "
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
            f"FROM source_d1_structure_day_facts WHERE {d1_predicate}",
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
            "d.target_session=%s AND d.source_package_id=%s "
            "AND d.target_capture_id=%s AND d.d1_history_session_count=%s "
            "AND d.calendar_revision=%s "
            "AND d.producer_contract=%s AND d.producer_version=%s "
            "AND d.material_change_contract=%s"
        )
        filters = []
        filter_params = []
        if structure_type is not None:
            filters.extend(("d.structure_type=%s", "d.material_change_anchor_count>0"))
            filter_params.append(structure_type)
        if material_change_reason is not None:
            filters.append(
                "EXISTS (SELECT 1 FROM source_d1_material_anchor_day_facts a "
                "WHERE a.d1_structure_id=d.id "
                "AND a.material_change_classification='MATERIAL_CHANGE' "
                + "AND %s = ANY(a.material_change_reasons))"
            )
            filter_params.append(material_change_reason)
        if board_kind is not None and board_native_id is not None:
            snapshot_id = next(
                snapshot for snapshot, kind in snapshot_specs if kind == board_kind
            )
            filters.append(
                "EXISTS (SELECT 1 FROM tdx_sector_catalog c "
                "JOIN tdx_sector_members m ON m.catalog_id=c.id "
                "WHERE c.snapshot_id=%s AND c.board_kind=%s AND c.board_native_id=%s "
                "AND m.source_symbol=d.source_symbol)"
            )
            filter_params.extend((snapshot_id, board_kind, board_native_id))
        if symbol is not None:
            filters.append("d.source_symbol=%s")
            filter_params.append(symbol)
        filter_sql = "".join(f" AND ({item})" for item in filters)
        selected_query = (
            "SELECT d.source_symbol, d.market, d.board "
            "FROM source_d1_structure_day_facts d "
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
        for snapshot_id, kind in snapshot_specs:
            anchor_filters = ["a.material_change_classification='MATERIAL_CHANGE'"]
            anchor_params = []
            if structure_type is not None:
                anchor_filters.append("d.structure_type=%s")
                anchor_params.append(structure_type)
            if material_change_reason is not None:
                anchor_filters.append("%s = ANY(a.material_change_reasons)")
                anchor_params.append(material_change_reason)
            cursor.execute(
                "WITH source_symbols AS (SELECT DISTINCT source_symbol "
                f"FROM source_d1_structure_day_facts WHERE {d1_predicate} "
                "), selected_anchors AS (SELECT d.source_symbol, count(*) AS material_anchor_count "
                "FROM source_d1_structure_day_facts d "
                "JOIN source_d1_material_anchor_day_facts a ON a.d1_structure_id=d.id "
                f"WHERE {d1_alias_predicate} AND "
                + " AND ".join(anchor_filters)
                + " GROUP BY d.source_symbol), stock_summary AS (SELECT s.source_symbol, "
                "coalesce(a.material_anchor_count, 0) AS material_anchor_count "
                "FROM source_symbols s LEFT JOIN selected_anchors a USING (source_symbol)) "
                "SELECT c.board_kind, c.board_native_id, c.board_name, "
                "count(DISTINCT m.source_symbol) FILTER (WHERE s.source_symbol IS NOT NULL), "
                "count(DISTINCT m.source_symbol) FILTER (WHERE s.material_anchor_count>0), "
                "coalesce(sum(s.material_anchor_count), 0) "
                "FROM tdx_sector_catalog c JOIN tdx_sector_members m "
                "ON m.catalog_id=c.id AND m.board_kind=c.board_kind "
                "AND m.board_native_id=c.board_native_id "
                "LEFT JOIN stock_summary s ON s.source_symbol=m.source_symbol "
                "WHERE c.snapshot_id=%s AND c.board_kind=%s "
                "GROUP BY c.board_kind, c.board_native_id, c.board_name "
                "ORDER BY c.board_name, c.board_native_id OFFSET %s LIMIT %s",
                d1_params
                + d1_params
                + tuple(anchor_params)
                + (snapshot_id, kind, offset, limit),
            )
            board_summaries.extend(
                {
                    "board_kind": row[0],
                    "board_native_id": row[1],
                    "board_name": row[2],
                    "source_stock_count": row[3],
                    "material_change_stock_count": row[4],
                    "material_change_anchor_count": int(row[5]),
                    "snapshot_id": snapshot_id,
                    "membership_scope": "NON_PIT_CURRENT_SNAPSHOT",
                }
                for row in cursor.fetchall()
            )
            cursor.execute(
                "WITH source_symbols AS (SELECT DISTINCT source_symbol "
                f"FROM source_d1_structure_day_facts WHERE {d1_predicate}) "
                "SELECT count(*), count(*) FILTER (WHERE m.source_symbol IS NULL) "
                "FROM source_symbols s LEFT JOIN (SELECT DISTINCT c.snapshot_id, m.source_symbol "
                "FROM tdx_sector_members m JOIN tdx_sector_catalog c ON c.id=m.catalog_id "
                "WHERE c.snapshot_id=%s) m "
                "ON m.source_symbol=s.source_symbol",
                d1_params + (snapshot_id,),
            )
            coverage_total, unmapped_count = cursor.fetchone()
            membership_coverage.append(
                {
                    "board_kind": kind,
                    "snapshot_id": snapshot_id,
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
                f"SELECT {', '.join(_D1_COLUMNS)} FROM source_d1_structure_day_facts "
                f"WHERE {d1_predicate} AND source_symbol = ANY(%s) "
                "ORDER BY source_symbol, structure_type",
                d1_params + (page_symbols,),
            )
            d1_selected = [_as_dict(_D1_COLUMNS, row) for row in cursor.fetchall()]
            d0_columns = _D0_COLUMNS
            cursor.execute(
                f"SELECT {', '.join(d0_columns)} FROM source_d0_launch_day_facts "
                f"WHERE {d0_predicate} AND source_symbol = ANY(%s) ORDER BY source_symbol",
                d0_params + (page_symbols,),
            )
            d0_selected = [_as_dict(d0_columns, row) for row in cursor.fetchall()]
            cursor.execute(
                "SELECT d.source_symbol, d.structure_type, "
                + ", ".join(f"a.{column}" for column in _ANCHOR_COLUMNS)
                + " FROM source_d1_structure_day_facts d "
                "JOIN source_d1_material_anchor_day_facts a ON a.d1_structure_id=d.id "
                f"WHERE {d1_alias_predicate} AND d.source_symbol = ANY(%s) "
                "ORDER BY d.source_symbol, d.structure_type, a.created_session",
                d1_params + (page_symbols,),
            )
            anchors_selected = []
            for row in cursor.fetchall():
                anchor = _as_dict(_ANCHOR_COLUMNS, row[2:])
                anchor["source_symbol"] = row[0]
                anchor["structure_type"] = row[1]
                anchors_selected.append(anchor)
        memberships_by_symbol: dict[str, list[dict[str, Any]]] = {}
        if snapshot_specs and page_symbols:
            snapshot_ids = [snapshot for snapshot, _kind in snapshot_specs]
            cursor.execute(
                "SELECT m.source_symbol, c.board_kind, c.board_native_id, c.board_name, "
                "m.membership_quality, c.snapshot_id "
                "FROM tdx_sector_members m JOIN tdx_sector_catalog c "
                "ON c.id=m.catalog_id AND c.board_kind=m.board_kind "
                "AND c.board_native_id=m.board_native_id "
                "WHERE c.snapshot_id = ANY(%s) AND m.source_symbol = ANY(%s) "
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
                        "snapshot_id": row[5],
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
        selected_board_observation = None
        if board_kind is not None and board_native_id is not None:
            selected_board_observation = _read_selected_board_observation(
                cursor,
                target_session=target_session,
                source_package_id=source_package_id,
                target_capture_id=target_capture_id,
                d1_history_session_count=d1_history_session_count,
                d1_predicate=d1_predicate,
                d1_params=d1_params,
                d0_predicate=d0_predicate,
                d0_params=d0_params,
                snapshot_specs=snapshot_specs,
                board_kind=board_kind,
                board_native_id=board_native_id,
                offset=offset,
                limit=limit,
            )
        return {
            "storage_status": "SAVED",
            "target_session": target_session.isoformat(),
            "before_session": _json_value(before_session),
            "source_package_id": source_package_id,
            "target_capture_id": target_capture_id,
            "d1_history_session_count": d1_history_session_count,
            "calendar_revision": calendar_revision,
            "d1_producer_contract": producer_contract,
            "d1_producer_version": d1_producer_version,
            "d1_material_change_contract": material_change_contract,
            "research_mode": "NON_PIT_RETROSPECTIVE",
            "price_coordinate": "RAW_UNADJUSTED",
            "result_status": result_status,
            "membership_snapshots": [
                {
                    "board_kind": kind,
                    "snapshot_id": snapshot_id,
                    "scope": "NON_PIT_CURRENT_SNAPSHOT",
                }
                for snapshot_id, kind in snapshot_specs
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
            "selected_board_observation": selected_board_observation,
            "execution": {
                "database_mode": "READ_ONLY",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            },
        }


def read_source_research_day_scope(
    config: Mapping[str, Any], *, target_session: date, source_package_id: int
) -> dict[str, Any]:
    """读取单日D1 typed事实的唯一已保存窗口键，不计算或修改事实。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
    ):
        raise ResearchDayPgError("RESEARCH_DAY_QUERY_INVALID", "PG研究事实范围键无效")
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        cursor.execute(
            "SELECT to_regclass('source_d1_structure_day_facts') IS NOT NULL"
        )
        if cursor.fetchone() != (True,):
            return {
                "status": "NOT_COMPUTED",
                "target_session": target_session.isoformat(),
                "source_package_id": source_package_id,
                "d1_history_session_count": None,
                "scope_count": 0,
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            }
        cursor.execute(
            "SELECT DISTINCT d1_history_session_count, target_capture_id, "
            "calendar_revision, producer_contract, producer_version, "
            "material_change_contract FROM source_d1_structure_day_facts "
            "WHERE target_session=%s AND source_package_id=%s "
            "ORDER BY d1_history_session_count, target_capture_id, calendar_revision, "
            "producer_contract, producer_version, material_change_contract",
            (target_session, source_package_id),
        )
        scopes = cursor.fetchall()
    if not scopes:
        return {
            "status": "NOT_COMPUTED",
            "target_session": target_session.isoformat(),
            "source_package_id": source_package_id,
            "d1_history_session_count": None,
            "scope_count": 0,
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
        }
    if len(scopes) != 1:
        raise ResearchDayPgError(
            "RESEARCH_DAY_SCOPE_AMBIGUOUS",
            "同一目标日和来源包对应多个D1静态事实读取键",
        )
    return {
        "status": "SAVED_SCOPE_UNIQUE",
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "d1_history_session_count": scopes[0][0],
        "target_capture_id": scopes[0][1],
        "calendar_revision": scopes[0][2],
        "producer_contract": scopes[0][3],
        "producer_version": scopes[0][4],
        "material_change_contract": scopes[0][5],
        "scope_count": 1,
        "database_write_attempted": False,
        "ddl_executed": False,
        "dml_executed": False,
    }


def read_source_board_comparison(
    config: Mapping[str, Any],
    *,
    target_session: date,
    source_package_id: int,
    d1_history_session_count: int,
    snapshot_ids: Mapping[str, int | None],
    board_a_kind: str,
    board_a_native_id: str,
    board_b_kind: str,
    board_b_native_id: str,
) -> dict[str, Any]:
    """在同一只读快照内比较两个当前板块及其已存来源级事实。"""
    if (
        type(target_session) is not date
        or type(source_package_id) is not int
        or source_package_id <= 0
        or type(d1_history_session_count) is not int
        or not 1 <= d1_history_session_count <= 32767
        or not isinstance(snapshot_ids, Mapping)
        or set(snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or board_a_kind not in {"INDUSTRY", "CONCEPT"}
        or board_b_kind not in {"INDUSTRY", "CONCEPT"}
        or not isinstance(board_a_native_id, str)
        or not board_a_native_id.strip()
        or not isinstance(board_b_native_id, str)
        or not board_b_native_id.strip()
    ):
        raise ResearchDayPgError("BOARD_COMPARISON_QUERY_INVALID", "双板块比较键无效")
    selected = (
        (board_a_kind, board_a_native_id),
        (board_b_kind, board_b_native_id),
    )
    if selected[0] == selected[1]:
        raise ResearchDayPgError(
            "BOARD_COMPARISON_DUPLICATE", "请选择两个不同的精确板块"
        )
    for kind, snapshot_id in snapshot_ids.items():
        if snapshot_id is not None and (
            type(snapshot_id) is not int or snapshot_id <= 0
        ):
            raise ResearchDayPgError(
                "BOARD_COMPARISON_SNAPSHOT_INVALID", "板块快照编号无效"
            )
    for kind, _native_id in selected:
        if snapshot_ids[kind] is None:
            raise ResearchDayPgError(
                "BOARD_COMPARISON_SNAPSHOT_MISSING",
                f"所选{kind}板块缺少精确成员快照",
            )

    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _,
        cursor,
        _connection_identity,
    ):
        for kind, snapshot_id in snapshot_ids.items():
            if snapshot_id is None:
                continue
            expected_source_kind = (
                "TDX_TDXHY_CFG" if kind == "INDUSTRY" else "TDX_INFOHARBOR_BLOCK_DAT"
            )
            cursor.execute(
                "SELECT source_kind,research_mode FROM tdx_sector_source_snapshots "
                "WHERE snapshot_id=%s",
                (snapshot_id,),
            )
            if cursor.fetchone() != (
                expected_source_kind,
                "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
            ):
                raise ResearchDayPgError(
                    "BOARD_COMPARISON_SNAPSHOT_MISMATCH",
                    f"{kind}成员快照来源或NON_PIT身份不匹配",
                )

        cursor.execute(
            "SELECT DISTINCT r.source_capture_id,c.normalized_count "
            "FROM source_raw_daily r JOIN source_captures c "
            "ON c.capture_id=r.source_capture_id "
            "AND c.source_package_id=r.source_package_id "
            "AND c.target_session=r.session_date "
            "WHERE r.session_date=%s AND r.source_package_id=%s "
            "ORDER BY r.source_capture_id LIMIT 2",
            (target_session, source_package_id),
        )
        captures = cursor.fetchall()
        if len(captures) != 1:
            raise ResearchDayPgError(
                "BOARD_COMPARISON_SOURCE_VERSION_AMBIGUOUS",
                "目标日与来源包没有唯一Raw捕获",
            )
        target_capture_id, expected_source_count = captures[0]
        cursor.execute(
            "SELECT source_symbol FROM source_raw_daily "
            "WHERE session_date=%s AND source_package_id=%s "
            "AND source_capture_id=%s ORDER BY source_symbol",
            (target_session, source_package_id, target_capture_id),
        )
        source_symbols = {row[0] for row in cursor.fetchall()}
        source_pool_status = (
            "AVAILABLE"
            if len(source_symbols) == expected_source_count
            else "INCOMPLETE"
        )

        board_sets: list[set[str]] = []
        board_sizes: list[int] = []
        for kind, native_id in selected:
            cursor.execute(
                "SELECT c.id FROM tdx_sector_catalog c "
                "WHERE c.snapshot_id=%s AND c.board_kind=%s AND c.board_native_id=%s",
                (snapshot_ids[kind], kind, native_id),
            )
            catalog_ids = [int(row[0]) for row in cursor.fetchall()]
            if len(catalog_ids) != 1:
                raise ResearchDayPgError(
                    "BOARD_COMPARISON_BOARD_NOT_UNIQUE",
                    "精确板块在绑定快照中不存在或不唯一",
                )
            cursor.execute(
                "SELECT source_symbol FROM tdx_sector_members "
                "WHERE catalog_id=%s ORDER BY source_symbol",
                (catalog_ids[0],),
            )
            members = {row[0] for row in cursor.fetchall()}
            board_sizes.append(len(members))
            board_sets.append(members)

        cursor.execute(
            "SELECT DISTINCT before_capture_id,producer_version,algorithm_contract,"
            "session_contract,calendar_revision FROM source_d0_launch_day_facts "
            "WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s "
            "ORDER BY before_capture_id,producer_version,algorithm_contract",
            (target_session, source_package_id, target_capture_id),
        )
        d0_scopes = cursor.fetchall()
        d0_by_symbol: dict[str, tuple[str | None, str | None]] = {}
        d0_status = "NOT_COMPUTED"
        if len(d0_scopes) == 1:
            d0_scope = d0_scopes[0]
            d0_predicate = (
                "target_session=%s AND source_package_id=%s "
                "AND before_capture_id=%s AND target_capture_id=%s "
                "AND producer_version=%s AND algorithm_contract=%s "
                "AND session_contract=%s AND calendar_revision=%s"
            )
            d0_params = (
                target_session,
                source_package_id,
                d0_scope[0],
                target_capture_id,
                d0_scope[1],
                d0_scope[2],
                d0_scope[3],
                d0_scope[4],
            )
            cursor.execute(
                "SELECT source_symbol,transition,after_state "
                f"FROM source_d0_launch_day_facts WHERE {d0_predicate}",
                d0_params,
            )
            rows = cursor.fetchall()
            d0_by_symbol = {row[0]: (row[1], row[2]) for row in rows}
            d0_status = (
                "AVAILABLE"
                if len(d0_by_symbol) == len(source_symbols)
                else "INCOMPLETE"
            )

        cursor.execute(
            "SELECT DISTINCT profile_revision,producer_contract,producer_version,"
            "history_session_count,calendar_id,calendar_revision,research_mode,"
            "price_coordinate FROM source_pool_seed_day_facts "
            "WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s "
            "ORDER BY profile_revision,producer_version",
            (target_session, source_package_id, target_capture_id),
        )
        seed_scopes = cursor.fetchall()
        seed_by_symbol: dict[str, str] = {}
        seed_status = "NOT_COMPUTED"
        if len(seed_scopes) == 1:
            seed_scope = seed_scopes[0]
            if (
                seed_scope[6] == "NON_PIT_RETROSPECTIVE"
                and seed_scope[7] == "RAW_UNADJUSTED"
            ):
                cursor.execute(
                    "SELECT source_symbol,source_seed_state FROM source_pool_seed_day_facts "
                    "WHERE target_session=%s AND source_package_id=%s AND source_capture_id=%s "
                    "AND profile_revision=%s AND producer_contract=%s AND producer_version=%s "
                    "AND history_session_count=%s AND calendar_id=%s AND calendar_revision=%s "
                    "AND research_mode=%s AND price_coordinate=%s",
                    (target_session, source_package_id, target_capture_id, *seed_scope),
                )
                rows = cursor.fetchall()
                seed_by_symbol = {row[0]: row[1] or "UNKNOWN" for row in rows}
                seed_status = (
                    "AVAILABLE"
                    if len(seed_by_symbol) == len(source_symbols)
                    else "INCOMPLETE"
                )
            else:
                seed_status = "UNKNOWN"
        elif len(seed_scopes) > 1:
            seed_status = "AMBIGUOUS"

        cursor.execute(
            "SELECT DISTINCT d1_history_session_count,calendar_revision,"
            "producer_contract,producer_version,material_change_contract,"
            "research_mode,price_coordinate FROM source_d1_structure_day_facts "
            "WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s "
            "AND d1_history_session_count=%s "
            "ORDER BY calendar_revision,producer_contract,producer_version",
            (
                target_session,
                source_package_id,
                target_capture_id,
                d1_history_session_count,
            ),
        )
        d1_scopes = cursor.fetchall()
        d1_by_symbol: dict[str, tuple[str, int, int]] = {}
        d1_status = "NOT_COMPUTED"
        if len(d1_scopes) == 1:
            d1_scope = d1_scopes[0]
            if (
                d1_scope[5] == "NON_PIT_RETROSPECTIVE"
                and d1_scope[6] == "RAW_UNADJUSTED"
            ):
                cursor.execute(
                    "SELECT source_symbol,"
                    "array_agg(DISTINCT target_price_quality ORDER BY target_price_quality),"
                    "coalesce(sum(material_change_anchor_count),0)::bigint,count(*)::int "
                    "FROM source_d1_structure_day_facts "
                    "WHERE target_session=%s AND source_package_id=%s AND target_capture_id=%s "
                    "AND d1_history_session_count=%s AND calendar_revision=%s "
                    "AND producer_contract=%s AND producer_version=%s "
                    "AND material_change_contract=%s AND research_mode=%s "
                    "AND price_coordinate=%s "
                    "GROUP BY source_symbol",
                    (
                        target_session,
                        source_package_id,
                        target_capture_id,
                        *d1_scope,
                    ),
                )
                rows = cursor.fetchall()
                d1_by_symbol = {
                    row[0]: (
                        "OBSERVED"
                        if set(row[1] or ()) == {"OBSERVED"}
                        else "UNKNOWN"
                        if "UNKNOWN" in set(row[1] or ())
                        else "PARTIAL",
                        int(row[2] or 0),
                        int(row[3]),
                    )
                    for row in rows
                }
                d1_status = (
                    "AVAILABLE"
                    if len(d1_by_symbol) == len(source_symbols)
                    and all(
                        item[2] == len(_STRUCTURE_TYPES)
                        for item in d1_by_symbol.values()
                    )
                    else "INCOMPLETE"
                )
            else:
                d1_status = "UNKNOWN"
        elif len(d1_scopes) > 1:
            d1_status = "AMBIGUOUS"

    intersection = board_sets[0] & board_sets[1]
    union = board_sets[0] | board_sets[1]
    set_groups = {
        "board_a": board_sets[0],
        "board_b": board_sets[1],
        "intersection": intersection,
        "board_a_only": board_sets[0] - board_sets[1],
        "board_b_only": board_sets[1] - board_sets[0],
        "union_deduplicated": union,
    }

    def cohort_summary(symbols: set[str]) -> dict[str, Any]:
        in_source = symbols & source_symbols
        new_symbols = {
            symbol
            for symbol in in_source
            if d0_by_symbol.get(symbol, (None, None))[0] == "FALSE_TO_TRUE"
        }
        seed_symbols = in_source & seed_by_symbol.keys()
        d1_symbols = in_source & d1_by_symbol.keys()
        return {
            "membership_count": len(symbols),
            "source_pool_member_count": len(in_source),
            "source_pool_denominator": len(source_symbols),
            "d0_new_count": len(new_symbols) if d0_status == "AVAILABLE" else None,
            "seed": {
                "status": seed_status,
                "stored_member_denominator": len(seed_symbols),
                "true_count": sum(
                    seed_by_symbol[item] == "TRUE" for item in seed_symbols
                )
                if seed_status == "AVAILABLE"
                else None,
                "unknown_count": sum(
                    seed_by_symbol[item] == "UNKNOWN" for item in seed_symbols
                )
                if seed_status == "AVAILABLE"
                else None,
            },
            "target_price": {
                "status": d1_status,
                "stored_member_denominator": len(d1_symbols),
                "observed_count": sum(
                    d1_by_symbol[item][0] == "OBSERVED" for item in d1_symbols
                )
                if d1_status == "AVAILABLE"
                else None,
                "unknown_count": sum(
                    d1_by_symbol[item][0] == "UNKNOWN" for item in d1_symbols
                )
                if d1_status == "AVAILABLE"
                else None,
            },
            "d1_material_member_count": sum(
                d1_by_symbol[item][1] > 0 for item in d1_symbols
            )
            if d1_status == "AVAILABLE"
            else None,
        }

    return {
        "status": "AVAILABLE" if source_pool_status == "AVAILABLE" else "INCOMPLETE",
        "target_session": target_session.isoformat(),
        "source_package_id": source_package_id,
        "source_capture_id": target_capture_id,
        "source_pool_count": len(source_symbols),
        "d0_scope_status": d0_status,
        "seed_scope_status": seed_status,
        "d1_scope_status": d1_status,
        "boards": [
            {
                "board_kind": selected[index][0],
                "board_native_id": selected[index][1],
                "snapshot_member_count": board_sizes[index],
            }
            for index in range(2)
        ],
        "comparison": {
            name: cohort_summary(symbols) for name, symbols in set_groups.items()
        },
        "membership_overlap_count": len(intersection),
        "membership_union_deduplicated_count": len(union),
        "execution": {
            "transaction": "READ ONLY PG SNAPSHOT",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
        },
    }


__all__ = [
    "ResearchDayPgError",
    "persist_source_research_day",
    "read_source_research_day",
    "read_source_board_comparison",
    "read_source_research_day_scope",
]
