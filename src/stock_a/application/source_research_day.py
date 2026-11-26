"""将现役D0/D1单日生产者投影成可幂等持久化的typed事实。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
import re
import time
from typing import Any, Mapping

from stock_a.application.pg_price_profiles import (
    PG_PROFILE_MAX_HISTORY_SESSIONS,
    source_symbol_price_profile_snapshots,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_d0_raw import (
    MAX_SOURCE_SYMBOLS,
    _official_calendar,
    query_source_d0_raw_launch,
)
from stock_a.application.source_d1_structures import (
    D1_PRODUCER_VERSION_5,
    expected_d1_structure_types_for_version,
)
from stock_a.application.source_market_observation import (
    prepare_d1_material_observation,
)
from stock_a.application.source_pool_seed import (
    compute_source_pool_seed,
    validate_seed_worker_count,
)
from stock_a.application.runtime_config import _CONFIG_ROOT
from stock_a.contracts import load_price_profile_contract
from stock_a.domain.research_day_facts import (
    D0LaunchFact,
    D1MaterialAnchorFact,
    D1StructureFact,
    ExactRational,
    ResearchDayFacts,
)
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.source_forward_path import exploratory_raw_path
from stock_a.storage.postgres.research_day_facts import (
    persist_source_research_day,
)
from stock_a.storage.postgres.research_facts import (
    prepare_forward_path_rows,
    read_forward_path_scope,
    write_forward_path_rows,
)
from stock_a.storage.postgres.static_research_facts import (
    persist_static_d0_reference_scenario,
    persist_static_research_day,
    read_source_pool_seed_forward_scope,
)
from stock_a.storage.postgres.baostock_price_facts import (
    read_baostock_price_catalog,
    read_baostock_price_history_batch,
)
from stock_a.application.pg_research import read_source_version_context

D1_DEFAULT_HISTORY_SESSION_COUNT = 40

_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_D1_STATUS_COUNTS = (
    "MATERIAL_CHANGE",
    "COUNT_ONLY",
    "UNCHANGED",
    "UNKNOWN",
    "NOT_COMPARABLE",
    "NOT_EVALUABLE",
    "NOT_APPLICABLE",
)
_D1_STRUCTURE_TYPES = {
    "PIVOT_LOW",
    "PIVOT_HIGH",
    "RANGE_FLOOR",
    "RANGE_CEILING",
    "GAP_ZONE",
}
_D1_STRUCTURE_TYPES_WITH_IMPULSE = _D1_STRUCTURE_TYPES | {
    "IMPULSE_BODY",
    "IMPULSE_LOW",
}


class SourceResearchDayError(ValueError):
    """单日研究批次的生产者输出或分母未闭合。"""

    def __init__(
        self, code: str, message: str, *, database_write_attempted: bool = False
    ):
        self.code = code
        self.database_write_attempted = database_write_attempted
        super().__init__(message)


def persist_source_t1_forward_from_saved(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    horizon: int = 1,
    history_session_count: int | None = None,
    profile_revision: str | None = None,
) -> dict[str, Any]:
    """复用已存Raw、Seed和价格路径算法，生产指定期限（默认T+1）typed范围。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(horizon) is not int
        or horizon not in (1, 3, 5, 10, 20)
        or history_session_count is not None
        and (type(history_session_count) is not int or history_session_count < 1)
        or profile_revision is not None
        and (not isinstance(profile_revision, str) or not profile_revision)
    ):
        raise SourceResearchDayError(
            "T1_FORWARD_SCOPE_INVALID",
            "Forward目标、期限、研究截止或paired capture无效",
        )
    target_calendar, _, _, target_calendar_sessions = _load_calendar(target_session)
    _, _, _, cutoff_calendar_sessions = _load_calendar(research_cutoff)
    if (
        target_session not in target_calendar_sessions
        or research_cutoff not in cutoff_calendar_sessions
        or target_session not in cutoff_calendar_sessions
    ):
        raise SourceResearchDayError(
            "T1_FORWARD_CALENDAR_INVALID", "T+1范围不在已核验的官方日历内"
        )
    target_index = cutoff_calendar_sessions.index(target_session)
    if target_index + horizon >= len(cutoff_calendar_sessions):
        return {
            "status": "NOT_DUE",
            "horizon": horizon,
            "target_session": target_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "database_write": False,
            "ddl_executed": False,
            "source_collection": False,
            "raw_recalculated": False,
            "seed_recalculated": False,
        }
    endpoint_session = cutoff_calendar_sessions[target_index + horizon]
    if endpoint_session > research_cutoff:
        return {
            "status": "NOT_DUE",
            "horizon": horizon,
            "target_session": target_session.isoformat(),
            "endpoint_session": endpoint_session.isoformat(),
            "research_cutoff": research_cutoff.isoformat(),
            "source_capture_id": source_capture_id,
            "database_write": False,
            "ddl_executed": False,
            "source_collection": False,
            "raw_recalculated": False,
            "seed_recalculated": False,
        }

    target_calendar = _load_calendar(target_session)[0]
    endpoint_calendar = _load_calendar(endpoint_session)[0]
    if endpoint_calendar["revision"] != target_calendar["revision"]:
        raise SourceResearchDayError(
            "T1_FORWARD_CALENDAR_REVISION_MISMATCH",
            "T0与T+1使用不同年度日历修订，现有路径业务键不能安全表达该跨年范围",
        )

    source = read_source_version_context(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    endpoint_source = read_source_version_context(
        config,
        target_session=endpoint_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    for label, item in (("T0", source), ("T+1", endpoint_source)):
        if (
            item.get("status") != "AVAILABLE"
            or item.get("version_status") != "COMPLETE"
            or item.get("source_capture_id") != source_capture_id
            or item.get("source_time_quality") != "NON_PIT_OBSERVED"
            or item.get("time_role") != "NON_PIT_RETROSPECTIVE"
            or item.get("source_available_at") is not None
            or item.get("price_coordinate") != "FORWARD_ADJUSTED"
        ):
            raise SourceResearchDayError(
                "T1_FORWARD_SOURCE_SCOPE_INVALID",
                f"{label}BaoStock QFQ paired capture或NON_PIT身份不符合后验同源生产范围",
            )

    seed_scope = read_source_pool_seed_forward_scope(
        config,
        target_session=target_session,
        source_capture_id=source["source_capture_id"],
        expected_source_count=source["research_expected_row_count"],
        profile_revision=profile_revision,
        history_session_count=history_session_count,
    )
    if seed_scope["calendar_revision"] != target_calendar["revision"]:
        raise SourceResearchDayError(
            "T1_FORWARD_SEED_CALENDAR_MISMATCH", "已存T0 Seed与官方交易日历修订不一致"
        )

    symbols = seed_scope["source_symbols"]
    sessions = tuple(
        cutoff_calendar_sessions[target_index : target_index + horizon + 1]
    )
    rows_by_symbol: dict[str, list[Mapping[str, Any]]] = {
        symbol: [] for symbol in symbols
    }
    session_versions: dict[str, dict[str, Any]] = {
        day.isoformat(): {
            "status": "CLOSED",
            "row_count": 0,
            "expected_row_count": 0,
            "completed_request_count": 0,
        }
        for day in sessions
    }
    for start in range(0, len(symbols), 100):
        symbol_batch = symbols[start : start + 100]
        offset = 0
        total = None
        batch_row_count = 0
        batch_coverage = None
        while True:
            page = read_baostock_price_history_batch(
                config,
                source_symbols=symbol_batch,
                sessions=sessions,
                source_capture_id=source_capture_id,
                price_coordinate="FORWARD_ADJUSTED",
                cutoff=research_cutoff,
                offset=offset,
                limit=1000,
            )
            if (
                page.get("source_capture_id") != source_capture_id
                or page.get("price_coordinate") != "FORWARD_ADJUSTED"
            ):
                raise SourceResearchDayError(
                    "T1_FORWARD_QFQ_IDENTITY_INVALID",
                    "T0/T+1 QFQ历史没有绑定同一paired capture。",
                )
            page_info = page["page"]
            if total is None:
                total = int(page_info["total"])
                batch_coverage = page.get("coverage", ())
            elif int(page_info["total"]) != total:
                raise SourceResearchDayError(
                    "T1_FORWARD_QFQ_DENOMINATOR_CHANGED",
                    "T0/T+1 QFQ历史分页期间分母发生变化。",
                )
            for row in page.get("rows", ()):
                rows_by_symbol[row["source_symbol"]].append(row)
                batch_row_count += 1
            next_offset = page_info.get("next_offset")
            if next_offset is None:
                break
            offset = int(next_offset)
        if total is None or batch_row_count != total:
            raise SourceResearchDayError(
                "T1_FORWARD_QFQ_HISTORY_INCOMPLETE",
                "T0/T+1 QFQ历史分页没有闭合到行数分母。",
            )
        if any(
            item.get("coverage_status") != "COMPLETE" for item in batch_coverage or ()
        ):
            raise SourceResearchDayError(
                "T1_FORWARD_QFQ_HISTORY_INCOMPLETE",
                "T0/T+1 QFQ历史请求或行覆盖未闭合。",
            )
        for item in batch_coverage or ():
            version = session_versions[item["session_date"]]
            version["row_count"] += int(item["row_count"])
            version["expected_row_count"] += int(item["expected_symbol_count"])
            version["completed_request_count"] += int(item["completed_request_count"])
    if any(
        version["row_count"] != version["expected_row_count"]
        or version["completed_request_count"] != version["expected_row_count"]
        for version in session_versions.values()
    ):
        raise SourceResearchDayError(
            "T1_FORWARD_QFQ_HISTORY_INCOMPLETE",
            "T0/T+1 QFQ历史会话覆盖未闭合。",
        )

    outcomes = {}
    for symbol in symbols:
        outcomes[symbol] = exploratory_raw_path(
            sessions,
            rows_by_symbol[symbol],
            horizon,
            target_session=endpoint_session,
            source_capture_id=source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
            saved_through=min(research_cutoff, endpoint_session),
        )
    paths = prepare_forward_path_rows(
        target=target_session,
        source=source,
        calendar_revision=seed_scope["calendar_revision"],
        session_versions=session_versions,
        outcomes=outcomes,
        source_capture_id=source_capture_id,
        horizon=horizon,
    )
    write_result = write_forward_path_rows(
        config,
        paths=paths,
        source_symbols=symbols,
        endpoint_source_capture_id=source_capture_id,
        research_cutoff=research_cutoff,
    )
    expected_cohort_rows = write_result.pop("_s2_t1_cohort_expected_rows")
    try:
        readback = read_forward_path_scope(
            config,
            paths=paths,
            source_symbols=symbols,
        )
    except Exception as error:
        raise SourceResearchDayError(
            "T1_FORWARD_READBACK_FAILED",
            "T+1写入事务已提交，但独立PG读回未通过；请核对精确T0范围后恢复",
            database_write_attempted=True,
        ) from error
    if horizon == 1:
        try:
            from stock_a.storage.postgres.source_s2_t1_cohort import (
                read_source_s2_t1_cohort_scope,
            )

            cohort_readback = read_source_s2_t1_cohort_scope(
                config,
                target_session=target_session,
                endpoint_session=endpoint_session,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
                endpoint_source_capture_id=source_capture_id,
                expected_rows=expected_cohort_rows,
            )
            if (
                cohort_readback.get("status") != "AVAILABLE"
                or cohort_readback.get("rows_read_back") != 12
            ):
                raise ValueError("S2/T+1统计读回未闭合")
        except Exception as error:
            raise SourceResearchDayError(
                "S2_T1_COHORT_READBACK_FAILED",
                "T+1事务已提交，但S2三态分组统计未能在异事务完整读回",
                database_write_attempted=True,
            ) from error
    changed = bool(
        write_result["inserted_paths"] or write_result["s2_t1_cohort_stats_inserted"]
    )
    return {
        "status": "PERSISTED" if changed else "IDEMPOTENT",
        "target_session": target_session.isoformat(),
        "endpoint_session": endpoint_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "endpoint_source_capture_id": endpoint_source["source_capture_id"],
        "profile_revision": seed_scope.get("profile_revision"),
        "producer_contract": seed_scope["producer_contract"],
        "producer_version": seed_scope["producer_version"],
        "history_session_count": seed_scope["history_session_count"],
        "s2_state_counts_t0": seed_scope["s2_state_counts"],
        "calendar_revision": seed_scope["calendar_revision"],
        "source_count": len(symbols),
        "expected_paths": len(paths),
        "inserted_paths": write_result["inserted_paths"],
        "s2_t1_cohort_stats_inserted": write_result["s2_t1_cohort_stats_inserted"],
        "idempotent": not changed,
        "path_rule_version": write_result["path_rule_version"],
        "write_transaction": write_result["transaction"],
        "readback": readback,
        "s2_t1_cohort_stats_readback": cohort_readback,
        "database_write_attempted": True,
        "database_changed": changed,
        "source_collection": False,
        "raw_recalculated": False,
        "seed_recalculated": False,
        "ddl_executed": False,
    }


def run_source_d0_launch_reference_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    persist: bool,
) -> dict[str, Any]:
    """只为相邻会话消费者生产所需的LAUNCH_CONFIRM前日D0事实。"""
    if persist is not True:
        raise SourceResearchDayError(
            "D0_REFERENCE_PERSIST_REQUIRED", "D0参考事实写入必须显式指定持久化"
        )
    if (
        target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise SourceResearchDayError(
            "D0_REFERENCE_SCOPE_INVALID", "D0参考事实日期或source_capture_id无效"
        )
    calendar = _official_calendar(target_session)
    sessions = calendar["sessions"]
    if target_session not in sessions or sessions.index(target_session) == 0:
        raise SourceResearchDayError(
            "D0_REFERENCE_SESSION_INVALID", "D0参考日必须是有前序官方会话的交易日"
        )
    catalog = read_baostock_price_catalog(
        config, cutoff=research_cutoff, source_capture_id=source_capture_id
    )
    target_versions = [
        item
        for item in catalog.get("price_coordinate_versions", ())
        if item.get("session_date") == target_session.isoformat()
    ]
    coordinate_versions = {
        item.get("price_coordinate"): item for item in target_versions
    }
    if (
        catalog.get("status") != "AVAILABLE"
        or set(coordinate_versions) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or any(
            item.get("source_capture_id") != source_capture_id
            or item.get("coverage_status") != "COMPLETE"
            or item.get("row_count") != item.get("expected_row_count")
            for item in coordinate_versions.values()
        )
    ):
        raise SourceResearchDayError(
            "D0_REFERENCE_RAW_VERSION_UNAVAILABLE",
            "D0参考日没有唯一且闭合的paired BaoStock价格来源",
        )
    source = read_source_version_context(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    page = query_source_d0_raw_launch(
        config,
        scenario="LAUNCH_CONFIRM",
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
        offset=0,
        limit=MAX_SOURCE_SYMBOLS,
    )
    source_version = page.get("source_version", {})
    expected_count = int(coordinate_versions["FORWARD_ADJUSTED"]["row_count"])
    if (
        source.get("status") != "AVAILABLE"
        or source_version.get("source_capture_id") != source.get("source_capture_id")
        or source_version.get("source_capture_id") != source_capture_id
        or page.get("target_session") != target_session.isoformat()
        or page.get("source_pool", {}).get("source_count") != expected_count
        or page.get("source_pool", {}).get("scenario_state_partition_closed")
        is not True
        or page.get("selection", {}).get("matched_count") != expected_count
        or page.get("selection", {}).get("next_offset") is not None
        or len(page.get("results", [])) != expected_count
    ):
        raise SourceResearchDayError(
            "D0_REFERENCE_PARTITION_OPEN",
            "D0参考事实未与闭合的BaoStock paired capture及完整分母一致",
        )
    persistence = persist_static_d0_reference_scenario(
        config, scenario="LAUNCH_CONFIRM", page=page
    )
    return {
        "status": persistence["status"],
        "target_session": target_session.isoformat(),
        "previous_session": sessions[sessions.index(target_session) - 1].isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source.get("source_capture_id"),
        "scenario": "LAUNCH_CONFIRM",
        "rows_expected": persistence["rows_expected"],
        "rows_inserted": persistence["rows_inserted"],
        "persistence": persistence,
        "execution": {
            "database_target": "127.0.0.1:5432/stock_a",
            "database_schema": "stock",
            "database_role": "stock_a",
            "transaction": persistence["transaction"],
            "only_requested_d0_scenario_written": True,
            "seed_written": False,
            "d1_written": False,
            "source_after_target_read": False,
            "database_write_attempted": persistence["database_write_attempted"],
            "ddl_executed": False,
            "dml_executed": persistence["dml_executed"],
        },
    }


def _validate_static_history_count(count: int | None) -> None:
    if count is not None and (
        type(count) is not int or not 1 <= count <= PG_PROFILE_MAX_HISTORY_SESSIONS
    ):
        raise SourceResearchDayError(
            "STATIC_PROFILE_WINDOW_INVALID", "画像历史会话数超出现有PG表约束"
        )


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise SourceResearchDayError("RESEARCH_DAY_FIELD_INVALID", f"{label}缺失")
    return value


def _optional_text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if type(value) is not int:
        raise SourceResearchDayError("RESEARCH_DAY_FIELD_INVALID", "D1计数不是整数")
    return value


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if type(value) is not bool:
        raise SourceResearchDayError("RESEARCH_DAY_FIELD_INVALID", "D1状态不是布尔值")
    return value


def _session(value: Any, label: str, *, optional: bool = False) -> date | None:
    if value is None and optional:
        return None
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise SourceResearchDayError(
            "RESEARCH_DAY_SESSION_INVALID", f"{label}不是合法官方会话日期"
        ) from error


def _numeric_id(value: Any, label: str, *, optional: bool = False) -> int | None:
    if value is None and optional:
        return None
    if type(value) is not int or value <= 0:
        raise SourceResearchDayError(
            "RESEARCH_DAY_NUMERIC_ID_INVALID", f"{label}不是正整数ID"
        )
    return value


def _exact_rational(value: Any, label: str) -> ExactRational | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        value = value.get("exact", value.get("value"))
        if isinstance(value, Mapping):
            value = value.get("exact")
    if value is None or isinstance(value, bool):
        raise SourceResearchDayError(
            "RESEARCH_DAY_RATIONAL_INVALID", f"{label}不是精确数值"
        )
    try:
        rational = Fraction(str(value))
    except (ValueError, ZeroDivisionError) as error:
        raise SourceResearchDayError(
            "RESEARCH_DAY_RATIONAL_INVALID", f"{label}不是精确有限数值"
        ) from error
    return ExactRational(rational.numerator, rational.denominator)


def _predicate_projection(
    item: Mapping[str, Any] | None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if item is None:
        return (), ()
    predicates = item.get("predicates")
    if not isinstance(predicates, Mapping) or not predicates:
        raise SourceResearchDayError(
            "D0_PREDICATE_PROJECTION_MISSING", "D0逐股事实缺少具名谓词结果"
        )
    states = []
    reasons = []
    for name, predicate in predicates.items():
        if not isinstance(name, str) or not isinstance(predicate, Mapping):
            raise SourceResearchDayError(
                "D0_PREDICATE_PROJECTION_INVALID", "D0谓词结构不合法"
            )
        state = _required_text(predicate.get("state"), f"D0谓词{name}状态")
        if state not in {"TRUE", "FALSE", "UNKNOWN"}:
            raise SourceResearchDayError(
                "D0_PREDICATE_PROJECTION_INVALID", "D0谓词状态不是三态"
            )
        reason = _optional_text(predicate.get("reason_code"))
        if state == "UNKNOWN" and reason is None:
            reason = (
                _optional_text(predicate.get("reason")) or "UNKNOWN_REASON_UNAVAILABLE"
            )
        states.append(f"{name}={state}")
        reasons.append(f"{name}:{reason}" if reason else f"{name}:NONE")
    return tuple(states), tuple(reasons)


def _status_count(summary: Mapping[str, Any], status: str) -> int:
    value = summary.get(status, 0)
    if type(value) is not int or value < 0:
        raise SourceResearchDayError(
            "D1_ANCHOR_STATUS_COUNT_INVALID", f"D1锚状态{status}计数无效"
        )
    return value


def _state_payload(item: Mapping[str, Any] | None) -> tuple[str | None, str | None]:
    if item is None:
        return None, None
    state = _required_text(item.get("scenario_raw"), "D0场景状态")
    if state not in {"TRUE", "FALSE", "UNKNOWN"}:
        raise SourceResearchDayError("D0_STATE_INVALID", "D0状态不是TRUE/FALSE/UNKNOWN")
    reason = _required_text(item.get("main_reason"), "D0状态原因")
    _predicate_projection(item)
    return state, reason


def _prepare_d0_facts(changes: Mapping[str, Any]) -> tuple[D0LaunchFact, ...]:
    if changes.get("pair_status") != "COMPARABLE":
        raise SourceResearchDayError("D0_PAIR_NOT_COMPARABLE", "相邻D0来源版本不可比较")
    key_set = changes.get("source_key_set", {})
    rows = changes.get("results")
    expected = key_set.get("source_union_count")
    if (
        not isinstance(rows, list)
        or type(expected) is not int
        or expected != len(rows)
        or key_set.get("union_partition_closed") is not True
        or changes.get("selection", {}).get("matched_count") != len(rows)
    ):
        raise SourceResearchDayError(
            "D0_UNION_PARTITION_OPEN", "D0前后来源键并集未闭合"
        )
    facts = []
    seen = set()
    for row in rows:
        symbol = _required_text(row.get("source_symbol"), "D0来源代码")
        market = _required_text(row.get("market"), "D0市场")
        if (
            _SYMBOL.fullmatch(symbol) is None
            or market not in {"SH", "SZ"}
            or symbol[:2] != market
        ):
            raise SourceResearchDayError(
                "D0_SOURCE_KEY_INVALID", "D0来源代码与市场不匹配"
            )
        if symbol in seen:
            raise SourceResearchDayError("D0_SOURCE_KEY_DUPLICATED", "D0来源代码重复")
        seen.add(symbol)
        before = row.get("before")
        after = row.get("after")
        before_state, before_reason = _state_payload(before)
        after_state, after_reason = _state_payload(after)
        transition = _required_text(row.get("transition"), "D0相邻转移")
        if before_state is None and after_state is None:
            raise SourceResearchDayError("D0_EMPTY_TRANSITION", "D0转移两端均缺少状态")
        facts.append(
            D0LaunchFact(
                source_symbol=symbol,
                market=market,
                before_state=before_state,
                before_reason=before_reason,
                after_state=after_state,
                after_reason=after_reason,
                transition=transition,
            )
        )
    return tuple(facts)


def _persist_static_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    expected_capture_id: int | None,
    expected_capture_source_count: int | None = None,
    expected_research_source_count: int | None = None,
    history_session_count: int | None = None,
    worker_count: int = 1,
    cross_section_universe_version: str | None = None,
    cross_section_universe_capture_id: int | None = None,
) -> dict[str, Any]:
    """由同一现役Core/Profile、RPS与D0生产者闭合全池静态事实。"""
    _validate_static_history_count(history_session_count)
    profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
    calendar = _official_calendar(target_session)
    source_ctx = read_source_version_context(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        price_coordinate="FORWARD_ADJUSTED",
    )
    data_universe_expected_count = source_ctx["capture_expected_row_count"]
    research_universe_expected_count = source_ctx["research_expected_row_count"]
    if (
        expected_capture_source_count is not None
        and (
            type(expected_capture_source_count) is not int
            or expected_capture_source_count != data_universe_expected_count
        )
        or expected_research_source_count is not None
        and (
            type(expected_research_source_count) is not int
            or expected_research_source_count != research_universe_expected_count
        )
        or type(research_universe_expected_count) is not int
        or research_universe_expected_count <= 0
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_SOURCE_DENOMINATOR_MISMATCH",
            "静态研究必须分别闭合原始捕获分母与主板/创业板评价分母。",
        )
    catalog = source_symbol_price_profile_snapshots(
        config, max_target_session=research_cutoff
    )
    matches = []
    for snapshot in catalog["snapshots"]:
        if (
            snapshot["target_session"] != target_session.isoformat()
            or snapshot["source_capture_id"] != source_capture_id
            or not snapshot["complete"]
            or snapshot["source_symbol_count"] != data_universe_expected_count
            or snapshot["price_coordinate"] != "FORWARD_ADJUSTED"
            or snapshot["history_calendar_id"] != calendar["calendar_id"]
            or (
                history_session_count is not None
                and snapshot["history_session_count"] != history_session_count
            )
        ):
            continue
        count = snapshot["history_session_count"]
        _target, window, revision = resolve_pg_history_window(
            target_session.isoformat(), count
        )
        if (
            snapshot["history_calendar_revision"] == revision
            and snapshot["window_start"] == window[0].isoformat()
            and snapshot["window_end"] == window[-1].isoformat()
        ):
            matches.append((count, window, None))
    if len(matches) != 1:
        raise SourceResearchDayError(
            "STATIC_PROFILE_WINDOW_AMBIGUOUS",
            "目标日没有唯一完整且符合现行语义的画像窗口",
        )
    history_count, history_window, profile_revision = matches[0]
    seed = compute_source_pool_seed(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        profile_revision=profile_revision,
        profile_contract=profile_contract,
        history_session_count=history_count,
        include_rps_read=True,
        worker_count=worker_count,
        cross_section_universe_version=cross_section_universe_version,
        cross_section_universe_capture_id=cross_section_universe_capture_id,
    )
    if (
        seed.get("stored_profile_status") != "AVAILABLE"
        or seed.get("source_pool_total") != research_universe_expected_count
        or seed.get("price_coordinate") != "FORWARD_ADJUSTED"
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_PROFILE_OR_DENOMINATOR_MISMATCH",
            "Seed静态事实必须绑定精确QFQ画像和完整来源池分母",
        )
    scenarios = {}
    for scenario in ("LAUNCH_CONFIRM", "RECOVERY_TURN"):
        kwargs = {}
        if scenario == "RECOVERY_TURN":
            kwargs["_rps_result"] = seed["rps_read"]
        page = query_source_d0_raw_launch(
            config,
            scenario=scenario,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            offset=0,
            limit=MAX_SOURCE_SYMBOLS,
            **kwargs,
        )
        source = page.get("source_version", {})
        if (
            (
                expected_capture_id is not None
                and source.get("source_capture_id") != expected_capture_id
            )
            or source.get("source_capture_id") != source_capture_id
            or seed.get("source_capture_id") != source_capture_id
        ):
            raise SourceResearchDayError(
                "STATIC_RESEARCH_SOURCE_IDENTITY_MISMATCH",
                "D0、Core/Profile与当日持久事实未绑定同一来源捕获",
            )
        scenarios[scenario] = page
    result = persist_static_research_day(
        config,
        scenarios=scenarios,
        seed=seed,
        history_window=history_window,
        profile_revision=profile_revision,
    )
    for field in (
        "ranking_universe_count",
        "evaluation_universe_count",
        "rps_source",
        "formal_research_universe",
    ):
        result[field] = seed.get(field)
    return result


def run_source_static_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    persist: bool = False,
    history_session_count: int | None = None,
    worker_count: int = 1,
    include_sector_pulse: bool = False,
    sector_snapshot_ids: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """由现役单日生产链写入静态事实，可附加双快照板块跨日脉冲。"""
    if persist is not True:
        raise SourceResearchDayError(
            "STATIC_RESEARCH_PERSIST_REQUIRED", "静态事实写入必须显式指定持久化"
        )
    if (
        type(include_sector_pulse) is not bool
        or (
            include_sector_pulse
            and (
                not isinstance(sector_snapshot_ids, Mapping)
                or set(sector_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
                or not all(
                    type(value) is int and value > 0
                    for value in sector_snapshot_ids.values()
                )
                or sector_snapshot_ids["INDUSTRY"] == sector_snapshot_ids["CONCEPT"]
            )
        )
        or (not include_sector_pulse and sector_snapshot_ids is not None)
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_SECTOR_PULSE_ARGUMENT_INVALID",
            "板块PULSE必须同时绑定行业与GN概念快照，且仅能显式启用",
        )
    validate_seed_worker_count(worker_count)
    _validate_static_history_count(history_session_count)
    if (
        target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_SCOPE_INVALID", "静态事实初始化范围或数据包编号无效"
        )
    catalog = read_baostock_price_catalog(
        config, cutoff=research_cutoff, source_capture_id=source_capture_id
    )
    target_versions = [
        item
        for item in catalog.get("price_coordinate_versions", ())
        if item.get("session_date") == target_session.isoformat()
    ]
    coordinate_versions = {
        item.get("price_coordinate"): item for item in target_versions
    }
    if (
        catalog.get("status") != "AVAILABLE"
        or set(coordinate_versions) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        or any(
            item.get("source_capture_id") != source_capture_id
            or item.get("coverage_status") != "COMPLETE"
            or item.get("row_count") != item.get("expected_row_count")
            for item in coordinate_versions.values()
        )
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_RAW_VERSION_UNAVAILABLE",
            "目标日没有唯一且闭合的paired BaoStock价格来源",
        )
    started = time.perf_counter()
    persistence = _persist_static_research_day(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        expected_capture_id=None,
        expected_capture_source_count=int(
            coordinate_versions["FORWARD_ADJUSTED"]["row_count"]
        ),
        history_session_count=history_session_count,
        worker_count=worker_count,
    )
    sector_pulse = None
    if include_sector_pulse:
        from stock_a.application.source_sector_pulse import (
            run_source_sector_pulse_day,
        )

        sector_pulse = run_source_sector_pulse_day(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            profile_revision=persistence["profile_revision"],
            industry_snapshot_id=sector_snapshot_ids["INDUSTRY"],
            concept_snapshot_id=sector_snapshot_ids["CONCEPT"],
        )
    pulse_persistence = sector_pulse["persistence"] if sector_pulse else {}
    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "source_capture_id": source_capture_id,
        "source_capture_count": 1,
        "source_count": coordinate_versions["FORWARD_ADJUSTED"]["row_count"],
        "persistence": persistence,
        "sector_pulse": sector_pulse,
        "execution": {
            "database_target": "127.0.0.1:5432/stock_a",
            "database_schema": "stock",
            "database_role": "stock_a",
            "raw_source_collection": False,
            "source_zip_opened": False,
            "existing_d0_d1_recomputed": False,
            "database_write_attempted": persistence["database_write_attempted"]
            or pulse_persistence.get("database_write_attempted", False),
            "ddl_executed": False,
            "dml_executed": persistence["dml_executed"]
            or pulse_persistence.get("dml_executed", False),
            "sector_pulse_full_pool_calculated": include_sector_pulse,
            "sector_pulse_source_collection": False,
            "total_wall_seconds": round(time.perf_counter() - started, 6),
        },
    }


def _prepare_d1_facts(
    d1: Mapping[str, Any],
) -> tuple[tuple[D1StructureFact, ...], tuple[D1MaterialAnchorFact, ...]]:
    if (
        d1.get("source_pool_partition_closed") is not True
        or d1.get("historical_anchor_changes", {}).get(
            "material_change_partition_closed"
        )
        is not True
    ):
        raise SourceResearchDayError("D1_PARTITION_OPEN", "D1来源或锚变化分母未闭合")
    pool_count = d1.get("source_pool_count")
    material = d1.get("historical_anchor_changes", {})
    structure_results = material.get("source_symbol_structure_results")
    symbol_summaries = material.get("source_symbol_material_summary")
    symbol_details = material.get("source_symbol_d1_details")
    if (
        type(pool_count) is not int
        or not isinstance(structure_results, Mapping)
        or not isinstance(symbol_summaries, Mapping)
        or not isinstance(symbol_details, Mapping)
        or len(structure_results) != pool_count
        or set(structure_results) != set(symbol_summaries)
    ):
        raise SourceResearchDayError(
            "D1_SYMBOL_PARTITION_OPEN", "D1缺少逐股结构、锚分母或质量事实"
        )
    facts: list[D1StructureFact] = []
    anchors: list[D1MaterialAnchorFact] = []
    for symbol in sorted(structure_results):
        if _SYMBOL.fullmatch(symbol) is None:
            raise SourceResearchDayError("D1_SOURCE_KEY_INVALID", "D1来源代码格式无效")
        structure_row = structure_results[symbol]
        summary = symbol_summaries[symbol]
        source = structure_row.get("target_source", {})
        market = _required_text(structure_row.get("market"), "D1市场")
        board = _required_text(structure_row.get("board"), "D1板别")
        if market not in {"SH", "SZ"} or symbol[:2] != market:
            raise SourceResearchDayError(
                "D1_SOURCE_KEY_INVALID", "D1来源代码与市场不匹配"
            )
        if board not in {"MAIN_BOARD", "GEM"}:
            raise SourceResearchDayError("D1_BOARD_INVALID", "D1来源板别无效")
        history_rows = structure_row.get("history_row_count")
        if type(history_rows) is not int or history_rows < 1:
            raise SourceResearchDayError(
                "D1_HISTORY_QUALITY_INVALID", "D1历史Raw行数无效"
            )
        type_results = structure_row.get("structures")
        anchor_counts = summary.get("anchor_counts_by_type")
        status_counts = summary.get("material_change_status_counts_by_type")
        if (
            not isinstance(type_results, Mapping)
            or not isinstance(anchor_counts, Mapping)
            or not isinstance(status_counts, Mapping)
        ):
            raise SourceResearchDayError(
                "D1_TYPE_SUMMARY_INVALID", "D1缺少五类结构结果"
            )
        d1_version = _optional_text(d1.get("producer_version")) or D1_PRODUCER_VERSION_5
        try:
            expected_d1_types = set(expected_d1_structure_types_for_version(d1_version))
        except Exception as error:
            raise SourceResearchDayError(
                "D1_PRODUCER_VERSION_INVALID", f"不支持的D1生产者版本: {d1_version}"
            ) from error
        if (
            set(type_results) != expected_d1_types
            or not set(anchor_counts).issubset(expected_d1_types)
            or set(status_counts) != expected_d1_types
        ):
            raise SourceResearchDayError(
                "D1_TYPE_SUMMARY_INVALID",
                f"D1结构类型不符合版本 {d1_version} 合同要求（期望{len(expected_d1_types)}类）",
            )
        detail_rows = symbol_details.get(symbol, [])
        if not isinstance(detail_rows, list):
            raise SourceResearchDayError("D1_DETAIL_INVALID", "D1锚明细不是列表")
        for kind, outcome in type_results.items():
            if not isinstance(outcome, Mapping):
                raise SourceResearchDayError(
                    "D1_TYPE_OUTCOME_INVALID", "D1类型结果结构无效"
                )
            state = _required_text(outcome.get("state"), "D1结构生成状态")
            if state not in {"GENERATED", "NOT_GENERATED", "UNKNOWN"}:
                raise SourceResearchDayError(
                    "D1_TYPE_OUTCOME_INVALID", "D1结构状态无效"
                )
            reason = _optional_text(outcome.get("reason"))
            anchor = outcome.get("anchor")
            if state == "GENERATED" and not isinstance(anchor, Mapping):
                raise SourceResearchDayError(
                    "D1_GENERATED_ANCHOR_MISSING", "D1生成状态缺少锚坐标"
                )
            if state != "GENERATED" and anchor is not None:
                raise SourceResearchDayError(
                    "D1_UNEXPECTED_ANCHOR", "非生成状态携带锚事实"
                )
            kind_detail_rows = [
                item for item in detail_rows if item.get("type") == kind
            ]
            per_type_statuses = status_counts.get(kind, {})
            if not isinstance(per_type_statuses, Mapping):
                raise SourceResearchDayError(
                    "D1_ANCHOR_STATUS_COUNT_INVALID", "D1锚状态计数缺失"
                )
            counts = {
                status: _status_count(per_type_statuses, status)
                for status in _D1_STATUS_COUNTS
            }
            historical_count = anchor_counts.get(kind, 0)
            if (
                type(historical_count) is not int
                or historical_count < 0
                or sum(counts.values()) != historical_count
            ):
                raise SourceResearchDayError(
                    "D1_ANCHOR_DENOMINATOR_OPEN", "逐股D1锚分类不守恒"
                )
            if counts["MATERIAL_CHANGE"] + counts["UNKNOWN"] + counts[
                "NOT_COMPARABLE"
            ] + counts["NOT_EVALUABLE"] != len(kind_detail_rows):
                raise SourceResearchDayError(
                    "D1_DETAIL_PARTITION_OPEN", "D1材料/质量锚明细与分类分母不一致"
                )
            generated_anchor = None
            if anchor is not None:
                generated_anchor = {
                    "created": _session(anchor.get("created_session"), "D1锚出生会话"),
                    "available": _session(
                        anchor.get("available_from_session"), "D1锚可用会话"
                    ),
                    "test": _session(
                        anchor.get("test_from_session"),
                        "D1锚开始测试会话",
                        optional=True,
                    ),
                    "coordinate": _optional_text(anchor.get("coordinate_id")),
                    "relation": _required_text(anchor.get("relation"), "D1锚关系"),
                    "lower": _exact_rational(anchor.get("lower"), "D1锚下界"),
                    "upper": _exact_rational(anchor.get("upper"), "D1锚上界"),
                    "source_ref": _optional_text(anchor.get("source_ref")),
                }
            facts.append(
                D1StructureFact(
                    source_symbol=symbol,
                    market=market,
                    board=board,
                    structure_type=kind,
                    generation_state=state,
                    generation_reason=reason,
                    history_row_count=history_rows,
                    target_bar_status=_required_text(
                        source.get("qfq_bar_status"), "D1目标QFQ Bar状态"
                    ),
                    target_bar_reason=_optional_text(source.get("qfq_bar_reason")),
                    target_price_quality=_required_text(
                        source.get("qfq_price_quality"), "D1目标QFQ价格质量"
                    ),
                    target_price_reason=_optional_text(source.get("qfq_price_reason")),
                    historical_anchor_count=historical_count,
                    material_change_anchor_count=counts["MATERIAL_CHANGE"],
                    count_only_anchor_count=counts["COUNT_ONLY"],
                    unchanged_anchor_count=counts["UNCHANGED"],
                    unknown_anchor_count=counts["UNKNOWN"],
                    not_comparable_anchor_count=counts["NOT_COMPARABLE"],
                    not_evaluable_anchor_count=counts["NOT_EVALUABLE"],
                    not_applicable_anchor_count=counts["NOT_APPLICABLE"],
                    persisted_detail_count=len(kind_detail_rows),
                    anchor_created_session=generated_anchor["created"]
                    if generated_anchor
                    else None,
                    anchor_available_from_session=generated_anchor["available"]
                    if generated_anchor
                    else None,
                    anchor_test_from_session=generated_anchor["test"]
                    if generated_anchor
                    else None,
                    coordinate_id=generated_anchor["coordinate"]
                    if generated_anchor
                    else None,
                    relation=generated_anchor["relation"] if generated_anchor else None,
                    lower=generated_anchor["lower"] if generated_anchor else None,
                    upper=generated_anchor["upper"] if generated_anchor else None,
                    source_ref=generated_anchor["source_ref"]
                    if generated_anchor
                    else None,
                )
            )
        for change in detail_rows:
            anchors.append(_prepare_d1_anchor(symbol, change))
    if len(facts) != pool_count * len(d1.get("structure_type_counts", {})):
        raise SourceResearchDayError(
            "D1_TYPE_PARTITION_OPEN", "D1逐股五类类型行数不闭合"
        )
    if len(anchors) != sum(fact.persisted_detail_count for fact in facts):
        raise SourceResearchDayError("D1_ANCHOR_PARTITION_OPEN", "D1锚明细计数不闭合")
    return tuple(facts), tuple(anchors)


def _prepare_d1_anchor(symbol: str, row: Mapping[str, Any]) -> D1MaterialAnchorFact:
    source = row.get("source") if isinstance(row.get("source"), Mapping) else {}
    birth = source.get("birth") if isinstance(source.get("birth"), Mapping) else {}
    previous = (
        source.get("previous") if isinstance(source.get("previous"), Mapping) else {}
    )
    pairing = row.get("anchor_pairing_identity")
    pairing = pairing if isinstance(pairing, Mapping) else {}
    comparison = row.get("comparison_context")
    comparison = comparison if isinstance(comparison, Mapping) else {}
    support = (
        row.get("support_change")
        if isinstance(row.get("support_change"), Mapping)
        else {}
    )
    before = support.get("before") if isinstance(support.get("before"), Mapping) else {}
    after = support.get("after") if isinstance(support.get("after"), Mapping) else {}
    gap = (
        row.get("gap_fill_change")
        if isinstance(row.get("gap_fill_change"), Mapping)
        else {}
    )
    gap_before = gap.get("before") if isinstance(gap.get("before"), Mapping) else {}
    gap_after = gap.get("after") if isinstance(gap.get("after"), Mapping) else {}
    created = _session(row.get("created_session"), "D1材料锚出生会话")
    previous_session = _session(
        row.get("comparison_context", {}).get("previous_official_session"),
        "D1材料锚前一观察会话",
    )
    if not isinstance(created, date) or not isinstance(previous_session, date):
        raise SourceResearchDayError("D1_ANCHOR_BIRTH_INVALID", "D1锚出生会话缺失")
    lower = _exact_rational(pairing.get("lower"), "D1历史锚下界")
    upper = _exact_rational(pairing.get("upper"), "D1历史锚上界")
    birth_capture = _numeric_id(
        birth.get("source_capture_id"), "D1锚出生捕获", optional=True
    )
    previous_capture = _numeric_id(
        previous.get("source_capture_id"), "D1前态捕获", optional=True
    )
    state_before = before.get("state")
    state_after = after.get("state")
    gap_before_value = gap_before.get("state")
    gap_after_value = gap_after.get("state")
    return D1MaterialAnchorFact(
        source_symbol=symbol,
        structure_type=_required_text(row.get("type"), "D1材料锚类型"),
        relation=_required_text(row.get("relation"), "D1材料锚关系"),
        created_session=created,
        previous_session=previous_session,
        coordinate_id=_optional_text(pairing.get("coordinate_id")),
        lower=lower,
        upper=upper,
        birth_status="AVAILABLE"
        if birth.get("session") == created.isoformat()
        else "UNKNOWN",
        birth_capture_id=birth_capture,
        previous_capture_id=previous_capture,
        official_sessions_adjacent=(
            comparison.get("official_sessions_adjacent") is True
        ),
        anchor_birth_before_previous_session=(
            comparison.get("anchor_birth_before_previous_session") is True
        ),
        change_classification=_required_text(
            row.get("change_classification"), "D1字段变化分类"
        ),
        material_change_classification=_required_text(
            row.get("material_change_classification"), "D1材料变化分类"
        ),
        support_change_classification=_required_text(
            support.get("classification"), "D1支撑变化分类"
        ),
        gap_fill_classification=_required_text(
            gap.get("classification"), "D1缺口填补分类"
        ),
        change_reasons=tuple(row.get("change_reasons") or ()),
        material_change_reasons=tuple(row.get("material_change_reasons") or ()),
        quality_reasons=tuple(row.get("quality_reasons") or ()),
        before_state=_optional_text(state_before),
        before_quality=_optional_text(before.get("quality")),
        before_acceptance=_optional_text(before.get("acceptance")),
        before_freshness=_optional_text(before.get("freshness")),
        before_breach_streak=_optional_int(before.get("breach_streak")),
        before_acceptance_streak=_optional_int(before.get("acceptance_streak")),
        before_test_count=_optional_int(before.get("test_count")),
        before_separated_sessions=_optional_int(before.get("separated_sessions")),
        before_retest_qualified=_optional_bool(before.get("retest_qualified")),
        after_state=_optional_text(state_after),
        after_quality=_optional_text(after.get("quality")),
        after_acceptance=_optional_text(after.get("acceptance")),
        after_freshness=_optional_text(after.get("freshness")),
        after_breach_streak=_optional_int(after.get("breach_streak")),
        after_acceptance_streak=_optional_int(after.get("acceptance_streak")),
        after_test_count=_optional_int(after.get("test_count")),
        after_separated_sessions=_optional_int(after.get("separated_sessions")),
        after_retest_qualified=_optional_bool(after.get("retest_qualified")),
        gap_before_state=_optional_text(gap_before_value),
        gap_before_quality=_optional_text(gap_before.get("quality")),
        gap_before_reason=_optional_text(gap_before.get("reason")),
        gap_after_state=_optional_text(gap_after_value),
        gap_after_quality=_optional_text(gap_after.get("quality")),
        gap_after_reason=_optional_text(gap_after.get("reason")),
    )


def prepare_source_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    d1_history_session_count: int,
    include_d0: bool = True,
    include_d1: bool = True,
    include_impulse: bool = True,
    cross_section_universe_version: str | None = None,
    cross_section_universe_capture_id: int | None = None,
) -> tuple[ResearchDayFacts, dict[str, Any]]:
    """按选中的研究域独立准备一个交易日；只读Raw，默认永不写PG。"""
    started = time.perf_counter()
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or type(source_capture_id) is not int
        or source_capture_id <= 0
        or type(include_d0) is not bool
        or type(include_d1) is not bool
        or type(include_impulse) is not bool
        or not (include_d0 or include_d1)
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_SCOPE_INVALID",
            "单日研究范围、数据包编号或域选择无效",
        )
    official = _official_calendar(target_session)
    try:
        target_index = official["sessions"].index(target_session)
    except ValueError as error:
        raise SourceResearchDayError(
            "RESEARCH_DAY_NOT_SESSION", "目标日不在现役官方日历"
        ) from error
    if target_index == 0:
        raise SourceResearchDayError(
            "RESEARCH_DAY_PRIOR_SESSION_MISSING", "目标日缺少相邻前态会话"
        )
    before_session = official["sessions"][target_index - 1]

    d0: Mapping[str, Any] = {}
    d1: Mapping[str, Any] = {}
    d0_rows = ()
    d1_rows = ()
    anchor_rows = ()
    d0_wall_seconds = 0.0
    d1_wall_seconds = 0.0
    prepared: Mapping[str, Any] = {}

    before_source_capture_id = source_capture_id
    if include_d0:
        before_catalog = read_baostock_price_catalog(config, cutoff=research_cutoff)
        before_caps = {
            int(item["source_capture_id"])
            for item in before_catalog.get("price_coordinate_versions", ())
            if item.get("session_date") == before_session.isoformat()
            and item.get("price_coordinate") == "FORWARD_ADJUSTED"
            and item.get("coverage_status") == "COMPLETE"
            and type(item.get("source_capture_id")) is int
        }
        if len(before_caps) == 1:
            before_source_capture_id = next(iter(before_caps))
        d0_started = time.perf_counter()
        d0 = query_source_d0_raw_launch(
            config,
            scenario="LAUNCH_CONFIRM",
            action="changes",
            before_session=before_session,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            before_source_capture_id=before_source_capture_id,
            transition="ALL",
            offset=0,
            limit=2 * MAX_SOURCE_SYMBOLS,
        )
        d0_wall_seconds = time.perf_counter() - d0_started
        d0_rows = _prepare_d0_facts(d0)

    if include_d1:
        _target, d1_sessions, _calendar_revision = resolve_pg_history_window(
            target_session.isoformat(), d1_history_session_count
        )
        if d1_sessions[-1] != target_session or d1_sessions[-2] != before_session:
            raise SourceResearchDayError(
                "D1_ADJACENT_SESSION_MISMATCH", "D1目标与前一官方会话不一致"
            )
        d1_started = time.perf_counter()
        prepared = prepare_d1_material_observation(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            d1_history_session_count=d1_history_session_count,
            industry_snapshot_id=None,
            concept_snapshot_id=None,
            include_source_symbol_structure_results=True,
            include_impulse=include_impulse,
        )
        d1_wall_seconds = time.perf_counter() - d1_started
        d1 = prepared["d1"]
        d1_rows, anchor_rows = _prepare_d1_facts(d1)

    after = d0.get("after", {})
    before = d0.get("before", {})
    after_source = after.get("source_version", {})
    before_source = before.get("source_version", {})
    d1_source = d1.get("source", {})
    if include_d0 and (
        d0.get("pair_status") != "COMPARABLE"
        or after.get("target_session") != target_session.isoformat()
        or before.get("target_session") != before_session.isoformat()
        or after_source.get("source_capture_id") != source_capture_id
        or before_source.get("source_capture_id") != before_source_capture_id
        or d0.get("source_key_set", {}).get("union_partition_closed") is not True
        or len(d0_rows) != d0.get("source_key_set", {}).get("source_union_count")
    ):
        raise SourceResearchDayError(
            "D0_SOURCE_PARTITION_OPEN",
            "D0前后来源、相邻日期或完整总体未闭合",
        )
    if include_d1 and (
        d1.get("target_session") != target_session.isoformat()
        or d1_source.get("source_capture_id") != source_capture_id
        or d1.get("source_pool_partition_closed") is not True
        or d1.get("source_pool_count")
        != len(d1_rows) // max(1, len(d1.get("structure_type_counts", {})))
        or len(d1_rows)
        != d1.get("source_pool_count") * len(d1.get("structure_type_counts", {}))
    ):
        raise SourceResearchDayError(
            "D1_SOURCE_PARTITION_OPEN", "D1目标来源或完整总体未闭合"
        )
    if (
        include_d0
        and include_d1
        and (
            d1_source.get("source_capture_id") != after_source.get("source_capture_id")
            or d1.get("source_pool_count")
            != after.get("source_pool", {}).get("source_count")
            or {item.source_symbol for item in d1_rows}
            != {item.source_symbol for item in d0_rows if item.after_state is not None}
        )
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_SOURCE_IDENTITY_MISMATCH",
            "D0与D1没有绑定同一目标来源捕获、内容、日期及完整总体",
        )

    if include_d0:
        source = after_source
        target_capture_id = _numeric_id(
            after_source.get("source_capture_id"), "D0目标来源捕获"
        )
        before_capture_id = _numeric_id(
            before_source.get("source_capture_id"), "D0前态来源捕获"
        )
        target_count = int(d0.get("source_key_set", {}).get("after_count"))
    else:
        source = d1_source
        target_capture_id = _numeric_id(
            d1_source.get("source_capture_id"), "D1目标来源捕获"
        )
        before_version = read_source_version_context(
            config,
            target_session=before_session,
            research_cutoff=research_cutoff,
            source_capture_id=source_capture_id,
            price_coordinate="FORWARD_ADJUSTED",
        )
        if before_version.get("status") != "AVAILABLE":
            raise SourceResearchDayError(
                "D1_PREVIOUS_SOURCE_UNAVAILABLE", "D1前一会话来源捕获不可读取"
            )
        before_capture_id = int(before_version["source_capture_id"])
        target_count = int(d1.get("source_pool_count"))

    if include_d1:
        calendar = d1.get("calendar", {})
    else:
        calendar = {
            "calendar_id": official["calendar_id"],
            "calendar_revision": official["calendar_revision"],
        }
    d1_cross_section_scope = (
        prepared.get("d1", {}).get("cross_section_scope")
        if include_d1 and isinstance(prepared.get("d1"), Mapping)
        else None
    )
    facts = ResearchDayFacts(
        before_session=before_session,
        target_session=target_session,
        source_capture_id=source_capture_id,
        before_capture_id=before_capture_id,
        target_capture_id=target_capture_id,
        source_time_quality=_required_text(
            source.get("source_time_quality"), "来源时钟质量"
        ),
        source_clock_role=_required_text(
            source.get("source_clock_role"), "来源时钟角色"
        ),
        source_available_at=_optional_text(source.get("source_available_at")),
        calendar_id=_required_text(calendar.get("calendar_id"), "官方日历标识"),
        calendar_revision=_required_text(
            calendar.get("calendar_revision"), "官方日历修订"
        ),
        d0_before_window_start=(
            _session(
                before.get("strict_official_sessions", [None])[0], "D0前态窗口起点"
            )
            if include_d0
            else before_session
        ),
        d0_target_window_start=(
            _session(after.get("strict_official_sessions", [None])[0], "D0目标窗口起点")
            if include_d0
            else target_session
        ),
        d0_before_source_count=(
            int(d0.get("source_key_set", {}).get("before_count", 0))
            if include_d0
            else 0
        ),
        d0_target_source_count=(
            int(d0.get("source_key_set", {}).get("after_count", 0)) if include_d0 else 0
        ),
        d0_union_source_count=(
            int(d0.get("source_key_set", {}).get("source_union_count", 0))
            if include_d0
            else 0
        ),
        d1_history_session_count=len(prepared.get("sessions", ())) if include_d1 else 0,
        d1_history_window_start=(
            prepared["sessions"][0] if include_d1 else target_session
        ),
        d1_history_window_end=(
            prepared["sessions"][-1] if include_d1 else target_session
        ),
        d1_source_count=target_count if include_d1 else 0,
        d0_producer_version=(
            _required_text(d0.get("producer_version"), "D0生产者版本")
            if include_d0
            else "SKIPPED_BY_USER"
        ),
        d0_algorithm_contract=(
            _required_text(after.get("algorithm_contract"), "D0算法合同")
            if include_d0
            else "SKIPPED_BY_USER"
        ),
        d0_session_contract=(
            _required_text(after.get("session_contract"), "D0会话合同")
            if include_d0
            else "SKIPPED_BY_USER"
        ),
        d1_producer_contract=(
            _required_text(d1.get("contract"), "D1生产者合同")
            if include_d1
            else "SKIPPED_BY_USER"
        ),
        d1_producer_version=(
            _required_text(d1.get("producer_version"), "D1生产者版本")
            if include_d1
            else "SKIPPED_BY_USER"
        ),
        d1_material_change_contract=(
            _required_text(
                d1.get("historical_anchor_changes", {}).get("material_change_contract"),
                "D1材料变化合同",
            )
            if include_d1
            else "SKIPPED_BY_USER"
        ),
        d0_facts=d0_rows,
        d1_structure_facts=d1_rows,
        d1_anchor_facts=anchor_rows,
    )
    _validate_day_facts(facts, include_d0=include_d0, include_d1=include_d1)
    report = _summarize_day(
        facts,
        d0,
        d1,
        d0_wall_seconds,
        d1_wall_seconds,
        time.perf_counter() - started,
        include_d0=include_d0,
        include_d1=include_d1,
    )
    report["research_cutoff"] = research_cutoff.isoformat()
    return facts, report


def _validate_day_facts(
    facts: ResearchDayFacts, *, include_d0: bool, include_d1: bool
) -> None:
    expected_d1_type_count = (
        len(expected_d1_structure_types_for_version(facts.d1_producer_version))
        if include_d1
        else 0
    )
    if (
        type(include_d0) is not bool
        or type(include_d1) is not bool
        or not (include_d0 or include_d1)
        or facts.target_session <= facts.before_session
        or (include_d0 and facts.d0_before_source_count < 0)
        or (include_d0 and facts.d0_target_source_count < 1)
        or (include_d0 and facts.d0_union_source_count != len(facts.d0_facts))
        or (include_d1 and facts.d1_source_count < 1)
        or (
            include_d1
            and len(facts.d1_structure_facts)
            != facts.d1_source_count * expected_d1_type_count
        )
        or (
            include_d0
            and len({item.source_symbol for item in facts.d0_facts})
            != len(facts.d0_facts)
        )
        or (
            include_d1
            and len(
                {
                    (item.source_symbol, item.structure_type)
                    for item in facts.d1_structure_facts
                }
            )
            != len(facts.d1_structure_facts)
        )
        or (
            include_d1
            and len(
                {
                    (item.source_symbol, item.structure_type, item.created_session)
                    for item in facts.d1_anchor_facts
                }
            )
            != len(facts.d1_anchor_facts)
        )
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_BUSINESS_KEY_INVALID", "单日事实业务键或总量无效"
        )
    if include_d1:
        symbols = {item.source_symbol for item in facts.d1_structure_facts}
        if any(
            item.official_sessions_adjacent is not True
            for item in facts.d1_anchor_facts
        ):
            raise SourceResearchDayError(
                "D1_ADJACENT_SESSION_INVALID", "D1锚明细未证明官方相邻会话"
            )
        if include_d0 and symbols != {
            item.source_symbol
            for item in facts.d0_facts
            if item.after_state is not None
        }:
            raise SourceResearchDayError(
                "RESEARCH_DAY_SYMBOL_SET_MISMATCH", "D0目标分母与D1逐股分母不同"
            )


def _summarize_day(
    facts: ResearchDayFacts,
    d0: Mapping[str, Any],
    d1: Mapping[str, Any],
    d0_wall_seconds: float,
    d1_wall_seconds: float,
    total_wall_seconds: float,
    *,
    include_d0: bool,
    include_d1: bool,
) -> dict[str, Any]:
    d0_state_counts = (
        d0.get("after", {}).get("source_pool", {}).get("scenario_state_counts", {})
    )
    d1_changes = d1.get("historical_anchor_changes", {})
    d0_summary = (
        {
            "producer_version": facts.d0_producer_version,
            "algorithm_contract": facts.d0_algorithm_contract,
            "before_source_count": facts.d0_before_source_count,
            "target_source_count": facts.d0_target_source_count,
            "union_source_count": facts.d0_union_source_count,
            "before_window_start": facts.d0_before_window_start.isoformat(),
            "target_window_start": facts.d0_target_window_start.isoformat(),
            "target_state_counts": dict(d0_state_counts),
            "transition_counts": dict(d0.get("transition_counts", {})),
            "typed_fact_rows": len(facts.d0_facts),
            "partition_closed": len(facts.d0_facts) == facts.d0_union_source_count,
            "reader_wall_seconds": d0_wall_seconds,
        }
        if include_d0
        else {
            "status": "SKIPPED_BY_USER",
            "calculation_performed": False,
            "typed_fact_rows": 0,
        }
    )
    d1_summary = (
        {
            "producer_contract": facts.d1_producer_contract,
            "producer_version": facts.d1_producer_version,
            "material_change_contract": facts.d1_material_change_contract,
            "history_session_count": facts.d1_history_session_count,
            "history_start": facts.d1_history_window_start.isoformat(),
            "history_end": facts.d1_history_window_end.isoformat(),
            "source_symbol_denominator": facts.d1_source_count,
            "structure_type_counts": d1.get("structure_type_counts", {}),
            "anchor_status_counts": d1_changes.get("material_change_status_counts", {}),
            "anchor_denominator": d1_changes.get("material_change_denominator"),
            "typed_stock_type_rows": len(facts.d1_structure_facts),
            "typed_anchor_detail_rows": len(facts.d1_anchor_facts),
            "source_pool_partition_closed": d1.get("source_pool_partition_closed"),
            "anchor_partition_closed": d1_changes.get(
                "material_change_partition_closed"
            ),
            "execution": d1.get("execution", {}),
            "reader_wall_seconds": d1_wall_seconds,
        }
        if include_d1
        else {
            "status": "SKIPPED_BY_USER",
            "calculation_performed": False,
            "typed_stock_type_rows": 0,
            "typed_anchor_detail_rows": 0,
        }
    )
    tables = []
    insert_rows = 0
    if include_d0:
        tables.append("source_d0_launch_day_facts")
        insert_rows += len(facts.d0_facts)
    if include_d1:
        tables.extend(
            (
                "source_d1_structure_day_facts",
                "source_d1_material_anchor_day_facts",
            )
        )
        insert_rows += len(facts.d1_structure_facts) + len(facts.d1_anchor_facts)
    return {
        "status": "DRY_RUN_READY",
        "target_session": facts.target_session.isoformat(),
        "before_session": facts.before_session.isoformat(),
        "research_cutoff": d1.get("research_cutoff"),
        "source_capture_id": facts.source_capture_id,
        "before_capture_id": facts.before_capture_id,
        "target_capture_id": facts.target_capture_id,
        "calendar": {
            "calendar_id": facts.calendar_id,
            "calendar_revision": facts.calendar_revision,
        },
        "source_quality": {
            "source_time_quality": facts.source_time_quality,
            "source_clock_role": facts.source_clock_role,
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "price_coordinate": "FORWARD_ADJUSTED",
            "time_role": "NON_PIT_RETROSPECTIVE",
        },
        "d0": d0_summary,
        "d1": d1_summary,
        "cross_section_scope": d1_summary.get("cross_section_scope"),
        "estimated_transaction_scope": {
            "tables": tables,
            "insert_rows_if_empty": insert_rows,
            "d0_rows": len(facts.d0_facts),
            "d1_stock_type_rows": len(facts.d1_structure_facts),
            "d1_anchor_detail_rows": len(facts.d1_anchor_facts),
            "atomic_scope": {
                "target_session": facts.target_session.isoformat(),
                "before_session": facts.before_session.isoformat(),
                "source_capture_id": facts.source_capture_id,
                "before_capture_id": facts.before_capture_id,
                "target_capture_id": facts.target_capture_id,
                "d0_algorithm_contract": facts.d0_algorithm_contract,
                "d0_session_contract": facts.d0_session_contract,
                "d1_history_session_count": facts.d1_history_session_count,
                "calendar_revision": facts.calendar_revision,
                "d1_producer_contract": facts.d1_producer_contract,
                "d1_producer_version": facts.d1_producer_version,
                "d1_material_change_contract": facts.d1_material_change_contract,
            },
            "same_input_retry": "ZERO_DIFFERENCE_AFTER_TYPED_READBACK",
            "different_content_same_key": "REJECT_AND_ROLLBACK",
        },
        "execution": {
            "database_target": "127.0.0.1:5432/stock_a",
            "database_schema": "stock",
            "database_role": "stock_a",
            "transaction": "READ ONLY SOURCE CALCULATIONS; NO WRITE TRANSACTION OPENED",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "source_zip_opened": False,
            "total_wall_seconds": total_wall_seconds,
        },
    }


def run_source_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    source_capture_id: int,
    d1_history_session_count: int,
    persist: bool = False,
    history_session_count: int | None = None,
    worker_count: int = 1,
    include_d0: bool = True,
    include_d1: bool = True,
    include_static: bool = True,
    cross_section_universe_version: str | None = None,
    cross_section_universe_capture_id: int | None = None,
) -> dict[str, Any]:
    """执行单日研究；保存域按D0/D1输出日期独立选择。"""
    if (
        type(include_d0) is not bool
        or type(include_d1) is not bool
        or type(include_static) is not bool
        or not (include_d0 or include_d1)
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_DOMAIN_SELECTION_INVALID",
            "至少选择D0或D1一个生产域，且域开关必须是布尔值。",
        )
    # 静态D0画像属于D0选择范围；关闭D0时不启动其独立计算器。
    include_static = include_static and include_d0
    validate_seed_worker_count(worker_count)
    if include_static:
        _validate_static_history_count(history_session_count)
    facts, report = prepare_source_research_day(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        source_capture_id=source_capture_id,
        d1_history_session_count=d1_history_session_count,
        include_d0=include_d0,
        include_d1=include_d1,
        cross_section_universe_version=cross_section_universe_version,
        cross_section_universe_capture_id=cross_section_universe_capture_id,
    )
    if persist:
        write_result = persist_source_research_day(
            config,
            facts,
            include_d0=include_d0,
            include_d1=include_d1,
        )
        if include_static:
            static_started = time.perf_counter()
            static_result = _persist_static_research_day(
                config,
                target_session=target_session,
                research_cutoff=research_cutoff,
                source_capture_id=source_capture_id,
                expected_capture_id=facts.target_capture_id,
                expected_research_source_count=(
                    facts.d1_source_count
                    if include_d1
                    else facts.d0_target_source_count
                ),
                history_session_count=history_session_count,
                worker_count=worker_count,
                cross_section_universe_version=cross_section_universe_version,
                cross_section_universe_capture_id=cross_section_universe_capture_id,
            )
            static_result["wall_seconds"] = round(
                time.perf_counter() - static_started, 6
            )
        else:
            static_result = {
                "status": "SKIPPED_BY_USER",
                "database_write_attempted": False,
                "ddl_executed": False,
                "dml_executed": False,
            }
        report["static_persistence"] = static_result
        report["profile_revision"] = static_result.get("profile_revision")
        report["producer_versions"] = static_result.get("producer_versions", {})
        report["rows_expected"] = {
            **write_result.get("rows_expected", {}),
            **static_result.get("rows_expected", {}),
        }
        report["rows_inserted"] = {
            **write_result.get("rows_inserted", {}),
            **static_result.get("rows_inserted", {}),
        }
        report["status"] = "PERSISTED"
        report["persistence"] = write_result
        report["execution"]["transaction"] = write_result["transaction"]
        if include_static:
            report["execution"]["transaction"] += "; " + static_result["transaction"]
        report["execution"]["database_write_attempted"] = write_result[
            "database_write_attempted"
        ] or static_result.get("database_write_attempted", False)
        report["execution"]["dml_executed"] = write_result[
            "dml_executed"
        ] or static_result.get("dml_executed", False)
    return report


__all__ = [
    "SourceResearchDayError",
    "prepare_source_research_day",
    "run_source_research_day",
]
