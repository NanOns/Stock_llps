"""将现役D0/D1单日生产者投影成可幂等持久化的typed事实。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
import hashlib
import re
import time
from typing import Any, Mapping
from uuid import UUID

from stock_a.application.pg_price_profiles import (
    SOURCE_SYMBOL_PROFILE_SCOPE,
    _profile_revision,
    _profile_semantics_sha256,
)
from stock_a.application.research_window import resolve_pg_history_window
from stock_a.application.source_d0_raw import (
    _official_calendar,
    query_source_d0_raw_launch,
)
from stock_a.application.source_market_observation import (
    prepare_d1_material_observation,
)
from stock_a.application.source_pool_seed import compute_source_pool_seed
from stock_a.application.runtime_config import _CONFIG_ROOT
from stock_a.contracts import load_price_profile_contract
from stock_a.domain.research_day_facts import (
    D0LaunchFact,
    D1MaterialAnchorFact,
    D1StructureFact,
    ExactRational,
    ResearchDayFacts,
)
from stock_a.storage.postgres.research_day_facts import (
    persist_source_research_day,
)
from stock_a.storage.postgres.static_research_facts import persist_static_research_day
from stock_a.storage.postgres.source_raw import read_raw_daily_catalog


_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
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


class SourceResearchDayError(ValueError):
    """单日研究批次的生产者输出或分母未闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


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


def _uuid_text(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    try:
        canonical = str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as error:
        raise SourceResearchDayError(
            "RESEARCH_DAY_UUID_INVALID", f"{label}不是有效UUID"
        ) from error
    if str(value) != canonical:
        raise SourceResearchDayError(
            "RESEARCH_DAY_UUID_INVALID", f"{label}必须是规范小写UUID"
        )
    return canonical


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
    package_sha256: str,
    namespace_id: str,
    expected_capture_id: str | None,
    expected_source_count: int,
) -> dict[str, Any]:
    """由同一现役Core/Profile、RPS与D0生产者闭合全池静态事实。"""
    history_count = 75
    _target, history_window, calendar_revision = resolve_pg_history_window(
        target_session.isoformat(), history_count
    )
    profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
    calendar = _official_calendar(target_session)
    profile_revision = _profile_revision(
        scope=SOURCE_SYMBOL_PROFILE_SCOPE,
        semantics_sha256=_profile_semantics_sha256(profile_contract),
        calendar_id=calendar["calendar_id"],
        calendar_revision=calendar_revision,
        history_session_count=len(history_window),
    )
    seed = compute_source_pool_seed(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        namespace_id=namespace_id,
        profile_revision=profile_revision,
        profile_contract=profile_contract,
        history_session_count=history_count,
        include_rps_read=True,
    )
    if (
        seed.get("profile_revision_matches_runtime") is not True
        or seed.get("stored_profile_status") != "AVAILABLE"
        or seed.get("source_pool_total") != expected_source_count
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_PROFILE_OR_DENOMINATOR_MISMATCH",
            "Seed静态事实必须绑定精确R6画像和完整来源池分母",
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
            package_sha256=package_sha256,
            offset=0,
            limit=5000,
            expected_namespace_id=namespace_id,
            **kwargs,
        )
        source = page.get("source_version", {})
        if (
            (
                expected_capture_id is not None
                and source.get("source_capture_id") != expected_capture_id
            )
            or source.get("package_sha256") != package_sha256
            or seed.get("namespace_id") != namespace_id
            or seed.get("package_sha256") != package_sha256
        ):
            raise SourceResearchDayError(
                "STATIC_RESEARCH_SOURCE_IDENTITY_MISMATCH",
                "D0、Core/Profile与当日持久事实未绑定同一来源捕获",
            )
        scenarios[scenario] = page
    semantics_sha256 = hashlib.sha256(
        (
            "SOURCE_POOL_SEED_ALG-19/20_R1:"
            + _profile_semantics_sha256(profile_contract)
            + ":CORE_RAW_SOURCE_RPS_R1:"
            + ":".join(
                scenarios[name]["producer_version"]
                for name in ("LAUNCH_CONFIRM", "RECOVERY_TURN")
            )
        ).encode("utf-8")
    ).hexdigest()
    return persist_static_research_day(
        config,
        scenarios=scenarios,
        seed=seed,
        history_window=history_window,
        profile_revision=profile_revision,
        producer_semantics_sha256=semantics_sha256,
    )


def run_source_static_research_day(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
) -> dict[str, Any]:
    """仅初始化新静态表；读取精确PG Raw目录，不重算已保存D0/D1三类事实。"""
    if target_session > research_cutoff or _SHA256.fullmatch(package_sha256) is None:
        raise SourceResearchDayError(
            "STATIC_RESEARCH_SCOPE_INVALID", "静态事实初始化范围或来源SHA无效"
        )
    versions = []
    offset = 0
    namespace_id = None
    while True:
        catalog = read_raw_daily_catalog(
            config,
            max_target_session=research_cutoff,
            offset=offset,
            limit=200,
        )
        namespace_id = catalog["namespace_id"]
        versions.extend(catalog["versions"])
        if catalog["next_offset"] is None:
            break
        offset = catalog["next_offset"]
    target_versions = [
        item for item in versions if item["session_date"] == target_session.isoformat()
    ]
    if (
        namespace_id is None
        or len(target_versions) != 1
        or target_versions[0]["package_sha256"] != package_sha256
        or target_versions[0]["version_status"] != "CLOSED"
        or target_versions[0]["row_count"] != target_versions[0]["expected_row_count"]
    ):
        raise SourceResearchDayError(
            "STATIC_RESEARCH_RAW_VERSION_UNAVAILABLE",
            "目标日没有唯一、同包且闭合的已存PG Raw来源版本",
        )
    version = target_versions[0]
    started = time.perf_counter()
    persistence = _persist_static_research_day(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        namespace_id=namespace_id,
        expected_capture_id=None,
        expected_source_count=version["row_count"],
    )
    return {
        "status": "PERSISTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "package_sha256": package_sha256,
        "namespace_id": namespace_id,
        "source_capture_count": version["source_capture_count"],
        "source_count": version["row_count"],
        "persistence": persistence,
        "execution": {
            "database_target": "127.0.0.1:5432/stock_a",
            "database_schema": "stock",
            "database_role": "stock_a",
            "raw_source_collection": False,
            "source_zip_opened": False,
            "existing_d0_d1_recomputed": False,
            "database_write_attempted": persistence["database_write_attempted"],
            "ddl_executed": False,
            "dml_executed": persistence["dml_executed"],
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
        if (
            set(type_results) != _D1_STRUCTURE_TYPES
            or not set(anchor_counts).issubset(_D1_STRUCTURE_TYPES)
            or set(status_counts) != _D1_STRUCTURE_TYPES
        ):
            raise SourceResearchDayError(
                "D1_TYPE_SUMMARY_INVALID", "D1结构类型不符合现役五类合同"
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
                    "id": _required_text(anchor.get("anchor_id"), "D1锚标识"),
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
                    "source_event_id": _optional_text(anchor.get("source_event_id")),
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
                        source.get("bar_status"), "D1目标Bar状态"
                    ),
                    target_bar_reason=_optional_text(source.get("bar_reason")),
                    target_price_quality=_required_text(
                        source.get("price_quality"), "D1目标价格质量"
                    ),
                    target_price_reason=_optional_text(source.get("price_reason")),
                    historical_anchor_count=historical_count,
                    material_change_anchor_count=counts["MATERIAL_CHANGE"],
                    count_only_anchor_count=counts["COUNT_ONLY"],
                    unchanged_anchor_count=counts["UNCHANGED"],
                    unknown_anchor_count=counts["UNKNOWN"],
                    not_comparable_anchor_count=counts["NOT_COMPARABLE"],
                    not_evaluable_anchor_count=counts["NOT_EVALUABLE"],
                    not_applicable_anchor_count=counts["NOT_APPLICABLE"],
                    persisted_detail_count=len(kind_detail_rows),
                    anchor_id=generated_anchor["id"] if generated_anchor else None,
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
                    source_event_id=generated_anchor["source_event_id"]
                    if generated_anchor
                    else None,
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
    target = source.get("target") if isinstance(source.get("target"), Mapping) else {}
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
    anchor_id = _required_text(row.get("anchor_id"), "D1材料锚标识")
    created = _session(row.get("created_session"), "D1材料锚出生会话")
    previous_session = _session(
        row.get("comparison_context", {}).get("previous_official_session"),
        "D1材料锚前一观察会话",
    )
    if not isinstance(created, date) or not isinstance(previous_session, date):
        raise SourceResearchDayError("D1_ANCHOR_BIRTH_INVALID", "D1锚出生会话缺失")
    lower = _exact_rational(pairing.get("lower"), "D1历史锚下界")
    upper = _exact_rational(pairing.get("upper"), "D1历史锚上界")
    birth_capture = _uuid_text(
        birth.get("source_capture_id"), "D1锚出生捕获", optional=True
    )
    previous_capture = _uuid_text(
        previous.get("source_capture_id"), "D1前态捕获", optional=True
    )
    birth_entry = _optional_text(birth.get("source_entry_sha256"))
    previous_entry = _optional_text(previous.get("source_entry_sha256"))
    target_entry = _optional_text(target.get("source_entry_sha256"))
    for value, label in (
        (birth_entry, "D1出生来源条目摘要"),
        (previous_entry, "D1前态来源条目摘要"),
        (target_entry, "D1目标来源条目摘要"),
    ):
        if value is not None and _SHA256.fullmatch(value) is None:
            raise SourceResearchDayError(
                "D1_SOURCE_ENTRY_DIGEST_INVALID", f"{label}无效"
            )
    state_before = before.get("state")
    state_after = after.get("state")
    gap_before_value = gap_before.get("state")
    gap_after_value = gap_after.get("state")
    return D1MaterialAnchorFact(
        source_symbol=symbol,
        structure_type=_required_text(row.get("type"), "D1材料锚类型"),
        anchor_id=anchor_id,
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
        birth_entry_sha256=birth_entry,
        previous_capture_id=previous_capture,
        previous_entry_sha256=previous_entry,
        target_entry_sha256=target_entry,
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
    package_sha256: str,
    d1_history_session_count: int,
) -> tuple[ResearchDayFacts, dict[str, Any]]:
    """同一生产者准备一个交易日；只读Raw，默认永不写PG。"""
    started = time.perf_counter()
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or not isinstance(package_sha256, str)
        or _SHA256.fullmatch(package_sha256) is None
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_SCOPE_INVALID", "单日研究范围或来源SHA无效"
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

    d0_started = time.perf_counter()
    d0 = query_source_d0_raw_launch(
        config,
        scenario="LAUNCH_CONFIRM",
        action="changes",
        before_session=before_session,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        before_package_sha256=package_sha256,
        transition="ALL",
        offset=0,
        limit=5000,
    )
    d0_wall_seconds = time.perf_counter() - d0_started
    d0_rows = _prepare_d0_facts(d0)

    _target, d1_sessions, _calendar_revision = resolve_pg_history_window(
        target_session.isoformat(), d1_history_session_count
    )
    if d1_sessions[-1] != target_session or d1_sessions[-2] != before_session:
        raise SourceResearchDayError(
            "D0_D1_ADJACENT_SESSION_MISMATCH", "D0与D1目标及相邻观察会话不一致"
        )
    d1_started = time.perf_counter()
    prepared = prepare_d1_material_observation(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        d1_history_session_count=d1_history_session_count,
        industry_snapshot_id=None,
        concept_snapshot_id=None,
        include_source_symbol_structure_results=True,
    )
    d1_wall_seconds = time.perf_counter() - d1_started
    d1 = prepared["d1"]
    d1_rows, anchor_rows = _prepare_d1_facts(d1)

    after = d0.get("after", {})
    before = d0.get("before", {})
    after_source = after.get("source_version", {})
    before_source = before.get("source_version", {})
    d1_source = d1.get("source", {})
    if (
        d0.get("pair_status") != "COMPARABLE"
        or after.get("target_session") != target_session.isoformat()
        or before.get("target_session") != before_session.isoformat()
        or after_source.get("package_sha256") != package_sha256
        or before_source.get("package_sha256") != package_sha256
        or d1.get("target_session") != target_session.isoformat()
        or d1_source.get("package_sha256") != package_sha256
        or d1_source.get("source_capture_id") != after_source.get("source_capture_id")
        or d1_source.get("source_content_id") != after_source.get("source_content_id")
        or d1.get("source_pool_count")
        != after.get("source_pool", {}).get("source_count")
        or d1.get("source_pool_count")
        != len(d1_rows) // len(d1.get("structure_type_counts", {}))
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_SOURCE_IDENTITY_MISMATCH",
            "D0与D1没有绑定同一目标来源捕获、内容、日期及完整总体",
        )
    target_count = d1.get("source_pool_count")
    if (
        d0.get("source_key_set", {}).get("union_partition_closed") is not True
        or len(d0_rows) != d0.get("source_key_set", {}).get("source_union_count")
        or len(d1_rows) != target_count * len(d1.get("structure_type_counts", {}))
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_PARTITION_OPEN", "D0/D1事实没有闭合到来源分母"
        )
    calendar = d1.get("calendar", {})
    source = d1_source
    facts = ResearchDayFacts(
        namespace_id=_required_text(
            d0.get("after", {}).get("namespace_id"), "D0命名空间"
        ),
        before_session=before_session,
        target_session=target_session,
        package_sha256=package_sha256,
        source_content_id=_required_text(
            source.get("source_content_id"), "目标来源内容摘要"
        ),
        before_capture_id=_uuid_text(
            before_source.get("source_capture_id"), "前态来源捕获"
        ),
        target_capture_id=_uuid_text(
            after_source.get("source_capture_id"), "目标来源捕获"
        ),
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
        calendar_sha256=_required_text(calendar.get("calendar_sha256"), "官方日历摘要"),
        d0_before_window_start=_session(
            before.get("strict_official_sessions", [None])[0], "D0前态窗口起点"
        ),
        d0_target_window_start=_session(
            after.get("strict_official_sessions", [None])[0], "D0目标窗口起点"
        ),
        d0_before_source_count=int(d0.get("source_key_set", {}).get("before_count")),
        d0_target_source_count=int(d0.get("source_key_set", {}).get("after_count")),
        d0_union_source_count=int(
            d0.get("source_key_set", {}).get("source_union_count")
        ),
        d1_history_session_count=len(prepared["sessions"]),
        d1_history_window_start=prepared["sessions"][0],
        d1_history_window_end=prepared["sessions"][-1],
        d1_source_count=int(target_count),
        d0_producer_version=_required_text(d0.get("producer_version"), "D0生产者版本"),
        d0_algorithm_contract=_required_text(
            after.get("algorithm_contract"), "D0算法合同"
        ),
        d0_session_contract=_required_text(after.get("session_contract"), "D0会话合同"),
        d1_producer_contract=_required_text(d1.get("contract"), "D1生产者合同"),
        d1_producer_version="STOCK_A_D1_FIVE_STRUCTURE_SOURCE_READER_R1",
        d1_material_change_contract=_required_text(
            d1.get("historical_anchor_changes", {}).get("material_change_contract"),
            "D1材料变化合同",
        ),
        d0_facts=d0_rows,
        d1_structure_facts=d1_rows,
        d1_anchor_facts=anchor_rows,
    )
    _validate_day_facts(facts)
    report = _summarize_day(
        facts, d0, d1, d0_wall_seconds, d1_wall_seconds, time.perf_counter() - started
    )
    return facts, report


def _validate_day_facts(facts: ResearchDayFacts) -> None:
    if (
        facts.target_session <= facts.before_session
        or facts.d0_before_source_count < 0
        or facts.d0_target_source_count < 1
        or facts.d0_union_source_count != len(facts.d0_facts)
        or facts.d1_source_count < 1
        or len(facts.d1_structure_facts) != facts.d1_source_count * 5
        or len({item.source_symbol for item in facts.d0_facts}) != len(facts.d0_facts)
        or len(
            {
                (item.source_symbol, item.structure_type)
                for item in facts.d1_structure_facts
            }
        )
        != len(facts.d1_structure_facts)
        or len(
            {
                (
                    item.source_symbol,
                    item.structure_type,
                    item.anchor_id,
                    item.created_session,
                )
                for item in facts.d1_anchor_facts
            }
        )
        != len(facts.d1_anchor_facts)
    ):
        raise SourceResearchDayError(
            "RESEARCH_DAY_BUSINESS_KEY_INVALID", "单日事实业务键或总量无效"
        )
    symbols = {item.source_symbol for item in facts.d1_structure_facts}
    if any(
        item.official_sessions_adjacent is not True for item in facts.d1_anchor_facts
    ):
        raise SourceResearchDayError(
            "D1_ADJACENT_SESSION_INVALID", "D1锚明细未证明官方相邻会话"
        )
    if symbols != {
        item.source_symbol for item in facts.d0_facts if item.after_state is not None
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
) -> dict[str, Any]:
    d0_state_counts = (
        d0.get("after", {}).get("source_pool", {}).get("scenario_state_counts", {})
    )
    d1_changes = d1.get("historical_anchor_changes", {})
    return {
        "status": "DRY_RUN_READY",
        "target_session": facts.target_session.isoformat(),
        "before_session": facts.before_session.isoformat(),
        "research_cutoff": d1.get("research_cutoff"),
        "package_sha256": facts.package_sha256,
        "source_content_id": facts.source_content_id,
        "before_capture_id": facts.before_capture_id,
        "target_capture_id": facts.target_capture_id,
        "namespace_id": facts.namespace_id,
        "calendar": {
            "calendar_id": facts.calendar_id,
            "calendar_revision": facts.calendar_revision,
            "calendar_sha256": facts.calendar_sha256,
        },
        "source_quality": {
            "source_time_quality": facts.source_time_quality,
            "source_clock_role": facts.source_clock_role,
            "pit_status": "NON_PIT_RETROSPECTIVE",
            "price_coordinate": "RAW_UNADJUSTED",
        },
        "d0": {
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
        },
        "d1": {
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
        },
        "estimated_transaction_scope": {
            "tables": [
                "stock.source_d0_launch_day_facts",
                "stock.source_d1_structure_day_facts",
                "stock.source_d1_material_anchor_day_facts",
            ],
            "insert_rows_if_empty": len(facts.d0_facts)
            + len(facts.d1_structure_facts)
            + len(facts.d1_anchor_facts),
            "d0_rows": len(facts.d0_facts),
            "d1_stock_type_rows": len(facts.d1_structure_facts),
            "d1_anchor_detail_rows": len(facts.d1_anchor_facts),
            "atomic_scope": {
                "namespace_id": facts.namespace_id,
                "target_session": facts.target_session.isoformat(),
                "before_session": facts.before_session.isoformat(),
                "package_sha256": facts.package_sha256,
                "before_capture_id": facts.before_capture_id,
                "target_capture_id": facts.target_capture_id,
                "d0_algorithm_contract": facts.d0_algorithm_contract,
                "d0_session_contract": facts.d0_session_contract,
                "d1_history_session_count": facts.d1_history_session_count,
                "calendar_revision": facts.calendar_revision,
                "calendar_sha256": facts.calendar_sha256,
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
    package_sha256: str,
    d1_history_session_count: int,
    persist: bool = False,
) -> dict[str, Any]:
    """执行单日研究；仅显式persist=True时调用PG Writers。"""
    facts, report = prepare_source_research_day(
        config,
        target_session=target_session,
        research_cutoff=research_cutoff,
        package_sha256=package_sha256,
        d1_history_session_count=d1_history_session_count,
    )
    if persist:
        write_result = persist_source_research_day(config, facts)
        static_started = time.perf_counter()
        static_result = _persist_static_research_day(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            namespace_id=facts.namespace_id,
            expected_capture_id=facts.target_capture_id,
            expected_source_count=facts.d1_source_count,
        )
        report["static_persistence"] = static_result
        report["static_persistence"]["wall_seconds"] = round(
            time.perf_counter() - static_started, 6
        )
        report["status"] = "PERSISTED"
        report["persistence"] = write_result
        report["execution"]["transaction"] = (
            write_result["transaction"] + "; " + static_result["transaction"]
        )
        report["execution"]["database_write_attempted"] = (
            write_result["database_write_attempted"]
            or static_result["database_write_attempted"]
        )
        report["execution"]["dml_executed"] = (
            write_result["dml_executed"] or static_result["dml_executed"]
        )
    return report


__all__ = [
    "SourceResearchDayError",
    "prepare_source_research_day",
    "run_source_research_day",
]
