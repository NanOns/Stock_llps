"""数据初始化与手动日更的共享计划器和单后台执行协调器。"""

from __future__ import annotations

import threading
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from stock_a.domain import FieldValue
from stock_a.research.entry import _load_calendar
from stock_a.research.entry_context.sessions import CORE_MAX_ACTUAL_LOOKBACK
from stock_a.storage.postgres import data_jobs as data_job_pg
from stock_a.storage.postgres import business_reset as business_reset_pg


class DataJobError(Exception):
    """数据任务计划或后台执行错误。"""

    def __init__(self, code: str, message: str, *, status: int = 400):
        self.code = code
        self.status = status
        super().__init__(message)


def _market_breadth_symbol_readback_mismatches(
    expected: Any, actual: Any
) -> tuple[str, ...]:
    """逐代码核对QFQ宽度计算与跨事务PG Reader读取的全部typed值。"""
    fields = (
        "ret1_t",
        "ret1_t_minus_3",
        "positive_t",
        "positive_t_minus_3",
        "delta_indicator",
        "classification",
        "primary_unknown_reason",
        "unknown_reason_detail",
    )
    expected_rows = {row.source_symbol: row for row in getattr(expected, "symbols", ())}
    actual_rows = {row.source_symbol: row for row in getattr(actual, "symbols", ())}
    if not expected_rows or set(expected_rows) != set(actual_rows):
        return ("source_symbol_set",)
    mismatches = set()
    for symbol, expected_row in expected_rows.items():
        actual_row = actual_rows[symbol]
        mismatches.update(
            field
            for field in fields
            if getattr(expected_row, field) != getattr(actual_row, field)
        )
    return tuple(sorted(mismatches))


def _market_breadth_change_readback_mismatches(
    expected: Mapping[str, Any], actual: Mapping[str, Any]
) -> tuple[str, ...]:
    """核对相邻宽度事实的精确身份、集合、计数和有理数。"""
    scalar_fields = (
        "before_session",
        "target_session",
        "before_source_capture_id",
        "target_source_capture_id",
        "before_calendar_id",
        "target_calendar_id",
        "before_calendar_revision",
        "target_calendar_revision",
        "before_contract_version",
        "target_contract_version",
        "before_price_coordinate",
        "target_price_coordinate",
        "comparison_status",
        "reason_code",
        "source_sets_equal",
        "eligible_sets_equal",
        "unknown_sets_equal",
        "unknown_reason_mismatch_count",
        "source_count_before",
        "source_count_target",
        "common_source_count",
        "eligible_count_before",
        "eligible_count_target",
        "common_eligible_count",
        "unknown_count_before",
        "unknown_count_target",
        "breadth_direction",
        "improved_symbol_count",
        "deteriorated_symbol_count",
        "unchanged_symbol_count",
        "unknown_symbol_count",
    )
    mismatches = {
        name for name in scalar_fields if actual.get(name) != expected.get(name)
    }
    for name in ("breadth_before", "breadth_target", "breadth_delta"):
        ratio = expected.get(name)
        numerator = actual.get(f"{name}_numerator")
        denominator = actual.get(f"{name}_denominator")
        if ratio is None:
            if numerator is not None or denominator is not None:
                mismatches.add(name)
            continue
        if (
            not isinstance(ratio, Fraction)
            or numerator is None
            or denominator is None
            or int(numerator) != ratio.numerator
            or int(denominator) != ratio.denominator
        ):
            mismatches.add(name)
    return tuple(sorted(mismatches))


_MAX_SESSION_COUNT = 32767
_MAX_PRICE_HISTORY_SESSIONS = 250
_BAOSTOCK_KNOWN_SECURITY_TYPES = frozenset({"1", "2", "3", "4", "5"})
_BAOSTOCK_SOURCE_BOARDS = frozenset(
    {"MAIN_BOARD", "GEM", "STAR_BOARD", "BEIJING_BOARD"}
)
_BAOSTOCK_SYMBOL_RE = re.compile(r"^(SH|SZ|BJ)\.[0-9]{6}$")
_STAGE_TABLES = {
    "SOURCE_BAOSTOCK_PRICES": ("stock.source_raw_daily",),
    "SOURCE_BAOSTOCK_UNIVERSE": (
        "stock.source_baostock_stock_basic",
        "stock.source_baostock_session_directory",
    ),
    "SOURCE_BAOSTOCK_MARKET_INDICES": ("stock.source_market_index_daily",),
    "SOURCE_SECTOR_MEMBERS": (
        "stock.tdx_sector_source_snapshots",
        "stock.tdx_sector_catalog",
        "stock.tdx_sector_members",
    ),
    "CORE_PROFILE": ("stock.source_symbol_price_profiles",),
    "CROSS_SECTION": (
        "stock.source_cross_section_day_facts",
        "stock.source_cross_section_endpoints",
    ),
    "PRICE_LIMITS": ("stock.source_price_limit_day_facts",),
    "LIMIT_UP_LADDER": ("stock.source_limit_up_streak_day_facts",),
    "MARKET_BREADTH": (
        "stock.source_market_breadth_days",
        "stock.source_market_breadth_symbols",
        "stock.source_market_breadth_change_days",
        "stock.r03_formal_market_breadth_facts",
    ),
    "MARKET_PARTICIPATION": (
        "stock.source_market_participation_days",
        "stock.source_market_participation_unknown_reasons",
    ),
    "MARKET_INDICES_ENV": (
        "stock.source_market_index_day_facts",
        "stock.source_market_environment_day_facts",
    ),
    "STATIC_RESEARCH": (
        "stock.source_d0_scenario_day_facts",
        "stock.source_pool_seed_day_facts",
    ),
    "D0_D1": (
        "stock.source_d0_launch_day_facts",
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_research_enrollments",
    ),
    "FOCUS_FORMAL": ("stock.source_d0_formal_admission_day_facts",),
    "FOCUS_RESEARCH_CONTEXT": (
        "stock.source_rule_evaluations",
        "stock.source_t0_early_observation_facts",
    ),
    "SECTOR_PULSE": ("stock.source_sector_cross_period_pulse_facts",),
    "SECTOR_TRANSITION": ("stock.source_sector_pulse_adjacent_transitions",),
    "SECTOR_FOLLOWUP": (
        "stock.source_sector_pulse_followup_facts",
        "stock.source_sector_pulse_cohort_group_facts",
        "stock.source_sector_pulse_cohort_reason_facts",
    ),
    "FORWARD_PATH": ("stock.source_forward_path_facts",),
    "COHORT_STATS": ("stock.source_cohort_group_stats",),
    "FEP_BASELINE": ("stock.source_fep_baseline_facts",),
    "OBSERVATION_SEGMENTS": ("stock.source_research_observation_segment_days",),
}


def _validated_baostock_symbol(value: Any, *, label: str) -> str:
    from stock_a.storage.postgres.baostock_universe import normalize_baostock_symbol

    parsed = normalize_baostock_symbol(value)
    if parsed is None:
        raise DataJobError(
            "BAOSTOCK_RESPONSE_SYMBOL_INVALID", f"{label}证券代码无法规范化"
        )
    return parsed[0]


def _validate_basic_catalog_response(rows: Any) -> tuple[str, ...]:
    """按响应字段合同独立核验本次主资料响应键，不推断权威在市总量。"""
    if not isinstance(rows, (tuple, list)) or not rows:
        raise DataJobError(
            "BAOSTOCK_STOCK_BASIC_RESPONSE_EMPTY", "BaoStock基本资料成功响应为空"
        )
    symbols = []
    required_fields = ("code", "code_name", "ipoDate", "outDate", "type", "status")
    for row in rows:
        if not isinstance(row, Mapping) or any(
            name not in row for name in required_fields
        ):
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_RESPONSE_FIELDS_INVALID",
                "BaoStock基本资料行缺少SDK响应合同字段",
            )
        symbol = _validated_baostock_symbol(row.get("code"), label="基本资料")
        if not isinstance(row.get("code_name"), str):
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_NAME_INVALID", f"{symbol}证券名称字段类型无效"
            )
        if str(row.get("type") or "").strip() not in _BAOSTOCK_KNOWN_SECURITY_TYPES:
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_TYPE_INVALID", f"{symbol}证券类型代码无效"
            )
        if str(row.get("status") or "").strip() not in {"0", "1"}:
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_STATUS_INVALID", f"{symbol}上市状态代码无效"
            )
        ipo_text = str(row.get("ipoDate") or "").strip()
        out_text = str(row.get("outDate") or "").strip()
        dates = []
        for text in (ipo_text, out_text):
            if not text:
                dates.append(None)
                continue
            try:
                parsed_date = date.fromisoformat(text)
            except ValueError as error:
                raise DataJobError(
                    "BAOSTOCK_STOCK_BASIC_DATE_INVALID",
                    f"{symbol}上市或退市日期格式无效",
                ) from error
            if parsed_date.isoformat() != text:
                raise DataJobError(
                    "BAOSTOCK_STOCK_BASIC_DATE_INVALID",
                    f"{symbol}上市或退市日期格式无效",
                )
            dates.append(parsed_date)
        if dates[0] is not None and dates[1] is not None and dates[1] < dates[0]:
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_DATE_ORDER_INVALID",
                f"{symbol}退市日期早于上市日期",
            )
        symbols.append(symbol)
    if len(symbols) != len(set(symbols)):
        raise DataJobError(
            "BAOSTOCK_STOCK_BASIC_DUPLICATE_SYMBOL", "基本资料响应含重复证券代码"
        )
    return tuple(sorted(symbols))


def _validate_session_directory_response(
    rows_by_session: Any, *, requested_sessions: Sequence[date]
) -> dict[date, tuple[tuple[str, str], ...]]:
    """要求逐日响应键完整、每个官方会话有成员且身份/交易状态有效。"""
    if not isinstance(rows_by_session, Mapping):
        raise DataJobError(
            "BAOSTOCK_SESSION_DIRECTORY_RESPONSE_INVALID", "逐日名录响应不是按会话分组"
        )
    expected_keys = {session.isoformat() for session in requested_sessions}
    if set(rows_by_session) != expected_keys:
        raise DataJobError(
            "BAOSTOCK_SESSION_DIRECTORY_DATES_INCOMPLETE",
            "逐日名录响应会话键与本次官方请求范围不一致",
        )
    normalized: dict[date, tuple[tuple[str, str], ...]] = {}
    for session in requested_sessions:
        rows = rows_by_session[session.isoformat()]
        if not isinstance(rows, (tuple, list)) or not rows:
            raise DataJobError(
                "BAOSTOCK_SESSION_DIRECTORY_DAY_EMPTY",
                f"{session.isoformat()}逐日名录成功响应为空",
            )
        members: list[tuple[str, str]] = []
        for row in rows:
            if (
                not isinstance(row, Mapping)
                or any(name not in row for name in ("code", "tradeStatus", "code_name"))
                or not isinstance(row.get("code_name"), str)
            ):
                raise DataJobError(
                    "BAOSTOCK_SESSION_DIRECTORY_FIELDS_INVALID",
                    f"{session.isoformat()}名录行缺少必需字段",
                )
            symbol = _validated_baostock_symbol(row.get("code"), label="逐日名录")
            status = str(row.get("tradeStatus") or "").strip()
            if status not in {"0", "1"}:
                raise DataJobError(
                    "BAOSTOCK_SESSION_DIRECTORY_STATUS_INVALID",
                    f"{session.isoformat()} {symbol}交易状态无效",
                )
            members.append((symbol, "TRADING" if status == "1" else "SUSPENDED"))
        if len({symbol for symbol, _ in members}) != len(members):
            raise DataJobError(
                "BAOSTOCK_SESSION_DIRECTORY_DUPLICATE_SYMBOL",
                f"{session.isoformat()}逐日名录存在重复证券代码",
            )
        normalized[session] = tuple(sorted(members))
    return normalized


def _index_decimal(value: Any, *, field: str, required: bool) -> Decimal | None:
    if value is None or isinstance(value, bool) or not str(value).strip():
        if required:
            raise DataJobError(
                "BAOSTOCK_INDEX_FIELD_MISSING", f"指数响应缺少必需数值字段{field}"
            )
        return None
    try:
        parsed = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as error:
        raise DataJobError(
            "BAOSTOCK_INDEX_FIELD_INVALID", f"指数响应数值字段{field}格式无效"
        ) from error
    if not parsed.is_finite():
        raise DataJobError(
            "BAOSTOCK_INDEX_FIELD_INVALID", f"指数响应数值字段{field}不是有限值"
        )
    return parsed


_STAGE_ORDER = {
    "BUSINESS_RESET": 1,
    "SOURCE_BAOSTOCK_PRICES": 1,
    "SOURCE_BAOSTOCK_UNIVERSE": 1,
    "SOURCE_BAOSTOCK_MARKET_INDICES": 1,
    "SOURCE_SECTOR_MEMBERS": 2,
    "PRICE_LIMITS": 2,
    "LIMIT_UP_LADDER": 3,
    "CROSS_SECTION": 3,
    "MARKET_BREADTH": 3,
    "CORE_PROFILE": 4,
    "MARKET_PARTICIPATION": 5,
    "MARKET_INDICES_ENV": 6,
    "D0_D1": 6,
    "STATIC_RESEARCH": 7,
    "FOCUS_FORMAL": 8,
    "FOCUS_RESEARCH_CONTEXT": 8,
    "SECTOR_PULSE": 9,
    "R03_SECTOR_NATIVE": 9,
    "SECTOR_TRANSITION": 10,
    "R03_SECTOR_ROTATION": 10,
    "R03_SECTOR_OVERLAP": 10,
    "SECTOR_FOLLOWUP": 11,
    "R03_SECTOR_LOO": 11,
    "FORWARD_PATH": 12,
    "COHORT_STATS": 13,
    "FEP_BASELINE": 14,
    "OBSERVATION_SEGMENTS": 15,
}
_RAW_JOB_SCOPE = "STOCK_A_R08_BIZ08_DATA_JOB_RAW_IMPORT_R1"

_MODULES = {
    "BAOSTOCK_PRICES": ("SOURCE", "BaoStock股票日线价格", True, True),
    "BAOSTOCK_UNIVERSE": ("SOURCE", "BaoStock证券主资料与逐日名录", True, True),
    "BAOSTOCK_MARKET_INDICES": ("SOURCE", "BaoStock通用参考指数日线", True, True),
    "TDX_SECTOR_MEMBERS": ("SOURCE", "行业与GN概念成员来源", True, True),
    "CORE_PROFILE": ("BASE", "Core / Profile", True, True),
    "CROSS_SECTION": ("BASE", "全市场横截面10因子", True, True),
    "PRICE_LIMITS": ("BASE", "涨跌停限价", True, True),
    "LIMIT_UP_LADDER": ("BASE", "连板梯队", True, True),
    "MARKET_BREADTH": ("BASE", "市场宽度", True, True),
    "MARKET_PARTICIPATION": ("BASE", "成交参与", True, True),
    "MARKET_INDICES_ENV": ("BASE", "参考指数因子与市场复合环境", True, True),
    "D0": ("RESEARCH", "D0 / Seed研究", True, True),
    "D1": ("RESEARCH", "D1结构与材料变化", True, True),
    "FOCUS_FORMAL": ("RESEARCH", "Focus正式准入", True, True),
    "SECTOR_PULSE": ("RESEARCH", "行业 / GN概念PULSE", True, True),
    "SECTOR_TRANSITION": ("RESEARCH", "行业 / GN概念相邻变化", True, True),
    "SECTOR_FOLLOWUP": ("RESEARCH", "板块跨日跟踪", True, True),
    "R03_SECTOR_NATIVE": ("RESEARCH", "R03正式板块Native与资格", True, True),
    "R03_SECTOR_ROTATION": ("RESEARCH", "R03正式Rotation生命周期", True, True),
    "R03_SECTOR_OVERLAP": ("RESEARCH", "R03正式Overlap簇与代表", True, True),
    "R03_SECTOR_LOO": ("RESEARCH", "R03正式LOO与支持板块", True, True),
    "FORWARD_PATH": ("RESEARCH", "Forward路径", True, True),
    "COHORT_STATS": ("RESEARCH", "Cohort分组统计", True, True),
    "FEP_BASELINE": ("RESEARCH", "FEP条件基线", True, True),
    "OBSERVATION_SEGMENTS": ("RESEARCH", "观察持续分段", True, True),
}
_ALL_SOURCE_MODULES = frozenset(
    {
        "BAOSTOCK_PRICES",
        "BAOSTOCK_UNIVERSE",
        "BAOSTOCK_MARKET_INDICES",
        "TDX_SECTOR_MEMBERS",
    }
)
_UNCOORDINATED_RESEARCH_CONSUMERS: tuple[tuple[str, str, str], ...] = ()
_CLEANUP_OPERATIONS = {"CLEAR_DERIVED", "CLEAR_AND_REGENERATE"}
_OPERATIONS = {
    "FILL_MISSING",
    "RECOMPUTE",
    "CLEAR_DERIVED",
    "CLEAR_AND_REGENERATE",
}


def _positive_integer(value: Any, name: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if type(value) is not int or not minimum <= value <= _MAX_SESSION_COUNT:
        raise DataJobError(
            "DATA_JOB_INPUT_INVALID",
            f"{name}必须是{minimum}至{_MAX_SESSION_COUNT}之间的整数。",
        )
    return value


def _request_fields(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise DataJobError("DATA_JOB_INPUT_INVALID", "任务参数必须是JSON对象。")
    allowed = {
        "mode",
        "target_session",
        "history_session_count",
        "profile_history_session_count",
        "d1_history_session_count",
        "d0_output_session_count",
        "d1_output_session_count",
        "source_modules",
        "base_modules",
        "research_modules",
        "module_output_session_counts",
        "output_session_count",
        "operation",
        "initialization_mode",
        "daily_mode",
        "observation_rule_ids",
        "replace_known_incomplete_observation_successors",
        "reset_confirmed",
        "reset_preview_fingerprint",
        "sector_snapshot_ids_by_session",
    }
    if set(body) - allowed:
        raise DataJobError("DATA_JOB_INPUT_INVALID", "任务参数包含未登记字段。")
    mode = body.get("mode")
    if mode not in {"INITIALIZE", "MANUAL_DAILY"}:
        raise DataJobError(
            "DATA_JOB_MODE_INVALID", "任务模式须为INITIALIZE或MANUAL_DAILY。"
        )
    initialization_mode = body.get("initialization_mode", "REUSE_RAW")
    daily_mode = body.get("daily_mode", "CUSTOM")
    if mode == "INITIALIZE":
        if initialization_mode not in {"REUSE_RAW", "RESET_ALL_BUSINESS_FACTS"}:
            raise DataJobError(
                "DATA_JOB_INITIALIZATION_MODE_INVALID", "初始化方式无效。"
            )
        if daily_mode != "CUSTOM":
            raise DataJobError(
                "DATA_JOB_DAILY_MODE_INVALID", "初始化任务不能使用日更预设。"
            )
    elif initialization_mode != "REUSE_RAW" or daily_mode not in {
        "FULL_THREE_LAYERS",
        "SOURCE_ONLY",
        "CUSTOM",
    }:
        raise DataJobError("DATA_JOB_DAILY_MODE_INVALID", "手动日更方式无效。")
    source_modules = body.get("source_modules")
    base_modules = body.get("base_modules")
    research_modules = body.get("research_modules")
    has_module_selection = any(
        key in body for key in ("source_modules", "base_modules", "research_modules")
    )
    selections = {
        "source": source_modules if source_modules is not None else [],
        "base": base_modules if base_modules is not None else [],
        "research": research_modules if research_modules is not None else [],
    }
    if any(
        not isinstance(values, list)
        or any(not isinstance(value, str) or value not in _MODULES for value in values)
        or len(values) != len(set(values))
        for values in selections.values()
    ):
        raise DataJobError("DATA_JOB_MODULE_INVALID", "任务模块选择无效。")
    modules = {value for values in selections.values() for value in values}
    for layer, values in selections.items():
        expected_layer = {"source": "SOURCE", "base": "BASE", "research": "RESEARCH"}[
            layer
        ]
        if any(_MODULES[value][0] != expected_layer for value in values):
            raise DataJobError(
                "DATA_JOB_MODULE_LAYER_INVALID", "模块被放入错误的数据层。"
            )
    counts = body.get("module_output_session_counts", {})
    if not isinstance(counts, dict) or any(
        not isinstance(key, str)
        or key not in modules
        or type(value) is not int
        or not 1 <= value <= _MAX_SESSION_COUNT
        for key, value in counts.items()
    ):
        raise DataJobError("DATA_JOB_OUTPUT_COUNT_INVALID", "模块输出会话配置无效。")
    operation = body.get("operation", "FILL_MISSING")
    if operation not in _OPERATIONS:
        raise DataJobError("DATA_JOB_OPERATION_INVALID", "任务执行方式无效。")
    if operation in _CLEANUP_OPERATIONS and (
        selections["source"]
        or len(selections["base"]) + len(selections["research"]) != 1
    ):
        raise DataJobError(
            "DATA_JOB_CLEANUP_SCOPE_INVALID",
            "派生清理只允许单个基础或研究模块，不能包含基础来源。",
        )
    legacy = not has_module_selection
    if legacy:
        # BIZ08请求保持兼容；新界面始终显式发送模块列表。
        selections = {
            "source": ["BAOSTOCK_PRICES", "TDX_SECTOR_MEMBERS"],
            "base": ["CORE_PROFILE", "MARKET_BREADTH", "MARKET_PARTICIPATION"],
            "research": [],
        }
        if body.get("d0_output_session_count", 0):
            selections["research"].append("D0")
        if body.get("d1_output_session_count", 0):
            selections["research"].append("D1")
        modules = {value for values in selections.values() for value in values}
        counts = {}
    if not modules:
        raise DataJobError("DATA_JOB_MODULE_SELECTION_EMPTY", "至少选择一个实际模块。")
    observation_rule_ids = body.get("observation_rule_ids")
    if observation_rule_ids is not None:
        from stock_a.storage.postgres.research_facts import RULES

        if (
            not isinstance(observation_rule_ids, list)
            or not observation_rule_ids
            or any(
                not isinstance(rule_id, str) or rule_id not in RULES
                for rule_id in observation_rule_ids
            )
            or len(observation_rule_ids) != len(set(observation_rule_ids))
            or "OBSERVATION_SEGMENTS" not in modules
        ):
            raise DataJobError(
                "DATA_JOB_OBSERVATION_RULE_SCOPE_INVALID",
                "观察段规则过滤必须是现行登记规则的非空唯一子集。",
            )
        observation_rule_ids = sorted(observation_rule_ids)
    replace_known_incomplete = body.get(
        "replace_known_incomplete_observation_successors", False
    )
    if type(replace_known_incomplete) is not bool:
        raise DataJobError(
            "DATA_JOB_OBSERVATION_REPAIR_SCOPE_INVALID",
            "观察段陈旧后继修复范围必须是布尔值。",
        )
    if replace_known_incomplete and (
        mode != "MANUAL_DAILY"
        or daily_mode != "CUSTOM"
        or selections["source"]
        or selections["base"]
        or selections["research"] != ["OBSERVATION_SEGMENTS"]
        or operation != "FILL_MISSING"
        or counts.get("OBSERVATION_SEGMENTS") != 1
        or observation_rule_ids is None
        or len(observation_rule_ids) != 1
    ):
        raise DataJobError(
            "DATA_JOB_OBSERVATION_REPAIR_SCOPE_INVALID",
            "陈旧观察后继修复仅允许单会话、单规则观察段FILL_MISSING任务。",
        )
    profile_history_count = body.get("profile_history_session_count")
    if profile_history_count is not None:
        profile_history_count = _positive_integer(
            profile_history_count, "profile_history_session_count"
        )
    elif legacy and mode == "INITIALIZE":
        # BIZ08/BIZ09请求曾把Raw历史数同时作为Profile窗口；旧请求仍按原语义解析。
        profile_history_count = body.get("history_session_count")
    d1_history_count = body.get("d1_history_session_count")
    if d1_history_count is not None:
        d1_history_count = _positive_integer(
            d1_history_count, "d1_history_session_count"
        )
        if d1_history_count < 2 or d1_history_count > 60:
            raise DataJobError(
                "DATA_JOB_D1_WINDOW_INVALID", "D1历史窗口须为2至60个官方会话。"
            )
    if mode == "INITIALIZE" and initialization_mode == "RESET_ALL_BUSINESS_FACTS":
        if not {"BAOSTOCK_PRICES", "TDX_SECTOR_MEMBERS"}.issubset(
            set(selections["source"])
        ):
            raise DataJobError(
                "DATA_JOB_RESET_SOURCE_SELECTION_INVALID",
                "清空全部业务事实后重建必须包含BaoStock价格与行业/GN成员来源。",
            )
    if mode == "MANUAL_DAILY" and daily_mode == "SOURCE_ONLY":
        if (
            selections["base"]
            or selections["research"]
            or not selections["source"]
            or not set(selections["source"]).issubset(_ALL_SOURCE_MODULES)
        ):
            raise DataJobError(
                "DATA_JOB_SOURCE_ONLY_SCOPE_INVALID",
                "SOURCE_ONLY只允许BaoStock价格、证券主资料/名录、参考指数和/或TDX行业成员来源，不得选择基础画像或研究模块。",
            )
    if mode == "MANUAL_DAILY" and daily_mode == "FULL_THREE_LAYERS":
        required_sources = set(_ALL_SOURCE_MODULES)
        required_base = {
            code
            for code, (layer, _label, implemented, available) in _MODULES.items()
            if layer == "BASE" and implemented and available
        }
        required_research = {
            code
            for code, (layer, _label, implemented, available) in _MODULES.items()
            if layer == "RESEARCH" and implemented and available
        }
        if (
            not required_sources.issubset(set(selections["source"]))
            or set(selections["base"]) != required_base
            or set(selections["research"]) != required_research
        ):
            raise DataJobError(
                "DATA_JOB_FULL_THREE_LAYERS_SCOPE_INVALID",
                "完整三层日更必须选择全部现役基础画像与研究模块，以及BaoStock价格/主资料名录/参考指数和TDX行业/GN成员来源。",
            )
    if mode == "INITIALIZE":
        history_count = _positive_integer(
            body.get("history_session_count"), "history_session_count"
        )
        if history_count > _MAX_PRICE_HISTORY_SESSIONS:
            raise DataJobError(
                "DATA_JOB_HISTORY_WINDOW_INVALID",
                f"初始化历史会话数必须不超过{_MAX_PRICE_HISTORY_SESSIONS}。",
            )
    return {
        "mode": mode,
        "target_session": body.get("target_session"),
        "history_session_count": body.get("history_session_count"),
        "profile_history_session_count": profile_history_count,
        "d1_history_session_count": d1_history_count,
        "d0_output_session_count": body.get("d0_output_session_count", 0),
        "d1_output_session_count": body.get("d1_output_session_count", 0),
        "source_modules": selections["source"],
        "base_modules": selections["base"],
        "research_modules": selections["research"],
        "selected_modules": sorted(modules),
        "module_output_session_counts": counts,
        "output_session_count": body.get("output_session_count"),
        "operation": operation,
        "initialization_mode": initialization_mode,
        "daily_mode": daily_mode,
        "observation_rule_ids": observation_rule_ids,
        "replace_known_incomplete_observation_successors": replace_known_incomplete,
        "reset_confirmed": body.get("reset_confirmed") is True,
        "reset_preview_fingerprint": body.get("reset_preview_fingerprint"),
        "has_module_selection": has_module_selection,
        "legacy_request": legacy,
        "sector_snapshot_ids_by_session": (
            body.get("sector_snapshot_ids_by_session")
            if isinstance(body.get("sector_snapshot_ids_by_session"), Mapping)
            else None
        ),
    }


def _parse_session(value: Any) -> date:
    if not isinstance(value, str):
        raise DataJobError("DATA_JOB_TARGET_INVALID", "目标会话必须是YYYY-MM-DD日期。")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise DataJobError(
            "DATA_JOB_TARGET_INVALID", "目标会话必须是YYYY-MM-DD有效日期。"
        ) from error
    if parsed.isoformat() != value:
        raise DataJobError("DATA_JOB_TARGET_INVALID", "目标会话日期格式无效。")
    return parsed


def _load_official_sessions(target: date) -> tuple[str, str, tuple[date, ...]]:
    try:
        payload, _metadata, _revision_chain, sessions = _load_calendar(target)
    except Exception as error:
        raise DataJobError(
            "OFFICIAL_CALENDAR_UNAVAILABLE",
            "没有已核验官方日历覆盖所选目标会话。",
            status=503,
        ) from error
    calendar_id = payload.get("calendar_id")
    calendar_revision = payload.get("revision")
    if not isinstance(calendar_id, str) or not isinstance(calendar_revision, str):
        raise DataJobError(
            "OFFICIAL_CALENDAR_INVALID", "官方日历身份或修订缺失。", status=503
        )
    return calendar_id, calendar_revision, tuple(sessions)


def recent_official_sessions(
    official_sessions: Sequence[date], *, through: date, count: int
) -> tuple[date, ...]:
    """按正式日历选择最近N个会话，不用自然日偏移推算交易日。"""
    if (
        type(through) is not date
        or type(count) is not int
        or count < 0
        or any(type(session) is not date for session in official_sessions)
        or tuple(sorted(set(official_sessions))) != tuple(official_sessions)
    ):
        raise DataJobError("OFFICIAL_SESSION_WINDOW_INVALID", "官方会话窗口参数无效。")
    if count == 0:
        return ()
    eligible = tuple(session for session in official_sessions if session <= through)
    if len(eligible) < count:
        raise DataJobError(
            "OFFICIAL_SESSION_WINDOW_INSUFFICIENT",
            "已核验官方日历会话不足以形成请求窗口。",
        )
    return eligible[-count:]


def select_session_windows(
    official_sessions: Sequence[date],
    *,
    through: date,
    raw_history_count: int,
    d0_output_count: int,
    d1_output_count: int,
) -> dict[str, tuple[date, ...]]:
    """分别解析Raw、D0、D1最近官方会话窗口，所有窗口使用同一官方日历。"""
    raw_count = _positive_integer(raw_history_count, "raw_history_count")
    d0_count = _positive_integer(d0_output_count, "d0_output_count", allow_zero=True)
    d1_count = _positive_integer(d1_output_count, "d1_output_count", allow_zero=True)
    return {
        "raw": recent_official_sessions(
            official_sessions, through=through, count=raw_count
        ),
        "d0": recent_official_sessions(
            official_sessions, through=through, count=d0_count
        ),
        "d1": recent_official_sessions(
            official_sessions, through=through, count=d1_count
        ),
    }


def _module_output_sessions(
    *,
    module: str,
    requested_count: int,
    sessions: Sequence[date],
    target: date,
    operation: str,
    raw_coverage: Mapping[str, Any],
) -> tuple[date, ...]:
    requested = _positive_integer(requested_count, f"{module}输出会话数")
    chosen = recent_official_sessions(sessions, through=target, count=requested)
    # FILL_MISSING候选不能按日期水位先行剔除。是否可跳过由任务完整请求键、
    # 来源包/捕获、算法/窗口身份以及实际PG表读回共同决定。
    return chosen


def _forward_path_dependency_status(
    due_horizons: Sequence[int], *, t1_seed_available: bool
) -> tuple[str, str | None, str | None]:
    if not due_horizons:
        return (
            "NOT_DUE",
            "FORWARD_PATH_NOT_DUE",
            "研究截止内尚无到期Forward后继会话。",
        )
    if not t1_seed_available:
        return (
            "NOT_COMPUTED",
            "FORWARD_T1_SEED_UNAVAILABLE",
            "T+1官方会话的精确Seed事实未在本任务计划或已存结果中闭合；保留未计算，可补齐依赖后重试。",
        )
    return "NOT_STARTED", None, None


def _unit(
    units: list[dict[str, Any]],
    *,
    stage: str,
    target_session: date,
    business_table: str,
    status: str = "NOT_STARTED",
    error_code: str | None = None,
    error_message: str | None = None,
    source_capture_id: int | None = None,
    price_coordinates: Sequence[str] | None = None,
) -> None:
    units.append(
        {
            "stage_order": _STAGE_ORDER[stage],
            "stage_code": stage,
            "target_session": target_session,
            "business_table": business_table,
            "planned_units": 1,
            "status": status,
            "error_code": error_code,
            "error_message": error_message,
            "source_capture_id": source_capture_id,
            "price_coordinates": list(price_coordinates or ()),
        }
    )


def _price_coordinate_versions(
    raw_coverage: Mapping[str, Any], *, cutoff: date
) -> tuple[dict[str, dict[str, Mapping[str, Any]]], str]:
    """按session×坐标读取PG paired capture；TDX package和进程内票据不能回退充数。"""
    if "price_coordinate_versions" not in raw_coverage:
        return {}, "WAITING_FOR_SOURCE_CAPTURE_READER"
    rows = raw_coverage.get("price_coordinate_versions")
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return {}, "PRICE_COORDINATE_COVERAGE_INVALID"
    versions: dict[str, dict[str, Mapping[str, Any]]] = {}
    seen_coordinates: set[tuple[str, str]] = set()
    for item in rows:
        if not isinstance(item, Mapping):
            return {}, "PRICE_COORDINATE_COVERAGE_INVALID"
        session_text = item.get("session_date")
        capture_id = item.get("source_capture_id")
        coordinate = item.get("price_coordinate")
        if (
            not isinstance(session_text, str)
            or type(capture_id) is not int
            or capture_id <= 0
            or coordinate not in {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        ):
            return {}, "PRICE_COORDINATE_COVERAGE_INVALID"
        try:
            session = date.fromisoformat(session_text)
        except ValueError:
            return {}, "PRICE_COORDINATE_COVERAGE_INVALID"
        if session.isoformat() != session_text or session > cutoff:
            return {}, "PRICE_COORDINATE_COVERAGE_INVALID"
        key = (session_text, coordinate)
        if key in seen_coordinates:
            return {}, "PRICE_COORDINATE_COVERAGE_DUPLICATE"
        seen_coordinates.add(key)
        by_coordinate = versions.setdefault(session_text, {})
        if (
            item.get("coverage_status") == "COMPLETE"
            and type(item.get("row_count")) is int
            and item.get("row_count", -1) >= 0
            and type(item.get("expected_row_count")) is int
            and item.get("expected_row_count", -1) >= 0
            and item.get("row_count") == item.get("expected_row_count")
        ):
            by_coordinate[coordinate] = item
    return versions, str(
        raw_coverage.get("price_coordinate_reader_status", "AVAILABLE")
    )


def build_data_job_plan(
    body: Any,
    *,
    cutoff: date,
    raw_coverage: Mapping[str, Any],
    execution_authorized: bool,
    sector_pair: Mapping[str, Any] | None = None,
    _forward_path_seed_availability_out: dict[str, bool] | None = None,
) -> dict[str, Any]:
    from datetime import datetime

    """按官方日历、BaoStock坐标捕获和截止日生成不写数据的任务计划。"""
    request = _request_fields(body)
    sector_pair = sector_pair if isinstance(sector_pair, Mapping) else {}
    if type(cutoff) is not date:
        raise DataJobError("DATA_JOB_CUTOFF_REQUIRED", "服务必须绑定明确研究截止日。")
    mode = request["mode"]
    latest_raw = raw_coverage.get("last_stored_session")
    raw_history_count = request["history_session_count"]
    profile_history = request["profile_history_session_count"]
    retrying_daily = mode == "MANUAL_DAILY" and request["target_session"] is not None
    if mode == "INITIALIZE":
        raw_history_count = _positive_integer(
            raw_history_count, "history_session_count"
        )
        if profile_history is None:
            profile_history = min(CORE_MAX_ACTUAL_LOOKBACK, raw_history_count)
        profile_history = _positive_integer(
            profile_history, "profile_history_session_count"
        )
        if profile_history > raw_history_count:
            raise DataJobError(
                "DATA_JOB_HISTORY_WINDOW_INVALID",
                "Profile历史窗口不能超过用户显式选择的价格来源会话数。",
            )
    else:
        needs_profile_window = bool(
            {"CORE_PROFILE", "MARKET_PARTICIPATION", "D0", "FOCUS_FORMAL"}
            & set(request["selected_modules"])
        )
        if profile_history is None:
            profile_history = raw_coverage.get("current_profile_history_session_count")
        if profile_history is None:
            profile_history = CORE_MAX_ACTUAL_LOOKBACK if needs_profile_window else 1
        profile_history = _positive_integer(
            profile_history, "profile_history_session_count"
        )

    output_counts = request["module_output_session_counts"]
    default_output_count = request["output_session_count"]
    if default_output_count is not None:
        default_output_count = _positive_integer(
            default_output_count, "output_session_count"
        )
    selected_modules = set(request["selected_modules"])
    output_count_fallback = (
        default_output_count if default_output_count is not None else 5
    )
    if request["legacy_request"]:
        d0_requested = _positive_integer(
            request["d0_output_session_count"],
            "d0_output_session_count",
            allow_zero=True,
        )
        d1_requested = _positive_integer(
            request["d1_output_session_count"],
            "d1_output_session_count",
            allow_zero=True,
        )
    else:
        d0_requested = output_counts.get(
            "D0", output_count_fallback if "D0" in selected_modules else 0
        )
        d1_requested = output_counts.get(
            "D1", output_count_fallback if "D1" in selected_modules else 0
        )
        d0_requested = _positive_integer(d0_requested, "D0输出会话数", allow_zero=True)
        d1_requested = _positive_integer(d1_requested, "D1输出会话数", allow_zero=True)
    selected_source_modules = set(request["source_modules"])
    selected_base_modules = set(request["base_modules"])
    selected_research_modules = set(request["research_modules"])
    operation = request["operation"]
    reset_preflight_required = (
        mode == "INITIALIZE"
        and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
    )
    capture_versions, price_reader_status = _price_coordinate_versions(
        raw_coverage, cutoff=cutoff
    )
    if (
        mode == "INITIALIZE"
        and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
    ):
        # 重置预览不能把随后会删除的PG捕获事实当成可复用版本。
        capture_versions = {}
    if mode == "INITIALIZE":
        calendar_id, calendar_revision, all_sessions = _load_official_sessions(cutoff)
        target = (
            _parse_session(request["target_session"])
            if request["target_session"] is not None
            else max((day for day in all_sessions if day <= cutoff), default=None)
        )
        if target is None:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "OFFICIAL_CALENDAR_UNAVAILABLE",
                ["NO_OFFICIAL_SESSION_THROUGH_CUTOFF"],
            )
        if target > cutoff:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "BLOCKED_NOT_AUTHORIZED",
                ["TARGET_AFTER_RESEARCH_CUTOFF"],
                target_session=target.isoformat(),
            )
        if target not in all_sessions:
            raise DataJobError(
                "DATA_JOB_TARGET_NOT_TRADING_SESSION",
                "目标日期不是官方交易会话。",
            )
        eligible = tuple(day for day in all_sessions if day <= target and day <= cutoff)
        daily_sessions = ()
    else:
        calendar_id, calendar_revision, all_sessions = _load_official_sessions(cutoff)
        if retrying_daily:
            target = _parse_session(request["target_session"])
            if target > cutoff:
                return _blocked_plan(
                    mode,
                    cutoff,
                    raw_coverage,
                    "BLOCKED_NOT_AUTHORIZED",
                    ["TARGET_AFTER_RESEARCH_CUTOFF"],
                    target_session=target.isoformat(),
                    calendar_id=calendar_id,
                    calendar_revision=calendar_revision,
                )
            if target not in all_sessions:
                raise DataJobError(
                    "DATA_JOB_TARGET_NOT_TRADING_SESSION",
                    "待恢复任务的目标日期不是官方交易会话。",
                )
            eligible = tuple(
                day for day in all_sessions if day <= target and day <= cutoff
            )
            daily_sessions = ()
        else:
            eligible = tuple(day for day in all_sessions if day <= cutoff)
            if not eligible:
                return _blocked_plan(
                    mode,
                    cutoff,
                    raw_coverage,
                    "OFFICIAL_CALENDAR_UNAVAILABLE",
                    ["NO_OFFICIAL_SESSION_THROUGH_CUTOFF"],
                    target_session=None,
                    calendar_id=calendar_id,
                    calendar_revision=calendar_revision,
                )
            if request["legacy_request"]:
                last_completed = raw_coverage.get("last_completed_research_session")
                last = (
                    _parse_session(last_completed)
                    if last_completed
                    else _parse_session(latest_raw)
                    if latest_raw
                    else None
                )
                daily_sessions = tuple(
                    day for day in eligible if last is None or day > last
                )
                if not daily_sessions:
                    if operation in {"RECOMPUTE", *_CLEANUP_OPERATIONS}:
                        daily_sessions = (eligible[-1],)
                    else:
                        return _blocked_plan(
                            mode,
                            cutoff,
                            raw_coverage,
                            "BLOCKED_NOT_AUTHORIZED",
                            ["NO_AUTHORIZED_NEW_OFFICIAL_SESSION"],
                            target_session=None,
                            calendar_id=calendar_id,
                            calendar_revision=calendar_revision,
                        )
                target = daily_sessions[-1]
            else:
                # 新版逐模块水位负责决定补算日期；全局研究水位不能遮住落后的单域。
                target = eligible[-1]
                daily_sessions = (target,)

    raw_request_count = (
        raw_history_count
        if mode == "INITIALIZE"
        else _positive_integer(
            output_counts.get("BAOSTOCK_PRICES", 1),
            "BaoStock价格输出会话数",
        )
    )
    if mode == "MANUAL_DAILY":
        raw_history_count = raw_request_count
    baostock_output_sessions = (
        recent_official_sessions(eligible, through=target, count=raw_request_count)
        if "BAOSTOCK_PRICES" in selected_source_modules
        else ()
    )
    if mode == "INITIALIZE":
        requested_target_count = raw_request_count
        if len(eligible) < requested_target_count:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "HISTORY_INSUFFICIENT",
                ["OFFICIAL_CALENDAR_WINDOW_TOO_SHORT"],
                target_session=target.isoformat(),
                requested_history_session_count=requested_target_count,
                official_sessions_available=len(eligible),
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
            )
        windows = select_session_windows(
            eligible,
            through=target,
            raw_history_count=requested_target_count,
            d0_output_count=d0_requested,
            d1_output_count=d1_requested,
        )
        raw_window = (
            windows["raw"] if "BAOSTOCK_PRICES" in selected_source_modules else ()
        )
        d0_count = d0_requested
        d1_count = d1_requested
    else:
        if len(eligible) < profile_history:
            return _blocked_plan(
                mode,
                cutoff,
                raw_coverage,
                "HISTORY_INSUFFICIENT",
                ["OFFICIAL_CALENDAR_WINDOW_TOO_SHORT"],
                target_session=target.isoformat(),
                requested_history_session_count=profile_history,
                official_sessions_available=len(eligible),
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
            )
        raw_window = (
            recent_official_sessions(eligible, through=target, count=raw_request_count)
            if "BAOSTOCK_PRICES" in selected_source_modules
            else ()
        )
        d0_count = (
            len(daily_sessions)
            if request["legacy_request"] and not retrying_daily
            else d0_requested
        )
        d1_count = (
            len(daily_sessions)
            if request["legacy_request"] and not retrying_daily
            else d1_requested
        )

    if d0_count > len(eligible) or d1_count > len(eligible):
        return _blocked_plan(
            mode,
            cutoff,
            raw_coverage,
            "HISTORY_INSUFFICIENT",
            ["REQUESTED_OUTPUT_SESSION_COUNT_EXCEEDS_CALENDAR"],
            target_session=target.isoformat(),
            requested_history_session_count=profile_history,
            official_sessions_available=len(eligible),
            d0_output_session_count=d0_count,
            d1_output_session_count=d1_count,
            calendar_id=calendar_id,
            calendar_revision=calendar_revision,
        )

    module_counts = request["module_output_session_counts"]
    module_output_sessions: dict[str, tuple[date, ...]] = {}
    for module in sorted(selected_base_modules | selected_research_modules):
        if module == "D0":
            count = d0_count
        elif module == "D1":
            count = d1_count
        else:
            fallback = (
                output_count_fallback
                if module
                in {
                    "CORE_PROFILE",
                    "CROSS_SECTION",
                    "PRICE_LIMITS",
                    "LIMIT_UP_LADDER",
                    "MARKET_BREADTH",
                    "MARKET_PARTICIPATION",
                    "MARKET_INDICES_ENV",
                    "SECTOR_PULSE",
                    "SECTOR_TRANSITION",
                    "SECTOR_FOLLOWUP",
                    "R03_SECTOR_NATIVE",
                    "R03_SECTOR_ROTATION",
                    "R03_SECTOR_OVERLAP",
                    "R03_SECTOR_LOO",
                    "FOCUS_FORMAL",
                    "D0",
                    "D1",
                    "FORWARD_PATH",
                    "COHORT_STATS",
                    "FEP_BASELINE",
                    "OBSERVATION_SEGMENTS",
                }
                else 5
            )
            count = module_counts.get(module, default_output_count or fallback)
        if count == 0:
            module_output_sessions[module] = ()
            continue
        module_output_sessions[module] = _module_output_sessions(
            module=module,
            requested_count=count,
            sessions=eligible,
            target=target,
            operation=operation,
            raw_coverage=raw_coverage,
        )

    sector_followup_due: dict[str, dict[int, date]] = {}
    if "SECTOR_FOLLOWUP" in selected_research_modules:
        official_through_cutoff = tuple(
            session for session in all_sessions if session <= cutoff
        )
        for t0 in module_output_sessions.get("SECTOR_FOLLOWUP", ()):
            index = official_through_cutoff.index(t0)
            due = {}
            for horizon in (1, 3, 5):
                endpoint_index = index + horizon
                if endpoint_index < len(official_through_cutoff):
                    due[horizon] = official_through_cutoff[endpoint_index]
            sector_followup_due[t0.isoformat()] = due

    sector_transition_pairs: dict[str, str | None] = {}
    if "SECTOR_TRANSITION" in selected_research_modules:
        transition_targets = module_output_sessions.get("SECTOR_TRANSITION", ())
        if not transition_targets:
            transition_targets = module_output_sessions.get("SECTOR_PULSE", ())
        for target_day in transition_targets:
            previous = tuple(session for session in eligible if session < target_day)
            sector_transition_pairs[target_day.isoformat()] = (
                previous[-1].isoformat() if previous else None
            )

    d0_sessions = module_output_sessions.get("D0", ())
    d1_sessions = module_output_sessions.get("D1", ())
    if request["legacy_request"] and mode == "MANUAL_DAILY" and not retrying_daily:
        d0_sessions = daily_sessions if d0_count else ()
        d1_sessions = daily_sessions if d1_count else ()
        module_output_sessions["D0"] = d0_sessions
        module_output_sessions["D1"] = d1_sessions
    focus_sessions = (
        module_output_sessions.get("FOCUS_FORMAL", ())
        if not request["legacy_request"]
        else d0_sessions
    )
    profile_dates = (
        tuple(sorted(module_output_sessions.get("CORE_PROFILE", ())))
        if not request["legacy_request"]
        else tuple(
            sorted(
                set(d0_sessions)
                | set(d1_sessions)
                | ({target} if mode == "INITIALIZE" else set())
            )
        )
    )
    market_sessions = (
        tuple(sorted(module_output_sessions.get("MARKET_BREADTH", ())))
        if not request["legacy_request"]
        else raw_window
        if mode == "INITIALIZE"
        else daily_sessions
    )
    participation_sessions = (
        tuple(sorted(module_output_sessions.get("MARKET_PARTICIPATION", ())))
        if not request["legacy_request"]
        else profile_dates
    )

    if (
        mode == "MANUAL_DAILY"
        and request["profile_history_session_count"] is None
        and selected_modules
        & {"CORE_PROFILE", "MARKET_PARTICIPATION", "D0", "FOCUS_FORMAL"}
    ):
        # 手动历史模拟按最早实际画像输出日，从PG真实paired capture和本次待采日
        # 推导可用窗口；不强行要求开发阶段尚未取得的70日输入。
        profile_target_days = tuple(
            sorted(
                set(profile_dates)
                | set(d0_sessions)
                | set(focus_sessions)
                | set(participation_sessions)
            )
        )
        paired_capture_sessions = {
            date.fromisoformat(session_text)
            for session_text, versions in capture_versions.items()
            if {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}.issubset(versions)
        }
        if "BAOSTOCK_PRICES" in selected_source_modules:
            paired_capture_sessions.update(baostock_output_sessions)
        if profile_target_days:
            available_windows = [
                sum(session <= day for session in paired_capture_sessions)
                for day in profile_target_days
            ]
            minimum_available_window = min(available_windows)
            if minimum_available_window > 0:
                profile_history = min(profile_history, minimum_available_window)

    stored_d1_window = raw_coverage.get("current_d1_history_session_count")
    d1_window_ambiguous = raw_coverage.get("d1_history_window_ambiguous") is True
    current_d1_window = request["d1_history_session_count"]
    if current_d1_window is None:
        current_d1_window = stored_d1_window
    if current_d1_window is None and not d1_window_ambiguous:
        from stock_a.application.source_research_day import (
            D1_DEFAULT_HISTORY_SESSION_COUNT,
        )

        current_d1_window = D1_DEFAULT_HISTORY_SESSION_COUNT
    required_raw_sessions: set[date] = set(raw_window)
    profile_inputs: dict[date, tuple[date, ...]] = {}
    d0_inputs: dict[date, tuple[date, ...]] = {}
    d0_profile_inputs: dict[date, tuple[date, ...]] = {}
    market_inputs: dict[date, tuple[date, ...]] = {}
    d1_inputs: dict[date, tuple[date, ...]] = {}
    for day in profile_dates:
        try:
            profile_inputs[day] = recent_official_sessions(
                eligible, through=day, count=profile_history
            )
            required_raw_sessions.update(profile_inputs[day])
        except DataJobError:
            profile_inputs[day] = ()
    for day in market_sessions:
        try:
            market_inputs[day] = recent_official_sessions(
                eligible, through=day, count=5
            )
            required_raw_sessions.update(market_inputs[day])
        except DataJobError:
            market_inputs[day] = ()
    for day in d0_sessions:
        previous = tuple(session for session in eligible if session < day)
        if previous:
            d0_inputs[day] = (previous[-1], day)
            required_raw_sessions.update(d0_inputs[day])
            try:
                d0_profile_inputs[day] = recent_official_sessions(
                    eligible, through=day, count=profile_history
                )
                required_raw_sessions.update(d0_profile_inputs[day])
            except DataJobError:
                d0_profile_inputs[day] = ()
        else:
            d0_inputs[day] = ()
    if d1_sessions and current_d1_window is not None:
        for day in d1_sessions:
            try:
                d1_inputs[day] = recent_official_sessions(
                    eligible, through=day, count=max(2, current_d1_window)
                )
                required_raw_sessions.update(d1_inputs[day])
            except DataJobError:
                d1_inputs[day] = ()
    if "SECTOR_PULSE" in selected_research_modules:
        required_raw_sessions.update(module_output_sessions.get("SECTOR_PULSE", ()))
    if "FOCUS_FORMAL" in selected_research_modules:
        required_raw_sessions.update(module_output_sessions.get("FOCUS_FORMAL", ()))
    if "SECTOR_TRANSITION" in selected_research_modules:
        for target_text, previous_text in sector_transition_pairs.items():
            required_raw_sessions.add(date.fromisoformat(target_text))
            if previous_text is not None:
                required_raw_sessions.add(date.fromisoformat(previous_text))
    if "SECTOR_FOLLOWUP" in selected_research_modules:
        for t0_text, due_targets in sector_followup_due.items():
            required_raw_sessions.add(date.fromisoformat(t0_text))
            required_raw_sessions.update(due_targets.values())
    if "MARKET_PARTICIPATION" in selected_base_modules:
        required_raw_sessions.update(
            module_output_sessions.get("MARKET_PARTICIPATION", ())
        )
    # 派生模块仍需从PG已存Raw输入绑定唯一paired capture；仅显式选择来源层
    # 时才允许发起来源请求。Forward还需加入截止内实际到期的端点会话。
    for module in (
        "PRICE_LIMITS",
        "LIMIT_UP_LADDER",
        "CROSS_SECTION",
        "MARKET_INDICES_ENV",
        "FORWARD_PATH",
        "COHORT_STATS",
        "FEP_BASELINE",
        "OBSERVATION_SEGMENTS",
    ):
        required_raw_sessions.update(module_output_sessions.get(module, ()))
    # C03：单选 CROSS_SECTION 时自动纳入最多 24 个官方会话预热（覆盖 rps20_delta3 = target - 23）
    if "CROSS_SECTION" in selected_base_modules:
        official_through_cutoff = tuple(
            session for session in all_sessions if session <= cutoff
        )
        for cs_day in module_output_sessions.get("CROSS_SECTION", ()):
            if cs_day in official_through_cutoff:
                cs_idx = official_through_cutoff.index(cs_day)
                required_raw_sessions.update(
                    official_through_cutoff[max(0, cs_idx - 23) : cs_idx + 1]
                )
    if "FORWARD_PATH" in selected_research_modules:
        official_through_cutoff = tuple(
            session for session in all_sessions if session <= cutoff
        )
        for target_day in module_output_sessions.get("FORWARD_PATH", ()):
            target_index = official_through_cutoff.index(target_day)
            for horizon in (1, 3, 5, 10, 20):
                endpoint_index = target_index + horizon
                if endpoint_index < len(official_through_cutoff):
                    required_raw_sessions.add(official_through_cutoff[endpoint_index])
    r03_plan_mods = selected_research_modules & {
        "R03_SECTOR_NATIVE",
        "R03_SECTOR_ROTATION",
        "R03_SECTOR_OVERLAP",
        "R03_SECTOR_LOO",
    }
    if r03_plan_mods:
        official_through_cutoff = tuple(
            session for session in all_sessions if session <= cutoff
        )
        for r03_mod in r03_plan_mods:
            for target_day in module_output_sessions.get(r03_mod, ()):
                required_raw_sessions.add(target_day)
                if target_day in official_through_cutoff:
                    t_idx = official_through_cutoff.index(target_day)
                    if t_idx >= 1:
                        required_raw_sessions.add(official_through_cutoff[t_idx - 1])
                    if t_idx >= 3:
                        required_raw_sessions.add(official_through_cutoff[t_idx - 3])
    raw_window = tuple(sorted(required_raw_sessions))
    if operation == "CLEAR_DERIVED":
        raw_window = ()

    price_coordinates = ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
    price_capture_inputs: list[dict[str, Any]] = []
    raw_window_sessions = {session.isoformat() for session in raw_window}
    for session_text, by_coordinate in sorted(capture_versions.items()):
        if session_text not in raw_window_sessions:
            continue
        for coordinate in price_coordinates:
            version = by_coordinate.get(coordinate)
            if version is not None:
                price_capture_inputs.append(
                    {
                        "session_date": session_text,
                        "source_capture_id": version["source_capture_id"],
                        "price_coordinate": coordinate,
                    }
                )
    stored_price_sessions = {
        session_text
        for session_text, versions in capture_versions.items()
        if all(coordinate in versions for coordinate in price_coordinates)
        and len(
            {
                int(versions[coordinate]["source_capture_id"])
                for coordinate in price_coordinates
            }
        )
        == 1
    }
    missing_price_inputs: list[dict[str, str]] = []
    for day in raw_window:
        session_text = day.isoformat()
        by_coordinate = capture_versions.get(session_text, {})
        missing_coordinates = [
            coordinate
            for coordinate in price_coordinates
            if coordinate not in by_coordinate
        ]
        if missing_coordinates:
            missing_price_inputs.extend(
                {
                    "session_date": session_text,
                    "price_coordinate": coordinate,
                }
                for coordinate in missing_coordinates
            )
        elif (
            len(
                {
                    int(by_coordinate[coordinate]["source_capture_id"])
                    for coordinate in price_coordinates
                }
            )
            != 1
        ):
            missing_price_inputs.extend(
                {"session_date": session_text, "price_coordinate": coordinate}
                for coordinate in price_coordinates
            )
    selected_window_capture_ids = {
        int(item["source_capture_id"])
        for item in price_capture_inputs
        if item["session_date"] in raw_window_sessions
    }
    endpoint_capture_ids_by_session: dict[str, int] = {}
    for day in raw_window:
        day_text = day.isoformat()
        by_coord = capture_versions.get(day_text, {})
        if all(coordinate in by_coord for coordinate in price_coordinates):
            cap_ids = {
                int(by_coord[coordinate]["source_capture_id"])
                for coordinate in price_coordinates
            }
            if len(cap_ids) == 1:
                endpoint_capture_ids_by_session[day_text] = next(iter(cap_ids))
    allow_per_session_captures = (
        "BAOSTOCK_PRICES" not in selected_source_modules
        and bool(selected_modules)
        and selected_modules
        <= {
            "R03_SECTOR_NATIVE",
            "R03_SECTOR_ROTATION",
            "R03_SECTOR_OVERLAP",
            "R03_SECTOR_LOO",
            "MARKET_BREADTH",
        }
    )
    price_capture_window_status = (
        "NO_CAPTURE_YET"
        if not selected_window_capture_ids
        else "PER_SESSION_PAIRED_CAPTURES"
        if (
            len(selected_window_capture_ids) > 1
            and allow_per_session_captures
            and not missing_price_inputs
        )
        else "MIXED_SOURCE_CAPTURE_IDS"
        if len(selected_window_capture_ids) > 1
        else "PAIRED_CAPTURE_ID_AVAILABLE"
        if not missing_price_inputs
        else "PAIRED_CAPTURE_INCOMPLETE"
    )
    if price_capture_window_status == "MIXED_SOURCE_CAPTURE_IDS":
        stored_price_sessions = set()
        missing_price_inputs = [
            {"session_date": day.isoformat(), "price_coordinate": coordinate}
            for day in raw_window
            for coordinate in price_coordinates
        ]
    missing_raw = sorted({item["session_date"] for item in missing_price_inputs})
    source_will_fill_prices = "BAOSTOCK_PRICES" in selected_source_modules
    fillable_price_sessions = {day.isoformat() for day in baostock_output_sessions}
    raw_input_unavailable = {
        session
        for session in missing_raw
        if not source_will_fill_prices or session not in fillable_price_sessions
    }
    d0_insufficient = [
        day.isoformat()
        for day in d0_sessions
        if not d0_inputs.get(day)
        or (
            not source_will_fill_prices
            and any(
                session.isoformat() in raw_input_unavailable
                for session in (
                    *d0_inputs.get(day, ()),
                    *d0_profile_inputs.get(day, ()),
                )
            )
        )
    ]
    d1_insufficient = [
        day.isoformat()
        for day in d1_sessions
        if current_d1_window is None
        or len(d1_inputs.get(day, ())) < max(2, current_d1_window)
        or (
            not source_will_fill_prices
            and any(
                item.isoformat() in raw_input_unavailable
                for item in d1_inputs.get(day, ())
            )
        )
    ]
    profile_window_by_date = {
        day: len(profile_inputs.get(day, ())) for day in profile_dates
    }
    profile_insufficient = [
        day.isoformat()
        for day in profile_dates
        if profile_window_by_date.get(day, 0)
        < min(CORE_MAX_ACTUAL_LOOKBACK, profile_history)
        or (
            not source_will_fill_prices
            and any(
                item.isoformat() in raw_input_unavailable
                for item in profile_inputs.get(day, ())
            )
        )
    ]
    completion_units = raw_coverage.get("completed_units_by_module", {})
    if not isinstance(completion_units, Mapping):
        completion_units = {}

    def completed_for_selected_raw(
        module: str,
        session: date,
        *,
        profile_window: int | None = None,
        d1_window: int | None = None,
        expected_calendar_revision: str | None = None,
    ) -> bool:
        versions = capture_versions.get(session.isoformat())
        if not isinstance(versions, Mapping) or any(
            coordinate not in versions for coordinate in price_coordinates
        ):
            return False
        expected_capture_ids = {
            coordinate: int(versions[coordinate]["source_capture_id"])
            for coordinate in price_coordinates
        }
        if len(set(expected_capture_ids.values())) != 1:
            return False

        def matches_price_key(item: Mapping[str, Any]) -> bool:
            actual = item.get("source_capture_ids_by_coordinate")
            return (
                item.get("target_session") == session.isoformat()
                and isinstance(actual, Mapping)
                and dict(actual) == expected_capture_ids
            )

        if profile_window is not None and profile_window < 1:
            return False
        candidates = completion_units.get(module, ())
        if not isinstance(candidates, Sequence):
            return False
        expected_profile_revision = None
        revision_calendar = expected_calendar_revision
        if profile_window is not None:
            from stock_a.application.pg_price_profiles import (
                SOURCE_SYMBOL_PROFILE_SCOPE,
                _profile_revision,
            )
            from stock_a.application.research_window import resolve_pg_history_window

            _target, _window, revision_calendar = resolve_pg_history_window(
                session.isoformat(), profile_window
            )
            expected_profile_revision = _profile_revision(
                scope=SOURCE_SYMBOL_PROFILE_SCOPE,
                calendar_id=calendar_id,
                calendar_revision=revision_calendar,
                history_session_count=profile_window,
            )
        if module == "CORE_PROFILE":
            for profile in raw_coverage.get("profile_completed_versions", ()):
                if (
                    matches_price_key(profile)
                    and profile.get("history_session_count") == profile_window
                    and profile.get("profile_revision") == expected_profile_revision
                ):
                    return True
        if module == "MARKET_BREADTH":
            for breadth in raw_coverage.get("market_breadth_completed_versions", ()):
                if (
                    matches_price_key(breadth)
                    and breadth.get("contract_version")
                    == "MKT-02_BAOSTOCK_QFQ_POSITIVE_RETURN_STATE_DELTA_R3"
                    and breadth.get("calendar_revision") == expected_calendar_revision
                ):
                    return True
        for candidate in candidates:
            if not matches_price_key(candidate):
                continue
            tables = candidate.get("business_tables", ())
            if not isinstance(tables, Sequence):
                continue
            if profile_window is not None and any(
                row.get("profile_history_session_count") != profile_window
                or row.get("profile_revision") != expected_profile_revision
                for row in tables
            ):
                continue
            if d1_window is not None and any(
                row.get("d1_history_session_count") != d1_window for row in tables
            ):
                continue
            if revision_calendar is not None and any(
                row.get("calendar_revision") != revision_calendar for row in tables
            ):
                continue
            return True
        return False

    planned_profile_ready = {
        item.isoformat()
        for item in profile_dates
        if item.isoformat() not in profile_insufficient
    }
    stored_profile_ready = {
        day.isoformat()
        for day in set(d0_sessions) | set(focus_sessions) | set(participation_sessions)
        if completed_for_selected_raw(
            "CORE_PROFILE",
            day,
            profile_window=len(profile_inputs.get(day, ()))
            or len(d0_profile_inputs.get(day, ()))
            or profile_history,
            expected_calendar_revision=calendar_revision,
        )
    }
    profile_ready_sessions = planned_profile_ready | stored_profile_ready
    sector_pulse_insufficient = {
        day.isoformat()
        for day in module_output_sessions.get("SECTOR_PULSE", ())
        if not source_will_fill_prices and day.isoformat() in raw_input_unavailable
    }
    d0_ready_sessions = {
        day.isoformat() for day in d0_sessions if day.isoformat() not in d0_insufficient
    } | {
        day.isoformat()
        for day in focus_sessions
        if completed_for_selected_raw(
            "D0", day, expected_calendar_revision=calendar_revision
        )
    }
    planned_breadth_ready = {
        day.isoformat()
        for day in market_sessions
        if len(market_inputs.get(day, ())) == 5
        and (
            source_will_fill_prices
            or not any(
                item.isoformat() in raw_input_unavailable
                for item in market_inputs.get(day, ())
            )
        )
        and operation != "CLEAR_DERIVED"
    }
    stored_breadth_ready = {
        day.isoformat()
        for day in participation_sessions
        if completed_for_selected_raw(
            "MARKET_BREADTH",
            day,
            expected_calendar_revision=calendar_revision,
        )
    }
    stored_breadth_ready.update(
        str(breadth["target_session"])
        for breadth in raw_coverage.get("market_breadth_completed_versions", ())
        if isinstance(breadth, Mapping)
        and breadth.get("calendar_revision") == calendar_revision
        and isinstance(
            capture_versions.get(str(breadth.get("target_session"))), Mapping
        )
        and all(
            coordinate in capture_versions[str(breadth.get("target_session"))]
            for coordinate in price_coordinates
        )
        and isinstance(breadth.get("source_capture_ids_by_coordinate"), Mapping)
        and dict(breadth.get("source_capture_ids_by_coordinate", {}))
        == {
            coordinate: int(
                capture_versions[str(breadth.get("target_session"))][coordinate][
                    "source_capture_id"
                ]
            )
            for coordinate in price_coordinates
            if coordinate in capture_versions[str(breadth.get("target_session"))]
        }
    )
    breadth_ready_sessions = planned_breadth_ready | stored_breadth_ready
    if "D0" in selected_research_modules:
        profile_insufficient.extend(
            day.isoformat()
            for day in d0_sessions
            if day.isoformat() not in profile_ready_sessions
        )
    if "FOCUS_FORMAL" in selected_research_modules:
        profile_insufficient.extend(
            day.isoformat()
            for day in focus_sessions
            if day.isoformat() not in profile_ready_sessions
        )
    profile_insufficient = sorted(set(profile_insufficient))
    sector_transition_insufficient = {
        target_text
        for target_text, previous_text in sector_transition_pairs.items()
        if previous_text is None
        or (
            not source_will_fill_prices
            and any(
                session.isoformat() in raw_input_unavailable
                for session in (
                    date.fromisoformat(target_text),
                    date.fromisoformat(previous_text),
                )
            )
        )
    }
    planned_seed_sessions = {
        day
        for day in d0_sessions
        if day.isoformat() not in d0_insufficient
        and day.isoformat() not in profile_insufficient
    }

    def seed_available(session: date) -> bool:
        if session in planned_seed_sessions:
            return True
        profile_window = (
            len(d0_profile_inputs.get(session, ()))
            or len(profile_inputs.get(session, ()))
            or None
        )
        return completed_for_selected_raw(
            "SEED",
            session,
            profile_window=profile_window,
            expected_calendar_revision=calendar_revision,
        )

    forward_path_sessions = module_output_sessions.get("FORWARD_PATH", ())
    if _forward_path_seed_availability_out is not None and forward_path_sessions:
        sessions_through_cutoff = tuple(
            session for session in all_sessions if session <= cutoff
        )
        session_indexes = {
            session: index for index, session in enumerate(sessions_through_cutoff)
        }
        for target_session in forward_path_sessions:
            index = session_indexes.get(target_session)
            if index is None or index + 1 >= len(sessions_through_cutoff):
                continue
            t1_session = sessions_through_cutoff[index + 1]
            _forward_path_seed_availability_out[t1_session.isoformat()] = (
                seed_available(t1_session)
            )

    sector_pulse_insufficient = sorted(
        {
            *sector_pulse_insufficient,
            *(
                day.isoformat()
                for day in module_output_sessions.get("SECTOR_PULSE", ())
                if not seed_available(day)
            ),
        }
    )
    sector_transition_insufficient.update(
        target_text
        for target_text, previous_text in sector_transition_pairs.items()
        if previous_text is None
        or not seed_available(date.fromisoformat(target_text))
        or not seed_available(date.fromisoformat(previous_text))
    )
    focus_insufficient = {
        day.isoformat()
        for day in focus_sessions
        if day.isoformat() in d0_insufficient
        or day.isoformat() in profile_insufficient
        or day.isoformat() not in d0_ready_sessions
    }
    market_insufficient = {
        day.isoformat()
        for day in market_sessions
        if len(market_inputs.get(day, ())) < 5
        or (
            not source_will_fill_prices
            and any(
                item.isoformat() in raw_input_unavailable
                for item in market_inputs.get(day, ())
            )
        )
    }
    market_participation_insufficient = {
        day.isoformat()
        for day in participation_sessions
        if day.isoformat() in profile_insufficient
        or day.isoformat() in market_insufficient
        or day.isoformat() not in profile_ready_sessions
        or day.isoformat() not in breadth_ready_sessions
        or (not source_will_fill_prices and day.isoformat() in raw_input_unavailable)
    }
    sector_followup_insufficient = {
        t0
        for t0, due in sector_followup_due.items()
        if not source_will_fill_prices
        and any(
            session.isoformat() in raw_input_unavailable
            for session in (date.fromisoformat(t0), *due.values())
        )
    }
    units: list[dict[str, Any]] = []
    if "BAOSTOCK_PRICES" in selected_source_modules:
        for day in baostock_output_sessions:
            versions = capture_versions.get(day.isoformat(), {})
            paired_capture_id = None
            if all(coordinate in versions for coordinate in price_coordinates):
                capture_ids = {
                    int(versions[coordinate]["source_capture_id"])
                    for coordinate in price_coordinates
                }
                if len(capture_ids) == 1:
                    paired_capture_id = next(iter(capture_ids))
            waiting_for_reader = price_reader_status != "AVAILABLE"
            _unit(
                units,
                stage="SOURCE_BAOSTOCK_PRICES",
                target_session=day,
                business_table="stock.source_raw_daily",
                status="NOT_COMPUTED" if waiting_for_reader else "NOT_STARTED",
                error_code=(
                    (
                        "SOURCE_CAPTURE_READER_NOT_CONNECTED"
                        if price_reader_status.startswith("WAITING")
                        else "PRICE_COORDINATE_COVERAGE_UNAVAILABLE"
                    )
                    if waiting_for_reader
                    else None
                ),
                error_message=(
                    (
                        "尚无BaoStock价格capture只读覆盖读取器；等待接通后再确认可复用输入。"
                        if price_reader_status.startswith("WAITING")
                        else "BaoStock价格capture只读覆盖读取无效或未通过；保留未计算状态。"
                    )
                    if waiting_for_reader
                    else None
                ),
                source_capture_id=paired_capture_id,
                price_coordinates=price_coordinates,
            )
    if "TDX_SECTOR_MEMBERS" in selected_source_modules:
        for table in _STAGE_TABLES["SOURCE_SECTOR_MEMBERS"]:
            _unit(
                units,
                stage="SOURCE_SECTOR_MEMBERS",
                target_session=target,
                business_table=table,
            )
    if "BAOSTOCK_UNIVERSE" in selected_source_modules:
        for table in _STAGE_TABLES["SOURCE_BAOSTOCK_UNIVERSE"]:
            _unit(
                units,
                stage="SOURCE_BAOSTOCK_UNIVERSE",
                target_session=target,
                business_table=table,
            )
    if "BAOSTOCK_MARKET_INDICES" in selected_source_modules:
        for table in _STAGE_TABLES["SOURCE_BAOSTOCK_MARKET_INDICES"]:
            _unit(
                units,
                stage="SOURCE_BAOSTOCK_MARKET_INDICES",
                target_session=target,
                business_table=table,
            )
    for day in module_output_sessions.get("PRICE_LIMITS", ()):
        _unit(
            units,
            stage="PRICE_LIMITS",
            target_session=date.fromisoformat(day) if isinstance(day, str) else day,
            business_table="stock.source_price_limit_day_facts",
            status="NOT_STARTED",
        )
    for day in module_output_sessions.get("LIMIT_UP_LADDER", ()):
        _unit(
            units,
            stage="LIMIT_UP_LADDER",
            target_session=date.fromisoformat(day) if isinstance(day, str) else day,
            business_table="stock.source_limit_up_streak_day_facts",
            status="NOT_STARTED",
        )
    for day in module_output_sessions.get("CROSS_SECTION", ()):
        day_obj = date.fromisoformat(day) if isinstance(day, str) else day
        for table in _STAGE_TABLES["CROSS_SECTION"]:
            _unit(
                units,
                stage="CROSS_SECTION",
                target_session=day_obj,
                business_table=table,
                status="NOT_STARTED",
            )
    for day in module_output_sessions.get("MARKET_INDICES_ENV", ()):
        day_obj = date.fromisoformat(day) if isinstance(day, str) else day
        for table in _STAGE_TABLES["MARKET_INDICES_ENV"]:
            _unit(
                units,
                stage="MARKET_INDICES_ENV",
                target_session=day_obj,
                business_table=table,
                status="NOT_STARTED",
            )
    for day in market_sessions:
        status = (
            "NOT_COMPUTED" if day.isoformat() in market_insufficient else "NOT_STARTED"
        )
        for table in _STAGE_TABLES["MARKET_BREADTH"]:
            _unit(
                units,
                stage="MARKET_BREADTH",
                target_session=day,
                business_table=table,
                status=status,
                error_code="MARKET_BREADTH_HISTORY_INSUFFICIENT"
                if status == "NOT_COMPUTED"
                else None,
                error_message="市场宽度要求的前序官方Raw会话不在本任务范围内。"
                if status == "NOT_COMPUTED"
                else None,
            )
    for day in profile_dates:
        status = (
            "NOT_COMPUTED" if day.isoformat() in profile_insufficient else "NOT_STARTED"
        )
        _unit(
            units,
            stage="CORE_PROFILE",
            target_session=day,
            business_table="stock.source_symbol_price_profiles",
            status=status,
            error_code="PROFILE_HISTORY_INSUFFICIENT"
            if status == "NOT_COMPUTED"
            else None,
            error_message="请求的历史窗口不在本任务Raw范围内，保留未计算状态。"
            if status == "NOT_COMPUTED"
            else None,
        )
    for day in participation_sessions:
        status = (
            "NOT_COMPUTED"
            if day.isoformat() in market_participation_insufficient
            else "NOT_STARTED"
        )
        for table in _STAGE_TABLES["MARKET_PARTICIPATION"]:
            _unit(
                units,
                stage="MARKET_PARTICIPATION",
                target_session=day,
                business_table=table,
                status=status,
                error_code="MARKET_PARTICIPATION_INPUT_UNAVAILABLE"
                if status == "NOT_COMPUTED"
                else None,
                error_message="市场宽度或该日Core/Profile窗口未闭合。"
                if status == "NOT_COMPUTED"
                else None,
            )
    for day in d0_sessions:
        insufficient_output = day.isoformat() in d0_insufficient
        insufficient_profile = day.isoformat() in profile_insufficient
        status = (
            "NOT_COMPUTED"
            if insufficient_output or insufficient_profile
            else "NOT_STARTED"
        )
        code = (
            "D0_OUTPUT_HISTORY_INSUFFICIENT"
            if insufficient_output
            else "PROFILE_HISTORY_INSUFFICIENT"
            if insufficient_profile
            else None
        )
        message = (
            "输出目标日或其前一官方会话不在本任务Raw范围内。"
            if insufficient_output
            else "静态研究所需Core/Profile历史窗口不可用。"
            if insufficient_profile
            else None
        )
        for table in _STAGE_TABLES["STATIC_RESEARCH"]:
            _unit(
                units,
                stage="STATIC_RESEARCH",
                target_session=day,
                business_table=table,
                status=status,
                error_code=code,
                error_message=message,
            )
    for day in d0_sessions:
        status = "NOT_COMPUTED" if day.isoformat() in d0_insufficient else "NOT_STARTED"
        for table in (
            "stock.source_d0_launch_day_facts",
            "stock.source_research_enrollments",
        ):
            _unit(
                units,
                stage="D0_D1",
                target_session=day,
                business_table=table,
                status=status,
                error_code="D0_OUTPUT_HISTORY_INSUFFICIENT"
                if status == "NOT_COMPUTED"
                else None,
                error_message="输出目标日或其前一官方会话不在本任务Raw范围内。"
                if status == "NOT_COMPUTED"
                else None,
            )
    for day in focus_sessions:
        formal_status = (
            "NOT_COMPUTED" if day.isoformat() in focus_insufficient else "NOT_STARTED"
        )
        rules_status = (
            "NOT_COMPUTED"
            if day.isoformat() in focus_insufficient
            or day.isoformat() in d1_insufficient
            else "NOT_STARTED"
        )
        t0_status = (
            "NOT_COMPUTED"
            if day.isoformat() in focus_insufficient
            or sector_pair.get("status") != "AVAILABLE"
            else "NOT_STARTED"
        )
        _unit(
            units,
            stage="FOCUS_FORMAL",
            target_session=day,
            business_table="stock.source_d0_formal_admission_day_facts",
            status=formal_status,
            error_code="FORMAL_ADMISSION_INPUT_INSUFFICIENT"
            if formal_status == "NOT_COMPUTED"
            else None,
            error_message="D0或Core/Profile前置事实未闭合，正式Focus准入保留未计算。"
            if formal_status == "NOT_COMPUTED"
            else None,
        )
        _unit(
            units,
            stage="FOCUS_RESEARCH_CONTEXT",
            target_session=day,
            business_table="stock.source_rule_evaluations",
            status=rules_status,
            error_code=(
                "FROZEN_RULE_INPUTS_UNAVAILABLE"
                if rules_status == "NOT_COMPUTED"
                else None
            ),
            error_message=(
                "冻结规则所需的D0/Seed/Profile或D1 QFQ输入不可用；不补造规则值。"
                if rules_status == "NOT_COMPUTED"
                else None
            ),
        )
        _unit(
            units,
            stage="FOCUS_RESEARCH_CONTEXT",
            target_session=day,
            business_table="stock.source_t0_early_observation_facts",
            status=t0_status,
            error_code=(
                "T0_EARLY_INPUTS_UNAVAILABLE" if t0_status == "NOT_COMPUTED" else None
            ),
            error_message=(
                "T0观察所需的D0/Profile输入或行业/GN快照对不可用；保留未计算。"
                if t0_status == "NOT_COMPUTED"
                else None
            ),
        )
    for day in d1_sessions:
        status = "NOT_COMPUTED" if day.isoformat() in d1_insufficient else "NOT_STARTED"
        for table in (
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
        ):
            _unit(
                units,
                stage="D0_D1",
                target_session=day,
                business_table=table,
                status=status,
                error_code="D1_HISTORY_WINDOW_UNAVAILABLE"
                if status == "NOT_COMPUTED"
                else None,
                error_message="现行D1算法窗口不在本任务Raw范围内或窗口配置不唯一。"
                if status == "NOT_COMPUTED"
                else None,
            )
    if d0_count == 0 and request["legacy_request"]:
        for stage, table in (
            ("STATIC_RESEARCH", "stock.source_d0_scenario_day_facts"),
            ("STATIC_RESEARCH", "stock.source_pool_seed_day_facts"),
            ("D0_D1", "stock.source_d0_launch_day_facts"),
            ("D0_D1", "stock.source_research_enrollments"),
            ("FOCUS_RESEARCH_CONTEXT", "stock.source_rule_evaluations"),
        ):
            _unit(
                units,
                stage=stage,
                target_session=target,
                business_table=table,
                status="SKIPPED_BY_USER",
                error_code="SKIPPED_BY_USER",
                error_message="用户关闭D0输出。",
            )
        _unit(
            units,
            stage="FOCUS_FORMAL",
            target_session=target,
            business_table="stock.source_d0_formal_admission_day_facts",
            status="SKIPPED_BY_USER",
            error_code="SKIPPED_BY_USER",
            error_message="用户关闭D0输出，正式Focus准入随之跳过。",
        )
        _unit(
            units,
            stage="FOCUS_RESEARCH_CONTEXT",
            target_session=target,
            business_table="stock.source_t0_early_observation_facts",
            status="SKIPPED_BY_USER",
            error_code="SKIPPED_BY_USER",
            error_message="用户关闭D0输出，T0早期观察随之跳过。",
        )
    if d1_count == 0 and request["legacy_request"]:
        for table in (
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
        ):
            _unit(
                units,
                stage="D0_D1",
                target_session=target,
                business_table=table,
                status="SKIPPED_BY_USER",
                error_code="SKIPPED_BY_USER",
                error_message="用户关闭D1输出。",
            )

    if operation in _CLEANUP_OPERATIONS:
        for unit in units:
            unit["status"] = "NOT_STARTED"
            unit["error_code"] = None
            unit["error_message"] = None

    blockers: list[str] = []
    source_io_required = bool(selected_source_modules & _ALL_SOURCE_MODULES)
    price_reader_required = (
        bool(raw_window) or "BAOSTOCK_PRICES" in selected_source_modules
    ) and not reset_preflight_required
    if not execution_authorized and source_io_required:
        blockers.append("DATA_EXECUTION_NOT_AUTHORIZED")
    if d1_sessions and current_d1_window is None:
        blockers.append(
            "D1_HISTORY_WINDOW_AMBIGUOUS"
            if d1_window_ambiguous
            else "D1_HISTORY_WINDOW_NOT_ESTABLISHED"
        )
    if any(item["status"] == "NOT_COMPUTED" for item in units):
        blockers.append("HISTORY_INSUFFICIENT_FOR_SOME_OUTPUT_DATES")
    if price_reader_required and price_reader_status != "AVAILABLE":
        blockers.append("SOURCE_CAPTURE_READER_NOT_CONNECTED")

    executable = (execution_authorized or not source_io_required) and (
        not price_reader_required or price_reader_status == "AVAILABLE"
    )
    if not executable:
        status = (
            "WAITING_FOR_SOURCE_READER"
            if price_reader_required and price_reader_status != "AVAILABLE"
            else "BLOCKED_NOT_AUTHORIZED"
        )
    elif any(item["status"] == "NOT_COMPUTED" for item in units):
        status = "READY_WITH_INCOMPLETE_UNITS"
    elif missing_raw:
        status = "READY_SOURCE_CHECK_REQUIRED"
    else:
        status = "READY"

    return {
        "mode": mode,
        "status": status,
        "executable": executable,
        "blocked_reasons": blockers,
        "target_session": target.isoformat(),
        "research_cutoff": cutoff.isoformat(),
        "history_session_count": raw_request_count,
        "profile_history_session_count": profile_history,
        "d0_output_session_count": d0_count,
        "d1_output_session_count": d1_count,
        "d1_calculation_session_count": current_d1_window,
        "calendar_id": calendar_id,
        "calendar_revision": calendar_revision,
        "raw_history_sessions": [day.isoformat() for day in raw_window],
        "baostock_price_output_sessions": [
            day.isoformat() for day in baostock_output_sessions
        ],
        "price_coordinates": list(price_coordinates),
        "price_capture_inputs": price_capture_inputs,
        "price_capture_reader_status": (
            "RESET_PREFLIGHT_REQUIRED"
            if reset_preflight_required
            else price_reader_status
        ),
        "missing_price_inputs": missing_price_inputs,
        "raw_history_session_count_available": len(raw_window),
        "raw_history_sessions_in_pg": len(raw_window) - len(missing_raw),
        "missing_raw_sessions": missing_raw,
        "source_capture_preview": {
            "status": (
                "RESET_PREFLIGHT_REQUIRED"
                if reset_preflight_required
                else price_reader_status
            ),
            "versions": price_capture_inputs,
            "window_status": price_capture_window_status,
            "source_capture_id": (
                next(iter(selected_window_capture_ids))
                if len(selected_window_capture_ids) == 1
                else None
            ),
            "identity_fields": ["source_capture_id", "price_coordinate"],
        },
        "d0_output_sessions": [day.isoformat() for day in d0_sessions],
        "d1_output_sessions": [day.isoformat() for day in d1_sessions],
        "profile_output_sessions": [day.isoformat() for day in profile_dates],
        "profile_history_session_counts_by_session": {
            day.isoformat(): len(
                profile_inputs.get(day, ()) or d0_profile_inputs.get(day, ())
            )
            for day in sorted(set(profile_dates) | set(d0_sessions))
        },
        "d0_profile_history_session_counts_by_session": {
            day.isoformat(): len(d0_profile_inputs.get(day, ())) for day in d0_sessions
        },
        "d1_history_session_counts_by_session": {
            day.isoformat(): len(d1_inputs.get(day, ())) for day in d1_sessions
        },
        "market_participation_sessions": [
            day.isoformat() for day in participation_sessions
        ],
        "market_breadth_sessions": [day.isoformat() for day in market_sessions],
        "sector_pulse_sessions": [
            day.isoformat() for day in module_output_sessions.get("SECTOR_PULSE", ())
        ],
        "sector_transition_pairs": sector_transition_pairs,
        "sector_transition_sessions": sorted(sector_transition_pairs),
        "sector_transition_insufficient_sessions": sorted(
            sector_transition_insufficient
        ),
        "sector_followup_sessions": [
            day.isoformat() for day in module_output_sessions.get("SECTOR_FOLLOWUP", ())
        ],
        "sector_followup_due_targets": {
            t0: {
                str(horizon): endpoint.isoformat() for horizon, endpoint in due.items()
            }
            for t0, due in sector_followup_due.items()
        },
        "sector_pulse_insufficient_sessions": sorted(sector_pulse_insufficient),
        "sector_followup_insufficient_sessions": sorted(sector_followup_insufficient),
        "ordered_sessions": [day.isoformat() for day in eligible],
        "allow_revision": operation in {"RECOMPUTE", "CLEAR_AND_REGENERATE"},
        "module_output_sessions": {
            **{
                module: [day.isoformat() for day in sessions]
                for module, sessions in module_output_sessions.items()
            },
            **(
                {
                    "BAOSTOCK_PRICES": [
                        day.isoformat() for day in baostock_output_sessions
                    ]
                }
                if "BAOSTOCK_PRICES" in selected_source_modules
                else {}
            ),
            **(
                {"TDX_SECTOR_MEMBERS": [target.isoformat()]}
                if "TDX_SECTOR_MEMBERS" in selected_source_modules
                else {}
            ),
        },
        "market_breadth_insufficient_history_sessions": sorted(market_insufficient),
        "market_participation_insufficient_sessions": sorted(
            market_participation_insufficient
        ),
        "unplanned_producers": [],
        "d0_insufficient_previous_sessions": d0_insufficient,
        "d1_insufficient_history_sessions": d1_insufficient,
        "profile_insufficient_history_sessions": profile_insufficient,
        "source_coverage": {
            "status": "RESET_SOURCE_PREFLIGHT_REQUIRED"
            if reset_preflight_required
            else "PG_PRICE_CAPTURE_REUSE_AVAILABLE"
            if price_capture_inputs and not missing_price_inputs
            else "WAITING_FOR_SOURCE_CAPTURE_READER"
            if price_reader_required and price_reader_status != "AVAILABLE"
            else "NOT_EXECUTED_DATA_FREEZE"
            if source_io_required and not execution_authorized
            else "NOT_CHECKED_NO_NETWORK"
            if source_io_required
            else "PG_PRICE_INPUT_UNAVAILABLE"
            if missing_price_inputs
            else "NO_SOURCE_IO_REQUIRED",
            "verified_external_sessions": None,
            "source_request_performed": False,
            "pg_closed_raw_session_count": raw_coverage.get("stored_session_count", 0),
            "pg_first_session": raw_coverage.get("first_stored_session"),
            "pg_last_session": raw_coverage.get("last_stored_session"),
            "price_coordinate_reader_status": (
                "RESET_PREFLIGHT_REQUIRED"
                if reset_preflight_required
                else price_reader_status
            ),
            "price_capture_version_count": len(price_capture_inputs),
        },
        "stage_units": units,
        "planned_unit_count": len(units),
        "endpoint_capture_ids_by_session": endpoint_capture_ids_by_session,
        "sector_snapshot_ids_by_session": (
            {
                str(k): {
                    "INDUSTRY": int(v["INDUSTRY"]),
                    "CONCEPT": int(v["CONCEPT"]),
                }
                for k, v in request["sector_snapshot_ids_by_session"].items()
                if isinstance(v, Mapping)
                and type(v.get("INDUSTRY")) is int
                and type(v.get("CONCEPT")) is int
            }
            if request.get("sector_snapshot_ids_by_session")
            else dict(sector_pair.get("snapshot_ids_by_session") or {})
        ),
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def _blocked_plan(
    mode: str,
    cutoff: date,
    raw_coverage: Mapping[str, Any],
    status: str,
    reasons: list[str],
    **extra: Any,
) -> dict[str, Any]:
    price_versions, price_reader_status = _price_coordinate_versions(
        raw_coverage, cutoff=cutoff
    )
    price_capture_inputs = [
        {
            "session_date": session_text,
            "source_capture_id": version["source_capture_id"],
            "price_coordinate": coordinate,
        }
        for session_text, versions in sorted(price_versions.items())
        for coordinate, version in sorted(versions.items())
    ]
    return {
        "mode": mode,
        "status": status,
        "executable": False,
        "blocked_reasons": reasons,
        "research_cutoff": cutoff.isoformat(),
        "source_coverage": {
            "status": "WAITING_FOR_SOURCE_CAPTURE_READER"
            if price_reader_status != "AVAILABLE"
            else "NOT_CHECKED_NO_NETWORK",
            "verified_external_sessions": None,
            "source_request_performed": False,
            "pg_closed_raw_session_count": raw_coverage.get("stored_session_count", 0),
            "pg_first_session": raw_coverage.get("first_stored_session"),
            "pg_last_session": raw_coverage.get("last_stored_session"),
            "price_coordinate_reader_status": price_reader_status,
            "price_capture_version_count": len(price_capture_inputs),
        },
        "price_coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
        "price_capture_inputs": price_capture_inputs,
        "source_capture_preview": {
            "status": price_reader_status,
            "versions": price_capture_inputs,
            "identity_fields": ["source_capture_id", "price_coordinate"],
        },
        "stage_units": [],
        "planned_unit_count": 0,
        "database_write_attempted": False,
        "source_io_attempted": False,
        **extra,
    }


def _job_request_key(plan: Mapping[str, Any]) -> str:
    identity = {
        "mode": plan["mode"],
        "target_session": plan["target_session"],
        "history_session_count": plan["history_session_count"],
        "profile_history_session_count": plan.get("profile_history_session_count"),
        "d0_output_session_count": plan["d0_output_session_count"],
        "d1_output_session_count": plan["d1_output_session_count"],
        "calendar_id": plan["calendar_id"],
        "calendar_revision": plan["calendar_revision"],
        "research_cutoff": plan["research_cutoff"],
        "selected_modules": plan.get("selected_modules", []),
        "module_output_session_counts": plan.get("module_output_session_counts", {}),
        "operation": plan.get("operation", "FILL_MISSING"),
        "initialization_mode": plan.get("initialization_mode", "REUSE_RAW"),
        "daily_mode": plan.get("daily_mode", "CUSTOM"),
        "reset_preview_fingerprint": plan.get("reset_preview_fingerprint"),
        "source_version_fingerprint": plan.get("source_version_fingerprint"),
        "sector_snapshot_ids": plan.get("sector_snapshot_ids"),
        "cleanup_preview_key": plan.get("cleanup_preview", {}).get("preview_key"),
    }
    canonical = json.dumps(
        identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _require_persisted_baostock_session_rows(
    actual_rows_by_session: Mapping[date, int],
    expected_rows_by_session: Mapping[date, int],
) -> None:
    """要求PG来源行闭合；缺失的规范事实不能由外部文件补造。"""
    if actual_rows_by_session != expected_rows_by_session:
        raise DataJobError(
            "SOURCE_BLOCKED",
            "PG已存BaoStock双坐标价格行与有效证券×会话总体不一致；不能从外部文件补造。",
            status=409,
        )


class DataJobCoordinator:
    """在单个预览服务进程中运行一项后台任务并持久化读回进度。"""

    def __init__(
        self,
        *,
        config: Mapping[str, Any],
        research_cutoff: date,
        execution_authorized: bool = False,
    ) -> None:
        self.config = config
        self.research_cutoff = research_cutoff
        self.execution_authorized = execution_authorized is True
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.startup_recovery = data_job_pg.recover_interrupted_jobs(config)

    def _coverage(self) -> dict[str, Any]:
        return data_job_pg.read_raw_coverage(self.config, cutoff=self.research_cutoff)

    def capabilities(self) -> dict[str, Any]:
        coverage = self._coverage()
        source_policy = self.config.get("source_policy", {})
        primary_policy = (
            source_policy.get("baostock_primary", {})
            if isinstance(source_policy, Mapping)
            else {}
        )
        primary_budget = (
            primary_policy.get("request_budget", {})
            if isinstance(primary_policy, Mapping)
            else {}
        )
        configured_history_limit = (
            primary_budget.get("max_history_session_count")
            if isinstance(primary_budget, Mapping)
            else None
        )
        history_session_limit = (
            configured_history_limit
            if type(configured_history_limit) is int
            and 1 <= configured_history_limit <= _MAX_PRICE_HISTORY_SESSIONS
            else None
        )
        latest = data_job_pg.read_recent_jobs(self.config, limit=1)
        from stock_a.application.source_sector_pulse import (
            resolve_source_sector_snapshot_pair,
        )

        sector_pair = resolve_source_sector_snapshot_pair(self.config)
        try:
            _calendar_id, _calendar_revision, all_sessions = _load_official_sessions(
                self.research_cutoff
            )
            target_sessions = [
                item.isoformat()
                for item in all_sessions
                if item <= self.research_cutoff
            ]
        except DataJobError:
            target_sessions = list(coverage.get("closed_sessions", ()))
        modules = []
        sector_source_can_be_planned = True
        for code, (layer, label, implemented, available) in _MODULES.items():
            reason = None
            if (
                code in {"SECTOR_PULSE", "SECTOR_TRANSITION"}
                and sector_pair.get("status") != "AVAILABLE"
            ):
                reason = (
                    "PG当前没有已配对快照；在本次任务中选择行业/GN成员来源后，"
                    "可先生成同次快照再计算。"
                    if sector_source_can_be_planned
                    else sector_pair.get("reason_code") or "行业与概念快照对不可用。"
                )
            cleanup_allowed = _MODULES[code][0] != "SOURCE"
            modules.append(
                {
                    "code": code,
                    "label": label,
                    "layer": layer,
                    "implemented": implemented,
                    "available": available,
                    "reason": reason,
                    "cleanup_allowed": cleanup_allowed,
                    "default_selected": (
                        code in _ALL_SOURCE_MODULES if layer == "SOURCE" else True
                    ),
                    "default_output_session_count": 5,
                }
            )
        return {
            "status": "AVAILABLE",
            "execution_authorized": self.execution_authorized,
            "execution_reason": None
            if self.execution_authorized
            else "BLOCKED_NOT_AUTHORIZED",
            "research_cutoff": self.research_cutoff.isoformat(),
            "raw_coverage": coverage,
            "history_session_count_required_for_initialize": True,
            "history_session_count_limits": {
                "minimum": 1,
                "maximum": history_session_limit,
            },
            "baostock_primary_policy": {
                key: primary_budget.get(key)
                for key in (
                    "max_login_batches",
                    "max_api_calls_per_batch",
                    "query_timeout_seconds",
                    "max_history_session_count",
                    "max_identity_total_seconds",
                    "max_retry_attempts",
                    "retry_backoff_seconds",
                )
            }
            if isinstance(primary_budget, Mapping)
            else {},
            "current_profile_history_session_count": coverage.get(
                "current_profile_history_session_count"
            ),
            "core_max_actual_lookback": CORE_MAX_ACTUAL_LOOKBACK,
            "modules": modules,
            "available_target_sessions": target_sessions,
            "sector_snapshot_pair": sector_pair,
            "latest_job": latest[0] if latest else None,
            "recent_jobs": data_job_pg.read_recent_jobs(self.config, limit=8),
            "source_request_performed": False,
            "database_write_attempted": False,
        }

    def plan(self, body: Any) -> dict[str, Any]:
        coverage = self._coverage()
        request = _request_fields(body)
        reset_preflight_required = (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        )
        sector_pair = {"status": "NOT_REQUESTED", "snapshot_ids": None}
        if {
            "SECTOR_PULSE",
            "SECTOR_TRANSITION",
            "SECTOR_FOLLOWUP",
            "R03_SECTOR_NATIVE",
            "R03_SECTOR_ROTATION",
            "R03_SECTOR_OVERLAP",
            "R03_SECTOR_LOO",
            "FOCUS_FORMAL",
        } & set(request["selected_modules"]):
            from stock_a.application.source_sector_pulse import (
                resolve_source_sector_snapshot_pair,
                resolve_source_sector_snapshot_pairs_by_session,
            )

            sector_pair = resolve_source_sector_snapshot_pair(self.config)
            if sector_pair.get("status") == "AVAILABLE":
                _, _, _all_sess = _load_official_sessions(self.research_cutoff)
                _recent_sess = recent_official_sessions(
                    _all_sess,
                    through=self.research_cutoff,
                    count=min(len(_all_sess), 30),
                )
                sector_pair = {
                    **sector_pair,
                    "snapshot_ids_by_session": resolve_source_sector_snapshot_pairs_by_session(
                        self.config, _recent_sess
                    ),
                }
        forward_path_seed_availability: dict[str, bool] = {}
        plan = build_data_job_plan(
            body,
            cutoff=self.research_cutoff,
            raw_coverage=coverage,
            execution_authorized=self.execution_authorized,
            sector_pair=sector_pair,
            _forward_path_seed_availability_out=forward_path_seed_availability,
        )
        source_policy = self.config.get("source_policy", {})
        primary_policy = (
            source_policy.get("baostock_primary", {})
            if isinstance(source_policy, Mapping)
            else {}
        )
        primary_budget = (
            primary_policy.get("request_budget", {})
            if isinstance(primary_policy, Mapping)
            else {}
        )
        if "BAOSTOCK_PRICES" in request["source_modules"]:
            required_policy_keys = (
                "max_login_batches",
                "max_api_calls_per_batch",
                "query_timeout_seconds",
                "max_history_session_count",
                "max_identity_total_seconds",
                "max_retry_attempts",
                "retry_backoff_seconds",
            )
            policy_valid = (
                isinstance(primary_budget, Mapping)
                and all(key in primary_budget for key in required_policy_keys)
                and type(primary_budget.get("max_login_batches")) is int
                and 1 <= primary_budget["max_login_batches"] <= 10000
                and type(primary_budget.get("max_api_calls_per_batch")) is int
                and 1 <= primary_budget["max_api_calls_per_batch"] <= 100
                and type(primary_budget.get("max_history_session_count")) is int
                and 1 <= primary_budget["max_history_session_count"] <= 250
                and type(primary_budget.get("max_retry_attempts")) is int
                and 0 <= primary_budget["max_retry_attempts"] <= 3
                and isinstance(
                    primary_budget.get("query_timeout_seconds"), (int, float)
                )
                and not isinstance(primary_budget.get("query_timeout_seconds"), bool)
                and 0.2 <= primary_budget["query_timeout_seconds"] <= 30
                and isinstance(
                    primary_budget.get("max_identity_total_seconds"), (int, float)
                )
                and not isinstance(
                    primary_budget.get("max_identity_total_seconds"), bool
                )
                and 0.5 <= primary_budget["max_identity_total_seconds"] <= 120
                and isinstance(
                    primary_budget.get("retry_backoff_seconds"), (int, float)
                )
                and not isinstance(primary_budget.get("retry_backoff_seconds"), bool)
                and 0 <= primary_budget["retry_backoff_seconds"] <= 60
            )
            policy_values = (
                {key: primary_budget.get(key) for key in required_policy_keys}
                if policy_valid
                else {}
            )
            universe_preview = coverage.get("baostock_universe_preview")
            symbol_count = (
                universe_preview.get("symbol_count")
                if isinstance(universe_preview, Mapping)
                else None
            )
            required_capture_pairs = {
                (session, coordinate)
                for session in plan.get("raw_history_sessions", ())
                for coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
            }
            capture_inputs = [
                item
                for item in plan.get("price_capture_inputs", ())
                if isinstance(item, Mapping)
            ]
            existing_capture_pairs = {
                (item.get("session_date"), item.get("price_coordinate"))
                for item in capture_inputs
            }
            existing_capture_ids = {
                item.get("source_capture_id")
                for item in capture_inputs
                if type(item.get("source_capture_id")) is int
                and item.get("source_capture_id") > 0
            }
            capture_reuse_ready = (
                not reset_preflight_required
                and plan.get("price_capture_reader_status") == "AVAILABLE"
                and bool(required_capture_pairs)
                and required_capture_pairs.issubset(existing_capture_pairs)
                and len(existing_capture_ids) == 1
            )
            budget_plan: dict[str, Any] = {
                "status": (
                    "PREFLIGHT_AT_EXECUTION"
                    if reset_preflight_required
                    else "READY_CAPTURE_REUSE"
                    if capture_reuse_ready
                    else "WAITING_FOR_SOURCE_UNIVERSE_READER"
                ),
                "api_calls_per_symbol_coordinate": 1,
                "coordinates": ["RAW_UNADJUSTED", "FORWARD_ADJUSTED"],
                "target_sessions": plan.get("baostock_price_output_sessions", []),
                "source_capture_inputs": plan.get("price_capture_inputs", []),
                "symbol_count": None,
                "identity_login_batches": 1,
                "identity_api_calls": 2,
                "planned_api_calls_total": None,
                "required_initial_login_batches": None,
                "configured": policy_values,
                "source_reader_status": coverage.get(
                    "baostock_universe_reader_status", "WAITING_FOR_SOURCE_READER"
                ),
                "network_requests_performed": False,
                "dynamic_universe_resolved_during": (
                    "RESET_SOURCE_PREFLIGHT_BEFORE_CLEAR"
                    if reset_preflight_required
                    else None
                ),
                "source_preflight_required_before_clear": reset_preflight_required,
            }
            if reset_preflight_required and policy_valid:
                budget_plan["status"] = "PREFLIGHT_AT_EXECUTION"
                if type(symbol_count) is int and symbol_count > 0:
                    max_api = int(primary_budget["max_api_calls_per_batch"])
                    budget_plan["estimated_symbol_count"] = symbol_count
                    budget_plan["estimated_price_api_calls"] = 2 * symbol_count
                    budget_plan["estimated_initial_login_batches"] = 1 + 2 * (
                        (symbol_count + max_api - 1) // max_api
                    )
            elif policy_valid and capture_reuse_ready:
                budget_plan.update(
                    {
                        "status": "READY_CAPTURE_REUSE",
                        "symbol_count": len(
                            {
                                str(item.get("source_symbol"))
                                for item in capture_inputs
                                if isinstance(item.get("source_symbol"), str)
                            }
                        )
                        or None,
                        "identity_login_batches": 0,
                        "identity_api_calls": 0,
                        "price_login_batches": 0,
                        "price_api_calls": 0,
                        "planned_api_calls_total": 0,
                        "required_initial_login_batches": 0,
                        "capture_reused": True,
                        "network_requests_performed": False,
                    }
                )
            elif policy_valid and type(symbol_count) is int and symbol_count > 0:
                max_api = int(primary_budget["max_api_calls_per_batch"])
                max_logins = int(primary_budget["max_login_batches"])
                identity_login_batches = 1
                identity_api_calls = 2
                price_login_batches = 2 * ((symbol_count + max_api - 1) // max_api)
                required_batches = identity_login_batches + price_login_batches
                price_api_calls = 2 * symbol_count
                planned_calls = identity_api_calls + price_api_calls
                budget_plan.update(
                    {
                        "status": (
                            "READY"
                            if required_batches <= max_logins
                            and planned_calls <= max_logins * max_api
                            else "BLOCKED_BUDGET"
                        ),
                        "symbol_count": symbol_count,
                        "identity_login_batches": identity_login_batches,
                        "identity_api_calls": identity_api_calls,
                        "price_login_batches": price_login_batches,
                        "price_api_calls": price_api_calls,
                        "planned_api_calls_total": planned_calls,
                        "required_initial_login_batches": required_batches,
                        "max_api_calls_per_batch": max_api,
                        "max_login_batches": max_logins,
                        "max_identity_total_seconds": primary_budget[
                            "max_identity_total_seconds"
                        ],
                        "max_retry_attempts": primary_budget["max_retry_attempts"],
                        "retry_backoff_seconds": primary_budget[
                            "retry_backoff_seconds"
                        ],
                    }
                )
            elif policy_valid and symbol_count == 0:
                budget_plan["status"] = "BLOCKED_EMPTY_SOURCE_UNIVERSE"
            plan["baostock_price_budget_plan"] = budget_plan
            plan["baostock_price_request_status"] = {
                "source_availability": "NOT_CHECKED_NO_NETWORK",
                "execution_authorized": self.execution_authorized,
                "network_requests_performed": False,
                "price_capture_reader_status": plan.get("price_capture_reader_status"),
            }
            if not policy_valid:
                plan.setdefault("blocked_reasons", []).append(
                    "BAOSTOCK_PRIMARY_POLICY_INVALID"
                )
                plan["executable"] = False
                plan["status"] = "BLOCKED_INPUT_UNAVAILABLE"
            elif budget_plan["status"] == "BLOCKED_BUDGET":
                plan.setdefault("blocked_reasons", []).append(
                    "BAOSTOCK_REQUEST_BUDGET_EXCEEDED"
                )
                plan["executable"] = False
                plan["status"] = "BLOCKED_REQUEST_BUDGET"
            elif budget_plan["status"] in {
                "WAITING_FOR_SOURCE_UNIVERSE_READER",
                "BLOCKED_EMPTY_SOURCE_UNIVERSE",
            }:
                wait_code = (
                    "BAOSTOCK_UNIVERSE_EMPTY"
                    if budget_plan["status"] == "BLOCKED_EMPTY_SOURCE_UNIVERSE"
                    else "BAOSTOCK_UNIVERSE_READER_NOT_CONNECTED"
                )
                plan.setdefault("blocked_reasons", []).append(wait_code)
                plan["executable"] = False
                plan["status"] = "WAITING_FOR_SOURCE_READER"
            if (
                policy_valid
                and request["mode"] == "INITIALIZE"
                and request["history_session_count"]
                > primary_budget.get("max_history_session_count", 0)
            ):
                plan.setdefault("blocked_reasons", []).append(
                    "HISTORY_SESSION_COUNT_EXCEEDS_BAOSTOCK_POLICY"
                )
                plan["executable"] = False
                plan["status"] = "BLOCKED_HISTORY_WINDOW"
        plan["sector_snapshot_pair"] = sector_pair
        plan["sector_snapshot_ids"] = sector_pair.get("snapshot_ids")
        completed = self._complete_plan(
            plan,
            request,
            coverage,
            sector_pair,
            forward_path_seed_availability,
        )
        if (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
            and isinstance(request.get("reset_preview_fingerprint"), str)
        ):
            for recent in data_job_pg.read_recent_jobs(self.config, limit=20):
                if (
                    recent.get("mode") != "INITIALIZE"
                    or recent.get("initialization_mode") != "RESET_ALL_BUSINESS_FACTS"
                    or recent.get("reset_preview_fingerprint")
                    != request["reset_preview_fingerprint"]
                    or recent.get("status")
                    not in {
                        "RUNNING",
                        "FAILED",
                        "INTERRUPTED",
                        "INCOMPLETE",
                        "BLOCKED",
                    }
                ):
                    continue
                if (
                    recent.get("target_session").isoformat()
                    != completed.get("target_session")
                    or recent.get("history_session_count")
                    != completed.get("history_session_count")
                    or recent.get("profile_history_session_count")
                    != completed.get("profile_history_session_count")
                    or recent.get("selected_modules")
                    != completed.get("selected_modules")
                    or recent.get("module_output_session_counts")
                    != [
                        completed["module_output_session_counts"][module]
                        for module in completed["selected_modules"]
                    ]
                ):
                    continue
                saved = data_job_pg.read_job(self.config, job_id=recent["job_id"])
                saved_reset_units = [
                    unit
                    for unit in (saved or {}).get("progress", ())
                    if unit.get("stage_code") == "BUSINESS_RESET"
                ]
                reset_already_cleared = bool(saved_reset_units) and all(
                    unit.get("status") == "CLEARED" for unit in saved_reset_units
                )
                if not (
                    reset_already_cleared
                    or recent.get("status") == "RUNNING"
                    or completed.get("reset_preview_fingerprint")
                    == request["reset_preview_fingerprint"]
                ):
                    continue
                completed["resume_existing_job"] = True
                completed["resume_request_key"] = recent["request_key"]
                completed["resume_job_id"] = str(recent["job_id"])
                completed["source_version_fingerprint"] = recent[
                    "source_version_fingerprint"
                ]
                completed["reset_preview_fingerprint"] = request[
                    "reset_preview_fingerprint"
                ]
                preview = completed.get("reset_preview")
                if isinstance(preview, dict):
                    preview["reset_preview_fingerprint"] = request[
                        "reset_preview_fingerprint"
                    ]
                    preview["resume_existing_reset"] = True
                    preview["resume_job_id"] = str(recent["job_id"])
                    preview["source_preflight_status"] = "REQUIRED_BEFORE_CONTINUE"
                    preview["source_preflight_message"] = (
                        "上次任务已提交清理事务；本次只会恢复尚未完成的来源/算法单元，不会再次清空业务事实。"
                    )
                    preview["irreversible_warning"] = (
                        "当前为既有任务恢复；不会再次执行全库清理。"
                    )
                source_reader_ready = (
                    completed.get("price_capture_reader_status")
                    == "RESET_PREFLIGHT_REQUIRED"
                    and completed.get("baostock_price_budget_plan", {}).get("status")
                    == "PREFLIGHT_AT_EXECUTION"
                    if request["mode"] == "INITIALIZE"
                    and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
                    else completed.get("price_capture_reader_status") == "AVAILABLE"
                    and completed.get("baostock_price_budget_plan", {}).get("status")
                    == "READY"
                )
                completed["executable"] = (
                    self.execution_authorized and source_reader_ready
                )
                completed["status"] = (
                    "RESET_RESUME_READY"
                    if completed["executable"]
                    else "WAITING_FOR_SOURCE_READER"
                    if self.execution_authorized
                    else "BLOCKED_NOT_AUTHORIZED"
                )
                if completed["executable"]:
                    completed["blocked_reasons"] = [
                        reason
                        for reason in completed.get("blocked_reasons", ())
                        if reason != "DATA_EXECUTION_NOT_AUTHORIZED"
                    ]
                break
        return completed

    def _complete_plan(
        self,
        plan: dict[str, Any],
        request: Mapping[str, Any],
        coverage: Mapping[str, Any],
        sector_pair: Mapping[str, Any],
        forward_path_seed_availability: Mapping[str, bool],
    ) -> dict[str, Any]:
        plan["selected_modules"] = request["selected_modules"]
        plan["source_modules"] = request["source_modules"]
        plan["base_modules"] = request["base_modules"]
        plan["research_modules"] = request["research_modules"]
        plan["initialization_mode"] = request["initialization_mode"]
        plan["daily_mode"] = request["daily_mode"]
        plan["observation_rule_ids"] = request["observation_rule_ids"]
        plan["replace_known_incomplete_observation_successors"] = request[
            "replace_known_incomplete_observation_successors"
        ]
        output_counts = dict(request["module_output_session_counts"])
        for module in request["selected_modules"]:
            if module in output_counts:
                continue
            if module == "BAOSTOCK_PRICES":
                output_counts[module] = max(
                    1, len(plan.get("baostock_price_output_sessions", ()))
                )
            elif module == "TDX_SECTOR_MEMBERS":
                output_counts[module] = 1
            elif module == "D0":
                output_counts[module] = max(
                    1, int(plan.get("d0_output_session_count") or 1)
                )
            elif module == "D1":
                output_counts[module] = max(
                    1, int(plan.get("d1_output_session_count") or 1)
                )
            else:
                output_counts[module] = max(
                    1,
                    int(request.get("output_session_count") or 5),
                )
        plan["module_output_session_counts"] = output_counts
        plan["operation"] = request["operation"]
        source_identity = [
            {
                "source_capture_id": capture_id,
                "price_coordinate": coordinate,
            }
            for capture_id, coordinate in sorted(
                {
                    (
                        int(item["source_capture_id"]),
                        str(item["price_coordinate"]),
                    )
                    for item in plan.get("price_capture_inputs", ())
                }
            )
        ]
        request_identity = {
            "algorithm_revision": "STOCK_A_DATA_JOB_ALGORITHM_R10",
            "profile_semantics_contract": "STOCK-A-PRICE-PROFILE-SEMANTICS/R2",
            "source_versions": source_identity,
            "mode": request["mode"],
            "initialization_mode": request["initialization_mode"],
            "daily_mode": request["daily_mode"],
            "source_modules": request["source_modules"],
            "base_modules": request["base_modules"],
            "research_modules": request["research_modules"],
            "observation_target_sessions": plan.get("module_output_sessions", {}).get(
                "OBSERVATION_SEGMENTS", ()
            ),
            "observation_rule_ids": request["observation_rule_ids"],
            "replace_known_incomplete_observation_successors": request[
                "replace_known_incomplete_observation_successors"
            ],
            "history_session_count": plan.get("history_session_count"),
            "profile_history_session_count": plan.get("profile_history_session_count"),
            "module_output_session_counts": output_counts,
            "d1_history_session_count": plan.get("d1_calculation_session_count"),
            "calendar_id": plan.get("calendar_id"),
            "calendar_revision": plan.get("calendar_revision"),
            "operation": request["operation"],
        }
        plan["source_version_fingerprint"] = hashlib.sha256(
            json.dumps(request_identity, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()
        if "SECTOR_PULSE" in request["selected_modules"]:
            sector_source_will_generate = (
                "TDX_SECTOR_MEMBERS" in request["source_modules"]
            )
            pulse_days = plan.get("sector_pulse_sessions", ())
            unavailable_days = set(plan.get("sector_pulse_insufficient_sessions", ()))
            for day in pulse_days:
                day_status = (
                    "NOT_COMPUTED"
                    if (
                        sector_pair.get("status") != "AVAILABLE"
                        and not sector_source_will_generate
                    )
                    or day in unavailable_days
                    else "NOT_STARTED"
                )
                _unit(
                    plan["stage_units"],
                    stage="SECTOR_PULSE",
                    target_session=date.fromisoformat(day),
                    business_table="stock.source_sector_cross_period_pulse_facts",
                    status=day_status,
                    error_code=(
                        "SECTOR_RAW_INPUT_UNAVAILABLE"
                        if day in unavailable_days
                        else sector_pair.get("reason_code")
                    )
                    if day_status == "NOT_COMPUTED"
                    else None,
                    error_message=(
                        "行业与GN概念来源快照对不存在；保留未计算。"
                        if (
                            sector_pair.get("status") != "AVAILABLE"
                            and not sector_source_will_generate
                        )
                        else "目标日QFQ不在已存完整paired capture中；保留未计算。"
                    )
                    if day_status == "NOT_COMPUTED"
                    else None,
                )
            plan["planned_unit_count"] = len(plan["stage_units"])
            if (
                sector_pair.get("status") != "AVAILABLE"
                and not sector_source_will_generate
            ):
                plan.setdefault("unplanned_producers", []).append(
                    {
                        "stage_code": "SECTOR_PULSE",
                        "status": "NOT_COMPUTED",
                        "reason_code": sector_pair.get("reason_code"),
                        "message": "行业与GN概念来源快照对不可用。",
                    }
                )
        _R03_TABLE_BY_MODULE = {
            "R03_SECTOR_NATIVE": "stock.r03_sector_native_day_facts",
            "R03_SECTOR_ROTATION": "stock.r03_sector_rotation_board_day_facts",
            "R03_SECTOR_OVERLAP": "stock.r03_sector_overlap_clusters",
            "R03_SECTOR_LOO": "stock.r03_sector_loo_support_facts",
        }
        for r03_mod, r03_table in _R03_TABLE_BY_MODULE.items():
            if r03_mod in request["selected_modules"]:
                sector_source_will_generate = (
                    "TDX_SECTOR_MEMBERS" in request["source_modules"]
                )
                for day in plan.get("module_output_sessions", {}).get(r03_mod, ()):
                    day_status = (
                        "NOT_COMPUTED"
                        if (
                            sector_pair.get("status") != "AVAILABLE"
                            and not sector_source_will_generate
                        )
                        or day in set(plan.get("raw_input_unavailable_sessions", ()))
                        else "NOT_STARTED"
                    )
                    _unit(
                        plan["stage_units"],
                        stage=r03_mod,
                        target_session=date.fromisoformat(day),
                        business_table=r03_table,
                        status=day_status,
                        error_code=sector_pair.get("reason_code")
                        if day_status == "NOT_COMPUTED"
                        else None,
                        error_message="行业与GN概念来源快照对不可用；保留未计算。"
                        if day_status == "NOT_COMPUTED"
                        else None,
                    )
                plan["planned_unit_count"] = len(plan["stage_units"])
        if "SECTOR_FOLLOWUP" in request["selected_modules"]:
            due_by_session = plan.get("sector_followup_due_targets", {})
            insufficient = set(plan.get("sector_followup_insufficient_sessions", ()))
            for day in plan.get("sector_followup_sessions", ()):
                due = due_by_session.get(day, {})
                status = (
                    "NOT_COMPUTED"
                    if day in insufficient
                    else "NOT_STARTED"
                    if due
                    else "NOT_DUE"
                )
                for table in _STAGE_TABLES["SECTOR_FOLLOWUP"]:
                    _unit(
                        plan["stage_units"],
                        stage="SECTOR_FOLLOWUP",
                        target_session=date.fromisoformat(day),
                        business_table=table,
                        status=status,
                        error_code=(
                            "FOLLOWUP_RAW_INPUT_UNAVAILABLE"
                            if day in insufficient
                            else None
                            if due
                            else "FOLLOWUP_NOT_DUE"
                        ),
                        error_message=None
                        if due and day not in insufficient
                        else "后继会话没有绑定到同一完整paired capture；保留未计算。"
                        if day in insufficient
                        else "研究截止内尚无到期后继会话；不读取截止后的行情。",
                    )
        if "FORWARD_PATH" in request["selected_modules"]:
            official_through_cutoff = _load_calendar(
                date.fromisoformat(plan["research_cutoff"])
            )[3]
            cutoff_date = date.fromisoformat(plan["research_cutoff"])
            eligible_cutoff = tuple(
                s for s in official_through_cutoff if s <= cutoff_date
            )
            for day_text in plan.get("module_output_sessions", {}).get(
                "FORWARD_PATH", ()
            ):
                day_date = date.fromisoformat(day_text)
                idx = (
                    eligible_cutoff.index(day_date)
                    if day_date in eligible_cutoff
                    else -1
                )
                due_horizons = [
                    h
                    for h in (1, 3, 5, 10, 20)
                    if idx >= 0 and idx + h < len(eligible_cutoff)
                ]
                t1_session = (
                    eligible_cutoff[idx + 1]
                    if idx >= 0 and idx + 1 < len(eligible_cutoff)
                    else None
                )
                status, error_code, error_message = _forward_path_dependency_status(
                    due_horizons,
                    t1_seed_available=(
                        t1_session is not None
                        and forward_path_seed_availability.get(t1_session.isoformat())
                        is True
                    ),
                )
                _unit(
                    plan["stage_units"],
                    stage="FORWARD_PATH",
                    target_session=day_date,
                    business_table="stock.source_forward_path_facts",
                    status=status,
                    error_code=error_code,
                    error_message=error_message,
                )
        if "COHORT_STATS" in request["selected_modules"]:
            for day_text in plan.get("module_output_sessions", {}).get(
                "COHORT_STATS", ()
            ):
                _unit(
                    plan["stage_units"],
                    stage="COHORT_STATS",
                    target_session=date.fromisoformat(day_text),
                    business_table="stock.source_cohort_group_stats",
                    status="NOT_STARTED",
                )
        if "FEP_BASELINE" in request["selected_modules"]:
            fep_days = plan.get("module_output_sessions", {}).get("FEP_BASELINE", ())
            if fep_days:
                _unit(
                    plan["stage_units"],
                    stage="FEP_BASELINE",
                    target_session=date.fromisoformat(fep_days[-1]),
                    business_table="stock.source_fep_baseline_facts",
                    status="NOT_STARTED",
                )
        if "OBSERVATION_SEGMENTS" in request["selected_modules"]:
            seg_days = plan.get("module_output_sessions", {}).get(
                "OBSERVATION_SEGMENTS", ()
            )
            for day_text in seg_days:
                _unit(
                    plan["stage_units"],
                    stage="OBSERVATION_SEGMENTS",
                    target_session=date.fromisoformat(day_text),
                    business_table="stock.source_research_observation_segment_days",
                    status="NOT_STARTED",
                )
        if "SECTOR_TRANSITION" in request["selected_modules"]:
            sector_source_will_generate = (
                "TDX_SECTOR_MEMBERS" in request["source_modules"]
            )
            unavailable = set(plan.get("sector_transition_insufficient_sessions", ()))
            for day in plan.get("sector_transition_sessions", ()):
                status = (
                    "NOT_COMPUTED"
                    if day in unavailable
                    or (
                        sector_pair.get("status") != "AVAILABLE"
                        and not sector_source_will_generate
                    )
                    else "NOT_STARTED"
                )
                _unit(
                    plan["stage_units"],
                    stage="SECTOR_TRANSITION",
                    target_session=date.fromisoformat(day),
                    business_table="stock.source_sector_pulse_adjacent_transitions",
                    status=status,
                    error_code=(
                        "SECTOR_TRANSITION_INPUT_UNAVAILABLE"
                        if status == "NOT_COMPUTED"
                        else None
                    ),
                    error_message=(
                        "精确前序QFQ捕获或行业/GN快照输入不可用；相邻变化保留未计算。"
                        if status == "NOT_COMPUTED"
                        else None
                    ),
                )
        if request["daily_mode"] == "FULL_THREE_LAYERS" or (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        ):
            for table, label, message in _UNCOORDINATED_RESEARCH_CONSUMERS:
                plan.setdefault("unplanned_producers", []).append(
                    {
                        "stage_code": label,
                        "business_table": f"stock.{table}",
                        "status": "SKIPPED_NOT_IMPLEMENTED",
                        "reason_code": "PRODUCER_NOT_IN_JOB_COORDINATOR",
                        "message": message,
                    }
                )
        if (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        ):
            target_sessions = tuple(plan.get("raw_history_sessions", ()))
            reset_preview = business_reset_pg.preview_business_fact_reset(
                self.config,
                target_sessions=target_sessions,
                source_modules=tuple(request["source_modules"]),
                history_session_count=int(plan["history_session_count"]),
                module_output_session_counts=output_counts,
            )
            reset_preview.update(
                {
                    "source_ready": False,
                    "sources_ready": False,
                    "source_preflight_status": "REQUIRED_BEFORE_DELETE",
                    "source_preflight_required_before_delete": True,
                    "source_preflight_message": "任务启动后将先验证BaoStock双坐标价格来源及TDX行业/GN成员来源；任一必需来源不完整时保留现有事实，不执行清理。",
                    "irreversible_warning": "业务事实清理提交后不能直接撤销；只有删除前的全部来源与窗口预检通过才会清理。",
                }
            )
            plan["reset_preview"] = reset_preview
            plan["reset_preview_fingerprint"] = reset_preview[
                "reset_preview_fingerprint"
            ]
            if reset_preview.get("complete") is not True:
                plan["executable"] = False
                plan["status"] = "RESET_PREVIEW_INCOMPLETE"
                plan.setdefault("blocked_reasons", []).append(
                    "RESET_PREVIEW_OR_DATABASE_IDENTITY_INCOMPLETE"
                )
            reset_target = date.fromisoformat(plan["target_session"])
            for row in reset_preview["affected_tables"]:
                _unit(
                    plan["stage_units"],
                    stage="BUSINESS_RESET",
                    target_session=reset_target,
                    business_table=row["table"],
                )
            plan["source_version_fingerprint"] = hashlib.sha256(
                json.dumps(
                    {
                        "request_fingerprint": plan["source_version_fingerprint"],
                        "reset_preview_fingerprint": plan["reset_preview_fingerprint"],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if not self.execution_authorized:
                plan["executable"] = False
                plan["status"] = "BLOCKED_NOT_AUTHORIZED"
                plan["blocked_reasons"] = list(
                    dict.fromkeys(
                        [
                            *plan.get("blocked_reasons", ()),
                            "DATA_EXECUTION_NOT_AUTHORIZED",
                        ]
                    )
                )
        if request["operation"] in _CLEANUP_OPERATIONS:
            cleanup_module = next(
                iter(request["base_modules"] + request["research_modules"])
            )
            cleanup_days = plan.get("module_output_sessions", {}).get(
                cleanup_module, []
            )
            cleanup_member_version: str | None = None
            if cleanup_module in {
                "R03_SECTOR_NATIVE",
                "R03_SECTOR_ROTATION",
                "R03_SECTOR_OVERLAP",
                "R03_SECTOR_LOO",
            }:
                from stock_a.application.source_sector_pulse import (
                    resolve_source_sector_snapshot_pair,
                )

                snap_pair = resolve_source_sector_snapshot_pair(
                    config=self.config,
                    research_cutoff=self.research_cutoff,
                )
                sids = (
                    snap_pair.get("snapshot_ids")
                    if isinstance(snap_pair, Mapping)
                    else None
                )
                if (
                    isinstance(snap_pair, Mapping)
                    and snap_pair.get("status") == "AVAILABLE"
                    and isinstance(sids, Mapping)
                    and sids.get("INDUSTRY") is not None
                    and sids.get("CONCEPT") is not None
                ):
                    cleanup_member_version = (
                        f"IND_{int(sids['INDUSTRY'])}:CON_{int(sids['CONCEPT'])}"
                    )
            plan["cleanup_preview"] = data_job_pg.preview_derived_cleanup(
                self.config,
                module_code=cleanup_module,
                target_sessions=tuple(date.fromisoformat(day) for day in cleanup_days),
                cutoff=self.research_cutoff,
                member_version=cleanup_member_version,
            )
            plan["cleanup_preview"]["module_label"] = _MODULES[cleanup_module][1]
            cleanup_identity = {
                "input_source_fingerprint": plan["source_version_fingerprint"],
                "cleanup_preview_key": plan["cleanup_preview"].get("preview_key"),
            }
            plan["source_version_fingerprint"] = hashlib.sha256(
                json.dumps(
                    cleanup_identity,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            if plan["cleanup_preview"].get("complete") is not True:
                plan["executable"] = False
                plan["status"] = "CLEANUP_SCOPE_UNAVAILABLE"
            elif not request["source_modules"]:
                plan["executable"] = True
                plan["status"] = (
                    "READY_WITH_INCOMPLETE_UNITS"
                    if any(
                        unit.get("status") == "NOT_COMPUTED"
                        for unit in plan["stage_units"]
                    )
                    else "READY"
                )
                plan["blocked_reasons"] = [
                    reason
                    for reason in plan.get("blocked_reasons", [])
                    if reason != "DATA_EXECUTION_NOT_AUTHORIZED"
                ]
        if request["operation"] == "CLEAR_DERIVED" and not request["source_modules"]:
            plan["executable"] = plan.get("cleanup_preview", {}).get("complete") is True
        plan["planned_unit_count"] = len(plan["stage_units"])
        plan["source_io_attempted"] = False
        plan["database_write_attempted"] = False
        return plan

    def read_job(self, job_id: UUID | None = None) -> dict[str, Any] | None:
        return data_job_pg.read_job(self.config, job_id=job_id)

    def recent_jobs(self) -> list[dict[str, Any]]:
        return data_job_pg.read_recent_jobs(self.config, limit=8)

    def start(self, body: Any) -> dict[str, Any]:
        plan = self.plan(body)
        request = _request_fields(body)
        if (
            request["mode"] == "INITIALIZE"
            and request["initialization_mode"] == "RESET_ALL_BUSINESS_FACTS"
        ):
            preview = plan.get("reset_preview")
            if not isinstance(preview, Mapping) or preview.get("complete") is not True:
                raise DataJobError(
                    "RESET_PREVIEW_INCOMPLETE",
                    "数据库身份或完整清理预览未通过，拒绝创建全库清理任务。",
                    status=409,
                )
            if request["reset_confirmed"] is not True:
                raise DataJobError(
                    "RESET_CONFIRMATION_REQUIRED",
                    "清理本项目全部业务事实前需要在菜单再次确认。",
                    status=409,
                )
            if not isinstance(preview, Mapping) or request[
                "reset_preview_fingerprint"
            ] != preview.get("reset_preview_fingerprint"):
                raise DataJobError(
                    "RESET_PREVIEW_STALE",
                    "重置影响范围自预览后发生变化；未创建任务，也未清理数据。请重新读取预览。",
                    status=409,
                )
        if not plan.get("executable"):
            return {"accepted": False, "plan": plan}
        request_key = plan.get("resume_request_key") or _job_request_key(plan)
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                latest = data_job_pg.read_job(self.config)
                if latest is not None and latest.get("request_key") == request_key:
                    return {
                        "accepted": True,
                        "status": "RUNNING",
                        "job": latest,
                        "reused": True,
                    }
                raise DataJobError(
                    "DATA_JOB_BUSY",
                    "已有不同模块、来源版本或操作范围的任务运行中。",
                    status=409,
                )
            task_id = uuid4()
            job = {
                "job_id": task_id,
                "request_key": request_key,
                "mode": plan["mode"],
                "target_session": date.fromisoformat(plan["target_session"]),
                "research_cutoff": self.research_cutoff,
                "history_session_count": plan["history_session_count"],
                "profile_history_session_count": plan.get(
                    "profile_history_session_count"
                ),
                "d0_output_session_count": plan["d0_output_session_count"],
                "d1_output_session_count": plan["d1_output_session_count"],
                "operation": plan["operation"],
                "initialization_mode": plan["initialization_mode"],
                "daily_mode": plan["daily_mode"],
                "reset_preview_fingerprint": plan.get("reset_preview_fingerprint"),
                "selected_modules": plan["selected_modules"],
                "module_output_session_counts": [
                    plan["module_output_session_counts"][module]
                    for module in plan["selected_modules"]
                ],
                "source_version_fingerprint": plan["source_version_fingerprint"],
                "calendar_id": plan["calendar_id"],
                "calendar_revision": plan["calendar_revision"],
                "planned_units": plan["planned_unit_count"],
            }
            result = data_job_pg.create_or_resume_job(
                self.config, job=job, progress=plan["stage_units"]
            )
            if result["status"] == "ALREADY_COMPLETE":
                return {"accepted": True, **result}
            if result.get("active") and result["status"] == "RUNNING":
                return {
                    "accepted": True,
                    **result,
                    "job": data_job_pg.read_job(self.config, job_id=result["job_id"]),
                }
            if result.get("reused") is True:
                resumed = data_job_pg.read_job(self.config, job_id=result["job_id"])
                closed_statuses = {
                    "COMPLETED",
                    "ALREADY_COMPLETE",
                    "NO_CHANGE",
                    "CLEARED",
                    "NOT_DUE",
                    "SKIPPED_BY_USER",
                    "SKIPPED_NOT_IMPLEMENTED",
                }
                persisted_progress = (
                    resumed.get("progress", ()) if isinstance(resumed, Mapping) else ()
                )
                if len(persisted_progress) == len(plan.get("stage_units", ())) and all(
                    isinstance(unit, Mapping) and unit.get("status") in closed_statuses
                    for unit in persisted_progress
                ):
                    completed = data_job_pg.finish_job(
                        self.config, job_id=result["job_id"]
                    )
                    return {
                        "accepted": True,
                        **completed,
                        "reused": True,
                        "completed_from_verified_progress": True,
                    }
            self._thread = threading.Thread(
                target=self._execute_job,
                args=(result["job_id"], plan),
                name="stock-a-data-job",
                daemon=True,
            )
            self._thread.start()
        return {
            "accepted": True,
            "status": "RUNNING",
            "job_id": result["job_id"],
            "reused": result.get("reused", False),
        }

    def _execute_job(self, job_id: UUID, plan: Mapping[str, Any]) -> None:
        prepared_reset_inputs = None
        try:
            execution_plan = dict(plan)
            if (
                plan.get("mode") == "INITIALIZE"
                and plan.get("initialization_mode") == "RESET_ALL_BUSINESS_FACTS"
            ):
                progress_job = data_job_pg.read_job(self.config, job_id=job_id)
                reset_progress = [
                    item
                    for item in (progress_job or {}).get("progress", ())
                    if item.get("stage_code") == "BUSINESS_RESET"
                ]
                reset_already_completed = bool(reset_progress) and all(
                    item.get("status") == "CLEARED" for item in reset_progress
                )
                expected_input_fingerprint = (
                    None
                    if reset_already_completed
                    else data_job_pg.read_reset_preflight_fingerprint(
                        self.config, job_id=job_id
                    )
                )
                prepared = _prepare_reset_source_inputs(
                    self.config,
                    plan,
                    job_id=job_id,
                    expected_preflight_fingerprint=expected_input_fingerprint,
                    reset_already_completed=reset_already_completed,
                )
                prepared_reset_inputs = prepared
                if not reset_already_completed:
                    data_job_pg.bind_reset_preflight_fingerprint(
                        self.config,
                        job_id=job_id,
                        fingerprint=prepared["preflight_fingerprint"],
                    )
                execution_plan["_prepared_baostock_reset_inputs"] = (
                    prepared_reset_inputs
                )
                execution_plan["_prepared_sector_snapshots"] = prepared[
                    "sector_snapshots"
                ]
                if not reset_already_completed:
                    reset_units = [
                        unit
                        for unit in plan.get("stage_units", ())
                        if unit.get("stage_code") == "BUSINESS_RESET"
                    ]
                    begin_callback = self._begin_units
                    begin_callback(job_id=job_id, units=reset_units)
                    preview = plan.get("reset_preview")
                    if not isinstance(preview, Mapping):
                        raise DataJobError(
                            "RESET_PREVIEW_REQUIRED",
                            "清理前缺少完整业务事实影响预览。",
                            status=409,
                        )
                    expected_counts = {
                        str(item["table"]): int(item["row_count"])
                        for item in preview.get("affected_tables", ())
                    }
                    reset_result = business_reset_pg.clear_business_facts(
                        self.config,
                        job_id=job_id,
                        expected_preview_fingerprint=str(
                            preview.get("reset_preview_fingerprint") or ""
                        ),
                        expected_table_counts=expected_counts,
                        target_sessions=tuple(preview.get("target_sessions", ())),
                        source_modules=tuple(preview.get("source_modules", ())),
                        history_session_count=int(
                            preview.get("history_session_count") or 0
                        ),
                        module_output_session_counts=preview.get(
                            "module_output_session_counts", {}
                        ),
                    )
                    deleted_rows = reset_result.get("deleted_rows_by_table", {})
                    for unit in reset_units:
                        data_job_pg.mark_business_reset_progress(
                            self.config,
                            job_id=job_id,
                            target_session=unit["target_session"],
                            business_table=unit["business_table"],
                            deleted_rows=int(
                                deleted_rows.get(unit["business_table"], 0)
                            ),
                        )
            if plan.get("operation") in _CLEANUP_OPERATIONS:
                _execute_derived_cleanup(
                    config=self.config,
                    job_id=job_id,
                    plan=plan,
                    begin_callback=self._begin_units,
                )
                if plan.get("operation") == "CLEAR_DERIVED":
                    data_job_pg.finish_job(self.config, job_id=job_id)
                    return
            _execute_shared_production_chain(
                config=self.config,
                job_id=job_id,
                plan=execution_plan,
                progress_callback=self._record_verified_unit,
                begin_callback=self._begin_units,
            )
            data_job_pg.finish_job(self.config, job_id=job_id)
        except Exception as error:  # noqa: BLE001 - 后台任务必须把任意阶段异常持久化为失败状态。
            error_code = getattr(error, "code", type(error).__name__)
            error_message = str(error)[:1000]
            status = (
                "BLOCKED"
                if str(error_code).startswith(("BLOCKED_", "NOT_AUTHORIZED"))
                else "FAILED"
            )
            data_job_pg.fail_job(
                self.config,
                job_id=job_id,
                status=status,
                error_code=error_code,
                error_message=error_message,
            )
        finally:
            prepared_reset_inputs = None

    def _begin_units(self, *, job_id: UUID, units: Sequence[Mapping[str, Any]]) -> None:
        data_job_pg.mark_progress_running_batch(self.config, job_id=job_id, units=units)

    def _record_verified_unit(
        self,
        *,
        job_id: UUID,
        stage_code: str,
        target_session: date,
        business_table: str,
        source_capture_id: int | None,
        profile_history_session_count: int | None,
        d1_history_session_count: int | None,
        expected_rows: int | None,
        inserted_rows: int,
        quality_status: str | None,
        no_change: bool = False,
        profile_revision: str | None = None,
        producer_version: str | Sequence[str] | None = None,
        calendar_revision: str | None = None,
        sector_snapshot_ids: Mapping[str, int] | None = None,
        followup_target_sessions: Sequence[date] | None = None,
        rule_ids: Sequence[str] | None = None,
        universe_capture_id: int | None = None,
        cross_section_universe_capture_id: int | None = None,
        cross_section_universe_version: str | None = None,
        cross_section_contract_version: str | None = None,
        universe_match_status: str | None = None,
        warning_code: str | None = None,
        warning_message: str | None = None,
    ) -> None:
        running = data_job_pg.mark_progress_running(
            self.config,
            job_id=job_id,
            stage_code=stage_code,
            target_session=target_session,
            business_table=business_table,
        )
        actual_rows = data_job_pg.read_business_table_count(
            self.config,
            business_table=business_table,
            target_session=target_session,
            source_capture_id=source_capture_id,
            profile_history_session_count=profile_history_session_count,
            d1_history_session_count=d1_history_session_count,
            profile_revision=profile_revision,
            producer_version=producer_version,
            calendar_revision=calendar_revision,
            sector_snapshot_ids=sector_snapshot_ids,
            followup_target_sessions=followup_target_sessions,
            rule_ids=rule_ids,
            universe_capture_id=universe_capture_id,
            cross_section_universe_capture_id=cross_section_universe_capture_id,
            cross_section_universe_version=cross_section_universe_version,
            cross_section_contract_version=cross_section_contract_version,
            universe_match_status=universe_match_status,
            research_cutoff=self.research_cutoff,
        )
        stored_producer_version = (
            "|".join(str(value) for value in producer_version)
            if isinstance(producer_version, Sequence)
            and not isinstance(producer_version, (str, bytes))
            else producer_version
        )
        if not running:
            previous = data_job_pg.read_progress_unit(
                self.config,
                job_id=job_id,
                stage_code=stage_code,
                target_session=target_session,
                business_table=business_table,
            )
            expected_identity = {
                "source_capture_id": source_capture_id,
                "profile_revision": profile_revision,
                "producer_version": stored_producer_version,
                "profile_history_session_count": profile_history_session_count,
                "d1_history_session_count": d1_history_session_count,
                "calendar_revision": calendar_revision,
                "industry_snapshot_id": (
                    sector_snapshot_ids.get("INDUSTRY")
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                "concept_snapshot_id": (
                    sector_snapshot_ids.get("CONCEPT")
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            }
            identity_matches = previous is not None and all(
                previous.get(key) == value for key, value in expected_identity.items()
            )
            if (
                previous is None
                or previous.get("verified_rows") != actual_rows
                or not identity_matches
            ):
                identity_differences = {
                    key: {
                        "stored": str(previous.get(key))[:160]
                        if previous is not None
                        else None,
                        "current": str(value)[:160],
                    }
                    for key, value in expected_identity.items()
                    if previous is None or previous.get(key) != value
                }
                raise DataJobError(
                    "DATA_JOB_READBACK_IDENTITY_CHANGED",
                    "已核验进度与本次读回身份不一致："
                    f"stage={stage_code}，target={target_session.isoformat()}，"
                    f"table={business_table}，actual_rows={actual_rows}，"
                    f"stored_verified_rows={previous.get('verified_rows') if previous is not None else None}，"
                    f"identity_differences={identity_differences}。",
                    status=409,
                )
            return
        data_job_pg.mark_progress_verified(
            self.config,
            job_id=job_id,
            stage_code=stage_code,
            target_session=target_session,
            business_table=business_table,
            verified_rows=actual_rows,
            inserted_rows=inserted_rows,
            updated_rows=0,
            expected_rows=expected_rows,
            quality_status=quality_status,
            source_capture_id=source_capture_id,
            profile_revision=profile_revision,
            producer_version=stored_producer_version,
            profile_history_session_count=profile_history_session_count,
            d1_history_session_count=d1_history_session_count,
            calendar_revision=calendar_revision,
            industry_snapshot_id=(
                sector_snapshot_ids.get("INDUSTRY")
                if isinstance(sector_snapshot_ids, Mapping)
                else None
            ),
            concept_snapshot_id=(
                sector_snapshot_ids.get("CONCEPT")
                if isinstance(sector_snapshot_ids, Mapping)
                else None
            ),
            no_change=no_change,
            warning_code=warning_code,
            warning_message=warning_message,
        )


def _prepare_reset_resume_inputs_from_persisted_capture(
    config: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    job_id: UUID,
    sessions: tuple[date, ...],
    cutoff: date,
    budget: Mapping[str, Any],
) -> dict[str, Any]:
    """在清理后的续跑中，仅从PG已闭合的捕获与价格行恢复。"""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from stock_a.sources.baostock.identity import (
        classify_baostock_board,
        normalize_baostock_symbol,
        parse_baostock_optional_date,
    )
    from stock_a.sources.tdx.remote_sector import (
        fetch_remote_sector_files,
        remote_sector_source_inputs,
        validate_remote_sector_files,
    )
    from stock_a.sources.tdx.sector_members import (
        parse_remote_sector_source_inputs_with_status,
    )
    from stock_a.storage.postgres import baostock_price_facts, source_raw
    from stock_a.storage.postgres.tdx_sector_members import _validate_limits

    saved_job = data_job_pg.read_job(config, job_id=job_id)
    source_capture_id = (
        saved_job.get("source_capture_id") if isinstance(saved_job, Mapping) else None
    )
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise DataJobError(
            "RESET_RESUME_CAPTURE_MISSING",
            "既有重初始化任务没有已存BaoStock捕获；拒绝重复请求价格或推断证券池。",
            status=409,
        )
    roster = baostock_price_facts.read_baostock_capture_roster(
        dict(config), source_capture_id=source_capture_id, cutoff=cutoff
    )
    if (
        roster.get("status") != "AVAILABLE"
        or roster.get("source_capture_id") != source_capture_id
        or roster.get("range_start_session") != sessions[0].isoformat()
        or roster.get("range_end_session") != sessions[-1].isoformat()
        or roster.get("requested_symbol_count") != len(roster.get("symbols", ()))
    ):
        raise DataJobError(
            "RESET_RESUME_CAPTURE_SCOPE_INVALID",
            "已存BaoStock捕获证券池、会话范围或配对请求目录未闭合；不重发价格请求。",
            status=409,
        )

    roster_rows = tuple(roster.get("symbols", ()))
    universe = tuple(
        sorted(
            str(item.get("source_symbol"))
            for item in roster_rows
            if isinstance(item, Mapping) and isinstance(item.get("source_symbol"), str)
        )
    )
    if (
        len(universe) != len(roster_rows)
        or not universe
        or len(set(universe)) != len(universe)
        or any(normalize_baostock_symbol(symbol) != symbol for symbol in universe)
    ):
        raise DataJobError(
            "RESET_RESUME_CAPTURE_SYMBOLS_INVALID",
            "已存BaoStock捕获证券代码集合无效或重复；不重发价格请求。",
            status=409,
        )

    eligible_sessions_by_symbol: dict[str, tuple[date, ...]] = {}
    board_by_symbol: dict[str, str] = {}
    board_counts: dict[str, int] = {}
    for item in roster_rows:
        symbol = str(item["source_symbol"])
        board_identity = classify_baostock_board(symbol)
        coordinates = item.get("coordinates")
        if (
            board_identity is None
            or board_identity[1] not in {"MAIN_BOARD", "GEM", "STAR_BOARD"}
            or item.get("eligible_windows_match") is not True
            or item.get("request_complete") is not True
            or not isinstance(coordinates, Mapping)
            or set(coordinates) != {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
            or any(
                coordinates[coordinate].get("request_status")
                not in {"COMPLETED", "NO_DATA"}
                for coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
            )
        ):
            raise DataJobError(
                "RESET_RESUME_CAPTURE_REQUESTS_INCOMPLETE",
                f"已存捕获{symbol}的板别、RAW/QFQ请求或有效窗口未闭合；不重发价格请求。",
                status=409,
            )
        try:
            eligible_start = date.fromisoformat(item["eligible_start_session"])
            eligible_end = date.fromisoformat(item["eligible_end_session"])
        except (KeyError, TypeError, ValueError) as error:
            raise DataJobError(
                "RESET_RESUME_ELIGIBLE_WINDOW_INVALID",
                f"已存捕获{symbol}的官方会话有效窗口无效。",
                status=409,
            ) from error
        eligible = tuple(
            day for day in sessions if eligible_start <= day <= eligible_end
        )
        if not eligible:
            raise DataJobError(
                "RESET_RESUME_ELIGIBLE_WINDOW_EMPTY",
                f"已存捕获{symbol}在当前价格会话窗口内没有有效交易会话。",
                status=409,
            )
        eligible_sessions_by_symbol[symbol] = eligible
        board_by_symbol[symbol] = board_identity[1]
        board_counts[board_identity[1]] = board_counts.get(board_identity[1], 0) + 1

    names_snapshot = baostock_price_facts.read_baostock_basic_names_for_capture(
        dict(config), source_capture_id=source_capture_id
    )
    names = tuple(names_snapshot.get("names", ()))
    named_symbols = tuple(
        sorted(
            normalize_baostock_symbol(item.get("code"))
            for item in names
            if isinstance(item, Mapping)
        )
    )
    if (
        names_snapshot.get("status") != "AVAILABLE"
        or names_snapshot.get("source_capture_id") != source_capture_id
        or named_symbols != universe
        or len(names) != len(universe)
    ):
        raise DataJobError(
            "RESET_RESUME_BASIC_NAMES_INCOMPLETE",
            "已存BaoStock基本资料名称快照与捕获证券集合不一致。",
            status=409,
        )

    captured_at = names_snapshot.get("captured_at")
    if (
        not isinstance(captured_at, datetime)
        or captured_at.tzinfo is None
        or captured_at.utcoffset() is None
    ):
        raise DataJobError(
            "RESET_RESUME_BASIC_NAMES_CLOCK_INVALID",
            "已存BaoStock名称快照缺少真实带时区捕获时钟。",
            status=409,
        )

    capture_uuid = UUID(str(roster.get("capture_uuid")))
    capture_material = {
        "start": sessions[0].isoformat(),
        "end": sessions[-1].isoformat(),
        "symbols": list(universe),
    }
    capture_digest = hashlib.sha256(
        json.dumps(capture_material, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    expected_capture_uuid = uuid5(
        NAMESPACE_URL, f"stock-a:baostock:paired-capture:{capture_digest}"
    )
    if capture_uuid != expected_capture_uuid:
        raise DataJobError(
            "RESET_RESUME_CAPTURE_UUID_MISMATCH",
            "已存BaoStock捕获UUID与其会话窗口和证券总体不一致。",
            status=409,
        )

    expected_rows_by_session = {
        day: sum(day in eligible for eligible in eligible_sessions_by_symbol.values())
        for day in sessions
    }
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute(
            """SELECT session_date,count(*)::bigint
               FROM stock.source_raw_daily WHERE source_capture_id=%s
               GROUP BY session_date ORDER BY session_date""",
            (source_capture_id,),
        )
        actual_rows_by_session = {row[0]: int(row[1]) for row in cursor.fetchall()}
    _require_persisted_baostock_session_rows(
        actual_rows_by_session, expected_rows_by_session
    )
    # PG中的RAW/QFQ规范行和配对捕获目录是本路径唯一的来源事实与恢复依据。

    from stock_a.application.tdx_sector_members import (
        read_tdx_sector_source_snapshot_catalog,
    )

    planned_pair = plan.get("sector_snapshot_pair")
    snapshot_ids = (
        planned_pair.get("snapshot_ids") if isinstance(planned_pair, Mapping) else None
    )
    if (
        not isinstance(snapshot_ids, Mapping)
        or set(snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or any(type(value) is not int or value <= 0 for value in snapshot_ids.values())
        or snapshot_ids["INDUSTRY"] == snapshot_ids["CONCEPT"]
    ):
        raise DataJobError(
            "RESET_RESUME_SECTOR_SNAPSHOT_PAIR_INVALID",
            "既有PG恢复任务缺少已验证的行业/GN快照身份；不重复请求来源。",
            status=409,
        )
    try:
        catalog = read_tdx_sector_source_snapshot_catalog(dict(config))
        if catalog.get("status") != "AVAILABLE":
            raise ValueError("PG行业/GN快照目录不可用")
        catalog_by_id = {
            int(item["snapshot_id"]): item
            for item in catalog.get("snapshots", ())
            if isinstance(item, Mapping) and type(item.get("snapshot_id")) is int
        }
        expected_kind = {
            "INDUSTRY": "TDX_TDXHY_CFG",
            "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
        }
        snapshots = []
        for kind in ("INDUSTRY", "CONCEPT"):
            item = catalog_by_id.get(snapshot_ids[kind])
            if (
                not isinstance(item, Mapping)
                or item.get("source_kind") != expected_kind[kind]
                or int(item.get("catalog_rows", 0)) <= 0
                or int(item.get("member_rows", 0)) <= 0
                or int(item.get("invalid_membership_quality_rows", 0)) != 0
            ):
                raise ValueError(f"既有{kind}快照身份或PG读回不完整")
            snapshots.append(dict(item))
    except Exception as error:
        raise DataJobError(
            "RESET_RESUME_SECTOR_PREFLIGHT_FAILED",
            "既有PG行业/GN成员快照未闭合；恢复时不重复请求行业来源。",
            status=503,
        ) from error
    sector_file_fingerprint = {
        "reuse_existing_pg_snapshot_ids": {
            kind: snapshot_ids[kind] for kind in ("INDUSTRY", "CONCEPT")
        },
        "snapshots": [
            {
                key: item.get(key)
                for key in (
                    "snapshot_id",
                    "source_kind",
                    "observed_at",
                    "parse_contract_version",
                    "source_quality",
                    "catalog_rows",
                    "member_rows",
                )
            }
            for item in snapshots
        ],
    }
    max_api = int(budget["max_api_calls_per_batch"])
    max_logins = int(budget["max_login_batches"])
    preflight = {
        "mode": "PERSISTED_CAPTURE_RESUME",
        "status": "EXISTING_CAPTURE_COMPLETE",
        "completion_status": "PERSISTED_CAPTURE_COMPLETE",
        "identity_requests_performed": False,
        "price_requests_performed": False,
        "source_network_called": False,
        "requested_symbols": len(universe),
        "target_sessions": [day.isoformat() for day in sessions],
        "price_api_calls": 0,
        "planned_price_api_calls": 0,
        "identity_api_calls": 0,
        "planned_total_api_calls": 0,
        "identity_login_batches": 0,
        "price_login_batches": 0,
        "planned_initial_price_login_batches": 0,
        "planned_total_login_batches": 0,
        "max_login_batches": max_logins,
        "max_api_calls_per_batch": max_api,
        "effective_total_api_budget": max_logins * max_api,
        "effective_price_retry_attempts": 0,
        "full_capture_deferred_until_after_clear": True,
        "source_requests_repeated_after_clear": False,
        "capture_reused": True,
    }
    identity = {
        "status": "REUSED_PERSISTED_BAOSTOCK_CAPTURE",
        "request_completed_at": captured_at.isoformat(),
        "login_attempted": False,
        "verified_source_capture_id": None,
        "source_capture_id": source_capture_id,
        "capture_uuid": str(capture_uuid),
    }
    fingerprint_material = {
        "contract": "STOCK_A_RESET_PERSISTED_CAPTURE_RESUME_R1",
        "source_capture_id": source_capture_id,
        "capture_uuid": str(capture_uuid),
        "target_sessions": [day.isoformat() for day in sessions],
        "source_symbols": list(universe),
        "basic_names_capture_clock": captured_at.isoformat(),
        "sector_parse_contract": "|".join(
            sorted(str(item["parse_contract_version"]) for item in snapshots)
        ),
        "sector_sources": sector_file_fingerprint,
    }
    preflight_fingerprint = hashlib.sha256(
        json.dumps(
            fingerprint_material,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "identity_snapshot": identity,
        "source_symbols": universe,
        "eligible_sessions_by_symbol": eligible_sessions_by_symbol,
        "board_by_symbol": board_by_symbol,
        "board_counts": board_counts,
        "basic_names": names,
        "price_preflight": preflight,
        "capture_uuid": capture_uuid,
        "price_preflight_symbols": universe,
        "preflight_login_batches": 0,
        "price_preflight_sessions": sessions,
        "sector_snapshots": {
            "reuse_existing_snapshot_ids": {
                kind: snapshot_ids[kind] for kind in ("INDUSTRY", "CONCEPT")
            }
        },
        "sector_source_root": "PG_EXISTING_TDX_SOURCE_SNAPSHOTS",
        "sector_source_fingerprint": sector_file_fingerprint,
        "preflight_fingerprint": preflight_fingerprint,
        "preflight_status": "REUSED_PERSISTED_PAIRED_CAPTURE",
        "source_request_performed": False,
    }


def _prepare_reset_source_inputs(
    config: Mapping[str, Any],
    plan: Mapping[str, Any],
    *,
    job_id: UUID,
    expected_preflight_fingerprint: str | None = None,
    reset_already_completed: bool = False,
) -> dict[str, Any]:
    """清理前完成BaoStock双坐标小样本与TDX成员来源预检。"""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from stock_a.application.baostock_daily_jobs import (
        BaoStockDailyJobError,
        fetch_baostock_price_window,
    )
    from stock_a.sources.baostock.client import query_identity_reference
    from stock_a.sources.baostock.evidence import _UNITS as documented_units
    from stock_a.sources.baostock.identity import (
        classify_baostock_board,
        normalize_baostock_symbol,
        parse_baostock_optional_date,
    )
    from stock_a.sources.tdx.remote_sector import (
        fetch_remote_sector_files,
        remote_sector_source_inputs,
        validate_remote_sector_files,
    )
    from stock_a.sources.tdx.sector_members import (
        parse_remote_sector_source_inputs_with_status,
    )
    from stock_a.storage.postgres.tdx_sector_members import _validate_limits

    sessions = tuple(
        date.fromisoformat(value) for value in plan.get("raw_history_sessions", ())
    )
    cutoff = date.fromisoformat(plan["research_cutoff"])
    if (
        not isinstance(job_id, UUID)
        or not sessions
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] > cutoff
        or any(session > cutoff for session in sessions)
    ):
        raise DataJobError(
            "RESET_PRICE_WINDOW_INVALID",
            "清理预检要求完整、递增且不超过预算的BaoStock官方价格窗口。",
            status=409,
        )
    _calendar_id, _calendar_revision, official_sessions = _load_official_sessions(
        cutoff
    )
    if any(session not in official_sessions for session in sessions):
        raise DataJobError(
            "RESET_PRICE_WINDOW_NOT_OFFICIAL",
            "清理预检窗口包含非官方会话；现有业务事实保持不变。",
            status=409,
        )

    source_policy = config.get("source_policy")
    primary = (
        source_policy.get("baostock_primary")
        if isinstance(source_policy, Mapping)
        else None
    )
    request_budget = (
        primary.get("request_budget") if isinstance(primary, Mapping) else None
    )
    if not isinstance(request_budget, Mapping):
        raise DataJobError(
            "RESET_BAOSTOCK_POLICY_INVALID",
            "清理预检缺少BaoStock唯一来源请求预算；现有业务事实保持不变。",
            status=409,
        )
    budget = dict(request_budget)
    required_budget = {
        "max_login_batches",
        "max_api_calls_per_batch",
        "query_timeout_seconds",
        "max_identity_total_seconds",
        "max_history_session_count",
        "max_retry_attempts",
        "retry_backoff_seconds",
    }
    if (
        set(budget) != required_budget
        or type(budget.get("max_login_batches")) is not int
        or not 1 <= budget["max_login_batches"] <= 1000
        or type(budget.get("max_api_calls_per_batch")) is not int
        or not 1 <= budget["max_api_calls_per_batch"] <= 100
        or type(budget.get("max_history_session_count")) is not int
        or not 1 <= budget["max_history_session_count"] <= 250
        or len(sessions) > budget.get("max_history_session_count", 0)
        or type(budget.get("max_retry_attempts")) is not int
        or not 0 <= budget["max_retry_attempts"] <= 3
        or isinstance(budget.get("query_timeout_seconds"), bool)
        or not isinstance(budget.get("query_timeout_seconds"), (int, float))
        or not 0.2 <= float(budget["query_timeout_seconds"]) <= 30
        or isinstance(budget.get("max_identity_total_seconds"), bool)
        or not isinstance(budget.get("max_identity_total_seconds"), (int, float))
        or not 0.5 <= float(budget["max_identity_total_seconds"]) <= 120
        or isinstance(budget.get("retry_backoff_seconds"), bool)
        or not isinstance(budget.get("retry_backoff_seconds"), (int, float))
        or not 0 <= float(budget["retry_backoff_seconds"]) <= 60
    ):
        raise DataJobError(
            "RESET_BAOSTOCK_POLICY_INVALID",
            "清理预检的BaoStock来源预算超出有界合同；现有业务事实保持不变。",
            status=409,
        )

    if reset_already_completed:
        return _prepare_reset_resume_inputs_from_persisted_capture(
            config,
            plan,
            job_id=job_id,
            sessions=sessions,
            cutoff=cutoff,
            budget=budget,
        )

    target_text = sessions[-1].isoformat()
    try:
        identity = query_identity_reference(
            (target_text,),
            timeout_seconds=float(budget["query_timeout_seconds"]),
            max_total_seconds=float(budget["max_identity_total_seconds"]),
            config=config,
        )
    except Exception as error:
        raise DataJobError(
            "RESET_BAOSTOCK_IDENTITY_PREFLIGHT_FAILED",
            "清理前BaoStock身份与动态证券名录预检失败；现有业务事实保持不变。",
            status=503,
        ) from error
    calls = identity.get("calls") if isinstance(identity, Mapping) else None
    basic = calls.get("stock_basic") if isinstance(calls, Mapping) else None
    daily_lists = calls.get("all_stock") if isinstance(calls, Mapping) else None
    target_list = (
        daily_lists.get(target_text) if isinstance(daily_lists, Mapping) else None
    )

    def identity_stage_summary(value: Any) -> str:
        if not isinstance(value, Mapping):
            return "MISSING"
        return "/".join(
            (
                str(value.get("status", "UNKNOWN")),
                f"complete={value.get('complete')}",
                f"code={value.get('query_error_code')}",
                f"local={value.get('local_error')}",
                f"rows={value.get('rows_seen', len(value.get('rows', ())))}",
            )
        )

    if (
        not isinstance(identity, Mapping)
        or identity.get("status") != "RESPONSE_RECEIVED"
        or type(identity.get("verified_source_capture_id")) is not int
        or identity.get("verified_source_capture_id", 0) <= 0
        or identity.get("login_attempted") is not True
        or identity.get("logout_attempted") is not True
        or identity.get("logout_error_code") != "0"
        or not isinstance(basic, Mapping)
        or basic.get("status") != "RESPONSE_RECEIVED"
        or basic.get("complete") is not True
        or not isinstance(basic.get("response_fields"), list)
        or any(
            not isinstance(field, str) or not field
            for field in basic["response_fields"]
        )
        or len(set(basic["response_fields"])) != len(basic["response_fields"])
        or not {"code", "code_name", "ipoDate", "outDate", "type", "status"}.issubset(
            set(basic["response_fields"])
        )
        or not isinstance(basic.get("rows"), list)
        or not isinstance(target_list, Mapping)
        or target_list.get("status") != "RESPONSE_RECEIVED"
        or target_list.get("complete") is not True
        or not isinstance(target_list.get("response_fields"), list)
        or any(
            not isinstance(field, str) or not field
            for field in target_list["response_fields"]
        )
        or len(set(target_list["response_fields"]))
        != len(target_list["response_fields"])
        or not {"code", "code_name", "tradeStatus"}.issubset(
            set(target_list["response_fields"])
        )
        or not isinstance(target_list.get("rows"), list)
    ):
        identity_summary = (
            f"status={identity.get('status', 'UNKNOWN') if isinstance(identity, Mapping) else 'MISSING'}; "
            f"reason={str(identity.get('reason', ''))[:180] if isinstance(identity, Mapping) else ''}; "
            f"login={identity.get('login_error_code') if isinstance(identity, Mapping) else None}; "
            f"logout={identity.get('logout_error_code') if isinstance(identity, Mapping) else None}; "
            f"verified_capture={identity.get('verified_source_capture_id') if isinstance(identity, Mapping) else None}; "
            f"basic[{identity_stage_summary(basic)}]; "
            f"all_stock[{identity_stage_summary(target_list)}]"
        )
        raise DataJobError(
            "RESET_BAOSTOCK_IDENTITY_PREFLIGHT_INCOMPLETE",
            "BaoStock身份、基本资料或目标日名录未完整真实返回；"
            f"现有业务事实保持不变。{identity_summary}",
            status=503,
        )

    identity_completed_at = identity.get("request_completed_at")
    try:
        identity_completed = datetime.fromisoformat(identity_completed_at)
    except (TypeError, ValueError) as error:
        raise DataJobError(
            "RESET_BAOSTOCK_IDENTITY_CLOCK_INVALID",
            "BaoStock清理前身份目录响应时钟无效，原业务事实保留。",
            status=503,
        ) from error
    if identity_completed.tzinfo is None or identity_completed.utcoffset() is None:
        raise DataJobError(
            "RESET_BAOSTOCK_IDENTITY_CLOCK_INVALID",
            "BaoStock清理前身份目录响应时钟缺少时区，原业务事实保留。",
            status=503,
        )

    basic_by_symbol: dict[str, Mapping[str, Any]] = {}
    for row in basic["rows"]:
        if not isinstance(row, Mapping):
            raise DataJobError(
                "RESET_BAOSTOCK_BASIC_ROW_INVALID",
                "BaoStock基本资料行无效。",
                status=503,
            )
        symbol = normalize_baostock_symbol(row.get("code"))
        if symbol is None:
            raise DataJobError(
                "RESET_BAOSTOCK_BASIC_CODE_INVALID",
                "BaoStock基本资料含无法规范化的来源代码；不能静默缩小身份分母。",
                status=503,
            )
        if row.get("type") not in _BAOSTOCK_KNOWN_SECURITY_TYPES:
            raise DataJobError(
                "RESET_BAOSTOCK_SECURITY_TYPE_UNKNOWN",
                f"BaoStock基本资料证券{symbol}类型缺失或不在已登记值域；不能从来源分母静默排除。",
                status=503,
            )
        if symbol in basic_by_symbol:
            raise DataJobError(
                "RESET_BAOSTOCK_BASIC_DUPLICATE",
                "BaoStock基本资料代码重复。",
                status=503,
            )
        basic_by_symbol[symbol] = row

    target_symbols: set[str] = set()
    target_trade_status_by_symbol: dict[str, str] = {}
    for row in target_list["rows"]:
        if not isinstance(row, Mapping):
            raise DataJobError(
                "RESET_BAOSTOCK_TARGET_LIST_ROW_INVALID",
                "BaoStock目标日名录行无效。",
                status=503,
            )
        symbol = normalize_baostock_symbol(row.get("code"))
        if symbol is None:
            raise DataJobError(
                "RESET_BAOSTOCK_TARGET_CODE_INVALID",
                "BaoStock目标日名录含无法规范化的来源代码；不能静默缩小身份分母。",
                status=503,
            )
        if symbol in target_symbols:
            raise DataJobError(
                "RESET_BAOSTOCK_TARGET_LIST_DUPLICATE",
                "BaoStock目标日名录代码重复。",
                status=503,
            )
        target_symbols.add(symbol)
        trade_status = row.get("tradeStatus")
        if trade_status not in {"0", "1"}:
            raise DataJobError(
                "RESET_BAOSTOCK_TARGET_TRADE_STATUS_INVALID",
                f"BaoStock目标日名录证券{symbol}交易状态不是0/1。",
                status=503,
            )
        target_trade_status_by_symbol[symbol] = trade_status

    missing_basic_symbols = target_symbols - set(basic_by_symbol)
    if missing_basic_symbols:
        raise DataJobError(
            "RESET_BAOSTOCK_TARGET_BASIC_MISMATCH",
            "BaoStock目标日名录中的代码缺少对应基本资料："
            + ",".join(sorted(missing_basic_symbols)[:10]),
            status=503,
        )

    universe: list[str] = []
    basic_names: list[dict[str, str]] = []
    eligible_sessions_by_symbol: dict[str, tuple[date, ...]] = {}
    board_by_symbol: dict[str, str] = {}
    active_universe_at_target: set[str] = set()
    unsupported_board_symbols: list[str] = []
    source_type_stock_count = 0
    known_nonstock_count = 0
    for symbol, row in sorted(basic_by_symbol.items()):
        if row["type"] != "1":
            known_nonstock_count += 1
            continue
        source_type_stock_count += 1
        board_identity = classify_baostock_board(symbol)
        if board_identity is None:
            raise DataJobError(
                "RESET_BAOSTOCK_BOARD_IDENTITY_UNKNOWN",
                f"BaoStock证券{symbol}无法分类板别；现有业务事实保持不变。",
                status=503,
            )
        if board_identity[1] not in _BAOSTOCK_SOURCE_BOARDS:
            unsupported_board_symbols.append(symbol)
            continue
        try:
            ipo_date = parse_baostock_optional_date(row["ipoDate"])
            out_date = parse_baostock_optional_date(row.get("outDate"))
            if ipo_date is None:
                raise ValueError("ipoDate不得为空")
        except (KeyError, TypeError, ValueError) as error:
            raise DataJobError(
                "RESET_BAOSTOCK_LISTING_DATE_INVALID",
                f"BaoStock基本资料证券{symbol}的上市区间无效；现有业务事实保持不变。",
                status=503,
            ) from error
        if out_date is not None and ipo_date > out_date:
            raise DataJobError(
                "RESET_BAOSTOCK_LISTING_INTERVAL_CONFLICT",
                f"BaoStock基本资料证券{symbol}上市日晚于退市日；现有业务事实保持不变。",
                status=503,
            )
        eligible = tuple(
            session
            for session in sessions
            if session >= ipo_date and (out_date is None or session < out_date)
        )
        if not eligible:
            continue
        display_name = row.get("code_name")
        if not isinstance(display_name, str) or not display_name.strip():
            raise DataJobError(
                "RESET_BAOSTOCK_NAME_MISSING",
                f"BaoStock基本资料证券{symbol}缺少名称。",
                status=503,
            )
        universe.append(symbol)
        eligible_sessions_by_symbol[symbol] = eligible
        board_by_symbol[symbol] = board_identity[1]
        basic_names.append({"code": symbol.lower(), "code_name": display_name.strip()})
        if sessions[-1] >= ipo_date and (out_date is None or sessions[-1] <= out_date):
            active_universe_at_target.add(symbol)
    if not universe:
        raise DataJobError(
            "RESET_BAOSTOCK_UNIVERSE_EMPTY",
            "BaoStock类型、上市区间与目标日名录交集为空；现有业务事实保持不变。",
            status=503,
        )
    board_counts: dict[str, int] = {}
    for board in board_by_symbol.values():
        board_counts[board] = board_counts.get(board, 0) + 1
    identity_scope = {
        "basic_rows_received": len(basic["rows"]),
        "basic_codes_normalized": len(basic_by_symbol),
        "target_rows_received": len(target_list["rows"]),
        "target_codes_normalized": len(target_symbols),
        "source_type_stock_count": source_type_stock_count,
        "known_nonstock_count": known_nonstock_count,
        "source_candidate_count": len(universe),
        "unsupported_board_count": len(unsupported_board_symbols),
        "unsupported_board_symbols": tuple(unsupported_board_symbols),
        "board_counts": board_counts,
    }
    target_stock_symbols = {
        symbol
        for symbol in target_symbols
        if symbol in basic_by_symbol
        and basic_by_symbol[symbol].get("type") == "1"
        and classify_baostock_board(symbol) is not None
        and classify_baostock_board(symbol)[1] in _BAOSTOCK_SOURCE_BOARDS
    }
    if target_stock_symbols != active_universe_at_target:
        raise DataJobError(
            "RESET_BAOSTOCK_TARGET_UNIVERSE_MISMATCH",
            "BaoStock截止日all-stock名录与基本资料有效type=1上市证券总体不一致；现有业务事实保持不变。",
            status=503,
        )

    max_api_calls = int(budget["max_api_calls_per_batch"])
    max_login_batches = int(budget["max_login_batches"])
    identity_login_batches = int(identity.get("login_attempted") is True)
    identity_api_calls = 2
    probe_symbols = tuple(
        symbol
        for symbol in universe
        if symbol in active_universe_at_target
        and target_trade_status_by_symbol.get(symbol) == "1"
    )[: min(3, max_api_calls)]
    if not probe_symbols:
        raise DataJobError(
            "RESET_BAOSTOCK_PRICE_PROBE_UNAVAILABLE",
            "清理前没有可用于双坐标行情验证的目标日交易证券；现有业务事实保持不变。",
            status=503,
        )
    probe_login_batches = 2
    required_price_login_batches = 2 * (
        (len(universe) + max_api_calls - 1) // max_api_calls
    )
    planned_price_api_calls = 2 * len(universe)
    planned_total_api_calls = (
        identity_api_calls + 2 * len(probe_symbols) + planned_price_api_calls
    )
    planned_total_login_batches = (
        identity_login_batches + probe_login_batches + required_price_login_batches
    )
    effective_total_api_budget = max_login_batches * max_api_calls
    if (
        identity_login_batches != 1
        or planned_total_login_batches > max_login_batches
        or planned_total_api_calls > effective_total_api_budget
    ):
        raise DataJobError(
            "RESET_BAOSTOCK_REQUEST_BUDGET_EXCEEDED",
            "身份目录后完整RAW/QFQ捕获超出runtime请求预算，尚未清理原业务事实。",
            status=409,
        )
    try:
        probe = fetch_baostock_price_window(
            probe_symbols,
            (sessions[-1],),
            cutoff=cutoff,
            units=documented_units,
            max_api_calls_per_batch=max_api_calls,
            max_login_batches=probe_login_batches,
            query_timeout_seconds=float(budget["query_timeout_seconds"]),
            max_history_session_count=int(budget["max_history_session_count"]),
            max_retry_attempts=0,
            retry_backoff_seconds=0,
            config=config,
        )
    except BaoStockDailyJobError as error:
        raise DataJobError(
            "RESET_BAOSTOCK_PRICE_PREFLIGHT_FAILED",
            "清理前BaoStock双坐标行情验证异常；现有业务事实保持不变。",
            status=503,
        ) from error
    valid_probe = any(
        record.raw_observation is not None
        and record.qfq_observation is not None
        and all(
            getattr(observation, field).quality == "OBSERVED"
            for observation in (record.raw_observation, record.qfq_observation)
            for field in ("open", "high", "low", "close", "volume", "amount")
        )
        for record in probe.get("records", ())
    )
    if (
        probe.get("status") != "COMPLETED"
        or probe.get("source_network_called") is not True
        or not valid_probe
    ):
        raise DataJobError(
            "RESET_BAOSTOCK_PRICE_PREFLIGHT_FAILED",
            "清理前未能验证BaoStock目标日RAW/QFQ价格和量额字段；现有业务事实保持不变。",
            status=503,
        )
    preflight_login_batches = int(probe.get("login_budget_batches", 0))
    price_preflight = {
        "mode": "IDENTITY_AND_PRICE_PROBE",
        "status": "IDENTITY_AND_PRICE_PROBE_COMPLETE",
        "completion_status": "IDENTITY_AND_PRICE_PROBE_COMPLETE",
        "identity_requests_performed": True,
        "price_requests_performed": True,
        "source_network_called": True,
        "requested_symbols": len(universe),
        "target_sessions": [session.isoformat() for session in sessions],
        "price_api_calls": int(probe.get("api_calls", 0)),
        "planned_price_api_calls": planned_price_api_calls,
        "identity_api_calls": identity_api_calls,
        "planned_total_api_calls": planned_total_api_calls,
        "identity_login_batches": identity_login_batches,
        "price_login_batches": preflight_login_batches,
        "planned_initial_price_login_batches": required_price_login_batches,
        "planned_total_login_batches": planned_total_login_batches,
        "max_login_batches": max_login_batches,
        "max_api_calls_per_batch": max_api_calls,
        "effective_total_api_budget": effective_total_api_budget,
        "effective_price_retry_attempts": 0,
        "full_capture_deferred_until_after_clear": True,
        "source_requests_repeated_after_clear": True,
    }
    try:
        remote_config = dict(config)
        remote_policy = dict(source_policy)
        remote_policy["configured_tdx_source_roots"] = []
        remote_config["source_policy"] = remote_policy
        remote = fetch_remote_sector_files(remote_config)
        validate_remote_sector_files(remote)
        inputs = remote_sector_source_inputs(host=remote.host, files=remote.files)
        parse_outcome = parse_remote_sector_source_inputs_with_status(
            inputs,
            observed_at=datetime.now(ZoneInfo("Asia/Shanghai")),
        )
        snapshots = parse_outcome.snapshots
        if parse_outcome.rejected_sources or {
            item.source_kind for item in snapshots
        } != {"TDX_TDXHY_CFG", "TDX_INFOHARBOR_BLOCK_DAT"}:
            raise ValueError("行业与GN概念来源未同时闭合")
        _validate_limits(snapshots)
    except Exception as error:
        raise DataJobError(
            "RESET_SECTOR_SOURCE_PREFLIGHT_FAILED",
            "清理前无法在线只读验证完整行业与GN概念成员来源；现有业务事实保持不变。",
            status=503,
        ) from error
    sector_file_fingerprint = {
        relative: {
            "size_bytes": evidence.size_bytes,
            "sha256": hashlib.sha256(evidence.raw).hexdigest(),
        }
        for relative, evidence in sorted(inputs.files.items())
    }
    capture_material = {
        "start": sessions[0].isoformat(),
        "end": sessions[-1].isoformat(),
        "symbols": list(universe),
    }
    capture_digest = hashlib.sha256(
        json.dumps(
            capture_material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    capture_uuid = uuid5(
        NAMESPACE_URL,
        f"stock-a:baostock:paired-capture:{capture_digest}",
    )
    fingerprint_material = {
        "contract": "STOCK_A_RESET_BAOSTOCK_IDENTITY_PREFLIGHT_R1",
        "target_sessions": [session.isoformat() for session in sessions],
        "source_symbols": universe,
        "board_counts": board_counts,
        "identity_scope": identity_scope,
        "eligible_sessions_by_symbol": {
            symbol: [session.isoformat() for session in eligible]
            for symbol, eligible in sorted(eligible_sessions_by_symbol.items())
        },
        "identity_target_session": target_text,
        "capture_uuid": str(capture_uuid),
        "identity_api_calls": price_preflight["identity_api_calls"],
        "planned_price_api_calls": price_preflight["planned_price_api_calls"],
        "planned_total_api_calls": price_preflight["planned_total_api_calls"],
        "planned_total_login_batches": price_preflight["planned_total_login_batches"],
        "sector_parse_contract": snapshots[0].parse_contract_version,
        "sector_sources": sector_file_fingerprint,
    }
    preflight_fingerprint = hashlib.sha256(
        json.dumps(
            fingerprint_material,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if (
        expected_preflight_fingerprint is not None
        and preflight_fingerprint != expected_preflight_fingerprint
    ):
        raise DataJobError(
            "RESET_INPUT_IDENTITY_CHANGED",
            "恢复任务时BaoStock身份股票池、有效上市窗或TDX成员来源已改变，拒绝使用旧预览。",
            status=409,
        )
    return {
        "identity_snapshot": identity,
        "source_symbols": tuple(universe),
        "eligible_sessions_by_symbol": eligible_sessions_by_symbol,
        "board_by_symbol": board_by_symbol,
        "board_counts": board_counts,
        "identity_scope": identity_scope,
        "basic_names": tuple(basic_names),
        "price_preflight": price_preflight,
        "capture_uuid": capture_uuid,
        "price_preflight_symbols": tuple(universe),
        "preflight_login_batches": preflight_login_batches,
        "price_preflight_sessions": sessions,
        "sector_snapshots": tuple(snapshots),
        "sector_source_root": inputs.root,
        "sector_source_fingerprint": sector_file_fingerprint,
        "preflight_fingerprint": preflight_fingerprint,
        "preflight_status": "COMPLETE_BEFORE_DELETE",
        "source_request_performed": True,
    }


def _execute_derived_cleanup(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    begin_callback: Any,
) -> dict[str, Any]:
    module = next(iter(plan["base_modules"] + plan["research_modules"]))
    targets = tuple(
        date.fromisoformat(item)
        for item in plan.get("module_output_sessions", {}).get(module, ())
    )
    preview = plan.get("cleanup_preview")
    if not isinstance(preview, Mapping) or preview.get("complete") is not True:
        raise DataJobError(
            "DATA_JOB_CLEANUP_PREVIEW_REQUIRED",
            "精确清理预检缺失或不完整；未执行清理。",
            status=409,
        )
    affected_tables = set(preview.get("affected_tables", ()))
    progress_tables = set(preview.get("progress_tables") or affected_tables)
    target_set = set(targets)
    units = [
        unit
        for unit in plan["stage_units"]
        if unit["target_session"] in target_set
        and unit["business_table"] in progress_tables
    ]
    if (
        not targets
        or not progress_tables
        or len(units) != len(targets) * len(progress_tables)
    ):
        raise DataJobError(
            "DATA_JOB_CLEANUP_PLAN_MISMATCH",
            "清理预览与当前任务进度单元不一致；未执行清理。",
            status=409,
        )
    begin_callback(job_id=job_id, units=units)
    result = data_job_pg.clear_derived_module(
        config,
        module,
        targets,
        date.fromisoformat(plan["research_cutoff"]),
        expected_preview_key=preview.get("preview_key"),
        current_job_id=job_id,
        member_version=preview.get("member_version_filter"),
    )
    deleted = result.get("deleted_rows_by_session", {})
    versions_by_session: dict[str, list[dict[str, Any]]] = {}
    for ver in result.get("versions", ()):
        v_session = str(ver.get("target_session", ""))
        v_key = ver.get("version_key")
        if v_session and isinstance(v_key, Mapping):
            versions_by_session.setdefault(v_session, []).append(dict(v_key))
    for unit in units:
        session = unit["target_session"].isoformat()
        table = unit["business_table"]
        deleted_rows = int(deleted.get(session, {}).get(table, 0))
        data_job_pg.mark_progress_cleared(
            config,
            job_id=job_id,
            stage_code=unit["stage_code"],
            target_session=unit["target_session"],
            business_table=table,
            deleted_rows=deleted_rows,
            version_keys=versions_by_session.get(session),
        )
    return result


def _persist_sector_membership_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    progress_callback: Any,
    begin_callback: Any,
    prepared_snapshots: Sequence[Any] | Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if "TDX_SECTOR_MEMBERS" not in set(plan.get("source_modules", ())):
        return None
    units = [
        unit
        for unit in plan.get("stage_units", ())
        if unit.get("stage_code") == "SOURCE_SECTOR_MEMBERS"
    ]
    if not units:
        return None
    begin_callback(job_id=job_id, units=units)
    from stock_a.application.tdx_sector_members import (
        read_tdx_sector_source_snapshot_catalog,
        refresh_remote_tdx_sector_members,
    )
    from stock_a.storage.postgres.tdx_sector_members import (
        persist_sector_source_snapshots,
    )

    if prepared_snapshots is None:
        result = refresh_remote_tdx_sector_members(dict(config), validate_only=False)
    elif isinstance(prepared_snapshots, Mapping) and isinstance(
        prepared_snapshots.get("reuse_existing_snapshot_ids"), Mapping
    ):
        snapshot_ids = prepared_snapshots["reuse_existing_snapshot_ids"]
        if (
            set(snapshot_ids) != {"INDUSTRY", "CONCEPT"}
            or any(
                type(value) is not int or value <= 0 for value in snapshot_ids.values()
            )
            or snapshot_ids["INDUSTRY"] == snapshot_ids["CONCEPT"]
        ):
            raise DataJobError(
                "SECTOR_REUSE_SNAPSHOT_IDS_INVALID",
                "复用行业/GN来源快照必须提供一对不同的正整数PG身份。",
                status=409,
            )
        catalog = read_tdx_sector_source_snapshot_catalog(dict(config))
        catalog_by_id = {
            int(item["snapshot_id"]): item
            for item in catalog.get("snapshots", ())
            if isinstance(item, Mapping) and type(item.get("snapshot_id")) is int
        }
        source_kind = {
            "INDUSTRY": "TDX_TDXHY_CFG",
            "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
        }
        snapshots = []
        independent_readback: dict[str, dict[str, int]] = {}
        for kind in ("INDUSTRY", "CONCEPT"):
            snapshot = catalog_by_id.get(snapshot_ids[kind])
            if (
                not isinstance(snapshot, Mapping)
                or snapshot.get("source_kind") != source_kind[kind]
                or int(snapshot.get("catalog_rows", 0)) <= 0
                or int(snapshot.get("member_rows", 0)) <= 0
                or int(snapshot.get("invalid_membership_quality_rows", 0)) != 0
            ):
                raise DataJobError(
                    "SECTOR_REUSE_SNAPSHOT_READBACK_INCOMPLETE",
                    f"复用的{kind}行业/GN快照未通过独立PG目录与成员行读回。",
                    status=409,
                )
            snapshots.append(dict(snapshot))
            independent_readback[str(snapshot_ids[kind])] = {
                "catalog_rows": int(snapshot["catalog_rows"]),
                "member_rows": int(snapshot["member_rows"]),
            }
        result = {
            "status": "IDEMPOTENT_ZERO_WRITE",
            "snapshots": snapshots,
            "source_root": "PG_EXISTING_TDX_SOURCE_SNAPSHOTS",
            "source_modified": False,
            "source_downloaded": False,
            "source_files_persisted_to_disk": False,
            "tdx_install_modified": False,
            "write_summary": {
                "status": "IDEMPOTENT_READBACK",
                "independent_readback": independent_readback,
            },
        }
    else:
        persisted = persist_sector_source_snapshots(dict(config), prepared_snapshots)
        result = {
            **persisted,
            "source_root": "TDX_REMOTE_PREVALIDATED_IN_MEMORY",
            "source_modified": False,
            "source_downloaded": True,
            "source_files_persisted_to_disk": False,
            "tdx_install_modified": False,
        }
    snapshots = result.get("snapshots", ())
    snapshot_ids = {
        "INDUSTRY": next(
            (
                int(item["snapshot_id"])
                for item in snapshots
                if item.get("source_kind") == "TDX_TDXHY_CFG"
            ),
            None,
        ),
        "CONCEPT": next(
            (
                int(item["snapshot_id"])
                for item in snapshots
                if item.get("source_kind") == "TDX_INFOHARBOR_BLOCK_DAT"
            ),
            None,
        ),
    }
    if any(value is None for value in snapshot_ids.values()):
        raise DataJobError(
            "SECTOR_SOURCE_SNAPSHOT_PAIR_INCOMPLETE",
            "行业与GN概念成员源快照没有在同一生产批次中闭合。",
            status=409,
        )
    if isinstance(plan, dict):
        old_sector_reason = plan.get("sector_snapshot_pair", {}).get("reason_code")
        plan["sector_snapshot_ids"] = snapshot_ids
        plan["sector_snapshot_pair"] = {
            "status": "AVAILABLE",
            "snapshot_ids": snapshot_ids,
            "source_kinds": {
                "INDUSTRY": "TDX_TDXHY_CFG",
                "CONCEPT": "TDX_INFOHARBOR_BLOCK_DAT",
            },
            "research_mode": "NON_PIT_RETROSPECTIVE_CURRENT_SNAPSHOT",
        }
        for unit in plan.get("stage_units", ()):
            if (
                unit.get("stage_code") in {"SECTOR_PULSE", "SECTOR_TRANSITION"}
                and unit.get("status") == "NOT_COMPUTED"
                and (
                    unit.get("error_code") == old_sector_reason
                    or str(unit.get("error_code") or "").startswith("SECTOR_SNAPSHOT")
                )
            ):
                unit.update(
                    {"status": "NOT_STARTED", "error_code": None, "error_message": None}
                )
        plan["unplanned_producers"] = [
            item
            for item in plan.get("unplanned_producers", ())
            if item.get("stage_code") != "SECTOR_PULSE"
        ]
    write_summary = result.get("write_summary", {})
    if not isinstance(write_summary, Mapping):
        write_summary = {}
    readback = result.get(
        "independent_readback",
        write_summary.get("independent_readback", {}),
    )
    if not isinstance(readback, Mapping):
        raise DataJobError(
            "SECTOR_SOURCE_READBACK_UNAVAILABLE",
            "行业与GN概念成员源缺少独立PG读回。",
            status=409,
        )
    catalog_rows = 0
    member_rows = 0
    for snapshot_id in snapshot_ids.values():
        counts = readback.get(str(snapshot_id), {})
        if not isinstance(counts, Mapping):
            raise DataJobError(
                "SECTOR_SOURCE_READBACK_INCOMPLETE",
                "行业或GN概念成员源缺少精确快照读回计数。",
                status=409,
            )
        catalog_rows += int(counts.get("catalog_rows", 0))
        member_rows += int(counts.get("member_rows", 0))
    table_counts = {
        "stock.tdx_sector_source_snapshots": len(snapshot_ids),
        "stock.tdx_sector_catalog": catalog_rows,
        "stock.tdx_sector_members": member_rows,
    }
    persistence = write_summary or result
    for unit in units:
        table = unit["business_table"]
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_SECTOR_MEMBERS",
            target_session=unit["target_session"],
            business_table=table,
            source_capture_id=None,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=table_counts[table],
            inserted_rows=int(
                persistence.get(
                    {
                        "stock.tdx_sector_source_snapshots": "inserted_snapshot_rows",
                        "stock.tdx_sector_catalog": "inserted_catalog_rows",
                        "stock.tdx_sector_members": "inserted_member_rows",
                    }[table],
                    0,
                )
            ),
            quality_status="COMPLETE_NON_PIT_CURRENT_MEMBERSHIP",
            no_change=(
                persistence.get("status") == "IDEMPOTENT_READBACK"
                or result.get("status") in {"IDEMPOTENT_ZERO_WRITE", "VALIDATED_ONLY"}
            ),
            producer_version="STOCK_A_TDX_SECTOR_SOURCE_MEMBERS_R1",
            sector_snapshot_ids=snapshot_ids,
        )
    return {**result, "sector_snapshot_ids": snapshot_ids}


def _baostock_universe_from_identity(
    identity: Mapping[str, Any],
    sessions: Sequence[date],
) -> dict[str, Any]:
    """按一次BaoStock基本资料与目标日名录构造动态股票有效窗口。"""
    from stock_a.sources.baostock.identity import (
        classify_baostock_board,
        normalize_baostock_symbol,
        parse_baostock_optional_date,
    )

    if not sessions:
        raise DataJobError(
            "BAOSTOCK_SESSION_WINDOW_EMPTY", "BaoStock股票池缺少官方会话范围。"
        )
    target_text = sessions[-1].isoformat()
    calls = identity.get("calls")
    basic = calls.get("stock_basic") if isinstance(calls, Mapping) else None
    all_stock = calls.get("all_stock") if isinstance(calls, Mapping) else None
    target_list = all_stock.get(target_text) if isinstance(all_stock, Mapping) else None
    if (
        identity.get("status") != "RESPONSE_RECEIVED"
        or type(identity.get("verified_source_capture_id")) is not int
        or identity.get("verified_source_capture_id", 0) <= 0
        or identity.get("login_attempted") is not True
        or identity.get("logout_attempted") is not True
        or identity.get("logout_error_code") != "0"
        or not isinstance(basic, Mapping)
        or basic.get("status") != "RESPONSE_RECEIVED"
        or basic.get("complete") is not True
        or not isinstance(basic.get("response_fields"), list)
        or any(
            not isinstance(field, str) or not field
            for field in basic["response_fields"]
        )
        or len(set(basic["response_fields"])) != len(basic["response_fields"])
        or not {"code", "code_name", "ipoDate", "outDate", "type", "status"}.issubset(
            set(basic["response_fields"])
        )
        or not isinstance(basic.get("rows"), list)
        or not isinstance(target_list, Mapping)
        or target_list.get("status") != "RESPONSE_RECEIVED"
        or target_list.get("complete") is not True
        or not isinstance(target_list.get("response_fields"), list)
        or any(
            not isinstance(field, str) or not field
            for field in target_list["response_fields"]
        )
        or len(set(target_list["response_fields"]))
        != len(target_list["response_fields"])
        or not {"code", "code_name"}.issubset(set(target_list["response_fields"]))
        or not isinstance(target_list.get("rows"), list)
    ):
        raise DataJobError(
            "BAOSTOCK_IDENTITY_UNIVERSE_INCOMPLETE",
            "BaoStock基本资料或目标日all-stock响应未通过完整性核验。",
            status=503,
        )
    basic_by_symbol: dict[str, Mapping[str, Any]] = {}
    for row in basic["rows"]:
        if not isinstance(row, Mapping):
            raise DataJobError(
                "BAOSTOCK_BASIC_ROW_INVALID", "BaoStock基本资料包含非法行。", status=503
            )
        symbol = normalize_baostock_symbol(row.get("code"))
        if symbol is None:
            raise DataJobError(
                "BAOSTOCK_BASIC_CODE_INVALID",
                "BaoStock基本资料含无法规范化的来源代码；不能静默缩小身份分母。",
                status=503,
            )
        if row.get("type") not in _BAOSTOCK_KNOWN_SECURITY_TYPES:
            raise DataJobError(
                "BAOSTOCK_SECURITY_TYPE_UNKNOWN",
                f"BaoStock基本资料证券{symbol}类型缺失或不在已登记值域；不能从来源分母静默排除。",
                status=503,
            )
        if symbol in basic_by_symbol:
            raise DataJobError(
                "BAOSTOCK_BASIC_DUPLICATE", "BaoStock基本资料证券代码重复。", status=503
            )
        basic_by_symbol[symbol] = row
    target_symbols: set[str] = set()
    for row in target_list["rows"]:
        if not isinstance(row, Mapping):
            raise DataJobError(
                "BAOSTOCK_TARGET_LIST_ROW_INVALID",
                "BaoStock目标日名录包含非法行。",
                status=503,
            )
        symbol = normalize_baostock_symbol(row.get("code"))
        if symbol is None:
            raise DataJobError(
                "BAOSTOCK_TARGET_CODE_INVALID",
                "BaoStock目标日名录含无法规范化的来源代码；不能静默缩小身份分母。",
                status=503,
            )
        if symbol in target_symbols:
            raise DataJobError(
                "BAOSTOCK_TARGET_LIST_DUPLICATE",
                "BaoStock目标日名录证券代码重复。",
                status=503,
            )
        target_symbols.add(symbol)

    missing_basic_symbols = target_symbols - set(basic_by_symbol)
    if missing_basic_symbols:
        raise DataJobError(
            "BAOSTOCK_TARGET_BASIC_MISMATCH",
            "BaoStock目标日名录中的代码缺少对应基本资料："
            + ",".join(sorted(missing_basic_symbols)[:10]),
            status=503,
        )

    symbols: list[str] = []
    eligible: dict[str, tuple[date, ...]] = {}
    boards: dict[str, str] = {}
    names: list[dict[str, str]] = []
    active_at_target: set[str] = set()
    unsupported_board_symbols: list[str] = []
    source_type_stock_count = 0
    known_nonstock_count = 0
    for symbol, row in sorted(basic_by_symbol.items()):
        if row["type"] != "1":
            known_nonstock_count += 1
            continue
        source_type_stock_count += 1
        board_identity = classify_baostock_board(symbol)
        if board_identity is None:
            raise DataJobError(
                "BAOSTOCK_BOARD_IDENTITY_UNKNOWN",
                f"BaoStock证券{symbol}无法分类板别；不能静默缩小来源分母。",
                status=503,
            )
        if board_identity[1] not in _BAOSTOCK_SOURCE_BOARDS:
            unsupported_board_symbols.append(symbol)
            continue
        try:
            ipo_date = parse_baostock_optional_date(row["ipoDate"])
            out_date = parse_baostock_optional_date(row.get("outDate"))
            if ipo_date is None:
                raise ValueError("ipoDate不得为空")
        except (KeyError, TypeError, ValueError) as error:
            raise DataJobError(
                "BAOSTOCK_LISTING_DATE_INVALID",
                f"BaoStock证券{symbol}上市区间日期无效。",
                status=503,
            ) from error
        if out_date is not None and ipo_date > out_date:
            raise DataJobError(
                "BAOSTOCK_LISTING_INTERVAL_CONFLICT",
                f"BaoStock证券{symbol}上市日晚于退市日。",
                status=503,
            )
        window = tuple(
            day
            for day in sessions
            if day >= ipo_date and (out_date is None or day <= out_date)
        )
        if not window:
            continue
        name = row.get("code_name")
        if not isinstance(name, str) or not name.strip():
            raise DataJobError(
                "BAOSTOCK_BASIC_NAME_MISSING",
                f"BaoStock证券{symbol}缺少code_name。",
                status=503,
            )
        symbols.append(symbol)
        eligible[symbol] = window
        boards[symbol] = board_identity[1]
        names.append({"code": symbol.lower(), "code_name": name.strip()})
        if sessions[-1] in window:
            active_at_target.add(symbol)
    target_stock_symbols = {
        symbol
        for symbol in target_symbols
        if symbol in basic_by_symbol
        and basic_by_symbol[symbol]["type"] == "1"
        and classify_baostock_board(symbol) is not None
        and classify_baostock_board(symbol)[1] in _BAOSTOCK_SOURCE_BOARDS
    }
    if not symbols or target_stock_symbols != active_at_target:
        raise DataJobError(
            "BAOSTOCK_TARGET_LIST_CROSSCHECK_FAILED",
            "BaoStock截止日all-stock名录与基本资料有效type=1上市证券总体不一致。",
            status=503,
        )
    board_counts: dict[str, int] = {}
    for board in boards.values():
        board_counts[board] = board_counts.get(board, 0) + 1
    return {
        "source_symbols": tuple(symbols),
        "eligible_sessions_by_symbol": eligible,
        "board_by_symbol": boards,
        "board_counts": board_counts,
        "basic_names": tuple(names),
        "target_symbols": target_symbols,
        "identity_scope": {
            "basic_rows_received": len(basic["rows"]),
            "basic_codes_normalized": len(basic_by_symbol),
            "target_rows_received": len(target_list["rows"]),
            "target_codes_normalized": len(target_symbols),
            "source_type_stock_count": source_type_stock_count,
            "known_nonstock_count": known_nonstock_count,
            "source_candidate_count": len(symbols),
            "unsupported_board_count": len(unsupported_board_symbols),
            "unsupported_board_symbols": tuple(unsupported_board_symbols),
            "board_counts": board_counts,
        },
        "identity_snapshot": identity,
    }


def _allocate_baostock_aux_capture(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_kind: str,
    range_start_session: date,
    range_end_session: date,
    requested_symbol_count: int,
    parser_contract: str,
) -> int:
    """为BaoStock主资料/名录或参考指数来源阶段幂等分配真实source_capture_id。"""
    from datetime import datetime, timezone
    from uuid import NAMESPACE_URL, uuid5

    from stock_a.sources.baostock.evidence import _UNITS as documented_units
    from stock_a.storage.postgres.source_raw import _transaction as _raw_tx

    capture_uuid = uuid5(
        NAMESPACE_URL,
        f"stock_a:{stage_kind}:{job_id}:{range_start_session.isoformat()}:{range_end_session.isoformat()}",
    )
    started = datetime.now(timezone.utc)
    with _raw_tx(config, write=True, statement_timeout_ms=30000) as (_, cursor, _):
        cursor.execute(
            """SELECT capture_id FROM stock.source_captures
               WHERE capture_uuid = %s""",
            (capture_uuid,),
        )
        existing = cursor.fetchone()
        if existing is not None:
            return int(existing[0])
        cursor.execute(
            """INSERT INTO stock.source_captures
                 (provider, capture_uuid, range_start_session, range_end_session,
                  price_coordinates, parser_contract, read_started_at, validated_at,
                  source_available_at, source_time_quality, source_clock_role,
                  normalized_count, requested_symbol_count, unit_contract_version)
               VALUES ('BAOSTOCK', %s, %s, %s, %s, %s, %s, %s, NULL,
                       'NON_PIT_OBSERVED', %s, 0, %s, %s)
               ON CONFLICT (capture_uuid) WHERE capture_uuid IS NOT NULL DO NOTHING
               RETURNING capture_id""",
            (
                capture_uuid,
                range_start_session,
                range_end_session,
                ["RAW_UNADJUSTED"],
                parser_contract,
                started,
                started,
                f"BAOSTOCK_{stage_kind}_OBSERVED_AT_CAPTURE",
                max(1, int(requested_symbol_count)),
                documented_units.version,
            ),
        )
        row = cursor.fetchone()
        if row is not None:
            return int(row[0])
        cursor.execute(
            "SELECT capture_id FROM stock.source_captures WHERE capture_uuid = %s",
            (capture_uuid,),
        )
        readback = cursor.fetchone()
        if readback is None:
            raise DataJobError(
                "BAOSTOCK_AUX_CAPTURE_ALLOCATION_FAILED",
                f"{stage_kind} 来源捕获记录写入后无法读回。",
                status=500,
            )
        return int(readback[0])


def _selected_source_modules(plan: Mapping[str, Any]) -> set[str]:
    """解析Coordinator现役扁平source_modules及旧阶段测试/适配器嵌套选择格式。"""
    source_modules = plan.get("source_modules")
    if isinstance(source_modules, Sequence) and not isinstance(
        source_modules, (str, bytes)
    ):
        return {value for value in source_modules if isinstance(value, str)}
    selected = plan.get("selected_modules", ())
    if isinstance(selected, Mapping):
        nested = selected.get("source", ())
        if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes)):
            return {value for value in nested if isinstance(value, str)}
    if isinstance(selected, Sequence) and not isinstance(selected, (str, bytes)):
        return set(selected).intersection(_ALL_SOURCE_MODULES)
    return set()


def _execute_baostock_universe_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    official_sessions: Sequence[date],
    progress_callback: Any,
    begin_callback: Any,
) -> dict[str, Any] | None:
    """采集并持久化BaoStock证券基本资料与逐日全证券在市名录，独立完成不随研究失败回滚。"""
    from stock_a.sources.baostock.client import (
        query_session_directory_rows,
        query_stock_basic_catalog,
    )
    from stock_a.storage.postgres.baostock_universe import (
        persist_baostock_session_directory,
        persist_baostock_stock_basic,
        read_baostock_session_directory_scope,
        read_baostock_stock_basic_scope,
    )

    selected_sources = _selected_source_modules(plan)
    if "BAOSTOCK_UNIVERSE" not in selected_sources:
        return None
    stage_units = [
        unit
        for unit in plan.get("stage_units", ())
        if unit.get("stage_code") == "SOURCE_BAOSTOCK_UNIVERSE"
        and unit.get("status") == "NOT_STARTED"
    ]
    if not stage_units:
        return None

    target_day = date.fromisoformat(plan["target_session"])
    cutoff = date.fromisoformat(plan["research_cutoff"])
    eligible_sessions = [d for d in official_sessions if d <= min(target_day, cutoff)]
    if not eligible_sessions:
        raise DataJobError(
            "BAOSTOCK_UNIVERSE_SESSION_EMPTY",
            "BaoStock证券主资料与逐日名录没有合法官方会话范围。",
            status=409,
        )
    if target_day not in eligible_sessions:
        raise DataJobError(
            "BAOSTOCK_UNIVERSE_TARGET_NOT_OFFICIAL",
            "逐日名录目标日必须是截止内官方交易会话",
            status=409,
        )
    raw_window = [
        date.fromisoformat(d) if isinstance(d, str) else d
        for d in plan.get("raw_window", ())
    ]
    if raw_window:
        approved_end = min(target_day, cutoff)
        if (
            tuple(sorted(set(raw_window))) != tuple(raw_window)
            or any(d not in official_sessions or d > approved_end for d in raw_window)
            or target_day not in raw_window
        ):
            raise DataJobError(
                "BAOSTOCK_DIRECTORY_WINDOW_OUTSIDE_AUTHORITY",
                "逐日名录窗口必须是目标日及其之前、截止内的唯一官方会话范围",
                status=409,
            )
        dir_sessions = tuple(raw_window)
    else:
        # 来源任务没有显式历史窗口时只取目标会话，不自行扩展预热范围。
        dir_sessions = (target_day,)

    transport = plan.get("_baostock_transport")
    all_universe_units = [
        u
        for u in plan.get("stage_units", ())
        if u.get("stage_code") == "SOURCE_BAOSTOCK_UNIVERSE"
    ]
    reused_capture_id = next(
        (
            int(u["source_capture_id"])
            for u in all_universe_units
            if u.get("status") == "COMPLETED" and u.get("source_capture_id") is not None
        ),
        None,
    )
    capture_id = (
        reused_capture_id
        if reused_capture_id is not None
        else _allocate_baostock_aux_capture(
            config,
            job_id=job_id,
            stage_kind="BAOSTOCK_UNIVERSE",
            range_start_session=dir_sessions[0],
            range_end_session=dir_sessions[-1],
            requested_symbol_count=len(dir_sessions),
            parser_contract="BAOSTOCK_UNIVERSE_BASIC_AND_DIRECTORY_R1",
        )
    )
    begin_callback(job_id=job_id, units=stage_units)
    basic_unit = next(
        (
            u
            for u in stage_units
            if u["business_table"] == "stock.source_baostock_stock_basic"
        ),
        None,
    )
    dir_unit = next(
        (
            u
            for u in stage_units
            if u["business_table"] == "stock.source_baostock_session_directory"
        ),
        None,
    )

    if basic_unit is not None:
        basic_res = query_stock_basic_catalog(
            config=config,
            transport=transport,
            enforce_quota=True,
        )
        if (
            not basic_res.get("complete")
            or basic_res.get("status") != "RESPONSE_RECEIVED"
        ):
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_FETCH_FAILED",
                f"BaoStock证券基本资料采集未完成: {basic_res.get('status')} ({basic_res.get('reason')})",
                status=502,
            )
        expected_basic_symbols = _validate_basic_catalog_response(basic_res.get("rows"))
        basic_write = persist_baostock_stock_basic(
            config,
            source_capture_id=capture_id,
            rows=basic_res.get("rows", ()),
        )
        actual_basic_rows = read_baostock_stock_basic_scope(
            config, source_capture_id=capture_id, cutoff=cutoff
        )
        actual_basic_symbols = tuple(
            item["source_symbol"] for item in actual_basic_rows
        )
        if actual_basic_symbols != expected_basic_symbols:
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_READBACK_MISMATCH",
                "证券基本资料PG读回代码集合与本次规范化响应不一致",
                status=409,
            )
        actual_basic = data_job_pg.read_business_table_count(
            config,
            business_table="stock.source_baostock_stock_basic",
            target_session=target_day,
            source_capture_id=capture_id,
            calendar_revision=plan.get("calendar_revision") or "",
            producer_version="BAOSTOCK_STOCK_BASIC_R1",
            research_cutoff=cutoff,
        )
        if actual_basic != len(expected_basic_symbols):
            raise DataJobError(
                "BAOSTOCK_STOCK_BASIC_READBACK_COUNT_MISMATCH",
                "证券基本资料独立读回总数与本次响应独立预期不一致",
                status=409,
            )
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_BAOSTOCK_UNIVERSE",
            target_session=target_day,
            business_table="stock.source_baostock_stock_basic",
            source_capture_id=capture_id,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=len(expected_basic_symbols),
            inserted_rows=int(basic_write["row_count"]),
            quality_status="NON_PIT_OBSERVED",
            no_change=basic_write.get("status") == "NO_CHANGE",
            producer_version="BAOSTOCK_STOCK_BASIC_R1",
            calendar_revision=plan.get("calendar_revision") or "",
        )
    else:
        actual_basic_rows = read_baostock_stock_basic_scope(
            config, source_capture_id=capture_id, cutoff=cutoff
        )

    if dir_unit is None:
        return {
            "source_capture_id": capture_id,
            "basic_row_count": len(actual_basic_rows),
            "directory_row_count": 0,
        }

    dir_res = query_session_directory_rows(
        session_dates=tuple(d.isoformat() for d in dir_sessions),
        config=config,
        transport=transport,
        enforce_quota=True,
    )
    if not dir_res.get("complete") or dir_res.get("status") != "RESPONSE_RECEIVED":
        raise DataJobError(
            "BAOSTOCK_SESSION_DIRECTORY_FETCH_FAILED",
            f"BaoStock逐日名录采集未完成: {dir_res.get('status')} ({dir_res.get('reason')})",
            status=502,
        )
    expected_directory_members = _validate_session_directory_response(
        dir_res.get("rows_by_session"), requested_sessions=dir_sessions
    )
    total_dir_rows = 0
    all_dir_no_change = True
    rows_by_day = dir_res.get("rows_by_session", {})
    for sess in dir_sessions:
        day_rows = rows_by_day.get(sess.isoformat(), ())
        write_res = persist_baostock_session_directory(
            config,
            source_capture_id=capture_id,
            session_date=sess,
            rows=day_rows,
        )
        expected_members = expected_directory_members[sess]
        expected_symbols = {symbol for symbol, _ in expected_members}
        actual_members = read_baostock_session_directory_scope(
            config,
            source_capture_id=capture_id,
            session_date=sess,
            cutoff=cutoff,
        )
        actual_member_status = {
            (item["source_symbol"], item["trade_status"]) for item in actual_members
        }
        if actual_member_status != set(expected_members):
            raise DataJobError(
                "BAOSTOCK_SESSION_DIRECTORY_READBACK_MISMATCH",
                f"{sess.isoformat()}名录PG成员/状态集合与规范化响应不一致",
                status=409,
            )
        if len(actual_members) != len(expected_symbols):
            raise DataJobError(
                "BAOSTOCK_SESSION_DIRECTORY_READBACK_COUNT_MISMATCH",
                f"{sess.isoformat()}名录读回总数与独立预期不一致",
                status=409,
            )
        total_dir_rows += len(expected_members)
        if write_res.get("status") != "NO_CHANGE":
            all_dir_no_change = False
    if dir_unit is not None:
        actual_dir = data_job_pg.read_business_table_count(
            config,
            business_table="stock.source_baostock_session_directory",
            target_session=target_day,
            source_capture_id=capture_id,
            calendar_revision=plan.get("calendar_revision") or "",
            producer_version="BAOSTOCK_SESSION_DIRECTORY_R1",
            research_cutoff=cutoff,
        )
        if actual_dir != total_dir_rows:
            raise DataJobError(
                "BAOSTOCK_SESSION_DIRECTORY_TOTAL_READBACK_MISMATCH",
                "逐日名录捕获范围PG读回总数与逐会话独立预期不一致",
                status=409,
            )
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_BAOSTOCK_UNIVERSE",
            target_session=target_day,
            business_table="stock.source_baostock_session_directory",
            source_capture_id=capture_id,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=total_dir_rows,
            inserted_rows=total_dir_rows,
            quality_status="NON_PIT_OBSERVED",
            no_change=all_dir_no_change,
            producer_version="BAOSTOCK_SESSION_DIRECTORY_R1",
            calendar_revision=plan.get("calendar_revision") or "",
        )
    return {
        "status": "COMPLETED",
        "source_capture_id": capture_id,
        "basic_row_count": int(basic_write["row_count"]),
        "directory_row_count": total_dir_rows,
        "directory_sessions": [d.isoformat() for d in dir_sessions],
    }


def _execute_baostock_market_indices_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    official_sessions: Sequence[date],
    progress_callback: Any,
    begin_callback: Any,
) -> dict[str, Any] | None:
    """采集并持久化配置启用的通用参考指数日线（INDEX_POINT），独立完成。"""
    from pathlib import Path

    from stock_a.contracts.loader import load_market_reference_indices_contract
    from stock_a.sources.baostock.client import query_index_daily_range
    from stock_a.storage.postgres.market_indices import (
        persist_market_index_daily_bars,
        read_market_index_daily_scope,
    )

    selected_sources = _selected_source_modules(plan)
    if "BAOSTOCK_MARKET_INDICES" not in selected_sources:
        return None
    stage_units = [
        unit
        for unit in plan.get("stage_units", ())
        if unit.get("stage_code") == "SOURCE_BAOSTOCK_MARKET_INDICES"
        and unit.get("status") == "NOT_STARTED"
    ]
    if not stage_units:
        return None

    target_day = date.fromisoformat(plan["target_session"])
    cutoff = date.fromisoformat(plan["research_cutoff"])
    eligible_sessions = [d for d in official_sessions if d <= min(target_day, cutoff)]
    if not eligible_sessions:
        raise DataJobError(
            "BAOSTOCK_INDEX_SESSION_EMPTY",
            "参考指数日线没有合法官方会话范围。",
            status=409,
        )
    if target_day not in eligible_sessions:
        raise DataJobError(
            "BAOSTOCK_INDEX_TARGET_NOT_OFFICIAL",
            "参考指数目标日必须是截止内官方交易会话",
            status=409,
        )
    # 只请求明确获准的官方窗口；短窗不足时因子按原合同保留未知，不向外扩展预热。
    raw_window = [
        date.fromisoformat(d) if isinstance(d, str) else d
        for d in plan.get("raw_window", ())
    ]
    if raw_window:
        approved_end = min(target_day, cutoff)
        if (
            tuple(sorted(set(raw_window))) != tuple(raw_window)
            or any(d not in official_sessions or d > approved_end for d in raw_window)
            or target_day not in raw_window
        ):
            raise DataJobError(
                "BAOSTOCK_INDEX_WINDOW_OUTSIDE_AUTHORITY",
                "参考指数窗口必须是目标日及其之前、截止内的唯一官方会话范围",
                status=409,
            )
        idx_window = tuple(
            session
            for session in eligible_sessions
            if raw_window[0] <= session <= raw_window[-1]
        )
    else:
        # 来源任务没有显式历史窗口时只取目标会话；历史因子按现有合同保留未知。
        idx_window = (target_day,)
    index_contract = load_market_reference_indices_contract(
        Path(config["paths"]["configuration_root"])
    )
    enabled_indices = [
        item for item in index_contract.get("indices", ()) if item.get("enabled")
    ]
    if not enabled_indices:
        raise DataJobError(
            "NO_ENABLED_MARKET_REFERENCE_INDICES",
            "参考指数目录合同中没有启用的指数。",
            status=409,
        )
    symbols = tuple(str(item["source_symbol"]) for item in enabled_indices)
    if len(set(symbols)) != len(symbols) or any(
        re.fullmatch(r"(SH|SZ)\.[0-9]{6}", symbol) is None for symbol in symbols
    ):
        raise DataJobError(
            "BAOSTOCK_INDEX_CONTRACT_SYMBOLS_INVALID",
            "启用参考指数清单含重复或非法证券代码",
            status=409,
        )
    official_code_by_sym = {
        str(item["source_symbol"]): str(
            item.get("official_code") or str(item["source_symbol"]).split(".")[-1]
        )
        for item in enabled_indices
    }
    capture_id = _allocate_baostock_aux_capture(
        config,
        job_id=job_id,
        stage_kind="BAOSTOCK_MARKET_INDICES",
        range_start_session=idx_window[0],
        range_end_session=idx_window[-1],
        requested_symbol_count=len(symbols),
        parser_contract="BAOSTOCK_MARKET_INDEX_DAILY_R1",
    )
    begin_callback(job_id=job_id, units=stage_units)
    transport = plan.get("_baostock_transport")
    idx_res = query_index_daily_range(
        symbols=symbols,
        start_date=idx_window[0].isoformat(),
        end_date=idx_window[-1].isoformat(),
        config=config,
        transport=transport,
        enforce_quota=True,
    )
    if not idx_res.get("complete") or idx_res.get("status") != "RESPONSE_RECEIVED":
        raise DataJobError(
            "BAOSTOCK_MARKET_INDEX_FETCH_FAILED",
            f"BaoStock通用参考指数日线采集未完成: {idx_res.get('status')} ({idx_res.get('reason')})",
            status=502,
        )
    rows_by_symbol = idx_res.get("rows_by_symbol")
    if not isinstance(rows_by_symbol, Mapping) or set(rows_by_symbol) != set(symbols):
        raise DataJobError(
            "BAOSTOCK_INDEX_RESPONSE_SYMBOL_SCOPE_MISMATCH",
            "指数响应的分组代码集合与配置启用的请求代码不一致",
            status=409,
        )
    normalized_rows: list[dict[str, Any]] = []
    requested_sessions = set(idx_window)
    expected_index_keys = {
        (symbol, session) for symbol in symbols for session in idx_window
    }
    actual_index_keys: set[tuple[str, date]] = set()
    from stock_a.storage.postgres.baostock_universe import normalize_baostock_symbol

    for sym in symbols:
        raw_rows = rows_by_symbol[sym]
        if not isinstance(raw_rows, (tuple, list)):
            raise DataJobError(
                "BAOSTOCK_INDEX_RESPONSE_ROWS_INVALID",
                f"{sym}指数响应不是行列表",
                status=409,
            )
        official_code = official_code_by_sym.get(sym, sym.split(".")[-1])
        for raw in raw_rows:
            required = ("date", "code", "open", "high", "low", "close", "preclose")
            if not isinstance(raw, Mapping) or any(
                name not in raw for name in required
            ):
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_FIELDS_INVALID",
                    f"{sym}指数响应行缺少必需字段",
                    status=409,
                )
            parsed_code = normalize_baostock_symbol(str(raw.get("code") or ""))
            if parsed_code is None or parsed_code[0] != sym:
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_CODE_MISMATCH",
                    f"指数响应行内code与请求代码{sym}不一致",
                    status=409,
                )
            sess_text = str(raw.get("date") or "").strip()
            try:
                sess_day = date.fromisoformat(sess_text)
            except ValueError as error:
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_DATE_INVALID",
                    f"{sym}指数响应日期格式无效",
                    status=409,
                ) from error
            if (
                sess_day.isoformat() != sess_text
                or sess_day not in requested_sessions
                or sess_day > cutoff
            ):
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_DATE_OUTSIDE_REQUEST",
                    f"{sym}指数响应日期不属于本次获准官方会话范围",
                    status=409,
                )
            key = (sym, sess_day)
            if key in actual_index_keys:
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_DUPLICATE_KEY",
                    f"{sym} {sess_text}指数响应含重复日线键",
                    status=409,
                )
            actual_index_keys.add(key)
            price_values = {
                name: _index_decimal(raw.get(name), field=name, required=True)
                for name in ("open", "high", "low", "close", "preclose")
            }
            if (
                any(value <= 0 for value in price_values.values())
                or price_values["high"] < price_values["low"]
                or price_values["high"]
                < max(price_values["open"], price_values["close"])
                or price_values["low"]
                > min(price_values["open"], price_values["close"])
            ):
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_PRICE_INVALID",
                    f"{sym} {sess_text}指数OHLC关系或正值约束无效",
                    status=409,
                )
            volume = _index_decimal(raw.get("volume"), field="volume", required=False)
            amount = _index_decimal(raw.get("amount"), field="amount", required=False)
            pct_chg = _index_decimal(raw.get("pctChg"), field="pctChg", required=False)
            if (volume is not None and volume < 0) or (
                amount is not None and amount < 0
            ):
                raise DataJobError(
                    "BAOSTOCK_INDEX_RESPONSE_QUANTITY_INVALID",
                    f"{sym} {sess_text}指数成交量额不能为负数",
                    status=409,
                )
            normalized_rows.append(
                {
                    "source_symbol": sym,
                    "official_code": official_code,
                    "session_date": sess_day,
                    "bar_status": "TARGET_PRESENT",
                    "open_points": price_values["open"],
                    "high_points": price_values["high"],
                    "low_points": price_values["low"],
                    "close_points": price_values["close"],
                    "preclose_points": price_values["preclose"],
                    "volume_raw": volume,
                    "amount_raw": amount,
                    "pct_chg_reported": pct_chg,
                    "price_quality": "OBSERVED",
                    "price_reason": None,
                }
            )
    if actual_index_keys != expected_index_keys:
        missing_keys = sorted(expected_index_keys - actual_index_keys)
        extra_keys = sorted(actual_index_keys - expected_index_keys)
        raise DataJobError(
            "BAOSTOCK_INDEX_RESPONSE_COVERAGE_INCOMPLETE",
            "参考指数响应代码×官方会话键覆盖不完整："
            f"缺失{len(missing_keys)}个（示例={missing_keys[:5]}），"
            f"超出{len(extra_keys)}个（示例={extra_keys[:5]}）",
            status=409,
        )
    write_res = persist_market_index_daily_bars(
        config,
        source_capture_id=capture_id,
        rows=normalized_rows,
    )
    expected_index_readback = {(sym, day) for sym, day in expected_index_keys}
    actual_index_readback = read_market_index_daily_scope(
        config,
        source_capture_id=capture_id,
        session_dates=idx_window,
        cutoff=cutoff,
    )
    actual_index_readback_keys = {
        (item["source_symbol"], item["session_date"]) for item in actual_index_readback
    }
    if actual_index_readback_keys != expected_index_readback:
        raise DataJobError(
            "BAOSTOCK_INDEX_READBACK_SCOPE_MISMATCH",
            "指数日线PG读回键集合与规范化请求预期不一致",
            status=409,
        )
    actual_rows = data_job_pg.read_business_table_count(
        config,
        business_table="stock.source_market_index_daily",
        target_session=target_day,
        source_capture_id=capture_id,
        calendar_revision=plan.get("calendar_revision") or "",
        producer_version="BAOSTOCK_MARKET_INDEX_DAILY_R1",
        research_cutoff=cutoff,
    )
    if actual_rows != len(expected_index_keys):
        raise DataJobError(
            "BAOSTOCK_INDEX_READBACK_COUNT_MISMATCH",
            "指数日线PG读回总数与请求代码×官方会话独立预期不一致",
            status=409,
        )
    for unit in stage_units:
        progress_callback(
            job_id=job_id,
            stage_code="SOURCE_BAOSTOCK_MARKET_INDICES",
            target_session=target_day,
            business_table="stock.source_market_index_daily",
            source_capture_id=capture_id,
            profile_history_session_count=None,
            d1_history_session_count=None,
            expected_rows=len(expected_index_keys),
            inserted_rows=int(write_res["row_count"]),
            quality_status="NON_PIT_OBSERVED",
            no_change=write_res.get("status") == "NO_CHANGE",
            producer_version="BAOSTOCK_MARKET_INDEX_DAILY_R1",
            calendar_revision=plan.get("calendar_revision") or "",
        )
    return {
        "status": "COMPLETED",
        "source_capture_id": capture_id,
        "row_count": int(write_res["row_count"]),
        "symbols": list(symbols),
    }


def _execute_baostock_source_stage(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    official_sessions: Sequence[date],
    progress_callback: Any,
    begin_callback: Any,
) -> dict[str, Any] | None:
    """创建单一paired捕获并持久化全市场RAW/QFQ价格请求与事实。"""
    from datetime import datetime, timezone

    from stock_a.application.baostock_daily_jobs import (
        BaoStockPriceWindowRecord,
        fetch_baostock_price_window,
    )
    from stock_a.sources.baostock.client import query_identity_reference
    from stock_a.sources.baostock.evidence import _UNITS as documented_units
    from stock_a.storage.postgres import baostock_price_facts
    from stock_a.storage.postgres.baostock_price_facts import (
        RAW_COORDINATE,
        QFQ_COORDINATE,
    )

    selected_sources = set(plan.get("source_modules", ()))
    if "BAOSTOCK_PRICES" not in selected_sources:
        return None
    sessions = tuple(
        date.fromisoformat(day) for day in plan.get("raw_history_sessions", ())
    )
    cutoff = date.fromisoformat(plan["research_cutoff"])
    if (
        not sessions
        or tuple(sorted(set(sessions))) != sessions
        or any(day not in official_sessions or day > cutoff for day in sessions)
    ):
        raise DataJobError(
            "BAOSTOCK_SOURCE_WINDOW_INVALID",
            "BaoStock来源范围不是截止内官方窗口。",
            status=409,
        )
    prepared = plan.get("_prepared_baostock_reset_inputs")
    if not isinstance(prepared, Mapping):
        required_pairs = {
            (day.isoformat(), coordinate)
            for day in sessions
            for coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
        }
        versions = {
            (item.get("session_date"), item.get("price_coordinate")): item
            for item in plan.get("price_capture_inputs", ())
            if isinstance(item, Mapping)
        }
        capture_ids = {
            item.get("source_capture_id")
            for key, item in versions.items()
            if key in required_pairs
        }
        if (
            required_pairs
            and required_pairs.issubset(versions)
            and len(capture_ids) == 1
            and type(next(iter(capture_ids), None)) is int
            and next(iter(capture_ids)) > 0
        ):
            reused_capture_id = int(next(iter(capture_ids)))
            data_job_pg.set_job_source_capture(
                config, job_id=job_id, source_capture_id=reused_capture_id
            )
            source_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "SOURCE_BAOSTOCK_PRICES"
            ]
            begin_callback(job_id=job_id, units=source_units)
            for unit in source_units:
                session = unit["target_session"]
                row_count = data_job_pg.read_business_table_count(
                    config,
                    business_table="stock.source_raw_daily",
                    target_session=session,
                    source_capture_id=reused_capture_id,
                    research_cutoff=cutoff,
                )
                progress_callback(
                    job_id=job_id,
                    stage_code="SOURCE_BAOSTOCK_PRICES",
                    target_session=session,
                    business_table="stock.source_raw_daily",
                    source_capture_id=reused_capture_id,
                    profile_history_session_count=None,
                    d1_history_session_count=None,
                    expected_rows=row_count,
                    inserted_rows=0,
                    quality_status="ALREADY_COMPLETE_PAIRED_CAPTURE",
                    no_change=True,
                    producer_version="STOCK_A_BAOSTOCK_SOURCE_CAPTURE_R2",
                )
            if isinstance(plan, dict):
                plan["_paired_source_capture_id"] = reused_capture_id
                plan["_source_symbols"] = tuple()
            return {
                "status": "ALREADY_COMPLETE",
                "source_capture_id": reused_capture_id,
                "reused": True,
            }
    source_policy = config.get("source_policy", {})
    primary = (
        source_policy.get("baostock_primary")
        if isinstance(source_policy, Mapping)
        else None
    )
    budget = primary.get("request_budget") if isinstance(primary, Mapping) else None
    if not isinstance(budget, Mapping):
        raise DataJobError(
            "BAOSTOCK_PRIMARY_POLICY_INVALID", "BaoStock来源预算缺失。", status=409
        )
    max_api = int(budget["max_api_calls_per_batch"])
    max_login_batches = int(budget["max_login_batches"])
    query_timeout = float(budget["query_timeout_seconds"])
    history_limit = int(budget["max_history_session_count"])
    # 每个DataJob attempt对每个证券/价格坐标最多发起一次外部请求。
    retry_attempts = 0
    backoff = float(budget["retry_backoff_seconds"])
    if len(sessions) > history_limit:
        raise DataJobError(
            "BAOSTOCK_SOURCE_WINDOW_OVER_BUDGET",
            "BaoStock来源窗口超过runtime.defaults中明确登记的历史会话预算。",
            status=409,
        )
    identity_scope: Mapping[str, Any] = {}
    if isinstance(prepared, Mapping):
        universe = tuple(prepared.get("source_symbols", ()))
        eligible_sessions_by_symbol = prepared.get("eligible_sessions_by_symbol", {})
        basic_names = tuple(prepared.get("basic_names", ()))
        identity_scope = prepared.get("identity_scope", {})
        identity = prepared.get("identity_snapshot")
        preflight = prepared.get("price_preflight")
        preflight_logins = int(prepared.get("preflight_login_batches", 0))
        identity_preflight_valid = (
            isinstance(preflight, Mapping)
            and preflight.get("mode") == "IDENTITY_AND_PRICE_PROBE"
            and preflight.get("completion_status")
            == "IDENTITY_AND_PRICE_PROBE_COMPLETE"
            and preflight.get("source_network_called") is True
            and preflight.get("identity_requests_performed") is True
            and preflight.get("price_requests_performed") is True
            and preflight_logins > 0
        )
        persisted_resume_valid = (
            isinstance(preflight, Mapping)
            and preflight.get("mode") == "PERSISTED_CAPTURE_RESUME"
            and preflight.get("completion_status") == "PERSISTED_CAPTURE_COMPLETE"
            and preflight.get("source_network_called") is False
            and preflight.get("identity_requests_performed") is False
            and preflight.get("price_requests_performed") is False
            and preflight.get("capture_reused") is True
            and preflight_logins == 0
        )
        if (
            not isinstance(preflight, Mapping)
            or not (identity_preflight_valid or persisted_resume_valid)
            or preflight.get("full_capture_deferred_until_after_clear") is not True
            or preflight.get("target_sessions")
            != [session.isoformat() for session in sessions]
            or tuple(prepared.get("price_preflight_sessions", ())) != sessions
            or tuple(prepared.get("price_preflight_symbols", ())) != tuple(universe)
        ):
            raise DataJobError(
                "RESET_BAOSTOCK_IDENTITY_PREFLIGHT_INCOMPLETE",
                "清理前身份/预算预检与当前全量来源范围不一致，拒绝清理。",
                status=409,
            )
    else:
        target_text = sessions[-1].isoformat()
        identity = query_identity_reference(
            (target_text,),
            timeout_seconds=query_timeout,
            max_total_seconds=float(budget["max_identity_total_seconds"]),
            config=config,
        )
        resolved = _baostock_universe_from_identity(identity, sessions)
        universe = resolved["source_symbols"]
        eligible_sessions_by_symbol = resolved["eligible_sessions_by_symbol"]
        basic_names = resolved["basic_names"]
        identity_scope = resolved["identity_scope"]
        preflight = None
        preflight_logins = 0
    if (
        not universe
        or not isinstance(eligible_sessions_by_symbol, Mapping)
        or set(eligible_sessions_by_symbol) != set(universe)
    ):
        raise DataJobError(
            "BAOSTOCK_UNIVERSE_INVALID",
            "BaoStock动态证券池或逐代码窗口未闭合。",
            status=409,
        )
    identity_logins = (
        int(identity.get("login_attempted") is True)
        if isinstance(identity, Mapping)
        else 0
    )
    login_used = identity_logins + preflight_logins
    window_material = {
        "start": sessions[0].isoformat(),
        "end": sessions[-1].isoformat(),
        "symbols": list(universe),
    }
    window_digest = hashlib.sha256(
        json.dumps(window_material, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    capture_uuid = uuid5(
        NAMESPACE_URL,
        f"stock-a:baostock:paired-capture:{window_digest}",
    )
    if isinstance(prepared, Mapping) and prepared.get("capture_uuid") != capture_uuid:
        raise DataJobError(
            "RESET_CAPTURE_UUID_MISMATCH",
            "清理前预算核验的paired capture身份与正式来源阶段不一致。",
            status=409,
        )
    captured_at = datetime.fromisoformat(identity["request_completed_at"])
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise DataJobError(
            "BAOSTOCK_IDENTITY_CLOCK_INVALID", "BaoStock身份响应缺少时区。", status=503
        )
    source_capture_id = baostock_price_facts.begin_baostock_price_capture(
        dict(config),
        capture_uuid=capture_uuid,
        range_start_session=sessions[0],
        range_end_session=sessions[-1],
        price_coordinates=(RAW_COORDINATE, QFQ_COORDINATE),
        requested_symbol_count=len(universe),
        started_at=datetime.now(timezone.utc),
        parser_contract="STOCK_A_BAOSTOCK_DUAL_COORDINATE_DAILY_R2",
        unit_contract=documented_units,
    )
    roster = baostock_price_facts.read_baostock_capture_roster(
        dict(config), source_capture_id=source_capture_id, cutoff=cutoff
    )
    if roster.get("status") not in {"AVAILABLE", "EMPTY"}:
        raise DataJobError(
            "BAOSTOCK_CAPTURE_ROSTER_UNAVAILABLE",
            "无法读取paired capture证券×坐标断点，拒绝重取成功请求。",
            status=409,
        )
    if roster.get("status") == "AVAILABLE":
        if (
            roster.get("range_start_session") != sessions[0].isoformat()
            or roster.get("range_end_session") != sessions[-1].isoformat()
            or roster.get("requested_symbol_count") != len(universe)
            or roster.get("source_capture_unit_contract_version")
            != documented_units.version
        ):
            raise DataJobError(
                "BAOSTOCK_CAPTURE_ROSTER_SCOPE_MISMATCH",
                "paired capture范围或单位合同与当前全市场总体不一致。",
                status=409,
            )
    roster_by_symbol = {
        str(item.get("source_symbol")): item
        for item in roster.get("symbols", ())
        if isinstance(item, Mapping) and isinstance(item.get("source_symbol"), str)
    }
    if set(roster_by_symbol) - set(universe):
        raise DataJobError(
            "BAOSTOCK_CAPTURE_ROSTER_UNIVERSE_MISMATCH",
            "paired capture已存请求代码不属于当前BaoStock动态总体。",
            status=409,
        )
    for symbol, roster_item in roster_by_symbol.items():
        eligible = tuple(eligible_sessions_by_symbol[symbol])
        if (
            roster_item.get("eligible_windows_match") is not True
            or roster_item.get("eligible_start_session") != eligible[0].isoformat()
            or roster_item.get("eligible_end_session") != eligible[-1].isoformat()
        ):
            raise DataJobError(
                "BAOSTOCK_CAPTURE_ROSTER_ELIGIBLE_WINDOW_MISMATCH",
                f"paired capture代码{symbol}上市有效窗口与当前BaoStock名单不同。",
                status=409,
            )
    data_job_pg.set_job_source_capture(
        config, job_id=job_id, source_capture_id=source_capture_id
    )
    names_result = baostock_price_facts.persist_baostock_basic_names(
        dict(config),
        source_capture_id=source_capture_id,
        names=basic_names,
        captured_at=captured_at,
    )
    source_units = [
        unit
        for unit in plan.get("stage_units", ())
        if unit.get("stage_code") == "SOURCE_BAOSTOCK_PRICES"
    ]
    begin_callback(job_id=job_id, units=source_units)
    before_counts = {
        session: data_job_pg.read_business_table_count(
            config,
            business_table="stock.source_raw_daily",
            target_session=session,
            source_capture_id=source_capture_id,
            research_cutoff=cutoff,
        )
        for session in sessions
    }
    batch_results: list[dict[str, Any]] = []
    request_complete_symbols = {
        coordinate: {
            symbol
            for symbol, row in roster_by_symbol.items()
            if isinstance(row.get("coordinates", {}).get(coordinate), Mapping)
            and row["coordinates"][coordinate].get("request_status")
            in {"COMPLETED", "NO_DATA"}
        }
        for coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
    }
    persisted_attempts: dict[str, dict[str, Mapping[str, Any]]] = {
        "RAW_UNADJUSTED": {},
        "FORWARD_ADJUSTED": {},
    }
    for symbol, roster_item in roster_by_symbol.items():
        coordinates_for_symbol = roster_item.get("coordinates", {})
        for coordinate in persisted_attempts:
            prior_row = (
                coordinates_for_symbol.get(coordinate)
                if isinstance(coordinates_for_symbol, Mapping)
                else None
            )
            if isinstance(prior_row, Mapping):
                persisted_attempts[coordinate][symbol] = {
                    "request_started_at": prior_row.get("last_attempt_started_at"),
                    "request_completed_at": prior_row.get("last_attempt_completed_at"),
                    "status": prior_row.get("request_status", "NOT_ATTEMPTED"),
                    "response_row_count": int(
                        prior_row.get("response_row_count", 0) or 0
                    ),
                    "attempt_count": int(prior_row.get("attempt_count", 0) or 0),
                    "local_error": prior_row.get("last_error_code"),
                    "returned_fields": tuple(prior_row.get("returned_fields", ())),
                }

    def persist_outcome(
        symbols: Sequence[str],
        outcome: Mapping[str, Any],
        *,
        count_login_batches: bool = True,
    ) -> None:
        nonlocal login_used
        eligible = {
            symbol: tuple(eligible_sessions_by_symbol[symbol])
            for symbol in symbols
            if tuple(eligible_sessions_by_symbol[symbol])
        }
        records = tuple(
            record
            for record in outcome.get("records", ())
            if isinstance(record, BaoStockPriceWindowRecord)
            and record.source_symbol in eligible
            and record.session_date in eligible[record.source_symbol]
        )
        attempts_by_coordinate = outcome.get("request_attempts", {})
        request_attempts: dict[str, dict[str, Any]] = {}
        for coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED"):
            coordinate_outcome = (
                attempts_by_coordinate.get(coordinate, {})
                if isinstance(attempts_by_coordinate, Mapping)
                else {}
            )
            coordinate_attempts: dict[str, Any] = {}
            for symbol in symbols:
                attempt = (
                    coordinate_outcome.get(symbol)
                    if isinstance(coordinate_outcome, Mapping)
                    else None
                )
                if isinstance(attempt, Mapping):
                    persisted_attempts[coordinate][symbol] = attempt
                    coordinate_attempts[symbol] = attempt
                    if (
                        attempt.get("status") == "RESPONSE_RECEIVED"
                        and type(attempt.get("attempt_count")) is int
                        and attempt.get("attempt_count", 0) > 0
                    ):
                        request_complete_symbols[coordinate].add(symbol)
                    continue
                prior_attempt = persisted_attempts[coordinate].get(symbol)
                if prior_attempt is not None:
                    coordinate_attempts[symbol] = prior_attempt
                else:
                    coordinate_attempts[symbol] = {
                        "request_started_at": None,
                        "request_completed_at": None,
                        "status": "NOT_ATTEMPTED_BUDGET",
                        "response_row_count": 0,
                        "attempt_count": 0,
                        "local_error": "REQUEST_NOT_ATTEMPTED",
                        "returned_fields": None,
                    }
            request_attempts[coordinate] = coordinate_attempts
        clocks = [
            datetime.fromisoformat(value)
            for coordinate_rows in request_attempts.values()
            for attempt in coordinate_rows.values()
            for value in (
                attempt.get("request_started_at"),
                attempt.get("request_completed_at"),
            )
            if isinstance(value, str)
        ]
        batch_started = min(clocks) if clocks else datetime.now(timezone.utc)
        batch_completed = max(clocks) if clocks else batch_started
        if batch_started.tzinfo is None or batch_completed.tzinfo is None:
            raise DataJobError(
                "BAOSTOCK_REQUEST_CLOCK_INVALID",
                "BaoStock逐请求时钟缺少时区。",
                status=409,
            )
        persisted = baostock_price_facts.persist_baostock_price_batch(
            dict(config),
            source_capture_id=source_capture_id,
            records=records,
            request_attempts=request_attempts,
            eligible_sessions_by_symbol=eligible,
            unit_contract=documented_units,
            batch_started_at=batch_started,
            batch_completed_at=batch_completed,
        )
        if count_login_batches:
            login_used += int(
                outcome.get("login_budget_batches", outcome.get("login_batches", 0))
                or 0
            )
        batch_results.append(
            {
                "source_symbols": list(symbols),
                "api_calls": int(outcome.get("api_calls", 0) or 0),
                "login_batches": int(outcome.get("login_batches", 0) or 0),
                **persisted,
            }
        )

    for coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED"):
        pending_symbols = [
            symbol
            for symbol in universe
            if symbol not in request_complete_symbols[coordinate]
            and (
                persisted_attempts[coordinate].get(symbol, {}).get("status")
                != "PERMANENT_FAILURE"
            )
        ]
        for offset in range(0, len(pending_symbols), max_api):
            symbols = tuple(pending_symbols[offset : offset + max_api])
            remaining_logins = max_login_batches - login_used
            if remaining_logins < 1:
                placeholder_records = []
                for symbol in symbols:
                    prior_fields = {
                        prior_coordinate: tuple(
                            persisted_attempts[prior_coordinate]
                            .get(symbol, {})
                            .get("returned_fields", ())
                        )
                        for prior_coordinate in ("RAW_UNADJUSTED", "FORWARD_ADJUSTED")
                        if persisted_attempts[prior_coordinate]
                        .get(symbol, {})
                        .get("returned_fields")
                    }
                    for session in eligible_sessions_by_symbol[symbol]:
                        placeholder_records.append(
                            BaoStockPriceWindowRecord(
                                source_symbol=symbol,
                                requested_source_code=symbol.lower(),
                                source_code=symbol.lower(),
                                session_date=session,
                                raw_observation=None,
                                qfq_ohlc=None,
                                qfq_observation=None,
                                response_fields_by_coordinate=prior_fields,
                                raw_captured_at=None,
                                qfq_captured_at=None,
                                coverage_status="NO_BAR_IN_EITHER_COORDINATE_UNKNOWN",
                            )
                        )
                outcome = {
                    "status": "INCOMPLETE",
                    "completion_status": "INCOMPLETE",
                    "login_batches": 0,
                    "records": tuple(placeholder_records),
                    "request_attempts": {
                        coordinate: {
                            symbol: {
                                "request_started_at": None,
                                "request_completed_at": None,
                                "status": "NOT_ATTEMPTED_BUDGET",
                                "response_row_count": 0,
                                "attempt_count": 0,
                                "local_error": "MAX_LOGIN_BATCHES_EXHAUSTED",
                                "returned_fields": None,
                            }
                            for symbol in symbols
                        }
                    },
                }
            else:
                request_attempt_offsets = {
                    request_coordinate: {
                        symbol: int(
                            persisted_attempts[request_coordinate]
                            .get(symbol, {})
                            .get("attempt_count", 0)
                            or 0
                        )
                        for symbol in symbols
                    }
                    for request_coordinate in (
                        "RAW_UNADJUSTED",
                        "FORWARD_ADJUSTED",
                    )
                }
                outcome = fetch_baostock_price_window(
                    symbols,
                    sessions,
                    cutoff=cutoff,
                    units=documented_units,
                    max_api_calls_per_batch=max_api,
                    max_login_batches=remaining_logins,
                    query_timeout_seconds=query_timeout,
                    max_history_session_count=history_limit,
                    max_retry_attempts=retry_attempts,
                    retry_backoff_seconds=backoff,
                    coordinates=(coordinate,),
                    request_attempt_offsets=request_attempt_offsets,
                    config=config,
                )
            persist_outcome(symbols, outcome)

    expected_by_session = {
        session: sum(
            session in tuple(eligible_sessions_by_symbol[symbol]) for symbol in universe
        )
        for session in sessions
    }
    final_roster = baostock_price_facts.read_baostock_capture_roster(
        dict(config), source_capture_id=source_capture_id, cutoff=cutoff
    )
    final_roster_by_symbol = {
        str(item.get("source_symbol")): item
        for item in final_roster.get("symbols", ())
        if isinstance(item, Mapping) and isinstance(item.get("source_symbol"), str)
    }
    request_scope_closed = (
        final_roster.get("status") == "AVAILABLE"
        and final_roster.get("requested_symbol_count") == len(universe)
        and final_roster.get("registration_complete") is True
        and set(final_roster_by_symbol) == set(universe)
        and all(
            item.get("eligible_windows_match") is True
            and item.get("request_complete") is True
            and item.get("eligible_start_session")
            == eligible_sessions_by_symbol[symbol][0].isoformat()
            and item.get("eligible_end_session")
            == eligible_sessions_by_symbol[symbol][-1].isoformat()
            for symbol, item in final_roster_by_symbol.items()
            if eligible_sessions_by_symbol.get(symbol)
        )
    )
    incomplete = not request_scope_closed
    all_session_rows_complete = True
    for unit in source_units:
        session = unit["target_session"]
        actual_rows = data_job_pg.read_business_table_count(
            config,
            business_table="stock.source_raw_daily",
            target_session=session,
            source_capture_id=source_capture_id,
            research_cutoff=cutoff,
        )
        expected_rows = expected_by_session[session]
        complete = not incomplete and actual_rows == expected_rows
        all_session_rows_complete = all_session_rows_complete and complete
        if complete:
            progress_callback(
                job_id=job_id,
                stage_code="SOURCE_BAOSTOCK_PRICES",
                target_session=session,
                business_table="stock.source_raw_daily",
                source_capture_id=source_capture_id,
                profile_history_session_count=None,
                d1_history_session_count=None,
                expected_rows=expected_rows,
                inserted_rows=max(0, actual_rows - int(before_counts[session] or 0)),
                quality_status="COMPLETE_RAW_AND_QFQ_REQUESTS",
                no_change=actual_rows == int(before_counts[session] or 0),
                producer_version="STOCK_A_BAOSTOCK_SOURCE_CAPTURE_R2",
            )
        else:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code="SOURCE_BAOSTOCK_PRICES",
                target_session=session,
                business_table="stock.source_raw_daily",
                status="INCOMPLETE",
                error_code="BAOSTOCK_CAPTURE_COVERAGE_INCOMPLETE",
                error_message=(
                    f"paired capture {source_capture_id} 该会话读回{actual_rows}/{expected_rows}行，"
                    "或至少一个证券×坐标请求未完成。"
                ),
                source_capture_id=source_capture_id,
            )
    if incomplete or not all_session_rows_complete:
        return {
            "status": "INCOMPLETE",
            "source_capture_id": source_capture_id,
            "capture_uuid": str(capture_uuid),
            "universe_count": len(universe),
            "identity_scope": identity_scope,
            "batch_results": batch_results,
            "names_result": names_result,
            "reason_code": "BAOSTOCK_CAPTURE_COVERAGE_INCOMPLETE",
        }
    if isinstance(plan, dict):
        plan["_paired_source_capture_id"] = source_capture_id
        plan["_paired_capture_uuid"] = str(capture_uuid)
        plan["_source_symbols"] = tuple(universe)
        plan["_eligible_sessions_by_symbol"] = eligible_sessions_by_symbol

    return {
        "status": "COMPLETED",
        "source_capture_id": source_capture_id,
        "capture_uuid": str(capture_uuid),
        "universe_count": len(universe),
        "identity_scope": identity_scope,
        "batch_results": batch_results,
        "names_result": names_result,
    }


def _execute_shared_production_chain(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    progress_callback: Any,
    begin_callback: Any,
) -> None:
    """运行唯一BaoStock paired capture、TDX成员和Core/Profile生产链（整任务共享BaoStock配额预算）。"""
    from stock_a.storage.postgres.baostock_quota import baostock_job_quota_scope

    with baostock_job_quota_scope():
        _execute_shared_production_chain_impl(
            config=config,
            job_id=job_id,
            plan=plan,
            progress_callback=progress_callback,
            begin_callback=begin_callback,
        )


def _execute_shared_production_chain_impl(
    *,
    config: Mapping[str, Any],
    job_id: UUID,
    plan: Mapping[str, Any],
    progress_callback: Any,
    begin_callback: Any,
) -> None:
    """实际执行BaoStock来源、TDX成员、基础画像与研究全链路。"""
    from stock_a.application.pg_price_profiles import (
        SOURCE_SYMBOL_PROFILE_SCOPE,
        run_source_symbol_price_profile,
    )
    from stock_a.application.runtime_config import _CONFIG_ROOT
    from stock_a.contracts import load_price_profile_contract

    selected_sources = set(plan.get("source_modules", ()))
    base_modules = set(plan.get("base_modules", ()))
    research_modules = set(plan.get("research_modules", ()))
    resolved_unit_keys: set[tuple[str, date, str]] = set()
    incomplete_stage_errors: list[tuple[str, str]] = []
    complete_progress_statuses = {
        "COMPLETED",
        "ALREADY_COMPLETE",
        "NO_CHANGE",
        "CLEARED",
        "NOT_DUE",
        "SKIPPED_BY_USER",
        "SKIPPED_NOT_IMPLEMENTED",
    }

    def unit_key(unit: Mapping[str, Any]) -> tuple[str, date, str]:
        return (
            str(unit["stage_code"]),
            unit["target_session"],
            str(unit["business_table"]),
        )

    def mark_units_incomplete(
        units: Sequence[Mapping[str, Any]],
        *,
        stage_code: str | None,
        error: Exception,
        message_prefix: str,
        unit_source_capture_id: int | None = None,
        profile_revision: str | None = None,
        producer_version: str | None = None,
        industry_snapshot_id: int | None = None,
        concept_snapshot_id: int | None = None,
    ) -> None:
        error_code = str(getattr(error, "code", type(error).__name__))
        for unit in units:
            unit_stage = stage_code or str(unit["stage_code"])
            unit_session = unit["target_session"]
            business_table = str(unit["business_table"])
            previous = data_job_pg.read_progress_unit(
                config,
                job_id=job_id,
                stage_code=unit_stage,
                target_session=unit_session,
                business_table=business_table,
            )
            if (
                isinstance(previous, Mapping)
                and previous.get("status") in complete_progress_statuses
            ):
                resolved_unit_keys.add(unit_key(unit))
                continue
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code=unit_stage,
                target_session=unit_session,
                business_table=business_table,
                status="INCOMPLETE",
                error_code=error_code,
                error_message=f"{message_prefix}：{str(error)[:500]}",
                source_capture_id=unit_source_capture_id
                if unit_source_capture_id is not None
                else source_capture_id,
                profile_revision=profile_revision,
                producer_version=producer_version,
                industry_snapshot_id=industry_snapshot_id,
                concept_snapshot_id=concept_snapshot_id,
            )
            resolved_unit_keys.add(unit_key(unit))

    def _invalidate_r03_for_upstream_recompute(
        from_session: date,
        *,
        upstream_module: str = "CORE_PROFILE",
        profile_revision_filter: str | None = None,
        universe_version_filter: str | None = None,
    ) -> None:
        """与清理路径共享同一 _resolve_r03_upstream_cascade_plan 与精确事务失效实现。"""
        from stock_a.storage.postgres import source_raw as _sr_inv

        if upstream_module == "CROSS_SECTION":
            v_key: dict[str, Any] = {
                "price_capture_id": source_capture_id,
                "universe_version": universe_version_filter,
            }
        elif upstream_module == "CORE_PROFILE":
            if not profile_revision_filter:
                return
            v_key = {
                "source_capture_id": source_capture_id,
                "profile_revision": profile_revision_filter,
            }
        else:
            v_key = {
                "source_capture_id": source_capture_id,
                "profile_revision": profile_revision_filter,
            }
        versions_arg = [{"target_session": from_session, "version_key": v_key}]
        with _sr_inv._transaction(config, write=True, statement_timeout_ms=10000) as (
            _c,
            _cur,
            _,
        ):
            r03_plan = data_job_pg._resolve_r03_upstream_cascade_plan(
                _cur,
                module_code=upstream_module,
                versions=versions_arg,
                cutoff=cutoff,
            )
            if not (
                r03_plan["sector_partitions"]
                or r03_plan["rotation_episode_cutoffs"]
                or r03_plan["market_partitions"]
            ):
                return
            _cnts, _sess, inv_rows = (
                data_job_pg.apply_r03_upstream_cascade_invalidation(
                    _cur,
                    r03_plan=r03_plan,
                    cutoff=cutoff,
                    current_job_id=job_id,
                    error_code="UPSTREAM_RECOMPUTED",
                    error_message="上游输入已修订重算，R03后继需按依赖重算。",
                )
            )
            for inv_jid in {r[0] for r in inv_rows}:
                _cur.execute(
                    """
                    UPDATE stock.data_jobs j
                    SET status='INCOMPLETE',
                        completed_units=(
                            SELECT count(*) FROM stock.data_job_progress p
                            WHERE p.job_id=j.job_id
                              AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
                                               'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
                        ),
                        last_error_code='UPSTREAM_RECOMPUTED',
                        last_error_message='上游输入已修订重算，R03板块后继需重算。',
                        updated_at=clock_timestamp(), completed_at=clock_timestamp()
                    WHERE job_id=%s AND status IN ('COMPLETED','NO_CHANGE')
                    """,
                    (inv_jid,),
                )

    prepared_sector_snapshots = plan.get("_prepared_sector_snapshots")
    if (
        prepared_sector_snapshots is None
        and plan.get("mode") == "INITIALIZE"
        and plan.get("initialization_mode") == "REUSE_RAW"
    ):
        snapshot_pair = plan.get("sector_snapshot_pair")
        snapshot_ids = (
            snapshot_pair.get("snapshot_ids")
            if isinstance(snapshot_pair, Mapping)
            else None
        )
        if isinstance(snapshot_ids, Mapping):
            prepared_sector_snapshots = {
                "reuse_existing_snapshot_ids": dict(snapshot_ids)
            }
    if prepared_sector_snapshots is None and plan.get("mode") == "MANUAL_DAILY":
        from datetime import datetime
        from zoneinfo import ZoneInfo

        cutoff_day = date.fromisoformat(plan["research_cutoff"])
        if cutoff_day < datetime.now(ZoneInfo("Asia/Shanghai")).date():
            snapshot_pair = plan.get("sector_snapshot_pair")
            snapshot_ids = (
                snapshot_pair.get("snapshot_ids")
                if isinstance(snapshot_pair, Mapping)
                else None
            )
            if isinstance(snapshot_ids, Mapping):
                prepared_sector_snapshots = {
                    "reuse_existing_snapshot_ids": dict(snapshot_ids)
                }
    _persist_sector_membership_stage(
        config=config,
        job_id=job_id,
        plan=plan,
        progress_callback=progress_callback,
        begin_callback=begin_callback,
        prepared_snapshots=prepared_sector_snapshots,
    )

    official_sessions_for_sources: Sequence[date] | None = None
    if selected_sources & {
        "BAOSTOCK_PRICES",
        "BAOSTOCK_UNIVERSE",
        "BAOSTOCK_MARKET_INDICES",
    }:
        _calendar_id, _calendar_revision, official_sessions_for_sources = (
            _load_official_sessions(date.fromisoformat(plan["research_cutoff"]))
        )

    if "BAOSTOCK_UNIVERSE" in selected_sources:
        assert official_sessions_for_sources is not None
        universe_result = _execute_baostock_universe_stage(
            config=config,
            job_id=job_id,
            plan=plan,
            official_sessions=official_sessions_for_sources,
            progress_callback=progress_callback,
            begin_callback=begin_callback,
        )
        if isinstance(universe_result, Mapping) and isinstance(plan, dict):
            plan["_universe_source_capture_id"] = int(
                universe_result["source_capture_id"]
            )

    if "BAOSTOCK_MARKET_INDICES" in selected_sources:
        assert official_sessions_for_sources is not None
        index_result = _execute_baostock_market_indices_stage(
            config=config,
            job_id=job_id,
            plan=plan,
            official_sessions=official_sessions_for_sources,
            progress_callback=progress_callback,
            begin_callback=begin_callback,
        )
        if isinstance(index_result, Mapping) and isinstance(plan, dict):
            plan["_index_source_capture_id"] = int(index_result["source_capture_id"])

    source_capture_id: int | None = None
    if "BAOSTOCK_PRICES" in selected_sources:
        assert official_sessions_for_sources is not None
        source_result = _execute_baostock_source_stage(
            config=config,
            job_id=job_id,
            plan=plan,
            official_sessions=official_sessions_for_sources,
            progress_callback=progress_callback,
            begin_callback=begin_callback,
        )
        if not isinstance(source_result, Mapping) or source_result.get(
            "status"
        ) not in {
            "COMPLETED",
            "ALREADY_COMPLETE",
        }:
            raise DataJobError(
                str(
                    (source_result or {}).get(
                        "reason_code", "BAOSTOCK_SOURCE_CAPTURE_INCOMPLETE"
                    )
                ),
                "BaoStock paired capture未完成；依赖它的研究阶段保持未执行。",
                status=409,
            )
        source_capture_id = int(source_result["source_capture_id"])
    elif base_modules or research_modules:
        capture_inputs = plan.get("price_capture_inputs", ())
        capture_ids = {
            item.get("source_capture_id")
            for item in capture_inputs
            if isinstance(item, Mapping)
            and item.get("price_coordinate") in {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"}
        }
        window_status = (
            plan.get("source_capture_preview", {}).get("window_status")
            if isinstance(plan.get("source_capture_preview"), Mapping)
            else None
        )
        ep_caps = plan.get("endpoint_capture_ids_by_session")
        target_cap = (
            ep_caps.get(plan.get("target_session"))
            if isinstance(ep_caps, Mapping)
            else None
        )
        if (
            window_status == "PER_SESSION_PAIRED_CAPTURES"
            and type(target_cap) is int
            and not plan.get("missing_price_inputs")
        ):
            source_capture_id = int(target_cap)
        elif (
            len(capture_ids) != 1
            or type(next(iter(capture_ids), None)) is not int
            or not capture_inputs
        ):
            raise DataJobError(
                "BAOSTOCK_PAIRED_CAPTURE_REQUIRED",
                "派生任务必须解析到覆盖所需会话和双坐标的同一PG source_capture_id。",
                status=409,
            )
        else:
            source_capture_id = int(next(iter(capture_ids)))
    if isinstance(plan, dict) and source_capture_id is not None:
        plan["_paired_source_capture_id"] = source_capture_id

    if not base_modules and not research_modules:
        if selected_sources - _ALL_SOURCE_MODULES:
            raise DataJobError(
                "DATA_JOB_SOURCE_MODULE_INVALID",
                "来源层只能选择BaoStock价格、主资料/名录、参考指数和TDX行业/GN成员关系。",
                status=409,
            )
        return
    assert source_capture_id is not None

    cutoff = date.fromisoformat(plan["research_cutoff"])
    if "PRICE_LIMITS" in base_modules:
        from stock_a.research.price_limits import PRICE_LIMIT_CONTRACT_VERSION
        from stock_a.storage.postgres.price_limits import (
            persist_source_price_limits_for_session,
        )

        limit_days = tuple(
            date.fromisoformat(day)
            for day in plan.get("module_output_sessions", {}).get("PRICE_LIMITS", ())
        )
        for day in limit_days:
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "PRICE_LIMITS"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                report = persist_source_price_limits_for_session(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                )
                for unit in day_units:
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=day,
                        source_capture_id=source_capture_id,
                        calendar_revision=report["calendar_revision"],
                        producer_version=PRICE_LIMIT_CONTRACT_VERSION,
                        research_cutoff=cutoff,
                    )
                    if actual_rows != report["row_count"]:
                        raise DataJobError(
                            "PRICE_LIMITS_READBACK_MISMATCH",
                            f"涨跌停限价({day})独立PG读回行数({actual_rows})与预期({report['row_count']})不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="PRICE_LIMITS",
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=report["row_count"],
                        inserted_rows=report["row_count"],
                        quality_status="COMPLETE_RAW_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=(report.get("status") == "NO_CHANGE"),
                        producer_version=PRICE_LIMIT_CONTRACT_VERSION,
                        calendar_revision=report["calendar_revision"],
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    day_units,
                    stage_code="PRICE_LIMITS",
                    error=error,
                    message_prefix="涨跌停限价生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"涨跌停限价({day})未闭合：{str(error)[:300]}",
                    )
                )
    if "LIMIT_UP_LADDER" in base_modules:
        from stock_a.research.limit_up_ladder import LIMIT_UP_LADDER_CONTRACT_VERSION
        from stock_a.storage.postgres.limit_up_ladder import (
            persist_limit_up_streaks_for_session,
        )

        ladder_days = tuple(
            date.fromisoformat(day)
            for day in plan.get("module_output_sessions", {}).get("LIMIT_UP_LADDER", ())
        )
        for day in ladder_days:
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "LIMIT_UP_LADDER"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                report = persist_limit_up_streaks_for_session(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                )
                for unit in day_units:
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=day,
                        source_capture_id=source_capture_id,
                        calendar_revision=report["calendar_revision"],
                        producer_version=LIMIT_UP_LADDER_CONTRACT_VERSION,
                        research_cutoff=cutoff,
                    )
                    if actual_rows != report["row_count"]:
                        raise DataJobError(
                            "LIMIT_UP_LADDER_READBACK_MISMATCH",
                            f"连板梯队({day})独立PG读回行数({actual_rows})与预期({report['row_count']})不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="LIMIT_UP_LADDER",
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=report["row_count"],
                        inserted_rows=report["row_count"],
                        quality_status="COMPLETE_RAW_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=(report.get("status") == "NO_CHANGE"),
                        producer_version=LIMIT_UP_LADDER_CONTRACT_VERSION,
                        calendar_revision=report["calendar_revision"],
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    day_units,
                    stage_code="LIMIT_UP_LADDER",
                    error=error,
                    message_prefix="连板梯队生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"连板梯队({day})未闭合：{str(error)[:300]}",
                    )
                )
    if "CROSS_SECTION" in base_modules:
        from stock_a.application.source_cross_section import (
            persist_source_cross_section_for_session,
        )
        from stock_a.research.cross_section import CROSS_SECTION_CONTRACT_VERSION

        cross_days = tuple(
            date.fromisoformat(day)
            for day in plan.get("module_output_sessions", {}).get("CROSS_SECTION", ())
        )
        for day in cross_days:
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "CROSS_SECTION"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                selected_universe_capture_id = plan.get("_universe_source_capture_id")
                report = persist_source_cross_section_for_session(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    directory_capture_id=selected_universe_capture_id,
                    stock_basic_capture_id=selected_universe_capture_id,
                )
                if isinstance(plan, dict) and report.get("universe_version"):
                    plan.setdefault("_cross_section_scope_by_day", {})[
                        day.isoformat()
                    ] = {
                        "universe_version": str(report["universe_version"]),
                        "universe_capture_id": report.get("universe_capture_id"),
                        "contract_version": str(report["contract_version"]),
                    }
                expected_by_table = {
                    "stock.source_cross_section_day_facts": report["row_count"],
                    "stock.source_cross_section_endpoints": report["endpoint_count"],
                }
                for unit in day_units:
                    table = unit["business_table"]
                    expected_rows = expected_by_table[table]
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=table,
                        target_session=day,
                        source_capture_id=source_capture_id,
                        calendar_revision=report["calendar_revision"],
                        producer_version=CROSS_SECTION_CONTRACT_VERSION,
                        research_cutoff=cutoff,
                        universe_version=report["universe_version"],
                        universe_capture_id=report.get("universe_capture_id"),
                    )
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "CROSS_SECTION_READBACK_MISMATCH",
                            f"全市场横截面({day}, {table}, {report['universe_version']})独立PG读回行数({actual_rows})与预期({expected_rows})不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="CROSS_SECTION",
                        target_session=day,
                        business_table=table,
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=expected_rows,
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=(report.get("status") == "NO_CHANGE"),
                        producer_version=CROSS_SECTION_CONTRACT_VERSION,
                        calendar_revision=report["calendar_revision"],
                        universe_capture_id=report.get("universe_capture_id"),
                    )
                    resolved_unit_keys.add(unit_key(unit))
                if (
                    plan.get("operation") == "RECOMPUTE"
                    and report.get("status") != "NO_CHANGE"
                ):
                    _invalidate_r03_for_upstream_recompute(
                        day,
                        upstream_module="CROSS_SECTION",
                        universe_version_filter=report.get("universe_version"),
                    )
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    day_units,
                    stage_code="CROSS_SECTION",
                    error=error,
                    message_prefix="全市场横截面生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"全市场横截面({day})未闭合：{str(error)[:300]}",
                    )
                )
    if "MARKET_BREADTH" in base_modules:
        from stock_a.application.source_market_context import (
            compute_source_market_context,
            prepare_source_market_breadth_change,
            read_source_market_context,
        )
        from stock_a.research.source_market_breadth import (
            SOURCE_MARKET_BREADTH_CONTRACT,
        )
        from stock_a.storage.postgres.market_breadth import (
            persist_market_breadth,
            read_market_breadth_change,
        )

        target_days = tuple(
            date.fromisoformat(day)
            for day in plan.get("module_output_sessions", {}).get("MARKET_BREADTH", ())
        )
        for day in target_days:
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "MARKET_BREADTH"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                market = compute_source_market_context(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                )
                breadth_change = prepare_source_market_breadth_change(config, market)
                persisted = persist_market_breadth(
                    config, market, breadth_change=breadth_change
                )
                from stock_a.application.r03_formal_sector import (
                    produce_formal_market_breadth_for_session,
                )

                ordered_mkt_days = [
                    date.fromisoformat(s) for s in plan.get("ordered_sessions", ())
                ]
                mkt_idx = ordered_mkt_days.index(day) if day in ordered_mkt_days else -1
                mkt_t3_day = ordered_mkt_days[mkt_idx - 3] if mkt_idx >= 3 else None
                mkt_prev_t = ordered_mkt_days[mkt_idx - 1] if mkt_idx >= 1 else None
                mkt_prev_t3 = ordered_mkt_days[mkt_idx - 4] if mkt_idx >= 4 else None
                cap_by_day_mkt = plan.get("endpoint_capture_ids_by_session")
                cap_mkt_t = (
                    int(cap_by_day_mkt[day.isoformat()])
                    if isinstance(cap_by_day_mkt, Mapping)
                    and day.isoformat() in cap_by_day_mkt
                    else source_capture_id
                )
                cap_mkt_t3 = (
                    int(cap_by_day_mkt[mkt_t3_day.isoformat()])
                    if isinstance(cap_by_day_mkt, Mapping)
                    and mkt_t3_day is not None
                    and mkt_t3_day.isoformat() in cap_by_day_mkt
                    else cap_mkt_t
                )
                from stock_a.storage.postgres import source_raw as _sr_mkt

                with _sr_mkt._transaction(
                    config, write=False, statement_timeout_ms=5000
                ) as (
                    _mc,
                    _mcur,
                    _,
                ):
                    _mcur.execute(
                        """
                        SELECT DISTINCT profile_revision
                        FROM stock.source_symbol_price_profiles
                        WHERE source_capture_id=%s AND target_session=%s
                          AND price_coordinate='FORWARD_ADJUSTED'
                        """,
                        (cap_mkt_t, day),
                    )
                    revs_t = [str(r[0]) for r in _mcur.fetchall() if r[0]]
                    revs_t3: list[str] = []
                    if mkt_t3_day is not None:
                        _mcur.execute(
                            """
                            SELECT DISTINCT profile_revision
                            FROM stock.source_symbol_price_profiles
                            WHERE source_capture_id=%s AND target_session=%s
                              AND price_coordinate='FORWARD_ADJUSTED'
                            """,
                            (cap_mkt_t3, mkt_t3_day),
                        )
                        revs_t3 = [str(r[0]) for r in _mcur.fetchall() if r[0]]
                if len(revs_t) > 1 or len(revs_t3) > 1:
                    raise DataJobError(
                        "MARKET_BREADTH_PROFILE_AMBIGUOUS",
                        f"市场宽度({day}/{mkt_t3_day})存在多个Profile修订(t={revs_t}, t3={revs_t3})，拒绝静默选首行或退回raw。",
                        status=409,
                    )
                formal_prof_t = revs_t[0] if len(revs_t) == 1 else None
                formal_prof_t3 = revs_t3[0] if len(revs_t3) == 1 else None
                cs_t = (
                    plan.get("_cross_section_scope_by_day", {}).get(day.isoformat())
                    if isinstance(plan.get("_cross_section_scope_by_day"), Mapping)
                    else None
                )
                cs_t3 = (
                    plan.get("_cross_section_scope_by_day", {}).get(
                        mkt_t3_day.isoformat()
                    )
                    if mkt_t3_day is not None
                    and isinstance(plan.get("_cross_section_scope_by_day"), Mapping)
                    else None
                )
                formal_breadth_res = produce_formal_market_breadth_for_session(
                    config,
                    target_session=day,
                    t3_session=mkt_t3_day,
                    source_capture_id=cap_mkt_t,
                    source_capture_id_t3=cap_mkt_t3,
                    profile_revision=formal_prof_t,
                    profile_revision_t3=formal_prof_t3,
                    prev_session_t=mkt_prev_t,
                    prev_session_t3=mkt_prev_t3,
                    universe_version_t=cs_t.get("universe_version")
                    if isinstance(cs_t, Mapping)
                    else None,
                    universe_version_t3=cs_t3.get("universe_version")
                    if isinstance(cs_t3, Mapping)
                    else None,
                    universe_capture_id=plan.get("_universe_source_capture_id"),
                    allow_revision=bool(plan.get("allow_revision", False)),
                )
                readback = read_source_market_context(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    include_symbols=True,
                )
                change_readback = read_market_breadth_change(
                    config,
                    target_session=day,
                    source_capture_id=source_capture_id,
                    calendar_revision=market.calendar["calendar_revision"],
                    contract_version=SOURCE_MARKET_BREADTH_CONTRACT,
                )
                stored_change = change_readback.get("change") or {}
                symbol_readback_mismatches = _market_breadth_symbol_readback_mismatches(
                    market.calculation, readback.calculation
                )
                change_readback_mismatches = _market_breadth_change_readback_mismatches(
                    breadth_change, stored_change
                )

                if (
                    readback.pg_result.get("storage_status") != "SAVED"
                    or readback.cohort.source_capture_id != source_capture_id
                    or readback.calculation.n_source != market.calculation.n_source
                    or readback.calculation.counts != market.calculation.counts
                    or readback.calculation.b_fixed_source
                    != market.calculation.b_fixed_source
                    or symbol_readback_mismatches
                    or change_readback.get("storage_status") != "SAVED"
                    or change_readback_mismatches
                ):
                    raise DataJobError(
                        "MARKET_BREADTH_CROSS_TRANSACTION_READBACK_MISMATCH",
                        "市场宽度或跨日变化事实独立读回与本次计算不一致："
                        f"逐股={symbol_readback_mismatches}，"
                        f"变化={change_readback_mismatches}。",
                        status=409,
                    )
                for unit in day_units:
                    table = unit["business_table"]
                    if table == "stock.r03_formal_market_breadth_facts":
                        from stock_a.application.r03_formal_sector import (
                            R03_CONTRACT_VERSION,
                        )

                        actual_rows = data_job_pg.read_business_table_count(
                            config,
                            business_table=table,
                            target_session=day,
                            source_capture_id=cap_mkt_t,
                            profile_revision=formal_prof_t,
                            producer_version=R03_CONTRACT_VERSION,
                            universe_version=formal_breadth_res.get(
                                "universe_version_t"
                            ),
                            universe_version_t3=formal_breadth_res.get(
                                "universe_version_t3"
                            ),
                            research_cutoff=cutoff,
                        )
                        expected_rows = 1
                        f_pg = formal_breadth_res.get("pg_result", {})
                        inserted_rows = int(f_pg.get("inserted", 0)) + int(
                            f_pg.get("updated", 0)
                        )
                        if actual_rows != expected_rows:
                            raise DataJobError(
                                "MARKET_BREADTH_ROW_COUNT_MISMATCH",
                                "正式市场宽度日级事实行数与预期(1)不一致。",
                                status=409,
                            )
                        with _sr_mkt._transaction(
                            config, write=False, statement_timeout_ms=5000
                        ) as (_rc, _rcur, _):
                            _rcur.execute(
                                """
                                SELECT status, n_t, n_t3, n_common, n_eligible,
                                       exact_numerator, exact_denominator
                                FROM stock.r03_formal_market_breadth_facts
                                WHERE trade_date=%s AND scope_key=%s
                                  AND universe_version_t=%s AND universe_version_t3=%s
                                  AND contract_version=%s
                                """,
                                (
                                    day,
                                    formal_breadth_res["scope_key"],
                                    str(formal_breadth_res["universe_version_t"]),
                                    str(formal_breadth_res["universe_version_t3"]),
                                    R03_CONTRACT_VERSION,
                                ),
                            )
                            _rb_row = _rcur.fetchone()
                        _exp_ex = formal_breadth_res.get("exact") or {}
                        _exp_tup = (
                            str(formal_breadth_res["status"]),
                            int(formal_breadth_res["n_t"]),
                            int(formal_breadth_res["n_t3"]),
                            int(formal_breadth_res["n_common"]),
                            int(formal_breadth_res["n_eligible"]),
                            _exp_ex.get("numerator"),
                            _exp_ex.get("denominator"),
                        )
                        if _rb_row is None or tuple(_rb_row) != _exp_tup:
                            raise DataJobError(
                                "MARKET_BREADTH_CROSS_TRANSACTION_READBACK_MISMATCH",
                                f"正式市场宽度异连接读回与计算不一致：readback={_rb_row}, expected={_exp_tup}",
                                status=409,
                            )
                        progress_callback(
                            job_id=job_id,
                            stage_code="MARKET_BREADTH",
                            target_session=day,
                            business_table=table,
                            source_capture_id=cap_mkt_t,
                            profile_history_session_count=None,
                            d1_history_session_count=None,
                            expected_rows=expected_rows,
                            inserted_rows=inserted_rows,
                            quality_status=f"COMPLETE_FORMAL_MARKET_{formal_breadth_res.get('status', 'UNKNOWN')}",
                            no_change=(
                                inserted_rows == 0 and int(f_pg.get("skipped", 0)) > 0
                            ),
                            profile_revision=formal_prof_t,
                            calendar_revision=market.calendar["calendar_revision"],
                            producer_version=R03_CONTRACT_VERSION,
                        )
                        resolved_unit_keys.add(unit_key(unit))
                        continue
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=table,
                        target_session=day,
                        source_capture_id=source_capture_id,
                        calendar_revision=market.calendar["calendar_revision"],
                        producer_version=SOURCE_MARKET_BREADTH_CONTRACT,
                        research_cutoff=cutoff,
                    )
                    expected_rows = (
                        1
                        if table
                        in {
                            "stock.source_market_breadth_days",
                            "stock.source_market_breadth_change_days",
                        }
                        else market.calculation.n_source
                    )
                    inserted_rows = (
                        int(persisted.get("status") == "WRITTEN")
                        if table == "stock.source_market_breadth_days"
                        else int(
                            persisted.get("breadth_change", {}).get("inserted_rows", 0)
                            or 0
                        )
                        if table == "stock.source_market_breadth_change_days"
                        else int(persisted.get("inserted_rows", 0) or 0)
                    )
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "MARKET_BREADTH_ROW_COUNT_MISMATCH",
                            "市场宽度事实行数与精确QFQ来源分母不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="MARKET_BREADTH",
                        target_session=day,
                        business_table=table,
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_rows,
                        quality_status=(
                            persisted.get("breadth_change", {}).get(
                                "comparison_status", "NOT_COMPUTED"
                            )
                            if table == "stock.source_market_breadth_change_days"
                            else "COMPLETE_QFQ_CAPTURE_READBACK"
                        ),
                        no_change=(
                            persisted.get("breadth_change", {}).get("status")
                            == "IDEMPOTENT_ALREADY_PRESENT"
                            if table == "stock.source_market_breadth_change_days"
                            else persisted.get("status") == "IDEMPOTENT_ALREADY_PRESENT"
                        ),
                        calendar_revision=market.calendar["calendar_revision"],
                        producer_version=SOURCE_MARKET_BREADTH_CONTRACT,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001 - 派生域保留确切失败原因。
                mark_units_incomplete(
                    day_units,
                    stage_code="MARKET_BREADTH",
                    error=error,
                    message_prefix="市场宽度QFQ生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"市场宽度({day.isoformat()})未闭合：{str(error)[:300]}",
                    )
                )

    profile_contract = load_price_profile_contract(_CONFIG_ROOT).document
    profile_revision_by_session: dict[str, str] = {}
    profile_blocked = set(plan.get("profile_insufficient_history_sessions", ()))
    profile_days = set(plan.get("profile_output_sessions", ())) - profile_blocked
    profile_windows = plan.get("profile_history_session_counts_by_session", {})
    for day_text in sorted(profile_days):
        day = date.fromisoformat(day_text)
        unit = next(
            (
                item
                for item in plan.get("stage_units", ())
                if item.get("stage_code") == "CORE_PROFILE"
                and item.get("target_session") == day
                and item.get("status") == "NOT_STARTED"
            ),
            None,
        )
        if unit is None:
            continue
        history_count = int(
            profile_windows.get(
                day_text, plan.get("profile_history_session_count") or 1
            )
        )
        begin_callback(job_id=job_id, units=[unit])
        day_cross_section_scope = (
            plan.get("_cross_section_scope_by_day", {}).get(day_text)
            if isinstance(plan.get("_cross_section_scope_by_day"), Mapping)
            else None
        )
        if not isinstance(day_cross_section_scope, Mapping):
            day_cross_section_scope = {}
        profile = run_source_symbol_price_profile(
            config,
            target_session=day,
            source_capture_id=source_capture_id,
            research_cutoff=cutoff,
            scope=SOURCE_SYMBOL_PROFILE_SCOPE,
            session_count=history_count,
            page_size=100,
            profile_contract=profile_contract,
            cross_section_universe_version=day_cross_section_scope.get(
                "universe_version"
            ),
            cross_section_universe_capture_id=day_cross_section_scope.get(
                "universe_capture_id"
            ),
        )
        profile_revision = profile.get("profile_revision")
        if isinstance(profile_revision, str) and profile_revision:
            profile_revision_by_session[day_text] = profile_revision
        progress_callback(
            job_id=job_id,
            stage_code="CORE_PROFILE",
            target_session=day,
            business_table="stock.source_symbol_price_profiles",
            source_capture_id=source_capture_id,
            profile_history_session_count=history_count,
            d1_history_session_count=None,
            expected_rows=int(profile.get("source_candidate_count", 0)),
            inserted_rows=int(profile.get("inserted_count", 0) or 0),
            quality_status=profile.get("status"),
            no_change=profile.get("status") == "IDEMPOTENT_READBACK",
            profile_revision=profile.get("profile_revision"),
            calendar_revision=profile.get("calendar_revision"),
            producer_version="STOCK_A_CORE_PROFILE_BAOSTOCK_QFQ_R2",
        )
        resolved_unit_keys.add(unit_key(unit))
        if (
            plan.get("operation") == "RECOMPUTE"
            and profile.get("status") != "IDEMPOTENT_READBACK"
        ):
            _invalidate_r03_for_upstream_recompute(
                day,
                upstream_module="CORE_PROFILE",
                profile_revision_filter=profile.get("profile_revision"),
            )

    # 成交参与使用同捕获、同日完整宽度分母和已保存QFQ Profile中的CNY成交比值。
    if "MARKET_PARTICIPATION" in base_modules:
        from stock_a.application.source_market_context import (
            read_source_market_context,
        )
        from stock_a.application.source_market_participation import (
            produce_market_participation,
        )

        target_days = tuple(
            date.fromisoformat(day)
            for day in plan.get("module_output_sessions", {}).get(
                "MARKET_PARTICIPATION", ()
            )
        )
        for day in target_days:
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "MARKET_PARTICIPATION"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                market = read_source_market_context(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    include_symbols=True,
                )
                if market.pg_result.get("storage_status") != "SAVED":
                    raise DataJobError(
                        "MARKET_PARTICIPATION_BREADTH_REQUIRED",
                        "成交参与依赖的精确市场宽度未保存。",
                        status=409,
                    )
                profile_revision = profile_revision_by_session.get(day.isoformat())
                if not profile_revision:
                    from stock_a.application.pg_price_profiles import (
                        source_symbol_price_profile_snapshots,
                    )

                    expected_profile_window = int(
                        profile_windows.get(
                            day.isoformat(),
                            plan.get("profile_history_session_count") or 1,
                        )
                    )
                    profile_catalog = source_symbol_price_profile_snapshots(
                        config, max_target_session=day
                    )
                    profile_candidates = [
                        item
                        for item in profile_catalog.get("snapshots", ())
                        if item.get("target_session") == day.isoformat()
                        and item.get("source_capture_id") == source_capture_id
                        and item.get("history_session_count") == expected_profile_window
                        and item.get("complete") is True
                    ]
                    profile_revisions = {
                        item.get("profile_revision")
                        for item in profile_candidates
                        if isinstance(item.get("profile_revision"), str)
                    }
                    if len(profile_revisions) != 1:
                        raise DataJobError(
                            "MARKET_PARTICIPATION_PROFILE_SCOPE_AMBIGUOUS",
                            "成交参与没有唯一绑定同日、同捕获、同窗口的完整Core/Profile修订。",
                            status=409,
                        )
                    profile_revision = next(iter(profile_revisions))
                outcome = produce_market_participation(
                    config,
                    market,
                    profile_revision=profile_revision,
                )
                readback = outcome.get("readback", {})
                profile_revision = outcome.get("profile_revision")
                producer_version = outcome.get("producer_version")
                unknown_count = len(
                    readback.get("denominator", {}).get("unknown_reason_counts", {})
                )
                for unit in day_units:
                    table = unit["business_table"]
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=table,
                        target_session=day,
                        source_capture_id=source_capture_id,
                        profile_revision=profile_revision,
                        producer_version=producer_version,
                        calendar_revision=market.calendar["calendar_revision"],
                        research_cutoff=cutoff,
                    )
                    expected_rows = (
                        1
                        if table == "stock.source_market_participation_days"
                        else unknown_count
                    )
                    inserted_rows = (
                        int(outcome["persistence"].get("status") == "WRITTEN")
                        if table == "stock.source_market_participation_days"
                        else int(
                            outcome["persistence"].get("inserted_reason_rows", 0) or 0
                        )
                    )
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "MARKET_PARTICIPATION_ROW_COUNT_MISMATCH",
                            "成交参与保存行数与UNKNOWN原因分解读回不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="MARKET_PARTICIPATION",
                        target_session=day,
                        business_table=table,
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_rows,
                        quality_status=str(readback.get("status") or "INCOMPLETE"),
                        no_change=outcome["persistence"].get("status")
                        == "IDEMPOTENT_ALREADY_PRESENT",
                        profile_revision=profile_revision,
                        producer_version=producer_version,
                        calendar_revision=outcome.get("calendar_revision"),
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001 - 派生域保留确切失败原因。
                mark_units_incomplete(
                    day_units,
                    stage_code="MARKET_PARTICIPATION",
                    error=error,
                    message_prefix="成交参与QFQ画像归约或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"成交参与({day.isoformat()})未闭合：{str(error)[:300]}",
                    )
                )

    # D0/Seed与D1共用同一日Capture键，算法窗口按各目标日显式选择。
    d0_days = (
        set(plan.get("d0_output_sessions", ())) if "D0" in research_modules else set()
    )
    d1_days = (
        set(plan.get("d1_output_sessions", ())) if "D1" in research_modules else set()
    )
    if d0_days or d1_days:
        from stock_a.application.source_research_day import run_source_research_day

        d0_history_windows = plan.get(
            "d0_profile_history_session_counts_by_session", {}
        )
        d1_history_windows = plan.get("d1_history_session_counts_by_session", {})
        for day_text in sorted(d0_days | d1_days):
            day = date.fromisoformat(day_text)
            include_d0 = day_text in d0_days
            include_d1 = day_text in d1_days
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") in {"STATIC_RESEARCH", "D0_D1"}
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
                and (unit.get("stage_code") != "STATIC_RESEARCH" or include_d0)
                and (
                    unit.get("stage_code") != "D0_D1"
                    or unit.get("business_table")
                    not in {
                        "stock.source_d0_launch_day_facts",
                        "stock.source_research_enrollments",
                    }
                    or include_d0
                )
                and (
                    unit.get("business_table")
                    not in {
                        "stock.source_d1_structure_day_facts",
                        "stock.source_d1_material_anchor_day_facts",
                    }
                    or include_d1
                )
            ]
            if not day_units:
                continue
            profile_history_count = int(
                d0_history_windows.get(
                    day_text,
                    profile_windows.get(
                        day_text, plan.get("profile_history_session_count") or 1
                    ),
                )
            )
            d1_history_count = int(
                d1_history_windows.get(
                    day_text, plan.get("d1_calculation_session_count") or 2
                )
            )
            begin_callback(job_id=job_id, units=day_units)
            try:
                day_cross_section_scope = (
                    plan.get("_cross_section_scope_by_day", {}).get(day_text)
                    if isinstance(plan.get("_cross_section_scope_by_day"), Mapping)
                    else None
                )
                if not isinstance(day_cross_section_scope, Mapping):
                    day_cross_section_scope = {}
                report = run_source_research_day(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    d1_history_session_count=d1_history_count,
                    persist=True,
                    history_session_count=(
                        profile_history_count if include_d0 else None
                    ),
                    include_d0=include_d0,
                    include_d1=include_d1,
                    include_static=include_d0,
                    cross_section_universe_version=day_cross_section_scope.get(
                        "universe_version"
                    ),
                    cross_section_universe_capture_id=day_cross_section_scope.get(
                        "universe_capture_id"
                    ),
                )
                if report.get("status") != "PERSISTED":
                    raise DataJobError(
                        "SOURCE_RESEARCH_DAY_NOT_PERSISTED",
                        "D0/D1单日生产器没有确认PG提交状态。",
                        status=409,
                    )
                persistence = report.get("persistence", {})
                static_result = report.get("static_persistence", {})
                profile_revision = static_result.get("profile_revision")
                if isinstance(plan, dict) and isinstance(static_result, Mapping):
                    selected_scope = plan.get("_cross_section_scope_by_day", {}).get(
                        day_text
                    )
                    if isinstance(selected_scope, dict):
                        for field in (
                            "universe_match_status",
                            "ranking_universe_count",
                            "evaluation_universe_count",
                        ):
                            selected_scope[field] = static_result.get(field)
                if isinstance(profile_revision, str) and profile_revision:
                    profile_revision_by_session[day_text] = profile_revision
                calendar_revision = report.get("calendar", {}).get("calendar_revision")
                if not isinstance(calendar_revision, str) or not calendar_revision:
                    raise DataJobError(
                        "SOURCE_RESEARCH_DAY_CALENDAR_MISSING",
                        "D0/D1持久化回执缺少官方日历修订。",
                        status=409,
                    )
                d1_cross_section_scope = report.get("cross_section_scope")
                if not isinstance(d1_cross_section_scope, Mapping):
                    d1_cross_section_scope = {}
                for unit in day_units:
                    table = unit["business_table"]
                    table_name = table.rsplit(".", 1)[-1]
                    if unit["stage_code"] == "STATIC_RESEARCH":
                        source_result = static_result
                        expected_rows = source_result.get("rows_expected", {}).get(
                            table_name
                        )
                        inserted_rows = source_result.get("rows_inserted", {}).get(
                            table_name, 0
                        )
                        producer_version = source_result.get(
                            "producer_versions", {}
                        ).get(table_name)
                        table_calendar_revisions = source_result.get(
                            "calendar_revisions", {}
                        ).get(table_name)
                        if isinstance(
                            table_calendar_revisions, Sequence
                        ) and not isinstance(table_calendar_revisions, (str, bytes)):
                            unique_calendar_revisions = tuple(
                                sorted(set(table_calendar_revisions))
                            )
                        elif isinstance(table_calendar_revisions, str):
                            unique_calendar_revisions = (table_calendar_revisions,)
                        else:
                            unique_calendar_revisions = ()
                        if len(unique_calendar_revisions) != 1:
                            raise DataJobError(
                                "SOURCE_RESEARCH_DAY_STATIC_CALENDAR_SCOPE_INVALID",
                                f"{table_name}静态事实没有唯一官方日历修订身份。",
                                status=409,
                            )
                        unit_calendar_revision = unique_calendar_revisions[0]
                    else:
                        source_result = persistence
                        if table_name == "source_research_enrollments":
                            enrollment = source_result.get("research_enrollments", {})
                            expected_rows = enrollment.get("expected")
                            inserted_rows = enrollment.get("inserted", 0)
                        else:
                            expected_rows = source_result.get("rows_expected", {}).get(
                                table_name
                            )
                            inserted_rows = source_result.get("rows_inserted", {}).get(
                                table_name, 0
                            )
                        producer_version = source_result.get(
                            "producer_versions", {}
                        ).get(table_name)
                        unit_calendar_revision = calendar_revision
                    if type(expected_rows) is not int or type(inserted_rows) is not int:
                        raise DataJobError(
                            "SOURCE_RESEARCH_DAY_TYPED_COUNT_MISSING",
                            f"单日生产回执缺少{table_name}的独立预期行数。",
                            status=409,
                        )
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=table,
                        target_session=day,
                        source_capture_id=source_capture_id,
                        profile_history_session_count=(
                            profile_history_count
                            if table_name == "source_pool_seed_day_facts"
                            else None
                        ),
                        d1_history_session_count=(
                            d1_history_count
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        profile_revision=(
                            profile_revision
                            if table_name == "source_pool_seed_day_facts"
                            else None
                        ),
                        universe_match_status=(
                            source_result.get("universe_match_status")
                            if table_name == "source_pool_seed_day_facts"
                            else None
                        ),
                        cross_section_universe_capture_id=(
                            source_result.get("cross_section_universe_capture_id")
                            if table_name == "source_pool_seed_day_facts"
                            else d1_cross_section_scope.get("universe_capture_id")
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        cross_section_universe_version=(
                            source_result.get("cross_section_universe_version")
                            if table_name == "source_pool_seed_day_facts"
                            else d1_cross_section_scope.get("universe_version")
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        cross_section_contract_version=(
                            source_result.get("cross_section_contract_version")
                            if table_name == "source_pool_seed_day_facts"
                            else d1_cross_section_scope.get("contract_version")
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        producer_version=producer_version,
                        calendar_revision=unit_calendar_revision,
                        research_cutoff=cutoff,
                    )
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "SOURCE_RESEARCH_DAY_ROW_COUNT_MISMATCH",
                            f"{table_name}独立PG行数{actual_rows}不等于单日typed预期{expected_rows}。"
                            f" capture={source_capture_id}"
                            f" producer_version={producer_version!r}"
                            f" calendar_revision={unit_calendar_revision!r}",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code=unit["stage_code"],
                        target_session=day,
                        business_table=table,
                        source_capture_id=source_capture_id,
                        profile_history_session_count=(
                            profile_history_count
                            if table_name
                            in {
                                "source_d0_scenario_day_facts",
                                "source_pool_seed_day_facts",
                            }
                            else None
                        ),
                        d1_history_session_count=(
                            d1_history_count
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        expected_rows=expected_rows,
                        inserted_rows=inserted_rows,
                        quality_status=str(source_result.get("status")),
                        no_change=inserted_rows == 0,
                        profile_revision=(
                            profile_revision
                            if table_name
                            in {
                                "source_d0_scenario_day_facts",
                                "source_pool_seed_day_facts",
                            }
                            else None
                        ),
                        universe_match_status=(
                            source_result.get("universe_match_status")
                            if table_name == "source_pool_seed_day_facts"
                            else None
                        ),
                        cross_section_universe_capture_id=(
                            source_result.get("cross_section_universe_capture_id")
                            if table_name == "source_pool_seed_day_facts"
                            else d1_cross_section_scope.get("universe_capture_id")
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        cross_section_universe_version=(
                            source_result.get("cross_section_universe_version")
                            if table_name == "source_pool_seed_day_facts"
                            else d1_cross_section_scope.get("universe_version")
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        cross_section_contract_version=(
                            source_result.get("cross_section_contract_version")
                            if table_name == "source_pool_seed_day_facts"
                            else d1_cross_section_scope.get("contract_version")
                            if table_name
                            in {
                                "source_d1_structure_day_facts",
                                "source_d1_material_anchor_day_facts",
                            }
                            else None
                        ),
                        producer_version=producer_version,
                        calendar_revision=unit_calendar_revision,
                    )
                    resolved_unit_keys.add(unit_key(unit))
                if plan.get("operation") == "RECOMPUTE" and str(
                    source_result.get("status")
                ) not in {"IDEMPOTENT_READBACK", "NO_CHANGE"}:
                    _invalidate_r03_for_upstream_recompute(
                        day,
                        upstream_module="D0",
                        profile_revision_filter=profile_revision,
                    )
            except Exception as error:  # noqa: BLE001 - 故障按真实单日域披露。
                mark_units_incomplete(
                    day_units,
                    stage_code=None,
                    error=error,
                    message_prefix="D0/D1计算、typed写入或读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"D0/D1({day.isoformat()})未闭合：{str(error)[:300]}",
                    )
                )

    if "MARKET_INDICES_ENV" in base_modules:
        from stock_a.research.market_indices import MARKET_INDEX_CONTRACT_VERSION
        from stock_a.storage.postgres.market_indices import (
            persist_market_indices_and_environment_for_session,
        )
        from stock_a.storage.postgres.source_raw import _transaction as _raw_tx

        env_days = tuple(
            sorted(
                date.fromisoformat(day)
                for day in plan.get("module_output_sessions", {}).get(
                    "MARKET_INDICES_ENV", ()
                )
            )
        )
        for day in env_days:
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "MARKET_INDICES_ENV"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                idx_capture_id = plan.get("_index_source_capture_id")
                if idx_capture_id is None:
                    with _raw_tx(config, write=False, statement_timeout_ms=10000) as (
                        _,
                        cur,
                        _,
                    ):
                        cur.execute(
                            """SELECT MAX(source_capture_id)
                               FROM stock.source_market_index_daily
                               WHERE session_date = %s""",
                            (day,),
                        )
                        idx_cap_row = cur.fetchone()
                        idx_capture_id = (
                            int(idx_cap_row[0])
                            if idx_cap_row and idx_cap_row[0] is not None
                            else None
                        )
                if idx_capture_id is None:
                    raise DataJobError(
                        "MARKET_INDEX_DAILY_NOT_AVAILABLE",
                        f"参考指数与市场环境({day})缺少已落库的通用参考指数日线捕获。",
                        status=409,
                    )
                report = persist_market_indices_and_environment_for_session(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    stock_capture_id=source_capture_id,
                    index_capture_id=idx_capture_id,
                )
                expected_by_table = {
                    "stock.source_market_index_day_facts": (
                        report["index_factor_rows"],
                        idx_capture_id,
                        MARKET_INDEX_CONTRACT_VERSION,
                    ),
                    "stock.source_market_environment_day_facts": (
                        report["environment_rows"],
                        source_capture_id,
                        "MARKET_PRESENTATION_R1",
                    ),
                }
                for unit in day_units:
                    table = unit["business_table"]
                    exp_rows, cap_val, prod_ver = expected_by_table[table]
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=table,
                        target_session=day,
                        source_capture_id=cap_val,
                        calendar_revision=report["calendar_revision"],
                        producer_version=prod_ver,
                        research_cutoff=cutoff,
                    )
                    if actual_rows != exp_rows:
                        raise DataJobError(
                            "MARKET_INDICES_ENV_READBACK_MISMATCH",
                            f"市场指数与环境({day}, {table})独立PG读回行数({actual_rows})与预期({exp_rows})不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="MARKET_INDICES_ENV",
                        target_session=day,
                        business_table=table,
                        source_capture_id=cap_val,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=exp_rows,
                        inserted_rows=exp_rows,
                        quality_status="COMPLETE_INDEX_ENV_CROSS_TRANSACTION_READBACK",
                        no_change=(report.get("status") == "NO_CHANGE"),
                        producer_version=prod_ver,
                        calendar_revision=report["calendar_revision"],
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    day_units,
                    stage_code="MARKET_INDICES_ENV",
                    error=error,
                    message_prefix="参考指数因子与市场复合环境未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"市场指数与环境({day})未闭合：{str(error)[:300]}",
                    )
                )
    # 市场宽度先绑定paired capture的QFQ历史坐标，保留既有B_fixed_source分类公式。
    if "FOCUS_FORMAL" in research_modules:
        from stock_a.application.source_formal_admission_production import (
            run_source_formal_admission_range_from_saved,
        )

        for day_text in plan.get("module_output_sessions", {}).get("FOCUS_FORMAL", ()):
            day = date.fromisoformat(day_text)
            unit = next(
                (
                    item
                    for item in plan.get("stage_units", ())
                    if item.get("stage_code") == "FOCUS_FORMAL"
                    and item.get("target_session") == day
                    and item.get("business_table")
                    == "stock.source_d0_formal_admission_day_facts"
                    and item.get("status") == "NOT_STARTED"
                ),
                None,
            )
            if unit is None:
                continue
            begin_callback(job_id=job_id, units=[unit])
            try:
                profile_revision = profile_revision_by_session.get(day_text)
                if not profile_revision:
                    from stock_a.application.pg_price_profiles import (
                        source_symbol_price_profile_snapshots,
                    )

                    expected_profile_window = int(
                        profile_windows.get(
                            day_text,
                            plan.get("profile_history_session_count") or 1,
                        )
                    )
                    profile_catalog = source_symbol_price_profile_snapshots(
                        config, max_target_session=day
                    )
                    profile_revisions = {
                        item.get("profile_revision")
                        for item in profile_catalog.get("snapshots", ())
                        if item.get("target_session") == day_text
                        and item.get("source_capture_id") == source_capture_id
                        and item.get("history_session_count") == expected_profile_window
                        and item.get("complete") is True
                        and isinstance(item.get("profile_revision"), str)
                    }
                    if len(profile_revisions) != 1:
                        raise DataJobError(
                            "FOCUS_PROFILE_REVISION_AMBIGUOUS",
                            f"{day_text}没有唯一完整的Core/Profile修订。",
                            status=409,
                        )
                    profile_revision = next(iter(profile_revisions))
                    profile_revision_by_session[day_text] = profile_revision
                report = run_source_formal_admission_range_from_saved(
                    config,
                    start_session=day,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    profile_revisions_by_session={day_text: profile_revision},
                )
                reports = report.get("reports", ())
                if report.get("status") != "PERSISTED" or len(reports) != 1:
                    raise DataJobError(
                        str(report.get("reason_code") or "FOCUS_FORMAL_NOT_PERSISTED"),
                        "Focus正式准入未从精确D0来源事实生成并读回。",
                        status=409,
                    )
                day_report = reports[0]
                profile_revision = day_report.get("profile_revision")
                producer_version = day_report.get("raw_producer_version")
                calendar_revision = day_report.get("calendar_revision")
                expected_rows = int(day_report.get("expected_source_count") or 0)
                actual_rows = data_job_pg.read_business_table_count(
                    config,
                    business_table=unit["business_table"],
                    target_session=day,
                    source_capture_id=source_capture_id,
                    profile_revision=profile_revision,
                    producer_version=producer_version,
                    calendar_revision=calendar_revision,
                    research_cutoff=cutoff,
                )
                if (
                    type(producer_version) is not str
                    or type(calendar_revision) is not str
                    or not profile_revision
                    or expected_rows <= 0
                    or actual_rows != expected_rows
                ):
                    raise DataJobError(
                        "FOCUS_FORMAL_READBACK_MISMATCH",
                        "Focus正式准入读取键或完整来源池行数未闭合。",
                        status=409,
                    )
                progress_callback(
                    job_id=job_id,
                    stage_code="FOCUS_FORMAL",
                    target_session=day,
                    business_table=unit["business_table"],
                    source_capture_id=source_capture_id,
                    profile_history_session_count=None,
                    d1_history_session_count=None,
                    expected_rows=expected_rows,
                    inserted_rows=int(day_report.get("inserted_rows", 0) or 0),
                    quality_status="COMPLETE_QFQ_NON_PIT_CAPTURE_READBACK",
                    no_change=bool(day_report.get("idempotent_rerun")),
                    profile_revision=profile_revision,
                    producer_version=producer_version,
                    calendar_revision=calendar_revision,
                )
                resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001 - Focus缺口准确留在对应研究域。
                mark_units_incomplete(
                    [unit],
                    stage_code="FOCUS_FORMAL",
                    error=error,
                    message_prefix="Focus正式准入事实未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"Focus正式准入({day.isoformat()})未闭合：{str(error)[:300]}",
                    )
                )

    def paired_sector_snapshots() -> dict[str, int]:
        snapshot_pair = plan.get("sector_snapshot_pair", {})
        snapshot_ids = plan.get("sector_snapshot_ids")
        if not isinstance(snapshot_ids, Mapping):
            snapshot_ids = (
                snapshot_pair.get("snapshot_ids")
                if isinstance(snapshot_pair, Mapping)
                else None
            )
        if (
            not isinstance(snapshot_ids, Mapping)
            or set(snapshot_ids) != {"INDUSTRY", "CONCEPT"}
            or any(
                type(value) is not int or value <= 0 for value in snapshot_ids.values()
            )
            or snapshot_ids["INDUSTRY"] == snapshot_ids["CONCEPT"]
        ):
            raise DataJobError(
                "SECTOR_SNAPSHOT_PAIR_UNAVAILABLE",
                "板块研究必须绑定精确、互异的行业与GN概念成员快照。",
                status=409,
            )
        return {kind: int(snapshot_ids[kind]) for kind in ("INDUSTRY", "CONCEPT")}

    def profile_revision_for_session(
        day: date, *, capture_id: int | None = None
    ) -> str:
        day_text = day.isoformat()
        ep_cap = int(capture_id) if capture_id is not None else int(source_capture_id)
        from stock_a.application.pg_research import read_source_version_context
        from stock_a.storage.postgres.static_research_facts import (
            read_source_pool_seed_forward_scope,
        )

        source_version = read_source_version_context(
            config,
            target_session=day,
            research_cutoff=cutoff,
            source_capture_id=ep_cap,
            price_coordinate="FORWARD_ADJUSTED",
        )
        if (
            source_version.get("status") != "AVAILABLE"
            or source_version.get("version_status") != "COMPLETE"
            or source_version.get("source_capture_id") != ep_cap
            or source_version.get("price_coordinate") != "FORWARD_ADJUSTED"
            or source_version.get("time_role") != "NON_PIT_RETROSPECTIVE"
            or type(source_version.get("capture_expected_row_count")) is not int
            or source_version.get("capture_expected_row_count", 0) <= 0
        ):
            raise DataJobError(
                "SECTOR_PROFILE_SOURCE_NOT_CLOSED",
                f"{day_text}(capture={ep_cap})没有截止内闭合的BaoStock QFQ paired capture。",
                status=409,
            )
        expected_revision = (
            profile_revision_by_session.get(day_text)
            if ep_cap == source_capture_id
            else None
        )
        if expected_revision is None:
            from stock_a.application.pg_price_profiles import (
                source_symbol_price_profile_snapshots,
            )

            expected_window = profile_windows.get(day_text)
            if expected_window is None and day_text in set(
                plan.get("profile_output_sessions", ())
            ):
                expected_window = plan.get("profile_history_session_count") or 1
            profile_catalog = source_symbol_price_profile_snapshots(
                config, max_target_session=day
            )
            profile_candidates = {
                item.get("profile_revision")
                for item in profile_catalog.get("snapshots", ())
                if item.get("target_session") == day_text
                and item.get("source_capture_id") == ep_cap
                and (
                    expected_window is None
                    or item.get("history_session_count") == expected_window
                )
                and item.get("complete") is True
                and isinstance(item.get("profile_revision"), str)
            }
            if len(profile_candidates) != 1:
                raise DataJobError(
                    "SECTOR_PROFILE_REVISION_AMBIGUOUS",
                    f"{day_text}(capture={ep_cap})没有唯一完整的Core/Profile修订与窗口。",
                    status=409,
                )
            expected_revision = next(iter(profile_candidates))
        seed_scope = read_source_pool_seed_forward_scope(
            config,
            target_session=day,
            source_capture_id=ep_cap,
            expected_source_count=source_version["research_expected_row_count"],
            profile_revision=expected_revision,
        )
        revision = seed_scope.get("profile_revision")
        if (
            not isinstance(revision, str)
            or not revision
            or seed_scope.get("source_count")
            != source_version["research_expected_row_count"]
            or seed_scope.get("price_coordinate") != "FORWARD_ADJUSTED"
            or seed_scope.get("research_mode") != "NON_PIT_RETROSPECTIVE"
        ):
            raise DataJobError(
                "SECTOR_SEED_SCOPE_NOT_CLOSED",
                f"{day_text}(capture={ep_cap})的QFQ Seed/profile修订或完整分母未闭合。",
                status=409,
            )
        if revision != expected_revision:
            raise DataJobError(
                "SECTOR_PROFILE_REVISION_MISMATCH",
                f"{day_text}(capture={ep_cap})本次Core/Profile与已存Seed修订不一致。",
                status=409,
            )
        if ep_cap == source_capture_id:
            profile_revision_by_session[day_text] = revision
        return revision

    def inserted_count(value: Any) -> int:
        if type(value) is int:
            return value
        if isinstance(value, Mapping):
            return sum(item for item in value.values() if type(item) is int)
        return 0

    snapshot_ids = None
    if {
        "SECTOR_PULSE",
        "SECTOR_TRANSITION",
        "SECTOR_FOLLOWUP",
        "R03_SECTOR_NATIVE",
        "R03_SECTOR_ROTATION",
        "R03_SECTOR_OVERLAP",
        "R03_SECTOR_LOO",
    } & research_modules:
        snapshot_ids = paired_sector_snapshots()

    if "SECTOR_PULSE" in research_modules:
        from stock_a.application.source_sector_pulse import run_source_sector_pulse_day
        from stock_a.storage.postgres.sector_pulse_facts import (
            read_source_sector_pulse_day,
        )

        for day_text in plan.get("sector_pulse_sessions", ()):
            day = date.fromisoformat(day_text)
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "SECTOR_PULSE"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            previous_progress = data_job_pg.read_progress_unit(
                config,
                job_id=job_id,
                stage_code="SECTOR_PULSE",
                target_session=day,
                business_table=day_units[0]["business_table"],
            )
            if isinstance(previous_progress, Mapping):
                try:
                    from stock_a.application.pg_research import (
                        read_source_version_context,
                    )

                    profile_revision = profile_revision_for_session(day)
                    pulse_readback = read_source_sector_pulse_day(
                        config,
                        target_session=day,
                        source_capture_id=source_capture_id,
                        source_pool_count=int(
                            read_source_version_context(
                                config,
                                target_session=day,
                                research_cutoff=cutoff,
                                source_capture_id=source_capture_id,
                                price_coordinate="FORWARD_ADJUSTED",
                            )["capture_expected_row_count"]
                        ),
                        snapshot_ids=snapshot_ids,
                        profile_revision=profile_revision,
                    )
                    expected_by_kind = {
                        kind: int(
                            pulse_readback.get("by_kind", {})
                            .get(kind, {})
                            .get("read_board_count", 0)
                        )
                        for kind in ("INDUSTRY", "CONCEPT")
                    }
                    expected_rows = sum(expected_by_kind.values())
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=day_units[0]["business_table"],
                        target_session=day,
                        source_capture_id=source_capture_id,
                        profile_revision=profile_revision,
                        sector_snapshot_ids=snapshot_ids,
                        research_cutoff=cutoff,
                    )
                    if (
                        pulse_readback.get("status") == "AVAILABLE"
                        and pulse_readback.get("profile_revision") == profile_revision
                        and all(value > 0 for value in expected_by_kind.values())
                        and actual_rows == expected_rows
                    ):
                        for unit in day_units:
                            progress_callback(
                                job_id=job_id,
                                stage_code="SECTOR_PULSE",
                                target_session=day,
                                business_table=unit["business_table"],
                                source_capture_id=source_capture_id,
                                profile_history_session_count=None,
                                d1_history_session_count=None,
                                expected_rows=expected_rows,
                                inserted_rows=0,
                                quality_status=(
                                    "COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK"
                                ),
                                no_change=True,
                                profile_revision=profile_revision,
                                producer_version="SOURCE_CROSS_PERIOD_PULSE_R1",
                                sector_snapshot_ids=snapshot_ids,
                            )
                            resolved_unit_keys.add(unit_key(unit))
                        continue
                except Exception:  # noqa: BLE001 - 读回不满足时继续现役生产器重算。
                    pass
            begin_callback(job_id=job_id, units=day_units)
            try:
                profile_revision = profile_revision_for_session(day)
                report = run_source_sector_pulse_day(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    profile_revision=profile_revision,
                    industry_snapshot_id=snapshot_ids["INDUSTRY"],
                    concept_snapshot_id=snapshot_ids["CONCEPT"],
                )
                if (
                    report.get("source_capture_id") != source_capture_id
                    or report.get("price_coordinate") != "FORWARD_ADJUSTED"
                    or report.get("time_role") != "NON_PIT_RETROSPECTIVE"
                    or report.get("profile_revision") != profile_revision
                    or report.get("status")
                    not in {
                        "WRITTEN_AND_TYPED_READBACK_VERIFIED",
                        "IDEMPOTENT_ALREADY_PRESENT",
                    }
                ):
                    raise DataJobError(
                        "SECTOR_PULSE_CAPTURE_IDENTITY_MISMATCH",
                        "PULSE结果没有绑定预期paired capture、profile和QFQ坐标。",
                        status=409,
                    )
                readback = read_source_sector_pulse_day(
                    config,
                    target_session=day,
                    source_capture_id=source_capture_id,
                    source_pool_count=int(report["source_pool_count"]),
                    snapshot_ids=snapshot_ids,
                    profile_revision=profile_revision,
                )
                expected_by_kind = report.get("board_count_by_kind", {})
                if (
                    readback.get("status") != "AVAILABLE"
                    or readback.get("profile_revision") != profile_revision
                    or set(readback.get("by_kind", {})) != {"INDUSTRY", "CONCEPT"}
                    or any(
                        readback["by_kind"][kind].get("status") != "AVAILABLE"
                        or readback["by_kind"][kind].get("read_board_count")
                        != expected_by_kind.get(kind)
                        or readback["by_kind"][kind].get("expected_board_count")
                        != expected_by_kind.get(kind)
                        for kind in ("INDUSTRY", "CONCEPT")
                    )
                ):
                    raise DataJobError(
                        "SECTOR_PULSE_CROSS_TRANSACTION_READBACK_MISMATCH",
                        "PULSE行业/GN两类事实在独立PG事务中未按快照、profile与分母闭合。",
                        status=409,
                    )
                readback_state_counts = {
                    state: 0 for state in ("TRUE", "FALSE", "UNKNOWN")
                }
                for group in readback["by_kind"].values():
                    for fact in group.get("facts", {}).values():
                        state = fact.get("state")
                        if state not in readback_state_counts:
                            raise DataJobError(
                                "SECTOR_PULSE_READBACK_STATE_INVALID",
                                "独立读回存在无效PULSE三态。",
                                status=409,
                            )
                        readback_state_counts[state] += 1
                if readback_state_counts != report.get("pulse_state_counts"):
                    raise DataJobError(
                        "SECTOR_PULSE_READBACK_VALUE_MISMATCH",
                        "PULSE独立读回三态数量与本次算法输出不一致。",
                        status=409,
                    )
                expected_rows = sum(
                    int(expected_by_kind[kind]) for kind in ("INDUSTRY", "CONCEPT")
                )
                actual_rows = data_job_pg.read_business_table_count(
                    config,
                    business_table=day_units[0]["business_table"],
                    target_session=day,
                    source_capture_id=source_capture_id,
                    profile_revision=profile_revision,
                    sector_snapshot_ids=snapshot_ids,
                    research_cutoff=cutoff,
                )
                if actual_rows != expected_rows:
                    raise DataJobError(
                        "SECTOR_PULSE_TYPED_ROW_COUNT_MISMATCH",
                        "PULSE表独立PG行数与行业/GN完整板块分母不一致。",
                        status=409,
                    )
                persistence = report.get("persistence", {})
                inserted_rows = inserted_count(
                    persistence.get("rows_inserted", {}).get(
                        "source_sector_cross_period_pulse_facts", 0
                    )
                    if isinstance(persistence.get("rows_inserted"), Mapping)
                    else persistence.get("rows_inserted", 0)
                )
                for unit in day_units:
                    progress_callback(
                        job_id=job_id,
                        stage_code="SECTOR_PULSE",
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_rows,
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=persistence.get("status")
                        == "IDEMPOTENT_ALREADY_PRESENT",
                        profile_revision=profile_revision,
                        producer_version="SOURCE_CROSS_PERIOD_PULSE_R1",
                        sector_snapshot_ids=snapshot_ids,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001 - 该派生域保留精确失败原因。
                mark_units_incomplete(
                    day_units,
                    stage_code="SECTOR_PULSE",
                    error=error,
                    message_prefix="行业/GN PULSE QFQ生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"行业/GN PULSE({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "SECTOR_TRANSITION" in research_modules:
        from stock_a.application.source_sector_pulse import (
            run_source_sector_pulse_transition_day,
        )

        for target_text, previous_text in sorted(
            plan.get("sector_transition_pairs", {}).items()
        ):
            if previous_text is None:
                continue
            target_day = date.fromisoformat(target_text)
            previous_day = date.fromisoformat(previous_text)
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "SECTOR_TRANSITION"
                and unit.get("target_session") == target_day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            try:
                from stock_a.application.pg_research import (
                    read_source_version_context,
                )
                from stock_a.storage.postgres.sector_pulse_facts import (
                    read_source_sector_pulse_day,
                )
                from stock_a.storage.postgres.sector_pulse_transitions import (
                    read_source_sector_pulse_transitions,
                )

                previous_profile_revision = profile_revision_for_session(previous_day)
                target_profile_revision = profile_revision_for_session(target_day)
                previous_source = read_source_version_context(
                    config,
                    target_session=previous_day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    price_coordinate="FORWARD_ADJUSTED",
                )
                target_source = read_source_version_context(
                    config,
                    target_session=target_day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    price_coordinate="FORWARD_ADJUSTED",
                )
                previous_pulse = read_source_sector_pulse_day(
                    config,
                    target_session=previous_day,
                    source_capture_id=source_capture_id,
                    source_pool_count=int(
                        previous_source["capture_expected_row_count"]
                    ),
                    snapshot_ids=snapshot_ids,
                    profile_revision=previous_profile_revision,
                )
                target_pulse = read_source_sector_pulse_day(
                    config,
                    target_session=target_day,
                    source_capture_id=source_capture_id,
                    source_pool_count=int(target_source["capture_expected_row_count"]),
                    snapshot_ids=snapshot_ids,
                    profile_revision=target_profile_revision,
                )
                previous_result = {
                    **previous_pulse,
                    "target_session": previous_day.isoformat(),
                    "profile_revision": previous_profile_revision,
                    "source_version": previous_source,
                }
                target_result = {
                    **target_pulse,
                    "target_session": target_day.isoformat(),
                    "profile_revision": target_profile_revision,
                    "source_version": target_source,
                }
                _calendar_id, prior_calendar_revision, _ = _load_official_sessions(
                    target_day
                )
                stored_transition = read_source_sector_pulse_transitions(
                    config,
                    previous_result=previous_result,
                    target_result=target_result,
                    calendar_identity={
                        "calendar_id": plan["calendar_id"],
                        "calendar_revision": prior_calendar_revision,
                    },
                )
                transition_rows = {
                    kind: int(
                        stored_transition.get("by_kind", {})
                        .get(kind, {})
                        .get("read_board_count", 0)
                    )
                    for kind in ("INDUSTRY", "CONCEPT")
                }
                expected_rows = sum(transition_rows.values())
                actual_rows = data_job_pg.read_business_table_count(
                    config,
                    business_table=day_units[0]["business_table"],
                    target_session=target_day,
                    source_capture_id=source_capture_id,
                    profile_revision=target_profile_revision,
                    calendar_revision=prior_calendar_revision,
                    sector_snapshot_ids=snapshot_ids,
                    followup_target_sessions=(previous_day,),
                    research_cutoff=cutoff,
                )
                if (
                    stored_transition.get("status") == "AVAILABLE"
                    and all(value > 0 for value in transition_rows.values())
                    and actual_rows == expected_rows
                ):
                    for unit in day_units:
                        progress_callback(
                            job_id=job_id,
                            stage_code="SECTOR_TRANSITION",
                            target_session=target_day,
                            business_table=unit["business_table"],
                            source_capture_id=source_capture_id,
                            profile_history_session_count=None,
                            d1_history_session_count=None,
                            expected_rows=expected_rows,
                            inserted_rows=0,
                            quality_status=(
                                "COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK"
                            ),
                            no_change=True,
                            profile_revision=target_profile_revision,
                            producer_version=(
                                "SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1"
                            ),
                            calendar_revision=prior_calendar_revision,
                            sector_snapshot_ids=snapshot_ids,
                            followup_target_sessions=(previous_day,),
                        )
                        resolved_unit_keys.add(unit_key(unit))
                    continue
            except Exception:  # noqa: BLE001 - 旧行读回不完整时走正式转折生产器。
                pass
            begin_callback(job_id=job_id, units=day_units)
            try:
                previous_profile_revision = profile_revision_for_session(previous_day)
                target_profile_revision = profile_revision_for_session(target_day)
                report = run_source_sector_pulse_transition_day(
                    config,
                    previous_session=previous_day,
                    target_session=target_day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    previous_profile_revision=previous_profile_revision,
                    target_profile_revision=target_profile_revision,
                    industry_snapshot_id=snapshot_ids["INDUSTRY"],
                    concept_snapshot_id=snapshot_ids["CONCEPT"],
                )
                expected_by_kind = report.get("transition_rows_expected", {})
                expected_rows = sum(
                    int(expected_by_kind.get(kind, 0))
                    for kind in ("INDUSTRY", "CONCEPT")
                )
                readback = report.get("readback", {})
                if (
                    report.get("source_capture_id") != source_capture_id
                    or report.get("price_coordinate") != "FORWARD_ADJUSTED"
                    or report.get("time_role") != "NON_PIT_RETROSPECTIVE"
                    or report.get("readback_status") != "AVAILABLE"
                    or readback.get("status") != "AVAILABLE"
                    or expected_rows <= 0
                ):
                    raise DataJobError(
                        "SECTOR_TRANSITION_CAPTURE_READBACK_INVALID",
                        "相邻PULSE变化缺少同一paired capture的QFQ独立读回。",
                        status=409,
                    )
                calendar_revision = report.get("calendar_identity", {}).get(
                    "calendar_revision"
                )
                actual_rows = data_job_pg.read_business_table_count(
                    config,
                    business_table=day_units[0]["business_table"],
                    target_session=target_day,
                    source_capture_id=source_capture_id,
                    profile_revision=target_profile_revision,
                    calendar_revision=calendar_revision,
                    sector_snapshot_ids=snapshot_ids,
                    followup_target_sessions=(previous_day,),
                    research_cutoff=cutoff,
                )
                if actual_rows != expected_rows:
                    raise DataJobError(
                        "SECTOR_TRANSITION_TYPED_ROW_COUNT_MISMATCH",
                        "相邻变化typed表独立PG行数与两个板块快照分母不一致。",
                        status=409,
                    )
                persistence = report.get("persistence", {})
                for unit in day_units:
                    progress_callback(
                        job_id=job_id,
                        stage_code="SECTOR_TRANSITION",
                        target_session=target_day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_count(
                            persistence.get("rows_inserted", 0)
                        ),
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=persistence.get("status")
                        == "IDEMPOTENT_ALREADY_PRESENT",
                        profile_revision=target_profile_revision,
                        producer_version="SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1",
                        calendar_revision=calendar_revision,
                        sector_snapshot_ids=snapshot_ids,
                        followup_target_sessions=(previous_day,),
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001 - 相邻变化保留独立状态。
                mark_units_incomplete(
                    day_units,
                    stage_code="SECTOR_TRANSITION",
                    error=error,
                    message_prefix="行业/GN相邻变化QFQ生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"行业/GN相邻变化({target_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "SECTOR_FOLLOWUP" in research_modules:
        from stock_a.application.source_sector_pulse_followup import (
            run_source_sector_pulse_followup_day,
        )

        due_by_session = plan.get("sector_followup_due_targets", {})
        for day_text in plan.get("sector_followup_sessions", ()):
            due_targets = due_by_session.get(day_text, {})
            if not due_targets:
                continue
            day = date.fromisoformat(day_text)
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "SECTOR_FOLLOWUP"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                report = run_source_sector_pulse_followup_day(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                )
                persistence = report.get("persistence", {})
                input_scope = report.get("input", {})
                calendar_revision = input_scope.get("calendar_revision")
                profile_revision = input_scope.get("profile_revision")
                readback_status = report.get("readback_status")
                expected_by_table = {
                    "source_sector_pulse_followup_facts": persistence.get(
                        "rows_expected"
                    ),
                    "source_sector_pulse_cohort_group_facts": persistence.get(
                        "cohort_group_rows_expected"
                    ),
                    "source_sector_pulse_cohort_reason_facts": persistence.get(
                        "cohort_reason_rows_expected"
                    ),
                }
                due_sessions = tuple(
                    date.fromisoformat(value)
                    for _horizon, value in sorted(
                        (
                            (int(horizon), endpoint)
                            for horizon, endpoint in due_targets.items()
                        )
                    )
                )
                followup_checks = {
                    "producer_persisted": report.get("status") == "PERSISTED",
                    "source_capture": report.get("source_capture_id")
                    == source_capture_id,
                    "qfq_coordinate": report.get("price_coordinate")
                    == "FORWARD_ADJUSTED",
                    "non_pit_time_role": report.get("time_role")
                    == "NON_PIT_RETROSPECTIVE",
                    "independent_readback": report.get(
                        "independent_transaction_readback"
                    )
                    == "PASS",
                    "readback_status": readback_status in {"AVAILABLE", "NOT_DUE"},
                    "profile_revision": bool(profile_revision),
                    "calendar_revision": bool(calendar_revision),
                    "due_horizons": bool(due_sessions),
                    "horizons_complete": not report.get("incomplete_horizons"),
                    "snapshot_ids": isinstance(
                        input_scope.get("snapshot_ids"), Mapping
                    ),
                }
                if not all(followup_checks.values()):
                    failed_checks = [
                        check for check, passed in followup_checks.items() if not passed
                    ]
                    raise DataJobError(
                        "SECTOR_FOLLOWUP_CAPTURE_READBACK_INVALID",
                        "PULSE后继独立闭合核验失败："
                        + "、".join(failed_checks)
                        + f"；producer={report.get('status')!r}，"
                        + f"capture={report.get('source_capture_id')!r}/{source_capture_id!r}，"
                        + f"readback={report.get('independent_transaction_readback')!r}/"
                        + f"{readback_status!r}，"
                        + f"incomplete_horizons={len(report.get('incomplete_horizons') or ())}。",
                        status=409,
                    )
                for unit in day_units:
                    table_name = unit["business_table"].rsplit(".", 1)[-1]
                    expected_rows = expected_by_table.get(table_name)
                    if type(expected_rows) is not int:
                        raise DataJobError(
                            "SECTOR_FOLLOWUP_TYPED_COUNT_MISSING",
                            f"后继生产回执缺少{table_name}的typed预期行数。",
                            status=409,
                        )
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=day,
                        source_capture_id=source_capture_id,
                        profile_revision=profile_revision,
                        calendar_revision=calendar_revision,
                        sector_snapshot_ids=input_scope["snapshot_ids"],
                        followup_target_sessions=due_sessions,
                        research_cutoff=cutoff,
                    )
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "SECTOR_FOLLOWUP_TYPED_ROW_COUNT_MISMATCH",
                            f"{table_name}独立PG行数与到期QFQ后继typed预期不一致。",
                            status=409,
                        )
                    inserted_field = {
                        "source_sector_pulse_followup_facts": "rows_inserted",
                        "source_sector_pulse_cohort_group_facts": "cohort_group_rows_inserted",
                        "source_sector_pulse_cohort_reason_facts": "cohort_reason_rows_inserted",
                    }[table_name]
                    progress_callback(
                        job_id=job_id,
                        stage_code="SECTOR_FOLLOWUP",
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_count(
                            persistence.get(inserted_field, 0)
                        ),
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=persistence.get("status")
                        == "IDEMPOTENT_ALREADY_PRESENT",
                        profile_revision=profile_revision,
                        producer_version="SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1",
                        calendar_revision=calendar_revision,
                        sector_snapshot_ids=input_scope["snapshot_ids"],
                        followup_target_sessions=due_sessions,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001 - 每张后继事实表各自标记未闭合。
                mark_units_incomplete(
                    day_units,
                    stage_code="SECTOR_FOLLOWUP",
                    error=error,
                    message_prefix="PULSE后继QFQ生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"PULSE后继({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    r03_selected = research_modules & {
        "R03_SECTOR_NATIVE",
        "R03_SECTOR_ROTATION",
        "R03_SECTOR_OVERLAP",
        "R03_SECTOR_LOO",
    }
    if r03_selected:
        from stock_a.application.r03_formal_sector import (
            R03_CONTRACT_VERSION,
            run_r03_formal_sector_for_session,
        )

        all_r03_days = sorted(
            {
                day_text
                for mod in r03_selected
                for day_text in plan.get("module_output_sessions", {}).get(mod, ())
            }
        )
        for day_text in all_r03_days:
            day = date.fromisoformat(day_text)
            day_modules = {
                mod
                for mod in r03_selected
                if day_text in plan.get("module_output_sessions", {}).get(mod, ())
            }
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") in day_modules
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            cap_t = source_capture_id
            profile_revision = None
            resolved_snapshots: dict[str, int] | None = None
            try:
                cap_by_day = plan.get("endpoint_capture_ids_by_session")
                cap_t = (
                    int(cap_by_day[day_text])
                    if isinstance(cap_by_day, Mapping) and day_text in cap_by_day
                    else source_capture_id
                )
                profile_revision = profile_revision_for_session(day, capture_id=cap_t)
                snap_by_day = plan.get("sector_snapshot_ids_by_session")
                if isinstance(snap_by_day, Mapping) and day_text in snap_by_day:
                    resolved_snapshots = snap_by_day[day_text]
                else:
                    resolved_snapshots = (
                        snapshot_ids
                        if snapshot_ids is not None
                        else paired_sector_snapshots()
                    )
                ordered_days = [
                    date.fromisoformat(s) for s in plan.get("ordered_sessions", ())
                ] or [date.fromisoformat(d_txt) for d_txt in all_r03_days]
                idx_day = ordered_days.index(day) if day in ordered_days else -1
                t1_day = ordered_days[idx_day - 1] if idx_day >= 1 else None
                t3_day = ordered_days[idx_day - 3] if idx_day >= 3 else None
                cap_t1 = (
                    int(cap_by_day[t1_day.isoformat()])
                    if isinstance(cap_by_day, Mapping)
                    and t1_day is not None
                    and t1_day.isoformat() in cap_by_day
                    else (
                        None
                        if (
                            t1_day is not None
                            and isinstance(cap_by_day, Mapping)
                            and cap_by_day
                        )
                        else cap_t
                    )
                )
                cap_t3 = (
                    int(cap_by_day[t3_day.isoformat()])
                    if isinstance(cap_by_day, Mapping)
                    and t3_day is not None
                    and t3_day.isoformat() in cap_by_day
                    else (
                        None
                        if (
                            t3_day is not None
                            and isinstance(cap_by_day, Mapping)
                            and cap_by_day
                        )
                        else cap_t
                    )
                )

                def _safe_endpoint_profile_rev(
                    ep_day: date | None, *, ep_cap: int | None
                ) -> str | None:
                    if ep_day is None or ep_cap is None:
                        return None
                    try:
                        return profile_revision_for_session(ep_day, capture_id=ep_cap)
                    except DataJobError as err:
                        if err.code in {
                            "SECTOR_PROFILE_SOURCE_NOT_CLOSED",
                            "SECTOR_SEED_SCOPE_MISSING",
                        }:
                            return None
                        if err.code == "SECTOR_PROFILE_REVISION_AMBIGUOUS":
                            # 区分确实缺 Profile（0条）与多条完整 Profile 真歧义
                            from stock_a.application.pg_price_profiles import (
                                source_symbol_price_profile_snapshots,
                            )

                            cat = source_symbol_price_profile_snapshots(
                                config, max_target_session=ep_day
                            )
                            cands = {
                                it.get("profile_revision")
                                for it in cat.get("snapshots", ())
                                if it.get("target_session") == ep_day.isoformat()
                                and it.get("source_capture_id") == ep_cap
                                and it.get("complete") is True
                            }
                            if not cands:
                                return None
                        raise

                prof_rev_t1 = _safe_endpoint_profile_rev(t1_day, ep_cap=cap_t1)
                prof_rev_t3 = _safe_endpoint_profile_rev(t3_day, ep_cap=cap_t3)
                cs_by_day = (
                    plan.get("_cross_section_scope_by_day", {})
                    if isinstance(plan.get("_cross_section_scope_by_day"), Mapping)
                    else {}
                )
                u_ver_t = (
                    cs_by_day.get(day_text, {}).get("universe_version")
                    if isinstance(cs_by_day.get(day_text), Mapping)
                    else None
                )
                u_ver_t1 = (
                    cs_by_day.get(t1_day.isoformat(), {}).get("universe_version")
                    if t1_day is not None
                    and isinstance(cs_by_day.get(t1_day.isoformat()), Mapping)
                    else None
                )
                u_ver_t3 = (
                    cs_by_day.get(t3_day.isoformat(), {}).get("universe_version")
                    if t3_day is not None
                    and isinstance(cs_by_day.get(t3_day.isoformat()), Mapping)
                    else None
                )
                if isinstance(snap_by_day, Mapping):
                    snaps_for_endpoints = {
                        "t": resolved_snapshots,
                        "t1": (
                            snap_by_day.get(t1_day.isoformat())
                            if t1_day is not None and t1_day.isoformat() in snap_by_day
                            else None
                        ),
                        "t3": (
                            snap_by_day.get(t3_day.isoformat())
                            if t3_day is not None and t3_day.isoformat() in snap_by_day
                            else None
                        ),
                    }
                else:
                    snaps_for_endpoints = {
                        "t": resolved_snapshots,
                        "t1": resolved_snapshots,
                        "t3": resolved_snapshots,
                    }
                r03_res = run_r03_formal_sector_for_session(
                    config,
                    target_session=day,
                    t1_session=t1_day,
                    t3_session=t3_day,
                    source_capture_id=cap_t,
                    profile_revision=profile_revision,
                    profile_revision_t1=prof_rev_t1,
                    profile_revision_t3=prof_rev_t3,
                    source_capture_id_t1=cap_t1,
                    source_capture_id_t3=cap_t3,
                    universe_version=u_ver_t,
                    universe_version_t1=u_ver_t1,
                    universe_version_t3=u_ver_t3,
                    universe_capture_id=plan.get("_universe_source_capture_id"),
                    strict_endpoints=True,
                    snapshot_ids=resolved_snapshots,
                    snapshot_ids_by_session=snaps_for_endpoints,
                    modules=day_modules,
                    allow_revision=bool(plan.get("allow_revision", False)),
                )
                expected_map = r03_res.get("expected_rows")
                if not isinstance(expected_map, Mapping):
                    raise DataJobError(
                        "R03_STAGE_EXPECTED_ROWS_MISSING",
                        f"R03生产({day_text})未返回独立expected_rows声明。",
                    )
                for unit in day_units:
                    mod = unit["stage_code"]
                    if mod not in expected_map:
                        raise DataJobError(
                            "R03_STAGE_EXPECTED_ROWS_MISSING",
                            f"{mod}({day_text})缺少独立expected_rows声明。",
                        )
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=day,
                        source_capture_id=cap_t,
                        profile_revision=profile_revision,
                        sector_snapshot_ids=resolved_snapshots,
                        research_cutoff=cutoff,
                    )
                    if mod == "R03_SECTOR_NATIVE":
                        ins = int(r03_res["native_pg"]["inserted"]) + int(
                            r03_res["native_pg"]["updated"]
                        )
                        no_chg = ins == 0 and int(r03_res["native_pg"]["skipped"]) > 0
                    elif mod == "R03_SECTOR_ROTATION":
                        ins = int(
                            r03_res["rotation_pg"].get("board_days_inserted", 0)
                        ) + int(r03_res["rotation_pg"].get("board_days_updated", 0))
                        no_chg = (
                            ins == 0
                            and int(r03_res["rotation_pg"].get("board_days_skipped", 0))
                            > 0
                        )
                    elif mod == "R03_SECTOR_OVERLAP":
                        ins = int(r03_res["overlap_pg"]["clusters_inserted"]) + int(
                            r03_res["overlap_pg"]["clusters_updated"]
                        )
                        no_chg = (
                            ins == 0
                            and int(r03_res["overlap_pg"]["clusters_skipped"]) > 0
                        )
                    else:
                        ins = int(r03_res["loo_written"])
                        no_chg = ins == 0
                    expected_rows = int(expected_map[mod])
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "R03_STAGE_ROW_COUNT_MISMATCH",
                            f"{mod}({day_text})预期行数{expected_rows}，实际读回{actual_rows}。",
                        )
                    expected_keys_map = r03_res.get("expected_keys") or {}
                    expected_sub_keys_map = r03_res.get("expected_sub_keys") or {}
                    if mod in expected_keys_map:
                        from stock_a.storage.postgres.r03_formal_sector import (
                            verify_r03_partition_keys,
                        )

                        key_check = verify_r03_partition_keys(
                            config,
                            module_code=mod,
                            trade_date=day,
                            scope_key=str(r03_res["scope_key"]),
                            member_version=str(r03_res["member_version"]),
                            contract_version=R03_CONTRACT_VERSION,
                            expected_keys=set(expected_keys_map[mod]),
                            expected_sub_keys=expected_sub_keys_map.get(mod),
                        )
                        if not key_check.get("matched"):
                            raise DataJobError(
                                "R03_STAGE_KEY_SET_MISMATCH",
                                f"{mod}({day_text})业务键集合与预期不匹配：missing={key_check.get('missing')}, extra={key_check.get('extra')}, sub_missing={key_check.get('sub_missing')}, sub_extra={key_check.get('sub_extra')}。",
                            )
                    progress_callback(
                        job_id=job_id,
                        stage_code=mod,
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=cap_t,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_count(ins),
                        quality_status="COMPLETE_R03_FORMAL_SECTOR",
                        no_change=no_chg,
                        profile_revision=profile_revision,
                        producer_version=R03_CONTRACT_VERSION,
                        calendar_revision=plan.get("calendar_revision"),
                        sector_snapshot_ids=resolved_snapshots,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                for failed_unit in day_units:
                    mark_units_incomplete(
                        [failed_unit],
                        stage_code=str(failed_unit["stage_code"]),
                        error=error,
                        message_prefix="R03正式板块生产或独立读回未闭合",
                        unit_source_capture_id=cap_t,
                        profile_revision=profile_revision,
                        producer_version=R03_CONTRACT_VERSION,
                        industry_snapshot_id=(
                            resolved_snapshots.get("INDUSTRY")
                            if isinstance(resolved_snapshots, Mapping)
                            else None
                        ),
                        concept_snapshot_id=(
                            resolved_snapshots.get("CONCEPT")
                            if isinstance(resolved_snapshots, Mapping)
                            else None
                        ),
                    )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"R03正式板块({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "FOCUS_FORMAL" in research_modules:
        from stock_a.application.source_frozen_rules import (
            SourceFrozenRulesError,
            persist_source_computable_rules_from_saved,
        )

        unavailable_rule_codes = {
            "SOURCE_RULE_D0_SCOPE_NOT_CLOSED",
            "SOURCE_RULE_SOURCE_SCOPE_NOT_CLOSED",
            "SOURCE_RULE_SEED_SCOPE_NOT_CLOSED",
            "SOURCE_RULE_D1_SCOPE_NOT_CLOSED",
            "SOURCE_RULE_CALENDAR_REVISION_MISMATCH",
            "SOURCE_RULE_D1_WINDOW_INVALID",
            "SOURCE_RULE_RPS_SCOPE_NOT_CLOSED",
            "SOURCE_RULE_DIAGNOSTIC_SCOPE_NOT_CLOSED",
        }
        for day_text in plan.get("module_output_sessions", {}).get("FOCUS_FORMAL", ()):
            day = date.fromisoformat(day_text)
            unit = next(
                (
                    item
                    for item in plan.get("stage_units", ())
                    if item.get("stage_code") == "FOCUS_RESEARCH_CONTEXT"
                    and item.get("target_session") == day
                    and item.get("business_table") == "stock.source_rule_evaluations"
                    and item.get("status") == "NOT_STARTED"
                ),
                None,
            )
            if unit is None:
                continue
            begin_callback(job_id=job_id, units=[unit])
            try:
                context_snapshot_ids = paired_sector_snapshots()
            except DataJobError:
                context_snapshot_ids = None
            try:
                profile_revision = profile_revision_for_session(day)
                history_count = int(
                    profile_windows.get(
                        day_text, plan.get("profile_history_session_count") or 1
                    )
                )
                day_cross_section_scope = (
                    plan.get("_cross_section_scope_by_day", {}).get(day_text)
                    if isinstance(plan.get("_cross_section_scope_by_day"), Mapping)
                    else None
                )
                if not isinstance(day_cross_section_scope, Mapping):
                    day_cross_section_scope = {}
                report = persist_source_computable_rules_from_saved(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    profile_revision=profile_revision,
                    history_session_count=history_count,
                    snapshot_ids=context_snapshot_ids,
                    cross_section_universe_capture_id=day_cross_section_scope.get(
                        "universe_capture_id"
                    ),
                    cross_section_universe_version=day_cross_section_scope.get(
                        "universe_version"
                    ),
                    cross_section_contract_version=day_cross_section_scope.get(
                        "contract_version"
                    ),
                )
                producer_version = report.get("persistence", {}).get("rule_version")
                if report.get("status") not in {"PERSISTED", "IDEMPOTENT"}:
                    raise DataJobError(
                        "SOURCE_RULE_PRODUCER_NOT_PERSISTED",
                        "冻结规则唯一生产者没有确认PG提交。",
                        status=409,
                    )
                expected_rows = int(report["persistence"]["expected_rows"])
                d1_history_count = int(report["d1_history_session_count"])
                calendar_revision = report["calendar_revision"]
                actual_rows = data_job_pg.read_business_table_count(
                    config,
                    business_table=unit["business_table"],
                    target_session=day,
                    source_capture_id=source_capture_id,
                    profile_history_session_count=history_count,
                    d1_history_session_count=d1_history_count,
                    profile_revision=profile_revision,
                    producer_version=producer_version,
                    calendar_revision=calendar_revision,
                    research_cutoff=cutoff,
                )
                if (
                    type(producer_version) is not str
                    or actual_rows != expected_rows
                    or report.get("source_symbol_count") <= 0
                    or expected_rows != sum(report["rule_scope_counts"].values())
                ):
                    raise DataJobError(
                        "SOURCE_RULE_TYPED_READBACK_MISMATCH",
                        "冻结规则事实独立PG读回未闭合到精确规则子集分母。",
                        status=409,
                    )
                progress_callback(
                    job_id=job_id,
                    stage_code="FOCUS_RESEARCH_CONTEXT",
                    target_session=day,
                    business_table=unit["business_table"],
                    source_capture_id=source_capture_id,
                    profile_history_session_count=history_count,
                    d1_history_session_count=d1_history_count,
                    expected_rows=expected_rows,
                    inserted_rows=int(
                        report["persistence"].get("inserted_rows", 0) or 0
                    ),
                    quality_status=(
                        f"PARTIAL_RULE_SET_{len(report['supported_rule_ids'])}_OF_9"
                    ),
                    no_change=int(report["persistence"].get("inserted_rows", 0) or 0)
                    == 0,
                    profile_revision=profile_revision,
                    producer_version=producer_version,
                    calendar_revision=calendar_revision,
                    sector_snapshot_ids=context_snapshot_ids,
                )
                resolved_unit_keys.add(unit_key(unit))
            except SourceFrozenRulesError as error:
                if error.code in unavailable_rule_codes:
                    data_job_pg.mark_progress_unavailable(
                        config,
                        job_id=job_id,
                        stage_code="FOCUS_RESEARCH_CONTEXT",
                        target_session=day,
                        business_table=unit["business_table"],
                        status="NOT_COMPUTED",
                        error_code=error.code,
                        error_message=str(error)[:500],
                        source_capture_id=source_capture_id,
                    )
                    resolved_unit_keys.add(unit_key(unit))
                    continue
                mark_units_incomplete(
                    [unit],
                    stage_code="FOCUS_RESEARCH_CONTEXT",
                    error=error,
                    message_prefix="冻结规则计算或PG写入未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        error.code,
                        f"冻结规则({day_text})未闭合：{str(error)[:300]}",
                    )
                )
            except Exception as error:  # noqa: BLE001 - 冻结规则与其余Focus事实分开报告。
                mark_units_incomplete(
                    [unit],
                    stage_code="FOCUS_RESEARCH_CONTEXT",
                    error=error,
                    message_prefix="冻结规则计算或PG独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"冻结规则({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "FOCUS_FORMAL" in research_modules:
        from stock_a.application.source_t0_early_observation import (
            HYPOTHESIS_VERSION,
            SourceT0EarlyObservationError,
            persist_source_t0_early_observations_from_saved,
        )

        for day_text in plan.get("module_output_sessions", {}).get("FOCUS_FORMAL", ()):
            day = date.fromisoformat(day_text)
            unit = next(
                (
                    item
                    for item in plan.get("stage_units", ())
                    if item.get("stage_code") == "FOCUS_RESEARCH_CONTEXT"
                    and item.get("target_session") == day
                    and item.get("business_table")
                    == "stock.source_t0_early_observation_facts"
                    and item.get("status") == "NOT_STARTED"
                ),
                None,
            )
            if unit is None:
                continue
            begin_callback(job_id=job_id, units=[unit])
            try:
                snapshot_ids = paired_sector_snapshots()
                profile_revision = profile_revision_for_session(day)
                report = persist_source_t0_early_observations_from_saved(
                    config,
                    target_session=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    snapshot_ids=snapshot_ids,
                )
                seed_scope = report.get("seed_scope", {})
                expected_rows = report.get("context_rows_expected")
                if (
                    report.get("status") not in {"PERSISTED", "IDEMPOTENT"}
                    or report.get("target_session") != day_text
                    or report.get("source_capture_id") != source_capture_id
                    or report.get("profile_revision") != profile_revision
                    or seed_scope.get("price_coordinate") != "FORWARD_ADJUSTED"
                    or seed_scope.get("research_mode") != "NON_PIT_RETROSPECTIVE"
                    or type(expected_rows) is not int
                    or expected_rows <= 0
                ):
                    raise DataJobError(
                        "T0_EARLY_PRODUCER_SCOPE_INVALID",
                        "T0板内观察没有闭合到精确捕获、QFQ Seed修订和已存登记。",
                        status=409,
                    )
                actual_rows = data_job_pg.read_business_table_count(
                    config,
                    business_table=unit["business_table"],
                    target_session=day,
                    source_capture_id=source_capture_id,
                    profile_history_session_count=int(
                        seed_scope["history_session_count"]
                    ),
                    profile_revision=profile_revision,
                    producer_version=HYPOTHESIS_VERSION,
                    sector_snapshot_ids=snapshot_ids,
                    research_cutoff=cutoff,
                )
                if actual_rows != expected_rows:
                    raise DataJobError(
                        "T0_EARLY_TYPED_READBACK_MISMATCH",
                        "T0板内观察独立PG行数与本次保存的精确快照上下文不一致。",
                        status=409,
                    )
                progress_callback(
                    job_id=job_id,
                    stage_code="FOCUS_RESEARCH_CONTEXT",
                    target_session=day,
                    business_table=unit["business_table"],
                    source_capture_id=source_capture_id,
                    profile_history_session_count=int(
                        seed_scope["history_session_count"]
                    ),
                    d1_history_session_count=None,
                    expected_rows=expected_rows,
                    inserted_rows=int(
                        report.get("persistence", {}).get("rows_inserted", 0) or 0
                    ),
                    quality_status="COMPLETE_QFQ_NON_PIT_T0_CONTEXT_READBACK",
                    no_change=(
                        int(report.get("persistence", {}).get("rows_inserted", 0) or 0)
                        == 0
                    ),
                    profile_revision=profile_revision,
                    producer_version=HYPOTHESIS_VERSION,
                    calendar_revision=seed_scope["calendar_revision"],
                    sector_snapshot_ids=snapshot_ids,
                )
                resolved_unit_keys.add(unit_key(unit))
            except SourceT0EarlyObservationError as error:
                if error.code == "T0_EARLY_NO_STORED_ENROLLMENTS":
                    data_job_pg.mark_progress_unavailable(
                        config,
                        job_id=job_id,
                        stage_code="FOCUS_RESEARCH_CONTEXT",
                        target_session=day,
                        business_table=unit["business_table"],
                        status="NOT_COMPUTED",
                        error_code=error.code,
                        error_message=(
                            "目标日没有已存T0登记；保留NOT_COMPUTED，不生成空范围观察。"
                        ),
                        source_capture_id=source_capture_id,
                    )
                    resolved_unit_keys.add(unit_key(unit))
                    continue
                mark_units_incomplete(
                    [unit],
                    stage_code="FOCUS_RESEARCH_CONTEXT",
                    error=error,
                    message_prefix="T0早期观察PG生产未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        error.code,
                        f"T0早期观察({day_text})未闭合：{str(error)[:300]}",
                    )
                )
            except Exception as error:  # noqa: BLE001 - Focus中T0研究维度独立保留状态。
                mark_units_incomplete(
                    [unit],
                    stage_code="FOCUS_RESEARCH_CONTEXT",
                    error=error,
                    message_prefix="T0早期观察PG生产或异连接读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"T0早期观察({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "FORWARD_PATH" in research_modules:
        from stock_a.application.source_research_day import (
            persist_source_t1_forward_from_saved,
        )
        from stock_a.research.entry_context.sessions import _load_calendar
        from stock_a.storage.postgres.research_facts import PATH_VERSION

        _, _, cal_revision, cal_sessions = _load_calendar(cutoff)
        for day_text in plan.get("module_output_sessions", {}).get("FORWARD_PATH", ()):
            day = date.fromisoformat(day_text)
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "FORWARD_PATH"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                idx = cal_sessions.index(day)
                due_horizons = [
                    h
                    for h in (1, 3, 5, 10, 20)
                    if idx + h < len(cal_sessions) and cal_sessions[idx + h] <= cutoff
                ]
                total_inserted = 0
                for h in due_horizons:
                    res = persist_source_t1_forward_from_saved(
                        config,
                        target_session=day,
                        research_cutoff=cutoff,
                        source_capture_id=source_capture_id,
                        horizon=h,
                    )
                    total_inserted += int(res.get("inserted_paths", 0) or 0)
                for unit in day_units:
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=day,
                        source_capture_id=source_capture_id,
                        calendar_revision=cal_revision,
                        research_cutoff=cutoff,
                    )
                    if actual_rows <= 0:
                        raise DataJobError(
                            "FORWARD_PATH_READBACK_EMPTY",
                            f"Forward路径({day_text})独立读回为空。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="FORWARD_PATH",
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=actual_rows,
                        inserted_rows=total_inserted,
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=total_inserted == 0,
                        producer_version=PATH_VERSION,
                        calendar_revision=cal_revision,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    day_units,
                    stage_code="FORWARD_PATH",
                    error=error,
                    message_prefix="Forward路径生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"Forward路径({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "COHORT_STATS" in research_modules:
        from stock_a.application.source_cohort_stats import persist_cohort_stats
        from stock_a.research.entry_context.sessions import _load_calendar
        from stock_a.storage.postgres.research_facts import RULE_VERSION

        _, _, cal_revision, _ = _load_calendar(cutoff)
        for day_text in plan.get("module_output_sessions", {}).get("COHORT_STATS", ()):
            day = date.fromisoformat(day_text)
            day_units = [
                unit
                for unit in plan.get("stage_units", ())
                if unit.get("stage_code") == "COHORT_STATS"
                and unit.get("target_session") == day
                and unit.get("status") == "NOT_STARTED"
            ]
            if not day_units:
                continue
            begin_callback(job_id=job_id, units=day_units)
            try:
                res = persist_cohort_stats(
                    config,
                    target=day,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                )
                for unit in day_units:
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=day,
                        source_capture_id=source_capture_id,
                        calendar_revision=cal_revision,
                        research_cutoff=cutoff,
                    )
                    if actual_rows < int(res.get("groups", 0)):
                        raise DataJobError(
                            "COHORT_STATS_READBACK_MISMATCH",
                            f"Cohort分组统计({day_text})独立读回行数不足。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="COHORT_STATS",
                        target_session=day,
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=actual_rows,
                        inserted_rows=int(res.get("groups", 0)),
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=False,
                        producer_version=RULE_VERSION,
                        calendar_revision=cal_revision,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    day_units,
                    stage_code="COHORT_STATS",
                    error=error,
                    message_prefix="Cohort分组统计生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"Cohort分组统计({day_text})未闭合：{str(error)[:300]}",
                    )
                )

    if "FEP_BASELINE" in research_modules:
        from stock_a.application.fep_baseline import produce_fep_baseline
        from stock_a.research.entry_context.sessions import _load_calendar
        from stock_a.storage.postgres.research_facts import RULE_VERSION

        _, _, cal_revision, _ = _load_calendar(cutoff)
        fep_units = [
            unit
            for unit in plan.get("stage_units", ())
            if unit.get("stage_code") == "FEP_BASELINE"
            and unit.get("status") == "NOT_STARTED"
        ]
        if fep_units:
            begin_callback(job_id=job_id, units=fep_units)
            try:
                res = produce_fep_baseline(
                    config,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    # 仅使用已闭合到完整来源总体的现役规则键；对象级诊断不混入Cohort/FEP分母。
                    rule_ids=(
                        "LAUNCH_CONFIRM",
                        "RECOVERY_TURN",
                        "s1",
                        "s2",
                        "seed",
                    ),
                    horizons=(1, 3, 5),
                )
                write_result = res.get("write", {})
                inserted_rows = (
                    int(write_result.get("inserted_row_count", 0) or 0)
                    if isinstance(write_result, Mapping)
                    else 0
                )
                for unit in fep_units:
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=unit["target_session"],
                        source_capture_id=source_capture_id,
                        calendar_revision=cal_revision,
                        research_cutoff=cutoff,
                    )
                    verified_rows = (
                        write_result.get("verified_row_count")
                        if isinstance(write_result, Mapping)
                        else None
                    )
                    if actual_rows <= 0:
                        raise DataJobError(
                            "FEP_BASELINE_READBACK_EMPTY",
                            "FEP条件基线独立读回为空。",
                            status=409,
                        )
                    if type(verified_rows) is int and actual_rows != verified_rows:
                        raise DataJobError(
                            "FEP_BASELINE_READBACK_MISMATCH",
                            f"FEP条件基线独立读回({actual_rows})与事务内核验({verified_rows})不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="FEP_BASELINE",
                        target_session=unit["target_session"],
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=actual_rows,
                        inserted_rows=inserted_rows,
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=inserted_rows == 0,
                        producer_version=RULE_VERSION,
                        calendar_revision=cal_revision,
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    fep_units,
                    stage_code="FEP_BASELINE",
                    error=error,
                    message_prefix="FEP条件基线生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"FEP条件基线未闭合：{str(error)[:300]}",
                    )
                )

    if "OBSERVATION_SEGMENTS" in research_modules:
        from stock_a.application.source_observation_segments import (
            produce_source_observation_segments,
        )
        from stock_a.research.entry_context.sessions import _load_calendar
        from stock_a.storage.postgres.research_facts import RULE_VERSION

        _, _, cal_revision, _ = _load_calendar(cutoff)
        seg_units = [
            unit
            for unit in plan.get("stage_units", ())
            if unit.get("stage_code") == "OBSERVATION_SEGMENTS"
            and unit.get("status") == "NOT_STARTED"
        ]
        if seg_units:
            begin_callback(job_id=job_id, units=seg_units)
            try:
                res = produce_source_observation_segments(
                    config,
                    research_cutoff=cutoff,
                    source_capture_id=source_capture_id,
                    rule_ids=plan.get("observation_rule_ids"),
                    target_sessions=tuple(
                        date.fromisoformat(day)
                        for day in plan.get("module_output_sessions", {}).get(
                            "OBSERVATION_SEGMENTS", ()
                        )
                    ),
                    replace_known_incomplete_successors=plan.get(
                        "replace_known_incomplete_observation_successors", False
                    ),
                )
                for unit in seg_units:
                    target_key = unit["target_session"].isoformat()
                    expected_rows = int(
                        res.get("target_session_counts", {}).get(target_key, 0)
                    )
                    inserted_rows = int(
                        res.get("target_session_inserted_counts", {}).get(target_key, 0)
                    )
                    actual_rows = data_job_pg.read_business_table_count(
                        config,
                        business_table=unit["business_table"],
                        target_session=unit["target_session"],
                        source_capture_id=source_capture_id,
                        calendar_revision=cal_revision,
                        producer_version=RULE_VERSION,
                        research_cutoff=cutoff,
                        rule_ids=plan.get("observation_rule_ids"),
                    )
                    if actual_rows != expected_rows:
                        raise DataJobError(
                            "OBSERVATION_SEGMENTS_READBACK_MISMATCH",
                            "观察持续分段独立读回行数"
                            f"({actual_rows})与本次目标键预期({expected_rows})不一致。",
                            status=409,
                        )
                    progress_callback(
                        job_id=job_id,
                        stage_code="OBSERVATION_SEGMENTS",
                        target_session=unit["target_session"],
                        business_table=unit["business_table"],
                        source_capture_id=source_capture_id,
                        profile_history_session_count=None,
                        d1_history_session_count=None,
                        expected_rows=expected_rows,
                        inserted_rows=inserted_rows,
                        quality_status="COMPLETE_QFQ_NON_PIT_CROSS_TRANSACTION_READBACK",
                        no_change=(
                            inserted_rows == 0
                            and int(res.get("repaired_incomplete_successor_count", 0))
                            == 0
                            and int(res.get("corrected_deleted_count", 0)) == 0
                        ),
                        producer_version=RULE_VERSION,
                        calendar_revision=cal_revision,
                        rule_ids=plan.get("observation_rule_ids"),
                    )
                    resolved_unit_keys.add(unit_key(unit))
            except Exception as error:  # noqa: BLE001
                mark_units_incomplete(
                    seg_units,
                    stage_code="OBSERVATION_SEGMENTS",
                    error=error,
                    message_prefix="观察持续分段生产或独立读回未闭合",
                )
                incomplete_stage_errors.append(
                    (
                        str(getattr(error, "code", type(error).__name__)),
                        f"观察持续分段未闭合：{str(error)[:300]}",
                    )
                )

    pending_units = []
    for unit in plan.get("stage_units", ()):
        if (
            unit.get("status") not in {"NOT_STARTED", "RUNNING"}
            or unit_key(unit) in resolved_unit_keys
            or unit.get("stage_code")
            in {
                "SOURCE_BAOSTOCK_PRICES",
                "SOURCE_SECTOR_MEMBERS",
                "CORE_PROFILE",
            }
        ):
            continue
        stored = data_job_pg.read_progress_unit(
            config,
            job_id=job_id,
            stage_code=unit["stage_code"],
            target_session=unit["target_session"],
            business_table=unit["business_table"],
        )
        if (
            isinstance(stored, Mapping)
            and stored.get("status") in complete_progress_statuses
        ):
            resolved_unit_keys.add(unit_key(unit))
            continue
        pending_units.append(unit)
    if pending_units:
        for unit in pending_units:
            data_job_pg.mark_progress_unavailable(
                config,
                job_id=job_id,
                stage_code=unit["stage_code"],
                target_session=unit["target_session"],
                business_table=unit["business_table"],
                status="INCOMPLETE",
                error_code="DATA_JOB_STAGE_NOT_RESOLVED",
                error_message="本任务的现役处理器未闭合该事实单元；保留INCOMPLETE状态。",
                source_capture_id=source_capture_id,
            )
        raise DataJobError(
            "DATA_JOB_STAGE_NOT_RESOLVED",
            "本任务结束时仍有派生事实单元未完成或未读回。",
            status=409,
        )
    if incomplete_stage_errors:
        code, message = incomplete_stage_errors[0]
        raise DataJobError(
            code,
            message,
            status=409,
        )


__all__ = [
    "DataJobCoordinator",
    "DataJobError",
    "build_data_job_plan",
    "recent_official_sessions",
    "select_session_windows",
]
