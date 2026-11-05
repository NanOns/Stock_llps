"""为来源市场宽度CLI和组合阅读命令提供唯一只读装配。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
import re
from typing import Any, Mapping, Sequence

from stock_a.research.entry_context.errors import ResearchEntryError
from stock_a.research.entry_context.sessions import _load_calendar
from stock_a.research.source_market_breadth import (
    RawPriceObservation,
    SourceCohortIdentity,
    SourceMarketBreadthInput,
    SourceMarketBreadthResult,
    calculate_source_market_breadth,
)
from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_source_market_breadth_history,
)

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ELIGIBLE_CLASSES = (
    "COMPARABLE_GAIN",
    "COMPARABLE_LOSS",
    "COMPARABLE_BOTH_POSITIVE",
    "COMPARABLE_BOTH_NONPOSITIVE",
)


class SourceMarketContextError(Exception):
    """来源市场装配的明确拒绝或不可评价原因。"""

    def __init__(self, code: str, message: str, *, category: str):
        self.code = code
        self.category = category
        super().__init__(message)


@dataclass(frozen=True)
class SourceMarketContextRead:
    """一个精确Raw来源版本的市场宽度读取与内核结果。"""

    target_session: date
    research_cutoff: date
    calendar: Mapping[str, Any]
    pg_result: Mapping[str, Any]
    source_version: Mapping[str, Any]
    cohort: SourceCohortIdentity
    calculation: SourceMarketBreadthResult


def fraction_payload(value: Fraction | None) -> dict[str, Any] | None:
    """把精确Fraction序列化为现行CLI字段。"""
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 40
        decimal_value = Decimal(value.numerator) / Decimal(value.denominator)
        display = f"{decimal_value:.12f}"
    return {
        "numerator": value.numerator,
        "denominator": value.denominator,
        "fraction": str(value),
        "display_decimal_12dp": display,
        "unit": "fraction",
    }


def read_source_market_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    research_cutoff: date,
    package_sha256: str,
) -> SourceMarketContextRead:
    """按固定官方日历和精确Raw来源包读取并计算一次来源宽度。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
    ):
        raise SourceMarketContextError(
            "MARKET_BREADTH_DATE_AFTER_CUTOFF",
            "目标交易会话不得晚于显式研究截止日",
            category="ARGUMENT",
        )
    if not isinstance(package_sha256, str) or _SHA256.fullmatch(package_sha256) is None:
        raise SourceMarketContextError(
            "MARKET_BREADTH_PACKAGE_SHA_INVALID",
            "完整TDX来源包SHA必须是64位小写十六进制值",
            category="ARGUMENT",
        )

    try:
        payload, metadata, revisions, sessions = _load_calendar(target_session)
        year_metadata = next(
            item for item in metadata if item["year"] == target_session.year
        )
        calendar = {
            "calendar_id": payload["calendar_id"],
            "calendar_revision": payload["revision"],
            "calendar_sha256": year_metadata["resource_sha256"],
            "revision_chain": revisions,
            "sessions": sessions,
        }
        target_index = sessions.index(target_session)
    except (ResearchEntryError, ValueError) as error:
        raise SourceMarketContextError(
            "MARKET_BREADTH_CALENDAR_INVALID",
            str(error),
            category="ARGUMENT",
        ) from error
    if target_index < 4:
        raise SourceMarketContextError(
            "MARKET_BREADTH_HISTORY_TOO_SHORT",
            "版本化官方日历中目标日前不足四个会话",
            category="NOT_EVALUABLE",
        )

    session_window = calendar["sessions"][target_index - 4 : target_index + 1]
    try:
        pg_result = read_source_market_breadth_history(
            config,
            target_session=target_session,
            research_cutoff=research_cutoff,
            package_sha256=package_sha256,
            session_window=session_window,
        )
    except RawSourcePgError:
        raise

    source_version = pg_result["source_version"]
    status_counts = source_version["target_status_counts"]
    cohort = SourceCohortIdentity(
        namespace_id=pg_result["namespace_id"],
        target_session=target_session,
        package_sha256=package_sha256,
        source_content_id=source_version["source_content_id"],
        source_capture_id=source_version["source_capture_id"],
        source_dataset_id=source_version["source_dataset_id"],
        source_byte_count=source_version["source_byte_count"],
        parser_contract=source_version["parser_contract"],
        source_time_quality=source_version["source_time_quality"],
        source_available_at=source_version["source_available_at"],
        capture_read_started_at=source_version["capture_read_started_at"],
        capture_validated_at=source_version["capture_validated_at"],
        source_clock_role=source_version["source_clock_role"],
        raw_row_count=source_version["raw_row_count"],
        capture_expected_row_count=source_version["capture_expected_row_count"],
        target_present_count=status_counts["TARGET_PRESENT"],
        target_missing_count=status_counts["TARGET_MISSING"],
        read_failed_count=status_counts["READ_FAILED"],
        capture_digest=source_version["capture_digest"],
        observed_raw_digest=source_version["observed_raw_digest"],
        calendar_id=calendar["calendar_id"],
        calendar_revision=calendar["calendar_revision"],
        calendar_sha256=calendar["calendar_sha256"],
    )
    observations = tuple(
        RawPriceObservation(**row) for row in pg_result["history_rows"]
    )
    try:
        calculation = calculate_source_market_breadth(
            SourceMarketBreadthInput(
                cohort=cohort,
                source_symbols=pg_result["source_symbols"],
                research_cutoff=research_cutoff,
                calendar_id=calendar["calendar_id"],
                calendar_revision=calendar["calendar_revision"],
                calendar_sha256=calendar["calendar_sha256"],
                official_sessions=calendar["sessions"],
                observations=observations,
            )
        )
    except ValueError as error:
        raise SourceMarketContextError(
            "MARKET_BREADTH_INPUT_REJECTED",
            str(error),
            category="UNAVAILABLE",
        ) from error

    return SourceMarketContextRead(
        target_session=target_session,
        research_cutoff=research_cutoff,
        calendar=calendar,
        pg_result=pg_result,
        source_version=source_version,
        cohort=cohort,
        calculation=calculation,
    )


def source_market_context_payload(
    market: SourceMarketContextRead,
) -> dict[str, Any]:
    """输出市场宽度摘要，不暴露逐来源代码计算明细。"""
    calculation = market.calculation
    cohort = market.cohort
    source_version = market.source_version
    pg_result = market.pg_result
    window = calculation.session_window
    status = (
        "UNKNOWN_ZERO_ELIGIBLE"
        if calculation.n_eligible == 0
        else "PARTIAL_WITH_UNKNOWN"
        if calculation.counts["UNKNOWN"]
        else "COMPLETE"
    )
    return {
        "status": status,
        "contract_version": calculation.contract_version,
        "analysis_scope": "SOURCE_SYMBOL_COHORT_FIXED_AT_T",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_session": market.target_session.isoformat(),
        "research_cutoff": market.research_cutoff.isoformat(),
        "official_calendar": {
            "calendar_id": market.calendar["calendar_id"],
            "revision": market.calendar["calendar_revision"],
            "sha256": market.calendar["calendar_sha256"],
            "lookback_sessions": {
                "t_minus_4": window[0].isoformat(),
                "t_minus_3": window[1].isoformat(),
                "t_minus_2": window[2].isoformat(),
                "t_minus_1": window[3].isoformat(),
                "t": window[4].isoformat(),
            },
            "raw_price_sessions": [
                {"role": "t", "session_date": window[4].isoformat()},
                {"role": "t_minus_1", "session_date": window[3].isoformat()},
                {"role": "t_minus_3", "session_date": window[1].isoformat()},
                {"role": "t_minus_4", "session_date": window[0].isoformat()},
            ],
        },
        "source_cohort": {
            "namespace_id": cohort.namespace_id,
            "target_session": market.target_session.isoformat(),
            "package_sha256": cohort.package_sha256,
            "source_content_id": cohort.source_content_id,
            "source_capture_id": cohort.source_capture_id,
            "source_dataset_id": cohort.source_dataset_id,
            "source_byte_count": cohort.source_byte_count,
            "parser_contract": cohort.parser_contract,
            "capture_digest": cohort.capture_digest,
            "source_raw_digest": cohort.capture_digest,
            "recomputed_target_raw_digest": cohort.observed_raw_digest,
            "raw_digest_verified": cohort.capture_digest == cohort.observed_raw_digest,
            "raw_row_count": cohort.raw_row_count,
            "target_present_count": cohort.target_present_count,
            "target_missing_count": cohort.target_missing_count,
            "read_failed_count": cohort.read_failed_count,
            "source_time_quality": cohort.source_time_quality,
            "source_available_at": cohort.source_available_at,
            "capture_read_started_at": cohort.capture_read_started_at,
            "capture_validated_at": cohort.capture_validated_at,
            "source_clock_role": cohort.source_clock_role,
        },
        "metric": {
            "name": "B_fixed_source",
            "formula": "MEAN(I(ret1_t>0)-I(ret1_t_minus_3>0))",
            "exact": fraction_payload(calculation.b_fixed_source),
            "unit": "fraction",
            "range": ["-1", "1"],
            "sign_return_difference_is_main_value": False,
            "ma20_profile_breadth_is_main_value": False,
        },
        "denominator": {
            "N_source": calculation.n_source,
            "N_eligible": calculation.n_eligible,
            "UNKNOWN": calculation.counts["UNKNOWN"],
            "eligible_partition_sum": sum(
                calculation.counts[name] for name in _ELIGIBLE_CLASSES
            ),
            "counts": calculation.counts,
            "coverage": fraction_payload(calculation.coverage),
            "unknown_reason_counts": calculation.unknown_reason_counts,
        },
        "formal_market_breadth": {
            "status": "NOT_READY_HISTORICAL_UNIVERSE",
            "reason": "本值只描述t日Raw来源代码集合，不代表t与t-3各自正式上市总体。",
            "missing_dependencies": [
                "t与t-3官方有效上市身份总体及其精确比较集合",
                "稳定security_id和日期有效身份映射",
                "按当时available_at核定的历史成员资格与PIT证据",
            ],
        },
        "database_read": {
            "database": "stock_a",
            "schema": "stock",
            "account": "stock_a",
            "server": "127.0.0.1:5432",
            "transaction": pg_result["transaction"],
            "history_raw_row_count": pg_result["history_row_count"],
            "source_zip_opened": pg_result["source_zip_opened"],
            "database_write_attempted": pg_result["database_write_attempted"],
            "ddl_executed": pg_result["ddl_executed"],
            "dml_executed": pg_result["dml_executed"],
            "dml_rows_affected": 0,
        },
    }


def _clock_identity(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    else:
        return str(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return f"NAIVE:{parsed.isoformat()}"
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _calendar_revision_covers_target(
    observed_revision: Any, target_session: date, expected_revision: str
) -> bool:
    """校验长窗口日历修订串包含目标年份的精确官方修订。"""
    if observed_revision == expected_revision:
        return True
    if not isinstance(observed_revision, str):
        return False
    components: dict[str, str] = {}
    for component in observed_revision.split("+"):
        match = re.fullmatch(r"(\d{4}):(.+)", component)
        if match is None or match.group(1) in components:
            return False
        components[match.group(1)] = match.group(2)
    return components.get(str(target_session.year)) == expected_revision


def validate_source_market_context_binding(
    market: SourceMarketContextRead,
    *,
    strength_results: Sequence[Mapping[str, Any]],
    discovery: Mapping[str, Any],
) -> None:
    """拒绝将不同来源修订、Raw摘要或日历的市场值拼入组合结果。"""
    d1_source = discovery.get("source")
    d1_calendar = discovery.get("official_calendar")
    if not isinstance(d1_source, Mapping) or not isinstance(d1_calendar, Mapping):
        raise SourceMarketContextError(
            "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
            "D1结果缺少封口来源或官方日历身份，拒绝组合输出",
            category="IDENTITY_MISMATCH",
        )

    market_identity = {
        "namespace_id": market.pg_result["namespace_id"],
        "source_capture_id": market.source_version["source_capture_id"],
        "target_session": market.target_session.isoformat(),
        "package_sha256": market.cohort.package_sha256,
        "source_content_id": market.source_version["source_content_id"],
        "source_raw_digest": market.source_version["observed_raw_digest"],
        "raw_row_count": int(market.source_version["raw_row_count"]),
        "capture_expected_row_count": int(
            market.source_version["capture_expected_row_count"]
        ),
    }
    d1_identity = {key: d1_source.get(key) for key in market_identity}
    d1_identity["research_cutoff"] = discovery.get("research_cutoff")
    expected_identity = dict(market_identity)
    expected_identity["research_cutoff"] = market.research_cutoff.isoformat()
    market_calendar = {
        "calendar_id": market.calendar["calendar_id"],
        "calendar_revision": market.calendar["calendar_revision"],
        "calendar_sha256": market.calendar["calendar_sha256"],
    }
    d1_calendar_identity = {
        "calendar_id": d1_calendar.get("calendar_id"),
        "calendar_revision": d1_calendar.get("calendar_revision"),
        "calendar_sha256": d1_calendar.get("calendar_sha256"),
    }
    if (
        d1_identity != expected_identity
        or d1_source.get("target_raw_binding_verified") is not True
        or market.pg_result["target_raw_digest"] != d1_source.get("source_raw_digest")
        or market_calendar != d1_calendar_identity
        or _clock_identity(market.source_version.get("source_available_at"))
        != _clock_identity(d1_source.get("source_available_at"))
    ):
        raise SourceMarketContextError(
            "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
            "市场宽度与D1来源捕获、Raw摘要、时钟或官方日历不一致",
            category="IDENTITY_MISMATCH",
        )

    for strength in strength_results:
        strength_source = strength.get("target_source_set")
        strength_calendar = strength.get("official_calendar")
        if not isinstance(strength_source, Mapping) or not isinstance(
            strength_calendar, Mapping
        ):
            raise SourceMarketContextError(
                "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
                "行业或GN强弱结果缺少来源身份或官方日历",
                category="IDENTITY_MISMATCH",
            )
        strength_identity = {
            "namespace_id": strength.get("namespace_id"),
            "source_capture_id": strength_source.get("source_capture_id"),
            "target_session": strength.get("target_session"),
            "package_sha256": strength_source.get("package_sha256"),
        }
        expected_strength_identity = {
            key: market_identity[key] for key in strength_identity
        }
        expected_strength_identity["research_cutoff"] = (
            market.research_cutoff.isoformat()
        )
        strength_identity["research_cutoff"] = strength.get("research_cutoff")
        if (
            strength_identity != expected_strength_identity
            or strength_calendar.get("calendar_id") != market_calendar["calendar_id"]
            or not _calendar_revision_covers_target(
                strength_calendar.get("calendar_revision"),
                market.target_session,
                market_calendar["calendar_revision"],
            )
        ):
            raise SourceMarketContextError(
                "SOURCE_MARKET_CONTEXT_IDENTITY_MISMATCH",
                "市场宽度与行业/GN强弱使用的来源包、捕获或官方日历不一致",
                category="IDENTITY_MISMATCH",
            )


def source_market_not_evaluable_payload(
    *,
    source_identity: Mapping[str, Any],
    official_calendar: Mapping[str, Any],
    target_session: date,
    research_cutoff: date,
    reason_code: str,
    reason: str,
) -> dict[str, Any]:
    """在官方市场窗口不足时保留组合结果并将市场组件标为UNKNOWN。"""
    n_source = int(source_identity["raw_row_count"])
    counts = {name: 0 for name in (*_ELIGIBLE_CLASSES, "UNKNOWN")}
    counts["UNKNOWN"] = n_source
    unknown_reasons = {reason_code: n_source} if n_source else {}
    return {
        "status": "NOT_EVALUABLE",
        "contract_version": "MKT-02_RAW_POSITIVE_RETURN_STATE_DELTA_R1",
        "analysis_scope": "SOURCE_SYMBOL_COHORT_FIXED_AT_T",
        "identity_status": "SOURCE_SYMBOL_ONLY",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "target_session": target_session.isoformat(),
        "research_cutoff": research_cutoff.isoformat(),
        "official_calendar": {
            "calendar_id": official_calendar.get("calendar_id"),
            "revision": official_calendar.get("calendar_revision"),
            "sha256": official_calendar.get("calendar_sha256"),
            "lookback_sessions": None,
            "not_evaluable_reason": reason,
        },
        "source_cohort": {
            "namespace_id": source_identity.get("namespace_id"),
            "target_session": target_session.isoformat(),
            "package_sha256": source_identity.get("package_sha256"),
            "source_content_id": source_identity.get("source_content_id"),
            "source_capture_id": source_identity.get("source_capture_id"),
            "source_raw_digest": source_identity.get("observed_raw_digest"),
            "raw_row_count": n_source,
            "source_time_quality": source_identity.get("source_time_quality"),
            "source_available_at": source_identity.get("source_available_at"),
        },
        "metric": {
            "name": "B_fixed_source",
            "formula": "MEAN(I(ret1_t>0)-I(ret1_t_minus_3>0))",
            "exact": None,
            "unit": "fraction",
            "range": ["-1", "1"],
            "not_evaluable_reason": reason,
        },
        "denominator": {
            "N_source": n_source,
            "N_eligible": 0,
            "UNKNOWN": n_source,
            "eligible_partition_sum": 0,
            "counts": counts,
            "coverage": fraction_payload(Fraction(0, 1)) if n_source else None,
            "unknown_reason_counts": unknown_reasons,
        },
        "not_evaluable_reason_code": reason_code,
        "formal_market_breadth": {
            "status": "NOT_READY_HISTORICAL_UNIVERSE",
            "reason": "来源级市场指标不可评价；该状态不代表正式市场总体。",
            "missing_dependencies": [
                "t与t-3官方有效上市身份总体及其精确比较集合",
                "稳定security_id和日期有效身份映射",
                "按当时available_at核定的历史成员资格与PIT证据",
            ],
        },
        "database_read": {
            "transaction": "NOT_OPENED_MARKET_WINDOW_NOT_EVALUABLE",
            "database_write_attempted": False,
            "ddl_executed": False,
            "dml_executed": False,
            "dml_rows_affected": 0,
        },
    }
