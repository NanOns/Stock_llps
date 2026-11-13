"""从已保存的同日Profile生产市场成交参与摘要。"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from decimal import Decimal, localcontext
from fractions import Fraction
import re
from typing import Any, Mapping

from stock_a.domain.market_presentation import (
    classify_market_participation,
    exact_median,
)
from stock_a.storage.postgres.market_participation import (
    read_market_participation,
    read_market_participation_profiles,
    persist_market_participation,
)


_SOURCE_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_PROFILE_SCOPE = "SOURCE_SYMBOL_NON_PIT_PRICE_PROFILE"
_PRICE_COORDINATE = "RAW_UNADJUSTED"
_PIT_STATUS = "NON_PIT_RETROSPECTIVE"


class SourceMarketParticipationError(Exception):
    """市场成交参与输入缺失、版本不唯一或质量不合法。"""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        category: str = "NOT_EVALUABLE",
        database_write_attempted: bool = False,
    ):
        self.code = code
        self.category = category
        self.database_write_attempted = database_write_attempted
        super().__init__(message)


def _calendar_covers_target(
    profile_revision: str,
    *,
    target_session: date,
    calendar_id: str,
    calendar_revision: str,
) -> bool:
    if profile_revision != calendar_revision:
        components: dict[str, str] = {}
        for component in profile_revision.split("+"):
            year, separator, revision = component.partition(":")
            if not separator or len(year) != 4 or year in components:
                return False
            components[year] = revision
        if components.get(str(target_session.year)) != calendar_revision:
            return False
    return bool(calendar_id)


def _clock_identity(value: Any) -> str | None:
    if value is None:
        return None
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return f"NAIVE:{parsed.isoformat()}"
    return parsed.isoformat()


def calculate_market_participation(
    market: Any, profile_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    """对一个已保存市场宽度来源池做一次精确Profile中位数归约。"""
    if not profile_rows:
        raise SourceMarketParticipationError(
            "MARKET_PARTICIPATION_PROFILES_NOT_FOUND",
            "该精确来源包与捕获没有已保存Profile；不创建空市场参与事实。",
        )
    target = market.target_session
    cohort = market.cohort
    breadth_symbols = {item.source_symbol for item in market.calculation.symbols}
    if len(breadth_symbols) != market.calculation.n_source:
        raise SourceMarketParticipationError(
            "MARKET_PARTICIPATION_SOURCE_POOL_OPEN",
            "市场宽度来源池代码集合未闭合。",
        )

    profile_symbols: set[str] = set()
    profile_revisions: set[str] = set()
    producer_contracts: set[str] = set()
    producer_versions: set[str] = set()
    source_time_qualities: set[str] = set()
    source_available_at: set[str | None] = set()
    price_coordinates: set[str] = set()
    pit_statuses: set[str] = set()
    calendar_ids: set[str] = set()
    calendar_revisions: set[str] = set()
    windows: set[tuple[Any, ...]] = set()
    amount_unit_statuses: set[str] = set()
    observed: list[Fraction] = []
    unknown_reasons: Counter[str] = Counter()

    for row in profile_rows:
        symbol = row.get("source_symbol")
        if not isinstance(symbol, str) or _SOURCE_SYMBOL.fullmatch(symbol) is None:
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_SYMBOL_INVALID", "Profile来源代码格式非法。"
            )
        if symbol in profile_symbols:
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_PROFILE_DUPLICATE",
                "同一来源代码存在多个画像修订，不能混合计算。",
            )
        profile_symbols.add(symbol)
        if symbol not in breadth_symbols:
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_PROFILE_OUT_OF_SCOPE",
                "Profile包含精确市场宽度来源池之外的代码。",
            )
        market_code = symbol[:2]
        board = row.get("board")
        if (
            row.get("market") != market_code
            or board not in {"MAIN_BOARD", "GEM"}
            or (board == "GEM" and market_code != "SZ")
        ):
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_BOARD_SCOPE_INVALID",
                "画像市场或板别不属于沪深主板、创业板来源范围。",
            )
        if (
            row.get("source_capture_id") != cohort.source_capture_id
            or row.get("calculation_scope") != _PROFILE_SCOPE
            or row.get("source_time_quality") != cohort.source_time_quality
            or _clock_identity(row.get("source_available_at"))
            != _clock_identity(cohort.source_available_at)
            or row.get("price_coordinate") != _PRICE_COORDINATE
            or row.get("pit_status") != _PIT_STATUS
            or row.get("history_calendar_id") != market.calendar["calendar_id"]
        ):
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_PROFILE_BINDING_MISMATCH",
                "Profile与市场宽度的来源捕获、时钟、价格坐标或日历身份不一致。",
            )
        if not _calendar_covers_target(
            row.get("history_calendar_revision", ""),
            target_session=target,
            calendar_id=market.calendar["calendar_id"],
            calendar_revision=market.calendar["calendar_revision"],
        ):
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_PROFILE_CALENDAR_MISMATCH",
                "Profile历史日历不包含目标会话的当前官方日历修订。",
            )
        if (
            row.get("history_window_end") != target
            or row.get("history_requested_session_count")
            != row.get("history_stored_session_count")
            or row.get("history_records_complete") is not True
            or row.get("history_records_status") != "RECORDS_COMPLETE"
        ):
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_PROFILE_HISTORY_INCOMPLETE",
                "Profile历史会话窗口未完整闭合。",
            )

        profile_revisions.add(str(row.get("profile_revision")))
        producer_contracts.add(str(row.get("producer_contract")))
        producer_versions.add(str(row.get("producer_version")))
        source_time_qualities.add(str(row.get("source_time_quality")))
        source_available_at.add(_clock_identity(row.get("source_available_at")))
        price_coordinates.add(str(row.get("price_coordinate")))
        pit_statuses.add(str(row.get("pit_status")))
        calendar_ids.add(str(row.get("history_calendar_id")))
        calendar_revisions.add(str(row.get("history_calendar_revision")))
        amount_unit_statuses.add(str(row.get("amount_unit_status")))
        windows.add(
            (
                row.get("history_window_start"),
                row.get("history_window_end"),
                row.get("history_requested_session_count"),
                row.get("history_stored_session_count"),
            )
        )

        quality = row.get("amount_ratio20_quality")
        value = row.get("amount_ratio20")
        numerator = row.get("amount_ratio20_exact_numerator")
        denominator = row.get("amount_ratio20_exact_denominator")
        reason = row.get("amount_ratio20_reason")
        if quality == "OBSERVED":
            if (
                value is None
                or not isinstance(value, Decimal)
                or not value.is_finite()
                or numerator is None
                or denominator is None
                or int(numerator) < 0
                or int(denominator) <= 0
                or reason is not None
            ):
                raise SourceMarketParticipationError(
                    "MARKET_PARTICIPATION_RATIO_INVALID",
                    "OBSERVED金额比值缺少合法精确值或质量原因不一致。",
                )
            ratio = Fraction(int(numerator), int(denominator))
            with localcontext() as context:
                context.prec = 60
                numeric_error = abs(
                    value - Decimal(ratio.numerator) / Decimal(ratio.denominator)
                )
            if numeric_error > Decimal("1e-22"):
                raise SourceMarketParticipationError(
                    "MARKET_PARTICIPATION_RATIO_NUMERIC_MISMATCH",
                    "Profile金额比值NUMERIC与精确有理数不一致。",
                )
            observed.append(ratio)
        elif quality == "UNKNOWN":
            if (
                value is not None
                or numerator is not None
                or denominator is not None
                or not isinstance(reason, str)
                or not reason.strip()
            ):
                raise SourceMarketParticipationError(
                    "MARKET_PARTICIPATION_UNKNOWN_INVALID",
                    "UNKNOWN金额比值必须为空且保留明确原因。",
                )
            unknown_reasons[reason] += 1
        else:
            raise SourceMarketParticipationError(
                "MARKET_PARTICIPATION_QUALITY_INVALID",
                "Profile金额比值质量必须是OBSERVED或UNKNOWN。",
            )

    if not profile_revisions or len(profile_revisions) != 1:
        raise SourceMarketParticipationError(
            "MARKET_PARTICIPATION_PROFILE_REVISION_MIXED",
            "精确来源池缺少唯一适用的Profile修订。",
        )
    if any(
        len(values) != 1
        for values in (
            producer_contracts,
            producer_versions,
            source_time_qualities,
            source_available_at,
            price_coordinates,
            pit_statuses,
            calendar_ids,
            calendar_revisions,
            windows,
            amount_unit_statuses,
        )
    ):
        raise SourceMarketParticipationError(
            "MARKET_PARTICIPATION_PROFILE_SET_MIXED",
            "Profile集合混合生产者、窗口、单位质量、时钟或日历修订。",
        )

    missing_symbols = breadth_symbols - profile_symbols
    if missing_symbols:
        unknown_reasons["PROFILE_NOT_STORED"] += len(missing_symbols)
    if not profile_rows:
        raise SourceMarketParticipationError(
            "MARKET_PARTICIPATION_PROFILES_NOT_FOUND",
            "该来源范围没有已保存Profile；不写入空市场参与事实。",
        )

    median = exact_median(observed)

    window_start, window_end, requested, stored = next(iter(windows))
    profile_revision = next(iter(profile_revisions))
    producer_contract = next(iter(producer_contracts))
    producer_version = next(iter(producer_versions))
    profile_history_calendar_id = next(iter(calendar_ids))
    profile_history_calendar_revision = next(iter(calendar_revisions))
    amount_unit_status = next(iter(amount_unit_statuses))
    profile_unknown_count = sum(
        1 for row in profile_rows if row["amount_ratio20_quality"] == "UNKNOWN"
    )
    profile_count = len(profile_rows)
    missing_count = len(missing_symbols)
    assessable_count = len(observed)
    status = (
        "UNKNOWN_ZERO_EVALUABLE"
        if assessable_count == 0
        else "PARTIAL_WITH_UNKNOWN"
        if profile_unknown_count or missing_count
        else "COMPLETE"
    )
    return {
        "target_session": target,
        "source_package_id": cohort.source_package_id,
        "source_capture_id": cohort.source_capture_id,
        "contract_version": "MARKET_PARTICIPATION_R1",
        "profile_revision": profile_revision,
        "producer_contract": producer_contract,
        "producer_version": producer_version,
        "source_dataset_id": cohort.source_dataset_id,
        "source_time_quality": cohort.source_time_quality,
        "source_available_at": cohort.source_available_at,
        "source_clock_role": cohort.source_clock_role,
        "calendar_id": market.calendar["calendar_id"],
        "calendar_revision": market.calendar["calendar_revision"],
        "profile_history_calendar_id": profile_history_calendar_id,
        "profile_history_calendar_revision": profile_history_calendar_revision,
        "history_window_start": window_start,
        "history_window_end": window_end,
        "history_requested_session_count": requested,
        "history_stored_session_count": stored,
        "amount_unit_status": amount_unit_status,
        "price_coordinate": _PRICE_COORDINATE,
        "pit_status": _PIT_STATUS,
        "source_pool_count": market.calculation.n_source,
        "profile_count": profile_count,
        "assessable_count": assessable_count,
        "profile_unknown_count": profile_unknown_count,
        "profile_missing_count": missing_count,
        "median_numerator": median.numerator if median is not None else None,
        "median_denominator": median.denominator if median is not None else None,
        "median_value": _median_decimal(median),
        "classification": classify_market_participation(median),
        "status": status,
        "unknown_reason_counts": dict(unknown_reasons),
    }


def _median_decimal(value: Fraction | None) -> Decimal | None:
    if value is None:
        return None
    with localcontext() as context:
        context.prec = 60
        return Decimal(value.numerator) / Decimal(value.denominator)


def produce_market_participation(
    config: Mapping[str, Any], market: Any
) -> dict[str, Any]:
    """读取一次已存Profile集合并原子保存参与摘要。"""
    rows = read_market_participation_profiles(
        config,
        target_session=market.target_session,
        source_package_id=market.cohort.source_package_id,
        source_capture_id=market.cohort.source_capture_id,
    )
    result = calculate_market_participation(market, rows)
    persisted = persist_market_participation(config, result)
    readback = read_market_participation(config, market=market)
    expected_fraction = (
        None
        if result["median_numerator"] is None
        else Fraction(result["median_numerator"], result["median_denominator"])
    )
    actual_fraction = readback.get("metric", {}).get("exact")
    actual_fraction = (
        None
        if actual_fraction is None
        else Fraction(actual_fraction["numerator"], actual_fraction["denominator"])
    )
    if (
        readback.get("status") != result["status"]
        or actual_fraction != expected_fraction
        or readback.get("denominator", {}).get("source_pool_count")
        != result["source_pool_count"]
        or readback.get("denominator", {}).get("profile_count")
        != result["profile_count"]
        or readback.get("denominator", {}).get("assessable_count")
        != result["assessable_count"]
        or readback.get("denominator", {}).get("unknown_reason_counts")
        != result["unknown_reason_counts"]
    ):
        raise SourceMarketParticipationError(
            "MARKET_PARTICIPATION_CROSS_TRANSACTION_READBACK_MISMATCH",
            "成交参与事实独立事务读回与离线生产值不一致。",
            category="UNAVAILABLE",
            database_write_attempted=True,
        )
    return {"persistence": persisted, "readback": readback}


def read_source_market_participation(
    config: Mapping[str, Any], market: Any
) -> dict[str, Any]:
    """将只读PG参与事实绑定到现有精确市场宽度来源。"""
    return read_market_participation(config, market=market)
