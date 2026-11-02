"""固定来源代码集合的RAW价格市场宽度纯计算内核。"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from fractions import Fraction
import re
from typing import Any
import uuid


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_SYMBOL = re.compile(r"^(SH|SZ)\.[0-9]{6}$")
_PRICE_COORDINATE = "RAW_UNADJUSTED"
SOURCE_MARKET_BREADTH_CONTRACT = "MKT-02_RAW_POSITIVE_RETURN_STATE_DELTA_R1"


@dataclass(frozen=True)
class SourceCohortIdentity:
    """目标日来源集合身份和封口摘要的显式输入。"""

    namespace_id: str
    source_set_id: str
    ingress_attempt_id: str
    target_session: date
    package_sha256: str
    revision_id: int
    source_content_id: str
    source_capture_id: str
    source_dataset_id: str
    source_byte_count: int
    parser_contract: str
    seal_status: str
    durability_status: str
    durability_source_capture_id: str
    durability_source_digest: str
    commit_ack_observed_at: str
    seal_digest_sha256: str
    seal_writer_transaction_id: str
    source_time_quality: str
    source_available_at: str | None
    capture_read_started_at: str
    capture_validated_at: str
    source_clock_role: str
    durable_at: str
    sealed_at: str
    raw_row_count: int
    target_present_count: int
    target_missing_count: int
    read_failed_count: int
    source_digest: str
    source_raw_digest: str
    observed_raw_digest: str
    calendar_id: str
    calendar_revision: str
    calendar_sha256: str


@dataclass(frozen=True)
class RawPriceObservation:
    """一个精确来源包、官方会话和来源代码的RAW收盘观察。"""

    namespace_id: str
    source_symbol: str
    session_date: date
    package_sha256: str
    raw_close: Decimal | Fraction | int | None
    price_quality: str
    price_reason: str | None
    bar_status: str
    price_coordinate: str = _PRICE_COORDINATE


@dataclass(frozen=True)
class SourceMarketBreadthInput:
    """纯核全部输入；数据库、日历资源和文件读取由调用方负责。"""

    cohort: SourceCohortIdentity
    source_symbols: tuple[str, ...]
    research_cutoff: date
    calendar_id: str
    calendar_revision: str
    calendar_sha256: str
    official_sessions: tuple[date, ...]
    observations: tuple[RawPriceObservation, ...]


@dataclass(frozen=True)
class SourceMarketBreadthSymbol:
    """单一来源代码的两端收益、正收益状态差和独立未知原因。"""

    source_symbol: str
    ret1_t: Fraction | None
    ret1_t_minus_3: Fraction | None
    positive_t: int | None
    positive_t_minus_3: int | None
    delta_indicator: int | None
    classification: str
    primary_unknown_reason: str | None
    unknown_reason_detail: str | None


@dataclass(frozen=True)
class SourceMarketBreadthResult:
    """来源代码固定篮宽度及互斥分母。"""

    target_session: date
    research_cutoff: date
    session_window: tuple[date, date, date, date, date]
    contract_version: str
    cohort: SourceCohortIdentity
    n_source: int
    n_eligible: int
    counts: dict[str, int]
    unknown_reason_counts: dict[str, int]
    coverage: Fraction | None
    b_fixed_source: Fraction | None
    symbols: tuple[SourceMarketBreadthSymbol, ...]


def _fraction(value: Any) -> Fraction:
    if isinstance(value, bool):
        raise ValueError("布尔值不能作为RAW价格")
    if isinstance(value, Fraction):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("RAW价格必须为有限值")
        return Fraction(value)
    if isinstance(value, int):
        return Fraction(value)
    raise ValueError("RAW价格必须是精确Decimal、Fraction或整数")


def _validate_input(inputs: SourceMarketBreadthInput) -> tuple[date, ...]:
    cohort = inputs.cohort
    if type(inputs.research_cutoff) is not date:
        raise ValueError("研究截止日必须是date")
    if (
        type(cohort.target_session) is not date
        or cohort.target_session > inputs.research_cutoff
    ):
        raise ValueError("来源集合目标日必须是截止日前的明确会话")
    if (
        not isinstance(cohort.package_sha256, str)
        or _SHA256.fullmatch(cohort.package_sha256) is None
    ):
        raise ValueError("来源集合必须绑定完整小写包SHA-256")
    if (
        not isinstance(cohort.calendar_sha256, str)
        or _SHA256.fullmatch(cohort.calendar_sha256) is None
    ):
        raise ValueError("来源集合必须绑定完整日历SHA-256")
    try:
        uuid.UUID(cohort.namespace_id)
        uuid.UUID(cohort.source_set_id)
        uuid.UUID(cohort.ingress_attempt_id)
        uuid.UUID(cohort.source_capture_id)
        uuid.UUID(cohort.durability_source_capture_id)
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError(
            "namespace、来源集合、ingress attempt与capture身份必须是UUID"
        ) from error
    if cohort.source_dataset_id != "TDX_VIPDATA_PACKAGE":
        raise ValueError("市场宽度来源必须是已登记的TDX VIPDATA包")
    if (
        inputs.calendar_id != cohort.calendar_id
        or inputs.calendar_revision != cohort.calendar_revision
        or inputs.calendar_sha256 != cohort.calendar_sha256
    ):
        raise ValueError("官方日历身份与SEALED来源集合不一致")
    if cohort.seal_status != "SEALED":
        raise ValueError("来源集合未封口")
    if cohort.durability_status != "VERIFIED_POST_COMMIT_READBACK":
        raise ValueError("来源集合缺少提交后持久读回证明")
    if (
        type(cohort.revision_id) is not int
        or cohort.revision_id < 1
        or type(cohort.raw_row_count) is not int
        or cohort.raw_row_count < 0
        or any(
            type(value) is not int or value < 0
            for value in (
                cohort.target_present_count,
                cohort.target_missing_count,
                cohort.read_failed_count,
            )
        )
        or cohort.raw_row_count != len(inputs.source_symbols)
        or cohort.target_present_count
        + cohort.target_missing_count
        + cohort.read_failed_count
        != cohort.raw_row_count
        or cohort.read_failed_count != 0
    ):
        raise ValueError("来源集合修订或Raw行数/状态分母不闭合")
    for digest in (
        cohort.source_digest,
        cohort.source_content_id,
        cohort.source_raw_digest,
        cohort.observed_raw_digest,
        cohort.durability_source_digest,
        cohort.seal_digest_sha256,
    ):
        if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
            raise ValueError("来源集合摘要必须为完整小写SHA-256")
    if cohort.source_raw_digest != cohort.observed_raw_digest:
        raise ValueError("目标日Raw业务键集合摘要与封口摘要不一致")
    if (
        type(cohort.source_byte_count) is not int
        or cohort.source_byte_count <= 0
        or not cohort.parser_contract
        or not cohort.source_clock_role
        or not isinstance(cohort.seal_writer_transaction_id, str)
        or not cohort.seal_writer_transaction_id.isdigit()
        or not cohort.commit_ack_observed_at
        or not cohort.capture_read_started_at
        or not cohort.capture_validated_at
        or not cohort.durable_at
        or not cohort.sealed_at
    ):
        raise ValueError("来源捕获时钟、封口交易或来源包身份不完整")
    if cohort.durability_source_digest != cohort.source_digest:
        raise ValueError("提交后持久性证明未绑定同一来源摘要")
    if cohort.durability_source_capture_id != cohort.source_capture_id:
        raise ValueError("提交后持久性证明未绑定同一来源捕获")
    if (
        cohort.source_available_at is not None
        and cohort.source_time_quality != "VERIFIED_SOURCE_TIME"
    ):
        raise ValueError("来源available_at与来源时间质量声明不一致")
    if (
        type(inputs.official_sessions) is not tuple
        or not inputs.official_sessions
        or any(type(session) is not date for session in inputs.official_sessions)
        or any(
            left >= right
            for left, right in zip(
                inputs.official_sessions, inputs.official_sessions[1:]
            )
        )
    ):
        raise ValueError("官方市场日历会话必须是唯一递增date序列")
    try:
        target_index = inputs.official_sessions.index(cohort.target_session)
    except ValueError as error:
        raise ValueError("目标日不在官方市场会话中") from error
    if target_index < 4:
        raise ValueError("官方会话历史不足以取得t至t-4")
    window = inputs.official_sessions[target_index - 4 : target_index + 1]
    if len(window) != 5 or window[-1] != cohort.target_session:
        raise ValueError("官方会话窗口未闭合")
    if any(session > inputs.research_cutoff for session in window):
        raise ValueError("官方会话窗口超过研究截止日")
    source_symbols = set(inputs.source_symbols)
    if any(
        not isinstance(symbol, str) or _SOURCE_SYMBOL.fullmatch(symbol) is None
        for symbol in inputs.source_symbols
    ) or len(source_symbols) != len(inputs.source_symbols):
        raise ValueError("来源代码集合必须为唯一的SH./SZ.来源代码")
    needed_sessions = {window[0], window[1], window[3], window[4]}
    seen: set[tuple[str, date]] = set()
    for observation in inputs.observations:
        if not isinstance(observation, RawPriceObservation):
            raise ValueError("价格观察必须使用RawPriceObservation")
        key = (observation.source_symbol, observation.session_date)
        if observation.source_symbol not in source_symbols:
            raise ValueError("价格观察包含固定来源集合以外的代码")
        if observation.session_date not in needed_sessions:
            raise ValueError("价格观察不属于t、t-1、t-3、t-4官方会话")
        if key in seen:
            raise ValueError("相同来源代码和官方会话存在重复RAW价格")
        seen.add(key)
    return window


def _observed_close(
    observation: RawPriceObservation | None,
    *,
    cohort: SourceCohortIdentity,
    expected_session: date,
) -> tuple[Fraction | None, str | None, str | None]:
    if observation is None:
        return None, "RAW_SESSION_ROW_MISSING", None
    if observation.session_date != expected_session:
        return None, "OFFICIAL_SESSION_MISMATCH", None
    if observation.namespace_id != cohort.namespace_id:
        return None, "SOURCE_NAMESPACE_MISMATCH", None
    if observation.package_sha256 != cohort.package_sha256:
        return None, "SOURCE_PACKAGE_MISMATCH", None
    if observation.price_coordinate != _PRICE_COORDINATE:
        return None, "PRICE_COORDINATE_NOT_RAW_UNADJUSTED", None
    if observation.bar_status == "CONFIRMED_SUSPENSION":
        return None, "CONFIRMED_SUSPENSION_NO_LEGAL_RAW_PRICE", observation.price_reason
    if observation.bar_status == "TARGET_MISSING":
        return None, "TARGET_BAR_MISSING", observation.price_reason
    if observation.bar_status == "READ_FAILED":
        return None, "RAW_READ_FAILED", observation.price_reason
    if observation.bar_status != "TARGET_PRESENT":
        return None, "BAR_STATUS_NOT_OBSERVED", observation.price_reason
    if observation.price_quality != "OBSERVED":
        reason = (
            observation.price_reason or f"PRICE_QUALITY_{observation.price_quality}"
        )
        return None, reason, observation.price_reason
    try:
        close = _fraction(observation.raw_close)
    except (ValueError, TypeError, ZeroDivisionError):
        return None, "RAW_CLOSE_INVALID", observation.price_reason
    if close <= 0:
        return None, "RAW_CLOSE_NOT_POSITIVE", observation.price_reason
    return close, None, None


def calculate_source_market_breadth(
    inputs: SourceMarketBreadthInput,
) -> SourceMarketBreadthResult:
    """按MKT-02计算固定来源篮的正收益状态差，不读取外部状态。"""
    window = _validate_input(inputs)
    target_session = window[4]
    t_minus_1 = window[3]
    t_minus_3 = window[1]
    t_minus_4 = window[0]
    bars = {
        (observation.source_symbol, observation.session_date): observation
        for observation in inputs.observations
    }
    symbols: list[SourceMarketBreadthSymbol] = []
    counts = {
        "COMPARABLE_GAIN": 0,
        "COMPARABLE_LOSS": 0,
        "COMPARABLE_BOTH_POSITIVE": 0,
        "COMPARABLE_BOTH_NONPOSITIVE": 0,
        "UNKNOWN": 0,
    }
    unknown_reasons: Counter[str] = Counter()

    for source_symbol in inputs.source_symbols:
        current_t, reason_t, detail_t = _observed_close(
            bars.get((source_symbol, target_session)),
            cohort=inputs.cohort,
            expected_session=target_session,
        )
        previous_t, reason_t1, detail_t1 = _observed_close(
            bars.get((source_symbol, t_minus_1)),
            cohort=inputs.cohort,
            expected_session=t_minus_1,
        )
        current_t3, reason_t3, detail_t3 = _observed_close(
            bars.get((source_symbol, t_minus_3)),
            cohort=inputs.cohort,
            expected_session=t_minus_3,
        )
        previous_t3, reason_t4, detail_t4 = _observed_close(
            bars.get((source_symbol, t_minus_4)),
            cohort=inputs.cohort,
            expected_session=t_minus_4,
        )

        ret_t = (
            current_t / previous_t - 1
            if current_t is not None and previous_t is not None
            else None
        )
        ret_t3 = (
            current_t3 / previous_t3 - 1
            if current_t3 is not None and previous_t3 is not None
            else None
        )
        positive_t = int(ret_t > 0) if ret_t is not None else None
        positive_t3 = int(ret_t3 > 0) if ret_t3 is not None else None
        missing_t = reason_t or reason_t1
        missing_t_detail = detail_t or detail_t1
        missing_t3 = reason_t3 or reason_t4
        missing_t3_detail = detail_t3 or detail_t4

        if ret_t is None or ret_t3 is None:
            if ret_t is None:
                primary_reason = f"RET1_T:{missing_t or 'PRICE_UNAVAILABLE'}"
                detail = missing_t_detail
            else:
                primary_reason = f"RET1_T_MINUS_3:{missing_t3 or 'PRICE_UNAVAILABLE'}"
                detail = missing_t3_detail
            counts["UNKNOWN"] += 1
            unknown_reasons[primary_reason] += 1
            symbols.append(
                SourceMarketBreadthSymbol(
                    source_symbol=source_symbol,
                    ret1_t=ret_t,
                    ret1_t_minus_3=ret_t3,
                    positive_t=positive_t,
                    positive_t_minus_3=positive_t3,
                    delta_indicator=None,
                    classification="UNKNOWN",
                    primary_unknown_reason=primary_reason,
                    unknown_reason_detail=detail,
                )
            )
            continue

        delta = positive_t - positive_t3
        if delta == 1:
            classification = "COMPARABLE_GAIN"
        elif delta == -1:
            classification = "COMPARABLE_LOSS"
        elif positive_t == 1:
            classification = "COMPARABLE_BOTH_POSITIVE"
        else:
            classification = "COMPARABLE_BOTH_NONPOSITIVE"
        counts[classification] += 1
        symbols.append(
            SourceMarketBreadthSymbol(
                source_symbol=source_symbol,
                ret1_t=ret_t,
                ret1_t_minus_3=ret_t3,
                positive_t=positive_t,
                positive_t_minus_3=positive_t3,
                delta_indicator=delta,
                classification=classification,
                primary_unknown_reason=None,
                unknown_reason_detail=None,
            )
        )

    n_source = len(inputs.source_symbols)
    n_eligible = n_source - counts["UNKNOWN"]
    if sum(counts.values()) != n_source:
        raise ValueError("来源集合未被互斥分类完整覆盖")
    breadth = (
        Fraction(counts["COMPARABLE_GAIN"] - counts["COMPARABLE_LOSS"], n_eligible)
        if n_eligible
        else None
    )
    coverage = Fraction(n_eligible, n_source) if n_source else None
    if breadth is not None and not Fraction(-1) <= breadth <= Fraction(1):
        raise ValueError("固定来源宽度超出[-1,1]fraction范围")
    return SourceMarketBreadthResult(
        target_session=inputs.cohort.target_session,
        research_cutoff=inputs.research_cutoff,
        session_window=window,
        contract_version=SOURCE_MARKET_BREADTH_CONTRACT,
        cohort=inputs.cohort,
        n_source=n_source,
        n_eligible=n_eligible,
        counts=counts,
        unknown_reason_counts=dict(sorted(unknown_reasons.items())),
        coverage=coverage,
        b_fixed_source=breadth,
        symbols=tuple(symbols),
    )
