"""复用仿射纯内核生产有界单股调整价格事实。"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import json
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from stock_a.canonical.adjustment import (
    AdjustmentContext,
    PARAMETER_VERSION,
    PRODUCER_VERSION,
    TICKED_COORDINATE,
    XrxdEvent,
    adjust_ohlc,
    build_affine_factor,
)
from stock_a.domain import ContractError, exact_number
from stock_a.storage.postgres.adjusted_prices import (
    AdjustedPriceFact,
    persist_adjusted_price_facts,
    read_adjustment_raw_window,
    read_corporate_action_events,
)


_SHANGHAI = ZoneInfo("Asia/Shanghai")
# 沪深A股采用的申报价格最小变动单位；变更须同步现行交易规则与算法合同。
_CN_A_SHARE_PRICE_TICK = Decimal("0.01")


class AdjustedPriceProductionError(RuntimeError):
    """调整价格输入、来源范围或仿射计算失败。"""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def official_session_close_cutoff(session: date) -> datetime:
    """按沪深交易规则14:57至15:00收盘集合竞价确定日收盘截止。"""

    if type(session) is not date:
        raise AdjustedPriceProductionError(
            "TARGET_SESSION_INVALID", "目标会话必须是date"
        )
    return datetime.combine(session, time(15, 0), tzinfo=_SHANGHAI)


def _fraction_decimal(value: Fraction) -> Decimal:
    with localcontext() as context:
        context.prec = max(
            50, len(str(abs(value.numerator))) + len(str(value.denominator)) + 16
        )
        result = Decimal(value.numerator) / Decimal(value.denominator)
    if Fraction(result) != value:
        raise AdjustedPriceProductionError(
            "ADJUSTED_PRICE_DECIMAL_NOT_EXACT", "tick价格无法以精确十进制持久化"
        )
    return result


def _source_event(row: Mapping[str, Any]) -> XrxdEvent:
    if row.get("source_quality") != "VERIFIED_OFFICIAL_NOTICE":
        raise AdjustedPriceProductionError(
            "BLOCKED_SOURCE_ACTION_UNVERIFIED", "公司行为来源质量不足，未计算调整价格"
        )
    if row.get("action_kind") != "CAPITAL_RESERVE_TO_STOCK":
        raise AdjustedPriceProductionError(
            "BLOCKED_SOURCE_ACTION_UNSUPPORTED", "公司行为类型尚未闭合，未计算调整价格"
        )
    if row.get("available_at_quality") not in {
        "EXACT_TIMESTAMP",
        "DATE_ONLY_NEXT_DAY_BOUNDARY",
    }:
        raise AdjustedPriceProductionError(
            "BLOCKED_SOURCE_ACTION_CLOCK_UNKNOWN", "事件可得时钟没有可用边界"
        )
    return XrxdEvent(
        event_ref=row["event_ref"],
        source_ref=row["source_ref"],
        security_id=f"SOURCE_SYMBOL:{row['source_symbol']}",
        effective_session=row["effective_session"],
        available_at=row["available_at"],
        cash_per_10=row["cash_per_10"],
        rights_price=row["rights_price"],
        bonus_per_10=row["bonus_per_10"],
        rights_per_10=row["rights_per_10"],
        same_day_order=0,
        parameter_version=PARAMETER_VERSION,
    )


def produce_adjusted_price_window(
    config: Mapping[str, Any],
    *,
    source_symbol: str,
    source_package_id: int,
    target_session: date,
    source_window_sessions: Sequence[date],
    research_cutoff: date,
    calendar_id: str,
    calendar_revision: str,
) -> dict[str, Any]:
    """仅生产一份有真实公告事件覆盖的相邻会话调整窗口。

    当前已核验来源是单份实施公告，因此只对公告生效会话及其前一官方
    会话生产事实。更宽历史范围或其他目标日保留NOT_COMPUTED，不推断无事件。
    """

    if type(target_session) is not date or type(research_cutoff) is not date:
        raise AdjustedPriceProductionError("SESSION_INVALID", "目标和截止必须是date")
    if target_session > research_cutoff:
        raise AdjustedPriceProductionError(
            "BLOCKED_TARGET_AFTER_CUTOFF", "调整目标晚于本次研究截止，未读取来源或PG"
        )
    sessions = tuple(source_window_sessions)
    if (
        not sessions
        or any(type(item) is not date for item in sessions)
        or tuple(sorted(set(sessions))) != sessions
        or sessions[-1] != target_session
        or any(item > research_cutoff for item in sessions)
    ):
        raise AdjustedPriceProductionError(
            "SOURCE_WINDOW_INVALID",
            "调整输入窗口必须是截止内升序官方会话且以目标会话结束",
        )
    if not all(
        isinstance(value, str) and value.strip()
        for value in (calendar_id, calendar_revision)
    ):
        raise AdjustedPriceProductionError(
            "CALENDAR_IDENTITY_INVALID", "官方日历身份或修订缺失"
        )

    raw_rows = read_adjustment_raw_window(
        config,
        source_symbol=source_symbol,
        source_package_id=source_package_id,
        target_session=target_session,
        session_count=len(sessions),
    )
    if tuple(row["session_date"] for row in raw_rows) != sessions:
        raise AdjustedPriceProductionError(
            "RAW_INPUT_WINDOW_INCOMPLETE", "PG Raw输入未覆盖请求的精确官方会话窗口"
        )
    if any(
        row["bar_status"] != "TARGET_PRESENT" or row["price_quality"] != "OBSERVED"
        for row in raw_rows
    ):
        raise AdjustedPriceProductionError(
            "RAW_PRICE_QUALITY_UNKNOWN", "调整窗口存在缺失或质量未知的Raw OHLC"
        )

    event_rows = read_corporate_action_events(
        config, source_symbol=source_symbol, through_session=target_session
    )
    if not event_rows:
        raise AdjustedPriceProductionError(
            "BLOCKED_SOURCE_ACTIONS_NOT_PROVEN",
            "没有该代码目标会话以前的真实公司行为来源证据",
        )
    if len(event_rows) != 1 or event_rows[0]["effective_session"] != target_session:
        raise AdjustedPriceProductionError(
            "ADJUSTMENT_EVENT_COVERAGE_INCOMPLETE",
            "现有公告证据只闭合单事件生效日的相邻会话窗口；其他日期或多事件窗口未计算",
        )
    if len(sessions) != 2:
        raise AdjustedPriceProductionError(
            "ADJUSTMENT_EVENT_WINDOW_NOT_PROVEN",
            "单公告样本只证明前一官方会话至生效会话的两会话窗口",
        )

    action = _source_event(event_rows[0])
    if any(
        row.get("market") != event_rows[0]["source_market"]
        or row.get("market") != source_symbol.split(".", 1)[0]
        for row in raw_rows
    ):
        raise AdjustedPriceProductionError(
            "ADJUSTMENT_EVENT_MARKET_MISMATCH", "公司行为公告市场与TDX来源市场不一致"
        )
    if action.security_id != f"SOURCE_SYMBOL:{source_symbol}":
        raise AdjustedPriceProductionError(
            "ADJUSTMENT_EVENT_SYMBOL_MISMATCH", "公司行为来源代码与Raw来源代码不一致"
        )
    if action.effective_session != target_session:
        raise AdjustedPriceProductionError(
            "ADJUSTMENT_EVENT_SESSION_MISMATCH", "公司行为生效会话与目标会话不匹配"
        )

    source_cutoff = official_session_close_cutoff(target_session)
    event_set_revision = hashlib.sha256(
        json.dumps(
            [
                (row["event_ref"], row["semantic_revision"].strip())
                for row in event_rows
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    target_basis = f"ADJ:{PRODUCER_VERSION}:{event_set_revision}:{calendar_revision}"
    target_raw = raw_rows[-1]
    if target_raw["session_date"] != target_session:
        raise AdjustedPriceProductionError(
            "TARGET_RAW_NOT_FOUND", "目标会话缺少精确Raw行"
        )
    facts = []
    for raw in raw_rows:
        context = AdjustmentContext(
            security_id=f"SOURCE_SYMBOL:{source_symbol}",
            source_basis="RAW:TDX_VIPDATA_PACKAGE",
            target_basis=target_basis,
            target_session=target_session,
            source_cutoff=source_cutoff,
            tick=_CN_A_SHARE_PRICE_TICK,
            coordinate_mode=TICKED_COORDINATE,
        )
        factor = build_affine_factor(
            (action,), source_session=raw["session_date"], context=context
        )
        raw_ohlc = tuple(
            exact_number(raw[name])
            for name in ("raw_open", "raw_high", "raw_low", "raw_close")
        )
        adjusted = adjust_ohlc(raw_ohlc, factor, tick=_CN_A_SHARE_PRICE_TICK)
        adjusted_decimal = tuple(_fraction_decimal(value) for value in adjusted)
        digest_input = {
            "source_package_id": raw["source_package_id"],
            "source_capture_id": raw["source_capture_id"],
            "source_symbol": source_symbol,
            "source_session": raw["session_date"].isoformat(),
            "target_session": target_session.isoformat(),
            "raw_ohlc": [
                str(raw[name])
                for name in ("raw_open", "raw_high", "raw_low", "raw_close")
            ],
            "event_ref": action.event_ref,
            "event_revision": event_rows[0]["semantic_revision"].strip(),
            "calendar_id": calendar_id,
            "calendar_revision": calendar_revision,
            "parameter_version": PARAMETER_VERSION,
            "algorithm_version": PRODUCER_VERSION,
        }
        input_digest = hashlib.sha256(
            json.dumps(
                digest_input, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()
        facts.append(
            AdjustedPriceFact(
                source_package_id=raw["source_package_id"],
                source_capture_id=raw["source_capture_id"],
                target_capture_id=target_raw["source_capture_id"],
                source_symbol=source_symbol,
                source_session=raw["session_date"],
                target_session=target_session,
                source_coordinate_id="RAW:TDX_VIPDATA_PACKAGE",
                target_coordinate_id=target_basis,
                event_set_revision=event_set_revision,
                event_refs=(action.event_ref,),
                applied_event_refs=factor.event_refs,
                alpha=factor.alpha,
                beta=factor.beta,
                price_tick=_CN_A_SHARE_PRICE_TICK,
                adjusted_ohlc=adjusted_decimal,
                price_quality="OBSERVED",
                adjustment_status="APPLIED"
                if factor.event_refs
                else "IDENTITY_VERIFIED",
                quality_reason=None,
                action_coverage_status="SINGLE_NOTICE_WINDOW",
                research_mode="RETROSPECTIVE_NON_PIT",
                calendar_id=calendar_id,
                calendar_revision=calendar_revision,
                parameter_version=PARAMETER_VERSION,
                algorithm_version=PRODUCER_VERSION,
                window_start_session=sessions[0],
                window_session_count=len(sessions),
                input_digest=input_digest,
            )
        )
    persisted = persist_adjusted_price_facts(config, facts)
    return {
        **persisted,
        "status": "COMMITTED_SINGLE_NOTICE_WINDOW",
        "source_symbol": source_symbol,
        "source_package_id": source_package_id,
        "target_session": target_session.isoformat(),
        "source_sessions": [day.isoformat() for day in sessions],
        "event_ref": action.event_ref,
        "event_revision": event_rows[0]["semantic_revision"].strip(),
        "event_available_at": action.available_at.isoformat(),
        "event_available_at_quality": event_rows[0]["available_at_quality"],
        "source_cutoff": source_cutoff.isoformat(),
        "research_mode": "RETROSPECTIVE_NON_PIT",
        "price_coordinate": target_basis,
        "algorithm_version": PRODUCER_VERSION,
        "parameter_version": PARAMETER_VERSION,
    }
