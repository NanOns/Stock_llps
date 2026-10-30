"""精确仿射复权与坐标运输纯内核。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction

from stock_a.canonical.identity import semantic_digest
from stock_a.domain import ContractError, FieldValue, exact_number
from stock_a.sources.tdx.actions import GbbqRecord


PARAMETER_VERSION = "TDX_XRXD_UNIT_R1"
PRODUCER_VERSION = "STOCK_A_AFFINE_R1"
MAX_EVENT_BUDGET = 10_000
DEFAULT_EVENT_BUDGET = 1_000
TICKED_COORDINATE = "TICKED"
EXACT_RESEARCH_COORDINATE = "EXACT_RESEARCH"


def _nonempty(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name}必须是非空字符串")
    return value


def _aware(value: object, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ContractError(f"{name}必须是带时区的datetime")
    return value


@dataclass(frozen=True)
class AdjustmentContext:
    """一次坐标转换的显式身份、目标、知识截止及数值合同。"""

    security_id: str
    source_basis: str
    target_basis: str
    target_session: date
    source_cutoff: datetime
    tick: int | Decimal | Fraction
    coordinate_mode: str = TICKED_COORDINATE
    parameter_version: str = PARAMETER_VERSION
    producer_version: str = PRODUCER_VERSION

    def __post_init__(self) -> None:
        _nonempty(self.security_id, "security_id")
        source_basis = _nonempty(self.source_basis, "source_basis")
        target_basis = _nonempty(self.target_basis, "target_basis")
        if not source_basis.startswith("RAW:"):
            raise ContractError("source_basis必须是RAW:开头的原生价格坐标")
        if self.coordinate_mode == TICKED_COORDINATE:
            if not target_basis.startswith("ADJ:"):
                raise ContractError("TICKED目标坐标必须使用ADJ:标识")
        elif self.coordinate_mode == EXACT_RESEARCH_COORDINATE:
            if not target_basis.startswith("RESEARCH:"):
                raise ContractError("精确研究坐标必须使用RESEARCH:标识")
        else:
            raise ContractError("coordinate_mode仅支持TICKED或EXACT_RESEARCH")
        if source_basis == target_basis:
            raise ContractError("源坐标与目标坐标必须不同")
        if not isinstance(self.target_session, date) or isinstance(
            self.target_session, datetime
        ):
            raise ContractError("target_session必须是date会话")
        _aware(self.source_cutoff, "source_cutoff")
        try:
            tick = exact_number(self.tick)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ContractError("tick必须是有限精确数且不能为bool/float") from exc
        if tick <= 0:
            raise ContractError("tick必须为正数")
        if self.parameter_version != PARAMETER_VERSION:
            raise ContractError(f"不支持parameter_version：{self.parameter_version}")
        if self.producer_version != PRODUCER_VERSION:
            raise ContractError(f"不支持producer_version：{self.producer_version}")

    @property
    def exact_tick(self) -> Fraction:
        return exact_number(self.tick)

    @property
    def target_coordinate_id(self) -> tuple[str, str, Fraction | None, str]:
        """目标坐标身份包含量化模式；正式坐标同时绑定精确tick。"""
        return (
            self.target_basis,
            self.coordinate_mode,
            self.exact_tick if self.coordinate_mode == TICKED_COORDINATE else None,
            self.producer_version,
        )


@dataclass(frozen=True)
class XrxdEvent:
    """一个已解释的类别1事件；available_at由调用方的来源合同注入。"""

    event_ref: str
    source_ref: str
    security_id: str
    effective_session: date
    available_at: datetime
    cash_per_10: int | Decimal | Fraction
    rights_price: int | Decimal | Fraction
    bonus_per_10: int | Decimal | Fraction
    rights_per_10: int | Decimal | Fraction
    same_day_order: int | None = None
    parameter_version: str = PARAMETER_VERSION
    source_record_index: int | None = None
    source_byte_offset: int | None = None

    def __post_init__(self) -> None:
        _nonempty(self.event_ref, "event_ref")
        _nonempty(self.source_ref, "source_ref")
        _nonempty(self.security_id, "security_id")
        if not isinstance(self.effective_session, date) or isinstance(
            self.effective_session, datetime
        ):
            raise ContractError("effective_session必须是date会话")
        _aware(self.available_at, "available_at")
        if self.parameter_version != PARAMETER_VERSION:
            raise ContractError(f"不支持parameter_version：{self.parameter_version}")
        if self.same_day_order is not None and (
            isinstance(self.same_day_order, bool)
            or not isinstance(self.same_day_order, int)
            or self.same_day_order < 0
        ):
            raise ContractError("same_day_order必须为非负整数或None")
        if (self.source_record_index is None) != (self.source_byte_offset is None):
            raise ContractError(
                "source_record_index与source_byte_offset必须同时提供或同时缺省"
            )
        if self.source_record_index is not None and (
            isinstance(self.source_record_index, bool)
            or not isinstance(self.source_record_index, int)
            or self.source_record_index < 0
            or isinstance(self.source_byte_offset, bool)
            or not isinstance(self.source_byte_offset, int)
            or self.source_byte_offset < 0
        ):
            raise ContractError("来源记录序号与字节偏移必须为非负整数")
        values = (
            self.cash_per_10,
            self.rights_price,
            self.bonus_per_10,
            self.rights_per_10,
        )
        names = ("cash_per_10", "rights_price", "bonus_per_10", "rights_per_10")
        for name, value in zip(names, values, strict=True):
            try:
                number = exact_number(value)
            except (ValueError, TypeError, OverflowError) as exc:
                raise ContractError(
                    f"{name}必须是有限精确数且不能为bool/float"
                ) from exc
            if number < 0:
                raise ContractError(f"{name}不能为负数")
        if self.m <= 0:
            raise ContractError("配股送转后的分母必须为正")

    @property
    def m(self) -> Fraction:
        return (
            10 + exact_number(self.bonus_per_10) + exact_number(self.rights_per_10)
        ) / 10

    @property
    def c(self) -> Fraction:
        return (
            exact_number(self.cash_per_10)
            - exact_number(self.rights_per_10) * exact_number(self.rights_price)
        ) / 10


@dataclass(frozen=True)
class UnsupportedAction:
    """已知非类别1动作；其实体和会话可定位时只阻断跨越该动作的窗口。"""

    event_ref: str
    source_ref: str
    security_id: str
    effective_session: date
    available_at: datetime
    category: int

    def __post_init__(self) -> None:
        _nonempty(self.event_ref, "event_ref")
        _nonempty(self.source_ref, "source_ref")
        _nonempty(self.security_id, "security_id")
        if not isinstance(self.effective_session, date) or isinstance(
            self.effective_session, datetime
        ):
            raise ContractError("effective_session必须是date会话")
        _aware(self.available_at, "available_at")
        if (
            isinstance(self.category, bool)
            or not isinstance(self.category, int)
            or self.category == 1
        ):
            raise ContractError("UnsupportedAction.category必须是非类别1整数")
        if not 2 <= self.category <= 14:
            raise ContractError("UnsupportedAction.category必须是已知未支持类别2到14")


@dataclass(frozen=True)
class UnresolvedAction:
    """作用实体或有效会话未确认的动作；可能影响当前窗口时必须阻断。"""

    event_ref: str
    source_ref: str
    security_id: str | None
    effective_session: date | None
    available_at: datetime
    reason: str

    def __post_init__(self) -> None:
        _nonempty(self.event_ref, "event_ref")
        _nonempty(self.source_ref, "source_ref")
        if self.security_id is not None:
            _nonempty(self.security_id, "security_id")
        if self.effective_session is not None and (
            not isinstance(self.effective_session, date)
            or isinstance(self.effective_session, datetime)
        ):
            raise ContractError("effective_session必须是date会话或None")
        _aware(self.available_at, "available_at")
        _nonempty(self.reason, "reason")
        if self.security_id is not None and self.effective_session is not None:
            raise ContractError("实体和有效会话均已确认时应使用已定位事件类型")


@dataclass(frozen=True)
class AdjustmentFactor:
    security_id: str
    source_basis: str
    target_basis: str
    source_session: date
    target_session: date
    alpha: Fraction
    beta: Fraction
    tick: Fraction
    coordinate_mode: str
    event_refs: tuple[str, ...]
    event_source_refs: tuple[tuple[str, str, int | None, int | None], ...]
    source_cutoff: datetime
    parameter_version: str
    producer_version: str

    def __post_init__(self) -> None:
        _nonempty(self.security_id, "security_id")
        source_basis = _nonempty(self.source_basis, "source_basis")
        target_basis = _nonempty(self.target_basis, "target_basis")
        if not source_basis.startswith("RAW:"):
            raise ContractError("source_basis必须是RAW:开头的原生价格坐标")
        if source_basis == target_basis:
            raise ContractError("源坐标与目标坐标必须不同")
        if not isinstance(self.source_session, date) or isinstance(
            self.source_session, datetime
        ):
            raise ContractError("source_session必须是date会话")
        if not isinstance(self.target_session, date) or isinstance(
            self.target_session, datetime
        ):
            raise ContractError("target_session必须是date会话")
        if self.source_session > self.target_session:
            raise ContractError("source_session不能晚于target_session")
        if not isinstance(self.alpha, Fraction) or not isinstance(self.beta, Fraction):
            raise ContractError("alpha和beta必须是精确Fraction")
        if self.alpha <= 0:
            raise ContractError("仿射尺度alpha必须为正")
        if not isinstance(self.event_refs, tuple) or any(
            not isinstance(ref, str) or not ref for ref in self.event_refs
        ):
            raise ContractError("event_refs必须是非空字符串组成的tuple")
        if not isinstance(self.event_source_refs, tuple) or any(
            not isinstance(pair, tuple)
            or len(pair) != 4
            or not isinstance(pair[0], str)
            or not pair[0]
            or not isinstance(pair[1], str)
            or not pair[1]
            or any(
                value is not None
                and (isinstance(value, bool) or not isinstance(value, int) or value < 0)
                for value in pair[2:]
            )
            for pair in self.event_source_refs
        ):
            raise ContractError("event_source_refs必须是事件引用与来源引用组成的tuple")
        if (
            tuple(event_ref for event_ref, _, _, _ in self.event_source_refs)
            != self.event_refs
        ):
            raise ContractError("event_source_refs必须与event_refs逐项对应")
        _aware(self.source_cutoff, "source_cutoff")
        _nonempty(self.parameter_version, "parameter_version")
        _nonempty(self.producer_version, "producer_version")
        if self.parameter_version != PARAMETER_VERSION:
            raise ContractError(f"不支持parameter_version：{self.parameter_version}")
        if self.producer_version != PRODUCER_VERSION:
            raise ContractError(f"不支持producer_version：{self.producer_version}")
        if not isinstance(self.tick, Fraction) or self.tick <= 0:
            raise ContractError("factor.tick必须是正Fraction")
        if self.coordinate_mode not in (TICKED_COORDINATE, EXACT_RESEARCH_COORDINATE):
            raise ContractError("factor.coordinate_mode不受支持")
        if (
            self.coordinate_mode == TICKED_COORDINATE
            and not self.target_basis.startswith("ADJ:")
        ):
            raise ContractError("TICKED factor目标坐标必须使用ADJ:标识")
        if (
            self.coordinate_mode == EXACT_RESEARCH_COORDINATE
            and not self.target_basis.startswith("RESEARCH:")
        ):
            raise ContractError("精确研究 factor目标坐标必须使用RESEARCH:标识")

    @property
    def target_coordinate_id(self) -> tuple[str, str, Fraction | None, str]:
        return (
            self.target_basis,
            self.coordinate_mode,
            self.tick if self.coordinate_mode == TICKED_COORDINATE else None,
            self.producer_version,
        )

    @property
    def source_dependency_identity(
        self,
    ) -> tuple[tuple[str, str, int | None, int | None], ...]:
        """只引用实际参与事件及其来源，不声称已计算内容digest。"""
        return self.event_source_refs


def event_from_gbbq(
    record: GbbqRecord,
    *,
    security_id: str,
    effective_session: date,
    available_at: datetime,
    same_day_order: int | None,
    event_ref: str,
) -> XrxdEvent:
    """把R00.04类别1记录转换成已绑定实体、会话和可得时钟的事件。"""

    if not isinstance(record, GbbqRecord):
        raise ContractError("record必须是R00.04 GbbqRecord")
    if not record.can_interpret_xrxd:
        # 保留R00.04稳定错误码，不把坏行改成零事件。
        record.xrxd_fields()
    fields = record.xrxd_fields()
    return XrxdEvent(
        event_ref=event_ref,
        source_ref=record.source_ref,
        security_id=security_id,
        effective_session=effective_session,
        available_at=available_at,
        cash_per_10=fields["cash_dividend_per_10"],
        rights_price=fields["rights_price"],
        bonus_per_10=fields["bonus_transfer_per_10"],
        rights_per_10=fields["rights_ratio_per_10"],
        same_day_order=same_day_order,
        parameter_version=record.unit_version,
        source_record_index=record.source_record_index,
        source_byte_offset=record.source_byte_offset,
    )


def _validate_event_set(
    events: tuple[XrxdEvent | UnsupportedAction | UnresolvedAction, ...],
    context: AdjustmentContext,
) -> None:
    refs: dict[str, XrxdEvent | UnsupportedAction | UnresolvedAction] = {}
    for event in events:
        if not isinstance(event, (XrxdEvent, UnsupportedAction, UnresolvedAction)):
            raise ContractError(
                "events只能包含已解释、已知不支持或作用范围未确认的事件"
            )
        if event.security_id is not None and event.security_id != context.security_id:
            if not isinstance(event, UnresolvedAction):
                raise ContractError("事件security_id与调整上下文不匹配")
        previous = refs.get(event.event_ref)
        if previous is not None:
            raise ContractError("重复event_ref；事件身份冲突或重复输入")
        refs[event.event_ref] = event
        if (
            isinstance(event, XrxdEvent)
            and event.parameter_version != context.parameter_version
        ):
            raise ContractError("事件parameter_version与调整上下文不匹配")


def build_affine_factor(
    events: tuple[XrxdEvent | UnsupportedAction | UnresolvedAction, ...]
    | list[XrxdEvent | UnsupportedAction | UnresolvedAction],
    *,
    source_session: date,
    context: AdjustmentContext,
    max_events: int = DEFAULT_EVENT_BUDGET,
) -> AdjustmentFactor:
    """显式源会话到目标会话的精确转换；只组合knowledge cutoff内事件。"""

    if not isinstance(context, AdjustmentContext):
        raise ContractError("context必须是AdjustmentContext")
    if not isinstance(source_session, date) or isinstance(source_session, datetime):
        raise ContractError("source_session必须是date会话")
    if source_session > context.target_session:
        raise ContractError("source_session不能晚于target_session")
    if (
        isinstance(max_events, bool)
        or not isinstance(max_events, int)
        or not 1 <= max_events <= MAX_EVENT_BUDGET
    ):
        raise ContractError(f"max_events必须是1到{MAX_EVENT_BUDGET}的整数")
    if not isinstance(events, (tuple, list)):
        raise ContractError("events必须是tuple或list有界输入")
    if len(events) > max_events:
        raise ContractError(f"公司行为事件数超过max_events={max_events}")
    event_tuple = tuple(events)
    _validate_event_set(event_tuple, context)
    eligible = []
    for event in event_tuple:
        if event.available_at > context.source_cutoff:
            continue
        if isinstance(event, UnresolvedAction):
            if (
                event.security_id is not None
                and event.security_id != context.security_id
            ):
                continue
            if event.effective_session is not None and not (
                source_session < event.effective_session <= context.target_session
            ):
                continue
            eligible.append(event)
        elif source_session < event.effective_session <= context.target_session:
            eligible.append(event)
    unresolved = [event for event in eligible if isinstance(event, UnresolvedAction)]
    if unresolved:
        raise ContractError(
            f"公司行为存在{len(unresolved)}个实体或有效会话未确认的事件，阻断实际依赖窗口"
        )
    active_dates: dict[date, list[XrxdEvent]] = {}
    for event in eligible:
        if isinstance(event, XrxdEvent):
            active_dates.setdefault(event.effective_session, []).append(event)
    for same_day in active_dates.values():
        if len(same_day) > 1:
            orders = [event.same_day_order for event in same_day]
            if any(order is None for order in orders) or len(set(orders)) != len(
                orders
            ):
                raise ContractError("同一有效会话的多个事件必须有唯一注册顺序")
    unsupported = [event for event in eligible if isinstance(event, UnsupportedAction)]
    if unsupported:
        raise ContractError(f"跨越{len(unsupported)}个未支持公司行为，不能确定坐标")
    eligible_xrxd = [event for event in eligible if isinstance(event, XrxdEvent)]
    eligible_xrxd.sort(
        key=lambda event: (event.effective_session, event.same_day_order or 0),
        reverse=True,
    )
    alpha, beta = Fraction(1), Fraction(0)
    for event in eligible_xrxd:
        alpha = alpha / event.m
        beta = beta - alpha * event.c
    return AdjustmentFactor(
        security_id=context.security_id,
        source_basis=context.source_basis,
        target_basis=context.target_basis,
        source_session=source_session,
        target_session=context.target_session,
        alpha=alpha,
        beta=beta,
        tick=context.exact_tick,
        coordinate_mode=context.coordinate_mode,
        event_refs=tuple(event.event_ref for event in eligible_xrxd),
        event_source_refs=tuple(
            (
                event.event_ref,
                event.source_ref,
                event.source_record_index,
                event.source_byte_offset,
            )
            for event in eligible_xrxd
        ),
        source_cutoff=context.source_cutoff,
        parameter_version=context.parameter_version,
        producer_version=context.producer_version,
    )


def affine_price(
    value: object, factor: AdjustmentFactor, *, tick: object | None = None
) -> Fraction:
    """按factor绑定坐标转换；兼容参数tick只能重复声明已绑定tick。"""
    if not isinstance(factor, AdjustmentFactor):
        raise ContractError("factor必须是AdjustmentFactor")
    try:
        price = exact_number(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError("价格必须是有限精确数且不能为bool/float") from exc
    if price <= 0:
        raise ContractError("RAW输入价格必须大于0")
    result = factor.alpha * price + factor.beta
    if tick is not None:
        try:
            supplied_tick = exact_number(tick)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ContractError("tick必须是有限精确数且不能为bool/float") from exc
        if supplied_tick != factor.tick:
            raise ContractError("传入tick与factor绑定tick不匹配")
    if factor.coordinate_mode == EXACT_RESEARCH_COORDINATE:
        return result
    return round_to_tick(result, factor.tick)


def inverse_affine_price(value: object, factor: AdjustmentFactor) -> Fraction:
    if not isinstance(factor, AdjustmentFactor):
        raise ContractError("factor必须是AdjustmentFactor")
    try:
        price = exact_number(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError("目标价格必须是有限精确数且不能为bool/float") from exc
    result = (price - factor.beta) / factor.alpha
    if result <= 0:
        raise ContractError("逆变换后RAW价格必须大于0")
    return result


def transport_difference(value: object, factor: AdjustmentFactor) -> Fraction:
    if not isinstance(factor, AdjustmentFactor):
        raise ContractError("factor必须是AdjustmentFactor")
    try:
        difference = exact_number(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError("差值必须是有限精确数且不能为bool/float") from exc
    return factor.alpha * difference


def adjust_ohlc(
    ohlc: tuple[object, object, object, object],
    factor: AdjustmentFactor,
    *,
    tick: object | None = None,
) -> tuple[Fraction, Fraction, Fraction, Fraction]:
    if not isinstance(factor, AdjustmentFactor):
        raise ContractError("factor必须是AdjustmentFactor")
    if not isinstance(ohlc, tuple) or len(ohlc) != 4:
        raise ContractError("OHLC必须为按开高低收排列的四元组")

    raw_values = []
    for value in ohlc:
        try:
            raw_price = exact_number(value)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ContractError(
                "RAW OHLC价格必须是有限精确数且不能为bool/float"
            ) from exc
        if raw_price <= 0:
            raise ContractError("RAW OHLC价格必须大于0")
        raw_values.append(raw_price)
    raw_open, raw_high, raw_low, raw_close = raw_values
    if raw_high < max(raw_open, raw_low, raw_close) or raw_low > min(
        raw_open, raw_high, raw_close
    ):
        raise ContractError("RAW OHLC包络不合法")

    if tick is not None:
        try:
            supplied_tick = exact_number(tick)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ContractError("OHLC tick必须是有限精确数且不能为bool/float") from exc
        if supplied_tick != factor.tick:
            raise ContractError("OHLC tick与factor绑定tick不匹配")

    values = tuple(factor.alpha * value + factor.beta for value in raw_values)
    rounded = tuple(
        value
        if factor.coordinate_mode == EXACT_RESEARCH_COORDINATE
        else round_to_tick(value, factor.tick)
        for value in values
    )
    return rounded  # type: ignore[return-value]


def round_to_tick(value: object, tick: object) -> Fraction:
    try:
        number = exact_number(value)
        quantum = exact_number(tick)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ContractError("价格和tick必须是有限精确数且不能为bool/float") from exc
    if quantum <= 0:
        raise ContractError("tick必须为正数")
    scaled = number / quantum
    sign = -1 if scaled < 0 else 1
    magnitude = abs(scaled)
    whole, remainder = divmod(magnitude.numerator, magnitude.denominator)
    if remainder * 2 >= magnitude.denominator:
        whole += 1
    return sign * whole * quantum


@dataclass(frozen=True)
class AdjustedOHLC:
    """带有质量和完整目标坐标摘要的OHLC字段投影。"""

    field: FieldValue
    coordinate_id: str


def adjust_ohlc_field(
    field: FieldValue,
    factor: AdjustmentFactor,
    *,
    source_coordinate_id: str,
) -> AdjustedOHLC:
    """保留输入质量并将合法RAW OHLC运输到factor绑定的目标坐标。"""
    if not isinstance(field, FieldValue):
        raise ContractError("OHLC字段必须包含值和质量")
    if not isinstance(factor, AdjustmentFactor):
        raise ContractError("factor必须是AdjustmentFactor")
    if (
        not isinstance(source_coordinate_id, str)
        or source_coordinate_id != factor.source_basis
    ):
        raise ContractError("OHLC源坐标与factor.source_basis不匹配")
    coordinate_id = semantic_digest(
        ("PRICE_COORDINATE_R1", factor.target_coordinate_id)
    )
    if field.quality != "OBSERVED":
        return AdjustedOHLC(field, coordinate_id)
    adjusted = adjust_ohlc(field.value, factor)
    return AdjustedOHLC(FieldValue(adjusted), coordinate_id)
