"""横截面 RPS、端点变化和相对市场的纯计算内核。"""

from __future__ import annotations

from dataclasses import dataclass
from bisect import bisect_left, bisect_right
from datetime import date
from fractions import Fraction
from typing import Mapping

from stock_a.domain import ContractError, FieldValue, exact_number, unknown
from stock_a.research.core import MarketSessionCalendar


@dataclass(frozen=True)
class PriceCoordinate:
    """一个实体在共同目标日的价格坐标身份。"""

    entity_id: str
    target_date: date
    coordinate_id: str
    revision: str


@dataclass(frozen=True)
class ReturnEndpoint:
    """指定端点Universe上的收益行及其逐实体共同目标坐标证明。"""

    endpoint_date: date
    return_period: int
    return_start_date: date
    universe_date: date
    universe_id: str
    universe_revision: str
    producer_version: str
    universe_members: tuple[str, ...]
    returns: Mapping[str, FieldValue]
    coordinates: Mapping[str, PriceCoordinate]
    window_statuses: Mapping[str, tuple[str, ...]]


@dataclass(frozen=True)
class CrossSectionInput:
    """单个目标实体的横截面输入；所有端点共用显式日历和目标坐标集。"""

    target_date: date
    target_entity_id: str
    calendar: MarketSessionCalendar
    target_coordinates: Mapping[str, PriceCoordinate]
    # rank_endpoints: rps{5,20}_{current,minus1,minus3}
    rank_endpoints: Mapping[str, ReturnEndpoint]
    # relative_endpoints: rel_market_{1,3,5,20}
    relative_endpoints: Mapping[str, ReturnEndpoint]


@dataclass(frozen=True)
class CrossSectionMetric:
    target_entity_id: str
    field_id: str
    value: FieldValue
    # denominator_count是全Universe成员数；RPS公式分母为known_count-1，均值分母为known_count。
    denominator_count: int
    known_count: int
    unknown_count: int
    universe_id: str
    universe_revision: str
    universe_date: date
    endpoint_date: date
    coordinate_id: str
    coordinate_revision: str
    producer_version: str
    comparison_universe_id: str | None = None
    comparison_universe_revision: str | None = None
    comparison_universe_date: date | None = None
    comparison_endpoint_date: date | None = None
    comparison_denominator_count: int | None = None
    comparison_known_count: int | None = None
    comparison_unknown_count: int | None = None


_RANK_KEYS = frozenset(
    f"rps{period}_{endpoint}"
    for period in (5, 20)
    for endpoint in ("current", "minus1", "minus3")
)
_REL_KEYS = frozenset(f"rel_market_{period}" for period in (1, 3, 5, 20))
_DELTA_KEYS = (
    ("rps5_delta1_pp", "rps5_current", "rps5_minus1"),
    ("rps5_delta3_pp", "rps5_current", "rps5_minus3"),
    ("rps20_delta1_pp", "rps20_current", "rps20_minus1"),
    ("rps20_delta3_pp", "rps20_current", "rps20_minus3"),
)


def _calendar_index(calendar: MarketSessionCalendar, session: date) -> int:
    try:
        return calendar.sessions.index(session)
    except ValueError as exc:
        raise ContractError("端点不是版本化官方市场会话") from exc


def _validate_calendar(
    calendar: MarketSessionCalendar, target_date: date
) -> dict[date, int]:
    if not isinstance(calendar, MarketSessionCalendar):
        raise ContractError("必须显式提供版本化官方市场会话日历")
    for name in ("calendar_id", "revision"):
        value = getattr(calendar, name)
        if not isinstance(value, str) or not value.strip():
            raise ContractError(f"市场会话日历缺少{name}")
    if not isinstance(calendar.sessions, tuple) or not calendar.sessions:
        raise ContractError("官方市场会话必须是非空有序元组")
    if any(type(item) is not date for item in calendar.sessions):
        raise ContractError("官方市场会话必须全部为date")
    if tuple(sorted(set(calendar.sessions))) != calendar.sessions:
        raise ContractError("官方市场会话不得重复或错序")
    if target_date not in calendar.sessions:
        raise ContractError("目标日不是版本化官方市场会话")
    return {session: index for index, session in enumerate(calendar.sessions)}


def _validate_endpoint(
    endpoint: ReturnEndpoint,
    *,
    target_date: date,
    calendar: MarketSessionCalendar,
    target_coordinates: Mapping[str, PriceCoordinate],
    return_period: int,
    relative: bool,
) -> tuple[str, ...]:
    if not isinstance(endpoint, ReturnEndpoint):
        raise ContractError("横截面端点必须是ReturnEndpoint")
    if type(endpoint.endpoint_date) is not date or endpoint.endpoint_date > target_date:
        raise ContractError("端点日期无效或晚于目标日期")
    if (
        type(endpoint.universe_date) is not date
        or type(endpoint.return_start_date) is not date
        or endpoint.universe_date > endpoint.endpoint_date
        or endpoint.return_start_date > endpoint.endpoint_date
    ):
        raise ContractError("Universe快照日或收益窗口起点无效")
    if (
        type(endpoint.return_period) is not int
        or endpoint.return_period != return_period
    ):
        raise ContractError("收益窗口长度与字段合同不一致")
    index = _calendar_index(calendar, endpoint.endpoint_date)
    if (
        index < return_period
        or calendar.sessions[index - return_period] != endpoint.return_start_date
    ):
        raise ContractError("收益窗口起点不是该官方市场会话窗口的精确起点")
    expected_universe_date = (
        endpoint.return_start_date if relative else endpoint.endpoint_date
    )
    if endpoint.universe_date != expected_universe_date:
        role = "相对市场起点Universe" if relative else "RPS排名端点Universe"
        raise ContractError(f"{role}日期与字段时间角色不一致")
    for name in ("universe_id", "universe_revision", "producer_version"):
        value = getattr(endpoint, name)
        if not isinstance(value, str) or not value.strip():
            raise ContractError(f"横截面端点缺少{name}")
    if (
        not isinstance(endpoint.universe_members, tuple)
        or not endpoint.universe_members
    ):
        raise ContractError("端点必须显式提供非空完整Universe成员清单")
    if any(not isinstance(key, str) or not key for key in endpoint.universe_members):
        raise ContractError("Universe实体标识必须是非空字符串")
    if tuple(sorted(set(endpoint.universe_members))) != endpoint.universe_members:
        raise ContractError("Universe成员清单不得重复且须按实体标识排序")
    if (
        not isinstance(endpoint.returns, Mapping)
        or not isinstance(endpoint.coordinates, Mapping)
        or not isinstance(endpoint.window_statuses, Mapping)
    ):
        raise ContractError("收益行、窗口状态和逐实体坐标绑定必须为映射")
    members = endpoint.universe_members
    if any(entity_id not in members for entity_id in endpoint.returns):
        raise ContractError("收益行包含不属于声明Universe的实体")
    if any(entity_id not in members for entity_id in endpoint.coordinates):
        raise ContractError("坐标绑定包含不属于声明Universe的实体")
    if any(entity_id not in members for entity_id in endpoint.window_statuses):
        raise ContractError("窗口状态包含不属于声明Universe的实体")
    legal_statuses = {"ACTUAL", "CONFIRMED_SUSPENSION", "PRE_LISTING", "UNKNOWN"}
    for entity_id, coordinate in endpoint.coordinates.items():
        expected = target_coordinates.get(entity_id)
        if (
            not isinstance(coordinate, PriceCoordinate)
            or not isinstance(expected, PriceCoordinate)
            or coordinate != expected
            or coordinate.entity_id != entity_id
            or type(coordinate.target_date) is not date
            or coordinate.target_date != target_date
            or not isinstance(coordinate.coordinate_id, str)
            or not coordinate.coordinate_id.strip()
            or not isinstance(coordinate.revision, str)
            or not coordinate.revision.strip()
        ):
            raise ContractError("端点含非法或错实体的逐实体坐标身份")
    for entity_id, statuses in endpoint.window_statuses.items():
        if (
            not isinstance(statuses, tuple)
            or len(statuses) != return_period + 1
            or any(
                not isinstance(status, str) or status not in legal_statuses
                for status in statuses
            )
        ):
            raise ContractError("端点含非法收益窗口状态元数据")
    for entity_id, item in endpoint.returns.items():
        if not isinstance(item, FieldValue):
            raise ContractError("每个收益行必须显式携带FieldValue质量")
        if item.quality == "OBSERVED":
            try:
                value = exact_number(item.value)
            except (ValueError, TypeError, ZeroDivisionError) as exc:
                raise ContractError("收益必须是有限精确数值且不能是bool/float") from exc
            if value <= -1:
                raise ContractError("正价格端点重算的收益必须严格大于-1")
            coordinate = endpoint.coordinates.get(entity_id)
            expected = target_coordinates.get(entity_id)
            if not isinstance(coordinate, PriceCoordinate) or not isinstance(
                expected, PriceCoordinate
            ):
                raise ContractError("已知收益缺少逐实体目标坐标绑定")
            if coordinate != expected:
                raise ContractError("收益坐标的实体、目标日或修订与共同目标坐标集不符")
            statuses = endpoint.window_statuses.get(entity_id)
            if (
                not isinstance(statuses, tuple)
                or len(statuses) != return_period + 1
                or statuses[0] != "ACTUAL"
                or statuses[-1] != "ACTUAL"
            ):
                raise ContractError("收益窗口必须显式绑定N+1个会话状态且两端为ACTUAL")
            if any(
                status not in {"ACTUAL", "CONFIRMED_SUSPENSION"} for status in statuses
            ):
                raise ContractError("收益窗口含未知/非实际缺口，不能冒充跨停牌收益")
    return members


def _field(endpoint: ReturnEndpoint, entity_id: str) -> FieldValue:
    if entity_id not in endpoint.universe_members:
        return unknown("ENTITY_NOT_IN_UNIVERSE")
    return endpoint.returns.get(entity_id, unknown("MISSING_RETURN_ROW"))


def _metadata(
    endpoint: ReturnEndpoint, members: tuple[str, ...]
) -> tuple[int, int, int]:
    known = sum(
        _field(endpoint, entity_id).quality == "OBSERVED" for entity_id in members
    )
    denominator = len(members)
    return denominator, known, denominator - known


def _metric(
    value: FieldValue,
    endpoint: ReturnEndpoint,
    members: tuple[str, ...],
    *,
    target_entity_id: str,
    target_coordinates: Mapping[str, PriceCoordinate],
    calendar: MarketSessionCalendar,
    field_id: str,
    comparison: tuple[ReturnEndpoint, tuple[str, ...]] | None = None,
) -> CrossSectionMetric:
    denominator, known, missing = _metadata(endpoint, members)
    coordinate = target_coordinates.get(target_entity_id)
    if (
        not isinstance(coordinate, PriceCoordinate)
        or coordinate.entity_id != target_entity_id
        or type(coordinate.target_date) is not date
        or not isinstance(coordinate.coordinate_id, str)
        or not coordinate.coordinate_id.strip()
        or not isinstance(coordinate.revision, str)
        or not coordinate.revision.strip()
    ):
        raise ContractError("输出目标坐标身份无效")
    comparison_fields = {}
    if comparison is not None:
        comparison_endpoint, comparison_members = comparison
        (
            comparison_denominator,
            comparison_known,
            comparison_missing,
        ) = _metadata(comparison_endpoint, comparison_members)
        comparison_fields = {
            "comparison_universe_id": comparison_endpoint.universe_id,
            "comparison_universe_revision": comparison_endpoint.universe_revision,
            "comparison_universe_date": comparison_endpoint.universe_date,
            "comparison_endpoint_date": comparison_endpoint.endpoint_date,
            "comparison_denominator_count": comparison_denominator,
            "comparison_known_count": comparison_known,
            "comparison_unknown_count": comparison_missing,
        }
    return CrossSectionMetric(
        target_entity_id=target_entity_id,
        field_id=field_id,
        value=value,
        denominator_count=denominator,
        known_count=known,
        unknown_count=missing,
        universe_id=endpoint.universe_id,
        universe_revision=endpoint.universe_revision,
        universe_date=endpoint.universe_date,
        endpoint_date=endpoint.endpoint_date,
        coordinate_id=coordinate.coordinate_id,
        coordinate_revision=coordinate.revision,
        producer_version=endpoint.producer_version,
        **comparison_fields,
    )


def _rank(
    endpoint: ReturnEndpoint, members: tuple[str, ...], target_entity_id: str
) -> FieldValue:
    known = sorted(
        exact_number(_field(endpoint, entity_id).value)
        for entity_id in members
        if _field(endpoint, entity_id).quality == "OBSERVED"
    )
    if len(known) < 2:
        return unknown("INSUFFICIENT_RANK_DENOMINATOR")
    target_field = _field(endpoint, target_entity_id)
    if target_field.quality != "OBSERVED":
        return unknown(target_field.reason or "TARGET_RETURN_UNKNOWN")
    target = exact_number(target_field.value)
    return _rank_known_values(known, target)


def _rank_known_values(ordered: list[Fraction], target: Fraction) -> FieldValue:
    """对同一已排序可评估横截面计算合同中秩。"""
    less = bisect_left(ordered, target)
    equal = bisect_right(ordered, target) - less
    return FieldValue(Fraction(100 * (less + Fraction(equal - 1, 2)), len(ordered) - 1))


def rank_return_endpoint(
    endpoint: ReturnEndpoint,
    *,
    target_date: date,
    calendar: MarketSessionCalendar,
    target_coordinates: Mapping[str, PriceCoordinate],
) -> dict[str, FieldValue]:
    """对显式完整端点一次排序，返回每个成员的RPS及原有未知原因。"""
    _validate_calendar(calendar, target_date)
    members = _validate_endpoint(
        endpoint,
        target_date=target_date,
        calendar=calendar,
        target_coordinates=target_coordinates,
        return_period=endpoint.return_period,
        relative=False,
    )
    ordered = sorted(
        exact_number(_field(endpoint, entity_id).value)
        for entity_id in members
        if _field(endpoint, entity_id).quality == "OBSERVED"
    )
    result = {}
    for entity_id in members:
        field = _field(endpoint, entity_id)
        if len(ordered) < 2:
            result[entity_id] = unknown("INSUFFICIENT_RANK_DENOMINATOR")
        elif field.quality != "OBSERVED":
            result[entity_id] = unknown(field.reason or "TARGET_RETURN_UNKNOWN")
        else:
            result[entity_id] = _rank_known_values(ordered, exact_number(field.value))
    return result


def _delta(first: CrossSectionMetric, second: CrossSectionMetric) -> FieldValue:
    if first.value.quality != "OBSERVED":
        return unknown(first.value.reason or "CURRENT_RPS_UNKNOWN")
    if second.value.quality != "OBSERVED":
        return unknown(second.value.reason or "PRIOR_RPS_UNKNOWN")
    return FieldValue(
        exact_number(first.value.value) - exact_number(second.value.value)
    )


def _relative(
    endpoint: ReturnEndpoint, members: tuple[str, ...], target_entity_id: str
) -> FieldValue:
    denominator = len(members)
    known_values = [
        exact_number(_field(endpoint, entity_id).value)
        for entity_id in members
        if _field(endpoint, entity_id).quality == "OBSERVED"
    ]
    missing = denominator - len(known_values)
    if Fraction(missing, denominator) > Fraction(1, 5):
        return unknown("MISSING_MEMBER_SHARE_EXCEEDS_20_PERCENT")
    target = _field(endpoint, target_entity_id)
    if target.quality != "OBSERVED":
        return unknown(target.reason or "TARGET_RETURN_UNKNOWN")
    if not known_values:
        return unknown("EMPTY_KNOWN_UNIVERSE")
    return FieldValue(
        exact_number(target.value) - sum(known_values, Fraction()) / len(known_values)
    )


def calculate_cross_section(inputs: CrossSectionInput) -> dict[str, CrossSectionMetric]:
    """计算RPS、端点重排变化及起点Universe相对收益，不执行IO。"""
    if not isinstance(inputs, CrossSectionInput):
        raise ContractError("横截面计算必须传入CrossSectionInput")
    if type(inputs.target_date) is not date:
        raise ContractError("横截面目标日必须是date")
    for name in ("target_entity_id",):
        value = getattr(inputs, name)
        if not isinstance(value, str) or not value.strip():
            raise ContractError(f"横截面输入缺少{name}")
    _validate_calendar(inputs.calendar, inputs.target_date)
    if _calendar_index(inputs.calendar, inputs.target_date) < 20:
        raise ContractError("版本化日历缺少相对收益窗口所需的20个前置会话")
    if not isinstance(inputs.target_coordinates, Mapping):
        raise ContractError("必须提供逐实体共同目标坐标集")
    if any(
        not isinstance(entity_id, str) or not entity_id
        for entity_id in inputs.target_coordinates
    ):
        raise ContractError("目标坐标集的实体标识必须为非空字符串")
    target_coordinate = inputs.target_coordinates.get(inputs.target_entity_id)
    if (
        not isinstance(target_coordinate, PriceCoordinate)
        or target_coordinate.entity_id != inputs.target_entity_id
        or target_coordinate.target_date != inputs.target_date
    ):
        raise ContractError("目标实体缺少与目标日绑定的价格坐标身份")
    for entity_id, coordinate in inputs.target_coordinates.items():
        if (
            not isinstance(coordinate, PriceCoordinate)
            or coordinate.entity_id != entity_id
            or coordinate.target_date != inputs.target_date
            or not isinstance(coordinate.coordinate_id, str)
            or not coordinate.coordinate_id.strip()
            or not isinstance(coordinate.revision, str)
            or not coordinate.revision.strip()
        ):
            raise ContractError("共同目标坐标集包含错实体、错日期或不完整的坐标身份")
    if (
        not isinstance(inputs.rank_endpoints, Mapping)
        or set(inputs.rank_endpoints) != _RANK_KEYS
    ):
        raise ContractError("RPS端点必须精确包含当前、前1和前3会话的5/20窗口")
    if (
        not isinstance(inputs.relative_endpoints, Mapping)
        or set(inputs.relative_endpoints) != _REL_KEYS
    ):
        raise ContractError("相对市场端点必须精确包含1/3/5/20窗口")

    all_endpoints = {**inputs.rank_endpoints, **inputs.relative_endpoints}
    member_sets: dict[str, tuple[str, ...]] = {}
    for key, endpoint in all_endpoints.items():
        if key.startswith("rps"):
            period = int(key.split("_", 1)[0].removeprefix("rps"))
        else:
            period = int(key.removeprefix("rel_market_"))
        member_sets[key] = _validate_endpoint(
            endpoint,
            target_date=inputs.target_date,
            calendar=inputs.calendar,
            target_coordinates=inputs.target_coordinates,
            return_period=period,
            relative=key.startswith("rel_market_"),
        )
    target_index = inputs.calendar.sessions.index(inputs.target_date)
    for period in (5, 20):
        for offset in (0, 1, 3):
            key = (
                f"rps{period}_current" if offset == 0 else f"rps{period}_minus{offset}"
            )
            endpoint = inputs.rank_endpoints[key]
            expected = inputs.calendar.sessions[target_index - offset]
            if endpoint.endpoint_date != expected:
                raise ContractError("RPS端点必须精确绑定目标日、前1或前3个官方市场会话")
    if any(
        endpoint.endpoint_date != inputs.target_date
        for endpoint in inputs.relative_endpoints.values()
    ):
        raise ContractError("相对市场各收益窗口必须绑定目标会话")

    result: dict[str, CrossSectionMetric] = {}
    for period in (5, 20):
        key = f"rps{period}_current"
        endpoint = inputs.rank_endpoints[key]
        result[f"cross.rps{period}"] = _metric(
            _rank(endpoint, member_sets[key], inputs.target_entity_id),
            endpoint,
            member_sets[key],
            target_entity_id=inputs.target_entity_id,
            target_coordinates=inputs.target_coordinates,
            calendar=inputs.calendar,
            field_id=f"cross.rps{period}",
        )
    for output, current_key, prior_key in _DELTA_KEYS:
        family = output.split("_", 1)[0]
        current_metric = result[f"cross.{family}"]
        prior_endpoint = inputs.rank_endpoints[prior_key]
        prior_rank = _metric(
            _rank(prior_endpoint, member_sets[prior_key], inputs.target_entity_id),
            prior_endpoint,
            member_sets[prior_key],
            target_entity_id=inputs.target_entity_id,
            target_coordinates=inputs.target_coordinates,
            calendar=inputs.calendar,
            field_id=f"cross.{output}:comparison",
        )
        result[f"cross.{output}"] = _metric(
            _delta(current_metric, prior_rank),
            inputs.rank_endpoints[current_key],
            member_sets[current_key],
            target_entity_id=inputs.target_entity_id,
            target_coordinates=inputs.target_coordinates,
            calendar=inputs.calendar,
            field_id=f"cross.{output}",
            comparison=(prior_endpoint, member_sets[prior_key]),
        )
    for key, endpoint in inputs.relative_endpoints.items():
        result[f"cross.{key}"] = _metric(
            _relative(endpoint, member_sets[key], inputs.target_entity_id),
            endpoint,
            member_sets[key],
            target_entity_id=inputs.target_entity_id,
            target_coordinates=inputs.target_coordinates,
            calendar=inputs.calendar,
            field_id=f"cross.{key}",
        )
    return result


CROSS_SECTION_CONTRACT_VERSION = "STOCK_A_CROSS_SECTION_10_R1"
CROSS_SECTION_10_FIELD_IDS = (
    "cross.rps5",
    "cross.rps20",
    "cross.rps5_delta1_pp",
    "cross.rps5_delta3_pp",
    "cross.rps20_delta1_pp",
    "cross.rps20_delta3_pp",
    "cross.rel_market_1",
    "cross.rel_market_3",
    "cross.rel_market_5",
    "cross.rel_market_20",
)


def _batch_rank_endpoint(
    endpoint: ReturnEndpoint,
    members: tuple[str, ...],
) -> tuple[dict[str, FieldValue], int]:
    """对单个端点做一次 O(n log n) 精确中秩排序并分发给全体成员。"""
    valid_pairs: list[tuple[Fraction, str]] = []
    for entity in members:
        fv = endpoint.returns[entity]
        if fv.quality == "OBSERVED":
            valid_pairs.append((exact_number(fv.value), entity))
    known_count = len(valid_pairs)
    if known_count < 2:
        return (
            {entity: unknown("INSUFFICIENT_EVALUABLE_MEMBERS") for entity in members},
            known_count,
        )
    valid_pairs.sort(key=lambda pair: pair[0])
    ranks: dict[str, FieldValue] = {}
    idx = 0
    denom = known_count - 1
    while idx < known_count:
        val = valid_pairs[idx][0]
        end = idx + 1
        while end < known_count and valid_pairs[end][0] == val:
            end += 1
        less = idx
        equal = end - idx
        score = (
            Fraction(100) * (Fraction(less) + Fraction(equal - 1, 2)) / Fraction(denom)
        )
        for pos in range(idx, end):
            ranks[valid_pairs[pos][1]] = FieldValue(score)
        idx = end
    for entity in members:
        if entity not in ranks:
            ranks[entity] = unknown("TARGET_RETURN_UNKNOWN")
    return ranks, known_count


def _batch_relative_endpoint(
    endpoint: ReturnEndpoint,
    members: tuple[str, ...],
) -> tuple[dict[str, FieldValue], int, int]:
    """对单个相对市场端点做一次 O(n) 起始日 Universe 等权均值计算并分发全池。"""
    denominator = len(members)
    if denominator == 0:
        return {}, 0, 0
    known_values: dict[str, Fraction] = {}
    for entity in members:
        fv = endpoint.returns[entity]
        if fv.quality == "OBSERVED":
            known_values[entity] = exact_number(fv.value)
    known_count = len(known_values)
    missing = denominator - known_count
    if known_count == 0:
        return (
            {entity: unknown("INSUFFICIENT_EVALUABLE_MEMBERS") for entity in members},
            denominator,
            0,
        )
    if Fraction(missing, denominator) > Fraction(1, 5):
        return (
            {
                entity: unknown("RELATIVE_MARKET_MISSING_RATIO_EXCEEDED")
                for entity in members
            },
            denominator,
            known_count,
        )
    market_mean = sum(known_values.values(), Fraction(0)) / Fraction(known_count)
    out: dict[str, FieldValue] = {}
    for entity in members:
        val = known_values.get(entity)
        if val is None:
            out[entity] = unknown("TARGET_RETURN_UNKNOWN")
        else:
            out[entity] = FieldValue(val - market_mean)
    return out, denominator, known_count


def calculate_cross_section_batch(
    *,
    target_date: date,
    calendar: MarketSessionCalendar,
    target_universe_members: Sequence[str],
    target_coordinates: Mapping[str, PriceCoordinate],
    rank_endpoints: Mapping[str, ReturnEndpoint | None],
    relative_endpoints: Mapping[str, ReturnEndpoint | None],
) -> dict[str, Any]:
    """全市场10项横截面因子批量短窗独立计算入口（ALG-09/10/11）。

    - 支持任意长窗端点为 None（窗口历史不足时仅局部降级该因子，绝不阻断短窗）；
    - 每个端点仅排序/求均值一次，再统一分发给目标日全体成员；
    - 严守中秩公式、起始日 Universe 绑定、缺失比例 >20% 拒绝与逐实体目标坐标校验。
    """
    if type(target_date) is not date:
        raise ContractError("横截面目标日必须是date")
    if not isinstance(calendar, MarketSessionCalendar):
        raise ContractError("横截面必须绑定版本化官方日历")
    if target_date not in calendar.sessions:
        raise ContractError("目标日不在官方交易日历中")
    target_members = tuple(target_universe_members)
    if len(set(target_members)) != len(target_members):
        raise ContractError("目标日Universe成员不得重复")
    target_index = calendar.sessions.index(target_date)

    # 校验并一次计算所有已提供的 rank_endpoints
    rank_maps: dict[str, dict[str, FieldValue]] = {}
    rank_known_counts: dict[str, int] = {}
    for period in (5, 20):
        for offset in (0, 1, 3):
            key = (
                f"rps{period}_current" if offset == 0 else f"rps{period}_minus{offset}"
            )
            ep = rank_endpoints.get(key)
            if ep is None:
                rank_known_counts[key] = 0
                continue
            if target_index < offset:
                raise ContractError(f"官方日历不足以支撑端点{key}")
            expected_date = calendar.sessions[target_index - offset]
            if ep.endpoint_date != expected_date:
                raise ContractError(f"RPS端点{key}日期错配：期望{expected_date}")
            members = _validate_endpoint(
                ep,
                target_date=target_date,
                calendar=calendar,
                target_coordinates=target_coordinates,
                return_period=period,
                relative=False,
            )
            r_map, k_cnt = _batch_rank_endpoint(ep, members)
            rank_maps[key] = r_map
            rank_known_counts[key] = k_cnt

    # 校验并一次计算所有已提供的 relative_endpoints (1, 3, 5, 20)
    rel_maps: dict[str, dict[str, FieldValue]] = {}
    rel_meta: dict[str, dict[str, Any]] = {}
    for period in (1, 3, 5, 20):
        key = f"rel_market_{period}"
        ep = relative_endpoints.get(key)
        if ep is None:
            rel_meta[key] = {
                "start_session": None,
                "denominator": 0,
                "known_count": 0,
            }
            continue
        if ep.endpoint_date != target_date:
            raise ContractError(f"相对市场端点{key}必须绑定目标日{target_date}")
        members = _validate_endpoint(
            ep,
            target_date=target_date,
            calendar=calendar,
            target_coordinates=target_coordinates,
            return_period=period,
            relative=True,
        )
        r_map, denom, k_cnt = _batch_relative_endpoint(ep, members)
        rel_maps[key] = r_map
        rel_meta[key] = {
            "start_session": ep.return_start_date,
            "denominator": denom,
            "known_count": k_cnt,
        }

    # 分发到目标日全体成员
    by_entity: dict[str, dict[str, FieldValue]] = {}
    for entity in target_members:
        fields: dict[str, FieldValue] = {}
        # rps5 / rps20
        for period in (5, 20):
            cur_key = f"rps{period}_current"
            cur_map = rank_maps.get(cur_key)
            cur_fv = (
                cur_map.get(entity, unknown("ENTITY_NOT_IN_ENDPOINT_UNIVERSE"))
                if cur_map is not None
                else unknown("ENDPOINT_WINDOW_UNAVAILABLE")
            )
            fields[f"cross.rps{period}"] = cur_fv
            for offset in (1, 3):
                prior_key = f"rps{period}_minus{offset}"
                prior_map = rank_maps.get(prior_key)
                out_key = f"cross.rps{period}_delta{offset}_pp"
                if cur_map is None or prior_map is None:
                    fields[out_key] = unknown("ENDPOINT_WINDOW_UNAVAILABLE")
                else:
                    prior_fv = prior_map.get(
                        entity, unknown("ENTITY_NOT_IN_PRIOR_UNIVERSE")
                    )
                    if cur_fv.quality == "OBSERVED" and prior_fv.quality == "OBSERVED":
                        fields[out_key] = FieldValue(
                            exact_number(cur_fv.value) - exact_number(prior_fv.value)
                        )
                    else:
                        fields[out_key] = unknown(
                            cur_fv.reason
                            if cur_fv.quality != "OBSERVED"
                            else (prior_fv.reason or "PRIOR_RPS_UNKNOWN")
                        )
        # rel_market_1 / 3 / 5 / 20
        for period in (1, 3, 5, 20):
            key = f"rel_market_{period}"
            r_map = rel_maps.get(key)
            if r_map is None:
                fields[f"cross.{key}"] = unknown("ENDPOINT_WINDOW_UNAVAILABLE")
            else:
                fields[f"cross.{key}"] = r_map.get(
                    entity, unknown("TARGET_NOT_IN_START_UNIVERSE")
                )
        by_entity[entity] = fields

    return {
        "target_date": target_date,
        "contract_version": CROSS_SECTION_CONTRACT_VERSION,
        "target_universe_count": len(target_members),
        "rps5_known_count": rank_known_counts.get("rps5_current", 0),
        "rps20_known_count": rank_known_counts.get("rps20_current", 0),
        "relative_metadata": rel_meta,
        "by_entity": by_entity,
    }
