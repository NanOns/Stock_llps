"""横截面 RPS、端点变化和相对市场的纯计算内核。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from fractions import Fraction
import hashlib
import json
from typing import Mapping

from stock_a.domain import ContractError, FieldValue, exact_number, unknown


@dataclass(frozen=True)
class ReturnEndpoint:
    """一个精确Universe内、在共同目标坐标重算的收益端点。

    调用边界须先将端点价格换到目标坐标，再从调整后价格计算收益；本内核
    校验坐标身份/版本，负责端点重排，不把坐标标签转换误作数值变换。
    """

    endpoint_date: date
    return_period: int
    universe_date: date
    universe_id: str
    universe_revision: str
    coordinate_id: str
    coordinate_revision: str
    producer_version: str
    returns: Mapping[str, FieldValue]


@dataclass(frozen=True)
class CrossSectionInput:
    """单个目标实体的横截面输入；映射键是固定的合同名称。"""

    target_date: date
    target_entity_id: str
    target_coordinate_id: str
    target_coordinate_revision: str
    # rank_endpoints: rps{5,20}_{current,minus1,minus3}
    rank_endpoints: Mapping[str, ReturnEndpoint]
    # relative_endpoints: rel_market_{1,3,5,20}
    relative_endpoints: Mapping[str, ReturnEndpoint]


@dataclass(frozen=True)
class CrossSectionMetric:
    target_entity_id: str
    field_id: str
    value: FieldValue
    # denominator_count是原Universe总成员数；known_count是实际公式分母。
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
    input_digest: str
    comparison_universe_id: str | None = None
    comparison_universe_revision: str | None = None
    comparison_universe_date: date | None = None
    comparison_endpoint_date: date | None = None
    comparison_denominator_count: int | None = None
    comparison_known_count: int | None = None
    comparison_unknown_count: int | None = None
    comparison_input_digest: str | None = None


_RANK_KEYS = frozenset(
    f"rps{period}_{endpoint}"
    for period in (5, 20)
    for endpoint in ("current", "minus1", "minus3")
)
_REL_KEYS = frozenset(f"rel_market_{period}" for period in (1, 3, 5, 20))
_DELTA_KEYS = (("rps5_delta1_pp", "rps5_current", "rps5_minus1"),
               ("rps5_delta3_pp", "rps5_current", "rps5_minus3"),
               ("rps20_delta1_pp", "rps20_current", "rps20_minus1"),
               ("rps20_delta3_pp", "rps20_current", "rps20_minus3"))


def _validate_endpoint(endpoint: ReturnEndpoint, *, target_date: date,
                        coordinate_id: str, coordinate_revision: str,
                        return_period: int, relative: bool = False) -> tuple[str, ...]:
    if not isinstance(endpoint, ReturnEndpoint):
        raise ContractError("横截面端点必须是ReturnEndpoint")
    if type(endpoint.endpoint_date) is not date or endpoint.endpoint_date > target_date:
        raise ContractError("端点日期无效或晚于目标日期")
    if type(endpoint.universe_date) is not date or endpoint.universe_date > endpoint.endpoint_date:
        raise ContractError("Universe快照日无效或晚于收益端点")
    if type(endpoint.return_period) is not int or endpoint.return_period != return_period:
        raise ContractError("收益窗口长度与字段合同不一致")
    if relative and endpoint.universe_date >= endpoint.endpoint_date:
        raise ContractError("相对市场必须引用目标收益窗口的起点Universe")
    if not relative and endpoint.universe_date != endpoint.endpoint_date:
        raise ContractError("RPS排名Universe必须绑定其排名端点日")
    for name in ("universe_id", "universe_revision", "coordinate_id",
                 "coordinate_revision", "producer_version"):
        value = getattr(endpoint, name)
        if not isinstance(value, str) or not value.strip():
            raise ContractError(f"横截面端点缺少{name}")
    if (endpoint.coordinate_id, endpoint.coordinate_revision) != (coordinate_id, coordinate_revision):
        raise ContractError("端点未换算到本次目标价格坐标及版本")
    if not isinstance(endpoint.returns, Mapping) or not endpoint.returns:
        raise ContractError("端点必须显式提供非空Universe收益映射")
    if any(not isinstance(key, str) or not key for key in endpoint.returns):
        raise ContractError("Universe实体标识必须是非空字符串")
    for item in endpoint.returns.values():
        if not isinstance(item, FieldValue):
            raise ContractError("每个Universe成员必须显式携带FieldValue质量")
        if item.quality == "OBSERVED":
            try:
                value = exact_number(item.value)
            except (ValueError, TypeError, ZeroDivisionError) as exc:
                raise ContractError("收益必须是有限精确数值且不能是bool/float") from exc
            if value <= -1:
                raise ContractError("正价格端点重算的收益必须严格大于-1")
    return tuple(sorted(endpoint.returns))


def _digest(endpoint: ReturnEndpoint, members: tuple[str, ...]) -> str:
    rows = []
    for entity_id in members:
        field = endpoint.returns[entity_id]
        if field.quality == "OBSERVED":
            number = exact_number(field.value)
            value = f"{number.numerator}/{number.denominator}"
        else:
            value = None
        rows.append((entity_id, field.quality, value, field.reason))
    encoded = json.dumps(
        [endpoint.endpoint_date.isoformat(), endpoint.return_period,
         endpoint.universe_date.isoformat(), endpoint.universe_id,
         endpoint.universe_revision, endpoint.coordinate_id,
         endpoint.coordinate_revision, endpoint.producer_version, rows],
        ensure_ascii=False, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _metadata(endpoint: ReturnEndpoint, members: tuple[str, ...]) -> tuple[int, int, int, str]:
    known = sum(endpoint.returns[item].quality == "OBSERVED" for item in members)
    denominator = len(members)
    return denominator, known, denominator - known, _digest(endpoint, members)


def _metric(value: FieldValue, endpoint: ReturnEndpoint,
            members: tuple[str, ...], *, target_entity_id: str, field_id: str,
            comparison: tuple[ReturnEndpoint, tuple[str, ...]] | None = None) -> CrossSectionMetric:
    denominator, known, missing, digest = _metadata(endpoint, members)
    comparison_fields = {}
    if comparison is not None:
        comparison_endpoint, comparison_members = comparison
        comparison_denominator, comparison_known, comparison_missing, comparison_digest = _metadata(
            comparison_endpoint, comparison_members)
        digest = hashlib.sha256(f"{digest}:{comparison_digest}".encode("ascii")).hexdigest()
        comparison_fields = {
            "comparison_universe_id": comparison_endpoint.universe_id,
            "comparison_universe_revision": comparison_endpoint.universe_revision,
            "comparison_universe_date": comparison_endpoint.universe_date,
            "comparison_endpoint_date": comparison_endpoint.endpoint_date,
            "comparison_denominator_count": comparison_denominator,
            "comparison_known_count": comparison_known,
            "comparison_unknown_count": comparison_missing,
            "comparison_input_digest": comparison_digest,
        }
    digest = hashlib.sha256(
        f"{digest}:{target_entity_id}:{field_id}".encode("utf-8")
    ).hexdigest()
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
        coordinate_id=endpoint.coordinate_id,
        coordinate_revision=endpoint.coordinate_revision,
        producer_version=endpoint.producer_version,
        input_digest=digest,
        **comparison_fields,
    )


def _rank(endpoint: ReturnEndpoint, members: tuple[str, ...], target_entity_id: str) -> FieldValue:
    known = [(entity_id, exact_number(endpoint.returns[entity_id].value))
             for entity_id in members
             if endpoint.returns[entity_id].quality == "OBSERVED"]
    if len(known) < 2:
        return unknown("INSUFFICIENT_RANK_DENOMINATOR")
    target_field = endpoint.returns.get(target_entity_id)
    if target_field is None:
        return unknown("TARGET_NOT_IN_ENDPOINT_UNIVERSE")
    if target_field.quality != "OBSERVED":
        return unknown(target_field.reason or "TARGET_RETURN_UNKNOWN")
    target = exact_number(target_field.value)
    less = sum(value < target for _, value in known)
    equal = sum(value == target for _, value in known)
    return FieldValue(Fraction(100 * (less + Fraction(equal - 1, 2)), len(known) - 1))


def _delta(first: CrossSectionMetric, second: CrossSectionMetric) -> FieldValue:
    if first.value.quality != "OBSERVED":
        return unknown(first.value.reason or "CURRENT_RPS_UNKNOWN")
    if second.value.quality != "OBSERVED":
        return unknown(second.value.reason or "PRIOR_RPS_UNKNOWN")
    return FieldValue(exact_number(first.value.value) - exact_number(second.value.value))


def _relative(endpoint: ReturnEndpoint, members: tuple[str, ...],
              target_entity_id: str) -> FieldValue:
    denominator = len(members)
    known_values = [exact_number(endpoint.returns[entity_id].value)
                    for entity_id in members
                    if endpoint.returns[entity_id].quality == "OBSERVED"]
    missing = denominator - len(known_values)
    if Fraction(missing, denominator) > Fraction(1, 5):
        return unknown("MISSING_MEMBER_SHARE_EXCEEDS_20_PERCENT")
    target = endpoint.returns.get(target_entity_id)
    if target is None:
        return unknown("TARGET_NOT_IN_START_UNIVERSE")
    if target.quality != "OBSERVED":
        return unknown(target.reason or "TARGET_RETURN_UNKNOWN")
    if not known_values:
        return unknown("EMPTY_KNOWN_UNIVERSE")
    return FieldValue(exact_number(target.value) - sum(known_values, Fraction()) / len(known_values))


def calculate_cross_section(inputs: CrossSectionInput) -> dict[str, CrossSectionMetric]:
    """计算RPS、端点重排变化及起点Universe相对收益，不执行IO。"""
    if not isinstance(inputs, CrossSectionInput):
        raise ContractError("横截面计算必须传入CrossSectionInput")
    if type(inputs.target_date) is not date:
        raise ContractError("横截面目标日必须是date")
    for name in ("target_entity_id", "target_coordinate_id", "target_coordinate_revision"):
        value = getattr(inputs, name)
        if not isinstance(value, str) or not value.strip():
            raise ContractError(f"横截面输入缺少{name}")
    if not isinstance(inputs.rank_endpoints, Mapping) or set(inputs.rank_endpoints) != _RANK_KEYS:
        raise ContractError("RPS端点必须精确包含当前、前1和前3会话的5/20窗口")
    if not isinstance(inputs.relative_endpoints, Mapping) or set(inputs.relative_endpoints) != _REL_KEYS:
        raise ContractError("相对市场端点必须精确包含1/3/5/20窗口")

    all_endpoints = {**inputs.rank_endpoints, **inputs.relative_endpoints}
    member_sets: dict[str, tuple[str, ...]] = {}
    for key, endpoint in all_endpoints.items():
        if key.startswith("rps"):
            period, relative = int(key.split("_", 1)[0].removeprefix("rps")), False
        else:
            period, relative = int(key.removeprefix("rel_market_")), True
        member_sets[key] = _validate_endpoint(
            endpoint, target_date=inputs.target_date,
            coordinate_id=inputs.target_coordinate_id,
            coordinate_revision=inputs.target_coordinate_revision,
            return_period=period, relative=relative,
        )
    for period in (5, 20):
        current = inputs.rank_endpoints[f"rps{period}_current"].endpoint_date
        prior1 = inputs.rank_endpoints[f"rps{period}_minus1"].endpoint_date
        prior3 = inputs.rank_endpoints[f"rps{period}_minus3"].endpoint_date
        if current != inputs.target_date or not prior3 < prior1 < current:
            raise ContractError("RPS端点日期必须为目标日及严格更早的前1/前3会话")
    if any(endpoint.endpoint_date != inputs.target_date
           for endpoint in inputs.relative_endpoints.values()):
        raise ContractError("相对市场各收益窗口必须绑定目标会话")

    result: dict[str, CrossSectionMetric] = {}
    for period in (5, 20):
        key = f"rps{period}_current"
        endpoint = inputs.rank_endpoints[key]
        result[f"cross.rps{period}"] = _metric(
            _rank(endpoint, member_sets[key], inputs.target_entity_id), endpoint, member_sets[key],
            target_entity_id=inputs.target_entity_id, field_id=f"cross.rps{period}")
    for output, current_key, prior_key in _DELTA_KEYS:
        # `output` encodes the family; reconstruct the matching current-rank key.
        family = output.split("_", 1)[0]
        current_metric = result[f"cross.{family}"]
        prior_endpoint = inputs.rank_endpoints[prior_key]
        prior_rank = _metric(_rank(prior_endpoint, member_sets[prior_key], inputs.target_entity_id),
                             prior_endpoint, member_sets[prior_key],
                             target_entity_id=inputs.target_entity_id, field_id=f"cross.{output}:comparison")
        result[f"cross.{output}"] = _metric(
            _delta(current_metric, prior_rank), inputs.rank_endpoints[current_key],
            member_sets[current_key],
            target_entity_id=inputs.target_entity_id, field_id=f"cross.{output}",
            comparison=(prior_endpoint, member_sets[prior_key]))
    for key, endpoint in inputs.relative_endpoints.items():
        result[f"cross.{key}"] = _metric(
            _relative(endpoint, member_sets[key], inputs.target_entity_id), endpoint, member_sets[key],
            target_entity_id=inputs.target_entity_id, field_id=f"cross.{key}")
    return result
