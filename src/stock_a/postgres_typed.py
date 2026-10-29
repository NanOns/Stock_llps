"""Canonical日线typed桥的纯映射与SQL计划；不持有连接，也不执行SQL。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from stock_a.canonical.selection import CanonicalReadSelection


_FIELDS = ("open", "high", "low", "close", "amount", "volume")
_PRICE_FIELDS = _FIELDS[:4]
_QUALITIES = {"OBSERVED", "UNKNOWN_DATA", "NOT_APPLICABLE"}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class TypedBridgeError(ValueError):
    """纯typed准备的稳定拒绝原因，不触发数据库或来源IO。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class StableIdentityBinding:
    """外部权威身份核验结果；来源代码自身不满足此合同。"""

    status: str
    entity_id: str
    identity_revision: str


@dataclass(frozen=True)
class UnitAuthority:
    """已独立验收的字段单位；调用方研究倍率不能充当本类型。"""

    status: str
    version: str
    amount_unit: str
    volume_unit: str


@dataclass(frozen=True)
class TypedFieldGroup:
    values: tuple[Decimal | int | None, ...]
    quality: str
    reason: str | None
    revision: str


@dataclass(frozen=True)
class CanonicalDailyCandidate:
    """供未来受控写入与读回核对使用的typed行；不是已持久化事实。"""

    namespace_id: str
    entity_id: str
    identity_revision: str
    session_date: date
    calendar_version: str
    canonical_revision: str
    source_capture_id: str
    source_sha256: str
    unit_version: str
    prices: TypedFieldGroup
    amount: TypedFieldGroup
    volume: TypedFieldGroup


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _finite_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise TypedBridgeError("FIELD_VALUE_INVALID", "布尔值不能作为数值字段")
    if isinstance(value, Decimal):
        result = value
    elif type(value) is int:
        result = Decimal(value)
    elif isinstance(value, Fraction):
        denominator = value.denominator
        for factor in (2, 5):
            while denominator % factor == 0:
                denominator //= factor
        if denominator != 1:
            raise TypedBridgeError("FIELD_VALUE_NOT_DECIMAL", "精确数值不能无损表示为PostgreSQL numeric")
        with localcontext() as context:
            context.prec = max(50, len(str(abs(value.numerator))) + len(str(value.denominator)) + 4)
            result = Decimal(value.numerator) / Decimal(value.denominator)
    else:
        raise TypedBridgeError("FIELD_VALUE_INVALID", "数值字段必须是Decimal、Fraction或整数")
    if not result.is_finite():
        raise TypedBridgeError("FIELD_VALUE_INVALID", "PostgreSQL numeric字段必须是有限数")
    return result


def _typed_group(
    *,
    values: tuple[Any, ...],
    quality: str,
    reason: str | None,
    unit: str,
    unit_version: str,
    semantic_context: Mapping[str, str],
    integer_only: bool = False,
) -> TypedFieldGroup:
    if quality not in _QUALITIES:
        raise TypedBridgeError("FIELD_QUALITY_INVALID", "Canonical质量不在typed字段合同内")
    if quality == "OBSERVED":
        if reason is not None or any(value is None for value in values):
            raise TypedBridgeError("FIELD_QUALITY_CONFLICT", "已知字段必须有值且不能携带未知原因")
        normalized: tuple[Decimal | int | None, ...] = tuple(
            value if integer_only and type(value) is int else _finite_decimal(value)
            for value in values
        )
        if integer_only and any(type(value) is not int for value in normalized):
            raise TypedBridgeError("FIELD_VALUE_INVALID", "成交量必须是整数股数")
    else:
        if reason is None or not isinstance(reason, str) or not reason.strip():
            raise TypedBridgeError("FIELD_QUALITY_CONFLICT", "未知或不适用字段必须保留明确原因")
        if len(reason.encode("utf-8")) > 128:
            raise TypedBridgeError("FIELD_REASON_TOO_LONG", "字段原因超过typed列合同的128字节上限")
        if any(value is not None for value in values):
            raise TypedBridgeError("FIELD_QUALITY_CONFLICT", "未知字段不能同时携带业务数值")
        normalized = tuple(None for _ in values)
    revision = _digest({
        "unit": unit,
        "unit_version": unit_version,
        "semantic_context": dict(sorted(semantic_context.items())),
        "quality": quality,
        "reason": reason,
        "values": [str(value) if isinstance(value, Decimal) else value for value in normalized],
    })
    return TypedFieldGroup(normalized, quality, reason, revision)


def map_canonical_selection_fields(selection: CanonicalReadSelection) -> dict[str, TypedFieldGroup]:
    """按价格、金额、数量三组质量无损映射选择结果，不要求或伪造实体身份。"""
    if not isinstance(selection, CanonicalReadSelection):
        raise TypedBridgeError("SELECTION_INVALID", "输入必须是现有CanonicalReadSelection")
    capture = selection.capture
    source_sha256 = getattr(capture, "content_sha256", None)
    if not isinstance(source_sha256, str) or _SHA256.fullmatch(source_sha256) is None:
        raise TypedBridgeError("SOURCE_REVISION_INVALID", "来源内容缺少有效SHA-256")
    by_name = {item.field_id.removeprefix("raw."): item for item in selection.target_fields}
    if len(by_name) != len(selection.target_fields) or set(by_name) != set(_FIELDS):
        raise TypedBridgeError("FIELD_SET_INVALID", "Canonical选择必须恰含六个来源字段且不得重复")
    if any(item.target_session != selection.target_session for item in by_name.values()):
        raise TypedBridgeError("SESSION_MISMATCH", "Canonical字段目标日期与选择日期不一致")
    unit_version = selection.units.version
    capture = selection.capture
    semantic_context = {
        "dataset_id": capture.dataset_id,
        "dataset_version": capture.dataset_version,
        "parser_version": capture.parser_version,
        "validation_contract": capture.validation_contract,
    }
    target_row = next((row for row in selection.rows if row.session == selection.target_session), None)
    if target_row is not None and target_row.coordinate_id is not None:
        if not isinstance(target_row.coordinate_id, str) or not target_row.coordinate_id.strip():
            raise TypedBridgeError("COORDINATE_BINDING_INVALID", "Canonical目标价格坐标绑定无效")
        semantic_context["price_coordinate_id"] = target_row.coordinate_id
    price_facts = tuple(by_name[name] for name in _PRICE_FIELDS)
    price_qualities = {(item.value.quality, item.value.reason) for item in price_facts}
    if len(price_qualities) != 1:
        raise TypedBridgeError("PRICE_GROUP_QUALITY_SPLIT", "OHLC四字段必须保留同一组质量与原因")
    price_quality, price_reason = next(iter(price_qualities))
    price_values = tuple(item.value.value for item in price_facts)
    if price_quality == "OBSERVED":
        prices = tuple(_finite_decimal(value) for value in price_values)
        opened, high, low, close = prices
        if low <= 0 or not low <= min(opened, close) <= max(opened, close) <= high:
            raise TypedBridgeError("PRICE_GROUP_INVALID", "OHLC不满足正价格及高低收开关系")
    else:
        prices = tuple(None for _ in _PRICE_FIELDS)
    price_group = _typed_group(
        values=prices, quality=price_quality, reason=price_reason,
        unit="CNY_PER_SHARE", unit_version=unit_version,
        semantic_context=semantic_context,
    )
    amount_fact = by_name["amount"]
    amount_value = amount_fact.value.value
    amount_group = _typed_group(
        values=(None if amount_value is None else _finite_decimal(amount_value),),
        quality=amount_fact.value.quality, reason=amount_fact.value.reason,
        unit=amount_fact.unit, unit_version=unit_version,
        semantic_context=semantic_context,
    )
    volume_fact = by_name["volume"]
    volume_value = volume_fact.value.value
    volume_group = _typed_group(
        values=(volume_value,), quality=volume_fact.value.quality,
        reason=volume_fact.value.reason, unit=volume_fact.unit,
        unit_version=unit_version, semantic_context=semantic_context,
        integer_only=True,
    )
    if amount_group.quality == "OBSERVED" and amount_group.values[0] < 0:
        raise TypedBridgeError("AMOUNT_INVALID", "已知成交金额必须非负")
    if volume_group.quality == "OBSERVED" and not 0 <= volume_group.values[0] <= 2**63 - 1:
        raise TypedBridgeError("VOLUME_INVALID", "已知成交量必须在bigint非负范围内")
    if price_group.quality != "OBSERVED" and any(value is not None for value in price_group.values):
        raise TypedBridgeError("PRICE_GROUP_QUALITY_CONFLICT", "非法OHLC不得保留部分价格值")
    return {"price": price_group, "amount": amount_group, "volume": volume_group}


def prepare_canonical_daily_candidate(
    selection: CanonicalReadSelection,
    *,
    namespace_id: str,
    identity: StableIdentityBinding,
    units: UnitAuthority,
) -> CanonicalDailyCandidate:
    """在真实身份与单位权威通过后生成typed行；当前TDX研究输入会被正确拒绝。"""
    groups = map_canonical_selection_fields(selection)
    if not isinstance(identity, StableIdentityBinding) or identity.status != "OFFICIAL_STABLE":
        raise TypedBridgeError("IDENTITY_NOT_BOUND", "缺少官方稳定security_id与有效身份版本，拒绝生成正式Canonical行")
    if (not isinstance(identity.entity_id, str) or not identity.entity_id
            or identity.entity_id == getattr(selection, "source_symbol", None)):
        raise TypedBridgeError("IDENTITY_NOT_BOUND", "来源代码不能替代稳定security_id")
    if not isinstance(identity.identity_revision, str) or _SHA256.fullmatch(identity.identity_revision) is None:
        raise TypedBridgeError("IDENTITY_REVISION_INVALID", "身份版本必须是有效SHA-256")
    if not isinstance(units, UnitAuthority) or units.status != "VERIFIED":
        raise TypedBridgeError("UNIT_AUTHORITY_NOT_BOUND", "调用方单位假设未通过独立验收，拒绝生成正式Canonical行")
    if (not isinstance(units.version, str) or not units.version.strip()
            or units.version != selection.units.version
            or units.amount_unit != "CNY" or units.volume_unit != "SHARES"):
        raise TypedBridgeError("UNIT_CONTRACT_MISMATCH", "单位权威与Canonical选择的版本或标准单位不一致")
    try:
        namespace = str(uuid.UUID(namespace_id))
        capture_id = str(uuid.UUID(selection.capture.capture_id))
    except (ValueError, AttributeError, TypeError) as error:
        raise TypedBridgeError("REFERENCE_INVALID", "namespace_id或source_capture_id不是有效UUID") from error
    if selection.calendar_evidence_status != "VERIFIED":
        raise TypedBridgeError("CALENDAR_NOT_VERIFIED", "缺少已核验的官方日历证据")
    calendar_version = _digest({"calendar_id": selection.calendar_id, "revision": selection.calendar_revision})
    semantic = {
        "namespace_id": namespace,
        "entity_id": identity.entity_id,
        "identity_revision": identity.identity_revision,
        "session_date": selection.target_session.isoformat(),
        "calendar_version": calendar_version,
        "unit_version": units.version,
        "field_revisions": {name: group.revision for name, group in groups.items()},
    }
    return CanonicalDailyCandidate(
        namespace_id=namespace,
        entity_id=identity.entity_id,
        identity_revision=identity.identity_revision,
        session_date=selection.target_session,
        calendar_version=calendar_version,
        canonical_revision=_digest(semantic),
        source_capture_id=capture_id,
        source_sha256=selection.capture.content_sha256,
        unit_version=units.version,
        prices=groups["price"], amount=groups["amount"], volume=groups["volume"],
    )


def canonical_daily_insert_plan(candidate: CanonicalDailyCandidate) -> tuple[str, tuple[Any, ...]]:
    """返回待审的参数化写入计划；本函数不执行DML。"""
    if not isinstance(candidate, CanonicalDailyCandidate):
        raise TypedBridgeError("CANDIDATE_INVALID", "写入计划必须来自已验证的typed候选")
    sql = """INSERT INTO stock.canonical_daily (
  namespace_id, entity_id, identity_revision, session_date, calendar_version,
  canonical_revision, source_capture_id,
  open, high, low, close, amount, volume,
  price_quality, price_reason, price_revision,
  amount_quality, amount_reason, amount_revision,
  volume_quality, volume_reason, volume_revision
) VALUES (
  %s, %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s, %s, %s, %s, %s
) ON CONFLICT (namespace_id, entity_id, session_date, canonical_revision) DO NOTHING"""
    params = (
        candidate.namespace_id, candidate.entity_id, candidate.identity_revision,
        candidate.session_date, candidate.calendar_version, candidate.canonical_revision,
        candidate.source_capture_id, *candidate.prices.values, candidate.amount.values[0],
        candidate.volume.values[0], candidate.prices.quality, candidate.prices.reason,
        candidate.prices.revision, candidate.amount.quality, candidate.amount.reason,
        candidate.amount.revision, candidate.volume.quality, candidate.volume.reason,
        candidate.volume.revision,
    )
    return sql, params


def canonical_daily_readback_plan(candidate: CanonicalDailyCandidate) -> tuple[str, tuple[Any, ...]]:
    """返回同一typed业务键/版本的参数化读回计划；本函数不执行SQL。"""
    if not isinstance(candidate, CanonicalDailyCandidate):
        raise TypedBridgeError("CANDIDATE_INVALID", "读回计划必须来自已验证的typed候选")
    sql = """SELECT namespace_id, entity_id, identity_revision, session_date,
  calendar_version, canonical_revision, source_capture_id,
  open, high, low, close, amount, volume,
  price_quality, price_reason, price_revision,
  amount_quality, amount_reason, amount_revision,
  volume_quality, volume_reason, volume_revision
FROM stock.canonical_daily
WHERE namespace_id = %s AND entity_id = %s AND session_date = %s
  AND canonical_revision = %s"""
    return sql, (
        candidate.namespace_id, candidate.entity_id, candidate.session_date,
        candidate.canonical_revision,
    )


_READBACK_COLUMNS = (
    "namespace_id", "entity_id", "identity_revision", "session_date",
    "calendar_version", "canonical_revision", "source_capture_id",
    "open", "high", "low", "close", "amount", "volume",
    "price_quality", "price_reason", "price_revision",
    "amount_quality", "amount_reason", "amount_revision",
    "volume_quality", "volume_reason", "volume_revision",
)


def _readback_expected(candidate: CanonicalDailyCandidate) -> dict[str, Any]:
    return {
        "namespace_id": candidate.namespace_id,
        "entity_id": candidate.entity_id,
        "identity_revision": candidate.identity_revision,
        "session_date": candidate.session_date,
        "calendar_version": candidate.calendar_version,
        "canonical_revision": candidate.canonical_revision,
        "source_capture_id": candidate.source_capture_id,
        "open": candidate.prices.values[0],
        "high": candidate.prices.values[1],
        "low": candidate.prices.values[2],
        "close": candidate.prices.values[3],
        "amount": candidate.amount.values[0],
        "volume": candidate.volume.values[0],
        "price_quality": candidate.prices.quality,
        "price_reason": candidate.prices.reason,
        "price_revision": candidate.prices.revision,
        "amount_quality": candidate.amount.quality,
        "amount_reason": candidate.amount.reason,
        "amount_revision": candidate.amount.revision,
        "volume_quality": candidate.volume.quality,
        "volume_reason": candidate.volume.reason,
        "volume_revision": candidate.volume.revision,
    }


def _rows_equal(expected: Any, actual: Any) -> bool:
    if isinstance(expected, Decimal):
        try:
            return _finite_decimal(actual) == expected
        except TypedBridgeError:
            return False
    if type(expected) is int:
        return type(actual) is int and actual == expected
    return type(actual) is type(expected) and actual == expected


def validate_canonical_daily_readback(
    candidate: CanonicalDailyCandidate,
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
) -> bool:
    """精确比对写后读回，防止DO NOTHING掩盖同修订异payload/来源冲突。"""
    if not isinstance(candidate, CanonicalDailyCandidate):
        raise TypedBridgeError("CANDIDATE_INVALID", "读回核验必须来自typed候选")
    materialized = tuple(rows)
    if not materialized:
        raise TypedBridgeError("CANONICAL_READBACK_MISSING", "写后精确读回未返回候选业务键")
    if len(materialized) != 1:
        raise TypedBridgeError("CANONICAL_READBACK_CARDINALITY", "同一Canonical业务键读回行数不是1")
    actual_row = materialized[0]
    if isinstance(actual_row, Mapping):
        if set(actual_row) != set(_READBACK_COLUMNS):
            raise TypedBridgeError("CANONICAL_READBACK_SHAPE", "读回字段集合与精确合同不一致")
        actual = dict(actual_row)
    elif isinstance(actual_row, Sequence) and not isinstance(actual_row, (str, bytes, bytearray)):
        if len(actual_row) != len(_READBACK_COLUMNS):
            raise TypedBridgeError("CANONICAL_READBACK_SHAPE", "读回列数与精确合同不一致")
        actual = dict(zip(_READBACK_COLUMNS, actual_row))
    else:
        raise TypedBridgeError("CANONICAL_READBACK_SHAPE", "读回行必须是具名映射或有序列")
    expected = _readback_expected(candidate)
    if any(not _rows_equal(value, actual[name]) for name, value in expected.items()):
        raise TypedBridgeError("CANONICAL_REVISION_CONFLICT", "同业务键和修订读回到不同payload或来源引用")
    return True
