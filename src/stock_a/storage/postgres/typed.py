"""Canonical日线typed桥的纯映射与SQL计划；不持有连接，也不执行SQL。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import json
import math
import re
import struct
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from stock_a.canonical.selection import CanonicalReadSelection


_FIELDS = ("open", "high", "low", "close", "amount", "volume")
_PRICE_FIELDS = _FIELDS[:4]
_QUALITIES = {"OBSERVED", "UNKNOWN_DATA", "NOT_APPLICABLE"}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AUTHORITY_PROOFS: dict[int, tuple[object, str, dict[str, Any]]] = {}
_MAX_AUTHORITY_PROOFS = 1024


class TypedBridgeError(ValueError):
    """纯typed准备的稳定拒绝原因，不触发数据库或来源IO。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class StableIdentityBinding:
    """经身份凭据验证器签发并绑定代码、有效期与来源捕获的稳定身份。"""

    status: str
    entity_id: str
    identity_revision: str
    source_symbol: str | None = None
    target_session: date | None = None
    valid_from: date | None = None
    valid_to: date | None = None
    source_capture_id: str | None = None
    source_sha256: str | None = None
    observed_at: datetime | None = None
    published_at: datetime | None = None
    available_at: datetime | None = None
    evidence_scope: str = "REAL_SOURCE"
    _authority_proof: object | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class UnitAuthority:
    """按字段、来源代码和日期范围签发的单位核对结果。"""

    status: str
    version: str
    amount_unit: str
    volume_unit: str
    amount_status: str = "UNKNOWN"
    volume_status: str = "UNKNOWN"
    amount_scale: Decimal | None = None
    volume_scale: int | None = None
    source_symbol: str | None = None
    target_session: date | None = None
    tdx_source_sha256: str | None = None
    baostock_source_sha256: str | None = None
    tdx_amount_raw: Fraction | None = None
    tdx_volume_raw: int | None = None
    evidence_scope: str = "REAL_SOURCE"
    _authority_proof: object | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class StableIdentityEvidence:
    """身份来源原始事实摘要；status由验证器推导，不接受调用方自行声明。"""

    source_authority: str
    source_symbol: str
    entity_id: str
    target_session: date
    valid_from: date
    valid_to: date | None
    source_capture_id: str
    source_sha256: str
    observed_at: datetime
    published_at: datetime | None = None
    available_at: datetime | None = None


@dataclass(frozen=True)
class UnitVerificationEvidence:
    """Bao未复权数值与TDX原始字段的单代码、单日独立核对输入。"""

    source_authority: str
    source_symbol: str
    target_session: date
    unit_version: str
    tdx_source_sha256: str
    baostock_source_sha256: str
    baostock_amount_cny: Decimal | None
    tdx_amount_raw: Fraction | None
    baostock_volume_shares: int | None
    tdx_volume_raw: int | None
    adjustflag: str
    coordinate: str
    observed_at: datetime
    baostock_capture_id: str | None = None


@dataclass(frozen=True)
class SourceContentCandidate:
    """只有原件或规范化响应的内容摘要与表示字节数；不包含源文件或响应payload。"""

    namespace_id: str
    content_id: str
    dataset_id: str
    byte_sha256: str
    byte_count: int


@dataclass(frozen=True)
class SourceCaptureCandidate:
    """来源首捕的typed摘要；时间角色显式且不替代available_at。"""

    namespace_id: str
    capture_id: str
    content_id: str
    parser_contract: str
    read_started_at: datetime
    validated_at: datetime
    source_available_at: datetime | None
    source_time_quality: str
    source_clock_role: str
    canonical_digest: str
    normalized_count: int


@dataclass(frozen=True)
class SecurityIdentityCandidate:
    """来源代码在一个有效期与一次观测下的身份事实；实体ID可保持未知。"""

    namespace_id: str
    source_symbol: str
    entity_id: str | None
    identity_status: str
    identity_revision: str
    valid_from: date | None
    valid_to: date | None
    source_capture_id: str
    observed_at: datetime
    published_at: datetime | None
    available_at: datetime | None
    security_name: str | None = None
    security_type: int | None = None
    listing_status: int | None = None
    target_session: date | None = None
    target_trade_status: int | None = None
    _authority_proof: object | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class CalendarSessionCandidate:
    """Canonical日线外键引用所需的单日官方交易日事实。"""

    namespace_id: str
    calendar_version: str
    session_date: date
    is_open: bool
    source_capture_id: str


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
    authority_scope: str = "REAL_SOURCE"
    _authority_proof: object | None = field(default=None, repr=False, compare=False)


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _aware_datetime(value: object) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def _remember_authority(kind: str, payload: dict[str, Any], factory):
    proof = object()
    result = factory(proof)
    _AUTHORITY_PROOFS[id(proof)] = (proof, kind, payload)
    while len(_AUTHORITY_PROOFS) > _MAX_AUTHORITY_PROOFS:
        _AUTHORITY_PROOFS.pop(next(iter(_AUTHORITY_PROOFS)))
    return result


def _authority_is_issued(authority: object, kind: str, fields: tuple[str, ...]) -> bool:
    proof = getattr(authority, "_authority_proof", None)
    record = _AUTHORITY_PROOFS.get(id(proof)) if proof is not None else None
    if record is None or record[0] is not proof or record[1] != kind:
        return False
    expected = record[2]
    return all(
        getattr(authority, name, object()) == expected.get(name) for name in fields
    )


def verify_stable_identity_evidence(
    evidence: StableIdentityEvidence,
) -> StableIdentityBinding:
    """唯一身份签发入口；Bao代码资料只投影为临时观察，不满足稳定实体合同。"""
    if not isinstance(evidence, StableIdentityEvidence):
        raise TypedBridgeError(
            "IDENTITY_EVIDENCE_INVALID", "稳定身份必须来自明确的身份来源证据"
        )
    source_symbol = (
        evidence.source_symbol.strip().upper()
        if isinstance(evidence.source_symbol, str)
        else ""
    )
    if re.fullmatch(r"(?:SH|SZ|BJ)\.[0-9]{6}", source_symbol) is None:
        raise TypedBridgeError("IDENTITY_EVIDENCE_INVALID", "身份来源代码格式无效")
    synthetic = evidence.source_authority == "SYNTHETIC_TEST"
    if (
        source_symbol == evidence.entity_id
        or not source_symbol
        or not isinstance(evidence.entity_id, str)
        or not evidence.entity_id.strip()
        or not _SHA256.fullmatch(evidence.source_sha256 or "")
        or not _aware_datetime(evidence.observed_at)
        or (
            evidence.published_at is not None
            and not _aware_datetime(evidence.published_at)
        )
        or (
            evidence.available_at is not None
            and not _aware_datetime(evidence.available_at)
        )
        or type(evidence.target_session) is not date
        or type(evidence.valid_from) is not date
        or (evidence.valid_to is not None and type(evidence.valid_to) is not date)
        or evidence.valid_from > evidence.target_session
        or (
            evidence.valid_to is not None
            and evidence.valid_to < evidence.target_session
        )
    ):
        raise TypedBridgeError(
            "IDENTITY_EVIDENCE_INVALID", "身份来源、稳定ID、有效区间或时钟字段不合法"
        )
    if synthetic:
        if not evidence.entity_id.startswith("SYNTHETIC:"):
            raise TypedBridgeError(
                "IDENTITY_EVIDENCE_INVALID", "合成身份必须使用隔离的SYNTHETIC命名空间"
            )
        evidence_scope = "SYNTHETIC_ONLY"
    elif evidence.source_authority == "EXCHANGE_STABLE_SECURITY_ID":
        raise TypedBridgeError(
            "IDENTITY_AUTHORITY_UNVERIFIED",
            "当前项目没有与交易所真实响应绑定的稳定security_id验证路径；拒绝正式身份签发",
        )
    else:
        raise TypedBridgeError(
            "IDENTITY_AUTHORITY_UNSUPPORTED",
            "当前来源未提供可验证的官方稳定security_id",
        )
    try:
        capture_id = str(uuid.UUID(evidence.source_capture_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise TypedBridgeError(
            "IDENTITY_EVIDENCE_INVALID", "身份来源捕获引用不是有效UUID"
        ) from error
    revision = _digest(
        {
            "contract": "STABLE_SECURITY_IDENTITY_R1",
            "source_authority": evidence.source_authority,
            "source_symbol": source_symbol,
            "entity_id": evidence.entity_id,
            "valid_from": evidence.valid_from.isoformat(),
            "valid_to": evidence.valid_to.isoformat() if evidence.valid_to else None,
            "source_sha256": evidence.source_sha256,
        }
    )
    payload = {
        "status": "OFFICIAL_STABLE",
        "entity_id": evidence.entity_id,
        "identity_revision": revision,
        "source_symbol": source_symbol,
        "target_session": evidence.target_session,
        "valid_from": evidence.valid_from,
        "valid_to": evidence.valid_to,
        "source_capture_id": capture_id,
        "source_sha256": evidence.source_sha256,
        "observed_at": evidence.observed_at,
        "published_at": evidence.published_at,
        "available_at": evidence.available_at,
        "evidence_scope": evidence_scope,
    }
    fields = tuple(payload)
    return _remember_authority(
        "IDENTITY",
        payload,
        lambda proof: StableIdentityBinding(**payload, _authority_proof=proof),
    )


def _float32_fraction(value: Fraction) -> Fraction | None:
    try:
        decoded = struct.unpack("<f", struct.pack("<f", float(value)))[0]
    except (OverflowError, struct.error, ValueError):
        return None
    if not math.isfinite(decoded):
        return None
    return Fraction.from_float(decoded)


def verify_unit_evidence(evidence: UnitVerificationEvidence) -> UnitAuthority:
    """独立重算同日同代码倍率；证明仅覆盖本样本，不能外推整个市场或日期范围。"""
    if not isinstance(evidence, UnitVerificationEvidence):
        raise TypedBridgeError(
            "UNIT_EVIDENCE_INVALID", "单位核验必须提供Bao与TDX原始字段证据"
        )
    source_symbol = (
        evidence.source_symbol.strip().upper()
        if isinstance(evidence.source_symbol, str)
        else ""
    )
    synthetic = evidence.source_authority == "SYNTHETIC_TEST"
    if evidence.source_authority not in {
        "BAOSTOCK_DAILY_VS_TDX_RAW_UNADJUSTED",
        "SYNTHETIC_TEST",
    }:
        raise TypedBridgeError("UNIT_EVIDENCE_INVALID", "单位核验证据来源未登记")
    if (
        re.fullmatch(r"(?:SH|SZ)\.[0-9]{6}", source_symbol) is None
        or type(evidence.target_session) is not date
        or not isinstance(evidence.unit_version, str)
        or not evidence.unit_version.strip()
        or not _SHA256.fullmatch(evidence.tdx_source_sha256 or "")
        or not _SHA256.fullmatch(evidence.baostock_source_sha256 or "")
        or evidence.adjustflag != "3"
        or evidence.coordinate != "RAW_UNADJUSTED"
        or not _aware_datetime(evidence.observed_at)
    ):
        raise TypedBridgeError(
            "UNIT_EVIDENCE_INVALID", "单位核对缺少同日未复权坐标、来源摘要或观测时钟"
        )

    if not synthetic:
        from stock_a.sources.baostock.client import resolve_verified_baostock_capture

        capture = resolve_verified_baostock_capture(evidence.baostock_capture_id)
        try:
            raw_amount = Decimal(capture.amount_raw) if capture is not None else None
            raw_volume = int(capture.volume_raw) if capture is not None else None
        except (ValueError, ArithmeticError):
            raw_amount, raw_volume = None, None
        if (
            capture is None
            or capture.source_symbol != source_symbol
            or capture.target_session != evidence.target_session
            or capture.adjustflag != evidence.adjustflag
            or capture.source_sha256 != evidence.baostock_source_sha256
            or capture.captured_at != evidence.observed_at
            or raw_amount != evidence.baostock_amount_cny
            or raw_volume != evidence.baostock_volume_shares
        ):
            raise TypedBridgeError(
                "UNIT_SOURCE_CAPTURE_UNBOUND",
                "真实单位核验必须绑定本进程已完成的BaoStock SDK响应、代码、日期、原始字段、捕获时钟和内容摘要",
            )

    amount_scale: Decimal | None = None
    amount_status = "UNKNOWN"
    bao_amount = evidence.baostock_amount_cny
    tdx_amount = evidence.tdx_amount_raw
    if (
        isinstance(bao_amount, Decimal)
        and bao_amount.is_finite()
        and bao_amount > 0
        and isinstance(tdx_amount, Fraction)
        and tdx_amount > 0
    ):
        possible_amount_scales = tuple(
            Decimal(value)
            for value in ("0.001", "0.01", "0.1", "1", "10", "100", "1000")
        )
        matches = [
            scale
            for scale in possible_amount_scales
            if _float32_fraction(Fraction(bao_amount) / Fraction(scale)) == tdx_amount
        ]
        if len(matches) == 1:
            amount_scale = matches[0]
            amount_status = "VERIFIED"

    volume_scale: int | None = None
    volume_status = "UNKNOWN"
    bao_volume = evidence.baostock_volume_shares
    tdx_volume = evidence.tdx_volume_raw
    if (
        type(bao_volume) is int
        and bao_volume > 0
        and type(tdx_volume) is int
        and tdx_volume > 0
    ):
        matches = [
            scale for scale in (1, 10, 100, 1000) if tdx_volume * scale == bao_volume
        ]
        if len(matches) == 1:
            volume_scale = matches[0]
            volume_status = "VERIFIED"

    evidence_scope = "SYNTHETIC_ONLY" if synthetic else "REAL_SOURCE"
    status = (
        "VERIFIED"
        if amount_status == volume_status == "VERIFIED"
        else ("PARTIAL" if "VERIFIED" in (amount_status, volume_status) else "UNKNOWN")
    )
    amount_unit = "CNY" if amount_status == "VERIFIED" else "UNKNOWN"
    volume_unit = "SHARES" if volume_status == "VERIFIED" else "UNKNOWN"
    payload = {
        "status": status,
        "version": evidence.unit_version,
        "amount_unit": amount_unit,
        "volume_unit": volume_unit,
        "amount_status": amount_status,
        "volume_status": volume_status,
        "amount_scale": amount_scale,
        "volume_scale": volume_scale,
        "source_symbol": source_symbol,
        "target_session": evidence.target_session,
        "tdx_source_sha256": evidence.tdx_source_sha256,
        "baostock_source_sha256": evidence.baostock_source_sha256,
        "tdx_amount_raw": tdx_amount,
        "tdx_volume_raw": tdx_volume,
        "evidence_scope": evidence_scope,
    }
    return _remember_authority(
        "UNIT",
        payload,
        lambda proof: UnitAuthority(**payload, _authority_proof=proof),
    )


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
            raise TypedBridgeError(
                "FIELD_VALUE_NOT_DECIMAL", "精确数值不能无损表示为PostgreSQL numeric"
            )
        with localcontext() as context:
            context.prec = max(
                50, len(str(abs(value.numerator))) + len(str(value.denominator)) + 4
            )
            result = Decimal(value.numerator) / Decimal(value.denominator)
    else:
        raise TypedBridgeError(
            "FIELD_VALUE_INVALID", "数值字段必须是Decimal、Fraction或整数"
        )
    if not result.is_finite():
        raise TypedBridgeError(
            "FIELD_VALUE_INVALID", "PostgreSQL numeric字段必须是有限数"
        )
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
        raise TypedBridgeError(
            "FIELD_QUALITY_INVALID", "Canonical质量不在typed字段合同内"
        )
    if quality == "OBSERVED":
        if reason is not None or any(value is None for value in values):
            raise TypedBridgeError(
                "FIELD_QUALITY_CONFLICT", "已知字段必须有值且不能携带未知原因"
            )
        normalized: tuple[Decimal | int | None, ...] = tuple(
            value if integer_only and type(value) is int else _finite_decimal(value)
            for value in values
        )
        if integer_only and any(type(value) is not int for value in normalized):
            raise TypedBridgeError("FIELD_VALUE_INVALID", "成交量必须是整数股数")
    else:
        if reason is None or not isinstance(reason, str) or not reason.strip():
            raise TypedBridgeError(
                "FIELD_QUALITY_CONFLICT", "未知或不适用字段必须保留明确原因"
            )
        if len(reason.encode("utf-8")) > 128:
            raise TypedBridgeError(
                "FIELD_REASON_TOO_LONG", "字段原因超过typed列合同的128字节上限"
            )
        if any(value is not None for value in values):
            raise TypedBridgeError(
                "FIELD_QUALITY_CONFLICT", "未知字段不能同时携带业务数值"
            )
        normalized = tuple(None for _ in values)
    revision = _digest(
        {
            "unit": unit,
            "unit_version": unit_version,
            "semantic_context": dict(sorted(semantic_context.items())),
            "quality": quality,
            "reason": reason,
            "values": [
                str(value) if isinstance(value, Decimal) else value
                for value in normalized
            ],
        }
    )
    return TypedFieldGroup(normalized, quality, reason, revision)


def map_canonical_selection_fields(
    selection: CanonicalReadSelection,
) -> dict[str, TypedFieldGroup]:
    """按价格、金额、数量三组质量无损映射选择结果，不要求或伪造实体身份。"""
    if not isinstance(selection, CanonicalReadSelection):
        raise TypedBridgeError(
            "SELECTION_INVALID", "输入必须是现有CanonicalReadSelection"
        )
    capture = selection.capture
    source_sha256 = getattr(capture, "content_sha256", None)
    if not isinstance(source_sha256, str) or _SHA256.fullmatch(source_sha256) is None:
        raise TypedBridgeError("SOURCE_REVISION_INVALID", "来源内容缺少有效SHA-256")
    by_name = {
        item.field_id.removeprefix("raw."): item for item in selection.target_fields
    }
    if len(by_name) != len(selection.target_fields) or set(by_name) != set(_FIELDS):
        raise TypedBridgeError(
            "FIELD_SET_INVALID", "Canonical选择必须恰含六个来源字段且不得重复"
        )
    if any(
        item.target_session != selection.target_session for item in by_name.values()
    ):
        raise TypedBridgeError(
            "SESSION_MISMATCH", "Canonical字段目标日期与选择日期不一致"
        )
    unit_version = selection.units.version
    capture = selection.capture
    semantic_context = {
        "dataset_id": capture.dataset_id,
        "dataset_version": capture.dataset_version,
        "parser_version": capture.parser_version,
        "validation_contract": capture.validation_contract,
    }
    target_row = next(
        (row for row in selection.rows if row.session == selection.target_session), None
    )
    if target_row is not None and target_row.coordinate_id is not None:
        if (
            not isinstance(target_row.coordinate_id, str)
            or not target_row.coordinate_id.strip()
        ):
            raise TypedBridgeError(
                "COORDINATE_BINDING_INVALID", "Canonical目标价格坐标绑定无效"
            )
        semantic_context["price_coordinate_id"] = target_row.coordinate_id
    price_facts = tuple(by_name[name] for name in _PRICE_FIELDS)
    price_context = {
        **semantic_context,
        "unit_basis": "|".join(item.unit_basis for item in price_facts),
    }
    price_qualities = {(item.value.quality, item.value.reason) for item in price_facts}
    if len(price_qualities) != 1:
        raise TypedBridgeError(
            "PRICE_GROUP_QUALITY_SPLIT", "OHLC四字段必须保留同一组质量与原因"
        )
    price_quality, price_reason = next(iter(price_qualities))
    price_values = tuple(item.value.value for item in price_facts)
    if price_quality == "OBSERVED":
        prices = tuple(_finite_decimal(value) for value in price_values)
        opened, high, low, close = prices
        if low <= 0 or not low <= min(opened, close) <= max(opened, close) <= high:
            raise TypedBridgeError(
                "PRICE_GROUP_INVALID", "OHLC不满足正价格及高低收开关系"
            )
    else:
        prices = tuple(None for _ in _PRICE_FIELDS)
    price_group = _typed_group(
        values=prices,
        quality=price_quality,
        reason=price_reason,
        unit="CNY_PER_SHARE",
        unit_version=unit_version,
        semantic_context=price_context,
    )
    amount_fact = by_name["amount"]
    amount_value = amount_fact.value.value
    amount_group = _typed_group(
        values=(None if amount_value is None else _finite_decimal(amount_value),),
        quality=amount_fact.value.quality,
        reason=amount_fact.value.reason,
        unit=amount_fact.unit,
        unit_version=unit_version,
        semantic_context={**semantic_context, "unit_basis": amount_fact.unit_basis},
    )
    volume_fact = by_name["volume"]
    volume_value = volume_fact.value.value
    volume_group = _typed_group(
        values=(volume_value,),
        quality=volume_fact.value.quality,
        reason=volume_fact.value.reason,
        unit=volume_fact.unit,
        unit_version=unit_version,
        semantic_context={**semantic_context, "unit_basis": volume_fact.unit_basis},
        integer_only=True,
    )
    if amount_group.quality == "OBSERVED" and amount_group.values[0] < 0:
        raise TypedBridgeError("AMOUNT_INVALID", "已知成交金额必须非负")
    if (
        volume_group.quality == "OBSERVED"
        and not 0 <= volume_group.values[0] <= 2**63 - 1
    ):
        raise TypedBridgeError("VOLUME_INVALID", "已知成交量必须在bigint非负范围内")
    if price_group.quality != "OBSERVED" and any(
        value is not None for value in price_group.values
    ):
        raise TypedBridgeError(
            "PRICE_GROUP_QUALITY_CONFLICT", "非法OHLC不得保留部分价格值"
        )
    return {"price": price_group, "amount": amount_group, "volume": volume_group}


def prepare_source_reference_candidates(
    namespace_id: str,
    capture: Any,
) -> tuple[SourceContentCandidate, SourceCaptureCandidate]:
    """从已完整扫描的来源封套准备内容摘要与首捕typed行，不保留原始字节。"""
    try:
        namespace = str(uuid.UUID(namespace_id))
        capture_id = str(uuid.UUID(capture.capture_id))
    except (ValueError, AttributeError, TypeError) as error:
        raise TypedBridgeError(
            "REFERENCE_INVALID", "来源封套缺少有效namespace或capture UUID"
        ) from error
    source_sha = getattr(capture, "content_sha256", None)
    digest = getattr(capture, "business_digest_sha256", None)
    if (
        not isinstance(source_sha, str)
        or not _SHA256.fullmatch(source_sha)
        or not isinstance(digest, str)
        or not _SHA256.fullmatch(digest)
    ):
        raise TypedBridgeError(
            "SOURCE_REVISION_INVALID", "来源封套缺少有效内容或业务摘要SHA-256"
        )
    if (
        type(getattr(capture, "byte_count", None)) is not int
        or capture.byte_count <= 0
        or type(getattr(capture, "record_count", None)) is not int
        or capture.record_count <= 0
    ):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "来源封套记录数和字节数必须为正整数"
        )
    started = getattr(capture, "observed_started_at", None)
    completed = getattr(capture, "observed_completed_at", None)
    if (
        not _aware_datetime(started)
        or not _aware_datetime(completed)
        or started > completed
    ):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "来源首捕时钟必须有时区且顺序正确"
        )
    dataset_id = getattr(capture, "dataset_id", None)
    parser = getattr(capture, "parser_version", None)
    contract = getattr(capture, "validation_contract", None)
    if not all(
        isinstance(value, str) and value.strip()
        for value in (dataset_id, parser, contract)
    ):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "来源封套缺少数据集、解析器或验证合同"
        )
    content_id = _digest({"dataset_id": dataset_id, "byte_sha256": source_sha})
    content = SourceContentCandidate(
        namespace, content_id, dataset_id, source_sha, capture.byte_count
    )
    entry = getattr(capture, "source_package_entry", None)
    available_at = (
        getattr(entry, "first_available_at", None) if entry is not None else None
    )
    if available_at is not None and not _aware_datetime(available_at):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "来源available_at必须是带时区时间或未知"
        )
    clock_role = (
        getattr(entry, "source_clock_role", "LOCAL_FILE_READ_TIME_ONLY")
        if entry is not None
        else "LOCAL_FILE_READ_TIME_ONLY"
    )
    time_quality = (
        "VERIFIED_SOURCE_TIME" if available_at is not None else "NON_PIT_OBSERVED"
    )
    capture_candidate = SourceCaptureCandidate(
        namespace_id=namespace,
        capture_id=capture_id,
        content_id=content_id,
        parser_contract=f"{parser}|{contract}",
        read_started_at=started,
        validated_at=completed,
        source_available_at=available_at,
        source_time_quality=time_quality,
        source_clock_role=clock_role,
        canonical_digest=digest,
        normalized_count=capture.record_count,
    )
    return content, capture_candidate


def prepare_baostock_source_reference_candidates(
    namespace_id: str,
    snapshot: Mapping[str, Any],
    *,
    capture_id: str | None = None,
) -> tuple[SourceContentCandidate, SourceCaptureCandidate]:
    """仅将本进程真实SDK已完成的Bao响应转为PG来源候选，不保存响应payload。"""
    if (
        not isinstance(snapshot, Mapping)
        or snapshot.get("status") != "RESPONSE_RECEIVED"
        or snapshot.get("source") != "BAOSTOCK"
    ):
        raise TypedBridgeError("SOURCE_CAPTURE_INVALID", "BaoStock来源快照未完整成功")
    calls = snapshot.get("calls")
    if not isinstance(calls, Mapping) or not calls:
        raise TypedBridgeError("SOURCE_CAPTURE_INVALID", "BaoStock来源快照缺少接口结果")
    flat_calls: list[Mapping[str, Any]] = []
    for value in calls.values():
        if isinstance(value, Mapping) and "query" in value:
            flat_calls.append(value)
        elif isinstance(value, Mapping):
            flat_calls.extend(
                item for item in value.values() if isinstance(item, Mapping)
            )
    if not flat_calls or any(
        item.get("status") != "RESPONSE_RECEIVED" or item.get("complete") is not True
        for item in flat_calls
    ):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "BaoStock来源快照含失败或未完整的接口结果"
        )
    rows_count = 0
    for item in flat_calls:
        rows = item.get("rows")
        fields = item.get("response_fields")
        if (
            not isinstance(rows, list)
            or not isinstance(fields, list)
            or item.get("rows_seen") != len(rows)
        ):
            raise TypedBridgeError(
                "SOURCE_CAPTURE_INVALID", "BaoStock接口行数与字段schema不匹配"
            )
        rows_count += len(rows)
    if rows_count <= 0:
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "BaoStock来源快照不含可规范化记录"
        )
    content_projection = {
        "contract": snapshot.get("contract"),
        "source": snapshot.get("source"),
        "calls": [
            {
                "query": item.get("query"),
                "target_session": item.get("target_session"),
                "response_fields": item.get("response_fields"),
                "rows": item.get("rows"),
            }
            for item in flat_calls
        ],
    }
    evidence_text = json.dumps(
        content_projection,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    source_sha = hashlib.sha256(evidence_text).hexdigest()
    canonical_digest = _digest(content_projection)
    try:
        namespace = str(uuid.UUID(namespace_id))
        started = min(
            datetime.fromisoformat(item["request_started_at"]) for item in flat_calls
        )
        completed = max(
            datetime.fromisoformat(item["request_completed_at"]) for item in flat_calls
        )
    except (ValueError, TypeError, KeyError, AttributeError) as error:
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "BaoStock接口时钟或来源引用格式非法"
        ) from error
    if (
        not _aware_datetime(started)
        or not _aware_datetime(completed)
        or started > completed
        or snapshot.get("capture_clock_role")
        != "CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE"
    ):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "BaoStock只读捕获时钟角色或顺序非法"
        )
    from stock_a.sources.baostock.client import (
        resolve_verified_baostock_identity_capture,
    )

    attestation = resolve_verified_baostock_identity_capture(
        snapshot.get("verified_source_capture_id")
    )
    if (
        attestation is None
        or attestation.source_sha256 != source_sha
        or attestation.canonical_digest != canonical_digest
        or attestation.normalized_count != rows_count
        or attestation.request_started_at != started
        or attestation.request_completed_at != completed
    ):
        raise TypedBridgeError(
            "SOURCE_CAPTURE_UNBOUND",
            "BaoStock正式来源候选必须绑定本进程真实SDK成功响应的内容、行数和观测时钟",
        )
    try:
        normalized_capture_id = str(uuid.UUID(attestation.capture_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise TypedBridgeError(
            "SOURCE_CAPTURE_INVALID", "BaoStock真实来源捕获引用非法"
        ) from error
    if capture_id is not None and capture_id != normalized_capture_id:
        raise TypedBridgeError(
            "SOURCE_CAPTURE_UNBOUND",
            "数据库capture_id必须使用真实SDK响应对应的捕获身份",
        )
    dataset_id = "BAOSTOCK_IDENTITY_REFERENCE"
    content_id = _digest({"dataset_id": dataset_id, "byte_sha256": source_sha})
    content = SourceContentCandidate(
        namespace, content_id, dataset_id, source_sha, len(evidence_text)
    )
    capture = SourceCaptureCandidate(
        namespace_id=namespace,
        capture_id=normalized_capture_id,
        content_id=content_id,
        parser_contract=f"{snapshot.get('contract')}|BaoStock-{snapshot.get('sdk_version')}",
        read_started_at=started,
        validated_at=completed,
        source_available_at=None,
        source_time_quality="NON_PIT_OBSERVED",
        source_clock_role="CAPTURED_AT_THIS_QUERY_NOT_HISTORICAL_FIRST_AVAILABLE",
        canonical_digest=canonical_digest,
        normalized_count=rows_count,
    )
    return content, capture


def prepare_security_identity_candidate(
    namespace_id: str,
    projection: Any,
    *,
    source_capture_id: str,
) -> SecurityIdentityCandidate:
    """签发来源观察候选；Bao只读投影不能生成稳定entity_id。"""
    try:
        namespace = str(uuid.UUID(namespace_id))
        capture_id = str(uuid.UUID(source_capture_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise TypedBridgeError(
            "REFERENCE_INVALID", "身份观察缺少有效namespace或source_capture UUID"
        ) from error
    if isinstance(projection, StableIdentityBinding):
        fields = (
            "status",
            "entity_id",
            "identity_revision",
            "source_symbol",
            "target_session",
            "valid_from",
            "valid_to",
            "source_capture_id",
            "source_sha256",
            "observed_at",
            "published_at",
            "available_at",
            "evidence_scope",
        )
        if (
            not _authority_is_issued(projection, "IDENTITY", fields)
            or projection.status != "OFFICIAL_STABLE"
            or projection.source_capture_id != capture_id
            or projection.evidence_scope != "REAL_SOURCE"
        ):
            raise TypedBridgeError(
                "IDENTITY_OBSERVATION_INVALID",
                "稳定身份观察必须来自匹配来源捕获的真实身份签发结果",
            )
        payload = {
            "namespace_id": namespace,
            "source_symbol": projection.source_symbol,
            "entity_id": projection.entity_id,
            "identity_status": projection.status,
            "identity_revision": projection.identity_revision,
            "valid_from": projection.valid_from,
            "valid_to": projection.valid_to,
            "source_capture_id": capture_id,
            "observed_at": projection.observed_at,
            "published_at": projection.published_at,
            "available_at": projection.available_at,
            "security_name": None,
            "security_type": None,
            "listing_status": None,
            "target_session": projection.target_session,
            "target_trade_status": None,
        }
        return _remember_authority(
            "IDENTITY_OBSERVATION",
            payload,
            lambda proof: SecurityIdentityCandidate(**payload, _authority_proof=proof),
        )

    symbol = getattr(projection, "source_symbol", None)
    target = getattr(projection, "target_session", None)
    status = getattr(projection, "identity_status", None)
    observed = getattr(projection, "observed_at", None)
    if (
        not isinstance(symbol, str)
        or not symbol.strip()
        or type(target) is not date
        or status not in {"PROVISIONAL", "UNMAPPED"}
        or not _aware_datetime(observed)
    ):
        raise TypedBridgeError(
            "IDENTITY_OBSERVATION_INVALID",
            "身份观察必须绑定代码、目标日、状态和实际观测时钟",
        )
    entity_id = getattr(projection, "entity_id", None)
    if entity_id is not None:
        raise TypedBridgeError(
            "IDENTITY_OBSERVATION_INVALID", "BaoStock暂定观察不允许携带稳定entity_id"
        )
    if re.fullmatch(r"(?:SH|SZ)\.[0-9]{6}", symbol) is None:
        raise TypedBridgeError(
            "IDENTITY_OBSERVATION_INVALID", "身份观察来源代码格式无效"
        )
    valid_from = getattr(projection, "ipo_date", None)
    valid_to = getattr(projection, "out_date", None)
    if (
        valid_from is not None
        and type(valid_from) is not date
        or valid_to is not None
        and type(valid_to) is not date
        or valid_from is not None
        and valid_to is not None
        and valid_from > valid_to
    ):
        raise TypedBridgeError("IDENTITY_OBSERVATION_INVALID", "身份有效期字段非法")
    name = getattr(projection, "security_name", None)
    if name is not None and (
        not isinstance(name, str) or len(name.encode("utf-8")) > 128
    ):
        raise TypedBridgeError(
            "IDENTITY_OBSERVATION_INVALID", "证券名称必须为空或不超过128字节"
        )
    security_type = getattr(projection, "security_type", None)
    listing_status = getattr(projection, "current_listing_status", None)
    trade_status = getattr(projection, "target_trade_status", None)
    published_at = getattr(projection, "published_at", None)
    available_at = getattr(projection, "available_at", None)
    if (published_at is not None and not _aware_datetime(published_at)) or (
        available_at is not None and not _aware_datetime(available_at)
    ):
        raise TypedBridgeError(
            "IDENTITY_OBSERVATION_INVALID", "来源发布时间和可用时间必须带时区或未知"
        )
    for value, allowed in (
        (security_type, {1, 2, 3, 4, 5}),
        (listing_status, {0, 1}),
        (trade_status, {0, 1}),
    ):
        if value is not None and (type(value) is not int or value not in allowed):
            raise TypedBridgeError(
                "IDENTITY_OBSERVATION_INVALID", "证券类型或状态枚举非法"
            )
    revision = _digest(
        {
            "contract": "SECURITY_IDENTITY_OBSERVATION_R1",
            "source_symbol": symbol,
            "entity_id": entity_id,
            "identity_status": status,
            "valid_from": valid_from.isoformat() if valid_from else None,
            "valid_to": valid_to.isoformat() if valid_to else None,
            "source_capture_id": capture_id,
            "observed_at": observed.isoformat(),
            "published_at": published_at.isoformat()
            if published_at is not None
            else None,
            "available_at": available_at.isoformat()
            if available_at is not None
            else None,
            "security_type": security_type,
            "listing_status": listing_status,
            "target_session": target.isoformat(),
            "target_trade_status": trade_status,
        }
    )
    payload = {
        "namespace_id": namespace,
        "source_symbol": symbol,
        "entity_id": None,
        "identity_status": status,
        "identity_revision": revision,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "source_capture_id": capture_id,
        "observed_at": observed,
        "published_at": published_at,
        "available_at": available_at,
        "security_name": name,
        "security_type": security_type,
        "listing_status": listing_status,
        "target_session": target,
        "target_trade_status": trade_status,
    }
    return _remember_authority(
        "IDENTITY_OBSERVATION",
        payload,
        lambda proof: SecurityIdentityCandidate(**payload, _authority_proof=proof),
    )


def prepare_calendar_session_candidate(
    namespace_id: str,
    calendar_version: str,
    session: date,
    *,
    source_capture_id: str,
) -> CalendarSessionCandidate:
    try:
        namespace = str(uuid.UUID(namespace_id))
        capture_id = str(uuid.UUID(source_capture_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise TypedBridgeError(
            "REFERENCE_INVALID", "交易日事实缺少有效namespace或source_capture UUID"
        ) from error
    if (
        not isinstance(calendar_version, str)
        or _SHA256.fullmatch(calendar_version) is None
        or type(session) is not date
    ):
        raise TypedBridgeError("CALENDAR_CANDIDATE_INVALID", "交易日日历版本或日期无效")
    return CalendarSessionCandidate(
        namespace, calendar_version, session, True, capture_id
    )


def _expect_readback(
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
    expected: Mapping[str, Any],
    *,
    code_prefix: str,
) -> bool:
    materialized = tuple(rows)
    if not materialized:
        raise TypedBridgeError(
            f"{code_prefix}_READBACK_MISSING", "幂等写入后精确读回没有返回业务键"
        )
    if len(materialized) != 1:
        raise TypedBridgeError(
            f"{code_prefix}_READBACK_CARDINALITY", "幂等读回的业务键行数不是1"
        )
    row = materialized[0]
    columns = tuple(expected)
    if isinstance(row, Mapping):
        if set(row) != set(columns):
            raise TypedBridgeError(
                f"{code_prefix}_READBACK_SHAPE", "幂等读回字段集合不符合合同"
            )
        actual = dict(row)
    elif isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        if len(row) != len(columns):
            raise TypedBridgeError(
                f"{code_prefix}_READBACK_SHAPE", "幂等读回列数不符合合同"
            )
        actual = dict(zip(columns, row, strict=True))
    else:
        raise TypedBridgeError(
            f"{code_prefix}_READBACK_SHAPE", "幂等读回行必须是映射或有序列"
        )
    if any(not _rows_equal(value, actual[key]) for key, value in expected.items()):
        raise TypedBridgeError(
            f"{code_prefix}_REVISION_CONFLICT", "相同业务键读回到不同来源或payload"
        )
    return True


def source_content_insert_plan(
    candidate: SourceContentCandidate,
) -> tuple[str, tuple[Any, ...]]:
    if not isinstance(candidate, SourceContentCandidate):
        raise TypedBridgeError("CONTENT_CANDIDATE_INVALID", "来源内容写入计划类型无效")
    sql = """INSERT INTO stock.source_contents (
  namespace_id, content_id, dataset_id, byte_sha, byte_count
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (namespace_id, content_id) DO NOTHING"""
    return sql, (
        candidate.namespace_id,
        candidate.content_id,
        candidate.dataset_id,
        candidate.byte_sha256,
        candidate.byte_count,
    )


def source_content_readback_plan(
    candidate: SourceContentCandidate,
) -> tuple[str, tuple[Any, ...]]:
    sql = """SELECT namespace_id::text, content_id, dataset_id, byte_sha, byte_count
FROM stock.source_contents WHERE namespace_id = %s AND content_id = %s"""
    return sql, (candidate.namespace_id, candidate.content_id)


def validate_source_content_readback(candidate: SourceContentCandidate, rows) -> bool:
    return _expect_readback(
        rows,
        {
            "namespace_id": candidate.namespace_id,
            "content_id": candidate.content_id,
            "dataset_id": candidate.dataset_id,
            "byte_sha": candidate.byte_sha256,
            "byte_count": candidate.byte_count,
        },
        code_prefix="SOURCE_CONTENT",
    )


def source_capture_insert_plan(
    candidate: SourceCaptureCandidate,
) -> tuple[str, tuple[Any, ...]]:
    if not isinstance(candidate, SourceCaptureCandidate):
        raise TypedBridgeError("CAPTURE_CANDIDATE_INVALID", "来源捕获写入计划类型无效")
    sql = """INSERT INTO stock.source_captures (
  namespace_id, capture_id, content_id, parser_contract, read_started_at,
  validated_at, source_available_at, source_time_quality, source_clock_role,
  canonical_digest, normalized_count
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (namespace_id, capture_id) DO NOTHING"""
    params = (
        candidate.namespace_id,
        candidate.capture_id,
        candidate.content_id,
        candidate.parser_contract,
        candidate.read_started_at,
        candidate.validated_at,
        candidate.source_available_at,
        candidate.source_time_quality,
        candidate.source_clock_role,
        candidate.canonical_digest,
        candidate.normalized_count,
    )
    return sql, params


def source_capture_readback_plan(
    candidate: SourceCaptureCandidate,
) -> tuple[str, tuple[Any, ...]]:
    sql = """SELECT namespace_id::text, capture_id::text, content_id, parser_contract,
  read_started_at, validated_at, source_available_at, source_time_quality,
  source_clock_role, canonical_digest, normalized_count
FROM stock.source_captures WHERE namespace_id = %s AND capture_id = %s"""
    return sql, (candidate.namespace_id, candidate.capture_id)


def validate_source_capture_readback(candidate: SourceCaptureCandidate, rows) -> bool:
    return _expect_readback(
        rows,
        {
            "namespace_id": candidate.namespace_id,
            "capture_id": candidate.capture_id,
            "content_id": candidate.content_id,
            "parser_contract": candidate.parser_contract,
            "read_started_at": candidate.read_started_at,
            "validated_at": candidate.validated_at,
            "source_available_at": candidate.source_available_at,
            "source_time_quality": candidate.source_time_quality,
            "source_clock_role": candidate.source_clock_role,
            "canonical_digest": candidate.canonical_digest,
            "normalized_count": candidate.normalized_count,
        },
        code_prefix="SOURCE_CAPTURE",
    )


def stable_entity_insert_plan(
    namespace_id: str, identity: StableIdentityBinding
) -> tuple[str, tuple[Any, ...]]:
    try:
        namespace = str(uuid.UUID(namespace_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise TypedBridgeError(
            "REFERENCE_INVALID", "实体候选namespace UUID无效"
        ) from error
    fields = (
        "status",
        "entity_id",
        "identity_revision",
        "source_symbol",
        "target_session",
        "valid_from",
        "valid_to",
        "source_capture_id",
        "source_sha256",
        "observed_at",
        "published_at",
        "available_at",
        "evidence_scope",
    )
    if (
        not isinstance(identity, StableIdentityBinding)
        or not _authority_is_issued(identity, "IDENTITY", fields)
        or identity.evidence_scope != "REAL_SOURCE"
    ):
        raise TypedBridgeError(
            "IDENTITY_NOT_BOUND", "实体表只接受真实来源签发的稳定身份"
        )
    sql = """INSERT INTO stock.entities (
  namespace_id, entity_id, identity_revision, source_capture_id, identity_state
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (namespace_id, entity_id, identity_revision) DO NOTHING"""
    return sql, (
        namespace,
        identity.entity_id,
        identity.identity_revision,
        identity.source_capture_id,
        identity.status,
    )


def stable_entity_readback_plan(
    namespace_id: str, identity: StableIdentityBinding
) -> tuple[str, tuple[Any, ...]]:
    sql = """SELECT namespace_id::text, entity_id, identity_revision, source_capture_id::text, identity_state
FROM stock.entities WHERE namespace_id = %s AND entity_id = %s AND identity_revision = %s"""
    return sql, (
        str(uuid.UUID(namespace_id)),
        identity.entity_id,
        identity.identity_revision,
    )


def validate_stable_entity_readback(
    namespace_id: str, identity: StableIdentityBinding, rows
) -> bool:
    return _expect_readback(
        rows,
        {
            "namespace_id": str(uuid.UUID(namespace_id)),
            "entity_id": identity.entity_id,
            "identity_revision": identity.identity_revision,
            "source_capture_id": identity.source_capture_id,
            "identity_state": identity.status,
        },
        code_prefix="STABLE_ENTITY",
    )


def security_identity_insert_plan(
    candidate: SecurityIdentityCandidate,
) -> tuple[str, tuple[Any, ...]]:
    fields = (
        "namespace_id",
        "source_symbol",
        "entity_id",
        "identity_status",
        "identity_revision",
        "valid_from",
        "valid_to",
        "source_capture_id",
        "observed_at",
        "published_at",
        "available_at",
        "security_name",
        "security_type",
        "listing_status",
        "target_session",
        "target_trade_status",
    )
    if not isinstance(candidate, SecurityIdentityCandidate) or not _authority_is_issued(
        candidate, "IDENTITY_OBSERVATION", fields
    ):
        raise TypedBridgeError("IDENTITY_CANDIDATE_INVALID", "身份观察写入计划类型无效")
    sql = """INSERT INTO stock.identity_observations (
  namespace_id, source_symbol, identity_revision, entity_id, identity_state,
  valid_from, valid_to, source_capture_id, observed_at, published_at, available_at,
  security_name, security_type, listing_status, target_session, target_trade_status
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (namespace_id, source_symbol, identity_revision) DO NOTHING"""
    params = tuple(
        getattr(candidate, name)
        for name in (
            "namespace_id",
            "source_symbol",
            "identity_revision",
            "entity_id",
            "identity_status",
            "valid_from",
            "valid_to",
            "source_capture_id",
            "observed_at",
            "published_at",
            "available_at",
            "security_name",
            "security_type",
            "listing_status",
            "target_session",
            "target_trade_status",
        )
    )
    return sql, params


def security_identity_readback_plan(
    candidate: SecurityIdentityCandidate,
) -> tuple[str, tuple[Any, ...]]:
    sql = """SELECT namespace_id::text, source_symbol, identity_revision, entity_id, identity_state,
  valid_from, valid_to, source_capture_id::text, observed_at, published_at, available_at,
  security_name, security_type, listing_status, target_session, target_trade_status
FROM stock.identity_observations
WHERE namespace_id = %s AND source_symbol = %s AND identity_revision = %s"""
    return sql, (
        candidate.namespace_id,
        candidate.source_symbol,
        candidate.identity_revision,
    )


def validate_security_identity_readback(
    candidate: SecurityIdentityCandidate, rows
) -> bool:
    expected = {
        "namespace_id": candidate.namespace_id,
        "source_symbol": candidate.source_symbol,
        "identity_revision": candidate.identity_revision,
        "entity_id": candidate.entity_id,
        "identity_state": candidate.identity_status,
        "valid_from": candidate.valid_from,
        "valid_to": candidate.valid_to,
        "source_capture_id": candidate.source_capture_id,
        "observed_at": candidate.observed_at,
        "published_at": candidate.published_at,
        "available_at": candidate.available_at,
        "security_name": candidate.security_name,
        "security_type": candidate.security_type,
        "listing_status": candidate.listing_status,
        "target_session": candidate.target_session,
        "target_trade_status": candidate.target_trade_status,
    }
    return _expect_readback(rows, expected, code_prefix="SECURITY_IDENTITY")


def calendar_session_insert_plan(
    candidate: CalendarSessionCandidate,
) -> tuple[str, tuple[Any, ...]]:
    sql = """INSERT INTO stock.calendar_sessions (
  namespace_id, calendar_version, session_date, is_open, source_capture_id
) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (namespace_id, calendar_version, session_date) DO NOTHING"""
    return sql, (
        candidate.namespace_id,
        candidate.calendar_version,
        candidate.session_date,
        candidate.is_open,
        candidate.source_capture_id,
    )


def calendar_session_readback_plan(
    candidate: CalendarSessionCandidate,
) -> tuple[str, tuple[Any, ...]]:
    sql = """SELECT namespace_id::text, calendar_version, session_date, is_open, source_capture_id::text
FROM stock.calendar_sessions
WHERE namespace_id = %s AND calendar_version = %s AND session_date = %s"""
    return sql, (
        candidate.namespace_id,
        candidate.calendar_version,
        candidate.session_date,
    )


def validate_calendar_session_readback(
    candidate: CalendarSessionCandidate, rows
) -> bool:
    return _expect_readback(
        rows,
        {
            "namespace_id": candidate.namespace_id,
            "calendar_version": candidate.calendar_version,
            "session_date": candidate.session_date,
            "is_open": candidate.is_open,
            "source_capture_id": candidate.source_capture_id,
        },
        code_prefix="CALENDAR_SESSION",
    )


def _authority_group(
    group: TypedFieldGroup,
    *,
    verified: bool,
    unit: str,
    unit_version: str,
    authority_revision: str,
) -> TypedFieldGroup:
    quality, reason, values = group.quality, group.reason, group.values
    if not verified and quality == "OBSERVED":
        quality, reason = "UNKNOWN_DATA", "UNIT_AUTHORITY_NOT_VERIFIED"
        values = tuple(None for _ in group.values)
    return _typed_group(
        values=values,
        quality=quality,
        reason=reason,
        unit=unit,
        unit_version=unit_version,
        semantic_context={
            "source_field_revision": group.revision,
            "unit_authority_revision": authority_revision,
        },
        integer_only=unit == "SHARES",
    )


def prepare_canonical_daily_candidate(
    selection: CanonicalReadSelection,
    *,
    namespace_id: str,
    identity: StableIdentityBinding,
    units: UnitAuthority,
    allow_synthetic_for_test: bool = False,
) -> CanonicalDailyCandidate:
    """只让签发器产生的身份和按字段核验的单位结果进入typed候选。"""
    groups = map_canonical_selection_fields(selection)
    identity_fields = (
        "status",
        "entity_id",
        "identity_revision",
        "source_symbol",
        "target_session",
        "valid_from",
        "valid_to",
        "source_capture_id",
        "source_sha256",
        "observed_at",
        "published_at",
        "available_at",
        "evidence_scope",
    )
    if (
        not isinstance(identity, StableIdentityBinding)
        or identity.status != "OFFICIAL_STABLE"
        or not _authority_is_issued(identity, "IDENTITY", identity_fields)
    ):
        raise TypedBridgeError(
            "IDENTITY_NOT_BOUND", "缺少由身份证据验证器签发的官方稳定security_id"
        )
    if (
        not isinstance(identity.entity_id, str)
        or not identity.entity_id.strip()
        or identity.entity_id == selection.source_symbol
        or identity.source_symbol != selection.source_symbol
        or identity.target_session != selection.target_session
        or identity.valid_from is None
        or identity.valid_from > selection.target_session
        or (
            identity.valid_to is not None
            and identity.valid_to < selection.target_session
        )
        or not isinstance(identity.identity_revision, str)
        or _SHA256.fullmatch(identity.identity_revision) is None
    ):
        raise TypedBridgeError(
            "IDENTITY_NOT_BOUND", "稳定身份未覆盖该代码和目标日，或版本字段非法"
        )
    if identity.evidence_scope == "SYNTHETIC_ONLY" and not allow_synthetic_for_test:
        raise TypedBridgeError("IDENTITY_NOT_BOUND", "合成身份只允许受控测试候选")

    unit_fields = (
        "status",
        "version",
        "amount_unit",
        "volume_unit",
        "amount_status",
        "volume_status",
        "amount_scale",
        "volume_scale",
        "source_symbol",
        "target_session",
        "tdx_source_sha256",
        "baostock_source_sha256",
        "tdx_amount_raw",
        "tdx_volume_raw",
        "evidence_scope",
    )
    if not isinstance(units, UnitAuthority) or not _authority_is_issued(
        units, "UNIT", unit_fields
    ):
        raise TypedBridgeError(
            "UNIT_AUTHORITY_NOT_BOUND", "单位结果未由同日跨源字段验证器签发"
        )
    if (
        not isinstance(units.version, str)
        or units.version != selection.units.version
        or units.source_symbol != selection.source_symbol
        or units.target_session != selection.target_session
        or units.tdx_source_sha256 != selection.capture.content_sha256
    ):
        raise TypedBridgeError(
            "UNIT_CONTRACT_MISMATCH",
            "单位证据与当前Canonical选择的来源、日期或版本不一致",
        )
    if units.evidence_scope == "SYNTHETIC_ONLY" and not allow_synthetic_for_test:
        raise TypedBridgeError(
            "UNIT_AUTHORITY_NOT_BOUND", "合成单位证据只允许受控测试候选"
        )
    if units.amount_status not in {
        "VERIFIED",
        "UNKNOWN",
    } or units.volume_status not in {"VERIFIED", "UNKNOWN"}:
        raise TypedBridgeError("UNIT_CONTRACT_MISMATCH", "单位证据的逐字段状态不合法")
    if units.amount_status == "VERIFIED" and (
        units.amount_unit != "CNY"
        or units.amount_scale != selection.units.amount_to_cny
        or selection.units.amount_to_cny is None
    ):
        raise TypedBridgeError(
            "UNIT_CONTRACT_MISMATCH", "成交额单位证据与实际解码倍率不一致"
        )
    if units.volume_status == "VERIFIED" and (
        units.volume_unit != "SHARES"
        or units.volume_scale != selection.units.volume_to_shares
        or selection.units.volume_to_shares is None
    ):
        raise TypedBridgeError(
            "UNIT_CONTRACT_MISMATCH", "成交量单位证据与实际解码倍率不一致"
        )
    if units.amount_status == "VERIFIED":
        normalized_amount = groups["amount"].values[0]
        raw_amount = (
            Fraction(normalized_amount) / Fraction(selection.units.amount_to_cny)
            if normalized_amount is not None
            else None
        )
        if units.tdx_amount_raw != raw_amount:
            raise TypedBridgeError(
                "UNIT_SOURCE_VALUE_MISMATCH",
                "单位证据中的TDX成交额与当前Canonical原始行不一致",
            )
    if units.volume_status == "VERIFIED":
        normalized_volume = groups["volume"].values[0]
        raw_volume = (
            normalized_volume // selection.units.volume_to_shares
            if normalized_volume is not None
            else None
        )
        if (
            normalized_volume is None
            or normalized_volume % selection.units.volume_to_shares
            or units.tdx_volume_raw != raw_volume
        ):
            raise TypedBridgeError(
                "UNIT_SOURCE_VALUE_MISMATCH",
                "单位证据中的TDX成交量与当前Canonical原始行不一致",
            )
    if units.amount_status == "UNKNOWN" and units.amount_unit != "UNKNOWN":
        raise TypedBridgeError("UNIT_CONTRACT_MISMATCH", "未验证成交额必须保持未知单位")
    if units.volume_status == "UNKNOWN" and units.volume_unit != "UNKNOWN":
        raise TypedBridgeError("UNIT_CONTRACT_MISMATCH", "未验证成交量必须保持未知单位")
    try:
        namespace = str(uuid.UUID(namespace_id))
        capture_id = str(uuid.UUID(selection.capture.capture_id))
    except (ValueError, AttributeError, TypeError) as error:
        raise TypedBridgeError(
            "REFERENCE_INVALID", "namespace_id或source_capture_id不是有效UUID"
        ) from error
    if selection.calendar_evidence_status != "VERIFIED":
        raise TypedBridgeError("CALENDAR_NOT_VERIFIED", "缺少已核验的官方日历证据")
    amount_authority_revision = _digest(
        {
            "status": units.amount_status,
            "unit": units.amount_unit,
            "scale": str(units.amount_scale)
            if units.amount_scale is not None
            else None,
            "version": units.version,
            "scope": units.evidence_scope,
            "symbol": units.source_symbol,
            "session": selection.target_session.isoformat(),
        }
    )
    volume_authority_revision = _digest(
        {
            "status": units.volume_status,
            "unit": units.volume_unit,
            "scale": units.volume_scale,
            "version": units.version,
            "scope": units.evidence_scope,
            "symbol": units.source_symbol,
            "session": selection.target_session.isoformat(),
        }
    )
    amount_group = _authority_group(
        groups["amount"],
        verified=units.amount_status == "VERIFIED",
        unit="CNY",
        unit_version=units.version,
        authority_revision=amount_authority_revision,
    )
    volume_group = _authority_group(
        groups["volume"],
        verified=units.volume_status == "VERIFIED",
        unit="SHARES",
        unit_version=units.version,
        authority_revision=volume_authority_revision,
    )
    calendar_version = _digest(
        {"calendar_id": selection.calendar_id, "revision": selection.calendar_revision}
    )
    semantic = {
        "namespace_id": namespace,
        "entity_id": identity.entity_id,
        "identity_revision": identity.identity_revision,
        "session_date": selection.target_session.isoformat(),
        "calendar_version": calendar_version,
        "unit_version": units.version,
        "field_revisions": {
            "price": groups["price"].revision,
            "amount": amount_group.revision,
            "volume": volume_group.revision,
        },
    }
    authority_scope = (
        "SYNTHETIC_ONLY"
        if (
            identity.evidence_scope == "SYNTHETIC_ONLY"
            or units.evidence_scope == "SYNTHETIC_ONLY"
        )
        else "REAL_SOURCE"
    )
    candidate_payload = {
        "namespace_id": namespace,
        "entity_id": identity.entity_id,
        "identity_revision": identity.identity_revision,
        "session_date": selection.target_session,
        "calendar_version": calendar_version,
        "canonical_revision": _digest(semantic),
        "source_capture_id": capture_id,
        "source_sha256": selection.capture.content_sha256,
        "unit_version": units.version,
        "prices": groups["price"],
        "amount": amount_group,
        "volume": volume_group,
        "authority_scope": authority_scope,
    }
    return _remember_authority(
        "CANONICAL_CANDIDATE",
        candidate_payload,
        lambda proof: CanonicalDailyCandidate(
            **candidate_payload, _authority_proof=proof
        ),
    )


def canonical_daily_insert_plan(
    candidate: CanonicalDailyCandidate,
    *,
    allow_synthetic_for_test: bool = False,
) -> tuple[str, tuple[Any, ...]]:
    """返回待审的参数化写入计划；本函数不执行DML。"""
    fields = (
        "namespace_id",
        "entity_id",
        "identity_revision",
        "session_date",
        "calendar_version",
        "canonical_revision",
        "source_capture_id",
        "source_sha256",
        "unit_version",
        "prices",
        "amount",
        "volume",
        "authority_scope",
    )
    if not isinstance(candidate, CanonicalDailyCandidate) or not _authority_is_issued(
        candidate, "CANONICAL_CANDIDATE", fields
    ):
        raise TypedBridgeError("CANDIDATE_INVALID", "写入计划必须来自已验证的typed候选")
    if candidate.authority_scope != "REAL_SOURCE" and not (
        allow_synthetic_for_test and candidate.authority_scope == "SYNTHETIC_ONLY"
    ):
        raise TypedBridgeError(
            "SYNTHETIC_CANDIDATE_NOT_WRITABLE", "合成候选禁止生成正式PG写入计划"
        )
    sql = """INSERT INTO stock.canonical_daily (
  namespace_id, entity_id, identity_revision, session_date, calendar_version,
  canonical_revision, source_capture_id, unit_version,
  open, high, low, close, amount, volume,
  price_quality, price_reason, price_revision,
  amount_quality, amount_reason, amount_revision,
  volume_quality, volume_reason, volume_revision
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s, %s, %s, %s, %s
) ON CONFLICT (namespace_id, entity_id, session_date, canonical_revision) DO NOTHING"""
    params = (
        candidate.namespace_id,
        candidate.entity_id,
        candidate.identity_revision,
        candidate.session_date,
        candidate.calendar_version,
        candidate.canonical_revision,
        candidate.source_capture_id,
        candidate.unit_version,
        *candidate.prices.values,
        candidate.amount.values[0],
        candidate.volume.values[0],
        candidate.prices.quality,
        candidate.prices.reason,
        candidate.prices.revision,
        candidate.amount.quality,
        candidate.amount.reason,
        candidate.amount.revision,
        candidate.volume.quality,
        candidate.volume.reason,
        candidate.volume.revision,
    )
    return sql, params


def canonical_daily_readback_plan(
    candidate: CanonicalDailyCandidate,
) -> tuple[str, tuple[Any, ...]]:
    """返回同一typed业务键/版本的参数化读回计划；本函数不执行SQL。"""
    fields = (
        "namespace_id",
        "entity_id",
        "identity_revision",
        "session_date",
        "calendar_version",
        "canonical_revision",
        "source_capture_id",
        "source_sha256",
        "unit_version",
        "prices",
        "amount",
        "volume",
        "authority_scope",
    )
    if not isinstance(candidate, CanonicalDailyCandidate) or not _authority_is_issued(
        candidate, "CANONICAL_CANDIDATE", fields
    ):
        raise TypedBridgeError("CANDIDATE_INVALID", "读回计划必须来自已验证的typed候选")
    sql = """SELECT namespace_id, entity_id, identity_revision, session_date,
  calendar_version, canonical_revision, source_capture_id, unit_version,
  open, high, low, close, amount, volume,
  price_quality, price_reason, price_revision,
  amount_quality, amount_reason, amount_revision,
  volume_quality, volume_reason, volume_revision
FROM stock.canonical_daily
WHERE namespace_id = %s AND entity_id = %s AND session_date = %s
  AND canonical_revision = %s"""
    return sql, (
        candidate.namespace_id,
        candidate.entity_id,
        candidate.session_date,
        candidate.canonical_revision,
    )


_READBACK_COLUMNS = (
    "namespace_id",
    "entity_id",
    "identity_revision",
    "session_date",
    "calendar_version",
    "canonical_revision",
    "source_capture_id",
    "unit_version",
    "open",
    "high",
    "low",
    "close",
    "amount",
    "volume",
    "price_quality",
    "price_reason",
    "price_revision",
    "amount_quality",
    "amount_reason",
    "amount_revision",
    "volume_quality",
    "volume_reason",
    "volume_revision",
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
        "unit_version": candidate.unit_version,
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
    fields = (
        "namespace_id",
        "entity_id",
        "identity_revision",
        "session_date",
        "calendar_version",
        "canonical_revision",
        "source_capture_id",
        "source_sha256",
        "unit_version",
        "prices",
        "amount",
        "volume",
        "authority_scope",
    )
    if not isinstance(candidate, CanonicalDailyCandidate) or not _authority_is_issued(
        candidate, "CANONICAL_CANDIDATE", fields
    ):
        raise TypedBridgeError("CANDIDATE_INVALID", "读回核验必须来自typed候选")
    materialized = tuple(rows)
    if not materialized:
        raise TypedBridgeError(
            "CANONICAL_READBACK_MISSING", "写后精确读回未返回候选业务键"
        )
    if len(materialized) != 1:
        raise TypedBridgeError(
            "CANONICAL_READBACK_CARDINALITY", "同一Canonical业务键读回行数不是1"
        )
    actual_row = materialized[0]
    if isinstance(actual_row, Mapping):
        if set(actual_row) != set(_READBACK_COLUMNS):
            raise TypedBridgeError(
                "CANONICAL_READBACK_SHAPE", "读回字段集合与精确合同不一致"
            )
        actual = dict(actual_row)
    elif isinstance(actual_row, Sequence) and not isinstance(
        actual_row, (str, bytes, bytearray)
    ):
        if len(actual_row) != len(_READBACK_COLUMNS):
            raise TypedBridgeError(
                "CANONICAL_READBACK_SHAPE", "读回列数与精确合同不一致"
            )
        actual = dict(zip(_READBACK_COLUMNS, actual_row))
    else:
        raise TypedBridgeError(
            "CANONICAL_READBACK_SHAPE", "读回行必须是具名映射或有序列"
        )
    expected = _readback_expected(candidate)
    if any(not _rows_equal(value, actual[name]) for name, value in expected.items()):
        raise TypedBridgeError(
            "CANONICAL_REVISION_CONFLICT", "同业务键和修订读回到不同payload或来源引用"
        )
    return True
