"""P03来源集合身份、尝试键和封口摘要的唯一确定性算法。"""

from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Any, Mapping
import uuid

from stock_a.canonical.seal_validation import canonical_json_bytes


_ATTEMPT_SCHEMA = "STOCK_A_SOURCE_SET_ATTEMPT_ID_R1"
_IDENTITY_SCHEMA = "STOCK_A_SOURCE_SET_IDENTITY_R1"
_SEAL_SCHEMA = "STOCK_A_SOURCE_SET_FINAL_SEAL_R1"


class SourceSetSealError(ValueError):
    """P03来源集合身份摘要缺少合同字段或类型错误。"""


def _canonical_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise SourceSetSealError("来源集合摘要时钟必须包含时区")
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_canonical_value(item) for item in value]
    return value


def _payload_digest(schema: str, payload: Mapping[str, Any]) -> str:
    return sha256(
        canonical_json_bytes(schema, _canonical_value(dict(payload)))
    ).hexdigest()


def source_set_attempt_id(
    *,
    namespace_id: str,
    target_session: str,
    package_sha256: str,
    revision_id: int,
    source_digest: str,
    plan_sha256: str,
    p02_binding_sha256: str,
) -> str:
    """生成不含durable_at的稳定PENDING定位键；它不是正式source_set身份。"""
    if type(revision_id) is not int or revision_id < 1:
        raise SourceSetSealError("尝试修订号必须为正整数")
    payload = {
        "namespace_id": str(uuid.UUID(namespace_id)),
        "target_session": target_session,
        "package_sha256": package_sha256,
        "revision_id": revision_id,
        "source_digest": source_digest,
        "plan_sha256": plan_sha256,
        "p02_binding_sha256": p02_binding_sha256,
    }
    digest = _payload_digest(_ATTEMPT_SCHEMA, payload)
    return str(uuid.uuid5(uuid.UUID(namespace_id), f"{_ATTEMPT_SCHEMA}:{digest}"))


def source_set_identity_digest(
    *,
    source_facts: Mapping[str, Any],
    p02_facts: Mapping[str, Any],
    durability_facts: Mapping[str, Any],
) -> str:
    """摘要依赖durable_at及来源、捕获、日历、P00和P02的完整typed绑定。"""
    required_source = (
        "namespace_id",
        "target_session",
        "package_sha256",
        "revision_id",
        "source_content_id",
        "source_capture_id",
        "source_digest",
        "source_raw_digest",
        "source_dataset_id",
        "source_byte_count",
        "parser_contract",
        "capture_expected_row_count",
        "raw_row_count",
        "target_present_count",
        "target_missing_count",
        "read_failed_count",
        "capture_read_started_at",
        "capture_validated_at",
        "source_available_at",
        "source_time_quality",
        "source_clock_role",
        "price_unit_status",
        "amount_unit_status",
        "volume_unit_status",
        "calendar_id",
        "calendar_revision",
        "calendar_sha256",
        "scope_id",
        "scope_contract_version",
        "scope_contract_digest",
        "scope_catalog_digest",
        "source_semantic_revision",
        "business_input_digest",
        "plan_sha256",
        "contract_sha256",
        "head_generation",
    )
    required_p02 = (
        "status",
        "plan_sha256",
        "policy_sha256",
        "binding_sha256",
        "policy_contract",
        "max_logins",
        "max_queries",
        "query_timeout_seconds",
        "reason_code",
        "data_quality",
        "actual_logins",
        "actual_queries",
        "network_accessed",
    )
    required_durability = (
        "writer_transaction_id",
        "observer_transaction_id",
        "pending_row_xmin",
        "pending_creation_transaction_id",
        "commit_ack_observed_at",
        "durable_at",
        "source_capture_id",
        "source_digest",
        "capability_digest",
    )
    for group, fields, label in (
        (source_facts, required_source, "来源"),
        (p02_facts, required_p02, "P02"),
        (durability_facts, required_durability, "持久性"),
    ):
        if not isinstance(group, Mapping) or any(key not in group for key in fields):
            raise SourceSetSealError(f"{label}身份缺少必要typed字段")
    payload = {
        "source": {key: source_facts[key] for key in required_source},
        "p02": {key: p02_facts[key] for key in required_p02},
        "durability": {key: durability_facts[key] for key in required_durability},
    }
    return _payload_digest(_IDENTITY_SCHEMA, payload)


def source_set_id_from_digest(identity_digest: str) -> str:
    """把完整身份摘要确定性映射为UUID；高128位保留SHA派生，版本/变体按RFC4122标记。"""
    if (
        not isinstance(identity_digest, str)
        or len(identity_digest) != 64
        or any(char not in "0123456789abcdef" for char in identity_digest)
    ):
        raise SourceSetSealError("source_set身份摘要必须是小写SHA-256")
    return str(uuid.UUID(bytes=bytes.fromhex(identity_digest[:32]), version=5))


def source_set_final_seal_digest(
    *,
    source_set_id: str,
    identity_digest: str,
    seal_writer_transaction_id: str,
    sealed_at: Any,
) -> str:
    """封口摘要绑定最终身份及执行SEALED更新的真实事务和观察时钟。"""
    if (
        not isinstance(seal_writer_transaction_id, str)
        or not seal_writer_transaction_id.isdecimal()
    ):
        raise SourceSetSealError("封口事务ID必须是十进制XID")
    return _payload_digest(
        _SEAL_SCHEMA,
        {
            "source_set_id": str(uuid.UUID(source_set_id)),
            "identity_digest": identity_digest,
            "seal_writer_transaction_id": seal_writer_transaction_id,
            "sealed_at": sealed_at,
        },
    )
