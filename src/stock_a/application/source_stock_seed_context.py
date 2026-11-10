"""为现有个股详情读取精确来源修订下已保存的Seed解释。"""

from __future__ import annotations

from datetime import date, datetime, timezone
import re
from typing import Any, Mapping

from stock_a.storage.postgres.source_raw import RawSourcePgError
from stock_a.storage.postgres.static_research_facts import (
    StaticResearchPgError,
    read_source_stock_seed_fact,
)


_SHA256 = re.compile(r"^[0-9a-f]{64}$")

_SEED_QUALITY_FIELDS = (
    ("core.bias20_atr", "core_bias20_atr"),
    ("core.liquidity_mean20", "core_liquidity_mean20"),
    ("core.price_damage", "core_price_damage"),
    ("profile.compression", "profile_compression"),
    ("profile.extension", "profile_extension"),
    ("profile.ma_structure", "profile_ma_structure"),
    ("rps5_delta3", "rps5_delta3"),
)


def _timestamp_identity(value: Any) -> str | None:
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


def _reasons(value: Any) -> list[str]:
    if not isinstance(value, (tuple, list)):
        return []
    return [item for item in value if isinstance(item, str) and item]


def _unavailable(
    *,
    status: str,
    reason_code: str,
    message: str,
    identity: Mapping[str, Any],
    partition: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "reason_code": reason_code,
        "message": message,
        "identity": dict(identity),
        "partition": dict(partition or {}),
        "formal_candidate_status": "NOT_EVALUATED",
        "database_write": False,
        "ddl_executed": False,
    }


def query_source_stock_seed_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    namespace_id: str,
    source_capture_id: str,
    source_content_id: str,
    source_symbol: str,
    profile_revision: str | None,
    expected_source_count: int,
    source_time_quality: str,
    source_clock_role: str,
    source_available_at: Any,
) -> dict[str, Any]:
    """不重算Seed，按完整PG来源身份投影一个来源股票的已存事实。"""
    identity = {
        "namespace_id": namespace_id,
        "target_session": target_session.isoformat(),
        "source_symbol": source_symbol,
        "package_sha256": package_sha256,
        "source_capture_id": source_capture_id,
        "source_content_id": source_content_id,
        "profile_revision": profile_revision,
    }
    if profile_revision is None:
        return _unavailable(
            status="NOT_COMPARABLE",
            reason_code="PROFILE_REVISION_UNAVAILABLE",
            message="没有唯一适用的画像修订；拒绝从其他修订选择Seed。",
            identity=identity,
        )
    if (
        not isinstance(profile_revision, str)
        or _SHA256.fullmatch(profile_revision) is None
    ):
        return _unavailable(
            status="NOT_COMPARABLE",
            reason_code="PROFILE_REVISION_INVALID",
            message="画像修订不是精确的小写SHA-256。",
            identity=identity,
        )

    try:
        stored = read_source_stock_seed_fact(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            namespace_id=namespace_id,
            source_capture_id=source_capture_id,
            source_content_id=source_content_id,
            profile_revision=profile_revision,
            source_symbol=source_symbol,
            expected_source_count=expected_source_count,
        )
    except StaticResearchPgError as error:
        return _unavailable(
            status="INCOMPLETE",
            reason_code=error.code,
            message=str(error),
            identity=identity,
        )
    except RawSourcePgError as error:
        return _unavailable(
            status="UNAVAILABLE",
            reason_code=error.code,
            message=str(error),
            identity=identity,
        )

    if stored["status"] != "SAVED":
        return _unavailable(
            status=stored["status"],
            reason_code=stored["reason_code"],
            message=stored["message"],
            identity=identity,
            partition=stored.get("partition"),
        )

    row = stored["row"]
    if (
        str(row["namespace_id"]) != namespace_id
        or row["target_session"] != target_session
        or row["source_symbol"] != source_symbol
        or str(row["package_sha256"]).strip() != package_sha256
        or str(row["source_capture_id"]) != source_capture_id
        or row["source_content_id"].strip() != source_content_id
        or str(row["profile_revision"]).strip() != profile_revision
        or row["source_time_quality"] != source_time_quality
        or row["source_clock_role"] != source_clock_role
        or _timestamp_identity(row["source_available_at"])
        != _timestamp_identity(source_available_at)
    ):
        return _unavailable(
            status="INCOMPLETE",
            reason_code="SEED_SOURCE_CONTEXT_MISMATCH",
            message="Seed事实与当前Raw捕获、内容摘要或来源时钟不一致。",
            identity=identity,
            partition=stored["partition"],
        )

    predicate_items = [
        {
            "predicate_id": predicate_id,
            "state": state,
            "reason_codes": [reason for reason in reason_text.split(";") if reason],
        }
        for predicate_id, state, reason_text in zip(
            row["predicate_ids"],
            row["predicate_states"],
            row["predicate_reasons"],
            strict=True,
        )
    ]
    quality_fields = {
        field_id: {
            "quality": row[f"{column_prefix}_quality"],
            "reason": row[f"{column_prefix}_reason"],
        }
        for field_id, column_prefix in _SEED_QUALITY_FIELDS
    }
    return {
        "status": "SAVED",
        "identity": identity,
        "partition": stored["partition"],
        "research_mode": row["research_mode"],
        "price_coordinate": row["price_coordinate"],
        "history_window": {
            "start": row["history_window_start"],
            "end": row["history_window_end"],
            "session_count": row["history_session_count"],
            "calendar_id": row["calendar_id"],
            "calendar_revision": row["calendar_revision"],
        },
        "source_seed_state": row["source_seed_state"],
        "qualification": {
            "safety": {
                "state": row["safety_state"],
                "reason_codes": _reasons(row["safety_reasons"]),
            },
            "s1": {
                "state": row["s1_state"],
                "reason_codes": _reasons(row["s1_reasons"]),
            },
            "s2": {
                "state": row["s2_state"],
                "reason_codes": _reasons(row["s2_reasons"]),
            },
            "source_seed": {
                "state": row["source_seed_state"],
                "reason_codes": _reasons(row["source_seed_reasons"]),
            },
        },
        "predicates": predicate_items,
        "quality": {
            "source_time_quality": row["source_time_quality"],
            "source_clock_role": row["source_clock_role"],
            "source_available_at": row["source_available_at"],
            "target_actual_quote": row["target_actual_quote"],
            "target_close_quality": row["target_close_quality"],
            "target_close_reason": row["target_close_reason"],
            "fields": quality_fields,
        },
        "formal_candidate_status": "NOT_EVALUATED",
        "database_write": False,
        "ddl_executed": False,
    }
