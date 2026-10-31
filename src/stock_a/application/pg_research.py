"""将单股PG Raw研究读取绑定到精确封口的P03来源集合。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres.source_set_ingress import read_p04_source_input


class PgResearchBindingError(Exception):
    """P04封口来源与用户请求的PG Raw未能精确闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _is_target_session(value: Any, target_session: date) -> bool:
    if isinstance(value, date):
        return value == target_session
    return value == target_session.isoformat()


def read_sealed_source_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    namespace_id: str | None,
    target_raw_rows: Sequence[Mapping[str, Any]] = (),
    expected_raw_row_count: int | None = None,
) -> dict[str, Any]:
    """只读读取P04精确SEALED并核对目标日Raw捕获与内容身份。"""
    try:
        source_set = read_p04_source_input(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
        )
    except Exception as error:
        raise PgResearchBindingError(
            getattr(error, "code", "PG_RESEARCH_P04_READ_FAILED"),
            "P04精确SEALED source_set只读读取失败。",
        ) from error
    if source_set.get("status") != "AVAILABLE":
        raise PgResearchBindingError(
            "PG_RESEARCH_P04_SOURCE_SET_UNAVAILABLE",
            "该目标日与来源包SHA没有可用的精确P04封口source_set；拒绝使用候选或其他版本回退。",
        )
    if (
        source_set.get("target_session") != target_session.isoformat()
        or source_set.get("package_sha256") != package_sha256
        or (namespace_id is not None and source_set.get("namespace_id") != namespace_id)
        or source_set.get("seal_status") != "SEALED"
        or source_set.get("durability_status") != "VERIFIED_POST_COMMIT_READBACK"
        or source_set.get("candidate_fallback") is not False
        or source_set.get("database_write") is not False
        or source_set.get("ddl_executed") is not False
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_P04_SOURCE_SET_INVALID",
            "P04读回未满足精确日期、包SHA、namespace、SEALED及提交后读回合同。",
        )

    if expected_raw_row_count is not None and (
        type(expected_raw_row_count) is not int
        or expected_raw_row_count != source_set.get("raw_row_count")
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_P04_RAW_COUNT_MISMATCH",
            "PG Raw目标日行数与P04封口分母不一致。",
        )

    target_rows = [
        row
        for row in target_raw_rows
        if _is_target_session(row.get("session_date"), target_session)
    ]
    if target_raw_rows and not target_rows:
        raise PgResearchBindingError(
            "PG_RESEARCH_TARGET_RAW_AMBIGUOUS",
            "本次PG读取未返回精确目标日Raw行。",
        )
    if target_rows:
        for target_row in target_rows:
            if (
                target_row.get("package_sha256") != package_sha256
                or target_row.get("namespace_id") != source_set.get("namespace_id")
                or target_row.get("source_content_id")
                != source_set.get("source_content_id")
                or target_row.get("source_capture_id")
                != source_set.get("source_capture_id")
            ):
                raise PgResearchBindingError(
                    "PG_RESEARCH_P04_RAW_REFERENCE_MISMATCH",
                    "目标日Raw行与P04 source_content/capture绑定不一致。",
                )

    p02 = source_set.get("p02_capability")
    bao_status = p02.get("status", "UNKNOWN") if isinstance(p02, Mapping) else "UNKNOWN"
    raw_revalidation = source_set.get("raw_revalidation")
    proof_level = (
        raw_revalidation.get("proof_level", "UNKNOWN")
        if isinstance(raw_revalidation, Mapping)
        else "UNKNOWN"
    )
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "namespace_id": source_set["namespace_id"],
        "source_set_id": source_set["source_set_id"],
        "revision_id": source_set["revision_id"],
        "seal_status": source_set["seal_status"],
        "durability_status": source_set["durability_status"],
        "source_time_quality": source_set.get("source_time_quality", "UNKNOWN"),
        "source_available_at": source_set.get("source_available_at"),
        "price_unit_status": source_set.get("price_unit_status", "UNKNOWN"),
        "amount_unit_status": source_set.get("amount_unit_status", "UNIT_UNVERIFIED"),
        "volume_unit_status": source_set.get("volume_unit_status", "UNIT_UNVERIFIED"),
        "source_content_id": source_set["source_content_id"],
        "source_capture_id": source_set["source_capture_id"],
        "source_digest": source_set["source_digest"],
        "source_raw_digest": source_set["source_raw_digest"],
        "raw_row_count": source_set["raw_row_count"],
        "capture_expected_row_count": source_set["capture_expected_row_count"],
        "raw_revalidation_proof_level": proof_level,
        "target_raw_binding_count": len(target_rows),
        "target_raw_binding_verified": bool(target_rows),
        "stable_security_id_status": "STABLE_SECURITY_ID_UNVERIFIED",
        "listing_status": "LISTING_STATUS_UNKNOWN",
        "suspension_status": "SUSPENSION_STATUS_UNKNOWN",
        "official_listing_universe_status": "UNKNOWN",
        "pit_status": "NON_PIT_RETROSPECTIVE",
        "price_coordinate": "RAW_UNADJUSTED",
        "baostock_status": bao_status,
        "database_write": False,
        "ddl_executed": False,
        "candidate_fallback": False,
    }
