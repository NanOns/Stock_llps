"""将明确来源版本直接绑定到PostgreSQL Raw研究行。"""

from __future__ import annotations

from datetime import date
from typing import Any, Mapping, Sequence

from stock_a.storage.postgres.source_raw import (
    RawSourcePgError,
    read_source_version_context as _read_source_version_context,
)


class PgResearchBindingError(Exception):
    """来源版本与用户请求的PG Raw事实无法精确闭合。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _is_target_session(value: Any, target_session: date) -> bool:
    if isinstance(value, date):
        return value == target_session
    return value == target_session.isoformat()


def bind_source_context_to_raw_rows(
    source_context: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    namespace_id: str,
    target_raw_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """核对所选来源版本与目标日Raw行的日期、包、内容和捕获引用。"""
    if (
        source_context.get("status") != "AVAILABLE"
        or source_context.get("target_session") != target_session.isoformat()
        or source_context.get("package_sha256") != package_sha256
        or source_context.get("namespace_id") != namespace_id
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_SOURCE_VERSION_MISMATCH",
            "来源版本上下文与请求日期、包SHA或namespace不一致。",
        )
    target_rows = [
        row
        for row in target_raw_rows
        if _is_target_session(row.get("session_date"), target_session)
    ]
    if len(target_rows) != 1:
        raise PgResearchBindingError(
            "PG_RESEARCH_TARGET_RAW_AMBIGUOUS",
            "单股Raw查询必须返回恰好一条精确目标日行。",
        )
    target_row = target_rows[0]
    if (
        target_row.get("package_sha256") != package_sha256
        or target_row.get("namespace_id") != namespace_id
        or target_row.get("source_content_id")
        != source_context.get("source_content_id")
        or target_row.get("source_capture_id")
        != source_context.get("source_capture_id")
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_RAW_REFERENCE_MISMATCH",
            "目标日Raw行与来源包内容或捕获引用不一致。",
        )
    bound_context = dict(source_context)
    bound_context["target_raw_binding_count"] = 1
    bound_context["target_raw_binding_verified"] = True
    return bound_context


def read_source_version_context(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    namespace_id: str | None,
    target_raw_rows: Sequence[Mapping[str, Any]] = (),
    expected_raw_row_count: int | None = None,
) -> dict[str, Any]:
    """从Raw、捕获与内容事实取上下文；旧profile键只作为行筛选值回传。"""
    try:
        source = _read_source_version_context(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
            expected_namespace_id=namespace_id,
        )
    except RawSourcePgError as error:
        raise PgResearchBindingError(error.code, str(error)) from error
    if source.get("status") != "AVAILABLE":
        raise PgResearchBindingError(
            "PG_RESEARCH_SOURCE_VERSION_UNAVAILABLE",
            "该日期与来源包SHA没有可用的Raw来源版本。",
        )
    if expected_raw_row_count is not None and (
        type(expected_raw_row_count) is not int
        or expected_raw_row_count != source.get("raw_row_count")
    ):
        raise PgResearchBindingError(
            "PG_RESEARCH_RAW_COUNT_MISMATCH",
            "目标日Raw行数与同一捕获记录的来源代码数不一致。",
        )
    if target_raw_rows:
        target_rows = [
            row
            for row in target_raw_rows
            if _is_target_session(row.get("session_date"), target_session)
        ]
        if not target_rows:
            raise PgResearchBindingError(
                "PG_RESEARCH_TARGET_RAW_MISSING", "读取中没有精确目标日Raw行。"
            )
        for row in target_rows:
            if (
                row.get("package_sha256") != package_sha256
                or str(row.get("namespace_id")) != source.get("namespace_id")
                or row.get("source_content_id") != source.get("source_content_id")
                or str(row.get("source_capture_id")) != source.get("source_capture_id")
            ):
                raise PgResearchBindingError(
                    "PG_RESEARCH_RAW_REFERENCE_MISMATCH",
                    "目标日Raw行跨包、跨namespace、跨内容或跨捕获。",
                )
    source["source_raw_digest"] = source.get("capture_digest")
    source["source_digest"] = source.get("capture_digest")
    source["capture_read_started_at"] = source.get("capture_started_at")
    source["target_raw_binding_count"] = len(target_raw_rows)
    source["target_raw_binding_verified"] = bool(target_raw_rows)
    source["database_write"] = False
    source["ddl_executed"] = False
    source["candidate_fallback"] = False
    return source
