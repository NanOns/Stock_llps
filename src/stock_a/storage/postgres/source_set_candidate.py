"""Stock A P03来源集合候选的固定PG读写。"""

from __future__ import annotations

from datetime import date, datetime
import re
from typing import Any, Mapping
import uuid

from stock_a.domain.source_set_candidate import (
    CANDIDATE_STATUS,
    FIELD_FAMILIES,
    FORMAL_P03_STATUS,
    IDENTITY_STATUS,
    INPUT_ROLES,
    PIT_STATUS,
    SourceSetCandidateError,
    source_set_candidate_digest,
)

from . import source_raw


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PERSIST_TASK_SCOPE = "R01_R02_P03_SOURCE_SET_BROWSER_RULE_R1"
_PERSIST_TARGET_SESSIONS = frozenset({date(2026, 10, 28), date(2026, 10, 29)})
_QUALITY_COUNT_FIELDS = (
    "raw_observed_count",
    "raw_unknown_count",
    "raw_not_applicable_count",
    "raw_bad_value_count",
    "normalized_observed_count",
    "normalized_unknown_count",
    "normalized_not_applicable_count",
    "normalized_bad_value_count",
)
_CANDIDATE_COLUMNS = (
    "namespace_id",
    "target_session",
    "package_sha256",
    "source_content_id",
    "source_capture_id",
    "source_byte_sha256",
    "source_dataset_id",
    "source_byte_count",
    "parser_contract",
    "capture_expected_row_count",
    "raw_row_count",
    "target_present_count",
    "target_missing_count",
    "read_failed_count",
    "raw_source_digest",
    "capture_canonical_digest",
    "capture_read_started_at",
    "capture_validated_at",
    "source_available_at",
    "source_time_quality",
    "source_clock_role",
    "calendar_id",
    "calendar_revision",
    "calendar_sha256",
    "candidate_sha256",
    "candidate_status",
    "formal_p03_status",
    "pit_status",
    "identity_status",
    "created_at",
)
_INPUT_COLUMNS = (
    "input_ordinal",
    "input_role",
    "field_family",
    "input_status",
    "source_content_id",
    "source_capture_id",
    "evidence_sha256",
    "observed_row_count",
    "expected_row_count",
    *_QUALITY_COUNT_FIELDS,
    "unit_status",
    "source_available_at",
    "source_time_quality",
    "source_clock_role",
    "reason_code",
)


class SourceSetCandidatePgError(RuntimeError):
    """候选来源读取、写入、幂等读回或身份核验失败。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _date_and_sha(target_session: date, package_sha256: str) -> None:
    if type(target_session) is not date:
        raise SourceSetCandidatePgError(
            "TARGET_SESSION_INVALID", "必须显式提供目标交易会话日期"
        )
    if not isinstance(package_sha256, str) or _SHA256.fullmatch(package_sha256) is None:
        raise SourceSetCandidatePgError(
            "PACKAGE_SHA_INVALID", "来源包SHA必须是64位小写十六进制值"
        )


def _quality_counts(summary: Mapping[str, Any], family: str) -> dict[str, int | None]:
    if family == "PRICE":
        prefix = "price"
        raw_values = (None, None, None, None)
        unit_statuses = ["RAW_UNADJUSTED"]
    elif family == "AMOUNT":
        prefix = "amount"
        raw_values = (
            summary["raw_amount_observed"],
            summary["raw_amount_unknown"],
            summary["raw_amount_not_applicable"],
            summary["raw_amount_bad_value"],
        )
        unit_statuses = summary["amount_unit_statuses"] or []
    else:
        prefix = "volume"
        raw_values = (
            summary["raw_volume_observed"],
            summary["raw_volume_unknown"],
            summary["raw_volume_not_applicable"],
            summary["raw_volume_bad_value"],
        )
        unit_statuses = summary["volume_unit_statuses"] or []
    normalized = (
        summary[f"{prefix}_observed"],
        summary[f"{prefix}_unknown"],
        summary[f"{prefix}_not_applicable"],
        summary[f"{prefix}_bad_value"],
    )
    if len(unit_statuses) != 1:
        raise SourceSetCandidatePgError(
            "FIELD_UNIT_STATUS_CONFLICT", f"{family}字段单位状态不唯一"
        )
    return {
        "raw_observed_count": raw_values[0],
        "raw_unknown_count": raw_values[1],
        "raw_not_applicable_count": raw_values[2],
        "raw_bad_value_count": raw_values[3],
        "normalized_observed_count": normalized[0],
        "normalized_unknown_count": normalized[1],
        "normalized_not_applicable_count": normalized[2],
        "normalized_bad_value_count": normalized[3],
        "unit_status": unit_statuses[0],
    }


def _quality_summary(
    cursor: Any, namespace_id: str, target_session: date, package_sha256: str
) -> dict[str, Any]:
    cursor.execute(
        """SELECT count(*)::integer AS raw_row_count,
       count(*) FILTER (WHERE r.bar_status='TARGET_PRESENT')::integer AS target_present_count,
       count(*) FILTER (WHERE r.bar_status='TARGET_MISSING')::integer AS target_missing_count,
       count(*) FILTER (WHERE r.bar_status='READ_FAILED')::integer AS read_failed_count,
       count(*) FILTER (WHERE r.bar_status NOT IN ('TARGET_PRESENT','TARGET_MISSING','READ_FAILED'))::integer AS unknown_bar_status_count,
       count(DISTINCT r.source_capture_id)::integer AS capture_count,
       count(DISTINCT r.source_content_id)::integer AS content_count,
       array_agg(DISTINCT r.source_capture_id::text ORDER BY r.source_capture_id::text) AS capture_ids,
       array_agg(DISTINCT r.source_content_id ORDER BY r.source_content_id) AS content_ids,
       min(c.normalized_count)::integer AS expected_row_count,
       max(c.normalized_count)::integer AS expected_row_count_max,
       min(c.read_started_at) AS capture_read_started_at,
       max(c.read_started_at) AS capture_read_started_at_max,
       min(c.validated_at) AS capture_validated_at,
       max(c.validated_at) AS capture_validated_at_max,
       array_agg(DISTINCT c.source_time_quality ORDER BY c.source_time_quality) AS source_time_qualities,
       array_agg(DISTINCT c.source_clock_role ORDER BY c.source_clock_role) AS source_clock_roles,
       count(DISTINCT c.source_available_at)::integer AS source_available_at_count,
       count(*) FILTER (WHERE c.capture_id IS NULL OR s.content_id IS NULL
           OR c.content_id IS DISTINCT FROM r.source_content_id)::integer AS broken_reference_count,
       array_agg(DISTINCT s.dataset_id ORDER BY s.dataset_id) AS dataset_ids,
       array_agg(DISTINCT s.byte_sha ORDER BY s.byte_sha) AS content_shas,
       array_agg(DISTINCT r.price_quality ORDER BY r.price_quality) AS price_qualities,
       count(*) FILTER (WHERE r.price_quality='OBSERVED')::integer AS price_observed,
       count(*) FILTER (WHERE r.price_quality='UNKNOWN')::integer AS price_unknown,
       count(*) FILTER (WHERE r.price_quality='NOT_APPLICABLE')::integer AS price_not_applicable,
       count(*) FILTER (WHERE r.price_quality='BAD_VALUE')::integer AS price_bad_value,
       count(*) FILTER (WHERE r.raw_amount_quality='RAW_SOURCE_VALUE_OBSERVED')::integer AS raw_amount_observed,
       count(*) FILTER (WHERE r.raw_amount_quality='UNKNOWN')::integer AS raw_amount_unknown,
       count(*) FILTER (WHERE r.raw_amount_quality='NOT_APPLICABLE')::integer AS raw_amount_not_applicable,
       count(*) FILTER (WHERE r.raw_amount_quality='BAD_VALUE')::integer AS raw_amount_bad_value,
       count(*) FILTER (WHERE r.amount_quality='OBSERVED')::integer AS amount_observed,
       count(*) FILTER (WHERE r.amount_quality='UNKNOWN')::integer AS amount_unknown,
       count(*) FILTER (WHERE r.amount_quality='NOT_APPLICABLE')::integer AS amount_not_applicable,
       count(*) FILTER (WHERE r.amount_quality='BAD_VALUE')::integer AS amount_bad_value,
       array_agg(DISTINCT r.amount_unit_status ORDER BY r.amount_unit_status) AS amount_unit_statuses,
       count(*) FILTER (WHERE r.raw_volume_quality='RAW_SOURCE_VALUE_OBSERVED')::integer AS raw_volume_observed,
       count(*) FILTER (WHERE r.raw_volume_quality='UNKNOWN')::integer AS raw_volume_unknown,
       count(*) FILTER (WHERE r.raw_volume_quality='NOT_APPLICABLE')::integer AS raw_volume_not_applicable,
       count(*) FILTER (WHERE r.raw_volume_quality='BAD_VALUE')::integer AS raw_volume_bad_value,
       count(*) FILTER (WHERE r.volume_quality='OBSERVED')::integer AS volume_observed,
       count(*) FILTER (WHERE r.volume_quality='UNKNOWN')::integer AS volume_unknown,
       count(*) FILTER (WHERE r.volume_quality='NOT_APPLICABLE')::integer AS volume_not_applicable,
       count(*) FILTER (WHERE r.volume_quality='BAD_VALUE')::integer AS volume_bad_value,
       array_agg(DISTINCT r.volume_unit_status ORDER BY r.volume_unit_status) AS volume_unit_statuses
 FROM stock.source_raw_daily r
 LEFT JOIN stock.source_captures c
   ON c.namespace_id=r.namespace_id AND c.capture_id=r.source_capture_id
 LEFT JOIN stock.source_contents s
   ON s.namespace_id=r.namespace_id AND s.content_id=r.source_content_id
 WHERE r.namespace_id=%s::uuid AND r.session_date=%s AND r.package_sha256=%s""",
        (namespace_id, target_session, package_sha256),
    )
    row = cursor.fetchone()
    return dict(zip((item.name for item in cursor.description), row, strict=True))


def _input_row(ordinal: int, role: str, **values: Any) -> dict[str, Any]:
    row = {name: None for name in _INPUT_COLUMNS}
    row.update(
        {
            "input_ordinal": ordinal,
            "input_role": role,
            "field_family": None,
            "input_status": "VERIFIED",
        }
    )
    row.update(values)
    return row


def _read_candidate_facts(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    calendar: Mapping[str, str],
    write: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with source_raw._transaction(config, write=write) as (
        _connection,
        cursor,
        namespace_id,
    ):
        summary = _quality_summary(cursor, namespace_id, target_session, package_sha256)
        raw_count = int(summary["raw_row_count"] or 0)
        if raw_count == 0:
            raise SourceSetCandidatePgError(
                "SOURCE_RAW_NO_MATCH", "PG中没有该明确DEV日期与来源包SHA的Raw记录"
            )
        capture_ids = summary["capture_ids"] or []
        content_ids = summary["content_ids"] or []
        expected = summary["expected_row_count"]
        if (
            summary["unknown_bar_status_count"] != 0
            or summary["read_failed_count"] != 0
            or summary["capture_count"] != 1
            or summary["content_count"] != 1
            or len(capture_ids) != 1
            or len(content_ids) != 1
            or expected is None
            or expected != summary["expected_row_count_max"]
            or raw_count != expected
            or summary["broken_reference_count"] != 0
            or summary["capture_read_started_at"]
            != summary["capture_read_started_at_max"]
            or summary["capture_validated_at"] != summary["capture_validated_at_max"]
            or len(summary["source_time_qualities"] or []) != 1
            or len(summary["source_clock_roles"] or []) != 1
            or (summary["dataset_ids"] or []) != ["TDX_VIPDATA_PACKAGE"]
            or (summary["content_shas"] or []) != [package_sha256]
        ):
            raise SourceSetCandidatePgError(
                "SOURCE_INPUT_NOT_CLOSED", "Raw分母、状态分区、时间或来源引用未闭合"
            )

        capture_id = capture_ids[0]
        content_id = content_ids[0]
        cursor.execute(
            """SELECT namespace_id::text,content_id,dataset_id,byte_sha,byte_count
 FROM stock.source_contents WHERE namespace_id=%s::uuid AND content_id=%s""",
            (namespace_id, content_id),
        )
        content_rows = cursor.fetchall()
        if len(content_rows) != 1:
            raise SourceSetCandidatePgError(
                "SOURCE_CONTENT_NO_MATCH", "Raw引用的来源内容记录不唯一"
            )
        content_namespace, actual_content_id, dataset_id, byte_sha, byte_count = (
            content_rows[0]
        )
        capture = source_raw._read_capture(cursor, namespace_id, capture_id)
        if capture is None:
            raise SourceSetCandidatePgError(
                "SOURCE_CAPTURE_NO_MATCH", "Raw引用的来源捕获记录不存在"
            )
        if (
            content_namespace != namespace_id
            or actual_content_id != capture.content_id
            or actual_content_id != content_id
            or dataset_id != "TDX_VIPDATA_PACKAGE"
            or byte_sha != package_sha256
            or capture.parser_contract != source_raw._PARSER_CONTRACT
            or capture.normalized_count != raw_count
        ):
            raise SourceSetCandidatePgError(
                "SOURCE_IDENTITY_CONFLICT",
                "内容SHA、捕获合同、规范化分母或Raw引用不一致",
            )

        raw_rows = source_raw._read_raw_rows(
            cursor, namespace_id, target_session, package_sha256
        )
        raw_digest = source_raw._row_digest(raw_rows)
        if len(raw_rows) != raw_count or raw_digest != capture.canonical_digest:
            raise SourceSetCandidatePgError(
                "RAW_CAPTURE_DIGEST_MISMATCH", "Raw完整行集摘要与真实捕获摘要不一致"
            )
        if summary["source_time_qualities"] != [capture.source_time_quality] or summary[
            "source_clock_roles"
        ] != [capture.source_clock_role]:
            raise SourceSetCandidatePgError(
                "SOURCE_CLOCK_CONFLICT", "捕获来源时钟汇总与真实捕获不一致"
            )

        header: dict[str, Any] = {
            "namespace_id": namespace_id,
            "target_session": target_session,
            "package_sha256": package_sha256,
            "source_content_id": content_id,
            "source_capture_id": capture_id,
            "source_byte_sha256": byte_sha,
            "source_dataset_id": dataset_id,
            "source_byte_count": int(byte_count),
            "parser_contract": capture.parser_contract,
            "capture_expected_row_count": int(capture.normalized_count),
            "raw_row_count": raw_count,
            "target_present_count": int(summary["target_present_count"]),
            "target_missing_count": int(summary["target_missing_count"]),
            "read_failed_count": int(summary["read_failed_count"]),
            "raw_source_digest": raw_digest,
            "capture_canonical_digest": capture.canonical_digest,
            "capture_read_started_at": capture.read_started_at,
            "capture_validated_at": capture.validated_at,
            "source_available_at": capture.source_available_at,
            "source_time_quality": capture.source_time_quality,
            "source_clock_role": capture.source_clock_role,
            "calendar_id": calendar["calendar_id"],
            "calendar_revision": calendar["calendar_revision"],
            "calendar_sha256": calendar["calendar_sha256"],
            "candidate_status": CANDIDATE_STATUS,
            "formal_p03_status": FORMAL_P03_STATUS,
            "pit_status": PIT_STATUS,
            "identity_status": IDENTITY_STATUS,
        }
        expected_count = int(capture.normalized_count)
        inputs = [
            _input_row(
                1,
                "TDX_CONTENT",
                source_content_id=content_id,
                evidence_sha256=package_sha256,
            ),
            _input_row(
                2,
                "TDX_CAPTURE",
                source_content_id=content_id,
                source_capture_id=capture_id,
                evidence_sha256=capture.canonical_digest,
                source_available_at=capture.source_available_at,
                source_time_quality=capture.source_time_quality,
                source_clock_role=capture.source_clock_role,
            ),
            _input_row(
                3,
                "TDX_RAW_DAILY",
                source_content_id=content_id,
                source_capture_id=capture_id,
                evidence_sha256=raw_digest,
                observed_row_count=raw_count,
                expected_row_count=expected_count,
            ),
        ]
        for ordinal, family in enumerate(FIELD_FAMILIES, start=4):
            inputs.append(
                _input_row(
                    ordinal,
                    "FIELD_QUALITY",
                    field_family=family,
                    observed_row_count=raw_count,
                    expected_row_count=expected_count,
                    **_quality_counts(summary, family),
                )
            )
        inputs.append(
            _input_row(
                7,
                "BAOSTOCK_CAPABILITY",
                input_status="NOT_REQUESTED",
                reason_code="OPTIONAL_SOURCE_NOT_REQUESTED",
            )
        )

    try:
        header["candidate_sha256"] = source_set_candidate_digest(header, inputs)
    except SourceSetCandidateError as error:
        raise SourceSetCandidatePgError(
            "CANDIDATE_QUALITY_REJECTED", str(error)
        ) from error
    return header, inputs


def _verify_source_unchanged(
    cursor: Any, namespace_id: str, header: Mapping[str, Any]
) -> None:
    """在写入候选的同一RR/serializable事务中复核真实输入。"""
    target_session = header["target_session"]
    package_sha256 = header["package_sha256"]
    if namespace_id != header["namespace_id"]:
        raise SourceSetCandidatePgError(
            "NAMESPACE_CHANGED", "PG部署的DEV命名空间已变化"
        )
    summary = _quality_summary(cursor, namespace_id, target_session, package_sha256)
    if (
        summary["raw_row_count"] != header["raw_row_count"]
        or summary["target_present_count"] != header["target_present_count"]
        or summary["target_missing_count"] != header["target_missing_count"]
        or summary["read_failed_count"] != 0
        or summary["unknown_bar_status_count"] != 0
        or summary["capture_count"] != 1
        or summary["content_count"] != 1
        or summary["expected_row_count"] != header["capture_expected_row_count"]
        or summary["expected_row_count_max"] != header["capture_expected_row_count"]
        or summary["broken_reference_count"] != 0
        or summary["capture_ids"] != [header["source_capture_id"]]
        or summary["content_ids"] != [header["source_content_id"]]
    ):
        raise SourceSetCandidatePgError(
            "SOURCE_CHANGED", "候选写入前Raw分母或来源引用发生变化"
        )

    capture = source_raw._read_capture(
        cursor, namespace_id, str(header["source_capture_id"])
    )
    cursor.execute(
        """SELECT dataset_id,byte_sha,byte_count FROM stock.source_contents
 WHERE namespace_id=%s::uuid AND content_id=%s""",
        (namespace_id, header["source_content_id"]),
    )
    content = cursor.fetchone()
    raw_rows = source_raw._read_raw_rows(
        cursor, namespace_id, target_session, package_sha256
    )
    if (
        capture is None
        or content
        != (
            header["source_dataset_id"],
            header["source_byte_sha256"],
            header["source_byte_count"],
        )
        or capture.content_id != header["source_content_id"]
        or capture.parser_contract != header["parser_contract"]
        or capture.normalized_count != header["capture_expected_row_count"]
        or capture.canonical_digest != header["capture_canonical_digest"]
        or capture.read_started_at != header["capture_read_started_at"]
        or capture.validated_at != header["capture_validated_at"]
        or capture.source_available_at != header["source_available_at"]
        or capture.source_time_quality != header["source_time_quality"]
        or capture.source_clock_role != header["source_clock_role"]
        or len(raw_rows) != header["raw_row_count"]
        or source_raw._row_digest(raw_rows) != header["raw_source_digest"]
    ):
        raise SourceSetCandidatePgError(
            "SOURCE_CHANGED", "内容、捕获时钟或Raw行集在候选提交前发生变化"
        )


def _insert_candidate(
    cursor: Any, header: Mapping[str, Any], inputs: list[Mapping[str, Any]]
) -> dict[str, Any]:
    cursor.execute(
        """SELECT target_session, package_sha256 FROM stock.source_set_candidates
 WHERE namespace_id=%s::uuid AND target_session=ANY(%s::date[])
 ORDER BY target_session, package_sha256""",
        (header["namespace_id"], sorted(_PERSIST_TARGET_SESSIONS)),
    )
    bounded_candidates = cursor.fetchall()
    if len(bounded_candidates) > 2 or any(
        session == header["target_session"] and package_sha != header["package_sha256"]
        for session, package_sha in bounded_candidates
    ):
        raise SourceSetCandidatePgError(
            "TASK_CANDIDATE_BOUND_EXCEEDED",
            "本批每个获准交易日最多允许一个精确包SHA候选，合计最多两条主行",
        )
    current_exists = any(
        session == header["target_session"] and package_sha == header["package_sha256"]
        for session, package_sha in bounded_candidates
    )
    if not current_exists and len(bounded_candidates) >= 2:
        raise SourceSetCandidatePgError(
            "TASK_CANDIDATE_BOUND_EXCEEDED",
            "本批候选主行已达到两个真实交易日的授权上限",
        )
    columns = [name for name in _CANDIDATE_COLUMNS if name != "created_at"]
    cursor.execute(
        f"INSERT INTO stock.source_set_candidates ({', '.join(columns)}) "
        f"VALUES ({', '.join(['%s'] * len(columns))}) "
        "ON CONFLICT (namespace_id, target_session, package_sha256) DO NOTHING",
        tuple(header.get(name) for name in columns),
    )
    cursor.execute(
        f"SELECT {', '.join(_CANDIDATE_COLUMNS)} FROM stock.source_set_candidates "
        "WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s",
        (header["namespace_id"], header["target_session"], header["package_sha256"]),
    )
    current = cursor.fetchone()
    if current is None:
        raise SourceSetCandidatePgError(
            "CANDIDATE_READBACK_MISSING", "候选主行插入后未读回"
        )
    current_header = dict(zip(_CANDIDATE_COLUMNS, current, strict=True))
    if current_header["candidate_sha256"] != header["candidate_sha256"]:
        raise SourceSetCandidatePgError(
            "CANDIDATE_IDENTITY_CONFLICT",
            "同一日期和来源SHA已有不同捕获或候选摘要；未覆盖现有值",
        )

    candidate_key = (
        header["namespace_id"],
        header["target_session"],
        header["package_sha256"],
    )
    for item in inputs:
        values = {name: item.get(name) for name in _INPUT_COLUMNS}
        cursor.execute(
            f"INSERT INTO stock.source_set_candidate_inputs "
            f"(namespace_id,target_session,package_sha256,{', '.join(_INPUT_COLUMNS)}) "
            f"VALUES (%s,%s,%s,{', '.join(['%s'] * len(_INPUT_COLUMNS))}) "
            "ON CONFLICT (namespace_id, target_session, package_sha256, input_ordinal) DO NOTHING",
            (*candidate_key, *(values[name] for name in _INPUT_COLUMNS)),
        )
    cursor.execute(
        f"SELECT {', '.join(_INPUT_COLUMNS)} FROM stock.source_set_candidate_inputs "
        "WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s "
        "ORDER BY input_ordinal",
        candidate_key,
    )
    stored_inputs = [
        dict(zip(_INPUT_COLUMNS, row, strict=True)) for row in cursor.fetchall()
    ]
    try:
        stored_digest = source_set_candidate_digest(current_header, stored_inputs)
    except SourceSetCandidateError as error:
        raise SourceSetCandidatePgError(
            "CANDIDATE_INPUT_CONFLICT", str(error)
        ) from error
    if len(stored_inputs) != len(inputs) or stored_digest != header["candidate_sha256"]:
        raise SourceSetCandidatePgError(
            "CANDIDATE_INPUT_CONFLICT",
            "候选输入读回与真实来源或字段质量摘要不一致；未覆盖现有值",
        )
    return _public_candidate(current_header, stored_inputs)


def _public_candidate(
    header: Mapping[str, Any], inputs: list[Mapping[str, Any]]
) -> dict[str, Any]:
    def public_value(value: Any) -> Any:
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        return value

    return {
        **{key: public_value(value) for key, value in header.items()},
        "inputs": [
            {key: public_value(value) for key, value in row.items()} for row in inputs
        ],
        "input_count": len(inputs),
        "database_write": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_recalculated": False,
    }


def prepare_source_set_candidate(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    calendar: Mapping[str, str],
    persist: bool = False,
    task_scope: str | None = None,
) -> dict[str, Any]:
    """核验精确PG来源并可选地幂等写入一个P03候选和七条输入引用。"""
    _date_and_sha(target_session, package_sha256)
    if not isinstance(calendar, Mapping) or any(
        not isinstance(calendar.get(key), str) or not calendar[key]
        for key in ("calendar_id", "calendar_revision", "calendar_sha256")
    ):
        raise SourceSetCandidatePgError(
            "CALENDAR_EVIDENCE_INVALID", "缺少版本化官方日历证据"
        )
    if persist and task_scope != _PERSIST_TASK_SCOPE:
        raise SourceSetCandidatePgError(
            "TASK_SCOPE_REJECTED", "候选写入必须绑定本批唯一授权任务范围"
        )
    if persist and target_session not in _PERSIST_TARGET_SESSIONS:
        raise SourceSetCandidatePgError(
            "TASK_SESSION_REJECTED",
            "本批持久化仅允许2026-10-28与2026-10-29两个真实交易日",
        )
    if not persist and task_scope is not None:
        raise SourceSetCandidatePgError(
            "TASK_SCOPE_REJECTED", "只读预检不得附带写入任务范围"
        )

    header, inputs = _read_candidate_facts(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        calendar=calendar,
        write=False,
    )
    if not persist:
        return {
            "status": "READY_TO_PERSIST",
            "candidate": _public_candidate(header, inputs),
            "database_write_attempted": False,
            "dml_executed": False,
        }

    with source_raw._transaction(config, write=True) as (
        _connection,
        cursor,
        namespace_id,
    ):
        _verify_source_unchanged(cursor, namespace_id, header)
        candidate = _insert_candidate(cursor, header, inputs)
    return {
        "status": "PERSISTED",
        "candidate": candidate,
        "database_write_attempted": True,
        "dml_executed": True,
        "idempotent": True,
    }


def read_source_set_candidate(
    config: Mapping[str, Any], *, target_session: date, package_sha256: str
) -> dict[str, Any]:
    """只读取得明确日期与SHA的唯一候选；缺失时明确NO_MATCH，无latest回退。"""
    _date_and_sha(target_session, package_sha256)
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        namespace_id,
    ):
        cursor.execute(
            f"SELECT {', '.join(_CANDIDATE_COLUMNS)} FROM stock.source_set_candidates "
            "WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s",
            (namespace_id, target_session, package_sha256),
        )
        row = cursor.fetchone()
        if row is None:
            return {
                "status": "NO_MATCH",
                "target_session": target_session.isoformat(),
                "package_sha256": package_sha256,
                "candidate": None,
                "database_write": False,
                "ddl_executed": False,
            }
        header = dict(zip(_CANDIDATE_COLUMNS, row, strict=True))
        cursor.execute(
            f"SELECT {', '.join(_INPUT_COLUMNS)} FROM stock.source_set_candidate_inputs "
            "WHERE namespace_id=%s::uuid AND target_session=%s AND package_sha256=%s "
            "ORDER BY input_ordinal",
            (namespace_id, target_session, package_sha256),
        )
        inputs = [
            dict(zip(_INPUT_COLUMNS, input_row, strict=True))
            for input_row in cursor.fetchall()
        ]
    try:
        actual_digest = source_set_candidate_digest(header, inputs)
    except SourceSetCandidateError as error:
        raise SourceSetCandidatePgError(
            "CANDIDATE_READBACK_INVALID", str(error)
        ) from error
    if len(inputs) != len(INPUT_ROLES) or actual_digest != header["candidate_sha256"]:
        raise SourceSetCandidatePgError(
            "CANDIDATE_READBACK_INVALID", "PG候选输入数量或稳定摘要读回不一致"
        )
    return {
        "status": "AVAILABLE",
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "candidate": _public_candidate(header, inputs),
        "database_write": False,
        "ddl_executed": False,
    }
