"""P03来源集合候选的纯身份、质量分母与摘要规则。"""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any, Mapping, Sequence
import uuid


CANDIDATE_STATUS = "SOURCE_SET_CANDIDATE_PERSISTED_NON_PIT"
FORMAL_P03_STATUS = "PREREQUISITE_NOT_CONNECTED"
PIT_STATUS = "NON_PIT_RETROSPECTIVE"
IDENTITY_STATUS = "SOURCE_SYMBOL_ONLY"
INPUT_ROLES = (
    "TDX_CONTENT",
    "TDX_CAPTURE",
    "TDX_RAW_DAILY",
    "FIELD_QUALITY",
    "FIELD_QUALITY",
    "FIELD_QUALITY",
    "BAOSTOCK_CAPABILITY",
)
FIELD_FAMILIES = ("PRICE", "AMOUNT", "VOLUME")


class SourceSetCandidateError(ValueError):
    """候选输入摘要或质量分母不符合固定来源集合合同。"""


def _digest_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise SourceSetCandidateError("来源时钟缺少时区")
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, (date, uuid.UUID)):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _digest_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_digest_value(item) for item in value]
    if value is None or type(value) in (str, int, bool):
        return value
    raise SourceSetCandidateError("候选摘要含不支持的类型")


def source_set_candidate_digest(
    header: Mapping[str, Any], inputs: Sequence[Mapping[str, Any]]
) -> str:
    """对精确来源、Raw闭合分母、字段质量、日历及小引用生成稳定摘要。"""
    validate_candidate_summary(header, inputs)
    payload = {
        "header": {
            key: value
            for key, value in header.items()
            if key not in {"candidate_sha256", "created_at"}
        },
        "inputs": [
            dict(row)
            for row in sorted(inputs, key=lambda item: int(item["input_ordinal"]))
        ],
    }
    encoded = json.dumps(
        _digest_value(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_candidate_summary(
    header: Mapping[str, Any], inputs: Sequence[Mapping[str, Any]]
) -> None:
    """拒绝不完整来源、错误引用、未闭合分母或虚构输入行。"""
    if len(inputs) != len(INPUT_ROLES):
        raise SourceSetCandidateError("候选必须包含七条具名输入引用")
    if tuple(row.get("input_role") for row in inputs) != INPUT_ROLES:
        raise SourceSetCandidateError("候选输入角色或顺序不符合固定合同")
    if tuple(row.get("input_ordinal") for row in inputs) != tuple(range(1, 8)):
        raise SourceSetCandidateError("候选输入序号必须连续且唯一")
    if tuple(row.get("field_family") for row in inputs[3:6]) != FIELD_FAMILIES:
        raise SourceSetCandidateError("价格、金额、数量字段质量分母缺失")

    expected = header.get("capture_expected_row_count")
    raw_rows = header.get("raw_row_count")
    present = header.get("target_present_count")
    missing = header.get("target_missing_count")
    failed = header.get("read_failed_count")
    if (
        type(expected) is not int
        or type(raw_rows) is not int
        or type(present) is not int
        or type(missing) is not int
        or type(failed) is not int
        or expected <= 0
        or raw_rows != expected
        or present < 0
        or missing < 0
        or failed != 0
        or present + missing + failed != raw_rows
    ):
        raise SourceSetCandidateError("Raw分母不闭合或存在READ_FAILED")

    for row in inputs[3:6]:
        if (
            row.get("observed_row_count") != expected
            or row.get("expected_row_count") != expected
        ):
            raise SourceSetCandidateError("字段质量没有覆盖完整Raw来源分母")
        for prefix in ("raw", "normalized"):
            values = [
                row.get(f"{prefix}_{suffix}_count")
                for suffix in (
                    "observed",
                    "unknown",
                    "not_applicable",
                    "bad_value",
                )
            ]
            if any(
                value is not None and (type(value) is not int or value < 0)
                for value in values
            ):
                raise SourceSetCandidateError("字段质量计数无效")
            if (
                any(value is not None for value in values)
                and sum(value or 0 for value in values) != expected
            ):
                raise SourceSetCandidateError("字段质量分类没有闭合到Raw分母")

    bao = inputs[6]
    if (
        bao.get("input_status") != "NOT_REQUESTED"
        or bao.get("source_capture_id") is not None
        or bao.get("source_content_id") is not None
        or bao.get("evidence_sha256") is not None
        or bao.get("reason_code") != "OPTIONAL_SOURCE_NOT_REQUESTED"
    ):
        raise SourceSetCandidateError("未请求的Bao能力不得填入捕获或内容占位")
