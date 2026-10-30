"""TDX只读扫描摘要的capture封套投影；不读取来源或持久化。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
import json
import re
from uuid import UUID

from stock_a.domain import ContractError
from stock_a.sources.tdx.day_source import DayFileSummary, FileSourceIdentity
from stock_a.sources.tdx.day_stream import DayScanSummary, FieldGroupCount


_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EXCLUDED_SCOPE_KEYS = frozenset(
    {"request_id", "cache_hit", "temporary_path", "wall_clock_duration"}
)


class TdxCaptureError(ContractError):
    """TDX捕获封套合同拒绝，带稳定机器原因码。"""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class TdxCaptureRequest:
    """调用方提供的数据集和解析合同；不会从路径或当前时间推断。"""

    capture_id: str
    dataset_id: str
    dataset_version: str
    query_scope: Mapping[str, object]
    query_start: date
    query_end: date
    parser_version: str
    validation_contract: str


@dataclass(frozen=True)
class TdxPackageEntryReference:
    """官网ZIP条目的来源身份与本次本地重验时钟；不代表PIT或官方成员身份。"""

    package_id: str
    package_sha256: str
    official_package_url: str
    package_verified_at: datetime
    package_file_size: int
    zip_entry_count: int
    zip_crc: str
    source_symbol: str
    zip_entry_path: str
    entry_sha256: str
    entry_byte_count: int
    target_session: date
    observed_started_at: datetime
    observed_completed_at: datetime
    published_at: datetime | None = None
    first_available_at: datetime | None = None
    source_clock_role: str = "LOCAL_VERIFIED_ZIP_ENTRY_READ_NON_PIT"


@dataclass(frozen=True)
class TdxCaptureEnvelope:
    """完成扫描的审计投影，不代表PIT、覆盖通过或source_set封口。"""

    capture_id: str
    dataset_id: str
    dataset_version: str
    query_scope_canonical_json: str
    query_start: date
    query_end: date
    parser_version: str
    validation_contract: str
    content_sha256: str
    business_digest_sha256: str
    record_count: int
    byte_count: int
    first_date: date
    last_date: date
    unit_version: str
    prices: FieldGroupCount
    amount: FieldGroupCount
    volume: FieldGroupCount
    observed_started_at: datetime
    observed_completed_at: datetime
    source_identity: FileSourceIdentity | None
    source_package_entry: TdxPackageEntryReference | None = None


def _required_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TdxCaptureError(
            "CAPTURE_IDENTITY_INVALID", f"捕获封套{name}必须是非空字符串"
        )
    return value


def _scope_value(value: object) -> object:
    if value is None or type(value) in (str, int, bool):
        return value
    if isinstance(value, list) or isinstance(value, tuple):
        return [_scope_value(item) for item in value]
    if isinstance(value, Mapping):
        projected = {}
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise TdxCaptureError(
                    "CAPTURE_SCOPE_INVALID", "查询范围对象键必须为非空字符串"
                )
            if key in _EXCLUDED_SCOPE_KEYS:
                continue
            projected[key] = _scope_value(item)
        return projected
    raise TdxCaptureError(
        "CAPTURE_SCOPE_INVALID", "查询范围只接受稳定JSON值，拒绝浮点和任意对象"
    )


def _validate_group(group: FieldGroupCount, record_count: int, name: str) -> None:
    if not isinstance(group, FieldGroupCount):
        raise TdxCaptureError("CAPTURE_QUALITY_INVALID", f"{name}质量计数类型无效")
    counts = (group.observed, group.unknown, group.not_applicable)
    if any(type(value) is not int or value < 0 for value in counts):
        raise TdxCaptureError(
            "CAPTURE_QUALITY_INVALID", f"{name}质量计数必须是非负整数"
        )
    if sum(counts) != record_count:
        raise TdxCaptureError(
            "CAPTURE_QUALITY_INVALID", f"{name}质量计数与扫描记录总数不一致"
        )
    if not isinstance(group.reasons, tuple):
        raise TdxCaptureError(
            "CAPTURE_QUALITY_INVALID", f"{name}原因计数必须为不可变元组"
        )
    seen = set()
    for item in group.reasons:
        if (
            not isinstance(item, tuple)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not item[0]
            or type(item[1]) is not int
            or item[1] <= 0
            or item[0] in seen
        ):
            raise TdxCaptureError(
                "CAPTURE_QUALITY_INVALID", f"{name}原因计数含非法或重复项"
            )
        seen.add(item[0])
    if (
        sum(count for _reason, count in group.reasons)
        != group.unknown + group.not_applicable
    ):
        raise TdxCaptureError(
            "CAPTURE_QUALITY_INVALID", f"{name}原因计数与非OBSERVED质量数量不一致"
        )


def _validate_source_identity(source: FileSourceIdentity, byte_count: int) -> None:
    if not isinstance(source, FileSourceIdentity):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "缺少经核验的来源句柄身份"
        )
    if (
        source.requested_access != "GENERIC_READ"
        or source.path_binding_verified is not True
        or source.handle_path_verified is not True
    ):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "来源句柄不是已核验只读路径绑定"
        )
    for value in (
        source.volume_serial_number,
        source.file_id,
        source.size_before,
        source.size_after,
        source.mtime_ns_before,
        source.mtime_ns_after,
    ):
        if type(value) is not int or value < 0:
            raise TdxCaptureError(
                "CAPTURE_SOURCE_IDENTITY_INVALID", "来源句柄版本字段非法"
            )
    if (
        source.size_before != byte_count
        or source.size_after != byte_count
        or source.mtime_ns_before != source.mtime_ns_after
    ):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_CHANGED", "来源句柄版本与完整扫描字节数不一致"
        )


def _validate_package_entry_reference(
    source: TdxPackageEntryReference,
    scan: DayScanSummary,
    request: TdxCaptureRequest,
) -> None:
    if not isinstance(source, TdxPackageEntryReference):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "缺少已核验的官网ZIP条目来源身份"
        )
    if (
        not isinstance(source.package_id, str)
        or source.package_id != f"TDX-HSJDAY-SHA256:{source.package_sha256}"
        or not isinstance(source.package_sha256, str)
        or not _HEX_SHA256.fullmatch(source.package_sha256)
        or not isinstance(source.entry_sha256, str)
        or not _HEX_SHA256.fullmatch(source.entry_sha256)
    ):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "ZIP包或条目SHA身份无效"
        )
    if source.official_package_url != "https://data.tdx.com.cn/vipdoc/hsjday.zip":
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "ZIP条目未绑定登记的通达信官网端点"
        )
    if (
        type(source.package_file_size) is not int
        or source.package_file_size <= 0
        or type(source.zip_entry_count) is not int
        or source.zip_entry_count <= 0
        or source.zip_crc != "PASS"
        or type(source.entry_byte_count) is not int
        or source.entry_byte_count != scan.byte_count
        or source.entry_sha256 != scan.raw_byte_sha256
    ):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "ZIP条目字节数或SHA与完整扫描摘要不一致"
        )
    if (
        not isinstance(source.source_symbol, str)
        or not source.source_symbol
        or not isinstance(source.zip_entry_path, str)
        or not source.zip_entry_path
        or type(source.target_session) is not date
    ):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "ZIP条目路径、来源代码或目标会话无效"
        )
    if (
        request.query_scope.get("source_symbol") != source.source_symbol
        or request.query_scope.get("target_session")
        != source.target_session.isoformat()
    ):
        raise TdxCaptureError(
            "CAPTURE_SOURCE_IDENTITY_INVALID", "ZIP条目来源代码或目标日与捕获请求不一致"
        )
    timestamps = (
        source.package_verified_at,
        source.observed_started_at,
        source.observed_completed_at,
    )
    if any(
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
        for value in timestamps
    ):
        raise TdxCaptureError(
            "CAPTURE_OBSERVATION_TIME_INVALID", "ZIP包验证及条目观测时间必须带时区"
        )
    if (
        source.observed_completed_at < source.observed_started_at
        or source.package_verified_at > source.observed_started_at
        or source.source_clock_role != "LOCAL_VERIFIED_ZIP_ENTRY_READ_NON_PIT"
    ):
        raise TdxCaptureError(
            "CAPTURE_OBSERVATION_TIME_INVALID", "ZIP条目本地重验时间顺序或时间角色无效"
        )
    if any(
        value is not None for value in (source.published_at, source.first_available_at)
    ):
        raise TdxCaptureError(
            "CAPTURE_OBSERVATION_TIME_INVALID", "未知官网发布时间或历史首获时间不得伪造"
        )


def build_tdx_capture_envelope(
    summary: DayFileSummary | DayScanSummary,
    request: TdxCaptureRequest,
    *,
    source_package_entry: TdxPackageEntryReference | None = None,
) -> TdxCaptureEnvelope:
    """将已完成的只读文件或官网ZIP条目扫描投影为不可变捕获封套。"""
    if not isinstance(request, TdxCaptureRequest):
        raise TdxCaptureError("CAPTURE_REQUEST_INVALID", "必须提供显式TDX捕获请求合同")
    if isinstance(summary, DayFileSummary):
        scan = summary.scan
        source_identity = summary.source
    elif isinstance(summary, DayScanSummary):
        scan = summary
        source_identity = None
    else:
        raise TdxCaptureError(
            "CAPTURE_SCAN_INVALID", "必须提供TDX日线文件或ZIP条目的扫描摘要"
        )
    if not isinstance(scan, DayScanSummary):
        raise TdxCaptureError("CAPTURE_SCAN_INVALID", "缺少TDX日线扫描摘要")
    if scan.complete is not True:
        raise TdxCaptureError(
            "CAPTURE_SCAN_INCOMPLETE", "来源扫描未完整到达EOF并完成最终核验"
        )
    if source_package_entry is None:
        if source_identity is None:
            raise TdxCaptureError(
                "CAPTURE_SOURCE_IDENTITY_INVALID",
                "完整扫描必须绑定只读文件句柄或已核验ZIP条目",
            )
    else:
        if source_identity is not None:
            raise TdxCaptureError(
                "CAPTURE_SOURCE_IDENTITY_INVALID",
                "Capture来源不得混合本地解压文件与ZIP条目身份",
            )
        _validate_package_entry_reference(source_package_entry, scan, request)
    capture_id = _required_text(request.capture_id, "capture_id")
    try:
        if str(UUID(capture_id)) != capture_id:
            raise ValueError("noncanonical")
    except (ValueError, AttributeError, TypeError) as error:
        raise TdxCaptureError(
            "CAPTURE_IDENTITY_INVALID", "capture_id必须是规范UUID字符串"
        ) from error
    dataset_id = _required_text(request.dataset_id, "dataset_id")
    dataset_version = _required_text(request.dataset_version, "dataset_version")
    parser_version = _required_text(request.parser_version, "parser_version")
    validation_contract = _required_text(
        request.validation_contract, "validation_contract"
    )
    if (
        type(request.query_start) is not date
        or type(request.query_end) is not date
        or request.query_start > request.query_end
    ):
        raise TdxCaptureError(
            "CAPTURE_QUERY_RANGE_INVALID", "查询日期范围必须是递增的date"
        )
    if not isinstance(request.query_scope, Mapping) or not request.query_scope:
        raise TdxCaptureError("CAPTURE_SCOPE_INVALID", "查询范围必须显式提供非空对象")
    scope_projection = _scope_value(request.query_scope)
    scope_json = json.dumps(
        scope_projection, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    if (
        type(scan.record_count) is not int
        or scan.record_count <= 0
        or type(scan.byte_count) is not int
        or scan.byte_count != scan.record_count * 32
    ):
        raise TdxCaptureError(
            "CAPTURE_SCAN_INVALID", "完整扫描的记录数与32字节记录长度不一致"
        )
    if (
        type(scan.first_date) is not date
        or type(scan.last_date) is not date
        or scan.first_date > scan.last_date
        or scan.first_date < request.query_start
        or scan.last_date > request.query_end
    ):
        raise TdxCaptureError(
            "CAPTURE_COVERAGE_INVALID", "扫描首末交易日不在显式查询范围内"
        )
    if not isinstance(scan.raw_byte_sha256, str) or not _HEX_SHA256.fullmatch(
        scan.raw_byte_sha256
    ):
        raise TdxCaptureError("CAPTURE_DIGEST_INVALID", "原始字节SHA-256格式无效")
    unit_version = _required_text(scan.unit_version, "unit_version")
    _validate_group(scan.prices, scan.record_count, "prices")
    _validate_group(scan.amount, scan.record_count, "amount")
    _validate_group(scan.volume, scan.record_count, "volume")
    for timestamp in (scan.read_started_at, scan.read_completed_at):
        if (
            not isinstance(timestamp, datetime)
            or timestamp.tzinfo is None
            or timestamp.utcoffset() is None
        ):
            raise TdxCaptureError(
                "CAPTURE_OBSERVATION_TIME_INVALID", "读取观测时间必须是带时区datetime"
            )
    if scan.read_completed_at < scan.read_started_at:
        raise TdxCaptureError(
            "CAPTURE_OBSERVATION_TIME_INVALID", "读取完成时间早于读取开始时间"
        )
    if source_identity is not None:
        _validate_source_identity(source_identity, scan.byte_count)
    digest_payload = {
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "query_scope": scope_projection,
        "query_start": request.query_start.isoformat(),
        "query_end": request.query_end.isoformat(),
        "parser_version": parser_version,
        "validation_contract": validation_contract,
        "raw_byte_sha256": scan.raw_byte_sha256,
        "record_count": scan.record_count,
        "byte_count": scan.byte_count,
        "first_date": scan.first_date.isoformat(),
        "last_date": scan.last_date.isoformat(),
        "unit_version": unit_version,
        "quality": {
            name: {
                "observed": getattr(scan, name).observed,
                "unknown": getattr(scan, name).unknown,
                "not_applicable": getattr(scan, name).not_applicable,
                "reasons": sorted(getattr(scan, name).reasons),
            }
            for name in ("prices", "amount", "volume")
        },
    }
    business_digest = sha256(
        json.dumps(
            digest_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return TdxCaptureEnvelope(
        capture_id=capture_id,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        query_scope_canonical_json=scope_json,
        query_start=request.query_start,
        query_end=request.query_end,
        parser_version=parser_version,
        validation_contract=validation_contract,
        content_sha256=scan.raw_byte_sha256,
        business_digest_sha256=business_digest,
        record_count=scan.record_count,
        byte_count=scan.byte_count,
        first_date=scan.first_date,
        last_date=scan.last_date,
        unit_version=unit_version,
        prices=scan.prices,
        amount=scan.amount,
        volume=scan.volume,
        observed_started_at=scan.read_started_at,
        observed_completed_at=scan.read_completed_at,
        source_identity=source_identity,
        source_package_entry=source_package_entry,
    )
