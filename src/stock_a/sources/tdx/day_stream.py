"""TDX日线单遍流扫描；不拥有调用方传入的流。"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
import hashlib
import math
import time
from typing import BinaryIO, Callable, Iterator

from stock_a.domain import ContractError
from stock_a.sources.tdx.day import DecodedDay, UnitContract, decode_day_record


_MAX_CHUNK_RECORDS = 32768
_RECORD_BYTES = 32
_GROUP_NAMES = ("prices", "amount", "volume")
_ERROR_MESSAGES = {
    "EMPTY_FILE": "输入为空，没有可扫描的日线记录",
    "INCOMPLETE_TAIL_RECORD": "文件末尾不是完整的32字节日线记录",
    "INVALID_DATE": "日线记录中的日期不是有效公历日期",
    "DUPLICATE_DATE": "日线记录日期重复，不能静默去重",
    "SAME_DAY_CONFLICT": "同一日期的已知规范字段存在冲突",
    "NON_INCREASING_DATE": "日线日期没有严格递增",
    "SOURCE_NOT_CONFIGURED": "未绑定非空来源保护根",
    "SOURCE_PATH_INVALID": "来源路径不是可读取的绝对普通文件路径",
    "SOURCE_PATH_OUTSIDE_ROOT": "来源文件最终目标位于所有已绑定保护根之外",
    "SOURCE_NOT_FOUND": "指定来源文件不存在",
    "SOURCE_ACCESS_DENIED": "操作系统拒绝读取来源文件",
    "SOURCE_HANDLE_UNVERIFIED": "无法核验实际来源文件句柄及其最终路径",
    "SOURCE_IO_ERROR": "读取来源时发生I/O错误",
    "SOURCE_CHANGED": "来源文件或路径在扫描期间发生变化",
    "READ_LIMIT_EXCEEDED": "读取超过显式字节数或记录数预算",
    "READ_TIMEOUT": "读取超过显式耗时预算",
    "STREAM_CONTRACT_ERROR": "输入流违反有界二进制读取合同",
    "SCAN_NOT_COMPLETE": "扫描尚未读到EOF并通过最终核验",
}


@dataclass(frozen=True)
class ReadLimits:
    """调用方明确给出的单次读取、总字节、总记录与耗时上限。"""

    max_bytes: int
    max_records: int
    chunk_records: int
    max_seconds: int | float | Decimal

    def __post_init__(self) -> None:
        for name in ("max_bytes", "max_records", "chunk_records"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ContractError(f"{name}必须是拒绝bool的正整数")
        if self.chunk_records > _MAX_CHUNK_RECORDS:
            raise ContractError("chunk_records不得超过32768条")
        value = self.max_seconds
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal, Fraction)):
            raise ContractError("max_seconds必须是有限正数")
        if isinstance(value, Decimal):
            finite = value.is_finite()
        elif isinstance(value, float):
            finite = math.isfinite(value)
        else:
            finite = True
        if not finite or value <= 0:
            raise ContractError("max_seconds必须是有限正数")
        try:
            seconds = float(value)
        except (OverflowError, TypeError, ValueError) as error:
            raise ContractError("max_seconds必须可表示为有限正数") from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ContractError("max_seconds必须可表示为有限正数")
        object.__setattr__(self, "max_seconds", seconds)


@dataclass(frozen=True)
class LocatedDay:
    record_index: int
    byte_offset: int
    decoded: DecodedDay


@dataclass(frozen=True)
class FieldGroupCount:
    observed: int
    unknown: int
    not_applicable: int
    reasons: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class DayScanSummary:
    complete: bool
    record_count: int
    byte_count: int
    first_date: date
    last_date: date
    raw_byte_sha256: str
    unit_version: str
    prices: FieldGroupCount
    amount: FieldGroupCount
    volume: FieldGroupCount
    read_started_at: datetime
    read_completed_at: datetime


class DayReadError(Exception):
    """带稳定错误码、原因类别和可用记录位置的读取错误。"""

    def __init__(
        self,
        code: str,
        reason_category: str,
        *,
        record_index: int | None = None,
        byte_offset: int | None = None,
        actual_tail_bytes: int | None = None,
        context: dict[str, object] | None = None,
        detail: str | None = None,
    ) -> None:
        if code not in _ERROR_MESSAGES:
            raise ValueError(f"未注册的日线读取错误码: {code}")
        self.code = code
        self.reason_category = reason_category
        self.record_index = record_index
        self.byte_offset = byte_offset
        self.actual_tail_bytes = actual_tail_bytes
        self.context = dict(context or {})
        suffix = f"；{detail}" if detail else ""
        where = ""
        if record_index is not None:
            where = f"（记录{record_index}"
            if byte_offset is not None:
                where += f"，字节偏移{byte_offset}"
            where += "）"
        super().__init__(f"{_ERROR_MESSAGES[code]}{where}{suffix}")


def _validate_units_and_limits(units: UnitContract, limits: ReadLimits) -> None:
    if not isinstance(units, UnitContract):
        raise ContractError("缺少有效的日线来源单位合同")
    if not isinstance(limits, ReadLimits):
        raise ContractError("缺少显式日线读取预算")


def _clock_value(clock: Callable[[], float]) -> float:
    if not callable(clock):
        raise ContractError("monotonic必须是可调用的单调时钟")
    value = clock()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ContractError("monotonic必须返回有限数值")
    result = float(value)
    if not math.isfinite(result):
        raise ContractError("monotonic必须返回有限数值")
    return result


class DayRecordScan(Iterator[LocatedDay]):
    """可显式耗尽或关闭的惰性扫描对象。"""

    def __init__(
        self,
        readonly_stream: BinaryIO,
        units: UnitContract,
        limits: ReadLimits,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        final_check: Callable[[], object] | None = None,
        started_at: float | None = None,
        read_started_at: datetime | None = None,
    ) -> None:
        _validate_units_and_limits(units, limits)
        if not callable(getattr(readonly_stream, "read", None)):
            raise ContractError("readonly_stream必须提供read(n)二进制接口")
        self._stream = readonly_stream
        self._units = units
        self._limits = limits
        self._monotonic = monotonic
        self._started_at = _clock_value(monotonic) if started_at is None else started_at
        self._read_started_at = read_started_at or datetime.now().astimezone()
        self._final_check = final_check
        self._block = b""
        self._block_pos = 0
        self._tail = b""
        self._byte_count = 0
        self._record_count = 0
        self._first_date: date | None = None
        self._last_date: date | None = None
        self._previous_decoded: DecodedDay | None = None
        self._previous_raw: bytes | None = None
        self._previous_index: int | None = None
        self._sha256 = hashlib.sha256()
        self._group_quality = {
            name: {"observed": 0, "unknown": 0, "not_applicable": 0, "reasons": Counter()}
            for name in _GROUP_NAMES
        }
        self._complete = False
        self._aborted = False
        self._summary: DayScanSummary | None = None

    def __iter__(self) -> DayRecordScan:
        return self

    @property
    def summary(self) -> DayScanSummary:
        if not self._complete or self._summary is None:
            raise DayReadError("SCAN_NOT_COMPLETE", "LIFECYCLE")
        return self._summary

    @property
    def complete(self) -> bool:
        return self._complete

    def close(self) -> None:
        """结束扫描但不关闭调用方拥有的流。"""
        if not self._complete:
            self._aborted = True
            self._block = b""
            self._tail = b""

    def _invalidate(self) -> None:
        """来源边界无法可靠关闭句柄时撤销尚未交付的摘要。"""
        self._complete = False
        self._summary = None
        self._aborted = True
        self._block = b""
        self._tail = b""

    def _check_time(self) -> None:
        if _clock_value(self._monotonic) - self._started_at > self._limits.max_seconds:
            raise DayReadError("READ_TIMEOUT", "BUDGET")

    def _read_size(self) -> int:
        chunk_room = self._limits.chunk_records * _RECORD_BYTES - len(self._tail)
        byte_room_with_probe = self._limits.max_bytes + 1 - self._byte_count
        record_room_with_probe = self._limits.max_records * _RECORD_BYTES + 1 - self._byte_count
        return min(chunk_room, byte_room_with_probe, record_room_with_probe)

    def _read_block(self) -> None:
        self._check_time()
        request_size = self._read_size()
        if request_size <= 0:
            raise DayReadError(
                "READ_LIMIT_EXCEEDED",
                "BUDGET",
                record_index=self._record_count,
                byte_offset=self._record_count * _RECORD_BYTES,
                context={"byte_count": self._byte_count},
            )
        try:
            data = self._stream.read(request_size)
        except OSError as error:
            raise DayReadError("SOURCE_IO_ERROR", "IO") from error
        self._check_time()
        if data is None or not isinstance(data, bytes) or len(data) > request_size:
            raise DayReadError(
                "STREAM_CONTRACT_ERROR",
                "STREAM_CONTRACT",
                record_index=self._record_count,
                byte_offset=self._record_count * _RECORD_BYTES,
                context={"requested_bytes": request_size, "returned_type": type(data).__name__},
            )
        if not data:
            self._block = b""
            self._block_pos = 0
            self._at_eof = True
            return
        self._at_eof = False
        self._byte_count += len(data)
        self._sha256.update(data)
        if self._byte_count > self._limits.max_bytes:
            raise DayReadError(
                "READ_LIMIT_EXCEEDED",
                "BUDGET",
                record_index=self._record_count,
                byte_offset=self._record_count * _RECORD_BYTES,
                context={"byte_count": self._byte_count, "max_bytes": self._limits.max_bytes},
            )
        self._block = data
        self._block_pos = 0

    def _take_record(self) -> bytes | None:
        while True:
            available = len(self._block) - self._block_pos
            required = _RECORD_BYTES - len(self._tail)
            if available >= required:
                if self._tail:
                    record = self._tail + self._block[self._block_pos:self._block_pos + required]
                    self._tail = b""
                    self._block_pos += required
                    return record
                record = self._block[self._block_pos:self._block_pos + _RECORD_BYTES]
                self._block_pos += _RECORD_BYTES
                return record
            if available:
                self._tail += self._block[self._block_pos:]
                self._block_pos = len(self._block)
                if len(self._tail) >= _RECORD_BYTES:
                    raise AssertionError("尾部缓冲超过一条记录")
            if self._at_eof:
                return None
            self._read_block()

    def _count_fields(self, decoded: DecodedDay) -> None:
        for name in _GROUP_NAMES:
            value = getattr(decoded, name)
            bucket = self._group_quality[name]
            if value.quality == "OBSERVED":
                bucket["observed"] += 1
            elif value.quality == "UNKNOWN_DATA":
                bucket["unknown"] += 1
                bucket["reasons"][value.reason] += 1
            else:
                bucket["not_applicable"] += 1
                bucket["reasons"][value.reason] += 1

    @staticmethod
    def _same_day_difference(
        previous: DecodedDay,
        current: DecodedDay,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        conflicts: list[str] = []
        unknown_groups: list[str] = []
        for name in _GROUP_NAMES:
            before = getattr(previous, name)
            after = getattr(current, name)
            if before.quality == after.quality == "OBSERVED":
                if before.value != after.value:
                    conflicts.append(name)
            else:
                unknown_groups.append(name)
        return tuple(conflicts), tuple(unknown_groups)

    def _located(self, raw: bytes) -> LocatedDay:
        index = self._record_count
        offset = index * _RECORD_BYTES
        if index >= self._limits.max_records or offset + _RECORD_BYTES > self._limits.max_bytes:
            raise DayReadError(
                "READ_LIMIT_EXCEEDED",
                "BUDGET",
                record_index=index,
                byte_offset=offset,
                context={"max_bytes": self._limits.max_bytes, "max_records": self._limits.max_records},
            )
        self._check_time()
        try:
            decoded = decode_day_record(raw, self._units)
        except ValueError as error:
            raise DayReadError(
                "INVALID_DATE",
                "DATA_INVALID",
                record_index=index,
                byte_offset=offset,
                detail="日期字段无法构成公历日期",
            ) from error
        previous = self._previous_decoded
        if previous is not None:
            previous_index = self._previous_index
            if decoded.session_date < previous.session_date:
                raise DayReadError(
                    "NON_INCREASING_DATE",
                    "STRUCTURE",
                    record_index=index,
                    byte_offset=offset,
                    context={"previous_record_index": previous_index},
                )
            if decoded.session_date == previous.session_date:
                conflicts, unknown_groups = self._same_day_difference(previous, decoded)
                if conflicts:
                    raise DayReadError(
                        "SAME_DAY_CONFLICT",
                        "STRUCTURE",
                        record_index=index,
                        byte_offset=offset,
                        context={
                            "previous_record_index": previous_index,
                            "conflicting_groups": conflicts,
                            "unknown_groups": unknown_groups,
                        },
                    )
                raise DayReadError(
                    "DUPLICATE_DATE",
                    "STRUCTURE",
                    record_index=index,
                    byte_offset=offset,
                    context={
                        "previous_record_index": previous_index,
                        "raw_bytes_equal": raw == self._previous_raw,
                        "unknown_groups": unknown_groups,
                        "reserved_equal": previous.reserved == decoded.reserved,
                    },
                    detail="未知字段无法证明规范业务值一致" if unknown_groups else "规范已知字段相同",
                )
        self._check_time()
        self._record_count += 1
        self._first_date = decoded.session_date if self._first_date is None else self._first_date
        self._last_date = decoded.session_date
        self._count_fields(decoded)
        self._previous_decoded = decoded
        self._previous_raw = raw
        self._previous_index = index
        self._check_time()
        return LocatedDay(index, offset, decoded)

    def _freeze_group(self, name: str) -> FieldGroupCount:
        bucket = self._group_quality[name]
        return FieldGroupCount(
            observed=bucket["observed"],
            unknown=bucket["unknown"],
            not_applicable=bucket["not_applicable"],
            reasons=tuple(sorted(bucket["reasons"].items())),
        )

    def _finish(self) -> None:
        self._check_time()
        if self._byte_count == 0:
            raise DayReadError("EMPTY_FILE", "DATA_INVALID")
        if self._tail:
            raise DayReadError(
                "INCOMPLETE_TAIL_RECORD",
                "STRUCTURE",
                record_index=self._record_count,
                byte_offset=self._record_count * _RECORD_BYTES,
                actual_tail_bytes=len(self._tail),
                context={"actual_tail_bytes": len(self._tail)},
            )
        final_identity = None
        if self._final_check is not None:
            self._check_time()
            final_identity = self._final_check()
            self._check_time()
        if self._first_date is None or self._last_date is None:
            raise DayReadError("EMPTY_FILE", "DATA_INVALID")
        completed_at = datetime.now().astimezone()
        self._summary = DayScanSummary(
            complete=True,
            record_count=self._record_count,
            byte_count=self._byte_count,
            first_date=self._first_date,
            last_date=self._last_date,
            raw_byte_sha256=self._sha256.hexdigest(),
            unit_version=self._units.version,
            prices=self._freeze_group("prices"),
            amount=self._freeze_group("amount"),
            volume=self._freeze_group("volume"),
            read_started_at=self._read_started_at,
            read_completed_at=completed_at,
        )
        self._final_identity = final_identity
        self._complete = True

    def __next__(self) -> LocatedDay:
        if self._complete or self._aborted:
            raise StopIteration
        if not hasattr(self, "_at_eof"):
            self._at_eof = False
        try:
            self._check_time()
            raw = self._take_record()
            if raw is None:
                self._finish()
                finished = True
                row = None
            else:
                finished = False
                row = self._located(raw)
        except BaseException:
            self._invalidate()
            raise
        if finished:
            raise StopIteration
        return row


def iter_day_records(
    readonly_stream: BinaryIO,
    units: UnitContract,
    limits: ReadLimits,
    *,
    monotonic: Callable[[], float] = time.monotonic,
) -> DayRecordScan:
    """创建惰性单遍扫描；调用方拥有并负责关闭readonly_stream。"""
    return DayRecordScan(readonly_stream, units, limits, monotonic=monotonic)
