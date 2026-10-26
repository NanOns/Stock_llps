"""绑定显式来源根并以Windows只读句柄扫描TDX日线文件。"""
from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import stat
import time
from typing import Callable, Iterable, Iterator

from stock_a.domain import ContractError
from stock_a.sources.tdx.day import UnitContract
from stock_a.sources.tdx.day_stream import (
    DayReadError,
    DayRecordScan,
    DayScanSummary,
    LocatedDay,
    ReadLimits,
    _clock_value,
)


_GENERIC_READ = 0x80000000
_FILE_SHARE_READ = 0x00000001
_FILE_SHARE_WRITE = 0x00000002
_FILE_SHARE_DELETE = 0x00000004
_OPEN_EXISTING = 3
_FILE_ATTRIBUTE_NORMAL = 0x00000080
_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_FILE_TYPE_DISK = 0x0001
_ERROR_FILE_NOT_FOUND = 2
_ERROR_PATH_NOT_FOUND = 3
_ERROR_ACCESS_DENIED = 5
_FILETIME_TO_UNIX_EPOCH_NS = 11_644_473_600_000_000_000


class _FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]


class _BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", wintypes.DWORD),
        ("ftCreationTime", _FILETIME),
        ("ftLastAccessTime", _FILETIME),
        ("ftLastWriteTime", _FILETIME),
        ("dwVolumeSerialNumber", wintypes.DWORD),
        ("nFileSizeHigh", wintypes.DWORD),
        ("nFileSizeLow", wintypes.DWORD),
        ("nNumberOfLinks", wintypes.DWORD),
        ("nFileIndexHigh", wintypes.DWORD),
        ("nFileIndexLow", wintypes.DWORD),
    ]


@dataclass(frozen=True)
class FileSourceIdentity:
    requested_path: str
    handle_path: str
    volume_serial_number: int
    file_id: int
    size_before: int
    size_after: int
    mtime_ns_before: int
    mtime_ns_after: int
    requested_access: str
    path_binding_verified: bool
    handle_path_verified: bool


@dataclass(frozen=True)
class DayFileSummary:
    scan: DayScanSummary
    source: FileSourceIdentity

    @property
    def complete(self) -> bool:
        return self.scan.complete


@dataclass(frozen=True)
class _HandleVersion:
    final_path: Path
    volume_serial_number: int
    file_id: int
    size: int
    mtime_ns: int


def _failure(
    code: str,
    reason_category: str,
    *,
    detail: str | None = None,
    cause: BaseException | None = None,
) -> DayReadError:
    error = DayReadError(code, reason_category, detail=detail)
    if cause is not None:
        error.__cause__ = cause
    return error


def _path_value(value: object, label: str) -> Path:
    if not isinstance(value, (str, os.PathLike)) or isinstance(value, bool):
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail=f"{label}类型无效")
    try:
        rendered = os.fspath(value)
        if not isinstance(rendered, str) or not rendered.strip():
            raise ValueError("路径为空")
        path = Path(rendered)
    except (TypeError, ValueError, OSError) as error:
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail=f"{label}格式无效", cause=error)
    if not path.is_absolute():
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail=f"{label}必须是绝对路径")
    return path


def _resolved(path: Path, *, missing_code: str, label: str) -> Path:
    try:
        return path.resolve(strict=True)
    except FileNotFoundError as error:
        raise _failure(missing_code, "PATH", detail=f"{label}不存在", cause=error)
    except PermissionError as error:
        raise _failure("SOURCE_ACCESS_DENIED", "PERMISSION", cause=error)
    except (OSError, RuntimeError, RecursionError, ValueError) as error:
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail=f"{label}无法解析", cause=error)


def _inside_roots(candidate: Path, roots: tuple[Path, ...]) -> bool:
    for root in roots:
        try:
            candidate.relative_to(root)
            return True
        except ValueError:
            continue
    return False


def _resolved_roots(source_roots: Iterable[str | os.PathLike[str]] | None) -> tuple[Path, ...]:
    if source_roots is None:
        raise _failure("SOURCE_NOT_CONFIGURED", "CONFIGURATION")
    if isinstance(source_roots, (str, bytes, os.PathLike)):
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail="source_roots必须是根路径序列")
    try:
        roots_input = tuple(source_roots)
    except TypeError as error:
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail="source_roots必须是可读序列", cause=error)
    if not roots_input:
        raise _failure("SOURCE_NOT_CONFIGURED", "CONFIGURATION")
    roots: list[Path] = []
    for value in roots_input:
        root = _path_value(value, "来源保护根")
        resolved = _resolved(root, missing_code="SOURCE_PATH_INVALID", label="来源保护根")
        try:
            mode = resolved.stat().st_mode
        except PermissionError as error:
            raise _failure("SOURCE_ACCESS_DENIED", "PERMISSION", cause=error)
        except OSError as error:
            raise _failure("SOURCE_IO_ERROR", "IO", cause=error)
        if not stat.S_ISDIR(mode):
            raise _failure("SOURCE_PATH_INVALID", "PATH", detail="来源保护根必须是目录")
        roots.append(resolved)
    return tuple(roots)


def _windows_kernel32():
    if os.name != "nt":
        raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="当前平台没有受支持的Windows句柄核验")
    try:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    except (AttributeError, OSError) as error:
        raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="无法载入Windows句柄API", cause=error)
    kernel32.CreateFileW.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.GetFileType.argtypes = (wintypes.HANDLE,)
    kernel32.GetFileType.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.GetFileInformationByHandle.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_BY_HANDLE_FILE_INFORMATION),
    )
    kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
    kernel32.GetFinalPathNameByHandleW.argtypes = (
        wintypes.HANDLE,
        wintypes.LPWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    kernel32.GetFinalPathNameByHandleW.restype = wintypes.DWORD
    return kernel32


def _open_readonly(path: Path):
    kernel32 = _windows_kernel32()
    handle = kernel32.CreateFileW(
        str(path),
        _GENERIC_READ,
        _FILE_SHARE_READ | _FILE_SHARE_WRITE | _FILE_SHARE_DELETE,
        None,
        _OPEN_EXISTING,
        _FILE_ATTRIBUTE_NORMAL,
        None,
    )
    invalid = ctypes.c_void_p(-1).value
    if handle in (None, invalid):
        error_number = ctypes.get_last_error()
        cause = ctypes.WinError(error_number)
        if error_number in (_ERROR_FILE_NOT_FOUND, _ERROR_PATH_NOT_FOUND):
            raise _failure("SOURCE_NOT_FOUND", "PATH", cause=cause)
        if error_number == _ERROR_ACCESS_DENIED:
            raise _failure("SOURCE_ACCESS_DENIED", "PERMISSION", cause=cause)
        raise _failure("SOURCE_IO_ERROR", "IO", cause=cause)
    try:
        import msvcrt

        descriptor = msvcrt.open_osfhandle(int(handle), os.O_RDONLY | os.O_BINARY)
    except BaseException as error:
        try:
            if not kernel32.CloseHandle(handle):
                close_error = ctypes.WinError(ctypes.get_last_error())
                add_note = getattr(error, "add_note", None)
                if callable(add_note):
                    add_note(f"关闭未接管的Windows来源句柄时另有错误：{close_error}")
        except BaseException as close_error:
            add_note = getattr(error, "add_note", None)
            if callable(add_note):
                add_note(f"关闭未接管的Windows来源句柄时另有错误：{close_error}")
        if not isinstance(error, (ImportError, OSError, OverflowError, TypeError)):
            raise
        raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="只读句柄无法接入受控流", cause=error)
    try:
        stream = os.fdopen(descriptor, "rb", buffering=0)
    except BaseException as error:
        try:
            os.close(descriptor)
        except BaseException as close_error:
            add_note = getattr(error, "add_note", None)
            if callable(add_note):
                add_note(f"关闭未接管的来源文件描述符时另有错误：{close_error}")
        raise
    return stream, kernel32


def _final_path(kernel32, handle: int) -> Path:
    capacity = 512
    while capacity <= 32768:
        buffer = ctypes.create_unicode_buffer(capacity)
        length = kernel32.GetFinalPathNameByHandleW(handle, buffer, capacity, 0)
        if length == 0:
            error = ctypes.WinError(ctypes.get_last_error())
            raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="无法取得句柄最终路径", cause=error)
        if length < capacity:
            value = buffer.value
            if value.startswith("\\\\?\\UNC\\"):
                value = "\\\\" + value[8:]
            elif value.startswith("\\\\?\\"):
                value = value[4:]
            try:
                return Path(value)
            except (OSError, ValueError) as error:
                raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="句柄最终路径格式无效", cause=error)
        capacity = length + 1
    raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="句柄最终路径超出平台上限")


def _handle_version(stream, kernel32) -> _HandleVersion:
    import msvcrt

    try:
        handle = msvcrt.get_osfhandle(stream.fileno())
    except (OSError, ValueError) as error:
        raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="实际文件句柄不可用", cause=error)
    if kernel32.GetFileType(handle) != _FILE_TYPE_DISK:
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail="来源目标不是普通磁盘文件")
    info = _BY_HANDLE_FILE_INFORMATION()
    if not kernel32.GetFileInformationByHandle(handle, ctypes.byref(info)):
        error = ctypes.WinError(ctypes.get_last_error())
        raise _failure("SOURCE_HANDLE_UNVERIFIED", "HANDLE", detail="无法读取句柄文件身份", cause=error)
    if info.dwFileAttributes & _FILE_ATTRIBUTE_DIRECTORY:
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail="来源目标是目录")
    size = (int(info.nFileSizeHigh) << 32) | int(info.nFileSizeLow)
    file_id = (int(info.nFileIndexHigh) << 32) | int(info.nFileIndexLow)
    write_ticks = (int(info.ftLastWriteTime.dwHighDateTime) << 32) | int(info.ftLastWriteTime.dwLowDateTime)
    final_path = _final_path(kernel32, handle)
    return _HandleVersion(
        final_path=final_path,
        volume_serial_number=int(info.dwVolumeSerialNumber),
        file_id=file_id,
        size=size,
        mtime_ns=write_ticks * 100 - _FILETIME_TO_UNIX_EPOCH_NS,
    )


def _stat_version(path: Path) -> os.stat_result:
    try:
        result = path.stat()
    except FileNotFoundError as error:
        raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="扫描期间来源路径消失", cause=error)
    except PermissionError as error:
        raise _failure("SOURCE_ACCESS_DENIED", "PERMISSION", cause=error)
    except OSError as error:
        raise _failure("SOURCE_IO_ERROR", "IO", cause=error)
    if not stat.S_ISREG(result.st_mode):
        raise _failure("SOURCE_PATH_INVALID", "PATH", detail="来源目标必须是普通文件")
    return result


def _path_identity_matches(path_stat: os.stat_result, handle: _HandleVersion) -> bool:
    # Windows的st_dev不是BY_HANDLE_FILE_INFORMATION卷序列号；st_ino映射文件索引。
    return (
        int(path_stat.st_ino) == handle.file_id
        and int(path_stat.st_size) == handle.size
        and int(path_stat.st_mtime_ns) == handle.mtime_ns
    )


class _FileDayScan(Iterator[LocatedDay]):
    def __init__(self, scan: DayRecordScan, stream) -> None:
        self._scan = scan
        self._stream = stream

    def __iter__(self) -> _FileDayScan:
        return self

    def __next__(self) -> LocatedDay:
        return next(self._scan)

    @property
    def summary(self) -> DayFileSummary:
        summary = self._scan.summary
        identity = getattr(self._scan, "_final_identity", None)
        if not isinstance(identity, FileSourceIdentity):
            raise DayReadError("SCAN_NOT_COMPLETE", "LIFECYCLE")
        return DayFileSummary(summary, identity)

    @property
    def complete(self) -> bool:
        return self._scan.complete

    @property
    def source_handle_closed(self) -> bool:
        return self._stream.closed

    def close(self) -> None:
        self._scan.close()
        self._close_stream()

    def _close_stream(self) -> None:
        if not self._stream.closed:
            try:
                self._stream.close()
            except OSError as error:
                self._scan._invalidate()
                raise _failure("SOURCE_IO_ERROR", "IO", detail="关闭来源句柄失败", cause=error)


class _DayFileContext:
    def __init__(
        self,
        source_path: str | os.PathLike[str],
        source_roots: Iterable[str | os.PathLike[str]] | None,
        units: UnitContract,
        limits: ReadLimits,
        monotonic: Callable[[], float],
    ) -> None:
        self._source_value = source_path
        self._roots_value = source_roots
        self._units = units
        self._limits = limits
        self._monotonic = monotonic
        self._stream = None
        self._scan: DayRecordScan | None = None
        self._file_scan: _FileDayScan | None = None
        self._entered = False

    def __enter__(self) -> _FileDayScan:
        if self._entered:
            raise ContractError("同一文件扫描上下文不能重复进入")
        self._entered = True
        if not isinstance(self._units, UnitContract) or not isinstance(self._limits, ReadLimits):
            raise ContractError("文件扫描需要有效单位合同和显式读取预算")
        started_at = _clock_value(self._monotonic)
        started_wall = datetime.now().astimezone()
        if self._roots_value is None or isinstance(self._roots_value, (str, bytes, os.PathLike)):
            root_values = self._roots_value
        else:
            try:
                root_values = tuple(self._roots_value)
            except TypeError as error:
                raise _failure("SOURCE_PATH_INVALID", "PATH", detail="source_roots必须是可读序列", cause=error)
        roots = _resolved_roots(root_values)
        requested_path = _path_value(self._source_value, "来源文件")
        initial_path = _resolved(requested_path, missing_code="SOURCE_NOT_FOUND", label="来源文件")
        if not _inside_roots(initial_path, roots):
            raise _failure("SOURCE_PATH_OUTSIDE_ROOT", "PATH")
        initial_stat = _stat_version(initial_path)
        if any(":" in part for part in initial_path.parts[1:]):
            raise _failure("SOURCE_PATH_INVALID", "PATH", detail="不接受NTFS备用数据流路径")
        if _clock_value(self._monotonic) - started_at > self._limits.max_seconds:
            raise DayReadError("READ_TIMEOUT", "BUDGET")
        stream = None
        try:
            stream, kernel32 = _open_readonly(requested_path)
            self._stream = stream
            before_handle = _handle_version(stream, kernel32)
            if not _inside_roots(before_handle.final_path, roots):
                raise _failure("SOURCE_PATH_OUTSIDE_ROOT", "PATH", detail="实际句柄最终目标在保护根外")
            if before_handle.final_path != initial_path:
                raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="打开时句柄目标与预检查路径不一致")
            if (
                int(initial_stat.st_ino) != before_handle.file_id
                or int(initial_stat.st_size) != before_handle.size
                or int(initial_stat.st_mtime_ns) != before_handle.mtime_ns
            ):
                raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="打开窗口内文件身份或版本发生变化")
            current_path = _resolved(requested_path, missing_code="SOURCE_CHANGED", label="来源文件")
            if current_path != initial_path:
                raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="来源别名在打开窗口内改变目标")
            current_stat = _stat_version(current_path)
            if not _path_identity_matches(current_stat, before_handle):
                raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="当前路径未绑定到已打开句柄")
            if _clock_value(self._monotonic) - started_at > self._limits.max_seconds:
                raise DayReadError("READ_TIMEOUT", "BUDGET")

            def final_check() -> FileSourceIdentity:
                current_handle = _handle_version(stream, kernel32)
                if not _inside_roots(current_handle.final_path, roots):
                    raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="最终句柄路径离开保护根")
                if _resolved_roots(root_values) != roots:
                    raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="来源保护根别名在扫描期间改变目标")
                try:
                    current_final_path = _resolved(
                        requested_path,
                        missing_code="SOURCE_CHANGED",
                        label="来源文件",
                    )
                except DayReadError as error:
                    if error.code == "SOURCE_NOT_FOUND":
                        raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="扫描期间来源路径消失", cause=error)
                    raise
                if current_final_path != initial_path or current_handle.final_path != initial_path:
                    raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="扫描期间来源路径绑定发生变化")
                current_stat = _stat_version(current_final_path)
                if not _path_identity_matches(current_stat, current_handle):
                    raise _failure("SOURCE_CHANGED", "SOURCE_VERSION", detail="当前路径不再指向扫描句柄")
                if (
                    current_handle.volume_serial_number != before_handle.volume_serial_number
                    or current_handle.file_id != before_handle.file_id
                    or current_handle.size != before_handle.size
                    or current_handle.mtime_ns != before_handle.mtime_ns
                ):
                    raise _failure("SOURCE_CHANGED", "SOURCE_VERSION")
                return FileSourceIdentity(
                    requested_path=str(requested_path),
                    handle_path=str(current_handle.final_path),
                    volume_serial_number=current_handle.volume_serial_number,
                    file_id=current_handle.file_id,
                    size_before=before_handle.size,
                    size_after=current_handle.size,
                    mtime_ns_before=before_handle.mtime_ns,
                    mtime_ns_after=current_handle.mtime_ns,
                    requested_access="GENERIC_READ",
                    path_binding_verified=True,
                    handle_path_verified=True,
                )

            self._scan = DayRecordScan(
                stream,
                self._units,
                self._limits,
                monotonic=self._monotonic,
                final_check=final_check,
                started_at=started_at,
                read_started_at=started_wall,
            )
            self._file_scan = _FileDayScan(self._scan, stream)
            return self._file_scan
        except BaseException as original:
            if stream is not None and not stream.closed:
                try:
                    stream.close()
                except OSError as close_error:
                    add_note = getattr(original, "add_note", None)
                    if callable(add_note):
                        add_note(f"关闭失败的来源句柄时另有错误：{close_error}")
            raise

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if self._scan is not None and not self._scan.complete:
            self._scan.close()
        if self._file_scan is not None:
            try:
                self._file_scan._close_stream()
            except DayReadError as close_error:
                if exc is None:
                    raise
                add_note = getattr(exc, "add_note", None)
                if callable(add_note):
                    add_note(f"关闭来源句柄时另有错误：{close_error}")
        return False


def open_day_file(
    source_path: str | os.PathLike[str],
    source_roots: Iterable[str | os.PathLike[str]] | None,
    units: UnitContract,
    limits: ReadLimits,
    *,
    monotonic: Callable[[], float] = time.monotonic,
) -> _DayFileContext:
    """返回文件扫描上下文；只在验证实际只读句柄后开始读取。"""
    return _DayFileContext(source_path, source_roots, units, limits, monotonic)
