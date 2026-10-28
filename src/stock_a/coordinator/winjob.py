"""受控 Windows 进程树及有界标准流处理。"""
from __future__ import annotations
import ctypes
import subprocess
import threading
import time
from collections import deque
from ctypes import wintypes

k = ctypes.WinDLL('kernel32', use_last_error=True)
nt = ctypes.WinDLL('ntdll', use_last_error=True)

MAX_LINE_BYTES = 1024 * 1024
MAX_STDOUT_TAIL = 2 * 1024 * 1024
MAX_STDERR_TAIL = 256 * 1024
MAX_STDIN_BYTES = 4 * 1024 * 1024
STDIN_TIMEOUT = 60


class BASIC_LIMIT(ctypes.Structure):
    _fields_ = [('PerProcessUserTimeLimit', ctypes.c_longlong), ('PerJobUserTimeLimit', ctypes.c_longlong), ('LimitFlags', wintypes.DWORD), ('MinimumWorkingSetSize', ctypes.c_size_t), ('MaximumWorkingSetSize', ctypes.c_size_t), ('ActiveProcessLimit', wintypes.DWORD), ('Affinity', ctypes.c_size_t), ('PriorityClass', wintypes.DWORD), ('SchedulingClass', wintypes.DWORD)]


class IO_COUNTERS(ctypes.Structure):
    _fields_ = [(x, ctypes.c_ulonglong) for x in ('ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount', 'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]


class EXTENDED_LIMIT(ctypes.Structure):
    _fields_ = [('BasicLimitInformation', BASIC_LIMIT), ('IoInfo', IO_COUNTERS), ('ProcessMemoryLimit', ctypes.c_size_t), ('JobMemoryLimit', ctypes.c_size_t), ('PeakProcessMemoryUsed', ctypes.c_size_t), ('PeakJobMemoryUsed', ctypes.c_size_t)]


class BASIC_ACCOUNTING(ctypes.Structure):
    _fields_ = [('TotalUserTime', ctypes.c_longlong), ('TotalKernelTime', ctypes.c_longlong), ('ThisPeriodTotalUserTime', ctypes.c_longlong), ('ThisPeriodTotalKernelTime', ctypes.c_longlong), ('TotalPageFaultCount', wintypes.DWORD), ('TotalProcesses', wintypes.DWORD), ('ActiveProcesses', wintypes.DWORD), ('TotalTerminatedProcesses', wintypes.DWORD)]


k.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
k.CreateJobObjectW.restype = wintypes.HANDLE
k.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
k.SetInformationJobObject.restype = wintypes.BOOL
k.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
k.AssignProcessToJobObject.restype = wintypes.BOOL
k.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
k.TerminateJobObject.restype = wintypes.BOOL
k.QueryInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p]
k.QueryInformationJobObject.restype = wintypes.BOOL
k.CloseHandle.argtypes = [wintypes.HANDLE]
k.CloseHandle.restype = wintypes.BOOL
nt.NtResumeProcess.argtypes = [wintypes.HANDLE]
nt.NtResumeProcess.restype = ctypes.c_long


def check(ok, what):
    if not ok:
        raise OSError(ctypes.get_last_error(), what)


def _terminate_and_confirm(handle, proc, timeout=10):
    check(k.TerminateJobObject(handle, 1), 'TerminateJobObject')
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError('子进程未在终止期限内退出') from exc
    deadline = time.monotonic() + timeout
    while True:
        counts = BASIC_ACCOUNTING()
        check(k.QueryInformationJobObject(handle, 1, ctypes.byref(counts), ctypes.sizeof(counts), None), 'QueryInformationJobObject')
        if counts.ActiveProcesses == 0:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError('子进程树未确认停止')
        time.sleep(.05)


def _join_io_threads(readers, feeder=None, timeout=5):
    if feeder:
        feeder.join(timeout=timeout)
    for reader in readers:
        reader.join(timeout=timeout)
    if (feeder and feeder.is_alive()) or any(reader.is_alive() for reader in readers):
        raise RuntimeError('已终止进程树，但管道线程未退出')


def _append_tail(parts, size, data, limit):
    parts.append(data)
    size += len(data)
    while size > limit and parts:
        excess = size - limit
        first = parts[0]
        if len(first) <= excess:
            size -= len(parts.popleft())
        else:
            parts[0] = first[excess:]
            size -= excess
    return size


def run(args, *, env, input_text=None, timeout=1800, stdin_timeout=STDIN_TIMEOUT, cwd=None, on_output=None):
    """并行写入 stdin 与排空输出；管道、内存尾段和单行均有上限。"""
    input_bytes = input_text.encode('utf-8') if isinstance(input_text, str) else input_text
    if input_bytes is not None and len(input_bytes) > MAX_STDIN_BYTES:
        raise ValueError('子进程输入超过 4 MiB 上限')
    handle = k.CreateJobObjectW(None, None)
    check(handle, 'CreateJobObjectW')
    proc = None
    readers = []
    feeder = None
    failures = []
    stats = {'stdout_bytes': 0, 'stderr_bytes': 0, 'stdout_lines': 0, 'stderr_lines': 0, 'stdout_truncated': False, 'stderr_truncated': False, 'oversized_lines': 0}
    captured = {'stdout': (deque(), 0, MAX_STDOUT_TAIL), 'stderr': (deque(), 0, MAX_STDERR_TAIL)}
    try:
        limits = EXTENDED_LIMIT()
        limits.BasicLimitInformation.LimitFlags = 0x2000
        check(k.SetInformationJobObject(handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)), 'SetInformationJobObject')
        proc = subprocess.Popen(args, stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, cwd=cwd, creationflags=0x4)
        try:
            check(k.AssignProcessToJobObject(handle, wintypes.HANDLE(int(proc._handle))), 'AssignProcessToJobObject')
            if nt.NtResumeProcess(wintypes.HANDLE(int(proc._handle))) != 0:
                raise RuntimeError('恢复子进程失败')
        except BaseException:
            proc.kill()
            proc.wait(timeout=10)
            raise

        def drain(stream_name, pipe):
            parts, size, limit = captured[stream_name]
            pending = bytearray()
            dropping = False
            try:
                while True:
                    chunk = pipe.read(16384)
                    if not chunk:
                        break
                    stats[stream_name + '_bytes'] += len(chunk)
                    if stats[stream_name + '_bytes'] > limit:
                        stats[stream_name + '_truncated'] = True
                    size = _append_tail(parts, size, chunk, limit)
                    for b in chunk:
                        if dropping:
                            if b == 10:
                                dropping = False
                                stats['oversized_lines'] += 1
                                if on_output:
                                    on_output(stream_name, '[OVERSIZED_OUTPUT_LINE]\n')
                            continue
                        pending.append(b)
                        if len(pending) > MAX_LINE_BYTES:
                            pending.clear()
                            dropping = True
                            continue
                        if b == 10:
                            stats[stream_name + '_lines'] += 1
                            line = bytes(pending).decode('utf-8', errors='replace')
                            pending.clear()
                            if on_output:
                                on_output(stream_name, line)
                if pending and not dropping:
                    stats[stream_name + '_lines'] += 1
                    if on_output:
                        on_output(stream_name, bytes(pending).decode('utf-8', errors='replace'))
            except BaseException as exc:
                failures.append(exc)
            finally:
                pipe.close()

        readers = [threading.Thread(target=drain, args=('stdout', proc.stdout), daemon=True), threading.Thread(target=drain, args=('stderr', proc.stderr), daemon=True)]
        for reader in readers:
            reader.start()

        if input_bytes is not None:
            def feed():
                try:
                    for offset in range(0, len(input_bytes), 65536):
                        proc.stdin.write(input_bytes[offset:offset + 65536])
                    proc.stdin.flush()
                except (BrokenPipeError, OSError) as exc:
                    failures.append(exc)
                finally:
                    try:
                        proc.stdin.close()
                    except OSError:
                        pass
            feeder = threading.Thread(target=feed, daemon=True)
            feeder.start()

        deadline = time.monotonic() + timeout
        feed_deadline = time.monotonic() + min(timeout, stdin_timeout)
        while proc.poll() is None:
            if any(not isinstance(x, (BrokenPipeError, OSError)) for x in failures):
                _terminate_and_confirm(handle, proc)
                _join_io_threads(readers, feeder)
                raise RuntimeError('读取子进程输出失败') from failures[0]
            if feeder and feeder.is_alive() and time.monotonic() >= feed_deadline:
                _terminate_and_confirm(handle, proc)
                _join_io_threads(readers, feeder)
                raise TimeoutError('子进程未及时消费 stdin；已终止并核验整个 Job')
            if time.monotonic() >= deadline:
                _terminate_and_confirm(handle, proc)
                _join_io_threads(readers, feeder)
                raise TimeoutError('子进程超时；已终止并核验整个 Job')
            try:
                proc.wait(timeout=.05)
            except subprocess.TimeoutExpired:
                pass
        if feeder:
            feeder.join(timeout=2)
            if feeder.is_alive():
                _terminate_and_confirm(handle, proc)
                _join_io_threads(readers, feeder)
                raise TimeoutError('stdin 写入线程未退出；已终止并核验整个 Job')
        for reader in readers:
            reader.join(timeout=10)
        if any(reader.is_alive() for reader in readers):
            _terminate_and_confirm(handle, proc)
            raise RuntimeError('子进程输出流未关闭')
        if failures and not all(isinstance(x, (BrokenPipeError,)) for x in failures):
            raise RuntimeError('读取子进程输出失败') from failures[0]
        _terminate_and_confirm(handle, proc)
        out = b''.join(captured['stdout'][0]).decode('utf-8', errors='replace')
        err = b''.join(captured['stderr'][0]).decode('utf-8', errors='replace')
        completed = subprocess.CompletedProcess(args, proc.returncode, out, err)
        completed.output_stats = dict(stats)
        return completed
    finally:
        if proc is not None and proc.poll() is None:
            try:
                _terminate_and_confirm(handle, proc)
            except BaseException:
                pass
        k.CloseHandle(handle)
