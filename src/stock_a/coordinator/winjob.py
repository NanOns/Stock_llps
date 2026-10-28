"""受控 Windows 进程树：创建时暂停、入 Job 后恢复，关闭 Job 时终止全树。"""
import ctypes, subprocess, time, threading
from ctypes import wintypes

k=ctypes.WinDLL('kernel32',use_last_error=True); nt=ctypes.WinDLL('ntdll',use_last_error=True)
class BASIC_LIMIT(ctypes.Structure):
    _fields_=[('PerProcessUserTimeLimit',ctypes.c_longlong),('PerJobUserTimeLimit',ctypes.c_longlong),('LimitFlags',wintypes.DWORD),('MinimumWorkingSetSize',ctypes.c_size_t),('MaximumWorkingSetSize',ctypes.c_size_t),('ActiveProcessLimit',wintypes.DWORD),('Affinity',ctypes.c_size_t),('PriorityClass',wintypes.DWORD),('SchedulingClass',wintypes.DWORD)]
class IO_COUNTERS(ctypes.Structure):
    _fields_=[(x,ctypes.c_ulonglong) for x in ('ReadOperationCount','WriteOperationCount','OtherOperationCount','ReadTransferCount','WriteTransferCount','OtherTransferCount')]
class EXTENDED_LIMIT(ctypes.Structure):
    _fields_=[('BasicLimitInformation',BASIC_LIMIT),('IoInfo',IO_COUNTERS),('ProcessMemoryLimit',ctypes.c_size_t),('JobMemoryLimit',ctypes.c_size_t),('PeakProcessMemoryUsed',ctypes.c_size_t),('PeakJobMemoryUsed',ctypes.c_size_t)]
class BASIC_ACCOUNTING(ctypes.Structure):
    _fields_=[('TotalUserTime',ctypes.c_longlong),('TotalKernelTime',ctypes.c_longlong),('ThisPeriodTotalUserTime',ctypes.c_longlong),('ThisPeriodTotalKernelTime',ctypes.c_longlong),('TotalPageFaultCount',wintypes.DWORD),('TotalProcesses',wintypes.DWORD),('ActiveProcesses',wintypes.DWORD),('TotalTerminatedProcesses',wintypes.DWORD)]
k.CreateJobObjectW.argtypes=[ctypes.c_void_p,wintypes.LPCWSTR]; k.CreateJobObjectW.restype=wintypes.HANDLE
k.SetInformationJobObject.argtypes=[wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD]; k.SetInformationJobObject.restype=wintypes.BOOL
k.AssignProcessToJobObject.argtypes=[wintypes.HANDLE,wintypes.HANDLE]; k.AssignProcessToJobObject.restype=wintypes.BOOL
k.TerminateJobObject.argtypes=[wintypes.HANDLE,wintypes.UINT]; k.TerminateJobObject.restype=wintypes.BOOL
k.QueryInformationJobObject.argtypes=[wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD,ctypes.c_void_p]; k.QueryInformationJobObject.restype=wintypes.BOOL
k.CloseHandle.argtypes=[wintypes.HANDLE]; k.CloseHandle.restype=wintypes.BOOL
nt.NtResumeProcess.argtypes=[wintypes.HANDLE]; nt.NtResumeProcess.restype=ctypes.c_long

def check(ok,what):
    if not ok: raise OSError(ctypes.get_last_error(),what)

def run(args, *, env, input_text=None, timeout=1800, cwd=None, on_output=None):
    handle=k.CreateJobObjectW(None,None); check(handle,'CreateJobObjectW')
    proc=None
    try:
        limits=EXTENDED_LIMIT(); limits.BasicLimitInformation.LimitFlags=0x2000
        check(k.SetInformationJobObject(handle,9,ctypes.byref(limits),ctypes.sizeof(limits)),'SetInformationJobObject')
        proc=subprocess.Popen(args,stdin=subprocess.PIPE if input_text is not None else subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',errors='replace',env=env,cwd=cwd,creationflags=0x4)
        try:
            check(k.AssignProcessToJobObject(handle,wintypes.HANDLE(int(proc._handle))),'AssignProcessToJobObject')
            if nt.NtResumeProcess(wintypes.HANDLE(int(proc._handle))) != 0: raise RuntimeError('恢复子进程失败')
        except BaseException:
            proc.kill(); proc.wait(timeout=10); raise
        if input_text is not None:
            try:
                proc.stdin.write(input_text); proc.stdin.close()
            except BrokenPipeError:
                pass
        captured={'stdout': [], 'stderr': []}
        failures=[]
        def drain(stream_name, pipe):
            try:
                for line in iter(pipe.readline, ''):
                    captured[stream_name].append(line)
                    if on_output:
                        on_output(stream_name, line)
            except BaseException as exc:
                failures.append(exc)
            finally:
                pipe.close()
        readers=[threading.Thread(target=drain,args=('stdout',proc.stdout),daemon=True),
                 threading.Thread(target=drain,args=('stderr',proc.stderr),daemon=True)]
        for reader in readers: reader.start()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            check(k.TerminateJobObject(handle,1),'TerminateJobObject timeout')
            proc.wait(timeout=10)
            for reader in readers: reader.join(timeout=10)
            raise TimeoutError('子进程超时，已终止整个 Job')
        for reader in readers: reader.join(timeout=10)
        if any(reader.is_alive() for reader in readers):
            check(k.TerminateJobObject(handle,1),'TerminateJobObject reader timeout')
            raise RuntimeError('子进程输出流未关闭')
        if failures: raise RuntimeError('读取子进程输出失败') from failures[0]
        out=''.join(captured['stdout']); err=''.join(captured['stderr'])
        # 主进程结束后终止并等待其全部后代，再交出写锁。
        check(k.TerminateJobObject(handle,1),'TerminateJobObject descendants')
        deadline=time.monotonic()+10
        while True:
            counts=BASIC_ACCOUNTING()
            check(k.QueryInformationJobObject(handle,1,ctypes.byref(counts),ctypes.sizeof(counts),None),'QueryInformationJobObject')
            if counts.ActiveProcesses==0: break
            if time.monotonic()>deadline: raise RuntimeError('子进程树未确认停止')
            time.sleep(.05)
        return subprocess.CompletedProcess(args,proc.returncode,out,err)
    finally:
        k.CloseHandle(handle)
