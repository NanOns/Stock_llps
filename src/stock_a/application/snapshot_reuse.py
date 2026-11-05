"""以PG可见性快照验证有界的来源观察结果进程内复用。"""

from __future__ import annotations

from copy import deepcopy
import threading
import time
from typing import Any, Callable, Mapping

from stock_a.storage.postgres.snapshot_witness import SnapshotWitness


class SnapshotWitnessUnavailable(RuntimeError):
    """不能证明当前请求与缓存的数据库快照一致。"""


class SnapshotChanged(RuntimeError):
    """完整读取期间可见性快照改变，拒绝返回未经证明的组合。"""


class SnapshotReuse:
    """最多保存一个成功结果；见证连接不重连，断开或过期即废弃。"""

    def __init__(self, config: Mapping[str, Any], *, ttl_seconds: float = 60.0):
        self._config = config
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._witness: SnapshotWitness | None = None
        self._namespace: str | None = None
        self._entry: tuple[Any, str, float, Any] | None = None
        self._worker: threading.Thread | None = None
        self._closed = False

    def _clear(self) -> None:
        witness, self._witness = self._witness, None
        self._namespace = None
        self._entry = None
        if witness is not None:
            try:
                witness.close()
            except Exception:
                pass

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._clear()
            self._condition.notify_all()
            worker = self._worker
        if worker is not None and worker is not threading.current_thread():
            worker.join()

    def _expire_worker(self) -> None:
        with self._condition:
            while not self._closed:
                if self._entry is None:
                    self._condition.wait()
                    continue
                remaining = self._entry[2] - time.monotonic()
                if remaining <= 0:
                    self._clear()
                else:
                    self._condition.wait(remaining)

    def get_or_build(
        self,
        key: tuple[Any, ...],
        namespace: str,
        build: Callable[[], Any],
        resolved_identity: Callable[[Any], str],
    ) -> tuple[Any, bool]:
        """快照见证缺失或变化时失败关闭；完整身份才能保存结果。"""
        with self._lock:
            if self._closed:
                raise RuntimeError("来源观察缓存已关闭")
            now = time.monotonic()
            if self._entry is not None and now >= self._entry[2]:
                self._clear()
            if self._witness is not None and self._namespace != namespace:
                self._clear()
            try:
                if self._witness is None:
                    self._witness = SnapshotWitness(self._config, namespace)
                    self._namespace = namespace
                before = self._witness.sample()
            except Exception as error:
                self._clear()
                raise SnapshotWitnessUnavailable("来源观察快照见证不可用") from error
            if self._entry is not None:
                saved_key, saved_snapshot, expires_at, saved = self._entry
                if (
                    saved_key[0] == key
                    and saved_snapshot == before
                    and time.monotonic() < expires_at
                ):
                    try:
                        copied = deepcopy(saved)
                    except Exception as error:
                        self._clear()
                        raise SnapshotWitnessUnavailable(
                            "来源观察缓存复制失败"
                        ) from error
                    if time.monotonic() < expires_at:
                        return copied, True
                self._entry = None
                self._condition.notify_all()
            try:
                result = build()
            except BaseException:
                self._clear()
                raise
            try:
                after = self._witness.sample()
            except Exception as error:
                self._clear()
                raise SnapshotWitnessUnavailable("来源观察快照复核失败") from error
            if before != after:
                self._clear()
                raise SnapshotChanged("来源观察期间数据库快照改变")
            try:
                if before == after:
                    self._entry = (
                        (key, resolved_identity(result)),
                        after,
                        time.monotonic() + self._ttl,
                        deepcopy(result),
                    )
                    if self._worker is None:
                        self._worker = threading.Thread(
                            target=self._expire_worker,
                            name="stock-a-snapshot-expiry",
                            daemon=True,
                        )
                        self._worker.start()
                    self._condition.notify_all()
            except Exception:
                self._clear()
            return result, False
