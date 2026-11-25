"""BaoStock 单机最简会话级咨询锁与自然日全局 API 工程配额计数（独立提交不随业务回滚）。"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import threading
from typing import Any

from stock_a.storage.postgres.source_raw import RawSourcePgError, _transaction

_CST = timezone(timedelta(hours=8))
_BAOSTOCK_SERIAL_LOCK_KEY = "STOCK_A_BAOSTOCK_GLOBAL_SERIAL"
DEFAULT_DAILY_ENGINEERING_BUDGET = 40000
DEFAULT_SINGLE_JOB_ENGINEERING_BUDGET = 15000
_JOB_QUOTA_STATE = threading.local()


@contextmanager
def baostock_job_quota_scope(
    *, single_job_limit: int = DEFAULT_SINGLE_JOB_ENGINEERING_BUDGET
) -> Iterator[dict[str, int]]:
    """为同一任务内多批次 BaoStock 调用提供整任务累计预算跟踪与上限保护。"""
    if type(single_job_limit) is not int or single_job_limit <= 0:
        raise ValueError("SINGLE_JOB_LIMIT_INVALID")
    previous = getattr(_JOB_QUOTA_STATE, "current", None)
    tracker = {
        "limit": single_job_limit,
        "used_calls": 0,
        "failed_calls": 0,
    }
    _JOB_QUOTA_STATE.current = tracker
    try:
        yield tracker
    finally:
        _JOB_QUOTA_STATE.current = previous


def _current_job_tracker() -> dict[str, int] | None:
    return getattr(_JOB_QUOTA_STATE, "current", None)


class BaoStockQuotaError(Exception):
    """BaoStock 工程日配额超限或并发锁冲突。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _beijing_today(now: datetime | None = None) -> date:
    current = now if now is not None else datetime.now(timezone.utc)
    return current.astimezone(_CST).date()


@contextmanager
def baostock_serial_lock(config: Mapping[str, Any]) -> Iterator[Callable[[], None]]:
    """获取 PG 会话级咨询锁以串行本机所有 BaoStock 在线入口；yield 锁存活检查回调，断线即报错停机。"""
    try:
        with _transaction(config, write=True, statement_timeout_ms=15000) as (
            conn,
            cursor,
            _,
        ):
            cursor.execute(
                "SELECT pg_try_advisory_lock(hashtext(%s))",
                (_BAOSTOCK_SERIAL_LOCK_KEY,),
            )
            acquired = bool(cursor.fetchone()[0])
            if not acquired:
                raise BaoStockQuotaError(
                    "BAOSTOCK_CONCURRENT_SESSION_BLOCKED",
                    "本机已有正在执行的 BaoStock 在线采集会话，拒绝并发连接",
                )
            conn.commit()

            def _verify_lock_alive() -> None:
                if getattr(conn, "closed", False):
                    raise BaoStockQuotaError(
                        "BAOSTOCK_SERIAL_LOCK_LOST",
                        "BaoStock 串行咨询锁连接已断开，立即停止后续请求",
                    )
                try:
                    cursor.execute("SELECT 1")
                    row = cursor.fetchone()
                    if not row or row[0] != 1:
                        raise BaoStockQuotaError(
                            "BAOSTOCK_SERIAL_LOCK_LOST",
                            "BaoStock 串行咨询锁连接健康检查失败，立即停止后续请求",
                        )
                    conn.commit()
                except BaoStockQuotaError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise BaoStockQuotaError(
                        "BAOSTOCK_SERIAL_LOCK_LOST",
                        f"BaoStock 串行咨询锁连接失联，立即停止后续请求: {exc}",
                    ) from exc

            try:
                yield _verify_lock_alive
            finally:
                try:
                    cursor.execute(
                        "SELECT pg_advisory_unlock(hashtext(%s))",
                        (_BAOSTOCK_SERIAL_LOCK_KEY,),
                    )
                    conn.commit()
                except Exception:
                    pass
    except RawSourcePgError as exc:
        if isinstance(exc.__cause__, BaoStockQuotaError):
            raise exc.__cause__ from None
        raise


def read_baostock_daily_quota_usage(
    config: Mapping[str, Any], *, quota_date: date | None = None
) -> dict[str, Any]:
    """只读查询指定北京时间自然日的 BaoStock 累计请求次数。"""
    target_day = quota_date if quota_date is not None else _beijing_today()
    with _transaction(config, write=False, statement_timeout_ms=10000) as (
        _,
        cursor,
        _,
    ):
        cursor.execute(
            """SELECT api_name, call_count, failed_count
               FROM stock.baostock_daily_api_quota_ledger
               WHERE quota_date = %s
               ORDER BY api_name""",
            (target_day,),
        )
        rows = cursor.fetchall()
    by_api = {r[0]: {"call_count": int(r[1]), "failed_count": int(r[2])} for r in rows}
    total_calls = sum(v["call_count"] for v in by_api.values())
    total_failed = sum(v["failed_count"] for v in by_api.values())
    return {
        "quota_date": target_day.isoformat(),
        "total_calls": total_calls,
        "total_failed": total_failed,
        "by_api": by_api,
    }


def preflight_baostock_api_quota(
    config: Mapping[str, Any],
    *,
    planned_calls: int,
    daily_limit: int = DEFAULT_DAILY_ENGINEERING_BUDGET,
    single_job_limit: int = DEFAULT_SINGLE_JOB_ENGINEERING_BUDGET,
    quota_date: date | None = None,
) -> dict[str, Any]:
    """任务启动前预检计划请求数是否超过内部工程日预算与单任务预算。"""
    if type(planned_calls) is not int or planned_calls < 0:
        raise ValueError("PLANNED_CALLS_INVALID")
    tracker = _current_job_tracker()
    effective_job_limit = tracker["limit"] if tracker is not None else single_job_limit
    job_used = tracker["used_calls"] if tracker is not None else 0
    if job_used + planned_calls > effective_job_limit:
        raise BaoStockQuotaError(
            "BAOSTOCK_SINGLE_JOB_BUDGET_EXCEEDED",
            f"整任务累计已用 {job_used} + 本批计划 {planned_calls} 超过内部工程单任务上限 {effective_job_limit}",
        )
    usage = read_baostock_daily_quota_usage(config, quota_date=quota_date)
    used = usage["total_calls"]
    remaining = max(0, daily_limit - used)
    if used + planned_calls > daily_limit:
        raise BaoStockQuotaError(
            "BAOSTOCK_DAILY_ENGINEERING_BUDGET_EXCEEDED",
            f"今日已用 {used} + 计划 {planned_calls} 超过内部工程日上限 {daily_limit}（剩余 {remaining}）",
        )
    return {
        "status": "ALLOWED",
        "quota_date": usage["quota_date"],
        "used_calls": used,
        "planned_calls": planned_calls,
        "daily_limit": daily_limit,
        "remaining_after_plan": daily_limit - used - planned_calls,
    }


def consume_baostock_api_quota(
    config: Mapping[str, Any],
    *,
    api_name: str,
    count: int = 1,
    failed: int = 0,
    daily_limit: int = DEFAULT_DAILY_ENGINEERING_BUDGET,
    quota_date: date | None = None,
) -> dict[str, Any]:
    """发送请求前在独立短事务内原子扣减日配额（不随上游业务事务回滚）。"""
    if not api_name or not isinstance(api_name, str):
        raise ValueError("API_NAME_REQUIRED")
    if type(count) is not int or count < 0 or type(failed) is not int or failed < 0:
        raise ValueError("QUOTA_COUNT_INVALID")
    target_day = quota_date if quota_date is not None else _beijing_today()
    day_lock_key = f"STOCK_A_BAOSTOCK_QUOTA_DAY:{target_day.isoformat()}"
    for attempt in range(5):
        try:
            with _transaction(config, write=True, statement_timeout_ms=10000) as (
                _,
                cursor,
                _,
            ):
                # 事务级咨询锁串行化同日预算 SUM 检查与 UPSERT 扣减，杜绝并发竞态突破上限
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))", (day_lock_key,)
                )
                cursor.execute(
                    """SELECT COALESCE(sum(call_count), 0)::bigint
                       FROM stock.baostock_daily_api_quota_ledger
                       WHERE quota_date = %s""",
                    (target_day,),
                )
                current_total = int(cursor.fetchone()[0])
                if current_total + count > daily_limit:
                    raise BaoStockQuotaError(
                        "BAOSTOCK_DAILY_ENGINEERING_BUDGET_EXCEEDED",
                        f"今日累计 {current_total} + 本次 {count} 超过内部工程日上限 {daily_limit}",
                    )
                cursor.execute(
                    """INSERT INTO stock.baostock_daily_api_quota_ledger (
                           quota_date, api_name, call_count, failed_count, updated_at
                       ) VALUES (%s, %s, %s, %s, now())
                       ON CONFLICT (quota_date, api_name) DO UPDATE SET
                           call_count = stock.baostock_daily_api_quota_ledger.call_count + EXCLUDED.call_count,
                           failed_count = stock.baostock_daily_api_quota_ledger.failed_count + EXCLUDED.failed_count,
                           updated_at = now()
                       RETURNING call_count, failed_count""",
                    (target_day, api_name, count, failed),
                )
                new_calls, new_failed = cursor.fetchone()
            return {
                "quota_date": target_day.isoformat(),
                "api_name": api_name,
                "api_call_count": int(new_calls),
                "api_failed_count": int(new_failed),
                "daily_total_calls": current_total + count,
            }
        except RawSourcePgError as exc:
            if isinstance(exc.__cause__, BaoStockQuotaError):
                raise exc.__cause__ from None
            if getattr(exc.__cause__, "sqlstate", None) == "40001" and attempt < 4:
                continue
            raise
    raise AssertionError("不可达的配额扣减重试状态")


def settle_baostock_api_quota(
    config: Mapping[str, Any],
    *,
    api_name: str,
    planned_calls: int,
    actual_calls: int,
    failed_calls: int = 0,
    quota_date: date | None = None,
) -> dict[str, Any]:
    """在请求返回后于独立短事务内按实际发送次数与失败次数结算事前预扣差额（不回滚已发调用）。"""
    if not api_name or not isinstance(api_name, str):
        raise ValueError("API_NAME_REQUIRED")
    if (
        type(planned_calls) is not int
        or planned_calls < 0
        or type(actual_calls) is not int
        or actual_calls < 0
        or type(failed_calls) is not int
        or failed_calls < 0
    ):
        raise ValueError("QUOTA_SETTLEMENT_COUNT_INVALID")
    delta_calls = actual_calls - planned_calls
    tracker = _current_job_tracker()
    if tracker is not None:
        tracker["used_calls"] = max(0, tracker["used_calls"] + actual_calls)
        tracker["failed_calls"] += failed_calls
    target_day = quota_date if quota_date is not None else _beijing_today()
    day_lock_key = f"STOCK_A_BAOSTOCK_QUOTA_DAY:{target_day.isoformat()}"
    for attempt in range(5):
        try:
            with _transaction(config, write=True, statement_timeout_ms=10000) as (
                _,
                cursor,
                _,
            ):
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))", (day_lock_key,)
                )
                cursor.execute(
                    """INSERT INTO stock.baostock_daily_api_quota_ledger (
                           quota_date, api_name, call_count, failed_count, updated_at
                       ) VALUES (%s, %s, %s, %s, now())
                       ON CONFLICT (quota_date, api_name) DO UPDATE SET
                           call_count = GREATEST(0, stock.baostock_daily_api_quota_ledger.call_count + %s),
                           failed_count = stock.baostock_daily_api_quota_ledger.failed_count + %s,
                           updated_at = now()
                       RETURNING call_count, failed_count""",
                    (
                        target_day,
                        api_name,
                        actual_calls,
                        failed_calls,
                        delta_calls,
                        failed_calls,
                    ),
                )
                new_calls, new_failed = cursor.fetchone()
            return {
                "quota_date": target_day.isoformat(),
                "api_name": api_name,
                "planned_calls": planned_calls,
                "actual_calls": actual_calls,
                "failed_calls": failed_calls,
                "delta_calls": delta_calls,
                "api_call_count": int(new_calls),
                "api_failed_count": int(new_failed),
            }
        except RawSourcePgError as exc:
            if getattr(exc.__cause__, "sqlstate", None) == "40001" and attempt < 4:
                continue
            raise
    raise AssertionError("不可达的配额结算重试状态")
