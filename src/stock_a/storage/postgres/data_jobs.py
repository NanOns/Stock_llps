"""初始化与手动日更任务的typed PostgreSQL状态。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any
from uuid import UUID

from . import source_raw


class DataJobPgError(Exception):
    """数据任务状态读写错误。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_TERMINAL_JOB_STATUSES = {
    "COMPLETED",
    "NO_CHANGE",
    "INCOMPLETE",
    "BLOCKED",
    "FAILED",
    "INTERRUPTED",
}
_COMPLETE_PROGRESS_STATUSES = {"COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"}


def read_raw_coverage(config: Mapping[str, Any], *, cutoff: date) -> dict[str, Any]:
    """仅读取截止日内闭合的Raw日期版本目录，不触碰截止日后的行情。"""
    if type(cutoff) is not date:
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_REQUIRED", "Raw覆盖核验必须绑定研究截止日"
        )
    offset = 0
    versions: list[dict[str, Any]] = []
    while True:
        page = source_raw.read_raw_daily_catalog(
            config, max_target_session=cutoff, offset=offset, limit=200
        )
        versions.extend(page["versions"])
        if page["next_offset"] is None:
            break
        offset = page["next_offset"]

    by_session: dict[str, list[dict[str, Any]]] = {}
    for version in versions:
        by_session.setdefault(version["session_date"], []).append(version)
    closed_by_session = {
        session: [item for item in choices if item["version_status"] == "CLOSED"]
        for session, choices in by_session.items()
    }
    sessions = sorted(
        session for session, choices in closed_by_session.items() if choices
    )
    ambiguous = sorted(
        session
        for session, choices in by_session.items()
        if len(closed_by_session.get(session, ())) != 1
        or len(choices) != len(closed_by_session.get(session, ()))
    )
    raw_versions_by_key = {
        (
            item["session_date"],
            int(item["source_package_id"]),
            int(item["source_capture_id"]),
        ): item
        for item in versions
        if item["version_status"] == "CLOSED"
    }
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT target_session,source_package_id,source_capture_id,
                      history_requested_session_count,profile_revision,count(*)
               FROM stock.source_symbol_price_profiles
               WHERE target_session<=%s
               GROUP BY target_session,source_package_id,source_capture_id,
                        history_requested_session_count,profile_revision
               ORDER BY target_session DESC,source_package_id,profile_revision""",
            (cutoff,),
        )
        profile_rows = cursor.fetchall()
        complete_profiles = []
        for (
            target_session,
            package_id,
            capture_id,
            history_count,
            _profile_revision,
            profile_row_count,
        ) in profile_rows:
            raw_version = raw_versions_by_key.get(
                (target_session.isoformat(), int(package_id), int(capture_id))
            )
            if (
                raw_version is not None
                and int(profile_row_count) == int(raw_version["expected_row_count"])
                and int(raw_version["row_count"])
                == int(raw_version["expected_row_count"])
            ):
                complete_profiles.append(
                    {
                        "target_session": target_session,
                        "source_package_id": int(package_id),
                        "history_session_count": int(history_count),
                    }
                )
        latest_profile_session = (
            max(item["target_session"] for item in complete_profiles)
            if complete_profiles
            else None
        )
        latest_profiles = [
            item
            for item in complete_profiles
            if item["target_session"] == latest_profile_session
        ]
        profile_windows = sorted(
            {item["history_session_count"] for item in latest_profiles}
        )
        profile_packages = sorted(
            {item["source_package_id"] for item in latest_profiles}
        )
        cursor.execute(
            """SELECT target_session,source_package_id
               FROM stock.data_jobs
               WHERE status IN ('COMPLETED','NO_CHANGE') AND target_session<=%s
               ORDER BY target_session DESC,completed_at DESC,job_id DESC LIMIT 1""",
            (cutoff,),
        )
        completed_job = cursor.fetchone()
        completed_job_session = completed_job[0] if completed_job else None
        completed_job_package = (
            int(completed_job[1])
            if completed_job and completed_job[1] is not None
            else None
        )
        if completed_job_session is not None and (
            latest_profile_session is None
            or completed_job_session >= latest_profile_session
        ):
            last_completed_session = completed_job_session
            last_completed_package = completed_job_package
        else:
            last_completed_session = latest_profile_session
            last_completed_package = (
                profile_packages[0] if len(profile_packages) == 1 else None
            )
        cursor.execute(
            """SELECT d1_history_session_count
               FROM stock.source_d1_structure_day_facts
               WHERE target_session=(
                   SELECT max(target_session) FROM stock.source_d1_structure_day_facts
                   WHERE target_session<=%s
               )
               GROUP BY d1_history_session_count
               ORDER BY d1_history_session_count""",
            (cutoff,),
        )
        d1_windows = [int(row[0]) for row in cursor.fetchall()]
    return {
        "cutoff": cutoff.isoformat(),
        "stored_session_count": len(sessions),
        "first_stored_session": sessions[0] if sessions else None,
        "last_stored_session": sessions[-1] if sessions else None,
        "closed_sessions": sessions,
        "closed_versions": [
            {
                "session_date": item["session_date"],
                "source_package_id": int(item["source_package_id"]),
                "source_capture_id": int(item["source_capture_id"]),
                "row_count": int(item["row_count"]),
                "expected_row_count": int(item["expected_row_count"]),
            }
            for item in versions
            if item["version_status"] == "CLOSED"
        ],
        "ambiguous_sessions": ambiguous,
        "last_completed_research_session": (
            last_completed_session.isoformat() if last_completed_session else None
        ),
        "last_completed_source_package_id": last_completed_package,
        "latest_complete_profile_session": (
            latest_profile_session.isoformat() if latest_profile_session else None
        ),
        "current_profile_history_session_count": (
            profile_windows[0] if len(profile_windows) == 1 else None
        ),
        "profile_history_window_ambiguous": len(profile_windows) > 1,
        "latest_profile_source_package_id": (
            profile_packages[0] if len(profile_packages) == 1 else None
        ),
        "profile_source_package_ambiguous": len(profile_packages) > 1,
        "current_d1_history_session_count": (
            d1_windows[0] if len(d1_windows) == 1 else None
        ),
        "d1_history_window_ambiguous": len(d1_windows) > 1,
        "source_coverage": "PG_CLOSED_RAW_ONLY",
        "source_request_performed": False,
        "database_write_attempted": False,
    }


def _job_row(cursor: Any, job_id: UUID) -> dict[str, Any] | None:
    cursor.execute(
        """SELECT job_id,request_key,mode,target_session,research_cutoff,
                  history_session_count,d0_output_session_count,d1_output_session_count,
                  calendar_id,calendar_revision,status,planned_units,completed_units,
                  source_package_id,created_at,started_at,updated_at,completed_at,
                  last_error_code,last_error_message
           FROM stock.data_jobs WHERE job_id=%s""",
        (job_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    names = [column.name for column in cursor.description]
    return dict(zip(names, row, strict=True))


def _progress_rows(cursor: Any, job_id: UUID) -> list[dict[str, Any]]:
    cursor.execute(
        """SELECT stage_order,stage_code,target_session,business_table,planned_units,
                  completed_units,expected_rows,verified_rows,inserted_rows,updated_rows,
                  status,quality_status,error_code,error_message,attempt_count,
                  started_at,updated_at,completed_at,verified_at
           FROM stock.data_job_progress
           WHERE job_id=%s
           ORDER BY stage_order,target_session,business_table""",
        (job_id,),
    )
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def read_job(
    config: Mapping[str, Any], *, job_id: UUID | None = None
) -> dict[str, Any] | None:
    """从唯一PG只读读取一项任务及其真实阶段/表进度。"""
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        if job_id is None:
            cursor.execute(
                """SELECT job_id FROM stock.data_jobs
                   ORDER BY created_at DESC,job_id DESC LIMIT 1"""
            )
            row = cursor.fetchone()
            if row is None:
                return None
            job_id = row[0]
        task = _job_row(cursor, job_id)
        if task is None:
            return None
        task["progress"] = _progress_rows(cursor, job_id)
    return task


def read_progress_unit(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_code: str,
    target_session: date,
    business_table: str,
) -> dict[str, Any] | None:
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT status,verified_rows,expected_rows,inserted_rows,updated_rows,
                      verified_at,error_code,error_message
               FROM stock.data_job_progress
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s""",
            (job_id, stage_code, target_session, business_table),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        names = [column.name for column in cursor.description]
        return dict(zip(names, row, strict=True))


def read_recent_jobs(
    config: Mapping[str, Any], *, limit: int = 8
) -> list[dict[str, Any]]:
    if type(limit) is not int or not 1 <= limit <= 20:
        raise DataJobPgError("DATA_JOB_PAGE_INVALID", "最近任务读取范围须为1至20")
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT job_id,request_key,mode,target_session,research_cutoff,
                      history_session_count,d0_output_session_count,d1_output_session_count,
                      calendar_id,calendar_revision,status,planned_units,completed_units,
                      source_package_id,created_at,started_at,updated_at,completed_at,
                      last_error_code,last_error_message
               FROM stock.data_jobs
               ORDER BY created_at DESC,job_id DESC LIMIT %s""",
            (limit,),
        )
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    return rows


def read_business_table_count(
    config: Mapping[str, Any],
    *,
    business_table: str,
    target_session: date,
    source_package_id: int,
    source_capture_id: int | None = None,
    profile_history_session_count: int | None = None,
    d1_history_session_count: int | None = None,
    profile_revision: str | None = None,
    producer_version: str | Sequence[str] | None = None,
    calendar_revision: str | None = None,
) -> int:
    """按固定表白名单和精确任务业务键独立读回已提交行数。"""
    queries = {
        "stock.source_packages": (
            "SELECT count(*) FROM stock.source_packages WHERE source_package_id=%s",
            lambda: (source_package_id,),
        ),
        "stock.source_captures": (
            """SELECT count(*) FROM stock.source_captures
               WHERE target_session=%s AND source_package_id=%s""",
            lambda: (target_session, source_package_id),
        ),
        "stock.source_raw_daily": (
            """SELECT count(*) FROM stock.source_raw_daily
               WHERE session_date=%s AND source_package_id=%s""",
            lambda: (target_session, source_package_id),
        ),
        "stock.source_symbol_price_profiles": (
            """SELECT count(*) FROM stock.source_symbol_price_profiles
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s AND profile_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
            ),
        ),
        "stock.source_d0_scenario_day_facts": (
            """SELECT count(*) FROM stock.source_d0_scenario_day_facts
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s
                 AND producer_version=ANY(%s::text[])
                 AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                [producer_version]
                if isinstance(producer_version, str)
                else list(producer_version or ()),
                calendar_revision,
            ),
        ),
        "stock.source_pool_seed_day_facts": (
            """SELECT count(*) FROM stock.source_pool_seed_day_facts
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s AND history_session_count=%s
                 AND profile_revision=%s AND producer_version=%s
                 AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_history_session_count,
                profile_revision,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_d0_launch_day_facts": (
            """SELECT count(*) FROM stock.source_d0_launch_day_facts
               WHERE target_session=%s AND source_package_id=%s
                 AND target_capture_id=%s AND producer_version=%s
                 AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_d1_structure_day_facts": (
            """SELECT count(*) FROM stock.source_d1_structure_day_facts
               WHERE target_session=%s AND source_package_id=%s
                 AND target_capture_id=%s AND d1_history_session_count=%s
                 AND producer_version=%s AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                d1_history_session_count,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_d1_material_anchor_day_facts": (
            """SELECT count(*)
               FROM stock.source_d1_material_anchor_day_facts a
               JOIN stock.source_d1_structure_day_facts d ON d.id=a.d1_structure_id
               WHERE d.target_session=%s AND d.source_package_id=%s
                 AND d.target_capture_id=%s AND d.d1_history_session_count=%s
                 AND d.producer_version=%s AND d.calendar_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                d1_history_session_count,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_research_enrollments": (
            """SELECT count(*)
               FROM stock.source_research_enrollments e
               JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
               WHERE d.target_session=%s AND d.source_package_id=%s
                 AND d.target_capture_id=%s AND d.producer_version=%s
                 AND d.calendar_revision=%s""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_d0_formal_admission_day_facts": (
            """SELECT count(*) FROM stock.source_d0_formal_admission_day_facts
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s
                 AND profile_revision IS NOT DISTINCT FROM %s
                 AND raw_producer_version=%s AND calendar_revision=%s
                 AND admission_contract='STOCK_A_D0_EVENT_ADMISSION_R1'""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_market_breadth_days": (
            """SELECT count(*) FROM stock.source_market_breadth_days
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s AND calendar_revision=%s
                 AND contract_version='MKT-02_RAW_POSITIVE_RETURN_STATE_DELTA_R1'""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                calendar_revision,
            ),
        ),
        "stock.source_market_breadth_symbols": (
            """SELECT count(*) FROM stock.source_market_breadth_symbols
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s AND calendar_revision=%s
                 AND contract_version='MKT-02_RAW_POSITIVE_RETURN_STATE_DELTA_R1'""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                calendar_revision,
            ),
        ),
        "stock.source_market_participation_days": (
            """SELECT count(*) FROM stock.source_market_participation_days
               WHERE target_session=%s AND source_package_id=%s
                 AND source_capture_id=%s AND profile_revision=%s
                 AND producer_version=%s
                 AND contract_version='MARKET_PARTICIPATION_R1'""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        ),
        "stock.source_market_participation_unknown_reasons": (
            """SELECT count(*)
               FROM stock.source_market_participation_unknown_reasons r
               JOIN stock.source_market_participation_days d
                 ON d.id=r.participation_day_id
               WHERE d.target_session=%s AND d.source_package_id=%s
                 AND d.source_capture_id=%s AND d.profile_revision=%s
                 AND d.producer_version=%s
                 AND d.contract_version='MARKET_PARTICIPATION_R1'""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        ),
    }
    selected = queries.get(business_table)
    if selected is None:
        raise DataJobPgError("DATA_JOB_TABLE_REJECTED", "业务表不在任务进度读回白名单")
    sql, parameters = selected
    if business_table in {
        "stock.source_symbol_price_profiles",
        "stock.source_pool_seed_day_facts",
    } and (source_capture_id is None or profile_history_session_count is None):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "表读回缺少来源捕获或画像窗口键"
        )
    if business_table == "stock.source_symbol_price_profiles" and not profile_revision:
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "画像表读回缺少Profile修订键"
        )
    if business_table in {
        "stock.source_d0_scenario_day_facts",
        "stock.source_pool_seed_day_facts",
        "stock.source_d0_launch_day_facts",
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_research_enrollments",
        "stock.source_d0_formal_admission_day_facts",
        "stock.source_market_participation_days",
        "stock.source_market_participation_unknown_reasons",
    } and (not producer_version or not calendar_revision):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "研究表读回缺少生产者或日历修订键"
        )
    if business_table == "stock.source_pool_seed_day_facts" and not profile_revision:
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "Seed表读回缺少Profile修订键"
        )
    if (
        business_table
        in {
            "stock.source_market_breadth_days",
            "stock.source_market_breadth_symbols",
        }
        and not calendar_revision
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "市场宽度表读回缺少日历修订键"
        )
    if business_table in {
        "stock.source_market_participation_days",
        "stock.source_market_participation_unknown_reasons",
    } and (not profile_revision or not producer_version):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "成交参与表读回缺少画像或生产者修订键"
        )
    if business_table != "stock.source_packages" and source_capture_id is None:
        raise DataJobPgError("DATA_JOB_READBACK_KEY_REQUIRED", "表读回缺少来源捕获键")
    if (
        business_table
        in {
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
        }
        and type(d1_history_session_count) is not int
    ):
        raise DataJobPgError("DATA_JOB_READBACK_KEY_REQUIRED", "D1表读回缺少计算窗口键")
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(sql, parameters())
        return int(cursor.fetchone()[0])


def set_job_source_package(
    config: Mapping[str, Any], *, job_id: UUID, source_package_id: int
) -> None:
    if type(source_package_id) is not int or source_package_id <= 0:
        raise DataJobPgError("DATA_JOB_SOURCE_PACKAGE_INVALID", "来源包ID必须是正整数")
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT source_package_id FROM stock.data_jobs WHERE job_id=%s FOR UPDATE",
            (job_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise DataJobPgError("DATA_JOB_NOT_FOUND", "任务主记录不存在")
        if row[0] is not None and int(row[0]) != source_package_id:
            raise DataJobPgError(
                "DATA_JOB_SOURCE_REVISION_CONFLICT",
                "同一任务输入已绑定不同来源包版本；拒绝覆盖并要求独立重试计划。",
            )
        cursor.execute(
            """UPDATE stock.data_jobs
               SET source_package_id=%s,updated_at=clock_timestamp()
               WHERE job_id=%s AND status='RUNNING'
                 AND (source_package_id IS NULL OR source_package_id=%s)""",
            (source_package_id, job_id, source_package_id),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError("DATA_JOB_STATE_CONFLICT", "来源包无法绑定到运行任务")


def recover_interrupted_jobs(config: Mapping[str, Any]) -> dict[str, int]:
    """服务重启时将残留RUNNING任务与单元标记为INTERRUPTED以便幂等重试。"""
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status='INTERRUPTED',error_code='PROCESS_RESTARTED',
                   error_message='服务进程在阶段完成前退出；重试将从已核验单元继续。',
                   updated_at=clock_timestamp()
               WHERE status='RUNNING'"""
        )
        progress_count = cursor.rowcount
        cursor.execute(
            """UPDATE stock.data_jobs
               SET status='INTERRUPTED',last_error_code='PROCESS_RESTARTED',
                   last_error_message='服务进程在任务完成前退出；重试将从已核验单元继续。',
                   updated_at=clock_timestamp(),completed_at=clock_timestamp()
               WHERE status='RUNNING'"""
        )
        job_count = cursor.rowcount
    return {"interrupted_jobs": job_count, "interrupted_units": progress_count}


def create_or_resume_job(
    config: Mapping[str, Any],
    *,
    job: Mapping[str, Any],
    progress: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """以同一业务输入复用任务记录，并拒绝第二个并行写入任务。"""
    required = {
        "job_id",
        "request_key",
        "mode",
        "target_session",
        "research_cutoff",
        "history_session_count",
        "d0_output_session_count",
        "d1_output_session_count",
        "calendar_id",
        "calendar_revision",
        "planned_units",
    }
    if not required.issubset(job):
        raise DataJobPgError("DATA_JOB_INPUT_INVALID", "任务输入缺少必要字段")
    job_id = job["job_id"]
    if not isinstance(job_id, UUID):
        raise DataJobPgError("DATA_JOB_ID_INVALID", "任务ID必须为UUID")
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT job_id,status FROM stock.data_jobs
               WHERE request_key=%s FOR UPDATE""",
            (job["request_key"],),
        )
        existing = cursor.fetchone()
        if existing is not None:
            existing_id, existing_status = existing
            if existing_status == "RUNNING":
                return {
                    "job_id": existing_id,
                    "status": "RUNNING",
                    "reused": True,
                    "active": True,
                }
            if existing_status in {"COMPLETED", "NO_CHANGE"}:
                return {
                    "job_id": existing_id,
                    "status": "ALREADY_COMPLETE",
                    "reused": True,
                }
            cursor.execute(
                """UPDATE stock.data_jobs
                   SET status='RUNNING',last_error_code=NULL,last_error_message=NULL,
                       started_at=clock_timestamp(),updated_at=clock_timestamp(),completed_at=NULL
                   WHERE job_id=%s""",
                (existing_id,),
            )
            cursor.execute(
                """UPDATE stock.data_job_progress
                   SET status='NOT_STARTED',completed_units=0,expected_rows=NULL,
                       verified_rows=NULL,inserted_rows=0,updated_rows=0,
                       quality_status=NULL,error_code=NULL,error_message=NULL,
                       updated_at=clock_timestamp(),completed_at=NULL
                   WHERE job_id=%s
                     AND status IN ('FAILED','INTERRUPTED','BLOCKED','INCOMPLETE')""",
                (existing_id,),
            )
            for unit in progress:
                if unit.get("status", "NOT_STARTED") != "NOT_STARTED":
                    continue
                cursor.execute(
                    """UPDATE stock.data_job_progress
                       SET status='NOT_STARTED',error_code=NULL,error_message=NULL,
                           updated_at=clock_timestamp(),completed_at=NULL
                       WHERE job_id=%s AND stage_code=%s AND target_session=%s
                         AND business_table=%s AND status='NOT_COMPUTED'""",
                    (
                        existing_id,
                        unit["stage_code"],
                        unit["target_session"],
                        unit["business_table"],
                    ),
                )
            cursor.execute(
                """UPDATE stock.data_jobs j
                   SET completed_units=(
                       SELECT coalesce(sum(p.completed_units),0)
                       FROM stock.data_job_progress p
                       WHERE p.job_id=j.job_id
                   ),updated_at=clock_timestamp()
                   WHERE j.job_id=%s""",
                (existing_id,),
            )
            cursor.execute(
                """SELECT count(*) FROM stock.data_job_progress
                   WHERE job_id=%s AND status IN ('RUNNING','NOT_STARTED')""",
                (existing_id,),
            )
            return {
                "job_id": existing_id,
                "status": "RUNNING",
                "reused": True,
                "active": False,
                "resumed": True,
                "remaining_units": int(cursor.fetchone()[0]),
            }

        cursor.execute(
            "SELECT job_id FROM stock.data_jobs WHERE status='RUNNING' LIMIT 1"
        )
        active = cursor.fetchone()
        if active is not None:
            raise DataJobPgError(
                "DATA_JOB_BUSY", "已有一项数据生产任务运行中；请先查看该任务进度。"
            )
        cursor.execute(
            """INSERT INTO stock.data_jobs
               (job_id,request_key,mode,target_session,research_cutoff,
                history_session_count,d0_output_session_count,d1_output_session_count,
                calendar_id,calendar_revision,status,planned_units,completed_units,
                created_at,started_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING',%s,0,
                       clock_timestamp(),clock_timestamp(),clock_timestamp())""",
            (
                job_id,
                job["request_key"],
                job["mode"],
                job["target_session"],
                job["research_cutoff"],
                job["history_session_count"],
                job["d0_output_session_count"],
                job["d1_output_session_count"],
                job["calendar_id"],
                job["calendar_revision"],
                job["planned_units"],
            ),
        )
        for unit in progress:
            unit_status = unit.get("status", "NOT_STARTED")
            unit_completed = int(
                unit_status in {"SKIPPED_BY_USER", "SKIPPED_NOT_IMPLEMENTED"}
            ) * int(unit.get("planned_units", 1))
            cursor.execute(
                """INSERT INTO stock.data_job_progress
                   (job_id,stage_order,stage_code,target_session,business_table,
                    planned_units,completed_units,status,error_code,error_message,
                    updated_at,completed_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp(),
                           CASE WHEN %s IN ('SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
                                THEN clock_timestamp() ELSE NULL END)
                   ON CONFLICT (job_id,stage_code,target_session,business_table) DO NOTHING""",
                (
                    job_id,
                    unit["stage_order"],
                    unit["stage_code"],
                    unit["target_session"],
                    unit["business_table"],
                    unit.get("planned_units", 1),
                    unit_completed,
                    unit_status,
                    unit.get("error_code"),
                    unit.get("error_message"),
                    unit_status,
                ),
            )
        cursor.execute(
            """UPDATE stock.data_jobs j
               SET completed_units=(
                   SELECT count(*) FROM stock.data_job_progress p
                   WHERE p.job_id=j.job_id
                     AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE',
                                      'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
               )
               WHERE j.job_id=%s""",
            (job_id,),
        )
    return {
        "job_id": job_id,
        "status": "RUNNING",
        "reused": False,
        "active": False,
    }


def mark_progress_running(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_code: str,
    target_session: date,
    business_table: str,
) -> bool:
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status='RUNNING',attempt_count=attempt_count+1,
                   started_at=coalesce(started_at,clock_timestamp()),
                   updated_at=clock_timestamp(),error_code=NULL,error_message=NULL
               WHERE job_id=%s AND stage_code=%s AND target_session=%s AND business_table=%s
                 AND status IN ('NOT_STARTED','FAILED','INTERRUPTED','INCOMPLETE')""",
            (job_id, stage_code, target_session, business_table),
        )
        if cursor.rowcount != 1:
            cursor.execute(
                """SELECT status FROM stock.data_job_progress
                   WHERE job_id=%s AND stage_code=%s AND target_session=%s
                     AND business_table=%s""",
                (job_id, stage_code, target_session, business_table),
            )
            row = cursor.fetchone()
            if row is not None and row[0] in _COMPLETE_PROGRESS_STATUSES:
                return False
            if row is not None and row[0] == "RUNNING":
                return True
            raise DataJobPgError(
                "DATA_JOB_PROGRESS_CONFLICT", "任务进度单元状态不允许开始或已被并行执行"
            )
    return True


def mark_progress_running_batch(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    units: Sequence[Mapping[str, Any]],
) -> None:
    """在一个状态事务中标记本阶段全部实际执行表为RUNNING。"""
    if not units:
        return
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        for unit in units:
            cursor.execute(
                """UPDATE stock.data_job_progress
                   SET status='RUNNING',attempt_count=attempt_count+1,
                       started_at=coalesce(started_at,clock_timestamp()),
                       updated_at=clock_timestamp(),error_code=NULL,error_message=NULL
                   WHERE job_id=%s AND stage_code=%s AND target_session=%s
                     AND business_table=%s
                     AND status IN ('NOT_STARTED','FAILED','INTERRUPTED','INCOMPLETE')""",
                (
                    job_id,
                    unit["stage_code"],
                    unit["target_session"],
                    unit["business_table"],
                ),
            )
            if cursor.rowcount == 1:
                continue
            cursor.execute(
                """SELECT status FROM stock.data_job_progress
                   WHERE job_id=%s AND stage_code=%s AND target_session=%s
                     AND business_table=%s""",
                (
                    job_id,
                    unit["stage_code"],
                    unit["target_session"],
                    unit["business_table"],
                ),
            )
            row = cursor.fetchone()
            if row is None or row[0] not in {
                "RUNNING",
                *_COMPLETE_PROGRESS_STATUSES,
            }:
                raise DataJobPgError(
                    "DATA_JOB_PROGRESS_CONFLICT",
                    "任务阶段批量开始时遇到不可执行的表进度状态。",
                )


def mark_progress_verified(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_code: str,
    target_session: date,
    business_table: str,
    verified_rows: int,
    inserted_rows: int,
    updated_rows: int,
    expected_rows: int | None,
    quality_status: str | None,
    no_change: bool = False,
) -> None:
    """仅供阶段生产事务完成后、独立查询确认行数的调用方使用。"""
    counts = (verified_rows, inserted_rows, updated_rows)
    if any(type(value) is not int or value < 0 for value in counts):
        raise DataJobPgError("DATA_JOB_ROW_COUNT_INVALID", "读回行数必须为非负整数")
    if expected_rows is not None and (
        type(expected_rows) is not int or expected_rows < 0
    ):
        raise DataJobPgError(
            "DATA_JOB_ROW_COUNT_INVALID", "预期行数必须为非负整数或未知"
        )
    status = (
        "NO_CHANGE"
        if no_change and inserted_rows == 0 and updated_rows == 0
        else "COMPLETED"
    )
    if expected_rows is not None and verified_rows != expected_rows:
        status = "INCOMPLETE"
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,planned_units=1,completed_units=1,
                   expected_rows=%s,verified_rows=%s,inserted_rows=%s,updated_rows=%s,
                   quality_status=%s,verified_at=clock_timestamp(),
                   completed_at=clock_timestamp(),updated_at=clock_timestamp()
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s AND status='RUNNING'""",
            (
                status,
                expected_rows,
                verified_rows,
                inserted_rows,
                updated_rows,
                quality_status,
                job_id,
                stage_code,
                target_session,
                business_table,
            ),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError(
                "DATA_JOB_PROGRESS_CONFLICT", "提交读回进度更新与任务单元状态不一致"
            )
        cursor.execute(
            """UPDATE stock.data_jobs j
               SET completed_units=(
                       SELECT count(*) FROM stock.data_job_progress p
                       WHERE p.job_id=j.job_id
                         AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE',
                                          'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
                   ),
                   updated_at=clock_timestamp()
               WHERE j.job_id=%s AND j.status='RUNNING'""",
            (job_id,),
        )


def mark_progress_error(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_code: str,
    target_session: date,
    business_table: str,
    status: str,
    error_code: str,
    error_message: str,
) -> None:
    if status not in {"FAILED", "INTERRUPTED", "BLOCKED", "NOT_COMPUTED", "INCOMPLETE"}:
        raise DataJobPgError("DATA_JOB_STATUS_INVALID", "任务进度错误状态不在允许范围")
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,error_code=%s,error_message=%s,
                   updated_at=clock_timestamp(),completed_at=NULL
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s""",
            (
                status,
                error_code,
                error_message,
                job_id,
                stage_code,
                target_session,
                business_table,
            ),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError(
                "DATA_JOB_PROGRESS_MISSING", "任务阶段/表进度记录不存在"
            )
        cursor.execute(
            """UPDATE stock.data_jobs
               SET status=CASE WHEN %s IN ('BLOCKED','INCOMPLETE') THEN 'INCOMPLETE' ELSE 'FAILED' END,
                   last_error_code=%s,last_error_message=%s,updated_at=clock_timestamp(),
                   completed_at=clock_timestamp()
               WHERE job_id=%s AND status='RUNNING'""",
            (status, error_code, error_message, job_id),
        )


def mark_progress_unavailable(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_code: str,
    target_session: date,
    business_table: str,
    status: str,
    error_code: str,
    error_message: str,
) -> None:
    """将有局部输入缺口的表单元留为未计算，不阻断其它独立阶段。"""
    if status not in {"NOT_COMPUTED", "BLOCKED", "INCOMPLETE"}:
        raise DataJobPgError("DATA_JOB_STATUS_INVALID", "局部未计算状态不在允许范围")
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,error_code=%s,error_message=%s,
                   updated_at=clock_timestamp(),completed_at=NULL
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s AND status IN ('NOT_STARTED','RUNNING')""",
            (
                status,
                error_code,
                error_message,
                job_id,
                stage_code,
                target_session,
                business_table,
            ),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError(
                "DATA_JOB_PROGRESS_MISSING", "局部未计算的任务进度记录不存在"
            )


def finish_job(config: Mapping[str, Any], *, job_id: UUID) -> dict[str, Any]:
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT count(*),count(*) FILTER (
                    WHERE status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE',
                                     'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')),
                    count(*) FILTER (WHERE status='NO_CHANGE')
               FROM stock.data_job_progress WHERE job_id=%s""",
            (job_id,),
        )
        total, complete, no_change = (int(value) for value in cursor.fetchone())
        status = "COMPLETED" if total == complete else "INCOMPLETE"
        if total > 0 and no_change == total:
            status = "NO_CHANGE"
        cursor.execute(
            """UPDATE stock.data_jobs
               SET status=%s,planned_units=%s,completed_units=%s,
                   updated_at=clock_timestamp(),completed_at=clock_timestamp()
               WHERE job_id=%s AND status='RUNNING'""",
            (status, total, complete, job_id),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError("DATA_JOB_STATE_CONFLICT", "任务无法完成状态转换")
    return {
        "job_id": job_id,
        "status": status,
        "planned_units": total,
        "completed_units": complete,
    }


def fail_job(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    status: str,
    error_code: str,
    error_message: str,
) -> None:
    if status not in _TERMINAL_JOB_STATUSES:
        raise DataJobPgError("DATA_JOB_STATUS_INVALID", "任务终态不在允许范围")
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,error_code=%s,error_message=%s,
                   updated_at=clock_timestamp(),completed_at=NULL
               WHERE job_id=%s AND status='RUNNING'""",
            (status, error_code, error_message, job_id),
        )
        cursor.execute(
            """UPDATE stock.data_jobs
               SET status=%s,last_error_code=%s,last_error_message=%s,
                   updated_at=clock_timestamp(),completed_at=clock_timestamp()
               WHERE job_id=%s AND status='RUNNING'""",
            (status, error_code, error_message, job_id),
        )
