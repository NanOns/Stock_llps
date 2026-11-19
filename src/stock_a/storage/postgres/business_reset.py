"""唯一Stock A业务事实重置的预览、范围校验与事务删除。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
import hashlib
import json
from typing import Any
import re
from uuid import UUID

from psycopg import sql

from . import source_raw


class BusinessFactsResetError(Exception):
    """业务事实重置范围或事务不能安全闭合。"""

    preserve_transaction_error = True

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


_PRESERVED_SYSTEM_TABLES = frozenset({"installation_identity", "migration_ledger"})
_BUSINESS_FACT_TABLES = frozenset(
    {
        "data_job_progress",
        "data_jobs",
        "source_baostock_daily",
        "source_baostock_unit_contracts",
        "source_captures",
        "source_adjusted_daily_prices",
        "source_corporate_action_events",
        "source_cohort_group_stats",
        "source_d0_formal_admission_day_facts",
        "source_d0_launch_day_facts",
        "source_d0_scenario_day_facts",
        "source_d1_material_anchor_day_facts",
        "source_d1_structure_day_facts",
        "source_fep_baseline_facts",
        "source_forward_path_facts",
        "source_market_breadth_days",
        "source_market_breadth_symbols",
        "source_market_participation_days",
        "source_market_participation_unknown_reasons",
        "source_packages",
        "source_pool_seed_day_facts",
        "source_raw_daily",
        "source_research_enrollments",
        "source_research_events",
        "source_research_observation_segment_days",
        "source_rps_pulse_t0_change_facts",
        "source_rule_evaluations",
        "source_s2_t1_cohort_stats",
        "source_sector_cross_period_pulse_facts",
        "source_sector_pulse_adjacent_transitions",
        "source_sector_pulse_cohort_group_facts",
        "source_sector_pulse_cohort_reason_facts",
        "source_sector_pulse_followup_facts",
        "source_symbol_display_names",
        "source_symbol_price_profiles",
        "source_t0_early_observation_facts",
        "tdx_sector_catalog",
        "tdx_sector_members",
        "tdx_sector_source_snapshots",
    }
)
_RESET_CONTRACT = "STOCK_A_BIZ10_BUSINESS_FACT_RESET_R1"


def _table_inventory(cursor: Any) -> tuple[set[str], dict[str, int]]:
    cursor.execute(
        """SELECT c.relname
           FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
           WHERE n.nspname='stock' AND c.relkind IN ('r','p','m')
           ORDER BY c.relname"""
    )
    actual = {str(row[0]) for row in cursor.fetchall()}
    unknown = actual - _PRESERVED_SYSTEM_TABLES - _BUSINESS_FACT_TABLES
    missing = _BUSINESS_FACT_TABLES - actual
    missing_system = _PRESERVED_SYSTEM_TABLES - actual
    if unknown:
        raise BusinessFactsResetError(
            "RESET_UNREGISTERED_BUSINESS_TABLE",
            "stock schema含未登记的数据表，不能推断其可安全清理："
            + ", ".join(sorted(unknown)),
        )
    if missing:
        raise BusinessFactsResetError(
            "RESET_REQUIRED_TABLE_MISSING",
            "stock schema缺少现行业务表，拒绝按不完整清单重置："
            + ", ".join(sorted(missing)),
        )
    if missing_system:
        raise BusinessFactsResetError(
            "RESET_REQUIRED_SYSTEM_TABLE_MISSING",
            "stock schema缺少必须保留的安装身份或迁移系统表："
            + ", ".join(sorted(missing_system)),
        )
    counts: dict[str, int] = {}
    for table in sorted(_BUSINESS_FACT_TABLES):
        cursor.execute(
            sql.SQL("SELECT count(*) FROM stock.{}").format(sql.Identifier(table))
        )
        counts[table] = int(cursor.fetchone()[0])
    return actual, counts


def _delete_order(cursor: Any) -> tuple[list[str], list[dict[str, str]]]:
    cursor.execute(
        """SELECT child_ns.nspname,child.relname,parent.relname,con.conname
           FROM pg_constraint con
           JOIN pg_class child ON child.oid=con.conrelid
           JOIN pg_namespace child_ns ON child_ns.oid=child.relnamespace
           JOIN pg_class parent ON parent.oid=con.confrelid
           JOIN pg_namespace parent_ns ON parent_ns.oid=parent.relnamespace
           WHERE con.contype='f' AND parent_ns.nspname='stock'
           ORDER BY child.relname,con.conname"""
    )
    rows = cursor.fetchall()
    edges: dict[str, set[str]] = {table: set() for table in _BUSINESS_FACT_TABLES}
    indegree = {table: 0 for table in _BUSINESS_FACT_TABLES}
    constraints: list[dict[str, str]] = []
    for child_schema_value, child_value, parent_value, constraint_value in rows:
        child_schema = str(child_schema_value)
        child = str(child_value)
        parent = str(parent_value)
        constraint = str(constraint_value)
        if parent not in _BUSINESS_FACT_TABLES:
            continue
        if child_schema != "stock" or child not in _BUSINESS_FACT_TABLES:
            raise BusinessFactsResetError(
                "RESET_PRESERVED_TABLE_DEPENDENCY",
                f"保留对象{child_schema}.{child}通过{constraint}引用业务事实stock.{parent}；拒绝删除。",
            )
        constraints.append({"child": child, "parent": parent, "constraint": constraint})
        if child == parent:
            continue
        if parent not in edges[child]:
            edges[child].add(parent)
            indegree[parent] += 1

    ready = sorted(table for table, degree in indegree.items() if degree == 0)
    ordered: list[str] = []
    while ready:
        child = ready.pop(0)
        ordered.append(child)
        for parent in sorted(edges[child]):
            indegree[parent] -= 1
            if indegree[parent] == 0:
                ready.append(parent)
                ready.sort()
    if len(ordered) != len(_BUSINESS_FACT_TABLES):
        raise BusinessFactsResetError(
            "RESET_FOREIGN_KEY_CYCLE",
            "业务表外键图不能形成显式子表优先的清理顺序。",
        )
    return ordered, constraints


def _fingerprint(
    *,
    counts: Mapping[str, int],
    order: list[str],
    constraints: list[dict[str, str]],
    target_sessions: tuple[str, ...],
    source_modules: tuple[str, ...],
    history_session_count: int,
    module_output_session_counts: Mapping[str, int],
) -> str:
    material = {
        "contract": _RESET_CONTRACT,
        "database": "stock_a",
        "schema": "stock",
        "counts": dict(sorted(counts.items())),
        "delete_order": order,
        "foreign_keys": constraints,
        "target_sessions": target_sessions,
        "source_modules": source_modules,
        "history_session_count": history_session_count,
        "module_output_session_counts": dict(
            sorted(module_output_session_counts.items())
        ),
        "preserved_system_tables": sorted(_PRESERVED_SYSTEM_TABLES),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def preview_business_fact_reset(
    config: Mapping[str, Any],
    *,
    target_sessions: tuple[str, ...],
    source_modules: tuple[str, ...],
    history_session_count: int,
    module_output_session_counts: Mapping[str, int],
) -> dict[str, Any]:
    """只读盘点本项目全部业务事实及保留的安装/迁移对象。"""
    if (
        not isinstance(target_sessions, tuple)
        or not target_sessions
        or any(not isinstance(value, str) for value in target_sessions)
        or tuple(sorted(set(target_sessions))) != target_sessions
        or not isinstance(source_modules, tuple)
        or type(history_session_count) is not int
        or history_session_count < 1
        or not isinstance(module_output_session_counts, Mapping)
    ):
        raise BusinessFactsResetError(
            "RESET_PREVIEW_INPUT_INVALID", "重置预览缺少动态目标会话或来源窗口。"
        )
    with source_raw._transaction(config, write=False, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        _actual, counts = _table_inventory(cursor)
        order, constraints = _delete_order(cursor)
        cursor.execute(
            "SELECT current_database(),current_schema(),session_user,current_user"
        )
        identity = cursor.fetchone()
        if identity != ("stock_a", "stock", "stock_a", "stock_a"):
            raise BusinessFactsResetError(
                "RESET_DATABASE_IDENTITY_REJECTED",
                "全库重置仅允许stock_a数据库、stock schema和stock_a业务账号。",
            )
    table_rows = [
        {"table": f"stock.{table}", "row_count": counts[table]} for table in order
    ]
    total = sum(counts.values())
    fingerprint = _fingerprint(
        counts=counts,
        order=order,
        constraints=constraints,
        target_sessions=target_sessions,
        source_modules=source_modules,
        history_session_count=history_session_count,
        module_output_session_counts=module_output_session_counts,
    )
    retained = [
        "stock.installation_identity",
        "stock.migration_ledger",
        "stock数据库、stock schema、表/索引/约束和账号角色",
        "唯一config/runtime.defaults.json运行配置",
        "当前正在执行的重初始化data_jobs记录及其进度",
    ]
    return {
        "status": "READY",
        "complete": True,
        "database_name": "stock_a",
        "schema": "stock",
        "affected_tables": table_rows,
        "affected_rows": total,
        "preserved_objects": retained,
        "retained_objects": retained,
        "target_sessions": list(target_sessions),
        "source_modules": list(source_modules),
        "history_session_count": history_session_count,
        "module_output_session_counts": dict(module_output_session_counts),
        "reset_preview_fingerprint": fingerprint,
        "foreign_key_constraints": constraints,
        "deletion_order": [f"stock.{name}" for name in order],
        "reset_execution_mode": "SINGLE_TRANSACTION_EXPLICIT_TRUNCATE_WITHOUT_CASCADE",
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def _delete_business_facts_in_cursor(
    cursor: Any,
    *,
    preserve_job_id: UUID,
    expected_counts: Mapping[str, int],
) -> dict[str, int]:
    _actual, counts = _table_inventory(cursor)
    _order, _constraints = _delete_order(cursor)
    cursor.execute(
        "SELECT count(*) FROM stock.data_jobs WHERE status='RUNNING' AND job_id<>%s",
        (preserve_job_id,),
    )
    if int(cursor.fetchone()[0]):
        raise BusinessFactsResetError(
            "RESET_OTHER_JOB_RUNNING", "另一个数据任务正在运行；未清理任何业务事实。"
        )
    cursor.execute(
        "SELECT count(*) FROM stock.data_jobs WHERE job_id=%s AND status='RUNNING'",
        (preserve_job_id,),
    )
    if int(cursor.fetchone()[0]) != 1:
        raise BusinessFactsResetError(
            "RESET_JOB_IDENTITY_MISSING",
            "当前重初始化任务记录不在PG中或已不处于运行态。",
        )

    normalized_counts = dict(counts)
    cursor.execute(
        "SELECT count(*) FROM stock.data_job_progress WHERE job_id=%s",
        (preserve_job_id,),
    )
    current_progress_rows = int(cursor.fetchone()[0])
    normalized_counts["data_jobs"] -= 1
    normalized_counts["data_job_progress"] -= current_progress_rows
    if normalized_counts != dict(expected_counts):
        raise BusinessFactsResetError(
            "RESET_PREVIEW_STALE",
            "预览后业务表行数发生变化；请重新读取完整影响清单。",
        )

    cursor.execute(
        "UPDATE stock.data_jobs SET source_package_id=NULL WHERE job_id=%s",
        (preserve_job_id,),
    )
    cursor.execute(
        """UPDATE stock.data_job_progress SET source_package_id=NULL,source_capture_id=NULL,
                   baostock_capture_id=NULL,profile_revision=NULL,producer_version=NULL,
                   profile_history_session_count=NULL,d1_history_session_count=NULL,
                   calendar_revision=NULL,industry_snapshot_id=NULL,concept_snapshot_id=NULL
           WHERE job_id=%s""",
        (preserve_job_id,),
    )
    cursor.execute(
        "CREATE TEMP TABLE stock_a_reset_current_job ON COMMIT DROP AS "
        "SELECT * FROM stock.data_jobs WHERE job_id=%s",
        (preserve_job_id,),
    )
    cursor.execute(
        "CREATE TEMP TABLE stock_a_reset_current_progress ON COMMIT DROP AS "
        "SELECT * FROM stock.data_job_progress WHERE job_id=%s",
        (preserve_job_id,),
    )

    # 显式列出本项目全部业务表而不使用CASCADE；所有Stock外键相关表都在同一语句中。
    truncate_tables = sql.SQL(",").join(
        sql.Identifier("stock", table) for table in sorted(_BUSINESS_FACT_TABLES)
    )
    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(truncate_tables))
    cursor.execute(
        "INSERT INTO stock.data_jobs SELECT * FROM pg_temp.stock_a_reset_current_job"
    )
    cursor.execute(
        "INSERT INTO stock.data_job_progress "
        "SELECT * FROM pg_temp.stock_a_reset_current_progress"
    )
    deleted = dict(counts)
    deleted["data_jobs"] -= 1
    deleted["data_job_progress"] -= current_progress_rows
    if deleted != dict(counts) | {
        "data_jobs": counts["data_jobs"] - 1,
        "data_job_progress": counts["data_job_progress"] - current_progress_rows,
    }:
        raise BusinessFactsResetError(
            "RESET_DELETE_COUNT_MISMATCH", "重置事务删除行数与预览不一致；事务回滚。"
        )
    for table in sorted(_BUSINESS_FACT_TABLES):
        cursor.execute(
            sql.SQL("SELECT count(*) FROM stock.{}").format(sql.Identifier(table))
        )
        verified_rows = int(cursor.fetchone()[0])
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status='CLEARED',completed_units=planned_units,
                   expected_rows=%s,verified_rows=%s,inserted_rows=0,updated_rows=0,
                   deleted_rows=%s,quality_status='BUSINESS_FACT_RESET',
                   error_code=NULL,error_message=NULL,
                   verified_at=clock_timestamp(),completed_at=clock_timestamp(),
                   updated_at=clock_timestamp()
               WHERE job_id=%s AND stage_code='BUSINESS_RESET'
                 AND business_table=%s""",
            (
                verified_rows,
                verified_rows,
                deleted[table],
                preserve_job_id,
                f"stock.{table}",
            ),
        )
        if cursor.rowcount != 1:
            raise BusinessFactsResetError(
                "RESET_PROGRESS_SCOPE_MISMATCH",
                f"业务表stock.{table}没有唯一重置进度单元；事务回滚。",
            )
    cursor.execute(
        """UPDATE stock.data_jobs j
           SET completed_units=(
               SELECT count(*) FROM stock.data_job_progress p
               WHERE p.job_id=j.job_id
                 AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
                                  'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
           ),updated_at=clock_timestamp()
           WHERE j.job_id=%s AND j.status='RUNNING'""",
        (preserve_job_id,),
    )
    return deleted


def clear_business_facts(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    expected_preview_fingerprint: str,
    expected_table_counts: Mapping[str, int],
    target_sessions: tuple[str, ...],
    source_modules: tuple[str, ...],
    history_session_count: int,
    module_output_session_counts: Mapping[str, int],
) -> dict[str, Any]:
    """在一笔可回滚事务中按真实外键子表优先清除全部业务事实。"""
    if (
        not isinstance(job_id, UUID)
        or not isinstance(expected_preview_fingerprint, str)
        or re.fullmatch(r"[0-9a-f]{64}", expected_preview_fingerprint) is None
    ):
        raise BusinessFactsResetError(
            "RESET_CONFIRMATION_INVALID", "重置任务身份或预览确认指纹无效。"
        )
    if not isinstance(expected_table_counts, Mapping) or set(expected_table_counts) != {
        f"stock.{table}" for table in _BUSINESS_FACT_TABLES
    }:
        raise BusinessFactsResetError(
            "RESET_PREVIEW_TABLES_INVALID", "重置预览没有完整登记所有业务事实表。"
        )
    if (
        not isinstance(target_sessions, tuple)
        or not target_sessions
        or any(not isinstance(value, str) for value in target_sessions)
        or tuple(sorted(set(target_sessions))) != target_sessions
        or not isinstance(source_modules, tuple)
        or any(not isinstance(value, str) for value in source_modules)
        or len(source_modules) != len(set(source_modules))
        or type(history_session_count) is not int
        or history_session_count < 1
        or not isinstance(module_output_session_counts, Mapping)
        or any(
            not isinstance(key, str) or type(value) is not int or value < 0
            for key, value in module_output_session_counts.items()
        )
    ):
        raise BusinessFactsResetError(
            "RESET_PREVIEW_INPUT_INVALID", "事务内重置指纹缺少预览绑定窗口。"
        )
    counts = {
        key.removeprefix("stock."): value
        for key, value in expected_table_counts.items()
    }
    if any(type(value) is not int or value < 0 for value in counts.values()):
        raise BusinessFactsResetError(
            "RESET_PREVIEW_COUNTS_INVALID", "重置预览表行数无效。"
        )
    with source_raw._transaction(config, write=True, statement_timeout_ms=30000) as (
        _connection,
        cursor,
        _scope,
    ):
        cursor.execute("LOCK TABLE stock.data_jobs IN ACCESS EXCLUSIVE MODE")
        cursor.execute(
            "SELECT count(*) FROM stock.data_jobs WHERE status='RUNNING' AND job_id<>%s",
            (job_id,),
        )
        if int(cursor.fetchone()[0]):
            raise BusinessFactsResetError(
                "RESET_OTHER_JOB_RUNNING",
                "另一个数据任务正在运行；未清理任何业务事实。",
            )
        lock_tables = sql.SQL(",").join(
            sql.Identifier("stock", table)
            for table in sorted(_BUSINESS_FACT_TABLES - {"data_jobs"})
        )
        cursor.execute(
            sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(lock_tables)
        )
        cursor.execute(
            """SELECT reset_preview_fingerprint
               FROM stock.data_jobs WHERE job_id=%s FOR UPDATE""",
            (job_id,),
        )
        job_row = cursor.fetchone()
        if job_row is None or job_row[0] != expected_preview_fingerprint:
            raise BusinessFactsResetError(
                "RESET_PREVIEW_JOB_BINDING_CHANGED",
                "重置任务中保存的确认指纹与本次预览不一致。",
            )
        _actual, current_counts = _table_inventory(cursor)
        order, constraints = _delete_order(cursor)
        current_fingerprint = _fingerprint(
            counts=current_counts,
            order=order,
            constraints=constraints,
            target_sessions=target_sessions,
            source_modules=source_modules,
            history_session_count=history_session_count,
            module_output_session_counts=module_output_session_counts,
        )
        if (
            current_counts != counts
            or current_fingerprint != expected_preview_fingerprint
        ):
            raise BusinessFactsResetError(
                "RESET_PREVIEW_STALE",
                "PG当前业务表行数或外键已不同于双重确认时的预览；未删除事实。",
            )
        deleted = _delete_business_facts_in_cursor(
            cursor, preserve_job_id=job_id, expected_counts=counts
        )
        cursor.execute(
            "SELECT count(*) FROM stock.data_jobs WHERE job_id=%s", (job_id,)
        )
        if int(cursor.fetchone()[0]) != 1:
            raise BusinessFactsResetError(
                "RESET_CURRENT_JOB_MISSING", "当前重初始化任务在删除后不可读回。"
            )
        cursor.execute(
            """SELECT count(*) FILTER (
                       WHERE stage_code='BUSINESS_RESET' AND status='CLEARED'
                   ), count(*) FILTER (WHERE stage_code='BUSINESS_RESET')
               FROM stock.data_job_progress WHERE job_id=%s""",
            (job_id,),
        )
        cleared_units, reset_units = (int(value) for value in cursor.fetchone())
        if reset_units != len(_BUSINESS_FACT_TABLES) or cleared_units != reset_units:
            raise BusinessFactsResetError(
                "RESET_CURRENT_PROGRESS_NOT_CLEARED",
                "当前重初始化事务没有原子读回全部逐表清理进度。",
            )
    return {
        "status": "CLEARED",
        "deleted_rows_by_table": {
            f"stock.{table}": rows for table, rows in deleted.items()
        },
        "deleted_rows": sum(deleted.values()),
        "preserved_current_job_id": str(job_id),
        "database_write_attempted": True,
        "source_io_attempted": False,
    }


__all__ = [
    "BusinessFactsResetError",
    "clear_business_facts",
    "preview_business_fact_reset",
]
