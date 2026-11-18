"""初始化与手动日更任务的typed PostgreSQL状态。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
import hashlib
import json
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
_COMPLETE_PROGRESS_STATUSES = {
    "COMPLETED",
    "ALREADY_COMPLETE",
    "NO_CHANGE",
}

_MODULE_PROGRESS_TABLES = {
    "TDX_RAW": frozenset({"stock.source_captures", "stock.source_raw_daily"}),
    "CORE_PROFILE": frozenset({"stock.source_symbol_price_profiles"}),
    "MARKET_BREADTH": frozenset(
        {
            "stock.source_market_breadth_days",
            "stock.source_market_breadth_symbols",
        }
    ),
    "MARKET_PARTICIPATION": frozenset(
        {
            "stock.source_market_participation_days",
            "stock.source_market_participation_unknown_reasons",
        }
    ),
    "SEED": frozenset({"stock.source_pool_seed_day_facts"}),
    "D0": frozenset(
        {
            "stock.source_d0_scenario_day_facts",
            "stock.source_pool_seed_day_facts",
            "stock.source_d0_launch_day_facts",
            "stock.source_research_enrollments",
        }
    ),
    "D1": frozenset(
        {
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
        }
    ),
    "FOCUS_FORMAL": frozenset({"stock.source_d0_formal_admission_day_facts"}),
    "SECTOR_PULSE": frozenset({"stock.source_sector_cross_period_pulse_facts"}),
    "SECTOR_FOLLOWUP": frozenset(
        {
            "stock.source_sector_pulse_followup_facts",
            "stock.source_sector_pulse_cohort_group_facts",
            "stock.source_sector_pulse_cohort_reason_facts",
        }
    ),
}

_CLEANUP_MODULES = {
    "MARKET_BREADTH": {
        "parent_table": "stock.source_market_breadth_days",
        "child_table": "stock.source_market_breadth_symbols",
        "progress_tables": (
            "stock.source_market_breadth_symbols",
            "stock.source_market_breadth_days",
        ),
        "algorithm_column": "contract_version",
        "algorithm_label": "市场宽度合同版本",
    },
    "D1": {
        "parent_table": "stock.source_d1_structure_day_facts",
        "child_table": "stock.source_d1_material_anchor_day_facts",
        "progress_tables": (
            "stock.source_d1_material_anchor_day_facts",
            "stock.source_d1_structure_day_facts",
        ),
        "algorithm_column": "producer_version",
        "algorithm_label": "D1生产者版本",
    },
    "MARKET_PARTICIPATION": {
        "parent_table": "stock.source_market_participation_days",
        "child_table": "stock.source_market_participation_unknown_reasons",
        "progress_tables": (
            "stock.source_market_participation_unknown_reasons",
            "stock.source_market_participation_days",
        ),
        "algorithm_column": "producer_version",
        "algorithm_label": "市场成交参与生产者版本",
    },
}


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
        cursor.execute(
            """SELECT d.target_session,d.source_package_id,d.source_capture_id,
                      d.contract_version,count(*)::bigint
               FROM stock.source_market_breadth_days d
               JOIN stock.source_market_breadth_symbols s
                 ON s.breadth_day_id=d.id
               WHERE d.target_session<=%s
                 AND d.contract_version='MKT-02_RAW_POSITIVE_RETURN_STATE_DELTA_R1'
               GROUP BY d.target_session,d.source_package_id,d.source_capture_id,
                        d.contract_version
               ORDER BY d.target_session,d.source_package_id,d.source_capture_id""",
            (cutoff,),
        )
        breadth_rows = cursor.fetchall()
        breadth_completed_sessions = sorted(
            {
                row[0].isoformat()
                for row in breadth_rows
                if (
                    (
                        version := raw_versions_by_key.get(
                            (row[0].isoformat(), int(row[1]), int(row[2]))
                        )
                    )
                    is not None
                    and int(row[4]) == int(version["expected_row_count"])
                    and int(version["row_count"]) == int(version["expected_row_count"])
                )
            }
        )
    module_watermarks = read_module_completion_watermarks(config, cutoff=cutoff)
    research_modules = (
        "SEED",
        "D0",
        "D1",
        "FOCUS_FORMAL",
        "SECTOR_PULSE",
        "SECTOR_FOLLOWUP",
    )
    research_dates = [
        date.fromisoformat(day)
        for module in research_modules
        for day in module_watermarks["module_completed_sessions"].get(module, ())
    ]
    last_completed_session = max(research_dates) if research_dates else None
    latest_research_modules = [
        module
        for module in research_modules
        if last_completed_session is not None
        and module_watermarks["module_completion_watermarks"].get(module)
        == last_completed_session.isoformat()
    ]
    completed_packages = {
        module_watermarks["module_completion_source_packages"].get(module)
        for module in latest_research_modules
    }
    completed_packages.discard(None)
    last_completed_package = (
        next(iter(completed_packages)) if len(completed_packages) == 1 else None
    )
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
        "profile_completed_sessions": sorted(
            {item["target_session"].isoformat() for item in complete_profiles}
        ),
        "market_breadth_completed_sessions": breadth_completed_sessions,
        "profile_source_package_ambiguous": len(profile_packages) > 1,
        "current_d1_history_session_count": (
            d1_windows[0] if len(d1_windows) == 1 else None
        ),
        "d1_history_window_ambiguous": len(d1_windows) > 1,
        "source_coverage": "PG_CLOSED_RAW_ONLY",
        "source_request_performed": False,
        "database_write_attempted": False,
        **module_watermarks,
    }


def read_module_completion_watermarks(
    config: Mapping[str, Any], *, cutoff: date
) -> dict[str, Any]:
    """按任务实际提交并读回的每个模块业务表计算完成会话。"""
    if type(cutoff) is not date:
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_REQUIRED", "模块完成水位核验必须绑定研究截止日"
        )
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT p.job_id,p.target_session,p.business_table,p.status,
                      p.expected_rows,p.verified_rows,j.source_package_id,
                      j.request_key,c.capture_id
               FROM stock.data_job_progress p
               JOIN stock.data_jobs j ON j.job_id=p.job_id
               LEFT JOIN stock.source_captures c
                 ON c.target_session=p.target_session
                AND c.source_package_id=j.source_package_id
               WHERE p.target_session<=%s
                 AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                 AND p.completed_at IS NOT NULL
                 AND (p.expected_rows IS NULL OR p.verified_rows=p.expected_rows)
               ORDER BY p.job_id,p.target_session,p.business_table""",
            (cutoff,),
        )
        rows = cursor.fetchall()

    by_job_module_session: dict[tuple[UUID, str, date], dict[str, Any]] = {}
    for (
        job_id,
        target_session,
        business_table,
        _status,
        _expected,
        _verified,
        package_id,
        request_key,
        capture_id,
    ) in rows:
        for module, required_tables in _MODULE_PROGRESS_TABLES.items():
            if business_table not in required_tables:
                continue
            key = (job_id, module, target_session)
            state = by_job_module_session.setdefault(
                key,
                {
                    "tables": set(),
                    "source_package_id": package_id,
                    "request_keys": set(),
                    "source_capture_ids": set(),
                },
            )
            state["tables"].add(business_table)
            if request_key:
                state["request_keys"].add(str(request_key))
            if capture_id is not None:
                state["source_capture_ids"].add(int(capture_id))

    sessions_by_module: dict[str, set[date]] = {
        module: set() for module in _MODULE_PROGRESS_TABLES
    }
    package_by_module_session: dict[tuple[str, date], set[int]] = {}
    completed_units_by_module: dict[str, list[dict[str, Any]]] = {
        module: [] for module in _MODULE_PROGRESS_TABLES
    }
    for (_job_id, module, target_session), state in by_job_module_session.items():
        if not _MODULE_PROGRESS_TABLES[module].issubset(state["tables"]):
            continue
        sessions_by_module[module].add(target_session)
        package_id = state["source_package_id"]
        if package_id is not None:
            package_by_module_session.setdefault((module, target_session), set()).add(
                int(package_id)
            )
        completed_units_by_module[module].append(
            {
                "target_session": target_session.isoformat(),
                "source_package_id": int(package_id)
                if package_id is not None
                else None,
                "source_capture_ids": sorted(state["source_capture_ids"]),
                "request_keys": sorted(state["request_keys"]),
            }
        )

    completed_sessions = {
        module: sorted(day.isoformat() for day in sessions)
        for module, sessions in sessions_by_module.items()
    }
    watermarks = {
        module: max(sessions).isoformat() if sessions else None
        for module, sessions in sessions_by_module.items()
    }
    latest_packages = {}
    for module, watermark in watermarks.items():
        if watermark is None:
            latest_packages[module] = None
            continue
        package_ids = package_by_module_session.get(
            (module, date.fromisoformat(watermark)), set()
        )
        latest_packages[module] = (
            next(iter(package_ids)) if len(package_ids) == 1 else None
        )
    for units in completed_units_by_module.values():
        units.sort(
            key=lambda item: (
                item["target_session"],
                item["source_package_id"] or 0,
                tuple(item["source_capture_ids"]),
            )
        )
    return {
        "completed_sessions_by_module": completed_sessions,
        "latest_completed_by_module": watermarks,
        "completed_units_by_module": completed_units_by_module,
        "module_completed_sessions": completed_sessions,
        "module_completion_watermarks": watermarks,
        "module_completion_source_packages": latest_packages,
    }


def _cleanup_scope(
    module_code: str,
    target_sessions: Sequence[date],
    cutoff: date,
) -> tuple[dict[str, Any], tuple[date, ...]]:
    if not isinstance(module_code, str) or module_code not in _CLEANUP_MODULES:
        raise DataJobPgError(
            "DATA_JOB_CLEANUP_MODULE_REJECTED", "该派生模块没有登记安全清理实现"
        )
    if type(cutoff) is not date:
        raise DataJobPgError("DATA_JOB_CUTOFF_REQUIRED", "派生清理必须绑定研究截止日")
    if isinstance(target_sessions, (str, bytes)) or not isinstance(
        target_sessions, Sequence
    ):
        raise DataJobPgError(
            "DATA_JOB_CLEANUP_SESSION_INVALID", "清理目标必须是明确的官方会话列表"
        )
    sessions = tuple(target_sessions)
    if (
        not sessions
        or any(type(session) is not date or session > cutoff for session in sessions)
        or len(set(sessions)) != len(sessions)
    ):
        raise DataJobPgError(
            "DATA_JOB_CLEANUP_SESSION_INVALID",
            "清理目标日期重复、无效或超过研究截止日",
        )
    return _CLEANUP_MODULES[module_code], tuple(sorted(sessions))


def _cleanup_fk_blockers(cursor: Any, spec: Mapping[str, Any]) -> list[dict[str, str]]:
    parent_table = str(spec["parent_table"])
    child_table = str(spec["child_table"])
    expected_relations = {
        parent_table: {child_table},
        child_table: set(),
    }
    relations_by_parent: dict[str, set[str]] = {
        parent_table: set(),
        child_table: set(),
    }
    details: list[tuple[str, str, str, str]] = []
    for table in (parent_table, child_table):
        cursor.execute("SELECT to_regclass(%s)::text", (table,))
        if cursor.fetchone()[0] is None:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_TABLE_MISSING",
                f"派生清理所需正式业务表不存在：{table}",
            )
        cursor.execute(
            """SELECT parent.relname,child.relname,c.conname,
                      pg_get_constraintdef(c.oid)
               FROM pg_constraint c
               JOIN pg_class parent ON parent.oid=c.confrelid
               JOIN pg_namespace parent_schema ON parent_schema.oid=parent.relnamespace
               JOIN pg_class child ON child.oid=c.conrelid
               JOIN pg_namespace child_schema ON child_schema.oid=child.relnamespace
               WHERE c.contype='f' AND parent_schema.nspname='stock'
                 AND child_schema.nspname='stock' AND parent.relname=%s
               ORDER BY child.relname,c.conname""",
            (table.removeprefix("stock."),),
        )
        for (
            referenced_name,
            referencing_name,
            constraint_name,
            definition,
        ) in cursor.fetchall():
            referenced_table = f"stock.{referenced_name}"
            referencing_table = f"stock.{referencing_name}"
            relations_by_parent[referenced_table].add(referencing_table)
            details.append(
                (
                    referenced_table,
                    referencing_table,
                    str(constraint_name),
                    str(definition),
                )
            )

    blockers = []
    for table, expected in expected_relations.items():
        for referencing_table in sorted(relations_by_parent[table] - expected):
            matching = next(
                item
                for item in details
                if item[0] == table and item[1] == referencing_table
            )
            blockers.append(
                {
                    "referenced_table": table,
                    "dependent_table": referencing_table,
                    "constraint": matching[2],
                    "reason": "依赖表未纳入此模块的有界清理顺序",
                }
            )
    if child_table not in relations_by_parent[parent_table]:
        blockers.append(
            {
                "referenced_table": parent_table,
                "dependent_table": child_table,
                "constraint": "",
                "reason": "预期的父子外键关系与现存PG结构不一致",
            }
        )
    return blockers


def _cleanup_version_rows(
    cursor: Any,
    module_code: str,
    target_sessions: tuple[date, ...],
    cutoff: date,
) -> list[dict[str, Any]]:
    if module_code == "MARKET_BREADTH":
        cursor.execute(
            """SELECT d.target_session,d.source_package_id,d.source_capture_id,
                      d.calendar_revision,d.contract_version,count(DISTINCT d.id),
                      count(DISTINCT s.source_symbol)
               FROM stock.source_market_breadth_days d
               LEFT JOIN stock.source_market_breadth_symbols s
                 ON s.breadth_day_id=d.id
               WHERE d.target_session=ANY(%s) AND d.target_session<=%s
               GROUP BY d.target_session,d.source_package_id,d.source_capture_id,
                        d.calendar_revision,d.contract_version
               ORDER BY d.target_session,d.source_package_id,d.source_capture_id,
                        d.calendar_revision,d.contract_version""",
            (list(target_sessions), cutoff),
        )
        rows = cursor.fetchall()
        return [
            {
                "target_session": row[0],
                "source_version": {
                    "source_package_id": int(row[1]),
                    "source_capture_id": int(row[2]),
                },
                "algorithm_version": str(row[4]),
                "calendar_revision": str(row[3]),
                "parent_rows": int(row[5]),
                "child_rows": int(row[6]),
                "version_key": {
                    "source_package_id": int(row[1]),
                    "source_capture_id": int(row[2]),
                    "calendar_revision": str(row[3]),
                    "contract_version": str(row[4]),
                },
            }
            for row in rows
        ]
    if module_code == "D1":
        cursor.execute(
            """SELECT d.target_session,d.source_package_id,d.target_capture_id,
                      d.d1_history_session_count,d.producer_version,d.calendar_revision,
                      count(DISTINCT d.id),count(DISTINCT a.id)
               FROM stock.source_d1_structure_day_facts d
               LEFT JOIN stock.source_d1_material_anchor_day_facts a
                 ON a.d1_structure_id=d.id
               WHERE d.target_session=ANY(%s) AND d.target_session<=%s
               GROUP BY d.target_session,d.source_package_id,d.target_capture_id,
                        d.d1_history_session_count,d.producer_version,d.calendar_revision
               ORDER BY d.target_session,d.source_package_id,d.target_capture_id,
                        d.d1_history_session_count,d.producer_version,d.calendar_revision""",
            (list(target_sessions), cutoff),
        )
        rows = cursor.fetchall()
        return [
            {
                "target_session": row[0],
                "source_version": {
                    "source_package_id": int(row[1]),
                    "source_capture_id": int(row[2]),
                },
                "algorithm_version": str(row[4]),
                "calendar_revision": str(row[5]),
                "d1_history_session_count": int(row[3]),
                "parent_rows": int(row[6]),
                "child_rows": int(row[7]),
                "version_key": {
                    "source_package_id": int(row[1]),
                    "source_capture_id": int(row[2]),
                    "d1_history_session_count": int(row[3]),
                    "producer_version": str(row[4]),
                    "calendar_revision": str(row[5]),
                },
            }
            for row in rows
        ]
    if module_code == "MARKET_PARTICIPATION":
        cursor.execute(
            """SELECT d.target_session,d.source_package_id,d.source_capture_id,
                      d.profile_revision,d.producer_version,count(DISTINCT d.id),
                      count(*) FILTER (WHERE r.participation_day_id IS NOT NULL)
               FROM stock.source_market_participation_days d
               LEFT JOIN stock.source_market_participation_unknown_reasons r
                 ON r.participation_day_id=d.id
               WHERE d.target_session=ANY(%s) AND d.target_session<=%s
               GROUP BY d.target_session,d.source_package_id,d.source_capture_id,
                        d.profile_revision,d.producer_version
               ORDER BY d.target_session,d.source_package_id,d.source_capture_id,
                        d.profile_revision,d.producer_version""",
            (list(target_sessions), cutoff),
        )
        rows = cursor.fetchall()
        return [
            {
                "target_session": row[0],
                "source_version": {
                    "source_package_id": int(row[1]),
                    "source_capture_id": int(row[2]),
                },
                "algorithm_version": str(row[4]),
                "profile_revision": str(row[3]),
                "parent_rows": int(row[5]),
                "child_rows": int(row[6]),
                "version_key": {
                    "source_package_id": int(row[1]),
                    "source_capture_id": int(row[2]),
                    "profile_revision": str(row[3]),
                    "producer_version": str(row[4]),
                },
            }
            for row in rows
        ]
    raise DataJobPgError(
        "DATA_JOB_CLEANUP_MODULE_REJECTED", "模块没有对应的精确版本查询"
    )


def _cleanup_preview_in_cursor(
    cursor: Any,
    *,
    module_code: str,
    target_sessions: tuple[date, ...],
    cutoff: date,
) -> dict[str, Any]:
    spec, sessions = _cleanup_scope(module_code, target_sessions, cutoff)
    blockers = _cleanup_fk_blockers(cursor, spec)
    versions = _cleanup_version_rows(cursor, module_code, sessions, cutoff)
    version_scopes = {
        json.dumps(
            {
                key: value
                for key, value in version["version_key"].items()
                if key
                not in {
                    "source_capture_id",
                    "target_source_capture_id",
                }
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for version in versions
    }
    if len(version_scopes) > 1:
        blockers.append(
            {
                "referenced_table": spec["parent_table"],
                "dependent_table": module_code,
                "constraint": "",
                "reason": "目标日期范围包含多个来源包或算法修订；当前请求未显式选择单一版本",
            }
        )
    if module_code == "MARKET_BREADTH":
        for version in versions:
            source_version = version.get("source_version", {})
            cursor.execute(
                """SELECT count(*) FROM stock.source_market_participation_days
                   WHERE target_session=%s AND source_package_id=%s
                     AND source_capture_id=%s""",
                (
                    version["target_session"],
                    source_version.get("source_package_id"),
                    source_version.get("source_capture_id"),
                ),
            )
            if int(cursor.fetchone()[0]) > 0:
                blockers.append(
                    {
                        "referenced_table": spec["parent_table"],
                        "dependent_table": "stock.source_market_participation_days",
                        "constraint": "业务依赖：成交参与消费同日市场宽度与同源捕获",
                        "reason": "先清理该日成交参与结果，再重新预览市场宽度；未纳入下游清理",
                    }
                )
                break
    tables = [spec["child_table"], spec["parent_table"]]
    affected_rows = sum(
        int(version["parent_rows"]) + int(version["child_rows"]) for version in versions
    )
    source_versions = sorted(
        {
            json.dumps(version["source_version"], ensure_ascii=False, sort_keys=True)
            for version in versions
        }
    )
    algorithm_versions = sorted({version["algorithm_version"] for version in versions})
    dependency_rows = [
        {
            "table": spec["child_table"],
            "relation": "外键子表；先删子表，再删父表",
            "rows": sum(int(version["child_rows"]) for version in versions),
            "handled": True,
        },
        *[
            {
                "table": blocker["dependent_table"],
                "relation": blocker["constraint"],
                "rows": None,
                "handled": False,
                "reason": blocker["reason"],
            }
            for blocker in blockers
        ],
    ]
    key_document = {
        "module_code": module_code,
        "target_sessions": [session.isoformat() for session in sessions],
        "cutoff": cutoff.isoformat(),
        "versions": [
            {
                **version,
                "target_session": version["target_session"].isoformat(),
            }
            for version in versions
        ],
        "blockers": blockers,
    }
    preview_key = hashlib.sha256(
        json.dumps(
            key_document,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "complete": not blockers,
        "module_code": module_code,
        "module_label": module_code,
        "source_version": "; ".join(source_versions)
        if source_versions
        else "无已存结果",
        "algorithm_version": ", ".join(algorithm_versions)
        if algorithm_versions
        else "无已存结果",
        "target_sessions": [session.isoformat() for session in sessions],
        "affected_tables": tables,
        "affected_rows": affected_rows,
        "dependents": dependency_rows,
        "raw_rows_untouched": True,
        "versions": key_document["versions"],
        "preview_key": preview_key,
        "reason_codes": ["UNRESOLVED_FOREIGN_KEY_DEPENDENCY"] if blockers else [],
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def preview_derived_cleanup(
    config: Mapping[str, Any],
    module_code: str,
    target_sessions: Sequence[date],
    cutoff: date,
) -> dict[str, Any]:
    """预览一个已登记派生模块的准确版本、日期、行数与外键依赖。"""
    _spec, sessions = _cleanup_scope(module_code, target_sessions, cutoff)
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        preview = _cleanup_preview_in_cursor(
            cursor,
            module_code=module_code,
            target_sessions=sessions,
            cutoff=cutoff,
        )
    return preview


def clear_derived_module(
    config: Mapping[str, Any],
    module_code: str,
    target_sessions: Sequence[date],
    cutoff: date,
    *,
    expected_preview_key: str | None = None,
    current_job_id: UUID | None = None,
) -> dict[str, Any]:
    """在单一PG事务内按精确模块版本删除派生事实并失效旧进度。"""
    spec, sessions = _cleanup_scope(module_code, target_sessions, cutoff)
    if not isinstance(expected_preview_key, str) or len(expected_preview_key) != 64:
        raise DataJobPgError(
            "DATA_JOB_CLEANUP_PREVIEW_REQUIRED", "执行清理前必须提交完整预览键"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        preview = _cleanup_preview_in_cursor(
            cursor,
            module_code=module_code,
            target_sessions=sessions,
            cutoff=cutoff,
        )
        if not preview["complete"]:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_DEPENDENCY_UNRESOLVED",
                "现存外键依赖没有完整的有界清理顺序；未删除任何事实",
            )
        if preview["preview_key"] != expected_preview_key:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_PREVIEW_STALE",
                "预览后来源版本、事实行数或依赖发生变化；未删除任何事实，请重新预览",
            )
        cursor.execute(
            """SELECT count(*) FROM stock.data_job_progress
               WHERE target_session=ANY(%s) AND business_table=ANY(%s)
                 AND status='RUNNING' AND (%s::uuid IS NULL OR job_id<>%s)""",
            (
                list(sessions),
                list(spec["progress_tables"]),
                current_job_id,
                current_job_id,
            ),
        )
        if int(cursor.fetchone()[0]):
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_BUSY",
                "目标模块日期存在正在运行的生产单元；未删除任何事实",
            )

        deleted_rows: dict[str, int] = {table: 0 for table in spec["progress_tables"]}
        deleted_by_session = {
            session.isoformat(): {table: 0 for table in spec["progress_tables"]}
            for session in sessions
        }
        for version in preview["versions"]:
            target_session = date.fromisoformat(version["target_session"])
            key = version["version_key"]
            if module_code == "MARKET_BREADTH":
                cursor.execute(
                    """DELETE FROM stock.source_market_breadth_symbols s
                       USING stock.source_market_breadth_days d
                       WHERE s.breadth_day_id=d.id AND d.target_session=%s
                         AND d.source_package_id=%s AND d.source_capture_id=%s
                         AND d.calendar_revision=%s AND d.contract_version=%s""",
                    (
                        target_session,
                        key["source_package_id"],
                        key["source_capture_id"],
                        key["calendar_revision"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.source_market_breadth_symbols"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_market_breadth_symbols"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_market_breadth_days
                       WHERE target_session=%s AND source_package_id=%s
                         AND source_capture_id=%s AND calendar_revision=%s
                         AND contract_version=%s""",
                    (
                        target_session,
                        key["source_package_id"],
                        key["source_capture_id"],
                        key["calendar_revision"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.source_market_breadth_days"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_market_breadth_days"
                ] += cursor.rowcount
            elif module_code == "D1":
                cursor.execute(
                    """DELETE FROM stock.source_d1_material_anchor_day_facts a
                       USING stock.source_d1_structure_day_facts d
                       WHERE a.d1_structure_id=d.id AND d.target_session=%s
                         AND d.source_package_id=%s AND d.target_capture_id=%s
                         AND d.d1_history_session_count=%s
                         AND d.producer_version=%s AND d.calendar_revision=%s""",
                    (
                        target_session,
                        key["source_package_id"],
                        key["source_capture_id"],
                        key["d1_history_session_count"],
                        key["producer_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_d1_material_anchor_day_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_d1_material_anchor_day_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_d1_structure_day_facts
                       WHERE target_session=%s AND source_package_id=%s
                         AND target_capture_id=%s AND d1_history_session_count=%s
                         AND producer_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_package_id"],
                        key["source_capture_id"],
                        key["d1_history_session_count"],
                        key["producer_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_d1_structure_day_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_d1_structure_day_facts"
                ] += cursor.rowcount
            elif module_code == "MARKET_PARTICIPATION":
                cursor.execute(
                    """DELETE FROM stock.source_market_participation_unknown_reasons r
                       USING stock.source_market_participation_days d
                       WHERE r.participation_day_id=d.id AND d.target_session=%s
                         AND d.source_package_id=%s AND d.source_capture_id=%s
                         AND d.profile_revision=%s AND d.producer_version=%s
                         AND d.contract_version='MARKET_PARTICIPATION_R1'""",
                    (
                        target_session,
                        key["source_package_id"],
                        key["source_capture_id"],
                        key["profile_revision"],
                        key["producer_version"],
                    ),
                )
                deleted_rows["stock.source_market_participation_unknown_reasons"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_market_participation_unknown_reasons"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_market_participation_days
                       WHERE target_session=%s AND source_package_id=%s
                         AND source_capture_id=%s AND profile_revision=%s
                         AND producer_version=%s
                         AND contract_version='MARKET_PARTICIPATION_R1'""",
                    (
                        target_session,
                        key["source_package_id"],
                        key["source_capture_id"],
                        key["profile_revision"],
                        key["producer_version"],
                    ),
                )
                deleted_rows["stock.source_market_participation_days"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_market_participation_days"
                ] += cursor.rowcount

        expected_counts = {
            table: sum(
                int(
                    version["child_rows"]
                    if table == spec["child_table"]
                    else version["parent_rows"]
                )
                for version in preview["versions"]
            )
            for table in spec["progress_tables"]
        }
        if deleted_rows != expected_counts:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_ROWCOUNT_MISMATCH",
                "清理影响行数与预览不一致；事务回滚并保留原事实",
            )

        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                   verified_rows=NULL,inserted_rows=0,updated_rows=0,
                   quality_status=NULL,error_code='DERIVED_RESULT_CLEARED',
                error_message='对应派生结果已按模块、来源版本及日期范围清理；需重新生成。',
                   deleted_rows=0,
                   updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
               WHERE target_session=ANY(%s) AND business_table=ANY(%s)
                 AND status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                 AND (%s::uuid IS NULL OR job_id<>%s)
               RETURNING job_id""",
            (
                list(sessions),
                list(spec["progress_tables"]),
                current_job_id,
                current_job_id,
            ),
        )
        invalidated_rows = cursor.fetchall()
        invalidated_job_ids = {row[0] for row in invalidated_rows}
        for invalidated_job_id in invalidated_job_ids:
            cursor.execute(
                """UPDATE stock.data_jobs j
                   SET status='INCOMPLETE',
                       completed_units=(
                           SELECT count(*) FROM stock.data_job_progress p
                           WHERE p.job_id=j.job_id
                             AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
                                              'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
                       ),
                       last_error_code='DERIVED_RESULT_CLEARED',
                       last_error_message='部分派生结果已清理，旧任务完成状态失效。',
                       updated_at=clock_timestamp(),completed_at=clock_timestamp()
                   WHERE job_id=%s AND status IN ('COMPLETED','NO_CHANGE')""",
                (invalidated_job_id,),
            )

        result = {
            **preview,
            "status": "CLEARED" if preview["affected_rows"] else "NO_CHANGE",
            "deleted_rows_by_table": deleted_rows,
            "deleted_rows_by_session": deleted_by_session,
            "invalidated_progress_units": len(invalidated_rows),
            "database_write_attempted": True,
            "source_io_attempted": False,
        }
    return result


def _job_row(cursor: Any, job_id: UUID) -> dict[str, Any] | None:
    cursor.execute(
        """SELECT job_id,request_key,mode,target_session,research_cutoff,
                  history_session_count,d0_output_session_count,d1_output_session_count,
                  operation,selected_modules,module_output_session_counts,
                  source_version_fingerprint,
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
                  deleted_rows,
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
                      deleted_rows,
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
                      operation,selected_modules,module_output_session_counts,
                      source_version_fingerprint,
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
    sector_snapshot_ids: Mapping[str, int] | None = None,
    followup_target_sessions: Sequence[date] | None = None,
    research_cutoff: date | None = None,
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
        "stock.source_sector_cross_period_pulse_facts": (
            """SELECT count(*) FROM stock.source_sector_cross_period_pulse_facts p
               JOIN stock.tdx_sector_catalog c ON c.id=p.catalog_id
               WHERE p.target_session=%s AND p.source_package_id=%s
                 AND p.source_capture_id=%s AND p.profile_revision=%s
                 AND p.algorithm_contract=%s AND c.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                "SOURCE_CROSS_PERIOD_PULSE_R1",
                [
                    sector_snapshot_ids[kind]
                    for kind in ("INDUSTRY", "CONCEPT")
                    if isinstance(sector_snapshot_ids, Mapping)
                    and type(sector_snapshot_ids.get(kind)) is int
                ],
            ),
        ),
        "stock.source_sector_pulse_followup_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_followup_facts f
               JOIN stock.source_sector_cross_period_pulse_facts p
                 ON p.id=f.pulse_fact_id
               WHERE p.target_session=%s AND p.source_package_id=%s
                 AND p.source_capture_id=%s
                 AND p.profile_revision=%s
                 AND f.target_session=ANY(%s::date[])
                 AND f.calendar_revision=%s
                 AND f.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'""",
            lambda: (
                target_session,
                source_package_id,
                source_capture_id,
                profile_revision,
                list(followup_target_sessions or ()),
                calendar_revision,
            ),
        ),
        "stock.source_sector_pulse_cohort_group_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_cohort_group_facts g
               WHERE g.t0_session=%s AND g.research_cutoff=%s
                 AND g.t0_source_package_id=%s AND g.t0_source_capture_id=%s
                 AND g.t0_profile_revision=%s
                 AND g.pulse_algorithm_contract='SOURCE_CROSS_PERIOD_PULSE_R1'
                 AND g.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
                 AND g.calendar_revision=%s
                 AND g.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                target_session,
                research_cutoff,
                source_package_id,
                source_capture_id,
                profile_revision,
                calendar_revision,
                [
                    sector_snapshot_ids[kind]
                    for kind in ("INDUSTRY", "CONCEPT")
                    if isinstance(sector_snapshot_ids, Mapping)
                    and type(sector_snapshot_ids.get(kind)) is int
                ],
            ),
        ),
        "stock.source_sector_pulse_cohort_reason_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_cohort_reason_facts r
               JOIN stock.source_sector_pulse_cohort_group_facts g
                 ON g.id=r.cohort_group_fact_id
               WHERE g.t0_session=%s AND g.research_cutoff=%s
                 AND g.t0_source_package_id=%s AND g.t0_source_capture_id=%s
                 AND g.t0_profile_revision=%s
                 AND g.pulse_algorithm_contract='SOURCE_CROSS_PERIOD_PULSE_R1'
                 AND g.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
                 AND g.calendar_revision=%s
                 AND g.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                target_session,
                research_cutoff,
                source_package_id,
                source_capture_id,
                profile_revision,
                calendar_revision,
                [
                    sector_snapshot_ids[kind]
                    for kind in ("INDUSTRY", "CONCEPT")
                    if isinstance(sector_snapshot_ids, Mapping)
                    and type(sector_snapshot_ids.get(kind)) is int
                ],
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
    if business_table == "stock.source_sector_cross_period_pulse_facts" and (
        not profile_revision
        or not isinstance(sector_snapshot_ids, Mapping)
        or set(sector_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or any(
            type(value) is not int or value <= 0
            for value in sector_snapshot_ids.values()
        )
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "板块PULSE读回缺少画像修订或双快照键"
        )
    if business_table in {
        "stock.source_sector_pulse_followup_facts",
        "stock.source_sector_pulse_cohort_group_facts",
        "stock.source_sector_pulse_cohort_reason_facts",
    } and (
        not profile_revision
        or type(research_cutoff) is not date
        or not isinstance(sector_snapshot_ids, Mapping)
        or set(sector_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or any(
            type(value) is not int or value <= 0
            for value in sector_snapshot_ids.values()
        )
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "板块后继读回缺少画像修订或双快照键"
        )
    if business_table == "stock.source_sector_pulse_followup_facts" and (
        type(research_cutoff) is not date
        or not followup_target_sessions
        or any(
            type(item) is not date or item > research_cutoff
            for item in followup_target_sessions
        )
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "板块后继读回缺少截止内目标会话清单"
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
        "operation",
        "selected_modules",
        "module_output_session_counts",
        "source_version_fingerprint",
        "calendar_id",
        "calendar_revision",
        "planned_units",
    }
    if not required.issubset(job):
        raise DataJobPgError("DATA_JOB_INPUT_INVALID", "任务输入缺少必要字段")
    selected_modules = job["selected_modules"]
    output_counts = job["module_output_session_counts"]
    if (
        not isinstance(selected_modules, (list, tuple))
        or any(not isinstance(item, str) or not item for item in selected_modules)
        or len(selected_modules) != len(set(selected_modules))
        or not isinstance(output_counts, (list, tuple))
        or len(output_counts) != len(selected_modules)
        or any(
            type(item) is not int or not 1 <= item <= 32767 for item in output_counts
        )
        or job["operation"]
        not in {
            "FILL_MISSING",
            "RECOMPUTE",
            "CLEAR_DERIVED",
            "CLEAR_AND_REGENERATE",
        }
        or not isinstance(job["source_version_fingerprint"], str)
        or len(job["source_version_fingerprint"]) != 64
    ):
        raise DataJobPgError("DATA_JOB_INPUT_INVALID", "模块、输出范围或来源摘要无效")
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
                       verified_rows=NULL,inserted_rows=0,updated_rows=0,deleted_rows=0,
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
                operation,selected_modules,module_output_session_counts,
                source_version_fingerprint,calendar_id,calendar_revision,
                status,planned_units,completed_units,
                created_at,started_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING',%s,0,
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
                job["operation"],
                list(selected_modules),
                list(output_counts),
                job["source_version_fingerprint"],
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
                     AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
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
                   updated_at=clock_timestamp(),error_code=NULL,error_message=NULL,
                   deleted_rows=CASE WHEN coalesce(deleted_rows,0)>0 THEN deleted_rows ELSE 0 END
               WHERE job_id=%s AND stage_code=%s AND target_session=%s AND business_table=%s
                 AND status IN ('NOT_STARTED','FAILED','INTERRUPTED','INCOMPLETE','CLEARED')""",
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
                       updated_at=clock_timestamp(),error_code=NULL,error_message=NULL,
                       deleted_rows=CASE WHEN coalesce(deleted_rows,0)>0 THEN deleted_rows ELSE 0 END
                   WHERE job_id=%s AND stage_code=%s AND target_session=%s
                     AND business_table=%s
                     AND status IN ('NOT_STARTED','FAILED','INTERRUPTED','INCOMPLETE','CLEARED')""",
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
                         AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
                                          'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')
                   ),
                   updated_at=clock_timestamp()
               WHERE j.job_id=%s AND j.status='RUNNING'""",
            (job_id,),
        )


def mark_progress_cleared(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    stage_code: str,
    target_session: date,
    business_table: str,
    deleted_rows: int,
    regenerate: bool = False,
) -> None:
    """读回派生表目标日期为零行后，记录清理任务的真实删除数。"""
    cleanup_tables = {
        table
        for spec in _CLEANUP_MODULES.values()
        for table in (spec["parent_table"], spec["child_table"])
    }
    if (
        not isinstance(job_id, UUID)
        or not isinstance(stage_code, str)
        or not stage_code.strip()
        or type(target_session) is not date
        or business_table not in cleanup_tables
        or type(deleted_rows) is not int
        or deleted_rows < 0
        or type(regenerate) is not bool
    ):
        raise DataJobPgError(
            "DATA_JOB_CLEANUP_PROGRESS_INVALID", "清理进度读回键或删除行数无效"
        )
    count_queries = {
        "stock.source_market_breadth_days": (
            "SELECT count(*) FROM stock.source_market_breadth_days WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_market_breadth_symbols": (
            "SELECT count(*) FROM stock.source_market_breadth_symbols WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_d1_structure_day_facts": (
            "SELECT count(*) FROM stock.source_d1_structure_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_d1_material_anchor_day_facts": (
            """SELECT count(*) FROM stock.source_d1_material_anchor_day_facts a
               JOIN stock.source_d1_structure_day_facts d ON d.id=a.d1_structure_id
               WHERE d.target_session=%s AND d.target_session<=%s""",
        ),
        "stock.source_market_participation_days": (
            "SELECT count(*) FROM stock.source_market_participation_days WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_market_participation_unknown_reasons": (
            """SELECT count(*) FROM stock.source_market_participation_unknown_reasons r
               JOIN stock.source_market_participation_days d
                 ON d.id=r.participation_day_id
               WHERE d.target_session=%s AND d.target_session<=%s""",
        ),
    }
    status = "CLEARED" if deleted_rows > 0 or regenerate else "NO_CHANGE"
    quality_status = status
    error_code = "DERIVED_RESULT_CLEARED" if status == "CLEARED" else None
    error_message = (
        "目标日期派生表已独立读回零行；Raw来源行未修改。"
        if status == "CLEARED"
        else None
    )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT status,research_cutoff FROM stock.data_jobs WHERE job_id=%s FOR UPDATE",
            (job_id,),
        )
        job = cursor.fetchone()
        if job is None or job[0] != "RUNNING" or target_session > job[1]:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_PROGRESS_SCOPE_INVALID",
                "清理进度任务未运行或目标会话超过任务截止；未登记完成",
            )
        cursor.execute(count_queries[business_table][0], (target_session, job[1]))
        if int(cursor.fetchone()[0]) != 0:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_READBACK_NOT_EMPTY",
                "派生表仍有目标日结果；清理进度不能标记完成",
            )
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,
                   planned_units=1,completed_units=1,
                   expected_rows=0,verified_rows=0,inserted_rows=0,updated_rows=0,
                   deleted_rows=%s,
                   quality_status=%s,error_code=%s,error_message=%s,
                   verified_at=clock_timestamp(),completed_at=clock_timestamp(),
                   updated_at=clock_timestamp()
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s AND status IN ('NOT_STARTED','RUNNING')""",
            (
                status,
                deleted_rows,
                quality_status,
                error_code,
                error_message,
                job_id,
                stage_code,
                target_session,
                business_table,
            ),
        )
        if cursor.rowcount != 1:
            cursor.execute(
                """SELECT status,deleted_rows,verified_rows FROM stock.data_job_progress
                   WHERE job_id=%s AND stage_code=%s AND target_session=%s
                     AND business_table=%s""",
                (job_id, stage_code, target_session, business_table),
            )
            previous = cursor.fetchone()
            if previous != (status, deleted_rows, 0):
                raise DataJobPgError(
                    "DATA_JOB_CLEANUP_PROGRESS_CONFLICT",
                    "清理进度单元不存在或与PG读回行数冲突",
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
    if status not in {"NOT_COMPUTED", "NOT_DUE", "BLOCKED", "INCOMPLETE"}:
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
                    WHERE status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
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
