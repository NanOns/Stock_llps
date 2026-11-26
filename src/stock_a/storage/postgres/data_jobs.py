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
_SECTOR_SNAPSHOT_PROGRESS_TABLES = frozenset(
    {
        "stock.tdx_sector_source_snapshots",
        "stock.tdx_sector_catalog",
        "stock.tdx_sector_members",
    }
)

_MODULE_PROGRESS_TABLES = {
    "BAOSTOCK_PRICES": frozenset({"stock.source_raw_daily"}),
    "BAOSTOCK_UNIVERSE": frozenset(
        {
            "stock.source_baostock_stock_basic",
            "stock.source_baostock_session_directory",
        }
    ),
    "BAOSTOCK_MARKET_INDICES": frozenset({"stock.source_market_index_daily"}),
    "TDX_SECTOR_MEMBERS": frozenset(
        {
            "stock.tdx_sector_source_snapshots",
            "stock.tdx_sector_catalog",
            "stock.tdx_sector_members",
        }
    ),
    "CORE_PROFILE": frozenset({"stock.source_symbol_price_profiles"}),
    "MARKET_BREADTH": frozenset(
        {
            "stock.source_market_breadth_days",
            "stock.source_market_breadth_symbols",
            "stock.source_market_breadth_change_days",
            "stock.r03_formal_market_breadth_facts",
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
    "FOCUS_RESEARCH_CONTEXT": frozenset(
        {
            "stock.source_rule_evaluations",
            "stock.source_t0_early_observation_facts",
        }
    ),
    "SECTOR_PULSE": frozenset(
        {
            "stock.source_sector_cross_period_pulse_facts",
        }
    ),
    "SECTOR_TRANSITION": frozenset({"stock.source_sector_pulse_adjacent_transitions"}),
    "SECTOR_FOLLOWUP": frozenset(
        {
            "stock.source_sector_pulse_followup_facts",
            "stock.source_sector_pulse_cohort_group_facts",
            "stock.source_sector_pulse_cohort_reason_facts",
        }
    ),
    "R03_SECTOR_NATIVE": frozenset({"stock.r03_sector_native_day_facts"}),
    "R03_SECTOR_ROTATION": frozenset({"stock.r03_sector_rotation_board_day_facts"}),
    "R03_SECTOR_OVERLAP": frozenset({"stock.r03_sector_overlap_clusters"}),
    "R03_SECTOR_LOO": frozenset({"stock.r03_sector_loo_support_facts"}),
    "PRICE_LIMITS": frozenset({"stock.source_price_limit_day_facts"}),
    "LIMIT_UP_LADDER": frozenset({"stock.source_limit_up_streak_day_facts"}),
    "CROSS_SECTION": frozenset(
        {
            "stock.source_cross_section_day_facts",
            "stock.source_cross_section_endpoints",
        }
    ),
    "MARKET_INDICES_ENV": frozenset(
        {
            "stock.source_market_index_day_facts",
            "stock.source_market_environment_day_facts",
        }
    ),
    "FORWARD_PATH": frozenset({"stock.source_forward_path_facts"}),
    "COHORT_STATS": frozenset({"stock.source_cohort_group_stats"}),
    "FEP_BASELINE": frozenset({"stock.source_fep_baseline_facts"}),
    "OBSERVATION_SEGMENTS": frozenset(
        {"stock.source_research_observation_segment_days"}
    ),
}

_CLEANUP_MODULES = {
    "CORE_PROFILE": {
        "parent_table": "stock.source_symbol_price_profiles",
        "child_table": None,
        "cleanup_tables": ("stock.source_symbol_price_profiles",),
        "progress_tables": ("stock.source_symbol_price_profiles",),
        "algorithm_column": "producer_version",
        "algorithm_label": "Core/Profile生产者版本",
    },
    "MARKET_BREADTH": {
        "parent_table": "stock.source_market_breadth_days",
        "child_table": "stock.source_market_breadth_symbols",
        "cleanup_tables": (
            "stock.source_market_breadth_change_days",
            "stock.source_market_breadth_symbols",
            "stock.source_market_breadth_days",
            "stock.r03_formal_market_breadth_facts",
        ),
        "progress_tables": (
            "stock.source_market_breadth_change_days",
            "stock.source_market_breadth_symbols",
            "stock.source_market_breadth_days",
            "stock.r03_formal_market_breadth_facts",
        ),
        "algorithm_column": "contract_version",
        "algorithm_label": "市场宽度合同版本",
    },
    "D1": {
        "parent_table": "stock.source_d1_structure_day_facts",
        "child_table": "stock.source_d1_material_anchor_day_facts",
        "cleanup_tables": (
            "stock.source_research_enrollments",
            "stock.source_rule_evaluations",
            "stock.source_d1_material_anchor_day_facts",
            "stock.source_d1_structure_day_facts",
        ),
        "progress_tables": (
            "stock.source_research_enrollments",
            "stock.source_rule_evaluations",
            "stock.source_d1_material_anchor_day_facts",
            "stock.source_d1_structure_day_facts",
        ),
        "required_relations": (
            (
                "stock.source_rule_evaluations",
                "stock.source_research_enrollments",
            ),
            (
                "stock.source_d1_structure_day_facts",
                "stock.source_d1_material_anchor_day_facts",
                "stock.source_research_observation_segment_days",
                "stock.source_rps_pulse_t0_change_facts",
            ),
        ),
        "algorithm_column": "producer_version",
        "algorithm_label": "D1生产者版本",
    },
    "D0": {
        "parent_table": "stock.source_d0_scenario_day_facts",
        "child_table": None,
        "cleanup_tables": (
            "stock.source_research_enrollments",
            "stock.source_d0_launch_day_facts",
            "stock.source_pool_seed_day_facts",
            "stock.source_d0_scenario_day_facts",
        ),
        "progress_tables": (
            "stock.source_d0_scenario_day_facts",
            "stock.source_pool_seed_day_facts",
            "stock.source_d0_launch_day_facts",
            "stock.source_research_enrollments",
        ),
        "required_relations": (
            (
                "stock.source_d0_launch_day_facts",
                "stock.source_research_enrollments",
            ),
        ),
        "algorithm_column": "producer_version",
        "algorithm_label": "D0/Seed生产者版本",
    },
    "FOCUS_FORMAL": {
        "parent_table": "stock.source_d0_formal_admission_day_facts",
        "child_table": None,
        "cleanup_tables": (
            "stock.source_t0_early_observation_facts",
            "stock.source_research_enrollments",
            "stock.source_rule_evaluations",
            "stock.source_d0_formal_admission_day_facts",
        ),
        "progress_tables": (
            "stock.source_d0_formal_admission_day_facts",
            "stock.source_rule_evaluations",
            "stock.source_t0_early_observation_facts",
        ),
        "row_checked_fk_tables": frozenset(
            {
                "stock.source_research_observation_segment_days",
                "stock.source_rps_pulse_t0_change_facts",
            }
        ),
        "algorithm_column": "raw_producer_version",
        "algorithm_label": "Focus正式准入生产者版本",
    },
    "SECTOR_PULSE": {
        "parent_table": "stock.source_sector_cross_period_pulse_facts",
        "child_table": None,
        "cleanup_tables": ("stock.source_sector_cross_period_pulse_facts",),
        "progress_tables": (
            "stock.source_sector_cross_period_pulse_facts",
            "stock.source_sector_pulse_adjacent_transitions",
        ),
        "algorithm_column": "algorithm_contract",
        "algorithm_label": "行业/GN概念PULSE算法版本",
    },
    "SECTOR_TRANSITION": {
        "parent_table": "stock.source_sector_pulse_adjacent_transitions",
        "child_table": None,
        "cleanup_tables": ("stock.source_sector_pulse_adjacent_transitions",),
        "progress_tables": ("stock.source_sector_pulse_adjacent_transitions",),
        "algorithm_column": "transition_contract",
        "algorithm_label": "板块相邻转移合同版本",
    },
    "SECTOR_FOLLOWUP": {
        "parent_table": "stock.source_sector_pulse_followup_facts",
        "child_table": None,
        "cleanup_tables": (
            "stock.source_sector_pulse_cohort_reason_facts",
            "stock.source_sector_pulse_cohort_group_facts",
            "stock.source_sector_pulse_followup_facts",
        ),
        "progress_tables": (
            "stock.source_sector_pulse_followup_facts",
            "stock.source_sector_pulse_cohort_group_facts",
            "stock.source_sector_pulse_cohort_reason_facts",
        ),
        "required_relations": (
            (
                "stock.source_sector_pulse_cohort_group_facts",
                "stock.source_sector_pulse_cohort_reason_facts",
            ),
        ),
        "algorithm_column": "followup_contract",
        "algorithm_label": "板块跨日后继合同版本",
    },
    "R03_SECTOR_NATIVE": {
        "parent_table": "stock.r03_sector_native_day_facts",
        "child_table": None,
        "cleanup_tables": (
            "stock.r03_sector_loo_candidate_facts",
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_rotation_day_states",
            "stock.r03_sector_rotation_basket_members",
            "stock.r03_sector_rotation_episodes",
            "stock.r03_sector_native_day_facts",
        ),
        "progress_tables": ("stock.r03_sector_native_day_facts",),
        "algorithm_column": "contract_version",
        "algorithm_label": "R03正式板块Native与资格合同版本",
    },
    "R03_SECTOR_ROTATION": {
        "parent_table": "stock.r03_sector_rotation_episodes",
        "child_table": "stock.r03_sector_rotation_board_day_facts",
        "cleanup_tables": (
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_rotation_day_states",
            "stock.r03_sector_rotation_basket_members",
            "stock.r03_sector_rotation_episodes",
        ),
        "progress_tables": (
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_rotation_episodes",
            "stock.r03_sector_rotation_basket_members",
            "stock.r03_sector_rotation_day_states",
        ),
        "algorithm_column": "contract_version",
        "algorithm_label": "R03正式Rotation生命周期合同版本",
    },
    "R03_SECTOR_OVERLAP": {
        "parent_table": "stock.r03_sector_overlap_clusters",
        "child_table": "stock.r03_sector_overlap_board_facts",
        "cleanup_tables": (
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_clusters",
        ),
        "progress_tables": (
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_overlap_board_facts",
        ),
        "algorithm_column": "contract_version",
        "algorithm_label": "R03正式Overlap聚类合同版本",
    },
    "R03_SECTOR_LOO": {
        "parent_table": "stock.r03_sector_loo_support_facts",
        "child_table": "stock.r03_sector_loo_candidate_facts",
        "cleanup_tables": (
            "stock.r03_sector_loo_candidate_facts",
            "stock.r03_sector_loo_support_facts",
        ),
        "progress_tables": (
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_loo_candidate_facts",
        ),
        "algorithm_column": "contract_version",
        "algorithm_label": "R03正式LOO支持板块合同版本",
    },
    "PRICE_LIMITS": {
        "parent_table": "stock.source_price_limit_day_facts",
        "child_table": None,
        "cleanup_tables": (
            "stock.source_limit_up_streak_day_facts",
            "stock.source_price_limit_day_facts",
        ),
        "progress_tables": ("stock.source_price_limit_day_facts",),
        "algorithm_column": "contract_version",
        "algorithm_label": "涨跌停限价合同版本",
    },
    "LIMIT_UP_LADDER": {
        "parent_table": "stock.source_limit_up_streak_day_facts",
        "child_table": None,
        "cleanup_tables": ("stock.source_limit_up_streak_day_facts",),
        "progress_tables": ("stock.source_limit_up_streak_day_facts",),
        "algorithm_column": "contract_version",
        "algorithm_label": "日线连板梯队合同版本",
    },
    "CROSS_SECTION": {
        "parent_table": "stock.source_cross_section_day_facts",
        "child_table": "stock.source_cross_section_endpoints",
        "cleanup_tables": (
            "stock.source_rps_pulse_t0_change_facts",
            "stock.source_research_observation_segment_days",
            "stock.source_t0_early_observation_facts",
            "stock.source_d0_formal_admission_day_facts",
            "stock.source_research_enrollments",
            "stock.source_rule_evaluations",
            "stock.source_cohort_group_stats",
            "stock.source_fep_baseline_facts",
            "stock.source_research_events",
            "stock.source_d0_launch_day_facts",
            "stock.source_d1_material_anchor_day_facts",
            "stock.source_d1_structure_day_facts",
            "stock.source_pool_seed_day_facts",
            "stock.source_d0_scenario_day_facts",
            "stock.source_symbol_price_profiles",
            "stock.source_cross_section_endpoints",
            "stock.source_cross_section_day_facts",
        ),
        "progress_tables": (
            "stock.source_cross_section_day_facts",
            "stock.source_cross_section_endpoints",
            "stock.source_symbol_price_profiles",
            "stock.source_pool_seed_day_facts",
            "stock.source_d0_scenario_day_facts",
            "stock.source_d0_launch_day_facts",
            "stock.source_d0_formal_admission_day_facts",
            "stock.source_d1_structure_day_facts",
            "stock.source_d1_material_anchor_day_facts",
            "stock.source_research_observation_segment_days",
            "stock.source_rps_pulse_t0_change_facts",
            "stock.source_rule_evaluations",
            "stock.source_research_enrollments",
            "stock.source_cohort_group_stats",
            "stock.source_fep_baseline_facts",
        ),
        "required_relations": (
            (
                "stock.source_d1_structure_day_facts",
                "stock.source_d1_material_anchor_day_facts",
            ),
            (
                "stock.source_d0_scenario_day_facts",
                "stock.source_d0_formal_admission_day_facts",
            ),
            (
                "stock.source_pool_seed_day_facts",
                "stock.source_d0_formal_admission_day_facts",
            ),
            (
                "stock.source_rule_evaluations",
                "stock.source_research_enrollments",
            ),
            (
                "stock.source_d0_launch_day_facts",
                "stock.source_research_enrollments",
            ),
            (
                "stock.source_research_enrollments",
                "stock.source_t0_early_observation_facts",
            ),
            (
                "stock.source_research_enrollments",
                "stock.source_research_observation_segment_days",
            ),
            (
                "stock.source_research_enrollments",
                "stock.source_rps_pulse_t0_change_facts",
            ),
            (
                "stock.source_t0_early_observation_facts",
                "stock.source_rps_pulse_t0_change_facts",
            ),
        ),
        "algorithm_column": "contract_version",
        "algorithm_label": "全市场横截面10因子合同版本",
    },
    "MARKET_INDICES_ENV": {
        "parent_table": "stock.source_market_environment_day_facts",
        "child_table": None,
        "cleanup_tables": ("stock.source_market_environment_day_facts",),
        "progress_tables": ("stock.source_market_environment_day_facts",),
        "algorithm_column": "rule_version",
        "algorithm_label": "市场复合环境展示规则版本",
    },
    "FORWARD_PATH": {
        "parent_table": "stock.source_forward_path_facts",
        "child_table": None,
        "cleanup_tables": ("stock.source_forward_path_facts",),
        "progress_tables": ("stock.source_forward_path_facts",),
        "algorithm_column": "path_rule_version",
        "algorithm_label": "Forward路径版本",
    },
    "COHORT_STATS": {
        "parent_table": "stock.source_cohort_group_stats",
        "child_table": None,
        "cleanup_tables": ("stock.source_cohort_group_stats",),
        "progress_tables": ("stock.source_cohort_group_stats",),
        "algorithm_column": "rule_version",
        "algorithm_label": "Cohort分组统计规则版本",
    },
    "FEP_BASELINE": {
        "parent_table": "stock.source_fep_baseline_facts",
        "child_table": None,
        "cleanup_tables": ("stock.source_fep_baseline_facts",),
        "progress_tables": ("stock.source_fep_baseline_facts",),
        "algorithm_column": "rule_version",
        "algorithm_label": "FEP基线规则版本",
    },
    "OBSERVATION_SEGMENTS": {
        "parent_table": "stock.source_research_observation_segment_days",
        "child_table": None,
        "cleanup_tables": ("stock.source_research_observation_segment_days",),
        "progress_tables": ("stock.source_research_observation_segment_days",),
        "algorithm_column": "rule_version",
        "algorithm_label": "观察持续分段规则版本",
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
    """读取截止日内BaoStock双坐标Raw覆盖及捕获请求闭合状态。"""
    if type(cutoff) is not date:
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_REQUIRED", "Raw覆盖核验必须绑定研究截止日"
        )
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """WITH request_codes AS (
                   SELECT capture_id,source_code,
                          count(*)=2
                            AND count(DISTINCT adjustment_flag)=2
                            AND count(DISTINCT eligible_start_session)=1
                            AND count(DISTINCT eligible_end_session)=1
                            AND bool_and(
                                request_status IN ('COMPLETED','NO_DATA')
                                AND (request_status='NO_DATA'
                                     OR completed_through_session>=range_end_session)
                            ) AS code_requests_complete,
                          min(eligible_start_session) AS eligible_start_session,
                          max(eligible_end_session) AS eligible_end_session,
                          coalesce(sum(response_row_count),0)::bigint
                              AS response_row_count
                   FROM stock.source_capture_requests
                   WHERE range_end_session<=%s
                   GROUP BY capture_id,source_code
               ), price_sessions AS (
                   SELECT DISTINCT d.session_date,d.source_capture_id
                   FROM stock.source_raw_daily d
                   JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
                   WHERE d.session_date<=%s
                     AND c.provider='BAOSTOCK'
                     AND c.range_end_session<=%s
                     AND c.price_coordinates @> ARRAY[
                         'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                     ]::text[]
               ), request_coverage AS (
                   SELECT s.session_date,s.source_capture_id,
                          count(*) FILTER (
                              WHERE s.session_date BETWEEN r.eligible_start_session
                                                       AND r.eligible_end_session
                          )::bigint AS expected_symbol_count,
                          coalesce(bool_and(r.code_requests_complete) FILTER (
                              WHERE s.session_date BETWEEN r.eligible_start_session
                                                       AND r.eligible_end_session
                          ),FALSE) AS requests_complete,
                          coalesce(sum(r.response_row_count) FILTER (
                              WHERE s.session_date BETWEEN r.eligible_start_session
                                                       AND r.eligible_end_session
                          ),0)::bigint AS capture_request_response_row_count
                   FROM price_sessions s
                   JOIN request_codes r ON r.capture_id=s.source_capture_id
                   GROUP BY s.session_date,s.source_capture_id
               ), raw_rows AS (
                   SELECT d.session_date,d.source_capture_id,
                          count(*)::bigint AS row_count,
                          count(DISTINCT d.source_symbol)::bigint AS symbol_count,
                          count(*) FILTER (WHERE d.price_quality IS NOT NULL)::bigint
                              AS raw_quality_row_count,
                          count(*) FILTER (WHERE d.price_quality='OBSERVED')::bigint
                              AS raw_observed_row_count,
                          count(*) FILTER (WHERE d.qfq_price_quality IS NOT NULL)::bigint
                              AS qfq_quality_row_count,
                          count(*) FILTER (WHERE d.qfq_price_quality='OBSERVED')::bigint
                              AS qfq_observed_row_count
                   FROM stock.source_raw_daily d
                   JOIN price_sessions s
                     ON s.session_date=d.session_date
                    AND s.source_capture_id=d.source_capture_id
                   GROUP BY d.session_date,d.source_capture_id
               )
               SELECT d.session_date,d.source_capture_id,r.requests_complete,
                      r.expected_symbol_count,r.capture_request_response_row_count,
                      d.row_count,d.symbol_count,
                      d.raw_quality_row_count,d.raw_observed_row_count,
                      d.qfq_quality_row_count,d.qfq_observed_row_count
               FROM raw_rows d
               JOIN request_coverage r
                 ON r.session_date=d.session_date
                AND r.source_capture_id=d.source_capture_id
               ORDER BY d.session_date,d.source_capture_id""",
            (cutoff, cutoff, cutoff),
        )
        coverage_rows = cursor.fetchall()
        cursor.execute(
            """SELECT target_session,source_capture_id,
                      history_requested_session_count,profile_revision,count(*)::bigint
               FROM stock.source_symbol_price_profiles
               WHERE target_session<=%s
               GROUP BY target_session,source_capture_id,
                        history_requested_session_count,profile_revision
               ORDER BY target_session DESC,source_capture_id,profile_revision""",
            (cutoff,),
        )
        profile_rows = cursor.fetchall()
        cursor.execute(
            """SELECT d.target_session,d.source_capture_id,
                      d.contract_version,d.calendar_revision,count(*)::bigint
               FROM stock.source_market_breadth_days d
               JOIN stock.source_market_breadth_symbols s
                 ON s.breadth_day_id=d.id
               WHERE d.target_session<=%s
                 AND d.contract_version='MKT-02_BAOSTOCK_QFQ_POSITIVE_RETURN_STATE_DELTA_R3'
               GROUP BY d.target_session,d.source_capture_id,
                        d.contract_version,d.calendar_revision
               ORDER BY d.target_session,d.source_capture_id""",
            (cutoff,),
        )
        breadth_rows = cursor.fetchall()
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

    price_coordinate_versions: list[dict[str, Any]] = []
    by_session_capture: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for row in coverage_rows:
        (
            session,
            capture_id,
            requests_complete,
            expected_symbols,
            request_response_rows,
            row_count,
            symbol_count,
            raw_quality_count,
            raw_observed_count,
            qfq_quality_count,
            qfq_observed_count,
        ) = row
        session_text = session.isoformat()
        capture_id = int(capture_id)
        expected_symbols = int(expected_symbols)
        row_count = int(row_count)
        closed_rows = (
            bool(requests_complete)
            and expected_symbols > 0
            and row_count == expected_symbols
            and int(symbol_count) == row_count
        )
        coordinate_values = (
            (
                "RAW_UNADJUSTED",
                int(raw_quality_count),
                int(raw_observed_count),
            ),
            (
                "FORWARD_ADJUSTED",
                int(qfq_quality_count),
                int(qfq_observed_count),
            ),
        )
        for coordinate, quality_count, observed_count in coordinate_values:
            status = (
                "COMPLETE"
                if closed_rows and quality_count == row_count
                else "INCOMPLETE"
            )
            version = {
                "session_date": session_text,
                "source_capture_id": capture_id,
                "price_coordinate": coordinate,
                "coverage_status": status,
                "requests_complete": bool(requests_complete),
                "row_count": row_count,
                "expected_row_count": expected_symbols,
                "capture_request_response_row_count": int(request_response_rows),
                "symbol_count": int(symbol_count),
                "quality_row_count": quality_count,
                "observed_value_row_count": observed_count,
            }
            price_coordinate_versions.append(version)
            by_session_capture.setdefault((session_text, capture_id), {})[
                coordinate
            ] = version

    captures_by_session: dict[str, list[int]] = {}
    for session_text, capture_id in by_session_capture:
        captures_by_session.setdefault(session_text, []).append(capture_id)
    complete_capture_by_key: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}
    for key, coordinates in by_session_capture.items():
        if set(coordinates) == {"RAW_UNADJUSTED", "FORWARD_ADJUSTED"} and all(
            item["coverage_status"] == "COMPLETE" for item in coordinates.values()
        ):
            complete_capture_by_key[key] = coordinates
    closed_sessions = sorted(
        session_text
        for session_text, capture_ids in captures_by_session.items()
        if len(capture_ids) == 1
        and (session_text, capture_ids[0]) in complete_capture_by_key
    )
    ambiguous_sessions = sorted(
        session_text
        for session_text, capture_ids in captures_by_session.items()
        if len(capture_ids) != 1
        or (session_text, capture_ids[0]) not in complete_capture_by_key
    )
    raw_version_by_key = {
        key: coordinates["RAW_UNADJUSTED"]
        for key, coordinates in complete_capture_by_key.items()
        if key[0] in closed_sessions
    }

    complete_profiles = []
    for (
        target_session,
        capture_id,
        history_count,
        profile_revision,
        profile_rows_count,
    ) in profile_rows:
        session_text = target_session.isoformat()
        key = (session_text, int(capture_id))
        raw_version = raw_version_by_key.get(key)
        if raw_version is not None and int(profile_rows_count) == int(
            raw_version["row_count"]
        ):
            complete_profiles.append(
                {
                    "target_session": target_session,
                    "source_capture_id": int(capture_id),
                    "source_capture_ids_by_coordinate": {
                        "RAW_UNADJUSTED": int(capture_id),
                        "FORWARD_ADJUSTED": int(capture_id),
                    },
                    "profile_revision": profile_revision,
                    "history_session_count": int(history_count),
                    "row_count": int(profile_rows_count),
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
    profile_captures = sorted({item["source_capture_id"] for item in latest_profiles})

    breadth_completed_versions = []
    for (
        target_session,
        capture_id,
        contract_version,
        calendar_revision,
        symbol_rows,
    ) in breadth_rows:
        session_text = target_session.isoformat()
        capture_id = int(capture_id)
        version = raw_version_by_key.get((session_text, capture_id))
        if version is not None and int(symbol_rows) == int(version["row_count"]):
            breadth_completed_versions.append(
                {
                    "target_session": session_text,
                    "source_capture_id": capture_id,
                    "source_capture_ids_by_coordinate": {
                        "RAW_UNADJUSTED": capture_id,
                        "FORWARD_ADJUSTED": capture_id,
                    },
                    "contract_version": contract_version,
                    "calendar_revision": calendar_revision,
                    "symbol_rows": int(symbol_rows),
                }
            )
    breadth_completed_sessions = sorted(
        {item["target_session"] for item in breadth_completed_versions}
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
    completed_captures = {
        module_watermarks["module_completion_source_captures"].get(module)
        for module in latest_research_modules
    }
    completed_captures.discard(None)
    last_completed_capture = (
        next(iter(completed_captures)) if len(completed_captures) == 1 else None
    )
    return {
        "cutoff": cutoff.isoformat(),
        "stored_session_count": len(closed_sessions),
        "first_stored_session": closed_sessions[0] if closed_sessions else None,
        "last_stored_session": closed_sessions[-1] if closed_sessions else None,
        "closed_sessions": closed_sessions,
        "closed_versions": [
            item
            for item in price_coordinate_versions
            if item["coverage_status"] == "COMPLETE"
        ],
        "ambiguous_sessions": ambiguous_sessions,
        "price_coordinate_versions": price_coordinate_versions,
        "price_coordinate_reader_status": "AVAILABLE",
        "last_completed_research_session": (
            last_completed_session.isoformat() if last_completed_session else None
        ),
        "last_completed_source_capture_id": last_completed_capture,
        "latest_complete_profile_session": (
            latest_profile_session.isoformat() if latest_profile_session else None
        ),
        "current_profile_history_session_count": (
            profile_windows[0] if len(profile_windows) == 1 else None
        ),
        "profile_history_window_ambiguous": len(profile_windows) > 1,
        "latest_profile_source_capture_id": (
            profile_captures[0] if len(profile_captures) == 1 else None
        ),
        "profile_completed_sessions": sorted(
            {item["target_session"].isoformat() for item in complete_profiles}
        ),
        "profile_completed_versions": complete_profiles,
        "market_breadth_completed_sessions": breadth_completed_sessions,
        "market_breadth_completed_versions": breadth_completed_versions,
        "current_d1_history_session_count": (
            d1_windows[0] if len(d1_windows) == 1 else None
        ),
        "d1_history_window_ambiguous": len(d1_windows) > 1,
        "source_coverage": "PG_BAOSTOCK_DUAL_COORDINATE_RAW",
        "source_request_performed": False,
        "database_write_attempted": False,
        **module_watermarks,
    }


def read_raw_symbols_by_session(
    config: Mapping[str, Any],
    *,
    target_sessions: Sequence[date],
    source_capture_id: int,
    cutoff: date,
) -> dict[str, Any]:
    """按真实BaoStock捕获读取截止日内各官方会话的证券代码。"""
    if type(cutoff) is not date:
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_REQUIRED", "Raw证券范围读取必须绑定研究截止日"
        )
    if type(source_capture_id) is not int or source_capture_id <= 0:
        raise DataJobPgError(
            "DATA_JOB_SOURCE_CAPTURE_INVALID", "Raw证券范围读取必须绑定正整数来源捕获ID"
        )
    if isinstance(target_sessions, (str, bytes)) or not isinstance(
        target_sessions, Sequence
    ):
        raise DataJobPgError(
            "DATA_JOB_SESSION_SCOPE_INVALID", "Raw证券范围必须是官方会话日期列表"
        )
    sessions = tuple(target_sessions)
    if (
        not sessions
        or any(type(session) is not date for session in sessions)
        or len(set(sessions)) != len(sessions)
    ):
        raise DataJobPgError(
            "DATA_JOB_SESSION_SCOPE_INVALID", "Raw证券范围为空、重复或包含无效日期"
        )
    after_cutoff = sorted(session for session in sessions if session > cutoff)
    if after_cutoff:
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_EXCEEDED",
            "Raw证券范围中存在超过研究截止日的会话；未读取来源数据",
        )
    ordered_sessions = tuple(sorted(sessions))
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT d.session_date,
                      array_agg(DISTINCT d.source_symbol ORDER BY d.source_symbol),
                      count(DISTINCT d.source_symbol)
               FROM stock.source_raw_daily d
               JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
               WHERE d.session_date=ANY(%s) AND d.source_capture_id=%s
                 AND d.session_date<=%s AND c.provider='BAOSTOCK'
                 AND c.price_coordinates @> ARRAY[
                     'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                 ]::text[]
               GROUP BY d.session_date
               ORDER BY d.session_date""",
            (list(ordered_sessions), source_capture_id, cutoff),
        )
        rows = cursor.fetchall()
    symbols_by_session = {session.isoformat(): [] for session in ordered_sessions}
    counts_by_session = {session.isoformat(): 0 for session in ordered_sessions}
    for session, symbols, symbol_count in rows:
        session_key = session.isoformat()
        normalized_symbols = sorted({str(symbol) for symbol in (symbols or ())})
        if len(normalized_symbols) != int(symbol_count):
            raise DataJobPgError(
                "DATA_JOB_RAW_SYMBOL_READBACK_INVALID",
                f"PG去重证券数与来源行计数不一致：{session_key}",
            )
        symbols_by_session[session_key] = normalized_symbols
        counts_by_session[session_key] = int(symbol_count)
    return {
        "target_sessions": [session.isoformat() for session in ordered_sessions],
        "source_capture_id": source_capture_id,
        "cutoff": cutoff.isoformat(),
        "symbols_by_session": symbols_by_session,
        "symbol_counts_by_session": counts_by_session,
        "missing_sessions": [
            session for session, count in counts_by_session.items() if count == 0
        ],
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def read_module_completion_watermarks(
    config: Mapping[str, Any], *, cutoff: date
) -> dict[str, Any]:
    """按唯一来源捕获、完整业务表集合和PG读回行数计算模块水位。"""
    if type(cutoff) is not date:
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_REQUIRED", "模块完成水位核验必须绑定研究截止日"
        )
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT p.job_id,p.target_session,p.business_table,p.status,
                      p.expected_rows,p.verified_rows,p.source_capture_id,
                      p.profile_revision,p.producer_version,p.profile_history_session_count,
                      p.d1_history_session_count,p.calendar_revision,
                      p.industry_snapshot_id,p.concept_snapshot_id,j.request_key,
                      p.universe_version_t,p.universe_version_t1,p.universe_version_t3,
                      p.source_capture_id_t1,p.source_capture_id_t3,
                      p.profile_revision_t1,p.profile_revision_t3,
                      p.prev_scope_key,p.prev_member_version,p.prev_trade_date,
                      p.industry_snapshot_id_t1,p.concept_snapshot_id_t1,
                      p.industry_snapshot_id_t3,p.concept_snapshot_id_t3,
                      p.source_capture_id_t4,p.prev_session_t,p.prev_session_t3
               FROM stock.data_job_progress p
               JOIN stock.data_jobs j ON j.job_id=p.job_id
               JOIN stock.source_captures c ON c.capture_id=p.source_capture_id
               WHERE p.target_session<=%s
                 AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                 AND p.completed_at IS NOT NULL
                 AND p.expected_rows IS NOT NULL
                 AND p.verified_rows=p.expected_rows
                 AND (
                     (p.business_table=ANY(ARRAY[
                         'stock.tdx_sector_source_snapshots',
                         'stock.tdx_sector_catalog',
                         'stock.tdx_sector_members'
                     ]::text[]) AND c.provider='TDX')
                     OR
                     (p.business_table<>ALL(ARRAY[
                         'stock.tdx_sector_source_snapshots',
                         'stock.tdx_sector_catalog',
                         'stock.tdx_sector_members'
                     ]::text[]) AND c.provider='BAOSTOCK')
                 )
               ORDER BY p.job_id,p.target_session,p.business_table""",
            (cutoff,),
        )
        rows = cursor.fetchall()

    by_job_module_session: dict[tuple[Any, ...], dict[str, Any]] = {}
    for (
        job_id,
        target_session,
        business_table,
        _status,
        expected_rows,
        verified_rows,
        capture_id,
        profile_revision,
        producer_version,
        profile_history_session_count,
        d1_history_session_count,
        calendar_revision,
        industry_snapshot_id,
        concept_snapshot_id,
        request_key,
        universe_version_t,
        universe_version_t1,
        universe_version_t3,
        source_capture_id_t1,
        source_capture_id_t3,
        profile_revision_t1,
        profile_revision_t3,
        prev_scope_key,
        prev_member_version,
        prev_trade_date,
        industry_snapshot_id_t1,
        concept_snapshot_id_t1,
        industry_snapshot_id_t3,
        concept_snapshot_id_t3,
        source_capture_id_t4,
        prev_session_t,
        prev_session_t3,
    ) in rows:
        for module, required_tables in _MODULE_PROGRESS_TABLES.items():
            if business_table not in required_tables:
                continue
            if module == "TDX_SECTOR_MEMBERS":
                if industry_snapshot_id is None or concept_snapshot_id is None:
                    continue
            elif capture_id is None:
                continue
            module_capture_id = int(capture_id) if capture_id is not None else None
            coordinate_capture_ids = (
                {
                    "RAW_UNADJUSTED": module_capture_id,
                    "FORWARD_ADJUSTED": module_capture_id,
                }
                if module != "TDX_SECTOR_MEMBERS" and module_capture_id is not None
                else {}
            )
            key = (
                job_id,
                module,
                target_session,
                module_capture_id,
                industry_snapshot_id,
                concept_snapshot_id,
            )
            state = by_job_module_session.setdefault(
                key,
                {
                    "tables": set(),
                    "table_results": {},
                    "source_capture_id": module_capture_id,
                    "source_capture_ids_by_coordinate": coordinate_capture_ids,
                    "industry_snapshot_id": industry_snapshot_id,
                    "concept_snapshot_id": concept_snapshot_id,
                    "profile_revision": profile_revision,
                    "producer_version": producer_version,
                    "universe_version_t": universe_version_t,
                    "universe_version_t1": universe_version_t1,
                    "universe_version_t3": universe_version_t3,
                    "source_capture_id_t1": source_capture_id_t1,
                    "source_capture_id_t3": source_capture_id_t3,
                    "profile_revision_t1": profile_revision_t1,
                    "profile_revision_t3": profile_revision_t3,
                    "prev_scope_key": prev_scope_key,
                    "prev_member_version": prev_member_version,
                    "prev_trade_date": prev_trade_date.isoformat()
                    if prev_trade_date
                    else None,
                    "industry_snapshot_id_t1": industry_snapshot_id_t1,
                    "concept_snapshot_id_t1": concept_snapshot_id_t1,
                    "industry_snapshot_id_t3": industry_snapshot_id_t3,
                    "concept_snapshot_id_t3": concept_snapshot_id_t3,
                    "source_capture_id_t4": source_capture_id_t4,
                    "prev_session_t": prev_session_t.isoformat()
                    if prev_session_t
                    else None,
                    "prev_session_t3": prev_session_t3.isoformat()
                    if prev_session_t3
                    else None,
                    "request_key": str(request_key) if request_key else None,
                },
            )
            for _k, _v in (
                ("profile_revision", profile_revision),
                ("producer_version", producer_version),
                ("universe_version_t", universe_version_t),
                ("universe_version_t1", universe_version_t1),
                ("universe_version_t3", universe_version_t3),
                ("source_capture_id_t1", source_capture_id_t1),
                ("source_capture_id_t3", source_capture_id_t3),
                ("profile_revision_t1", profile_revision_t1),
                ("profile_revision_t3", profile_revision_t3),
                ("prev_scope_key", prev_scope_key),
                ("prev_member_version", prev_member_version),
                (
                    "prev_trade_date",
                    prev_trade_date.isoformat() if prev_trade_date else None,
                ),
                ("industry_snapshot_id_t1", industry_snapshot_id_t1),
                ("concept_snapshot_id_t1", concept_snapshot_id_t1),
                ("industry_snapshot_id_t3", industry_snapshot_id_t3),
                ("concept_snapshot_id_t3", concept_snapshot_id_t3),
                ("source_capture_id_t4", source_capture_id_t4),
                (
                    "prev_session_t",
                    prev_session_t.isoformat() if prev_session_t else None,
                ),
                (
                    "prev_session_t3",
                    prev_session_t3.isoformat() if prev_session_t3 else None,
                ),
            ):
                if _v is not None and state.get(_k) is None:
                    state[_k] = _v
            state["tables"].add(business_table)
            state["table_results"][business_table] = {
                "business_table": business_table,
                "expected_rows": int(expected_rows),
                "verified_rows": int(verified_rows),
                "source_capture_id": module_capture_id,
                "source_capture_ids_by_coordinate": coordinate_capture_ids,
                "profile_revision": profile_revision,
                "producer_version": producer_version,
                "profile_history_session_count": profile_history_session_count,
                "d1_history_session_count": d1_history_session_count,
                "calendar_revision": calendar_revision,
                "industry_snapshot_id": industry_snapshot_id,
                "concept_snapshot_id": concept_snapshot_id,
                "universe_version_t": universe_version_t,
                "universe_version_t1": universe_version_t1,
                "universe_version_t3": universe_version_t3,
                "source_capture_id_t1": source_capture_id_t1,
                "source_capture_id_t3": source_capture_id_t3,
                "profile_revision_t1": profile_revision_t1,
                "profile_revision_t3": profile_revision_t3,
                "prev_scope_key": prev_scope_key,
                "prev_member_version": prev_member_version,
                "prev_trade_date": prev_trade_date.isoformat()
                if prev_trade_date
                else None,
                "industry_snapshot_id_t1": industry_snapshot_id_t1,
                "concept_snapshot_id_t1": concept_snapshot_id_t1,
                "industry_snapshot_id_t3": industry_snapshot_id_t3,
                "concept_snapshot_id_t3": concept_snapshot_id_t3,
                "source_capture_id_t4": source_capture_id_t4,
                "prev_session_t": prev_session_t.isoformat()
                if prev_session_t
                else None,
                "prev_session_t3": prev_session_t3.isoformat()
                if prev_session_t3
                else None,
                "request_key": str(request_key) if request_key else None,
            }

    sessions_by_module: dict[str, set[date]] = {
        module: set() for module in _MODULE_PROGRESS_TABLES
    }
    captures_by_module_session: dict[tuple[str, date], set[int]] = {}
    completed_units_by_module: dict[str, list[dict[str, Any]]] = {
        module: [] for module in _MODULE_PROGRESS_TABLES
    }
    for key, state in by_job_module_session.items():
        _job_id, module, target_session, capture_id = key[:4]
        if not _MODULE_PROGRESS_TABLES[module].issubset(state["tables"]):
            continue
        sessions_by_module[module].add(target_session)
        if capture_id is not None:
            captures_by_module_session.setdefault((module, target_session), set()).add(
                int(capture_id)
            )
        completed_units_by_module[module].append(
            {
                "job_id": str(_job_id),
                "target_session": target_session.isoformat(),
                "source_capture_id": capture_id,
                "source_capture_ids_by_coordinate": state[
                    "source_capture_ids_by_coordinate"
                ],
                "industry_snapshot_id": state["industry_snapshot_id"],
                "concept_snapshot_id": state["concept_snapshot_id"],
                "profile_revision": state["profile_revision"],
                "producer_version": state["producer_version"],
                "universe_version_t": state["universe_version_t"],
                "universe_version_t1": state["universe_version_t1"],
                "universe_version_t3": state["universe_version_t3"],
                "source_capture_id_t1": state["source_capture_id_t1"],
                "source_capture_id_t3": state["source_capture_id_t3"],
                "profile_revision_t1": state["profile_revision_t1"],
                "profile_revision_t3": state["profile_revision_t3"],
                "prev_scope_key": state["prev_scope_key"],
                "prev_member_version": state["prev_member_version"],
                "prev_trade_date": state["prev_trade_date"],
                "industry_snapshot_id_t1": state["industry_snapshot_id_t1"],
                "concept_snapshot_id_t1": state["concept_snapshot_id_t1"],
                "industry_snapshot_id_t3": state["industry_snapshot_id_t3"],
                "concept_snapshot_id_t3": state["concept_snapshot_id_t3"],
                "source_capture_id_t4": state["source_capture_id_t4"],
                "prev_session_t": state["prev_session_t"],
                "prev_session_t3": state["prev_session_t3"],
                "request_key": state["request_key"],
                "business_tables": [
                    state["table_results"][table]
                    for table in sorted(state["table_results"])
                ],
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
    latest_captures = {}
    for module, watermark in watermarks.items():
        if watermark is None:
            latest_captures[module] = None
            continue
        capture_ids = captures_by_module_session.get(
            (module, date.fromisoformat(watermark)), set()
        )
        latest_captures[module] = (
            next(iter(capture_ids)) if len(capture_ids) == 1 else None
        )
    for units in completed_units_by_module.values():
        units.sort(
            key=lambda item: (
                item["target_session"],
                item["source_capture_id"] or 0,
                item["industry_snapshot_id"] or 0,
                item["concept_snapshot_id"] or 0,
            )
        )
    return {
        "completed_sessions_by_module": completed_sessions,
        "latest_completed_by_module": watermarks,
        "completed_units_by_module": completed_units_by_module,
        "module_completed_sessions": completed_sessions,
        "module_completion_watermarks": watermarks,
        "module_completion_source_captures": latest_captures,
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


def _cross_section_cleanup_details(
    cursor: Any,
    *,
    target_sessions: Sequence[date],
    source_capture_id: int,
    cutoff: date,
) -> dict[str, Any]:
    """一次解析横截面直接后继和其R4实际历史窗口覆盖的D1链。"""
    from stock_a.storage.postgres.research_day_facts import _D1_PRODUCER_VERSION_7

    selected = tuple(sorted(set(target_sessions)))
    cursor.execute(
        """WITH RECURSIVE scope AS (
               SELECT unnest(%s::date[]) AS target_session
           ), profile_rows AS (
               SELECT DISTINCT p.target_session,p.profile_revision
               FROM stock.source_symbol_price_profiles p
               JOIN scope s ON s.target_session=p.target_session
               WHERE p.source_capture_id=%s
            ), root_d1 AS (
               SELECT DISTINCT d.id,d.target_session,d.source_capture_id,
                      d.d1_history_session_count,d.calendar_revision,
                      d.history_window_start
               FROM stock.source_d1_structure_day_facts d
               WHERE d.source_capture_id=%s AND d.producer_version=%s
                 AND d.target_session<=%s
                 AND EXISTS (
                     SELECT 1 FROM scope s
                     WHERE d.history_window_start<=s.target_session
                       AND s.target_session<=d.target_session
                 )
            ), root_rules AS (
               SELECT DISTINCT r.id,r.target_session,r.source_capture_id,
                      r.rule_id,r.rule_version,r.profile_revision,
                      r.history_session_count,r.d1_history_session_count,
                      r.event_window_start,r.calendar_revision
               FROM stock.source_rule_evaluations r
               WHERE r.source_capture_id=%s
                 AND (
                     (r.target_session,r.profile_revision) IN (
                         SELECT target_session,profile_revision FROM profile_rows
                     )
                      OR EXISTS (
                          SELECT 1 FROM root_d1 d
                          WHERE d.target_session=r.target_session
                            AND d.source_capture_id=r.source_capture_id
                            AND d.d1_history_session_count=r.d1_history_session_count
                            AND d.calendar_revision=r.calendar_revision
                            AND d.history_window_start=r.event_window_start
                      )
                  )
            ), affected_cohort_stats AS (
               SELECT DISTINCT c.ctid,c.target_session
               FROM stock.source_cohort_group_stats c
               JOIN root_rules r
                 ON c.target_session=r.target_session
                AND c.source_capture_id=r.source_capture_id
                AND c.rule_id=r.rule_id AND c.rule_version=r.rule_version
                AND c.profile_revision=r.profile_revision
                AND c.history_session_count=r.history_session_count
                AND c.d1_history_session_count=r.d1_history_session_count
                AND c.event_window_start=r.event_window_start
                AND c.calendar_revision=r.calendar_revision
               WHERE c.research_cutoff<=%s
            ), affected_t0_fep AS (
               SELECT DISTINCT f.id,f.research_cutoff,f.target_session,
                      f.rule_id,f.rule_version,f.profile_family,
                      f.calendar_revision,f.path_rule_version,
                      f.source_time_quality,f.parser_contract,f.lane,f.horizon,
                      f.group_code,f.metric_code,f.condition_code
               FROM stock.source_fep_baseline_facts f
               JOIN root_rules r
                 ON f.target_session=r.target_session
                AND f.source_capture_id=r.source_capture_id
                AND f.rule_id=r.rule_id AND f.rule_version=r.rule_version
                AND f.profile_revision=r.profile_revision
                AND f.history_session_count=r.history_session_count
                AND f.d1_history_session_count=r.d1_history_session_count
                AND f.event_window_start=r.event_window_start
                AND f.calendar_revision=r.calendar_revision
               WHERE f.summary_scope='T0' AND f.research_cutoff<=%s
            ), affected_fep_baselines AS (
               SELECT id,research_cutoff FROM affected_t0_fep
               UNION
               SELECT DISTINCT f.id,f.research_cutoff
               FROM stock.source_fep_baseline_facts f
               JOIN affected_t0_fep t0
                 ON f.research_cutoff=t0.research_cutoff
                AND f.rule_id=t0.rule_id AND f.rule_version=t0.rule_version
                AND f.profile_family=t0.profile_family
                AND f.calendar_revision=t0.calendar_revision
                AND f.path_rule_version=t0.path_rule_version
                AND f.source_time_quality=t0.source_time_quality
                AND f.parser_contract=t0.parser_contract
                AND f.lane=t0.lane AND f.horizon=t0.horizon
                AND f.group_code=t0.group_code
                AND f.metric_code=t0.metric_code
                AND f.condition_code=t0.condition_code
               WHERE f.summary_scope='EQUAL_DATE'
                 AND f.source_capture_id IS NULL
                 AND f.period_start_session<=t0.target_session
                 AND t0.target_session<=f.period_end_session
            ), affected_fep_progress_sessions AS (
               SELECT DISTINCT p.target_session
               FROM stock.data_job_progress p
               JOIN stock.data_jobs j ON j.job_id=p.job_id
               WHERE p.business_table='stock.source_fep_baseline_facts'
                 AND p.source_capture_id=(SELECT source_capture_id FROM root_rules LIMIT 1)
                 AND j.source_capture_id=p.source_capture_id
                 AND j.research_cutoff IN (
                     SELECT DISTINCT research_cutoff FROM affected_fep_baselines
                 )
            ), affected_research_events AS (
               SELECT DISTINCT e.id,e.target_session
               FROM stock.source_research_events e
               JOIN root_d1 d
                 ON e.target_session=d.target_session
                AND e.source_capture_id=d.source_capture_id
                AND e.target_capture_id=d.source_capture_id
                AND e.d1_history_session_count=d.d1_history_session_count
                AND e.calendar_revision=d.calendar_revision
               WHERE e.event_version='SOURCE_D1_ANCHOR_R1'
            ), root_d0 AS (
               SELECT DISTINCT d.id,d.target_session
               FROM stock.source_d0_launch_day_facts d
               WHERE d.source_capture_id=%s
                 AND d.target_session IN (SELECT target_session FROM profile_rows)
           ), root_seed AS (
               SELECT DISTINCT f.id,f.target_session
               FROM stock.source_pool_seed_day_facts f
               JOIN profile_rows p ON p.target_session=f.target_session
                                  AND p.profile_revision=f.profile_revision
               WHERE f.source_capture_id=%s
           ), affected_enrollments(id) AS (
               SELECT e.id
               FROM stock.source_research_enrollments e
               WHERE e.rule_fact_id IN (SELECT id FROM root_rules)
                  OR e.previous_rule_fact_id IN (SELECT id FROM root_rules)
                  OR e.d0_fact_id IN (SELECT id FROM root_d0)
               UNION
               SELECT child.id
               FROM stock.source_research_enrollments child
               JOIN affected_enrollments parent
                 ON child.parent_enrollment_id=parent.id
           ), affected_enrollment_dates AS (
               SELECT a.id,COALESCE(r.target_session,d.target_session) AS target_session
               FROM affected_enrollments a
               LEFT JOIN stock.source_research_enrollments e ON e.id=a.id
               LEFT JOIN stock.source_rule_evaluations r ON r.id=e.rule_fact_id
               LEFT JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
           ), affected_t0 AS (
               SELECT t.id
               FROM stock.source_t0_early_observation_facts t
               WHERE t.source_capture_id=%s
                 AND (
                     t.target_session IN (SELECT target_session FROM profile_rows)
                     OR t.target_session IN (SELECT target_session FROM root_d1)
                     OR t.enrollment_id IN (SELECT id FROM affected_enrollments)
                     OR t.rule_fact_id IN (SELECT id FROM root_rules)
                     OR t.d0_fact_id IN (SELECT id FROM root_d0)
                     OR t.seed_fact_id IN (SELECT id FROM root_seed)
                 )
           ), affected_observation_segments AS (
               SELECT o.id,o.t0_session
               FROM stock.source_research_observation_segment_days o
               WHERE o.anchor_rule_fact_id IN (SELECT id FROM root_rules)
                  OR o.evidence_rule_fact_id IN (SELECT id FROM root_rules)
                  OR o.source_enrollment_id IN (SELECT id FROM affected_enrollments)
                  OR o.parent_enrollment_id IN (SELECT id FROM affected_enrollments)
                  OR o.linked_enrollment_id IN (SELECT id FROM affected_enrollments)
           ), affected_pulse_changes AS (
                SELECT DISTINCT p.target_session
               FROM stock.source_rps_pulse_t0_change_facts p
               WHERE p.source_t0_fact_id IN (SELECT id FROM affected_t0)
                  OR p.enrollment_id IN (SELECT id FROM affected_enrollments)
           ), dependent_sessions AS (
               SELECT target_session FROM root_d1
               WHERE target_session<>ALL(%s::date[])
               UNION
               SELECT target_session FROM affected_enrollment_dates
               WHERE target_session IS NOT NULL
                 AND target_session<>ALL(%s::date[])
               UNION
               SELECT target_session FROM affected_pulse_changes
               WHERE target_session<>ALL(%s::date[])
               UNION
               SELECT t0_session FROM affected_observation_segments
               WHERE t0_session<>ALL(%s::date[])
                UNION
                SELECT target_session FROM affected_fep_progress_sessions
                WHERE target_session<>ALL(%s::date[])
           )
           SELECT
             (SELECT count(*) FROM stock.source_symbol_price_profiles p
              WHERE p.target_session IN (SELECT target_session FROM profile_rows)
                AND p.source_capture_id=%s),
             (SELECT count(*) FROM root_seed),
             (SELECT count(*) FROM stock.source_d0_scenario_day_facts f
              WHERE f.target_session IN (SELECT target_session FROM profile_rows)
                AND f.source_capture_id=%s),
             (SELECT count(*) FROM root_d0),
             (SELECT count(*) FROM stock.source_d0_formal_admission_day_facts f
              WHERE f.target_session IN (SELECT target_session FROM profile_rows)
                AND f.source_capture_id=%s),
             (SELECT count(*) FROM root_d1),
             (SELECT count(*) FROM stock.source_d1_material_anchor_day_facts a
              WHERE a.d1_structure_id IN (SELECT id FROM root_d1)),
             (SELECT count(*) FROM root_rules),
             (SELECT count(*) FROM affected_enrollments),
             (SELECT count(*) FROM affected_t0),
             (SELECT count(*) FROM stock.source_rps_pulse_t0_change_facts p
              WHERE p.source_t0_fact_id IN (SELECT id FROM affected_t0)
                 OR p.enrollment_id IN (SELECT id FROM affected_enrollments)),
             (SELECT count(*) FROM affected_observation_segments),
             (SELECT count(*) FROM affected_cohort_stats),
             (SELECT count(*) FROM affected_fep_baselines),
             (SELECT count(*) FROM affected_research_events),
             COALESCE((SELECT array_agg(target_session ORDER BY target_session)
                       FROM dependent_sessions),ARRAY[]::date[]),
             COALESCE((SELECT array_agg(DISTINCT target_session ORDER BY target_session)
                       FROM profile_rows),ARRAY[]::date[]),
             COALESCE((SELECT array_agg(DISTINCT profile_revision ORDER BY profile_revision)
                       FROM profile_rows),ARRAY[]::text[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_rules),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_d0),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_seed),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_enrollments),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_t0),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_d1),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id)
                       FROM affected_observation_segments),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id)
                       FROM affected_research_events),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id)
                       FROM affected_fep_baselines),ARRAY[]::bigint[])""",
        (
            list(selected),
            source_capture_id,
            source_capture_id,
            _D1_PRODUCER_VERSION_7,
            cutoff,
            source_capture_id,
            cutoff,
            cutoff,
            source_capture_id,
            source_capture_id,
            source_capture_id,
            list(selected),
            list(selected),
            list(selected),
            list(selected),
            list(selected),
            source_capture_id,
            source_capture_id,
            source_capture_id,
        ),
    )
    row = cursor.fetchone()
    table_names = (
        "stock.source_symbol_price_profiles",
        "stock.source_pool_seed_day_facts",
        "stock.source_d0_scenario_day_facts",
        "stock.source_d0_launch_day_facts",
        "stock.source_d0_formal_admission_day_facts",
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_rule_evaluations",
        "stock.source_research_enrollments",
        "stock.source_t0_early_observation_facts",
        "stock.source_rps_pulse_t0_change_facts",
        "stock.source_research_observation_segment_days",
        "stock.source_cohort_group_stats",
        "stock.source_fep_baseline_facts",
        "stock.source_research_events",
    )
    id_names = (
        "dependent_target_sessions",
        "profile_sessions",
        "profile_revisions",
        "rule_fact_ids",
        "d0_fact_ids",
        "seed_fact_ids",
        "enrollment_ids",
        "t0_fact_ids",
        "d1_structure_ids",
        "observation_segment_ids",
    )
    return {
        "dependent_rows": {
            **{table: int(row[index]) for index, table in enumerate(table_names[:12])},
            **{
                table: int(row[12 + index])
                for index, table in enumerate(table_names[12:])
            },
        },
        **{name: tuple(row[15 + index] or ()) for index, name in enumerate(id_names)},
        "research_event_ids": tuple(row[25] or ()),
        "fep_baseline_ids": tuple(row[26] or ()),
    }


def _cross_section_cleanup_dependencies(
    cursor: Any, *, target_session: date, source_capture_id: int
) -> dict[str, Any]:
    """核对横截面所派生Profile/Seed/D0/D1及真实相邻登记引用。"""
    from stock_a.storage.postgres.research_day_facts import _D1_PRODUCER_VERSION_7

    cursor.execute(
        """WITH RECURSIVE scope AS (
               SELECT %s::date AS target_session, %s::bigint AS source_capture_id
           ), root_profiles AS (
               SELECT DISTINCT p.profile_revision
               FROM stock.source_symbol_price_profiles p,scope s
               WHERE p.target_session=s.target_session
                 AND p.source_capture_id=s.source_capture_id
           ), root_d1 AS (
               SELECT d.id,d.target_session
               FROM stock.source_d1_structure_day_facts d,scope s
               WHERE d.target_session=s.target_session
                 AND d.source_capture_id=s.source_capture_id
                 AND d.producer_version=%s
           ), root_rules AS (
               SELECT r.id,r.target_session
               FROM stock.source_rule_evaluations r,scope s
               WHERE r.target_session=s.target_session
                 AND r.source_capture_id=s.source_capture_id
                 AND (
                     r.profile_revision IN (SELECT profile_revision FROM root_profiles)
                     OR EXISTS (SELECT 1 FROM root_d1)
                 )
           ), root_d0 AS (
               SELECT d.id,d.target_session
               FROM stock.source_d0_launch_day_facts d,scope s
               WHERE d.target_session=s.target_session
                 AND d.source_capture_id=s.source_capture_id
                 AND EXISTS (SELECT 1 FROM root_profiles)
           ), root_seed AS (
               SELECT f.id FROM stock.source_pool_seed_day_facts f,scope s
               WHERE f.target_session=s.target_session
                 AND f.source_capture_id=s.source_capture_id
                 AND f.profile_revision IN (SELECT profile_revision FROM root_profiles)
           ), affected_enrollments(id) AS (
               SELECT e.id
               FROM stock.source_research_enrollments e
               WHERE e.rule_fact_id IN (SELECT id FROM root_rules)
                  OR e.previous_rule_fact_id IN (SELECT id FROM root_rules)
                  OR e.d0_fact_id IN (SELECT id FROM root_d0)
               UNION
               SELECT child.id
               FROM stock.source_research_enrollments child
               JOIN affected_enrollments parent
                 ON child.parent_enrollment_id=parent.id
           ), affected_enrollment_dates AS (
               SELECT a.id,COALESCE(r.target_session,d.target_session) AS target_session
               FROM affected_enrollments a
               LEFT JOIN stock.source_research_enrollments e ON e.id=a.id
               LEFT JOIN stock.source_rule_evaluations r ON r.id=e.rule_fact_id
               LEFT JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
           ), affected_t0 AS (
               SELECT t.id,t.enrollment_id
               FROM stock.source_t0_early_observation_facts t,scope s
               WHERE ((t.target_session=s.target_session
                      AND t.source_capture_id=s.source_capture_id)
                     AND (EXISTS (SELECT 1 FROM root_profiles)
                          OR EXISTS (SELECT 1 FROM root_d1)))
                  OR t.enrollment_id IN (SELECT id FROM affected_enrollments)
                  OR t.rule_fact_id IN (SELECT id FROM root_rules)
                  OR t.d0_fact_id IN (SELECT id FROM root_d0)
                  OR t.seed_fact_id IN (SELECT id FROM root_seed)
           )
           SELECT
               (SELECT count(*) FROM stock.source_symbol_price_profiles p,scope s
                WHERE p.target_session=s.target_session
                  AND p.source_capture_id=s.source_capture_id
                  AND p.profile_revision IN (SELECT profile_revision FROM root_profiles)),
               (SELECT count(*) FROM root_seed),
               (SELECT count(*) FROM stock.source_d0_scenario_day_facts f,scope s
                WHERE f.target_session=s.target_session
                  AND f.source_capture_id=s.source_capture_id
                  AND EXISTS (SELECT 1 FROM root_profiles)),
               (SELECT count(*) FROM stock.source_d0_launch_day_facts f,scope s
                WHERE f.target_session=s.target_session
                  AND f.source_capture_id=s.source_capture_id
                  AND EXISTS (SELECT 1 FROM root_profiles)),
               (SELECT count(*) FROM stock.source_d0_formal_admission_day_facts f,scope s
                WHERE f.target_session=s.target_session
                  AND f.source_capture_id=s.source_capture_id
                  AND EXISTS (SELECT 1 FROM root_profiles)),
               (SELECT count(*) FROM root_d1),
               (SELECT count(*) FROM stock.source_d1_material_anchor_day_facts a
                JOIN root_d1 d ON d.id=a.d1_structure_id),
               (SELECT count(*) FROM root_rules),
               (SELECT count(*) FROM affected_enrollments),
               (SELECT count(*) FROM affected_t0),
               (SELECT count(*) FROM stock.source_rps_pulse_t0_change_facts p
                WHERE p.source_t0_fact_id IN (SELECT id FROM affected_t0)
                   OR p.enrollment_id IN (SELECT id FROM affected_enrollments)),
               (SELECT count(*) FROM stock.source_research_observation_segment_days o
                WHERE o.anchor_rule_fact_id IN (SELECT id FROM root_rules)
                   OR o.evidence_rule_fact_id IN (SELECT id FROM root_rules)
                   OR o.source_enrollment_id IN (SELECT id FROM affected_enrollments)
                   OR o.parent_enrollment_id IN (SELECT id FROM affected_enrollments)
                   OR o.linked_enrollment_id IN (SELECT id FROM affected_enrollments)),
               COALESCE((
                   SELECT array_agg(DISTINCT d.target_session ORDER BY d.target_session)
                   FROM affected_enrollment_dates d,scope s
                   WHERE d.target_session IS NOT NULL
                     AND d.target_session<>s.target_session
               ),ARRAY[]::date[])""",
        (
            target_session,
            source_capture_id,
            _D1_PRODUCER_VERSION_7,
        ),
    )
    row = cursor.fetchone()
    table_names = (
        "stock.source_symbol_price_profiles",
        "stock.source_pool_seed_day_facts",
        "stock.source_d0_scenario_day_facts",
        "stock.source_d0_launch_day_facts",
        "stock.source_d0_formal_admission_day_facts",
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_rule_evaluations",
        "stock.source_research_enrollments",
        "stock.source_t0_early_observation_facts",
        "stock.source_rps_pulse_t0_change_facts",
        "stock.source_research_observation_segment_days",
    )
    return {
        "dependent_rows": {
            table: int(row[index]) for index, table in enumerate(table_names)
        },
        "dependent_target_sessions": tuple(row[12] or ()),
    }


def _cross_section_cleanup_target_ids(
    cursor: Any, *, target_session: date, source_capture_id: int
) -> dict[str, tuple[Any, ...]]:
    """在同一事务中冻结需要清理的准确业务ID集合。"""
    from stock_a.storage.postgres.research_day_facts import _D1_PRODUCER_VERSION_7

    cursor.execute(
        """WITH RECURSIVE scope AS (
               SELECT %s::date AS target_session,%s::bigint AS source_capture_id
           ), root_profiles AS (
               SELECT DISTINCT p.profile_revision
               FROM stock.source_symbol_price_profiles p,scope s
               WHERE p.target_session=s.target_session
                 AND p.source_capture_id=s.source_capture_id
           ), root_d1 AS (
               SELECT d.id FROM stock.source_d1_structure_day_facts d,scope s
               WHERE d.target_session=s.target_session
                 AND d.source_capture_id=s.source_capture_id
                 AND d.producer_version=%s
           ), root_rules AS (
               SELECT r.id FROM stock.source_rule_evaluations r,scope s
               WHERE r.target_session=s.target_session
                 AND r.source_capture_id=s.source_capture_id
                 AND (
                     r.profile_revision IN (SELECT profile_revision FROM root_profiles)
                     OR EXISTS (SELECT 1 FROM root_d1)
                 )
           ), root_d0 AS (
               SELECT d.id FROM stock.source_d0_launch_day_facts d,scope s
               WHERE d.target_session=s.target_session
                 AND d.source_capture_id=s.source_capture_id
                 AND EXISTS (SELECT 1 FROM root_profiles)
           ), root_seed AS (
               SELECT f.id FROM stock.source_pool_seed_day_facts f,scope s
               WHERE f.target_session=s.target_session
                 AND f.source_capture_id=s.source_capture_id
                 AND f.profile_revision IN (SELECT profile_revision FROM root_profiles)
           ), affected_enrollments(id) AS (
               SELECT e.id
               FROM stock.source_research_enrollments e
               WHERE e.rule_fact_id IN (SELECT id FROM root_rules)
                  OR e.previous_rule_fact_id IN (SELECT id FROM root_rules)
                  OR e.d0_fact_id IN (SELECT id FROM root_d0)
               UNION
               SELECT child.id
               FROM stock.source_research_enrollments child
               JOIN affected_enrollments parent
                 ON child.parent_enrollment_id=parent.id
           ), affected_t0(id) AS (
               SELECT t.id
               FROM stock.source_t0_early_observation_facts t,scope s
               WHERE ((t.target_session=s.target_session
                       AND t.source_capture_id=s.source_capture_id)
                      AND (EXISTS (SELECT 1 FROM root_profiles)
                           OR EXISTS (SELECT 1 FROM root_d1)))
                  OR t.enrollment_id IN (SELECT id FROM affected_enrollments)
                  OR t.rule_fact_id IN (SELECT id FROM root_rules)
                  OR t.d0_fact_id IN (SELECT id FROM root_d0)
                  OR t.seed_fact_id IN (SELECT id FROM root_seed)
           )
           SELECT
             COALESCE((SELECT array_agg(profile_revision ORDER BY profile_revision)
                       FROM root_profiles),ARRAY[]::text[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_rules),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_d0),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_seed),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_enrollments),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_t0),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM root_d1),ARRAY[]::bigint[])""",
        (target_session, source_capture_id, _D1_PRODUCER_VERSION_7),
    )
    row = cursor.fetchone()
    names = (
        "profile_revisions",
        "rule_fact_ids",
        "d0_fact_ids",
        "seed_fact_ids",
        "enrollment_ids",
        "t0_fact_ids",
        "d1_structure_ids",
    )
    return {name: tuple(row[index] or ()) for index, name in enumerate(names)}


def _cross_section_d1_descendant_dependencies(
    cursor: Any,
    *,
    target_sessions: Sequence[date],
    source_capture_id: int,
    cutoff: date,
) -> dict[str, Any]:
    """找出D1 R4实际历史窗口覆盖被改会话的后继事实及其相邻登记。"""
    from stock_a.storage.postgres.research_day_facts import _D1_PRODUCER_VERSION_7

    selected = tuple(sorted(set(target_sessions)))
    cursor.execute(
        """WITH RECURSIVE selected_sessions AS (
               SELECT unnest(%s::date[]) AS target_session
           ), affected_d1 AS (
               SELECT DISTINCT d.id,d.target_session
               FROM stock.source_d1_structure_day_facts d
               WHERE d.source_capture_id=%s AND d.producer_version=%s
                 AND d.target_session<=%s
                 AND d.target_session>=(SELECT min(target_session) FROM selected_sessions)
                 AND d.target_session<>ALL(%s::date[])
                 AND EXISTS (
                     SELECT 1 FROM selected_sessions s
                     WHERE d.history_window_start<=s.target_session
                       AND s.target_session<d.target_session
                 )
           ), affected_rules AS (
               SELECT DISTINCT r.id,r.target_session,r.rule_id,r.rule_version,
                               r.profile_revision,r.history_session_count,
                               r.d1_history_session_count,r.event_window_start,
                               r.calendar_revision
               FROM stock.source_rule_evaluations r
               WHERE r.source_capture_id=%s
                 AND r.target_session IN (SELECT target_session FROM affected_d1)
           ), affected_enrollments(id) AS (
               SELECT e.id
               FROM stock.source_research_enrollments e
               WHERE e.rule_fact_id IN (SELECT id FROM affected_rules)
                  OR e.previous_rule_fact_id IN (SELECT id FROM affected_rules)
               UNION
               SELECT child.id
               FROM stock.source_research_enrollments child
               JOIN affected_enrollments parent
                 ON child.parent_enrollment_id=parent.id
           ), affected_enrollment_dates AS (
               SELECT a.id,r.target_session
               FROM affected_enrollments a
               JOIN stock.source_research_enrollments e ON e.id=a.id
               JOIN stock.source_rule_evaluations r ON r.id=e.rule_fact_id
           ), affected_t0 AS (
               SELECT t.id
               FROM stock.source_t0_early_observation_facts t
               WHERE t.source_capture_id=%s
                 AND (t.target_session IN (SELECT target_session FROM affected_d1)
                      OR t.rule_fact_id IN (SELECT id FROM affected_rules)
                      OR t.enrollment_id IN (SELECT id FROM affected_enrollments))
           ), affected_observation_segments AS (
               SELECT o.id,o.t0_session,o.observation_session
               FROM stock.source_research_observation_segment_days o
               WHERE o.anchor_rule_fact_id IN (SELECT id FROM affected_rules)
                  OR o.evidence_rule_fact_id IN (SELECT id FROM affected_rules)
                  OR o.source_enrollment_id IN (SELECT id FROM affected_enrollments)
                  OR o.parent_enrollment_id IN (SELECT id FROM affected_enrollments)
                  OR o.linked_enrollment_id IN (SELECT id FROM affected_enrollments)
           )
           SELECT
             (SELECT count(*) FROM affected_d1),
             (SELECT count(*) FROM stock.source_d1_material_anchor_day_facts a
              WHERE a.d1_structure_id IN (SELECT id FROM affected_d1)),
             (SELECT count(*) FROM affected_rules),
             (SELECT count(*) FROM affected_enrollments),
             (SELECT count(*) FROM affected_t0),
             (SELECT count(*) FROM stock.source_rps_pulse_t0_change_facts p
              WHERE p.source_t0_fact_id IN (SELECT id FROM affected_t0)
                 OR p.enrollment_id IN (SELECT id FROM affected_enrollments)),
             (SELECT count(*) FROM affected_observation_segments),
             COALESCE((
                 SELECT array_agg(target_session ORDER BY target_session)
                 FROM (
                     SELECT target_session FROM affected_d1
                     UNION
                     SELECT target_session FROM affected_enrollment_dates
                 ) q
             ),ARRAY[]::date[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_d1),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_rules),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_enrollments),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_t0),ARRAY[]::bigint[]),
             COALESCE((SELECT array_agg(id ORDER BY id) FROM affected_observation_segments),ARRAY[]::bigint[])""",
        (
            list(selected),
            source_capture_id,
            _D1_PRODUCER_VERSION_7,
            cutoff,
            list(selected),
            source_capture_id,
            source_capture_id,
        ),
    )
    row = cursor.fetchone()
    table_names = (
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_rule_evaluations",
        "stock.source_research_enrollments",
        "stock.source_t0_early_observation_facts",
        "stock.source_rps_pulse_t0_change_facts",
        "stock.source_research_observation_segment_days",
    )
    id_names = (
        "d1_structure_ids",
        "rule_fact_ids",
        "enrollment_ids",
        "t0_fact_ids",
        "observation_segment_ids",
    )
    return {
        "dependent_rows": {
            table: int(row[index]) for index, table in enumerate(table_names)
        },
        "dependent_target_sessions": tuple(row[7] or ()),
        "dependent_ids": {
            name: tuple(row[8 + index] or ()) for index, name in enumerate(id_names)
        },
    }


def _cleanup_fk_blockers(cursor: Any, spec: Mapping[str, Any]) -> list[dict[str, str]]:
    cleanup_tables = tuple(
        str(table)
        for table in spec.get(
            "cleanup_tables", (spec["child_table"], spec["parent_table"])
        )
    )
    cleanup_table_set = set(cleanup_tables)
    relations_by_parent: dict[str, set[str]] = {
        table: set() for table in cleanup_tables
    }
    details: list[tuple[str, str, str, str]] = []
    for table in cleanup_tables:
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
            relations_by_parent.setdefault(referenced_table, set()).add(
                referencing_table
            )
            details.append(
                (
                    referenced_table,
                    referencing_table,
                    str(constraint_name),
                    str(definition),
                )
            )

    row_checked_fk_tables = set(spec.get("row_checked_fk_tables", ()))
    blockers = []
    for table in cleanup_tables:
        for referencing_table in sorted(
            relations_by_parent[table] - cleanup_table_set - row_checked_fk_tables
        ):
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
    for referenced_table, dependent_table in spec.get("required_relations", ()):
        if dependent_table not in relations_by_parent.get(referenced_table, set()):
            blockers.append(
                {
                    "referenced_table": referenced_table,
                    "dependent_table": dependent_table,
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
    *,
    member_version: str | None = None,
    scope_key: str | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
) -> list[dict[str, Any]]:
    """只按capture键盘点现役派生结果和其真实下游依赖。"""
    if module_code == "CORE_PROFILE":
        cursor.execute(
            """SELECT target_session,source_capture_id,profile_revision,
                      producer_version,count(*)
               FROM stock.source_symbol_price_profiles
               WHERE target_session=ANY(%s) AND target_session<=%s
                 AND source_capture_id IS NOT NULL
               GROUP BY target_session,source_capture_id,profile_revision,producer_version
               ORDER BY target_session,source_capture_id,profile_revision,producer_version""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[3]),
                "profile_revision": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "profile_revision": str(row[2]),
                    "producer_version": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "MARKET_BREADTH":
        cursor.execute(
            """SELECT d.target_session,d.source_capture_id,d.calendar_revision,
                      d.contract_version,count(DISTINCT d.id),
                      count(DISTINCT s.source_symbol)
               FROM stock.source_market_breadth_days d
               LEFT JOIN stock.source_market_breadth_symbols s
                 ON s.breadth_day_id=d.id
               WHERE d.target_session=ANY(%s) AND d.target_session<=%s
                 AND d.source_capture_id IS NOT NULL
               GROUP BY d.target_session,d.source_capture_id,
                        d.calendar_revision,d.contract_version
               ORDER BY d.target_session,d.source_capture_id,
                        d.calendar_revision,d.contract_version""",
            (list(target_sessions), cutoff),
        )
        versions = [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[3]),
                "calendar_revision": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": int(row[5]),
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "calendar_revision": str(row[2]),
                    "contract_version": str(row[3]),
                    "formal_breadth_keys": [],
                },
            }
            for row in cursor.fetchall()
        ]
        by_day_cap: dict[tuple[date, int], dict[str, Any]] = {}
        for version in versions:
            target = version["target_session"]
            capture_id = version["source_version"]["source_capture_id"]
            revision = version["calendar_revision"]
            contract = version["algorithm_version"]
            by_day_cap.setdefault((target, capture_id), version)
            cursor.execute(
                """SELECT count(*)::bigint,
                          coalesce(array_agg(DISTINCT target_session
                                             ORDER BY target_session),'{}'::date[])
                   FROM stock.source_market_breadth_change_days
                   WHERE (target_session=%s AND target_source_capture_id=%s
                          AND target_calendar_revision=%s
                          AND target_contract_version=%s)
                      OR (before_session=%s AND before_source_capture_id=%s
                          AND before_calendar_revision=%s
                          AND before_contract_version=%s)""",
                (
                    target,
                    capture_id,
                    revision,
                    contract,
                    target,
                    capture_id,
                    revision,
                    contract,
                ),
            )
            dependent_count, dependent_targets = cursor.fetchone()
            version["dependent_rows"] = {
                "stock.source_market_breadth_change_days": int(dependent_count),
                "stock.r03_formal_market_breadth_facts": 0,
            }
            version["dependent_target_sessions"] = tuple(dependent_targets or ())
        cursor.execute(
            """SELECT trade_date, scope_key, universe_version_t, universe_version_t3, contract_version
               FROM stock.r03_formal_market_breadth_facts
               WHERE trade_date=ANY(%s) AND trade_date<=%s
                 AND (%s::text IS NULL OR scope_key=%s)
                 AND (%s::text IS NULL OR universe_version_t=%s)
                 AND (%s::text IS NULL OR universe_version_t3=%s)
               ORDER BY trade_date, scope_key, universe_version_t, universe_version_t3, contract_version""",
            (
                list(target_sessions),
                cutoff,
                scope_key,
                scope_key,
                universe_version_t,
                universe_version_t,
                universe_version_t3,
                universe_version_t3,
            ),
        )
        formal_only_filter = (
            scope_key is not None
            or universe_version_t is not None
            or universe_version_t3 is not None
        )
        if formal_only_filter:
            versions = []
            by_day_cap = {}
        for f_day, f_scope, f_ut, f_ut3, f_contract in cursor.fetchall():
            f_cap: int | None = None
            if str(f_scope).startswith("BAOSTOCK_CAP_"):
                cap_part = str(f_scope).split(":", 1)[0].removeprefix("BAOSTOCK_CAP_")
                if cap_part.isdigit():
                    f_cap = int(cap_part)
            formal_key = {
                "scope_key": str(f_scope),
                "universe_version_t": str(f_ut),
                "universe_version_t3": str(f_ut3),
                "contract_version": str(f_contract),
            }
            if f_cap is not None and (f_day, f_cap) in by_day_cap:
                target_v = by_day_cap[(f_day, f_cap)]
                target_v["dependent_rows"]["stock.r03_formal_market_breadth_facts"] += 1
                target_v["version_key"]["formal_breadth_keys"].append(formal_key)
            else:
                versions.append(
                    {
                        "target_session": f_day,
                        "source_version": {"scope_key": str(f_scope)},
                        "algorithm_version": str(f_contract),
                        "calendar_revision": "",
                        "parent_rows": 0,
                        "child_rows": 0,
                        "dependent_rows": {
                            "stock.source_market_breadth_change_days": 0,
                            "stock.r03_formal_market_breadth_facts": 1,
                        },
                        "dependent_target_sessions": (),
                        "version_key": {
                            "source_capture_id": f_cap,
                            "calendar_revision": None,
                            "contract_version": str(f_contract),
                            "formal_breadth_keys": [formal_key],
                        },
                    }
                )
        return versions

    if module_code == "D1":
        cursor.execute(
            """SELECT d.target_session,d.source_capture_id,
                      d.d1_history_session_count,d.producer_version,
                      d.calendar_revision,count(DISTINCT d.id),count(DISTINCT a.id)
               FROM stock.source_d1_structure_day_facts d
               LEFT JOIN stock.source_d1_material_anchor_day_facts a
                 ON a.d1_structure_id=d.id
               WHERE d.target_session=ANY(%s) AND d.target_session<=%s
                 AND d.source_capture_id IS NOT NULL
               GROUP BY d.target_session,d.source_capture_id,
                        d.d1_history_session_count,d.producer_version,d.calendar_revision
               ORDER BY d.target_session,d.source_capture_id,
                        d.d1_history_session_count,d.producer_version,d.calendar_revision""",
            (list(target_sessions), cutoff),
        )
        rows = cursor.fetchall()
        versions = []
        dependent_by_scope: dict[
            tuple[Any, ...], tuple[int, int, list[dict[str, Any]]]
        ] = {}
        for row in rows:
            target_session = row[0]
            capture_id = int(row[1])
            history_count = int(row[2])
            producer_version = str(row[3])
            calendar_revision = str(row[4])
            scope = (target_session, capture_id, history_count, calendar_revision)
            if scope not in dependent_by_scope:
                cursor.execute(
                    """SELECT count(*) FROM stock.source_rule_evaluations
                       WHERE target_session=%s AND source_capture_id=%s
                         AND d1_history_session_count=%s AND calendar_revision=%s""",
                    scope,
                )
                rule_rows = int(cursor.fetchone()[0])
                cursor.execute(
                    """SELECT count(DISTINCT e.id)
                       FROM stock.source_research_enrollments e
                       JOIN stock.source_rule_evaluations r ON r.id=e.rule_fact_id
                       WHERE r.target_session=%s AND r.source_capture_id=%s
                         AND r.d1_history_session_count=%s AND r.calendar_revision=%s""",
                    scope,
                )
                enrollment_rows = int(cursor.fetchone()[0])
                cursor.execute(
                    """SELECT current_rule.target_session,count(DISTINCT e.id)
                       FROM stock.source_research_enrollments e
                       JOIN stock.source_rule_evaluations current_rule
                         ON current_rule.id=e.rule_fact_id
                       JOIN stock.source_rule_evaluations previous_rule
                         ON previous_rule.id=e.previous_rule_fact_id
                       WHERE previous_rule.target_session=%s
                         AND previous_rule.source_capture_id=%s
                         AND previous_rule.d1_history_session_count=%s
                         AND previous_rule.calendar_revision=%s
                         AND current_rule.target_session>%s
                       GROUP BY current_rule.target_session
                       ORDER BY current_rule.target_session""",
                    (*scope, target_session),
                )
                later_sessions = [
                    {"target_session": target_row[0], "rows": int(target_row[1])}
                    for target_row in cursor.fetchall()
                ]
                dependent_by_scope[scope] = (
                    rule_rows,
                    enrollment_rows,
                    later_sessions,
                )
            rule_rows, enrollment_rows, later_sessions = dependent_by_scope[scope]
            versions.append(
                {
                    "target_session": target_session,
                    "source_version": {"source_capture_id": capture_id},
                    "algorithm_version": producer_version,
                    "calendar_revision": calendar_revision,
                    "d1_history_session_count": history_count,
                    "parent_rows": int(row[5]),
                    "child_rows": int(row[6]),
                    "dependent_rows": {
                        "stock.source_rule_evaluations": rule_rows,
                        "stock.source_research_enrollments": enrollment_rows,
                    },
                    "dependent_target_sessions": [],
                    "blocking_dependent_target_sessions": later_sessions,
                    "version_key": {
                        "source_capture_id": capture_id,
                        "d1_history_session_count": history_count,
                        "producer_version": producer_version,
                        "calendar_revision": calendar_revision,
                    },
                }
            )
        return versions

    if module_code == "D0":
        cursor.execute(
            """WITH scopes AS (
                   SELECT target_session,source_capture_id
                   FROM stock.source_d0_scenario_day_facts
                   WHERE target_session=ANY(%s) AND target_session<=%s
                     AND source_capture_id IS NOT NULL
                   UNION
                   SELECT target_session,source_capture_id
                   FROM stock.source_pool_seed_day_facts
                   WHERE target_session=ANY(%s) AND target_session<=%s
                     AND source_capture_id IS NOT NULL
                   UNION
                   SELECT target_session,source_capture_id
                   FROM stock.source_d0_launch_day_facts
                   WHERE target_session=ANY(%s) AND target_session<=%s
                     AND source_capture_id IS NOT NULL
               )
               SELECT s.target_session,s.source_capture_id,
                 (SELECT count(*) FROM stock.source_d0_scenario_day_facts d
                  WHERE d.target_session=s.target_session
                    AND d.source_capture_id=s.source_capture_id),
                 (SELECT count(*) FROM stock.source_pool_seed_day_facts p
                  WHERE p.target_session=s.target_session
                    AND p.source_capture_id=s.source_capture_id),
                 (SELECT count(*) FROM stock.source_d0_launch_day_facts d
                  WHERE d.target_session=s.target_session
                    AND d.source_capture_id=s.source_capture_id),
                 (SELECT count(DISTINCT e.id)
                  FROM stock.source_research_enrollments e
                  JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
                  WHERE d.target_session=s.target_session
                    AND d.source_capture_id=s.source_capture_id),
                 jsonb_build_object(
                   'D0',coalesce((SELECT jsonb_agg(DISTINCT producer_version
                                                    ORDER BY producer_version)
                     FROM stock.source_d0_scenario_day_facts d
                     WHERE d.target_session=s.target_session
                       AND d.source_capture_id=s.source_capture_id),'[]'::jsonb),
                   'SEED',coalesce((SELECT jsonb_agg(DISTINCT producer_version
                                                      ORDER BY producer_version)
                     FROM stock.source_pool_seed_day_facts p
                     WHERE p.target_session=s.target_session
                       AND p.source_capture_id=s.source_capture_id),'[]'::jsonb),
                   'D0_LAUNCH',coalesce((SELECT jsonb_agg(DISTINCT producer_version
                                                          ORDER BY producer_version)
                     FROM stock.source_d0_launch_day_facts d
                     WHERE d.target_session=s.target_session
                       AND d.source_capture_id=s.source_capture_id),'[]'::jsonb)
                 )
               FROM scopes s
               ORDER BY s.target_session,s.source_capture_id""",
            (
                list(target_sessions),
                cutoff,
                list(target_sessions),
                cutoff,
                list(target_sessions),
                cutoff,
            ),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": json.dumps(
                    row[6], ensure_ascii=False, sort_keys=True
                ),
                "parent_rows": int(row[2]),
                "child_rows": 0,
                "dependent_rows": {
                    "stock.source_pool_seed_day_facts": int(row[3]),
                    "stock.source_d0_launch_day_facts": int(row[4]),
                    "stock.source_research_enrollments": int(row[5]),
                },
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "algorithm_versions": row[6],
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "FOCUS_FORMAL":
        cursor.execute(
            """SELECT target_session,source_capture_id,profile_revision,
                      raw_producer_version,calendar_revision,count(*)
               FROM stock.source_d0_formal_admission_day_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
                 AND source_capture_id IS NOT NULL
                 AND admission_contract='STOCK_A_D0_EVENT_ADMISSION_R1'
               GROUP BY target_session,source_capture_id,profile_revision,
                        raw_producer_version,calendar_revision
               ORDER BY target_session,source_capture_id,profile_revision,
                        raw_producer_version,calendar_revision""",
            (list(target_sessions), cutoff),
        )
        rows = cursor.fetchall()
        versions: list[dict[str, Any]] = []
        for row in rows:
            target_session = row[0]
            capture_id = int(row[1])
            profile_revision = row[2]
            raw_producer_version = str(row[3])
            calendar_revision = str(row[4])
            cursor.execute(
                """SELECT count(*) FROM stock.source_rule_evaluations
                   WHERE target_session=%s AND source_capture_id=%s
                     AND profile_revision IS NOT DISTINCT FROM %s""",
                (target_session, capture_id, profile_revision),
            )
            rule_rows = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.source_t0_early_observation_facts t
                   JOIN stock.source_rule_evaluations r ON r.id=t.rule_fact_id
                   WHERE r.target_session=%s AND r.source_capture_id=%s
                     AND r.profile_revision IS NOT DISTINCT FROM %s""",
                (target_session, capture_id, profile_revision),
            )
            t0_rows = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(DISTINCT e.id) FROM stock.source_research_enrollments e
                   JOIN stock.source_rule_evaluations r ON r.id=e.rule_fact_id
                   WHERE r.target_session=%s AND r.source_capture_id=%s
                     AND r.profile_revision IS NOT DISTINCT FROM %s""",
                (target_session, capture_id, profile_revision),
            )
            enrollment_rows = int(cursor.fetchone()[0])
            versions.append(
                {
                    "target_session": target_session,
                    "source_version": {"source_capture_id": capture_id},
                    "algorithm_version": raw_producer_version,
                    "profile_revision": profile_revision,
                    "parent_rows": int(row[5]),
                    "child_rows": 0,
                    "dependent_rows": {
                        "stock.source_rule_evaluations": rule_rows,
                        "stock.source_research_enrollments": enrollment_rows,
                        "stock.source_t0_early_observation_facts": t0_rows,
                    },
                    "version_key": {
                        "source_capture_id": capture_id,
                        "profile_revision": profile_revision,
                        "raw_producer_version": raw_producer_version,
                        "calendar_revision": calendar_revision,
                        "admission_contract": "STOCK_A_D0_EVENT_ADMISSION_R1",
                    },
                }
            )
        return versions

    if module_code == "R03_SECTOR_NATIVE":
        cursor.execute(
            """SELECT trade_date, scope_key, member_version, contract_version, count(*)
               FROM stock.r03_sector_native_day_facts
               WHERE trade_date=ANY(%s) AND trade_date<=%s
                 AND (%s::text IS NULL OR member_version=%s)
                 AND (%s::text IS NULL OR scope_key=%s)
               GROUP BY trade_date, scope_key, member_version, contract_version
               ORDER BY trade_date, scope_key, member_version, contract_version""",
            (
                list(target_sessions),
                cutoff,
                member_version,
                member_version,
                scope_key,
                scope_key,
            ),
        )
        native_rows = cursor.fetchall()
        native_versions: list[dict[str, Any]] = []
        grouped_min_day: dict[tuple[str, str, str], date] = {}
        grouped_selected_days: dict[tuple[str, str, str], set[date]] = {}
        for row in native_rows:
            gkey = (str(row[1]), str(row[2]), str(row[3]))
            grouped_selected_days.setdefault(gkey, set()).add(row[0])
            if gkey not in grouped_min_day or row[0] < grouped_min_day[gkey]:
                grouped_min_day[gkey] = row[0]

        subsequent_attached: set[tuple[str, str, str]] = set()
        for row in native_rows:
            t_day = row[0]
            s_key, m_ver, c_ver = str(row[1]), str(row[2]), str(row[3])
            gkey = (s_key, m_ver, c_ver)
            is_root = gkey not in subsequent_attached
            extra_days: list[date] = []
            if is_root:
                subsequent_attached.add(gkey)
                min_day = grouped_min_day[gkey]
                selected_days = grouped_selected_days[gkey]
                cursor.execute(
                    """
                    SELECT DISTINCT d FROM (
                        SELECT trade_date AS d FROM stock.r03_sector_native_day_facts
                        WHERE trade_date > %s AND trade_date <= %s AND scope_key=%s AND member_version=%s AND contract_version=%s
                        UNION
                        SELECT trade_date AS d FROM stock.r03_sector_loo_support_facts
                        WHERE trade_date >= %s AND trade_date <= %s AND scope_key=%s AND member_version=%s AND contract_version=%s
                        UNION
                        SELECT trade_date AS d FROM stock.r03_sector_overlap_clusters
                        WHERE trade_date >= %s AND trade_date <= %s AND scope_key=%s AND member_version=%s AND contract_version=%s
                        UNION
                        SELECT trade_date AS d FROM stock.r03_sector_rotation_board_day_facts
                        WHERE trade_date >= %s AND trade_date <= %s AND scope_key=%s AND member_version=%s AND contract_version=%s
                        UNION
                        SELECT s.trade_date AS d FROM stock.r03_sector_rotation_day_states s
                        JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
                        WHERE s.trade_date >= %s AND s.trade_date <= %s AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                    ) sub
                    WHERE NOT (d = ANY(%s))
                    ORDER BY d
                    """,
                    (
                        min_day,
                        cutoff,
                        s_key,
                        m_ver,
                        c_ver,
                        min_day,
                        cutoff,
                        s_key,
                        m_ver,
                        c_ver,
                        min_day,
                        cutoff,
                        s_key,
                        m_ver,
                        c_ver,
                        min_day,
                        cutoff,
                        s_key,
                        m_ver,
                        c_ver,
                        min_day,
                        cutoff,
                        s_key,
                        m_ver,
                        c_ver,
                        list(selected_days),
                    ),
                )
                extra_days = [r[0] for r in cursor.fetchall()]

            scope_days = [t_day, *extra_days]
            all_affected_days = sorted(grouped_selected_days[gkey] | set(extra_days))
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_loo_candidate_facts c
                   JOIN stock.r03_sector_loo_support_facts f ON f.id=c.loo_fact_id
                   WHERE f.trade_date=ANY(%s) AND f.scope_key=%s AND f.member_version=%s AND f.contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            loo_cand_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_loo_support_facts
                   WHERE trade_date=ANY(%s) AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            loo_sup_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_overlap_board_facts
                   WHERE trade_date=ANY(%s) AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            ov_board_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_overlap_pair_facts
                   WHERE trade_date=ANY(%s) AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            ov_pair_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_overlap_clusters
                   WHERE trade_date=ANY(%s) AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            ov_cluster_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_rotation_board_day_facts
                   WHERE trade_date=ANY(%s) AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            rot_board_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_rotation_day_states s
                   JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
                   WHERE s.trade_date=ANY(%s) AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s""",
                (scope_days, s_key, m_ver, c_ver),
            )
            rot_state_cnt = int(cursor.fetchone()[0])
            extra_native_cnt = 0
            orphan_bm_cnt = 0
            orphan_ep_cnt = 0
            if extra_days:
                cursor.execute(
                    """SELECT count(*) FROM stock.r03_sector_native_day_facts
                       WHERE trade_date=ANY(%s) AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                    (extra_days, s_key, m_ver, c_ver),
                )
                extra_native_cnt = int(cursor.fetchone()[0])
            if is_root:
                cursor.execute(
                    """SELECT count(*) FROM stock.r03_sector_rotation_basket_members bm
                       JOIN stock.r03_sector_rotation_episodes e ON e.id=bm.episode_id
                       WHERE e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id AND NOT (s.trade_date=ANY(%s))
                         )""",
                    (s_key, m_ver, c_ver, all_affected_days),
                )
                orphan_bm_cnt = int(cursor.fetchone()[0])
                cursor.execute(
                    """SELECT count(*) FROM stock.r03_sector_rotation_episodes e
                       WHERE e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id AND NOT (s.trade_date=ANY(%s))
                         )""",
                    (s_key, m_ver, c_ver, all_affected_days),
                )
                orphan_ep_cnt = int(cursor.fetchone()[0])
            native_versions.append(
                {
                    "target_session": t_day,
                    "source_version": {"scope_key": s_key},
                    "algorithm_version": c_ver,
                    "parent_rows": int(row[4]),
                    "child_rows": 0,
                    "dependent_rows": {
                        "stock.r03_sector_loo_candidate_facts": loo_cand_cnt,
                        "stock.r03_sector_loo_support_facts": loo_sup_cnt,
                        "stock.r03_sector_overlap_pair_facts": ov_pair_cnt,
                        "stock.r03_sector_overlap_board_facts": ov_board_cnt,
                        "stock.r03_sector_overlap_clusters": ov_cluster_cnt,
                        "stock.r03_sector_rotation_board_day_facts": rot_board_cnt,
                        "stock.r03_sector_rotation_day_states": rot_state_cnt,
                        "stock.r03_sector_rotation_basket_members": orphan_bm_cnt,
                        "stock.r03_sector_rotation_episodes": orphan_ep_cnt,
                        "stock.r03_sector_native_day_facts": extra_native_cnt,
                    },
                    "dependent_target_sessions": tuple(extra_days),
                    "version_key": {
                        "scope_key": s_key,
                        "member_version": m_ver,
                        "contract_version": c_ver,
                    },
                }
            )
        return native_versions

    if module_code == "R03_SECTOR_OVERLAP":
        cursor.execute(
            """SELECT trade_date, scope_key, member_version, contract_version, count(*)
               FROM stock.r03_sector_overlap_clusters
               WHERE trade_date=ANY(%s) AND trade_date<=%s
                 AND (%s::text IS NULL OR member_version=%s)
               GROUP BY trade_date, scope_key, member_version, contract_version
               ORDER BY trade_date, scope_key, member_version, contract_version""",
            (list(target_sessions), cutoff, member_version, member_version),
        )
        versions = []
        for row in cursor.fetchall():
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_overlap_board_facts
                   WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (row[0], row[1], row[2], row[3]),
            )
            child_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_overlap_pair_facts
                   WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (row[0], row[1], row[2], row[3]),
            )
            pair_cnt = int(cursor.fetchone()[0])
            versions.append(
                {
                    "target_session": row[0],
                    "source_version": {"scope_key": str(row[1])},
                    "algorithm_version": str(row[3]),
                    "parent_rows": int(row[4]),
                    "child_rows": child_cnt,
                    "dependent_rows": {
                        "stock.r03_sector_overlap_pair_facts": pair_cnt,
                    },
                    "version_key": {
                        "scope_key": str(row[1]),
                        "member_version": str(row[2]),
                        "contract_version": str(row[3]),
                    },
                }
            )
        return versions

    if module_code == "R03_SECTOR_LOO":
        cursor.execute(
            """SELECT f.trade_date, f.scope_key, f.member_version, f.contract_version,
                      count(DISTINCT f.id), count(c.id)
               FROM stock.r03_sector_loo_support_facts f
               LEFT JOIN stock.r03_sector_loo_candidate_facts c ON c.loo_fact_id=f.id
               WHERE f.trade_date=ANY(%s) AND f.trade_date<=%s
                 AND (%s::text IS NULL OR f.member_version=%s)
               GROUP BY f.trade_date, f.scope_key, f.member_version, f.contract_version
               ORDER BY f.trade_date, f.scope_key, f.member_version, f.contract_version""",
            (list(target_sessions), cutoff, member_version, member_version),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"scope_key": str(row[1])},
                "algorithm_version": str(row[3]),
                "parent_rows": int(row[4]),
                "child_rows": int(row[5]),
                "version_key": {
                    "scope_key": str(row[1]),
                    "member_version": str(row[2]),
                    "contract_version": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "R03_SECTOR_ROTATION":
        cursor.execute(
            """SELECT DISTINCT d_day, s_key, m_ver, c_ver FROM (
                   SELECT trade_date AS d_day, scope_key AS s_key, member_version AS m_ver, contract_version AS c_ver
                   FROM stock.r03_sector_rotation_board_day_facts
                   WHERE trade_date=ANY(%s) AND trade_date<=%s
                     AND (%s::text IS NULL OR member_version=%s)
                   UNION
                   SELECT s.trade_date AS d_day, e.scope_key AS s_key, e.member_version AS m_ver, e.contract_version AS c_ver
                   FROM stock.r03_sector_rotation_day_states s
                   JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
                   WHERE s.trade_date=ANY(%s) AND s.trade_date<=%s
                     AND (%s::text IS NULL OR e.member_version=%s)
               ) t
               ORDER BY d_day, s_key, m_ver, c_ver""",
            (
                list(target_sessions),
                cutoff,
                member_version,
                member_version,
                list(target_sessions),
                cutoff,
                member_version,
                member_version,
            ),
        )
        rot_versions: list[dict[str, Any]] = []
        for t_day, s_key, m_ver, c_ver in cursor.fetchall():
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_rotation_board_day_facts
                   WHERE trade_date=%s AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                (t_day, s_key, m_ver, c_ver),
            )
            board_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(*) FROM stock.r03_sector_rotation_day_states s
                   JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
                   WHERE s.trade_date=%s AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s""",
                (t_day, s_key, m_ver, c_ver),
            )
            state_cnt = int(cursor.fetchone()[0])
            cursor.execute(
                """SELECT count(DISTINCT e.id), count(DISTINCT bm.id)
                   FROM stock.r03_sector_rotation_episodes e
                   LEFT JOIN stock.r03_sector_rotation_basket_members bm ON bm.episode_id=e.id
                   WHERE e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                     AND e.t0_session=%s
                     AND NOT EXISTS (
                         SELECT 1 FROM stock.r03_sector_rotation_day_states s2
                         WHERE s2.episode_id=e.id AND NOT (s2.trade_date=ANY(%s))
                     )""",
                (s_key, m_ver, c_ver, t_day, list(target_sessions)),
            )
            ep_cnt, bm_cnt = cursor.fetchone()
            rot_versions.append(
                {
                    "target_session": t_day,
                    "source_version": {"scope_key": str(s_key)},
                    "algorithm_version": str(c_ver),
                    "parent_rows": int(ep_cnt or 0),
                    "child_rows": board_cnt,
                    "dependent_rows": {
                        "stock.r03_sector_rotation_day_states": state_cnt,
                        "stock.r03_sector_rotation_basket_members": int(bm_cnt or 0),
                    },
                    "version_key": {
                        "scope_key": str(s_key),
                        "member_version": str(m_ver),
                        "contract_version": str(c_ver),
                    },
                }
            )
        return rot_versions

    if module_code == "SECTOR_PULSE":
        cursor.execute(
            """SELECT target_session,source_capture_id,profile_revision,
                      algorithm_contract,count(*)
               FROM stock.source_sector_cross_period_pulse_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
                 AND source_capture_id IS NOT NULL
               GROUP BY target_session,source_capture_id,profile_revision,algorithm_contract
               ORDER BY target_session,source_capture_id,profile_revision,algorithm_contract""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[3]),
                "profile_revision": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "profile_revision": str(row[2]),
                    "algorithm_contract": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "SECTOR_TRANSITION":
        cursor.execute(
            """SELECT p.target_session,p.source_capture_id,t.target_pulse_id,
                      t.transition_contract,t.calendar_id,t.calendar_revision,
                      count(*),coalesce(array_agg(DISTINCT t.previous_pulse_id
                                                  ORDER BY t.previous_pulse_id),
                                        ARRAY[]::bigint[])
               FROM stock.source_sector_pulse_adjacent_transitions t
               JOIN stock.source_sector_cross_period_pulse_facts p
                 ON p.id=t.target_pulse_id
               WHERE p.target_session=ANY(%s) AND p.target_session<=%s
                 AND p.source_capture_id IS NOT NULL
               GROUP BY p.target_session,p.source_capture_id,t.target_pulse_id,
                        t.transition_contract,t.calendar_id,t.calendar_revision
               ORDER BY p.target_session,p.source_capture_id,t.target_pulse_id,
                        t.transition_contract,t.calendar_id,t.calendar_revision""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[3]),
                "parent_rows": int(row[6]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "target_pulse_id": int(row[2]),
                    "transition_contract": str(row[3]),
                    "calendar_id": str(row[4]),
                    "calendar_revision": str(row[5]),
                    "previous_pulse_ids": [int(value) for value in row[7]],
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "PRICE_LIMITS":
        cursor.execute(
            """SELECT target_session,source_capture_id,contract_version,calendar_revision,count(*)
               FROM stock.source_price_limit_day_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
               GROUP BY target_session,source_capture_id,contract_version,calendar_revision
               ORDER BY target_session,source_capture_id,contract_version,calendar_revision""",
            (list(target_sessions), cutoff),
        )
        limit_versions: list[dict[str, Any]] = []
        for row in cursor.fetchall():
            cursor.execute(
                """SELECT count(*)
                   FROM stock.source_limit_up_streak_day_facts
                   WHERE target_session=%s AND source_capture_id=%s
                     AND calendar_revision=%s""",
                (row[0], int(row[1]), str(row[3])),
            )
            streak_rows = int(cursor.fetchone()[0])
            limit_versions.append(
                {
                    "target_session": row[0],
                    "source_version": {"source_capture_id": int(row[1])},
                    "algorithm_version": str(row[2]),
                    "parent_rows": int(row[4]),
                    "child_rows": 0,
                    "dependent_rows": {
                        "stock.source_limit_up_streak_day_facts": streak_rows,
                    },
                    "version_key": {
                        "source_capture_id": int(row[1]),
                        "contract_version": str(row[2]),
                        "calendar_revision": str(row[3]),
                    },
                }
            )
        return limit_versions

    if module_code == "LIMIT_UP_LADDER":
        cursor.execute(
            """SELECT target_session,source_capture_id,contract_version,calendar_revision,count(*)
               FROM stock.source_limit_up_streak_day_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
               GROUP BY target_session,source_capture_id,contract_version,calendar_revision
               ORDER BY target_session,source_capture_id,contract_version,calendar_revision""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "contract_version": str(row[2]),
                    "calendar_revision": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "CROSS_SECTION":
        cursor.execute(
            """SELECT target_session, price_capture_id, universe_version,
                      contract_version, calendar_revision, count(*)
               FROM stock.source_cross_section_day_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
               GROUP BY target_session, price_capture_id, universe_version,
                        contract_version, calendar_revision
               ORDER BY target_session, price_capture_id, universe_version,
                        contract_version, calendar_revision""",
            (list(target_sessions), cutoff),
        )
        cs_versions = []
        for row in cursor.fetchall():
            cursor.execute(
                """SELECT count(*)
                   FROM stock.source_cross_section_endpoints
                   WHERE target_session=%s AND price_capture_id=%s
                     AND universe_version=%s AND contract_version=%s
                     AND calendar_revision=%s""",
                (row[0], int(row[1]), str(row[2]), str(row[3]), str(row[4])),
            )
            ep_rows = int(cursor.fetchone()[0])
            cs_versions.append(
                {
                    "target_session": row[0],
                    "source_version": {
                        "source_capture_id": int(row[1]),
                    },
                    "algorithm_version": str(row[3]),
                    "parent_rows": int(row[5]),
                    "child_rows": ep_rows,
                    "table_counts": {
                        "stock.source_cross_section_day_facts": int(row[5]),
                        "stock.source_cross_section_endpoints": ep_rows,
                    },
                    "version_key": {
                        "price_capture_id": int(row[1]),
                        "universe_version": str(row[2]),
                        "contract_version": str(row[3]),
                        "calendar_revision": str(row[4]),
                    },
                }
            )
        return cs_versions

    if module_code == "MARKET_INDICES_ENV":
        cursor.execute(
            """SELECT target_session, stock_capture_id, index_capture_id,
                      primary_index_symbol, index_config_version,
                      rule_version, calendar_id, calendar_revision, count(*)
               FROM stock.source_market_environment_day_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
               GROUP BY target_session, stock_capture_id, index_capture_id,
                        primary_index_symbol, index_config_version,
                        rule_version, calendar_id, calendar_revision
               ORDER BY target_session, stock_capture_id, index_capture_id,
                        primary_index_symbol, index_config_version,
                        rule_version, calendar_id, calendar_revision""",
            (list(target_sessions), cutoff),
        )
        env_versions = []
        for row in cursor.fetchall():
            idx_cap = int(row[2]) if row[2] is not None else None
            env_versions.append(
                {
                    "target_session": row[0],
                    "source_version": {
                        "stock_capture_id": int(row[1]),
                        "index_capture_id": idx_cap,
                    },
                    "algorithm_version": str(row[5]),
                    "parent_rows": int(row[8]),
                    "child_rows": 0,
                    "table_counts": {
                        "stock.source_market_environment_day_facts": int(row[8]),
                    },
                    "version_key": {
                        "stock_capture_id": int(row[1]),
                        "index_capture_id": idx_cap,
                        "primary_index_symbol": str(row[3]),
                        "index_config_version": str(row[4]),
                        "rule_version": str(row[5]),
                        "calendar_id": str(row[6]),
                        "calendar_revision": str(row[7]),
                    },
                }
            )
        return env_versions

    if module_code == "FORWARD_PATH":
        cursor.execute(
            """SELECT target_session,source_capture_id,path_rule_version,calendar_revision,count(*)
               FROM stock.source_forward_path_facts
               WHERE target_session=ANY(%s) AND target_session<=%s
               GROUP BY target_session,source_capture_id,path_rule_version,calendar_revision
               ORDER BY target_session,source_capture_id,path_rule_version,calendar_revision""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "path_rule_version": str(row[2]),
                    "calendar_revision": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "COHORT_STATS":
        cursor.execute(
            """SELECT target_session,source_capture_id,rule_version,calendar_revision,count(*)
               FROM stock.source_cohort_group_stats
               WHERE target_session=ANY(%s) AND target_session<=%s
                 AND research_cutoff<=%s
               GROUP BY target_session,source_capture_id,rule_version,calendar_revision
               ORDER BY target_session,source_capture_id,rule_version,calendar_revision""",
            (list(target_sessions), cutoff, cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "rule_version": str(row[2]),
                    "calendar_revision": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "FEP_BASELINE":
        cursor.execute(
            """SELECT COALESCE(target_session, period_end_session) AS sess,
                      source_capture_id,rule_version,calendar_revision,count(*)
               FROM stock.source_fep_baseline_facts
               WHERE COALESCE(target_session, period_end_session)=ANY(%s)
                 AND research_cutoff<=%s AND source_capture_id IS NOT NULL
               GROUP BY 1,2,3,4 ORDER BY 1,2,3,4""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "rule_version": str(row[2]),
                    "calendar_revision": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "OBSERVATION_SEGMENTS":
        cursor.execute(
            """SELECT t0_session,source_capture_id,rule_version,calendar_revision,count(*)
               FROM stock.source_research_observation_segment_days
               WHERE t0_session=ANY(%s) AND t0_session<=%s
               GROUP BY t0_session,source_capture_id,rule_version,calendar_revision
               ORDER BY t0_session,source_capture_id,rule_version,calendar_revision""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": 0,
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "rule_version": str(row[2]),
                    "calendar_revision": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "SECTOR_FOLLOWUP":
        cursor.execute(
            """WITH scopes AS (
                   SELECT p.target_session AS t0_session,p.source_capture_id
                   FROM stock.source_sector_cross_period_pulse_facts p
                   WHERE p.target_session=ANY(%s) AND p.target_session<=%s
                     AND p.source_capture_id IS NOT NULL
                   UNION
                   SELECT g.t0_session,g.t0_source_capture_id
                   FROM stock.source_sector_pulse_cohort_group_facts g
                   WHERE g.t0_session=ANY(%s) AND g.t0_session<=%s
                     AND g.t0_source_capture_id IS NOT NULL
               )
               SELECT s.t0_session,s.source_capture_id,
                 (SELECT count(*) FROM stock.source_sector_pulse_followup_facts f
                  JOIN stock.source_sector_cross_period_pulse_facts p
                    ON p.id=f.pulse_fact_id
                  WHERE p.target_session=s.t0_session
                    AND p.source_capture_id=s.source_capture_id),
                 (SELECT count(*) FROM stock.source_sector_pulse_cohort_group_facts g
                  WHERE g.t0_session=s.t0_session
                    AND g.t0_source_capture_id=s.source_capture_id),
                 (SELECT count(*) FROM stock.source_sector_pulse_cohort_reason_facts r
                  JOIN stock.source_sector_pulse_cohort_group_facts g
                    ON g.id=r.cohort_group_fact_id
                  WHERE g.t0_session=s.t0_session
                    AND g.t0_source_capture_id=s.source_capture_id),
                 jsonb_build_object(
                   'pulse',coalesce((SELECT jsonb_agg(DISTINCT jsonb_build_array(
                     p.profile_revision,p.algorithm_contract) ORDER BY
                     jsonb_build_array(p.profile_revision,p.algorithm_contract))
                     FROM stock.source_sector_cross_period_pulse_facts p
                     WHERE p.target_session=s.t0_session
                       AND p.source_capture_id=s.source_capture_id),'[]'::jsonb),
                   'cohort',coalesce((SELECT jsonb_agg(DISTINCT jsonb_build_array(
                     g.t0_profile_revision,g.cohort_contract,g.followup_contract,
                     g.calendar_revision) ORDER BY jsonb_build_array(
                     g.t0_profile_revision,g.cohort_contract,g.followup_contract,
                     g.calendar_revision))
                     FROM stock.source_sector_pulse_cohort_group_facts g
                     WHERE g.t0_session=s.t0_session
                       AND g.t0_source_capture_id=s.source_capture_id),'[]'::jsonb)
                 )
               FROM scopes s
               ORDER BY s.t0_session,s.source_capture_id""",
            (list(target_sessions), cutoff, list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": json.dumps(
                    row[5], ensure_ascii=False, sort_keys=True
                ),
                "parent_rows": int(row[2]),
                "child_rows": 0,
                "dependent_rows": {
                    "stock.source_sector_pulse_cohort_group_facts": int(row[3]),
                    "stock.source_sector_pulse_cohort_reason_facts": int(row[4]),
                },
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "algorithm_versions": row[5],
                },
            }
            for row in cursor.fetchall()
        ]

    if module_code == "MARKET_PARTICIPATION":
        cursor.execute(
            """SELECT d.target_session,d.source_capture_id,d.profile_revision,
                      d.producer_version,count(DISTINCT d.id),
                      count(*) FILTER (WHERE r.participation_day_id IS NOT NULL)
               FROM stock.source_market_participation_days d
               LEFT JOIN stock.source_market_participation_unknown_reasons r
                 ON r.participation_day_id=d.id
               WHERE d.target_session=ANY(%s) AND d.target_session<=%s
                 AND d.source_capture_id IS NOT NULL
               GROUP BY d.target_session,d.source_capture_id,d.profile_revision,d.producer_version
               ORDER BY d.target_session,d.source_capture_id,d.profile_revision,d.producer_version""",
            (list(target_sessions), cutoff),
        )
        return [
            {
                "target_session": row[0],
                "source_version": {"source_capture_id": int(row[1])},
                "algorithm_version": str(row[3]),
                "profile_revision": str(row[2]),
                "parent_rows": int(row[4]),
                "child_rows": int(row[5]),
                "version_key": {
                    "source_capture_id": int(row[1]),
                    "profile_revision": str(row[2]),
                    "producer_version": str(row[3]),
                },
            }
            for row in cursor.fetchall()
        ]
    raise DataJobPgError(
        "DATA_JOB_CLEANUP_MODULE_REJECTED", "模块没有对应的精确来源捕获版本查询"
    )


def _escape_like_literal(value: str) -> str:
    """转义 SQL LIKE 通配符，防止 profile_revision 中的 % / _ 被误解释。"""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _parse_r03_scope_and_member(
    scope_key: str, member_version: str | None
) -> tuple[int | None, str | None, int | None, int | None]:
    cap_id: int | None = None
    prof_rev: str | None = None
    for part in str(scope_key).split(":"):
        if part.startswith("BAOSTOCK_CAP_"):
            raw = part[len("BAOSTOCK_CAP_") :]
            if raw.isdigit():
                cap_id = int(raw)
        elif part.startswith("PROF_"):
            prof_rev = part[len("PROF_") :] or None
    ind_id: int | None = None
    con_id: int | None = None
    if member_version:
        for part in str(member_version).split(":"):
            if part.startswith("IND_"):
                raw = part[len("IND_") :]
                if raw.isdigit():
                    ind_id = int(raw)
            elif part.startswith("CON_"):
                raw = part[len("CON_") :]
                if raw.isdigit():
                    con_id = int(raw)
    return cap_id, prof_rev, ind_id, con_id


def _resolve_r03_upstream_cascade_plan(
    cursor: Any,
    *,
    module_code: str,
    versions: Sequence[Mapping[str, Any]],
    cutoff: date,
) -> dict[str, Any]:
    """根据实际上游版本精确解析受影响的 R03 板块分区（含无 Episode 后继、t+1/t+3 端点依赖与跨版 Episode 后继）与正式市场五元组分区。"""
    empty: dict[str, Any] = {
        "sector_partitions": set(),
        "rotation_episode_cutoffs": {},
        "market_partitions": set(),
        "counts_by_table": {},
        "affected_sessions": set(),
    }
    if (
        module_code not in {"D0", "CORE_PROFILE", "CROSS_SECTION", "TDX_SECTOR_MEMBERS"}
        or not versions
    ):
        return empty

    from stock_a.application.data_jobs import _load_official_sessions

    _cal_id, _cal_rev, official_sessions = _load_official_sessions(cutoff)
    sess_index = {d: idx for idx, d in enumerate(official_sessions)}

    sector_partitions: set[tuple[date, str, str, str]] = set()
    rotation_episode_cutoffs: dict[int, date] = {}
    market_partitions: set[tuple[date, str, str, str, str]] = set()

    r03_sector_prog_tables = [
        "stock.r03_sector_native_day_facts",
        "stock.r03_sector_rotation_board_day_facts",
        "stock.r03_sector_overlap_clusters",
        "stock.r03_sector_loo_support_facts",
    ]

    def _add_sector_prog_rows(rows: Sequence[tuple[Any, ...]]) -> None:
        for td, cap, prof, ind_id, con_id, pver in rows:
            if cap is None or prof is None or not pver:
                continue
            if ind_id is None and con_id is None:
                continue
            sk = f"BAOSTOCK_CAP_{int(cap)}:PROF_{prof}:NON_PIT"
            mv = f"IND_{int(ind_id or 0)}:CON_{int(con_id or 0)}"
            sector_partitions.add((td, sk, mv, str(pver)))

    def _collect_core_profile_deps(cap_id: int, prof_rev: str, p_day: date) -> None:
        p_idx = sess_index.get(p_day)
        p_next_1 = (
            official_sessions[p_idx + 1]
            if p_idx is not None and p_idx + 1 < len(official_sessions)
            else None
        )
        p_next_3 = (
            official_sessions[p_idx + 3]
            if p_idx is not None and p_idx + 3 < len(official_sessions)
            else None
        )
        p_next_4 = (
            official_sessions[p_idx + 4]
            if p_idx is not None and p_idx + 4 < len(official_sessions)
            else None
        )
        exact_scope = f"BAOSTOCK_CAP_{cap_id}:PROF_{prof_rev}:NON_PIT"
        cursor.execute(
            """SELECT trade_date, scope_key, universe_version_t, universe_version_t3, contract_version
               FROM stock.r03_formal_market_breadth_facts
               WHERE trade_date = %s AND scope_key = %s""",
            (p_day, exact_scope),
        )
        for td, sk, ut, ut3, cv in cursor.fetchall():
            market_partitions.add((td, str(sk), str(ut), str(ut3), str(cv)))
        if p_next_3 is not None and p_next_3 <= cutoff:
            cursor.execute(
                """SELECT target_session, source_capture_id, profile_revision,
                          universe_version_t, universe_version_t3, producer_version
                   FROM stock.data_job_progress
                   WHERE target_session = %s
                     AND business_table = 'stock.r03_formal_market_breadth_facts'
                     AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                     AND source_capture_id_t3 = %s AND profile_revision_t3 = %s""",
                (p_next_3, cap_id, prof_rev),
            )
            for td, m_cap, m_prof, ut, ut3, pver in cursor.fetchall():
                if m_cap is not None and ut and ut3 and pver:
                    m_sk = (
                        f"BAOSTOCK_CAP_{int(m_cap)}:PROF_{m_prof}:NON_PIT"
                        if m_prof
                        else f"BAOSTOCK_CAP_{int(m_cap)}:MARKET_QFQ:NON_PIT"
                    )
                    market_partitions.add((td, m_sk, str(ut), str(ut3), str(pver)))
        if p_next_1 is not None and p_next_1 <= cutoff:
            cursor.execute(
                """SELECT target_session, source_capture_id, profile_revision,
                          universe_version_t, universe_version_t3, producer_version
                   FROM stock.data_job_progress
                   WHERE target_session = %s
                     AND business_table = 'stock.r03_formal_market_breadth_facts'
                     AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                     AND prev_session_t = %s AND source_capture_id_t1 = %s AND profile_revision_t1 = %s""",
                (p_next_1, p_day, cap_id, prof_rev),
            )
            for td, m_cap, m_prof, ut, ut3, pver in cursor.fetchall():
                if m_cap is not None and ut and ut3 and pver:
                    m_sk = (
                        f"BAOSTOCK_CAP_{int(m_cap)}:PROF_{m_prof}:NON_PIT"
                        if m_prof
                        else f"BAOSTOCK_CAP_{int(m_cap)}:MARKET_QFQ:NON_PIT"
                    )
                    market_partitions.add((td, m_sk, str(ut), str(ut3), str(pver)))
        if p_next_4 is not None and p_next_4 <= cutoff:
            cursor.execute(
                """SELECT target_session, source_capture_id, profile_revision,
                          universe_version_t, universe_version_t3, producer_version
                   FROM stock.data_job_progress
                   WHERE target_session = %s
                     AND business_table = 'stock.r03_formal_market_breadth_facts'
                     AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                     AND prev_session_t3 = %s AND source_capture_id_t4 = %s""",
                (p_next_4, p_day, cap_id),
            )
            for td, m_cap, m_prof, ut, ut3, pver in cursor.fetchall():
                if m_cap is not None and ut and ut3 and pver:
                    m_sk = (
                        f"BAOSTOCK_CAP_{int(m_cap)}:PROF_{m_prof}:NON_PIT"
                        if m_prof
                        else f"BAOSTOCK_CAP_{int(m_cap)}:MARKET_QFQ:NON_PIT"
                    )
                    market_partitions.add((td, m_sk, str(ut), str(ut3), str(pver)))
        for tbl in r03_sector_prog_tables:
            cursor.execute(
                f"""SELECT DISTINCT trade_date, scope_key, member_version, contract_version
                    FROM {tbl}
                    WHERE trade_date = %s AND scope_key = %s""",
                (p_day, exact_scope),
            )
            for td, sk, mv, cv in cursor.fetchall():
                sector_partitions.add((td, str(sk), str(mv), str(cv)))
        cursor.execute(
            """SELECT DISTINCT target_session, source_capture_id, profile_revision,
                              industry_snapshot_id, concept_snapshot_id, producer_version
               FROM stock.data_job_progress
               WHERE target_session = %s AND business_table = ANY(%s)
                 AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                 AND source_capture_id = %s AND profile_revision = %s""",
            (p_day, r03_sector_prog_tables, cap_id, prof_rev),
        )
        _add_sector_prog_rows(cursor.fetchall())
        if p_next_1 is not None and p_next_1 <= cutoff:
            cursor.execute(
                """SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                  industry_snapshot_id, concept_snapshot_id, producer_version
                   FROM stock.data_job_progress
                   WHERE target_session = %s AND business_table = ANY(%s)
                     AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                     AND source_capture_id_t1 = %s AND profile_revision_t1 = %s""",
                (p_next_1, r03_sector_prog_tables, cap_id, prof_rev),
            )
            _add_sector_prog_rows(cursor.fetchall())
        if p_next_3 is not None and p_next_3 <= cutoff:
            cursor.execute(
                """SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                  industry_snapshot_id, concept_snapshot_id, producer_version
                   FROM stock.data_job_progress
                   WHERE target_session = %s AND business_table = ANY(%s)
                     AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                     AND source_capture_id_t3 = %s AND profile_revision_t3 = %s""",
                (p_next_3, r03_sector_prog_tables, cap_id, prof_rev),
            )
            _add_sector_prog_rows(cursor.fetchall())

    for v in versions:
        v_day = (
            v["target_session"]
            if isinstance(v["target_session"], date)
            else date.fromisoformat(str(v["target_session"]))
        )
        v_key = v["version_key"]
        idx_v = sess_index.get(v_day)
        next_1 = (
            official_sessions[idx_v + 1]
            if idx_v is not None and idx_v + 1 < len(official_sessions)
            else None
        )
        next_3 = (
            official_sessions[idx_v + 3]
            if idx_v is not None and idx_v + 3 < len(official_sessions)
            else None
        )

        if module_code == "CORE_PROFILE":
            cap_id = int(v_key["source_capture_id"])
            prof_rev = str(v_key["profile_revision"])
            _collect_core_profile_deps(cap_id, prof_rev, v_day)

        elif module_code == "D0":
            # 纯 Seed 不清正式市场宽度；仅按实际消费的 t / t-1 / t-3 引用清理 R03 板块分区
            cap_id = int(v_key["source_capture_id"])
            prof_rev = v_key.get("profile_revision")
            if prof_rev:
                scope_cond = "scope_key = %s"
                scope_arg = f"BAOSTOCK_CAP_{cap_id}:PROF_{prof_rev}:NON_PIT"
            else:
                scope_cond = "scope_key LIKE %s ESCAPE '\\'"
                scope_arg = _escape_like_literal(f"BAOSTOCK_CAP_{cap_id}:") + "%"
            for tbl in r03_sector_prog_tables:
                cursor.execute(
                    f"""SELECT DISTINCT trade_date, scope_key, member_version, contract_version
                        FROM {tbl}
                        WHERE trade_date = %s AND {scope_cond}""",
                    (v_day, scope_arg),
                )
                for td, sk, mv, cv in cursor.fetchall():
                    sector_partitions.add((td, str(sk), str(mv), str(cv)))
            for dep_day, cap_col, prof_col in (
                (next_1, "source_capture_id_t1", "profile_revision_t1"),
                (next_3, "source_capture_id_t3", "profile_revision_t3"),
            ):
                if dep_day is not None and dep_day <= cutoff:
                    sql_d0 = f"""SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                                industry_snapshot_id, concept_snapshot_id, producer_version
                                 FROM stock.data_job_progress
                                 WHERE target_session = %s AND business_table = ANY(%s)
                                   AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                                   AND {cap_col} = %s"""
                    args_d0: list[Any] = [dep_day, r03_sector_prog_tables, cap_id]
                    if prof_rev:
                        sql_d0 += f" AND {prof_col} = %s"
                        args_d0.append(str(prof_rev))
                    cursor.execute(sql_d0, tuple(args_d0))
                    _add_sector_prog_rows(cursor.fetchall())

        elif module_code == "TDX_SECTOR_MEMBERS":
            snap_pairs: list[tuple[str, int]] = []
            if v_key.get("industry_snapshot_id") is not None:
                snap_pairs.append(("INDUSTRY", int(v_key["industry_snapshot_id"])))
            if v_key.get("concept_snapshot_id") is not None:
                snap_pairs.append(("CONCEPT", int(v_key["concept_snapshot_id"])))
            if v_key.get("snapshot_id") is not None:
                bk_hint = str(v_key.get("board_kind") or "").upper()
                if bk_hint in {"INDUSTRY", "CONCEPT"}:
                    snap_pairs.append((bk_hint, int(v_key["snapshot_id"])))
                else:
                    snap_pairs.append(("INDUSTRY", int(v_key["snapshot_id"])))
                    snap_pairs.append(("CONCEPT", int(v_key["snapshot_id"])))
            for s_kind, s_id in snap_pairs:
                cols_to_check = (
                    (
                        "industry_snapshot_id",
                        "industry_snapshot_id_t1",
                        "industry_snapshot_id_t3",
                    )
                    if s_kind == "INDUSTRY"
                    else (
                        "concept_snapshot_id",
                        "concept_snapshot_id_t1",
                        "concept_snapshot_id_t3",
                    )
                )
                for s_col in cols_to_check:
                    cursor.execute(
                        f"""SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                            industry_snapshot_id, concept_snapshot_id, producer_version
                            FROM stock.data_job_progress
                            WHERE target_session >= %s AND target_session <= %s
                              AND business_table = ANY(%s)
                              AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                              AND {s_col} = %s""",
                        (v_day, cutoff, r03_sector_prog_tables, s_id),
                    )
                    _add_sector_prog_rows(cursor.fetchall())
                mv_pattern = (
                    f"IND_{s_id}:%" if s_kind == "INDUSTRY" else f"%:CON_{s_id}"
                )
                for tbl in r03_sector_prog_tables:
                    cursor.execute(
                        f"""SELECT DISTINCT trade_date, scope_key, member_version, contract_version
                            FROM {tbl}
                            WHERE trade_date >= %s AND trade_date <= %s
                              AND member_version LIKE %s ESCAPE '\\'""",
                        (v_day, cutoff, mv_pattern),
                    )
                    for td, sk, mv, cv in cursor.fetchall():
                        sector_partitions.add((td, str(sk), str(mv), str(cv)))

        else:
            # CROSS_SECTION：精确匹配实际消费该 (price_capture_id, universe_version) 的市场与 R03 板块分区
            cap_id = int(v_key["price_capture_id"])
            u_ver = v_key.get("universe_version")
            cap_like = _escape_like_literal(f"BAOSTOCK_CAP_{cap_id}:") + "%"
            if u_ver:
                u_str = str(u_ver)
                cursor.execute(
                    """SELECT trade_date, scope_key, universe_version_t, universe_version_t3, contract_version
                       FROM stock.r03_formal_market_breadth_facts
                       WHERE trade_date = %s AND scope_key LIKE %s ESCAPE '\\'
                         AND universe_version_t = %s""",
                    (v_day, cap_like, u_str),
                )
                for td, sk, ut, ut3, cv in cursor.fetchall():
                    market_partitions.add((td, str(sk), str(ut), str(ut3), str(cv)))
                if next_3 is not None and next_3 <= cutoff:
                    cursor.execute(
                        """SELECT target_session, source_capture_id, profile_revision,
                                  universe_version_t, universe_version_t3, producer_version
                           FROM stock.data_job_progress
                           WHERE target_session = %s
                             AND business_table = 'stock.r03_formal_market_breadth_facts'
                             AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                             AND universe_version_t3 = %s
                             AND source_capture_id_t3 = %s""",
                        (next_3, u_str, cap_id),
                    )
                    for td, m_cap, m_prof, ut, ut3, pver in cursor.fetchall():
                        if m_cap is not None and ut and ut3 and pver:
                            m_sk = (
                                f"BAOSTOCK_CAP_{int(m_cap)}:PROF_{m_prof}:NON_PIT"
                                if m_prof
                                else f"BAOSTOCK_CAP_{int(m_cap)}:MARKET_QFQ:NON_PIT"
                            )
                            market_partitions.add(
                                (td, m_sk, str(ut), str(ut3), str(pver))
                            )

                # R03 板块：仅选择在 data_job_progress 中实际引用了 (cap_id, u_str) 作为 t / t-1 / t-3 的分区（含零结果）
                for dep_day, cap_col, u_col in (
                    (v_day, "source_capture_id", "universe_version_t"),
                    (next_1, "source_capture_id_t1", "universe_version_t1"),
                    (next_3, "source_capture_id_t3", "universe_version_t3"),
                ):
                    if dep_day is not None and dep_day <= cutoff:
                        cursor.execute(
                            f"""SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                                industry_snapshot_id, concept_snapshot_id, producer_version
                                FROM stock.data_job_progress
                                WHERE target_session = %s AND business_table = ANY(%s)
                                  AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                                  AND {cap_col} = %s AND {u_col} = %s""",
                            (dep_day, r03_sector_prog_tables, cap_id, u_str),
                        )
                        _add_sector_prog_rows(cursor.fetchall())
            # 若 CROSS_SECTION 清理同时连带清理了下游 CORE_PROFILE，则同步纳入这些被清理 Profile 的精确 R03 依赖
            dep_ids = v.get("dependent_ids")
            if isinstance(dep_ids, Mapping):
                dep_p_sessions = [
                    d if isinstance(d, date) else date.fromisoformat(str(d))
                    for d in (dep_ids.get("profile_sessions") or ())
                ]
                dep_p_revs = [
                    str(r) for r in (dep_ids.get("profile_revisions") or ()) if r
                ]
                for dp_day in dep_p_sessions:
                    for dp_rev in dep_p_revs:
                        _collect_core_profile_deps(cap_id, dp_rev, dp_day)
            elif not u_ver:
                cursor.execute(
                    """SELECT trade_date, scope_key, universe_version_t, universe_version_t3, contract_version
                       FROM stock.r03_formal_market_breadth_facts
                       WHERE trade_date = %s AND scope_key LIKE %s ESCAPE '\\'""",
                    (v_day, cap_like),
                )
                for td, sk, ut, ut3, cv in cursor.fetchall():
                    market_partitions.add((td, str(sk), str(ut), str(ut3), str(cv)))
                for dep_day, cap_col in (
                    (v_day, "source_capture_id"),
                    (next_1, "source_capture_id_t1"),
                    (next_3, "source_capture_id_t3"),
                ):
                    if dep_day is not None and dep_day <= cutoff:
                        cursor.execute(
                            f"""SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                                industry_snapshot_id, concept_snapshot_id, producer_version
                                FROM stock.data_job_progress
                                WHERE target_session = %s AND business_table = ANY(%s)
                                  AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                                  AND {cap_col} = %s""",
                            (dep_day, r03_sector_prog_tables, cap_id),
                        )
                        _add_sector_prog_rows(cursor.fetchall())

    # 3) 沿着真实前态链 (prev_trade_date, prev_scope_key, prev_member_version) 传递追踪无 Episode（Native/Overlap/LOO）与跨版后继分区
    if sector_partitions:
        part_frontier = list(sector_partitions)
        while part_frontier:
            next_frontier: list[tuple[date, str, str, str]] = []
            for p_td, p_sk, p_mv, p_cv in part_frontier:
                cursor.execute(
                    """SELECT DISTINCT target_session, source_capture_id, profile_revision,
                                      industry_snapshot_id, concept_snapshot_id, producer_version
                       FROM stock.data_job_progress
                       WHERE target_session > %s AND target_session <= %s
                         AND business_table = ANY(%s)
                         AND status IN ('COMPLETED', 'ALREADY_COMPLETE', 'NO_CHANGE')
                         AND prev_trade_date = %s
                         AND prev_scope_key = %s AND prev_member_version = %s
                         AND producer_version = %s""",
                    (p_td, cutoff, r03_sector_prog_tables, p_td, p_sk, p_mv, p_cv),
                )
                for td, cap, prof, ind_id, con_id, pver in cursor.fetchall():
                    if (
                        cap is None
                        or prof is None
                        or ind_id is None
                        or con_id is None
                        or not pver
                    ):
                        continue
                    nxt = (
                        td,
                        f"BAOSTOCK_CAP_{int(cap)}:PROF_{prof}:NON_PIT",
                        f"IND_{int(ind_id)}:CON_{int(con_id)}",
                        str(pver),
                    )
                    if nxt not in sector_partitions:
                        sector_partitions.add(nxt)
                        next_frontier.append(nxt)
            part_frontier = next_frontier

        # 4) 从已受影响分区收集对应的 Rotation Episode 及其截断日
        p_list_ep = sorted(sector_partitions)
        cursor.execute(
            """SELECT DISTINCT b.episode_id, b.trade_date
               FROM stock.r03_sector_rotation_board_day_facts b
               JOIN unnest(%s::date[], %s::text[], %s::text[], %s::text[]) AS p(td, sk, mv, cv)
                 ON b.trade_date = p.td AND b.scope_key = p.sk
                AND b.member_version = p.mv AND b.contract_version = p.cv
               WHERE b.episode_id IS NOT NULL""",
            (
                [p[0] for p in p_list_ep],
                [p[1] for p in p_list_ep],
                [p[2] for p in p_list_ep],
                [p[3] for p in p_list_ep],
            ),
        )
        for ep_id, td in cursor.fetchall():
            ep_i = int(ep_id)
            if (
                ep_i not in rotation_episode_cutoffs
                or td < rotation_episode_cutoffs[ep_i]
            ):
                rotation_episode_cutoffs[ep_i] = td

    # 通过 Rotation Episode 及 parent_episode_id 链追踪跨版（不同 capture/profile/member）的后继日级事实
    if rotation_episode_cutoffs:
        ep_frontier = set(rotation_episode_cutoffs.keys())
        while ep_frontier:
            cursor.execute(
                """SELECT id, t0_session
                   FROM stock.r03_sector_rotation_episodes
                   WHERE parent_episode_id = ANY(%s) AND t0_session <= %s""",
                (sorted(ep_frontier), cutoff),
            )
            new_children = set()
            for ch_id, ch_t0 in cursor.fetchall():
                ch_i = int(ch_id)
                if (
                    ch_i not in rotation_episode_cutoffs
                    or ch_t0 < rotation_episode_cutoffs[ch_i]
                ):
                    rotation_episode_cutoffs[ch_i] = ch_t0
                    new_children.add(ch_i)
            ep_frontier = new_children

        for ep_i, min_ep_d in list(rotation_episode_cutoffs.items()):
            cursor.execute(
                """SELECT DISTINCT trade_date, scope_key, member_version, contract_version
                   FROM stock.r03_sector_rotation_board_day_facts
                   WHERE episode_id = %s AND trade_date >= %s AND trade_date <= %s""",
                (ep_i, min_ep_d, cutoff),
            )
            for td, sk, mv, cv in cursor.fetchall():
                sector_partitions.add((td, str(sk), str(mv), str(cv)))

    counts_by_table: dict[str, int] = {}
    affected_sessions: set[date] = set()
    if sector_partitions:
        part_list = sorted(sector_partitions)
        p_dates = [p[0] for p in part_list]
        p_scopes = [p[1] for p in part_list]
        p_mems = [p[2] for p in part_list]
        p_contracts = [p[3] for p in part_list]
        affected_sessions.update(p_dates)

        cursor.execute(
            """SELECT count(*)::bigint
               FROM stock.r03_sector_loo_candidate_facts c
               JOIN stock.r03_sector_loo_support_facts f ON f.id = c.loo_fact_id
               JOIN unnest(%s::date[], %s::text[], %s::text[], %s::text[])
                 AS p(td, sk, mv, cv)
                 ON f.trade_date=p.td AND f.scope_key=p.sk
                AND f.member_version=p.mv AND f.contract_version=p.cv""",
            (p_dates, p_scopes, p_mems, p_contracts),
        )
        cnt = int(cursor.fetchone()[0])
        if cnt:
            counts_by_table["stock.r03_sector_loo_candidate_facts"] = cnt

        for tbl in (
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_native_day_facts",
        ):
            cursor.execute(
                f"""SELECT count(*)::bigint
                    FROM {tbl} t
                    JOIN unnest(%s::date[], %s::text[], %s::text[], %s::text[])
                      AS p(td, sk, mv, cv)
                      ON t.trade_date=p.td AND t.scope_key=p.sk
                     AND t.member_version=p.mv AND t.contract_version=p.cv""",
                (p_dates, p_scopes, p_mems, p_contracts),
            )
            cnt = int(cursor.fetchone()[0])
            if cnt:
                counts_by_table[tbl] = cnt

    if rotation_episode_cutoffs:
        ep_ids = list(rotation_episode_cutoffs.keys())
        ep_dates = [rotation_episode_cutoffs[eid] for eid in ep_ids]
        cursor.execute(
            """SELECT count(*)::bigint
               FROM stock.r03_sector_rotation_day_states s
               JOIN unnest(%s::bigint[], %s::date[]) AS ep(eid, min_d)
                 ON s.episode_id = ep.eid
               WHERE s.trade_date >= ep.min_d AND s.trade_date <= %s""",
            (ep_ids, ep_dates, cutoff),
        )
        cnt_ds = int(cursor.fetchone()[0])
        if cnt_ds:
            counts_by_table["stock.r03_sector_rotation_day_states"] = cnt_ds
        cursor.execute(
            """SELECT count(*)::bigint
               FROM stock.r03_sector_rotation_basket_members bm
               WHERE bm.episode_id = ANY(%s::bigint[])
                 AND NOT EXISTS (
                     SELECT 1 FROM stock.r03_sector_rotation_day_states s
                     LEFT JOIN unnest(%s::bigint[], %s::date[]) AS ep(eid, min_d)
                       ON s.episode_id = ep.eid
                     WHERE s.episode_id = bm.episode_id
                       AND (ep.eid IS NULL OR s.trade_date < ep.min_d OR s.trade_date > %s)
                 )""",
            (ep_ids, ep_ids, ep_dates, cutoff),
        )
        cnt_bm = int(cursor.fetchone()[0])
        if cnt_bm:
            counts_by_table["stock.r03_sector_rotation_basket_members"] = cnt_bm
        cursor.execute(
            """SELECT count(*)::bigint
               FROM stock.r03_sector_rotation_episodes e
               WHERE e.id = ANY(%s::bigint[])
                 AND NOT EXISTS (
                     SELECT 1 FROM stock.r03_sector_rotation_day_states s
                     LEFT JOIN unnest(%s::bigint[], %s::date[]) AS ep(eid, min_d)
                       ON s.episode_id = ep.eid
                     WHERE s.episode_id = e.id
                       AND (ep.eid IS NULL OR s.trade_date < ep.min_d OR s.trade_date > %s)
                 )""",
            (ep_ids, ep_ids, ep_dates, cutoff),
        )
        cnt_ep = int(cursor.fetchone()[0])
        if cnt_ep:
            counts_by_table["stock.r03_sector_rotation_episodes"] = cnt_ep

    if market_partitions:
        m_list = sorted(market_partitions)
        m_dates = [m[0] for m in m_list]
        m_scopes = [m[1] for m in m_list]
        m_ut = [m[2] for m in m_list]
        m_ut3 = [m[3] for m in m_list]
        m_contracts = [m[4] for m in m_list]
        affected_sessions.update(m_dates)
        cursor.execute(
            """SELECT count(*)::bigint
               FROM stock.r03_formal_market_breadth_facts f
               JOIN unnest(%s::date[], %s::text[], %s::text[], %s::text[], %s::text[])
                 AS m(td, sk, ut, ut3, cv)
                 ON f.trade_date=m.td AND f.scope_key=m.sk
                AND f.universe_version_t=m.ut AND f.universe_version_t3=m.ut3
                AND f.contract_version=m.cv""",
            (m_dates, m_scopes, m_ut, m_ut3, m_contracts),
        )
        cnt_m = int(cursor.fetchone()[0])
        if cnt_m:
            counts_by_table["stock.r03_formal_market_breadth_facts"] = cnt_m

    return {
        "sector_partitions": sector_partitions,
        "rotation_episode_cutoffs": rotation_episode_cutoffs,
        "market_partitions": market_partitions,
        "counts_by_table": counts_by_table,
        "affected_sessions": affected_sessions,
    }


def _cascade_invalidate_market_environment_for_formal_breadth(
    cursor: Any,
    *,
    deleted_formal_keys: Sequence[tuple[date, str, str, str]],
    current_job_id: UUID | None = None,
    error_code: str = "DERIVED_RESULT_CLEARED",
) -> tuple[int, list[tuple[Any, Any]]]:
    """清理或失效正式市场宽度时，精确级联删除依赖它的复合市场环境及后续滞后会话，并重置对应进度。"""
    if not deleted_formal_keys:
        return 0, []
    deleted_env_rows: list[tuple[date, int, str]] = []
    for td, sk, ut, ut3 in deleted_formal_keys:
        cursor.execute(
            """DELETE FROM stock.source_market_environment_day_facts
               WHERE formal_scope_key = %s
                 AND universe_version_t = %s
                 AND universe_version_t3 = %s
                 AND target_session >= %s
               RETURNING target_session, stock_capture_id, profile_revision""",
            (sk, ut, ut3, td),
        )
        deleted_env_rows.extend(cursor.fetchall())
    inv_rows: list[tuple[Any, Any]] = []
    for env_td, env_cap, env_prof in sorted(set(deleted_env_rows)):
        conds = [
            "p.target_session=%s",
            "p.business_table='stock.source_market_environment_day_facts'",
            "p.source_capture_id=%s",
            "p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')",
            "(%s::uuid IS NULL OR p.job_id<>%s)",
        ]
        args: list[Any] = [env_td, int(env_cap), current_job_id, current_job_id]
        if env_prof:
            conds.append("(p.profile_revision IS NULL OR p.profile_revision=%s)")
            args.append(str(env_prof))
        cursor.execute(
            f"""UPDATE stock.data_job_progress AS p
               SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                   verified_rows=NULL,inserted_rows=0,updated_rows=0,
                   quality_status=NULL,error_code=%s,
                   error_message='正式市场宽度已清理或修订；复合市场环境及后续滞后状态需重新生成。',
                   deleted_rows=0,
                   updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
               WHERE {" AND ".join(conds)}
               RETURNING p.job_id, p.business_table""",
            (error_code, *args),
        )
        inv_rows.extend(cursor.fetchall())
    return len(deleted_env_rows), inv_rows


def apply_r03_upstream_cascade_invalidation(
    cursor: Any,
    *,
    r03_plan: Mapping[str, Any],
    cutoff: date,
    current_job_id: UUID | None = None,
    error_code: str = "DERIVED_RESULT_CLEARED",
    error_message: str = "对应派生结果或其依赖的上游横截面/Profile/前态已清理；需按依赖重新生成。",
) -> tuple[dict[str, int], set[date], list[tuple[Any, str]]]:
    """根据 _resolve_r03_upstream_cascade_plan 的结果精确事务删除受影响 R03 分区并失效对应 data_job_progress。"""
    r03_cascade_counts: dict[str, int] = {}
    r03_cascade_sessions: set[date] = set()
    invalidated_rows: list[tuple[Any, str]] = []

    if r03_plan["sector_partitions"]:
        part_list = sorted(r03_plan["sector_partitions"])
        p_dates = [p[0] for p in part_list]
        p_scopes = [p[1] for p in part_list]
        p_mems = [p[2] for p in part_list]
        p_contracts = [p[3] for p in part_list]
        cursor.execute(
            """DELETE FROM stock.r03_sector_loo_candidate_facts c
               USING stock.r03_sector_loo_support_facts f,
                     unnest(%s::date[], %s::text[], %s::text[], %s::text[]) AS p(td, sk, mv, cv)
               WHERE c.loo_fact_id=f.id AND f.trade_date=p.td AND f.scope_key=p.sk
                 AND f.member_version=p.mv AND f.contract_version=p.cv
               RETURNING f.trade_date""",
            (p_dates, p_scopes, p_mems, p_contracts),
        )
        for row in cursor.fetchall():
            r03_cascade_counts["stock.r03_sector_loo_candidate_facts"] = (
                r03_cascade_counts.get("stock.r03_sector_loo_candidate_facts", 0) + 1
            )
            r03_cascade_sessions.add(row[0])
        for tbl in (
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_native_day_facts",
        ):
            cursor.execute(
                f"""DELETE FROM {tbl} t
                    USING unnest(%s::date[], %s::text[], %s::text[], %s::text[]) AS p(td, sk, mv, cv)
                    WHERE t.trade_date=p.td AND t.scope_key=p.sk
                      AND t.member_version=p.mv AND t.contract_version=p.cv
                    RETURNING t.trade_date""",
                (p_dates, p_scopes, p_mems, p_contracts),
            )
            for row in cursor.fetchall():
                r03_cascade_counts[tbl] = r03_cascade_counts.get(tbl, 0) + 1
                r03_cascade_sessions.add(row[0])

    if r03_plan["rotation_episode_cutoffs"]:
        ep_ids = list(r03_plan["rotation_episode_cutoffs"].keys())
        ep_dates = [r03_plan["rotation_episode_cutoffs"][eid] for eid in ep_ids]
        cursor.execute(
            """DELETE FROM stock.r03_sector_rotation_day_states s
               USING unnest(%s::bigint[], %s::date[]) AS ep(eid, min_d)
               WHERE s.episode_id=ep.eid AND s.trade_date>=ep.min_d AND s.trade_date<=%s
               RETURNING s.trade_date""",
            (ep_ids, ep_dates, cutoff),
        )
        for row in cursor.fetchall():
            r03_cascade_counts["stock.r03_sector_rotation_day_states"] = (
                r03_cascade_counts.get("stock.r03_sector_rotation_day_states", 0) + 1
            )
            r03_cascade_sessions.add(row[0])
        cursor.execute(
            """DELETE FROM stock.r03_sector_rotation_basket_members bm
               WHERE bm.episode_id = ANY(%s::bigint[])
                 AND NOT EXISTS (SELECT 1 FROM stock.r03_sector_rotation_day_states s WHERE s.episode_id=bm.episode_id)""",
            (ep_ids,),
        )
        if cursor.rowcount:
            r03_cascade_counts["stock.r03_sector_rotation_basket_members"] = (
                cursor.rowcount
            )
        cursor.execute(
            """UPDATE stock.r03_sector_rotation_episodes e SET parent_episode_id=NULL
               WHERE e.parent_episode_id = ANY(%s::bigint[])
                 AND NOT EXISTS (SELECT 1 FROM stock.r03_sector_rotation_day_states s WHERE s.episode_id=e.parent_episode_id)""",
            (ep_ids,),
        )
        cursor.execute(
            """DELETE FROM stock.r03_sector_rotation_episodes e
               WHERE e.id = ANY(%s::bigint[])
                 AND NOT EXISTS (SELECT 1 FROM stock.r03_sector_rotation_day_states s WHERE s.episode_id=e.id)""",
            (ep_ids,),
        )
        if cursor.rowcount:
            r03_cascade_counts["stock.r03_sector_rotation_episodes"] = cursor.rowcount

    if r03_plan["market_partitions"]:
        m_list = sorted(r03_plan["market_partitions"])
        m_dates = [m[0] for m in m_list]
        m_scopes = [m[1] for m in m_list]
        m_ut = [m[2] for m in m_list]
        m_ut3 = [m[3] for m in m_list]
        m_contracts = [m[4] for m in m_list]
        cursor.execute(
            """DELETE FROM stock.r03_formal_market_breadth_facts f
               USING unnest(%s::date[], %s::text[], %s::text[], %s::text[], %s::text[])
                 AS m(td, sk, ut, ut3, cv)
               WHERE f.trade_date=m.td AND f.scope_key=m.sk
                 AND f.universe_version_t=m.ut AND f.universe_version_t3=m.ut3
                 AND f.contract_version=m.cv
               RETURNING f.trade_date, f.scope_key, f.universe_version_t, f.universe_version_t3""",
            (m_dates, m_scopes, m_ut, m_ut3, m_contracts),
        )
        deleted_formal_env_keys: list[tuple[date, str, str, str]] = []
        for row in cursor.fetchall():
            r03_cascade_counts["stock.r03_formal_market_breadth_facts"] = (
                r03_cascade_counts.get("stock.r03_formal_market_breadth_facts", 0) + 1
            )
            r03_cascade_sessions.add(row[0])
            deleted_formal_env_keys.append(
                (row[0], str(row[1]), str(row[2]), str(row[3]))
            )
        if deleted_formal_env_keys:
            env_del_cnt, env_inv = (
                _cascade_invalidate_market_environment_for_formal_breadth(
                    cursor,
                    deleted_formal_keys=deleted_formal_env_keys,
                    current_job_id=current_job_id,
                    error_code=error_code,
                )
            )
            if env_del_cnt > 0:
                r03_cascade_counts["stock.source_market_environment_day_facts"] = (
                    r03_cascade_counts.get(
                        "stock.source_market_environment_day_facts", 0
                    )
                    + env_del_cnt
                )
            invalidated_rows.extend(env_inv)

    if r03_plan["sector_partitions"]:
        r03_sector_prog_tables = [
            "stock.r03_sector_native_day_facts",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_rotation_episodes",
            "stock.r03_sector_rotation_basket_members",
            "stock.r03_sector_rotation_day_states",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_overlap_board_facts",
            "stock.r03_sector_overlap_pair_facts",
            "stock.r03_sector_loo_support_facts",
            "stock.r03_sector_loo_candidate_facts",
        ]
        for td, sk, mv, cv in sorted(r03_plan["sector_partitions"]):
            c_id, p_rev, i_id, co_id = _parse_r03_scope_and_member(sk, mv)
            p_conds = ["p.target_session=%s", "p.business_table=ANY(%s)"]
            p_args: list[Any] = [td, r03_sector_prog_tables]
            if c_id is not None:
                p_conds.append("p.source_capture_id=%s")
                p_args.append(c_id)
            if p_rev is not None:
                p_conds.append("p.profile_revision=%s")
                p_args.append(p_rev)
            if i_id is not None:
                p_conds.append("p.industry_snapshot_id=%s")
                p_args.append(i_id)
            if co_id is not None:
                p_conds.append("p.concept_snapshot_id=%s")
                p_args.append(co_id)
            if cv:
                p_conds.append("p.producer_version=%s")
                p_args.append(str(cv))
            p_args.extend([current_job_id, current_job_id])
            cursor.execute(
                f"""UPDATE stock.data_job_progress AS p
                   SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                       verified_rows=NULL,inserted_rows=0,updated_rows=0,
                       quality_status=NULL,error_code=%s,error_message=%s,
                       deleted_rows=0,
                       updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
                   WHERE {" AND ".join(p_conds)}
                     AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                     AND (%s::uuid IS NULL OR p.job_id<>%s)
                   RETURNING job_id, business_table""",
                (error_code, error_message, *p_args),
            )
            invalidated_rows.extend(cursor.fetchall())

    if r03_plan["market_partitions"]:
        for td, sk, ut, ut3, cv in sorted(r03_plan["market_partitions"]):
            c_id, p_rev, _i_id, _co_id = _parse_r03_scope_and_member(sk, None)
            m_conds = [
                "p.target_session=%s",
                "p.business_table='stock.r03_formal_market_breadth_facts'",
                "p.universe_version_t=%s",
                "p.universe_version_t3=%s",
                "p.producer_version=%s",
            ]
            m_args: list[Any] = [td, str(ut), str(ut3), str(cv)]
            if c_id is not None:
                m_conds.append("p.source_capture_id=%s")
                m_args.append(c_id)
            if p_rev is not None:
                m_conds.append("p.profile_revision=%s")
                m_args.append(p_rev)
            else:
                m_conds.append("p.profile_revision IS NULL")
            m_args.extend([current_job_id, current_job_id])
            cursor.execute(
                f"""UPDATE stock.data_job_progress AS p
                   SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                       verified_rows=NULL,inserted_rows=0,updated_rows=0,
                       quality_status=NULL,error_code=%s,
                       error_message='正式市场宽度依赖的Profile或Universe已清理；需重新生成。',
                       deleted_rows=0,
                       updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
                   WHERE {" AND ".join(m_conds)}
                     AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                     AND (%s::uuid IS NULL OR p.job_id<>%s)
                   RETURNING job_id, business_table""",
                (error_code, *m_args),
            )
            invalidated_rows.extend(cursor.fetchall())

    return r03_cascade_counts, r03_cascade_sessions, invalidated_rows


def _cleanup_preview_in_cursor(
    cursor: Any,
    *,
    module_code: str,
    target_sessions: tuple[date, ...],
    cutoff: date,
    member_version: str | None = None,
    scope_key: str | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
) -> dict[str, Any]:
    spec, sessions = _cleanup_scope(module_code, target_sessions, cutoff)
    blockers = _cleanup_fk_blockers(cursor, spec)
    versions = _cleanup_version_rows(
        cursor,
        module_code,
        sessions,
        cutoff,
        member_version=member_version,
        scope_key=scope_key,
        universe_version_t=universe_version_t,
        universe_version_t3=universe_version_t3,
    )
    if module_code == "CROSS_SECTION":
        by_capture: dict[int, list[dict[str, Any]]] = {}
        for version in versions:
            by_capture.setdefault(
                version["version_key"]["price_capture_id"], []
            ).append(version)
        for source_capture_id, scoped_versions in by_capture.items():
            selected_scope_sessions = tuple(
                sorted({version["target_session"] for version in scoped_versions})
            )
            details = _cross_section_cleanup_details(
                cursor,
                target_sessions=selected_scope_sessions,
                source_capture_id=source_capture_id,
                cutoff=cutoff,
            )
            dependent_ids = {
                name: details[name]
                for name in (
                    "profile_sessions",
                    "profile_revisions",
                    "rule_fact_ids",
                    "d0_fact_ids",
                    "seed_fact_ids",
                    "enrollment_ids",
                    "t0_fact_ids",
                    "d1_structure_ids",
                    "observation_segment_ids",
                    "fep_baseline_ids",
                    "research_event_ids",
                )
            }
            scoped_versions[0]["dependent_rows"] = details["dependent_rows"]
            scoped_versions[0]["dependent_target_sessions"] = details[
                "dependent_target_sessions"
            ]
            scoped_versions[0]["dependent_ids"] = dependent_ids
            for version in scoped_versions[1:]:
                version["dependent_rows"] = {}
                version["dependent_target_sessions"] = ()
                version["dependent_ids"] = {}
    elif module_code == "MARKET_INDICES_ENV":
        grouped_scopes: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for version in versions:
            key = version["version_key"]
            scope_key = (
                key["stock_capture_id"],
                key["index_capture_id"],
                key["primary_index_symbol"],
                key["index_config_version"],
                key["calendar_id"],
                key["calendar_revision"],
                key["rule_version"],
            )
            grouped_scopes.setdefault(scope_key, []).append(version)
        selected_sessions = set(sessions)
        for scope_key, scoped_versions in grouped_scopes.items():
            root_sessions = sorted(
                version["target_session"] for version in scoped_versions
            )
            first_session = root_sessions[0]
            (
                stock_capture_id,
                index_capture_id,
                primary,
                index_config,
                cal_id,
                cal_rev,
                rule_ver,
            ) = scope_key
            cursor.execute(
                """SELECT target_session
                   FROM stock.source_market_environment_day_facts
                   WHERE target_session>%s AND target_session<=%s
                     AND target_session<>ALL(%s::date[])
                     AND stock_capture_id=%s
                     AND index_capture_id IS NOT DISTINCT FROM %s
                     AND primary_index_symbol=%s
                     AND index_config_version IS NOT DISTINCT FROM %s
                     AND calendar_id=%s AND calendar_revision=%s
                     AND rule_version=%s
                   ORDER BY target_session""",
                (
                    first_session,
                    cutoff,
                    list(selected_sessions),
                    stock_capture_id,
                    index_capture_id,
                    primary,
                    index_config,
                    cal_id,
                    cal_rev,
                    rule_ver,
                ),
            )
            dependent_sessions = tuple(row[0] for row in cursor.fetchall())
            if dependent_sessions:
                scoped_versions[0].setdefault("dependent_rows", {})[
                    "stock.source_market_environment_day_facts"
                ] = len(dependent_sessions)
                scoped_versions[0]["dependent_target_sessions"] = dependent_sessions
    source_scopes_by_session: dict[date, set[str]] = {}
    for version in versions:
        scope_version = version["source_version"]
        if module_code == "CROSS_SECTION":
            # CROSS_SECTION清理API不接Universe选择键，业务操作即同日、同价格捕获的全scope清理；
            # 多Universe版本不应伪装成多个价格捕获。不同price_capture_id仍保持歧义阻断。
            scope_version = {
                "source_capture_id": version["version_key"]["price_capture_id"]
            }
        source_scope = json.dumps(
            scope_version,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        source_scopes_by_session.setdefault(version["target_session"], set()).add(
            source_scope
        )
    if any(len(scopes) > 1 for scopes in source_scopes_by_session.values()):
        blockers.append(
            {
                "referenced_table": spec["parent_table"],
                "dependent_table": module_code,
                "constraint": "",
                "reason": "同一目标会话包含多个来源包或捕获；请先按来源版本收敛清理范围",
            }
        )
    if module_code == "MARKET_BREADTH":
        for version in versions:
            source_version = version.get("source_version", {})
            cursor.execute(
                """SELECT count(*) FROM stock.source_market_participation_days
                   WHERE target_session=%s AND source_capture_id=%s""",
                (
                    version["target_session"],
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
    if module_code == "D1":
        for version in versions:
            downstream = version.get("blocking_dependent_target_sessions", ())
            if not downstream:
                continue
            dates = sorted({item["target_session"].isoformat() for item in downstream})
            rows = sum(int(item["rows"]) for item in downstream)
            blockers.append(
                {
                    "referenced_table": spec["parent_table"],
                    "dependent_table": "stock.source_research_enrollments",
                    "constraint": "业务依赖：后续研究登记引用该D1规则的相邻前态",
                    "reason": (
                        "该D1事实仍被后续会话作为前态引用；先处理后续研究登记并重新预览，"
                        f"目标会话={', '.join(dates)}，引用行数={rows}"
                    ),
                }
            )
    if module_code == "FOCUS_FORMAL":
        for version in versions:
            key = version["version_key"]
            rule_scope = (
                version["target_session"],
                key["source_capture_id"],
                key["profile_revision"],
            )
            for dependent_table, sql in (
                (
                    "stock.source_research_enrollments",
                    """SELECT count(DISTINCT e.id)
                       FROM stock.source_research_enrollments e
                       JOIN stock.source_rule_evaluations current_rule
                         ON current_rule.id=e.rule_fact_id
                       JOIN stock.source_rule_evaluations previous_rule
                         ON previous_rule.id=e.previous_rule_fact_id
                       WHERE previous_rule.target_session=%s
                         AND previous_rule.source_capture_id=%s
                         AND previous_rule.profile_revision IS NOT DISTINCT FROM %s
                         AND current_rule.target_session>%s""",
                ),
                (
                    "stock.source_research_observation_segment_days",
                    """SELECT count(DISTINCT o.id)
                       FROM stock.source_research_observation_segment_days o
                       LEFT JOIN stock.source_rule_evaluations r
                         ON r.id=o.anchor_rule_fact_id OR r.id=o.evidence_rule_fact_id
                       LEFT JOIN stock.source_research_enrollments e
                         ON e.id=o.source_enrollment_id
                         OR e.id=o.parent_enrollment_id
                         OR e.id=o.linked_enrollment_id
                       LEFT JOIN stock.source_rule_evaluations er ON er.id=e.rule_fact_id
                       WHERE (
                           r.target_session=%s AND r.source_capture_id=%s
                           AND r.profile_revision IS NOT DISTINCT FROM %s
                       ) OR (
                           er.target_session=%s AND er.source_capture_id=%s
                           AND er.profile_revision IS NOT DISTINCT FROM %s
                       )""",
                ),
                (
                    "stock.source_rps_pulse_t0_change_facts",
                    """SELECT count(*)
                       FROM stock.source_rps_pulse_t0_change_facts p
                       LEFT JOIN stock.source_rule_evaluations r ON r.id=p.rule_fact_id
                       LEFT JOIN stock.source_t0_early_observation_facts t
                         ON t.id=p.source_t0_fact_id
                       LEFT JOIN stock.source_rule_evaluations tr ON tr.id=t.rule_fact_id
                       WHERE (
                           r.target_session=%s AND r.source_capture_id=%s
                           AND r.profile_revision IS NOT DISTINCT FROM %s
                       ) OR (
                           tr.target_session=%s AND tr.source_capture_id=%s
                           AND tr.profile_revision IS NOT DISTINCT FROM %s
                       )""",
                ),
            ):
                if dependent_table == "stock.source_research_enrollments":
                    params = (*rule_scope, version["target_session"])
                else:
                    params = (*rule_scope, *rule_scope)
                cursor.execute(sql, params)
                dependent_rows = int(cursor.fetchone()[0])
                if dependent_rows:
                    blockers.append(
                        {
                            "referenced_table": "stock.source_rule_evaluations",
                            "dependent_table": dependent_table,
                            "constraint": "外键依赖：下游Focus登记或观察段仍引用该日规则事实",
                            "reason": (
                                f"PG读回到{dependent_rows}条下游事实引用"
                                "stock.source_rule_evaluations；请先清理下游观察段/登记后再清理FOCUS_FORMAL"
                            ),
                        }
                    )
    if module_code == "CORE_PROFILE":
        dependent_queries = (
            (
                "stock.source_pool_seed_day_facts",
                """SELECT count(*) FROM stock.source_pool_seed_day_facts
                   WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s""",
            ),
            (
                "stock.source_market_participation_days",
                """SELECT count(*) FROM stock.source_market_participation_days
                   WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s""",
            ),
            (
                "stock.source_sector_cross_period_pulse_facts",
                """SELECT count(*) FROM stock.source_sector_cross_period_pulse_facts
                   WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s""",
            ),
            (
                "stock.source_d0_formal_admission_day_facts",
                """SELECT count(*) FROM stock.source_d0_formal_admission_day_facts
                   WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s""",
            ),
            (
                "stock.source_rule_evaluations",
                """SELECT count(*) FROM stock.source_rule_evaluations
                   WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s""",
            ),
        )
        for version in versions:
            key = version["version_key"]
            for dependent_table, query in dependent_queries:
                cursor.execute(
                    query,
                    (
                        version["target_session"],
                        key["source_capture_id"],
                        key["profile_revision"],
                    ),
                )
                dependent_rows = int(cursor.fetchone()[0])
                if dependent_rows:
                    blockers.append(
                        {
                            "referenced_table": spec["parent_table"],
                            "dependent_table": dependent_table,
                            "constraint": "业务依赖：下游结果绑定该Core/Profile修订",
                            "reason": (
                                "先清理该日期依赖模块，再重新预览Core/Profile；"
                                f"PG读回到{dependent_rows}条下游事实"
                            ),
                        }
                    )
    # R03_SECTOR_NATIVE 采用同事务精确版本级联清理（含 LOO/Overlap/Rotation 及未选中后继），不再旧式阻断
    if module_code == "SECTOR_PULSE":
        for version in versions:
            source_version = version["source_version"]
            cursor.execute(
                """SELECT count(*) FROM stock.source_sector_pulse_cohort_group_facts
                   WHERE t0_session=%s AND t0_source_capture_id=%s""",
                (
                    version["target_session"],
                    source_version["source_capture_id"],
                ),
            )
            dependent_rows = int(cursor.fetchone()[0])
            if dependent_rows:
                blockers.append(
                    {
                        "referenced_table": spec["parent_table"],
                        "dependent_table": (
                            "stock.source_sector_pulse_cohort_group_facts"
                        ),
                        "constraint": "业务依赖：板块分组后继引用T0 PULSE来源身份",
                        "reason": (
                            "先清理同一T0来源版本的SECTOR_FOLLOWUP，再重新预览PULSE；"
                            f"PG读回到{dependent_rows}条下游分组"
                        ),
                    }
                )
    if module_code == "SECTOR_TRANSITION":
        for version in versions:
            key = version["version_key"]
            cursor.execute(
                """SELECT count(*)
                   FROM stock.source_sector_pulse_cohort_group_facts g
                   JOIN stock.source_sector_pulse_adjacent_transitions t
                     ON t.target_pulse_id=%s
                   JOIN stock.source_sector_cross_period_pulse_facts p
                     ON p.id=t.previous_pulse_id
                   WHERE g.t0_session=p.target_session
                     AND g.t0_source_capture_id=p.source_capture_id
                     AND g.t0_source_capture_id=p.source_capture_id
                     AND g.cohort_axis='ADJACENT_TRANSITION'""",
                (key["target_pulse_id"],),
            )
            dependent_rows = int(cursor.fetchone()[0])
            if dependent_rows:
                blockers.append(
                    {
                        "referenced_table": spec["parent_table"],
                        "dependent_table": (
                            "stock.source_sector_pulse_cohort_group_facts"
                        ),
                        "constraint": "业务依赖：相邻转移分组读取本转移事实",
                        "reason": (
                            "先清理该转移前一会话的SECTOR_FOLLOWUP分组，再重新预览转移；"
                            f"PG读回到{dependent_rows}条分组"
                        ),
                    }
                )
    if module_code == "D0":
        for version in versions:
            source_version = version["source_version"]
            for dependent_table, query in (
                (
                    "stock.source_sector_cross_period_pulse_facts",
                    """SELECT count(*) FROM stock.source_sector_cross_period_pulse_facts
                       WHERE target_session=%s AND source_capture_id=%s""",
                ),
                (
                    "stock.source_sector_pulse_cohort_group_facts",
                    """SELECT count(*) FROM stock.source_sector_pulse_cohort_group_facts
                       WHERE t0_session=%s AND t0_source_capture_id=%s""",
                ),
            ):
                cursor.execute(
                    query,
                    (
                        version["target_session"],
                        source_version["source_capture_id"],
                    ),
                )
                dependent_rows = int(cursor.fetchone()[0])
                if dependent_rows:
                    blockers.append(
                        {
                            "referenced_table": spec["parent_table"],
                            "dependent_table": dependent_table,
                            "constraint": "业务依赖：板块研究使用同日D0/Seed范围",
                            "reason": (
                                "先清理该日期的SECTOR_PULSE及板块后继，再重新预览D0/Seed；"
                                f"PG读回到{dependent_rows}条下游事实"
                            ),
                        }
                    )
    if module_code == "PRICE_LIMITS":
        selected_session_set = set(sessions)
        for version in versions:
            key = version["version_key"]
            cursor.execute(
                """SELECT target_session, count(*)
                   FROM stock.source_limit_up_streak_day_facts
                   WHERE target_session>%s AND target_session<=%s
                     AND source_capture_id=%s AND calendar_revision=%s
                   GROUP BY target_session
                   ORDER BY target_session""",
                (
                    version["target_session"],
                    cutoff,
                    key["source_capture_id"],
                    key["calendar_revision"],
                ),
            )
            downstream_dates = [
                (row[0].isoformat(), int(row[1]))
                for row in cursor.fetchall()
                if row[0] not in selected_session_set
            ]
            if downstream_dates:
                dates_text = ", ".join(f"{d}({c}行)" for d, c in downstream_dates)
                blockers.append(
                    {
                        "referenced_table": spec["parent_table"],
                        "dependent_table": "stock.source_limit_up_streak_day_facts",
                        "constraint": "业务依赖：后续会话日线连板递推依赖本会话限价事实",
                        "reason": (
                            "后续日期存在未纳入本次清理的连板事实，禁止越界静默删除或留存过期连板："
                            f"{dates_text}；请先清理对应后续日期的LIMIT_UP_LADDER或扩大清理日期范围"
                        ),
                    }
                )
    tables = list(
        spec.get("cleanup_tables", (spec["child_table"], spec["parent_table"]))
    )
    affected_sessions = set(sessions)
    for version in versions:
        affected_sessions.update(
            session
            for session in version.get("dependent_target_sessions", ())
            if type(session) is date and session <= cutoff
        )
    ordered_affected_sessions = tuple(sorted(affected_sessions))
    rows_by_table = {table: 0 for table in tables}
    for version in versions:
        rows_by_table[spec["parent_table"]] += int(version.get("parent_rows", 0))
        if spec.get("child_table"):
            rows_by_table[spec["child_table"]] += int(version.get("child_rows", 0))
        for table, count in version.get("dependent_rows", {}).items():
            if table in rows_by_table:
                rows_by_table[table] += int(count)
    r03_cascade_preview = _resolve_r03_upstream_cascade_plan(
        cursor, module_code=module_code, versions=versions, cutoff=cutoff
    )
    for r03_tbl, r03_cnt in r03_cascade_preview["counts_by_table"].items():
        if r03_cnt > 0:
            if r03_tbl not in tables:
                tables.append(r03_tbl)
            rows_by_table[r03_tbl] = rows_by_table.get(r03_tbl, 0) + int(r03_cnt)
    if r03_cascade_preview["affected_sessions"]:
        affected_sessions.update(r03_cascade_preview["affected_sessions"])
        ordered_affected_sessions = tuple(sorted(affected_sessions))
    affected_rows = sum(rows_by_table.values())
    source_versions = sorted(
        {
            json.dumps(version["source_version"], ensure_ascii=False, sort_keys=True)
            for version in versions
        }
    )
    algorithm_versions = sorted({version["algorithm_version"] for version in versions})
    dependency_rows = [
        *[
            {
                "table": table,
                "relation": "纳入本次精确模块清理顺序",
                "rows": rows_by_table[table],
                "handled": True,
            }
            for table in tables
        ],
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
    versions_for_key = []
    for version in versions:
        serialized_version = {
            **version,
            "target_session": version["target_session"].isoformat(),
            "dependent_target_sessions": [
                item.isoformat() if isinstance(item, date) else str(item)
                for item in version.get("dependent_target_sessions", ())
            ],
            "dependent_ids": {
                name: [
                    item.isoformat() if isinstance(item, date) else item
                    for item in values
                ]
                for name, values in version.get("dependent_ids", {}).items()
            },
        }
        versions_for_key.append(serialized_version)
    key_document = {
        "module_code": module_code,
        "target_sessions": [session.isoformat() for session in sessions],
        "affected_target_sessions": [
            session.isoformat() for session in ordered_affected_sessions
        ],
        "cutoff": cutoff.isoformat(),
        "versions": versions_for_key,
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
        "progress_tables": list(spec.get("progress_tables", tables)),
        "affected_rows_by_table": rows_by_table,
        "affected_rows": affected_rows,
        "dependents": dependency_rows,
        "raw_rows_untouched": True,
        "versions": key_document["versions"],
        "preview_key": preview_key,
        "reason_codes": (["UNRESOLVED_CLEANUP_DEPENDENCY"] if blockers else []),
        "database_write_attempted": False,
        "source_io_attempted": False,
    }


def preview_derived_cleanup(
    config: Mapping[str, Any],
    module_code: str,
    target_sessions: Sequence[date],
    cutoff: date,
    *,
    member_version: str | None = None,
    scope_key: str | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
) -> dict[str, Any]:
    """预览一个已登记派生模块的准确版本、日期、行数与外键依赖。"""
    _spec, sessions = _cleanup_scope(module_code, target_sessions, cutoff)
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        preview = _cleanup_preview_in_cursor(
            cursor,
            module_code=module_code,
            target_sessions=sessions,
            cutoff=cutoff,
            member_version=member_version,
            scope_key=scope_key,
            universe_version_t=universe_version_t,
            universe_version_t3=universe_version_t3,
        )
    if member_version is not None:
        preview["member_version_filter"] = member_version
    if scope_key is not None:
        preview["scope_key_filter"] = scope_key
    if universe_version_t is not None:
        preview["universe_version_t_filter"] = universe_version_t
    if universe_version_t3 is not None:
        preview["universe_version_t3_filter"] = universe_version_t3
    return preview


def _cleanup_preview_date(value: Any) -> date:
    """将预览JSON序列化的官方会话恢复为date键。"""
    if type(value) is date:
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_PREVIEW_SESSION_INVALID",
                "清理预览中的下游会话不是有效ISO日期",
            ) from error
    raise DataJobPgError(
        "DATA_JOB_CLEANUP_PREVIEW_SESSION_INVALID",
        "清理预览中的下游会话类型无效",
    )


def clear_derived_module(
    config: Mapping[str, Any],
    module_code: str,
    target_sessions: Sequence[date],
    cutoff: date,
    *,
    expected_preview_key: str | None = None,
    current_job_id: UUID | None = None,
    member_version: str | None = None,
    scope_key: str | None = None,
    universe_version_t: str | None = None,
    universe_version_t3: str | None = None,
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
            member_version=member_version,
            scope_key=scope_key,
            universe_version_t=universe_version_t,
            universe_version_t3=universe_version_t3,
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
        cleanup_count_tables = tuple(
            dict.fromkeys((*spec["cleanup_tables"], *spec["progress_tables"]))
        )
        busy_scope_sql = "target_session=ANY(%s)"
        busy_scope_parameters: tuple[Any, ...] = (list(sessions),)
        if module_code in {"CROSS_SECTION", "MARKET_INDICES_ENV"}:
            busy_scope_pairs = set()
            for version in preview["versions"]:
                key = version["version_key"]
                source_capture_id = (
                    key["price_capture_id"]
                    if module_code == "CROSS_SECTION"
                    else key["stock_capture_id"]
                )
                busy_scope_pairs.add(
                    (date.fromisoformat(version["target_session"]), source_capture_id)
                )
                busy_scope_pairs.update(
                    (_cleanup_preview_date(dependent_session), source_capture_id)
                    for dependent_session in version.get(
                        "dependent_target_sessions", ()
                    )
                )
            busy_scope_pairs = sorted(busy_scope_pairs)
            busy_scope_sql = """EXISTS (
                SELECT 1 FROM unnest(%s::date[],%s::bigint[])
                    AS affected(target_session,source_capture_id)
                WHERE affected.target_session=p.target_session
                  AND (p.source_capture_id=affected.source_capture_id
                       OR p.source_capture_id IS NULL)
            )"""
            busy_scope_parameters = (
                [item[0] for item in busy_scope_pairs],
                [item[1] for item in busy_scope_pairs],
            )
        cursor.execute(
            f"""SELECT count(*) FROM stock.data_job_progress AS p
               WHERE {busy_scope_sql} AND p.business_table=ANY(%s)
                 AND p.status='RUNNING' AND (%s::uuid IS NULL OR p.job_id<>%s)""",
            (
                *busy_scope_parameters,
                list(cleanup_count_tables),
                current_job_id,
                current_job_id,
            ),
        )
        if int(cursor.fetchone()[0]):
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_BUSY",
                "目标模块日期存在正在运行的生产单元；未删除任何事实",
            )
        deleted_rows: dict[str, int] = {table: 0 for table in cleanup_count_tables}
        deleted_by_session = {
            session.isoformat(): {table: 0 for table in cleanup_count_tables}
            for session in sessions
        }
        for version in preview["versions"]:
            target_session = date.fromisoformat(version["target_session"])
            key = version["version_key"]
            if module_code == "CORE_PROFILE":
                cursor.execute(
                    """DELETE FROM stock.source_symbol_price_profiles
                       WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s
                         AND producer_version=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["profile_revision"],
                        key["producer_version"],
                    ),
                )
                deleted_rows["stock.source_symbol_price_profiles"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_symbol_price_profiles"
                ] += cursor.rowcount
            elif module_code == "MARKET_BREADTH":
                if key.get("source_capture_id") is not None and key.get(
                    "calendar_revision"
                ):
                    cursor.execute(
                        """DELETE FROM stock.source_market_breadth_change_days
                           WHERE (target_session=%s AND target_source_capture_id=%s
                                  AND target_calendar_revision=%s
                                  AND target_contract_version=%s)
                              OR (before_session=%s AND before_source_capture_id=%s
                                  AND before_calendar_revision=%s
                                  AND before_contract_version=%s)
                           RETURNING target_session""",
                        (
                            target_session,
                            key["source_capture_id"],
                            key["calendar_revision"],
                            key["contract_version"],
                            target_session,
                            key["source_capture_id"],
                            key["calendar_revision"],
                            key["contract_version"],
                        ),
                    )
                    changed_market_sessions = [row[0] for row in cursor.fetchall()]
                    deleted_rows["stock.source_market_breadth_change_days"] += len(
                        changed_market_sessions
                    )
                    for changed_session in changed_market_sessions:
                        session_counts = deleted_by_session.setdefault(
                            changed_session.isoformat(),
                            {table: 0 for table in cleanup_count_tables},
                        )
                        session_counts["stock.source_market_breadth_change_days"] += 1
                    cursor.execute(
                        """DELETE FROM stock.source_market_breadth_symbols s
                           USING stock.source_market_breadth_days d
                           WHERE s.breadth_day_id=d.id AND d.target_session=%s
                             AND d.source_capture_id=%s
                             AND d.calendar_revision=%s AND d.contract_version=%s""",
                        (
                            target_session,
                            key["source_capture_id"],
                            key["calendar_revision"],
                            key["contract_version"],
                        ),
                    )
                    deleted_rows["stock.source_market_breadth_symbols"] += (
                        cursor.rowcount
                    )
                    deleted_by_session[target_session.isoformat()][
                        "stock.source_market_breadth_symbols"
                    ] += cursor.rowcount
                    cursor.execute(
                        """DELETE FROM stock.source_market_breadth_days
                           WHERE target_session=%s AND source_capture_id=%s AND calendar_revision=%s
                             AND contract_version=%s""",
                        (
                            target_session,
                            key["source_capture_id"],
                            key["calendar_revision"],
                            key["contract_version"],
                        ),
                    )
                    deleted_rows["stock.source_market_breadth_days"] += cursor.rowcount
                    deleted_by_session[target_session.isoformat()][
                        "stock.source_market_breadth_days"
                    ] += cursor.rowcount
                deleted_formal_env_keys: list[tuple[date, str, str, str]] = []
                for fkey in key.get("formal_breadth_keys", ()):
                    cursor.execute(
                        """DELETE FROM stock.r03_formal_market_breadth_facts
                           WHERE trade_date=%s AND scope_key=%s
                             AND universe_version_t=%s AND universe_version_t3=%s
                             AND contract_version=%s""",
                        (
                            target_session,
                            fkey["scope_key"],
                            fkey["universe_version_t"],
                            fkey["universe_version_t3"],
                            fkey["contract_version"],
                        ),
                    )
                    deleted_rows["stock.r03_formal_market_breadth_facts"] += (
                        cursor.rowcount
                    )
                    deleted_by_session[target_session.isoformat()][
                        "stock.r03_formal_market_breadth_facts"
                    ] += cursor.rowcount
                    if cursor.rowcount > 0:
                        deleted_formal_env_keys.append(
                            (
                                target_session,
                                str(fkey["scope_key"]),
                                str(fkey["universe_version_t"]),
                                str(fkey["universe_version_t3"]),
                            )
                        )
                if deleted_formal_env_keys:
                    _env_del, _env_inv = (
                        _cascade_invalidate_market_environment_for_formal_breadth(
                            cursor,
                            deleted_formal_keys=deleted_formal_env_keys,
                            current_job_id=current_job_id,
                            error_code="DERIVED_RESULT_CLEARED",
                        )
                    )
                    if (
                        _env_del > 0
                        and "stock.source_market_environment_day_facts" in deleted_rows
                    ):
                        deleted_rows["stock.source_market_environment_day_facts"] += (
                            _env_del
                        )
            elif module_code == "D1":
                cursor.execute(
                    """DELETE FROM stock.source_research_enrollments e
                       WHERE e.rule_fact_id IN (
                           SELECT r.id FROM stock.source_rule_evaluations r
                           WHERE r.target_session=%s AND r.source_capture_id=%s
                             AND r.d1_history_session_count=%s
                             AND r.calendar_revision=%s
                       )""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["d1_history_session_count"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_research_enrollments"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_research_enrollments"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_rule_evaluations
                       WHERE target_session=%s AND source_capture_id=%s AND d1_history_session_count=%s
                         AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["d1_history_session_count"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_rule_evaluations"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_rule_evaluations"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_d1_material_anchor_day_facts a
                       USING stock.source_d1_structure_day_facts d
                       WHERE a.d1_structure_id=d.id AND d.target_session=%s
                         AND d.source_capture_id=%s
                         AND d.d1_history_session_count=%s
                         AND d.producer_version=%s AND d.calendar_revision=%s""",
                    (
                        target_session,
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
                       WHERE target_session=%s AND source_capture_id=%s
                         AND d1_history_session_count=%s
                         AND producer_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
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
                         AND d.source_capture_id=%s
                         AND d.profile_revision=%s AND d.producer_version=%s
                         AND d.contract_version='MARKET_PARTICIPATION_R1'""",
                    (
                        target_session,
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
                       WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s
                         AND producer_version=%s
                         AND contract_version='MARKET_PARTICIPATION_R1'""",
                    (
                        target_session,
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
            elif module_code == "D0":
                cursor.execute(
                    """DELETE FROM stock.source_research_enrollments e
                       USING stock.source_d0_launch_day_facts d
                       WHERE e.d0_fact_id=d.id AND d.target_session=%s
                         AND d.source_capture_id=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_research_enrollments"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_research_enrollments"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_d0_launch_day_facts
                       WHERE target_session=%s AND source_capture_id=%s
                    """,
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_d0_launch_day_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_d0_launch_day_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_pool_seed_day_facts
                       WHERE target_session=%s AND source_capture_id=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_pool_seed_day_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_pool_seed_day_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_d0_scenario_day_facts
                       WHERE target_session=%s AND source_capture_id=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_d0_scenario_day_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_d0_scenario_day_facts"
                ] += cursor.rowcount
            elif module_code == "FOCUS_FORMAL":
                rule_scope = (
                    target_session,
                    key["source_capture_id"],
                    key["profile_revision"],
                )
                cursor.execute(
                    """DELETE FROM stock.source_t0_early_observation_facts t
                       WHERE t.rule_fact_id IN (
                           SELECT r.id FROM stock.source_rule_evaluations r
                           WHERE r.target_session=%s AND r.source_capture_id=%s
                             AND r.profile_revision IS NOT DISTINCT FROM %s
                       )""",
                    rule_scope,
                )
                deleted_rows["stock.source_t0_early_observation_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_t0_early_observation_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_research_enrollments e
                       WHERE e.rule_fact_id IN (
                           SELECT r.id FROM stock.source_rule_evaluations r
                           WHERE r.target_session=%s AND r.source_capture_id=%s
                             AND r.profile_revision IS NOT DISTINCT FROM %s
                       )""",
                    rule_scope,
                )
                deleted_rows["stock.source_research_enrollments"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_research_enrollments"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_rule_evaluations
                       WHERE target_session=%s AND source_capture_id=%s
                         AND profile_revision IS NOT DISTINCT FROM %s""",
                    rule_scope,
                )
                deleted_rows["stock.source_rule_evaluations"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_rule_evaluations"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_d0_formal_admission_day_facts
                       WHERE target_session=%s AND source_capture_id=%s
                         AND profile_revision IS NOT DISTINCT FROM %s
                         AND raw_producer_version=%s AND calendar_revision=%s
                         AND admission_contract=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["profile_revision"],
                        key["raw_producer_version"],
                        key["calendar_revision"],
                        key["admission_contract"],
                    ),
                )
                deleted_rows["stock.source_d0_formal_admission_day_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_d0_formal_admission_day_facts"
                ] += cursor.rowcount
            elif module_code == "R03_SECTOR_NATIVE":
                pass
            elif module_code == "R03_SECTOR_ROTATION":
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_board_day_facts
                       WHERE trade_date=%s AND scope_key=%s AND member_version=%s
                         AND contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_rotation_board_day_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_rotation_board_day_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_day_states s
                       USING stock.r03_sector_rotation_episodes e
                       WHERE s.episode_id=e.id AND s.trade_date=%s
                         AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_rotation_day_states"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_rotation_day_states"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_basket_members bm
                       USING stock.r03_sector_rotation_episodes e
                       WHERE bm.episode_id=e.id
                         AND e.scope_key=%s AND e.member_version=%s
                         AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id
                         )""",
                    (
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_rotation_basket_members"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_rotation_basket_members"
                ] += cursor.rowcount
                cursor.execute(
                    """UPDATE stock.r03_sector_rotation_episodes e
                       SET parent_episode_id=NULL
                       WHERE e.parent_episode_id IN (
                           SELECT ep.id FROM stock.r03_sector_rotation_episodes ep
                           WHERE ep.scope_key=%s AND ep.member_version=%s
                             AND ep.contract_version=%s
                             AND NOT EXISTS (
                                 SELECT 1 FROM stock.r03_sector_rotation_day_states s
                                 WHERE s.episode_id=ep.id
                             )
                       )
                         AND e.scope_key=%s AND e.member_version=%s
                         AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id
                         )""",
                    (
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_episodes e
                       WHERE e.scope_key=%s AND e.member_version=%s
                         AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id
                         )""",
                    (
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_rotation_episodes"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_rotation_episodes"
                ] += cursor.rowcount
            elif module_code == "R03_SECTOR_OVERLAP":
                cursor.execute(
                    """DELETE FROM stock.r03_sector_overlap_pair_facts
                       WHERE trade_date=%s AND scope_key=%s AND member_version=%s
                         AND contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_overlap_pair_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_overlap_pair_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.r03_sector_overlap_board_facts
                       WHERE trade_date=%s AND scope_key=%s AND member_version=%s
                         AND contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_overlap_board_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_overlap_board_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.r03_sector_overlap_clusters
                       WHERE trade_date=%s AND scope_key=%s AND member_version=%s
                         AND contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_overlap_clusters"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_overlap_clusters"
                ] += cursor.rowcount
            elif module_code == "R03_SECTOR_LOO":
                cursor.execute(
                    """DELETE FROM stock.r03_sector_loo_candidate_facts c
                       USING stock.r03_sector_loo_support_facts f
                       WHERE c.loo_fact_id=f.id AND f.trade_date=%s AND f.scope_key=%s
                         AND f.member_version=%s AND f.contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_loo_candidate_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_loo_candidate_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.r03_sector_loo_support_facts
                       WHERE trade_date=%s AND scope_key=%s AND member_version=%s
                         AND contract_version=%s""",
                    (
                        target_session,
                        key["scope_key"],
                        key["member_version"],
                        key["contract_version"],
                    ),
                )
                deleted_rows["stock.r03_sector_loo_support_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.r03_sector_loo_support_facts"
                ] += cursor.rowcount
            elif module_code == "SECTOR_PULSE":
                cursor.execute(
                    """DELETE FROM stock.source_sector_cross_period_pulse_facts
                       WHERE target_session=%s AND source_capture_id=%s AND profile_revision=%s
                         AND algorithm_contract=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["profile_revision"],
                        key["algorithm_contract"],
                    ),
                )
                deleted_rows["stock.source_sector_cross_period_pulse_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_sector_cross_period_pulse_facts"
                ] += cursor.rowcount
            elif module_code == "SECTOR_TRANSITION":
                cursor.execute(
                    """DELETE FROM stock.source_sector_pulse_adjacent_transitions
                       WHERE target_pulse_id=%s AND transition_contract=%s
                         AND calendar_id=%s AND calendar_revision=%s
                         AND previous_pulse_id=ANY(%s::bigint[])""",
                    (
                        key["target_pulse_id"],
                        key["transition_contract"],
                        key["calendar_id"],
                        key["calendar_revision"],
                        key["previous_pulse_ids"],
                    ),
                )
                deleted_rows["stock.source_sector_pulse_adjacent_transitions"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_sector_pulse_adjacent_transitions"
                ] += cursor.rowcount
            elif module_code == "PRICE_LIMITS":
                # 仅清理预览范围内同捕获、同日历、同会话的连板事实（后续未纳入日期已在预览阶段安全阻断）
                cursor.execute(
                    """DELETE FROM stock.source_limit_up_streak_day_facts
                       WHERE target_session=%s AND source_capture_id=%s
                         AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_limit_up_streak_day_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_limit_up_streak_day_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_price_limit_day_facts
                       WHERE target_session=%s AND source_capture_id=%s
                         AND contract_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["contract_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_price_limit_day_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_price_limit_day_facts"
                ] += cursor.rowcount
            elif module_code == "LIMIT_UP_LADDER":
                cursor.execute(
                    """DELETE FROM stock.source_limit_up_streak_day_facts
                       WHERE target_session=%s AND source_capture_id=%s
                         AND contract_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["contract_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_limit_up_streak_day_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_limit_up_streak_day_facts"
                ] += cursor.rowcount
            elif module_code == "CROSS_SECTION":
                cursor.execute(
                    """DELETE FROM stock.source_cross_section_endpoints
                       WHERE target_session=%s AND price_capture_id=%s
                         AND universe_version=%s AND contract_version=%s
                         AND calendar_revision=%s""",
                    (
                        target_session,
                        key["price_capture_id"],
                        key["universe_version"],
                        key["contract_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_cross_section_endpoints"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_cross_section_endpoints"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_cross_section_day_facts
                       WHERE target_session=%s AND price_capture_id=%s
                         AND universe_version=%s AND contract_version=%s
                         AND calendar_revision=%s""",
                    (
                        target_session,
                        key["price_capture_id"],
                        key["universe_version"],
                        key["contract_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_cross_section_day_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_cross_section_day_facts"
                ] += cursor.rowcount
            elif module_code == "MARKET_INDICES_ENV":
                cursor.execute(
                    """DELETE FROM stock.source_market_environment_day_facts
                       WHERE target_session=%s AND stock_capture_id=%s
                         AND index_capture_id IS NOT DISTINCT FROM %s
                         AND primary_index_symbol=%s
                         AND index_config_version IS NOT DISTINCT FROM %s
                         AND calendar_id=%s AND calendar_revision=%s
                         AND rule_version=%s""",
                    (
                        target_session,
                        key["stock_capture_id"],
                        key["index_capture_id"],
                        key["primary_index_symbol"],
                        key["index_config_version"],
                        key["calendar_id"],
                        key["calendar_revision"],
                        key["rule_version"],
                    ),
                )
                deleted_rows["stock.source_market_environment_day_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_market_environment_day_facts"
                ] += cursor.rowcount
            elif module_code == "FORWARD_PATH":
                cursor.execute(
                    """DELETE FROM stock.source_forward_path_facts
                       WHERE target_session=%s AND source_capture_id=%s
                         AND path_rule_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["path_rule_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_forward_path_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_forward_path_facts"
                ] += cursor.rowcount
            elif module_code == "COHORT_STATS":
                cursor.execute(
                    """DELETE FROM stock.source_cohort_group_stats
                       WHERE target_session=%s AND source_capture_id=%s
                         AND rule_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["rule_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_cohort_group_stats"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_cohort_group_stats"
                ] += cursor.rowcount
            elif module_code == "FEP_BASELINE":
                cursor.execute(
                    """DELETE FROM stock.source_fep_baseline_facts
                       WHERE COALESCE(target_session, period_end_session)=%s
                         AND source_capture_id=%s AND rule_version=%s
                         AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["rule_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_fep_baseline_facts"] += cursor.rowcount
                deleted_by_session[target_session.isoformat()][
                    "stock.source_fep_baseline_facts"
                ] += cursor.rowcount
            elif module_code == "OBSERVATION_SEGMENTS":
                cursor.execute(
                    """DELETE FROM stock.source_research_observation_segment_days
                       WHERE t0_session=%s AND source_capture_id=%s
                         AND rule_version=%s AND calendar_revision=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                        key["rule_version"],
                        key["calendar_revision"],
                    ),
                )
                deleted_rows["stock.source_research_observation_segment_days"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_research_observation_segment_days"
                ] += cursor.rowcount
            elif module_code == "SECTOR_FOLLOWUP":
                cursor.execute(
                    """DELETE FROM stock.source_sector_pulse_followup_facts f
                       USING stock.source_sector_cross_period_pulse_facts p
                       WHERE p.id=f.pulse_fact_id AND p.target_session=%s
                         AND p.source_capture_id=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_sector_pulse_followup_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_sector_pulse_followup_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_sector_pulse_cohort_reason_facts r
                       USING stock.source_sector_pulse_cohort_group_facts g
                       WHERE g.id=r.cohort_group_fact_id AND g.t0_session=%s
                         AND g.t0_source_capture_id=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_sector_pulse_cohort_reason_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_sector_pulse_cohort_reason_facts"
                ] += cursor.rowcount
                cursor.execute(
                    """DELETE FROM stock.source_sector_pulse_cohort_group_facts
                       WHERE t0_session=%s AND t0_source_capture_id=%s""",
                    (
                        target_session,
                        key["source_capture_id"],
                    ),
                )
                deleted_rows["stock.source_sector_pulse_cohort_group_facts"] += (
                    cursor.rowcount
                )
                deleted_by_session[target_session.isoformat()][
                    "stock.source_sector_pulse_cohort_group_facts"
                ] += cursor.rowcount

        if module_code == "R03_SECTOR_NATIVE":
            grouped_native: dict[tuple[str, str, str], set[date]] = {}
            for version in preview["versions"]:
                key = version["version_key"]
                gkey = (
                    key["scope_key"],
                    key["member_version"],
                    key["contract_version"],
                )
                day_set = grouped_native.setdefault(gkey, set())
                day_set.add(date.fromisoformat(version["target_session"]))
                for dep_day in version.get("dependent_target_sessions", ()):
                    day_set.add(_cleanup_preview_date(dep_day))

            def _record_native_del(table: str, session_d: date) -> None:
                deleted_rows[table] += 1
                sc = deleted_by_session.setdefault(
                    session_d.isoformat(),
                    {name: 0 for name in cleanup_count_tables},
                )
                sc[table] += 1

            for (s_key, m_ver, c_ver), day_set in grouped_native.items():
                affected_days = sorted(day_set)
                min_day = affected_days[0]
                cursor.execute(
                    """DELETE FROM stock.r03_sector_loo_candidate_facts c
                       USING stock.r03_sector_loo_support_facts f
                       WHERE c.loo_fact_id=f.id AND f.trade_date=ANY(%s)
                         AND f.scope_key=%s AND f.member_version=%s AND f.contract_version=%s
                       RETURNING f.trade_date""",
                    (affected_days, s_key, m_ver, c_ver),
                )
                for row in cursor.fetchall():
                    _record_native_del("stock.r03_sector_loo_candidate_facts", row[0])
                for tbl in (
                    "stock.r03_sector_loo_support_facts",
                    "stock.r03_sector_overlap_pair_facts",
                    "stock.r03_sector_overlap_board_facts",
                    "stock.r03_sector_overlap_clusters",
                    "stock.r03_sector_rotation_board_day_facts",
                ):
                    cursor.execute(
                        f"""DELETE FROM {tbl}
                            WHERE trade_date=ANY(%s) AND scope_key=%s
                              AND member_version=%s AND contract_version=%s
                            RETURNING trade_date""",
                        (affected_days, s_key, m_ver, c_ver),
                    )
                    for row in cursor.fetchall():
                        _record_native_del(tbl, row[0])
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_day_states s
                       USING stock.r03_sector_rotation_episodes e
                       WHERE s.episode_id=e.id AND s.trade_date=ANY(%s)
                         AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                       RETURNING s.trade_date""",
                    (affected_days, s_key, m_ver, c_ver),
                )
                for row in cursor.fetchall():
                    _record_native_del("stock.r03_sector_rotation_day_states", row[0])
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_basket_members bm
                       USING stock.r03_sector_rotation_episodes e
                       WHERE bm.episode_id=e.id
                         AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id
                         )
                       RETURNING e.t0_session""",
                    (s_key, m_ver, c_ver),
                )
                for row in cursor.fetchall():
                    _record_native_del(
                        "stock.r03_sector_rotation_basket_members", min_day
                    )
                cursor.execute(
                    """UPDATE stock.r03_sector_rotation_episodes e
                       SET parent_episode_id=NULL
                       WHERE e.parent_episode_id IN (
                           SELECT ep.id FROM stock.r03_sector_rotation_episodes ep
                           WHERE ep.scope_key=%s AND ep.member_version=%s AND ep.contract_version=%s
                             AND NOT EXISTS (
                                 SELECT 1 FROM stock.r03_sector_rotation_day_states s
                                 WHERE s.episode_id=ep.id
                             )
                       )
                         AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id
                         )""",
                    (s_key, m_ver, c_ver, s_key, m_ver, c_ver),
                )
                cursor.execute(
                    """DELETE FROM stock.r03_sector_rotation_episodes e
                       WHERE e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s
                         AND NOT EXISTS (
                             SELECT 1 FROM stock.r03_sector_rotation_day_states s
                             WHERE s.episode_id=e.id
                         )
                       RETURNING e.t0_session""",
                    (s_key, m_ver, c_ver),
                )
                for row in cursor.fetchall():
                    _record_native_del("stock.r03_sector_rotation_episodes", min_day)
                cursor.execute(
                    """DELETE FROM stock.r03_sector_native_day_facts
                       WHERE trade_date=ANY(%s) AND scope_key=%s
                         AND member_version=%s AND contract_version=%s
                       RETURNING trade_date""",
                    (affected_days, s_key, m_ver, c_ver),
                )
                for row in cursor.fetchall():
                    _record_native_del("stock.r03_sector_native_day_facts", row[0])

        if module_code == "MARKET_INDICES_ENV":
            grouped_scopes: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
            for version in preview["versions"]:
                key = version["version_key"]
                scope_key = (
                    key["stock_capture_id"],
                    key["index_capture_id"],
                    key["primary_index_symbol"],
                    key["index_config_version"],
                    key["calendar_id"],
                    key["calendar_revision"],
                    key["rule_version"],
                )
                grouped_scopes.setdefault(scope_key, []).append(version)
            selected_sessions = set(sessions)
            for scope_key, scoped_versions in grouped_scopes.items():
                root_sessions = sorted(
                    date.fromisoformat(version["target_session"])
                    for version in scoped_versions
                )
                (
                    stock_capture_id,
                    index_capture_id,
                    primary,
                    index_config,
                    cal_id,
                    cal_rev,
                    rule_ver,
                ) = scope_key
                cursor.execute(
                    """DELETE FROM stock.source_market_environment_day_facts
                       WHERE target_session>%s AND target_session<=%s
                         AND target_session<>ALL(%s::date[])
                         AND stock_capture_id=%s
                         AND index_capture_id IS NOT DISTINCT FROM %s
                         AND primary_index_symbol=%s
                         AND index_config_version IS NOT DISTINCT FROM %s
                         AND calendar_id=%s AND calendar_revision=%s
                         AND rule_version=%s
                       RETURNING target_session""",
                    (
                        root_sessions[0],
                        cutoff,
                        list(selected_sessions),
                        stock_capture_id,
                        index_capture_id,
                        primary,
                        index_config,
                        cal_id,
                        cal_rev,
                        rule_ver,
                    ),
                )
                for row in cursor.fetchall():
                    table = "stock.source_market_environment_day_facts"
                    deleted_rows[table] += 1
                    session_counts = deleted_by_session.setdefault(
                        row[0].isoformat(),
                        {name: 0 for name in cleanup_count_tables},
                    )
                    session_counts[table] += 1

        if module_code == "CROSS_SECTION":
            scopes_by_capture: dict[int, list[dict[str, Any]]] = {}
            for version in preview["versions"]:
                key = version["version_key"]
                scopes_by_capture.setdefault(key["price_capture_id"], []).append(
                    version
                )

            def record_deletion(table: str, count: int, session: date) -> None:
                if not count:
                    return
                deleted_rows[table] += count
                session_counts = deleted_by_session.setdefault(
                    session.isoformat(),
                    {name: 0 for name in cleanup_count_tables},
                )
                session_counts[table] += count

            for source_capture_id, scoped_versions in scopes_by_capture.items():
                selected_scope_sessions = tuple(
                    sorted(
                        {
                            date.fromisoformat(version["target_session"])
                            for version in scoped_versions
                        }
                    )
                )
                target_session = selected_scope_sessions[0]
                target_ids = scoped_versions[0].get("dependent_ids", {})
                enrollment_sessions: dict[int, date] = {}
                if target_ids["enrollment_ids"]:
                    cursor.execute(
                        """SELECT e.id,COALESCE(r.target_session,d.target_session)
                           FROM stock.source_research_enrollments e
                           LEFT JOIN stock.source_rule_evaluations r ON r.id=e.rule_fact_id
                           LEFT JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
                           WHERE e.id=ANY(%s::bigint[])""",
                        (list(target_ids["enrollment_ids"]),),
                    )
                    enrollment_sessions = {
                        int(row[0]): row[1] or target_session
                        for row in cursor.fetchall()
                    }

                cursor.execute(
                    """DELETE FROM stock.source_rps_pulse_t0_change_facts
                       WHERE source_t0_fact_id=ANY(%s::bigint[])
                          OR enrollment_id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (
                        list(target_ids["t0_fact_ids"]),
                        list(target_ids["enrollment_ids"]),
                    ),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_rps_pulse_t0_change_facts", 1, row[0])

                cursor.execute(
                    """DELETE FROM stock.source_research_observation_segment_days
                       WHERE anchor_rule_fact_id=ANY(%s::bigint[])
                          OR evidence_rule_fact_id=ANY(%s::bigint[])
                          OR source_enrollment_id=ANY(%s::bigint[])
                          OR parent_enrollment_id=ANY(%s::bigint[])
                          OR linked_enrollment_id=ANY(%s::bigint[])
                       RETURNING t0_session""",
                    (
                        list(target_ids["rule_fact_ids"]),
                        list(target_ids["rule_fact_ids"]),
                        list(target_ids["enrollment_ids"]),
                        list(target_ids["enrollment_ids"]),
                        list(target_ids["enrollment_ids"]),
                    ),
                )
                for row in cursor.fetchall():
                    record_deletion(
                        "stock.source_research_observation_segment_days", 1, row[0]
                    )

                cursor.execute(
                    """DELETE FROM stock.source_cohort_group_stats
                       USING stock.source_rule_evaluations r
                       WHERE r.id=ANY(%s::bigint[])
                         AND source_cohort_group_stats.target_session=r.target_session
                         AND source_cohort_group_stats.source_capture_id=r.source_capture_id
                         AND source_cohort_group_stats.rule_id=r.rule_id
                         AND source_cohort_group_stats.rule_version=r.rule_version
                         AND source_cohort_group_stats.profile_revision=r.profile_revision
                         AND source_cohort_group_stats.history_session_count=r.history_session_count
                         AND source_cohort_group_stats.d1_history_session_count=r.d1_history_session_count
                         AND source_cohort_group_stats.event_window_start=r.event_window_start
                         AND source_cohort_group_stats.calendar_revision=r.calendar_revision
                         AND source_cohort_group_stats.research_cutoff<=%s
                       RETURNING source_cohort_group_stats.target_session""",
                    (list(target_ids["rule_fact_ids"]), cutoff),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_cohort_group_stats", 1, row[0])

                cursor.execute(
                    """DELETE FROM stock.source_fep_baseline_facts
                       WHERE id=ANY(%s::bigint[])
                       RETURNING research_cutoff""",
                    (list(target_ids["fep_baseline_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_fep_baseline_facts", 1, row[0])

                cursor.execute(
                    """DELETE FROM stock.source_research_events
                       WHERE id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (list(target_ids["research_event_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_research_events", 1, row[0])

                cursor.execute(
                    """DELETE FROM stock.source_t0_early_observation_facts
                       WHERE id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (list(target_ids["t0_fact_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion(
                        "stock.source_t0_early_observation_facts", 1, row[0]
                    )

                cursor.execute(
                    """DELETE FROM stock.source_research_enrollments
                       WHERE id=ANY(%s::bigint[])
                       RETURNING id""",
                    (list(target_ids["enrollment_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion(
                        "stock.source_research_enrollments",
                        1,
                        enrollment_sessions.get(int(row[0]), target_session),
                    )

                cursor.execute(
                    """DELETE FROM stock.source_rule_evaluations
                       WHERE id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (list(target_ids["rule_fact_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_rule_evaluations", 1, row[0])

                if target_ids["profile_sessions"]:
                    cursor.execute(
                        """DELETE FROM stock.source_d0_formal_admission_day_facts
                           WHERE target_session=ANY(%s) AND source_capture_id=%s
                           RETURNING target_session""",
                        (list(target_ids["profile_sessions"]), source_capture_id),
                    )
                    for row in cursor.fetchall():
                        record_deletion(
                            "stock.source_d0_formal_admission_day_facts", 1, row[0]
                        )

                cursor.execute(
                    """DELETE FROM stock.source_d0_launch_day_facts
                       WHERE id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (list(target_ids["d0_fact_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_d0_launch_day_facts", 1, row[0])

                cursor.execute(
                    """DELETE FROM stock.source_d1_material_anchor_day_facts
                       WHERE d1_structure_id=ANY(%s::bigint[])
                       RETURNING created_session""",
                    (list(target_ids["d1_structure_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion(
                        "stock.source_d1_material_anchor_day_facts", 1, row[0]
                    )

                cursor.execute(
                    """DELETE FROM stock.source_d1_structure_day_facts
                       WHERE id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (list(target_ids["d1_structure_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_d1_structure_day_facts", 1, row[0])

                cursor.execute(
                    """DELETE FROM stock.source_pool_seed_day_facts
                       WHERE id=ANY(%s::bigint[])
                       RETURNING target_session""",
                    (list(target_ids["seed_fact_ids"]),),
                )
                for row in cursor.fetchall():
                    record_deletion("stock.source_pool_seed_day_facts", 1, row[0])

                if target_ids["profile_sessions"]:
                    cursor.execute(
                        """DELETE FROM stock.source_d0_scenario_day_facts
                           WHERE target_session=ANY(%s) AND source_capture_id=%s
                           RETURNING target_session""",
                        (list(target_ids["profile_sessions"]), source_capture_id),
                    )
                    for row in cursor.fetchall():
                        record_deletion("stock.source_d0_scenario_day_facts", 1, row[0])

                    cursor.execute(
                        """DELETE FROM stock.source_symbol_price_profiles
                           WHERE target_session=ANY(%s) AND source_capture_id=%s
                           RETURNING target_session""",
                        (list(target_ids["profile_sessions"]), source_capture_id),
                    )
                    for row in cursor.fetchall():
                        record_deletion("stock.source_symbol_price_profiles", 1, row[0])

        expected_counts = {
            table: int(preview.get("affected_rows_by_table", {}).get(table, 0))
            for table in cleanup_count_tables
        }
        if deleted_rows != expected_counts:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_ROWCOUNT_MISMATCH",
                "清理影响行数与预览不一致；事务回滚并保留原事实",
            )

        r03_plan = _resolve_r03_upstream_cascade_plan(
            cursor, module_code=module_code, versions=preview["versions"], cutoff=cutoff
        )
        r03_cascade_counts, r03_cascade_sessions, r03_inv_rows = (
            apply_r03_upstream_cascade_invalidation(
                cursor,
                r03_plan=r03_plan,
                cutoff=cutoff,
                current_job_id=current_job_id,
            )
        )
        r03_cascade_tables = [k for k, v in r03_cascade_counts.items() if v > 0]

        invalidated_tables = list(
            dict.fromkeys([*cleanup_count_tables, *r03_cascade_tables])
        )
        invalidated_sessions = sorted(set(sessions) | r03_cascade_sessions)
        downstream_invalidated_tables: list[str] = list(r03_cascade_tables)
        downstream_existing_fact_counts: dict[str, int] = {
            k: v for k, v in r03_cascade_counts.items() if v > 0
        }
        downstream_subsequent_sessions: list[str] = [
            d.isoformat() for d in sorted(r03_cascade_sessions - set(sessions))
        ]
        if module_code == "CROSS_SECTION" and sessions:
            downstream_tables = tuple(
                table
                for table in spec["progress_tables"]
                if table
                not in {
                    "stock.source_cross_section_day_facts",
                    "stock.source_cross_section_endpoints",
                }
            )
            downstream_invalidated_tables = list(
                dict.fromkeys([*downstream_tables, *r03_cascade_tables])
            )
            downstream_existing_fact_counts = {
                **{
                    table: int(preview["affected_rows_by_table"].get(table, 0))
                    for table in downstream_tables
                },
                **{k: v for k, v in r03_cascade_counts.items() if v > 0},
            }
        elif module_code == "R03_SECTOR_NATIVE" and sessions:
            subsequent = sorted(
                {
                    _cleanup_preview_date(session)
                    for version in preview["versions"]
                    for session in version.get("dependent_target_sessions", ())
                }
            )
            downstream_invalidated_tables = [
                tbl
                for tbl in cleanup_count_tables
                if tbl != "stock.r03_sector_native_day_facts"
            ]
            downstream_existing_fact_counts = {
                tbl: int(preview.get("affected_rows_by_table", {}).get(tbl, 0))
                for tbl in downstream_invalidated_tables
            }
            if subsequent:
                downstream_subsequent_sessions = [d.isoformat() for d in subsequent]
                invalidated_sessions = sorted(
                    set(invalidated_sessions) | set(subsequent)
                )
        elif module_code == "MARKET_INDICES_ENV" and sessions:
            subsequent = sorted(
                {
                    _cleanup_preview_date(session)
                    for version in preview["versions"]
                    for session in version.get("dependent_target_sessions", ())
                }
            )
            if subsequent:
                downstream_subsequent_sessions = [d.isoformat() for d in subsequent]
                downstream_invalidated_tables = [
                    "stock.source_market_environment_day_facts"
                ]
                invalidated_sessions = sorted(
                    set(invalidated_sessions) | set(subsequent)
                )
                downstream_existing_fact_counts[
                    "stock.source_market_environment_day_facts_subsequent"
                ] = len(subsequent)

        invalidated_rows: list[tuple[Any, str]] = []
        if module_code == "MARKET_BREADTH":
            source_mkt_pairs = sorted(
                {
                    (
                        date.fromisoformat(str(v["target_session"])),
                        int(v["version_key"]["source_capture_id"]),
                    )
                    for v in preview["versions"]
                    if v["version_key"].get("source_capture_id") is not None
                    and (
                        int(v.get("parent_rows", 0)) > 0
                        or int(v.get("child_rows", 0)) > 0
                        or int(
                            (v.get("dependent_rows") or {}).get(
                                "stock.source_market_breadth_change_days", 0
                            )
                        )
                        > 0
                    )
                }
            )
            if source_mkt_pairs:
                cursor.execute(
                    """UPDATE stock.data_job_progress AS p
                       SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                           verified_rows=NULL,inserted_rows=0,updated_rows=0,
                           quality_status=NULL,error_code='DERIVED_RESULT_CLEARED',
                           error_message='对应来源级市场宽度结果已清理；需按依赖重新生成。',
                           deleted_rows=0,
                           updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
                       FROM unnest(%s::date[],%s::bigint[]) AS affected(target_session,source_capture_id)
                       WHERE p.target_session=affected.target_session
                         AND p.source_capture_id=affected.source_capture_id
                         AND p.business_table=ANY(ARRAY[
                             'stock.source_market_breadth_days',
                             'stock.source_market_breadth_symbols',
                             'stock.source_market_breadth_change_days'
                         ]::text[])
                         AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                         AND (%s::uuid IS NULL OR p.job_id<>%s)
                       RETURNING p.job_id, p.business_table""",
                    (
                        [p[0] for p in source_mkt_pairs],
                        [p[1] for p in source_mkt_pairs],
                        current_job_id,
                        current_job_id,
                    ),
                )
                invalidated_rows.extend(cursor.fetchall())
            for v in preview["versions"]:
                td = date.fromisoformat(str(v["target_session"]))
                for fkey in v.get("version_key", {}).get("formal_breadth_keys", ()):
                    c_id, p_rev, _i, _co = _parse_r03_scope_and_member(
                        str(fkey["scope_key"]), None
                    )
                    m_conds = [
                        "p.target_session=%s",
                        "p.business_table='stock.r03_formal_market_breadth_facts'",
                        "p.universe_version_t=%s",
                        "p.universe_version_t3=%s",
                        "p.producer_version=%s",
                    ]
                    m_args: list[Any] = [
                        td,
                        str(fkey["universe_version_t"]),
                        str(fkey["universe_version_t3"]),
                        str(fkey["contract_version"]),
                    ]
                    if c_id is not None:
                        m_conds.append("p.source_capture_id=%s")
                        m_args.append(c_id)
                    if p_rev is not None:
                        m_conds.append("p.profile_revision=%s")
                        m_args.append(p_rev)
                    else:
                        m_conds.append("p.profile_revision IS NULL")
                    m_args.extend([current_job_id, current_job_id])
                    cursor.execute(
                        f"""UPDATE stock.data_job_progress AS p
                           SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                               verified_rows=NULL,inserted_rows=0,updated_rows=0,
                               quality_status=NULL,error_code='DERIVED_RESULT_CLEARED',
                               error_message='对应正式市场宽度分区已清理；需按精确版本重新生成。',
                               deleted_rows=0,
                               updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
                           WHERE {" AND ".join(m_conds)}
                             AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                             AND (%s::uuid IS NULL OR p.job_id<>%s)
                           RETURNING job_id, business_table""",
                        tuple(m_args),
                    )
                    invalidated_rows.extend(cursor.fetchall())
        elif module_code in {
            "R03_SECTOR_NATIVE",
            "R03_SECTOR_ROTATION",
            "R03_SECTOR_OVERLAP",
            "R03_SECTOR_LOO",
        }:
            r03_prog_tables = [
                t
                for t in cleanup_count_tables
                if t != "stock.r03_formal_market_breadth_facts"
            ]
            for v in preview["versions"]:
                vk = v.get("version_key") or {}
                sk = str(vk.get("scope_key") or "")
                mv = str(vk.get("member_version") or "")
                cv = str(vk.get("contract_version") or v.get("algorithm_version") or "")
                if not sk or not mv:
                    continue
                c_id, p_rev, i_id, co_id = _parse_r03_scope_and_member(sk, mv)
                target_days_for_v = [date.fromisoformat(str(v["target_session"]))]
                target_days_for_v.extend(
                    _cleanup_preview_date(s)
                    for s in v.get("dependent_target_sessions", ())
                )
                for td in sorted(set(target_days_for_v)):
                    p_conds = ["p.target_session=%s", "p.business_table=ANY(%s)"]
                    p_args: list[Any] = [td, r03_prog_tables]
                    if c_id is not None:
                        p_conds.append("p.source_capture_id=%s")
                        p_args.append(c_id)
                    if p_rev is not None:
                        p_conds.append("p.profile_revision=%s")
                        p_args.append(p_rev)
                    if i_id is not None:
                        p_conds.append("p.industry_snapshot_id=%s")
                        p_args.append(i_id)
                    if co_id is not None:
                        p_conds.append("p.concept_snapshot_id=%s")
                        p_args.append(co_id)
                    if cv:
                        p_conds.append("p.producer_version=%s")
                        p_args.append(cv)
                    p_args.extend([current_job_id, current_job_id])
                    cursor.execute(
                        f"""UPDATE stock.data_job_progress AS p
                           SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                               verified_rows=NULL,inserted_rows=0,updated_rows=0,
                               quality_status=NULL,error_code='DERIVED_RESULT_CLEARED',
                               error_message='对应R03正式板块分区或其前态已清理；需按依赖重新生成。',
                               deleted_rows=0,
                               updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
                           WHERE {" AND ".join(p_conds)}
                             AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                             AND (%s::uuid IS NULL OR p.job_id<>%s)
                           RETURNING job_id, business_table""",
                        tuple(p_args),
                    )
                    invalidated_rows.extend(cursor.fetchall())
        else:
            progress_scope_sql = "p.target_session=ANY(%s)"
            progress_scope_parameters: tuple[Any, ...] = (invalidated_sessions,)
            if module_code in {"CROSS_SECTION", "MARKET_INDICES_ENV"}:
                scope_pairs = set()
                for version in preview["versions"]:
                    key = version["version_key"]
                    source_capture_id = (
                        key["price_capture_id"]
                        if module_code == "CROSS_SECTION"
                        else key["stock_capture_id"]
                    )
                    scope_pairs.add(
                        (
                            date.fromisoformat(str(version["target_session"])),
                            source_capture_id,
                        )
                    )
                    scope_pairs.update(
                        (_cleanup_preview_date(dependent_session), source_capture_id)
                        for dependent_session in version.get(
                            "dependent_target_sessions", ()
                        )
                    )
                scope_pairs = sorted(scope_pairs)
                progress_scope_sql = """EXISTS (
                    SELECT 1 FROM unnest(%s::date[],%s::bigint[])
                        AS affected(target_session,source_capture_id)
                    WHERE affected.target_session=p.target_session
                      AND affected.source_capture_id=p.source_capture_id
                )"""
                progress_scope_parameters = (
                    [item[0] for item in scope_pairs],
                    [item[1] for item in scope_pairs],
                )
            cursor.execute(
                f"""UPDATE stock.data_job_progress AS p
                   SET status='NOT_COMPUTED',completed_units=0,expected_rows=NULL,
                       verified_rows=NULL,inserted_rows=0,updated_rows=0,
                       quality_status=NULL,error_code='DERIVED_RESULT_CLEARED',
                       error_message='对应派生结果或其依赖的上游横截面/前态已清理；需按依赖重新生成。',
                       deleted_rows=0,
                       updated_at=clock_timestamp(),completed_at=NULL,verified_at=NULL
                   WHERE {progress_scope_sql} AND p.business_table=ANY(%s)
                     AND p.status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE')
                     AND (%s::uuid IS NULL OR p.job_id<>%s)
                   RETURNING job_id, business_table""",
                (
                    *progress_scope_parameters,
                    [
                        t
                        for t in cleanup_count_tables
                        if t != "stock.r03_formal_market_breadth_facts"
                    ],
                    current_job_id,
                    current_job_id,
                ),
            )
            invalidated_rows.extend(cursor.fetchall())
        invalidated_rows.extend(r03_inv_rows)
        invalidated_job_ids = {row[0] for row in invalidated_rows}
        downstream_invalidated_progress_count = sum(
            1 for row in invalidated_rows if row[1] in downstream_invalidated_tables
        )
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
            "downstream_invalidated_tables": downstream_invalidated_tables,
            "downstream_invalidated_progress_count": downstream_invalidated_progress_count,
            "downstream_existing_fact_counts": downstream_existing_fact_counts,
            "downstream_subsequent_sessions": downstream_subsequent_sessions,
            "invalidated_progress_units": len(invalidated_rows),
            "database_write_attempted": True,
            "source_io_attempted": False,
        }
    return result


def _job_row(cursor: Any, job_id: UUID) -> dict[str, Any] | None:
    cursor.execute(
        """SELECT job_id,request_key,mode,initialization_mode,daily_mode,
                  profile_history_session_count,reset_preview_fingerprint,
                  target_session,research_cutoff,
                  history_session_count,d0_output_session_count,d1_output_session_count,
                  operation,selected_modules,module_output_session_counts,
                  source_version_fingerprint,
                  calendar_id,calendar_revision,status,planned_units,completed_units,
                  source_capture_id,created_at,started_at,updated_at,completed_at,
                  last_error_code,last_error_message
           FROM stock.data_jobs WHERE job_id=%s""",
        (job_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    names = [column.name for column in cursor.description]
    result = dict(zip(names, row, strict=True))
    selected_modules = result.get("selected_modules")
    if (
        result.get("mode") == "INITIALIZE"
        and result.get("initialization_mode") == "RESET_ALL_BUSINESS_FACTS"
        and isinstance(selected_modules, (list, tuple))
        and set(selected_modules) == {"BAOSTOCK", "TDX_SECTOR_MEMBERS"}
        and len(selected_modules) == 2
    ):
        result["selected_modules"] = [
            "BAOSTOCK_PRICES",
            "TDX_SECTOR_MEMBERS",
        ]
    return result


def _progress_rows(cursor: Any, job_id: UUID) -> list[dict[str, Any]]:
    cursor.execute(
        """SELECT stage_order,stage_code,target_session,business_table,planned_units,
                  completed_units,expected_rows,verified_rows,inserted_rows,updated_rows,
                  source_capture_id,
                  profile_revision,producer_version,profile_history_session_count,
                  d1_history_session_count,calendar_revision,
                  industry_snapshot_id,concept_snapshot_id,deleted_rows,
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
                      deleted_rows,source_capture_id,profile_revision,producer_version,
                      profile_history_session_count,d1_history_session_count,
                      calendar_revision,industry_snapshot_id,concept_snapshot_id,
                      universe_version_t,universe_version_t1,universe_version_t3,
                      source_capture_id_t1,source_capture_id_t3,
                      profile_revision_t1,profile_revision_t3,
                      prev_scope_key,prev_member_version,prev_trade_date,
                      industry_snapshot_id_t1,concept_snapshot_id_t1,
                      industry_snapshot_id_t3,concept_snapshot_id_t3,
                      source_capture_id_t4,prev_session_t,prev_session_t3,
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
            """SELECT job_id,request_key,mode,initialization_mode,daily_mode,
                      profile_history_session_count,reset_preview_fingerprint,
                      target_session,research_cutoff,
                      history_session_count,d0_output_session_count,d1_output_session_count,
                      operation,selected_modules,module_output_session_counts,
                      source_version_fingerprint,
                      calendar_id,calendar_revision,status,planned_units,completed_units,
                      source_capture_id,created_at,started_at,updated_at,completed_at,
                      last_error_code,last_error_message
               FROM stock.data_jobs
               ORDER BY created_at DESC,job_id DESC LIMIT %s""",
            (limit,),
        )
        names = [column.name for column in cursor.description]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    for row in rows:
        selected_modules = row.get("selected_modules")
        if (
            row.get("mode") == "INITIALIZE"
            and row.get("initialization_mode") == "RESET_ALL_BUSINESS_FACTS"
            and isinstance(selected_modules, (list, tuple))
            and set(selected_modules) == {"BAOSTOCK", "TDX_SECTOR_MEMBERS"}
            and len(selected_modules) == 2
        ):
            row["selected_modules"] = [
                "BAOSTOCK_PRICES",
                "TDX_SECTOR_MEMBERS",
            ]
    return rows


def read_business_table_count(
    config: Mapping[str, Any],
    *,
    business_table: str,
    target_session: date,
    source_capture_id: int | None,
    profile_history_session_count: int | None = None,
    d1_history_session_count: int | None = None,
    profile_revision: str | None = None,
    producer_version: str | Sequence[str] | None = None,
    rule_ids: Sequence[str] | None = None,
    calendar_revision: str | None = None,
    sector_snapshot_ids: Mapping[str, int] | None = None,
    followup_target_sessions: Sequence[date] | None = None,
    research_cutoff: date | None = None,
    universe_version: str | None = None,
    universe_version_t3: str | None = None,
    universe_capture_id: int | None = None,
    cross_section_universe_capture_id: int | None = None,
    cross_section_universe_version: str | None = None,
    cross_section_contract_version: str | None = None,
    universe_match_status: str | None = None,
) -> int:
    """按唯一来源捕获键和实际业务维度独立读回已提交行数。"""
    if (
        type(target_session) is not date
        or type(research_cutoff) is not date
        or target_session > research_cutoff
        or (
            rule_ids is not None
            and (
                not isinstance(rule_ids, Sequence)
                or isinstance(rule_ids, (str, bytes))
                or not rule_ids
                or any(
                    not isinstance(rule_id, str) or not rule_id for rule_id in rule_ids
                )
                or len(set(rule_ids)) != len(rule_ids)
            )
        )
        or universe_capture_id is not None
        and (type(universe_capture_id) is not int or universe_capture_id <= 0)
        or cross_section_universe_capture_id is not None
        and (
            type(cross_section_universe_capture_id) is not int
            or cross_section_universe_capture_id <= 0
        )
        or cross_section_universe_version is not None
        and (
            not isinstance(cross_section_universe_version, str)
            or not cross_section_universe_version.strip()
        )
        or cross_section_contract_version is not None
        and (
            not isinstance(cross_section_contract_version, str)
            or not cross_section_contract_version.strip()
        )
        or universe_match_status is not None
        and (
            not isinstance(universe_match_status, str)
            or not universe_match_status.strip()
        )
    ):
        raise DataJobPgError(
            "DATA_JOB_CUTOFF_REQUIRED", "事实读回必须绑定不超过截止日的官方会话"
        )
    snapshots = [
        sector_snapshot_ids[kind]
        for kind in ("INDUSTRY", "CONCEPT")
        if isinstance(sector_snapshot_ids, Mapping)
        and type(sector_snapshot_ids.get(kind)) is int
    ]
    producer_versions = (
        [producer_version]
        if isinstance(producer_version, str)
        else list(producer_version or ())
    )
    queries = {
        "stock.source_captures": (
            """SELECT count(*) FROM stock.source_captures
               WHERE capture_id=%s AND provider IN ('BAOSTOCK','TDX')
                 AND range_end_session<=%s""",
            lambda: (source_capture_id, research_cutoff),
        ),
        "stock.source_capture_requests": (
            """SELECT count(*) FROM stock.source_capture_requests
               WHERE capture_id=%s AND range_start_session<=%s
                 AND range_end_session>=%s AND range_end_session<=%s
                 AND EXISTS (
                     SELECT 1 FROM stock.source_captures c
                     WHERE c.capture_id=source_capture_requests.capture_id
                       AND c.provider='BAOSTOCK'
                 )""",
            lambda: (
                source_capture_id,
                target_session,
                target_session,
                research_cutoff,
            ),
        ),
        "stock.tdx_sector_source_snapshots": (
            "SELECT count(*) FROM stock.tdx_sector_source_snapshots WHERE snapshot_id=ANY(%s::bigint[])",
            lambda: (snapshots,),
        ),
        "stock.tdx_sector_catalog": (
            "SELECT count(*) FROM stock.tdx_sector_catalog WHERE snapshot_id=ANY(%s::bigint[])",
            lambda: (snapshots,),
        ),
        "stock.tdx_sector_members": (
            """SELECT count(*) FROM stock.tdx_sector_members m
               JOIN stock.tdx_sector_catalog c ON c.id=m.catalog_id
               WHERE c.snapshot_id=ANY(%s::bigint[])""",
            lambda: (snapshots,),
        ),
        "stock.source_raw_daily": (
            """SELECT count(*) FROM stock.source_raw_daily d
               JOIN stock.source_captures c ON c.capture_id=d.source_capture_id
               WHERE d.session_date=%s AND d.source_capture_id=%s
                 AND d.session_date<=%s AND c.provider='BAOSTOCK'
                 AND c.range_end_session<=%s
                 AND c.price_coordinates @> ARRAY[
                     'RAW_UNADJUSTED','FORWARD_ADJUSTED'
                 ]::text[]""",
            lambda: (
                target_session,
                source_capture_id,
                research_cutoff,
                research_cutoff,
            ),
        ),
        "stock.source_symbol_price_profiles": (
            """SELECT count(*) FROM stock.source_symbol_price_profiles
               WHERE target_session=%s AND source_capture_id=%s
                 AND profile_revision=%s""",
            lambda: (target_session, source_capture_id, profile_revision),
        ),
        "stock.source_d0_scenario_day_facts": (
            """SELECT count(*) FROM stock.source_d0_scenario_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND producer_version=ANY(%s::text[])
                 AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_versions,
                calendar_revision,
            ),
        ),
        "stock.source_pool_seed_day_facts": (
            """SELECT count(*) FROM stock.source_pool_seed_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND history_session_count=%s AND profile_revision=%s
                 AND producer_version=ANY(%s::text[]) AND calendar_revision=%s
                 AND cross_section_universe_capture_id IS NOT DISTINCT FROM %s
                 AND cross_section_universe_version IS NOT DISTINCT FROM %s
                 AND cross_section_contract_version IS NOT DISTINCT FROM %s
                 AND universe_match_status IS NOT DISTINCT FROM %s""",
            lambda: (
                target_session,
                source_capture_id,
                profile_history_session_count,
                profile_revision,
                producer_versions,
                calendar_revision,
                cross_section_universe_capture_id,
                cross_section_universe_version,
                cross_section_contract_version,
                universe_match_status,
            ),
        ),
        "stock.source_d0_launch_day_facts": (
            """SELECT count(*) FROM stock.source_d0_launch_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND producer_version=%s AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_d1_structure_day_facts": (
            """SELECT count(*) FROM stock.source_d1_structure_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND d1_history_session_count=%s
                 AND producer_version=%s AND calendar_revision=%s
                 AND cross_section_universe_capture_id IS NOT DISTINCT FROM %s
                 AND cross_section_universe_version IS NOT DISTINCT FROM %s
                 AND cross_section_contract_version IS NOT DISTINCT FROM %s""",
            lambda: (
                target_session,
                source_capture_id,
                d1_history_session_count,
                producer_version,
                calendar_revision,
                cross_section_universe_capture_id,
                cross_section_universe_version,
                cross_section_contract_version,
            ),
        ),
        "stock.source_d1_material_anchor_day_facts": (
            """SELECT count(*) FROM stock.source_d1_material_anchor_day_facts a
               JOIN stock.source_d1_structure_day_facts d ON d.id=a.d1_structure_id
               WHERE d.target_session=%s AND d.source_capture_id=%s
                 AND d.d1_history_session_count=%s
                 AND d.producer_version=%s AND d.calendar_revision=%s
                 AND d.cross_section_universe_capture_id IS NOT DISTINCT FROM %s
                 AND d.cross_section_universe_version IS NOT DISTINCT FROM %s
                 AND d.cross_section_contract_version IS NOT DISTINCT FROM %s""",
            lambda: (
                target_session,
                source_capture_id,
                d1_history_session_count,
                producer_version,
                calendar_revision,
                cross_section_universe_capture_id,
                cross_section_universe_version,
                cross_section_contract_version,
            ),
        ),
        "stock.source_research_enrollments": (
            """SELECT count(*) FROM stock.source_research_enrollments e
               JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
               WHERE d.target_session=%s AND d.source_capture_id=%s
                 AND d.producer_version=%s AND d.calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_d0_formal_admission_day_facts": (
            """SELECT count(*) FROM stock.source_d0_formal_admission_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND profile_revision IS NOT DISTINCT FROM %s
                 AND raw_producer_version=%s AND calendar_revision=%s
                 AND admission_contract='STOCK_A_D0_EVENT_ADMISSION_R1'""",
            lambda: (
                target_session,
                source_capture_id,
                profile_revision,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_t0_early_observation_facts": (
            """SELECT count(*)
               FROM stock.source_t0_early_observation_facts o
               JOIN stock.source_pool_seed_day_facts s ON s.id=o.seed_fact_id
               WHERE o.target_session=%s AND o.source_capture_id=%s
                 AND o.hypothesis_version=%s
                 AND o.research_mode='NON_PIT_EXPLORATORY'
                 AND o.price_coordinate='FORWARD_ADJUSTED'
                 AND s.profile_revision=%s
                 AND s.history_session_count=%s
                 AND s.producer_version='SOURCE_POOL_SEED_CORE_PROFILE_R2'
                 AND (o.membership_snapshot_id=ANY(%s::bigint[])
                      OR o.membership_snapshot_id IS NULL)""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                profile_revision,
                profile_history_session_count,
                snapshots,
            ),
        ),
        "stock.source_rule_evaluations": (
            """SELECT count(*)
               FROM stock.source_rule_evaluations r
               WHERE r.target_session=%s AND r.source_capture_id=%s
                 AND r.rule_version=%s AND r.profile_revision=%s
                 AND r.history_session_count=%s
                 AND r.d1_history_session_count=%s
                 AND r.calendar_revision=%s AND r.scope='NON_PIT_EXPLORATORY'
                 AND r.event_window_start=(
                     SELECT min(d.history_window_start)
                     FROM stock.source_d1_structure_day_facts d
                     WHERE d.target_session=%s AND d.source_capture_id=%s
                       AND d.d1_history_session_count=%s
                       AND d.calendar_revision=%s
                       AND d.producer_version IN ('STOCK_A_D1_BAOSTOCK_QFQ_R3', 'STOCK_A_D1_BAOSTOCK_QFQ_R4')
                       AND d.research_mode='NON_PIT_RETROSPECTIVE'
                       AND d.price_coordinate='FORWARD_ADJUSTED'
                     HAVING count(DISTINCT d.history_window_start)=1
                 )""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                profile_revision,
                profile_history_session_count,
                d1_history_session_count,
                calendar_revision,
                target_session,
                source_capture_id,
                d1_history_session_count,
                calendar_revision,
            ),
        ),
        "stock.source_price_limit_day_facts": (
            """SELECT count(*) FROM stock.source_price_limit_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND contract_version=%s AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_limit_up_streak_day_facts": (
            """SELECT count(*) FROM stock.source_limit_up_streak_day_facts
               WHERE target_session=%s AND source_capture_id=%s
                 AND contract_version=%s AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_baostock_stock_basic": (
            """SELECT count(*) FROM stock.source_baostock_stock_basic
               WHERE source_capture_id=%s""",
            lambda: (source_capture_id,),
        ),
        "stock.source_baostock_session_directory": (
            """SELECT count(*) FROM stock.source_baostock_session_directory
               WHERE source_capture_id=%s AND session_date<=%s""",
            lambda: (source_capture_id, target_session),
        ),
        "stock.source_market_index_daily": (
            """SELECT count(*) FROM stock.source_market_index_daily
               WHERE source_capture_id=%s AND session_date<=%s""",
            lambda: (source_capture_id, target_session),
        ),
        "stock.source_cross_section_day_facts": (
            """SELECT count(*) FROM stock.source_cross_section_day_facts
               WHERE target_session=%s AND price_capture_id=%s
                 AND contract_version=%s AND calendar_revision=%s
                 AND (%s::text IS NULL OR universe_version=%s)
                 AND (%s::bigint IS NULL OR universe_capture_id IS NOT DISTINCT FROM %s)""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
                universe_version,
                universe_version,
                universe_capture_id,
                universe_capture_id,
            ),
        ),
        "stock.source_cross_section_endpoints": (
            """SELECT count(*) FROM stock.source_cross_section_endpoints
               WHERE target_session=%s AND price_capture_id=%s
                 AND contract_version=%s AND calendar_revision=%s
                 AND (%s::text IS NULL OR universe_version=%s)
                 AND (%s::bigint IS NULL OR universe_capture_id IS NOT DISTINCT FROM %s)""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
                universe_version,
                universe_version,
                universe_capture_id,
                universe_capture_id,
            ),
        ),
        "stock.source_market_index_day_facts": (
            """SELECT count(*) FROM stock.source_market_index_day_facts
               WHERE target_session=%s AND index_capture_id=%s
                 AND contract_version=%s AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_market_environment_day_facts": (
            """SELECT count(*) FROM stock.source_market_environment_day_facts
               WHERE target_session=%s AND stock_capture_id=%s
                 AND rule_version=%s AND calendar_revision=%s""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
            ),
        ),
        "stock.source_forward_path_facts": (
            """SELECT count(*) FROM stock.source_forward_path_facts
               WHERE target_session=%s AND source_capture_id=%s""",
            lambda: (target_session, source_capture_id),
        ),
        "stock.source_cohort_group_stats": (
            """SELECT count(*) FROM stock.source_cohort_group_stats
               WHERE target_session=%s AND source_capture_id=%s
                 AND research_cutoff=%s""",
            lambda: (
                target_session,
                source_capture_id,
                research_cutoff,
            ),
        ),
        "stock.source_fep_baseline_facts": (
            """SELECT count(*) FROM stock.source_fep_baseline_facts
               WHERE research_cutoff=%s
                 AND (source_capture_id=%s
                      OR (source_capture_id IS NULL AND summary_scope='EQUAL_DATE'))""",
            lambda: (research_cutoff, source_capture_id),
        ),
        "stock.source_research_observation_segment_days": (
            """SELECT count(*) FROM stock.source_research_observation_segment_days
               WHERE t0_session=%s AND source_capture_id=%s
                 AND rule_version=%s AND calendar_revision=%s
                 AND observation_session<=%s
                 AND (%s=false OR rule_id=ANY(%s::text[]))""",
            lambda: (
                target_session,
                source_capture_id,
                producer_version,
                calendar_revision,
                research_cutoff,
                bool(rule_ids),
                list(rule_ids or ()),
            ),
        ),
        "stock.source_market_breadth_days": (
            """SELECT count(*) FROM stock.source_market_breadth_days
               WHERE target_session=%s AND source_capture_id=%s
                 AND calendar_revision=%s
                 AND contract_version='MKT-02_BAOSTOCK_QFQ_POSITIVE_RETURN_STATE_DELTA_R3'""",
            lambda: (target_session, source_capture_id, calendar_revision),
        ),
        "stock.source_market_breadth_change_days": (
            """SELECT count(*) FROM stock.source_market_breadth_change_days
               WHERE target_session=%s AND target_source_capture_id=%s
                 AND target_calendar_revision=%s
                 AND target_contract_version='MKT-02_BAOSTOCK_QFQ_POSITIVE_RETURN_STATE_DELTA_R3'""",
            lambda: (target_session, source_capture_id, calendar_revision),
        ),
        "stock.source_market_breadth_symbols": (
            """SELECT count(*) FROM stock.source_market_breadth_symbols
               WHERE target_session=%s AND source_capture_id=%s
                 AND calendar_revision=%s
                 AND contract_version='MKT-02_BAOSTOCK_QFQ_POSITIVE_RETURN_STATE_DELTA_R3'""",
            lambda: (target_session, source_capture_id, calendar_revision),
        ),
        "stock.r03_formal_market_breadth_facts": (
            """SELECT count(*) FROM stock.r03_formal_market_breadth_facts
               WHERE trade_date=%s AND contract_version=%s
                 AND (%s::text IS NULL OR scope_key=%s)
                 AND (%s::text IS NULL OR universe_version_t=%s)
                 AND (%s::text IS NULL OR universe_version_t3=%s)""",
            lambda: (
                target_session,
                producer_version or "R03_FORMAL_SECTOR_V1",
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else f"BAOSTOCK_CAP_{source_capture_id}:MARKET_QFQ:NON_PIT"
                    if source_capture_id is not None
                    else None
                ),
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else f"BAOSTOCK_CAP_{source_capture_id}:MARKET_QFQ:NON_PIT"
                    if source_capture_id is not None
                    else None
                ),
                universe_version,
                universe_version,
                universe_version_t3,
                universe_version_t3,
            ),
        ),
        "stock.source_market_participation_days": (
            """SELECT count(*) FROM stock.source_market_participation_days
               WHERE target_session=%s AND source_capture_id=%s
                 AND profile_revision=%s AND producer_version=%s
                 AND contract_version='MARKET_PARTICIPATION_R1'""",
            lambda: (
                target_session,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        ),
        "stock.source_market_participation_unknown_reasons": (
            """SELECT count(*) FROM stock.source_market_participation_unknown_reasons r
               JOIN stock.source_market_participation_days d
                 ON d.id=r.participation_day_id
               WHERE d.target_session=%s AND d.source_capture_id=%s
                 AND d.profile_revision=%s AND d.producer_version=%s
                 AND d.contract_version='MARKET_PARTICIPATION_R1'""",
            lambda: (
                target_session,
                source_capture_id,
                profile_revision,
                producer_version,
            ),
        ),
        "stock.r03_sector_native_day_facts": (
            """SELECT count(*) FROM stock.r03_sector_native_day_facts
               WHERE trade_date=%s AND contract_version='R03_FORMAL_SECTOR_V1'
                 AND (%s::text IS NULL OR scope_key=%s)
                 AND (%s::text IS NULL OR member_version=%s)""",
            lambda: (
                target_session,
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            ),
        ),
        "stock.r03_sector_rotation_board_day_facts": (
            """SELECT count(*) FROM stock.r03_sector_rotation_board_day_facts
               WHERE trade_date=%s AND contract_version='R03_FORMAL_SECTOR_V1'
                 AND (%s::text IS NULL OR scope_key=%s)
                 AND (%s::text IS NULL OR member_version=%s)""",
            lambda: (
                target_session,
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            ),
        ),
        "stock.r03_sector_rotation_day_states": (
            """SELECT count(*) FROM stock.r03_sector_rotation_day_states s
               JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
               WHERE s.trade_date=%s AND e.contract_version='R03_FORMAL_SECTOR_V1'
                 AND (%s::text IS NULL OR e.scope_key=%s)
                 AND (%s::text IS NULL OR e.member_version=%s)""",
            lambda: (
                target_session,
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            ),
        ),
        "stock.r03_sector_overlap_clusters": (
            """SELECT count(*) FROM stock.r03_sector_overlap_clusters
               WHERE trade_date=%s AND contract_version='R03_FORMAL_SECTOR_V1'
                 AND (%s::text IS NULL OR scope_key=%s)
                 AND (%s::text IS NULL OR member_version=%s)""",
            lambda: (
                target_session,
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            ),
        ),
        "stock.r03_sector_loo_support_facts": (
            """SELECT count(*) FROM stock.r03_sector_loo_support_facts
               WHERE trade_date=%s AND contract_version='R03_FORMAL_SECTOR_V1'
                 AND (%s::text IS NULL OR scope_key=%s)
                 AND (%s::text IS NULL OR member_version=%s)""",
            lambda: (
                target_session,
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"BAOSTOCK_CAP_{source_capture_id}:PROF_{profile_revision}:NON_PIT"
                    if source_capture_id is not None and profile_revision is not None
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
                (
                    f"IND_{sector_snapshot_ids.get('INDUSTRY', 0)}:CON_{sector_snapshot_ids.get('CONCEPT', 0)}"
                    if isinstance(sector_snapshot_ids, Mapping)
                    else None
                ),
            ),
        ),
        "stock.source_sector_cross_period_pulse_facts": (
            """SELECT count(*) FROM stock.source_sector_cross_period_pulse_facts p
               JOIN stock.tdx_sector_catalog c ON c.id=p.catalog_id
               WHERE p.target_session=%s AND p.source_capture_id=%s
                 AND p.profile_revision=%s AND p.algorithm_contract=%s
                 AND c.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                target_session,
                source_capture_id,
                profile_revision,
                "SOURCE_CROSS_PERIOD_PULSE_R1",
                snapshots,
            ),
        ),
        "stock.source_sector_pulse_adjacent_transitions": (
            """SELECT count(*)
               FROM stock.source_sector_pulse_adjacent_transitions t
               JOIN stock.source_sector_cross_period_pulse_facts previous
                 ON previous.id=t.previous_pulse_id
               JOIN stock.source_sector_cross_period_pulse_facts target
                 ON target.id=t.target_pulse_id
               JOIN stock.tdx_sector_catalog previous_catalog
                 ON previous_catalog.id=previous.catalog_id
               JOIN stock.tdx_sector_catalog target_catalog
                 ON target_catalog.id=target.catalog_id
               WHERE t.board_kind=previous.board_kind
                 AND t.board_kind=target.board_kind
                 AND t.board_native_id=previous.board_native_id
                 AND t.board_native_id=target.board_native_id
                 AND previous.target_session=%s AND target.target_session=%s
                 AND target.source_capture_id=%s
                 AND target.profile_revision=%s
                 AND t.transition_contract='SOURCE_CROSS_PERIOD_PULSE_TRANSITION_R1'
                 AND t.calendar_revision=%s
                 AND previous_catalog.snapshot_id=ANY(%s::bigint[])
                 AND target_catalog.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                (followup_target_sessions or (target_session,))[0],
                target_session,
                source_capture_id,
                profile_revision,
                calendar_revision,
                snapshots,
                snapshots,
            ),
        ),
        "stock.source_sector_pulse_followup_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_followup_facts f
               JOIN stock.source_sector_cross_period_pulse_facts p
                 ON p.id=f.pulse_fact_id
               WHERE p.target_session=%s AND p.source_capture_id=%s
                 AND p.profile_revision=%s
                 AND f.target_session=ANY(%s::date[])
                 AND f.calendar_revision=%s
                 AND f.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'""",
            lambda: (
                target_session,
                source_capture_id,
                profile_revision,
                list(followup_target_sessions or ()),
                calendar_revision,
            ),
        ),
        "stock.source_sector_pulse_cohort_group_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_cohort_group_facts g
               WHERE g.t0_session=%s AND g.research_cutoff=%s
                 AND g.t0_source_capture_id=%s AND g.t0_profile_revision=%s
                 AND g.pulse_algorithm_contract='SOURCE_CROSS_PERIOD_PULSE_R1'
                 AND g.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
                 AND g.calendar_revision=%s AND g.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                target_session,
                research_cutoff,
                source_capture_id,
                profile_revision,
                calendar_revision,
                snapshots,
            ),
        ),
        "stock.source_sector_pulse_cohort_reason_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_cohort_reason_facts r
               JOIN stock.source_sector_pulse_cohort_group_facts g
                 ON g.id=r.cohort_group_fact_id
               WHERE g.t0_session=%s AND g.research_cutoff=%s
                 AND g.t0_source_capture_id=%s AND g.t0_profile_revision=%s
                 AND g.pulse_algorithm_contract='SOURCE_CROSS_PERIOD_PULSE_R1'
                 AND g.followup_contract='SOURCE_SECTOR_PULSE_FOLLOWUP_CLOSE_R1'
                 AND g.calendar_revision=%s AND g.snapshot_id=ANY(%s::bigint[])""",
            lambda: (
                target_session,
                research_cutoff,
                source_capture_id,
                profile_revision,
                calendar_revision,
                snapshots,
            ),
        ),
    }
    selected = queries.get(business_table)
    if selected is None:
        raise DataJobPgError("DATA_JOB_TABLE_REJECTED", "业务表不在任务进度读回白名单")
    sql, parameters = selected
    capture_bound_tables = {
        "stock.source_captures",
        "stock.source_raw_daily",
        "stock.source_capture_requests",
        "stock.source_symbol_price_profiles",
        "stock.source_d0_scenario_day_facts",
        "stock.source_pool_seed_day_facts",
        "stock.source_d0_launch_day_facts",
        "stock.source_d1_structure_day_facts",
        "stock.source_d1_material_anchor_day_facts",
        "stock.source_research_enrollments",
        "stock.source_d0_formal_admission_day_facts",
        "stock.source_rule_evaluations",
        "stock.source_t0_early_observation_facts",
        "stock.source_market_breadth_days",
        "stock.source_market_breadth_symbols",
        "stock.source_market_breadth_change_days",
        "stock.source_market_participation_days",
        "stock.source_market_participation_unknown_reasons",
        "stock.source_sector_cross_period_pulse_facts",
        "stock.source_sector_pulse_adjacent_transitions",
        "stock.source_sector_pulse_followup_facts",
        "stock.source_sector_pulse_cohort_group_facts",
        "stock.source_sector_pulse_cohort_reason_facts",
    }
    if business_table in capture_bound_tables and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "读回事实缺少真实来源捕获数字键"
        )
    if business_table in {
        "stock.source_symbol_price_profiles",
        "stock.source_pool_seed_day_facts",
        "stock.source_rule_evaluations",
        "stock.source_t0_early_observation_facts",
    } and (profile_history_session_count is None or not profile_revision):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "画像读回缺少捕获、窗口或修订键"
        )
    if business_table == "stock.source_rule_evaluations" and (
        type(d1_history_session_count) is not int
        or d1_history_session_count <= 0
        or not calendar_revision
        or not producer_versions
        or len(producer_versions) != 1
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "冻结规则读回缺少版本、D1窗口或日历修订键"
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
    if business_table == "stock.source_t0_early_observation_facts" and (
        not profile_revision
        or not isinstance(sector_snapshot_ids, Mapping)
        or set(sector_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or any(
            type(value) is not int or value <= 0
            for value in sector_snapshot_ids.values()
        )
        or sector_snapshot_ids["INDUSTRY"] == sector_snapshot_ids["CONCEPT"]
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "T0观察读回缺少画像修订或互异双快照键"
        )
    if business_table == "stock.source_sector_pulse_adjacent_transitions" and (
        not profile_revision
        or not calendar_revision
        or not isinstance(sector_snapshot_ids, Mapping)
        or set(sector_snapshot_ids) != {"INDUSTRY", "CONCEPT"}
        or any(
            type(value) is not int or value <= 0
            for value in sector_snapshot_ids.values()
        )
        or not followup_target_sessions
        or len(followup_target_sessions) != 1
        or type(followup_target_sessions[0]) is not date
        or followup_target_sessions[0] >= target_session
        or (
            type(research_cutoff) is date
            and followup_target_sessions[0] > research_cutoff
        )
    ):
        sector_snapshot_identity = (
            {str(key): value for key, value in sector_snapshot_ids.items()}
            if isinstance(sector_snapshot_ids, Mapping)
            else None
        )
        prior_sessions = (
            tuple(
                item.isoformat() if isinstance(item, date) else repr(item)
                for item in followup_target_sessions
            )
            if followup_target_sessions
            else ()
        )
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED",
            "板块相邻转移读回键无效："
            f"profile_revision_present={bool(profile_revision)}，"
            f"calendar_revision_present={bool(calendar_revision)}，"
            f"snapshot_ids={sector_snapshot_identity}，"
            f"previous_sessions={prior_sessions}，"
            f"target={target_session.isoformat() if isinstance(target_session, date) else None}，"
            f"cutoff={research_cutoff.isoformat() if isinstance(research_cutoff, date) else None}。",
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
            "stock.source_market_breadth_change_days",
        }
        and not calendar_revision
    ):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "市场宽度表读回缺少日历修订键"
        )
    if business_table in {
        "stock.tdx_sector_source_snapshots",
        "stock.tdx_sector_catalog",
        "stock.tdx_sector_members",
    } and (not isinstance(sector_snapshot_ids, Mapping) or not snapshots):
        raise DataJobPgError(
            "DATA_JOB_READBACK_KEY_REQUIRED", "板块读回缺少行业/GN快照键"
        )
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(sql, parameters())
        return int(cursor.fetchone()[0])


def set_job_source_capture(
    config: Mapping[str, Any], *, job_id: UUID, source_capture_id: int
) -> None:
    """将运行任务绑定到一条真实BaoStock或TDX通用来源捕获。"""
    if (
        not isinstance(job_id, UUID)
        or type(source_capture_id) is not int
        or source_capture_id <= 0
    ):
        raise DataJobPgError(
            "DATA_JOB_SOURCE_CAPTURE_INVALID", "来源捕获键必须是正整数和有效任务UUID"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT source_capture_id,status FROM stock.data_jobs WHERE job_id=%s FOR UPDATE",
            (job_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise DataJobPgError("DATA_JOB_NOT_FOUND", "任务主记录不存在")
        if row[1] != "RUNNING":
            raise DataJobPgError(
                "DATA_JOB_STATE_CONFLICT", "只有运行任务可绑定来源捕获"
            )
        if row[0] is not None and int(row[0]) != source_capture_id:
            raise DataJobPgError(
                "DATA_JOB_SOURCE_CAPTURE_CONFLICT",
                "同一任务已绑定不同来源捕获；拒绝混用并要求显式新任务",
            )
        cursor.execute(
            "SELECT provider FROM stock.source_captures WHERE capture_id=%s",
            (source_capture_id,),
        )
        capture = cursor.fetchone()
        if capture is None or capture[0] not in {"BAOSTOCK", "TDX"}:
            raise DataJobPgError(
                "DATA_JOB_SOURCE_CAPTURE_NOT_FOUND", "来源捕获不存在或提供方身份无效"
            )
        cursor.execute(
            """UPDATE stock.data_jobs
               SET source_capture_id=%s,
                   updated_at=clock_timestamp()
               WHERE job_id=%s AND status='RUNNING'
                 AND (source_capture_id IS NULL OR source_capture_id=%s)""",
            (source_capture_id, job_id, source_capture_id),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError(
                "DATA_JOB_STATE_CONFLICT", "来源捕获无法绑定到运行任务"
            )


def read_reset_preflight_fingerprint(
    config: Mapping[str, Any], *, job_id: UUID
) -> str | None:
    if not isinstance(job_id, UUID):
        raise DataJobPgError("DATA_JOB_ID_INVALID", "重置任务ID无效")
    with source_raw._transaction(config, write=False) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT reset_preflight_fingerprint FROM stock.data_jobs WHERE job_id=%s",
            (job_id,),
        )
        row = cursor.fetchone()
    if row is None:
        raise DataJobPgError("DATA_JOB_NOT_FOUND", "重置任务主记录不存在")
    return str(row[0]) if row[0] is not None else None


def bind_reset_preflight_fingerprint(
    config: Mapping[str, Any], *, job_id: UUID, fingerprint: str
) -> None:
    if (
        not isinstance(job_id, UUID)
        or not isinstance(fingerprint, str)
        or len(fingerprint) != 64
        or any(char not in "0123456789abcdef" for char in fingerprint)
    ):
        raise DataJobPgError(
            "RESET_PREFLIGHT_FINGERPRINT_INVALID", "重置输入预检指纹必须为SHA-256"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_jobs
               SET reset_preflight_fingerprint=%s,updated_at=clock_timestamp()
               WHERE job_id=%s AND mode='INITIALIZE'
                 AND initialization_mode='RESET_ALL_BUSINESS_FACTS'
                 AND status='RUNNING' AND reset_preview_fingerprint IS NOT NULL
                 AND (reset_preflight_fingerprint IS NULL
                      OR reset_preflight_fingerprint=%s)""",
            (fingerprint, job_id, fingerprint),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError(
                "RESET_INPUT_IDENTITY_CONFLICT",
                "重置任务已绑定不同的来源输入，拒绝覆盖或混用。",
            )


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
    initialization_mode = job.get("initialization_mode", "REUSE_RAW")
    daily_mode = job.get("daily_mode", "CUSTOM")
    profile_history_session_count = job.get(
        "profile_history_session_count", job["history_session_count"]
    )
    reset_preview_fingerprint = job.get("reset_preview_fingerprint")
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
        or not isinstance(initialization_mode, str)
        or not initialization_mode.strip()
        or not isinstance(daily_mode, str)
        or not daily_mode.strip()
        or type(profile_history_session_count) is not int
        or not 1 <= profile_history_session_count <= 32767
        or (
            reset_preview_fingerprint is not None
            and (
                not isinstance(reset_preview_fingerprint, str)
                or len(reset_preview_fingerprint) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in reset_preview_fingerprint
                )
            )
        )
        or (
            initialization_mode == "RESET_ALL_BUSINESS_FACTS"
            and reset_preview_fingerprint is None
        )
    ):
        raise DataJobPgError("DATA_JOB_INPUT_INVALID", "模块、输出范围或来源摘要无效")
    # 迁移54的模块CHECK尚未登记BAOSTOCK_PRICES；仅将本次精确来源组合映射为旧别名，
    # 迁移55会在核验唯一来源初始化任务后于同一事务内改回正式模块名。
    identity = config.get("application_identity")
    migration_version = (
        identity.get("migration_version") if isinstance(identity, Mapping) else None
    )
    if (
        migration_version == 54
        and job.get("mode") == "INITIALIZE"
        and initialization_mode == "RESET_ALL_BUSINESS_FACTS"
        and daily_mode == "CUSTOM"
        and set(selected_modules) == {"BAOSTOCK_PRICES", "TDX_SECTOR_MEMBERS"}
        and len(selected_modules) == 2
    ):
        selected_modules = [
            "BAOSTOCK" if module == "BAOSTOCK_PRICES" else module
            for module in selected_modules
        ]
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
            if job["operation"] == "RECOMPUTE":
                cursor.execute(
                    """UPDATE stock.data_job_progress
                       SET status='NOT_STARTED',planned_units=1,completed_units=0,
                           expected_rows=NULL,verified_rows=NULL,inserted_rows=0,
                           updated_rows=0,deleted_rows=0,profile_revision=NULL,
                           producer_version=NULL,profile_history_session_count=NULL,
                           d1_history_session_count=NULL,calendar_revision=NULL,
                           industry_snapshot_id=NULL,concept_snapshot_id=NULL,
                           universe_version_t=NULL,universe_version_t1=NULL,
                           universe_version_t3=NULL,source_capture_id_t1=NULL,
                           source_capture_id_t3=NULL,profile_revision_t1=NULL,
                           profile_revision_t3=NULL,prev_scope_key=NULL,
                           prev_member_version=NULL,
                           quality_status=NULL,error_code=NULL,error_message=NULL,
                           verified_at=NULL,updated_at=clock_timestamp(),completed_at=NULL
                       WHERE job_id=%s
                         AND status NOT IN ('NOT_DUE','SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED')""",
                    (existing_id,),
                )
            else:
                cursor.execute(
                    """UPDATE stock.data_job_progress
                       SET status='NOT_STARTED',completed_units=0,expected_rows=NULL,
                           verified_rows=NULL,inserted_rows=0,updated_rows=0,deleted_rows=0,
                           profile_revision=NULL,producer_version=NULL,
                           profile_history_session_count=NULL,d1_history_session_count=NULL,
                           calendar_revision=NULL,industry_snapshot_id=NULL,concept_snapshot_id=NULL,
                           universe_version_t=NULL,universe_version_t1=NULL,
                           universe_version_t3=NULL,source_capture_id_t1=NULL,
                           source_capture_id_t3=NULL,profile_revision_t1=NULL,
                           profile_revision_t3=NULL,prev_scope_key=NULL,
                           prev_member_version=NULL,
                           quality_status=NULL,error_code=NULL,error_message=NULL,
                           verified_at=NULL,updated_at=clock_timestamp(),completed_at=NULL
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
               (job_id,request_key,mode,initialization_mode,daily_mode,
                profile_history_session_count,reset_preview_fingerprint,
                target_session,research_cutoff,
                history_session_count,d0_output_session_count,d1_output_session_count,
                operation,selected_modules,module_output_session_counts,
                source_version_fingerprint,calendar_id,calendar_revision,
                status,planned_units,completed_units,
                created_at,started_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING',%s,0,
                       clock_timestamp(),clock_timestamp(),clock_timestamp())""",
            (
                job_id,
                job["request_key"],
                job["mode"],
                initialization_mode,
                daily_mode,
                profile_history_session_count,
                reset_preview_fingerprint,
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
    source_capture_id: int | None = None,
    profile_revision: str | None = None,
    producer_version: str | None = None,
    calendar_revision: str | None = None,
    industry_snapshot_id: int | None = None,
    concept_snapshot_id: int | None = None,
    universe_version_t: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    prev_scope_key: str | None = None,
    prev_member_version: str | None = None,
    prev_trade_date: date | None = None,
    industry_snapshot_id_t1: int | None = None,
    concept_snapshot_id_t1: int | None = None,
    industry_snapshot_id_t3: int | None = None,
    concept_snapshot_id_t3: int | None = None,
    source_capture_id_t4: int | None = None,
    prev_session_t: date | None = None,
    prev_session_t3: date | None = None,
) -> bool:
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise DataJobPgError(
            "DATA_JOB_SOURCE_CAPTURE_INVALID", "开始进度的来源捕获ID无效"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status='RUNNING',attempt_count=attempt_count+1,
                   started_at=coalesce(started_at,clock_timestamp()),
                   updated_at=clock_timestamp(),error_code=NULL,error_message=NULL,
                   source_capture_id=coalesce(%s,source_capture_id),
                   profile_revision=coalesce(%s,profile_revision),
                   producer_version=coalesce(%s,producer_version),
                   calendar_revision=coalesce(%s,calendar_revision),
                   industry_snapshot_id=coalesce(%s,industry_snapshot_id),
                   concept_snapshot_id=coalesce(%s,concept_snapshot_id),
                   universe_version_t=coalesce(%s,universe_version_t),
                   universe_version_t1=coalesce(%s,universe_version_t1),
                   universe_version_t3=coalesce(%s,universe_version_t3),
                   source_capture_id_t1=coalesce(%s,source_capture_id_t1),
                   source_capture_id_t3=coalesce(%s,source_capture_id_t3),
                   profile_revision_t1=coalesce(%s,profile_revision_t1),
                   profile_revision_t3=coalesce(%s,profile_revision_t3),
                   prev_scope_key=coalesce(%s,prev_scope_key),
                   prev_member_version=coalesce(%s,prev_member_version),
                   prev_trade_date=coalesce(%s,prev_trade_date),
                   industry_snapshot_id_t1=coalesce(%s,industry_snapshot_id_t1),
                   concept_snapshot_id_t1=coalesce(%s,concept_snapshot_id_t1),
                   industry_snapshot_id_t3=coalesce(%s,industry_snapshot_id_t3),
                   concept_snapshot_id_t3=coalesce(%s,concept_snapshot_id_t3),
                   source_capture_id_t4=coalesce(%s,source_capture_id_t4),
                   prev_session_t=coalesce(%s,prev_session_t),
                   prev_session_t3=coalesce(%s,prev_session_t3),
                   deleted_rows=CASE WHEN coalesce(deleted_rows,0)>0 THEN deleted_rows ELSE 0 END
               WHERE job_id=%s AND stage_code=%s AND target_session=%s AND business_table=%s
                 AND status IN ('NOT_STARTED','FAILED','INTERRUPTED','INCOMPLETE','CLEARED')""",
            (
                source_capture_id,
                profile_revision,
                producer_version,
                calendar_revision,
                industry_snapshot_id,
                concept_snapshot_id,
                universe_version_t,
                universe_version_t1,
                universe_version_t3,
                source_capture_id_t1,
                source_capture_id_t3,
                profile_revision_t1,
                profile_revision_t3,
                prev_scope_key,
                prev_member_version,
                prev_trade_date,
                industry_snapshot_id_t1,
                concept_snapshot_id_t1,
                industry_snapshot_id_t3,
                concept_snapshot_id_t3,
                source_capture_id_t4,
                prev_session_t,
                prev_session_t3,
                job_id,
                stage_code,
                target_session,
                business_table,
            ),
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
    """在一个状态事务中标记本阶段全部实际执行表为RUNNING并保留精确分区键。"""
    if not units:
        return
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        for unit in units:
            capture_id = unit.get("source_capture_id")
            if capture_id is not None and (
                type(capture_id) is not int or capture_id <= 0
            ):
                raise DataJobPgError(
                    "DATA_JOB_SOURCE_CAPTURE_INVALID", "批量开始进度含无效来源捕获ID"
                )
            key_cols = (
                "source_capture_id",
                "profile_revision",
                "producer_version",
                "calendar_revision",
                "industry_snapshot_id",
                "concept_snapshot_id",
                "universe_version_t",
                "universe_version_t1",
                "universe_version_t3",
                "source_capture_id_t1",
                "source_capture_id_t3",
                "profile_revision_t1",
                "profile_revision_t3",
                "prev_scope_key",
                "prev_member_version",
                "prev_trade_date",
                "industry_snapshot_id_t1",
                "concept_snapshot_id_t1",
                "industry_snapshot_id_t3",
                "concept_snapshot_id_t3",
                "source_capture_id_t4",
                "prev_session_t",
                "prev_session_t3",
            )
            overwrite_nulls = bool(unit.get("overwrite_nulls", False))
            set_exprs = []
            key_vals: list[Any] = []
            for col in key_cols:
                val = capture_id if col == "source_capture_id" else unit.get(col)
                if overwrite_nulls or col in unit:
                    set_exprs.append(f"{col}=%s")
                else:
                    set_exprs.append(f"{col}=coalesce(%s,{col})")
                key_vals.append(val)
            cursor.execute(
                f"""UPDATE stock.data_job_progress
                   SET status='RUNNING',attempt_count=attempt_count+1,
                       started_at=coalesce(started_at,clock_timestamp()),
                       updated_at=clock_timestamp(),error_code=NULL,error_message=NULL,
                       {", ".join(set_exprs)},
                       deleted_rows=CASE WHEN coalesce(deleted_rows,0)>0 THEN deleted_rows ELSE 0 END
                   WHERE job_id=%s AND stage_code=%s AND target_session=%s
                     AND business_table=%s
                     AND status IN ('NOT_STARTED','FAILED','INTERRUPTED','INCOMPLETE','CLEARED')""",
                (
                    *key_vals,
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
    source_capture_id: int | None = None,
    profile_revision: str | None = None,
    producer_version: str | None = None,
    profile_history_session_count: int | None = None,
    d1_history_session_count: int | None = None,
    calendar_revision: str | None = None,
    industry_snapshot_id: int | None = None,
    concept_snapshot_id: int | None = None,
    universe_version_t: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    prev_scope_key: str | None = None,
    prev_member_version: str | None = None,
    prev_trade_date: date | None = None,
    industry_snapshot_id_t1: int | None = None,
    concept_snapshot_id_t1: int | None = None,
    industry_snapshot_id_t3: int | None = None,
    concept_snapshot_id_t3: int | None = None,
    source_capture_id_t4: int | None = None,
    prev_session_t: date | None = None,
    prev_session_t3: date | None = None,
    no_change: bool = False,
    warning_code: str | None = None,
    warning_message: str | None = None,
) -> None:
    """独立读回后，仅按真实来源捕获数字键登记提交进度。"""
    counts = (verified_rows, inserted_rows, updated_rows)
    if any(type(value) is not int or value < 0 for value in counts):
        raise DataJobPgError("DATA_JOB_ROW_COUNT_INVALID", "读回行数必须为非负整数")
    if expected_rows is not None and (
        type(expected_rows) is not int or expected_rows < 0
    ):
        raise DataJobPgError(
            "DATA_JOB_ROW_COUNT_INVALID", "预期行数必须为非负整数或未知"
        )
    if (warning_code is None) != (warning_message is None) or (
        warning_code is not None
        and (
            not isinstance(warning_code, str)
            or not warning_code.strip()
            or not isinstance(warning_message, str)
            or not warning_message.strip()
        )
    ):
        raise DataJobPgError(
            "DATA_JOB_WARNING_INVALID", "完成阶段的告警代码与说明必须同时有效"
        )
    for field_name, value in (
        ("source_capture_id", source_capture_id),
        ("industry_snapshot_id", industry_snapshot_id),
        ("concept_snapshot_id", concept_snapshot_id),
    ):
        if value is not None and (type(value) is not int or value <= 0):
            raise DataJobPgError(
                "DATA_JOB_OUTPUT_IDENTITY_INVALID",
                f"阶段来源身份字段{field_name}必须为正整数或空值",
            )
    for field_name, value in (
        ("profile_history_session_count", profile_history_session_count),
        ("d1_history_session_count", d1_history_session_count),
    ):
        if value is not None and (type(value) is not int or value < 1):
            raise DataJobPgError(
                "DATA_JOB_OUTPUT_IDENTITY_INVALID",
                f"阶段窗口字段{field_name}必须为正整数或空值",
            )
    for field_name, value in (
        ("profile_revision", profile_revision),
        ("producer_version", producer_version),
        ("calendar_revision", calendar_revision),
    ):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise DataJobPgError(
                "DATA_JOB_OUTPUT_IDENTITY_INVALID",
                f"阶段版本字段{field_name}必须为非空文本或空值",
            )
    progress_fact_tables = set().union(*_MODULE_PROGRESS_TABLES.values())
    if (
        business_table in progress_fact_tables
        and business_table not in _SECTOR_SNAPSHOT_PROGRESS_TABLES
        and (type(source_capture_id) is not int or source_capture_id <= 0)
    ):
        raise DataJobPgError(
            "DATA_JOB_OUTPUT_IDENTITY_INVALID",
            "来源及派生结果完成状态必须绑定真实source_capture_id",
        )
    if business_table in _SECTOR_SNAPSHOT_PROGRESS_TABLES and (
        industry_snapshot_id is None or concept_snapshot_id is None
    ):
        raise DataJobPgError(
            "DATA_JOB_OUTPUT_IDENTITY_INVALID",
            "行业/GN概念成员源状态必须绑定同次双快照ID",
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
                   source_capture_id=%s,
                   profile_revision=%s,producer_version=%s,
                   profile_history_session_count=%s,d1_history_session_count=%s,
                   calendar_revision=%s,industry_snapshot_id=%s,concept_snapshot_id=%s,
                   universe_version_t=%s,universe_version_t1=%s,universe_version_t3=%s,
                   source_capture_id_t1=%s,source_capture_id_t3=%s,
                   profile_revision_t1=%s,profile_revision_t3=%s,
                   prev_scope_key=%s,prev_member_version=%s,
                   prev_trade_date=%s,
                   industry_snapshot_id_t1=%s,concept_snapshot_id_t1=%s,
                   industry_snapshot_id_t3=%s,concept_snapshot_id_t3=%s,
                   source_capture_id_t4=%s,prev_session_t=%s,prev_session_t3=%s,
                   quality_status=%s,error_code=%s,error_message=%s,
                   verified_at=clock_timestamp(),
                   completed_at=clock_timestamp(),updated_at=clock_timestamp()
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s AND status='RUNNING'""",
            (
                status,
                expected_rows,
                verified_rows,
                inserted_rows,
                updated_rows,
                source_capture_id,
                profile_revision,
                producer_version,
                profile_history_session_count,
                d1_history_session_count,
                calendar_revision,
                industry_snapshot_id,
                concept_snapshot_id,
                universe_version_t,
                universe_version_t1,
                universe_version_t3,
                source_capture_id_t1,
                source_capture_id_t3,
                profile_revision_t1,
                profile_revision_t3,
                prev_scope_key,
                prev_member_version,
                prev_trade_date,
                industry_snapshot_id_t1,
                concept_snapshot_id_t1,
                industry_snapshot_id_t3,
                concept_snapshot_id_t3,
                source_capture_id_t4,
                prev_session_t,
                prev_session_t3,
                quality_status,
                warning_code,
                warning_message,
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
                   source_capture_id=CASE
                       WHEN %s=ANY(ARRAY[
                           'stock.source_baostock_stock_basic'::text,
                           'stock.source_baostock_session_directory'::text,
                           'stock.source_market_index_daily'::text
                       ]) THEN j.source_capture_id
                       ELSE coalesce(j.source_capture_id,%s)
                   END,

                   updated_at=clock_timestamp()
               WHERE j.job_id=%s AND j.status='RUNNING'""",
            (business_table, source_capture_id, job_id),
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
    version_keys: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    """读回派生表目标日期（或精确版本）为零行后，记录清理任务的真实删除数。"""
    cleanup_tables = {
        table for spec in _CLEANUP_MODULES.values() for table in spec["progress_tables"]
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
        "stock.source_symbol_price_profiles": (
            "SELECT count(*) FROM stock.source_symbol_price_profiles WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_d0_formal_admission_day_facts": (
            "SELECT count(*) FROM stock.source_d0_formal_admission_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_d0_scenario_day_facts": (
            "SELECT count(*) FROM stock.source_d0_scenario_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_pool_seed_day_facts": (
            "SELECT count(*) FROM stock.source_pool_seed_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_d0_launch_day_facts": (
            "SELECT count(*) FROM stock.source_d0_launch_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_research_enrollments": (
            """SELECT count(DISTINCT e.id)
               FROM stock.source_research_enrollments e
               JOIN stock.source_d0_launch_day_facts d ON d.id=e.d0_fact_id
               WHERE d.target_session=%s AND d.target_session<=%s""",
        ),
        "stock.r03_sector_native_day_facts": (
            """SELECT count(*) FROM stock.r03_sector_native_day_facts
               WHERE trade_date=%s AND trade_date<=%s AND contract_version='R03_FORMAL_SECTOR_V1'""",
        ),
        "stock.r03_sector_rotation_board_day_facts": (
            """SELECT count(*) FROM stock.r03_sector_rotation_board_day_facts
               WHERE trade_date=%s AND trade_date<=%s AND contract_version='R03_FORMAL_SECTOR_V1'""",
        ),
        "stock.r03_sector_rotation_day_states": (
            """SELECT count(*) FROM stock.r03_sector_rotation_day_states s
               JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
               WHERE s.trade_date=%s AND s.trade_date<=%s AND e.contract_version='R03_FORMAL_SECTOR_V1'""",
        ),
        "stock.r03_sector_overlap_clusters": (
            """SELECT count(*) FROM stock.r03_sector_overlap_clusters
               WHERE trade_date=%s AND trade_date<=%s AND contract_version='R03_FORMAL_SECTOR_V1'""",
        ),
        "stock.r03_sector_loo_support_facts": (
            """SELECT count(*) FROM stock.r03_sector_loo_support_facts
               WHERE trade_date=%s AND trade_date<=%s AND contract_version='R03_FORMAL_SECTOR_V1'""",
        ),
        "stock.source_sector_cross_period_pulse_facts": (
            "SELECT count(*) FROM stock.source_sector_cross_period_pulse_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_sector_pulse_adjacent_transitions": (
            """SELECT count(*) FROM stock.source_sector_pulse_adjacent_transitions t
               JOIN stock.source_sector_cross_period_pulse_facts p
                 ON p.id=t.target_pulse_id
               WHERE p.target_session=%s AND p.target_session<=%s""",
        ),
        "stock.source_sector_pulse_followup_facts": (
            "SELECT count(*) FROM stock.source_sector_pulse_followup_facts WHERE t0_session=%s AND t0_session<=%s",
        ),
        "stock.source_sector_pulse_cohort_group_facts": (
            "SELECT count(*) FROM stock.source_sector_pulse_cohort_group_facts WHERE t0_session=%s AND t0_session<=%s",
        ),
        "stock.source_sector_pulse_cohort_reason_facts": (
            """SELECT count(*) FROM stock.source_sector_pulse_cohort_reason_facts r
               JOIN stock.source_sector_pulse_cohort_group_facts g
                 ON g.id=r.cohort_group_fact_id
               WHERE g.t0_session=%s AND g.t0_session<=%s""",
        ),
        "stock.source_price_limit_day_facts": (
            "SELECT count(*) FROM stock.source_price_limit_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_limit_up_streak_day_facts": (
            "SELECT count(*) FROM stock.source_limit_up_streak_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_baostock_stock_basic": (
            "SELECT count(*) FROM stock.source_baostock_stock_basic WHERE %s<=%s",
        ),
        "stock.source_baostock_session_directory": (
            "SELECT count(*) FROM stock.source_baostock_session_directory WHERE session_date=%s AND session_date<=%s",
        ),
        "stock.source_market_index_daily": (
            "SELECT count(*) FROM stock.source_market_index_daily WHERE session_date=%s AND session_date<=%s",
        ),
        "stock.source_cross_section_day_facts": (
            "SELECT count(*) FROM stock.source_cross_section_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_cross_section_endpoints": (
            "SELECT count(*) FROM stock.source_cross_section_endpoints WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_market_index_day_facts": (
            "SELECT count(*) FROM stock.source_market_index_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_market_environment_day_facts": (
            "SELECT count(*) FROM stock.source_market_environment_day_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_forward_path_facts": (
            "SELECT count(*) FROM stock.source_forward_path_facts WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_cohort_group_stats": (
            "SELECT count(*) FROM stock.source_cohort_group_stats WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_fep_baseline_facts": (
            "SELECT count(*) FROM stock.source_fep_baseline_facts WHERE research_cutoff=%s AND research_cutoff<=%s",
        ),
        "stock.source_research_observation_segment_days": (
            "SELECT count(*) FROM stock.source_research_observation_segment_days WHERE t0_session=%s AND t0_session<=%s",
        ),
        "stock.source_market_breadth_days": (
            "SELECT count(*) FROM stock.source_market_breadth_days WHERE target_session=%s AND target_session<=%s",
        ),
        "stock.source_market_breadth_change_days": (
            "SELECT count(*) FROM stock.source_market_breadth_change_days WHERE target_session=%s AND target_session<=%s",
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
        "stock.source_rule_evaluations": (
            "SELECT count(*) FROM stock.source_rule_evaluations WHERE target_session=%s AND target_session<=%s",
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
        "stock.r03_formal_market_breadth_facts": (
            "SELECT count(*) FROM stock.r03_formal_market_breadth_facts WHERE trade_date=%s AND trade_date<=%s",
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
        r03_version_filtered = {
            "stock.r03_sector_native_day_facts",
            "stock.r03_sector_rotation_board_day_facts",
            "stock.r03_sector_overlap_clusters",
            "stock.r03_sector_loo_support_facts",
        }
        if business_table in r03_version_filtered and version_keys:
            rem_count = 0
            for vk in version_keys:
                cursor.execute(
                    f"""SELECT count(*) FROM {business_table}
                        WHERE trade_date=%s AND trade_date<=%s
                          AND scope_key=%s AND member_version=%s AND contract_version=%s""",
                    (
                        target_session,
                        job[1],
                        vk["scope_key"],
                        vk["member_version"],
                        vk["contract_version"],
                    ),
                )
                rem_count += int(cursor.fetchone()[0])
        elif business_table == "stock.r03_sector_rotation_day_states" and version_keys:
            rem_count = 0
            for vk in version_keys:
                cursor.execute(
                    """SELECT count(*) FROM stock.r03_sector_rotation_day_states s
                       JOIN stock.r03_sector_rotation_episodes e ON e.id=s.episode_id
                       WHERE s.trade_date=%s AND s.trade_date<=%s
                         AND e.scope_key=%s AND e.member_version=%s AND e.contract_version=%s""",
                    (
                        target_session,
                        job[1],
                        vk["scope_key"],
                        vk["member_version"],
                        vk["contract_version"],
                    ),
                )
                rem_count += int(cursor.fetchone()[0])
        elif business_table == "stock.r03_formal_market_breadth_facts" and version_keys:
            rem_count = 0
            for vk in version_keys:
                f_keys = vk.get("formal_breadth_keys")
                if f_keys:
                    for fk in f_keys:
                        cursor.execute(
                            """SELECT count(*) FROM stock.r03_formal_market_breadth_facts
                               WHERE trade_date=%s AND trade_date<=%s
                                 AND scope_key=%s AND universe_version_t=%s
                                 AND universe_version_t3=%s AND contract_version=%s""",
                            (
                                target_session,
                                job[1],
                                fk["scope_key"],
                                fk["universe_version_t"],
                                fk["universe_version_t3"],
                                fk["contract_version"],
                            ),
                        )
                        rem_count += int(cursor.fetchone()[0])
                elif "universe_version_t" in vk and "universe_version_t3" in vk:
                    cursor.execute(
                        """SELECT count(*) FROM stock.r03_formal_market_breadth_facts
                           WHERE trade_date=%s AND trade_date<=%s
                             AND scope_key=%s AND universe_version_t=%s
                             AND universe_version_t3=%s AND contract_version=%s""",
                        (
                            target_session,
                            job[1],
                            vk["scope_key"],
                            vk["universe_version_t"],
                            vk["universe_version_t3"],
                            vk["contract_version"],
                        ),
                    )
                    rem_count += int(cursor.fetchone()[0])
                else:
                    cursor.execute(
                        count_queries[business_table][0], (target_session, job[1])
                    )
                    rem_count += int(cursor.fetchone()[0])
        else:
            cursor.execute(count_queries[business_table][0], (target_session, job[1]))
            rem_count = int(cursor.fetchone()[0])
        if rem_count != 0:
            raise DataJobPgError(
                "DATA_JOB_CLEANUP_READBACK_NOT_EMPTY",
                "派生表仍有目标日结果；清理进度不能标记完成",
            )
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,
                   planned_units=1,completed_units=1,
                   expected_rows=0,verified_rows=0,inserted_rows=0,updated_rows=0,
                   source_capture_id=NULL,
                   profile_revision=NULL,producer_version=NULL,
                   profile_history_session_count=NULL,d1_history_session_count=NULL,
                   calendar_revision=NULL,industry_snapshot_id=NULL,concept_snapshot_id=NULL,
                   universe_version_t=NULL,universe_version_t1=NULL,
                   universe_version_t3=NULL,source_capture_id_t1=NULL,
                   source_capture_id_t3=NULL,profile_revision_t1=NULL,
                   profile_revision_t3=NULL,prev_scope_key=NULL,
                   prev_member_version=NULL,
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


def mark_business_reset_progress(
    config: Mapping[str, Any],
    *,
    job_id: UUID,
    target_session: date,
    business_table: str,
    deleted_rows: int,
) -> dict[str, Any]:
    """核验全业务重置后的空表，并记录保留的当前任务状态。"""
    if (
        not isinstance(job_id, UUID)
        or type(target_session) is not date
        or not isinstance(business_table, str)
        or not business_table.startswith("stock.")
        or not all(
            character.islower() or character.isdigit() or character in "_."
            for character in business_table
        )
        or type(deleted_rows) is not int
        or deleted_rows < 0
        or not (
            business_table.startswith("stock.source_")
            or business_table.startswith("stock.tdx_")
            or business_table in {"stock.data_jobs", "stock.data_job_progress"}
        )
    ):
        raise DataJobPgError(
            "DATA_JOB_RESET_PROGRESS_INVALID", "整库重置进度表名、任务键或行数无效"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            "SELECT status,research_cutoff FROM stock.data_jobs WHERE job_id=%s FOR UPDATE",
            (job_id,),
        )
        job = cursor.fetchone()
        if job is None or job[0] != "RUNNING" or target_session > job[1]:
            raise DataJobPgError(
                "DATA_JOB_RESET_PROGRESS_SCOPE_INVALID",
                "整库重置进度任务未运行或目标日期超过研究截止",
            )
        cursor.execute("SELECT to_regclass(%s)::text", (business_table,))
        if cursor.fetchone()[0] is None:
            raise DataJobPgError(
                "DATA_JOB_RESET_TABLE_MISSING",
                f"整库重置目标表不存在：{business_table}",
            )
        if business_table == "stock.data_jobs":
            cursor.execute(
                """SELECT count(*),count(*) FILTER (WHERE job_id=%s)
                   FROM stock.data_jobs""",
                (job_id,),
            )
            total_rows, current_rows = (int(value) for value in cursor.fetchone())
            expected_rows = 1
            if total_rows != 1 or current_rows != 1:
                raise DataJobPgError(
                    "DATA_JOB_RESET_READBACK_NOT_CLOSED",
                    "整库重置后data_jobs必须只保留当前重置任务一行",
                )
        elif business_table == "stock.data_job_progress":
            cursor.execute(
                """SELECT count(*),count(*) FILTER (WHERE job_id=%s)
                   FROM stock.data_job_progress""",
                (job_id,),
            )
            total_rows, current_rows = (int(value) for value in cursor.fetchone())
            expected_rows = current_rows
            if total_rows != current_rows or current_rows < 1:
                raise DataJobPgError(
                    "DATA_JOB_RESET_READBACK_NOT_CLOSED",
                    "整库重置后data_job_progress只能保留当前任务的阶段记录",
                )
        else:
            cursor.execute(f"SELECT count(*) FROM {business_table}")
            expected_rows = int(cursor.fetchone()[0])
            if expected_rows != 0:
                raise DataJobPgError(
                    "DATA_JOB_RESET_READBACK_NOT_EMPTY",
                    f"整库重置后业务表仍有{expected_rows}行：{business_table}",
                )
        cursor.execute(
            """SELECT status,deleted_rows,expected_rows,verified_rows
               FROM stock.data_job_progress
               WHERE job_id=%s AND stage_code='BUSINESS_RESET'
                 AND target_session=%s AND business_table=%s FOR UPDATE""",
            (job_id, target_session, business_table),
        )
        progress = cursor.fetchone()
        if progress is None:
            raise DataJobPgError(
                "DATA_JOB_RESET_PROGRESS_CONFLICT",
                "整库重置阶段/表进度记录不存在",
            )
        if progress[0] == "CLEARED":
            if progress != ("CLEARED", deleted_rows, expected_rows, expected_rows):
                raise DataJobPgError(
                    "DATA_JOB_RESET_PROGRESS_CONFLICT",
                    "已清理进度与独立PG读回或提交删除数不一致",
                )
        elif progress[0] in {"NOT_STARTED", "RUNNING"}:
            cursor.execute(
                """UPDATE stock.data_job_progress
                   SET status='CLEARED',planned_units=1,completed_units=1,
                       expected_rows=%s,verified_rows=%s,inserted_rows=0,updated_rows=0,
                       deleted_rows=%s,source_capture_id=NULL,
                       profile_revision=NULL,producer_version=NULL,
                       profile_history_session_count=NULL,d1_history_session_count=NULL,
                       calendar_revision=NULL,industry_snapshot_id=NULL,concept_snapshot_id=NULL,
                       quality_status='CLEARED',error_code=NULL,error_message=NULL,
                       verified_at=clock_timestamp(),completed_at=clock_timestamp(),
                       updated_at=clock_timestamp()
                   WHERE job_id=%s AND stage_code='BUSINESS_RESET'
                     AND target_session=%s AND business_table=%s
                     AND status IN ('NOT_STARTED','RUNNING')""",
                (
                    expected_rows,
                    expected_rows,
                    deleted_rows,
                    job_id,
                    target_session,
                    business_table,
                ),
            )
            if cursor.rowcount != 1:
                raise DataJobPgError(
                    "DATA_JOB_RESET_PROGRESS_CONFLICT",
                    "整库重置阶段/表进度记录与并发更新冲突",
                )
        else:
            raise DataJobPgError(
                "DATA_JOB_RESET_PROGRESS_CONFLICT",
                "整库重置阶段/表进度记录已处于不可覆盖状态",
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
    return {
        "business_table": business_table,
        "deleted_rows": deleted_rows,
        "expected_rows": expected_rows,
        "verified_rows": expected_rows,
        "status": "CLEARED",
    }


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
    source_capture_id: int | None = None,
) -> None:
    if status not in {"FAILED", "INTERRUPTED", "BLOCKED", "NOT_COMPUTED", "INCOMPLETE"}:
        raise DataJobPgError("DATA_JOB_STATUS_INVALID", "任务进度错误状态不在允许范围")
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise DataJobPgError(
            "DATA_JOB_SOURCE_CAPTURE_INVALID", "失败进度的来源捕获ID无效"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,error_code=%s,error_message=%s,

                   source_capture_id=coalesce(%s,source_capture_id),
                   profile_revision=NULL,producer_version=NULL,
                   profile_history_session_count=NULL,d1_history_session_count=NULL,
                   calendar_revision=NULL,industry_snapshot_id=NULL,concept_snapshot_id=NULL,
                   updated_at=clock_timestamp(),completed_at=NULL
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s""",
            (
                status,
                error_code,
                error_message,
                source_capture_id,
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
    source_capture_id: int | None = None,
    profile_revision: str | None = None,
    producer_version: str | None = None,
    industry_snapshot_id: int | None = None,
    concept_snapshot_id: int | None = None,
    universe_version_t: str | None = None,
    universe_version_t1: str | None = None,
    universe_version_t3: str | None = None,
    source_capture_id_t1: int | None = None,
    source_capture_id_t3: int | None = None,
    profile_revision_t1: str | None = None,
    profile_revision_t3: str | None = None,
    prev_scope_key: str | None = None,
    prev_member_version: str | None = None,
) -> None:
    """将有局部输入缺口的表单元留为未计算，保留精确版本绑定以防被其他版本完成记录遮挡。"""
    if status not in {"NOT_COMPUTED", "NOT_DUE", "BLOCKED", "INCOMPLETE"}:
        raise DataJobPgError("DATA_JOB_STATUS_INVALID", "局部未计算状态不在允许范围")
    if source_capture_id is not None and (
        type(source_capture_id) is not int or source_capture_id <= 0
    ):
        raise DataJobPgError(
            "DATA_JOB_SOURCE_CAPTURE_INVALID", "未计算进度的来源捕获ID无效"
        )
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """UPDATE stock.data_job_progress
               SET status=%s,error_code=%s,error_message=%s,
                   source_capture_id=coalesce(%s,source_capture_id),
                   profile_revision=coalesce(%s,profile_revision),
                   producer_version=coalesce(%s,producer_version),
                   profile_history_session_count=NULL,d1_history_session_count=NULL,
                   calendar_revision=NULL,
                   industry_snapshot_id=coalesce(%s,industry_snapshot_id),
                   concept_snapshot_id=coalesce(%s,concept_snapshot_id),
                   universe_version_t=coalesce(%s,universe_version_t),
                   universe_version_t1=coalesce(%s,universe_version_t1),
                   universe_version_t3=coalesce(%s,universe_version_t3),
                   source_capture_id_t1=coalesce(%s,source_capture_id_t1),
                   source_capture_id_t3=coalesce(%s,source_capture_id_t3),
                   profile_revision_t1=coalesce(%s,profile_revision_t1),
                   profile_revision_t3=coalesce(%s,profile_revision_t3),
                   prev_scope_key=coalesce(%s,prev_scope_key),
                   prev_member_version=coalesce(%s,prev_member_version),
                   updated_at=clock_timestamp(),completed_at=NULL
               WHERE job_id=%s AND stage_code=%s AND target_session=%s
                 AND business_table=%s AND status IN ('NOT_STARTED','RUNNING')""",
            (
                status,
                error_code,
                error_message,
                source_capture_id,
                profile_revision,
                producer_version,
                industry_snapshot_id,
                concept_snapshot_id,
                universe_version_t,
                universe_version_t1,
                universe_version_t3,
                source_capture_id_t1,
                source_capture_id_t3,
                profile_revision_t1,
                profile_revision_t3,
                prev_scope_key,
                prev_member_version,
                job_id,
                stage_code,
                target_session,
                business_table,
            ),
        )
        if cursor.rowcount != 1:
            raise DataJobPgError(
                "DATA_JOB_PROGRESS_MISSING",
                "局部进度记录键未命中："
                f"{stage_code}/{target_session.isoformat()}/{business_table}"
                f"，任务{job_id}",
            )


def finish_job(config: Mapping[str, Any], *, job_id: UUID) -> dict[str, Any]:
    with source_raw._transaction(config, write=True) as (_connection, cursor, _scope):
        cursor.execute(
            """SELECT count(*),count(*) FILTER (
                    WHERE status IN ('COMPLETED','ALREADY_COMPLETE','NO_CHANGE','CLEARED',
                                     'SKIPPED_BY_USER','SKIPPED_NOT_IMPLEMENTED','NOT_DUE')),
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

                   profile_revision=NULL,producer_version=NULL,
                   profile_history_session_count=NULL,d1_history_session_count=NULL,
                   calendar_revision=NULL,industry_snapshot_id=NULL,concept_snapshot_id=NULL,
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
