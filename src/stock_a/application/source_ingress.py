"""SOURCE_INGRESS范围内的真实P00计划与P02零请求能力核算。"""

from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from stock_a import __version__
from stock_a.application.planning import (
    CapabilityEvidence,
    DayPlanRequest,
    ScopeRevisionClosureContract,
    assemble_day_plan,
)
from stock_a.application.source_set_candidate import (
    _official_calendar,
    prepare_source_set_candidate,
)
from stock_a.contracts import LoadedDayChainContract, load_day_chain_contract
from stock_a.storage.postgres.identity import inspect_postgres
from stock_a.storage.postgres import source_raw
from stock_a.storage.postgres.source_set_ingress import inspect_p04_source_input


class SourceIngressError(ValueError):
    """来源准入计划或可选补充源能力合同拒绝。"""


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    return sha256(
        json.dumps(
            dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _calendar_sessions(target_session: date, expected_sha256: str) -> tuple[date, ...]:
    calendar = _official_calendar(target_session)
    if calendar["calendar_sha256"] != expected_sha256:
        raise SourceIngressError("官方日历摘要在计划读取期间发生变化")
    sessions = calendar["sessions"]
    if tuple(sorted(set(sessions))) != sessions or target_session not in sessions:
        raise SourceIngressError("目标会话不在唯一版本化沪深官方日历中")
    return sessions


def _scope_revision_contract(
    contract: LoadedDayChainContract, scope_id: str
) -> ScopeRevisionClosureContract:
    policy = contract["revision_policy"]["semantic_revision_closure"]
    catalog_document = policy["scope_contracts"]
    catalog = {
        name: (
            item["contract_id"],
            item["contract_version"],
            tuple(item["required_semantic_revision_keys"]),
            item["binding_digest"],
        )
        for name, item in catalog_document.items()
    }
    selected = catalog.get(scope_id)
    if selected is None:
        raise SourceIngressError("所选scope不在正式语义revision闭包目录中")
    return ScopeRevisionClosureContract(
        scope_id=scope_id,
        contract_id=selected[0],
        contract_version=selected[1],
        required_keys=selected[2],
        binding_digest=selected[3],
        scope_catalog=catalog,
        catalog_digest=policy["scope_catalog_digest"],
    )


def _accepted_source_state(config: Mapping[str, Any]) -> tuple[bool | None, str]:
    """仅在正式表不存在或都为空时声明首次观察；候选表不是接受前态。"""
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        _namespace,
    ):
        cursor.execute(
            """SELECT to_regclass('stock.source_sets')::text,
                      to_regclass('stock.source_set_capabilities')::text,
                      to_regclass('stock.source_set_candidates')::text"""
        )
        source_sets, source_capabilities, candidate_table = cursor.fetchone()
        if source_sets is None and source_capabilities is None:
            return (
                True,
                "stock.source_sets/source_set_capabilities不存在；candidate表不计为接受前态",
            )
        if source_sets is None or source_capabilities is None:
            return (
                None,
                "正式source_set主表与能力表未成对存在；拒绝把不完整关系当首次或接受前态",
            )
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM stock.source_sets LIMIT 1), "
            "EXISTS (SELECT 1 FROM stock.source_set_capabilities LIMIT 1)"
        )
        source_set_has_rows, capability_has_rows = cursor.fetchone()
        if not source_set_has_rows and not capability_has_rows:
            return (
                True,
                "正式source_set与能力表都为空；candidate表不计为接受前态",
            )
    return (
        None,
        "正式来源集合已有记录但accepted-state精确读回schema尚未绑定；拒绝猜测首次运行或APPEND",
    )


def _path_key(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return str(Path(value).resolve(strict=True)).casefold()
    except (OSError, RuntimeError, ValueError):
        return None


def _phase0_evidence(
    phase0_result: Mapping[str, Any] | None,
    *,
    config: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> tuple[CapabilityEvidence, dict[str, Any]]:
    """从真实读取证据投影SOURCE_INGRESS能力，不重写Phase0总体结论。"""
    p04_pending = [
        "SECURITY_ENTITY_AND_LIFECYCLE",
        "OFFICIAL_TRADING_CALENDAR_AND_STATUS",
    ]
    source_policy = config.get("source_policy", {})
    roots = (
        source_policy.get("configured_tdx_source_roots")
        if isinstance(source_policy, Mapping)
        else None
    )
    exact_pg_source = (
        isinstance(candidate.get("source_content_id"), str)
        and isinstance(candidate.get("source_capture_id"), str)
        and candidate.get("raw_row_count")
        == candidate.get("capture_expected_row_count")
        and candidate.get("read_failed_count") == 0
        and candidate.get("raw_source_digest")
        == candidate.get("capture_canonical_digest")
    )
    if not isinstance(phase0_result, Mapping):
        available = (
            exact_pg_source
            and source_policy.get("all_source_roots_read_only") is True
            and isinstance(roots, list)
            and len(roots) == 1
        )
        return (
            CapabilityEvidence(
                "READY" if available else "UNKNOWN",
                "source-ingress-pg-facts:" + str(candidate.get("candidate_sha256"))
                if available
                else None,
                {
                    "scope": "SOURCE_INGRESS",
                    "evidence_type": "EXACT_PG_CONTENT_CAPTURE_RAW",
                    "full_phase0_status": "NOT_SCANNED_BY_READ_ONLY_API",
                    "p04_pending": p04_pending,
                },
            ),
            {
                "status": "NOT_RUN",
                "scope_status": "SOURCE_INGRESS_READY" if available else "UNKNOWN",
                "scope": "SOURCE_INGRESS",
                "digest": None,
                "p04_pending": p04_pending,
                "full_phase0_rewritten": False,
            },
        )

    status = phase0_result.get("status")
    if status not in {"FULL_PASS", "DEGRADED_PASS", "BLOCKED"}:
        raise SourceIngressError("阶段0输入必须是真实来源检查结果")
    digest = sha256(
        json.dumps(
            phase0_result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    report_root = _path_key(phase0_result.get("source_root_final_path"))
    configured_root = (
        _path_key(roots[0]) if isinstance(roots, list) and len(roots) == 1 else None
    )
    markets = phase0_result.get("market_directories")
    sampled_reads_valid = isinstance(markets, Mapping) and set(markets) == {
        "sh",
        "sz",
        "bj",
    }
    if sampled_reads_valid:
        for market in ("sh", "sz", "bj"):
            market_row = markets[market]
            sample = (
                market_row.get("sample") if isinstance(market_row, Mapping) else None
            )
            identity = (
                sample.get("source_identity") if isinstance(sample, Mapping) else None
            )
            sampled_reads_valid = bool(
                sampled_reads_valid
                and isinstance(market_row, Mapping)
                and market_row.get("scope_complete") is True
                and isinstance(sample, Mapping)
                and isinstance(identity, Mapping)
                and identity.get("path_binding_verified") is True
                and identity.get("handle_path_verified") is True
                and isinstance(sample.get("raw_byte_sha256"), str)
                and len(sample["raw_byte_sha256"]) == 64
                and bool(sample.get("read_started_at"))
                and bool(sample.get("read_completed_at"))
            )
    scope_ready = (
        exact_pg_source
        and source_policy.get("all_source_roots_read_only") is True
        and report_root is not None
        and report_root == configured_root
        and sampled_reads_valid
    )
    blockers = [
        item.get("capability")
        for item in phase0_result.get("capabilities", ())
        if isinstance(item, Mapping)
        and item.get("required") is True
        and item.get("status") not in {"FULL_PASS", "DEGRADED_PASS"}
    ]
    return (
        CapabilityEvidence(
            "READY" if scope_ready else "DENY",
            f"source-ingress-phase0-projection:{digest}" if scope_ready else None,
            {
                "scope": "SOURCE_INGRESS",
                "evidence_type": "PHASE0_PATH_READ_AND_EXACT_PG_SOURCE_FACTS",
                "full_phase0_status": status,
                "full_phase0_digest": digest,
                "full_phase0_blocked_capabilities": blockers,
                "p04_pending": p04_pending,
                "field_unit_admission_is_separate": True,
            },
        ),
        {
            "status": status,
            "scope_status": "SOURCE_INGRESS_READY" if scope_ready else "BLOCKED",
            "scope": "SOURCE_INGRESS",
            "digest": digest,
            "p04_pending": p04_pending,
            "full_phase0_rewritten": False,
        },
    )


def _durability_evidence(config: Mapping[str, Any]) -> dict[str, Any]:
    """读取PG提交时间可追溯性；不以created_at或读取时钟代填。"""
    with source_raw._transaction(config, write=False) as (
        _connection,
        cursor,
        _namespace,
    ):
        cursor.execute(
            """SELECT current_setting('track_commit_timestamp'),
                      current_setting('synchronous_commit'),
                      current_setting('fsync')"""
        )
        track_commit_timestamp, synchronous_commit, fsync = cursor.fetchone()
    if track_commit_timestamp != "on":
        return {
            "status": "NOT_PROVABLE",
            "durable_at": None,
            "method": "PG_COMMIT_TIMESTAMP_DISABLED",
            "track_commit_timestamp": track_commit_timestamp,
            "synchronous_commit": synchronous_commit,
            "fsync": fsync,
            "reason_code": "PG_COMMIT_TIME_NOT_RECORDED",
        }
    return {
        "status": "NOT_PROVABLE",
        "durable_at": None,
        "method": "PG_ROW_COMMIT_BINDING_NOT_VERIFIED",
        "track_commit_timestamp": track_commit_timestamp,
        "synchronous_commit": synchronous_commit,
        "fsync": fsync,
        "reason_code": "PG_ROW_COMMIT_BINDING_NOT_VERIFIED",
    }


def calculate_baostock_capability(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    plan_sha256: str,
) -> dict[str, Any]:
    """按固定零请求策略核算P02；该函数不会登录或发送网络请求。"""
    if type(target_session) is not date:
        raise SourceIngressError("P02必须绑定显式目标交易会话")
    if (
        not isinstance(package_sha256, str)
        or len(package_sha256) != 64
        or any(c not in "0123456789abcdef" for c in package_sha256)
    ):
        raise SourceIngressError("P02必须绑定完整小写来源包SHA-256")
    if (
        not isinstance(plan_sha256, str)
        or len(plan_sha256) != 64
        or any(c not in "0123456789abcdef" for c in plan_sha256)
    ):
        raise SourceIngressError("P02必须绑定完整P00计划摘要")
    source_policy = config.get("source_policy")
    policy = (
        source_policy.get("baostock_supplement")
        if isinstance(source_policy, Mapping)
        else None
    )
    expected_budget = {
        "max_logins": 1,
        "max_queries_per_target_session": 10,
        "query_timeout_seconds": 30,
    }
    if (
        not isinstance(policy, Mapping)
        or policy.get("contract") != "STOCK-A-OPTIONAL-BAOSTOCK-SUPPLEMENT/R1"
        or policy.get("request_policy") != "NOT_REQUESTED"
        or policy.get("reason_code") != "OPTIONAL_SOURCE_NOT_REQUESTED"
        or policy.get("required_for_core") is not False
        or policy.get("request_budget") != expected_budget
    ):
        raise SourceIngressError("BaoStock策略未知或预算不符合登记合同，拒绝请求")
    return {
        "status": "NOT_REQUESTED",
        "capability": "UNKNOWN_NOT_MEASURED",
        "impact": "OPTIONAL_SUPPLEMENT_ONLY_CORE_UNBLOCKED",
        "reason_code": "OPTIONAL_SOURCE_NOT_REQUESTED",
        "target_session": target_session.isoformat(),
        "package_sha256": package_sha256,
        "plan_sha256": plan_sha256,
        "policy_contract": policy["contract"],
        "policy_sha256": _canonical_sha256(policy),
        "budget": dict(expected_budget),
        "actual_requests": {"logins": 0, "queries": 0},
        "source_available_at": None,
        "data_quality": "NOT_MEASURED_NO_RESPONSE",
        "network_accessed": False,
    }


def build_source_ingress_plan(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    phase0_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """按精确PG来源事实生成P00并核算P02；不写PG、不打开来源ZIP。"""
    calendar = _official_calendar(target_session)
    candidate_result = prepare_source_set_candidate(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
    )
    if candidate_result.get("status") != "COMPUTED_READ_ONLY":
        raise SourceIngressError("精确PG候选计算未返回可核验结果")
    candidate = candidate_result["candidate"]
    bao_policy = config["source_policy"]["baostock_supplement"]
    bao_policy_sha256 = _canonical_sha256(bao_policy)
    source_state_absent, accepted_state_reason = _accepted_source_state(config)
    if source_state_absent is None:
        raise SourceIngressError(accepted_state_reason)
    p04_source_input = inspect_p04_source_input(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
    )
    durability = _durability_evidence(config)
    seal_blockers = []
    if durability["status"] != "VERIFIED":
        seal_blockers.append("DURABLE_AT_NOT_PROVABLE")
    if p04_source_input["status"] == "P04_INGRESS_READER_NOT_CONNECTED":
        seal_blockers.append("P04_INGRESS_READER_NOT_CONNECTED")
    seal_blockers.append("P02_EVIDENCE_NOT_PERSISTED_WITH_PLAN_POLICY_BINDING")
    if p04_source_input["status"] not in {"AVAILABLE", "NO_MATCH"}:
        seal_blockers.append("FORMAL_SOURCE_SET_STORAGE_NOT_CONNECTED")
    pg_identity = inspect_postgres(config, timeout_seconds=5)
    contract = load_day_chain_contract(Path(config["paths"]["configuration_root"]))
    scope = contract["scope_bindings"].get("SOURCE_INGRESS")
    if not isinstance(scope, Mapping):
        raise SourceIngressError("已装配机器合同缺少SOURCE_INGRESS")
    phase0_capability, phase0_summary = _phase0_evidence(
        phase0_result, config=config, candidate=candidate
    )
    sessions = _calendar_sessions(target_session, calendar["calendar_sha256"])
    target = target_session.isoformat()
    scope_contract = _scope_revision_contract(contract, "SOURCE_INGRESS")
    source_digest = candidate["candidate_sha256"]
    capture_time = candidate["capture_validated_at"]
    if not isinstance(capture_time, str) or not capture_time:
        raise SourceIngressError("P01捕获缺少真实validated_at来源时钟")

    validated_inputs = {
        "ext.validated_configuration": CapabilityEvidence(
            "READY",
            "config/runtime.defaults.json",
            {
                "path": config["paths"]["configuration_root"],
                "baostock_policy": "NOT_REQUESTED",
            },
        ),
        "ext.calendar_authority": CapabilityEvidence(
            "READY",
            "official-calendar:" + calendar["calendar_sha256"],
            {
                "calendar_id": calendar["calendar_id"],
                "calendar_revision": calendar["calendar_revision"],
            },
        ),
        "ext.accepted_state_catalog": CapabilityEvidence(
            "READY" if source_state_absent else "UNKNOWN",
            "stock_catalog:SOURCE_INGRESS",
            {
                "status": "FIRST_OBSERVATION"
                if source_state_absent
                else "ACCEPTED_STATE_UNVERIFIED",
                "reason": accepted_state_reason,
            },
        ),
        "ext.contract_registry": CapabilityEvidence(
            "READY",
            "day-chain.contract.json:" + contract.assembled_sha256,
            {"contract_version": contract["version"]},
        ),
        "ext.build_identity": CapabilityEvidence(
            "READY",
            "stock-a:" + __version__,
            {
                "package_version": __version__,
                "application_id": config["application_identity"]["application_id"],
                "plan_scope": "SOURCE_INGRESS",
            },
        ),
    }
    env = {
        "validated_configuration": CapabilityEvidence(
            "READY", "config/runtime.defaults.json", {}
        ),
        "calendar_authority": CapabilityEvidence(
            "READY", "official-calendar:" + calendar["calendar_sha256"], {}
        ),
        "accepted_state_catalog": CapabilityEvidence(
            "READY" if source_state_absent else "UNKNOWN",
            "stock_catalog:SOURCE_INGRESS",
            {
                "status": "FIRST_OBSERVATION"
                if source_state_absent
                else "ACCEPTED_STATE_UNVERIFIED"
            },
        ),
        "contract_registry": CapabilityEvidence(
            "READY", "day-chain.contract.json:" + contract.assembled_sha256, {}
        ),
        "build_identity": CapabilityEvidence(
            "READY",
            "stock-a:" + __version__,
            {
                "package_version": __version__,
                "application_id": config["application_identity"]["application_id"],
            },
        ),
        "pg_deployment": CapabilityEvidence(
            "READY" if pg_identity.get("status") == "DB_MIGRATION_READY" else "DENY",
            "pg-identity:" + str(pg_identity.get("status")),
            {
                "database": config["postgresql"]["database"],
                "schema": config["postgresql"]["business_schema"],
                "status": pg_identity.get("status"),
                "storage_location_status": "USER_CONFIRMED_NOT_INDEPENDENTLY_VERIFIED",
            },
        ),
        "source_scope_evidence": phase0_capability,
        "readonly_source_roots": CapabilityEvidence(
            "READY"
            if config["source_policy"].get("all_source_roots_read_only") is True
            else "DENY",
            "runtime-config:source-roots",
            {"roots": config["source_policy"]["configured_tdx_source_roots"]},
        ),
    }
    field_ids = [item["field_id"] for item in scope["field_registry"]]
    owner_ids = list(scope["owner_roles"])
    consumers = list(scope["required_consumers"])
    frozen = {
        "target_session": target,
        "source_cutoff": capture_time,
        "universe": "EXACT_TDX_PACKAGE_CANDIDATES_ONLY",
        "member_revision": "NOT_APPLICABLE_P04_OUTSIDE_SCOPE",
        "previous_accepted_context": {
            "first_run": source_state_absent,
            "accepted_state_reason": accepted_state_reason,
            "accepted_state_status": "FIRST_OBSERVATION",
        },
        "required_owner_roles": owner_ids,
        "required_field_contracts": field_ids,
        "required_consumers": consumers,
        "source_gap_and_short_history_policy": "SOURCE_CANDIDATE_ONLY; gaps remain explicit; no P04/PIT inference",
        "algorithm_versions": {"candidate": "SOURCE_SET_CANDIDATE_R1"},
        "parameter_versions": {
            "official_calendar_sha256": calendar["calendar_sha256"],
            "baostock_policy": bao_policy_sha256,
            "baostock_request_budget": dict(bao_policy["request_budget"]),
        },
        "formal_scenario_roles": ["SOURCE_INGRESS_NON_PIT_CANDIDATE"],
        "ui_build": "web/preview/app.js",
        "expected_head_generation": None,
    }
    business_identity = {
        "values": {
            "target_session": target,
            "package_sha256": package_sha256,
            "source_content_id": candidate["source_content_id"],
            "source_capture_id": candidate["source_capture_id"],
            "candidate_sha256": source_digest,
            "row_count": candidate["raw_row_count"],
        },
        "quality": {
            "present": candidate["target_present_count"],
            "missing": candidate["target_missing_count"],
            "failed": candidate["read_failed_count"],
            "available_at": candidate["source_available_at"],
            "amount_unit": candidate["inputs"][4]["unit_status"],
            "volume_unit": candidate["inputs"][5]["unit_status"],
        },
        "provenance": {
            "dataset_id": candidate["source_dataset_id"],
            "parser_contract": candidate["parser_contract"],
            "capture_validated_at": capture_time,
            "clock_role": candidate["source_clock_role"],
        },
        "parameters": {
            "calendar_id": calendar["calendar_id"],
            "calendar_revision": calendar["calendar_revision"],
            "calendar_sha256": calendar["calendar_sha256"],
            "bao_request_policy": "NOT_REQUESTED",
            "bao_policy_sha256": bao_policy_sha256,
            "bao_request_budget": dict(bao_policy["request_budget"]),
        },
        "producer": {
            "module": "stock_a.application.source_ingress.build_source_ingress_plan",
            "p00_kernel": scope["day_plan_pure_kernel"]["symbol"],
            "candidate_digest": source_digest,
        },
    }
    plan_result = assemble_day_plan(
        DayPlanRequest(
            contract=contract,
            contract_sha256=contract.assembled_sha256,
            scope_id="SOURCE_INGRESS",
            target_session=target_session,
            official_sessions=sessions,
            calendar_id=calendar["calendar_id"],
            calendar_revision=calendar["calendar_revision"],
            scope_revision_contract=scope_contract,
            semantic_revisions={"source": candidate["parser_contract"]},
            previous=None,
            historical_repair_required=False,
            validated_inputs=validated_inputs,
            environment_capabilities=env,
            frozen_context=frozen,
            business_identity=business_identity,
            prior_context={
                "first_run": source_state_absent,
                "reason": accepted_state_reason,
            },
        )
    )
    plan = plan_result.day_plan
    if plan_result.environment_gate.status != "READY":
        seal_blockers.append("P00_SCOPE_ENVIRONMENT_GATE_NOT_READY")
    bao = calculate_baostock_capability(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        plan_sha256=plan.plan_sha256,
    )
    return {
        "ok": True,
        "scope": "SOURCE_INGRESS",
        "target_session": target,
        "package_sha256": package_sha256,
        "plan": {
            "status": plan.decision_state,
            "mode": plan.mode,
            "plan_sha256": plan.plan_sha256,
            "contract_sha256": plan.contract_sha256,
            "scheduled_stage_ids": list(plan.scheduled_stage_ids),
            "required_stage_ids": list(plan.required_stage_ids),
            "candidate_ready": plan.candidate_ready,
            "reason_codes": list(plan.reason_codes),
        },
        "plan_record_kind": "PREVIEW_COMPUTED_PLAN",
        "source_scope_status": "SOURCE_INGRESS_READY"
        if plan_result.environment_gate.status == "READY"
        else "SOURCE_INGRESS_BLOCKED",
        "p04_scope_status": "P04_PENDING",
        "environment_gate": {
            "status": plan_result.environment_gate.status,
            "reason_codes": list(plan_result.environment_gate.reason_codes),
            "capabilities": [
                {
                    "capability": key,
                    "status": status,
                    "evidence_ref": ref,
                    "metadata": dict(metadata),
                }
                for key, status, ref, metadata in plan_result.environment_gate.capability_states
            ],
        },
        "p01_capture": {
            "source_content_id": candidate["source_content_id"],
            "source_capture_id": candidate["source_capture_id"],
            "capture_validated_at": capture_time,
            "source_available_at": candidate["source_available_at"],
            "source_time_quality": candidate["source_time_quality"],
            "source_clock_role": candidate["source_clock_role"],
            "expected_rows": candidate["capture_expected_row_count"],
            "stored_rows": candidate["raw_row_count"],
            "present_rows": candidate["target_present_count"],
            "missing_rows": candidate["target_missing_count"],
            "failed_rows": candidate["read_failed_count"],
            "candidate_sha256": source_digest,
            "formal_source_seal": "BLOCKED" if seal_blockers else "READY",
            "formal_source_seal_blockers": seal_blockers,
            "non_pit_source_facts": "AVAILABLE_NON_PIT_RESEARCH_ONLY",
            "strict_pit_qualification": "BLOCKED",
            "strict_pit_blockers": [
                reason
                for reason, blocked in (
                    (
                        "SOURCE_AVAILABLE_AT_UNKNOWN",
                        candidate["source_available_at"] is None,
                    ),
                    (
                        "SOURCE_TIME_NOT_VERIFIED",
                        candidate["source_time_quality"] != "VERIFIED_SOURCE_TIME",
                    ),
                    ("DURABLE_AT_NOT_PROVABLE", durability["status"] != "VERIFIED"),
                )
                if blocked
            ],
            "durability_evidence": durability,
            "field_admission": {
                "PRICE": {
                    "status": "RAW_UNADJUSTED_NON_PIT",
                    "observed_rows": candidate["inputs"][3][
                        "normalized_observed_count"
                    ],
                    "unknown_rows": candidate["inputs"][3]["normalized_unknown_count"],
                    "unit_status": candidate["inputs"][3]["unit_status"],
                },
                "AMOUNT": {
                    "status": "UNIT_UNVERIFIED_NORMALIZED_FACTOR_BLOCKED",
                    "unit_status": candidate["inputs"][4]["unit_status"],
                },
                "VOLUME": {
                    "status": "UNIT_UNVERIFIED_NORMALIZED_FACTOR_BLOCKED",
                    "unit_status": candidate["inputs"][5]["unit_status"],
                },
            },
            "p04_input": p04_source_input,
        },
        "p02_baostock": {
            **bao,
            "record_kind": "PREVIEW_COMPUTED_CAPABILITY_ACCOUNTING",
            "formal_candidate_binding": "NOT_PERSISTED_WITH_PLAN_POLICY_BINDING",
        },
        "build_identity": {
            "application_id": config["application_identity"]["application_id"],
            "package_version": __version__,
        },
        "phase0": phase0_summary,
        "accepted_state": {
            "first_run": source_state_absent,
            "status": "FIRST_OBSERVATION",
            "reason": accepted_state_reason,
        },
        "database_write_attempted": False,
        "network_accessed": False,
        "p04_identity_and_strict_pit_required_for_p03": False,
        "p04_source_set_reader_required_for_formal_p03": True,
        "strict_pit_required_for_candidate_p03": False,
        "formal_source_seal": {
            "status": "BLOCKED" if seal_blockers else "READY",
            "blockers": seal_blockers,
            "migration_0006_created_or_executed": False,
        },
    }
