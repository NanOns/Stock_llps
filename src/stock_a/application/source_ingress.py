"""SOURCE_INGRESS范围内的真实P00计划与P02零请求能力核算。"""

from __future__ import annotations

from datetime import date
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from stock_a import __version__
from stock_a.application.planning import (
    AcceptedRunIdentity,
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
from stock_a.storage.postgres.source_set_ingress import (
    inspect_p04_source_input,
    read_accepted_source_state,
)


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


def _accepted_source_state(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    source_digest: str,
) -> dict[str, Any]:
    """从正式精确读者构造P00前态；候选、Raw和未封口行不构成accepted-state。"""
    result = read_accepted_source_state(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        source_digest=source_digest,
    )
    if result.get("status") in {"BLOCKED", "RANGE_REPAIR_REQUIRED"}:
        raise SourceIngressError(
            str(result.get("reason", "正式accepted-state读回失败"))
        )
    if result.get("status") == "RESUME_REQUIRED":
        pending = result.get("pending")
        if (
            not isinstance(pending, Mapping)
            or pending.get("package_sha256") != package_sha256
            or pending.get("source_digest") != source_digest
            or result.get("same_day_sealed_conflict") is True
        ):
            raise SourceIngressError(
                "目标日PENDING记录与当前输入不同或存在同日SEALED冲突，禁止隐式覆盖"
            )
    previous_row = result.get("previous")
    if previous_row is None:
        if result.get("status") not in {"FIRST_OBSERVATION", "RESUME_REQUIRED"}:
            raise SourceIngressError("accepted-state读者返回状态与前态不一致")
        if result.get("status") == "RESUME_REQUIRED":
            result["plan_status"] = "FIRST_OBSERVATION"
        return {**result, "previous_identity": None}
    if result.get("status") not in {
        "APPEND",
        "NOOP",
        "RESUME_REQUIRED",
    } or not isinstance(previous_row, Mapping):
        raise SourceIngressError("accepted-state读者返回了不完整正式关系")
    try:
        previous = AcceptedRunIdentity(
            target_session=previous_row["target_session"],
            head_generation=previous_row["head_generation"],
            calendar_id=previous_row["calendar_id"],
            calendar_revision=previous_row["calendar_revision"],
            scope_id=previous_row["scope_id"],
            scope_contract_version=previous_row["scope_contract_version"],
            scope_contract_digest=previous_row["scope_contract_digest"],
            scope_catalog_digest=previous_row["scope_catalog_digest"],
            semantic_revisions=previous_row["semantic_revisions"],
            business_input_digest=previous_row["business_input_digest"],
        )
    except (KeyError, TypeError) as error:
        raise SourceIngressError("accepted-state缺少P00所需的精确身份字段") from error
    if type(previous.target_session) is not date:
        raise SourceIngressError("accepted-state目标日不是官方date")
    if result.get("status") == "RESUME_REQUIRED":
        result["plan_status"] = "ACCEPTED"
    return {**result, "previous_identity": previous}


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
        return (
            CapabilityEvidence(
                "UNKNOWN",
                None,
                {
                    "scope": "SOURCE_INGRESS",
                    "evidence_type": "EXACT_PG_CONTENT_CAPTURE_RAW",
                    "full_phase0_status": "NOT_SCANNED_BY_READ_ONLY_API",
                    "pg_history_is_not_phase0": exact_pg_source,
                    "configured_roots_readonly_is_not_a_scan": (
                        source_policy.get("all_source_roots_read_only") is True
                        and isinstance(roots, list)
                        and len(roots) == 1
                    ),
                    "p04_pending": p04_pending,
                },
            ),
            {
                "status": "NOT_RUN",
                "scope_status": "UNKNOWN",
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
    stable_market_evidence = {}
    if sampled_reads_valid:
        for market in ("sh", "sz", "bj"):
            market_row = markets[market]
            sample = market_row["sample"]
            identity = sample["source_identity"]
            stable_market_evidence[market] = {
                "scope_complete": market_row["scope_complete"],
                "market_status": market_row.get("market_status"),
                "day_file_count": market_row.get("day_file_count"),
                "sample": {
                    "raw_byte_sha256": sample["raw_byte_sha256"],
                    "byte_count": sample["byte_count"],
                    "record_count": sample["record_count"],
                    "first_date": sample.get("first_date"),
                    "last_date": sample.get("last_date"),
                    "source_identity": {
                        key: identity.get(key)
                        for key in (
                            "volume_serial_number",
                            "file_id",
                            "size_before",
                            "size_after",
                            "path_binding_verified",
                            "handle_path_verified",
                        )
                    },
                },
            }
    scope_projection = {
        "scope": "SOURCE_INGRESS",
        "source_root_final_path": report_root,
        "configured_root": configured_root,
        "source_roots_read_only": source_policy.get("all_source_roots_read_only")
        is True,
        "market_sample_identities": stable_market_evidence,
        "pg_content_id": candidate.get("source_content_id"),
        "pg_capture_id": candidate.get("source_capture_id"),
        "pg_candidate_digest": candidate.get("candidate_sha256"),
        "pg_raw_row_count": candidate.get("raw_row_count"),
        "pg_raw_digest": candidate.get("raw_source_digest"),
    }
    scope_digest = sha256(
        json.dumps(
            scope_projection, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    scope_ready = (
        exact_pg_source
        and source_policy.get("all_source_roots_read_only") is True
        and report_root is not None
        and report_root == configured_root
        and sampled_reads_valid
    )
    return (
        CapabilityEvidence(
            "READY" if scope_ready else "DENY",
            f"source-ingress-phase0-projection:{scope_digest}" if scope_ready else None,
            {
                "scope": "SOURCE_INGRESS",
                "evidence_type": "PHASE0_PATH_READ_AND_EXACT_PG_SOURCE_FACTS",
                "source_scope_digest": scope_digest,
                "field_unit_admission_is_separate": True,
            },
        ),
        {
            "status": status,
            "scope_status": "SOURCE_INGRESS_READY" if scope_ready else "BLOCKED",
            "scope": "SOURCE_INGRESS",
            "digest": digest,
            "source_scope_digest": scope_digest,
            "p04_pending": p04_pending,
            "full_phase0_rewritten": False,
        },
    )


def _durability_evidence(config: Mapping[str, Any]) -> dict[str, Any]:
    """读取PG设置作诊断；没有事务提交ACK和独立读回时一律不证明durability。"""
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
    return {
        "status": "NOT_PROVABLE",
        "durable_at": None,
        "server_commit_at": None,
        "commit_ack_observed_at": None,
        "post_commit_observed_at": None,
        "method": "COMMIT_ACK_PLUS_SEPARATE_TRANSACTION_EXACT_READBACK_REQUIRED",
        "track_commit_timestamp": track_commit_timestamp,
        "synchronous_commit": synchronous_commit,
        "fsync": fsync,
        "reason_code": "NO_BOUND_COMMIT_ACK_FOR_EXACT_SOURCE_SET",
    }


def _p02_binding_sha256(capability: Mapping[str, Any]) -> str:
    """对P02状态、计划、策略和真实请求计数形成可复算的独立绑定摘要。"""
    identity = {
        key: capability.get(key)
        for key in (
            "target_session",
            "package_sha256",
            "plan_sha256",
            "policy_contract",
            "policy_sha256",
            "status",
            "reason_code",
            "actual_requests",
            "network_accessed",
            "data_quality",
        )
    }
    return _canonical_sha256(identity)


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
    capability = {
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
    capability["binding_sha256"] = _p02_binding_sha256(capability)
    capability["binding_status"] = "PREVIEW_COMPUTED_NOT_PERSISTED"
    return capability


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
    accepted_state = _accepted_source_state(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        source_digest=candidate["candidate_sha256"],
    )
    previous = accepted_state["previous_identity"]
    accepted_state_reason = str(accepted_state.get("reason", ""))
    planning_state = str(accepted_state.get("plan_status", accepted_state["status"]))
    previous_p02 = (accepted_state.get("previous") or {}).get("p02_capability")
    historical_repair_required = bool(
        previous is not None
        and isinstance(previous_p02, Mapping)
        and previous_p02.get("policy_sha256") != bao_policy_sha256
    )
    p04_source_input = inspect_p04_source_input(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
    )
    if p04_source_input.get("status") == "AVAILABLE":
        durability = {
            "status": "VERIFIED",
            "durable_at": p04_source_input.get("durable_at"),
            "server_commit_at": None,
            "commit_ack_observed_at": p04_source_input.get("commit_ack_observed_at"),
            "post_commit_observed_at": p04_source_input.get("durable_at"),
            "method": "PENDING_COMMIT_ACK_PLUS_SEPARATE_READBACK_AND_SEALED_ROW_READBACK",
            "track_commit_timestamp": None,
            "synchronous_commit": None,
            "fsync": None,
            "reason_code": None,
        }
    else:
        durability = _durability_evidence(config)
    seal_blockers = []
    if p04_source_input.get("status") != "AVAILABLE":
        seal_blockers.append("FORMAL_SOURCE_SET_NOT_SEALED")
        if p04_source_input.get("status") not in {
            "NO_MATCH",
            "P04_INGRESS_READER_NOT_CONNECTED",
        }:
            seal_blockers.append("FORMAL_SOURCE_SET_READER_BLOCKED")
        if durability["status"] != "VERIFIED":
            seal_blockers.append("DURABLE_AT_NOT_PROVABLE")
        seal_blockers.append("P02_EVIDENCE_PENDING_FORMAL_SOURCE_SET_BINDING")
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
            "READY",
            "stock_catalog:SOURCE_INGRESS",
            {
                "status": planning_state,
                "previous_source_set_id": (accepted_state.get("previous") or {}).get(
                    "source_set_id"
                ),
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
            "READY",
            "stock_catalog:SOURCE_INGRESS",
            {
                "status": planning_state,
                "previous_source_set_id": (accepted_state.get("previous") or {}).get(
                    "source_set_id"
                ),
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
            "status": planning_state,
            "previous": (
                {
                    key: (value.isoformat() if isinstance(value, date) else value)
                    for key, value in (accepted_state.get("previous") or {}).items()
                    if key
                    in {
                        "namespace_id",
                        "target_session",
                        "source_set_id",
                        "revision_id",
                        "package_sha256",
                        "source_capture_id",
                        "source_digest",
                        "plan_sha256",
                        "durable_at",
                    }
                }
                if accepted_state.get("previous")
                else None
            ),
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
        "expected_head_generation": (
            None if previous is None else previous.head_generation
        ),
    }
    business_identity = {
        "values": {
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
            previous=previous,
            historical_repair_required=historical_repair_required,
            validated_inputs=validated_inputs,
            environment_capabilities=env,
            frozen_context=frozen,
            business_identity=business_identity,
            prior_context={
                "first_run": previous is None,
                "accepted_identity_ref": (
                    f"stock.source_sets:{accepted_state['previous']['namespace_id']}"
                    f":{accepted_state['previous']['source_set_id']}"
                    f":{accepted_state['previous']['revision_id']}"
                    if accepted_state.get("previous")
                    else None
                ),
                "status": planning_state,
                "previous_source_set_id": (accepted_state.get("previous") or {}).get(
                    "source_set_id"
                ),
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
            "input_digest": plan.input_digest,
            "expected_parent_generation": plan.expected_parent_generation,
            "scheduled_stage_ids": list(plan.scheduled_stage_ids),
            "required_stage_ids": list(plan.required_stage_ids),
            "candidate_ready": plan.candidate_ready,
            "reason_codes": list(plan.reason_codes),
        },
        "plan_record_kind": "PREVIEW_COMPUTED_PLAN",
        "scope_contract_version": scope_contract.contract_version,
        "scope_contract_digest": scope_contract.binding_digest,
        "scope_catalog_digest": scope_contract.catalog_digest,
        "source_scope_status": "SOURCE_INGRESS_READY"
        if plan_result.environment_gate.status == "READY"
        else "SOURCE_INGRESS_BLOCKED",
        "p04_scope_status": (
            "P04_FORMAL_SOURCE_AVAILABLE"
            if p04_source_input.get("status") == "AVAILABLE"
            else "P04_PENDING"
        ),
        "p04_post_write_check": (
            "ALREADY_VERIFIED_BY_EXACT_TYPED_READER"
            if p04_source_input.get("status") == "AVAILABLE"
            else "REQUIRED_AFTER_TYPED_COMMIT_BY_EXACT_DATE_AND_PACKAGE_SHA"
        ),
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
            "formal_candidate_binding": (
                "BOUND_IN_FORMAL_SOURCE_SET"
                if p04_source_input.get("status") == "AVAILABLE"
                else "PENDING_FORMAL_SOURCE_SET_COMMIT"
            ),
            "formal_binding_sha256": bao["binding_sha256"],
        },
        "build_identity": {
            "application_id": config["application_identity"]["application_id"],
            "package_version": __version__,
        },
        "phase0": phase0_summary,
        "accepted_state": {
            "first_run": previous is None,
            "status": accepted_state["status"],
            "reason": accepted_state_reason,
            "previous": (
                {
                    key: (value.isoformat() if isinstance(value, date) else value)
                    for key, value in (accepted_state.get("previous") or {}).items()
                    if key
                    in {
                        "namespace_id",
                        "target_session",
                        "source_set_id",
                        "revision_id",
                        "package_sha256",
                        "source_capture_id",
                        "source_digest",
                        "plan_sha256",
                        "durable_at",
                    }
                }
                if accepted_state.get("previous")
                else None
            ),
        },
        "database_write_attempted": False,
        "network_accessed": False,
        "p04_identity_and_strict_pit_required_for_p03": False,
        "p04_source_set_reader_post_write_check_required": True,
        "strict_pit_required_for_candidate_p03": False,
        "formal_source_seal": {
            "status": "BLOCKED" if seal_blockers else "READY",
            "blockers": seal_blockers,
            "migration_0006_created_or_executed": False,
        },
    }


def persist_formal_source_set(
    config: Mapping[str, Any],
    *,
    target_session: date,
    package_sha256: str,
    phase0_result: Mapping[str, Any],
) -> dict[str, Any]:
    """以真实Phase0、P00/P02和精确PG候选执行PENDING→ACK→读回→封口。"""
    from stock_a.storage.postgres.source_set_candidate import (
        persist_source_set_candidate as persist_pg_candidate,
    )
    from stock_a.storage.postgres.source_set_ingress import read_p04_source_input
    from stock_a.storage.postgres.source_set_writer import (
        persist_formal_source_set as write_p03,
    )

    calendar = _official_calendar(target_session)
    ingress = build_source_ingress_plan(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        phase0_result=phase0_result,
    )
    if ingress.get("source_scope_status") != "SOURCE_INGRESS_READY":
        raise SourceIngressError("真实Phase0和精确PG Raw未证明SOURCE_INGRESS范围可写")
    if ingress.get("environment_gate", {}).get("status") != "READY":
        raise SourceIngressError("P00 SOURCE_INGRESS环境准入未就绪")

    accepted_state = ingress.get("accepted_state", {})
    if accepted_state.get("status") == "NOOP":
        readback = read_p04_source_input(
            config,
            target_session=target_session,
            package_sha256=package_sha256,
        )
        if readback.get("status") != "AVAILABLE" or readback.get(
            "source_digest"
        ) != ingress.get("p01_capture", {}).get("candidate_sha256"):
            raise SourceIngressError(
                "同日既有SEALED来源没有精确P04读回，不能作为幂等NOOP"
            )
        return {
            "status": "NOOP",
            "target_session": target_session.isoformat(),
            "package_sha256": package_sha256,
            "source_set_id": readback["source_set_id"],
            "source_digest": readback["source_digest"],
            "p00_plan_sha256": ingress["plan"]["plan_sha256"],
            "p02_binding_sha256": ingress["p02_baostock"]["binding_sha256"],
            "p04_readback": readback,
            "candidate_database_write": False,
            "formal_database_write": False,
            "formal_p03_complete": True,
            "database_write_attempted": False,
        }

    if ingress.get("plan", {}).get("mode") not in {"INITIAL", "APPEND"}:
        raise SourceIngressError("当前P00模式不是允许正式来源写入的INITIAL或APPEND")
    candidate_persist = persist_pg_candidate(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        calendar=calendar,
        bao_capability=ingress["p02_baostock"],
    )
    formal = write_p03(
        config,
        target_session=target_session,
        package_sha256=package_sha256,
        calendar=calendar,
        ingress=ingress,
    )
    return {
        **formal,
        "p00_plan": ingress["plan"],
        "p00_scope_status": ingress["source_scope_status"],
        "p00_environment_gate": ingress["environment_gate"],
        "p02_baostock": ingress["p02_baostock"],
        "candidate_persist": candidate_persist,
        "formal_p03_complete": formal.get("seal_status") == "SEALED",
        "database_write_attempted": True,
    }
