"""数据链机器合同与精确PG证据的只读投影。"""

from collections.abc import Mapping
from typing import Any

from stock_a.contracts import DayChainContractError
from stock_a.service.serialization import canonical_sha

_READINESS_GROUPS = (
    {"id": "source", "label": "来源", "stage_ids": ("P00", "P01", "P02", "P03")},
    {"id": "base_facts", "label": "基础事实", "stage_ids": ("P04", "P05", "P06")},
    {"id": "base_profile", "label": "基础画像", "stage_ids": ("P07", "P08", "P09")},
    {
        "id": "research",
        "label": "研究域",
        "stage_ids": tuple(f"P{i:02d}" for i in range(10, 20)),
    },
    {
        "id": "publication_view",
        "label": "发布与用户呈现",
        "stage_ids": ("P20", "P21", "P22", "P23", "P24", "P25"),
    },
)

_STAGE_CAPABILITIES: dict[str, dict[str, str]] = {
    "P00": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "NOT_CHECKED",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.application.planning.assemble_day_plan",
        "consumer": "当前28766预览不执行或持久化正式day_plan",
        "status_reason": "计划装配内核存在；本地预览没有正式DAG执行与day_plan持久化生产者。",
    },
    "P01": {
        "implementation_level": "PG_SOURCE_FACT",
        "data_status": "AVAILABLE",
        "consumer_status": "CONNECTED_PREVIEW",
        "evidence_type": "REAL_PG",
        "producer": "stock.source_raw_daily + stock.source_captures + stock.source_contents",
        "consumer": "Raw目录、来源分页、Raw单股及本状态GET",
        "status_reason": "所选日期和包SHA的来源捕获及PG观察元数据已核对。",
    },
    "P02": {
        "implementation_level": "DESIGN_ONLY",
        "data_status": "NOT_CHECKED",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.sources.baostock连接器代码（未绑定本次阶段采集）",
        "consumer": "可选补充源能力状态尚未接入本页查询；Bao数据可选，不阻断TDX Core",
        "status_reason": "存在独立连接器代码，但本只读请求没有采集BaoStock，也没有声明其对本日期可用。",
    },
    "P03": {
        "implementation_level": "DESIGN_ONLY",
        "data_status": "MISSING",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "尚无接入本地预览的数据源封口生产者",
        "consumer": "P04日期身份及基础目录、P05正式RAW状态",
        "status_reason": "所选日期缺少正式source_set/source_capabilities产物；当前运行代码没有登记来源封口生产者。这是已知阶段断点，不是PG查询失败。",
    },
    "P04": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "MISSING",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.canonical.identity语义摘要内核（未接为当日身份目录生产者）",
        "consumer": "P05正式RAW状态及后续规范计算尚未接通",
        "status_reason": "所选日期没有接通正式身份、有效期交易日历和官方Universe目录；身份摘要纯内核不能代替这些日期事实。",
    },
    "P05": {
        "implementation_level": "PG_SOURCE_FACT",
        "data_status": "AVAILABLE",
        "consumer_status": "CONNECTED_PREVIEW",
        "evidence_type": "REAL_PG",
        "producer": "stock.source_raw_daily精确日期/SHA来源代码观察",
        "consumer": "Raw目录、原始字段、来源分页及用户触发的单股历史预览",
        "status_reason": "PG已保存来源代码级Raw观察；这不是正式identity/universe下的raw_bars、基础事实持久化或完整RAW阶段。",
    },
    "P06": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "MISSING",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.canonical.adjustment与adjustment_periods纯内核",
        "consumer": "正式复权坐标、周月周期及reference_series生产链未接通",
        "status_reason": "复权和周期内核存在；本次GET不运行计算，且没有接通P05规范输入或正式持久化生产者。周/月规则仍标记NOT_IMPLEMENTED；量额单位按来源质量逐字段核实，relative未接通的字段仍显示CONTRACT_UNBOUND。",
    },
    "P07": {
        "implementation_level": "ON_DEMAND_PREVIEW",
        "data_status": "NOT_CHECKED",
        "consumer_status": "CONNECTED_PREVIEW",
        "evidence_type": "STATIC",
        "producer": "用户明确POST /api/preview/stock -> stock_a.research.core.calculate_core",
        "consumer": "现有28766单股研究预览；无正式Foundation/Owner持久化",
        "status_reason": "Core43只在用户明确触发单股研究预览时计算；本GET不计算Core，也不把查询能力当作正式P07结果。",
    },
    "P08": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "MISSING",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.research.cross_section.calculate_cross_section纯内核",
        "consumer": "没有连接正式Foundation成员集或持久化横截面消费者",
        "status_reason": "所选日期没有正式横截面产物；横截面内核存在，但来源候选不是有效上市总体，当前没有可接通的Foundation分母。",
    },
    "P09": {
        "implementation_level": "ON_DEMAND_PREVIEW",
        "data_status": "NOT_CHECKED",
        "consumer_status": "CONNECTED_PREVIEW",
        "evidence_type": "STATIC",
        "producer": "用户明确POST /api/preview/stock -> stock_a.research.entry Profile11预览",
        "consumer": "现有28766单股研究预览；无正式Foundation/Owner持久化",
        "status_reason": "Profile11只在用户明确触发单股研究预览时计算；本GET不计算Profile，也不代表正式基础画像产物。",
    },
    "P15": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "NOT_CHECKED",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.research.structure结构与锚点内核",
        "consumer": "没有接通P15正式同日输入和下游状态消费者",
        "status_reason": "结构/锚点纯计算内核存在；没有正式阶段执行或持久化证据。",
    },
    "P16": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "NOT_CHECKED",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.research.state.reduce_episode状态内核",
        "consumer": "没有接通正式状态、episode及差异事件生产链",
        "status_reason": "状态转换内核存在；历史前态及同日输入未在本页范围读取，不能宣称P16完成。",
    },
    "P17": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "NOT_CHECKED",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.research.risk.compose风险内核",
        "consumer": "没有接通正式风险行和完整画像消费者",
        "status_reason": "风险计算内核存在；依赖的研究域输入、正式生产者与持久化结果没有在本页接通。",
    },
    "P18": {
        "implementation_level": "PURE_KERNEL",
        "data_status": "NOT_CHECKED",
        "consumer_status": "NOT_CONNECTED",
        "evidence_type": "STATIC",
        "producer": "stock_a.research.risk.priority_status资格内核（部分）",
        "consumer": "没有接通正式Priority、雷达或Near-Miss发布消费者",
        "status_reason": "局部资格内核存在；P18完整依赖闭包及输出均未作为正式阶段运行。",
    },
    "P25": {
        "implementation_level": "ON_DEMAND_PREVIEW",
        "data_status": "NOT_CHECKED",
        "consumer_status": "CONNECTED_PREVIEW",
        "evidence_type": "STATIC",
        "producer": "现有Stock A 28766开发预览页面",
        "consumer": "本机研究预览用户；未接入正式P25显示确认和聚合完成消费者",
        "status_reason": "HTTP开发页可见不等于接受Publication、Head、正式API读回或frontend_view_ack；不声明FULL_CHAIN_DISPLAYED。",
    },
}


def _formal_source_set_readback_matches(
    source_ingress_result: Mapping[str, Any] | None,
) -> bool:
    """只把P00/P02/P03的精确typed封口读回视为正式消费者证据。"""
    if not isinstance(source_ingress_result, Mapping):
        return False
    plan = source_ingress_result.get("plan")
    bao = source_ingress_result.get("p02_baostock")
    capture = source_ingress_result.get("p01_capture")
    p04 = capture.get("p04_input") if isinstance(capture, Mapping) else None
    capability = p04.get("p02_capability") if isinstance(p04, Mapping) else None
    if not all(
        isinstance(item, Mapping) for item in (plan, bao, capture, p04, capability)
    ):
        return False
    plan_sha = plan.get("plan_sha256")
    policy_sha = bao.get("policy_sha256")
    requests = bao.get("actual_requests")
    return bool(
        p04.get("status") == "AVAILABLE"
        and p04.get("seal_status") == "SEALED"
        and p04.get("durability_status") == "VERIFIED_POST_COMMIT_READBACK"
        and p04.get("candidate_fallback") is False
        and p04.get("database_write") is False
        and p04.get("ddl_executed") is False
        and p04.get("target_session") == source_ingress_result.get("target_session")
        and p04.get("package_sha256") == source_ingress_result.get("package_sha256")
        and p04.get("source_capture_id") == capture.get("source_capture_id")
        and p04.get("plan_sha256") == plan_sha
        and capability.get("plan_sha256") == plan_sha
        and capability.get("policy_sha256") == policy_sha
        and capability.get("status") == bao.get("status")
        and capability.get("actual_logins") == (requests or {}).get("logins")
        and capability.get("actual_queries") == (requests or {}).get("queries")
        and capability.get("network_accessed") is bao.get("network_accessed")
    )


def build_chain_readiness_payload(
    contract: Mapping[str, Any],
    source_version: Mapping[str, Any],
    *,
    checked_at: str,
    source_set_candidate: Mapping[str, Any] | None = None,
    source_ingress_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """将唯一机器合同和本次精确PG证据组合为只读准备状态。"""
    stages = contract.get("stages")
    artifacts = contract.get("artifact_groups")
    scope = contract.get("scope_bindings", {}).get("FOUNDATION_EOD")
    bindings = scope.get("stage_bindings") if isinstance(scope, Mapping) else None
    expected_ids = [f"P{index:02d}" for index in range(26)]
    if (
        not isinstance(stages, list)
        or len(stages) != 26
        or [item.get("id") for item in stages if isinstance(item, Mapping)]
        != expected_ids
        or not isinstance(artifacts, Mapping)
        or not isinstance(bindings, list)
    ):
        raise DayChainContractError(
            "DAY_CHAIN_READINESS_CONTRACT_INVALID",
            "数据链准备页要求完整的P00至P25机器合同及FOUNDATION_EOD绑定",
        )
    scope_ids = {row.get("stage_id") for row in bindings if isinstance(row, Mapping)}
    if not scope_ids or not scope_ids.issubset(set(expected_ids)):
        raise DayChainContractError(
            "DAY_CHAIN_READINESS_SCOPE_INVALID",
            "FOUNDATION_EOD阶段绑定必须属于机器合同的P00至P25",
        )
    bindings_by_id = {
        row["stage_id"]: row
        for row in bindings
        if isinstance(row, Mapping) and isinstance(row.get("stage_id"), str)
    }

    counts = source_version["bar_status_counts"]
    has_failed = counts["READ_FAILED"] > 0
    source_data_status = (
        "PARTIAL"
        if source_version["version_status"] == "PARTIAL" or has_failed
        else "AVAILABLE"
    )
    scopes = contract.get("scope_policy", {}).get("scopes", {})
    source_ingress = scopes.get("SOURCE_INGRESS", {})
    required_source_stages = source_ingress.get("required_stages", ())
    source_binding = contract.get("scope_bindings", {}).get("SOURCE_INGRESS", {})
    source_bindings = source_binding.get("stage_bindings", ())
    source_bound_ids = {
        row.get("stage_id") for row in source_bindings if isinstance(row, Mapping)
    }
    if (
        not isinstance(required_source_stages, (list, tuple))
        or not required_source_stages
        or any(
            stage_id not in {f"P{i:02d}" for i in range(26)}
            for stage_id in required_source_stages
        )
    ):
        raise DayChainContractError(
            "DAY_CHAIN_SOURCE_INGRESS_SCOPE_INVALID",
            "来源准入要求必须来自有效SOURCE_INGRESS阶段合同",
        )
    candidate_status = (
        source_set_candidate.get("status", "NO_MATCH")
        if isinstance(source_set_candidate, Mapping)
        else "NO_MATCH"
    )
    formal_source_set_readback = _formal_source_set_readback_matches(
        source_ingress_result
    )

    stage_rows: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}
    for stage in stages:
        stage_id = stage["id"]
        capability = _STAGE_CAPABILITIES.get(
            stage_id,
            {
                "implementation_level": "DESIGN_ONLY",
                "data_status": "NOT_APPLICABLE",
                "consumer_status": "NOT_CONNECTED",
                "evidence_type": "STATIC",
                "producer": "当前运行代码没有登记此阶段的生产者绑定",
                "consumer": "机器合同所列下游尚未由正式运行路径接通",
                "status_reason": "该阶段目前只有设计合同或算法引用；未接通正式阶段执行、输入读取和持久化消费者。这是未实现状态，不是所选数据查询失败。",
            },
        )
        capability = dict(capability)
        if stage_id == "P01":
            capability["data_status"] = source_data_status
            capability["evidence_type"] = "REAL_PG"
            if formal_source_set_readback:
                capability.update(
                    {
                        "implementation_level": "PG_SOURCE_FACT",
                        "consumer_status": "CONNECTED",
                        "evidence_type": "SEALED_TYPED_PG_READBACK",
                        "status_reason": "P03正式source_set已按相同捕获与Raw摘要封口并由P04精确读回。",
                    }
                )
        if stage_id == "P05":
            capability["data_status"] = "SOURCE_RAW_ONLY"
            capability["evidence_type"] = "REAL_PG_SOURCE_OBSERVATION"
            capability["status_reason"] = (
                f"PG有{source_version['stored_row_count']}/{source_version['capture_expected_row_count']}条source_raw_daily来源代码观察；"
                "这些记录未绑定正式P04身份与Universe，不是P05 raw_bars，也不能代表官方上市总体。"
            )
        if stage_id == "P00" and isinstance(source_ingress_result, Mapping):
            plan = source_ingress_result.get("plan", {})
            gate = source_ingress_result.get("environment_gate", {})
            gate_status = gate.get("status")
            capability.update(
                {
                    "implementation_level": "RUNNABLE_P00_PLAN",
                    "data_status": plan.get("status", "UNKNOWN"),
                    "consumer_status": (
                        "CONNECTED"
                        if formal_source_set_readback
                        else (
                            "CONNECTED_PREVIEW"
                            if gate_status == "READY"
                            else "BLOCKED_ENVIRONMENT"
                        )
                    ),
                    "evidence_type": "REAL_PG_AND_CONTRACT",
                    "producer": "stock_a.application.source_ingress.build_source_ingress_plan → stock_a.application.planning.assemble_day_plan",
                    "consumer": "source-ingress-plan CLI、GET /api/preview/chain-readiness、预览页",
                    "status_reason": (
                        f"{source_ingress_result.get('plan_record_kind', 'PREVIEW_COMPUTED_PLAN')}："
                        f"精确版本P00计划={plan.get('mode')}，摘要={plan.get('plan_sha256')}；"
                        f"环境门={gate_status}，原因={','.join(gate.get('reason_codes', [])) or '无'}。"
                        "该计划未持久接受，计划本身不会执行P01至P03。"
                    ),
                }
            )
        if stage_id == "P02" and isinstance(source_ingress_result, Mapping):
            bao = source_ingress_result.get("p02_baostock", {})
            capability.update(
                {
                    "implementation_level": "RUNNABLE_CAPABILITY_ACCOUNTING",
                    "data_status": bao.get("status", "UNKNOWN"),
                    "consumer_status": (
                        "CONNECTED"
                        if formal_source_set_readback
                        else "CONNECTED_PREVIEW"
                    ),
                    "evidence_type": "REAL_POLICY_ACCOUNTING",
                    "producer": "stock_a.application.source_ingress.calculate_baostock_capability",
                    "consumer": "source-ingress-plan CLI、GET /api/preview/chain-readiness、预览页",
                    "status_reason": (
                        f"本日期Bao策略={bao.get('status')}，实际登录/查询="
                        f"{bao.get('actual_requests', {}).get('logins', 0)}/"
                        f"{bao.get('actual_requests', {}).get('queries', 0)}；能力保持"
                        f"{bao.get('capability', 'UNKNOWN')}，零请求且可选补充不阻断Core；"
                        f"{bao.get('record_kind', 'PREVIEW_COMPUTED_CAPABILITY_ACCOUNTING')}。"
                    ),
                }
            )
        if stage_id == "P03" and formal_source_set_readback:
            p04_input = source_ingress_result["p01_capture"]["p04_input"]
            capability.update(
                {
                    "implementation_level": "FORMAL_TYPED_SOURCE_SET",
                    "data_status": "AVAILABLE_FORMAL_SOURCE_SET",
                    "consumer_status": "CONNECTED",
                    "candidate_consumer_status": "CONNECTED",
                    "evidence_type": "SEALED_TYPED_PG_READBACK",
                    "producer": "stock.source_sets + stock.source_set_capabilities typed rows",
                    "consumer": "P04 exact source_set reader",
                    "formal_source_set_status": "SEALED_AND_READ_BACK",
                    "p04_input_status": p04_input["status"],
                    "p02_binding_status": "EXACT_PLAN_AND_POLICY_SHA_MATCH",
                    "status_reason": (
                        f"精确封口{p04_input.get('source_set_id')}已由P04按目标日、包SHA、"
                        "捕获、P00计划和P02策略摘要读回；稳定身份解析仍属于后续P04能力。"
                    ),
                }
            )
        elif stage_id == "P03" and candidate_status == "AVAILABLE":
            candidate = source_set_candidate.get("candidate", {})
            p02_binding = source_set_candidate.get("p02_binding", {})
            if isinstance(source_ingress_result, Mapping):
                ingress_capture = source_ingress_result.get("p01_capture", {})
                blockers = ingress_capture.get("formal_source_seal_blockers", [])
                p04_input = ingress_capture.get("p04_input", {})
                capability.update(
                    {
                        "implementation_level": "PG_CANDIDATE",
                        "data_status": "AVAILABLE_CANDIDATE",
                        "consumer_status": "NOT_CONNECTED",
                        "candidate_consumer_status": "CONNECTED_PREVIEW",
                        "evidence_type": "REAL_PG_NON_PIT_CANDIDATE",
                        "producer": "stock_a.storage.postgres.source_set_candidate.persist_source_set_candidate",
                        "consumer": "P04正式source_set输入读取；当前未接通，候选仅供只读诊断",
                        "formal_source_set_status": ingress_capture.get(
                            "formal_source_seal", "BLOCKED"
                        ),
                        "p04_input_status": p04_input.get(
                            "status", "P04_INGRESS_READER_NOT_CONNECTED"
                        ),
                        "p02_binding_status": p02_binding.get(
                            "status", "NOT_BOUND_TO_P00_PLAN_OR_POLICY"
                        ),
                        "status_reason": (
                            f"只读候选摘要{candidate.get('candidate_sha256', '未知')}；"
                            "NON_PIT价格Raw事实可供受限后验研究。source_available_at未知只阻断严格PIT；"
                            "量额单位未知只阻断相应规范化字段。正式seal仍被"
                            f"{','.join(blockers) or '未确认前置'}阻断；"
                            f"旧候选P02绑定={p02_binding.get('status', 'NOT_BOUND_TO_P00_PLAN_OR_POLICY')}。"
                        ),
                    }
                )
        prior_inputs = [
            {
                key: item[key]
                for key in ("role", "temporal_role", "first_day", "component")
                if key in item
            }
            for item in stage.get("prior_inputs", ())
            if isinstance(item, Mapping)
        ]
        output_consumers: list[dict[str, Any]] = []
        next_consumer_ids: set[str] = set()
        for output in stage.get("outputs", ()):
            artifact = artifacts.get(output)
            if (
                not isinstance(artifact, Mapping)
                or artifact.get("producer") != stage_id
            ):
                continue
            consumers = artifact.get("consumers", ())
            terminal = artifact.get("terminal_consumers", ())
            output_consumers.append(
                {
                    "output": output,
                    "consumers": list(consumers)
                    if isinstance(consumers, (list, tuple))
                    else [],
                    "terminal_consumers": list(terminal)
                    if isinstance(terminal, (list, tuple))
                    else [],
                    "data_role": artifact.get("data_role"),
                }
            )
            next_consumer_ids.update(
                value for value in consumers if isinstance(value, str)
            )
        row = {
            "stage_id": stage_id,
            "name_zh": stage["name_zh"],
            "completion_role": stage.get("completion_role"),
            "scope_binding": (
                "SOURCE_INGRESS_BOUND"
                if stage_id in source_bound_ids
                else (
                    "FOUNDATION_BOUND_SOURCE_INGRESS_UNBOUND"
                    if stage_id in required_source_stages and stage_id in bindings_by_id
                    else (
                        "SOURCE_INGRESS_REQUIRED_UNBOUND"
                        if stage_id in required_source_stages
                        else (
                            bindings_by_id[stage_id].get(
                                "kernel_role", "FOUNDATION_BOUND"
                            )
                            if stage_id in bindings_by_id
                            else "OUTSIDE_FOUNDATION_EOD_FULL_CHAIN_DESIGN"
                        )
                    )
                )
            ),
            "implementation_level": capability["implementation_level"],
            "data_status": capability["data_status"],
            "consumer_status": capability["consumer_status"],
            "evidence_type": capability["evidence_type"],
            "producer": capability["producer"],
            "consumer": capability["consumer"],
            "required_inputs": list(stage.get("required_inputs", ())),
            "optional_inputs": list(stage.get("optional_inputs", ())),
            "prior_inputs": prior_inputs,
            "outputs": list(stage.get("outputs", ())),
            "output_consumers": output_consumers,
            "next_consumer_stage_ids": sorted(next_consumer_ids),
            "source_date": source_version["source_date"],
            "package_sha256": source_version["package_sha256"],
            "status_reason": capability["status_reason"],
            "checked_at": checked_at,
            "is_first_break": False,
        }
        stage_rows.append(row)
        by_id[stage_id] = row

    first_break_row = None
    for stage_id in required_source_stages:
        row = by_id[stage_id]
        if (
            stage_id not in source_bound_ids
            or row["implementation_level"] == "DESIGN_ONLY"
            or row["data_status"] in {"MISSING", "NOT_CHECKED"}
            or row["consumer_status"] != "CONNECTED"
        ):
            first_break_row = row
            break
    first_break_id = (
        first_break_row["stage_id"] if first_break_row is not None else None
    )
    for row in stage_rows:
        row["is_first_break"] = row["stage_id"] == first_break_id
    first_break_reason = (
        first_break_row["status_reason"]
        if first_break_row is not None
        else "SOURCE_INGRESS全部要求阶段均已具备正式绑定、实际数据和连接消费者。"
    )
    if first_break_row is not None and first_break_id not in source_bound_ids:
        first_break_reason = f"SOURCE_INGRESS要求阶段{first_break_id}尚无正式scope绑定；{first_break_reason}"
    first_break_kind = (
        "SOURCE_INGRESS_COMPLETE"
        if first_break_row is None
        else (
            "SOURCE_INGRESS_STAGE_NOT_BOUND"
            if first_break_id not in source_bound_ids
            else "SOURCE_INGRESS_STAGE_NOT_READY"
        )
    )

    groups = []
    for group in _READINESS_GROUPS:
        group_stages = [by_id[stage_id] for stage_id in group["stage_ids"]]
        broken = next((row for row in group_stages if row["is_first_break"]), None)
        if broken is None:
            broken = next(
                (row for row in group_stages if row["data_status"] == "MISSING"), None
            )
        groups.append(
            {
                "group_id": group["id"],
                "label": group["label"],
                "stage_ids": list(group["stage_ids"]),
                "first_break_stage_id": broken["stage_id"] if broken else None,
                "summary": (
                    f"本组首先缺少：{broken['stage_id']} {broken['name_zh']}。{broken['status_reason']}"
                    if broken
                    else "按机器合同展示能力、输入和消费者；未运行阶段不会误标为数据故障。"
                ),
            }
        )
    first_break = {
        "stage_id": first_break_id,
        "name_zh": by_id[first_break_id]["name_zh"] if first_break_id else None,
        "kind": first_break_kind,
        "scope": "SOURCE_INGRESS",
        "status_reason": first_break_reason,
    }
    return {
        "ok": True,
        "status": "FORMAL_FOUNDATION_NOT_READY",
        "highest_available_scope": "SOURCE_SYMBOL_ONLY / RAW_UNADJUSTED / NON_PIT_QUERY_PREVIEW_CAPABILITY",
        "formal_foundation_status": "NOT_CONNECTED",
        "day_chain_complete": False,
        "official_listing_population": "UNKNOWN_NOT_CONNECTED",
        "source_version": dict(source_version),
        "source_set_candidate": dict(
            source_set_candidate or {"status": "NO_MATCH", "candidate": None}
        ),
        "source_ingress": dict(source_ingress_result or {"status": "NOT_RUN"}),
        "source_observations": {
            "status": source_data_status,
            "version_status": source_version["version_status"],
            "stored_rows": source_version["stored_row_count"],
            "expected_rows": source_version["capture_expected_row_count"],
            "bar_status_counts": dict(counts),
            "status_reason": (
                "来源版本本身存在未达到捕获期望数或READ_FAILED缺口；此来源质量问题与正式P03未接通是两项独立状态。"
                if source_data_status == "PARTIAL"
                else "PG来源代码观察分母闭合；这只描述来源包，不代表正式P03 source_set 或全市场总体已接通。"
            ),
        },
        "first_break": first_break,
        "groups": groups,
        "stages": stage_rows,
        "contract": {
            "contract_id": contract.get("contract_id"),
            "version": contract.get("version"),
            "sha256": canonical_sha(contract),
            "scope_id": "FOUNDATION_EOD",
        },
        "source_date": source_version["source_date"],
        "package_sha256": source_version["package_sha256"],
        "evidence_type": "REAL_PG_AND_STATIC_CODE_BINDINGS",
        "formal_source_set_readback": formal_source_set_readback,
        "producer": source_version["producer"],
        "consumer": source_version["consumer"],
        "status_reason": "来源候选与研究预览可用范围已核实；正式source_set、identity/universe、Canonical/Owner/Publication/Head链及P25正式显示确认未接通。",
        "checked_at": checked_at,
        "database_write": False,
        "ddl_executed": False,
        "source_zip_opened": False,
        "source_recalculated": False,
        "core_calculated": False,
    }
