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


def build_chain_readiness_payload(
    contract: Mapping[str, Any],
    source_version: Mapping[str, Any],
    *,
    checked_at: str,
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
    first_break_id = "P03"
    first_break_reason = (
        "真实捕获及来源代码级Raw观察之后，正式来源封口source_set尚无已接通生产者；"
        "正式Canonical链从P03开始断开。来源观察自身的完整性单独显示，不改变此正式首断点。"
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
        if stage_id in {"P01", "P05"}:
            capability["data_status"] = source_data_status
            capability["evidence_type"] = "REAL_PG"
            if stage_id == "P05":
                capability["status_reason"] = (
                    f"PG精确读取到{source_version['stored_row_count']}/{source_version['capture_expected_row_count']}条来源代码观察，"
                    "bar状态分区和来源引用均已核验；这是source_raw_daily来源观察，不是正式identity/universe下的raw_bars或Foundation持久化。"
                    if source_data_status == "AVAILABLE"
                    else f"PG精确读取到{source_version['stored_row_count']}/{source_version['capture_expected_row_count']}条来源代码观察；"
                    "当前版本为PARTIAL或含READ_FAILED，且这些观察仍不等于正式identity/universe下的raw_bars。"
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
                bindings_by_id[stage_id].get("kernel_role", "FOUNDATION_BOUND")
                if stage_id in bindings_by_id
                else "OUTSIDE_FOUNDATION_EOD_FULL_CHAIN_DESIGN"
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
            "is_first_break": stage_id == first_break_id,
        }
        stage_rows.append(row)
        by_id[stage_id] = row

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
        "name_zh": by_id[first_break_id]["name_zh"],
        "kind": "FORMAL_SOURCE_SET_NOT_CONNECTED",
        "scope": "现有来源观察之后的正式业务链",
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
