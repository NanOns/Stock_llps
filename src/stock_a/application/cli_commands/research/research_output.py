"""Stock A命令行research_output职责模块。"""

from __future__ import annotations

from typing import Any


def _summary_field(value: Any) -> str:
    if not isinstance(value, dict):
        return "—"
    quality = value.get("quality", "UNKNOWN_DATA")
    if quality != "OBSERVED":
        return f"未知:{value.get('reason') or '原因未明'}"
    rendered = value.get("value")
    if isinstance(rendered, dict):
        rendered = rendered.get("decimal", rendered.get("exact"))
    return f"{rendered} [{quality}]"


def _summary_fact(value: Any) -> str:
    if not isinstance(value, dict):
        return "—"
    value_payload = value.get("value", {})
    unit_trust = value.get("unit_trust")
    if not isinstance(unit_trust, dict) and isinstance(value_payload, dict):
        unit_trust = value_payload.get("unit_trust")
    unit_note = ""
    if isinstance(unit_trust, dict):
        interpretation = unit_trust.get("interpretation")
        if interpretation:
            unit_note = "；单位假设:" + str(interpretation)
        elif unit_trust.get("status") == "CALLER_DECLARED_ASSUMPTION":
            unit_note = "；单位假设:调用方声明"
    return (
        f"{value.get('field_id', 'unknown')}={_summary_field(value_payload)}"
        f" [公式/来源:{value.get('formula_source', '未登记')}"
        f"；坐标:{value.get('coordinate_role', '未登记')}"
        f"{'; 身份:' + value['coordinate_id'] if value.get('coordinate_id') else ''}"
        f"{unit_note}]"
    )


def _powershell_quote(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _render_basic_fact_summary(result: dict[str, Any]) -> str:
    dimensions = result.get("basic_fact_dimensions", {})
    groups = dimensions.get("dimensions", {})
    source_quality = dimensions.get("source_quality", {})
    raw = source_quality.get("raw_availability", {})
    core = source_quality.get("core_availability", {})
    canonical = result.get("canonical_selection", {})
    canonical_source = canonical.get("source", {})
    canonical_fields = canonical.get("fields", {})
    canonical_observed = sum(
        item.get("quality") == "OBSERVED" for item in canonical_fields.values()
    )
    lines = [
        f"基础量价研究解释 | {result.get('requested_symbol', '未知代码')} | 目标日 {result.get('target_session', '未知')} | NON_PIT_RETROSPECTIVE",
        (
            f"来源末日 {result.get('source_last_session', '未知')}；本机只读扫描 "
            f"{result.get('read_time', {}).get('started_at', '未知')} 至 "
            f"{result.get('read_time', {}).get('completed_at', '未知')}"
        ),
        (
            f"身份 {result.get('identity', {}).get('status', 'SOURCE_SYMBOL_ONLY')}；"
            f"价格坐标 {result.get('adjustment', {}).get('coordinate', 'RAW_UNADJUSTED')}；"
            "来源权威/首捕时钟未绑定；这是历史研究预览，不是实时或严格PIT数据。"
        ),
    ]
    reconciliation = result.get("identity_reconciliation")
    identity = result.get("identity", {})
    if reconciliation:
        lines.append(
            f"Bao身份={identity.get('security_name') or '未知'} / {reconciliation.get('bao_trade_status')}；"
            f"归并={reconciliation.get('status')}；TDX目标Bar={reconciliation.get('tdx_target_bar_status')}；"
            f"身份观察时钟={identity.get('observed_at') or '不可用'}；"
            f"角色={identity.get('time_role') or 'NON_PIT_RETROSPECTIVE'}。"
        )
        capture = result.get("bao_identity_capture", {})
        if capture:
            lines.append(
                f"Bao查询={capture.get('status')}；捕获区间={capture.get('request_started_at')} 至 "
                f"{capture.get('request_completed_at')}；采集时钟不代表历史首可用时钟。"
            )
    lines.append(
        f"Canonical只读选择：{canonical.get('selection_status', '未生成')}；"
        f"目标字段OBSERVED={canonical_observed}/{len(canonical_fields)}；"
    )
    group_labels = (
        ("price_trend", "价格与趋势"),
        ("position_volatility", "位置与波动"),
        ("participation", "量额参与"),
    )
    for key, label in group_labels:
        facts = groups.get(key, {})
        rendered = (
            "；".join(_summary_fact(item) for item in facts.values()) or "无可显示字段"
        )
        lines.append(f"{label}：{rendered}")
    lines.append(
        f"可用性：兼容状态={result.get('status')}；"
        f"Raw={raw.get('status')} {raw.get('observed_fields', 0)}/{raw.get('total_fields', 0)}；"
        f"Core={core.get('status')} {core.get('observed_fields', 0)}/{core.get('total_fields', 0)}"
    )
    window = source_quality.get("window_context", {})
    if window:
        lines.append(
            f"回看窗口：日历={window.get('calendar_revision', '未知')}，"
            f"会话={window.get('calendar_session_count', 0)}，"
            f"实际bar={window.get('actual_price_bars', 0)}，"
            f"缺bar会话={window.get('missing_bar_sessions', 0)}，"
            f"Core回看完整={window.get('lookback_complete')}，"
            f"原因={window.get('lookback_reason') or '无'}，"
            f"使用目标日后记录={window.get('future_rows_used')}"
        )
    unknowns = source_quality.get("unknown_facts_by_dimension", {})
    for key, label in group_labels:
        reasons = unknowns.get(key, {})
        if reasons:
            lines.append(
                f"{label}未知原因："
                + "；".join(
                    f"{field_id}={reason}" for field_id, reason in reasons.items()
                )
            )
    explanation = result.get("availability_explanation", {})
    if explanation.get("missing_target_bar_explanation"):
        lines.append("目标日行情：" + explanation["missing_target_bar_explanation"])
    source = result.get("source", {})
    budgets = source.get("budgets", {})
    if budgets:
        lines.append(
            f"读取预算：{source.get('byte_count', 0)}/{budgets.get('max_bytes', 0)}字节，"
            f"{source.get('record_count', 0)}/{budgets.get('max_records', 0)}条；"
            "金额/数量单位为调用方声明假设。"
        )
    preview = result.get("profile_preview")
    if preview:
        lines.append(
            f"基础画像组件预览：{preview['counts']['observed']}项有值，"
            f"{preview['counts']['unknown_data']}项数据未知，"
            f"{preview['counts']['not_implemented'] + preview['counts']['contract_unbound']}项能力未接通；"
            "全部仅为RAW_UNADJUSTED/NON_PIT描述预览，正式profile()仍阻断。"
        )
        for component in preview["components"]:
            if component["status"] == "EVALUATED":
                result_text = (
                    component["value"]
                    if component["quality"] == "OBSERVED"
                    else component.get("reason")
                )
                inputs = "；".join(
                    f"{item['rule_input_field']}←{item['source_field_id']}({item['value']['quality']})"
                    for item in component["inputs"]
                )
                lines.append(
                    f"{component['field_id']}：{component['quality']}={result_text}；"
                    f"规则={component['rule_id']}；输入={inputs or '无'}"
                )
            else:
                missing = "、".join(
                    item["field_id"] for item in component.get("missing_inputs", [])
                )
                lines.append(
                    f"{component['field_id']}：{component['status']}；缺少={missing or '未登记生产者'}；"
                    f"原因={component['reason']}"
                )
    bao = result.get("baostock_evidence")
    if bao:
        fields = bao.get("fields", {})
        comparisons = bao.get("comparisons", {})
        lines.append(
            f"BaoStock补充来源：{bao.get('status')}；代码={bao.get('requested_source_code')}；"
            f"请求时钟={bao.get('request_started_at')}至{bao.get('request_completed_at')}；"
            "NON_PIT_RETROSPECTIVE/RAW_UNADJUSTED/SOURCE_SYMBOL_ONLY；TDX主结果未替换。"
        )
        for key, label in (
            ("open", "开盘"),
            ("high", "最高"),
            ("low", "最低"),
            ("close", "收盘"),
        ):
            field = fields.get(key, {})
            comparison = bao.get("price_comparisons", {}).get(key, {})
            lines.append(
                f"Bao {label}：{_summary_field(field)}；未复权对照={comparison.get('status', '—')}；"
                f"差(Bao−TDX)={comparison.get('difference_baostock_minus_tdx')}；"
                f"adjustflag={comparison.get('observed_adjustflag') or '未核实'}"
            )
        for key, label in (
            ("volume", "成交量"),
            ("amount", "成交金额"),
            ("turn", "换手"),
        ):
            field = fields.get(key, {})
            lines.append(
                f"Bao {label}：{_summary_field(field)}；对照={comparisons.get(key, {}).get('status', '—')}"
                f"/{comparisons.get(key, {}).get('observed_relation_under_declared_units', '—')}"
            )
        for key, label in (("tradestatus", "来源交易状态"), ("isST", "来源ST")):
            field = fields.get(key, {})
            lines.append(
                f"Bao {label}：{field.get('status', 'UNKNOWN')}；原值={field.get('raw_value')}；"
                f"质量={field.get('quality')}；原因={field.get('reason') or '无'}"
            )
        amount_explanation = bao.get("amount_precision_explanation", {})
        volume_difference = bao.get("volume_difference", {})
        lines.append(
            f"金额精度解释={amount_explanation.get('status', 'NOT_COMPARABLE')}；"
            f"金额精确差(Bao−TDX CNY)={amount_explanation.get('difference_bao_minus_tdx_cny')}；"
            f"float32 ULP(CNY)={amount_explanation.get('tdx_float32_ulp_cny')}；"
            f"差值/ULP={amount_explanation.get('difference_in_tdx_float32_ulps')}；"
            f"投影={amount_explanation.get('bao_amount_float32_projection_in_tdx_storage_units')}。"
        )
        lines.append(
            f"成交量差状态={volume_difference.get('status', 'NOT_COMPARABLE')}；"
            f"精确差(Bao−TDX股)={volume_difference.get('difference_bao_minus_tdx_shares')}；"
            f"原因={volume_difference.get('reason') or '数值相同'}。金额精度仅在声明单位下解释金额，不解释整数成交量；"
            "差值不构成来源单位权威或TDX错误判定。"
        )
        lines.append(
            "本查询只解释当前可得字段；换手历史分位按实际历史分母显示，不写来源或数据库。"
        )
    return "\n".join(lines)


def _render_universe_summary(result: dict[str, Any]) -> str:
    discovery = result["discovery"]
    denominators = result["denominators"]
    page = result["page"]
    budget = result["budget"]
    profile_denominators = result.get("profile_preview_denominators")
    source_quality = result.get("source_target_quality")
    performance = result.get("performance")
    lines = [
        f"TDX来源候选盘后观察 | {result['target_session']} | {result['candidate_contract']} | {result['price_coordinate']}",
        (
            f"来源目录项={discovery['directory_entries_scanned']}，.day文件={discovery['day_file_entries']}，"
            f"范围候选={denominators['source_candidate_total']}，越界类别排除={discovery['excluded_out_of_scope']}，"
            f"无效文件名={discovery['invalid_day_filenames']}"
        ),
        (
            f"过滤后分母={denominators['filtered_candidate_total']}；本页offset={page['offset']}、"
            f"limit={page['limit']}、返回={page['returned']}、页后剩余={page['remaining_after_page']}、"
            f"本页未评价={denominators['page_not_evaluated']}、全过滤集合未评价={denominators['not_evaluated_in_filtered_candidate_set']}"
        ),
        (
            f"本页分类 available={denominators['page_available']}、failed={denominators['page_failed']}、"
            f"partial_unknown={denominators['page_partial_unknown']}、not_evaluated={denominators['page_not_evaluated']}；"
            f"Raw/Core状态见逐行列"
        ),
        (
            f"本页目标Bar={result['coverage_summary']['actual_bar_on_target_count']}、"
            f"目标缺Bar={result['coverage_summary']['missing_target_bar_count']}、"
            f"读失败={result['coverage_summary']['source_read_failed_count']}、"
            f"已读文件末日最大值={result['coverage_summary']['latest_source_session'] or '无'}；"
            "此最大值只属于本页已完整读完的文件，不是全市场正式可得时钟"
        ),
        (
            f"读取={budget['used_bytes']}字节/{budget['used_records']}条，"
            f"发现={budget['discovery_elapsed_seconds']}秒，总耗时={budget['elapsed_seconds']}秒；"
            f"未复权、NON_PIT，不代表全候选池已读取或全部指标已知"
        ),
        (
            "代码 | 市场/板别 | 目标Bar及逐字段质量 | 来源末日 | Raw收盘 | MA5 | MA20 | MA60 | "
            "ATR20 | ret5 | ret20 | 收盘/MA20 | Amount | Volume | Raw/Core | "
            "未知原因/错误 | 单股回查"
        ),
    ]
    if isinstance(source_quality, dict):
        lines.insert(
            2,
            (
                f"同包全候选目标Bar：总数={source_quality['candidate_total']}，有Bar={source_quality['target_present']}，"
                f"缺Bar={source_quality['target_missing']}，读取失败={source_quality['read_failed']}，"
                f"未评价={source_quality['not_evaluated']}；分区闭合="
                f"{source_quality['candidate_partition_complete'] and source_quality['evaluation_partition_complete']}；"
                f"评价范围={source_quality.get('evaluation_mode', 'UNKNOWN')}；"
                f"有效上市分母仍为UNKNOWN；来源扫描={source_quality['read_usage']['bytes']}字节/"
                f"{source_quality['read_usage']['records']}条/{source_quality['read_usage']['elapsed_seconds']}秒"
            ),
        )
    if isinstance(performance, dict):
        full_scan = performance.get("full_pool_target_scan_seconds")
        lines.insert(
            3,
            (
                "分段耗时：原件完整性校验="
                f"{performance['package_integrity_validation_seconds']}秒，候选目录="
                f"{performance['candidate_pool_metadata_seconds']}秒，页条目解码="
                f"{performance['page_entry_decode_seconds']}秒，Core+Profile="
                f"{performance['page_core_profile_seconds']}秒，全池质量扫描="
                f"{full_scan if full_scan is not None else '未执行'}秒，总命令="
                f"{performance['total_command_seconds']}秒"
            ),
        )
    if profile_denominators is not None:
        components = profile_denominators["components"]
        lines.insert(
            5,
            (
                f"Profile本页分母：请求对象={profile_denominators['page_requested']}，"
                f"已返回画像={profile_denominators['objects_with_profile_preview']}，"
                f"失败={profile_denominators['objects_failed']}，未评价={profile_denominators['objects_not_evaluated']}；"
                f"组件总数={components['total']}，evaluated={components['evaluated']}，"
                f"observed={components['observed']}，unknown_data={components['unknown_data']}，"
                f"not_implemented={components['not_implemented']}，contract_unbound={components['contract_unbound']}"
            ),
        )
        lines[-1] += " | Profile组件（字段=质量/状态:值或原因）"
    for item in result["summary"]:
        raw = item.get("raw_availability") or {}
        core = item.get("core_availability") or {}
        raw_text = f"{raw.get('status', '未评价')} {raw.get('observed_fields', 0)}/{raw.get('total_fields', 0)}"
        core_text = f"{core.get('status', '未评价')} {core.get('observed_fields', 0)}/{core.get('total_fields', 0)}"
        reasons = dict(core.get("unknown_reasons", {}))
        for reason, count in (raw.get("unknown_reasons") or {}).items():
            reasons[f"Raw:{reason}"] = count
        reason_text = ";".join(
            f"{name}×{count}" for name, count in sorted(reasons.items())
        )
        if item.get("error_code"):
            reason_text = ";".join(filter(None, (reason_text, item["error_code"])))
        source_quality_row = item.get("source_bar_quality") or {}
        field_quality_text = (
            ",".join(
                f"{name}={source_quality_row.get(f'{name}_quality') or 'UNKNOWN'}"
                for name in ("price", "amount", "volume")
            )
            if source_quality_row
            else "未评价"
        )
        bar_text = (
            f"{item.get('source_bar_status') or '页内未评价'}:{item.get('source_bar_reason') or '—'}"
            f"[{field_quality_text}]"
        )
        detail = item.get("detail_query", {})
        detail_command = (
            "stock-a "
            + " ".join(
                _powershell_quote(value) for value in detail.get("arguments", [])
            )
            if detail
            else "—"
        )
        row = [
            item["symbol"],
            f"{item['market']}/{item['board']}",
            bar_text,
            item.get("source_last_session") or "—",
            _summary_field(item.get("raw_close")),
            _summary_field(item.get("ma5")),
            _summary_field(item.get("ma20")),
            _summary_field(item.get("ma60")),
            _summary_field(item.get("atr20")),
            _summary_field(item.get("ret5")),
            _summary_field(item.get("ret20")),
            _summary_field(item.get("close_vs_ma20")),
            _summary_field(item.get("amount")),
            _summary_field(item.get("volume")),
            f"{raw_text}/{core_text}",
            reason_text or "—",
            detail_command,
        ]
        if profile_denominators is not None:
            preview = item.get("profile_preview")
            if isinstance(preview, dict):
                component_text = []
                for component in preview["components"]:
                    field_id = component["field_id"]
                    if component["status"] == "EVALUATED":
                        if component["quality"] == "OBSERVED":
                            value_text = component.get("value")
                            component_text.append(f"{field_id}=OBSERVED:{value_text}")
                        else:
                            component_text.append(
                                f"{field_id}=UNKNOWN_DATA:{component.get('reason') or '原因未明'}"
                            )
                    else:
                        component_text.append(
                            f"{field_id}={component['status']}:{component.get('reason') or '原因未登记'}"
                        )
                row.append("；".join(component_text))
            else:
                row.append(
                    f"{item.get('status')}:{item.get('error_code') or '未返回画像'}"
                )
        lines.append(" | ".join(row))
    return "\n".join(lines)
