/* Stock A预览页面source_readiness职责模块；保持原业务逻辑和交互。 */
"use strict";
function clearChainReadiness(message) {
  $("chainState").className = "chain-state";
  $("chainState").textContent = message;
  $("chainChecked").textContent = "尚未查询";
  $("sourceIngressSummary").textContent =
    "等待精确日期与来源包版本的实时PG核算。";
  $("sourceSetCandidate").textContent = "等待精确日期与包版本查询。";
  $("formalSourceSet").textContent = "等待精确日期与来源包版本的正式封口读取。";
  $("chainOverview").replaceChildren();
  $("chainGroups").replaceChildren();
}
function readinessDetail(parent, label, value) {
  const dt = document.createElement("dt");
  dt.textContent = label;
  const dd = document.createElement("dd");
  dd.textContent = value;
  parent.append(dt, dd);
}
function renderSourceIngress(data) {
  const root = $("sourceIngressSummary");
  root.replaceChildren();
  const ingress = data.source_ingress;
  if (!ingress || ingress.status === "NOT_RUN") {
    appendText(root, "p", "本次没有SOURCE_INGRESS运行结果。 ");
    return;
  }
  const grid = document.createElement("div");
  grid.className = "source-ingress-grid";
  const p00 = document.createElement("div");
  p00.className = "source-ingress-card";
  appendText(
    p00,
    "strong",
    `P00 ${ingress.plan?.mode || "—"} · ${ingress.plan?.status || "—"} · ${ingress.environment_gate?.status || "—"}`,
  );
  appendText(p00, "p", `计划SHA ${ingress.plan?.plan_sha256 || "—"}`);
  appendText(
    p00,
    "p",
    `计划类型 ${ingress.plan_record_kind || "—"}；来源范围 ${ingress.source_scope_status || "—"}；下游身份 ${ingress.p04_scope_status || "P04_PENDING"}`,
  );
  appendText(
    p00,
    "p",
    `完整阶段0 ${ingress.phase0?.status || "NOT_RUN"}；来源范围证据 ${ingress.phase0?.scope_status || ingress.source_scope_status || "—"}；完整阶段0结论不由范围投影覆盖。`,
  );
  appendText(
    p00,
    "p",
    `阶段要求 ${ingress.plan?.required_stage_ids?.join("、") || "—"}；本次排程 ${ingress.plan?.scheduled_stage_ids?.join("、") || "无"}`,
  );
  const blocked = (ingress.environment_gate?.capabilities || [])
    .filter((item) => item.status !== "READY")
    .map((item) => `${item.capability}=${item.status}`);
  appendText(
    p00,
    "p",
    `环境门原因 ${ingress.environment_gate?.reason_codes?.join("、") || "无"}；未就绪能力 ${blocked.join("、") || "无"}`,
  );
  appendText(
    p00,
    "p",
    `P01捕获 ${ingress.p01_capture?.source_capture_id || "—"} · ${ingress.p01_capture?.capture_validated_at || "—"}；available_at=${ingress.p01_capture?.source_available_at ?? "未知"}`,
  );
  appendText(
    p00,
    "p",
    `真实分母 ${ingress.p01_capture?.stored_rows ?? "—"}/${ingress.p01_capture?.expected_rows ?? "—"}；有Bar ${ingress.p01_capture?.present_rows ?? "—"} / 缺Bar ${ingress.p01_capture?.missing_rows ?? "—"} / 失败 ${ingress.p01_capture?.failed_rows ?? "—"}`,
  );
  appendText(
    p00,
    "p",
    `NON_PIT来源事实 ${ingress.p01_capture?.non_pit_source_facts || "未知"}；严格PIT ${ingress.p01_capture?.strict_pit_qualification || "未知"}。价格 ${ingress.p01_capture?.field_admission?.PRICE?.status || "未知"}；金额 ${ingress.p01_capture?.field_admission?.AMOUNT?.status || "未知"}；数量 ${ingress.p01_capture?.field_admission?.VOLUME?.status || "未知"}。`,
  );
  const p04InputStatus =
    ingress.p01_capture?.p04_input?.status ||
    "P04_INGRESS_READER_NOT_CONNECTED";
  appendText(
    p00,
    "p",
    `P03正式产物 ${p04InputStatus === "AVAILABLE" ? "SEALED并由P04精确读回" : "尚未由P04读回"}；` +
      `本次新封口前置 ${ingress.p01_capture?.formal_source_seal || "未知"}，` +
      `阻断项 ${(ingress.p01_capture?.formal_source_seal_blockers || []).join("、") || "无"}；` +
      `P04输入读取 ${p04InputStatus}。`,
  );
  appendText(
    p00,
    "p",
    `耐久时间证据 ${ingress.p01_capture?.durability_evidence?.status || "未知"} · ${ingress.p01_capture?.durability_evidence?.reason_code || "无原因码"}；不以capture或查询时间代填。`,
  );

  const p02 = document.createElement("div");
  p02.className = "source-ingress-card";
  const bao = ingress.p02_baostock || {};
  appendText(
    p02,
    "strong",
    `P02 BaoStock ${bao.status || "—"} · 能力 ${bao.capability || "—"}`,
  );
  appendText(
    p02,
    "p",
    `实际请求：登录 ${bao.actual_requests?.logins ?? "—"} 次，查询 ${bao.actual_requests?.queries ?? "—"} 次；网络访问 ${bao.network_accessed === false ? "否" : "未知"}`,
  );
  appendText(
    p02,
    "p",
    `已登记预算：每批登录 ${bao.budget?.max_logins ?? "—"} 次、每目标日查询 ${bao.budget?.max_queries_per_target_session ?? "—"} 次、单次 ${bao.budget?.query_timeout_seconds ?? "—"} 秒。`,
  );
  appendText(
    p02,
    "p",
    `影响：${bao.impact || "未知"}；${bao.reason_code || "无原因码"}。零请求不代表Bao能力可用。`,
  );
  appendText(
    p02,
    "p",
    `P02绑定计划SHA ${bao.plan_sha256 || "—"}；配置摘要 ${bao.policy_sha256 || "—"}`,
  );
  appendText(
    p02,
    "p",
    `P02记录类型 ${bao.record_kind || "—"}；历史候选绑定 ${bao.formal_candidate_binding || "—"}。零请求账不冒充已持久接受。`,
  );
  grid.append(p00, p02);
  root.append(grid);
}
function renderSourceSetCandidate(data) {
  const root = $("sourceSetCandidate");
  root.replaceChildren();
  const candidate = data.source_set_candidate?.candidate;
  if (!candidate) {
    appendText(
      root,
      "p",
      "该精确日期与TDX包SHA没有已保存来源集合候选；本区只展示候选预览。",
    );
    return;
  }
  appendText(
    root,
    "p",
    `PG保存的来源集合候选 · ${candidate.target_session} · ${candidate.candidate_status}；此候选自身不是P03正式封口。`,
    "candidate-status",
  );
  const details = document.createElement("details");
  details.className = "candidate-details";
  appendText(
    details,
    "summary",
    `查看真实来源身份、质量分母与输入引用（${candidate.input_count}项）`,
  );
  const facts = [
    ["来源包SHA", candidate.package_sha256],
    ["PG内容身份", candidate.source_content_id],
    ["真实捕获UUID", candidate.source_capture_id],
    [
      "来源捕获时钟",
      `${candidate.capture_read_started_at} → ${candidate.capture_validated_at}`,
    ],
    ["来源真实可得时间", candidate.source_available_at || "未知（未填充）"],
    [
      "时钟质量 / 角色",
      `${candidate.source_time_quality} · ${candidate.source_clock_role}`,
    ],
    [
      "Raw分母与状态",
      `${candidate.raw_row_count}/${candidate.capture_expected_row_count}；有Bar ${candidate.target_present_count} / 缺Bar ${candidate.target_missing_count} / 失败 ${candidate.read_failed_count}`,
    ],
    [
      "Raw / 捕获摘要",
      `${candidate.raw_source_digest} / ${candidate.capture_canonical_digest}`,
    ],
    [
      "字段质量单位状态",
      `价格 RAW_UNADJUSTED；金额 ${candidate.inputs[4].unit_status}；数量 ${candidate.inputs[5].unit_status}`,
    ],
    [
      "候选摘要 / 日历",
      `${candidate.candidate_sha256} · ${candidate.calendar_revision} (${candidate.calendar_sha256})`,
    ],
    ["身份 / PIT", `${candidate.identity_status} · ${candidate.pit_status}`],
    [
      "历史P02绑定",
      `${data.source_set_candidate?.p02_binding?.status || "NOT_BOUND_TO_P00_PLAN_OR_POLICY"}；plan SHA ${data.source_set_candidate?.p02_binding?.plan_sha256 || "无"}；policy SHA ${data.source_set_candidate?.p02_binding?.policy_sha256 || "无"}`,
    ],
    [
      "P04来源输入读取",
      `${data.source_ingress?.p01_capture?.p04_input?.status || "P04_INGRESS_READER_NOT_CONNECTED"}；正式source_set缺失时不回退候选。`,
    ],
  ];
  const list = document.createElement("dl");
  list.className = "candidate-facts";
  for (const [label, value] of facts)
    readinessDetail(list, label, String(value));
  details.append(list);
  const inputs = document.createElement("ul");
  inputs.className = "candidate-inputs";
  for (const input of candidate.inputs) {
    const description =
      input.input_role === "BAOSTOCK_CAPABILITY"
        ? "BaoStock未请求；无虚构捕获或内容身份。"
        : `${input.input_role}${input.field_family ? ` · ${input.field_family}` : ""} · ${input.input_status}${input.evidence_sha256 ? ` · ${input.evidence_sha256}` : ""}`;
    appendText(inputs, "li", description);
  }
  details.append(inputs);
  root.append(details);
}
function renderFormalSourceSet(data) {
  const root = $("formalSourceSet");
  root.replaceChildren();
  const sourceSet = data.formal_source_set || {};
  if (sourceSet.status !== "AVAILABLE") {
    const status = sourceSet.status || "NOT_RUN";
    appendText(
      root,
      "p",
      `当前精确日期与来源包SHA的P04读取状态：${status}。候选预览不作为正式来源集合回退。`,
    );
    return;
  }
  appendText(
    root,
    "p",
    `SEALED · ${sourceSet.target_session} · revision ${sourceSet.revision_id} · source_set_id ${sourceSet.source_set_id}`,
    "formal-source-status",
  );
  appendText(
    root,
    "p",
    "本页已按精确日期和包SHA读回SEALED来源集合；TDX市场代码与可验证OHLC可作NON_PIT价格研究。稳定security_id、官方上市总体、上市/停复牌历史及严格PIT分别保持未知。",
  );
  const list = document.createElement("dl");
  list.className = "formal-source-facts";
  const p02 = sourceSet.p02_capability || {};
  const ack = sourceSet.commit_ack || {};
  const facts = [
    ["精确来源包 SHA", sourceSet.package_sha256],
    [
      "来源内容 / 捕获",
      `${sourceSet.source_content_id} / ${sourceSet.source_capture_id}`,
    ],
    [
      "捕获读取起止时钟",
      `${sourceSet.capture_read_started_at} / ${sourceSet.capture_validated_at}`,
    ],
    [
      "P00计划 SHA / P02绑定 SHA",
      `${sourceSet.plan_sha256} / ${p02.binding_sha256}`,
    ],
    ["P02策略 SHA / 状态", `${p02.policy_sha256} / ${p02.status}`],
    ["PENDING COMMIT ACK观察", ack.commit_ack_observed_at],
    ["PENDING独立读回 durable_at", sourceSet.durable_at],
    ["SEALED时钟", sourceSet.sealed_at],
    [
      "来源可得时间 / 时钟资格",
      `${sourceSet.source_available_at || "未知（未填充）"} / ${sourceSet.source_time_quality}`,
    ],
    [
      "来源性质",
      `${sourceSet.identity_status} · ${sourceSet.stable_security_identity} · ${sourceSet.source_time_quality}`,
    ],
    [
      "Raw分母",
      `${sourceSet.raw_row_count}/${sourceSet.capture_expected_row_count}；有Bar ${sourceSet.target_present_count} / 缺Bar ${sourceSet.target_missing_count} / 失败 ${sourceSet.read_failed_count}`,
    ],
    [
      "Raw核验级别",
      sourceSet.raw_revalidation?.proof_level || "未提供；拒绝推断深度已核验",
    ],
    [
      "价格 / 成交额 / 成交量单位",
      `${sourceSet.price_unit_status} / ${sourceSet.amount_unit_status} / ${sourceSet.volume_unit_status}`,
    ],
    ["封口摘要", sourceSet.seal_digest_sha256],
  ];
  for (const [label, value] of facts)
    readinessDetail(list, label, value == null ? "—" : String(value));
  root.append(list);
  const details = document.createElement("details");
  appendText(details, "summary", "查看提交回执与Raw独立核验");
  const evidence = document.createElement("dl");
  evidence.className = "formal-source-facts";
  for (const [label, value] of [
    ["PENDING创建事务 XID", ack.writer_transaction_id],
    ["PENDING独立观察事务 XID", ack.observer_transaction_id],
    ["PENDING行 xmin", ack.pending_row_xmin],
    ["PENDING创建事务全量 XID", ack.pending_creation_transaction_id],
    ["P02 capability SHA", p02.capability_digest],
    ["COMMIT ACK capability SHA", ack.capability_digest],
    ["候选 / Raw / 捕获核验", JSON.stringify(sourceSet.raw_revalidation || {})],
    [
      "身份与PIT限制",
      `${sourceSet.identity_status}；稳定证券身份尚未解析；当前仅后验NON_PIT来源事实。`,
    ],
  ])
    readinessDetail(evidence, label, value == null ? "—" : String(value));
  details.append(evidence);
  root.append(details);
}
function renderReadiness(data) {
  const v = data.source_version,
    c = v.bar_status_counts,
    source = data.source_observations,
    partial = source.status === "PARTIAL",
    versionLabel = `来源版本 ${v.version_status} · 来源观察 ${source.status}`,
    failureText =
      c.READ_FAILED > 0
        ? `；其中READ_FAILED ${c.READ_FAILED}，来源质量不完整`
        : "";
  $("chainState").className = `chain-state${partial ? " partial" : ""}`;
  const chainDetail = partial
    ? `来源观察自身存在缺口；SOURCE_INGRESS首断点 ${data.first_break.stage_id || "无"}。`
    : `正式Foundation仍未接通；SOURCE_INGRESS首断点 ${data.first_break.stage_id || "无"}。`;
  $("chainState").textContent = [
    `${v.source_date} · ${versionLabel} · Raw ${v.stored_row_count}/${v.capture_expected_row_count} · `,
    `有Bar ${c.TARGET_PRESENT} / 来源无Bar ${c.TARGET_MISSING} / 读取失败 ${c.READ_FAILED}${failureText}。`,
    `${chainDetail} 来源候选数 ≠ 官方有效上市证券数。`,
  ].join("");
  $("chainChecked").textContent = `只读核查 ${data.checked_at}`;
  renderSourceIngress(data);
  renderSourceSetCandidate(data);
  renderFormalSourceSet(data);
  const stats = [
    ["PG与来源命名空间", `stock_a.stock · ${v.namespace_id}`],
    ["当前最高可用范围", data.highest_available_scope],
    [
      "正式Foundation / 整日链",
      `${data.formal_foundation_status} / day_chain_complete=false`,
    ],
    [
      "来源观察完整性",
      `${source.status} · ${source.stored_rows}/${source.expected_rows} · ${source.status_reason}`,
    ],
    [
      "正式链首断点",
      data.first_break.stage_id
        ? `${data.first_break.stage_id} ${data.first_break.name_zh} · ${data.first_break.scope}`
        : `无 · ${data.first_break.status_reason}`,
    ],
    [
      "来源时钟与质量",
      `${v.source_clock_role} · ${v.source_time_quality}；捕获开始 ${v.capture_read_started_at}，校验 ${v.capture_validated_at}`,
    ],
    [
      "价格/量额质量与单位",
      `价格 ${v.price_qualities.join(", ") || "—"}；金额单位 ${v.amount_unit_statuses.join(", ") || "—"}；成交量单位 ${v.volume_unit_statuses.join(", ") || "—"}`,
    ],
    [
      "来源分母",
      `包内候选 ${v.capture_expected_row_count}；官方有效上市总体 UNKNOWN_NOT_CONNECTED`,
    ],
    [
      "合同版本与SHA",
      `${data.contract.contract_id} ${data.contract.version} · ${data.contract.sha256}`,
    ],
  ];
  const overview = $("chainOverview");
  overview.replaceChildren();
  for (const [label, value] of stats) {
    const card = document.createElement("div");
    card.className = "chain-stat";
    appendText(card, "span", label);
    appendText(card, "strong", value);
    overview.append(card);
  }
  const groupRoot = $("chainGroups");
  groupRoot.replaceChildren();
  const stageById = new Map(
    data.stages.map((stage) => [stage.stage_id, stage]),
  );
  for (const group of data.groups) {
    const details = document.createElement("details");
    details.className = "chain-group";
    const summary = document.createElement("summary");
    appendText(
      summary,
      "span",
      `${group.label} · ${group.stage_ids.length}个合同阶段`,
    );
    appendText(
      summary,
      "span",
      group.first_break_stage_id
        ? `首断点 ${group.first_break_stage_id}`
        : "查看状态",
    );
    details.append(summary);
    appendText(details, "p", group.summary);
    for (const stageId of group.stage_ids) {
      const stage = stageById.get(stageId);
      if (!stage) continue;
      const item = document.createElement("details");
      item.className = "chain-stage";
      const head = document.createElement("summary");
      appendText(head, "span", `${stage.stage_id} · ${stage.name_zh}`);
      const chips = document.createElement("span");
      chips.className = "chain-chip";
      chips.textContent =
        `${implementationLabels[stage.implementation_level] || stage.implementation_level} · ` +
        `${dataLabels[stage.data_status] || stage.data_status}`;
      head.append(chips);
      item.append(head);
      if (stage.is_first_break)
        appendText(
          item,
          "div",
          `首个断点：${stage.status_reason}`,
          "chain-first-break",
        );
      const dl = document.createElement("dl");
      dl.className = "chain-detail";
      readinessDetail(
        dl,
        "实现层级",
        `${implementationLabels[stage.implementation_level] || stage.implementation_level} (${stage.implementation_level})`,
      );
      readinessDetail(
        dl,
        "本日期数据状况",
        `${dataLabels[stage.data_status] || stage.data_status} (${stage.data_status})`,
      );
      readinessDetail(
        dl,
        "消费者接通",
        `${consumerLabels[stage.consumer_status] || stage.consumer_status} (${stage.consumer_status}) · ${stage.consumer}`,
      );
      readinessDetail(dl, "当前生产者", stage.producer);
      readinessDetail(dl, "所需输入", stage.required_inputs.join("、") || "无");
      readinessDetail(
        dl,
        "可选 / 前态输入",
        [
          ...stage.optional_inputs,
          ...stage.prior_inputs.map(
            (input) =>
              `${input.role} (${input.temporal_role || "PREVIOUS_ACCEPTED"})`,
          ),
        ].join("、") || "无",
      );
      readinessDetail(
        dl,
        "合同产物与下一消费者",
        stage.outputs
          .map((output) => {
            const edge = stage.output_consumers.find(
              (row) => row.output === output,
            );
            return edge
              ? `${output} → ${[...edge.consumers, ...edge.terminal_consumers].join(", ") || "无"}`
              : output;
          })
          .join("；") || "无",
      );
      readinessDetail(
        dl,
        "证据、日期与SHA",
        `${stage.evidence_type} · ${stage.source_date} · ${stage.package_sha256}`,
      );
      readinessDetail(dl, "当前说明", stage.status_reason);
      item.append(dl);
      details.append(item);
    }
    groupRoot.append(details);
  }
}
async function loadReadiness() {
  const version = selectedVersion(),
    requestId = ++state.readinessRequest;
  clearChainReadiness(
    version
      ? "正在按当前明确日期和完整SHA读取只读准备状态…"
      : "请先从PG目录选择目标日期和来源包版本。",
  );
  if (!version) return;
  const key = `${version.target_session}|${version.package_sha256}`;
  try {
    const data = await api(
      `/api/preview/chain-readiness?${new URLSearchParams(version)}`,
    );
    if (
      requestId !== state.readinessRequest ||
      key !==
        `${selectedVersion()?.target_session}|${selectedVersion()?.package_sha256}`
    )
      return;
    renderReadiness(data);
  } catch (error) {
    if (requestId !== state.readinessRequest) return;
    $("chainState").className = "chain-state error";
    $("chainState").textContent =
      `准备状态读取失败：${error.message}。当前版本未核实，不显示旧结果。`;
  }
}
