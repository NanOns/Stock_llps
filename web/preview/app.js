"use strict";
const $ = (id) => document.getElementById(id);
const state = {
  versions: [],
  version: null,
  offset: 0,
  limit: 20,
  total: 0,
  pageRequest: 0,
  readinessRequest: 0,
  stockRequest: 0,
  catalogRequest: 0,
  profileSnapshots: [],
  profileRequest: 0,
  profileOffset: 0,
  profileLimit: 100,
  profileTotal: 0,
  changeOffset: 0,
  changeLimit: 30,
  changeTotal: 0,
  changeRequest: 0,
  changeSnapshots: [],
  changeBinding: null,
  loading: false,
  timeline: null,
  timelineContext: null,
  timelineRequest: 0,
  timelineNavigation: false,
  rawPeriods: null,
  rawPeriodContext: null,
  rawPeriodRequest: 0,
  rawPeriodCache: new Map(),
  chartMode: "day",
  chartSelection: null,
  chartPeriodContext: null,
};
const labels = {
  TARGET_PRESENT: "有Bar",
  TARGET_MISSING: "来源无Bar",
  READ_FAILED: "读取失败",
  MAIN_BOARD: "主板",
  GEM: "创业板",
  OBSERVED: "可观察",
  UNKNOWN: "未知",
  UNKNOWN_DATA: "数据未知",
  NOT_IMPLEMENTED: "未实现",
  CONTRACT_UNBOUND: "合同未绑定",
  NOT_APPLICABLE: "不适用",
  NOT_EVALUATED: "未评价",
  UNIT_AUTHORITY_NOT_VERIFIED: "量额单位权威未核实",
  SHORT_HISTORY: "历史窗口不足",
  UNKNOWN_SESSION_GAP: "未知交易会话缺口",
  PG_HISTORY_SESSION_MISSING: "PG缺少所需历史会话",
  HISTORY_WINDOW_INCOMPLETE: "历史窗口不完整",
  EVALUATED: "已计算",
  TDX_TARGET_BAR_PRESENT: "目标日有Bar",
  TDX_TARGET_BAR_MISSING: "目标日缺Bar",
  TDX_READ_FAILED: "目标日读取失败",
  TARGET_SESSION_BAR_MISSING: "目标会话缺少Bar",
  RECORDS_COMPLETE: "PG窗口记录齐全",
  HISTORY_ROW_NOT_STORED: "历史Raw行未保存",
  SOURCE_SYMBOL_ONLY: "来源代码级，未核稳定证券身份",
  STABLE_SECURITY_ID_UNVERIFIED: "稳定security_id未核验",
  LISTING_STATUS_UNKNOWN: "上市状态未知",
  SUSPENSION_STATUS_UNKNOWN: "停牌状态未知",
  NON_PIT_RETROSPECTIVE: "NON_PIT回溯研究",
  UNKNOWN_NOT_CONNECTED: "官方上市总体未接通",
  RAW_UNADJUSTED: "原始未复权坐标",
  USABLE: "价格窗口可用",
  INSUFFICIENT_VALID_BARS: "有效Bar不足",
  PRICE_FIELDS_UNKNOWN: "价格字段未知",
  RESEARCH_PREVIEW_NON_PIT: "来源代码非PIT研究",
  CONSUMER_FAILED: "内核消费失败",
  COMPLETE: "完整闭合",
  PARTIAL: "部分覆盖",
  UNFINISHED: "周期未闭合",
  ABOVE: "高于MA20",
  BELOW: "低于MA20",
  EQUAL: "等于MA20",
  COMPARABLE: "可比较",
  FIRST_EVALUABLE: "首次可评价",
  NOT_COMPARABLE: "不可比较",
  CROSS_ABOVE: "下方跨至上方",
  CROSS_BELOW: "上方跨至下方",
  HOLD_ABOVE: "保持高于",
  HOLD_BELOW: "保持低于",
  HOLD_EQUAL: "保持相等",
  ENTER_EQUAL_FROM_ABOVE: "由上方进入相等",
  ENTER_EQUAL_FROM_BELOW: "由下方进入相等",
  LEAVE_EQUAL_TO_ABOVE: "由相等转为上方",
  LEAVE_EQUAL_TO_BELOW: "由相等转为下方",
  HISTORY_ROW_NOT_STORED: "PG未保存Raw行",
};
const profileLabels = {
  "profile.trend": "趋势",
  "profile.position": "价格位置",
  "profile.ma_structure": "均线结构",
  "profile.relative": "相对强弱",
  "profile.compression": "压缩状态",
  "profile.participation": "成交参与",
  "profile.extension": "延伸程度",
  "profile.near_high20": "接近20会话高点",
  "profile.drawdown60": "60会话回撤",
  "profile.closed_week_trend": "闭合周趋势",
  "profile.closed_month_trend": "闭合月趋势",
};
const text = (v) => {
  if (v === null || v === undefined) return "—";
  if (typeof v === "object") {
    if (v.exact !== undefined) return `${v.exact} (${v.decimal ?? ""})`;
    return JSON.stringify(v);
  }
  return labels[String(v)] || String(v);
};
const cell = (row, value) => {
  const td = document.createElement("td");
  td.textContent = text(value);
  row.append(td);
  return td;
};
function tableRow(parent, values) {
  const tr = document.createElement("tr");
  values.forEach((v) => cell(tr, v));
  parent.append(tr);
  return tr;
}
function current() {
  return state.version;
}
function setMessage(message, ok = false) {
  const el = $("stockError");
  el.hidden = !message;
  el.textContent = message || "";
  el.classList.toggle("success", ok);
}
async function api(url, options) {
  const response = await fetch(url, { cache: "no-store", ...options });
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error("服务器返回了无法读取的响应。");
  }
  if (!response.ok || payload.ok === false)
    throw new Error(
      `${payload.code || "请求失败"}：${payload.message || "请检查输入和PG目录。"}`,
    );
  return payload;
}
function selectedVersion() {
  const option = $("versionSelect").selectedOptions[0];
  if (!option || !option.value.includes("|")) return null;
  const [target_session, package_sha256] = option.value.split("|");
  return { target_session, package_sha256 };
}
function clearResult() {
  $("stockResult").hidden = true;
  $("stockResult").classList.remove("timeline-only");
  $("coreRows").replaceChildren();
  $("allCoreRows").replaceChildren();
  $("profileRows").replaceChildren();
  $("rawSummary").replaceChildren();
  setMessage("");
}
const implementationLabels = {
  DESIGN_ONLY: "仅有机器合同设计",
  PURE_KERNEL: "纯计算内核可用，正式生产者未接通",
  RUNNABLE_P00_PLAN: "P00计划已接通真实来源与合同",
  RUNNABLE_CAPABILITY_ACCOUNTING: "来源能力核算已接通",
  PG_CANDIDATE: "PG NON_PIT来源候选",
  PG_SOURCE_FACT: "PG来源事实已保存（来源代码级观察）",
  ON_DEMAND_PREVIEW: "用户触发的开发研究预览",
  FORMAL_PERSISTED: "正式PG持久结果",
  PUBLISHED: "正式发布",
};
const dataLabels = {
  AVAILABLE: "已发现可用记录",
  PARTIAL: "部分记录或存在读取失败",
  MISSING: "所需记录缺失",
  NOT_CHECKED: "本请求未检查",
  BLOCKED: "计划被环境门阻止",
  UNKNOWN: "能力未知",
  NOT_REQUESTED: "本次未请求",
  AVAILABLE_CANDIDATE: "已保存候选，非正式封口",
  SOURCE_RAW_ONLY: "仅来源Raw观察，不是正式P05",
  NOT_APPLICABLE: "不适用于当前阶段",
};
const consumerLabels = {
  CONNECTED_PREVIEW: "已接入开发预览",
  CONNECTED_FORMAL: "已接入正式消费",
  BLOCKED_ENVIRONMENT: "当前环境门阻止阶段调度",
  NOT_CONNECTED: "正式下游未接通",
  NOT_APPLICABLE: "不适用",
};
const appendText = (parent, tag, value, className) => {
  const node = document.createElement(tag);
  if (className) node.className = className;
  node.textContent = value;
  parent.append(node);
  return node;
};
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
async function loadCatalog() {
  invalidateTimeline();
  const catalogRequest = ++state.catalogRequest;
  state.pageRequest++;
  state.readinessRequest++;
  state.stockRequest++;
  clearResult();
  clearChainReadiness("正在从PostgreSQL只读读取来源日期与版本目录…");
  $("versionSelect").disabled = true;
  $("versionSelect").replaceChildren(new Option("正在读取PG目录…", ""));
  $("population").textContent = "正在从PostgreSQL只读读取来源日期与版本目录。";
  try {
    const data = await api("/api/preview/catalog?offset=0&limit=100");
    if (catalogRequest !== state.catalogRequest) return;
    state.versions = data.versions;
    $("versionSelect").replaceChildren(
      new Option("— 请选择目标日期与来源包版本 —", ""),
    );
    for (const v of data.versions) {
      const title = [
        `${v.session_date} · ${v.package_sha256.slice(0, 12)}… · ${v.version_status} · `,
        `Raw ${v.row_count}/${v.expected_row_count}（有Bar `,
        `${v.bar_status_counts.TARGET_PRESENT || 0} / 缺Bar ${v.bar_status_counts.TARGET_MISSING || 0}）`,
      ].join("");
      const option = new Option(title, `${v.session_date}|${v.package_sha256}`);
      $("versionSelect").append(option);
    }
    $("versionSelect").disabled = state.versions.length === 0;
    state.version = null;
    state.offset = 0;
    $("candidateRows").replaceChildren();
    $("pageCount").textContent = "—";
    $("pageStatus").textContent = state.versions.length
      ? "请选择PG日期和版本"
      : "PG目录为空";
    $("prevPage").disabled = true;
    $("nextPage").disabled = true;
    $("lastPage").disabled = true;
    $("population").textContent = state.versions.length
      ? `已读取${data.total_version_count}个真实PG日期/版本。请选择明确版本；Raw来源候选不代表官方上市总体。`
      : "PG中没有可选的Raw来源版本。";
    clearChainReadiness(
      state.versions.length
        ? "请选择一个精确日期和完整包SHA。"
        : "PG目录没有可选来源版本。",
    );
  } catch (error) {
    if (catalogRequest !== state.catalogRequest) return;
    $("population").textContent = `目录读取失败：${error.message}`;
    $("candidateRows").replaceChildren();
    tableRow($("candidateRows"), ["PG目录不可用", "", "", "", ""]);
    $("versionSelect").disabled = true;
    clearChainReadiness(`PG目录不可用：${error.message}`);
  }
}
function describePopulation(page) {
  const version = page.source_version;
  const versionText =
    version.status === "CLOSED"
      ? `来源包Raw记录闭合：${version.stored_raw_rows}/${version.capture_expected_rows}`
      : `来源包Raw部分入库：${version.stored_raw_rows}/${version.capture_expected_rows}；不得按全市场完成解释`;
  return `${page.target_session} · 包SHA ${page.package_sha256} · ${versionText}。当前筛选来源候选=${page.filtered_total}；有效上市总体=UNKNOWN（NOT_CONNECTED）。这只是该来源包的候选分母。`;
}
async function loadPage() {
  const v = selectedVersion();
  if (!v) {
    state.version = null;
    state.total = 0;
    state.pageRequest++;
    state.stockRequest++;
    clearResult();
    $("candidateRows").replaceChildren();
    $("pageCount").textContent = "—";
    $("pageStatus").textContent = "请选择PG日期和版本";
    $("population").textContent = "等待选择PG来源版本。";
    $("prevPage").disabled = true;
    $("nextPage").disabled = true;
    $("lastPage").disabled = true;
    return;
  }
  const requestId = ++state.pageRequest;
  state.stockRequest++;
  const key = `${v.target_session}|${v.package_sha256}`;
  state.version = v;
  clearResult();
  $("candidateRows").replaceChildren();
  $("pageStatus").textContent = "正在读取PG分页…";
  const query = new URLSearchParams({
    target_session: v.target_session,
    package_sha256: v.package_sha256,
    offset: String(state.offset),
    limit: String(state.limit),
    market: $("marketSelect").value,
    board: $("boardSelect").value,
  });
  try {
    const data = await api(`/api/preview/source-page?${query}`);
    if (
      requestId !== state.pageRequest ||
      key !==
        `${selectedVersion()?.target_session}|${selectedVersion()?.package_sha256}`
    )
      return;
    const p = data.page;
    state.total = p.filtered_total;
    $("population").textContent = describePopulation(p);
    $("pageCount").textContent =
      `本页 ${p.page_count} / 筛选分母 ${p.filtered_total}`;
    $("pageStatus").textContent =
      `偏移 ${p.offset}，后续 ${p.next_offset === null ? 0 : p.filtered_total - p.next_offset}`;
    $("prevPage").disabled = p.offset === 0;
    $("nextPage").disabled = p.next_offset === null;
    $("candidateRows").replaceChildren();
    if (!p.results.length) {
      tableRow($("candidateRows"), ["当前筛选没有来源记录", "", "", "", ""]);
      return;
    }
    for (const item of p.results) {
      const tr = document.createElement("tr");
      cell(tr, item.source_symbol);
      cell(tr, `${item.market} / ${text(item.board)}`);
      cell(tr, text(item.bar_status));
      cell(
        tr,
        `${item.source_object_status || "TDX_SOURCE_SYMBOL_OBSERVED"} · ${item.stable_security_id_status || "STABLE_SECURITY_ID_UNVERIFIED"}`,
      );
      const td = document.createElement("td");
      const button = document.createElement("button");
      button.className = "row-select";
      button.type = "button";
      button.textContent = "查看";
      button.addEventListener("click", () => {
        $("symbolInput").value = item.source_symbol;
        $("stockForm").requestSubmit();
      });
      td.append(button);
      tr.append(td);
      $("candidateRows").append(tr);
    }
  } catch (error) {
    if (requestId !== state.pageRequest) return;
    $("population").textContent = `候选页读取失败：${error.message}`;
    $("pageStatus").textContent = "分页不可用";
    $("prevPage").disabled = true;
    $("nextPage").disabled = true;
    tableRow($("candidateRows"), ["PG分页失败", "", "", "", ""]);
  }
}
labels["TARGET_SESSION_BAR_MISSING"] = "目标日来源记录无Bar，不推断停牌或退市";
labels["CURRENT_SESSION_UNKNOWN"] = "目标日状态未知";
function rawItems(raw, formalSourceSet) {
  const pairs = [
    ["目标日Bar", `${text(raw.bar_status)} · ${text(raw.bar_reason)}`],
    [
      "原始OHLC",
      Object.entries(raw.values)
        .filter(([key]) => ["open", "high", "low", "close"].includes(key))
        .map(([key, value]) => `${key}=${text(value)}`)
        .join(" · "),
    ],
    [
      "原始金额",
      `${text(raw.values.amount)} · 字段质量 ${text(raw.quality.amount_quality)} · ` +
        `原值质量 ${text(raw.quality.raw_amount_quality)} · ${text(raw.quality.amount_unit_status)}`,
    ],
    [
      "原始数量",
      `${text(raw.values.volume)} · 字段质量 ${text(raw.quality.volume_quality)} · ` +
        `原值质量 ${text(raw.quality.raw_volume_quality)} · ${text(raw.quality.volume_unit_status)}`,
    ],
    [
      "价格质量",
      `${text(raw.quality.price_quality)} · ${text(raw.quality.price_reason)}`,
    ],
    ["来源包SHA", text(raw.source.package_sha256)],
    [
      "P03 SEALED / P04来源集合",
      `${text(formalSourceSet?.seal_status)} · ${text(formalSourceSet?.source_set_id)} · revision ${text(formalSourceSet?.revision_id)}`,
    ],
    [
      "来源对象 / 目标日Bar",
      `${text(raw.source_object_status)} · ${text(raw.target_bar_status)}`,
    ],
    [
      "来源时钟",
      `${text(raw.source.source_clock_role)} · ${text(raw.source.source_time_quality)}`,
    ],
    [
      "本机观察时钟",
      `${text(raw.source.observed_started_at)} → ${text(raw.source.observed_validated_at)}`,
    ],
    ["来源条目SHA", text(raw.source.source_entry_sha256)],
    [
      "身份与坐标",
      `${raw.identity_status} · ${raw.price_coordinate} · ${raw.pit_status}`,
    ],
    [
      "身份/上市/停复牌状态",
      `${text(raw.stable_security_id_status)} · ${text(raw.listing_status)} · ${text(raw.suspension_status)}`,
    ],
  ];
  const root = $("rawSummary");
  root.replaceChildren();
  for (const [label, value] of pairs) {
    const item = document.createElement("div");
    item.className = "raw-item";
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = value;
    item.append(span, strong);
    root.append(item);
  }
}
function renderFields(parent, fields, select) {
  parent.replaceChildren();
  for (const name of select) {
    const field = fields[name] || {};
    tableRow(parent, [name, field.value, field.quality, field.reason]);
  }
}
function formatOhlc(ohlc) {
  if (!ohlc || ohlc.open == null) return "未闭合，不展示部分OHLC";
  return `O ${text(ohlc.open)} · H ${text(ohlc.high)} · L ${text(ohlc.low)} · C ${text(ohlc.close)}`;
}
function renderRawPeriodRows(parent, periods) {
  parent.replaceChildren();
  for (const period of periods || []) {
    const row = tableRow(parent, [
      `${period.period_key} · ${period.calendar_start_session}—${period.calendar_end_session}`,
      `${period.calendar_closed ? "日历已闭合" : "未闭合"} · ${text(period.coverage_status)}${period.reason_codes?.length ? ` · ${period.reason_codes.join(" / ")}` : ""}`,
      formatOhlc(period.ohlc),
      `${period.valid_price_bar_count}/${period.calendar_expected_session_count} · 窗口内 ${period.elapsed_expected_session_count}`,
      `${period.target_missing_count} / ${period.read_failed_count} / ${period.missing_history_count}`,
      "",
    ]);
    const detailCell = row.lastElementChild;
    if (!period.calendar_closed || !period.daily_rows?.length) {
      detailCell.textContent = "无闭合周期明细";
      continue;
    }
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = `展开 ${period.daily_rows.length} 个会话的来源与质量`;
    const wrap = document.createElement("div");
    wrap.className = "table-wrap period-detail-wrap";
    const table = document.createElement("table");
    table.className = "period-detail-table";
    const head = document.createElement("thead");
    tableRow(head, [
      "交易会话",
      "Bar状态",
      "价格质量 / 原因",
      "原始OHLC",
      "包SHA-256",
      "捕获 / 内容 / 条目SHA",
    ]);
    const body = document.createElement("tbody");
    for (const daily of period.daily_rows) {
      const dailyOhlc = daily.valid_price_bar
        ? `O ${text(daily.open)} · H ${text(daily.high)} · L ${text(daily.low)} · C ${text(daily.close)}`
        : "无有效OHLC";
      tableRow(body, [
        daily.session,
        `${text(daily.bar_status)} · ${text(daily.bar_reason)}`,
        `${text(daily.price_quality)} · ${text(daily.price_reason)}`,
        dailyOhlc,
        daily.package_sha256,
        `${text(daily.source_capture_id)} · ${text(daily.source_content_id)} · ${text(daily.source_entry_sha256)}`,
      ]);
    }
    table.append(head, body);
    wrap.append(table);
    details.append(summary, wrap);
    detailCell.append(details);
  }
  if (!periods?.length) {
    tableRow(parent, [
      "无周期",
      "官方日历或Raw历史窗口没有可展示周期。",
      "",
      "",
      "",
      "",
    ]);
  }
}
function rawPeriodBinding(data) {
  return {
    target_session: data.target_session,
    package_sha256: data.package_sha256,
    symbol: data.symbol || data.source_symbol,
    namespace_id: data.namespace_id,
    source_set_id: data.snapshot_identity?.source_set_id ?? null,
  };
}
function rawPeriodCacheKey(binding) {
  return [
    binding.target_session,
    binding.package_sha256,
    binding.symbol,
    binding.namespace_id,
    binding.source_set_id || "NONE",
  ].join("|");
}
function rawPeriodsMatch(binding) {
  if (!state.rawPeriods || !state.rawPeriodContext) return false;
  return (
    rawPeriodCacheKey(state.rawPeriodContext) === rawPeriodCacheKey(binding)
  );
}
function clearRawPeriodState(
  message = "等待精确日期、代码及来源版本的周期查询。",
) {
  state.rawPeriodRequest++;
  state.rawPeriods = null;
  state.rawPeriodContext = null;
  renderRawPeriods(null, message);
}
async function loadRawPeriods(
  version,
  symbol,
  expectedIdentity,
  isCurrent = () => true,
) {
  const binding = {
    target_session: version.target_session,
    package_sha256: version.package_sha256,
    symbol,
    namespace_id: expectedIdentity?.namespace_id || null,
    source_set_id: expectedIdentity?.source_set_id || null,
  };
  if (!binding.namespace_id)
    throw new Error("周期查询缺少已核验的namespace身份。");
  if (!isCurrent()) return null;
  const requestId = ++state.rawPeriodRequest;
  const cacheKey = rawPeriodCacheKey(binding);
  const cached = state.rawPeriodCache.get(cacheKey);
  if (cached) {
    state.rawPeriods = cached;
    state.rawPeriodContext = rawPeriodBinding(cached);
    renderRawPeriods(cached);
    return cached;
  }
  state.rawPeriods = null;
  state.rawPeriodContext = null;
  renderRawPeriods(null, "正在读取精确来源身份下的自然周/月PG Raw…");
  const query = new URLSearchParams({
    target_session: binding.target_session,
    package_sha256: binding.package_sha256,
    symbol: binding.symbol,
    namespace_id: binding.namespace_id,
    source_set_id: binding.source_set_id || "NONE",
  });
  const data = await api(`/api/source-symbol/raw-periods?${query}`);
  if (requestId !== state.rawPeriodRequest || !isCurrent()) return null;
  const actual = rawPeriodBinding(data);
  if (rawPeriodCacheKey(actual) !== cacheKey) {
    throw new Error(
      "周期响应日期、代码、namespace、source_set或完整包SHA不匹配，已拒绝展示。",
    );
  }
  state.rawPeriods = data;
  state.rawPeriodContext = actual;
  state.rawPeriodCache.set(cacheKey, data);
  while (state.rawPeriodCache.size > 6) {
    state.rawPeriodCache.delete(state.rawPeriodCache.keys().next().value);
  }
  renderRawPeriods(data);
  return data;
}
function renderRawPeriods(data, errorMessage = null) {
  state.rawPeriods = data;
  state.rawPeriodContext = data ? rawPeriodBinding(data) : null;
  const status = $("rawPeriodStatus");
  const identity = $("rawPeriodIdentity");
  const trends = $("rawPeriodTrends");
  if (!data) {
    status.textContent = `来源代码级周期观察未返回：${errorMessage || "未知错误"}。每日Core结果仍独立可用。`;
    identity.textContent = "";
    trends.replaceChildren();
    renderRawPeriodRows($("rawWeekRows"), []);
    renderRawPeriodRows($("rawMonthRows"), []);
    renderCandlestickChart(state.timeline);
    return;
  }
  const window = data.history_window;
  status.textContent =
    `来源代码级 ${data.research_mode} · ${data.price_coordinate} · ${data.pit_status}；` +
    `窗口 ${window.start_session} 至 ${window.end_session}，官方会话 ${window.official_session_count}，` +
    `PG Raw 行 ${window.stored_raw_row_count}，有效价格Bar ${window.valid_price_bar_count}，` +
    `缺Bar ${window.target_missing_count}，读取失败 ${window.read_failed_count}，` +
    `未保存会话 ${window.missing_history_sessions.length}。`;
  identity.textContent =
    `${data.source_symbol} · 目标日 ${data.target_session} · 包SHA ${data.package_sha256} · ` +
    `日历 ${data.calendar_revision} · 复权/公司行为 ${data.adjustment_status}/${data.corporate_action_status} · ` +
    `正式闭周/月 ${data.formal_period_profile.closed_week}/${data.formal_period_profile.closed_month}：${data.formal_period_profile.reason}`;
  trends.replaceChildren();
  for (const kind of ["week", "month"]) {
    const item = data.trends[kind];
    const card = document.createElement("div");
    card.className = "period-trend-card";
    appendText(
      card,
      "strong",
      kind === "week" ? "闭合周趋势观察" : "闭合月趋势观察",
    );
    appendText(
      card,
      "span",
      item.status === "OBSERVED"
        ? `${item.value} · ${item.input_period_keys.join(" → ")} · ${item.rule_id}`
        : `UNKNOWN · ${item.complete_period_count}/${item.required_period_count} 个连续完整闭合周期 · ${item.reason}`,
    );
    appendText(
      card,
      "small",
      "仅为NON_PIT_RAW_DIAGNOSTIC；不代表正式Profile或交易信号。",
    );
    trends.append(card);
  }
  renderRawPeriodRows($("rawWeekRows"), data.periods.week);
  renderRawPeriodRows($("rawMonthRows"), data.periods.month);
  renderCandlestickChart(state.timeline);
}
const coreNames = [
  "core.ma20",
  "core.ma60",
  "core.atr20",
  "core.ret20",
  "core.pos60",
];
const loadPageOriginal = loadPage;
loadPage = async function () {
  await loadPageOriginal();
  $("lastPage").disabled =
    state.total === 0 || state.offset + state.limit >= state.total;
};
$("lastPage").addEventListener("click", () => {
  if (state.total) {
    state.offset = Math.floor((state.total - 1) / state.limit) * state.limit;
    loadPage();
  }
});
async function queryStock(event) {
  event.preventDefault();
  const requestId = ++state.stockRequest;
  const keepTimeline = state.timelineNavigation;
  const previousTimelineContext = keepTimeline ? state.timelineContext : null;
  state.timelineNavigation = false;
  if (!keepTimeline) {
    state.timeline = null;
    state.timelineContext = null;
    state.chartPeriodContext = null;
    state.chartSelection = null;
    state.timelineRequest++;
    renderTimeline(null);
  }
  clearRawPeriodState(
    "正在读取所选日期、来源代码、namespace和source_set的自然周/月…",
  );
  const v = selectedVersion();
  if (!v) {
    setMessage("请先从真实PG目录选择目标日期和来源包版本。");
    return;
  }
  const symbol = $("symbolInput").value.trim();
  const count = Number($("windowInput").value);
  const key = `${v.target_session}|${v.package_sha256}|${symbol}|${count}`;
  const requestIsCurrent = () => {
    const selected = selectedVersion();
    return (
      requestId === state.stockRequest &&
      key ===
        `${selected?.target_session}|${selected?.package_sha256}|${$("symbolInput").value.trim()}|${Number($("windowInput").value)}`
    );
  };
  if (!symbol || !Number.isInteger(count) || count < 1 || count > 75) {
    clearResult();
    setMessage("来源代码与会话数无效；会话数须为1至75。");
    return;
  }
  clearResult();
  $("queryStock").disabled = true;
  setMessage("正在按显式日期、包SHA和窗口只读读取PG并计算现役Core/Profile…");
  try {
    const raw = await api(
      `/api/preview/raw?${new URLSearchParams({ ...v, symbol })}`,
    );
    if (!requestIsCurrent()) return;
    const result = await api("/api/preview/stock", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        symbol,
        target_session: v.target_session,
        package_sha256: v.package_sha256,
        session_count: count,
      }),
    });
    if (!requestIsCurrent()) return;
    const a = raw.snapshot_identity,
      b = result.snapshot_identity;
    if (
      a.namespace_id !== b.namespace_id ||
      a.target_session !== b.target_session ||
      a.package_sha256 !== b.package_sha256 ||
      a.source_set_id !== b.source_set_id ||
      b.target_session !== v.target_session ||
      b.package_sha256 !== v.package_sha256
    )
      throw new Error("RAW与Core/Profile预览身份不一致，已拒绝合并展示。");
    if (
      keepTimeline &&
      state.chartPeriodContext?.target_session === v.target_session &&
      state.chartPeriodContext?.package_sha256 === v.package_sha256 &&
      state.chartPeriodContext?.symbol === symbol
    )
      state.chartPeriodContext = {
        ...state.chartPeriodContext,
        namespace_id: b.namespace_id,
        source_set_id: b.source_set_id || null,
      };
    $("stockResult").hidden = false;
    setMessage("研究预览已返回；数值为RAW_UNADJUSTED / NON_PIT。", true);
    $("resultTitle").textContent = `${symbol} · ${v.target_session}`;
    $("resultMode").textContent =
      `${result.status} · ${result.source_object_status} · ${result.target_bar_status} · ${result.price_coordinate} · ${result.identity_status} · 正式准入 ${result.formal_foundation_status}`;
    $("snapshotId").textContent =
      `预览身份SHA256：${result.snapshot_identity.identity_sha256}（非正式context token） · 来源包SHA：${v.package_sha256}`;
    $("windowText").textContent =
      `窗口 ${result.window.start_session} 至 ${result.window.end_session}：` +
      `实际Bar ${result.window.real_bar_count}/${result.window.requested_sessions}，` +
      `TDX缺Bar/读取失败 ${result.window.source_bar_gap_count}，` +
      `PG未保存历史会话 ${result.window.unavailable_history_row_count}；` +
      `Raw行覆盖 ${result.window.history_records_status}；` +
      `MA20/ATR20价格窗口 ${result.window.price_window_status} ` +
      `(${result.window.ma20_window_valid_bar_count}/${result.window.ma20_window_expected_bar_count}; ` +
      `${result.window.price_window_valid_bar_count}/${result.window.price_window_expected_bar_count})，` +
      `未使用未来会话。缺Bar不证明停牌、退市或上市前。`;
    $("resultMode").dataset.namespace = result.snapshot_identity.namespace_id;
    rawItems(raw.raw, raw.formal_source_set);
    renderFields($("coreRows"), result.core.fields, coreNames);
    renderFields(
      $("allCoreRows"),
      result.core.fields,
      Object.keys(result.core.fields).sort(),
    );
    $("profileRows").replaceChildren();
    for (const component of result.profile_preview.components) {
      const componentKey = component.field_id || "";
      tableRow($("profileRows"), [
        `${profileLabels[componentKey] || componentKey} · ${componentKey}`,
        component.status === "EVALUATED" ? component.quality : component.status,
        component.value,
        component.reason,
      ]);
    }
    try {
      await loadRawPeriods(v, symbol, b, requestIsCurrent);
    } catch (error) {
      if (!requestIsCurrent()) return;
      renderRawPeriods(null, error.message);
    }
    if (state.timeline) renderTimeline(state.timeline);
    if (
      keepTimeline &&
      state.chartPeriodContext?.target_session === v.target_session &&
      state.chartPeriodContext?.package_sha256 === v.package_sha256 &&
      state.chartPeriodContext?.symbol === symbol
    ) {
      await queryTimeline({
        namespace_id: b.namespace_id,
        source_set_id: b.source_set_id || null,
        origin: previousTimelineContext?.origin || "manual",
        returnSelection: previousTimelineContext?.returnSelection || null,
        persistedProfileSnapshot:
          previousTimelineContext?.persistedProfileSnapshot || null,
      });
    }
  } catch (error) {
    if (!requestIsCurrent()) return;
    $("stockResult").hidden = Boolean(!keepTimeline || !state.timeline);
    setMessage(error.message);
    if (keepTimeline && state.timeline) renderTimeline(state.timeline);
  } finally {
    if (requestIsCurrent()) $("queryStock").disabled = false;
  }
}
function selectedProfileSnapshot() {
  const key = $("profileSnapshotSelect").value;
  return state.profileSnapshots.find(
    (item) =>
      `${item.target_session}|${item.source_set_id}|${item.profile_revision}` ===
      key,
  );
}
function timelineField(field) {
  if (!field) return "—";
  if (field.quality !== "OBSERVED")
    return `UNKNOWN · ${text(field.reason || field.quality)}`;
  const value = field.value;
  if (value && typeof value === "object")
    return text(value.decimal ?? value.exact ?? value);
  return text(value);
}
function priorHighRelationLabel(relation) {
  const labels = {
    ABOVE: "高于前20会话最高价",
    BELOW: "低于前20会话最高价",
    EQUAL: "等于前20会话最高价",
  };
  return labels[relation] || "UNKNOWN";
}
const priceProfileFieldIds = new Set([
  "profile.trend",
  "profile.position",
  "profile.ma_structure",
  "profile.compression",
  "profile.extension",
  "profile.near_high20",
  "profile.drawdown60",
  "profile.closed_week_trend",
  "profile.closed_month_trend",
]);
const timelinePriceFieldLabels = {
  "core.ma5": "MA5",
  "core.ma10": "MA10",
  "core.ma20": "MA20",
  "core.ma60": "MA60",
  "core.atr20": "ATR20",
  "core.prior_high20": "严格前20会话最高价",
};
const priceRelationKindLabels = {
  STRICT_CROSS_UP: "严格上穿",
  STRICT_CROSS_DOWN: "严格下破",
  EQUAL_BOUNDARY: "等值边界变化",
  RELATIVE_PRIOR_HIGH_POSITION_CHANGED: "相对各自滚动前高的位置变化",
  NOT_COMPARABLE: "UNKNOWN / 不可比",
};
const priceRelationLabels = {
  BELOW: "低于",
  EQUAL: "等于",
  ABOVE: "高于",
};
function closeRelationLabel(relation, reason) {
  if (relation === "ABOVE") return "收盘高于均线";
  if (relation === "BELOW") return "收盘低于均线";
  if (relation === "EQUAL") return "收盘等于均线";
  return `UNKNOWN · ${text(reason || "PRICE_RELATION_UNKNOWN")}`;
}
function formatMovingAverageOrder(order) {
  if (order?.quality !== "OBSERVED")
    return `UNKNOWN · ${text(order?.reason || "均线字段未全部可观测")}`;
  return (order.groups || [])
    .map((group) =>
      group
        .map((fieldId) => timelinePriceFieldLabels[fieldId] || fieldId)
        .join(" = "),
    )
    .join(" > ");
}
function renderPersistedProfileObservation(point) {
  const status = $("priceStructureProfileStatus");
  const rows = $("priceStructureProfileRows");
  rows.replaceChildren();
  const snapshot = state.timelineContext?.persistedProfileSnapshot;
  const selectedSourceSet =
    state.timeline?.selection?.source_set_id ||
    state.timeline?.formal_source_set?.source_set_id ||
    null;
  const exactMatch = Boolean(
    snapshot?.profile_revision &&
      snapshot.target_session === point.session_date &&
      snapshot.package_sha256 === point.package_sha256 &&
      snapshot.namespace_id === point.namespace_id &&
      snapshot.source_set_id === selectedSourceSet &&
      snapshot.field_ids?.length,
  );
  if (!exactMatch) {
    status.textContent =
      "本次轨迹未附带与所选日期、source_set、完整包SHA及profile_revision一致的Profile11快照；此处仅展示RAW Core事实，不重新评价持久画像或选股状态。";
    tableRow(rows, [
      "未绑定精确Profile11",
      "NOT_EVALUATED",
      "UNKNOWN",
      "本次轨迹未提供精确Profile11快照；不据此推断PG中不存在其它持久画像。",
    ]);
    return;
  }
  const comparable = snapshot.semantic_compatible === true;
  status.textContent =
    `已绑定PG持久化快照 · ${snapshot.target_session} · source_set ${snapshot.source_set_id} · 包SHA ${snapshot.package_sha256} · Profile修订 ${snapshot.profile_revision}。` +
    (comparable
      ? " 此快照与两日变化合同兼容。"
      : " 仅展示本快照原值；两期语义不兼容，不能作变化比较。") +
    " 选股资格仍为未评价。";
  let shown = 0;
  snapshot.field_ids.forEach((fieldId, index) => {
    if (!priceProfileFieldIds.has(fieldId)) return;
    tableRow(rows, [
      `${profileLabels[fieldId] || fieldId} · ${fieldId}`,
      snapshot.statuses?.[index] ?? "UNKNOWN",
      snapshot.qualities?.[index] ?? "UNKNOWN",
      snapshot.reasons?.[index] ?? "未提供原因",
    ]);
    shown++;
  });
  if (!shown)
    tableRow(rows, [
      "无价格相关分项",
      "NOT_EVALUATED",
      "UNKNOWN",
      "精确快照没有返回可展示的价格相关Profile11字段。",
    ]);
}
function renderPriceStructureObservation(point) {
  const panel = $("priceStructurePanel");
  const summary = $("priceStructureSummary");
  const rows = $("priceStructureRows");
  if (!panel || !summary || !rows) return;
  summary.replaceChildren();
  rows.replaceChildren();
  if (!point) {
    $("priceStructureIdentity").textContent = "";
    $("priceStructureProfileStatus").textContent = "";
    $("priceStructureProfileRows").replaceChildren();
    panel.hidden = true;
    return;
  }
  panel.hidden = false;
  const sourceSetId =
    state.timeline?.formal_source_set?.source_set_id ||
    state.timeline?.selection?.source_set_id ||
    "UNKNOWN";
  $("priceStructureIdentity").textContent =
    `${point.session_date} · ${state.timeline?.symbol || "—"} · RAW_UNADJUSTED / ${state.timeline?.pit_status || "NON_PIT_RETROSPECTIVE"} · namespace ${point.namespace_id || "UNKNOWN"} · 轨迹截止日source_set ${sourceSetId} · 包SHA ${point.package_sha256 || "UNKNOWN"}`;
  const ohlc = dayOhlc(point);
  const summaries = [
    [
      "RAW OHLC与收盘",
      ohlc
        ? `O ${ohlc.open.exact} · H ${ohlc.high.exact} · L ${ohlc.low.exact} · C ${ohlc.close.exact}`
        : `无合法OHLC · ${text(point.raw_ohlc?.reason || point.price_reason || point.raw_status)}`,
    ],
    [
      "MA5 / 10 / 20 / 60排列",
      formatMovingAverageOrder(point.moving_average_order),
    ],
    ["观察边界", "来源代码RAW事实 · NON_PIT · 选股NOT_EVALUATED"],
  ];
  for (const [label, value] of summaries) {
    const card = document.createElement("div");
    card.className = "price-structure-stat";
    appendText(card, "span", label);
    appendText(card, "strong", value);
    summary.append(card);
  }
  const fieldRows = [
    ...["core.ma5", "core.ma10", "core.ma20", "core.ma60"].map((fieldId) => ({
      fieldId,
      relation: point.close_vs_ma?.[fieldId],
      window: point.moving_average_windows?.[fieldId],
    })),
    { fieldId: "core.atr20", window: point.atr20_window },
    { fieldId: "core.prior_high20", window: point.prior_high20_window },
  ];
  for (const item of fieldRows) {
    const fieldId = item.fieldId;
    const field =
      point.core_price_fields?.[fieldId] ||
      {
        "core.ma5": point.core_ma5,
        "core.ma10": point.core_ma10,
        "core.ma20": point.core_ma20,
        "core.ma60": point.core_ma60,
        "core.atr20": point.core_atr20,
        "core.prior_high20": point.core_prior_high20,
      }[fieldId];
    const relation = item.relation
      ? closeRelationLabel(item.relation.relation, item.relation.reason)
      : fieldId === "core.prior_high20"
        ? point.close_vs_prior_high20
          ? priorHighRelationLabel(point.close_vs_prior_high20)
          : `UNKNOWN · ${text(point.prior_high20_relation_unknown_reason)}`
        : fieldId.startsWith("core.ma")
          ? closeRelationLabel(null, field?.reason || "PRICE_RELATION_UNKNOWN")
          : "不适用：显示Core字段质量和原因";
    const window = item.window;
    const windowText = window
      ? `${window.valid_bar_count}/${window.required_valid_bar_count}个有效Bar；${window.official_session_count}个官方会话`
      : "窗口统计未提供";
    tableRow(rows, [
      fieldId === "core.prior_high20"
        ? `${timelinePriceFieldLabels[fieldId]}（排除本日）`
        : timelinePriceFieldLabels[fieldId],
      `${timelineField(field)} · ${text(field?.quality || "UNKNOWN")}`,
      relation,
      windowText,
    ]);
  }
  renderPersistedProfileObservation(point);
}
function priceRelationEvidenceText(evidence) {
  if (!evidence) return "无字段证据";
  const rendered = timelineField(evidence);
  return evidence.reason && evidence.quality !== "OBSERVED"
    ? `${rendered} · 原因 ${text(evidence.reason)}`
    : `${rendered} · ${text(evidence.quality || "UNKNOWN")}`;
}

function priceRelationSideText(observation, side) {
  const isBefore = side === "before";
  const close = observation[`${side}_close`];
  const threshold = observation[`${side}_threshold`];
  const relation = observation[`${side}_relation`];
  const thresholdName =
    observation.field_id === "core.prior_high20" ? "前20会话最高价" : "均线";
  return `收盘 ${priceRelationEvidenceText(close)}；${thresholdName} ${priceRelationEvidenceText(threshold)}；关系 ${priceRelationLabels[relation] || (relation ? text(relation) : "UNKNOWN")}${isBefore ? "（前态）" : "（当日）"}`;
}

function locatePriceRelationObservation(observation) {
  const points = state.timeline?.points || [];
  const index = points.findIndex(
    (point) => point.session_date === observation.session_date,
  );
  if (index < 0) return;
  state.chartMode = "day";
  state.chartSelection = { mode: "day", index };
  renderCandlestickChart(state.timeline);
  $("priceStructurePanel")?.scrollIntoView({ block: "start" });
}

function renderPriceRelationHistory(history) {
  const panel = $("priceRelationHistory");
  const summary = $("priceRelationHistorySummary");
  const status = $("priceRelationHistoryStatus");
  const rows = $("priceRelationHistoryRows");
  if (!panel || !summary || !status || !rows) return;
  summary.replaceChildren();
  rows.replaceChildren();
  if (!history) {
    panel.hidden = true;
    status.textContent = "";
    return;
  }
  panel.hidden = false;
  const selectedSourceSet =
    state.timeline?.selection?.source_set_id || "无SEALED source_set";
  for (const [label, value] of [
    [
      "官方会话 / 紧邻对",
      `${history.official_session_count} / ${history.official_session_pair_count}`,
    ],
    [
      "可比较会话对 / 全字段可比",
      `${history.comparable_session_pair_count} / ${history.fully_comparable_session_pair_count}`,
    ],
    [
      "变化事件 / 不可比字段",
      `${history.event_count} / ${history.unknown_count}`,
    ],
  ]) {
    const card = document.createElement("div");
    card.className = "price-relation-history-stat";
    card.textContent = `${label}：${text(value)}`;
    summary.append(card);
  }
  const fieldSummary = document.createElement("p");
  fieldSummary.className = "muted";
  fieldSummary.textContent = Object.entries(history.field_statistics || {})
    .map(([fieldId, counts]) => {
      const fieldLabel = timelinePriceFieldLabels[fieldId] || fieldId;
      return `${fieldLabel}：可比${counts.comparable_count}，变化${counts.change_count}，未变化${counts.no_relation_change_count}，不可比${counts.not_comparable_count}`;
    })
    .join("；");
  summary.append(fieldSummary);
  const identity = state.timeline?.snapshot_identity || {};
  status.textContent =
    `${state.timeline?.symbol || "—"} · ${state.timeline?.target_session || "—"} · ` +
    `source_set ${selectedSourceSet} · namespace ${state.timeline?.namespace_id || "UNKNOWN"} · ` +
    `完整包SHA ${state.timeline?.package_sha256 || "UNKNOWN"} · ` +
    `事件身份SHA ${text(history.identity_ref || identity.identity_sha256 || "UNKNOWN")}。` +
    "已知关系未变化不列为事件；UNKNOWN / 不可比原因独立统计。";

  const fieldFilter = $("priceRelationFieldFilter").value;
  const kindFilter = $("priceRelationKindFilter").value;
  const showUnknown = $("showPriceRelationUnknown").checked;
  const visible = (history.observations || []).filter((observation) => {
    const unknown = observation.comparison_status === "NOT_COMPARABLE";
    if (fieldFilter !== "ALL" && observation.field_id !== fieldFilter)
      return false;
    if (kindFilter === "NOT_COMPARABLE") return unknown;
    if (unknown) return showUnknown && kindFilter === "ALL";
    if (observation.comparison_status !== "RELATION_CHANGED") return false;
    return kindFilter === "ALL" || observation.event_kind === kindFilter;
  });
  for (const observation of visible) {
    const unknown = observation.comparison_status === "NOT_COMPARABLE";
    const row = document.createElement("tr");
    row.dataset.status = observation.comparison_status;
    cell(row, observation.session_date);
    cell(row, observation.previous_official_session);
    cell(
      row,
      timelinePriceFieldLabels[observation.field_id] || observation.field_id,
    );
    cell(row, priceRelationSideText(observation, "before"));
    cell(row, priceRelationSideText(observation, "after"));
    cell(
      row,
      unknown
        ? observation.reason_summary || "存在不可比依赖，原因未提供"
        : priceRelationKindLabels[observation.event_kind] ||
            observation.event_kind,
    );
    const captureText = `${text(observation.before_source_capture_id || "无前态捕获引用")} → ${text(observation.after_source_capture_id || "无当日捕获引用")}`;
    cell(
      row,
      `${observation.before_close?.quality || "UNKNOWN"} / ${observation.before_threshold?.quality || "UNKNOWN"} → ${observation.after_close?.quality || "UNKNOWN"} / ${observation.after_threshold?.quality || "UNKNOWN"}；${text(observation.producer)} · ${text(observation.semantic_version)}；捕获 ${captureText}；身份 ${text(observation.identity_ref || "UNKNOWN")}`,
    );
    const action = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "定位K线 / Core";
    button.addEventListener("click", () =>
      locatePriceRelationObservation(observation),
    );
    action.append(button);
    row.append(action);
    rows.append(row);
  }
  if (visible.length) {
    status.textContent += ` 当前筛选显示${visible.length}条变化或不可比记录。`;
  } else if (kindFilter === "NOT_COMPARABLE" || showUnknown) {
    status.textContent += "当前字段与筛选下没有可显示的匹配记录。";
  } else if (history.event_count === 0 && history.unknown_count > 0) {
    status.textContent += `当前已计算但没有变化事件；另有${history.unknown_count}条不可比字段，可勾选显示未知原因。`;
  } else if (history.event_count === 0) {
    status.textContent +=
      "已计算，但没有满足变化条件的事件；UNKNOWN已另行统计。";
  } else {
    status.textContent +=
      "当前筛选下没有匹配事件；可恢复全部字段和变化类型查看完整集合。";
  }
}

function invalidateTimeline(errorMessage = "") {
  state.timelineRequest++;
  state.timeline = null;
  state.timelineContext = null;
  state.chartPeriodContext = null;
  state.chartSelection = null;
  $("queryTimeline").disabled = false;
  clearRawPeriodState(
    "日期、代码或窗口已变化；旧周期图已清除。请重新查询精确上下文。",
  );
  renderTimeline(null, errorMessage);
}
function openTimelineSession(point) {
  const context = state.timelineContext;
  if (!context || !state.timeline || point.raw_row_present !== true) return;
  const value = `${point.session_date}|${context.package_sha256}`;
  const select = $("versionSelect");
  const option = Array.from(select.options).find(
    (item) => item.value === value,
  );
  if (
    !option ||
    !state.versions.some(
      (item) =>
        item.session_date === point.session_date &&
        item.package_sha256 === context.package_sha256,
    )
  )
    return;
  if (context.origin !== "change" && !context.returnSelection) {
    state.timelineContext = {
      ...context,
      returnSelection: {
        target_session: context.target_session,
        package_sha256: context.package_sha256,
        symbol: context.symbol,
        session_count: context.session_count,
        namespace_id: context.namespace_id || state.timeline.namespace_id,
        source_set_id:
          context.source_set_id ||
          state.timeline.selection?.source_set_id ||
          null,
      },
    };
  }
  state.timelineNavigation = true;
  state.chartSelection = null;
  state.chartPeriodContext = {
    target_session: point.session_date,
    package_sha256: context.package_sha256,
    symbol: context.symbol,
    namespace_id: state.timeline.namespace_id,
    source_set_id: state.timeline.selection?.source_set_id || null,
  };
  state.timeline = null;
  renderTimeline(null);
  $("symbolInput").value = context.symbol;
  $("windowInput").value = String(context.session_count);
  select.value = value;
  $("stockForm").requestSubmit();
}
function returnToTimelineCutoff() {
  const context = state.timelineContext;
  const selection = context?.returnSelection;
  if (!context || !selection || context.origin === "change") return;
  const value = `${selection.target_session}|${selection.package_sha256}`;
  if (
    !Array.from($("versionSelect").options).some((item) => item.value === value)
  )
    return;
  state.timelineNavigation = true;
  state.chartPeriodContext = {
    target_session: selection.target_session,
    package_sha256: selection.package_sha256,
    symbol: selection.symbol || context.symbol,
    namespace_id: selection.namespace_id || state.timeline?.namespace_id,
    source_set_id: selection.source_set_id || null,
  };
  $("symbolInput").value = selection.symbol || context.symbol;
  $("windowInput").value = String(
    selection.session_count || context.session_count,
  );
  $("versionSelect").value = value;
  $("stockForm").requestSubmit();
}
function returnToChanges() {
  const context = state.timelineContext;
  const selection = context?.returnSelection;
  if (!context || context.origin !== "change" || !selection) return;
  const value = `${selection.target_session}|${selection.package_sha256}`;
  if (
    !Array.from($("versionSelect").options).some((item) => item.value === value)
  )
    return;
  $("versionSelect").value = value;
  $("symbolInput").value = selection.symbol;
  $("windowInput").value = String(selection.session_count);
  state.stockRequest++;
  state.timelineRequest++;
  state.timeline = null;
  state.timelineContext = null;
  state.timelineNavigation = false;
  state.chartPeriodContext = null;
  state.chartSelection = null;
  clearRawPeriodState("返回变化详情；下一次轨迹查询会按精确身份读取周期数据。");
  renderTimeline(null);
  clearResult();
  $("changeDetail").scrollIntoView({ block: "start" });
}
function renderTimelineReturnControls(context, selected) {
  const returnRoot = $("timelineReturn");
  if (context?.origin === "change") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回画像变化详情";
    button.addEventListener("click", returnToChanges);
    returnRoot.append(button);
  }
  if (
    context?.origin !== "change" &&
    context?.returnSelection &&
    selected &&
    selected.target_session !== context.returnSelection.target_session
  ) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = `返回轨迹截止日 ${context.returnSelection.target_session}`;
    button.addEventListener("click", returnToTimelineCutoff);
    returnRoot.append(button);
  }
}
const SVG_NS = "http://www.w3.org/2000/svg";
function svgElement(name, attributes = {}, value = null) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, attribute] of Object.entries(attributes))
    node.setAttribute(key, String(attribute));
  if (value !== null) node.textContent = String(value);
  return node;
}
function exactPriceText(field) {
  const value = field?.value;
  if (value && typeof value === "object") {
    if (value.exact !== undefined && value.decimal !== undefined)
      return `${value.exact} (${value.decimal})`;
    return String(value.exact ?? value.decimal ?? "");
  }
  return value === null || value === undefined ? "" : String(value);
}
function chartPriceNumber(field) {
  const value = field?.value;
  if (value && typeof value === "object") {
    if (value.decimal !== undefined) return Number(value.decimal);
    if (value.exact !== undefined) {
      const exact = String(value.exact);
      const [numerator, denominator] = exact.split("/");
      return denominator
        ? Number(numerator) / Number(denominator)
        : Number(exact);
    }
  }
  return Number(value);
}
function validOhlc(fields, quality) {
  if (quality !== "OBSERVED") return null;
  const names = ["open", "high", "low", "close"];
  const result = {};
  for (const name of names) {
    const field = fields?.[name];
    if (field?.quality !== "OBSERVED") return null;
    const number = chartPriceNumber(field);
    const exact = exactPriceText(field);
    if (!Number.isFinite(number) || number <= 0 || !exact) return null;
    result[name] = { number, exact };
  }
  if (
    result.high.number <
      Math.max(result.open.number, result.low.number, result.close.number) ||
    result.low.number >
      Math.min(result.open.number, result.high.number, result.close.number)
  )
    return null;
  return result;
}
function dayOhlc(point) {
  return validOhlc(point.raw_ohlc, point.raw_ohlc?.quality);
}
function periodOhlc(period, binding) {
  const samePackage =
    Array.isArray(period.source_package_sha256s) &&
    period.source_package_sha256s.length > 0 &&
    period.source_package_sha256s.every(
      (value) => value === binding.package_sha256,
    );
  if (
    !samePackage ||
    period.calendar_closed !== true ||
    period.coverage_status !== "COMPLETE"
  )
    return null;
  const fields = Object.fromEntries(
    ["open", "high", "low", "close"].map((name) => [
      name,
      {
        value: period.ohlc?.[name],
        quality: period.ohlc?.[name] ? "OBSERVED" : "UNKNOWN",
      },
    ]),
  );
  return validOhlc(fields, "OBSERVED");
}
function pointDetail(point) {
  const fields = point.raw_ohlc || {};
  const values = dayOhlc(point);
  const source = state.timeline?.formal_source_set || {};
  const root = $("chartDetail");
  renderPriceStructureObservation(point);
  root.replaceChildren();
  appendText(root, "h4", `${point.session_date} · 日K来源与Core观察`);
  const details = document.createElement("dl");
  const pairs = [
    [
      "RAW OHLC",
      values
        ? `O ${values.open.exact} · H ${values.high.exact} · L ${values.low.exact} · C ${values.close.exact}`
        : `不出蜡烛 · ${text(fields.reason || point.price_reason || point.raw_status)}`,
    ],
    [
      "价格质量",
      `${text(fields.quality || point.price_quality)} · ${text(fields.reason || point.price_reason)}`,
    ],
    ["Core MA20", timelineField(point.core_ma20)],
    [
      "收盘相对MA20",
      point.close_vs_ma20
        ? text(point.close_vs_ma20)
        : `UNKNOWN · ${text(point.relation_unknown_reason)}`,
    ],
    [
      "原始坐标 / 时间资格",
      `${state.timeline?.price_coordinate || "RAW_UNADJUSTED"} · ${state.timeline?.pit_status || "NON_PIT_RETROSPECTIVE"} · ${state.timeline?.identity_status || "SOURCE_SYMBOL_ONLY"}`,
    ],
    [
      "目标 / 代码 / 包SHA",
      `${state.timeline?.target_session || "—"} · ${state.timeline?.symbol || "—"} · ${point.package_sha256}`,
    ],
    [
      "namespace / 轨迹截止日source_set",
      `${point.namespace_id || state.timeline?.namespace_id || "—"} · ${source.source_set_id || state.timeline?.selection?.source_set_id || "UNKNOWN"}`,
    ],
    [
      "来源捕获 / 内容 / 条目SHA",
      `${text(point.source_capture_id)} · ${text(point.source_content_id)} · ${text(point.source_entry_sha256)}`,
    ],
    [
      "MA20窗口",
      `${point.ma20_window?.valid_bar_count ?? 0}/${point.ma20_window?.required_valid_bar_count ?? 20}个有效Bar；${point.ma20_window?.official_session_count ?? 0}个官方会话`,
    ],
    [
      "研究边界",
      "NON_PIT来源代码级历史观察，不是复权价格、正式Profile或交易信号。",
    ],
  ];
  for (const [label, value] of pairs) {
    appendText(details, "dt", label);
    appendText(details, "dd", value);
  }
  root.append(details);
  if (!point.raw_row_present) {
    appendText(
      root,
      "p",
      "PG未保存该官方会话Raw行；不可发起单日Core请求。",
      "muted",
    );
    return;
  }
  const button = document.createElement("button");
  button.type = "button";
  button.className = "quiet";
  button.textContent = "查看当日Core / 周月";
  const exactOption = state.versions.some(
    (version) =>
      version.session_date === point.session_date &&
      version.package_sha256 === point.package_sha256,
  );
  button.disabled = !exactOption;
  if (exactOption)
    button.addEventListener("click", () => openTimelineSession(point));
  else
    button.title = "目录中没有相同日期和完整SHA的来源版本，不能回退其他版本。";
  root.append(button);
}
function periodDetail(period, mode) {
  const root = $("chartDetail");
  root.replaceChildren();
  const type = mode === "week" ? "自然周" : "自然月";
  appendText(
    root,
    "h4",
    `${type} ${period.period_key} · ${period.calendar_start_session}—${period.calendar_end_session}`,
  );
  const ohlc = periodOhlc(period, state.rawPeriodContext || {});
  appendText(
    root,
    "p",
    ohlc
      ? `闭合且完整 · RAW_UNADJUSTED · O ${ohlc.open.exact} · H ${ohlc.high.exact} · L ${ohlc.low.exact} · C ${ohlc.close.exact}`
      : `不出完整周期蜡烛 · ${text(period.coverage_status)} · ${(period.reason_codes || []).map(text).join(" / ") || "周期未闭合或来源覆盖不足"}`,
  );
  appendText(
    root,
    "p",
    `官方会话 ${period.elapsed_expected_session_count}/${period.calendar_expected_session_count} · 有效Bar ${period.valid_price_bar_count} · 缺Bar ${period.target_missing_count} · 读取失败 ${period.read_failed_count} · PG未保存 ${period.missing_history_count} · source_set ${state.rawPeriods?.snapshot_identity?.source_set_id || "UNKNOWN"}`,
    "muted",
  );
  if (!period.daily_rows?.length) {
    appendText(root, "p", "此周期没有可展开的闭合会话明细。", "muted");
    return;
  }
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  const table = document.createElement("table");
  table.className = "period-detail-table";
  const head = document.createElement("thead");
  tableRow(head, [
    "交易会话",
    "Bar / 价格质量",
    "RAW OHLC",
    "来源证据",
    "单日研究",
  ]);
  const body = document.createElement("tbody");
  for (const daily of period.daily_rows) {
    const row = tableRow(body, [
      daily.session,
      `${text(daily.bar_status)} · ${text(daily.price_quality)} · ${text(daily.bar_reason || daily.price_reason)}`,
      daily.valid_price_bar
        ? `O ${text(daily.open)} · H ${text(daily.high)} · L ${text(daily.low)} · C ${text(daily.close)}`
        : "无有效OHLC",
      `${text(daily.package_sha256)} · ${text(daily.source_capture_id)} · ${text(daily.source_content_id)} · ${text(daily.source_entry_sha256)}`,
      "",
    ]);
    const action = row.lastElementChild;
    const point = state.timeline?.points?.find(
      (item) => item.session_date === daily.session,
    );
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent =
      daily.bar_status === "HISTORY_ROW_NOT_STORED"
        ? "PG未保存"
        : "查看当日Core";
    const exactOption = state.versions.some(
      (version) =>
        version.session_date === daily.session &&
        version.package_sha256 === state.rawPeriodContext?.package_sha256,
    );
    button.disabled = !point || !point.raw_row_present || !exactOption;
    if (!button.disabled)
      button.addEventListener("click", () => openTimelineSession(point));
    else
      button.title =
        "缺少该会话的已保存Raw行或精确目录版本，不会发起跨版本请求。";
    action.append(button);
  }
  table.append(head, body);
  wrap.append(table);
  root.append(wrap);
}
function chartContextForTimeline(data) {
  return {
    target_session: data?.selection?.target_session || data?.target_session,
    package_sha256: data?.selection?.package_sha256 || data?.package_sha256,
    symbol: data?.selection?.source_symbol || data?.symbol,
    namespace_id: data?.selection?.namespace_id || data?.namespace_id,
    source_set_id:
      data?.selection?.source_set_id ||
      data?.snapshot_identity?.source_set_id ||
      null,
  };
}
function chartSlotLabel(item, mode) {
  if (mode === "day") return item.point.session_date;
  return `${item.period.period_key} · ${item.period.calendar_start_session}—${item.period.calendar_end_session}`;
}
function makeCandleTitle(item, mode) {
  const label = chartSlotLabel(item, mode);
  if (!item.ohlc)
    return `${label} · ${item.quality || "UNKNOWN"} · ${item.reason || "不出蜡烛"}`;
  return `${label} · RAW_UNADJUSTED · O ${item.ohlc.open.exact} · H ${item.ohlc.high.exact} · L ${item.ohlc.low.exact} · C ${item.ohlc.close.exact}`;
}
function renderCandlestickChart(data) {
  const chart = $("candlestickChart");
  const identity = $("chartIdentity");
  const notice = $("chartNotice");
  const detail = $("chartDetail");
  if (!chart || !identity || !notice || !detail) return;
  chart.replaceChildren();
  const mode = state.chartMode;
  for (const [name, buttonId] of Object.entries({
    day: "chartModeDay",
    week: "chartModeWeek",
    month: "chartModeMonth",
  }))
    $(buttonId)?.setAttribute("aria-pressed", String(mode === name));
  if (!data) {
    identity.textContent = "";
    notice.textContent = "查询轨迹后可查看与历史证据绑定的K线图。";
    if (!state.chartSelection) detail.replaceChildren();
    renderPriceStructureObservation(null);
    return;
  }
  const timelineBinding = chartContextForTimeline(data);
  const periodBinding = state.chartPeriodContext || timelineBinding;
  identity.textContent = `${mode === "day" ? "日K轨迹" : mode === "week" ? "自然周K" : "自然月K"} · ${periodBinding.symbol} · 截止 ${periodBinding.target_session} · ${periodBinding.package_sha256} · namespace ${periodBinding.namespace_id} · source_set ${periodBinding.source_set_id || "无SEALED集合"} · RAW_UNADJUSTED / NON_PIT / SOURCE_SYMBOL_ONLY`;
  let items = [];
  let periods = [];
  if (mode === "day") {
    items = (data.points || []).map((point) => ({
      point,
      ohlc: dayOhlc(point),
      quality: point.raw_ohlc?.quality || point.price_quality,
      reason: point.raw_ohlc?.reason || point.price_reason || point.raw_status,
    }));
    notice.textContent = `实际官方会话 ${items.length} 个；有效RAW蜡烛 ${items.filter((item) => item.ohlc).length} 个。缺Bar、读取失败、未保存Raw、坏值均保留原会话位置且不补价；黄色线仅连接连续可观测Core MA20。`;
  } else {
    if (!rawPeriodsMatch(periodBinding)) {
      notice.textContent =
        "尚无与当前轨迹日期、完整SHA、来源代码、namespace及source_set完全一致的周期结果；切换后按当前身份进行一次只读读取。";
      if (!state.chartSelection) detail.replaceChildren();
      return;
    }
    const periodData = state.rawPeriods;
    periods = periodData.periods?.[mode] || [];
    items = periods.map((period) => ({
      period,
      ohlc: periodOhlc(period, periodBinding),
      quality: period.coverage_status,
      reason: (period.reason_codes || []).join(" / "),
    }));
    const unfinished = periods.filter(
      (period) => period.calendar_closed !== true,
    );
    notice.textContent =
      `目标 ${periodData.target_session} · 闭合周期 ${periods.filter((period) => period.calendar_closed).length} 个 · 完整K线 ${items.filter((item) => item.ohlc).length} 个 · 部分/未知 ${items.filter((item) => item.period.calendar_closed && !item.ohlc).length} 个` +
      (unfinished.length
        ? `。${unfinished.map((period) => `${period.period_key} 尚未闭合，正式OHLC不出图`).join("；")}`
        : "。") +
      "未出完整K线的周期仍占位显示；点击可查看覆盖原因与可用日线来源。";
  }
  if (!items.length) {
    appendText(
      chart,
      "p",
      "官方日历与所选来源上下文没有可显示的周期槽。",
      "muted",
    );
    if (!state.chartSelection) detail.replaceChildren();
    return;
  }
  const validItems = items.filter((item) => item.ohlc);
  if (!validItems.length)
    appendText(
      chart,
      "p",
      "当前真实窗口没有合法且可观测的完整OHLC；图中仅保留缺口，不补造价格。",
      "muted",
    );
  const ma20Numbers =
    mode === "day"
      ? items.map((item) => {
          const field = item.point.core_ma20;
          if (!item.ohlc || field?.quality !== "OBSERVED") return null;
          const number = chartPriceNumber(field);
          return Number.isFinite(number) && number > 0 ? number : null;
        })
      : [];
  const prices = validItems.flatMap((item) => [
    item.ohlc.high.number,
    item.ohlc.low.number,
  ]);
  prices.push(...ma20Numbers.filter((value) => value !== null));
  let low = prices.length ? Math.min(...prices) : 0;
  let high = prices.length ? Math.max(...prices) : 1;
  const padding =
    high === low ? Math.max(Math.abs(high) * 0.01, 0.01) : (high - low) * 0.06;
  if (prices.length) {
    low -= padding;
    high += padding;
  }
  const width = 1000;
  const height = 340;
  const margin = { left: 68, right: 14, top: 18, bottom: 48 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const y = (value) =>
    margin.top + ((high - value) / (high - low)) * plotHeight;
  const slot = plotWidth / items.length;
  const x = (index) => margin.left + (index + 0.5) * slot;
  const svg = svgElement("svg", {
    class: "candlestick-svg",
    viewBox: `0 0 ${width} ${height}`,
    role: "group",
    "aria-label": `${identity.textContent}；图中仅绘制真实且质量合格的RAW OHLC。`,
    preserveAspectRatio: "xMidYMid meet",
  });
  svg.append(svgElement("title", {}, identity.textContent));
  for (let tick = 0; tick <= 4 && prices.length; tick++) {
    const value = high - ((high - low) * tick) / 4;
    const yValue = y(value);
    svg.append(
      svgElement("line", {
        x1: margin.left,
        x2: width - margin.right,
        y1: yValue,
        y2: yValue,
        class: "candle-gridline",
      }),
      svgElement(
        "text",
        {
          x: margin.left - 8,
          y: yValue + 4,
          class: "candle-axis-label",
          "text-anchor": "end",
        },
        value.toFixed(2),
      ),
    );
  }
  if (mode === "day") {
    let segment = [];
    const flushSegment = () => {
      if (segment.length > 1)
        svg.append(
          svgElement("path", { d: segment.join(" "), class: "candle-ma20" }),
        );
      segment = [];
    };
    ma20Numbers.forEach((value, index) => {
      if (value === null) {
        flushSegment();
      } else {
        segment.push(`${segment.length ? "L" : "M"}${x(index)},${y(value)}`);
      }
    });
    flushSegment();
  }
  items.forEach((item, index) => {
    const group = svgElement("g", {
      class: `candle-hit-target ${item.ohlc && item.ohlc.close.number >= item.ohlc.open.number ? "candle-up" : "candle-down"}`,
      role: "button",
      tabindex: "0",
      "aria-label": makeCandleTitle(item, mode),
    });
    group.append(svgElement("title", {}, makeCandleTitle(item, mode)));
    if (item.ohlc) {
      const center = x(index);
      const bodyWidth = Math.max(3, Math.min(11, slot * 0.48));
      const highY = y(item.ohlc.high.number);
      const lowY = y(item.ohlc.low.number);
      const openY = y(item.ohlc.open.number);
      const closeY = y(item.ohlc.close.number);
      group.append(
        svgElement("line", {
          x1: center,
          x2: center,
          y1: highY,
          y2: lowY,
          class: "candle-wick",
        }),
        svgElement("rect", {
          x: center - bodyWidth / 2,
          y: Math.min(openY, closeY),
          width: bodyWidth,
          height: Math.max(2, Math.abs(closeY - openY)),
          class: "candle-body",
        }),
      );
    } else {
      const center = x(index);
      const mid = margin.top + plotHeight / 2;
      group.setAttribute("class", "candle-hit-target candle-gap-mark");
      group.append(
        svgElement("line", {
          x1: center - 3,
          x2: center + 3,
          y1: mid - 4,
          y2: mid + 4,
          class: "candle-gap-mark",
        }),
        svgElement("line", {
          x1: center + 3,
          x2: center - 3,
          y1: mid - 4,
          y2: mid + 4,
          class: "candle-gap-mark",
        }),
      );
    }
    const selectItem = () => {
      state.chartSelection = { mode, index };
      if (mode === "day") {
        pointDetail(item.point);
      } else {
        periodDetail(item.period, mode);
      }
    };
    group.addEventListener("click", selectItem);
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectItem();
      }
    });
    svg.append(group);
    if (
      index === 0 ||
      index === items.length - 1 ||
      index % Math.ceil(items.length / 5) === 0
    ) {
      const label =
        mode === "day"
          ? item.point.session_date.slice(5)
          : item.period.period_key;
      svg.append(
        svgElement(
          "text",
          {
            x: x(index),
            y: height - 15,
            class: "candle-date-label",
            "text-anchor": "middle",
          },
          label,
        ),
      );
    }
  });
  chart.append(svg);
  const legend = document.createElement("div");
  legend.className = "candle-legend";
  appendText(legend, "span", "空心：收盘不低于开盘");
  appendText(legend, "span", "实心：收盘低于开盘");
  appendText(legend, "span", "斜线缺口：无合法可观测OHLC，不补价");
  if (mode === "day")
    appendText(legend, "span", "橙线：Core MA20，仅连接连续有效点");
  chart.append(legend);
  if (state.chartSelection?.mode === mode) {
    const selected = items[state.chartSelection.index];
    if (selected) {
      if (mode === "day") pointDetail(selected.point);
      else periodDetail(selected.period, mode);
    }
  } else if (mode === "day" && items.length) {
    const index = items.length - 1;
    state.chartSelection = { mode, index };
    pointDetail(items[index].point);
  } else {
    detail.replaceChildren();
    appendText(
      detail,
      "p",
      "点击一根K线查看精确价格、周期覆盖、来源版本与单日研究入口。",
      "muted",
    );
  }
}
async function setChartMode(mode) {
  state.chartMode = mode;
  state.chartSelection = null;
  renderCandlestickChart(state.timeline);
  if (mode === "day" || !state.timeline) return;
  const binding =
    state.chartPeriodContext || chartContextForTimeline(state.timeline);
  if (rawPeriodsMatch(binding)) return;
  const version = {
    target_session: binding.target_session,
    package_sha256: binding.package_sha256,
  };
  try {
    await loadRawPeriods(
      version,
      binding.symbol,
      {
        namespace_id: binding.namespace_id,
        source_set_id: binding.source_set_id,
      },
      () =>
        state.timeline !== null &&
        rawPeriodCacheKey(
          state.chartPeriodContext || chartContextForTimeline(state.timeline),
        ) === rawPeriodCacheKey(binding),
    );
  } catch (error) {
    if (state.timeline) renderRawPeriods(null, error.message);
  }
}
function renderTimeline(data, errorMessage = "") {
  const panel = $("timelinePanel");
  const rows = $("timelineRows");
  const status = $("timelineStatus");
  const returnRoot = $("timelineReturn");
  rows.replaceChildren();
  returnRoot.replaceChildren();
  if (!data) {
    panel.hidden = true;
    if (errorMessage) {
      panel.hidden = false;
      status.textContent = `历史轨迹未返回：${errorMessage}`;
      renderTimelineReturnControls(state.timelineContext, selectedVersion());
    }
    renderCandlestickChart(null);
    renderPriceRelationHistory(null);
    return;
  }
  panel.hidden = false;
  const counts = Object.entries(data.event_counts || {})
    .map(([event, count]) => `${text(event)} ${count}`)
    .join(" · ");
  status.textContent =
    `${data.symbol} · 截止 ${data.target_session} · ${data.total_session_count} 个官方会话 · ` +
    `Raw行 ${data.raw_row_count} · 有效价格Bar ${data.valid_price_bar_count} · ` +
    `MA20关系可评价 ${data.known_count}/${data.total_session_count} · ` +
    `未知 ${data.unknown_count} · 可比较跨日 ${data.comparable_transition_count} · ` +
    `不可比较 ${data.not_comparable_count} · 首次可评价 ${data.first_evaluable_count}次 ` +
    `（${data.first_evaluable_session || "尚无"}）。` +
    `事件：${counts || "无"}。${data.price_coordinate} / ${data.research_mode}；` +
    `${data.producer || "stock_a.research.core:calculate_core"} · ${data.semantic_version || "CORE_PRIMITIVES_FIELDSET_R2"}。` +
    "身份SOURCE_SYMBOL_ONLY/NON_PIT；选股NOT_EVALUATED/NOT_CONNECTED；严格PIT、复权与RPS资格UNKNOWN；Raw候选数不等于官方上市总体。";
  renderTimelineReturnControls(state.timelineContext, selectedVersion());
  renderPriceRelationHistory(data.price_relation_history);
  for (const point of data.points || []) {
    const tr = document.createElement("tr");
    const rawOhlc = point.raw_ohlc;
    const rawOhlcText = rawOhlc
      ? [
          `O ${timelineField(rawOhlc.open)}`,
          `H ${timelineField(rawOhlc.high)}`,
          `L ${timelineField(rawOhlc.low)}`,
          `C ${timelineField(rawOhlc.close)}`,
          rawOhlc.quality === "OBSERVED"
            ? "OBSERVED"
            : `${text(rawOhlc.quality)} · ${text(rawOhlc.reason)}`,
        ].join(" · ")
      : "UNKNOWN · 轨迹未提供RAW OHLC字段";
    cell(tr, point.session_date);
    cell(
      tr,
      `${text(point.raw_status)} · ${text(point.price_quality)}${point.price_reason ? ` · ${point.price_reason}` : ""}`,
    );
    cell(tr, rawOhlcText);
    cell(tr, timelineField(point.raw_close));
    cell(tr, timelineField(point.core_ma20));
    cell(
      tr,
      point.close_vs_ma20
        ? text(point.close_vs_ma20)
        : `UNKNOWN · ${text(point.relation_unknown_reason)}`,
    );
    cell(
      tr,
      `${text(point.comparison?.status)} · ${text(point.comparison?.previous_relation)} → ${text(point.comparison?.event)}${point.comparison?.reason ? ` · ${text(point.comparison.reason)}` : ""}`,
    );
    cell(
      tr,
      `${point.ma20_window.valid_bar_count}/${point.ma20_window.required_valid_bar_count} · ` +
        `MA20 ${point.ma20_window.official_session_count}会话；` +
        `prior_high20 ${point.prior_high20_window.valid_bar_count}/${point.prior_high20_window.required_valid_bar_count} · ` +
        `${point.prior_high20_window.official_session_count}会话`,
    );
    cell(
      tr,
      `${timelineField(point.core_prior_high20)} · ` +
        (point.close_vs_prior_high20
          ? priorHighRelationLabel(point.close_vs_prior_high20)
          : `UNKNOWN · ${text(point.prior_high20_relation_unknown_reason)}`),
    );
    const actionCell = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    const exactOption = state.versions.some(
      (version) =>
        version.session_date === point.session_date &&
        version.package_sha256 === point.package_sha256,
    );
    button.disabled = point.raw_row_present !== true || !exactOption;
    if (!point.raw_row_present) {
      button.textContent = "PG未保存，不可展开";
      button.title =
        "该官方会话没有此来源代码的PG Raw行，不会发起单日Core请求。";
    } else if (!exactOption) {
      button.textContent = "缺少精确目录版本";
      button.title = "目录中没有同日期、同完整包SHA的版本，不能回退其他版本。";
    } else {
      button.textContent = "查看当日Core / 周月";
      button.addEventListener("click", () => openTimelineSession(point));
    }
    actionCell.append(button);
    tr.append(actionCell);
    rows.append(tr);
  }
  if (!data.points?.length)
    tableRow(rows, ["无官方会话轨迹", "", "", "", "", "", "", "", "", ""]);
  renderCandlestickChart(data);
}
async function queryTimeline(binding = null) {
  const requestId = ++state.timelineRequest;
  const version = selectedVersion();
  const symbol = $("symbolInput").value.trim();
  const sessionCount = Number($("windowInput").value);
  if (!version || !symbol || !Number.isInteger(sessionCount)) {
    state.timeline = null;
    state.timelineContext = null;
    $("queryTimeline").disabled = false;
    renderTimeline(
      null,
      "请先选择目标日和完整来源包版本，并填写来源代码及会话数。",
    );
    return;
  }
  state.timeline = null;
  state.chartSelection = null;
  clearRawPeriodState("正在按当前轨迹的来源身份读取周期证据。");
  const context = {
    ...version,
    symbol,
    session_count: sessionCount,
    namespace_id: binding?.namespace_id || null,
    source_set_id: binding?.source_set_id || null,
    origin: binding?.origin || "manual",
    persistedProfileSnapshot: binding?.persistedProfileSnapshot || null,
    returnSelection: binding?.returnSelection || null,
  };
  state.timelineContext = context;
  renderCandlestickChart(null);
  const requestKey = `${version.target_session}|${version.package_sha256}|${symbol}|${sessionCount}`;
  const requestIsCurrent = () => {
    const currentVersion = selectedVersion();
    return (
      requestId === state.timelineRequest &&
      state.timelineContext === context &&
      requestKey ===
        `${currentVersion?.target_session}|${currentVersion?.package_sha256}|${$("symbolInput").value.trim()}|${Number($("windowInput").value)}`
    );
  };
  $("queryTimeline").disabled = true;
  $("timelinePanel").hidden = false;
  $("timelineStatus").textContent =
    "正在按所选来源代码和截止日进行一次有界PG只读历史读取，并逐日复用现役Core…";
  $("timelineRows").replaceChildren();
  try {
    const data = await api("/api/preview/stock-timeline", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        symbol,
        target_session: version.target_session,
        package_sha256: version.package_sha256,
        session_count: sessionCount,
        ...(context.source_set_id && context.namespace_id
          ? {
              source_set_id: context.source_set_id,
              namespace_id: context.namespace_id,
            }
          : {}),
      }),
    });
    if (!requestIsCurrent()) return;
    const relationHistory = data.price_relation_history;
    const identityRef = data.snapshot_identity?.identity_sha256;
    if (
      data.snapshot_identity?.namespace_id !== data.namespace_id ||
      data.snapshot_identity?.target_session !== version.target_session ||
      data.snapshot_identity?.package_sha256 !== version.package_sha256 ||
      data.selection?.source_symbol !== symbol ||
      data.selection?.target_session !== version.target_session ||
      data.selection?.package_sha256 !== version.package_sha256 ||
      data.selection?.namespace_id !== data.namespace_id ||
      data.selection?.source_set_id !==
        (data.snapshot_identity?.source_set_id ?? null) ||
      (context.namespace_id && data.namespace_id !== context.namespace_id) ||
      (context.source_set_id &&
        data.snapshot_identity?.source_set_id !== context.source_set_id)
    )
      throw new Error("轨迹响应身份与精确变化快照不一致，已拒绝展示。");
    if (
      !relationHistory ||
      relationHistory.identity_ref !== identityRef ||
      relationHistory.official_session_count !== data.total_session_count ||
      relationHistory.comparison_count !==
        Math.max(0, data.total_session_count - 1) * 5
    )
      throw new Error(
        "价格变化记录没有绑定当前完整轨迹身份或官方会话分母，已拒绝展示。",
      );
    const pointDates = (data.points || []).map((point) => point.session_date);
    for (const item of relationHistory.observations || []) {
      const index = pointDates.indexOf(item.session_date);
      if (
        item.identity_ref !== identityRef ||
        index < 1 ||
        pointDates[index - 1] !== item.previous_official_session
      )
        throw new Error(
          "价格变化记录不是同一精确轨迹中的紧邻官方会话，已拒绝展示。",
        );
    }
    const eventKeys = new Set(
      (relationHistory.events || []).map((item) => item.event_key),
    );
    if (
      [...eventKeys].some(
        (eventKey) =>
          !(relationHistory.observations || []).some(
            (item) =>
              `${item.session_date}|${item.field_id}|${item.event_kind}` ===
                eventKey && item.comparison_status === "RELATION_CHANGED",
          ),
      ) ||
      eventKeys.size !== relationHistory.event_count
    )
      throw new Error("价格变化事件与关系观察集合不一致，已拒绝展示。");
    state.timeline = data;
    state.chartPeriodContext = chartContextForTimeline(data);
    renderTimeline(data);
    if (state.chartMode !== "day") {
      try {
        await loadRawPeriods(
          version,
          symbol,
          data.snapshot_identity,
          () => requestIsCurrent() && state.timeline === data,
        );
      } catch (error) {
        if (requestIsCurrent() && state.timeline === data)
          renderRawPeriods(null, error.message);
      }
    }
  } catch (error) {
    if (!requestIsCurrent()) return;
    renderTimeline(null, error.message);
  } finally {
    if (requestIsCurrent()) $("queryTimeline").disabled = false;
  }
}
function clearProfileDetail() {
  $("profileDetail").hidden = true;
  $("profileDetail").replaceChildren();
}
function showProfileDetail(item) {
  const root = $("profileDetail");
  root.replaceChildren();
  root.hidden = false;
  appendText(
    root,
    "h3",
    `${item.source_symbol} · ${item.target_session} · 画像详情`,
  );
  const meta = document.createElement("dl");
  meta.className = "profile-detail-meta";
  const details = [
    ["来源范围", item.calculation_scope],
    ["研究资格", `${item.identity_status} · ${item.pit_status}`],
    ["source_set_id", item.source_set_id],
    ["来源包SHA-256", item.package_sha256],
    [
      "画像修订 / 合同SHA",
      `${item.profile_revision} · ${item.producer_contract_sha256}`,
    ],
    [
      "窗口与日历",
      `${item.history_calendar_id} ${item.history_calendar_revision} · ${item.history_window_start} 至 ${item.history_window_end}`,
    ],
    [
      "窗口记录 / 有效价格Bar",
      `${item.history_stored_session_count}/${item.history_requested_session_count} · ${item.history_actual_price_bar_count}`,
    ],
    [
      "MA20 / ATR20实际有效Bar",
      `${item.ma20_window_valid_bar_count}/${item.ma20_window_expected_bar_count} · ${item.atr20_window_valid_bar_count}/${item.atr20_window_expected_bar_count}`,
    ],
    [
      "历史记录覆盖",
      `${item.history_records_status} · ${item.history_records_complete ? "记录齐全" : "存在未持久Raw行"}`,
    ],
    [
      "价格窗口状态",
      `${item.price_window_status} · ${item.price_window_reason || "无"}`,
    ],
    ["窗口输入摘要SHA-256", item.history_window_sha256],
    [
      "量额单位资格",
      `成交额 ${item.amount_unit_status} · 成交量 ${item.volume_unit_status}`,
    ],
  ];
  for (const [label, value] of details) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = text(value);
    meta.append(dt, dd);
  }
  root.append(meta);
  appendText(root, "h3", "目标日PG Raw OHLC（按需联读）");
  const rawGrid = document.createElement("div");
  rawGrid.className = "raw-grid";
  for (const [label, value] of [
    ["开盘", item.target_raw_open],
    ["最高", item.target_raw_high],
    ["最低", item.target_raw_low],
    ["收盘", item.target_raw_close],
    ["价格质量", item.target_raw_price_quality],
    ["价格原因", item.target_raw_price_reason],
  ]) {
    const card = document.createElement("div");
    appendText(card, "span", label, "raw-label");
    appendText(card, "strong", text(value));
    rawGrid.append(card);
  }
  root.append(rawGrid);
  appendText(root, "h3", "Core价格字段 / 质量 / 原因");
  const coreTable = document.createElement("table");
  coreTable.innerHTML =
    "<thead><tr><th>字段</th><th>NUMERIC值</th><th>精确分数</th><th>质量</th><th>原因</th></tr></thead>";
  const coreBody = document.createElement("tbody");
  for (const name of [
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "atr20",
    "clv",
    "prior_high20",
    "ret20",
    "pos60",
    "bias20_atr",
  ]) {
    tableRow(coreBody, [
      `core.${name}`,
      item[name],
      item[`${name}_exact_numerator`] == null
        ? "—"
        : `${item[`${name}_exact_numerator`]}/${item[`${name}_exact_denominator`]}`,
      item[`${name}_quality`],
      item[`${name}_reason`],
    ]);
  }
  coreTable.append(coreBody);
  const coreWrap = document.createElement("div");
  coreWrap.className = "table-wrap";
  coreWrap.append(coreTable);
  root.append(coreWrap);
  appendText(root, "h3", "Profile 11分项状态 / 质量 / 原因");
  const profileTable = document.createElement("table");
  profileTable.innerHTML =
    "<thead><tr><th>字段</th><th>状态</th><th>质量</th><th>原因</th></tr></thead>";
  const profileBody = document.createElement("tbody");
  item.profile_field_ids.forEach((field, index) => {
    tableRow(profileBody, [
      field,
      item.profile_statuses[index],
      item.profile_qualities[index],
      item.profile_reasons[index] || "—",
    ]);
  });
  profileTable.append(profileBody);
  const profileWrap = document.createElement("div");
  profileWrap.className = "table-wrap";
  profileWrap.append(profileTable);
  root.append(profileWrap);
}
const changeTypeLabels = {
  BEFORE_ONLY: "仅起始快照存在",
  AFTER_ONLY: "仅结束快照存在",
  TARGET_BAR_STATUS_CHANGED: "目标Bar状态变化",
  PRICE_WINDOW_STATUS_CHANGED: "价格窗口变化",
  RAW_CLOSE_VS_MA20_CHANGED: "收盘/MA20关系变化",
  PROFILE_STATE_CHANGED: "Profile状态/质量变化",
  CORE_QUALITY_CHANGED: "Core质量/原因变化",
};
const highlightCategoryLabels = {
  CLOSE_MA20_BELOW_TO_ABOVE: "收盘由MA20下方转到上方",
  CLOSE_MA20_ABOVE_TO_BELOW: "收盘由MA20上方转到下方",
  CLOSE_MA20_EQUAL_TRANSITION: "收盘与MA20相等关系进入或离开",
  TARGET_BAR_STATUS: "目标Bar状态变化",
  PRICE_WINDOW_STATUS: "价格窗口状态或原因变化",
  PROFILE_STATE: "Profile状态或质量变化",
  CORE_QUALITY: "Core质量或原因变化",
  SOURCE_SET_MEMBERSHIP: "来源集合成员变化",
  RECORDED_CHANGE: "其它已记录变化",
};
function sessionRelationLabel(relation) {
  if (relation?.status === "CONSECUTIVE_OFFICIAL_SESSIONS")
    return `${relation.before_session}（前一官方会话）→ ${relation.after_session}（当日）`;
  if (relation?.status === "NONCONSECUTIVE_OFFICIAL_SESSIONS")
    return `${relation.before_session} → ${relation.after_session}（非相邻官方会话，仅描述所选日期关系变化）`;
  return `${relation?.before_session || "起始快照"} → ${relation?.after_session || "结束快照"}（官方会话相邻性未知，仅描述所选日期关系变化）`;
}
function changeRelationReason(item, relation) {
  const before = text(item.before_close_ma20_relation);
  const after = text(item.after_close_ma20_relation);
  if (
    relation?.status === "CONSECUTIVE_OFFICIAL_SESSIONS" &&
    item.highlight_category === "CLOSE_MA20_BELOW_TO_ABOVE"
  )
    return `前一官方会话收盘低于MA20 → 当日收盘高于MA20（两端价格与MA20均可观察）`;
  if (
    relation?.status === "CONSECUTIVE_OFFICIAL_SESSIONS" &&
    item.highlight_category === "CLOSE_MA20_ABOVE_TO_BELOW"
  )
    return `前一官方会话收盘高于MA20 → 当日收盘低于MA20（两端价格与MA20均可观察）`;
  return `所选两期关系：${before} → ${after}；仅当两端价格与MA20质量均为OBSERVED且Core语义、RAW坐标兼容时分类。`;
}
function highlightQualityText(item) {
  const price = `收盘 ${text(item.before_raw_price_quality)} → ${text(item.after_raw_price_quality)}`;
  const priceReason =
    item.after_raw_price_quality === "OBSERVED"
      ? item.before_raw_price_reason
      : item.after_raw_price_reason || item.before_raw_price_reason;
  const ma20 = `MA20 ${text(item.before_ma20_quality)} → ${text(item.after_ma20_quality)}`;
  const ma20Reason =
    item.after_ma20_quality === "OBSERVED"
      ? item.before_ma20_reason
      : item.after_ma20_reason || item.before_ma20_reason;
  const reasons = [priceReason, ma20Reason, item.after_window_reason]
    .filter(Boolean)
    .map(text);
  return [
    `${price}；${ma20}`,
    `目标Bar ${text(item.before_bar_status)} → ${text(item.after_bar_status)}`,
    `窗口 ${text(item.before_window_status)} → ${text(item.after_window_status)}`,
    `Core语义 ${item.core_compatible ? "兼容" : "UNKNOWN/不可比"} · Profile语义 ${item.profile_compatible ? "兼容" : "UNKNOWN/不可比"}`,
    reasons.length
      ? `原因：${[...new Set(reasons)].join("；")}`
      : "价格原因：—",
  ].join("；");
}
function appendHighlightSourceIdentity(parent, item, data) {
  const source = document.createElement("div");
  source.className = "highlight-identity";
  const short = (value, length = 8) =>
    value ? `${value.slice(0, length)}…` : "UNKNOWN";
  const identitySide = item.after_present ? "after" : "before";
  const identity = document.createElement("span");
  identity.textContent = `${text(item[`${identitySide}_identity_status`])} · ${short(item.before_source_set_id)}→${short(item.after_source_set_id)} · SHA ${short(item[`${identitySide}_package_sha256`], 12)} · rev ${short(item.before_profile_revision, 12)}→${short(item.after_profile_revision, 12)}`;
  source.append(identity);
  const qualityIdentity = document.createElement("span");
  qualityIdentity.textContent = `security_id ${text(item[`${identitySide}_stable_security_id_status`])} · 上市 ${text(item[`${identitySide}_listing_status`])} · 停牌 ${text(item[`${identitySide}_suspension_status`])} · ${text(item[`${identitySide}_pit_status`])} · ${text(data.formal_listing_universe || "UNKNOWN_NOT_CONNECTED")}`;
  source.append(qualityIdentity);
  const details = document.createElement("details");
  const summary = document.createElement("summary");
  summary.textContent = "展开完整来源版本身份";
  details.append(summary);
  const full = document.createElement("div");
  full.textContent = `namespace ${data.namespace_id}；前 source_set ${item.before_source_set_id}；后 source_set ${item.after_source_set_id}；前包SHA ${item.before_package_sha256}；后包SHA ${item.after_package_sha256}；前 profile_revision ${item.before_profile_revision}；后 profile_revision ${item.after_profile_revision}；前后坐标 ${text(item.before_price_coordinate)} → ${text(item.after_price_coordinate)}`;
  details.append(full);
  source.append(details);
  parent.append(source);
}
function renderChangeHighlights(data, relation) {
  const root = $("changeHighlightsRows");
  const summary = $("changeHighlightsSummary");
  const count = $("changeHighlightsCount");
  root.replaceChildren();
  const highlights = data.highlights;
  if (!highlights || !Array.isArray(highlights.rows)) {
    count.textContent = "摘要不可用";
    summary.textContent =
      "服务端没有返回同版本重点变化摘要，不能用当前分页替代。";
    tableRow(root, [summary.textContent, "", "", "", "", ""]);
    return;
  }
  const validCounts =
    Number.isInteger(highlights.eligible_count) &&
    Number.isInteger(highlights.display_count) &&
    Number.isInteger(highlights.undisplayed_count) &&
    highlights.display_count === highlights.rows.length &&
    highlights.display_count <= 30 &&
    highlights.eligible_count ===
      highlights.display_count + highlights.undisplayed_count;
  if (!validCounts) {
    count.textContent = "摘要计数异常";
    summary.textContent = "重点变化计数未闭合；已隐藏结果，等待检查。";
    tableRow(root, [summary.textContent, "", "", "", "", ""]);
    return;
  }
  count.textContent = `${highlights.display_count}/30`;
  summary.textContent =
    highlights.eligible_count === 0
      ? "已查询、没有符合展示规则的变化。此结果与未计算、来源不可用分开；30只展示上限不是市场分母。"
      : `同版本变化全集按规则命中 ${highlights.eligible_count} 只；显示 ${highlights.display_count}/30，命中但未显示 ${highlights.undisplayed_count} 只。此列表不是全市场分母，原全集仍可搜索和分页。`;
  for (const item of highlights.rows) {
    const row = document.createElement("tr");
    cell(row, item.source_symbol);
    const reasonCell = document.createElement("td");
    const reasonList = document.createElement("div");
    reasonList.className = "highlight-reason-list";
    const primary = document.createElement("strong");
    primary.textContent =
      highlightCategoryLabels[item.highlight_category] ||
      item.highlight_category ||
      "已记录变化";
    reasonList.append(primary);
    if (item.highlight_category?.startsWith("CLOSE_MA20_")) {
      const transition = document.createElement("span");
      transition.textContent = changeRelationReason(item, relation);
      reasonList.append(transition);
    }
    const allReasons = document.createElement("span");
    allReasons.textContent = `全部变化原因：${(item.change_types || [])
      .map((type) => changeTypeLabels[type] || type)
      .join("、")}`;
    reasonList.append(allReasons);
    reasonCell.append(reasonList);
    row.append(reasonCell);
    cell(
      row,
      `${text(item.before_close_ma20_relation)} → ${text(item.after_close_ma20_relation)}`,
    );
    const qualityCell = document.createElement("td");
    qualityCell.className = "highlight-quality";
    qualityCell.textContent = highlightQualityText(item);
    row.append(qualityCell);
    const identityCell = document.createElement("td");
    appendHighlightSourceIdentity(identityCell, item, data);
    row.append(identityCell);
    const detail = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet highlight-detail-action";
    button.textContent = "查看变化详情";
    button.addEventListener("click", () => showChangeDetail(item));
    detail.append(button);
    row.append(detail);
    root.append(row);
  }
  if (!highlights.rows.length)
    tableRow(root, ["已查询，没有符合展示规则的对象。", "", "", "", "", ""]);
}
function resetChangeDetail() {
  $("changeDetail").hidden = true;
  $("changeDetail").replaceChildren();
}
function resolveChangeTimelineSide(item, side, binding, versions) {
  const snapshot = binding?.[`${side}_snapshot`];
  const member = item[`${side}_present`] === true;
  const rawStored = item[`${side}_window_status`] !== "HISTORY_ROW_NOT_STORED";
  const exactVersion = snapshot
    ? versions.find(
        (version) =>
          version.session_date === snapshot.target_session &&
          version.package_sha256 === snapshot.package_sha256,
      )
    : null;
  return { snapshot, member, rawStored, exactVersion };
}
function changeTimelineSnapshot(item, side) {
  return resolveChangeTimelineSide(
    item,
    side,
    state.changeBinding,
    state.versions,
  );
}
function openChangeTimeline(item, side) {
  const { snapshot, member, rawStored, exactVersion } = changeTimelineSnapshot(
    item,
    side,
  );
  if (!member || !snapshot?.complete || !rawStored || !exactVersion) return;
  const selected = selectedVersion();
  const fallback = state.changeBinding?.after_snapshot;
  const returnDate = selected?.target_session || fallback?.target_session;
  const returnSha = selected?.package_sha256 || fallback?.package_sha256;
  if (
    !returnDate ||
    !returnSha ||
    !state.versions.some(
      (version) =>
        version.session_date === returnDate &&
        version.package_sha256 === returnSha,
    )
  )
    return;
  $("versionSelect").value =
    `${snapshot.target_session}|${snapshot.package_sha256}`;
  $("symbolInput").value = item.source_symbol;
  $("windowInput").value = "75";
  state.stockRequest++;
  clearResult();
  $("stockResult").classList.add("timeline-only");
  $("stockResult").hidden = false;
  $("resultTitle").textContent =
    `${item.source_symbol} · ${snapshot.target_session}`;
  $("resultMode").textContent =
    "变化快照关联历史轨迹 · SOURCE_SYMBOL_ONLY · NON_PIT · RAW_UNADJUSTED";
  $("snapshotId").textContent =
    `source_set_id ${snapshot.source_set_id} · namespace ${state.changeBinding.namespace_id} · 包SHA ${snapshot.package_sha256}`;
  $("windowText").textContent =
    "此视图只读取该快照绑定的官方会话轨迹；每日Core/周月详情须从存在Raw行的轨迹点进入。";
  state.timeline = null;
  queryTimeline({
    origin: "change",
    source_set_id: snapshot.source_set_id,
    namespace_id: state.changeBinding.namespace_id,
    persistedProfileSnapshot: {
      target_session: snapshot.target_session,
      package_sha256: snapshot.package_sha256,
      source_set_id: snapshot.source_set_id,
      namespace_id: state.changeBinding.namespace_id,
      profile_revision: item[`${side}_profile_revision`],
      field_ids: item[`${side}_profile_field_ids`] || [],
      statuses: item[`${side}_profile_statuses`] || [],
      qualities: item[`${side}_profile_qualities`] || [],
      reasons: item[`${side}_profile_reasons`] || [],
      semantic_compatible: item.profile_compatible === true,
    },
    returnSelection: {
      target_session: returnDate,
      package_sha256: returnSha,
      symbol: item.source_symbol,
      session_count: 75,
    },
  });
}
function appendChangeTimelineLinks(root, item) {
  appendText(root, "h3", "前后封口快照 · 单股75日Core轨迹");
  const actions = document.createElement("div");
  actions.className = "timeline-change-actions";
  for (const [side, label] of [
    ["before", "起始快照"],
    ["after", "结束快照"],
  ]) {
    const { snapshot, member, rawStored, exactVersion } =
      changeTimelineSnapshot(item, side);
    const action = document.createElement("div");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.disabled =
      !member || !snapshot?.complete || !rawStored || !exactVersion;
    button.textContent = snapshot
      ? `查看${label} ${snapshot.target_session} · 75日历史轨迹`
      : `查看${label}历史轨迹`;
    if (!member) {
      button.title = `${label}快照中没有该来源代码。`;
    } else if (!rawStored) {
      button.title = `${label}目标日没有保存该来源代码的Raw行，不能生成历史轨迹。`;
    } else if (!exactVersion) {
      button.title = `${label}快照的日期和完整包SHA不在当前PG目录中，不能回退版本。`;
    } else {
      button.addEventListener("click", () => openChangeTimeline(item, side));
    }
    action.append(button);
    actions.append(action);
  }
  root.append(actions);
  appendText(
    root,
    "p",
    "轨迹按变化快照的日期、完整包SHA、source_set与namespace绑定；缺Bar保留解释，未保存Raw会话不可展开。结果为SOURCE_SYMBOL_ONLY、NON_PIT研究，不表示选股资格。",
    "muted",
  );
}
function showChangeDetail(item) {
  const root = $("changeDetail");
  root.replaceChildren();
  root.hidden = false;
  appendText(
    root,
    "h3",
    `${item.source_symbol} · ${item.before_profile_revision} → ${item.after_profile_revision}`,
  );
  const meta = document.createElement("dl");
  meta.className = "profile-detail-meta";
  for (const [label, value] of [
    [
      "实际变化类型",
      (item.change_types || [])
        .map((type) => changeTypeLabels[type] || type)
        .join("、"),
    ],
    [
      "Core/Profile语义可比",
      `${item.core_compatible ? "Core可比" : "Core不可比"} · ${item.profile_compatible ? "Profile可比" : "Profile不可比"}`,
    ],
    [
      "官方会话关系",
      sessionRelationLabel(state.changeBinding?.session_relation),
    ],
    [
      "来源身份与坐标",
      `${text(item.before_identity_status)} → ${text(item.after_identity_status)} · ${text(item.before_price_coordinate)} → ${text(item.after_price_coordinate)} · security_id ${text(item.after_stable_security_id_status)} · 上市 ${text(item.after_listing_status)} · 停牌 ${text(item.after_suspension_status)} · PIT ${text(item.after_pit_status)}`,
    ],
    [
      "收盘 / MA20关系",
      `起始快照收盘 ${text(item.before_raw_close)} (${text(item.before_raw_price_quality)} / ${text(item.before_raw_price_reason)}) vs MA20 ${text(item.before_ma20)} (${text(item.before_ma20_quality)} / ${text(item.before_ma20_reason)}) → ${text(item.before_close_ma20_relation)}；结束快照收盘 ${text(item.after_raw_close)} (${text(item.after_raw_price_quality)} / ${text(item.after_raw_price_reason)}) vs MA20 ${text(item.after_ma20)} (${text(item.after_ma20_quality)} / ${text(item.after_ma20_reason)}) → ${text(item.after_close_ma20_relation)}`,
    ],
    [
      "目标Bar / 窗口",
      `${text(item.before_bar_status)} → ${text(item.after_bar_status)} · ${text(item.before_window_status)} ${text(item.before_window_reason)} → ${text(item.after_window_status)} ${text(item.after_window_reason)}`,
    ],
    ["选股状态", "NOT_EVALUATED：来源变化观察不构成入选或未入选判定"],
  ])
    readinessDetail(meta, label, text(value));
  root.append(meta);
  appendChangeTimelineLinks(root, item);
  appendText(root, "h3", "Profile11状态、质量与UNKNOWN原因");
  if (!item.profile_compatible) {
    appendText(
      root,
      "p",
      "字段身份或Profile语义不兼容，不能比较前后状态。",
      "muted",
    );
  } else {
    const table = document.createElement("table");
    table.innerHTML =
      "<thead><tr><th>字段</th><th>前日状态 / 质量 / 原因</th><th>后日状态 / 质量 / 原因</th></tr></thead>";
    const body = document.createElement("tbody");
    (item.before_profile_field_ids || []).forEach((field, index) =>
      tableRow(body, [
        field,
        `${text(item.before_profile_statuses?.[index])} / ${text(item.before_profile_qualities?.[index])} / ${text(item.before_profile_reasons?.[index])}`,
        `${text(item.after_profile_statuses?.[index])} / ${text(item.after_profile_qualities?.[index])} / ${text(item.after_profile_reasons?.[index])}`,
      ]),
    );
    table.append(body);
    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    wrap.append(table);
    root.append(wrap);
  }
  appendText(root, "h3", "Core质量或原因变化");
  if (item.core_quality_changes?.length) {
    const table = document.createElement("table");
    table.innerHTML =
      "<thead><tr><th>字段</th><th>前日质量 / 原因</th><th>后日质量 / 原因</th></tr></thead>";
    const body = document.createElement("tbody");
    for (const change of item.core_quality_changes)
      tableRow(body, [
        change.field_id,
        `${text(change.before_quality)} / ${text(change.before_reason)}`,
        `${text(change.after_quality)} / ${text(change.after_reason)}`,
      ]);
    table.append(body);
    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    wrap.append(table);
    root.append(wrap);
  } else appendText(root, "p", "此对象没有Core质量或原因变化。", "muted");
  root.scrollIntoView({ behavior: "smooth", block: "start" });
}
function populateChangeSnapshots(snapshots) {
  const complete = snapshots
    .filter((item) => item.complete)
    .slice()
    .sort((a, b) => a.target_session.localeCompare(b.target_session));
  for (const id of ["changeBeforeSelect", "changeAfterSelect"]) {
    const select = $(id);
    select.replaceChildren(new Option("— 选择完整PG画像快照 —", ""));
    for (const item of complete)
      select.append(
        new Option(
          `${item.target_session} · ${item.source_symbol_count}/${item.expected_source_symbol_count} · rev ${item.profile_revision.slice(0, 10)}`,
          item.target_session,
        ),
      );
    select.disabled = complete.length < 2;
  }
  if (complete.length >= 2) {
    $("changeBeforeSelect").value = complete.at(-2).target_session;
    $("changeAfterSelect").value = complete.at(-1).target_session;
    $("searchChanges").disabled = false;
    loadChanges();
  } else {
    $("searchChanges").disabled = true;
    $("changeSummary").textContent =
      "需要两个不同日期且分母闭合的已保存画像版本才能比较。";
    $("changeHighlightsCount").textContent = "等待精确快照";
    $("changeHighlightsSummary").textContent =
      "需要两个不同日期且分母闭合的已保存画像版本；未评价不表示没有变化。";
    $("changeHighlightsRows").replaceChildren();
    tableRow($("changeHighlightsRows"), [
      "等待精确的两日画像版本。",
      "",
      "",
      "",
      "",
      "",
    ]);
  }
}
async function loadChanges() {
  if (state.timelineContext?.origin === "change") invalidateTimeline();
  state.changeBinding = null;
  const before = $("changeBeforeSelect").value;
  const after = $("changeAfterSelect").value;
  const requestId = ++state.changeRequest;
  resetChangeDetail();
  $("changeRows").replaceChildren();
  $("changeHighlightsRows").replaceChildren();
  $("changeHighlightsCount").textContent = "查询中";
  $("changeHighlightsSummary").textContent =
    "正在读取所选两日的完整PG变化全集…";
  if (!before || !after || before >= after) {
    $("changeSummary").textContent = "请选择递增的起始与结束画像日期。";
    $("changeHighlightsCount").textContent = "等待精确快照";
    $("changeHighlightsSummary").textContent =
      "请选择两个递增且完整的PG画像快照。";
    tableRow($("changeHighlightsRows"), [
      "等待精确的两日画像版本。",
      "",
      "",
      "",
      "",
      "",
    ]);
    $("changePageStatus").textContent = "—";
    $("changePrev").disabled =
      $("changeNext").disabled =
      $("changeLast").disabled =
        true;
    return;
  }
  const params = new URLSearchParams({
    before_session: before,
    after_session: after,
    offset: String(state.changeOffset),
    limit: String(state.changeLimit),
    change_type: $("changeTypeSelect").value,
  });
  const symbol = $("changeSymbolInput").value.trim();
  if (symbol) params.set("symbol", symbol);
  $("changeSummary").textContent = "正在比较精确的PG画像来源版本…";
  $("searchChanges").disabled = true;
  try {
    const data = await api(
      `/api/source-symbol-price-profile-changes?${params}`,
    );
    if (requestId !== state.changeRequest) return;
    const s = data.summary;
    const p = data.page;
    state.changeBinding = {
      namespace_id: data.namespace_id,
      before_snapshot: data.before_snapshot,
      after_snapshot: data.after_snapshot,
      before_session: before,
      after_session: after,
      session_relation: data.session_relation,
      offset: p.offset,
      limit: p.limit,
      change_type: $("changeTypeSelect").value,
      symbol: $("changeSymbolInput").value.trim(),
    };
    renderChangeHighlights(data, data.session_relation);
    state.changeTotal = p.total;
    $("changeCount").textContent = `${s.changed_source_symbol_count}个变化对象`;
    $("changeSummary").textContent =
      `${sessionRelationLabel(data.session_relation)} · 快照分母 ${data.before_snapshot.source_symbol_count}/${data.before_snapshot.expected_source_symbol_count} 与 ${data.after_snapshot.source_symbol_count}/${data.after_snapshot.expected_source_symbol_count} · 并集 ${s.source_symbol_union_count} · 交集 ${s.intersection_count} · 起始独有 ${s.before_only_count} · 结束独有 ${s.after_only_count} · 跨source_set ${s.cross_source_set_pair_count} · 跨价格坐标 ${s.cross_price_coordinate_pair_count} · Core/Profile不可比 ${s.core_semantics_incomparable_count}/${s.profile_semantics_incomparable_count} · 收盘/MA20未知 ${s.close_ma20_unknown_count} · Profile未观察 ${s.profile_unobserved_pair_count} · 完整变化 ${s.changed_source_symbol_count} · 当前筛选 ${p.total} · 选股未评价。`;
    $("changePageStatus").textContent =
      `偏移 ${p.offset} · 本页 ${p.count}/${p.limit} · 筛选总计 ${p.total}`;
    $("changePrev").disabled = p.offset === 0;
    $("changeNext").disabled = p.next_offset === null;
    $("changeLast").disabled = p.total === 0 || p.offset + p.limit >= p.total;
    if (!data.rows.length)
      tableRow($("changeRows"), [
        "没有匹配的实际变化；可在下方画像搜索中逐股查看来源池状态和UNKNOWN原因。",
        "",
        "",
        "",
        "",
        "",
      ]);
    for (const item of data.rows) {
      const row = document.createElement("tr");
      cell(row, item.source_symbol);
      cell(
        row,
        (item.change_types || [])
          .map((type) => changeTypeLabels[type] || type)
          .join("、"),
      );
      cell(
        row,
        `${text(item.before_close_ma20_relation)} → ${text(item.after_close_ma20_relation)}`,
      );
      cell(
        row,
        item.profile_compatible
          ? (item.profile_field_changes || [])
              .map((change) => change.field_id)
              .join("、") || "无"
          : "不可比",
      );
      cell(
        row,
        (item.core_quality_changes || [])
          .map((change) => change.field_id)
          .join("、") || "无",
      );
      const detail = document.createElement("td");
      const button = document.createElement("button");
      button.type = "button";
      button.className = "quiet";
      button.textContent = "详情 / 原因";
      button.addEventListener("click", () => showChangeDetail(item));
      detail.append(button);
      row.append(detail);
      $("changeRows").append(row);
    }
  } catch (error) {
    if (requestId !== state.changeRequest) return;
    $("changeHighlightsCount").textContent = "摘要查询失败";
    $("changeHighlightsSummary").textContent =
      `PG重点变化摘要不可用：${error.message}`;
    tableRow($("changeHighlightsRows"), [error.message, "", "", "", "", ""]);
    tableRow($("changeRows"), [error.message, "", "", "", "", ""]);
    $("changeCount").textContent = "变化查询失败";
    $("changeSummary").textContent = `PG变化查询未完成：${error.message}`;
    $("changePageStatus").textContent = "查询失败";
    $("changePrev").disabled =
      $("changeNext").disabled =
      $("changeLast").disabled =
        true;
  } finally {
    if (requestId === state.changeRequest) $("searchChanges").disabled = false;
  }
}
async function loadProfileSnapshots() {
  state.profileRequest++;
  $("profileSnapshotSelect").disabled = true;
  $("profileSnapshotSelect").replaceChildren(
    new Option("正在读取PG画像目录…", ""),
  );
  try {
    const data = await api("/api/source-symbol-price-profiles/snapshots");
    $("dataBoundary").textContent =
      `研究冻结于${data.research_cutoff}。当前页面展示来源候选，不代表官方上市总体、稳定证券身份、交易状态或选股结论；自动日更、Head发布及生产切换暂停。`;
    state.profileSnapshots = data.snapshots || [];
    populateChangeSnapshots(state.profileSnapshots);
    $("profileSnapshotSelect").replaceChildren(
      new Option("— 请选择精确画像日期 / source_set / revision —", ""),
    );
    for (const item of state.profileSnapshots) {
      const key = `${item.target_session}|${item.source_set_id}|${item.profile_revision}`;
      const option = new Option(
        `${item.target_session} · ${item.source_set_id.slice(0, 8)} · ${item.source_symbol_count}/${item.expected_source_symbol_count} · ${item.history_session_count}会话 · rev ${item.profile_revision.slice(0, 10)}`,
        key,
      );
      option.disabled = !item.complete;
      $("profileSnapshotSelect").append(option);
    }
    $("profileSnapshotSelect").disabled = state.profileSnapshots.length === 0;
    $("profileCount").textContent = state.profileSnapshots.length
      ? `${state.profileSnapshots.length}个真实PG画像版本`
      : "没有已保存画像";
    $("profileMessage").textContent = state.profileSnapshots.length
      ? "请选择精确日期、P03 source_set和窗口修订。搜索仅读取PG持久画像，不回退latest。"
      : "PG画像表中没有已保存记录。";
  } catch (error) {
    $("dataBoundary").textContent =
      "研究截止信息读取失败，无法确认当前页面的数据范围；自动日更、Head发布及生产切换暂停。";
    state.profileSnapshots = [];
    populateChangeSnapshots([]);
    $("profileCount").textContent = "PG画像目录不可用";
    $("profileMessage").textContent = `画像目录读取失败：${error.message}`;
  }
}
async function loadProfilePage() {
  const snapshot = selectedProfileSnapshot();
  const requestId = ++state.profileRequest;
  clearProfileDetail();
  $("profileRows").replaceChildren();
  if (!snapshot) {
    $("profileMessage").textContent =
      "请先选择一个完整、已持久化的精确画像版本。";
    $("profilePageStatus").textContent = "—";
    $("profilePrev").disabled = true;
    $("profileNext").disabled = true;
    $("profileLast").disabled = true;
    return;
  }
  const params = new URLSearchParams({
    target_session: snapshot.target_session,
    source_set_id: snapshot.source_set_id,
    package_sha256: snapshot.package_sha256,
    profile_revision: snapshot.profile_revision,
    offset: String(state.profileOffset),
    limit: String(state.profileLimit),
    market: $("profileMarketSelect").value,
    board: $("profileBoardSelect").value,
    target_bar_status: $("profileBarStatusSelect").value,
    price_window_status: $("profileWindowStatusSelect").value,
  });
  const symbol = $("profileSymbolInput").value.trim();
  if (symbol) params.set("symbol", symbol);
  $("profileMessage").textContent =
    "正在按精确日期、来源集、包SHA和窗口修订读取PG分页…";
  try {
    const data = await api(`/api/source-symbol-price-profiles?${params}`);
    if (requestId !== state.profileRequest) return;
    const page = data.page;
    state.profileTotal = page.total;
    $("profileCount").textContent = `${page.count}条 / 筛选分母${page.total}`;
    $("profileMessage").textContent =
      `${data.target_session} · P03 ${data.source_set_id} · 窗口 ${data.history_window.start} 至 ${data.history_window.end}（${data.history_window.requested_session_count}会话） · NON_PIT来源代码研究，不是正式上市Universe。`;
    $("profilePageStatus").textContent =
      `偏移 ${page.offset} · 本页 ${page.count}/${page.limit} · 总计 ${page.total}`;
    $("profilePrev").disabled = page.offset === 0;
    $("profileNext").disabled = page.next_offset === null;
    $("profileLast").disabled =
      page.total === 0 || page.offset + page.limit >= page.total;
    for (const item of data.rows) {
      const tr = document.createElement("tr");
      cell(tr, item.source_symbol);
      cell(tr, `${item.market} / ${text(item.board)}`);
      cell(
        tr,
        `${text(item.target_bar_status)} · ${text(item.target_raw_price_quality)}`,
      );
      cell(
        tr,
        `${item.history_stored_session_count}/${item.history_requested_session_count}行 · ${item.history_actual_price_bar_count}有效Bar · ${text(item.history_records_status)}`,
      );
      cell(tr, `${text(item.ma20)} · ${text(item.ma20_quality)}`);
      cell(tr, `${text(item.atr20)} · ${text(item.atr20_quality)}`);
      cell(tr, `${text(item.clv)} · ${text(item.clv_quality)}`);
      cell(
        tr,
        `${text(item.price_window_status)}${item.price_window_reason ? ` · ${item.price_window_reason}` : ""}`,
      );
      const detailCell = document.createElement("td");
      const detailButton = document.createElement("button");
      detailButton.type = "button";
      detailButton.className = "quiet";
      detailButton.textContent = "详情";
      detailButton.addEventListener("click", () => showProfileDetail(item));
      detailCell.append(detailButton);
      tr.append(detailCell);
      $("profileRows").append(tr);
    }
  } catch (error) {
    if (requestId !== state.profileRequest) return;
    tableRow($("profileRows"), [
      "没有匹配的PG画像",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
    ]);
    $("profileMessage").textContent = `画像查询未返回记录：${error.message}`;
    $("profileCount").textContent = "0条匹配记录";
    $("profilePageStatus").textContent = "无匹配结果";
    $("profilePrev").disabled = true;
    $("profileNext").disabled = true;
    $("profileLast").disabled = true;
  }
}
$("reloadCatalog").addEventListener("click", loadCatalog);
$("versionSelect").addEventListener("change", () => {
  state.offset = 0;
  state.stockRequest++;
  if (!state.timelineNavigation) invalidateTimeline();
  $("queryStock").disabled = false;
  clearResult();
  loadPage();
  loadReadiness();
});
for (const id of ["symbolInput", "windowInput"]) {
  $(id).addEventListener("input", () => {
    state.stockRequest++;
    $("queryStock").disabled = false;
    if (!state.timelineNavigation) invalidateTimeline();
  });
}
$("marketSelect").addEventListener("change", () => {
  state.offset = 0;
  loadPage();
});
$("boardSelect").addEventListener("change", () => {
  state.offset = 0;
  loadPage();
});
$("prevPage").addEventListener("click", () => {
  state.offset = Math.max(0, state.offset - state.limit);
  loadPage();
});
$("nextPage").addEventListener("click", () => {
  if (state.offset + state.limit < state.total) {
    state.offset += state.limit;
    loadPage();
  }
});
$("stockForm").addEventListener("submit", queryStock);
$("queryTimeline").addEventListener("click", queryTimeline);
$("chartModeDay").addEventListener("click", () => setChartMode("day"));
$("chartModeWeek").addEventListener("click", () => setChartMode("week"));
$("chartModeMonth").addEventListener("click", () => setChartMode("month"));
$("searchProfiles").addEventListener("click", () => {
  state.profileOffset = 0;
  loadProfilePage();
});
$("profileSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    state.profileOffset = 0;
    loadProfilePage();
  }
});
$("profileSnapshotSelect").addEventListener("change", () => {
  state.profileOffset = 0;
  loadProfilePage();
});
for (const id of [
  "profileMarketSelect",
  "profileBoardSelect",
  "profileBarStatusSelect",
  "profileWindowStatusSelect",
]) {
  $(id).addEventListener("change", () => {
    state.profileOffset = 0;
    loadProfilePage();
  });
}
$("profilePrev").addEventListener("click", () => {
  state.profileOffset = Math.max(0, state.profileOffset - state.profileLimit);
  loadProfilePage();
});
$("profileNext").addEventListener("click", () => {
  if (state.profileOffset + state.profileLimit < state.profileTotal) {
    state.profileOffset += state.profileLimit;
    loadProfilePage();
  }
});
$("profileLast").addEventListener("click", () => {
  if (state.profileTotal) {
    state.profileOffset =
      Math.floor((state.profileTotal - 1) / state.profileLimit) *
      state.profileLimit;
    loadProfilePage();
  }
});
$("searchChanges").addEventListener("click", () => {
  state.changeOffset = 0;
  loadChanges();
});
$("changeSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    state.changeOffset = 0;
    loadChanges();
  }
});
for (const id of [
  "changeBeforeSelect",
  "changeAfterSelect",
  "changeTypeSelect",
]) {
  $(id).addEventListener("change", () => {
    state.changeOffset = 0;
    loadChanges();
  });
}
$("changePrev").addEventListener("click", () => {
  state.changeOffset = Math.max(0, state.changeOffset - state.changeLimit);
  loadChanges();
});
$("changeNext").addEventListener("click", () => {
  if (state.changeOffset + state.changeLimit < state.changeTotal) {
    state.changeOffset += state.changeLimit;
    loadChanges();
  }
});
$("changeLast").addEventListener("click", () => {
  if (state.changeTotal) {
    state.changeOffset =
      Math.floor((state.changeTotal - 1) / state.changeLimit) *
      state.changeLimit;
    loadChanges();
  }
});
$("priceRelationFieldFilter").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
$("priceRelationKindFilter").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
$("showPriceRelationUnknown").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
loadProfileSnapshots();
loadCatalog();
