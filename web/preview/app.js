"use strict";
const $ = (id) => document.getElementById(id);
const state = {
  versions: [],
  namespace_id: null,
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
  profileFieldSummary: null,
  changeOffset: 0,
  changeLimit: 30,
  changeTotal: 0,
  changeRequest: 0,
  pairRequest: 0,
  relationRequest: 0,
  relationPageRequest: 0,
  relationOffset: 0,
  relationLimit: 30,
  relationTotal: 0,
  relationData: null,
  relationQuery: null,
  relationDetailRequest: 0,
  relationDetailRow: null,
  relationDetailBindingKey: null,
  comparisonBasket: [],
  comparisonMessage: null,
  comparisonRequest: 0,
  comparisonTimelines: [],
  comparisonBaseSession: null,
  comparisonPeriodEntries: [],
  comparisonPeriodKind: "WEEK",
  comparisonPeriodBaseKey: null,
  comparisonReturnForm: null,
  comparisonEventWindow: 5,
  comparisonEventField: "ALL",
  comparisonEventKind: "ALL",
  profileBreadthRequest: 0,
  profileBreadthData: null,
  profileBreadthBinding: null,
  profileBreadthPendingKey: null,
  profileBreadthMemberRequest: 0,
  profileBreadthMemberData: null,
  profileBreadthMemberQuery: null,
  profileBreadthMemberOffset: 0,
  profileBreadthMemberLimit: 30,
  profileBreadthMemberReturn: null,
  changeSnapshots: [],
  changeBinding: null,
  changeDetailItem: null,
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
const previewQueryDebounceMs = 160;
let relationRefreshTimer = null;
let relationRefreshPending = false;
let relationRequestInFlight = false;
let profileSearchTimer = null;
let changeRefreshTimer = null;
let changeRefreshPending = false;
let changeRequestInFlight = false;
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
  RAW_ROW_MISSING: "PG未保存目标日Raw行",
  TARGET_BAR_MISSING: "目标会话缺少Bar",
  RAW_PRICE_QUALITY_UNKNOWN: "Raw收盘质量未知",
  PRICE_COORDINATE_UNSUPPORTED: "价格坐标不支持",
  PRICE_COORDINATE_INCOMPATIBLE: "两端价格坐标不兼容",
  CORE_SEMANTICS_UNKNOWN: "Core语义缺失",
  CORE_SEMANTICS_INCOMPATIBLE: "Core语义不兼容",
  CURRENT_SESSION_UNKNOWN: "当前交易会话数据未知",
  ZERO_PRICE_RANGE: "目标日最高价与最低价相同",
  MA20_QUALITY_UNKNOWN: "MA20质量未知",
  PRICE_VALUE_MISSING: "价格值缺失",
  RELATION_UNKNOWN: "关系未知",
  RAW_CLOSE_QUALITY_UNKNOWN: "Raw收盘质量未知",
  SOURCE_LABEL_CHANGED: "来源市场或板别标签变化",
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
const profileCoreFields = [
  { value: "core.ma5", key: "ma5", label: "MA5" },
  { value: "core.ma10", key: "ma10", label: "MA10" },
  { value: "core.ma20", key: "ma20", label: "MA20" },
  { value: "core.ma60", key: "ma60", label: "MA60" },
  { value: "core.atr20", key: "atr20", label: "ATR20" },
  { value: "core.clv", key: "clv", label: "CLV" },
  {
    value: "core.prior_high20",
    key: "prior_high20",
    label: "前20会话最高价",
  },
  { value: "core.ret20", key: "ret20", label: "20会话收益率" },
  { value: "core.pos60", key: "pos60", label: "60会话价格位置" },
  { value: "core.bias20_atr", key: "bias20_atr", label: "相对MA20的ATR偏离" },
];
const profileFieldLabels = {
  "profile.closed_month_trend": "闭月趋势",
  "profile.closed_week_trend": "闭周趋势",
  "profile.compression": "压缩状态",
  "profile.drawdown60": "60会话回撤",
  "profile.extension": "延展状态",
  "profile.ma_structure": "均线结构",
  "profile.near_high20": "接近前高",
  "profile.participation": "参与度",
  "profile.position": "价格位置",
  "profile.relative": "相对市场",
  "profile.trend": "趋势状态",
};
const pricePositionDefinitions = {
  c_vs_ma20: { label: "收盘 vs MA20", left: "RAW收盘", right: "MA20" },
  c_vs_prior_high20: {
    label: "收盘 vs 严格前20会话高点",
    left: "RAW收盘",
    right: "严格前20会话高点",
  },
  ma5_vs_ma20: { label: "MA5 vs MA20", left: "MA5", right: "MA20" },
};
const pricePositionStatusLabels = {
  ABOVE: "高于",
  EQUAL: "等于",
  BELOW: "低于",
  UNKNOWN: "未知",
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
  $("stockProfileRows").replaceChildren();
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
  state.namespace_id = null;
  $("versionSelect").disabled = true;
  $("versionSelect").replaceChildren(new Option("正在读取PG目录…", ""));
  $("population").textContent = "正在从PostgreSQL只读读取来源日期与版本目录。";
  try {
    const data = await api("/api/preview/catalog?offset=0&limit=100");
    if (catalogRequest !== state.catalogRequest) return;
    state.namespace_id = data.namespace_id;
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
    return false;
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
    return false;
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
    $("stockProfileRows").replaceChildren();
    for (const component of result.profile_preview.components) {
      const componentKey = component.field_id || "";
      tableRow($("stockProfileRows"), [
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
        returnBinding: previousTimelineContext?.returnBinding || null,
        persistedProfileSnapshot:
          previousTimelineContext?.persistedProfileSnapshot || null,
      });
    }
    return true;
  } catch (error) {
    if (!requestIsCurrent()) return;
    $("stockResult").hidden = Boolean(!keepTimeline || !state.timeline);
    setMessage(error.message);
    if (keepTimeline && state.timeline) renderTimeline(state.timeline);
    return false;
  } finally {
    if (requestIsCurrent()) $("queryStock").disabled = false;
  }
}
function selectedProfileSnapshot() {
  const key = $("profileSnapshotSelect").value;
  return state.profileSnapshots.find(
    (item) => profileSnapshotKey(item) === key,
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
function comparisonIdentityOf(item) {
  return {
    target_session: item.target_session,
    namespace_id: item.namespace_id,
    source_set_id: item.source_set_id,
    package_sha256: item.package_sha256,
    profile_revision: item.profile_revision,
  };
}
function sameComparisonIdentity(left, right) {
  if (!left || !right) return false;
  return [
    "target_session",
    "namespace_id",
    "source_set_id",
    "package_sha256",
    "profile_revision",
  ].every((key) => left[key] === right[key]);
}
function relationComparisonCandidate(item) {
  const data = state.relationData;
  const snapshot = data?.after_snapshot;
  if (!snapshot || !data.namespace_id) return null;
  return {
    source_symbol: item.source_symbol,
    target_session: snapshot.target_session,
    namespace_id: data.namespace_id,
    source_set_id: item.after_source_set_id,
    package_sha256: item.after_package_sha256,
    profile_revision: item.after_profile_revision,
    origin_label: "两日RAW结构结果",
  };
}
function addComparisonCandidate(candidate) {
  if (
    !candidate?.source_symbol ||
    !candidate.target_session ||
    !candidate.namespace_id ||
    !candidate.source_set_id ||
    !/^[0-9a-f]{64}$/.test(candidate.package_sha256 || "") ||
    !/^[0-9a-f]{64}$/.test(candidate.profile_revision || "") ||
    !/^(SH|SZ)\.[0-9]{6}$/.test(candidate.source_symbol)
  ) {
    state.comparisonMessage =
      "来源行缺少完整SEALED身份或不是SH./SZ.来源代码，未加入。";
    renderComparisonBasket();
    return;
  }
  const identity = comparisonIdentityOf(candidate);
  const currentIdentity = state.comparisonBasket[0]
    ? comparisonIdentityOf(state.comparisonBasket[0])
    : null;
  if (currentIdentity && !sameComparisonIdentity(identity, currentIdentity)) {
    state.comparisonMessage =
      "该来源属于不同目标日、namespace、source_set、包SHA或画像修订；请先清空对照篮再选。";
    renderComparisonBasket();
    return;
  }
  if (
    state.comparisonBasket.some(
      (item) => item.source_symbol === candidate.source_symbol,
    )
  ) {
    state.comparisonMessage = `${candidate.source_symbol} 已在临时对照篮中。`;
    renderComparisonBasket();
    return;
  }
  if (state.comparisonBasket.length >= 3) {
    state.comparisonMessage = "临时对照最多选择3个不同来源代码；请先移除一只。";
    renderComparisonBasket();
    return;
  }
  state.comparisonBasket.push({ ...candidate });
  state.comparisonMessage =
    `已加入 ${candidate.source_symbol}（${candidate.origin_label}）；选择身份固定为 ` +
    `${candidate.target_session} / ${candidate.source_set_id} / ${candidate.profile_revision}。`;
  invalidateComparisonResults(
    "选择已变化；请重新读取同一快照下的75个官方会话。",
  );
  renderComparisonBasket();
}
function makeComparisonAddButton(candidate) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "quiet";
  button.textContent = "加入对照";
  button.disabled = !candidate;
  if (!candidate) button.title = "此行没有完整SEALED画像身份。";
  button.addEventListener("click", () => addComparisonCandidate(candidate));
  return button;
}
function invalidateComparisonResults(message) {
  state.comparisonRequest++;
  state.comparisonTimelines = [];
  state.comparisonBaseSession = null;
  state.comparisonPeriodEntries = [];
  state.comparisonPeriodBaseKey = null;
  $("comparisonResults").hidden = true;
  $("comparisonEventHistory").hidden = true;
  $("comparisonIdentity").replaceChildren();
  $("comparisonCards").replaceChildren();
  $("comparisonHead").replaceChildren();
  $("comparisonRows").replaceChildren();
  $("comparisonPeriodHead").replaceChildren();
  $("comparisonPeriodRows").replaceChildren();
  $("comparisonPeriodCount").textContent = "等待周期";
  $("comparisonPeriodStatus").textContent = message;
  $("comparisonEventSummaryRows").replaceChildren();
  $("comparisonEventListRows").replaceChildren();
  $("comparisonEventMatrixHead").replaceChildren();
  $("comparisonEventMatrixRows").replaceChildren();
  $("comparisonStatus").textContent = message;
}
function renderComparisonBasket() {
  const root = $("comparisonBasket");
  root.replaceChildren();
  const basket = state.comparisonBasket;
  $("comparisonCount").textContent = `${basket.length}/3`;
  $("compareSymbols").disabled = basket.length < 2 || basket.length > 3;
  $("clearComparison").disabled =
    basket.length === 0 && state.comparisonTimelines.length === 0;
  $("comparisonNotice").textContent =
    state.comparisonMessage ||
    (basket.length
      ? `同一快照 ${basket[0].target_session} · namespace ${basket[0].namespace_id} · source_set ${basket[0].source_set_id} · SHA ${basket[0].package_sha256} · profile_revision ${basket[0].profile_revision}。`
      : "从两日RAW关系结果或完整PG画像页加入同一来源快照的代码。");
  if (!basket.length) {
    appendText(root, "span", "尚未选择来源代码。", "muted");
    return;
  }
  for (const item of basket) {
    const chip = document.createElement("div");
    chip.className = "comparison-item";
    appendText(chip, "strong", item.source_symbol);
    appendText(chip, "span", item.origin_label, "muted");
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "quiet";
    remove.textContent = "移除";
    remove.addEventListener("click", () =>
      removeComparisonCandidate(item.source_symbol),
    );
    chip.append(remove);
    root.append(chip);
  }
}
function removeComparisonCandidate(sourceSymbol) {
  state.comparisonBasket = state.comparisonBasket.filter(
    (item) => item.source_symbol !== sourceSymbol,
  );
  state.comparisonMessage = `已移除 ${sourceSymbol}；原筛选条件和分页保持不变。`;
  invalidateComparisonResults(
    "选择已变化；请重新读取同一快照下的75个官方会话。",
  );
  renderComparisonBasket();
}
function clearComparisonBasket(
  message = "临时对照已清空；筛选结果和分页保持不变。",
) {
  if (state.timelineContext?.origin === "comparison") returnFromComparison();
  state.comparisonBasket = [];
  state.comparisonMessage = message;
  invalidateComparisonResults(message);
  renderComparisonBasket();
}
function preserveComparisonOnSnapshotChange(snapshot, sourceLabel) {
  if (!state.comparisonBasket.length) return;
  if (
    snapshot &&
    sameComparisonIdentity(
      snapshot,
      comparisonIdentityOf(state.comparisonBasket[0]),
    )
  )
    return;
  state.comparisonMessage = `${sourceLabel}已切换；临时对照仍绑定原完整快照身份。不同身份的来源代码会被拒绝，原篮和结果保持不变。`;
  renderComparisonBasket();
}
function exactFraction(raw) {
  if (raw === null || raw === undefined) return null;
  const source = String(raw).trim();
  const ratio = source.match(/^([+-]?\d+)\s*\/\s*([+-]?\d+)$/);
  if (ratio) {
    const denominator = BigInt(ratio[2]);
    if (denominator === 0n) return null;
    const sign = denominator < 0n ? -1n : 1n;
    return {
      numerator: BigInt(ratio[1]) * sign,
      denominator: denominator * sign,
    };
  }
  const decimal = source.match(/^([+-]?)(\d+)(?:\.(\d*))?(?:e([+-]?\d+))?$/i);
  if (!decimal) return null;
  const digits = `${decimal[2]}${decimal[3] || ""}`;
  let numerator = BigInt(digits || "0");
  if (decimal[1] === "-") numerator = -numerator;
  let scale = (decimal[3] || "").length - Number(decimal[4] || 0);
  if (!Number.isSafeInteger(scale) || Math.abs(scale) > 100) return null;
  if (scale < 0) {
    numerator *= 10n ** BigInt(-scale);
    scale = 0;
  }
  return { numerator, denominator: 10n ** BigInt(scale) };
}
function rawCloseFraction(field) {
  if (!field || field.quality !== "OBSERVED") return null;
  const value = field.value;
  const exact =
    value && typeof value === "object" ? (value.exact ?? value.decimal) : value;
  const fraction = exactFraction(exact);
  return fraction?.numerator > 0n ? fraction : null;
}
function normalizedIndexFraction(close, base) {
  if (!close || !base || base.numerator <= 0n) return null;
  return {
    numerator: 100n * close.numerator * base.denominator,
    denominator: close.denominator * base.numerator,
  };
}
function formatIndexFraction(fraction) {
  if (!fraction || fraction.numerator < 0n || fraction.denominator <= 0n)
    return "UNKNOWN";
  const doubledCents = fraction.numerator * 200n;
  const cents =
    (doubledCents + fraction.denominator) / (2n * fraction.denominator);
  const whole = cents / 100n;
  const decimal = String(cents % 100n).padStart(2, "0");
  return `${whole}.${decimal}`;
}
function comparisonPeriodCloseFraction(period) {
  if (
    !period ||
    period.calendar_closed !== true ||
    period.coverage_status !== "COMPLETE" ||
    period.reason_codes?.length ||
    period.valid_price_bar_count !== period.calendar_expected_session_count ||
    period.stored_raw_row_count !== period.calendar_expected_session_count ||
    period.elapsed_expected_session_count !==
      period.calendar_expected_session_count ||
    period.window_truncated_session_count !== 0 ||
    period.target_missing_count !== 0 ||
    period.read_failed_count !== 0 ||
    period.invalid_price_count !== 0 ||
    period.missing_history_count !== 0
  )
    return null;
  const close = period.ohlc?.close;
  const fraction = exactFraction(close?.exact ?? close?.decimal);
  return fraction?.numerator > 0n ? fraction : null;
}
function comparisonPeriodMetadataMatches(left, right) {
  return (
    left?.calendar_ordinal === right?.calendar_ordinal &&
    left?.calendar_start_session === right?.calendar_start_session &&
    left?.calendar_end_session === right?.calendar_end_session &&
    left?.calendar_expected_session_count ===
      right?.calendar_expected_session_count &&
    left?.elapsed_expected_session_count ===
      right?.elapsed_expected_session_count
  );
}
function comparisonPeriodAxis(entries, selectionCount, periodKind) {
  const periodName = periodKind === "WEEK" ? "week" : "month";
  const periodsByEntry = entries.map((entry) => {
    const periods = entry.data?.periods?.[periodName] || [];
    return new Map(periods.map((period) => [period.period_key, period]));
  });
  const keys = new Set(
    periodsByEntry.flatMap((periods) => [...periods.keys()]),
  );
  const rows = [...keys]
    .map((key) => {
      const periods = periodsByEntry.map((periods) => periods.get(key) || null);
      const reference = periods.find(Boolean);
      const aligned =
        entries.length === selectionCount &&
        periods.every(
          (period) =>
            period && comparisonPeriodMetadataMatches(period, reference),
        );
      return {
        key,
        periods,
        metadata: reference,
        aligned,
      };
    })
    .sort((left, right) => {
      const ordinalDifference =
        left.metadata.calendar_ordinal - right.metadata.calendar_ordinal;
      if (ordinalDifference) return ordinalDifference;
      return left.key < right.key ? -1 : left.key > right.key ? 1 : 0;
    });
  const baseRow =
    entries.length === selectionCount && entries.every((entry) => entry.data)
      ? rows.find(
          (row) =>
            row.aligned &&
            row.periods.every((period) =>
              comparisonPeriodCloseFraction(period),
            ),
        ) || null
      : null;
  const bases = new Map();
  if (baseRow) {
    baseRow.periods.forEach((period, index) => {
      bases.set(
        entries[index].selection.source_symbol,
        comparisonPeriodCloseFraction(period),
      );
    });
  }
  return {
    rows: rows.map((row) => ({
      ...row,
      cells: entries.map((entry, index) => {
        const period = row.periods[index];
        const close = period?.calendar_closed
          ? exactFraction(
              period.ohlc?.close?.exact ?? period.ohlc?.close?.decimal,
            )
          : null;
        const validClose = comparisonPeriodCloseFraction(period);
        let reason = null;
        let indexFraction = null;
        if (!entry.data) {
          reason = entry.pending
            ? "正在读取该代码的周期数据"
            : `周期不可用：${entry.error || "75会话轨迹未返回"}`;
        } else if (!period) {
          reason = "来源未返回该自然周期";
        } else if (!row.aligned) {
          reason = "官方周期范围与篮内来源不一致，拒绝横向比较";
        } else if (!period.calendar_closed) {
          reason = `自然周期未闭合：${period.reason_codes.join("、") || "官方日历期末晚于目标日"}`;
        } else if (period.coverage_status !== "COMPLETE") {
          reason = `周期覆盖${period.coverage_status}：${period.reason_codes.join("、") || "来源覆盖不完整"}`;
        } else if (!validClose) {
          reason = "完整闭合周期没有合法RAW收盘价";
        } else if (!baseRow) {
          reason = "没有所有篮内代码共同完整闭合的基准周期";
        } else if (
          row.metadata.calendar_ordinal < baseRow.metadata.calendar_ordinal
        ) {
          reason = `早于共同基准${baseRow.key}`;
        } else {
          indexFraction = normalizedIndexFraction(
            validClose,
            bases.get(entry.selection.source_symbol),
          );
          if (!indexFraction) reason = "RAW收盘或共同基准不是正数";
        }
        return { entry, period, close, indexFraction, reason };
      }),
    })),
    baseRow,
    bases,
  };
}
function comparisonPeriodPriceText(value) {
  if (!value) return "UNKNOWN";
  return text(value.decimal ?? value.exact ?? "UNKNOWN");
}
function comparisonFieldText(field) {
  if (!field || field.quality !== "OBSERVED") {
    return `UNKNOWN · ${text(field?.reason || field?.quality || "字段未提供")}`;
  }
  const value = field.value;
  const displayed =
    value && typeof value === "object"
      ? (value.decimal ?? value.exact ?? "—")
      : (value ?? "—");
  return `${text(displayed)} · OBSERVED`;
}
function comparisonRelationText(relation, reason) {
  return relation
    ? `${priceRelationLabels[relation] || text(relation)}`
    : `UNKNOWN · ${text(reason || "关系字段未提供")}`;
}
function comparisonAxis(entries, selectionCount) {
  const valid = entries.filter((entry) => entry.data);
  const sessionDates = Array.from(
    new Set(
      valid.flatMap((entry) =>
        entry.data.points.map((point) => point.session_date),
      ),
    ),
  ).sort();
  const maps = new Map(
    valid.map((entry) => [
      entry.selection.source_symbol,
      new Map(entry.data.points.map((point) => [point.session_date, point])),
    ]),
  );
  let baseSession = null;
  if (valid.length === selectionCount && valid.length >= 2) {
    baseSession =
      sessionDates.find((session) =>
        valid.every((entry) =>
          rawCloseFraction(
            maps.get(entry.selection.source_symbol)?.get(session)?.raw_close,
          ),
        ),
      ) || null;
  }
  const bases = new Map();
  if (baseSession) {
    for (const entry of valid) {
      bases.set(
        entry.selection.source_symbol,
        rawCloseFraction(
          maps.get(entry.selection.source_symbol).get(baseSession)?.raw_close,
        ),
      );
    }
  }
  return { valid, sessionDates, maps, baseSession, bases };
}
function renderComparisonSummary(entry) {
  const card = document.createElement("article");
  card.className = "comparison-card";
  appendText(
    card,
    "h3",
    `${entry.selection.source_symbol} · ${entry.selection.target_session}`,
  );
  if (!entry.data) {
    appendText(card, "p", `75会话未返回：${entry.error}`, "message");
    return card;
  }
  const data = entry.data;
  const target = data.points.find(
    (point) => point.session_date === entry.selection.target_session,
  );
  appendText(
    card,
    "p",
    `RAW收盘 ${comparisonFieldText(target?.raw_close)} · Bar ${text(target?.raw_status)} · ${text(target?.price_reason || "无价格原因")}`,
  );
  appendText(
    card,
    "p",
    `MA20 ${comparisonFieldText(target?.core_ma20)} · C/MA20 ${comparisonRelationText(target?.close_vs_ma20, target?.relation_unknown_reason)}`,
  );
  appendText(
    card,
    "p",
    `严格前20高点 ${comparisonFieldText(target?.core_prior_high20)} · C/前高20 ${comparisonRelationText(target?.close_vs_prior_high20, target?.prior_high20_relation_unknown_reason)}`,
  );
  appendText(
    card,
    "p",
    `MA5/10/20/60排列 ${formatMovingAverageOrder(target?.moving_average_order)} · 有效Bar ${data.valid_price_bar_count}/${data.total_session_count}`,
  );
  appendText(
    card,
    "p",
    `namespace ${data.namespace_id} · source_set ${data.snapshot_identity.source_set_id} · package SHA ${data.package_sha256} · profile_revision ${entry.selection.profile_revision}`,
  );
  appendText(
    card,
    "p",
    `${data.price_coordinate} · ${data.identity_status} · ${data.pit_status} · 历史行 ${data.raw_row_count}/${data.total_session_count}`,
    "muted",
  );
  return card;
}
function comparisonEventFieldIds(entries) {
  const fieldIds = new Set();
  for (const entry of entries) {
    const history = entry.data?.price_relation_history;
    Object.keys(history?.field_statistics || {}).forEach((fieldId) =>
      fieldIds.add(fieldId),
    );
    (history?.observations || []).forEach((observation) => {
      if (observation.field_id) fieldIds.add(observation.field_id);
    });
  }
  const knownOrder = new Map(
    Object.keys(timelinePriceFieldLabels).map((fieldId, index) => [
      fieldId,
      index,
    ]),
  );
  return Array.from(fieldIds).sort((left, right) => {
    const leftRank = knownOrder.get(left) ?? Number.MAX_SAFE_INTEGER;
    const rightRank = knownOrder.get(right) ?? Number.MAX_SAFE_INTEGER;
    return leftRank - rightRank || left.localeCompare(right);
  });
}
function comparisonEventFieldLabel(fieldId) {
  const label = timelinePriceFieldLabels[fieldId] || fieldId;
  return label + "（" + fieldId + "）";
}
function comparisonEventIndexes(entries) {
  return entries.map((entry) => {
    const observations = new Map();
    for (const observation of entry.data?.price_relation_history
      ?.observations || []) {
      observations.set(
        observation.session_date + "|" + observation.field_id,
        observation,
      );
    }
    return { entry, observations };
  });
}
function comparisonEventFilterOptions(entries) {
  const eventKinds = new Set();
  for (const entry of entries) {
    for (const observation of entry.data?.price_relation_history
      ?.observations || []) {
      if (
        observation.comparison_status === "RELATION_CHANGED" &&
        observation.event_kind
      )
        eventKinds.add(observation.event_kind);
    }
  }
  const options = [
    { value: "ALL", label: "全部状态与事件" },
    { value: "RELATION_CHANGED", label: "所有关系变化" },
    { value: "NO_RELATION_CHANGE", label: "关系未变化" },
    { value: "NOT_COMPARABLE", label: "不可比 / UNKNOWN" },
    { value: "NOT_RECORDED", label: "未知 / 未记录" },
  ];
  for (const eventKind of Array.from(eventKinds).sort()) {
    options.push({
      value: "EVENT:" + eventKind,
      label:
        (priceRelationKindLabels[eventKind] || eventKind) +
        "（" +
        eventKind +
        "）",
    });
  }
  return options;
}
function setComparisonEventSelect(select, options, selectedValue) {
  select.replaceChildren();
  for (const item of options) {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    select.append(option);
  }
  select.value = options.some((item) => item.value === selectedValue)
    ? selectedValue
    : "ALL";
}
function comparisonEventFilterMatches(filter, observation) {
  if (filter === "ALL") return true;
  if (!observation) return filter === "NOT_RECORDED";
  if (filter.startsWith("EVENT:"))
    return (
      observation.comparison_status === "RELATION_CHANGED" &&
      observation.event_kind === filter.slice("EVENT:".length)
    );
  if (filter === "RELATION_CHANGED")
    return observation.comparison_status === "RELATION_CHANGED";
  return observation.comparison_status === filter;
}
function comparisonEventStatusLabel(status) {
  const labels = {
    RELATION_CHANGED: "真实关系变化",
    NO_RELATION_CHANGE: "关系未变化",
    NOT_COMPARABLE: "不可比较 / UNKNOWN",
    NOT_RECORDED: "UNKNOWN / NOT_RECORDED",
    NOT_RETURNED: "轨迹未返回",
  };
  return labels[status] || status;
}
function comparisonEventEvidence(observation) {
  const before = observation.before_relation || "UNKNOWN";
  const after = observation.after_relation || "UNKNOWN";
  const kind =
    observation.event_kind &&
    (priceRelationKindLabels[observation.event_kind] || observation.event_kind);
  const quality =
    "前态 " +
    priceRelationSideText(observation, "before") +
    "；当日 " +
    priceRelationSideText(observation, "after");
  const reason =
    observation.reason_summary || (observation.reason_codes || []).join("、");
  return {
    relation:
      (priceRelationLabels[before] || before) +
      " → " +
      (priceRelationLabels[after] || after),
    kind,
    quality,
    reason,
  };
}
function renderComparisonEventHistory(axis) {
  const panel = $("comparisonEventHistory");
  const entries = state.comparisonTimelines;
  const basket = state.comparisonBasket;
  const summaryRows = $("comparisonEventSummaryRows");
  const eventListRows = $("comparisonEventListRows");
  const matrixHead = $("comparisonEventMatrixHead");
  const matrixRows = $("comparisonEventMatrixRows");
  if (!panel) return;
  summaryRows.replaceChildren();
  eventListRows.replaceChildren();
  matrixHead.replaceChildren();
  matrixRows.replaceChildren();
  if (!entries.length) {
    panel.hidden = true;
    return;
  }
  panel.hidden = false;

  const fieldIds = comparisonEventFieldIds(entries);
  const fieldOptions = [
    { value: "ALL", label: "全部实际字段" },
    ...fieldIds.map((fieldId) => ({
      value: fieldId,
      label: comparisonEventFieldLabel(fieldId),
    })),
  ];
  setComparisonEventSelect(
    $("comparisonEventField"),
    fieldOptions,
    state.comparisonEventField,
  );
  state.comparisonEventField = $("comparisonEventField").value;
  const eventOptions = comparisonEventFilterOptions(entries);
  setComparisonEventSelect(
    $("comparisonEventKind"),
    eventOptions,
    state.comparisonEventKind,
  );
  state.comparisonEventKind = $("comparisonEventKind").value;

  const windowCount = Number(state.comparisonEventWindow);
  const windowDates = axis.sessionDates.slice(-windowCount);
  const windowDateSet = new Set(windowDates);
  const indexes = comparisonEventIndexes(entries);
  const selectedFieldIds =
    state.comparisonEventField === "ALL"
      ? fieldIds
      : fieldIds.filter((fieldId) => fieldId === state.comparisonEventField);
  const summaryWindow =
    windowDates.length > 0
      ? windowDates[0] + " 至 " + windowDates[windowDates.length - 1]
      : "没有共同官方会话日期";
  const failed = entries.filter((entry) => !entry.data);
  const summaryParts = [
    "窗口取 comparisonAxis 的最近 " +
      windowCount +
      " 个官方会话：" +
      summaryWindow +
      "（实际 " +
      windowDates.length +
      "/" +
      windowCount +
      " 个）",
    "当前临时篮 " + basket.length + " 只",
    "筛选字段：" +
      (state.comparisonEventField === "ALL"
        ? "全部实际字段"
        : comparisonEventFieldLabel(state.comparisonEventField)),
    "事件筛选：" +
      ($("comparisonEventKind").selectedOptions[0]?.textContent ||
        "全部状态与事件"),
  ];
  if (failed.length)
    summaryParts.push(
      "未返回轨迹：" +
        failed
          .map(
            (entry) =>
              entry.selection.source_symbol + "（" + entry.error + "）",
          )
          .join("；"),
    );

  for (const { entry, observations } of indexes) {
    for (const fieldId of selectedFieldIds) {
      const counts = {
        changed: 0,
        unchanged: 0,
        notComparable: 0,
        notRecorded: 0,
      };
      if (entry.data) {
        for (const sessionDate of windowDates) {
          const observation = observations.get(sessionDate + "|" + fieldId);
          if (!observation) {
            counts.notRecorded += 1;
          } else if (observation.comparison_status === "RELATION_CHANGED") {
            counts.changed += 1;
          } else if (observation.comparison_status === "NO_RELATION_CHANGE") {
            counts.unchanged += 1;
          } else if (observation.comparison_status === "NOT_COMPARABLE") {
            counts.notComparable += 1;
          } else {
            counts.notRecorded += 1;
          }
        }
      }
      const row = document.createElement("tr");
      cell(row, entry.selection.source_symbol);
      cell(row, comparisonEventFieldLabel(fieldId));
      cell(row, entry.data ? windowDates.length : "—");
      cell(row, entry.data ? counts.changed + counts.unchanged : "—");
      cell(row, entry.data ? counts.changed : "—");
      cell(row, entry.data ? counts.unchanged : "—");
      cell(row, entry.data ? counts.notComparable : "—");
      cell(row, entry.data ? counts.notRecorded : "—");
      cell(row, entry.data ? "75会话轨迹有效" : "轨迹未返回：" + entry.error);
      summaryRows.append(row);
    }
  }

  const eventGroups = new Map();
  for (const { entry } of indexes) {
    for (const observation of entry.data?.price_relation_history
      ?.observations || []) {
      if (
        !windowDateSet.has(observation.session_date) ||
        observation.comparison_status !== "RELATION_CHANGED" ||
        (state.comparisonEventField !== "ALL" &&
          observation.field_id !== state.comparisonEventField) ||
        !comparisonEventFilterMatches(state.comparisonEventKind, observation)
      )
        continue;
      const groupKey = observation.session_date + "|" + observation.field_id;
      if (!eventGroups.has(groupKey))
        eventGroups.set(groupKey, {
          sessionDate: observation.session_date,
          fieldId: observation.field_id,
          members: [],
        });
      eventGroups.get(groupKey).members.push({
        sourceSymbol: entry.selection.source_symbol,
        observation,
      });
    }
  }
  const orderedGroups = Array.from(eventGroups.values()).sort(
    (left, right) =>
      right.sessionDate.localeCompare(left.sessionDate) ||
      fieldIds.indexOf(left.fieldId) - fieldIds.indexOf(right.fieldId),
  );
  let commonEventGroupCount = 0;
  for (const group of orderedGroups) {
    const allSelected = indexes.map(({ entry, observations }) => ({
      sourceSymbol: entry.selection.source_symbol,
      observation: observations.get(group.sessionDate + "|" + group.fieldId),
    }));
    const eventKinds = new Set(
      allSelected.map(({ observation }) => observation?.event_kind),
    );
    const commonEvent =
      allSelected.length === basket.length &&
      allSelected.length >= 2 &&
      allSelected.every(
        ({ observation }) =>
          observation?.comparison_status === "RELATION_CHANGED" &&
          Boolean(observation.event_kind),
      ) &&
      eventKinds.size === 1;
    if (commonEvent) commonEventGroupCount += 1;

    const row = document.createElement("tr");
    cell(row, group.sessionDate);
    cell(row, comparisonEventFieldLabel(group.fieldId));
    cell(
      row,
      commonEvent
        ? "同日同字段同类型 · " + Array.from(eventKinds)[0]
        : "各代码实际变化分别列出",
    );
    const membersCell = document.createElement("td");
    const memberList = document.createElement("div");
    memberList.className = "comparison-event-members";
    for (const member of group.members) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "quiet";
      button.dataset.eventKey = [
        member.sourceSymbol,
        member.observation.session_date,
        member.observation.field_id,
        member.observation.event_kind,
      ].join("|");
      button.textContent =
        member.sourceSymbol +
        " · " +
        (priceRelationKindLabels[member.observation.event_kind] ||
          member.observation.event_kind) +
        " · 定位日K/Core";
      button.title =
        member.observation.reason_summary ||
        member.observation.before_relation +
          " → " +
          member.observation.after_relation;
      button.addEventListener("click", () =>
        openComparisonTimeline(
          member.sourceSymbol,
          member.observation.session_date,
        ),
      );
      memberList.append(button);
    }
    membersCell.append(memberList);
    row.append(membersCell);
    eventListRows.append(row);
  }
  if (!orderedGroups.length) {
    const row = document.createElement("tr");
    const message = document.createElement("td");
    message.colSpan = 4;
    message.className = "muted";
    message.textContent = "当前窗口与字段 / 事件筛选下没有真实变化事件。";
    row.append(message);
    eventListRows.append(row);
  }

  const selectedSymbols = entries.map((entry) => entry.selection.source_symbol);
  const headerRow = document.createElement("tr");
  cell(headerRow, "官方会话日期");
  cell(headerRow, "价格关系字段");
  for (const sourceSymbol of selectedSymbols) cell(headerRow, sourceSymbol);
  cell(headerRow, "同日同字段判定");
  matrixHead.append(headerRow);

  let visibleMatrixRows = 0;
  for (const sessionDate of [...axis.sessionDates].reverse()) {
    for (const fieldId of fieldIds) {
      if (
        state.comparisonEventField !== "ALL" &&
        fieldId !== state.comparisonEventField
      )
        continue;
      const cells = indexes.map(({ entry, observations }) => ({
        entry,
        observation: observations.get(sessionDate + "|" + fieldId) || null,
      }));
      if (
        !cells.some(({ observation }) =>
          comparisonEventFilterMatches(state.comparisonEventKind, observation),
        )
      )
        continue;
      const eventKinds = new Set(
        cells.map(({ observation }) => observation?.event_kind),
      );
      const commonEvent =
        cells.length === basket.length &&
        cells.length >= 2 &&
        cells.every(
          ({ observation }) =>
            observation?.comparison_status === "RELATION_CHANGED" &&
            Boolean(observation.event_kind),
        ) &&
        eventKinds.size === 1;
      const row = document.createElement("tr");
      row.dataset.sessionDate = sessionDate;
      row.dataset.fieldId = fieldId;
      row.dataset.targetSession = String(
        sessionDate === basket[0]?.target_session,
      );
      cell(row, sessionDate);
      cell(row, comparisonEventFieldLabel(fieldId));
      for (const item of cells) {
        const tableCell = document.createElement("td");
        const content = document.createElement("div");
        const observation = item.observation;
        let status = "NOT_RECORDED";
        if (!item.entry.data) status = "NOT_RETURNED";
        else if (observation) status = observation.comparison_status;
        content.className = "comparison-event-cell";
        content.dataset.status = status;
        appendText(content, "strong", comparisonEventStatusLabel(status));
        if (observation) {
          const evidence = comparisonEventEvidence(observation);
          appendText(
            content,
            "span",
            "关系 " +
              evidence.relation +
              (evidence.kind
                ? " · " + evidence.kind + "（" + observation.event_kind + "）"
                : ""),
          );
          appendText(content, "span", evidence.quality);
          if (evidence.reason)
            appendText(content, "span", "原因 " + evidence.reason);
          if (observation.comparison_status === "NO_RELATION_CHANGE")
            appendText(
              content,
              "span",
              "仅表示前后关系相同，不表示收盘价没有变动。",
              "muted",
            );
          if (observation.comparison_status === "RELATION_CHANGED") {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "quiet";
            button.dataset.eventKey = [
              item.entry.selection.source_symbol,
              observation.session_date,
              observation.field_id,
              observation.event_kind,
            ].join("|");
            button.textContent = "定位当日K线 / Core";
            button.addEventListener("click", () =>
              openComparisonTimeline(
                item.entry.selection.source_symbol,
                observation.session_date,
              ),
            );
            content.append(button);
          }
        } else {
          const startSession = item.entry.data?.points?.[0]?.session_date;
          const reason = !item.entry.data
            ? item.entry.error || "该代码的轨迹没有返回"
            : sessionDate === startSession
              ? "这是轨迹首个官方会话，没有前态可比较"
              : "该来源代码没有返回此日期与字段的观察记录";
          appendText(content, "span", reason, "muted");
        }
        tableCell.append(content);
        row.append(tableCell);
      }
      cell(
        row,
        commonEvent
          ? "同日同字段同类型 · " + Array.from(eventKinds)[0]
          : "没有满足全体同类变化条件",
      );
      matrixRows.append(row);
      visibleMatrixRows += 1;
    }
  }
  if (!visibleMatrixRows) {
    const row = document.createElement("tr");
    const message = document.createElement("td");
    message.colSpan = entries.length + 3;
    message.className = "muted";
    message.textContent = "当前字段 / 事件筛选下没有匹配的日期×字段行。";
    row.append(message);
    matrixRows.append(row);
  }
  $("comparisonEventCount").textContent =
    axis.sessionDates.length + "个官方会话 · " + fieldIds.length + "个实际字段";
  summaryParts.push(
    "矩阵 " + visibleMatrixRows + " 行",
    "窗口内实际变化 " +
      orderedGroups.reduce((total, group) => total + group.members.length, 0) +
      " 条",
    "满足全体同日同字段同类型 " + commonEventGroupCount + " 组",
  );
  $("comparisonEventSummaryStatus").textContent = summaryParts.join(" · ");
}
function renderComparisonResults() {
  const entries = state.comparisonTimelines;
  const basket = state.comparisonBasket;
  const successful = entries.filter((entry) => entry.data);
  const failed = entries.filter((entry) => !entry.data);
  const axis = comparisonAxis(entries, basket.length);
  state.comparisonBaseSession = axis.baseSession;
  $("comparisonResults").hidden = entries.length === 0;
  $("comparisonCards").replaceChildren();
  for (const entry of entries)
    $("comparisonCards").append(renderComparisonSummary(entry));
  const identity = basket[0];
  if (identity) {
    $("comparisonIdentity").textContent =
      `统一目标 ${identity.target_session} · namespace ${identity.namespace_id} · source_set ${identity.source_set_id} · 完整 package SHA ${identity.package_sha256} · profile_revision ${identity.profile_revision} · ` +
      `官方会话窗 ${successful[0]?.data?.calendar_id || "—"}/${successful[0]?.data?.calendar_revision || "—"} · RAW_UNADJUSTED / NON_PIT。` +
      (axis.baseSession
        ? `共同有效收盘基准 ${axis.baseSession}；只按精确有理数计算、显示时四舍五入2位。`
        : `共同有效收盘基准 UNKNOWN${failed.length ? "（至少一只历史未返回，不能用子集建立基准）" : ""}。`);
  }
  $("comparisonStatus").textContent =
    `${entries.length}只请求 · ${successful.length}只轨迹有效 · ${failed.length}只未返回 · ` +
    `${axis.sessionDates.length}个官方会话日期${axis.baseSession ? ` · t0=${axis.baseSession}` : " · 指数基准UNKNOWN"}。`;
  const head = $("comparisonHead");
  head.replaceChildren();
  const headerRow = document.createElement("tr");
  const dateHeader = document.createElement("th");
  dateHeader.textContent = "官方会话日期";
  headerRow.append(dateHeader);
  for (const entry of entries) {
    const th = document.createElement("th");
    appendText(th, "strong", entry.selection.source_symbol);
    const open = document.createElement("button");
    open.type = "button";
    open.className = "quiet";
    open.textContent = "查看单股日K/周月K与Core";
    open.disabled = !entry.data;
    open.addEventListener("click", () =>
      openComparisonTimeline(entry.selection.source_symbol),
    );
    const actions = document.createElement("div");
    actions.className = "comparison-source-actions";
    actions.append(open);
    th.append(actions);
    headerRow.append(th);
  }
  head.append(headerRow);
  const body = $("comparisonRows");
  body.replaceChildren();
  if (!axis.sessionDates.length) {
    tableRow(body, [
      "没有可显示的官方会话轨迹。",
      ...entries.map((entry) => entry.error || ""),
    ]);
    renderComparisonEventHistory(axis);
    renderComparisonPeriodResults();
    return;
  }
  for (const session of axis.sessionDates) {
    const row = document.createElement("tr");
    row.dataset.targetSession = String(session === identity?.target_session);
    cell(row, session);
    for (const entry of entries) {
      const td = document.createElement("td");
      const facts = document.createElement("div");
      facts.className = "comparison-symbol-facts";
      const point = axis.maps.get(entry.selection.source_symbol)?.get(session);
      if (!entry.data || !point) {
        appendText(
          facts,
          "span",
          `RAW收盘 UNKNOWN · ${entry.error || "该官方会话未返回轨迹点"}`,
        );
        appendText(facts, "span", "MA20 UNKNOWN · 该日无对应Core点");
        appendText(facts, "span", "前高20 UNKNOWN · 该日无对应Core点");
        appendText(
          facts,
          "span",
          `RAW指数 UNKNOWN · ${axis.baseSession ? "该代码或日期数据未返回" : "无共同基准"}`,
          "comparison-index",
        );
        td.append(facts);
        row.append(td);
        continue;
      }
      const close = point.raw_close;
      const ma20 = point.core_ma20;
      const priorHigh = point.core_prior_high20;
      const closeReason =
        close?.quality === "OBSERVED"
          ? point.price_reason || close.reason || "无"
          : close?.reason ||
            point.price_reason ||
            close?.quality ||
            "未提供原因";
      appendText(
        facts,
        "span",
        `RAW收盘 ${comparisonFieldText(close)} · ${text(point.raw_status)} · 原因 ${text(closeReason)}`,
      );
      appendText(
        facts,
        "span",
        `MA20 ${comparisonFieldText(ma20)} · C/MA20 ${comparisonRelationText(point.close_vs_ma20, point.relation_unknown_reason)}`,
      );
      appendText(
        facts,
        "span",
        `严格前高20 ${comparisonFieldText(priorHigh)} · C/前高20 ${comparisonRelationText(point.close_vs_prior_high20, point.prior_high20_relation_unknown_reason)}`,
      );
      appendText(
        facts,
        "span",
        `MA5/10/20/60 ${formatMovingAverageOrder(point.moving_average_order)}`,
      );
      let indexText;
      if (!axis.baseSession) {
        indexText = "UNKNOWN · 无共同有效收盘基准";
      } else if (session < axis.baseSession) {
        indexText = `共同基准 ${axis.baseSession} 以前无指数`;
      } else {
        const closeFraction = rawCloseFraction(close);
        const indexFraction = normalizedIndexFraction(
          closeFraction,
          axis.bases.get(entry.selection.source_symbol),
        );
        indexText = indexFraction
          ? formatIndexFraction(indexFraction)
          : `UNKNOWN · ${text(close?.reason || point.price_reason || close?.quality || "RAW收盘不可观察")}`;
      }
      appendText(
        facts,
        "strong",
        `相对已存RAW价格指数 ${indexText}`,
        "comparison-index",
      );
      td.append(facts);
      row.append(td);
    }
    body.append(row);
  }
  renderComparisonEventHistory(axis);
  renderComparisonPeriodResults();
}
function comparisonPeriodAvailability(entry, periodKind) {
  const symbol = entry.selection.source_symbol;
  if (!entry.data) {
    return `${symbol}：${entry.pending ? "周期请求仍在读取" : entry.error || "周期数据不可用"}`;
  }
  const periods = entry.data.periods[periodKind === "WEEK" ? "week" : "month"];
  const completeKeys = periods
    .filter((period) => comparisonPeriodCloseFraction(period))
    .map((period) => period.period_key);
  if (completeKeys.length)
    return `${symbol}：COMPLETE闭合周期 ${completeKeys.join("、")}`;
  const observedReasons = periods
    .filter((period) => period.coverage_status !== "COMPLETE")
    .map(
      (period) =>
        `${period.period_key} ${period.coverage_status} ${period.reason_codes.join("、") || "无原因码"}`,
    );
  observedReasons.push(
    ...periods
      .filter(
        (period) =>
          period.coverage_status === "COMPLETE" &&
          !comparisonPeriodCloseFraction(period),
      )
      .map((period) => `${period.period_key} COMPLETE但RAW收盘无效`),
  );
  return `${symbol}：没有合法COMPLETE闭合收盘${observedReasons.length ? `（${observedReasons.join("；")}）` : ""}`;
}
function comparisonPeriodOhlcText(period) {
  if (!period.calendar_closed) return "未闭合；不展示周期OHLC";
  const ohlc = period.ohlc || {};
  return `O ${comparisonPeriodPriceText(ohlc.open)} · H ${comparisonPeriodPriceText(ohlc.high)} · L ${comparisonPeriodPriceText(ohlc.low)} · C ${comparisonPeriodPriceText(ohlc.close)}`;
}
function renderComparisonPeriodResults() {
  const entries = state.comparisonPeriodEntries;
  const kind = state.comparisonPeriodKind;
  const kindLabel = kind === "WEEK" ? "ISO自然周" : "自然月";
  const axis = comparisonPeriodAxis(
    entries,
    state.comparisonBasket.length,
    kind,
  );
  state.comparisonPeriodBaseKey = axis.baseRow?.key || null;
  $("comparisonPeriodHead").replaceChildren();
  const header = document.createElement("tr");
  const periodHeader = document.createElement("th");
  periodHeader.textContent = "自然周期与官方会话范围";
  header.append(periodHeader);
  for (const entry of entries) {
    const th = document.createElement("th");
    appendText(th, "strong", entry.selection.source_symbol);
    if (!entry.data && !entry.pending)
      appendText(th, "span", "周期数据不可用", "muted");
    header.append(th);
  }
  $("comparisonPeriodHead").append(header);
  const body = $("comparisonPeriodRows");
  body.replaceChildren();
  const successfulCount = entries.filter((entry) => entry.data).length;
  $("comparisonPeriodCount").textContent =
    `${axis.rows.length}个${kindLabel} · ${successfulCount}/${entries.length}只周期响应`;
  const identity = state.comparisonBasket[0];
  const targetText = identity
    ? `目标 ${identity.target_session} · namespace ${identity.namespace_id} · SEALED source_set ${identity.source_set_id} · package SHA ${identity.package_sha256} · RAW_UNADJUSTED / NON_PIT_RETROSPECTIVE。`
    : "等待同一SEALED来源快照的比较篮。";
  if (axis.baseRow) {
    $("comparisonPeriodStatus").textContent =
      `${targetText} ${kindLabel}共同t0=${axis.baseRow.key}（${axis.baseRow.metadata.calendar_start_session}至${axis.baseRow.metadata.calendar_end_session}）；仅所有选中代码均为COMPLETE、合法RAW收盘且官方周期范围一致的行计算指数。`;
  } else {
    const details = entries.map((entry) =>
      comparisonPeriodAvailability(entry, kind),
    );
    $("comparisonPeriodStatus").textContent =
      `${targetText} ${kindLabel}共同t0=UNKNOWN；全篮没有可验证的共同完整闭合周期，所有同期指数均标为UNKNOWN。来源状态：${details.join("；")}。`;
  }
  if (!entries.length) {
    tableRow(body, ["尚未读取来源周期。"]);
    return;
  }
  if (!axis.rows.length) {
    tableRow(body, [
      "没有可对齐的自然周期。",
      ...entries.map((entry) =>
        entry.pending ? "周期读取中…" : entry.error || "周期接口没有返回周期行",
      ),
    ]);
    return;
  }
  for (const rowData of axis.rows) {
    const row = document.createElement("tr");
    const periodCell = document.createElement("td");
    const periodFacts = document.createElement("div");
    periodFacts.className = "comparison-symbol-facts";
    appendText(periodFacts, "strong", rowData.key);
    appendText(
      periodFacts,
      "span",
      `${rowData.metadata.calendar_start_session} → ${rowData.metadata.calendar_end_session} · ordinal ${rowData.metadata.calendar_ordinal}`,
    );
    appendText(
      periodFacts,
      "span",
      `官方预期会话 ${rowData.metadata.calendar_expected_session_count} · 本窗经过会话 ${rowData.metadata.elapsed_expected_session_count}`,
    );
    if (!rowData.aligned)
      appendText(
        periodFacts,
        "span",
        "来源缺少该周期或周期日历元数据不一致；禁止横向计算",
        "message",
      );
    periodCell.append(periodFacts);
    row.append(periodCell);
    for (const [index, item] of rowData.cells.entries()) {
      const td = document.createElement("td");
      const facts = document.createElement("div");
      facts.className = "comparison-symbol-facts";
      if (!item.period) {
        appendText(
          facts,
          "span",
          item.entry.pending
            ? "周期读取中…"
            : `UNKNOWN · ${item.entry.error || "来源未返回该自然周期"}`,
          "message",
        );
      } else {
        const period = item.period;
        const closedText = period.calendar_closed ? "已闭合" : "未闭合";
        appendText(
          facts,
          "span",
          `${closedText} · ${period.coverage_status} · RAW行 ${period.stored_raw_row_count}/${period.elapsed_expected_session_count} · 有效价格Bar ${period.valid_price_bar_count}/${period.elapsed_expected_session_count}`,
        );
        appendText(
          facts,
          "span",
          `缺目标 ${period.target_missing_count} · 读取失败 ${period.read_failed_count} · 坏价 ${period.invalid_price_count} · 历史未保存 ${period.missing_history_count} · 窗口截断 ${period.window_truncated_session_count}`,
        );
        appendText(facts, "span", comparisonPeriodOhlcText(period));
        appendText(
          facts,
          "strong",
          item.indexFraction
            ? `周期RAW价格指数 ${formatIndexFraction(item.indexFraction)}（t0=${axis.baseRow.key}）`
            : `周期RAW价格指数 UNKNOWN · ${item.reason || "未形成共同完整基准"}`,
          "comparison-index",
        );
        appendText(
          facts,
          "span",
          `原因码：${period.reason_codes.join("、") || "无"}`,
          "muted",
        );
        if (item.entry.data && state.comparisonTimelines[index]?.data) {
          const open = document.createElement("button");
          open.type = "button";
          open.className = "quiet";
          open.textContent = "查看该代码日K/周月详情";
          open.addEventListener("click", () =>
            openComparisonTimeline(
              item.entry.selection.source_symbol,
              period.ohlc?.close_session || item.entry.selection.target_session,
            ),
          );
          facts.append(open);
        }
      }
      td.append(facts);
      row.append(td);
    }
    body.append(row);
  }
}
function validateMa20DirectionHistory(data) {
  const history = data.ma20_direction_history;
  const binding = history?.binding;
  const identityRef = data.snapshot_identity?.identity_sha256;
  if (
    !history ||
    !binding ||
    history.identity_ref !== identityRef ||
    binding.target_session !== data.target_session ||
    binding.source_symbol !== data.symbol ||
    binding.namespace_id !== data.namespace_id ||
    binding.package_sha256 !== data.package_sha256 ||
    binding.source_set_id !== (data.formal_source_set?.source_set_id ?? null) ||
    binding.source_set_revision !==
      (data.formal_source_set?.revision_id ?? null) ||
    binding.source_set_status !==
      (data.formal_source_set?.status ?? "UNKNOWN") ||
    binding.calendar_id !== data.calendar_id ||
    binding.calendar_revision !== data.calendar_revision ||
    binding.price_coordinate !== data.price_coordinate ||
    binding.coordinate_id !== data.points?.[0]?.coordinate_id
  )
    throw new Error("MA20方向记录没有绑定当前轨迹、来源集合、日历和价格坐标。");
  for (const requestedCount of [5, 10]) {
    const item = history.windows?.[String(requestedCount)];
    const actualCount = Math.min(requestedCount, data.points.length);
    const expectedPairCount = Math.max(actualCount - 1, 0);
    const counts = item?.counts || {};
    const pairs = item?.pairs;
    if (
      !item ||
      item.requested_session_count !== requestedCount ||
      item.actual_session_count !== actualCount ||
      item.expected_pair_count !== expectedPairCount ||
      item.pair_count !== expectedPairCount ||
      !Array.isArray(pairs) ||
      pairs.length !== expectedPairCount ||
      ["UP", "SAME", "DOWN", "UNKNOWN"].reduce(
        (total, direction) => total + Number(counts[direction] || 0),
        0,
      ) !== expectedPairCount
    )
      throw new Error("MA20方向窗口的实际官方会话分母或四态计数不完整。");
    const firstPoint = data.points.length - actualCount;
    const observedCounts = { UP: 0, SAME: 0, DOWN: 0, UNKNOWN: 0 };
    for (let index = 0; index < pairs.length; index++) {
      const pair = pairs[index];
      const previous = data.points[firstPoint + index];
      const current = data.points[firstPoint + index + 1];
      if (
        pair.from_session !== previous.session_date ||
        pair.to_session !== current.session_date ||
        !Object.hasOwn(observedCounts, pair.direction)
      )
        throw new Error("MA20方向记录跨越了缺失或非紧邻官方会话。");
      observedCounts[pair.direction]++;
    }
    if (
      ["UP", "SAME", "DOWN", "UNKNOWN"].some(
        (direction) => counts[direction] !== observedCounts[direction],
      )
    )
      throw new Error("MA20方向四态计数与相邻会话明细不一致。");
  }
  return history;
}
function validateComparisonTimeline(data, selection) {
  if (
    data.price_coordinate !== "RAW_UNADJUSTED" ||
    data.symbol !== selection.source_symbol ||
    data.target_session !== selection.target_session ||
    data.package_sha256 !== selection.package_sha256 ||
    data.namespace_id !== selection.namespace_id ||
    data.selection?.source_symbol !== selection.source_symbol ||
    data.selection?.target_session !== selection.target_session ||
    data.selection?.package_sha256 !== selection.package_sha256 ||
    data.selection?.session_count !== 75 ||
    data.selection?.namespace_id !== selection.namespace_id ||
    data.selection?.source_set_id !== selection.source_set_id ||
    data.snapshot_identity?.namespace_id !== selection.namespace_id ||
    data.snapshot_identity?.target_session !== selection.target_session ||
    data.snapshot_identity?.package_sha256 !== selection.package_sha256 ||
    data.snapshot_identity?.source_set_id !== selection.source_set_id ||
    data.formal_source_set?.status !== "AVAILABLE" ||
    data.formal_source_set?.source_set_id !== selection.source_set_id ||
    data.total_session_count > 75 ||
    !Array.isArray(data.points) ||
    data.points.length !== data.total_session_count
  )
    throw new Error(
      "75会话响应的代码、RAW坐标或完整SEALED身份不匹配，已拒绝加入对照。",
    );
  validateMa20DirectionHistory(data);
  const dates = data.points.map((point) => point.session_date);
  if (
    dates.some((value, index) => index > 0 && dates[index - 1] >= value) ||
    data.points.some(
      (point) =>
        point.source_symbol !== selection.source_symbol ||
        point.package_sha256 !== selection.package_sha256 ||
        point.namespace_id !== selection.namespace_id,
    )
  )
    throw new Error("轨迹日期或逐日来源身份不完整，已拒绝同日合并。");
  const history = data.price_relation_history;
  const fieldIds = Object.keys(history?.field_statistics || {});
  const observations = history?.observations;
  if (
    !history ||
    history.identity_ref !== data.snapshot_identity.identity_sha256 ||
    fieldIds.length !== 5 ||
    history.comparison_count !==
      Math.max(0, data.total_session_count - 1) * 5 ||
    !Array.isArray(observations) ||
    observations.length !== history.comparison_count
  )
    throw new Error(
      "价格关系 observations 未绑定完整轨迹或官方会话×字段分母，已拒绝多股对齐。",
    );
  const dateIndex = new Map(dates.map((value, index) => [value, index]));
  const observationKeys = new Set();
  for (const observation of observations) {
    const index = dateIndex.get(observation.session_date);
    const key = observation.session_date + "|" + observation.field_id;
    if (
      index === undefined ||
      index < 1 ||
      dates[index - 1] !== observation.previous_official_session ||
      !fieldIds.includes(observation.field_id) ||
      observation.identity_ref !== history.identity_ref ||
      observationKeys.has(key)
    )
      throw new Error(
        "价格关系 observations 存在非紧邻日期、重复字段或不同响应身份，已拒绝多股对齐。",
      );
    observationKeys.add(key);
  }
  if (observationKeys.size !== history.comparison_count)
    throw new Error("价格关系 observation 键数与完整分母不一致。");
  const eventKeys = new Set(
    (history.events || []).map((event) => event.event_key),
  );
  if (
    eventKeys.size !== history.event_count ||
    [...eventKeys].some(
      (eventKey) =>
        !observations.some(
          (observation) =>
            observation.session_date +
              "|" +
              observation.field_id +
              "|" +
              observation.event_kind ===
              eventKey && observation.comparison_status === "RELATION_CHANGED",
        ),
    )
  )
    throw new Error(
      "价格关系 events 不是完整 observations 中的变化子集，已拒绝多股对齐。",
    );
  return data;
}
function validateComparisonRawPeriods(data, selection, timeline) {
  const identity = data?.snapshot_identity;
  const formalSourceSet = data?.formal_source_set;
  const responseSelection = data?.selection;
  if (
    data?.ok !== true ||
    data.research_mode !== "NON_PIT_RAW_DIAGNOSTIC" ||
    data.identity_status !== "SOURCE_SYMBOL_ONLY" ||
    data.pit_status !== "NON_PIT_RETROSPECTIVE" ||
    data.price_coordinate !== "RAW_UNADJUSTED" ||
    data.target_session !== selection.target_session ||
    data.source_symbol !== selection.source_symbol ||
    data.symbol !== selection.source_symbol ||
    data.package_sha256 !== selection.package_sha256 ||
    data.namespace_id !== selection.namespace_id ||
    data.calendar_id !== timeline.calendar_id ||
    data.calendar_revision !== timeline.calendar_revision ||
    data.history_window?.start_session !== timeline.session_start ||
    data.history_window?.end_session !== selection.target_session ||
    responseSelection?.source_symbol !== selection.source_symbol ||
    responseSelection?.target_session !== selection.target_session ||
    responseSelection?.package_sha256 !== selection.package_sha256 ||
    responseSelection?.namespace_id !== selection.namespace_id ||
    responseSelection?.source_set_id !== selection.source_set_id ||
    identity?.identity_sha256 == null ||
    identity.namespace_id !== selection.namespace_id ||
    identity.target_session !== selection.target_session ||
    identity.package_sha256 !== selection.package_sha256 ||
    identity.source_set_id !== selection.source_set_id ||
    identity.identity_type !== timeline.snapshot_identity.identity_type ||
    identity.application_id !== timeline.snapshot_identity.application_id ||
    identity.preview_mode !== timeline.snapshot_identity.preview_mode ||
    identity.core_source_sha256 !==
      timeline.snapshot_identity.core_source_sha256 ||
    identity.research_entry_source_sha256 !==
      timeline.snapshot_identity.research_entry_source_sha256 ||
    identity.profile_contract_sha256 !==
      timeline.snapshot_identity.profile_contract_sha256 ||
    identity.price_coordinate !== "RAW_UNADJUSTED" ||
    formalSourceSet?.status !== "AVAILABLE" ||
    formalSourceSet?.source_set_id !== selection.source_set_id ||
    formalSourceSet?.revision_id !== timeline.formal_source_set?.revision_id ||
    !Array.isArray(data.periods?.week) ||
    !Array.isArray(data.periods?.month)
  )
    throw new Error(
      "闭周/月响应的来源、SEALED快照、日历、窗口或RAW坐标与75会话轨迹不一致。",
    );
  for (const [kind, periods] of Object.entries(data.periods)) {
    const seen = new Set();
    for (const period of periods) {
      if (
        period.kind !== kind.toUpperCase() ||
        typeof period.period_key !== "string" ||
        !Number.isSafeInteger(period.calendar_ordinal) ||
        !/^\d{4}-\d{2}-\d{2}$/.test(period.calendar_start_session || "") ||
        !/^\d{4}-\d{2}-\d{2}$/.test(period.calendar_end_session || "") ||
        !Number.isSafeInteger(period.calendar_expected_session_count) ||
        !Number.isSafeInteger(period.elapsed_expected_session_count) ||
        typeof period.calendar_closed !== "boolean" ||
        !["COMPLETE", "PARTIAL", "UNFINISHED"].includes(
          period.coverage_status,
        ) ||
        !Array.isArray(period.reason_codes) ||
        seen.has(period.period_key) ||
        !Array.isArray(period.source_package_sha256s) ||
        period.source_package_sha256s.some(
          (packageSha) => packageSha !== selection.package_sha256,
        )
      )
        throw new Error("闭周/月响应含错序、重复或来源身份不完整的周期行。");
      seen.add(period.period_key);
      for (const field of [
        "stored_raw_row_count",
        "valid_price_bar_count",
        "target_missing_count",
        "read_failed_count",
        "invalid_price_count",
        "missing_history_count",
        "window_truncated_session_count",
      ]) {
        if (!Number.isSafeInteger(period[field]) || period[field] < 0)
          throw new Error("闭周/月响应含无效的来源覆盖计数。");
      }
    }
  }
  return data;
}
function comparisonBasketRequestKey(basket) {
  return basket
    .map(
      (item) =>
        `${item.source_symbol}|${item.target_session}|${item.namespace_id}|${item.source_set_id}|${item.package_sha256}|${item.profile_revision}`,
    )
    .join(";");
}
function comparisonRequestMatches(
  requestId,
  currentRequestId,
  requestKey,
  currentKey,
) {
  return requestId === currentRequestId && requestKey === currentKey;
}
function comparisonRequestIsCurrent(requestId, requestKey) {
  return comparisonRequestMatches(
    requestId,
    state.comparisonRequest,
    requestKey,
    comparisonBasketRequestKey(state.comparisonBasket),
  );
}
async function loadComparisonRawPeriods(entries, requestId, requestKey) {
  const periodEntries = await Promise.all(
    entries.map(async (entry) => {
      if (!entry.data) {
        return {
          selection: entry.selection,
          data: null,
          error: entry.error,
          pending: false,
        };
      }
      try {
        const query = new URLSearchParams({
          target_session: entry.selection.target_session,
          package_sha256: entry.selection.package_sha256,
          symbol: entry.selection.source_symbol,
          namespace_id: entry.selection.namespace_id,
          source_set_id: entry.selection.source_set_id,
        });
        const response = await api(`/api/source-symbol/raw-periods?${query}`);
        return {
          selection: entry.selection,
          data: validateComparisonRawPeriods(
            response,
            entry.selection,
            entry.data,
          ),
          error: null,
          pending: false,
        };
      } catch (error) {
        return {
          selection: entry.selection,
          data: null,
          error: error.message,
          pending: false,
        };
      }
    }),
  );
  if (!comparisonRequestIsCurrent(requestId, requestKey)) return false;
  state.comparisonPeriodEntries = periodEntries;
  renderComparisonPeriodResults();
  return true;
}
async function compareSelectedSymbols() {
  const basket = state.comparisonBasket.map((item) => ({ ...item }));
  if (basket.length < 2 || basket.length > 3) {
    state.comparisonMessage = "请选择2至3个同一精确SEALED快照的不同来源代码。";
    renderComparisonBasket();
    return;
  }
  const identity = comparisonIdentityOf(basket[0]);
  if (
    basket.some(
      (item) => !sameComparisonIdentity(comparisonIdentityOf(item), identity),
    )
  ) {
    state.comparisonMessage = "选择篮包含不同来源版本；请清空后重新选择。";
    renderComparisonBasket();
    return;
  }
  const requestId = ++state.comparisonRequest;
  const requestKey = comparisonBasketRequestKey(basket);
  state.comparisonTimelines = [];
  state.comparisonBaseSession = null;
  state.comparisonPeriodEntries = [];
  state.comparisonPeriodBaseKey = null;
  $("comparisonResults").hidden = true;
  $("comparisonStatus").textContent =
    `正在并行读取${basket.length}只股票各75个官方会话…`;
  $("compareSymbols").disabled = true;
  const entries = await Promise.all(
    basket.map(async (selection) => {
      try {
        const data = await api("/api/preview/stock-timeline", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            symbol: selection.source_symbol,
            target_session: selection.target_session,
            package_sha256: selection.package_sha256,
            session_count: 75,
            source_set_id: selection.source_set_id,
            namespace_id: selection.namespace_id,
          }),
        });
        return {
          selection,
          data: validateComparisonTimeline(data, selection),
          error: null,
        };
      } catch (error) {
        return { selection, data: null, error: error.message };
      }
    }),
  );
  if (!comparisonRequestIsCurrent(requestId, requestKey)) return;
  const reference = entries.find((entry) => entry.data)?.data;
  if (reference) {
    for (const entry of entries) {
      if (
        entry.data &&
        (entry.data.calendar_id !== reference.calendar_id ||
          entry.data.calendar_revision !== reference.calendar_revision ||
          entry.data.session_start !== reference.session_start ||
          entry.data.total_session_count !== reference.total_session_count)
      ) {
        entry.data = null;
        entry.error =
          "官方会话日历版本或窗口与其它代码不一致，已拒绝跨轴对齐。";
      }
    }
  }
  state.comparisonTimelines = entries;
  state.comparisonPeriodEntries = entries.map((entry) => ({
    selection: entry.selection,
    data: null,
    error: entry.data ? null : entry.error,
    pending: Boolean(entry.data),
  }));
  $("comparisonPeriodStatus").textContent =
    "正在按同一目标、SEALED来源身份和官方日历读取现役PG周/月周期…";
  $("comparisonResults").hidden = false;
  renderComparisonResults();
  $("comparisonResults").scrollIntoView({ behavior: "smooth", block: "start" });
  const periodsLoaded = await loadComparisonRawPeriods(
    entries,
    requestId,
    requestKey,
  );
  if (!periodsLoaded) return;
  renderComparisonBasket();
}
function openComparisonTimeline(sourceSymbol, sessionDate = null) {
  const entry = state.comparisonTimelines.find(
    (item) => item.selection.source_symbol === sourceSymbol,
  );
  if (!entry?.data) return;
  if (!state.comparisonReturnForm) {
    state.comparisonReturnForm = {
      version: $("versionSelect").value,
      symbol: $("symbolInput").value,
      window: $("windowInput").value,
      periodKind: state.comparisonPeriodKind,
      periodBaseKey: state.comparisonPeriodBaseKey,
      eventWindow: state.comparisonEventWindow,
      eventField: state.comparisonEventField,
      eventKind: state.comparisonEventKind,
      formValues: Object.fromEntries(
        Array.from(document.querySelectorAll("input, select"))
          .filter((element) => element.id)
          .map((element) => [element.id, element.value]),
      ),
      pgContext: {
        offset: state.offset,
        limit: state.limit,
        total: state.total,
        profileOffset: state.profileOffset,
        profileLimit: state.profileLimit,
        profileTotal: state.profileTotal,
        changeOffset: state.changeOffset,
        changeLimit: state.changeLimit,
        changeTotal: state.changeTotal,
        relationOffset: state.relationOffset,
        relationLimit: state.relationLimit,
        relationTotal: state.relationTotal,
        relationQuery: state.relationQuery ? { ...state.relationQuery } : null,
        profileBreadthMemberOffset: state.profileBreadthMemberOffset,
        profileBreadthMemberLimit: state.profileBreadthMemberLimit,
        profileBreadthMemberQuery: state.profileBreadthMemberQuery
          ? { ...state.profileBreadthMemberQuery }
          : null,
      },
    };
  }
  state.timelineRequest++;
  state.timeline = entry.data;
  state.timelineContext = {
    target_session: entry.selection.target_session,
    package_sha256: entry.selection.package_sha256,
    symbol: entry.selection.source_symbol,
    session_count: 75,
    namespace_id: entry.selection.namespace_id,
    source_set_id: entry.selection.source_set_id,
    origin: "comparison",
  };
  state.chartPeriodContext = chartContextForTimeline(entry.data);
  state.chartMode = "day";
  const pointIndex = sessionDate
    ? entry.data.points.findIndex((point) => point.session_date === sessionDate)
    : entry.data.points.findIndex(
        (point) => point.session_date === entry.selection.target_session,
      );
  state.chartSelection =
    pointIndex >= 0 ? { mode: "day", index: pointIndex } : null;
  clearRawPeriodState(
    "已复用多股对照已读取的75会话；闭周/月视图按需读现役PG Raw周期接口。",
  );
  clearResult();
  $("stockResult").classList.add("timeline-only");
  $("stockResult").hidden = false;
  $("resultTitle").textContent =
    `${sourceSymbol} · ${entry.selection.target_session}`;
  $("resultMode").textContent =
    "复用多股对照的现役75会话轨迹 · SOURCE_SYMBOL_ONLY · RAW_UNADJUSTED · NON_PIT";
  $("snapshotId").textContent = entry.selection.package_sha256;
  $("windowText").textContent = "75个官方会话";
  $("versionSelect").value =
    `${entry.selection.target_session}|${entry.selection.package_sha256}`;
  $("symbolInput").value = sourceSymbol;
  $("windowInput").value = "75";
  renderTimeline(entry.data);
  $("timelinePanel").scrollIntoView({ behavior: "smooth", block: "start" });
}
function returnFromComparison() {
  const returnForm = state.comparisonReturnForm;
  state.timelineRequest++;
  state.timeline = null;
  state.timelineContext = null;
  state.chartPeriodContext = null;
  state.chartSelection = null;
  state.chartMode = "day";
  clearRawPeriodState("返回多股对照；原临时选择、t0和筛选分页仍保留。");
  renderTimeline(null);
  clearResult();
  if (returnForm) {
    $("versionSelect").value = returnForm.version;
    $("symbolInput").value = returnForm.symbol;
    $("windowInput").value = returnForm.window;
    state.comparisonPeriodKind = returnForm.periodKind;
    $("comparisonPeriodKind").value = returnForm.periodKind;
    state.comparisonEventWindow = returnForm.eventWindow;
    state.comparisonEventField = returnForm.eventField;
    state.comparisonEventKind = returnForm.eventKind;
    $("comparisonEventWindow").value = String(returnForm.eventWindow);
    $("comparisonEventField").value = returnForm.eventField;
    $("comparisonEventKind").value = returnForm.eventKind;
    for (const [id, value] of Object.entries(returnForm.formValues || {})) {
      const control = $(id);
      if (control) control.value = value;
    }
    Object.assign(state, returnForm.pgContext || {});
  }
  state.comparisonReturnForm = null;
  renderComparisonPeriodResults();
  $("comparisonPeriodComparison").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
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

function ma20DirectionLabel(direction) {
  return (
    {
      UP: "UP · MA20上升",
      SAME: "SAME · MA20持平",
      DOWN: "DOWN · MA20下降",
      UNKNOWN: "UNKNOWN · 证据不足",
    }[direction] || text(direction || "UNKNOWN")
  );
}

function renderMa20DirectionHistory(history) {
  const identity = $("ma20DirectionIdentity");
  const summary = $("ma20DirectionSummary");
  const rows = $("ma20DirectionRows");
  if (!identity || !summary || !rows) return;
  identity.textContent = "";
  summary.textContent = "";
  rows.replaceChildren();
  if (!history) {
    tableRow(rows, ["等待历史轨迹查询。", "", "", "", "", ""]);
    return;
  }
  const windowKey = $("ma20DirectionWindow").value;
  const windowData = history.windows?.[windowKey];
  const binding = history.binding || {};
  if (!windowData) {
    summary.textContent = "MA20逐会话方向响应缺少所选窗口。";
    tableRow(rows, ["窗口不可用。", "", "", "", "", ""]);
    return;
  }
  const counts = windowData.counts || {};
  const pairSum = ["UP", "SAME", "DOWN", "UNKNOWN"].reduce(
    (total, key) => total + Number(counts[key] || 0),
    0,
  );
  summary.textContent =
    `近${windowKey}个官方会话：实际${windowData.actual_session_count}个（` +
    `${windowData.start_session || "—"} 至 ${windowData.end_session || "—"}），` +
    `相邻对 ${windowData.pair_count}/${windowData.expected_pair_count}；` +
    `UP ${counts.UP || 0} · SAME ${counts.SAME || 0} · ` +
    `DOWN ${counts.DOWN || 0} · UNKNOWN ${counts.UNKNOWN || 0} · ` +
    `四态合计 ${pairSum}。` +
    (windowData.actual_session_count < Number(windowKey)
      ? " 当前响应实际官方会话少于请求窗口，分母按实际相邻会话计算。"
      : "");
  identity.textContent =
    `${binding.target_session || "—"} · ${binding.source_symbol || "—"} · ` +
    `source_set ${binding.source_set_id || "未知"} / ${binding.source_set_status || "UNKNOWN"} ` +
    `rev ${binding.source_set_revision || "未知"} · namespace ${binding.namespace_id || "UNKNOWN"} · ` +
    `日历 ${binding.calendar_id || "UNKNOWN"}/${binding.calendar_revision || "UNKNOWN"} · ` +
    `${binding.price_coordinate || "UNKNOWN"} / ${binding.coordinate_id || "UNKNOWN"} · ` +
    `package SHA ${binding.package_sha256 || "UNKNOWN"} · ` +
    `身份 ${history.identity_ref || "UNKNOWN"}。`;
  for (const pair of windowData.pairs || []) {
    const row = document.createElement("tr");
    cell(row, pair.from_session || "—");
    cell(row, pair.to_session || "—");
    cell(
      row,
      `${timelineField(pair.from_ma20)} · ${text(pair.from_ma20?.quality || "UNKNOWN")}`,
    );
    cell(
      row,
      `${timelineField(pair.to_ma20)} · ${text(pair.to_ma20?.quality || "UNKNOWN")}`,
    );
    cell(row, ma20DirectionLabel(pair.direction));
    cell(
      row,
      (pair.unknown_reasons || [])
        .map((item) => `${text(item.side)}: ${text(item.code)}`)
        .join("；") || "—",
    );
    rows.append(row);
  }
  if (!windowData.pairs?.length)
    tableRow(rows, [
      "实际不足两个官方会话，当前没有相邻会话对。",
      "",
      "",
      "",
      "",
      "",
    ]);
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
function exactSymbolReturnState(context) {
  const binding = context?.returnBinding;
  const selection = context?.returnSelection;
  if (
    context?.origin !== "exact-symbol" ||
    binding?.origin !== "exact-symbol" ||
    !binding.namespace_id ||
    !binding.symbol ||
    !binding.before_snapshot?.complete ||
    !binding.after_snapshot?.complete ||
    binding.before_snapshot.target_session >=
      binding.after_snapshot.target_session ||
    context.namespace_id !== binding.namespace_id ||
    selection?.namespace_id !== binding.namespace_id ||
    state.namespace_id !== binding.namespace_id ||
    selection?.symbol !== binding.symbol ||
    selection?.session_count !== 75
  )
    return null;
  const beforeSnapshot = state.changeSnapshots.find(
    (snapshot) =>
      profileSnapshotKey(snapshot) ===
      profileSnapshotKey(binding.before_snapshot),
  );
  const afterSnapshot = state.changeSnapshots.find(
    (snapshot) =>
      profileSnapshotKey(snapshot) ===
      profileSnapshotKey(binding.after_snapshot),
  );
  const returnVersion = state.versions.find(
    (version) =>
      version.session_date === selection.target_session &&
      version.package_sha256 === selection.package_sha256,
  );
  if (!beforeSnapshot || !afterSnapshot || !returnVersion) return null;
  return { binding, selection, beforeSnapshot, afterSnapshot, returnVersion };
}

async function returnToChanges() {
  const context = state.timelineContext;
  const selection = context?.returnSelection;
  const detailItem = state.changeDetailItem;
  if (context?.origin === "exact-symbol") {
    const returnState = exactSymbolReturnState(context);
    if (!returnState) {
      $("timelineStatus").textContent =
        "原始精确双日快照或结束版本当前不可用；未回退其它日期或套用旧序位。";
      return;
    }
    const { binding, selection: originalSelection } = returnState;
    const currentBefore = selectedChangeSnapshot("changeBeforeSelect");
    const currentAfter = selectedChangeSnapshot("changeAfterSelect");
    const currentVersion = selectedVersion();
    const queryContextChanged =
      !currentBefore ||
      profileSnapshotKey(currentBefore) !==
        profileSnapshotKey(binding.before_snapshot) ||
      !currentAfter ||
      profileSnapshotKey(currentAfter) !==
        profileSnapshotKey(binding.after_snapshot) ||
      $("changeSymbolInput").value.trim().toUpperCase() !== binding.symbol ||
      $("changeTypeSelect").value !== binding.change_filter ||
      $("changeProfileFieldSelect").value !==
        (binding.profile_change_field || "") ||
      $("changeProfileKindSelect").value !==
        (binding.profile_change_kind || "") ||
      $("changeBeforeBias20AtrStateSelect").value !==
        (binding.before_bias20_atr_state || "ALL") ||
      $("changeAfterBias20AtrStateSelect").value !==
        (binding.after_bias20_atr_state || "ALL") ||
      state.changeOffset !== binding.change_offset ||
      state.changeLimit !== binding.change_limit ||
      state.changeBinding?.origin !== "exact-symbol" ||
      state.changeBinding?.symbol !== binding.symbol ||
      !state.changeDetailItem ||
      state.changeDetailItem.source_symbol !== binding.symbol;
    const mainSelectionChanged =
      !currentVersion ||
      currentVersion.target_session !== originalSelection.target_session ||
      currentVersion.package_sha256 !== originalSelection.package_sha256 ||
      $("symbolInput").value.trim().toUpperCase() !== binding.symbol ||
      Number($("windowInput").value) !== originalSelection.session_count;
    const returnNotice =
      queryContextChanged || mainSelectionChanged
        ? `已恢复原精确双日查询 ${binding.before_snapshot.target_session} → ${binding.after_snapshot.target_session} · ${binding.symbol}；重点候选序位已按原source_set、完整包SHA和profile revision重新读取。`
        : null;

    $("changeBeforeSelect").value = profileSnapshotKey(binding.before_snapshot);
    $("changeAfterSelect").value = profileSnapshotKey(binding.after_snapshot);
    $("changeSymbolInput").value = binding.symbol;
    $("changeTypeSelect").value = binding.change_filter || "ALL";
    $("changeProfileFieldSelect").value = binding.profile_change_field || "";
    $("changeProfileKindSelect").disabled = !binding.profile_change_field;
    $("changeProfileKindSelect").value = binding.profile_change_kind || "";
    $("changeBeforeBias20AtrStateSelect").value =
      binding.before_bias20_atr_state || "ALL";
    $("changeAfterBias20AtrStateSelect").value =
      binding.after_bias20_atr_state || "ALL";
    state.changeOffset = binding.change_offset ?? 0;
    state.changeLimit = binding.change_limit ?? state.changeLimit;
    $("versionSelect").value =
      `${originalSelection.target_session}|${originalSelection.package_sha256}`;
    $("symbolInput").value = originalSelection.symbol;
    $("windowInput").value = String(originalSelection.session_count);
    state.stockRequest++;
    state.timelineRequest++;
    state.timeline = null;
    state.timelineContext = null;
    state.timelineNavigation = false;
    state.chartPeriodContext = null;
    state.chartSelection = null;
    clearRawPeriodState(
      "返回精确代码核对结果；下一次轨迹查询会按精确身份读取周期数据。",
    );
    renderTimeline(null);
    clearResult();
    if (queryContextChanged) await loadChanges();
    await compareSourceProfilePair(returnNotice);
    $("changeDetail").scrollIntoView({ block: "start" });
    return;
  }
  if (
    !context ||
    !["change", "relation"].includes(context.origin) ||
    !selection
  )
    return;
  const changeReturnBinding =
    context.origin === "change" &&
    context.returnBinding?.return_origin === "change-list"
      ? context.returnBinding
      : null;
  if (changeReturnBinding) {
    const beforeSnapshot = state.changeSnapshots.find(
      (snapshot) =>
        profileSnapshotKey(snapshot) ===
        profileSnapshotKey(changeReturnBinding.before_snapshot),
    );
    const afterSnapshot = state.changeSnapshots.find(
      (snapshot) =>
        profileSnapshotKey(snapshot) ===
        profileSnapshotKey(changeReturnBinding.after_snapshot),
    );
    if (
      !beforeSnapshot?.complete ||
      !afterSnapshot?.complete ||
      changeReturnBinding.namespace_id !== state.namespace_id
    )
      return;
    $("changeBeforeSelect").value = profileSnapshotKey(beforeSnapshot);
    $("changeAfterSelect").value = profileSnapshotKey(afterSnapshot);
    $("changeTypeSelect").value = changeReturnBinding.change_type || "ALL";
    $("changeProfileFieldSelect").value =
      changeReturnBinding.profile_field_id || "";
    $("changeProfileKindSelect").disabled =
      !changeReturnBinding.profile_field_id;
    $("changeProfileKindSelect").value =
      changeReturnBinding.profile_change_kind || "";
    $("changeBeforeBias20AtrStateSelect").value =
      changeReturnBinding.before_bias20_atr_state || "ALL";
    $("changeAfterBias20AtrStateSelect").value =
      changeReturnBinding.after_bias20_atr_state || "ALL";
    $("changeSymbolInput").value = changeReturnBinding.symbol || "";
    state.changeOffset = changeReturnBinding.change_offset ?? 0;
    state.changeLimit = changeReturnBinding.change_limit ?? state.changeLimit;
    state.changeBinding = { ...changeReturnBinding, origin: "change" };
  }
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
  if (detailItem) showChangeDetail(detailItem);
  $("changeDetail").scrollIntoView({ block: "start" });
}
function returnToProfileBreadthMembers() {
  const returnState = state.profileBreadthMemberReturn;
  if (
    !returnState ||
    returnState.binding !== state.profileBreadthBinding ||
    !state.profileBreadthMemberData
  )
    return;
  if (state.timelineContext?.origin === "breadth") {
    const selection = returnState.selectedVersion;
    if (selection) {
      const value = `${selection.target_session}|${selection.package_sha256}`;
      if (
        Array.from($("versionSelect").options).some(
          (option) => option.value === value,
        )
      )
        $("versionSelect").value = value;
    }
    state.stockRequest++;
    state.timelineRequest++;
    state.timeline = null;
    state.timelineContext = null;
    state.timelineNavigation = false;
    state.chartPeriodContext = null;
    state.chartSelection = null;
    clearRawPeriodState("已返回来源池分类成员；历史轨迹状态已清除。");
    renderTimeline(null);
    clearResult();
  }
  state.pairRequest++;
  resetChangeDetail();
  loadProfileBreadthMembers(returnState.query, returnState.offset);
  $("profileBreadthMembers").scrollIntoView({ block: "start" });
}
function renderTimelineReturnControls(context, selected) {
  const returnRoot = $("timelineReturn");
  if (context?.origin === "comparison") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回多股对照";
    button.addEventListener("click", returnFromComparison);
    returnRoot.append(button);
  } else if (context?.origin === "breadth") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回广度分类成员";
    button.addEventListener("click", returnToProfileBreadthMembers);
    returnRoot.append(button);
  } else if (context?.origin === "change") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回画像变化详情";
    button.addEventListener("click", returnToChanges);
    returnRoot.append(button);
  } else if (context?.origin === "relation") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回RAW关系详情";
    button.addEventListener("click", returnToChanges);
    returnRoot.append(button);
  } else if (
    context?.origin === "exact-symbol" &&
    exactSymbolReturnState(context)
  ) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回精确代码核对结果";
    button.addEventListener("click", returnToChanges);
    returnRoot.append(button);
  } else if (context?.origin === "profile") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "返回画像筛选结果";
    button.addEventListener("click", returnToProfileSearch);
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
    renderMa20DirectionHistory(null);
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
  renderMa20DirectionHistory(data.ma20_direction_history);
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
    returnBinding: binding?.returnBinding || null,
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
    validateMa20DirectionHistory(data);
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
    ["命名空间", item.namespace_id],
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
  renderPricePositionDetail(root, item);
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
  const actions = document.createElement("div");
  actions.className = "profile-detail-actions";
  const timelineButton = document.createElement("button");
  timelineButton.type = "button";
  timelineButton.textContent = "查看75会话 RAW K / Core";
  timelineButton.addEventListener("click", () => {
    void openProfileTimeline(item);
  });
  actions.append(timelineButton);
  const returnButton = document.createElement("button");
  returnButton.type = "button";
  returnButton.className = "quiet";
  returnButton.textContent = "返回筛选结果";
  returnButton.addEventListener("click", returnToProfileSearch);
  actions.append(returnButton);
  root.append(actions);
}
async function openProfileTimeline(item) {
  const root = $("profileDetail");
  const value = `${item.target_session}|${item.package_sha256}`;
  const version = state.versions.find(
    (candidate) =>
      candidate.session_date === item.target_session &&
      candidate.package_sha256 === item.package_sha256,
  );
  const option = Array.from($("versionSelect").options).find(
    (candidate) => candidate.value === value,
  );
  if (!version || !option || !item.namespace_id || !item.source_set_id) {
    appendText(
      root,
      "p",
      "当前PG目录没有这条画像对应的精确RAW来源版本或source_set；已停止跳转，没有回退其它版本。",
      "message",
    );
    return;
  }
  $("versionSelect").value = value;
  $("symbolInput").value = item.source_symbol;
  $("windowInput").value = "75";
  const loaded = await queryStock({ preventDefault() {} });
  if (!loaded) {
    appendText(
      root,
      "p",
      "精确来源版本的RAW / Core读取未完成；画像筛选上下文仍保留。",
      "message",
    );
    return;
  }
  const persistedProfileSnapshot = {
    target_session: item.target_session,
    namespace_id: item.namespace_id,
    source_set_id: item.source_set_id,
    package_sha256: item.package_sha256,
    profile_revision: item.profile_revision,
  };
  await queryTimeline({
    namespace_id: item.namespace_id,
    source_set_id: item.source_set_id,
    origin: "profile",
    persistedProfileSnapshot,
  });
  $("timelinePanel").scrollIntoView({ behavior: "smooth", block: "start" });
}
function returnToProfileSearch() {
  clearProfileDetail();
  $("profileBrowserTitle").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
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
const bias20AtrStateLabels = {
  ALL: "全部状态（不筛选）",
  FAR_BELOW_ONE: "MA20下方超过1 ATR",
  NEAR_BELOW_ONE: "MA20下方1 ATR以内",
  AT_MA20: "等于MA20",
  NEAR_ABOVE_ONE: "MA20上方1 ATR以内",
  FAR_ABOVE_ONE: "MA20上方超过1 ATR",
  UNKNOWN: "未知（质量或坐标不可用）",
  ABSENT: "无该日持久画像",
  SINGLE_SIDE: "仅单侧有画像",
  PAIR_INCOMPARABLE: "两侧语义或坐标不可比",
  PAIR_UNKNOWN: "两侧质量不足以比较",
  COMPARABLE: "两侧可比",
};
const priceRelationFieldLabels = {
  c_vs_ma20: "收盘与MA20",
  c_vs_prior_high20: "收盘与排除当日的前20高点",
  ma5_vs_ma20: "MA5与MA20",
};
const priceRelationTransitionLabels = {
  ALL: "全部关系",
  STRICT_CROSS_UP: "严格上穿",
  STRICT_CROSS_DOWN: "严格下破",
  EQUAL_BOUNDARY: "经过等值边界",
  SAME_RELATION: "关系未变化",
  NOT_COMPARABLE: "不可比较",
  ONLY_BEFORE: "仅起始日有来源",
  ONLY_AFTER: "仅结束日有来源",
};
const priceRelationStatusLabels = {
  ABOVE: "高于",
  EQUAL: "等于",
  BELOW: "低于",
  UNKNOWN: "不可评价",
};
const priceMa20DirectionLabels = {
  ALL: "全部方向",
  INCREASED: "增加",
  UNCHANGED: "不变",
  DECREASED: "减少",
  UNKNOWN: "UNKNOWN（证据不足）",
};
const priceMa20DirectionReasonLabels = {
  BEFORE_PROFILE_MISSING: "起始日无画像",
  AFTER_PROFILE_MISSING: "结束日无画像",
  PRICE_COORDINATE_MISMATCH: "两端价格坐标不同",
  PRICE_COORDINATE_UNSUPPORTED: "价格坐标不支持",
  CORE_SEMANTICS_INCOMPARABLE: "Core语义不可比较",
  BEFORE_MA20_QUALITY_UNKNOWN: "起始日MA20质量未知",
  AFTER_MA20_QUALITY_UNKNOWN: "结束日MA20质量未知",
  BEFORE_MA20_VALUE_MISSING: "起始日MA20值缺失",
  AFTER_MA20_VALUE_MISSING: "结束日MA20值缺失",
  BEFORE_MA20_VALUE_INVALID: "起始日MA20数值无效",
  AFTER_MA20_VALUE_INVALID: "结束日MA20数值无效",
};
function priceMa20DirectionReasonText(reason) {
  const [code, detail] = String(reason || "UNKNOWN").split(/:(.*)/s);
  const label = priceMa20DirectionReasonLabels[code] || code;
  return detail ? `${label}：${detail}` : label;
}
const profilePairStatusLabels = {
  BOTH_PRESENT: "两侧均有持久画像",
  BEFORE_ONLY: "仅起始快照有画像",
  AFTER_ONLY: "仅结束快照有画像",
  NO_SAVED_SOURCE_CODE_MATCH: "两份已存来源集合都没有这个代码",
  RECORDED_CHANGE: "当前分类记录到状态变化",
  NO_RECORDED_STATE_CHANGE: "当前分类没有记录到状态变化",
  NOT_COMPARABLE: "来源或语义不可比",
  NOT_APPLICABLE: "不适用",
  COMPARABLE: "可比较",
  PARTIALLY_COMPARABLE: "部分可比较",
  UNKNOWN: "未知",
  SAME_VALUE: "数值相同",
  DIFFERENT_VALUES: "数值不同",
};
const corePriceLabels = {
  ma5: "MA5",
  ma10: "MA10",
  ma20: "MA20",
  ma60: "MA60",
  atr20: "ATR20",
  clv: "CLV",
  prior_high20: "排除当前日的前高20",
  ret20: "20会话收益",
  pos60: "60会话位置",
  bias20_atr: "MA20偏离ATR",
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
  highlights.rows.forEach((item, index) => {
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
    button.addEventListener("click", () =>
      showChangeDetail({
        ...item,
        highlight_lookup: {
          status: "DISPLAYED",
          reason: "SAME_PRODUCER_CANDIDATE",
          candidate: true,
          position: index + 1,
          category: item.highlight_category,
          priority: item.highlight_priority,
          eligible_count: highlights.eligible_count,
          display_count: highlights.display_count,
          undisplayed_count: highlights.undisplayed_count,
          display_limit: 30,
        },
      }),
    );
    detail.append(button);
    row.append(detail);
    root.append(row);
  });
  if (!highlights.rows.length)
    tableRow(root, ["已查询，没有符合展示规则的对象。", "", "", "", "", ""]);
}
function resetChangeDetail() {
  state.changeDetailItem = null;
  $("changeDetail").hidden = true;
  $("changeDetail").replaceChildren();
}
function appendHighlightLookup(root, item, lookup = item?.highlight_lookup) {
  if (!lookup) return;
  appendText(root, "h3", "首页重点变化榜核验（最多30条）");
  const messages = {
    DISPLAYED: `同一服务端生产者确认该代码符合重点候选规则，序位第${lookup.position}，已包含在本次榜单展示中。`,
    ELIGIBLE_NOT_DISPLAYED: `同一服务端生产者确认该代码符合重点候选规则，序位第${lookup.position}；超过首页最多${lookup.display_limit}条的展示上限，所以没有出现在首页榜单。`,
    OTHER_CHANGE_NOT_ELIGIBLE: `双日画像记录了${(item?.change_types || []).map((type) => changeTypeLabels[type] || type).join("、") || "实际变化"}；同一服务端重点候选生产者没有将它列为候选。`,
    NOT_TRIGGERED:
      "两端相关质量可比；当前比较语义没有记录会触发重点变化候选的差异。数值可能不同但仍未触发状态变化；这不等于正式选股未入选。",
    NO_SAVED_SOURCE_ROWS:
      "两端已存来源集合都没有这个代码的画像行，因此没有可进入本次重点变化候选的双日来源记录；不据此推断上市、退市、停牌或正式选股资格。",
    UNKNOWN_NOT_VERIFIED:
      "目前无法核实该代码的重点榜资格。存在单侧记录、字段质量UNKNOWN或语义不可比时，不从前30列表缺项推断排名或未计算。",
  };
  const message = messages[lookup?.status] || messages.UNKNOWN_NOT_VERIFIED;
  appendText(root, "p", message, "population highlight-lookup-message");
  if (lookup && Number.isInteger(lookup.eligible_count)) {
    appendText(
      root,
      "p",
      `同一双日版本候选 ${lookup.eligible_count} 只 · 展示 ${lookup.display_count}/${lookup.display_limit} · 符合候选但未展示 ${lookup.undisplayed_count} 只。该数值不是市场股票分母。`,
      "muted",
    );
  }
  if (lookup?.category) {
    appendText(
      root,
      "p",
      `服务端类别：${highlightCategoryLabels[lookup.category] || lookup.category} · 类别优先级 ${lookup.priority}。`,
      "muted",
    );
  }
}
function exactPairComparisonCandidate(item) {
  const binding = state.changeBinding;
  const supportsSourceBasket = ["exact-symbol", "change"].includes(
    binding?.origin,
  );
  const snapshot = supportsSourceBasket && binding.after_snapshot;
  if (
    !snapshot?.complete ||
    item.after_present !== true ||
    (binding.origin === "exact-symbol" &&
      item.source_symbol !== binding.symbol) ||
    item.after_source_set_id !== snapshot.source_set_id ||
    item.after_package_sha256 !== snapshot.package_sha256 ||
    item.after_profile_revision !== snapshot.profile_revision
  )
    return null;
  return {
    source_symbol: item.source_symbol,
    target_session: snapshot.target_session,
    namespace_id: binding.namespace_id,
    source_set_id: snapshot.source_set_id,
    package_sha256: snapshot.package_sha256,
    profile_revision: snapshot.profile_revision,
    origin_label:
      binding.origin === "change" ? "双日状态筛查" : "精确双日画像核对",
  };
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
  const returnVersion = state.versions.find(
    (version) =>
      version.session_date === returnDate &&
      version.package_sha256 === returnSha,
  );
  if (!returnVersion) return;
  const returnBinding =
    state.changeBinding?.origin === "exact-symbol"
      ? {
          ...state.changeBinding,
          before_snapshot: { ...state.changeBinding.before_snapshot },
          after_snapshot: { ...state.changeBinding.after_snapshot },
          change_filter: $("changeTypeSelect").value,
          profile_change_field: $("changeProfileFieldSelect").value,
          profile_change_kind: $("changeProfileKindSelect").value,
          before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
          after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
          change_offset: state.changeOffset,
          change_limit: state.changeLimit,
        }
      : state.changeBinding?.origin === "change"
        ? {
            ...state.changeBinding,
            before_snapshot: { ...state.changeBinding.before_snapshot },
            after_snapshot: { ...state.changeBinding.after_snapshot },
            change_type: $("changeTypeSelect").value,
            profile_field_id: $("changeProfileFieldSelect").value,
            profile_change_kind: $("changeProfileKindSelect").value,
            before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect")
              .value,
            after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
            change_offset: state.changeOffset,
            change_limit: state.changeLimit,
            return_origin: "change-list",
          }
        : null;
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
    origin: state.changeBinding?.origin || "change",
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
      target_session: returnVersion.session_date,
      package_sha256: returnVersion.package_sha256,
      namespace_id: state.changeBinding.namespace_id,
      symbol: item.source_symbol,
      session_count: 75,
    },
    returnBinding,
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
  state.changeDetailItem = item;
  const root = $("changeDetail");
  root.replaceChildren();
  root.hidden = false;
  appendText(
    root,
    "h3",
    `${item.source_symbol} · ${item.before_profile_revision || "无前侧记录"} → ${item.after_profile_revision || "无后侧记录"}`,
  );
  if (state.changeBinding?.origin === "breadth") {
    appendText(
      root,
      "p",
      `从来源池广度分类“${breadthMemberQueryLabel(state.changeBinding.breadth_query)}”打开。返回时保留原始分类、精确快照和分页位置。`,
      "muted",
    );
    const back = document.createElement("button");
    back.type = "button";
    back.className = "quiet";
    back.textContent = "返回广度分类成员";
    back.addEventListener("click", returnToProfileBreadthMembers);
    root.append(back);
  }
  if (state.changeBinding?.origin === "relation") {
    appendText(
      root,
      "p",
      `从RAW关系${priceRelationFieldLabels[state.relationQuery?.relation_field] || ""}筛选结果打开；返回会保留A类、B结束日条件、快照身份和偏移${state.relationOffset}。`,
      "muted",
    );
    const back = document.createElement("button");
    back.type = "button";
    back.className = "quiet";
    back.textContent = "返回RAW关系筛选结果";
    back.addEventListener("click", returnToRelationScreen);
    root.append(back);
  }
  if (item.pair_status) {
    const explanation = document.createElement("p");
    explanation.className = "population profile-pair-explanation";
    if (item.state_change_status === "NO_RECORDED_STATE_CHANGE") {
      explanation.textContent =
        "两侧均有已保存画像；所比较字段没有记录到符合当前变化分类的差异。此结果不表示收盘价相同，也不表示正式选股未入选。";
      if (item.raw_close_comparison_status === "DIFFERENT_VALUES")
        explanation.textContent +=
          " 可核RAW收盘数值不同，但未触发当前已记录状态变化。";
      if (
        item.close_ma20_comparison_status === "UNKNOWN" ||
        item.core_comparison_status === "PARTIALLY_COMPARABLE" ||
        item.profile_comparison_status === "PARTIALLY_COMPARABLE"
      )
        explanation.textContent +=
          " 部分价格关系、Core或Profile字段的质量为UNKNOWN/仅部分可比；未记录到分类变化不表示这些数值相同。";
    } else if (item.state_change_status === "RECORDED_CHANGE") {
      explanation.textContent = `现役变化分类记录到：${(item.change_types || []).map((type) => changeTypeLabels[type] || type).join("、")}。这属于来源代码级NON_PIT观察，不代表正式选股结论。`;
    } else if (item.state_change_status === "NOT_COMPARABLE") {
      explanation.textContent =
        "两侧都有画像，但Core或Profile语义不兼容；不可比较字段的空变化标签不能解释为无变化。仍可查看两侧有证据的Raw数值和逐字段质量。";
    } else if (item.pair_status === "BEFORE_ONLY") {
      explanation.textContent =
        "该来源代码只在起始快照有画像记录；没有后侧记录，不能据此推断停牌、退市或正式未入选。";
    } else if (item.pair_status === "AFTER_ONLY") {
      explanation.textContent =
        "该来源代码只在结束快照有画像记录；没有前侧记录，不能据此推断新上市或正式入选。";
    }
    root.append(explanation);
  }
  appendHighlightLookup(root, item);
  const meta = document.createElement("dl");
  meta.className = "profile-detail-meta";
  const pairMeta = item.pair_status
    ? [
        [
          "两侧画像记录",
          profilePairStatusLabels[item.pair_status] || item.pair_status,
        ],
        [
          "当前状态变化分类",
          profilePairStatusLabels[item.state_change_status] ||
            item.state_change_status,
        ],
        [
          "RAW OHLC可比性",
          profilePairStatusLabels[item.raw_ohlc_comparison_status] ||
            item.raw_ohlc_comparison_status,
        ],
        [
          "收盘数值对照",
          profilePairStatusLabels[item.raw_close_comparison_status] ||
            item.raw_close_comparison_status,
        ],
        [
          "Core / Profile数值质量",
          `${profilePairStatusLabels[item.core_comparison_status] || item.core_comparison_status} / ${profilePairStatusLabels[item.profile_comparison_status] || item.profile_comparison_status}`,
        ],
      ]
    : [];
  for (const [label, value] of [
    ...pairMeta,
    [
      "实际变化类型",
      (item.change_types || [])
        .map((type) => changeTypeLabels[type] || type)
        .join("、") || (item.pair_status ? "无（当前分类未记录）" : ""),
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
  if (
    item.before_bias20_atr_state &&
    item.after_bias20_atr_state &&
    item.bias20_atr_pair_compatibility
  ) {
    appendText(root, "h3", "双日 MA20 / ATR 精确距离状态");
    for (const [side, label] of [
      ["before", "起始"],
      ["after", "结束"],
    ]) {
      const endpointValue = (field) => text(item[`${side}_${field}`]);
      appendText(
        root,
        "p",
        `${label}：${changeBias20AtrStateText(item, side)} · 坐标 ${endpointValue("price_coordinate")} · 目标价质量 ${endpointValue("target_price_quality")} · RAW收盘 ${endpointValue("raw_close")} (${endpointValue("raw_price_quality")} / ${endpointValue("raw_price_reason")}) · MA20 ${endpointValue("ma20")} (${endpointValue("ma20_quality")} / ${endpointValue("ma20_reason")}) · ATR20 ${endpointValue("atr20")} (${endpointValue("atr20_quality")} / ${endpointValue("atr20_reason")})`,
        "population",
      );
    }
    appendText(
      root,
      "p",
      `两端可比性：${bias20AtrStateLabels[item.bias20_atr_pair_compatibility] || item.bias20_atr_pair_compatibility} · ${item.bias20_atr_transition_status} · ${text(item.bias20_atr_pair_reason)}。只有COMPARABLE且状态不同才表示本次两个已存快照的距离状态不同；不构成未来预警或交易信号。`,
      "muted",
    );
  }
  if (item.pair_status) appendProfilePairValues(root, item);
  appendPriceRelationTransitions(root, item);
  appendChangeTimelineLinks(root, item);
  if (["exact-symbol", "change"].includes(state.changeBinding?.origin)) {
    appendText(root, "h3", "加入结束日来源版多股临时对照");
    const candidate = exactPairComparisonCandidate(item);
    const add = makeComparisonAddButton(candidate);
    if (!candidate) {
      add.title =
        "结束快照没有该代码的完整SEALED画像身份，无法加入来源版对照。";
      appendText(
        root,
        "p",
        "该代码不在结束快照或缺少完整SEALED身份；原有临时对照篮与筛选保持不变。",
        "muted",
      );
    }
    root.append(add);
    appendText(
      root,
      "p",
      "需在同一结束日、namespace、source_set、完整包SHA和profile_revision下选2–3只；这是临时来源版对照，不代表正式Focus或选股名单。",
      "muted",
    );
  }
  appendText(root, "h3", "Profile11状态、质量与UNKNOWN原因");
  if (!item.profile_compatible && !item.pair_status) {
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
    const fieldValues = (item, side) => {
      const ids = item[`${side}_profile_field_ids`] || [];
      const statuses = item[`${side}_profile_statuses`] || [];
      const qualities = item[`${side}_profile_qualities`] || [];
      const reasons = item[`${side}_profile_reasons`] || [];
      if (
        ids.length !== statuses.length ||
        ids.length !== qualities.length ||
        ids.length !== reasons.length
      )
        return new Map();
      return new Map(
        ids.map((field, index) => [
          field,
          {
            status: statuses[index],
            quality: qualities[index],
            reason: reasons[index],
          },
        ]),
      );
    };
    const beforeFields = fieldValues(item, "before");
    const afterFields = fieldValues(item, "after");
    const fieldIds = [
      ...new Set([
        ...(item.before_profile_field_ids || []),
        ...(item.after_profile_field_ids || []),
      ]),
    ];
    for (const field of fieldIds) {
      const beforeField = beforeFields.get(field);
      const afterField = afterFields.get(field);
      const describe = (value) =>
        value
          ? `${text(value.status)} / ${text(value.quality)} / ${text(value.reason)}`
          : "该侧无此字段";
      tableRow(body, [field, describe(beforeField), describe(afterField)]);
    }
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
function relationEndpointValue(item, side, field) {
  const isClose = field === "raw_close";
  return {
    value: item[`${side}_${isClose ? "raw_close" : field}`],
    quality:
      item[`${side}_${isClose ? "raw_price_quality" : `${field}_quality`}`],
    reason: item[`${side}_${isClose ? "raw_price_reason" : `${field}_reason`}`],
  };
}
function appendPriceRelationTransitions(root, item) {
  const transitions = item.price_relation_transitions;
  if (!transitions) return;
  appendText(root, "h3", "三类RAW价格关系的两端值与转移原因");
  appendText(
    root,
    "p",
    "收盘与排除当日的前20高点比较的是两个目标日各自的滚动阈值，不代表同一固定阻力位或突破确认。正式选股状态仍为NOT_EVALUATED。",
    "muted",
  );
  const table = document.createElement("table");
  table.innerHTML =
    "<thead><tr><th>关系字段</th><th>起始日数值 / 质量</th><th>结束日数值 / 质量</th><th>两端关系</th><th>转移与具体原因</th></tr></thead>";
  const body = document.createElement("tbody");
  for (const [field, definition] of Object.entries(transitions)) {
    const fields = {
      c_vs_ma20: ["raw_close", "ma20"],
      c_vs_prior_high20: ["raw_close", "prior_high20"],
      ma5_vs_ma20: ["ma5", "ma20"],
    }[field];
    const endpoint = (side) =>
      fields
        .map((valueField) => {
          const value = relationEndpointValue(item, side, valueField);
          return `${valueField}=${text(value.value)} (${text(value.quality)}; ${text(value.reason)})`;
        })
        .join(" vs ");
    tableRow(body, [
      priceRelationFieldLabels[field] || field,
      endpoint("before"),
      endpoint("after"),
      `${priceRelationStatusLabels[definition.before_relation] || definition.before_relation} → ${priceRelationStatusLabels[definition.after_relation] || definition.after_relation}`,
      `${priceRelationTransitionLabels[definition.transition_type] || definition.transition_type}${definition.transition_reason ? `：${definition.transition_reason}` : ""}`,
    ]);
  }
  table.append(body);
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  wrap.append(table);
  root.append(wrap);
}
function appendProfilePairValues(root, item) {
  appendText(root, "h3", "两端RAW OHLC（原生未复权价格）");
  appendText(
    root,
    "p",
    `前侧 ${text(state.changeBinding?.before_snapshot?.target_session)} · Raw行${item.before_raw_record_present ? "存在" : "缺失"} · ${text(item.before_raw_price_quality)} / ${text(item.before_raw_price_reason)}；后侧 ${text(state.changeBinding?.after_snapshot?.target_session)} · Raw行${item.after_raw_record_present ? "存在" : "缺失"} · ${text(item.after_raw_price_quality)} / ${text(item.after_raw_price_reason)}。量额单位未核实，不影响合法RAW OHLC价格展示。`,
    "muted",
  );
  const rawTable = document.createElement("table");
  rawTable.innerHTML =
    "<thead><tr><th>字段</th><th>起始快照</th><th>结束快照</th></tr></thead>";
  const rawBody = document.createElement("tbody");
  for (const [field, label] of [
    ["open", "开盘"],
    ["high", "最高"],
    ["low", "最低"],
    ["close", "收盘"],
  ])
    tableRow(rawBody, [
      label,
      `${text(item[`before_raw_${field}`])} / ${text(item.before_raw_price_quality)} / ${text(item.before_raw_price_reason)}`,
      `${text(item[`after_raw_${field}`])} / ${text(item.after_raw_price_quality)} / ${text(item.after_raw_price_reason)}`,
    ]);
  rawTable.append(rawBody);
  const rawWrap = document.createElement("div");
  rawWrap.className = "table-wrap";
  rawWrap.append(rawTable);
  root.append(rawWrap);
  appendText(root, "h3", "Core两日数值、质量与原因");
  const coreTable = document.createElement("table");
  coreTable.innerHTML =
    "<thead><tr><th>字段</th><th>起始快照：值 / 质量 / 原因</th><th>结束快照：值 / 质量 / 原因</th></tr></thead>";
  const coreBody = document.createElement("tbody");
  for (const [field, label] of Object.entries(corePriceLabels))
    tableRow(coreBody, [
      label,
      `${text(item[`before_${field}`])} / ${text(item[`before_${field}_quality`])} / ${text(item[`before_${field}_reason`])}`,
      `${text(item[`after_${field}`])} / ${text(item[`after_${field}_quality`])} / ${text(item[`after_${field}_reason`])}`,
    ]);
  coreTable.append(coreBody);
  const coreWrap = document.createElement("div");
  coreWrap.className = "table-wrap";
  coreWrap.append(coreTable);
  root.append(coreWrap);
}
async function compareSourceProfilePair(notice = null) {
  if (typeof notice !== "string") notice = null;
  const symbol = $("changeSymbolInput").value.trim().toUpperCase();
  const before = selectedChangeSnapshot("changeBeforeSelect");
  const after = selectedChangeSnapshot("changeAfterSelect");
  const root = $("changeDetail");
  resetChangeDetail();
  const requestId = ++state.pairRequest;
  if (!before || !after || before.target_session >= after.target_session) {
    root.hidden = false;
    appendText(root, "p", "请先选择日期递增的两个精确PG画像快照。", "message");
    return;
  }
  if (!/^(SH|SZ)\.[0-9]{6}$/.test(symbol)) {
    root.hidden = false;
    appendText(
      root,
      "p",
      "请输入SH.或SZ.加六位数字的精确来源代码。",
      "message",
    );
    return;
  }
  const params = new URLSearchParams({
    before_session: before.target_session,
    after_session: after.target_session,
    symbol,
    before_source_set_id: before.source_set_id,
    before_package_sha256: before.package_sha256,
    before_profile_revision: before.profile_revision,
    after_source_set_id: after.source_set_id,
    after_package_sha256: after.package_sha256,
    after_profile_revision: after.profile_revision,
  });
  root.hidden = false;
  appendText(
    root,
    "p",
    "正在读取这两份精确画像快照中的单个来源代码…",
    "population",
  );
  $("compareProfilePair").disabled = true;
  try {
    const data = await api(`/api/source-symbol-price-profile-pair?${params}`);
    if (requestId !== state.pairRequest) return;
    if (data.status === "NO_SAVED_SOURCE_CODE_MATCH") {
      state.changeBinding = {
        namespace_id: data.namespace_id,
        before_snapshot: data.before_snapshot,
        after_snapshot: data.after_snapshot,
        session_relation: data.session_relation,
        offset: 0,
        limit: 1,
        change_type: "ALL",
        symbol,
        before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
        after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
        origin: "exact-symbol",
      };
      root.replaceChildren();
      root.hidden = false;
      appendText(root, "h3", `${symbol} · 本两份已存来源集合没有这个代码`);
      appendText(
        root,
        "p",
        "两份精确PG画像快照中都没有该来源代码的持久画像记录。此结果不判断退市、停牌、官方上市总体或正式选股资格。",
        "population",
      );
      appendHighlightLookup(root, null, data.highlight_lookup);
      appendText(
        root,
        "p",
        `${before.target_session} · source_set ${before.source_set_id} · SHA ${before.package_sha256} · revision ${before.profile_revision} → ${after.target_session} · source_set ${after.source_set_id} · SHA ${after.package_sha256} · revision ${after.profile_revision}`,
        "muted",
      );
      if (notice) appendText(root, "p", notice, "muted");
      return;
    }
    state.changeBinding = {
      namespace_id: data.namespace_id,
      before_snapshot: data.before_snapshot,
      after_snapshot: data.after_snapshot,
      session_relation: data.session_relation,
      offset: 0,
      limit: 1,
      change_type: "ALL",
      symbol,
      before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
      after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
      origin: "exact-symbol",
    };
    showChangeDetail({
      ...data.item,
      highlight_lookup: data.highlight_lookup,
    });
    if (notice) appendText(root, "p", notice, "muted");
  } catch (error) {
    if (requestId !== state.pairRequest) return;
    root.replaceChildren();
    root.hidden = false;
    appendText(root, "p", `双日画像对照未完成：${error.message}`, "message");
  } finally {
    if (requestId === state.pairRequest)
      $("compareProfilePair").disabled = false;
  }
}
function populateChangeSnapshots(snapshots) {
  populateChangeProfileFields();
  const complete = snapshots
    .filter((item) => item.complete)
    .slice()
    .sort(
      (a, b) =>
        a.target_session.localeCompare(b.target_session) ||
        a.source_set_id.localeCompare(b.source_set_id) ||
        a.profile_revision.localeCompare(b.profile_revision),
    );
  state.changeSnapshots = complete;
  for (const id of ["changeBeforeSelect", "changeAfterSelect"]) {
    const select = $(id);
    select.replaceChildren(new Option("— 选择完整PG画像快照 —", ""));
    for (const item of complete) {
      const option = new Option(
        `${item.target_session} · ${item.source_set_id.slice(0, 8)} · SHA ${item.package_sha256.slice(0, 8)} · rev ${item.profile_revision.slice(0, 12)} · ${item.source_symbol_count}/${item.expected_source_symbol_count}`,
        profileSnapshotKey(item),
      );
      option.title = `${item.target_session} · source_set ${item.source_set_id} · package SHA ${item.package_sha256} · profile revision ${item.profile_revision}`;
      select.append(option);
    }
    select.disabled = complete.length < 2;
  }
  const dates = [...new Set(complete.map((item) => item.target_session))];
  if (dates.length >= 2) {
    const beforeVersions = complete.filter(
      (item) => item.target_session === dates.at(-2),
    );
    const afterVersions = complete.filter(
      (item) => item.target_session === dates.at(-1),
    );
    $("searchChanges").disabled = false;
    if (beforeVersions.length === 1 && afterVersions.length === 1) {
      $("changeBeforeSelect").value = profileSnapshotKey(beforeVersions[0]);
      $("changeAfterSelect").value = profileSnapshotKey(afterVersions[0]);
      loadChanges();
      loadPriceRelations();
    } else {
      $("changeBeforeSelect").value = "";
      $("changeAfterSelect").value = "";
      clearRelationScreen(
        "至少一个日期有多个精确来源版本；请选择唯一前后快照后再查询RAW关系。",
      );
      $("changeSummary").textContent =
        "至少一个日期有多个精确来源版本；请在两个快照选择器中明确选择具体revision。";
      $("changeHighlightsCount").textContent = "等待精确快照";
      $("changeHighlightsSummary").textContent =
        "请选择日期递增的两个精确PG画像快照；不会自动猜测或回退latest。";
    }
  } else {
    $("searchChanges").disabled = true;
    clearRelationScreen(
      "需要两个不同日期且分母闭合的已保存画像版本才能查询RAW关系。",
    );
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
function populateChangeProfileFields() {
  const fieldSelect = $("changeProfileFieldSelect");
  const selectedField = fieldSelect.value;
  fieldSelect.replaceChildren(new Option("不按字段质量/状态变化筛选", ""));
  for (const field of Object.keys(profileFieldLabels)) {
    fieldSelect.append(new Option(profileFieldLabel(field), field));
  }
  fieldSelect.value = Object.hasOwn(profileFieldLabels, selectedField)
    ? selectedField
    : "";
  const kindSelect = $("changeProfileKindSelect");
  kindSelect.disabled = !fieldSelect.value;
  if (!fieldSelect.value) {
    kindSelect.value = "";
  } else if (!kindSelect.value) {
    kindSelect.value = "ANY_STATE_CHANGED";
  }
}
function profileSnapshotKey(snapshot) {
  return [
    snapshot.target_session,
    snapshot.source_set_id,
    snapshot.package_sha256,
    snapshot.profile_revision,
  ].join("|");
}
function selectedChangeSnapshot(id) {
  const key = $(id).value;
  return state.changeSnapshots.find(
    (snapshot) => profileSnapshotKey(snapshot) === key,
  );
}
function relationDetailQueryKey(query, relationData, sourceSymbol) {
  if (!query || !relationData || !sourceSymbol) return null;
  const snapshotIdentity = (snapshot) => ({
    target_session: snapshot?.target_session,
    source_set_id: snapshot?.source_set_id,
    package_sha256: snapshot?.package_sha256,
    profile_revision: snapshot?.profile_revision,
  });
  const before = snapshotIdentity(query.before);
  const after = snapshotIdentity(query.after);
  if (
    !before.target_session ||
    !before.source_set_id ||
    !before.package_sha256 ||
    !before.profile_revision ||
    !after.target_session ||
    !after.source_set_id ||
    !after.package_sha256 ||
    !after.profile_revision ||
    !relationData.namespace_id
  )
    return null;
  return JSON.stringify({
    relation_request: state.relationRequest,
    namespace_id: relationData.namespace_id,
    before,
    after,
    source_symbol: sourceSymbol,
    relation_field: query.relation_field,
    transition_type: query.transition_type,
    ma20_direction: query.ma20_direction,
    end_position_filters: (query.end_position_filters || []).map((item) => [
      item.field,
      item.status,
    ]),
    offset: query.offset,
    limit: query.limit,
  });
}
function relationDetailRequestIsCurrent(requestId, bindingKey, sourceSymbol) {
  const binding = state.changeBinding;
  return (
    requestId === state.relationDetailRequest &&
    bindingKey !== null &&
    bindingKey ===
      relationDetailQueryKey(
        state.relationQuery,
        state.relationData,
        sourceSymbol,
      ) &&
    bindingKey === state.relationDetailBindingKey &&
    binding?.origin === "relation" &&
    binding.relation_query_key === bindingKey &&
    binding.source_symbol === sourceSymbol
  );
}
function relationDetailResponseMatches(
  data,
  query,
  relationData,
  sourceSymbol,
) {
  const sameSnapshot = (actual, expected) =>
    actual?.target_session === expected?.target_session &&
    actual?.source_set_id === expected?.source_set_id &&
    actual?.package_sha256 === expected?.package_sha256 &&
    actual?.profile_revision === expected?.profile_revision;
  return (
    data?.source_symbol === sourceSymbol &&
    (data?.item?.source_symbol === undefined ||
      data?.item?.source_symbol === sourceSymbol) &&
    data?.namespace_id === relationData?.namespace_id &&
    sameSnapshot(data?.before_snapshot, query?.before) &&
    sameSnapshot(data?.after_snapshot, query?.after) &&
    sameSnapshot(data?.before_snapshot, relationData?.before_snapshot) &&
    sameSnapshot(data?.after_snapshot, relationData?.after_snapshot)
  );
}
function invalidateRelationDetailRequest() {
  state.relationDetailRequest++;
  state.relationDetailRow = null;
  state.relationDetailBindingKey = null;
  if (state.changeBinding?.origin === "relation") {
    state.changeBinding = null;
    if (state.timelineContext?.origin === "relation") invalidateTimeline();
    resetChangeDetail();
  }
}
function clearRelationScreen(message = "请选择两个完整PG画像快照。") {
  invalidateRelationDetailRequest();
  state.relationRequest++;
  state.relationPageRequest++;
  state.relationData = null;
  state.relationQuery = null;
  state.relationTotal = 0;
  state.relationOffset = 0;
  $("priceRelationScreenCount").textContent = "等待精确快照";
  $("priceRelationScreenPage").textContent = "—";
  $("priceRelationScreenRows").replaceChildren();
  tableRow($("priceRelationScreenRows"), [message, "", "", "", "", "", ""]);
  $("priceRelationScreenSummary").textContent = message;
  $("priceRelationPrev").disabled = true;
  $("priceRelationNext").disabled = true;
  $("priceRelationLast").disabled = true;
}
function relationSnapshotParams(params, prefix, snapshot) {
  params.set(prefix + "_source_set_id", snapshot.source_set_id);
  params.set(prefix + "_package_sha256", snapshot.package_sha256);
  params.set(prefix + "_profile_revision", snapshot.profile_revision);
}
function relationValueText(item, field, side) {
  const value = relationEndpointValue(item, side, field);
  return (
    field +
    "=" +
    text(value.value) +
    " (" +
    text(value.quality) +
    "；" +
    text(value.reason) +
    ")"
  );
}
function relationEndpointText(item, field, side) {
  const definition = item.price_relation_transitions?.[field];
  if (!definition) return "未计算";
  const relation = definition[side + "_relation"];
  const reason = definition[side + "_reason"];
  return (
    (priceRelationStatusLabels[relation] || relation) +
    " · " +
    relationValueText(item, definition.left_field, side) +
    " vs " +
    relationValueText(item, definition.right_field, side) +
    (reason ? " · " + reason : "")
  );
}
function relationEndFilterText(item, filters) {
  if (!filters?.length) return "无结束日附加条件";
  return filters
    .map((filter) => {
      const transition = item.price_relation_transitions?.[filter.field];
      const actual = transition?.after_relation;
      const reason = transition?.after_reason;
      return (
        priceRelationFieldLabels[filter.field] +
        " " +
        priceRelationStatusLabels[filter.status] +
        "；实际" +
        (priceRelationStatusLabels[actual] || actual || "未知") +
        (reason ? "（" + reason + "）" : "")
      );
    })
    .join(" AND ");
}
function renderRelationScreen(data, requestId) {
  if (requestId !== state.relationRequest) return;
  const field = data.relation_field;
  const counts = data.summary.transition_counts;
  const ma20Counts = data.summary.ma20_direction_counts;
  const ma20UnknownReasons = data.summary.ma20_direction_unknown_reason_counts
    .map((item) => `${priceMa20DirectionReasonText(item.reason)}=${item.count}`)
    .join("；");
  const reasonText = data.summary.unknown_reason_counts
    .map(
      (item) =>
        (item.side === "BEFORE" ? "前侧 " : "后侧 ") +
        (item.reason || "未提供原因") +
        "=" +
        item.count,
    )
    .join("；");
  const sourceCodeStatus = !state.relationQuery?.source_symbol
    ? "未按来源代码筛选"
    : data.source_code_presence?.status === "NO_PERSISTED_SOURCE_ROW"
      ? "该代码在两份快照中都没有持久来源行"
      : data.page.total === 0
        ? "该代码有持久来源行，但未匹配当前方向、关系或结束日条件"
        : "该代码符合当前方向、关系和结束日条件";
  $("priceRelationScreenCount").textContent =
    data.page.total === 0
      ? "0个当前组合命中（已计算但无匹配）"
      : data.page.total + " 个当前组合命中";
  $("priceRelationScreenSummary").textContent =
    sessionRelationLabel(data.session_relation) +
    " · 完整来源并集 U=" +
    data.summary.source_symbol_union_count +
    "，两端交集 I=" +
    data.summary.intersection_count +
    "，仅前=" +
    data.summary.before_only_count +
    "，仅后=" +
    data.summary.after_only_count +
    "。" +
    "双日MA20方向（完整并集N=" +
    data.summary.source_symbol_union_count +
    "）：增加 " +
    ma20Counts.INCREASED +
    "、不变 " +
    ma20Counts.UNCHANGED +
    "、减少 " +
    ma20Counts.DECREASED +
    "、UNKNOWN " +
    ma20Counts.UNKNOWN +
    "；UNKNOWN主因 " +
    (ma20UnknownReasons || "无UNKNOWN") +
    "。当前方向筛选 " +
    (priceMa20DirectionLabels[data.ma20_direction] || data.ma20_direction) +
    "。" +
    priceRelationFieldLabels[field] +
    "：可比较 " +
    data.summary.comparable_count +
    "（上穿 " +
    counts.strict_cross_up +
    "、下破 " +
    counts.strict_cross_down +
    "、等值边界 " +
    counts.equal_boundary +
    "、未变化 " +
    counts.same_relation +
    "），不可比较 " +
    data.summary.not_comparable_count +
    "（前侧UNKNOWN " +
    data.summary.before_unknown_count +
    "、后侧UNKNOWN " +
    data.summary.after_unknown_count +
    "）。当前关系类别 " +
    priceRelationTransitionLabels[data.transition_type] +
    "；结束日AND条件 " +
    (data.end_position_filters
      .map(
        (item) =>
          priceRelationFieldLabels[item.field] +
          " " +
          priceRelationStatusLabels[item.status],
      )
      .join(" AND ") || "无") +
    "；精确来源筛选 " +
    (state.relationQuery?.source_symbol || "无") +
    "（" +
    sourceCodeStatus +
    "）" +
    "；命中 " +
    data.page.total +
    "。未知原因：" +
    (reasonText || "本字段无UNKNOWN端点") +
    "。namespace " +
    data.namespace_id +
    "；起始 " +
    data.before_snapshot.target_session +
    " / source_set " +
    data.before_snapshot.source_set_id +
    " / SHA " +
    data.before_snapshot.package_sha256 +
    " / revision " +
    data.before_snapshot.profile_revision +
    "；结束 " +
    data.after_snapshot.target_session +
    " / source_set " +
    data.after_snapshot.source_set_id +
    " / SHA " +
    data.after_snapshot.package_sha256 +
    " / revision " +
    data.after_snapshot.profile_revision +
    "。SOURCE_SYMBOL_ONLY · RAW_UNADJUSTED · NON_PIT；正式选股未评价。";
  const root = $("priceRelationScreenRows");
  root.replaceChildren();
  if (!data.rows.length) {
    tableRow(root, [
      data.page.total === 0
        ? "当前组合没有符合来源；空结果不会回退到其它类别或旧变化列表。"
        : "该偏移没有结果；可返回首页或末页。",
      "",
      "",
      "",
      "",
      "",
      "",
    ]);
  }
  for (const item of data.rows) {
    const row = document.createElement("tr");
    const identity =
      item.source_symbol +
      " · " +
      item.before_source_set_id +
      " / " +
      item.before_package_sha256 +
      " / " +
      item.before_profile_revision +
      " → " +
      item.after_source_set_id +
      " / " +
      item.after_package_sha256 +
      " / " +
      item.after_profile_revision;
    cell(row, identity);
    cell(row, relationEndpointText(item, field, "before"));
    cell(row, relationEndpointText(item, field, "after"));
    const transition = item.price_relation_transitions[field];
    cell(
      row,
      (priceRelationTransitionLabels[transition.transition_type] ||
        transition.transition_type) +
        (transition.transition_reason
          ? " · " + transition.transition_reason
          : ""),
    );
    const direction = item.ma20_direction || "UNKNOWN";
    const beforeMa20 =
      item.before_ma20 === null || item.before_ma20 === undefined
        ? "—"
        : String(item.before_ma20);
    const afterMa20 =
      item.after_ma20 === null || item.after_ma20 === undefined
        ? "—"
        : String(item.after_ma20);
    const directionReason = item.ma20_direction_reason
      ? " · " + priceMa20DirectionReasonText(item.ma20_direction_reason)
      : "";
    cell(
      row,
      `存储MA20 ${beforeMa20} → ${afterMa20} · ${priceMa20DirectionLabels[direction] || direction}${directionReason}`,
    );
    cell(row, relationEndFilterText(item, data.end_position_filters));
    const detail = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent = "双日详情 / 75日轨迹";
    button.addEventListener("click", () => openRelationDetail(item));
    detail.append(
      button,
      makeComparisonAddButton(relationComparisonCandidate(item)),
    );
    row.append(detail);
    root.append(row);
  }
  $("priceRelationScreenPage").textContent =
    "偏移 " +
    data.page.offset +
    " · 本页 " +
    data.page.count +
    "/" +
    data.page.limit +
    " · 当前组合总计 " +
    data.page.total;
  $("priceRelationPrev").disabled = data.page.offset === 0;
  $("priceRelationNext").disabled = data.page.next_offset === null;
  $("priceRelationLast").disabled =
    data.page.last_offset === null || data.page.offset >= data.page.last_offset;
}
function scheduleRelationRefresh(delay) {
  if (relationRefreshTimer !== null) window.clearTimeout(relationRefreshTimer);
  relationRefreshTimer = window.setTimeout(() => {
    relationRefreshTimer = null;
    if (relationRequestInFlight) return;
    void runPriceRelations(state.relationRequest);
  }, delay);
}
function loadPriceRelations() {
  if (state.timelineContext?.origin === "relation") invalidateTimeline();
  invalidateRelationDetailRequest();
  state.relationRequest++;
  relationRefreshPending = true;
  state.relationData = null;
  state.relationQuery = null;
  $("priceRelationScreenCount").textContent = "筛选更新中";
  $("priceRelationScreenSummary").textContent =
    "筛选已变化；连续操作合并后读取最新组合。";
  $("priceRelationScreenRows").replaceChildren();
  tableRow($("priceRelationScreenRows"), ["等待筛选稳定…", "", "", "", "", ""]);
  $("priceRelationScreenPage").textContent = "查询中";
  $("priceRelationPrev").disabled = true;
  $("priceRelationNext").disabled = true;
  $("priceRelationLast").disabled = true;
  $("searchPriceRelations").disabled = true;
  if (relationRefreshTimer !== null) window.clearTimeout(relationRefreshTimer);
  relationRefreshTimer = window.setTimeout(() => {
    relationRefreshTimer = null;
    if (!relationRequestInFlight) void runPriceRelations(state.relationRequest);
  }, previewQueryDebounceMs);
}
async function runPriceRelations(requestId) {
  if (
    !relationRefreshPending ||
    requestId !== state.relationRequest ||
    relationRequestInFlight
  )
    return;
  relationRefreshPending = false;
  const before = selectedChangeSnapshot("changeBeforeSelect");
  const after = selectedChangeSnapshot("changeAfterSelect");
  if (!before || !after || before.target_session >= after.target_session) {
    clearRelationScreen("请选择日期递增的两个完整PG画像快照。");
    $("searchPriceRelations").disabled = false;
    return;
  }
  const positionFields = [
    $("endPosition1Field").value,
    $("endPosition2Field").value,
  ];
  const positionStatuses = [
    $("endPosition1Status").value,
    $("endPosition2Status").value,
  ];
  if (
    positionFields.some((field, index) => field && !positionStatuses[index])
  ) {
    clearRelationScreen(
      "结束日字段已选；请为该条件选择ABOVE、EQUAL、BELOW或UNKNOWN，或清空字段。",
    );
    $("searchPriceRelations").disabled = false;
    return;
  }
  const activeFields = positionFields.filter(Boolean);
  if (new Set(activeFields).size !== activeFields.length) {
    clearRelationScreen("两个结束日AND条件必须使用不同价格关系字段。");
    $("searchPriceRelations").disabled = false;
    return;
  }
  const sourceSymbol = $("relationSymbolInput").value.trim().toUpperCase();
  if (sourceSymbol && !/^(SH|SZ)\.[0-9]{6}$/.test(sourceSymbol)) {
    clearRelationScreen(
      "精确代码筛选请输入SH.或SZ.加六位数字；正式来源总体仍由完整PG快照计算。",
    );
    $("searchPriceRelations").disabled = false;
    return;
  }
  const relationField = $("priceRelationFieldSelect").value;
  const transitionType = $("priceTransitionTypeSelect").value;
  const ma20Direction = $("ma20DirectionSelect").value;
  const endPositionFilters = positionFields
    .map((field, index) => ({ field, status: positionStatuses[index] }))
    .filter((item) => item.field);
  const query = {
    before,
    after,
    relation_field: relationField,
    transition_type: transitionType,
    ma20_direction: ma20Direction,
    end_position_filters: endPositionFilters,
    source_symbol: sourceSymbol,
    offset: state.relationOffset,
    limit: state.relationLimit,
  };
  state.relationQuery = query;
  state.relationData = null;
  $("priceRelationScreenCount").textContent = "完整来源池查询中";
  $("priceRelationScreenSummary").textContent =
    "在同一只读一致视图中计算双日MA20方向、三字段RAW关系、结束日条件、完整分母与SQL分页。";
  $("priceRelationScreenRows").replaceChildren();
  tableRow($("priceRelationScreenRows"), [
    "读取完整来源集合…",
    "",
    "",
    "",
    "",
    "",
    "",
  ]);
  $("searchPriceRelations").disabled = true;
  const params = new URLSearchParams({
    before_session: before.target_session,
    after_session: after.target_session,
    relation_field: relationField,
    transition_type: transitionType,
    ma20_direction: ma20Direction,
    offset: String(query.offset),
    limit: String(query.limit),
  });
  relationSnapshotParams(params, "before", before);
  relationSnapshotParams(params, "after", after);
  for (let index = 0; index < endPositionFilters.length; index++) {
    const filter = endPositionFilters[index];
    params.set("end_position_" + (index + 1) + "_field", filter.field);
    params.set("end_position_" + (index + 1) + "_status", filter.status);
  }
  if (sourceSymbol) params.set("symbol", sourceSymbol);
  relationRequestInFlight = true;
  try {
    const data = await api(
      "/api/source-symbol-price-profile-relation-screen?" + params,
    );
    if (requestId !== state.relationRequest) return;
    state.relationData = data;
    state.relationTotal = data.page.total;
    renderRelationScreen(data, requestId);
  } catch (error) {
    if (requestId !== state.relationRequest) return;
    $("priceRelationScreenCount").textContent = "关系筛选失败";
    $("priceRelationScreenSummary").textContent =
      "RAW关系查询未完成：" + error.message;
    $("priceRelationScreenRows").replaceChildren();
    tableRow($("priceRelationScreenRows"), [error.message, "", "", "", "", ""]);
    $("priceRelationScreenPage").textContent = "查询失败";
    $("priceRelationPrev").disabled = true;
    $("priceRelationNext").disabled = true;
    $("priceRelationLast").disabled = true;
  } finally {
    if (requestId === state.relationRequest)
      $("searchPriceRelations").disabled = false;
    relationRequestInFlight = false;
    if (relationRefreshPending && relationRefreshTimer === null)
      scheduleRelationRefresh(0);
  }
}
async function openRelationDetail(relationRow) {
  const query = state.relationQuery;
  const relationData = state.relationData;
  if (!query || !relationData?.rows?.includes(relationRow)) return;
  const sourceSymbol = relationRow.source_symbol;
  const bindingKey = relationDetailQueryKey(query, relationData, sourceSymbol);
  if (!bindingKey) return;
  const requestId = ++state.relationDetailRequest;
  state.relationDetailBindingKey = bindingKey;
  state.relationDetailRow = relationRow;
  const params = new URLSearchParams({
    before_session: query.before.target_session,
    after_session: query.after.target_session,
    symbol: relationRow.source_symbol,
  });
  relationSnapshotParams(params, "before", query.before);
  relationSnapshotParams(params, "after", query.after);
  const root = $("changeDetail");
  root.replaceChildren();
  root.hidden = false;
  appendText(
    root,
    "p",
    "正在从精确两端画像读取详情；查询不依赖旧变化成员名单。",
    "population",
  );
  state.changeBinding = {
    origin: "relation",
    namespace_id: state.relationData?.namespace_id,
    before_snapshot: state.relationData?.before_snapshot,
    after_snapshot: state.relationData?.after_snapshot,
    session_relation: state.relationData?.session_relation,
    offset: query.offset,
    limit: query.limit,
    relation_query: query,
    relation_query_key: bindingKey,
    relation_request: state.relationRequest,
    source_symbol: sourceSymbol,
  };
  try {
    const data = await api("/api/source-symbol-price-profile-pair?" + params);
    if (
      !relationDetailRequestIsCurrent(requestId, bindingKey, sourceSymbol) ||
      !relationDetailResponseMatches(data, query, relationData, sourceSymbol)
    )
      return;
    if (data.status !== "AVAILABLE" || !data.item) {
      root.replaceChildren();
      appendText(
        root,
        "p",
        "持久双日画像行当前不可用；原RAW关系结果仍保留，未推断上市/退市或选股状态。",
        "message",
      );
      return;
    }
    state.changeBinding.session_relation = data.session_relation;
    showChangeDetail({
      ...data.item,
      price_relation_transitions: relationRow.price_relation_transitions,
    });
  } catch (error) {
    if (!relationDetailRequestIsCurrent(requestId, bindingKey, sourceSymbol))
      return;
    root.replaceChildren();
    root.hidden = false;
    appendText(root, "p", "双日RAW详情读取失败：" + error.message, "message");
  }
}
function returnToRelationScreen() {
  const binding = state.changeBinding;
  if (
    binding?.origin === "relation" &&
    (binding.relation_query_key !==
      relationDetailQueryKey(
        state.relationQuery,
        state.relationData,
        binding.source_symbol,
      ) ||
      binding.relation_request !== state.relationRequest)
  ) {
    invalidateRelationDetailRequest();
    $("priceRelationScreenTitle").scrollIntoView({
      behavior: "smooth",
      block: "start",
    });
    return;
  }
  if (state.timelineContext?.origin === "relation") {
    returnToChanges();
    return;
  }
  invalidateRelationDetailRequest();
  state.pairRequest++;
  resetChangeDetail();
  $("priceRelationScreenTitle").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}
function clearProfileBreadth(message = "请选择两个完整PG画像快照。") {
  state.profileBreadthRequest++;
  state.profileBreadthMemberRequest++;
  state.profileBreadthData = null;
  state.profileBreadthBinding = null;
  state.profileBreadthPendingKey = null;
  state.profileBreadthMemberData = null;
  state.profileBreadthMemberQuery = null;
  state.profileBreadthMemberReturn = null;
  $("profileBreadthCount").textContent = "等待精确快照";
  $("profileBreadthMembers").hidden = true;
  $("profileBreadthMembersRows").replaceChildren();
  $("profileBreadthMembersPage").textContent = "—";
  const root = $("profileBreadthSummary");
  root.replaceChildren();
  appendText(root, "p", message, "muted");
  $("profileBreadthOpenFull").disabled = true;
  $("profileBreadthOpenPair").disabled = true;
}
function breadthFraction(fraction, digits = 2) {
  if (!fraction || !fraction.denominator) return "未知（分母为0）";
  const numerator = Number(fraction.numerator);
  const denominator = Number(fraction.denominator);
  const percent = (100 * numerator) / denominator;
  return `${fraction.numerator}/${fraction.denominator} · ${percent.toFixed(digits)}%`;
}
function breadthReason(reason) {
  const [code, detail] = String(reason || "UNKNOWN").split(/:(.*)/s);
  const primary = labels[code] || code;
  return detail ? `${primary}：${text(detail)}` : primary;
}
function breadthMemberCountButton(parent, title, count, query) {
  const dt = document.createElement("dt");
  dt.textContent = title;
  const dd = document.createElement("dd");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "breadth-count-action";
  button.textContent = String(count);
  button.setAttribute(
    "aria-label",
    `${title}，${count}个来源代码，查看分类成员`,
  );
  button.addEventListener("click", () => loadProfileBreadthMembers(query, 0));
  dd.append(button);
  parent.append(dt, dd);
}
function breadthRelationDetail(relation) {
  return {
    mode: "ENDPOINT",
    side: relation.side,
    relation: relation.relation,
  };
}
function renderProfileBreadth(data) {
  const root = $("profileBreadthSummary");
  root.replaceChildren();
  const before = data.before;
  const after = data.after;
  const paired = data.paired;
  const membership = data.membership;
  const relationNames = ["ABOVE", "BELOW", "EQUAL", "UNKNOWN"];
  const sides = [
    ["before", "起始日", before],
    ["after", "结束日", after],
  ];
  const sideGrid = appendText(root, "div", "", "breadth-side-grid");
  for (const [key, title, side] of sides) {
    const card = document.createElement("section");
    card.className = "breadth-side";
    appendText(
      card,
      "h4",
      `${title} · ${data[`${key}_snapshot`].target_session}`,
    );
    appendText(
      card,
      "p",
      `全来源分母 ${side.source_symbol_count}；可评价 ${side.eligible_count}（${breadthFraction(side.eligible_coverage_fraction)}）；可评价项中高于MA20 ${breadthFraction(side.above_eligible_fraction)}`,
      "muted",
    );
    const positionList = document.createElement("dl");
    positionList.className = "breadth-position-list";
    for (const relation of relationNames)
      breadthMemberCountButton(
        positionList,
        text(relation),
        side.positions[relation],
        breadthRelationDetail({
          side: key === "before" ? "BEFORE" : "AFTER",
          relation,
        }),
      );
    card.append(positionList);
    const unknown = document.createElement("div");
    appendText(unknown, "h5", "UNKNOWN互斥主原因");
    if (side.unknown_reasons.length) {
      const list = document.createElement("ul");
      for (const item of side.unknown_reasons)
        appendText(list, "li", `${breadthReason(item.reason)} · ${item.count}`);
      unknown.append(list);
    } else appendText(unknown, "p", "无UNKNOWN项", "muted");
    card.append(unknown);
    const boardWrap = document.createElement("div");
    boardWrap.className = "table-wrap";
    const boardTable = document.createElement("table");
    boardTable.className = "breadth-table";
    const boardHead = document.createElement("thead");
    const boardHeadRow = document.createElement("tr");
    for (const heading of [
      "来源标签（非上市总体）",
      "ABOVE",
      "BELOW",
      "EQUAL",
      "UNKNOWN",
      "来源数",
    ])
      appendText(boardHeadRow, "th", heading);
    boardHead.append(boardHeadRow);
    boardTable.append(boardHead);
    const boardBody = document.createElement("tbody");
    for (const group of side.board_distribution) {
      const row = document.createElement("tr");
      cell(
        row,
        `${text(group.market)} / ${text(group.board)}${group.research_board_label ? " · 来源标注" : " · 标签未核"}`,
      );
      for (const relation of relationNames)
        cell(row, String(group.positions[relation]));
      cell(row, String(group.source_count));
      boardBody.append(row);
    }
    boardTable.append(boardBody);
    boardWrap.append(boardTable);
    card.append(boardWrap);
    sideGrid.append(card);
  }

  const pairSection = document.createElement("section");
  pairSection.className = "breadth-paired";
  appendText(
    pairSection,
    "h4",
    `两端共同且兼容来源集合 C=${paired.compatible_count} · 交集 ${membership.intersection_count}`,
  );
  const incomparableButton = document.createElement("button");
  incomparableButton.type = "button";
  incomparableButton.className =
    "breadth-count-action breadth-incomparable-action";
  incomparableButton.textContent = `不可比 ${paired.incomparable_count} · 查看来源代码`;
  incomparableButton.addEventListener("click", () =>
    loadProfileBreadthMembers({ mode: "INCOMPARABLE" }, 0),
  );
  pairSection.append(incomparableButton);
  appendText(
    pairSection,
    "p",
    `来源代码并集 ${membership.source_symbol_union_count}；仅起始 ${membership.before_only_count}；仅结束 ${membership.after_only_count}。本端日内广度按各自完整快照分母；两日变化只按C计算。`,
    "muted",
  );
  const matrixWrap = document.createElement("div");
  matrixWrap.className = "table-wrap";
  const matrix = document.createElement("table");
  matrix.className = "breadth-table breadth-matrix";
  const matrixHead = document.createElement("thead");
  const matrixHeadRow = document.createElement("tr");
  for (const heading of ["起始 ↓ / 结束 →", "BELOW", "EQUAL", "ABOVE"])
    appendText(matrixHeadRow, "th", heading);
  matrixHead.append(matrixHeadRow);
  matrix.append(matrixHead);
  const matrixBody = document.createElement("tbody");
  for (const from of ["BELOW", "EQUAL", "ABOVE"]) {
    const row = document.createElement("tr");
    cell(row, text(from));
    for (const to of ["BELOW", "EQUAL", "ABOVE"]) {
      const td = document.createElement("td");
      const count = paired.transition_matrix[`${from}_TO_${to}`];
      const button = document.createElement("button");
      button.type = "button";
      button.className = "breadth-count-action";
      button.textContent = String(count);
      button.setAttribute(
        "aria-label",
        `${text(from)}转为${text(to)}，${count}个来源代码，查看分类成员`,
      );
      button.addEventListener("click", () =>
        loadProfileBreadthMembers(
          { mode: "TRANSITION", from_relation: from, to_relation: to },
          0,
        ),
      );
      td.append(button);
      row.append(td);
    }
    matrixBody.append(row);
  }
  matrix.append(matrixBody);
  matrixWrap.append(matrix);
  pairSection.append(matrixWrap);
  const delta = paired.delta_above_percentage_points;
  const deltaView =
    delta && delta.denominator
      ? `${delta.numerator}/${delta.denominator}个百分点 · ${(Number(delta.numerator) / Number(delta.denominator)).toFixed(2)}个百分点`
      : "未知（共同可比集合为空）";
  appendText(
    pairSection,
    "p",
    `C内ABOVE ${paired.paired_above_before_count} → ${paired.paired_above_after_count}；同口径变化 ${deltaView}。严格下方跨上方 ${paired.strict_cross_up_count} · 上方跨下方 ${paired.strict_cross_down_count} · 涉及EQUAL的转移 ${paired.equal_transition_count}。`,
    "breadth-delta",
  );
  if (paired.incomparable_reasons.length) {
    const reasons = document.createElement("ul");
    for (const item of paired.incomparable_reasons)
      appendText(
        reasons,
        "li",
        `${breadthReason(item.reason)} · ${item.count}`,
      );
    pairSection.append(reasons);
  }
  root.append(pairSection);

  const identity = document.createElement("details");
  identity.className = "breadth-identity";
  appendText(identity, "summary", "展开完整来源版本与研究身份");
  for (const [key, title] of sides) {
    const snapshot = data[`${key}_snapshot`];
    appendText(
      identity,
      "p",
      `${title} ${snapshot.target_session} · source_set ${snapshot.source_set_id} · package SHA ${snapshot.package_sha256} · profile revision ${snapshot.profile_revision} · 日历 ${snapshot.history_calendar_id}/${snapshot.history_calendar_revision} · 窗口 ${snapshot.window_start} 至 ${snapshot.window_end} · ${snapshot.history_session_count}个官方会话`,
    );
  }
  appendText(
    identity,
    "p",
    `namespace ${data.namespace_id} · ${data.research_scope} · ${data.price_coordinate} · ${data.pit_status} · 正式上市总体 ${text(data.formal_listing_universe)}。本查询只读PG，不打开来源包、不重算Core。`,
  );
  root.append(identity);
  $("profileBreadthCount").textContent =
    `全来源 ${before.source_symbol_count} / ${after.source_symbol_count} · C ${paired.compatible_count}`;
  $("profileBreadthOpenFull").disabled = false;
  $("profileBreadthOpenPair").disabled = false;
  state.profileBreadthData = data;
}
async function loadProfileBreadth(before, after) {
  const key = `${profileSnapshotKey(before)}→${profileSnapshotKey(after)}`;
  if (
    state.profileBreadthBinding === key &&
    (state.profileBreadthData || state.profileBreadthPendingKey === key)
  )
    return;
  const requestId = ++state.profileBreadthRequest;
  state.profileBreadthMemberRequest++;
  state.profileBreadthBinding = key;
  state.profileBreadthPendingKey = key;
  state.profileBreadthData = null;
  state.profileBreadthMemberData = null;
  state.profileBreadthMemberQuery = null;
  state.profileBreadthMemberReturn = null;
  $("profileBreadthMembers").hidden = true;
  $("profileBreadthMembersRows").replaceChildren();
  $("profileBreadthMembersPage").textContent = "—";
  $("profileBreadthCount").textContent = "完整来源池统计中";
  const root = $("profileBreadthSummary");
  root.replaceChildren();
  appendText(
    root,
    "p",
    "正在读取两份精确封口画像的全来源代码池与已存MA20…",
    "muted",
  );
  $("profileBreadthOpenFull").disabled = true;
  $("profileBreadthOpenPair").disabled = true;
  const params = new URLSearchParams({
    before_session: before.target_session,
    after_session: after.target_session,
    before_source_set_id: before.source_set_id,
    before_package_sha256: before.package_sha256,
    before_profile_revision: before.profile_revision,
    after_source_set_id: after.source_set_id,
    after_package_sha256: after.package_sha256,
    after_profile_revision: after.profile_revision,
  });
  try {
    const data = await api(
      `/api/source-symbol-price-profile-breadth?${params}`,
    );
    if (
      requestId !== state.profileBreadthRequest ||
      key !== state.profileBreadthBinding
    )
      return;
    renderProfileBreadth(data);
  } catch (error) {
    if (
      requestId !== state.profileBreadthRequest ||
      key !== state.profileBreadthBinding
    )
      return;
    state.profileBreadthData = null;
    $("profileBreadthCount").textContent = "来源池统计失败";
    root.replaceChildren();
    appendText(root, "p", `没有可用广度结果：${error.message}`, "message");
  } finally {
    if (state.profileBreadthPendingKey === key)
      state.profileBreadthPendingKey = null;
  }
}
function breadthMemberQueryLabel(query) {
  if (query.mode === "ENDPOINT")
    return `${query.side === "BEFORE" ? "起始日" : "结束日"} · ${text(query.relation)}`;
  if (query.mode === "TRANSITION")
    return `${text(query.from_relation)} → ${text(query.to_relation)}`;
  return "两端存在但价格/Core或来源标签不可比";
}
function breadthMemberRawText(item, side) {
  const prefix = side === "BEFORE" ? "before" : "after";
  if (!item[`${prefix}_present`]) return "该日来源集合中没有此代码";
  const relation = item[`${prefix}_source_pool_close_ma20_relation`];
  const unknown = item[`${prefix}_source_pool_unknown_reason`];
  const reason = unknown ? ` · ${breadthReason(unknown)}` : "";
  const rawQuality = `${text(item[`${prefix}_raw_price_quality`])}${
    item[`${prefix}_raw_price_reason`]
      ? `：${text(item[`${prefix}_raw_price_reason`])}`
      : ""
  }`;
  const ma20Quality = `${text(item[`${prefix}_ma20_quality`])}${
    item[`${prefix}_ma20_reason`]
      ? `：${text(item[`${prefix}_ma20_reason`])}`
      : ""
  }`;
  return (
    `收盘 ${text(item[`${prefix}_raw_close`])} ` +
    `(${rawQuality}) / ` +
    `MA20 ${text(item[`${prefix}_ma20`])} ` +
    `(${ma20Quality}) · ` +
    `${text(relation)}${reason}`
  );
}
function renderProfileBreadthMembers(data, query) {
  const panel = $("profileBreadthMembers");
  const page = data.page;
  panel.hidden = false;
  $("profileBreadthMembersTitle").textContent =
    `来源分类成员 · ${breadthMemberQueryLabel(query)}`;
  $("profileBreadthMembersStatus").textContent =
    `共${page.total}个精确来源代码 · 当前${page.offset + 1}–${page.offset + page.count} · ` +
    `第${page.total ? Math.floor(page.offset / page.limit) + 1 : 0}/` +
    `${page.total ? Math.ceil(page.total / page.limit) : 0}页 · ` +
    `每页${page.limit} · ${sessionRelationLabel(data.session_relation)} · ` +
    `SOURCE_SYMBOL_ONLY / RAW_UNADJUSTED / NON_PIT；选股未评价。`;
  const rows = $("profileBreadthMembersRows");
  rows.replaceChildren();
  if (!data.rows.length) {
    tableRow(rows, ["该分类没有来源代码。", "", "", "", ""]);
  }
  for (const item of data.rows) {
    const row = document.createElement("tr");
    const codeCell = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet breadth-member-open";
    button.textContent = `${item.source_symbol} · 双日画像`;
    button.title =
      "打开两份精确来源画像；可继续查看75会话Raw/Core并返回本分类页。";
    button.addEventListener("click", () => openBreadthMemberPair(item));
    codeCell.append(button);
    row.append(codeCell);
    cell(row, breadthMemberRawText(item, "BEFORE"));
    cell(row, breadthMemberRawText(item, "AFTER"));
    cell(
      row,
      [
        item.before_source_pool_unknown_reason
          ? `起始：${breadthReason(item.before_source_pool_unknown_reason)}`
          : "",
        item.after_source_pool_unknown_reason
          ? `结束：${breadthReason(item.after_source_pool_unknown_reason)}`
          : "",
        !item.source_pool_pair_compatible &&
        !item.before_source_pool_unknown_reason &&
        !item.after_source_pool_unknown_reason
          ? "Core语义、价格坐标或来源market/board标签不兼容"
          : "",
      ]
        .filter(Boolean)
        .join("；") || "—",
    );
    cell(
      row,
      `${text(item.before_market)} / ${text(item.before_board)} → ` +
        `${text(item.after_market)} / ${text(item.after_board)} · ` +
        `${text(item.before_identity_status)} / ${text(item.after_identity_status)} · ` +
        `${text(item.before_price_coordinate)} / ${text(item.after_price_coordinate)}`,
    );
    rows.append(row);
  }
  $("profileBreadthMembersPage").textContent =
    `偏移 ${page.offset} · 本页 ${page.count}/${page.limit} · 分类总数 ${page.total}`;
  $("profileBreadthMembersPrev").disabled = page.offset === 0;
  $("profileBreadthMembersNext").disabled = page.next_offset === null;
  $("profileBreadthMembersLast").disabled =
    page.last_offset === null || page.offset >= page.last_offset;
  state.profileBreadthMemberData = data;
}
async function loadProfileBreadthMembers(query, offset) {
  const breadth = state.profileBreadthData;
  const binding = state.profileBreadthBinding;
  if (
    !breadth ||
    !binding ||
    binding !==
      `${profileSnapshotKey(breadth.before_snapshot)}→${profileSnapshotKey(breadth.after_snapshot)}`
  )
    return;
  const requestId = ++state.profileBreadthMemberRequest;
  state.profileBreadthMemberQuery = { ...query };
  state.profileBreadthMemberOffset = offset;
  state.profileBreadthMemberData = null;
  const panel = $("profileBreadthMembers");
  panel.hidden = false;
  $("profileBreadthMembersTitle").textContent =
    `来源分类成员 · ${breadthMemberQueryLabel(query)}`;
  $("profileBreadthMembersStatus").textContent = "正在读取精确来源成员…";
  $("profileBreadthMembersRows").replaceChildren();
  $("profileBreadthMembersPage").textContent = "查询中";
  $("profileBreadthMembersPrev").disabled = true;
  $("profileBreadthMembersNext").disabled = true;
  $("profileBreadthMembersLast").disabled = true;
  const params = new URLSearchParams({
    before_session: breadth.before_snapshot.target_session,
    after_session: breadth.after_snapshot.target_session,
    before_source_set_id: breadth.before_snapshot.source_set_id,
    before_package_sha256: breadth.before_snapshot.package_sha256,
    before_profile_revision: breadth.before_snapshot.profile_revision,
    after_source_set_id: breadth.after_snapshot.source_set_id,
    after_package_sha256: breadth.after_snapshot.package_sha256,
    after_profile_revision: breadth.after_snapshot.profile_revision,
    mode: query.mode,
    offset: String(offset),
    limit: String(state.profileBreadthMemberLimit),
  });
  if (query.mode === "ENDPOINT") {
    params.set("side", query.side);
    params.set("relation", query.relation);
  } else if (query.mode === "TRANSITION") {
    params.set("from", query.from_relation);
    params.set("to", query.to_relation);
  }
  try {
    const data = await api(
      `/api/source-symbol-price-profile-breadth-members?${params}`,
    );
    if (
      requestId !== state.profileBreadthMemberRequest ||
      binding !== state.profileBreadthBinding ||
      data.before_snapshot.source_set_id !==
        breadth.before_snapshot.source_set_id ||
      data.before_snapshot.package_sha256 !==
        breadth.before_snapshot.package_sha256 ||
      data.before_snapshot.profile_revision !==
        breadth.before_snapshot.profile_revision ||
      data.after_snapshot.source_set_id !==
        breadth.after_snapshot.source_set_id ||
      data.after_snapshot.package_sha256 !==
        breadth.after_snapshot.package_sha256 ||
      data.after_snapshot.profile_revision !==
        breadth.after_snapshot.profile_revision
    )
      return;
    renderProfileBreadthMembers(data, query);
  } catch (error) {
    if (requestId !== state.profileBreadthMemberRequest) return;
    panel.hidden = false;
    $("profileBreadthMembersStatus").textContent =
      `来源成员读取失败：${error.message}`;
    $("profileBreadthMembersRows").replaceChildren();
    tableRow($("profileBreadthMembersRows"), [error.message, "", "", "", ""]);
    $("profileBreadthMembersPage").textContent = "读取失败";
  }
}
async function openBreadthMemberPair(item) {
  const breadth = state.profileBreadthData;
  const query = state.profileBreadthMemberQuery;
  if (!breadth || !query || !state.profileBreadthMemberData) return;
  const requestId = ++state.pairRequest;
  const memberBinding = state.profileBreadthBinding;
  state.profileBreadthMemberReturn = {
    query: { ...query },
    offset: state.profileBreadthMemberOffset,
    limit: state.profileBreadthMemberLimit,
    binding: memberBinding,
    selectedVersion: selectedVersion(),
  };
  const params = new URLSearchParams({
    before_session: breadth.before_snapshot.target_session,
    after_session: breadth.after_snapshot.target_session,
    symbol: item.source_symbol,
    before_source_set_id: breadth.before_snapshot.source_set_id,
    before_package_sha256: breadth.before_snapshot.package_sha256,
    before_profile_revision: breadth.before_snapshot.profile_revision,
    after_source_set_id: breadth.after_snapshot.source_set_id,
    after_package_sha256: breadth.after_snapshot.package_sha256,
    after_profile_revision: breadth.after_snapshot.profile_revision,
  });
  resetChangeDetail();
  const root = $("changeDetail");
  root.hidden = false;
  appendText(root, "p", "正在读取该成员对应的精确双日画像…", "population");
  root.scrollIntoView({ behavior: "smooth", block: "start" });
  try {
    const data = await api(`/api/source-symbol-price-profile-pair?${params}`);
    if (
      requestId !== state.pairRequest ||
      memberBinding !== state.profileBreadthBinding ||
      data.source_symbol !== item.source_symbol ||
      data.before_snapshot.source_set_id !==
        breadth.before_snapshot.source_set_id ||
      data.after_snapshot.source_set_id !== breadth.after_snapshot.source_set_id
    )
      return;
    state.changeBinding = {
      namespace_id: data.namespace_id,
      before_snapshot: data.before_snapshot,
      after_snapshot: data.after_snapshot,
      session_relation: data.session_relation,
      offset: 0,
      limit: 1,
      change_type: "ALL",
      symbol: item.source_symbol,
      origin: "breadth",
      breadth_query: { ...query },
    };
    showChangeDetail(data.item);
  } catch (error) {
    if (requestId !== state.pairRequest) return;
    root.replaceChildren();
    root.hidden = false;
    appendText(root, "p", `双日画像读取失败：${error.message}`, "message");
  }
}
function scheduleChangeRefresh(delay) {
  if (changeRefreshTimer !== null) window.clearTimeout(changeRefreshTimer);
  changeRefreshTimer = window.setTimeout(() => {
    changeRefreshTimer = null;
    if (changeRequestInFlight) return;
    void runChanges(state.changeRequest);
  }, delay);
}
function loadChanges() {
  if (state.timelineContext?.origin === "change") invalidateTimeline();
  invalidateRelationDetailRequest();
  const requestId = ++state.changeRequest;
  changeRefreshPending = true;
  state.changeBinding = null;
  state.pairRequest++;
  resetChangeDetail();
  const before = selectedChangeSnapshot("changeBeforeSelect");
  const after = selectedChangeSnapshot("changeAfterSelect");
  const nextBreadthBinding =
    before && after
      ? `${profileSnapshotKey(before)}→${profileSnapshotKey(after)}`
      : null;
  if (
    state.profileBreadthBinding &&
    state.profileBreadthBinding !== nextBreadthBinding
  )
    clearProfileBreadth("两日画像快照已变化；正在重新读取对应来源池。");
  state.changeTotal = 0;
  $("changeRows").replaceChildren();
  tableRow($("changeRows"), [
    "筛选已变化，正在合并连续操作…",
    "",
    "",
    "",
    "",
    "",
    "",
  ]);
  $("changeHighlightsRows").replaceChildren();
  tableRow($("changeHighlightsRows"), [
    "正在读取最新筛选…",
    "",
    "",
    "",
    "",
    "",
  ]);
  $("changeCount").textContent = "更新中";
  $("changeSummary").textContent = "筛选已变化，正在合并连续操作…";
  $("changePageStatus").textContent = "查询中";
  $("changePrev").disabled = true;
  $("changeNext").disabled = true;
  $("changeLast").disabled = true;
  $("changeHighlightsCount").textContent = "更新中";
  $("changeHighlightsSummary").textContent =
    "仅在筛选稳定后读取一次最新的完整PG变化全集。";
  $("searchChanges").disabled = true;
  if (changeRefreshTimer !== null) window.clearTimeout(changeRefreshTimer);
  changeRefreshTimer = window.setTimeout(() => {
    changeRefreshTimer = null;
    if (!changeRequestInFlight) void runChanges(requestId);
  }, previewQueryDebounceMs);
}
async function runChanges(requestId) {
  if (
    !changeRefreshPending ||
    requestId !== state.changeRequest ||
    changeRequestInFlight
  )
    return;
  changeRefreshPending = false;
  const beforeSnapshot = selectedChangeSnapshot("changeBeforeSelect");
  const afterSnapshot = selectedChangeSnapshot("changeAfterSelect");
  const before = beforeSnapshot?.target_session;
  const after = afterSnapshot?.target_session;
  resetChangeDetail();
  $("changeRows").replaceChildren();
  $("changeHighlightsRows").replaceChildren();
  $("changeHighlightsCount").textContent = "查询中";
  $("changeHighlightsSummary").textContent =
    "正在读取所选两日的完整PG变化全集…";
  if (
    !beforeSnapshot ||
    !afterSnapshot ||
    !before ||
    !after ||
    before >= after
  ) {
    clearProfileBreadth(
      "请选择两个递增且完整的PG画像快照；广度不会保留前一组数据。",
    );
    $("changeSummary").textContent = "请选择递增的起始与结束画像日期。";
    $("changeHighlightsCount").textContent = "等待精确快照";
    $("changeHighlightsSummary").textContent =
      "请选择两个递增且完整的PG画像快照。";
    $("searchChanges").disabled = false;
    tableRow($("changeHighlightsRows"), [
      "等待精确的两日画像版本。",
      "",
      "",
      "",
      "",
      "",
    ]);
    $("changePageStatus").textContent = "—";
    $("searchChanges").disabled = false;
    $("changePrev").disabled =
      $("changeNext").disabled =
      $("changeLast").disabled =
        true;
    return;
  }
  void loadProfileBreadth(beforeSnapshot, afterSnapshot);
  const params = new URLSearchParams({
    before_session: before,
    after_session: after,
    offset: String(state.changeOffset),
    limit: String(state.changeLimit),
    change_type: $("changeTypeSelect").value,
    before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
    after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
  });
  const profileFieldId = $("changeProfileFieldSelect").value || null;
  const profileChangeKind = profileFieldId
    ? $("changeProfileKindSelect").value
    : null;
  if (profileFieldId) {
    params.set("profile_field_id", profileFieldId);
    params.set("profile_change_kind", profileChangeKind);
  }
  for (const [prefix, snapshot] of [
    ["before", beforeSnapshot],
    ["after", afterSnapshot],
  ]) {
    params.set(`${prefix}_source_set_id`, snapshot.source_set_id);
    params.set(`${prefix}_package_sha256`, snapshot.package_sha256);
    params.set(`${prefix}_profile_revision`, snapshot.profile_revision);
  }
  const symbol = $("changeSymbolInput").value.trim();
  if (symbol) params.set("symbol", symbol);
  $("changeSummary").textContent = "正在比较精确的PG画像来源版本…";
  $("searchChanges").disabled = true;
  changeRequestInFlight = true;
  try {
    const data = await api(
      `/api/source-symbol-price-profile-changes?${params}`,
    );
    if (requestId !== state.changeRequest) return;
    const s = data.summary;
    const p = data.page;
    const expectedProfileFieldFilter = profileFieldId
      ? { field_id: profileFieldId, kind: profileChangeKind }
      : null;
    if (
      (expectedProfileFieldFilter === null &&
        data.profile_field_filter !== null) ||
      (expectedProfileFieldFilter !== null &&
        (data.profile_field_filter?.field_id !== profileFieldId ||
          data.profile_field_filter?.kind !== profileChangeKind)) ||
      data.rows.some((item) => {
        const selected = (item.profile_field_changes || []).find(
          (change) => change.field_id === profileFieldId,
        );
        if (!profileFieldId) return false;
        if (!selected) return true;
        if (profileChangeKind === "QUALITY_CHANGED")
          return selected.before_quality === selected.after_quality;
        if (profileChangeKind === "STATUS_CHANGED")
          return selected.before_status === selected.after_status;
        return (
          selected.before_quality === selected.after_quality &&
          selected.before_status === selected.after_status
        );
      })
    )
      throw new Error(
        "Profile11双日字段筛选与精确变化响应不一致，已停止展示。",
      );
    const expectedBias20AtrStateFilter = {
      before: $("changeBeforeBias20AtrStateSelect").value,
      after: $("changeAfterBias20AtrStateSelect").value,
      uses_full_source_union:
        $("changeBeforeBias20AtrStateSelect").value !== "ALL" ||
        $("changeAfterBias20AtrStateSelect").value !== "ALL",
    };
    const snapshotMatches = (received, expected) =>
      Boolean(
        received?.complete &&
        profileSnapshotKey(received) === profileSnapshotKey(expected) &&
        typeof received.history_calendar_id === "string" &&
        received.history_calendar_id.length > 0 &&
        typeof received.history_calendar_revision === "string" &&
        received.history_calendar_revision.length > 0 &&
        received.window_start === expected.window_start &&
        received.window_end === expected.window_end &&
        received.history_session_count === expected.history_session_count &&
        received.source_symbol_count === expected.source_symbol_count &&
        received.expected_source_symbol_count ===
          expected.expected_source_symbol_count,
      );
    if (
      data.namespace_id !== state.namespace_id ||
      beforeSnapshot.namespace_id !== state.namespace_id ||
      afterSnapshot.namespace_id !== state.namespace_id ||
      !snapshotMatches(data.before_snapshot, beforeSnapshot) ||
      !snapshotMatches(data.after_snapshot, afterSnapshot) ||
      data.before_snapshot.history_calendar_id !==
        data.after_snapshot.history_calendar_id ||
      data.before_snapshot.history_calendar_revision !==
        data.after_snapshot.history_calendar_revision ||
      data.bias20_atr_state_filter?.before !==
        expectedBias20AtrStateFilter.before ||
      data.bias20_atr_state_filter?.after !==
        expectedBias20AtrStateFilter.after ||
      data.bias20_atr_state_filter?.uses_full_source_union !==
        expectedBias20AtrStateFilter.uses_full_source_union ||
      data.rows.some(
        (item) =>
          !item.before_bias20_atr_state ||
          !item.after_bias20_atr_state ||
          !item.bias20_atr_pair_compatibility ||
          !item.bias20_atr_transition_status,
      )
    )
      throw new Error(
        "双日MA20/ATR状态筛选与PG完整并集响应身份不一致，已停止展示。",
      );
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
      profile_field_id: profileFieldId,
      profile_change_kind: profileChangeKind,
      before_bias20_atr_state: expectedBias20AtrStateFilter.before,
      after_bias20_atr_state: expectedBias20AtrStateFilter.after,
      symbol: $("changeSymbolInput").value.trim(),
      origin: "change",
    };
    renderChangeHighlights(data, data.session_relation);
    state.changeTotal = p.total;
    const hasDistanceFilter =
      expectedBias20AtrStateFilter.uses_full_source_union;
    $("changeCount").textContent = hasDistanceFilter
      ? `${p.total}条MA20/ATR状态筛查命中`
      : `${s.changed_source_symbol_count}个变化对象`;
    $("changeSummary").textContent =
      `${sessionRelationLabel(data.session_relation)} · 快照分母 ${data.before_snapshot.source_symbol_count}/${data.before_snapshot.expected_source_symbol_count} 与 ${data.after_snapshot.source_symbol_count}/${data.after_snapshot.expected_source_symbol_count} · 并集 ${s.source_symbol_union_count} · 交集 ${s.intersection_count} · 起始独有 ${s.before_only_count} · 结束独有 ${s.after_only_count} · 跨source_set ${s.cross_source_set_pair_count} · 跨价格坐标 ${s.cross_price_coordinate_pair_count} · Core/Profile不可比 ${s.core_semantics_incomparable_count}/${s.profile_semantics_incomparable_count} · 收盘/MA20未知 ${s.close_ma20_unknown_count} · Profile未观察 ${s.profile_unobserved_pair_count} · 完整变化 ${s.changed_source_symbol_count} · ${profileFieldId ? `Profile11 ${profileFieldLabel(profileFieldId)} ${profileChangeKind}` : "Profile11字段变化未过滤"} · 距离筛选 ${bias20AtrStateLabels[expectedBias20AtrStateFilter.before]} → ${bias20AtrStateLabels[expectedBias20AtrStateFilter.after]} · 当前命中 ${p.total} · ${hasDistanceFilter ? "距离筛查可包含现役变化分类为空的代码；并不代表状态发生变化或交易信号。" : "选股未评价。"}`;
    $("changePageStatus").textContent =
      `偏移 ${p.offset} · 本页 ${p.count}/${p.limit} · 筛选总计 ${p.total}`;
    $("changePrev").disabled = p.offset === 0;
    $("changeNext").disabled = p.next_offset === null;
    $("changeLast").disabled = p.total === 0 || p.offset + p.limit >= p.total;
    if (!data.rows.length) {
      const emptyMessage =
        p.total === 0
          ? expectedBias20AtrStateFilter.uses_full_source_union
            ? "真实零匹配：完整双日来源并集中没有同时满足所选起始与结束距离状态及其它AND条件的代码。"
            : profileFieldId
              ? `真实零匹配：该精确双日快照中没有${profileFieldLabel(profileFieldId)}的${profileChangeKind}记录；完整来源并集和未变化/不可比统计仍保持原分母。`
              : "没有符合当前条件的实际变化；完整来源并集和未变化/不可比统计仍保持原分母。"
          : p.offset >= p.total
            ? `当前偏移${p.offset}已越过末页；筛选总数仍为${p.total}，请返回第一页查看。`
            : "当前偏移没有成员；完整筛选总数仍按PG全集计算。";
      tableRow($("changeRows"), [emptyMessage, "", "", "", "", "", ""]);
    }
    for (const item of data.rows) {
      const row = document.createElement("tr");
      cell(row, item.source_symbol);
      cell(
        row,
        (item.change_types || [])
          .map((type) => changeTypeLabels[type] || type)
          .join("、"),
      );
      row.append(changeBias20AtrDistanceCell(item));
      cell(
        row,
        `${text(item.before_close_ma20_relation)} → ${text(item.after_close_ma20_relation)}`,
      );
      const selectedProfileChange = profileFieldId
        ? (item.profile_field_changes || []).find(
            (change) => change.field_id === profileFieldId,
          )
        : null;
      const profileChangeText = selectedProfileChange
        ? `${profileFieldLabel(profileFieldId)} · 质量 ${text(selectedProfileChange.before_quality)} → ${text(selectedProfileChange.after_quality)} · 状态 ${text(selectedProfileChange.before_status)} → ${text(selectedProfileChange.after_status)} · 原因 ${text(selectedProfileChange.before_reason)} → ${text(selectedProfileChange.after_reason)}`
        : item.profile_compatible
          ? (item.profile_field_changes || [])
              .map((change) => change.field_id)
              .join("、") || "无"
          : "不可比";
      cell(row, profileChangeText);
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
    tableRow($("changeRows"), [error.message, "", "", "", "", "", ""]);
    $("changeCount").textContent = "变化查询失败";
    $("changeSummary").textContent = `PG变化查询未完成：${error.message}`;
    $("changePageStatus").textContent = "查询失败";
    $("changePrev").disabled =
      $("changeNext").disabled =
      $("changeLast").disabled =
        true;
  } finally {
    if (requestId === state.changeRequest) $("searchChanges").disabled = false;
    changeRequestInFlight = false;
    if (changeRefreshPending && changeRefreshTimer === null)
      scheduleChangeRefresh(0);
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
    state.profileSnapshots = (data.snapshots || []).map((item) => ({
      ...item,
      namespace_id:
        item.namespace_id || data.namespace_id || state.namespace_id,
    }));
    populateChangeSnapshots(state.profileSnapshots);
    $("profileSnapshotSelect").replaceChildren(
      new Option("— 请选择精确画像日期 / source_set / revision —", ""),
    );
    for (const item of state.profileSnapshots) {
      const key = profileSnapshotKey(item);
      const option = new Option(
        `${item.target_session} · ${item.source_set_id.slice(0, 8)} · ${item.source_symbol_count}/${item.expected_source_symbol_count} · ${item.history_session_count}会话 · rev ${item.profile_revision.slice(0, 10)}`,
        key,
      );
      option.title =
        `${item.target_session} · namespace ${item.namespace_id || "未提供"} · ` +
        `source_set_id ${item.source_set_id} · package SHA ${item.package_sha256} · ` +
        `profile_revision ${item.profile_revision}`;
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
function selectedProfilePosition(id) {
  const value = $(id).value;
  if (!value) return null;
  const [field, status] = value.split(":");
  return { field, status };
}
function profileFieldLabel(field) {
  const label = profileFieldLabels[field] || field;
  return `${label} · ${field}`;
}
function populateProfileFieldOptions(options, selected = "") {
  const select = $("profileFieldSelect");
  select.replaceChildren(new Option("不按Profile11字段筛选", ""));
  for (const field of options || []) {
    select.append(new Option(profileFieldLabel(field), field));
  }
  select.value = (options || []).includes(selected) ? selected : "";
}
function populateProfileConditionOptions(summary, fieldId, selected = "ALL") {
  const select = $("profileConditionSelect");
  select.replaceChildren(new Option("仅查看质量分布，不筛质量/状态", "ALL"));
  if (!fieldId || !summary || summary.field_id !== fieldId) {
    select.disabled = true;
    select.value = "ALL";
    return;
  }
  for (const item of summary.quality_counts || []) {
    select.append(
      new Option(
        `质量 · ${item.quality}（${item.count}）`,
        `quality:${item.quality}`,
      ),
    );
  }
  for (const item of summary.status_counts || []) {
    select.append(
      new Option(
        `状态 · ${item.status}（${item.count}）`,
        `status:${item.status}`,
      ),
    );
  }
  select.disabled = false;
  select.value = Array.from(select.options).some(
    (option) => option.value === selected,
  )
    ? selected
    : "ALL";
}
function profileFieldSummaryText(summary) {
  if (!summary) return "Profile11质量/状态未过滤";
  const qualities = (summary.quality_counts || [])
    .map((item) => `${item.quality} ${item.count}`)
    .join("、");
  const statuses = (summary.status_counts || [])
    .map((item) => `${item.status} ${item.count}`)
    .join("、");
  const reasons = (summary.reason_counts || [])
    .filter((item) => item.quality !== "OBSERVED" || item.reason)
    .map(
      (item) =>
        `${item.quality}/${item.status} 原因 ${item.reason || "未提供原因"} ${item.count}`,
    )
    .join("；");
  return (
    `Profile11 ${profileFieldLabel(summary.field_id)} · ` +
    `完整来源字段分母 ${summary.source_denominator} · ` +
    `质量 ${qualities || "无记录"} · 状态 ${statuses || "无记录"}` +
    (reasons ? ` · 原因 ${reasons}` : "")
  );
}
function profilePositionConditionText(condition) {
  const definition = pricePositionDefinitions[condition.field];
  return `${definition.label} ${pricePositionStatusLabels[condition.status]} ${condition.status}`;
}
function profileBias20AtrRowText(item) {
  const quality = item.bias20_atr_quality || "UNKNOWN";
  const numerator = String(item.bias20_atr_exact_numerator ?? "");
  const denominator = String(item.bias20_atr_exact_denominator ?? "");
  if (
    quality !== "OBSERVED" ||
    item.price_coordinate !== "RAW_UNADJUSTED" ||
    !/^-?\d+$/.test(numerator) ||
    !/^\d+$/.test(denominator) ||
    !/[1-9]/.test(denominator) ||
    item.ma20_quality !== "OBSERVED" ||
    item.ma20 == null ||
    item.atr20_quality !== "OBSERVED" ||
    item.atr20 == null ||
    item.target_raw_price_quality !== "OBSERVED" ||
    item.target_raw_close == null
  ) {
    const reason =
      item.bias20_atr_unknown_reason ||
      item.bias20_atr_reason ||
      "未知原因未提供";
    return (
      `UNKNOWN · ${text(reason)} · Core ${text(quality)} · ` +
      `RAW价格 ${text(item.target_raw_price_quality)}`
    );
  }
  return (
    `${text(item.bias20_atr)} ATR · 精确 ${text(numerator)}/${text(denominator)} · ` +
    `OBSERVED · ${text(item.price_coordinate)}`
  );
}
function changeBias20AtrStateText(item, side) {
  const state = item[`${side}_bias20_atr_state`] || "UNKNOWN";
  const label = bias20AtrStateLabels[state] || state;
  if (state === "ABSENT") return `${label}（不推断停牌、退市或上市）`;
  const numerator = String(item[`${side}_bias20_atr_exact_numerator`] ?? "");
  const denominator = String(
    item[`${side}_bias20_atr_exact_denominator`] ?? "",
  );
  const fraction =
    numerator && denominator ? `${numerator}/${denominator}` : "不可用";
  const reason = item[`${side}_bias20_atr_state_reason`] || "无额外原因";
  const quality = item[`${side}_bias20_atr_quality`] || "UNKNOWN";
  return `${label} (${state}) · bias ${text(item[`${side}_bias20_atr`])} · 精确 ${fraction} · 质量 ${quality} · 原因 ${reason}`;
}
function changeBias20AtrDistanceCell(item) {
  const root = document.createElement("td");
  root.className = "change-distance-cell";
  appendText(root, "div", `起始：${changeBias20AtrStateText(item, "before")}`);
  appendText(root, "div", `结束：${changeBias20AtrStateText(item, "after")}`);
  appendText(
    root,
    "div",
    `比较：${bias20AtrStateLabels[item.bias20_atr_pair_compatibility] || item.bias20_atr_pair_compatibility} · ${item.bias20_atr_transition_status} · ${text(item.bias20_atr_pair_reason)}`,
  );
  return root;
}
function profilePositionRowText(item, conditions) {
  if (!conditions.length) return "未启用价格位置条件";
  return conditions
    .map((condition) => {
      const position = item.price_positions?.[condition.field];
      if (!position)
        return `${profilePositionConditionText(condition)} · 响应缺少比较结果`;
      const operand = (value) =>
        `${value.field} ${text(value.value)} · ${text(value.quality)} · 原因 ${text(value.reason)}`;
      return `${pricePositionDefinitions[condition.field].label}：${operand(position.left)}；${operand(position.right)}；${pricePositionStatusLabels[position.status] || position.status}${position.reason ? ` · ${position.reason}` : ""}`;
    })
    .join(" AND ");
}
function renderPricePositionDetail(root, item) {
  appendText(root, "h3", "RAW价格相对位置 / 数值 / 质量 / 原因");
  appendText(
    root,
    "p",
    "比较只使用RAW_UNADJUSTED坐标与PG NUMERIC值；UNKNOWN保留各输入质量原因。相对滚动前20会话高点的位置不代表固定前高突破或交易信号。",
    "muted",
  );
  const table = document.createElement("table");
  table.innerHTML =
    "<thead><tr><th>比较</th><th>左侧NUMERIC / 质量 / 原因</th><th>右侧NUMERIC / 质量 / 原因</th><th>关系 / 原因</th></tr></thead>";
  const body = document.createElement("tbody");
  for (const [field, definition] of Object.entries(pricePositionDefinitions)) {
    const position = item.price_positions?.[field];
    if (!position) continue;
    const format = (operand) =>
      `${operand.field} · ${text(operand.value)} · ${text(operand.quality)} · 原因 ${text(operand.reason)}`;
    tableRow(body, [
      definition.label,
      format(position.left),
      format(position.right),
      `${pricePositionStatusLabels[position.status] || position.status} · ${position.status}${position.reason ? ` · ${position.reason}` : ""}`,
    ]);
  }
  table.append(body);
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  wrap.append(table);
  root.append(wrap);
}
async function loadProfilePage() {
  if (state.timelineContext?.origin === "profile") invalidateTimeline();
  const snapshot = selectedProfileSnapshot();
  const requestId = ++state.profileRequest;
  const coreField = $("profileCoreFieldSelect").value;
  const coreQuality = $("profileCoreQualitySelect").value;
  const bias20AtrBand = $("profileBias20AtrBandSelect").value;
  const profileFieldId = $("profileFieldSelect").value || null;
  const profileCondition = $("profileConditionSelect").value;
  const profileQuality = profileCondition.startsWith("quality:")
    ? profileCondition.slice("quality:".length)
    : null;
  const profileStatus = profileCondition.startsWith("status:")
    ? profileCondition.slice("status:".length)
    : null;
  const hasCoreField = coreField !== "ALL";
  const hasCoreQuality = coreQuality !== "ALL";
  const selectedPositions = [
    selectedProfilePosition("profilePosition1Select"),
    selectedProfilePosition("profilePosition2Select"),
  ].filter(Boolean);
  const selectedCore = profileCoreFields.find(
    (item) => item.value === coreField,
  );
  $("profileCoreValueHeader").textContent = selectedCore
    ? `${selectedCore.label} · 数值 / 质量 / 原因`
    : "所选Core字段 / 值 / 质量 / 原因";
  clearProfileDetail();
  $("profileRows").replaceChildren();
  $("profileCount").textContent = "查询中";
  $("profilePageStatus").textContent = "查询中";
  $("profilePrev").disabled = true;
  $("profileNext").disabled = true;
  $("profileLast").disabled = true;
  $("profileReturnFirst").hidden = true;
  if (hasCoreField !== hasCoreQuality) {
    $("profileMessage").textContent =
      "Core字段与质量状态需要成对选择；旧结果已清除，当前筛选尚未执行。";
    $("profileCount").textContent = "筛选未执行";
    $("profilePageStatus").textContent = "请完成配对选择";
    $("profilePrev").disabled = true;
    $("profileNext").disabled = true;
    $("profileLast").disabled = true;
    return;
  }
  if (profileCondition !== "ALL" && !profileFieldId) {
    $("profileMessage").textContent =
      "Profile11质量或状态筛选必须同时选择字段；旧结果已清除。";
    $("profileCount").textContent = "筛选未执行";
    $("profilePageStatus").textContent = "请完成配对选择";
    return;
  }
  if (
    new Set(selectedPositions.map((item) => item.field)).size !==
    selectedPositions.length
  ) {
    $("profileMessage").textContent =
      "两个RAW位置条件必须使用不同的比较字段；旧结果已清除。";
    $("profileCount").textContent = "筛选未执行";
    $("profilePageStatus").textContent = "请调整位置条件";
    return;
  }
  if (!snapshot) {
    $("profileMessage").textContent =
      "请先选择一个完整、已持久化的精确画像版本。";
    $("profilePageStatus").textContent = "—";
    $("profileCount").textContent = "未查询";
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
    bias20_atr_band: bias20AtrBand,
  });
  if (hasCoreField) {
    params.set("core_field", coreField);
    params.set("core_quality", coreQuality);
  }
  if (profileFieldId) params.set("profile_field_id", profileFieldId);
  if (profileQuality) params.set("profile_quality", profileQuality);
  if (profileStatus) params.set("profile_status", profileStatus);
  selectedPositions.forEach((condition, index) => {
    const number = index + 1;
    params.set(`position_${number}_field`, condition.field);
    params.set(`position_${number}_status`, condition.status);
  });
  const symbol = $("profileSymbolInput").value.trim();
  if (symbol) params.set("symbol", symbol);
  $("profileMessage").textContent =
    "正在按精确日期、来源集、包SHA和窗口修订读取PG分页…";
  try {
    const data = await api(`/api/source-symbol-price-profiles?${params}`);
    if (requestId !== state.profileRequest) return;
    const page = data.page;
    const profileFilter = data.profile_field_filter;
    const expectedPageStatus =
      page.count > 0
        ? "AVAILABLE"
        : page.total === 0
          ? "NO_MATCH"
          : page.offset >= page.total
            ? "EMPTY_PAGE_OUT_OF_RANGE"
            : null;
    if (
      !expectedPageStatus ||
      data.status !== expectedPageStatus ||
      !profileFilter ||
      profileFilter.field_id !== profileFieldId ||
      profileFilter.quality !== profileQuality ||
      profileFilter.status !== profileStatus ||
      !Array.isArray(data.profile_field_options) ||
      (profileFieldId &&
        (!data.profile_field_summary ||
          data.profile_field_summary.field_id !== profileFieldId ||
          data.profile_field_summary.source_denominator !==
            snapshot.source_symbol_count))
    )
      throw new Error(
        "Profile11字段分布或筛选身份与精确快照不一致，已停止展示。",
      );
    if (
      data.target_session !== snapshot.target_session ||
      data.namespace_id !== snapshot.namespace_id ||
      data.source_set_id !== snapshot.source_set_id ||
      data.package_sha256 !== snapshot.package_sha256 ||
      data.profile_revision !== snapshot.profile_revision ||
      data.bias20_atr_band !== bias20AtrBand ||
      !data.bias20_atr_band_definition ||
      data.source_pool_total !== snapshot.source_symbol_count ||
      page.offset !== state.profileOffset ||
      page.limit !== state.profileLimit ||
      !Array.isArray(data.price_position_filters) ||
      data.price_position_filters.length !== selectedPositions.length ||
      data.price_position_filters.some(
        (condition, index) =>
          condition.field !== selectedPositions[index].field ||
          condition.status !== selectedPositions[index].status,
      ) ||
      data.rows.some((item) =>
        selectedPositions.some(
          (condition) =>
            item.price_positions?.[condition.field]?.status !==
            condition.status,
        ),
      ) ||
      data.rows.some((item) =>
        profileFieldId
          ? item.selected_profile_field?.field_id !== profileFieldId ||
            (profileQuality &&
              item.selected_profile_field?.quality !== profileQuality) ||
            (profileStatus &&
              item.selected_profile_field?.status !== profileStatus)
          : item.selected_profile_field !== null,
      )
    )
      throw new Error(
        "画像响应与选定的精确来源版本或分页上下文不一致，已停止展示。",
      );
    populateProfileFieldOptions(
      data.profile_field_options,
      profileFieldId || "",
    );
    state.profileFieldSummary = profileFieldId
      ? data.profile_field_summary
      : null;
    populateProfileConditionOptions(
      state.profileFieldSummary,
      profileFieldId,
      profileCondition,
    );
    const binding =
      `${snapshot.target_session} · namespace ${data.namespace_id || snapshot.namespace_id || "未提供"} · ` +
      `source_set_id ${data.source_set_id} · 包SHA ${data.package_sha256} · ` +
      `profile_revision ${data.profile_revision}`;
    state.profileTotal = page.total;
    $("profileCount").textContent =
      `${page.count}条 / 筛选命中${page.total} / 完整来源池${data.source_pool_total}`;
    const filterDescription = [
      data.bias20_atr_band_definition.label,
      hasCoreField
        ? `${selectedCore.label} / ${coreQuality}`
        : "Core质量未过滤",
      selectedPositions.length
        ? selectedPositions.map(profilePositionConditionText).join(" AND ")
        : "价格位置未过滤",
      profileQuality
        ? `${profileFieldLabel(profileFieldId)} · 质量 ${profileQuality}`
        : profileStatus
          ? `${profileFieldLabel(profileFieldId)} · 状态 ${profileStatus}`
          : "Profile11质量/状态未过滤",
    ].join(" · ");
    const windowDescription = data.history_window?.start
      ? ` · 窗口 ${data.history_window.start} 至 ${data.history_window.end}（${data.history_window.requested_session_count}会话）`
      : "";
    const pageNotice =
      data.status === "NO_MATCH"
        ? "当前来源版本没有符合此条件的记录。"
        : data.status === "EMPTY_PAGE_OUT_OF_RANGE"
          ? "筛选结果仍有成员，但当前偏移已超过末页；可返回第一页。"
          : "";
    $("profileMessage").textContent = [
      pageNotice,
      binding,
      `距离定义：${data.bias20_atr_band_definition.definition}`,
      filterDescription,
      windowDescription,
      `完整来源池 ${data.source_pool_total}`,
      `筛选后命中 ${page.total}`,
      profileFieldSummaryText(state.profileFieldSummary),
      "NON_PIT来源代码研究，不是正式上市Universe或选股结论。",
    ]
      .filter(Boolean)
      .join(" · ");
    $("profilePageStatus").textContent =
      `偏移 ${page.offset} · 本页 ${page.count}/${page.limit} · 筛选命中 ${page.total} · 来源池 ${data.source_pool_total}`;
    const pageOutOfRange = data.status === "EMPTY_PAGE_OUT_OF_RANGE";
    $("profileReturnFirst").hidden = !pageOutOfRange;
    $("profilePrev").disabled = page.offset === 0;
    $("profileNext").disabled = page.next_offset === null;
    $("profileLast").disabled =
      page.total === 0 || page.offset + page.limit >= page.total;
    if (!data.rows.length) {
      tableRow($("profileRows"), [
        page.total === 0
          ? "当前来源版本没有符合此条件的记录。"
          : pageOutOfRange
            ? "当前偏移超过末页；筛选全集仍有成员。"
            : "当前偏移没有成员。",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
      ]);
    }
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
      cell(tr, profileBias20AtrRowText(item));
      cell(tr, `${text(item.clv)} · ${text(item.clv_quality)}`);
      const selectedCoreValue = selectedCore
        ? `${selectedCore.label} · ${text(item[selectedCore.key])} · ${text(item[`${selectedCore.key}_quality`])} · 原因 ${text(item[`${selectedCore.key}_reason`])}`
        : "筛选字段可在详情中查看值、质量与原因";
      cell(tr, selectedCoreValue);
      const profileField = item.selected_profile_field;
      cell(
        tr,
        profileField
          ? `${profileFieldLabel(profileField.field_id)} · 状态 ${text(profileField.status)} · 质量 ${text(profileField.quality)} · 原因 ${text(profileField.reason)}`
          : "未选择Profile11字段",
      );
      const positionCell = cell(
        tr,
        profilePositionRowText(item, selectedPositions),
      );
      positionCell.className = "profile-price-position";
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
      detailCell.append(
        detailButton,
        makeComparisonAddButton({
          source_symbol: item.source_symbol,
          target_session: item.target_session,
          namespace_id: item.namespace_id,
          source_set_id: item.source_set_id,
          package_sha256: item.package_sha256,
          profile_revision: item.profile_revision,
          origin_label: "完整PG价格画像",
        }),
      );
      tr.append(detailCell);
      $("profileRows").append(tr);
    }
  } catch (error) {
    if (requestId !== state.profileRequest) return;
    tableRow($("profileRows"), [
      "画像查询失败",
      "",
      "",
      "",
      "",
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
    $("profileCount").textContent = "画像查询失败";
    $("profilePageStatus").textContent = "无匹配结果";
    $("profilePrev").disabled = true;
    $("profileNext").disabled = true;
    $("profileLast").disabled = true;
    $("profileReturnFirst").hidden = true;
  }
}
function clearProfilePageResults(message, count = "待查询") {
  if (state.timelineContext?.origin === "profile") invalidateTimeline();
  state.profileRequest++;
  clearProfileDetail();
  $("profileRows").replaceChildren();
  $("profileCount").textContent = count;
  $("profilePageStatus").textContent = "—";
  $("profilePrev").disabled = true;
  $("profileNext").disabled = true;
  $("profileLast").disabled = true;
  $("profileReturnFirst").hidden = true;
  $("profileMessage").textContent = message;
}
function submitProfileSearch() {
  if (profileSearchTimer !== null) {
    window.clearTimeout(profileSearchTimer);
    profileSearchTimer = null;
  }
  state.profileOffset = 0;
  const hasCoreField = $("profileCoreFieldSelect").value !== "ALL";
  const hasCoreQuality = $("profileCoreQualitySelect").value !== "ALL";
  if (hasCoreField !== hasCoreQuality) {
    clearProfilePageResults(
      "Core字段与质量状态需要成对选择；旧结果已清除，当前筛选尚未执行。",
      "筛选未执行",
    );
    return;
  }
  void loadProfilePage();
}
function scheduleProfileSearch() {
  if (profileSearchTimer !== null) window.clearTimeout(profileSearchTimer);
  state.profileOffset = 0;
  clearProfilePageResults(
    "筛选条件已变化；连续操作合并后查询最新条件。",
    "筛选更新中",
  );
  profileSearchTimer = window.setTimeout(() => {
    profileSearchTimer = null;
    submitProfileSearch();
  }, previewQueryDebounceMs);
}
$("reloadCatalog").addEventListener("click", async () => {
  await loadCatalog();
  await loadProfileSnapshots();
});
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
    if (!state.timelineNavigation) {
      invalidateTimeline();
      clearResult();
    }
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
  submitProfileSearch();
});
$("profileSymbolInput").addEventListener("input", () => {
  state.profileOffset = 0;
  clearProfilePageResults(
    "来源代码输入已变化；旧结果已清除，请查询新的精确条件。",
  );
});
$("profileSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    submitProfileSearch();
  }
});
$("profileSnapshotSelect").addEventListener("change", () => {
  preserveComparisonOnSnapshotChange(
    selectedProfileSnapshot(),
    "完整PG价格画像快照",
  );
  scheduleProfileSearch();
});
for (const id of [
  "profileMarketSelect",
  "profileBoardSelect",
  "profileBias20AtrBandSelect",
  "profileBarStatusSelect",
  "profileWindowStatusSelect",
  "profileCoreFieldSelect",
  "profileCoreQualitySelect",
  "profilePosition1Select",
  "profilePosition2Select",
]) {
  $(id).addEventListener("change", () => {
    scheduleProfileSearch();
  });
}
$("profileFieldSelect").addEventListener("change", () => {
  state.profileOffset = 0;
  $("profileConditionSelect").value = "ALL";
  scheduleProfileSearch();
});
$("profileConditionSelect").addEventListener("change", () => {
  scheduleProfileSearch();
});
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
$("profileReturnFirst").addEventListener("click", () => {
  state.profileOffset = 0;
  loadProfilePage();
});
$("searchChanges").addEventListener("click", () => {
  state.changeOffset = 0;
  loadChanges();
});
$("searchPriceRelations").addEventListener("click", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
$("profileBreadthOpenFull").addEventListener("click", () => {
  const snapshot = state.profileBreadthData?.after_snapshot;
  if (!snapshot) return;
  const exact = state.profileSnapshots.find(
    (item) =>
      item.target_session === snapshot.target_session &&
      item.source_set_id === snapshot.source_set_id &&
      item.package_sha256 === snapshot.package_sha256 &&
      item.profile_revision === snapshot.profile_revision,
  );
  if (!exact) {
    $("profileMessage").textContent =
      "该精确结束快照不在当前PG目录中；已停止跳转，没有回退其它版本。";
    return;
  }
  $("profileSnapshotSelect").value = profileSnapshotKey(exact);
  $("profileSymbolInput").value = "";
  $("profileMarketSelect").value = "ALL";
  $("profileBoardSelect").value = "ALL";
  $("profileBarStatusSelect").value = "ALL";
  $("profileWindowStatusSelect").value = "ALL";
  state.profileOffset = 0;
  $("returnToSourceBreadth").hidden = false;
  $("profileBrowserTitle").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
  loadProfilePage();
});
$("profileBreadthOpenPair").addEventListener("click", () => {
  $("changeSymbolInput").focus();
  $("changeSymbolInput").scrollIntoView({
    behavior: "smooth",
    block: "center",
  });
  $("changeSymbolInput").placeholder = "SH.或SZ.加六位数字";
});
$("profileBreadthMembersPrev").addEventListener("click", () => {
  const previous = Math.max(
    0,
    state.profileBreadthMemberOffset - state.profileBreadthMemberLimit,
  );
  if (state.profileBreadthMemberQuery)
    loadProfileBreadthMembers(state.profileBreadthMemberQuery, previous);
});
$("profileBreadthMembersNext").addEventListener("click", () => {
  const next =
    state.profileBreadthMemberOffset + state.profileBreadthMemberLimit;
  if (state.profileBreadthMemberQuery)
    loadProfileBreadthMembers(state.profileBreadthMemberQuery, next);
});
$("profileBreadthMembersLast").addEventListener("click", () => {
  const last = state.profileBreadthMemberData?.page.last_offset;
  if (last !== null && last !== undefined && state.profileBreadthMemberQuery)
    loadProfileBreadthMembers(state.profileBreadthMemberQuery, last);
});
$("returnToSourceBreadth").addEventListener("click", () => {
  $("sourceBreadthTitle").scrollIntoView({
    behavior: "smooth",
    block: "center",
  });
  $("returnToSourceBreadth").hidden = true;
});
$("compareProfilePair").addEventListener("click", compareSourceProfilePair);
$("changeSymbolInput").addEventListener("input", () => {
  state.pairRequest++;
  if (state.timelineContext?.origin === "change") invalidateTimeline();
  state.changeBinding = null;
  resetChangeDetail();
});
$("changeSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    compareSourceProfilePair();
  }
});
for (const id of ["changeBeforeSelect", "changeAfterSelect"]) {
  $(id).addEventListener("change", () => {
    if (id === "changeAfterSelect")
      preserveComparisonOnSnapshotChange(
        selectedChangeSnapshot(id),
        "两日结束快照",
      );
    state.changeOffset = 0;
    state.relationOffset = 0;
    clearRelationScreen(
      "两日快照已变化；旧RAW关系结果已清除，正在读取新绑定。",
    );
    loadChanges();
    loadPriceRelations();
  });
}
$("versionSelect").addEventListener("change", () => {
  const selected = selectedVersion();
  const binding = state.comparisonBasket[0];
  if (
    binding &&
    (!selected ||
      selected.target_session !== binding.target_session ||
      selected.package_sha256 !== binding.package_sha256)
  )
    clearComparisonBasket(
      "预览来源日期或包SHA已切换；旧临时对照已失效并清空。",
    );
});
$("compareSymbols").addEventListener("click", compareSelectedSymbols);
$("clearComparison").addEventListener("click", () => clearComparisonBasket());
function refreshComparisonEventHistory() {
  if (!state.comparisonTimelines.length) return;
  renderComparisonEventHistory(
    comparisonAxis(state.comparisonTimelines, state.comparisonBasket.length),
  );
}
$("comparisonEventWindow").addEventListener("change", () => {
  state.comparisonEventWindow = Number($("comparisonEventWindow").value);
  refreshComparisonEventHistory();
});
$("comparisonEventField").addEventListener("change", () => {
  state.comparisonEventField = $("comparisonEventField").value;
  refreshComparisonEventHistory();
});
$("comparisonEventKind").addEventListener("change", () => {
  state.comparisonEventKind = $("comparisonEventKind").value;
  refreshComparisonEventHistory();
});
$("comparisonPeriodKind").addEventListener("change", () => {
  state.comparisonPeriodKind = $("comparisonPeriodKind").value;
  renderComparisonPeriodResults();
});
$("changeTypeSelect").addEventListener("change", () => {
  state.changeOffset = 0;
  loadChanges();
});
$("changeProfileFieldSelect").addEventListener("change", () => {
  const fieldId = $("changeProfileFieldSelect").value;
  const kindSelect = $("changeProfileKindSelect");
  kindSelect.disabled = !fieldId;
  kindSelect.value = fieldId ? "ANY_STATE_CHANGED" : "";
  state.changeOffset = 0;
  loadChanges();
});
$("changeProfileKindSelect").addEventListener("change", () => {
  state.changeOffset = 0;
  loadChanges();
});
for (const id of [
  "changeBeforeBias20AtrStateSelect",
  "changeAfterBias20AtrStateSelect",
]) {
  $(id).addEventListener("change", () => {
    state.changeOffset = 0;
    loadChanges();
  });
}
$("priceRelationFieldSelect").addEventListener("change", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
$("priceTransitionTypeSelect").addEventListener("change", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
$("ma20DirectionSelect").addEventListener("change", () => {
  state.relationOffset = 0;
  loadPriceRelations();
});
for (const [fieldId, statusId] of [
  ["endPosition1Field", "endPosition1Status"],
  ["endPosition2Field", "endPosition2Status"],
]) {
  $(fieldId).addEventListener("change", () => {
    const status = $(statusId);
    status.disabled = !$(fieldId).value;
    if (!$(fieldId).value) status.value = "";
    state.relationOffset = 0;
    loadPriceRelations();
  });
  $(statusId).addEventListener("change", () => {
    state.relationOffset = 0;
    loadPriceRelations();
  });
}
$("relationSymbolInput").addEventListener("input", () => {
  state.relationOffset = 0;
  clearRelationScreen("精确来源代码筛选已变化；点击“查询RAW关系”读取新组合。");
});
$("relationSymbolInput").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    state.relationOffset = 0;
    loadPriceRelations();
  }
});
$("priceRelationPrev").addEventListener("click", () => {
  state.relationOffset = Math.max(
    0,
    state.relationOffset - state.relationLimit,
  );
  loadPriceRelations();
});
$("priceRelationNext").addEventListener("click", () => {
  if (state.relationOffset + state.relationLimit < state.relationTotal) {
    state.relationOffset += state.relationLimit;
    loadPriceRelations();
  }
});
$("priceRelationLast").addEventListener("click", () => {
  if (state.relationTotal) {
    state.relationOffset =
      Math.floor((state.relationTotal - 1) / state.relationLimit) *
      state.relationLimit;
    loadPriceRelations();
  }
});
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
$("ma20DirectionWindow").addEventListener("change", () =>
  renderMa20DirectionHistory(state.timeline?.ma20_direction_history || null),
);
$("priceRelationKindFilter").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
$("showPriceRelationUnknown").addEventListener("change", () =>
  renderPriceRelationHistory(state.timeline?.price_relation_history || null),
);
renderComparisonBasket();
void (async () => {
  await loadCatalog();
  await loadProfileSnapshots();
})();
