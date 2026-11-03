/* Stock A预览页面shared职责模块；保持原业务逻辑和交互。 */
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
