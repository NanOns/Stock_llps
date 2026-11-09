import { byId } from "./state.js";

const reasonNames = {
  SUPPORT_STATE_TRANSITION: "支撑状态变化",
  SUPPORT_ACCEPTANCE_TRANSITION: "支撑接受状态变化",
  SUPPORT_FRESHNESS_TRANSITION: "支撑新鲜度变化",
  RETEST_QUALIFICATION_TRANSITION: "回测资格变化",
  FIRST_VALID_TEST_OBSERVED: "首次有效测试",
  FIRST_SEPARATED_SESSION_OBSERVED: "首次间隔会话观察",
  GAP_FILL_STAGE_TRANSITION: "缺口回补阶段变化",
};

export function setD1MaterialStatus(text) {
  byId("d1MaterialStatus").textContent = text;
}

export function renderD1MaterialMarket(data) {
  const summary = data.market;
  const selectedReason = data.selected_material_change_reason;
  const reasonLabel =
    selectedReason === "ALL"
      ? "全部原因"
      : reasonNames[selectedReason] || selectedReason;

  const groups = summary.source_symbol_group_counts || {};
  byId("d1MaterialQuality").hidden = !(
    byId("sectors").classList.contains("active") ||
    byId("stocks").classList.contains("active")
  );
  byId("d1MaterialQuality").textContent =
    `${data.target_session} · D1 材料变化 · ${reasonLabel} · ${data.d1_history_session_count} 会话 · 来源池 ${summary.source_pool_symbol_count}，材料变化代码 ${summary.material_change_distinct_symbol_count}、锚点 ${summary.material_change_anchor_count}；不是正式候选。质量与分组：未知 ${groups.UNKNOWN || 0} 个来源代码、不可比较 ${groups.NOT_COMPARABLE || 0} 个、无已证锚点 ${groups.NO_PROVED_ANCHOR || 0} 个。UNKNOWN、NOT_COMPARABLE 与 NO_PROVED_ANCHOR 不解释为正式未入选或没有行情。未选择后继期限，不读取T+N Raw。`;

  const missing = Object.entries(data.membership_availability || {})
    .filter(([, status]) => status !== "AVAILABLE")
    .map(([kind]) => (kind === "INDUSTRY" ? "行业" : "概念"));
  if (missing.length)
    byId("d1MaterialQuality").textContent +=
      ` ${missing.join("、")}成员不可用，该板块范围为 UNKNOWN。`;
}
