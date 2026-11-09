import { byId, display, element } from "./state.js";

const reasonNames = {
  SUPPORT_STATE_TRANSITION: "支撑状态变化",
  SUPPORT_ACCEPTANCE_TRANSITION: "支撑接受状态变化",
  SUPPORT_FRESHNESS_TRANSITION: "支撑新鲜度变化",
  RETEST_QUALIFICATION_TRANSITION: "回测资格变化",
  FIRST_VALID_TEST_OBSERVED: "首次有效测试",
  FIRST_SEPARATED_SESSION_OBSERVED: "首次间隔会话观察",
  GAP_FILL_STAGE_TRANSITION: "缺口回补阶段变化",
};

function metric(label, value, detail) {
  const card = element("article");
  card.append(
    element("span", label),
    element("strong", display(value)),
    element("small", detail),
  );
  return card;
}

export function setD1MaterialStatus(text) {
  byId("d1MaterialStatus").textContent = text;
}

export function renderD1MaterialMarket(data, openBoard, openStock) {
  const summary = data.market;
  byId("metrics").replaceChildren(
    metric("来源代码池", summary.source_pool_symbol_count, "同日完整来源分母"),
    metric(
      "材料变化代码",
      summary.material_change_distinct_symbol_count,
      "来源观察，不是正式候选",
    ),
    metric(
      "材料变化锚点",
      summary.material_change_anchor_count,
      "按锚事件计数，可跨原因重叠",
    ),
    metric("行业 / GN 板块", data.boards.total_count, "当前成员快照 · NON_PIT"),
  );
  const selectedReason = data.selected_material_change_reason;
  const reasonLabel =
    selectedReason === "ALL"
      ? "全部原因"
      : reasonNames[selectedReason] || selectedReason;
  byId("observationLabel").textContent =
    `${data.target_session} · D1 材料变化 · ${reasonLabel} · ${data.d1_history_session_count} 个官方会话 · NON_PIT`;
  const groups = summary.source_symbol_group_counts || {};
  byId("d1MaterialQuality").hidden = false;
  byId("d1MaterialQuality").textContent =
    `质量与分组：未知 ${groups.UNKNOWN || 0} 个来源代码、不可比较 ${groups.NOT_COMPARABLE || 0} 个、无已证锚点 ${groups.NO_PROVED_ANCHOR || 0} 个。UNKNOWN、NOT_COMPARABLE 与 NO_PROVED_ANCHOR 不解释为正式未入选或没有行情。未选择后继期限，不读取T+N Raw。`;
  byId("profileChangeSummary").textContent = "D1 材料变化不依赖画像修订";
  byId("profileChangeList").textContent =
    "当前路径使用同一来源池的原始结构锚与材料变化原因；画像质量不参与D1分母。";
  byId("profileChangeCard")?.setAttribute("hidden", "");

  const overviewBoards = byId("overviewBoards");
  overviewBoards.replaceChildren();
  overviewBoards.classList.toggle("has-items", !!data.boards.summaries.length);
  if (!data.boards.summaries.length) {
    overviewBoards.textContent = "当前筛选没有可展示的变化板块。";
  } else {
    data.boards.summaries.slice(0, 15).forEach((board, index) => {
      const button = element("button", null, "list-row");
      button.type = "button";
      const left = element("span", null, "row-main");
      left.append(
        element("strong", board.board_name),
        element(
          "small",
          `${board.board_kind === "INDUSTRY" ? "行业" : "GN 概念"} · 成员 ${display(board.snapshot_member_count)} · NON_PIT`,
        ),
      );
      const right = element("span", null, "row-value");
      right.append(
        element("span", display(board.material_change_distinct_symbol_count)),
        element("small", "变化成员"),
      );
      button.append(
        element("span", String(index + 1).padStart(2, "0"), "row-rank"),
        left,
        right,
        element("span", "›", "row-arrow"),
      );
      button.addEventListener("click", () => openBoard(board));
      overviewBoards.append(button);
    });
  }

  const stocks = data.stocks.results.slice(0, 30);
  const overviewStocks = byId("overviewStocks");
  overviewStocks.replaceChildren();
  overviewStocks.classList.toggle("has-items", !!stocks.length);
  if (!stocks.length) {
    overviewStocks.textContent =
      "当前筛选没有可展示的变化代码；未知、不可比较或无已证锚点不等于正式未入选。";
  } else {
    stocks.forEach((stock, index) => {
      const button = element("button", null, "list-row");
      button.type = "button";
      const left = element("span", null, "row-main");
      left.append(
        element("strong", stock.source_symbol),
        element("small", "D1 材料变化 · RAW_UNADJUSTED · NON_PIT"),
      );
      const right = element("span", null, "row-value");
      right.append(
        element("span", display(stock.material_change_anchor_count)),
        element("small", "结构锚点"),
      );
      button.append(
        element("span", String(index + 1).padStart(2, "0"), "row-rank"),
        left,
        right,
        element("span", "›", "row-arrow"),
      );
      button.addEventListener("click", () => openStock(stock.source_symbol));
      overviewStocks.append(button);
    });
  }
  byId("contextText").textContent =
    `${data.target_session} · D1材料变化来源视图 · 不代表正式候选或上涨预测。未知、不可比较和未证明锚点保留为独立状态。`;
}
