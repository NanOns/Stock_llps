import {
  byId,
  display,
  displayStockCode,
  displayStockName,
  element,
} from "./state.js";

const typeNames = {
  PIVOT_LOW: "枢轴低点",
  PIVOT_HIGH: "枢轴高点",
  RANGE_FLOOR: "区间底部",
  RANGE_CEILING: "区间顶部",
  GAP_ZONE: "缺口区域",
};
const reasonNames = {
  SUPPORT_STATE_TRANSITION: "支撑状态变化",
  SUPPORT_ACCEPTANCE_TRANSITION: "支撑接受状态变化",
  SUPPORT_FRESHNESS_TRANSITION: "支撑新鲜度变化",
  RETEST_QUALIFICATION_TRANSITION: "回测资格变化",
  FIRST_VALID_TEST_OBSERVED: "首次有效测试",
  FIRST_SEPARATED_SESSION_OBSERVED: "首次间隔会话观察",
  GAP_FILL_STAGE_TRANSITION: "缺口回补阶段变化",
};

export function renderD1Stocks(data, openStock) {
  const rows = data.stocks.results;
  const list = byId("stockList");
  list.replaceChildren();
  list.classList.toggle("has-items", !!rows.length);
  if (!rows.length) {
    list.textContent = "所选结构与原因范围没有材料变化代码。";
  } else {
    rows.slice(0, 30).forEach((stock, index) => {
      const button = element("button", null, "list-row");
      button.type = "button";
      const left = element("span", null, "row-main");
      left.append(
        element("strong", displayStockName(stock)),
        element(
          "small",
          `代码 ${displayStockCode(stock)} · 来源材料观察 · 非正式候选`,
        ),
      );
      const right = element("span", null, "row-value");
      right.append(
        element("span", display(stock.material_change_anchor_count)),
        element("small", "锚点"),
      );
      button.append(
        element("span", String(index + 1).padStart(2, "0"), "row-rank"),
        left,
        right,
        element("span", "›", "row-arrow"),
      );
      button.addEventListener("click", () => openStock(stock.source_symbol));
      list.append(button);
    });
  }
  byId("stockPage").textContent =
    `偏移 ${data.stocks.offset} · 总数 ${display(data.stocks.total_count)}`;
  byId("stockPrev").disabled = data.stocks.offset === 0;
  byId("stockNext").disabled = data.stocks.next_offset == null;
}

export function renderD1StockDetail(stock, root = byId("stockContent")) {
  root.replaceChildren();
  if (!stock || stock.source_pool_status !== "AVAILABLE") {
    root.append(
      element(
        "p",
        "该代码不在所选目标日来源池；不能推断正式未入选、停牌、退市或没有行情。",
        "rule",
      ),
    );
    if (root.id === "stockContent")
      byId("stockStatus").textContent = "来源池中没有可用D1事实。";
    return;
  }
  const summary = stock.selected_scope_summary || {};
  root.append(
    element(
      "p",
      `来源池状态 ${stock.source_pool_status} · 当前归类 ${summary.group || "UNKNOWN"} · 正式资格 UNKNOWN/NOT_EVALUATED · NON_PIT · RAW_UNADJUSTED`,
      "rule",
    ),
    element(
      "p",
      summary.group === "NO_SELECTED_REASON"
        ? "未命中所选具名原因；不能据此认定有其他材料变化或全股无 D1 变化。"
        : summary.group === "NO_PROVED_ANCHOR"
          ? "所选结构和历史窗内没有已证锚点；不代表正式未入选或没有行情。"
          : "",
      "rule",
    ),
    element("h3", "所选范围的结构与质量"),
  );
  const facts = element("div", null, "fact-grid");
  facts.append(
    fact("范围锚点", summary.anchor_count),
    fact("材料变化锚点", summary.material_change_anchor_count),
    fact("UNKNOWN 锚点", summary.material_change_status_counts?.UNKNOWN),
    fact(
      "NOT_COMPARABLE 锚点",
      summary.material_change_status_counts?.NOT_COMPARABLE,
    ),
    fact("已证材料变化原因", summary.selected_reason_anchor_count),
    fact("状态分组", summary.group),
  );
  root.append(facts);
  const detail = stock.d1_detail?.historical_change_detail;
  const anchors = detail?.filtered_anchor_changes || [];
  root.append(
    element("h3", `材料变化锚点（${anchors.length} 条；仅来源描述）`),
  );
  if (!anchors.length) {
    root.append(
      element(
        "p",
        "所选筛选没有已证材料变化锚点。UNKNOWN、NOT_COMPARABLE 与 NO_PROVED_ANCHOR 均不代表正式未入选或没有行情。",
        "rule",
      ),
    );
  }
  for (const anchor of anchors) {
    const card = element("article", null, "card");
    card.append(
      element(
        "strong",
        `${anchor.created_session || anchor.anchor_session || anchor.session || "锚点日期"} · ${typeNames[anchor.type] || anchor.type}`,
      ),
      element(
        "p",
        (anchor.material_change_reasons || [])
          .map((reason) => reasonNames[reason] || reason)
          .join("、") || "原因未提供",
      ),
      element(
        "p",
        `材料状态 ${anchor.material_change_classification || "UNKNOWN"} · 支撑状态 ${anchor.support_change?.before?.state || "—"} → ${anchor.support_change?.after?.state || "—"} · 变化字段 ${(anchor.support_change?.changed_fields || []).join("、") || "—"}`,
      ),
      element(
        "p",
        `数据质量原因 ${(anchor.quality_reasons || []).join("、") || "未记录质量异常"} · 变化解释 ${(anchor.change_reasons || []).join("、") || "未提供"}`,
      ),
    );
    if (anchor.support_change?.reason_codes?.length)
      card.append(
        element(
          "p",
          `支撑变化原因：${anchor.support_change.reason_codes.join("、")}`,
        ),
      );
    if (anchor.gap_fill_change?.reason_codes?.length)
      card.append(
        element(
          "p",
          `缺口阶段原因：${anchor.gap_fill_change.reason_codes.join("、")}`,
        ),
      );
    if (anchor.comparison_context)
      card.append(
        element(
          "p",
          `前一官方会话 ${anchor.comparison_context.previous_official_session || "—"} → ${anchor.comparison_context.target_session || "—"} · 锚点早于前会话 ${anchor.comparison_context.anchor_birth_before_previous_session ? "是" : "否"}`,
        ),
      );
    root.append(card);
  }
  const qualityAnchors = detail?.quality_anchor_changes || [];
  if (qualityAnchors.length) {
    root.append(
      element(
        "h3",
        `无法归为材料变化的质量锚点（${qualityAnchors.length} 条）`,
      ),
    );
    for (const anchor of qualityAnchors) {
      const status = anchor.material_change_classification || "UNKNOWN";
      const card = element("article", null, "card");
      card.append(
        element(
          "strong",
          `${anchor.created_session || "锚点日期"} · ${typeNames[anchor.type] || anchor.type} · ${status}`,
        ),
        element(
          "p",
          `质量原因 ${(anchor.quality_reasons || []).join("、") || "来源没有提供具体原因码"}`,
        ),
        element(
          "p",
          `支撑比较 ${anchor.support_change?.classification || "UNKNOWN"} · ${(anchor.support_change?.reason_codes || []).join("、") || "无支撑变化原因码"}`,
        ),
      );
      root.append(card);
    }
  }
  root.append(element("h3", "当前行业与概念成员关系"));
  const memberships = stock.current_snapshot_memberships || [];
  root.append(
    element(
      "p",
      memberships.length
        ? memberships
            .map(
              (item) =>
                `${item.board_name || "板块名称未知"} · 观测 ${item.observed_at || "UNKNOWN"} · NON_PIT`,
            )
            .join("；")
        : "已读取的成员快照未证明该代码属于其中板块；缺失快照及历史成员状态为未知。",
    ),
  );
  if (root.id === "stockContent")
    byId("stockStatus").textContent =
      `已读取 ${anchors.length} 条所选范围材料锚点；正式候选、上涨预测、Risk、Priority 未计算。`;
}

function fact(label, value) {
  const item = element("div", null, "fact");
  item.append(element("span", label), element("strong", display(value)));
  return item;
}
