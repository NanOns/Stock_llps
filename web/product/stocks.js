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

export function renderBaoStockPriceSource(data) {
  const section = element("section", null, "card");
  section.append(element("h3", "BaoStock股票价格源"));
  const row = data?.row && typeof data.row === "object" ? data.row : data;
  const status = data?.status || row?.status || "NOT_AVAILABLE";
  if (status !== "AVAILABLE") {
    const reason =
      data?.message ||
      data?.reason_code ||
      "该目标日没有已保存的BaoStock捕获。";
    section.append(
      element(
        "p",
        `${status} · ${reason}；不回退到其它交易日或其它价格来源。`,
        "rule",
      ),
    );
    return section;
  }

  const { fields, labels, qfq } = buildBaoStockPriceSourceView(row);
  const source = {
    ...row,
    source_capture_id: data?.source_capture_id ?? row.source_capture_id,
    source_symbol: data?.source_symbol ?? row.source_symbol,
    target_session: data?.target_session ?? row.session_date,
    capture_id: data?.source_capture_id ?? row.source_capture_id,
    captured_at: row.capture_read_started_at,
    adjustment_flag:
      row.adjustment_flag ?? (qfq ? row.qfq_adjustflag : row.raw_adjustflag),
    unit_contract: {
      version: row.unit_contract_version,
      close_input_unit: row.close_input_unit,
      close_to_cny: row.close_to_cny,
      amount_input_unit: row.amount_input_unit,
      amount_to_cny: row.amount_to_cny,
      volume_input_unit: row.volume_input_unit,
      volume_to_shares: row.volume_to_shares,
      turn_input_unit: row.turn_input_unit,
    },
    fields,
  };

  section.append(
    element(
      "p",
      `来源代码 ${source.source_symbol || "UNKNOWN"} · 会话 ${source.target_session || "UNKNOWN"} · 捕获 ${source.capture_id || "UNKNOWN"} · 捕获时刻 ${source.captured_at || "UNKNOWN"}`,
      "rule",
    ),
    element(
      "p",
      `${source.pit_status || "NON_PIT"} · ${source.price_coordinate || "RAW_UNADJUSTED"} · adjustflag=${source.adjustment_flag || "UNKNOWN"} · 时钟 ${source.capture_clock_role || "UNKNOWN"}`,
      "muted",
    ),
  );
  const contract = source.unit_contract || {};
  section.append(
    element(
      "p",
      `单位合同 ${contract.version || "UNKNOWN"} · 价格输入 ${contract.close_input_unit || "UNKNOWN"} × ${contract.close_to_cny || "UNKNOWN"} → 人民币元/股 · 成交额输入 ${contract.amount_input_unit || "UNKNOWN"} × ${contract.amount_to_cny || "UNKNOWN"} → 人民币元 · 成交量输入 ${contract.volume_input_unit || "UNKNOWN"} × ${contract.volume_to_shares || "UNKNOWN"} → 股 · 换手输入 ${contract.turn_input_unit || "UNKNOWN"}`,
      "muted",
    ),
  );

  const grid = element("div", null, "fact-grid");
  for (const [name, label] of Object.entries(labels)) {
    const field = source.fields?.[name] || {};
    const quality = field.quality || "UNKNOWN_DATA";
    const value =
      quality === "OBSERVED"
        ? `${field.value ?? "UNKNOWN"} ${field.unit || ""}`.trim()
        : `UNKNOWN · ${field.reason || quality}`;
    const fact = element("div", null, "fact");
    fact.append(element("span", label), element("strong", value));
    grid.append(fact);
  }
  section.append(grid);
  return section;
}

export function buildBaoStockPriceSourceView(row) {
  const qfq = row?.price_coordinate === "FORWARD_ADJUSTED";
  const coordinatePrefix = qfq ? "qfq_" : "raw_";
  const priceQuality = qfq ? "qfq_price_quality" : "price_quality";
  const priceReason = qfq ? "qfq_price_reason" : "price_reason";
  const fields = {
    open: {
      value: row?.[`${coordinatePrefix}open`],
      quality: row?.[priceQuality],
      reason: row?.[priceReason],
    },
    high: {
      value: row?.[`${coordinatePrefix}high`],
      quality: row?.[priceQuality],
      reason: row?.[priceReason],
    },
    low: {
      value: row?.[`${coordinatePrefix}low`],
      quality: row?.[priceQuality],
      reason: row?.[priceReason],
    },
    close: {
      value: row?.[`${coordinatePrefix}close`],
      quality: row?.[priceQuality],
      reason: row?.[priceReason],
    },
    volume: {
      value: row?.raw_volume,
      quality: row?.volume_quality,
      reason: row?.volume_reason,
      unit: "股",
      price_coordinate: "RAW_UNADJUSTED",
    },
    amount: {
      value: row?.raw_amount,
      quality: row?.amount_quality,
      reason: row?.amount_reason,
      unit: "元",
      price_coordinate: "RAW_UNADJUSTED",
    },
    turn: {
      value: row?.raw_turn_value,
      quality: row?.raw_turn_quality,
      reason: row?.raw_turn_reason,
      unit: "比例",
      source_coordinate: "RAW_UNADJUSTED",
    },
    tradestatus: {
      value: qfq ? row?.qfq_tradestatus_value : row?.raw_tradestatus_value,
      quality: qfq
        ? row?.qfq_tradestatus_quality
        : row?.raw_tradestatus_quality,
      reason: qfq ? row?.qfq_tradestatus_reason : row?.raw_tradestatus_reason,
    },
    is_st: {
      value: qfq ? row?.qfq_is_st_value : row?.raw_is_st_value,
      quality: qfq ? row?.qfq_is_st_quality : row?.raw_is_st_quality,
      reason: qfq ? row?.qfq_is_st_reason : row?.raw_is_st_reason,
    },
  };
  return {
    qfq,
    fields,
    labels: {
      open: "开盘",
      high: "最高",
      low: "最低",
      close: "收盘",
      volume: "成交量（RAW未复权）",
      amount: "成交额（RAW未复权）",
      turn: "换手率（RAW当日比例）",
      tradestatus: "交易状态",
      is_st: "ST标记",
    },
  };
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
