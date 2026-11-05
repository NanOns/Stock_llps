/* Stock A预览页面price_timeline职责模块；保持原业务逻辑和交互。 */
"use strict";
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
    state.timeline?.source_version?.source_set_id ||
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
      "本次轨迹未附带与所选日期、完整包SHA及profile_revision一致的Profile11快照；此处仅展示RAW Core事实，不重新评价持久画像或选股状态。";
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
    `已绑定PG持久化快照 · ${snapshot.target_session} · 包SHA ${snapshot.package_sha256} · Profile修订 ${snapshot.profile_revision}。` +
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
  $("priceStructureIdentity").textContent =
    `${point.session_date} · ${state.timeline?.symbol || "—"} · RAW_UNADJUSTED / ${state.timeline?.pit_status || "NON_PIT_RETROSPECTIVE"} · namespace ${point.namespace_id || "UNKNOWN"} · 来源包SHA ${point.package_sha256 || "UNKNOWN"}`;
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
    `namespace ${state.timeline?.namespace_id || "UNKNOWN"} · ` +
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
    `来源版本 · namespace ${binding.namespace_id || "UNKNOWN"} · ` +
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
        ? `已恢复原精确双日查询 ${binding.before_snapshot.target_session} → ${binding.after_snapshot.target_session} · ${binding.symbol}；重点候选序位已按原完整包SHA和profile revision重新读取。`
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
