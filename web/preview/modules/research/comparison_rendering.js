/* Stock A预览页面comparison_rendering职责模块；保持原业务逻辑和交互。 */
"use strict";
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
      `统一目标 ${identity.target_session} · namespace ${identity.namespace_id} · 完整 package SHA ${identity.package_sha256} · profile_revision ${identity.profile_revision} · ` +
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
    ? `目标 ${identity.target_session} · namespace ${identity.namespace_id} · package SHA ${identity.package_sha256} · profile_revision ${identity.profile_revision} · RAW_UNADJUSTED / NON_PIT_RETROSPECTIVE。`
    : "等待同一精确来源版本的比较篮。";
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
