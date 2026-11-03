/* Stock A预览页面profile_search职责模块；保持原业务逻辑和交互。 */
"use strict";
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
