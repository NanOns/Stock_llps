/* Stock A预览页面change_list职责模块；保持原业务逻辑和交互。 */
"use strict";
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
    params.set(`${prefix}_source_package_id`, snapshot.source_package_id);
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
      `${sessionRelationLabel(data.session_relation)} · 快照分母 ${data.before_snapshot.source_symbol_count}/${data.before_snapshot.expected_source_symbol_count} 与 ${data.after_snapshot.source_symbol_count}/${data.after_snapshot.expected_source_symbol_count} · 并集 ${s.source_symbol_union_count} · 交集 ${s.intersection_count} · 起始独有 ${s.before_only_count} · 结束独有 ${s.after_only_count} · 跨价格坐标 ${s.cross_price_coordinate_pair_count} · Core/Profile不可比 ${s.core_semantics_incomparable_count}/${s.profile_semantics_incomparable_count} · 收盘/MA20未知 ${s.close_ma20_unknown_count} · Profile未观察 ${s.profile_unobserved_pair_count} · 完整变化 ${s.changed_source_symbol_count} · ${profileFieldId ? `Profile11 ${profileFieldLabel(profileFieldId)} ${profileChangeKind}` : "Profile11字段变化未过滤"} · 距离筛选 ${bias20AtrStateLabels[expectedBias20AtrStateFilter.before]} → ${bias20AtrStateLabels[expectedBias20AtrStateFilter.after]} · 当前命中 ${p.total} · ${hasDistanceFilter ? "距离筛查可包含现役变化分类为空的代码；并不代表状态发生变化或交易信号。" : "选股未评价。"}`;
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
