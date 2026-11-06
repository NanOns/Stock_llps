/* Stock A预览页面price_relations职责模块；保持原业务逻辑和交互。 */
"use strict";
function relationDetailQueryKey(query, relationData, sourceSymbol) {
  if (!query || !relationData || !sourceSymbol) return null;
  const snapshotIdentity = (snapshot) => ({
    target_session: snapshot?.target_session,
    package_sha256: snapshot?.package_sha256,
    profile_revision: snapshot?.profile_revision,
  });
  const before = snapshotIdentity(query.before);
  const after = snapshotIdentity(query.after);
  if (
    !before.target_session ||
    !before.package_sha256 ||
    !before.profile_revision ||
    !after.target_session ||
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
    " / SHA " +
    data.before_snapshot.package_sha256 +
    " / revision " +
    data.before_snapshot.profile_revision +
    "；结束 " +
    data.after_snapshot.target_session +
    " / SHA " +
    data.after_snapshot.package_sha256 +
    " / revision " +
    data.after_snapshot.profile_revision +
    "。来源代码级RAW_UNADJUSTED · NON_PIT；上市总体未知。";
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
      item.before_package_sha256 +
      " / " +
      item.before_profile_revision +
      " → " +
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
