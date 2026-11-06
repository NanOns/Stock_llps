/* Stock A预览页面profile_breadth职责模块；保持原业务逻辑和交互。 */
"use strict";
function clearProfileBreadth(message = "请选择两个完整PG画像快照。") {
  state.profileBreadthRequest++;
  state.profileBreadthMemberRequest++;
  state.profileBreadthData = null;
  state.profileBreadthBinding = null;
  state.profileBreadthPendingKey = null;
  state.profileBreadthMemberData = null;
  state.profileBreadthMemberQuery = null;
  state.profileBreadthMemberReturn = null;
  $("profileBreadthCount").textContent = "等待精确快照";
  $("profileBreadthMembers").hidden = true;
  $("profileBreadthMembersRows").replaceChildren();
  $("profileBreadthMembersPage").textContent = "—";
  const root = $("profileBreadthSummary");
  root.replaceChildren();
  appendText(root, "p", message, "muted");
  $("profileBreadthOpenFull").disabled = true;
  $("profileBreadthOpenPair").disabled = true;
}
function breadthFraction(fraction, digits = 2) {
  if (!fraction || !fraction.denominator) return "未知（分母为0）";
  const numerator = Number(fraction.numerator);
  const denominator = Number(fraction.denominator);
  const percent = (100 * numerator) / denominator;
  return `${fraction.numerator}/${fraction.denominator} · ${percent.toFixed(digits)}%`;
}
function breadthReason(reason) {
  const [code, detail] = String(reason || "UNKNOWN").split(/:(.*)/s);
  const primary = labels[code] || code;
  return detail ? `${primary}：${text(detail)}` : primary;
}
function breadthMemberCountButton(parent, title, count, query) {
  const dt = document.createElement("dt");
  dt.textContent = title;
  const dd = document.createElement("dd");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "breadth-count-action";
  button.textContent = String(count);
  button.setAttribute(
    "aria-label",
    `${title}，${count}个来源代码，查看分类成员`,
  );
  button.addEventListener("click", () => loadProfileBreadthMembers(query, 0));
  dd.append(button);
  parent.append(dt, dd);
}
function breadthRelationDetail(relation) {
  return {
    mode: "ENDPOINT",
    side: relation.side,
    relation: relation.relation,
  };
}
function renderProfileBreadth(data) {
  const root = $("profileBreadthSummary");
  root.replaceChildren();
  const before = data.before;
  const after = data.after;
  const paired = data.paired;
  const membership = data.membership;
  const relationNames = ["ABOVE", "BELOW", "EQUAL", "UNKNOWN"];
  const sides = [
    ["before", "起始日", before],
    ["after", "结束日", after],
  ];
  const sideGrid = appendText(root, "div", "", "breadth-side-grid");
  for (const [key, title, side] of sides) {
    const card = document.createElement("section");
    card.className = "breadth-side";
    appendText(
      card,
      "h4",
      `${title} · ${data[`${key}_snapshot`].target_session}`,
    );
    appendText(
      card,
      "p",
      `全来源分母 ${side.source_symbol_count}；可评价 ${side.eligible_count}（${breadthFraction(side.eligible_coverage_fraction)}）；可评价项中高于MA20 ${breadthFraction(side.above_eligible_fraction)}`,
      "muted",
    );
    const positionList = document.createElement("dl");
    positionList.className = "breadth-position-list";
    for (const relation of relationNames)
      breadthMemberCountButton(
        positionList,
        text(relation),
        side.positions[relation],
        breadthRelationDetail({
          side: key === "before" ? "BEFORE" : "AFTER",
          relation,
        }),
      );
    card.append(positionList);
    const unknown = document.createElement("div");
    appendText(unknown, "h5", "UNKNOWN互斥主原因");
    if (side.unknown_reasons.length) {
      const list = document.createElement("ul");
      for (const item of side.unknown_reasons)
        appendText(list, "li", `${breadthReason(item.reason)} · ${item.count}`);
      unknown.append(list);
    } else appendText(unknown, "p", "无UNKNOWN项", "muted");
    card.append(unknown);
    const boardWrap = document.createElement("div");
    boardWrap.className = "table-wrap";
    const boardTable = document.createElement("table");
    boardTable.className = "breadth-table";
    const boardHead = document.createElement("thead");
    const boardHeadRow = document.createElement("tr");
    for (const heading of [
      "来源标签（非上市总体）",
      "ABOVE",
      "BELOW",
      "EQUAL",
      "UNKNOWN",
      "来源数",
    ])
      appendText(boardHeadRow, "th", heading);
    boardHead.append(boardHeadRow);
    boardTable.append(boardHead);
    const boardBody = document.createElement("tbody");
    for (const group of side.board_distribution) {
      const row = document.createElement("tr");
      cell(
        row,
        `${text(group.market)} / ${text(group.board)}${group.research_board_label ? " · 来源标注" : " · 标签未核"}`,
      );
      for (const relation of relationNames)
        cell(row, String(group.positions[relation]));
      cell(row, String(group.source_count));
      boardBody.append(row);
    }
    boardTable.append(boardBody);
    boardWrap.append(boardTable);
    card.append(boardWrap);
    sideGrid.append(card);
  }

  const pairSection = document.createElement("section");
  pairSection.className = "breadth-paired";
  appendText(
    pairSection,
    "h4",
    `两端共同且兼容来源集合 C=${paired.compatible_count} · 交集 ${membership.intersection_count}`,
  );
  const incomparableButton = document.createElement("button");
  incomparableButton.type = "button";
  incomparableButton.className =
    "breadth-count-action breadth-incomparable-action";
  incomparableButton.textContent = `不可比 ${paired.incomparable_count} · 查看来源代码`;
  incomparableButton.addEventListener("click", () =>
    loadProfileBreadthMembers({ mode: "INCOMPARABLE" }, 0),
  );
  pairSection.append(incomparableButton);
  appendText(
    pairSection,
    "p",
    `来源代码并集 ${membership.source_symbol_union_count}；仅起始 ${membership.before_only_count}；仅结束 ${membership.after_only_count}。本端日内广度按各自完整快照分母；两日变化只按C计算。`,
    "muted",
  );
  const matrixWrap = document.createElement("div");
  matrixWrap.className = "table-wrap";
  const matrix = document.createElement("table");
  matrix.className = "breadth-table breadth-matrix";
  const matrixHead = document.createElement("thead");
  const matrixHeadRow = document.createElement("tr");
  for (const heading of ["起始 ↓ / 结束 →", "BELOW", "EQUAL", "ABOVE"])
    appendText(matrixHeadRow, "th", heading);
  matrixHead.append(matrixHeadRow);
  matrix.append(matrixHead);
  const matrixBody = document.createElement("tbody");
  for (const from of ["BELOW", "EQUAL", "ABOVE"]) {
    const row = document.createElement("tr");
    cell(row, text(from));
    for (const to of ["BELOW", "EQUAL", "ABOVE"]) {
      const td = document.createElement("td");
      const count = paired.transition_matrix[`${from}_TO_${to}`];
      const button = document.createElement("button");
      button.type = "button";
      button.className = "breadth-count-action";
      button.textContent = String(count);
      button.setAttribute(
        "aria-label",
        `${text(from)}转为${text(to)}，${count}个来源代码，查看分类成员`,
      );
      button.addEventListener("click", () =>
        loadProfileBreadthMembers(
          { mode: "TRANSITION", from_relation: from, to_relation: to },
          0,
        ),
      );
      td.append(button);
      row.append(td);
    }
    matrixBody.append(row);
  }
  matrix.append(matrixBody);
  matrixWrap.append(matrix);
  pairSection.append(matrixWrap);
  const delta = paired.delta_above_percentage_points;
  const deltaView =
    delta && delta.denominator
      ? `${delta.numerator}/${delta.denominator}个百分点 · ${(Number(delta.numerator) / Number(delta.denominator)).toFixed(2)}个百分点`
      : "未知（共同可比集合为空）";
  appendText(
    pairSection,
    "p",
    `C内ABOVE ${paired.paired_above_before_count} → ${paired.paired_above_after_count}；同口径变化 ${deltaView}。严格下方跨上方 ${paired.strict_cross_up_count} · 上方跨下方 ${paired.strict_cross_down_count} · 涉及EQUAL的转移 ${paired.equal_transition_count}。`,
    "breadth-delta",
  );
  if (paired.incomparable_reasons.length) {
    const reasons = document.createElement("ul");
    for (const item of paired.incomparable_reasons)
      appendText(
        reasons,
        "li",
        `${breadthReason(item.reason)} · ${item.count}`,
      );
    pairSection.append(reasons);
  }
  root.append(pairSection);

  const identity = document.createElement("details");
  identity.className = "breadth-identity";
  appendText(identity, "summary", "展开完整来源版本与研究身份");
  for (const [key, title] of sides) {
    const snapshot = data[`${key}_snapshot`];
    appendText(
      identity,
      "p",
      `${title} ${snapshot.target_session} · package SHA ${snapshot.package_sha256} · profile revision ${snapshot.profile_revision} · 日历 ${snapshot.history_calendar_id}/${snapshot.history_calendar_revision} · 窗口 ${snapshot.window_start} 至 ${snapshot.window_end} · ${snapshot.history_session_count}个官方会话`,
    );
  }
  appendText(
    identity,
    "p",
    `namespace ${data.namespace_id} · ${data.research_scope} · ${data.price_coordinate} · ${data.pit_status} · 正式上市总体 ${text(data.formal_listing_universe)}。本查询只读PG，不打开来源包、不重算Core。`,
  );
  root.append(identity);
  $("profileBreadthCount").textContent =
    `全来源 ${before.source_symbol_count} / ${after.source_symbol_count} · C ${paired.compatible_count}`;
  $("profileBreadthOpenFull").disabled = false;
  $("profileBreadthOpenPair").disabled = false;
  state.profileBreadthData = data;
}
async function loadProfileBreadth(before, after) {
  const key = `${profileSnapshotKey(before)}→${profileSnapshotKey(after)}`;
  if (
    state.profileBreadthBinding === key &&
    (state.profileBreadthData || state.profileBreadthPendingKey === key)
  )
    return;
  const requestId = ++state.profileBreadthRequest;
  state.profileBreadthMemberRequest++;
  state.profileBreadthBinding = key;
  state.profileBreadthPendingKey = key;
  state.profileBreadthData = null;
  state.profileBreadthMemberData = null;
  state.profileBreadthMemberQuery = null;
  state.profileBreadthMemberReturn = null;
  $("profileBreadthMembers").hidden = true;
  $("profileBreadthMembersRows").replaceChildren();
  $("profileBreadthMembersPage").textContent = "—";
  $("profileBreadthCount").textContent = "完整来源池统计中";
  const root = $("profileBreadthSummary");
  root.replaceChildren();
  appendText(
    root,
    "p",
    "正在读取两端精确画像的全来源代码池与已存MA20…",
    "muted",
  );
  $("profileBreadthOpenFull").disabled = true;
  $("profileBreadthOpenPair").disabled = true;
  const params = new URLSearchParams({
    before_session: before.target_session,
    after_session: after.target_session,
    before_package_sha256: before.package_sha256,
    before_profile_revision: before.profile_revision,
    after_package_sha256: after.package_sha256,
    after_profile_revision: after.profile_revision,
  });
  try {
    const data = await api(
      `/api/source-symbol-price-profile-breadth?${params}`,
    );
    if (
      requestId !== state.profileBreadthRequest ||
      key !== state.profileBreadthBinding
    )
      return;
    renderProfileBreadth(data);
  } catch (error) {
    if (
      requestId !== state.profileBreadthRequest ||
      key !== state.profileBreadthBinding
    )
      return;
    state.profileBreadthData = null;
    $("profileBreadthCount").textContent = "来源池统计失败";
    root.replaceChildren();
    appendText(root, "p", `没有可用广度结果：${error.message}`, "message");
  } finally {
    if (state.profileBreadthPendingKey === key)
      state.profileBreadthPendingKey = null;
  }
}
function breadthMemberQueryLabel(query) {
  if (query.mode === "ENDPOINT")
    return `${query.side === "BEFORE" ? "起始日" : "结束日"} · ${text(query.relation)}`;
  if (query.mode === "TRANSITION")
    return `${text(query.from_relation)} → ${text(query.to_relation)}`;
  return "两端存在但价格/Core或来源标签不可比";
}
function breadthMemberRawText(item, side) {
  const prefix = side === "BEFORE" ? "before" : "after";
  if (!item[`${prefix}_present`]) return "该日来源集合中没有此代码";
  const relation = item[`${prefix}_source_pool_close_ma20_relation`];
  const unknown = item[`${prefix}_source_pool_unknown_reason`];
  const reason = unknown ? ` · ${breadthReason(unknown)}` : "";
  const rawQuality = `${text(item[`${prefix}_raw_price_quality`])}${
    item[`${prefix}_raw_price_reason`]
      ? `：${text(item[`${prefix}_raw_price_reason`])}`
      : ""
  }`;
  const ma20Quality = `${text(item[`${prefix}_ma20_quality`])}${
    item[`${prefix}_ma20_reason`]
      ? `：${text(item[`${prefix}_ma20_reason`])}`
      : ""
  }`;
  return (
    `收盘 ${text(item[`${prefix}_raw_close`])} ` +
    `(${rawQuality}) / ` +
    `MA20 ${text(item[`${prefix}_ma20`])} ` +
    `(${ma20Quality}) · ` +
    `${text(relation)}${reason}`
  );
}
function renderProfileBreadthMembers(data, query) {
  const panel = $("profileBreadthMembers");
  const page = data.page;
  panel.hidden = false;
  $("profileBreadthMembersTitle").textContent =
    `来源分类成员 · ${breadthMemberQueryLabel(query)}`;
  $("profileBreadthMembersStatus").textContent =
    `共${page.total}个精确来源代码 · 当前${page.offset + 1}–${page.offset + page.count} · ` +
    `第${page.total ? Math.floor(page.offset / page.limit) + 1 : 0}/` +
    `${page.total ? Math.ceil(page.total / page.limit) : 0}页 · ` +
    `每页${page.limit} · ${sessionRelationLabel(data.session_relation)} · ` +
    `SOURCE_SYMBOL_ONLY / RAW_UNADJUSTED / NON_PIT；选股未评价。`;
  const rows = $("profileBreadthMembersRows");
  rows.replaceChildren();
  if (!data.rows.length) {
    tableRow(rows, ["该分类没有来源代码。", "", "", "", ""]);
  }
  for (const item of data.rows) {
    const row = document.createElement("tr");
    const codeCell = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet breadth-member-open";
    button.textContent = `${item.source_symbol} · 双日画像`;
    button.title =
      "打开两份精确来源画像；可继续查看75会话Raw/Core并返回本分类页。";
    button.addEventListener("click", () => openBreadthMemberPair(item));
    codeCell.append(button);
    row.append(codeCell);
    cell(row, breadthMemberRawText(item, "BEFORE"));
    cell(row, breadthMemberRawText(item, "AFTER"));
    cell(
      row,
      [
        item.before_source_pool_unknown_reason
          ? `起始：${breadthReason(item.before_source_pool_unknown_reason)}`
          : "",
        item.after_source_pool_unknown_reason
          ? `结束：${breadthReason(item.after_source_pool_unknown_reason)}`
          : "",
        !item.source_pool_pair_compatible &&
        !item.before_source_pool_unknown_reason &&
        !item.after_source_pool_unknown_reason
          ? "Core语义、价格坐标或来源market/board标签不兼容"
          : "",
      ]
        .filter(Boolean)
        .join("；") || "—",
    );
    cell(
      row,
      `${text(item.before_market)} / ${text(item.before_board)} → ` +
        `${text(item.after_market)} / ${text(item.after_board)} · ` +
        `${text(item.before_identity_status)} / ${text(item.after_identity_status)} · ` +
        `${text(item.before_price_coordinate)} / ${text(item.after_price_coordinate)}`,
    );
    rows.append(row);
  }
  $("profileBreadthMembersPage").textContent =
    `偏移 ${page.offset} · 本页 ${page.count}/${page.limit} · 分类总数 ${page.total}`;
  $("profileBreadthMembersPrev").disabled = page.offset === 0;
  $("profileBreadthMembersNext").disabled = page.next_offset === null;
  $("profileBreadthMembersLast").disabled =
    page.last_offset === null || page.offset >= page.last_offset;
  state.profileBreadthMemberData = data;
}
async function loadProfileBreadthMembers(query, offset) {
  const breadth = state.profileBreadthData;
  const binding = state.profileBreadthBinding;
  if (
    !breadth ||
    !binding ||
    binding !==
      `${profileSnapshotKey(breadth.before_snapshot)}→${profileSnapshotKey(breadth.after_snapshot)}`
  )
    return;
  const requestId = ++state.profileBreadthMemberRequest;
  state.profileBreadthMemberQuery = { ...query };
  state.profileBreadthMemberOffset = offset;
  state.profileBreadthMemberData = null;
  const panel = $("profileBreadthMembers");
  panel.hidden = false;
  $("profileBreadthMembersTitle").textContent =
    `来源分类成员 · ${breadthMemberQueryLabel(query)}`;
  $("profileBreadthMembersStatus").textContent = "正在读取精确来源成员…";
  $("profileBreadthMembersRows").replaceChildren();
  $("profileBreadthMembersPage").textContent = "查询中";
  $("profileBreadthMembersPrev").disabled = true;
  $("profileBreadthMembersNext").disabled = true;
  $("profileBreadthMembersLast").disabled = true;
  const params = new URLSearchParams({
    before_session: breadth.before_snapshot.target_session,
    after_session: breadth.after_snapshot.target_session,
    before_package_sha256: breadth.before_snapshot.package_sha256,
    before_profile_revision: breadth.before_snapshot.profile_revision,
    after_package_sha256: breadth.after_snapshot.package_sha256,
    after_profile_revision: breadth.after_snapshot.profile_revision,
    mode: query.mode,
    offset: String(offset),
    limit: String(state.profileBreadthMemberLimit),
  });
  if (query.mode === "ENDPOINT") {
    params.set("side", query.side);
    params.set("relation", query.relation);
  } else if (query.mode === "TRANSITION") {
    params.set("from", query.from_relation);
    params.set("to", query.to_relation);
  }
  try {
    const data = await api(
      `/api/source-symbol-price-profile-breadth-members?${params}`,
    );
    if (
      requestId !== state.profileBreadthMemberRequest ||
      binding !== state.profileBreadthBinding ||
      data.before_snapshot.package_sha256 !==
        breadth.before_snapshot.package_sha256 ||
      data.before_snapshot.profile_revision !==
        breadth.before_snapshot.profile_revision ||
      data.after_snapshot.package_sha256 !==
        breadth.after_snapshot.package_sha256 ||
      data.after_snapshot.profile_revision !==
        breadth.after_snapshot.profile_revision
    )
      return;
    renderProfileBreadthMembers(data, query);
  } catch (error) {
    if (requestId !== state.profileBreadthMemberRequest) return;
    panel.hidden = false;
    $("profileBreadthMembersStatus").textContent =
      `来源成员读取失败：${error.message}`;
    $("profileBreadthMembersRows").replaceChildren();
    tableRow($("profileBreadthMembersRows"), [error.message, "", "", "", ""]);
    $("profileBreadthMembersPage").textContent = "读取失败";
  }
}
async function openBreadthMemberPair(item) {
  const breadth = state.profileBreadthData;
  const query = state.profileBreadthMemberQuery;
  if (!breadth || !query || !state.profileBreadthMemberData) return;
  const requestId = ++state.pairRequest;
  const memberBinding = state.profileBreadthBinding;
  state.profileBreadthMemberReturn = {
    query: { ...query },
    offset: state.profileBreadthMemberOffset,
    limit: state.profileBreadthMemberLimit,
    binding: memberBinding,
    selectedVersion: selectedVersion(),
  };
  const params = new URLSearchParams({
    before_session: breadth.before_snapshot.target_session,
    after_session: breadth.after_snapshot.target_session,
    symbol: item.source_symbol,
    before_package_sha256: breadth.before_snapshot.package_sha256,
    before_profile_revision: breadth.before_snapshot.profile_revision,
    after_package_sha256: breadth.after_snapshot.package_sha256,
    after_profile_revision: breadth.after_snapshot.profile_revision,
  });
  resetChangeDetail();
  const root = $("changeDetail");
  root.hidden = false;
  appendText(root, "p", "正在读取该成员对应的精确双日画像…", "population");
  root.scrollIntoView({ behavior: "smooth", block: "start" });
  try {
    const data = await api(`/api/source-symbol-price-profile-pair?${params}`);
    if (
      requestId !== state.pairRequest ||
      memberBinding !== state.profileBreadthBinding ||
      data.source_symbol !== item.source_symbol ||
      data.before_snapshot.package_sha256 !==
        breadth.before_snapshot.package_sha256 ||
      data.after_snapshot.package_sha256 !==
        breadth.after_snapshot.package_sha256
    )
      return;
    state.changeBinding = {
      namespace_id: data.namespace_id,
      before_snapshot: data.before_snapshot,
      after_snapshot: data.after_snapshot,
      session_relation: data.session_relation,
      offset: 0,
      limit: 1,
      change_type: "ALL",
      symbol: item.source_symbol,
      origin: "breadth",
      breadth_query: { ...query },
    };
    showChangeDetail(data.item);
  } catch (error) {
    if (requestId !== state.pairRequest) return;
    root.replaceChildren();
    root.hidden = false;
    appendText(root, "p", `双日画像读取失败：${error.message}`, "message");
  }
}
