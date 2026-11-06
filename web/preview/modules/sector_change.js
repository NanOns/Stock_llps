/* Stock A预览页面行业/GN变化联动模块。 */
"use strict";

const sectorChangeLabels = {
  INDUSTRY: "行业",
  CONCEPT: "GN概念",
  CHANGED: "有变化",
  UNCHANGED: "未变化",
  UNKNOWN: "未知",
  NOT_COMPARABLE: "不可比较",
  NOT_EVALUABLE: "不可评价",
  NO_TRUE_D1_EVENT_IN_WINDOW: "所选事件窗内没有TRUE事件",
  MEMBER_NOT_IN_EXACT_TARGET_SOURCE_POOL: "成员不在精确目标来源池",
};
const sectorChangeStateOrder = [
  "CHANGED",
  "UNCHANGED",
  "UNKNOWN",
  "NOT_COMPARABLE",
  "NOT_EVALUABLE",
];

function sectorSelectedVersion() {
  const value = $("sectorVersionSelect").value;
  if (!value.includes("|")) return null;
  const [target_session, package_sha256] = value.split("|");
  return { target_session, package_sha256 };
}

function sectorLabel(value) {
  if (value === null || value === undefined || value === "") return "—";
  return sectorChangeLabels[value] || text(value);
}

function sectorMetric(value) {
  if (value === null || value === undefined) return "不可计算";
  if (typeof value === "object") {
    if (value.decimal !== undefined) return value.decimal;
    if (value.exact !== undefined) return value.exact;
    if (
      Number.isInteger(value.numerator) &&
      Number.isInteger(value.denominator)
    )
      return value.denominator
        ? (value.numerator / value.denominator).toFixed(4)
        : "不可计算";
  }
  return String(value);
}

function sectorClearOutput(message = "请先选择完整身份和D1事件窗。") {
  const sector = state.sectorChange;
  sector.request++;
  sector.data = null;
  sector.selectedBoard = null;
  sector.boardOffset = 0;
  sector.stockOffset = 0;
  sector.memberOffset = 0;
  $("sectorChangeStatus").textContent = message;
  $("sectorChangeIdentity").hidden = true;
  $("sectorMarketSummary").hidden = true;
  $("sectorBoardSection").hidden = true;
  $("sectorChangedStocksSection").hidden = true;
  $("sectorBoardDetail").hidden = true;
  $("sectorBoardRows").replaceChildren();
  $("sectorChangedStockRows").replaceChildren();
  $("sectorMemberRows").replaceChildren();
  $("sectorChangeCount").textContent = "等待精确来源身份";
  $("sectorChangeQuery").disabled = !sectorInputsReady();
}

function sectorInputsReady() {
  return Boolean(
    sectorSelectedVersion() &&
    $("sectorProfileRevisionSelect").value &&
    $("sectorIndustrySnapshotSelect").value &&
    $("sectorConceptSnapshotSelect").value &&
    $("sectorEventWindowSelect").value,
  );
}

function sectorInvalidate(message) {
  sectorClearOutput(message);
  $("sectorChangeQuery").disabled = !sectorInputsReady();
}

function sectorExactProfile(version) {
  return state.profileSnapshots.filter(
    (snapshot) =>
      snapshot.target_session === version.target_session &&
      snapshot.package_sha256 === version.package_sha256 &&
      snapshot.complete === true,
  );
}

function sectorPopulateProfileRevisions(version) {
  const select = $("sectorProfileRevisionSelect");
  select.replaceChildren(new Option("— 请选择完整Profile修订 —", ""));
  for (const snapshot of sectorExactProfile(version)) {
    const option = new Option(
      `rev ${snapshot.profile_revision.slice(0, 12)}… · ${snapshot.source_symbol_count}/${snapshot.expected_source_symbol_count} · ${snapshot.history_session_count}会话`,
      snapshot.profile_revision,
    );
    option.title =
      `namespace ${snapshot.namespace_id || state.namespace_id} · ` +
      `target ${snapshot.target_session} · package ${snapshot.package_sha256} · ` +
      `profile_revision ${snapshot.profile_revision}`;
    select.append(option);
  }
  select.disabled = select.options.length <= 1;
}

function sectorPopulateSnapshots(context) {
  const sourceById = {
    sectorIndustrySnapshotSelect: "TDX_TDXHY_CFG",
    sectorConceptSnapshotSelect: "TDX_INFOHARBOR_BLOCK_DAT",
  };
  for (const [id, sourceKind] of Object.entries(sourceById)) {
    const select = $(id);
    const current = select.value;
    select.replaceChildren(
      new Option(
        sourceKind === "TDX_TDXHY_CFG"
          ? "— 请选择PG行业快照 —"
          : "— 请选择PG GN快照 —",
        "",
      ),
    );
    const choices = (context.snapshots || []).filter(
      (snapshot) => snapshot.source_kind === sourceKind,
    );
    for (const snapshot of choices) {
      const kindCount =
        sourceKind === "TDX_TDXHY_CFG"
          ? snapshot.industry_member_rows
          : snapshot.concept_member_rows;
      const observedLabel = snapshot.observed_after_target
        ? "后验观测·NON_PIT"
        : "目标日内观测";
      const option = new Option(
        `${snapshot.observed_at} · ${observedLabel} · ${kindCount}成员关系 · ${snapshot.snapshot_id.slice(0, 8)}…`,
        snapshot.snapshot_id,
      );
      option.title =
        `${snapshot.snapshot_id} · ${snapshot.source_kind} · ` +
        `${snapshot.source_time_role} · ${snapshot.temporal_relationship} · ` +
        `成员关系 ${kindCount} · 质量 ${snapshot.source_quality}`;
      select.append(option);
    }
    select.disabled = choices.length === 0;
    if (choices.some((snapshot) => snapshot.snapshot_id === current))
      select.value = current;
  }
}

async function loadSectorChangeContext() {
  const version = sectorSelectedVersion();
  const request = ++state.sectorChange.contextRequest;
  sectorInvalidate(
    "正在读取精确Profile修订、行业/GN快照目录和官方会话窗。旧结果已清除。",
  );
  for (const id of [
    "sectorProfileRevisionSelect",
    "sectorIndustrySnapshotSelect",
    "sectorConceptSnapshotSelect",
    "sectorEventWindowSelect",
  ]) {
    $(id).disabled = true;
    $(id).replaceChildren(new Option("正在读取PG目录…", ""));
  }
  if (!version) {
    sectorInvalidate("请选择PG中已有的目标日期与来源包版本。");
    return;
  }
  sectorPopulateProfileRevisions(version);
  try {
    const context = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: version.target_session })}`,
    );
    if (
      request !== state.sectorChange.contextRequest ||
      `${sectorSelectedVersion()?.target_session}|${sectorSelectedVersion()?.package_sha256}` !==
        `${version.target_session}|${version.package_sha256}`
    )
      return;
    state.sectorChange.context = context;
    sectorPopulateSnapshots(context);
    const select = $("sectorEventWindowSelect");
    select.replaceChildren(new Option("— 请选择D1事件窗 —", ""));
    for (const window of context.window_options || []) {
      select.append(
        new Option(
          `${window.event_session_count}个此前官方会话 · ${window.event_window_start} → ${window.target_session}（Raw历史${window.pg_history_session_count}）`,
          `${window.event_window_start}|${window.event_session_count}|${window.pg_history_session_count}`,
        ),
      );
    }
    select.disabled = !context.window_options?.length;
    $("sectorChangeStatus").textContent =
      `${version.target_session} · 截止 ${context.research_cutoff} · namespace ${context.namespace_id}。` +
      `请选择精确Profile修订、行业/GN快照和事件窗；观察时间仅表示本地留存观测，不等于来源可用时间。`;
  } catch (error) {
    if (request !== state.sectorChange.contextRequest) return;
    $("sectorChangeStatus").textContent =
      `行业/GN目录读取失败：${error.message}`;
  }
  $("sectorChangeQuery").disabled = !sectorInputsReady();
}

function sectorCurrentRequest() {
  const version = sectorSelectedVersion();
  const windowParts = $("sectorEventWindowSelect").value.split("|");
  const board = state.sectorChange.selectedBoard;
  return {
    target_session: version.target_session,
    research_cutoff: state.sectorChange.context.research_cutoff,
    package_sha256: version.package_sha256,
    profile_revision: $("sectorProfileRevisionSelect").value,
    industry_snapshot_id: $("sectorIndustrySnapshotSelect").value,
    concept_snapshot_id: $("sectorConceptSnapshotSelect").value,
    event_window_start: windowParts[0],
    pg_history_session_count: Number(windowParts[2]),
    board_kind: $("sectorBoardKindFilter").value,
    changed_only: $("sectorChangedOnly").checked,
    sort_by: $("sectorSortBy").value,
    direction: $("sectorDirection").value,
    offset: state.sectorChange.boardOffset,
    limit: 15,
    detail_board_kind: board?.board_kind || null,
    detail_board_native_id: board?.board_native_id || null,
    member_state: board ? $("sectorMemberState").value : "ALL",
    member_search: board ? $("sectorMemberSearch").value.trim() || null : null,
    member_offset: board ? state.sectorChange.memberOffset : 0,
    member_limit: board ? 30 : 0,
    include_changed_stock_page: true,
    stock_offset: state.sectorChange.stockOffset,
    stock_limit: 30,
  };
}

function sectorMetricPath(board, field) {
  const metrics = board?.returns?.[field]?.common_evaluable;
  const coverage = metrics?.coverage;
  return (
    `${sectorMetric(metrics?.target_median)} / Δ ${sectorMetric(metrics?.delta)} · ` +
    `${coverage?.numerator ?? 0}/${coverage?.denominator ?? board?.source_member_total ?? 0}`
  );
}

function renderSectorMetrics(data) {
  const counts = data.source_counts || {};
  const d1 = data.d1_discovery || {};
  const stateCounts =
    d1.support_event_counts || counts.d1_support_event_counts || {};
  const cards = [
    ["Raw来源池分母", counts.d1_source_pool_count],
    ["D1原始事件", counts.d1_raw_event_count],
    ["D1变化去重来源代码", counts.d1_unique_changed_source_symbol_count],
    ["行业 / GN完整板块", counts.legal_board_total_count],
    ["支撑变化事件 CHANGED", stateCounts.CHANGED],
    [
      "未变化 / 未知 / 不可比较",
      `${stateCounts.UNCHANGED ?? 0} / ${stateCounts.UNKNOWN ?? 0} / ${stateCounts.NOT_COMPARABLE ?? 0}`,
    ],
  ];
  const root = $("sectorMarketSummary");
  root.replaceChildren();
  for (const [label, value] of cards) {
    const article = document.createElement("article");
    const strong = document.createElement("strong");
    strong.textContent =
      value === null || value === undefined ? "—" : String(value);
    const span = document.createElement("span");
    span.textContent = label;
    article.append(strong, span);
    root.append(article);
  }
  root.hidden = false;
}

function sectorSelectBoard(board) {
  state.sectorChange.selectedBoard = {
    board_kind: board.board_kind,
    board_native_id: board.board_native_id,
  };
  state.sectorChange.memberOffset = 0;
  loadSectorChangeView();
}

function sectorStockButton(symbol, row, target) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "row-select";
  button.textContent = "打开Core / Profile / 历史";
  button.addEventListener("click", () => openSectorStock(symbol, row, target));
  return button;
}

function sectorCellButton(row, label, callback) {
  const td = document.createElement("td");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "row-select";
  button.textContent = label;
  button.addEventListener("click", callback);
  td.append(button);
  row.append(td);
}

function renderSectorBoards(data) {
  const root = $("sectorBoardRows");
  root.replaceChildren();
  if (!data.boards?.length) {
    tableRow(root, [
      "无符合本次视图条件的变化板块；请检查目标日、事件窗、板块类型与“只看有变化板块”筛选。",
      "",
      "",
      "",
      "",
      "",
    ]);
  }
  for (const board of data.boards || []) {
    const row = document.createElement("tr");
    const name = `${board.board_name} · ${board.board_native_id}`;
    const source = (data.strength_sources || []).find(
      (item) => item.board_kind === board.board_kind,
    );
    sectorCellButton(row, name, () => sectorSelectBoard(board));
    cell(
      row,
      `${sectorLabel(board.board_kind)} · ${source?.snapshot?.observed_at || "观测时间未知"}`,
    );
    cell(row, `${board.source_member_total} 个来源成员`);
    const stateCounts = board.d1_link?.member_state_counts || {};
    cell(
      row,
      `${board.d1_link?.changed_member_count ?? 0} 变化 / ${stateCounts.UNKNOWN ?? 0} 未知 / ${stateCounts.NOT_COMPARABLE ?? 0} 不可比较`,
    );
    cell(
      row,
      `5日 ${sectorMetricPath(board, "ret5")} · 20日 ${sectorMetricPath(board, "ret20")}`,
    );
    cell(
      row,
      `${board.target_core_profile_mapped_member_count ?? 0}/${board.source_member_total} 映射 · ${sectorLabel(board.minimum_quality_requirements?.research_fact_gate)}`,
    );
    root.append(row);
  }
  const page = data.page || {};
  $("sectorBoardSummary").textContent =
    `完整板块分母 ${page.filtered_total ?? 0}；本页 ${page.count ?? 0}/15。` +
    `所有板块先全量联算、排序和过滤后再分页。缺历史或质量不够的指标显示“不可计算”。`;
  $("sectorBoardPage").textContent =
    `偏移 ${page.offset ?? 0} · ${page.filtered_total ?? 0} 个符合筛选的板块`;
  $("sectorBoardPrev").disabled = !page.offset;
  $("sectorBoardNext").disabled =
    page.next_offset === null || page.next_offset === undefined;
  $("sectorBoardSection").hidden = false;
}

function renderSectorStocks(data) {
  const page = data.changed_stock_page || {};
  const root = $("sectorChangedStockRows");
  root.replaceChildren();
  for (const stock of page.stocks || []) {
    const row = document.createElement("tr");
    cell(row, stock.source_symbol);
    cell(
      row,
      `${stock.changed_anchor_event_count ?? 0} 次变化 / ${stock.raw_d1_event_count ?? 0} 个事件`,
    );
    const action = document.createElement("td");
    action.append(
      sectorStockButton(stock.source_symbol, stock, data.target_session),
    );
    row.append(action);
    root.append(row);
  }
  if (!page.stocks?.length)
    tableRow(root, ["所选事件窗没有可展示的CHANGED来源代码。", "", ""]);
  $("sectorChangedStocksCount").textContent =
    `去重总数 ${page.total_count ?? 0}`;
  $("sectorStockPage").textContent =
    `偏移 ${page.page?.offset ?? 0} · 总数 ${page.total_count ?? 0}`;
  $("sectorStockPrev").disabled = (page.page?.offset ?? 0) === 0;
  $("sectorStockNext").disabled =
    page.page?.next_offset === null || page.page?.next_offset === undefined;
  $("sectorChangedStocksSection").hidden = false;
}

function renderSectorMemberDetail(data) {
  const detail = data.detail_board;
  if (!detail) {
    $("sectorBoardDetail").hidden = true;
    return;
  }
  $("sectorBoardDetail").hidden = false;
  $("sectorBoardDetailTitle").textContent =
    `${detail.board_name} · 板内来源成员`;
  const counts = detail.d1_link?.member_state_counts || {};
  $("sectorBoardDetailSummary").textContent =
    `快照 ${detail.snapshot_id} · ${detail.source_kind} · 观测 ${data.strength_sources?.find((item) => item.board_kind === detail.board_kind)?.snapshot?.observed_at || "见来源目录"} · ` +
    `成员完整分母 ${detail.member_count}；变化 ${counts.CHANGED ?? 0}、未变化 ${counts.UNCHANGED ?? 0}、未知 ${counts.UNKNOWN ?? 0}、不可比较 ${counts.NOT_COMPARABLE ?? 0}、不可评价 ${counts.NOT_EVALUABLE ?? 0}。`;
  const page = detail.member_page || {};
  const root = $("sectorMemberRows");
  root.replaceChildren();
  for (const member of page.members || []) {
    const row = document.createElement("tr");
    cell(row, member.source_symbol);
    cell(row, sectorLabel(member.change_state));
    cell(row, `${member.anchor_events?.length ?? 0} 个锚事件`);
    const reasons = (member.unknown_reasons || []).map(sectorLabel);
    const events = (member.anchor_events || []).map(
      (event) =>
        `${event.session} ${sectorLabel(event.change_state)}${event.unknown_reason ? `（${sectorLabel(event.unknown_reason)}）` : ""}`,
    );
    const explanation = [...events, ...reasons];
    cell(
      row,
      explanation.length
        ? explanation.join("；")
        : "事件窗内没有支撑变化事件；状态按完整Raw历史计算",
    );
    const action = document.createElement("td");
    action.append(
      sectorStockButton(member.source_symbol, member, data.target_session),
    );
    row.append(action);
    root.append(row);
  }
  if (!page.members?.length)
    tableRow(root, [
      "当前状态、搜索与分页条件没有成员；完整成员总数仍在上方显示。",
      "",
      "",
      "",
      "",
    ]);
  $("sectorMemberSummary").textContent =
    `成员关系总数 ${detail.member_count}；当前状态/代码筛选后 ${page.filtered_total ?? 0}，本页 ${page.count ?? 0}/30。` +
    `UNKNOWN、不可比较与不可评价均按内核返回原因展示；未列入列表时需检查所选事件窗和筛选条件。`;
  $("sectorMemberPage").textContent =
    `偏移 ${page.offset ?? 0} · 筛选总数 ${page.filtered_total ?? 0}`;
  $("sectorMemberPrev").disabled = (page.offset ?? 0) === 0;
  $("sectorMemberNext").disabled =
    page.next_offset === null || page.next_offset === undefined;
}

function renderSectorChange(data) {
  state.sectorChange.data = data;
  const identity = $("sectorChangeIdentity");
  identity.hidden = false;
  identity.textContent =
    `${data.target_session} · namespace ${data.namespace_id} · package ${data.package_sha256} · ` +
    `Profile rev ${$("sectorProfileRevisionSelect").value} · NON_PIT回溯研究 · 来源代码级，不是正式选股。`;
  renderSectorMetrics(data);
  renderSectorBoards(data);
  renderSectorStocks(data);
  renderSectorMemberDetail(data);
  const discovery = data.d1_discovery || {};
  $("sectorChangeCount").textContent =
    `${data.source_counts?.d1_unique_changed_source_symbol_count ?? 0} 个全局去重变化代码`;
  $("sectorChangeStatus").textContent =
    `D1事件窗 ${discovery.event_window_start || "—"} → ${data.target_session}；Raw历史 ${discovery.execution?.history_session_count ?? sectorCurrentRequest().pg_history_session_count} 个官方会话。` +
    `板块成员快照为当前观测快照，历史归属NON_PIT。所有API与CLI读取PG，不写业务表。`;
}

async function loadSectorChangeView() {
  if (!sectorInputsReady()) {
    sectorInvalidate("请选择精确Profile修订、行业/GN快照和D1事件窗后再读取。");
    return;
  }
  const request = ++state.sectorChange.request;
  const payload = sectorCurrentRequest();
  const identity = [
    payload.target_session,
    payload.package_sha256,
    payload.profile_revision,
    payload.industry_snapshot_id,
    payload.concept_snapshot_id,
    payload.event_window_start,
    payload.pg_history_session_count,
  ].join("|");
  $("sectorChangeQuery").disabled = true;
  $("sectorChangeStatus").textContent =
    "正在完整读取PG行业/GN成员、来源强弱和D1历史。首次读取可能较慢；不会抓取行情或写入数据库。";
  try {
    const data = await api("/api/preview/sector-change-stocks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const currentVersion = sectorSelectedVersion();
    const currentIdentity = [
      currentVersion?.target_session,
      currentVersion?.package_sha256,
      $("sectorProfileRevisionSelect").value,
      $("sectorIndustrySnapshotSelect").value,
      $("sectorConceptSnapshotSelect").value,
      $("sectorEventWindowSelect").value.split("|")[0],
      $("sectorEventWindowSelect").value.split("|")[2],
    ].join("|");
    if (request !== state.sectorChange.request || currentIdentity !== identity)
      return;
    renderSectorChange(data);
  } catch (error) {
    if (request !== state.sectorChange.request) return;
    sectorClearOutput(`变化研究读取失败：${error.message}`);
    $("sectorChangeCount").textContent = "读取失败";
  } finally {
    if (request === state.sectorChange.request)
      $("sectorChangeQuery").disabled = !sectorInputsReady();
  }
}

async function openSectorStock(symbol, row, targetSession) {
  const version = sectorSelectedVersion();
  const chosenVersion = state.versions.find(
    (item) =>
      item.session_date === version.target_session &&
      item.package_sha256 === version.package_sha256,
  );
  if (!chosenVersion) {
    $("sectorChangeStatus").textContent =
      "精确来源版本已不在当前PG目录；未回退到其他日期。";
    return;
  }
  state.sectorChange.returnTarget = targetSession;
  $("versionSelect").value =
    `${version.target_session}|${version.package_sha256}`;
  state.version = {
    target_session: version.target_session,
    package_sha256: version.package_sha256,
  };
  $("symbolInput").value = symbol;
  $("windowInput").value = "75";
  $("sectorReturnToBoard").hidden = false;
  const succeeded = await queryStock({ preventDefault() {} });
  if (!succeeded) return;
  const profileSnapshot = sectorExactProfile(version).find(
    (item) => item.profile_revision === $("sectorProfileRevisionSelect").value,
  );
  await queryTimeline({
    namespace_id: state.namespace_id,
    origin: "sector-change",
    persistedProfileSnapshot: profileSnapshot
      ? {
          target_session: profileSnapshot.target_session,
          namespace_id: profileSnapshot.namespace_id || state.namespace_id,
          package_sha256: profileSnapshot.package_sha256,
          profile_revision: profileSnapshot.profile_revision,
        }
      : null,
  });
  $("stockResult").scrollIntoView({ block: "start" });
}

function returnToSectorBoard() {
  if (!state.sectorChange.data) {
    $("sectorChangeStatus").textContent =
      "原板块结果已失效；请按当前身份重新读取。";
    $("sectorChangeExplorer").scrollIntoView({ block: "start" });
    return;
  }
  $("sectorReturnToBoard").hidden = true;
  $("sectorChangeExplorer").scrollIntoView({ block: "start" });
}

function sectorChangeFilterChanged() {
  state.sectorChange.boardOffset = 0;
  state.sectorChange.stockOffset = 0;
  state.sectorChange.selectedBoard = null;
  state.sectorChange.memberOffset = 0;
  loadSectorChangeView();
}

let sectorChangeExplorerInitialized = false;
function initializeSectorChangeExplorer() {
  const select = $("sectorVersionSelect");
  select.replaceChildren(new Option("— 请选择真实PG目标日期与来源包 —", ""));
  for (const version of state.versions) {
    const option = new Option(
      `${version.session_date} · ${version.package_sha256.slice(0, 12)}… · ${version.version_status} · Raw ${version.row_count}/${version.expected_row_count}`,
      `${version.session_date}|${version.package_sha256}`,
    );
    select.append(option);
  }
  select.disabled = !state.versions.length;
  if (!sectorChangeExplorerInitialized) {
    select.addEventListener("change", loadSectorChangeContext);
    for (const id of [
      "sectorProfileRevisionSelect",
      "sectorIndustrySnapshotSelect",
      "sectorConceptSnapshotSelect",
      "sectorEventWindowSelect",
    ])
      $(id).addEventListener("change", () => sectorInvalidate());
    $("sectorChangeQuery").addEventListener("click", loadSectorChangeView);
    $("sectorBoardKindFilter").addEventListener(
      "change",
      sectorChangeFilterChanged,
    );
    $("sectorSortBy").addEventListener("change", sectorChangeFilterChanged);
    $("sectorDirection").addEventListener("change", sectorChangeFilterChanged);
    $("sectorChangedOnly").addEventListener(
      "change",
      sectorChangeFilterChanged,
    );
    $("sectorBoardPrev").addEventListener("click", () => {
      state.sectorChange.boardOffset = Math.max(
        0,
        state.sectorChange.boardOffset - 15,
      );
      loadSectorChangeView();
    });
    $("sectorBoardNext").addEventListener("click", () => {
      state.sectorChange.boardOffset += 15;
      loadSectorChangeView();
    });
    $("sectorStockPrev").addEventListener("click", () => {
      state.sectorChange.stockOffset = Math.max(
        0,
        state.sectorChange.stockOffset - 30,
      );
      loadSectorChangeView();
    });
    $("sectorStockNext").addEventListener("click", () => {
      state.sectorChange.stockOffset += 30;
      loadSectorChangeView();
    });
    $("sectorMemberState").addEventListener("change", () => {
      state.sectorChange.memberOffset = 0;
      loadSectorChangeView();
    });
    $("sectorMemberSearchButton").addEventListener("click", () => {
      state.sectorChange.memberOffset = 0;
      loadSectorChangeView();
    });
    $("sectorMemberSearch").addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        state.sectorChange.memberOffset = 0;
        loadSectorChangeView();
      }
    });
    $("sectorMemberPrev").addEventListener("click", () => {
      state.sectorChange.memberOffset = Math.max(
        0,
        state.sectorChange.memberOffset - 30,
      );
      loadSectorChangeView();
    });
    $("sectorMemberNext").addEventListener("click", () => {
      state.sectorChange.memberOffset += 30;
      loadSectorChangeView();
    });
    $("sectorReturnToBoard").addEventListener("click", returnToSectorBoard);
    sectorChangeExplorerInitialized = true;
  }
  $("sectorChangeCount").textContent = state.versions.length
    ? `${state.versions.length}个PG来源版本`
    : "PG来源版本为空";
}
