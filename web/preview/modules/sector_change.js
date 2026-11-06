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
let sectorObservationMode = "D1";
const sectorD1ViewSettings = {
  sortBy: "changed_stocks",
  direction: "STRONGER",
  changedOnly: true,
};

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
  sector.singleD0Available = false;
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
  $("sectorSymbolObservation").hidden = true;
  $("sectorReturnToBoard").hidden = true;
  $("sectorBoardRows").replaceChildren();
  $("sectorChangedStockRows").replaceChildren();
  $("sectorMemberRows").replaceChildren();
  $("sectorChangeCount").textContent = "等待精确来源身份";
  $("sectorChangeQuery").disabled = !sectorInputsReady();
}

function sectorSetObservationMode() {
  const nextMode = $("sectorObservationMode").value;
  if (nextMode === sectorObservationMode) return;
  if (sectorObservationMode === "D1") {
    sectorD1ViewSettings.sortBy = $("sectorSortBy").value;
    sectorD1ViewSettings.direction = $("sectorDirection").value;
    sectorD1ViewSettings.changedOnly = $("sectorChangedOnly").checked;
  }
  sectorObservationMode = nextMode;
  const isD0 = sectorObservationMode === "D0";
  $("sectorSortBy").value = isD0
    ? "d0_launch_new"
    : sectorD1ViewSettings.sortBy;
  $("sectorDirection").value = isD0
    ? "STRONGER"
    : sectorD1ViewSettings.direction;
  $("sectorChangedOnly").checked = isD0
    ? false
    : sectorD1ViewSettings.changedOnly;
  $("sectorSortBy").disabled = isD0;
  $("sectorDirection").disabled = isD0;
  $("sectorChangedOnly").disabled = isD0;
  $("sectorMemberStateControl").hidden = isD0;
  $("sectorMemberSearchControl").hidden = false;
  $("sectorBoardHeading").textContent = isD0
    ? "D0新增启动成员板块"
    : "发生D1支撑变化的板块";
  $("sectorBoardDescription").textContent = isD0
    ? "仅显示有FALSE_TO_TRUE新增对象的板块，按新增成员数排序；首页最多15个，完整分母与分页保留。成员快照为当前观测后验NON_PIT，不代表正式选股资格。"
    : "首页最多15个；排序只供研究阅读，不代表预测或优先级。完整板块数和翻页保留，成员关系为后验NON_PIT。";
  $("sectorBoardChangeHeader").textContent = isD0
    ? "D0新增启动成员"
    : "D1变化成员";
  $("sectorChangedOnlyText").textContent = isD0
    ? "仅有D0新增板块"
    : "只看有变化板块";
  $("sectorStockHeading").textContent = isD0
    ? "全局去重的D0新增启动股票"
    : "全局去重的D1变化股票";
  $("sectorStockDescription").textContent = isD0
    ? "按来源市场与代码全局去重；板块成员人数不能相加代替全局人数；每页最多30只。D0结果为诊断观察，不是正式选股。"
    : "同一来源代码即使属于多个GN概念，也只计一次；每页最多30只。";
  $("sectorStockStateHeader").textContent = isD0
    ? "相邻会话状态"
    : "变化事件数";
  $("sectorMemberStateHeader").textContent = isD0 ? "D0新增状态" : "D1状态";
  $("sectorMemberEventsHeader").textContent = isD0 ? "转移" : "事件数";
  $("sectorMemberExplainHeader").textContent = isD0
    ? "来源谓词前后态"
    : "本次状态与原因";
  $("sectorEventWindowLabel").textContent = isD0
    ? "D1历史上下文窗（D0不按此过滤）"
    : "D1事件窗";
  state.sectorChange.boardOffset = 0;
  state.sectorChange.stockOffset = 0;
  state.sectorChange.selectedBoard = null;
  state.sectorChange.memberOffset = 0;
  sectorInvalidate("观察模式已切换；旧板块、成员与个股结果已清除。");
  if (sectorInputsReady()) loadSectorChangeView();
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
  state.sectorChange.context = null;
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
  const isD0 = sectorObservationMode === "D0";
  return {
    observation_mode: sectorObservationMode,
    target_session: version.target_session,
    research_cutoff: state.sectorChange.context.research_cutoff,
    package_sha256: version.package_sha256,
    profile_revision: $("sectorProfileRevisionSelect").value,
    industry_snapshot_id: $("sectorIndustrySnapshotSelect").value,
    concept_snapshot_id: $("sectorConceptSnapshotSelect").value,
    event_window_start: windowParts[0],
    pg_history_session_count: Number(windowParts[2]),
    board_kind: $("sectorBoardKindFilter").value,
    changed_only: isD0 ? false : $("sectorChangedOnly").checked,
    sort_by: isD0 ? "d0_launch_new" : $("sectorSortBy").value,
    direction: isD0 ? "STRONGER" : $("sectorDirection").value,
    offset: state.sectorChange.boardOffset,
    limit: 15,
    detail_board_kind: board?.board_kind || null,
    detail_board_native_id: board?.board_native_id || null,
    member_state: board && !isD0 ? $("sectorMemberState").value : "ALL",
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
  const launch = data.d0_launch_new || {};
  const transitions = launch.transition_counts || {};
  const unknownTransitions = Object.entries(transitions)
    .filter(([transition]) => transition.includes("UNKNOWN"))
    .reduce((total, [, count]) => total + count, 0);
  const isD0 = data.observation_mode === "D0";
  const cards = isD0
    ? [
        [
          "官方相邻会话",
          `${launch.before?.target_session || "—"} → ${launch.after?.target_session || data.target_session}`,
        ],
        ["前后均有来源代码（三态含UNKNOWN）", launch.source_pool_count],
        ["前后来源键并集", launch.source_union_count],
        ["FALSE_TO_TRUE新增", launch.false_to_true_count],
        ["含UNKNOWN转移", unknownTransitions],
        ["行业 / GN完整板块", counts.legal_board_total_count],
        [
          "当前板块映射 / 未映射",
          `${launch.mapped_distinct_source_symbol_count ?? 0} / ${launch.unmapped_distinct_source_symbol_count ?? 0}`,
        ],
      ]
    : [
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
  const isD0 = data.observation_mode === "D0";
  const root = $("sectorBoardRows");
  root.replaceChildren();
  if (!data.boards?.length) {
    tableRow(root, [
      isD0
        ? "当前输入没有可展示的D0新增启动板块（NO_MATCH）；完整来源分母仍在摘要中。"
        : "无符合本次视图条件的变化板块；请检查目标日、事件窗、板块类型与“只看有变化板块”筛选。",
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
      isD0
        ? `${board.d0_launch_new_member_count ?? 0} 个D0新增启动成员`
        : `${board.d1_link?.changed_member_count ?? 0} 变化 / ${stateCounts.UNKNOWN ?? 0} 未知 / ${stateCounts.NOT_COMPARABLE ?? 0} 不可比较`,
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
    `${isD0 ? "有D0新增对象的完整板块数" : "完整板块分母"} ${page.filtered_total ?? 0}；本页 ${page.count ?? 0}/15。` +
    `所有板块先全量联算、排序和过滤后再分页。缺历史或质量不够的指标显示“不可计算”。`;
  $("sectorBoardPage").textContent =
    `偏移 ${page.offset ?? 0} · ${page.filtered_total ?? 0} 个符合筛选的板块`;
  $("sectorBoardPrev").disabled = !page.offset;
  $("sectorBoardNext").disabled =
    page.next_offset === null || page.next_offset === undefined;
  $("sectorBoardSection").hidden = false;
}

function renderSectorStocks(data) {
  const isD0 = data.observation_mode === "D0";
  const launch = data.d0_launch_new || {};
  const page = isD0
    ? {
        stocks: launch.stocks || [],
        total_count: launch.false_to_true_count,
        page: launch.pagination,
      }
    : data.changed_stock_page || {};
  const root = $("sectorChangedStockRows");
  root.replaceChildren();
  for (const stock of page.stocks || []) {
    const row = document.createElement("tr");
    cell(row, stock.source_symbol);
    cell(
      row,
      isD0
        ? `${stock.before?.scenario_raw || "UNKNOWN"} → ${stock.after?.scenario_raw || "UNKNOWN"} · ${stock.transition || "—"}`
        : `${stock.changed_anchor_event_count ?? 0} 次变化 / ${stock.raw_d1_event_count ?? 0} 个事件`,
    );
    const action = document.createElement("td");
    action.append(
      sectorStockButton(stock.source_symbol, stock, data.target_session),
    );
    row.append(action);
    root.append(row);
  }
  if (!page.stocks?.length)
    tableRow(root, [
      isD0
        ? "当前相邻会话没有可展示的FALSE_TO_TRUE新增代码。"
        : "所选事件窗没有可展示的CHANGED来源代码。",
      "",
      "",
    ]);
  $("sectorChangedStocksCount").textContent = isD0
    ? `D0全局去重 ${page.total_count ?? 0} · 前后均有来源代码 ${launch.source_pool_count ?? 0}（含UNKNOWN）`
    : `去重总数 ${page.total_count ?? 0}`;
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
  if (data.observation_mode === "D0") {
    if ($("sectorMemberSearch").value.trim()) {
      const page = detail.member_page || {};
      const root = $("sectorMemberRows");
      root.replaceChildren();
      for (const member of page.members || []) {
        const change = member.d0_change;
        const row = document.createElement("tr");
        cell(row, member.source_symbol);
        cell(row, change?.transition || "来源缺行");
        cell(
          row,
          change?.transition === "FALSE_TO_TRUE" ? "本次新增" : "非本次新增",
        );
        cell(row, sectorD0Description(change));
        const action = document.createElement("td");
        action.append(
          sectorStockButton(member.source_symbol, member, data.target_session),
        );
        row.append(action);
        root.append(row);
      }
      if (!page.members?.length)
        tableRow(root, ["当前板块没有匹配成员", "", "", "", ""]);
      $("sectorBoardDetailSummary").textContent =
        `成员完整分母 ${detail.member_count}；当前成员快照为后验 NON_PIT。`;
      $("sectorMemberSummary").textContent =
        `匹配 ${page.filtered_total ?? 0}；非新增不代表正式选股淘汰。`;
      $("sectorMemberPage").textContent =
        `偏移 ${page.offset ?? 0} · 匹配 ${page.filtered_total ?? 0}`;
      $("sectorMemberPrev").disabled = (page.offset ?? 0) === 0;
      $("sectorMemberNext").disabled = page.next_offset == null;
      return;
    }
    const d0Page = detail.d0_launch_new_member_page || {};
    const page = d0Page.page || {};
    const members = d0Page.members || [];
    const root = $("sectorMemberRows");
    root.replaceChildren();
    for (const member of members) {
      const row = document.createElement("tr");
      cell(row, member.source_symbol);
      cell(row, "新增启动");
      cell(row, member.transition || "FALSE_TO_TRUE");
      const before = member.before?.scenario_raw || "UNKNOWN";
      const after = member.after?.scenario_raw || "UNKNOWN";
      cell(
        row,
        `${before} → ${after} · CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT`,
      );
      const action = document.createElement("td");
      action.append(
        sectorStockButton(member.source_symbol, member, data.target_session),
      );
      row.append(action);
      root.append(row);
    }
    if (!members.length)
      tableRow(root, [
        "该板块本次没有D0新增成员；板块成员完整分母仍保留。",
        "",
        "",
        "",
        "",
      ]);
    $("sectorBoardDetailSummary").textContent =
      `成员完整分母 ${detail.member_count}；D0新增启动成员 ${d0Page.total_count ?? 0}；成员归属来自当前观测快照，历史回看为NON_PIT。`;
    $("sectorMemberSummary").textContent =
      `D0新增成员总数 ${d0Page.total_count ?? 0}；本页 ${page.count ?? 0}/30。` +
      `其余成员不因此构成正式选股未入选；UNKNOWN转移和未映射分母见上方D0来源池摘要。`;
    $("sectorMemberPage").textContent =
      `偏移 ${page.offset ?? 0} · D0新增成员 ${d0Page.total_count ?? 0}`;
    $("sectorMemberPrev").disabled = (page.offset ?? 0) === 0;
    $("sectorMemberNext").disabled =
      page.next_offset === null || page.next_offset === undefined;
    return;
  }
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
  const isD0 = data.observation_mode === "D0";
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
  if (isD0) {
    const launch = data.d0_launch_new || {};
    $("sectorChangeCount").textContent =
      `${launch.false_to_true_count ?? 0} 个D0全局去重新增`;
    $("sectorChangeStatus").textContent =
      `D0相邻官方会话 ${launch.before?.target_session || "—"} → ${launch.after?.target_session || data.target_session}；` +
      `FALSE_TO_TRUE ${launch.false_to_true_count ?? 0}/${launch.source_pool_count ?? 0} 个前后均有来源代码；` +
      `含UNKNOWN转移的来源分母单列在摘要。成员快照为当前观测后验NON_PIT；D0仅为来源诊断，不代表正式资格。API仅读取PG。`;
  } else {
    $("sectorChangeCount").textContent =
      `${data.source_counts?.d1_unique_changed_source_symbol_count ?? 0} 个全局去重变化代码`;
    $("sectorChangeStatus").textContent =
      `D1事件窗 ${discovery.event_window_start || "—"} → ${data.target_session}；Raw历史 ${discovery.execution?.history_session_count ?? sectorCurrentRequest().pg_history_session_count} 个官方会话。` +
      `板块成员快照为当前观测快照，历史归属NON_PIT。所有API与CLI读取PG，不写业务表。`;
  }
}

async function loadSectorChangeView() {
  if (!sectorInputsReady()) {
    if (
      sectorSelectedVersion() &&
      state.sectorChange.context &&
      $("sectorExplainSymbol").value.trim()
    ) {
      await loadSectorSingleD0();
      return;
    }
    sectorInvalidate("请选择精确Profile修订、行业/GN快照和D1事件窗后再读取。");
    return;
  }
  const request = ++state.sectorChange.request;
  const payload = sectorCurrentRequest();
  const requestedSymbol = $("sectorExplainSymbol").value.trim().toUpperCase();
  if (requestedSymbol) payload.explain_symbol = requestedSymbol;
  const identity = [
    payload.observation_mode,
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
      $("sectorObservationMode").value,
      currentVersion?.target_session,
      currentVersion?.package_sha256,
      $("sectorProfileRevisionSelect").value,
      $("sectorIndustrySnapshotSelect").value,
      $("sectorConceptSnapshotSelect").value,
      $("sectorEventWindowSelect").value.split("|")[0],
      $("sectorEventWindowSelect").value.split("|")[2],
    ].join("|");
    if (
      request !== state.sectorChange.request ||
      currentIdentity !== identity ||
      $("sectorExplainSymbol").value.trim().toUpperCase() !== requestedSymbol
    )
      return;
    renderSectorChange(data);
    renderSectorSymbolObservation(data);
  } catch (error) {
    if (request !== state.sectorChange.request) return;
    sectorClearOutput(`变化研究读取失败：${error.message}`);
    $("sectorChangeCount").textContent = "读取失败";
  } finally {
    if (request === state.sectorChange.request)
      $("sectorChangeQuery").disabled = !sectorInputsReady();
  }
}

async function loadSectorSingleD0() {
  const version = sectorSelectedVersion();
  const symbol = $("sectorExplainSymbol").value.trim().toUpperCase();
  const request = ++state.sectorChange.request;
  $("sectorSymbolObservation").hidden = true;
  $("sectorChangeStatus").textContent =
    "板块或画像输入未齐，正在只读查询单股D0；D1和归属不作推断。";
  try {
    const data = await api("/api/preview/stock-timeline", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source_observation_only: true,
        symbol,
        target_session: version.target_session,
        package_sha256: version.package_sha256,
        namespace_id: state.sectorChange.context.namespace_id,
        research_cutoff: state.sectorChange.context.research_cutoff,
      }),
    });
    const current = sectorSelectedVersion();
    if (
      request !== state.sectorChange.request ||
      current?.target_session !== version.target_session ||
      current?.package_sha256 !== version.package_sha256 ||
      $("sectorExplainSymbol").value.trim().toUpperCase() !== symbol
    )
      return;
    const root = $("sectorSymbolObservation");
    root.textContent = `${symbol} · D0 ${sectorD0Description(data.d0_change)}；目标Raw ${data.d0_change?.after ? "存在" : "缺行"}；D1与板块归属 NOT_EVALUATED，正式选股 NOT_EVALUATED。`;
    if (data.d0_change?.after) {
      root.append(document.createTextNode(" "));
      root.append(sectorStockButton(symbol, {}, version.target_session));
    }
    state.sectorChange.singleD0Available = true;
    root.hidden = false;
    $("sectorChangeStatus").textContent =
      "仅单股D0已读取；未指定完整板块/画像输入，榜单和归属未计算。";
  } catch (error) {
    if (request === state.sectorChange.request)
      sectorClearOutput(`单股来源观察失败：${error.message}`);
  }
}

function sectorD0Description(change) {
  if (!change) return "前后来源均无该代码或本次未计算";
  const endpoint = (item) => {
    if (!item) return "来源缺行";
    const reasons = [
      ...new Set(
        Object.values(item.predicates || {})
          .filter((predicate) => ["FALSE", "UNKNOWN"].includes(predicate.state))
          .map(
            (predicate) =>
              `${predicate.state}: ${predicate.reason || predicate.reason_code}`,
          ),
      ),
    ];
    return `${item.scenario_raw}（${reasons.join("；") || item.main_reason || "无"}）`;
  };
  return `${endpoint(change.before)} → ${endpoint(change.after)} · ${change.transition}`;
}

function renderSectorSymbolObservation(data) {
  const root = $("sectorSymbolObservation");
  const item = data.symbol_observation;
  root.replaceChildren();
  root.hidden = !item;
  if (!item) return;
  const d1 = item.d1;
  const transition = item.d0_change?.transition;
  const qualifies =
    data.observation_mode === "D0"
      ? transition === "FALSE_TO_TRUE"
      : d1.change_state === "CHANGED";
  const stocks =
    data.observation_mode === "D0"
      ? data.d0_launch_new?.stocks
      : data.changed_stock_page?.stocks;
  const visible = (stocks || []).some(
    (stock) => stock.source_symbol === item.source_symbol,
  );
  const unevaluable =
    data.observation_mode === "D0"
      ? !transition ||
        transition.includes("UNKNOWN") ||
        transition.endsWith("_ONLY")
      : ["UNKNOWN", "NOT_COMPARABLE", "NOT_EVALUABLE"].includes(
          d1.change_state,
        );
  const viewReason = !item.in_target_raw_pool
    ? "目标Raw来源缺行"
    : qualifies
      ? visible
        ? "位于当前个股页"
        : "符合本次变化条件；当前页未显示，请检查分页"
      : unevaluable
        ? `状态 ${data.observation_mode === "D0" ? transition || "来源缺行" : d1.change_state}，未能判为本次变化，未列入变化榜`
        : `状态 ${data.observation_mode === "D0" ? transition : d1.change_state} 不符合本次变化榜条件`;
  const events =
    (d1.anchor_events || [])
      .map(
        (event) =>
          `${event.session}: ${event.change_state} ${event.unknown_reason || ""}`,
      )
      .join("；") || "事件窗内无锚事件";
  const boards =
    item.memberships
      .map((board) => `${sectorLabel(board.board_kind)} ${board.board_name}`)
      .join("、") || item.membership_status;
  const selectedBoard = state.sectorChange.selectedBoard;
  let boardReason = "未选择板块";
  if (selectedBoard) {
    const isMember = item.memberships.some(
      (board) =>
        board.board_kind === selectedBoard.board_kind &&
        board.board_native_id === selectedBoard.board_native_id,
    );
    const search = $("sectorMemberSearch").value.trim().toUpperCase();
    const memberState = $("sectorMemberState").value;
    const detail = data.detail_board || {};
    const memberPage =
      data.observation_mode === "D0" && !search
        ? detail.d0_launch_new_member_page?.members
        : detail.member_page?.members;
    if (!isMember) boardReason = "不属于当前所选板块";
    else if (search && !item.source_symbol.includes(search))
      boardReason = "被当前板内代码筛选排除";
    else if (
      data.observation_mode === "D1" &&
      memberState !== "ALL" &&
      memberState !== d1.change_state
    )
      boardReason = "被当前板内D1状态筛选排除";
    else if (data.observation_mode === "D0" && !search && !qualifies)
      boardReason = "默认板内表只显示本次D0新增，当前状态未列入";
    else if (
      (memberPage || []).some(
        (member) => member.source_symbol === item.source_symbol,
      )
    )
      boardReason = "位于当前板内页";
    else boardReason = "符合当前板内筛选但不在本页，受分页截取";
  }
  const missing = (item.missing_membership_kinds || [])
    .map(sectorLabel)
    .join("、");
  root.textContent = `${item.source_symbol} · 目标Raw ${item.in_target_raw_pool ? "存在" : "不存在"}；D0 ${sectorD0Description(item.d0_change)}；D1 ${d1.change_state}：${events} ${(d1.unknown_reasons || []).join("、")}；当前行业/GN归属（后验 NON_PIT）：${boards}${missing ? `；未匹配类型：${missing}` : ""}；${boardReason}；${viewReason}。正式选股 NOT_EVALUATED。`;
  if (item.in_target_raw_pool) {
    root.append(document.createTextNode(" "));
    root.append(
      sectorStockButton(item.source_symbol, item, data.target_session),
    );
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
  if (!state.sectorChange.data && !state.sectorChange.singleD0Available) {
    $("sectorChangeStatus").textContent =
      "原板块结果已失效；请按当前身份重新读取。";
    $("sectorChangeExplorer").scrollIntoView({ block: "start" });
    return;
  }
  $("sectorReturnToBoard").hidden = true;
  $("sectorChangeExplorer").scrollIntoView({ block: "start" });
}

function sectorChangeFilterChanged() {
  if (sectorObservationMode === "D1") {
    sectorD1ViewSettings.sortBy = $("sectorSortBy").value;
    sectorD1ViewSettings.direction = $("sectorDirection").value;
    sectorD1ViewSettings.changedOnly = $("sectorChangedOnly").checked;
  }
  state.sectorChange.boardOffset = 0;
  state.sectorChange.stockOffset = 0;
  state.sectorChange.selectedBoard = null;
  state.sectorChange.memberOffset = 0;
  loadSectorChangeView();
}

let sectorChangeExplorerInitialized = false;
function initializeSectorChangeExplorer() {
  state.sectorChange.contextRequest++;
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
  for (const [id, placeholder] of [
    ["sectorProfileRevisionSelect", "— 请选择完整修订 —"],
    ["sectorIndustrySnapshotSelect", "— 请选择PG行业快照 —"],
    ["sectorConceptSnapshotSelect", "— 请选择PG GN快照 —"],
    ["sectorEventWindowSelect", "— 先选择目标日 —"],
  ]) {
    const dependent = $(id);
    dependent.replaceChildren(new Option(placeholder, ""));
    dependent.disabled = true;
  }
  sectorInvalidate(
    "PG目录已读取；请选择精确身份后重新读取。旧板块结果已失效。",
  );
  if (!sectorChangeExplorerInitialized) {
    select.addEventListener("change", loadSectorChangeContext);
    $("sectorObservationMode").addEventListener(
      "change",
      sectorSetObservationMode,
    );
    for (const id of [
      "sectorProfileRevisionSelect",
      "sectorIndustrySnapshotSelect",
      "sectorConceptSnapshotSelect",
      "sectorEventWindowSelect",
    ])
      $(id).addEventListener("change", () => sectorInvalidate());
    $("sectorChangeQuery").addEventListener("click", loadSectorChangeView);
    $("sectorExplainButton").addEventListener("click", loadSectorChangeView);
    $("sectorExplainSymbol").addEventListener("input", () => {
      state.sectorChange.request++;
      $("sectorSymbolObservation").hidden = true;
    });
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
