"use strict";
const $ = (id) => document.getElementById(id);
const state = {
  versions: [],
  profiles: [],
  context: null,
  selected: null,
  data: null,
  board: null,
  boardOffset: 0,
  stockOffset: 0,
  memberOffset: 0,
  request: 0,
  stockRequest: 0,
  profileChangeRequest: 0,
  researchReady: null,
  researchController: null,
  stockController: null,
  profileChangeController: null,
};
const names = {
  overview: ["今日总览", "从真实变化出发，了解市场中正在发生什么。", "01"],
  sectors: ["板块研究", "从行业与 GN 概念进入成员与个股。", "02"],
  stocks: ["个股研究", "查看来源画像、价格结构和规则解释。", "03"],
  focus: ["关注跟踪", "读取来源级冻结规则与后继观察。", "04"],
  market: ["市场研究", "查看来源池宽度和覆盖。", "05"],
  diagnostics: ["数据与诊断", "查看所选来源与质量。", "06"],
};
function node(tag, text, cls) {
  const n = document.createElement(tag);
  if (text !== undefined && text !== null) n.textContent = String(text);
  if (cls) n.className = cls;
  return n;
}
function message(id, text) {
  $(id).textContent = text;
}
function format(value) {
  if (value === undefined || value === null) return "—";
  if (
    typeof value === "object" &&
    typeof value.decimal === "string" &&
    value.decimal.length > 12 &&
    Number.isFinite(Number(value.decimal))
  )
    return `≈${Number(value.decimal).toPrecision(6)}`;
  if (typeof value === "object")
    return (
      value.decimal ??
      value.exact ??
      (value.numerator !== undefined
        ? `${value.numerator}/${value.denominator}`
        : JSON.stringify(value))
    );
  return String(value);
}
let lastApiError = null;
let activeRequests = 0;
const requestWaiters = [];
function abortError() {
  return new DOMException("请求已取消。", "AbortError");
}
function acquireRequest(signal) {
  if (signal?.aborted) return Promise.reject(abortError());
  if (activeRequests < 3) {
    activeRequests++;
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    const waiter = { resolve, reject, signal, onAbort: null };
    waiter.onAbort = () => {
      const index = requestWaiters.indexOf(waiter);
      if (index >= 0) requestWaiters.splice(index, 1);
      reject(abortError());
    };
    signal?.addEventListener("abort", waiter.onAbort, { once: true });
    requestWaiters.push(waiter);
  });
}
function releaseRequest() {
  while (requestWaiters.length) {
    const waiter = requestWaiters.shift();
    waiter.signal?.removeEventListener("abort", waiter.onAbort);
    if (waiter.signal?.aborted) {
      waiter.reject(abortError());
      continue;
    }
    waiter.resolve();
    return;
  }
  activeRequests--;
}
async function api(path, body, signal) {
  await acquireRequest(signal);
  try {
    const response = await fetch(
      path,
      body === undefined
        ? { signal }
        : {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
            signal,
          },
    );
    const result = await response.json();
    if (!response.ok || result.ok === false)
      throw new Error(
        `${result.code || response.status}：${result.message || "请求失败"}`,
      );
    return result;
  } catch (error) {
    if (error.name !== "AbortError")
      lastApiError = {
        time: new Date().toISOString(),
        route: path.split("?")[0],
        reason: error.message,
      };
    throw error;
  } finally {
    releaseRequest();
  }
}
let activePage = "overview";
let extraRequest = 0;
let cohortRequest = 0;
let focusStockRequest = 0;
let focusKeysLoading = false;
let cohortOffset = 0;
let currentStockSymbol = null;
let currentStockSource = null;
function gotoPage(page) {
  activePage = page;
  const extra = ["focus", "market", "diagnostics"].includes(page);
  $("extraContext").hidden = !extra;
  $("refreshResearch").hidden = extra;
  $("contextNotice").hidden = extra;
  ++extraRequest;
  ++cohortRequest;
  ++focusStockRequest;
  document
    .querySelectorAll(".page")
    .forEach((n) => n.classList.toggle("active", n.id === page));
  document
    .querySelectorAll(".nav-item")
    .forEach((n) => n.classList.toggle("active", n.dataset.page === page));
  const data = names[page];
  message("breadcrumb", data[0]);
  message("pageTitle", data[0]);
  message("pageSubtitle", data[1]);
  message("headingIndex", data[2]);
  if (["focus", "market", "diagnostics"].includes(page)) void loadExtra(page);
}
document
  .querySelectorAll("[data-page]")
  .forEach((n) => n.addEventListener("click", () => gotoPage(n.dataset.page)));
document
  .querySelectorAll("[data-goto]")
  .forEach((n) => n.addEventListener("click", () => gotoPage(n.dataset.goto)));
document
  .querySelectorAll("[data-close]")
  .forEach((n) =>
    n.addEventListener("click", () => $(n.dataset.close).close()),
  );
$("openContext").addEventListener("click", () =>
  $("contextDialog").showModal(),
);
$("configureContext").addEventListener("click", () =>
  $("contextDialog").showModal(),
);
function resetSelect(id, text) {
  const s = $(id);
  s.replaceChildren(new Option(text, ""));
  s.disabled = true;
}
function addChoices(id, items, label, value) {
  const s = $(id);
  for (const item of items) s.add(new Option(label(item), value(item)));
  s.disabled = !items.length;
}
async function init() {
  try {
    const [catalog, profiles] = await Promise.all([
      api("/api/preview/catalog?offset=0&limit=100"),
      api("/api/source-symbol-price-profiles/snapshots"),
    ]);
    state.versions = catalog.versions || [];
    state.profiles = profiles.snapshots || [];
    resetSelect("extraVersion", "请选择来源日期与版本");
    addChoices(
      "extraVersion",
      state.versions,
      (v) => `${v.session_date} · ${v.package_sha256.slice(0, 12)}…`,
      (v) => `${v.session_date}|${v.package_sha256}`,
    );
    resetSelect("versionSelect", "请选择真实交易日与来源包");
    addChoices(
      "versionSelect",
      state.versions,
      (v) =>
        `${v.session_date} · ${v.package_sha256.slice(0, 12)}… · ${v.row_count}/${v.expected_row_count}`,
      (v) => `${v.session_date}|${v.package_sha256}`,
    );
    message("contextStatus", `${state.versions.length} 个来源日期版本`);
  } catch (error) {
    message("contextStatus", `目录读取失败：${error.message}`);
    message("contextText", `PG 目录不可用：${error.message}`);
  }
}
$("versionSelect").addEventListener("change", async () => {
  const selected = $("versionSelect").value;
  const token = ++state.request;
  state.context = null;
  for (const id of [
    "profileSelect",
    "industrySelect",
    "conceptSelect",
    "windowSelect",
  ])
    resetSelect(id, "正在读取…");
  if (!selected) return;
  const [date, sha] = selected.split("|");
  const matching = state.profiles.filter(
    (p) => p.complete && p.target_session === date && p.package_sha256 === sha,
  );
  resetSelect("profileSelect", "请选择画像修订");
  addChoices(
    "profileSelect",
    matching,
    (p) =>
      `${p.profile_revision.slice(0, 12)}… · ${p.source_symbol_count}/${p.expected_source_symbol_count} · ${p.history_session_count}会话`,
    (p) => p.profile_revision,
  );
  try {
    const c = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: date })}`,
    );
    if (token !== state.request) return;
    state.context = c;
    for (const [id, kind] of [
      ["industrySelect", "TDX_TDXHY_CFG"],
      ["conceptSelect", "TDX_INFOHARBOR_BLOCK_DAT"],
    ]) {
      resetSelect(id, "请选择成员快照");
      addChoices(
        id,
        (c.snapshots || []).filter((s) => s.source_kind === kind),
        (s) => `${s.observed_at} · ${s.member_rows} 条 · NON_PIT`,
        (s) => s.snapshot_id,
      );
    }
    resetSelect("windowSelect", "请选择观察窗口");
    addChoices(
      "windowSelect",
      c.window_options || [],
      (w) =>
        `${w.event_session_count} 个此前会话 · ${w.event_window_start} → ${w.target_session}`,
      (w) => `${w.event_window_start}|${w.pg_history_session_count}`,
    );
    message("contextStatus", `截止 ${c.research_cutoff} · 请明确选择所有版本`);
  } catch (error) {
    if (token === state.request)
      message("contextStatus", `范围读取失败：${error.message}`);
  }
});
$("applyContext").addEventListener("click", () => {
  const [target_session, package_sha256] = $("versionSelect").value.split("|");
  const [event_window_start, history] = $("windowSelect").value.split("|");
  const profile_revision = $("profileSelect").value,
    industry_snapshot_id = $("industrySelect").value,
    concept_snapshot_id = $("conceptSelect").value;
  const profile = state.profiles.find(
    (item) =>
      item.target_session === target_session &&
      item.package_sha256 === package_sha256 &&
      item.profile_revision === profile_revision &&
      item.complete,
  );
  if (
    !state.context ||
    !target_session ||
    !package_sha256 ||
    !profile_revision ||
    !industry_snapshot_id ||
    !concept_snapshot_id ||
    !event_window_start ||
    !history ||
    !profile ||
    !Number.isInteger(profile.history_session_count)
  ) {
    message("contextStatus", "请选择所有必要的真实版本。");
    return;
  }
  state.selected = {
    target_session,
    package_sha256,
    profile_revision,
    industry_snapshot_id,
    concept_snapshot_id,
    event_window_start,
    pg_history_session_count: Number(history),
    stock_history_session_count: profile.history_session_count,
    research_cutoff: state.context.research_cutoff,
    namespace_id: state.context.namespace_id,
    mode: $("modeSelect").value,
  };
  state.board = null;
  state.boardOffset = state.stockOffset = state.memberOffset = 0;
  state.data = null;
  ++state.stockRequest;
  message("currentDate", target_session);
  message(
    "contextText",
    `${target_session} · 来源 ${package_sha256.slice(0, 12)}… · 画像 ${profile_revision.slice(0, 12)}… · 当前成员快照回看 NON_PIT`,
  );
  $("contextDialog").close();
  $("extraVersion").value = `${target_session}|${package_sha256}`;
  loadResearch();
  if (["focus", "market", "diagnostics"].includes(activePage))
    void loadExtra(activePage);
});
function requestBody() {
  const c = state.selected;
  return {
    observation_mode: c.mode,
    target_session: c.target_session,
    research_cutoff: c.research_cutoff,
    package_sha256: c.package_sha256,
    profile_revision: c.profile_revision,
    industry_snapshot_id: c.industry_snapshot_id,
    concept_snapshot_id: c.concept_snapshot_id,
    event_window_start: c.event_window_start,
    pg_history_session_count: c.pg_history_session_count,
    board_kind: $("boardKind").value,
    board_search: $("boardSearch").value.trim() || null,
    changed_only: c.mode === "D0" ? false : $("changedOnly").checked,
    sort_by: c.mode === "D0" ? "d0_launch_new" : "changed_stocks",
    direction: "STRONGER",
    offset: state.boardOffset,
    limit: 15,
    detail_board_kind: state.board?.board_kind || null,
    detail_board_native_id: state.board?.board_native_id || null,
    member_state: state.board ? $("memberState").value : "ALL",
    member_search: state.board ? $("memberSearch").value.trim() || null : null,
    member_offset: state.board ? state.memberOffset : 0,
    member_limit: state.board ? 30 : 0,
    include_changed_stock_page: true,
    stock_offset: state.stockOffset,
    stock_limit: 30,
  };
}
async function loadResearch() {
  if (!state.selected) {
    $("contextDialog").showModal();
    return;
  }
  const token = ++state.request;
  state.researchController?.abort();
  ++state.stockRequest;
  state.stockController?.abort();
  const controller = new AbortController();
  state.researchController = controller;
  void loadProfileChanges();
  state.data = null;
  for (const id of ["overviewBoards", "overviewStocks", "stockList"]) {
    $(id).classList.remove("has-items");
    message(id, "正在读取当前研究范围，旧结果已清除。");
  }
  $("metrics").replaceChildren(
    countCard("来源代码池", "—", "正在读取"),
    countCard("变化代码", "—", "正在读取"),
    countCard("变化事件", "—", "正在读取"),
    countCard("行业 / GN 板块", "—", "正在读取"),
  );
  const pendingRow = node("tr");
  const pendingCell = node(
    "td",
    "正在读取当前研究范围，旧结果已清除。",
    "empty",
  );
  pendingCell.colSpan = 6;
  pendingRow.append(pendingCell);
  $("boardRows").replaceChildren(pendingRow);
  message("boardSummary", "正在读取当前研究范围…");
  message("boardPage", "—");
  message("stockPage", "—");
  for (const id of ["boardPrev", "boardNext", "stockPrev", "stockNext"])
    $(id).disabled = true;
  if (state.board) {
    $("memberList").classList.remove("has-items");
    message("memberList", "正在读取当前成员页，旧结果已清除。");
    message("memberPage", "—");
    $("memberPrev").disabled = $("memberNext").disabled = true;
  }
  $("refreshResearch").disabled = true;
  message("observationLabel", "正在读取 PG 研究结果…");
  try {
    const pending = api(
      "/api/preview/sector-change-stocks",
      requestBody(),
      controller.signal,
    );
    state.researchReady = pending;
    const data = await pending;
    if (token !== state.request) return;
    state.data = data;
    renderResearch(data);
  } catch (error) {
    if (token === state.request) {
      if (error.name === "AbortError") return;
      state.data = null;
      message("observationLabel", `读取失败：${error.message}`);
      message("overviewBoards", `研究读取失败：${error.message}`);
      message("overviewStocks", "未取得结果，不能推断无变化。");
      $("overviewBoards").classList.remove("has-items");
      $("overviewStocks").classList.remove("has-items");
    }
  } finally {
    if (token === state.request) $("refreshResearch").disabled = false;
  }
}
$("refreshResearch").addEventListener("click", loadResearch);
function countCard(label, value, detail) {
  const a = node("article");
  a.append(
    node("span", label),
    node("strong", format(value)),
    node("small", detail),
  );
  return a;
}
function rowButton(title, subtitle, value, subvalue, action, index) {
  const b = node("button", null, "list-row");
  b.type = "button";
  const main = node("span", null, "row-main");
  main.append(node("strong", title), node("small", subtitle));
  const right = node("span", null, "row-value");
  right.append(node("span", value), node("small", subvalue));
  if (index !== undefined)
    b.append(node("span", String(index + 1).padStart(2, "0"), "row-rank"));
  b.append(main, right, node("span", "›", "row-arrow"));
  b.addEventListener("click", action);
  return b;
}
function fillList(id, items, empty, make) {
  const root = $(id);
  root.replaceChildren();
  root.classList.toggle("has-items", !!items.length);
  if (!items.length) {
    root.textContent = empty;
    return;
  }
  items.forEach((item, i) => root.append(make(item, i)));
}
function boardCount(board) {
  return state.selected.mode === "D0"
    ? (board.d0_launch_new?.member_count ??
        board.d0_launch_new_member_count ??
        "—")
    : (board.d1_link?.changed_member_count ?? "—");
}
function boardRow(board, i) {
  return rowButton(
    board.board_name,
    `${board.board_kind === "INDUSTRY" ? "行业" : "GN 概念"} · 成员 ${board.source_member_total}`,
    format(boardCount(board)),
    "变化成员",
    () => openBoard(board),
    i,
  );
}
function stockRows(data) {
  return state.selected.mode === "D0"
    ? data.d0_launch_new?.stocks || []
    : data.changed_stock_page?.stocks || [];
}
function stockRow(stock, i) {
  const code = stock.source_symbol;
  const changes =
    stock.changed_anchor_event_count ?? stock.transition ?? "来源变化";
  return rowButton(
    code,
    "来源代码 · RAW_UNADJUSTED",
    format(changes),
    state.selected.mode === "D1" ? "支撑变化" : "相邻状态",
    () => openStock(code, stock),
    i,
  );
}

function profileChangeTypeLabel(type) {
  return (
    {
      PROFILE_STATE_CHANGED: "画像状态变化",
      RAW_CLOSE_VS_MA20_CHANGED: "收盘与 MA20 关系变化",
      PRICE_DAMAGE_STATE_CHANGED: "价格损伤状态变化",
      MA_SLOPE20_ATR_STATE_CHANGED: "MA20 斜率状态变化",
      PROFILE_QUALITY_CHANGED: "画像质量变化",
      CORE_QUALITY_CHANGED: "核心字段质量变化",
    }[type] ||
    type ||
    "画像字段变化"
  );
}

async function loadProfileChanges() {
  state.profileChangeController?.abort();
  const controller = new AbortController();
  state.profileChangeController = controller;
  const token = ++state.profileChangeRequest;
  const context = state.selected;
  const list = $("profileChangeList");
  list.classList.remove("has-items");
  message("profileChangeSummary", "读取独立画像变化…");
  message("profileChangeList", "正在读取相邻会话画像变化；不计入 D1 统计。");
  if (!context) return;
  const previous = (state.context?.window_options || []).find(
    (item) => item.event_session_count === 1,
  );
  if (!previous || previous.event_window_start >= context.target_session) {
    message("profileChangeSummary", "相邻官方会话不可用");
    message("profileChangeList", "当前范围没有可用的此前官方会话。");
    return;
  }
  const sameIdentity = (item, session) =>
    item.complete &&
    item.target_session === session &&
    item.package_sha256 === context.package_sha256 &&
    item.profile_revision === context.profile_revision;
  const beforeProfiles = state.profiles.filter((item) =>
    sameIdentity(item, previous.event_window_start),
  );
  const afterProfiles = state.profiles.filter((item) =>
    sameIdentity(item, context.target_session),
  );
  if (beforeProfiles.length !== 1 || afterProfiles.length !== 1) {
    message("profileChangeSummary", "精确修订不可配对");
    message(
      "profileChangeList",
      "所选来源包与画像修订在相邻会话中没有唯一完整配对；本项未回退到其他修订。",
    );
    return;
  }
  const query = new URLSearchParams({
    before_session: previous.event_window_start,
    after_session: context.target_session,
    before_package_sha256: context.package_sha256,
    before_profile_revision: context.profile_revision,
    after_package_sha256: context.package_sha256,
    after_profile_revision: context.profile_revision,
    offset: "0",
    limit: "15",
  });
  try {
    const result = await api(
      `/api/source-symbol-price-profile-changes?${query}`,
      undefined,
      controller.signal,
    );
    if (token !== state.profileChangeRequest || context !== state.selected)
      return;
    const rows = result.rows || [];
    const changed =
      result.summary?.changed_source_symbol_count ?? result.page?.total ?? "—";
    const denominator =
      result.core_transitions?.source_symbol_union_count ?? "—";
    message(
      "profileChangeSummary",
      `${format(changed)} / ${format(denominator)} 个来源代码有画像变化 · 展示 ${rows.length} 条`,
    );
    fillList(
      "profileChangeList",
      rows,
      "相邻会话没有可展示的画像变化；不代表 D1 结果。",
      (item, index) => {
        const types = (item.change_types || []).map(profileChangeTypeLabel);
        return rowButton(
          item.source_symbol,
          `${previous.event_window_start} → ${context.target_session} · ${types.join("、") || "状态不可比较"} · RAW_UNADJUSTED · NON_PIT`,
          (item.change_types || []).length,
          "项变化",
          () =>
            openStock(item.source_symbol, { change_state: "NOT_EVALUATED" }),
          index,
        );
      },
    );
  } catch (error) {
    if (token !== state.profileChangeRequest) return;
    if (error.name === "AbortError") return;
    message("profileChangeSummary", "画像变化读取失败");
    message("profileChangeList", `画像变化不可用：${error.message}`);
  }
}

function renderResearch(data) {
  const c = data.source_counts || {},
    d = data.d1_discovery || {},
    events = d.support_event_counts || c.d1_support_event_counts || {},
    launch = data.d0_launch_new || {};
  const isD0 = state.selected.mode === "D0";
  const d0Unknown = launch.transition_counts
    ? Object.entries(launch.transition_counts)
        .filter(([key]) => key.includes("UNKNOWN"))
        .reduce((sum, [, value]) => sum + value, 0)
    : "未知";
  $("metrics").replaceChildren(
    countCard(
      "来源代码池",
      isD0 ? launch.source_pool_count : c.d1_source_pool_count,
      "来源池分母，不是官方上市总体",
    ),
    countCard(
      "变化代码",
      isD0
        ? launch.false_to_true_count
        : c.d1_unique_changed_source_symbol_count,
      "按来源代码去重",
    ),
    countCard(
      "变化事件",
      isD0 ? launch.false_to_true_count : events.CHANGED,
      isD0 ? "FALSE → TRUE" : "D1 支撑状态变化",
    ),
    countCard(
      "行业 / GN 板块",
      c.legal_board_total_count,
      "当前成员快照 · NON_PIT",
    ),
  );
  message(
    "observationLabel",
    isD0
      ? `${launch.before?.target_session || "—"} → ${state.selected.target_session} · D0 来源新增 · 含未知转移 ${d0Unknown}，单端缺行按来源集合另计`
      : `${state.selected.event_window_start} → ${state.selected.target_session} · D1 支撑变化 · 未知 ${events.UNKNOWN ?? "—"} / 不可比较 ${events.NOT_COMPARABLE ?? "—"}`,
  );
  const boards = data.boards || [],
    stocks = stockRows(data);
  fillList(
    "overviewBoards",
    boards.filter((board) => Number(boardCount(board)) > 0),
    "当前条件没有可展示的变化板块；未知与不可比较不等于无变化。",
    boardRow,
  );
  fillList(
    "overviewStocks",
    stocks,
    "当前条件没有可展示的变化代码；请检查观察窗口和质量。",
    stockRow,
  );
  fillList("stockList", stocks, "当前条件没有可展示的变化代码。", stockRow);
  const rows = $("boardRows");
  rows.replaceChildren();
  if (!boards.length) {
    const tr = node("tr"),
      td = node("td", "当前条件没有可展示的板块。", "empty");
    td.colSpan = 6;
    tr.append(td);
    rows.append(tr);
  }
  boards.forEach((board) => {
    const tr = node("tr");
    const title = node("td");
    title.append(node("strong", board.board_name));
    tr.append(
      title,
      node("td", board.board_kind === "INDUSTRY" ? "行业" : "GN 概念"),
      node("td", format(board.source_member_total)),
      node(
        "td",
        `${format(board.target_core_profile_mapped_member_count)} / ${format(board.target_core_profile_unmapped_member_count)}`,
      ),
      node("td", format(boardCount(board))),
    );
    const td = node("td");
    const button = node("button", "查看成员 →", "text-button");
    button.addEventListener("click", () => openBoard(board));
    td.append(button);
    tr.append(td);
    rows.append(tr);
  });
  const page = data.page || {};
  message(
    "boardSummary",
    `符合筛选 ${format(page.filtered_total)} 个 · 完整板块 ${format(c.legal_board_total_count)} 个`,
  );
  message(
    "boardPage",
    `偏移 ${state.boardOffset} · ${format(page.filtered_total)} 个符合筛选`,
  );
  $("boardPrev").disabled = state.boardOffset === 0;
  $("boardNext").disabled = page.next_offset == null;
  const sp = isD0
    ? launch.pagination || {}
    : data.changed_stock_page?.page || {};
  message(
    "stockPage",
    `偏移 ${state.stockOffset} · 总数 ${format(isD0 ? launch.false_to_true_count : data.changed_stock_page?.total_count)}`,
  );
  $("stockPrev").disabled = state.stockOffset === 0;
  $("stockNext").disabled = sp.next_offset == null;
  if (state.board && data.detail_board) renderMembers(data.detail_board);
}
function openBoard(board) {
  state.board = {
    board_kind: board.board_kind,
    board_native_id: board.board_native_id,
    board_name: board.board_name,
  };
  state.memberOffset = 0;
  $("memberSearch").value = "";
  $("memberState").value = "ALL";
  message("boardTitle", board.board_name);
  message(
    "boardMeta",
    `${board.board_kind === "INDUSTRY" ? "行业" : "GN 概念"} · 当前成员快照 · NON_PIT`,
  );
  message(
    "boardCoverage",
    `成员 ${format(board.source_member_total)} · 当前来源池映射 ${format(board.target_core_profile_mapped_member_count)} · 未映射 ${format(board.target_core_profile_unmapped_member_count)}`,
  );
  message("memberList", "正在读取成员…");
  $("memberList").classList.remove("has-items");
  if (!$("boardDialog").open) $("boardDialog").showModal();
  loadResearch();
}
function renderMembers(detail) {
  const page = detail.member_page || {};
  const members = page.members || [];
  message("boardTitle", detail.board_name);
  const counts = detail.d1_link?.member_state_counts || {};
  message(
    "boardMeta",
    `${detail.board_kind === "INDUSTRY" ? "行业" : "GN 概念"} · 当前成员快照 NON_PIT · D1变化 ${counts.CHANGED ?? "—"} / 未知 ${counts.UNKNOWN ?? "—"} / 不可比较 ${counts.NOT_COMPARABLE ?? "—"}`,
  );
  fillList("memberList", members, "当前筛选没有成员。", (m) =>
    rowButton(
      m.source_symbol,
      `D1 ${m.change_state || "NOT_EVALUATED"} ${(m.unknown_reasons || []).join("、")} ${state.selected.mode === "D0" ? `· D0 ${m.d0_change?.transition || "来源缺行"}` : ""}`,
      format(
        m.change_state === "NOT_EVALUABLE"
          ? null
          : (m.anchor_events?.length ?? m.raw_d1_event_count ?? null),
      ),
      "锚事件",
      () => openStock(m.source_symbol, m),
    ),
  );
  message(
    "memberPage",
    `偏移 ${page.offset ?? 0} · 筛选 ${format(page.filtered_total)} / 成员 ${format(detail.member_count)}`,
  );
  $("memberPrev").disabled = !page.offset;
  $("memberNext").disabled = page.next_offset == null;
}
$("boardPrev").addEventListener("click", () => {
  state.boardOffset = Math.max(0, state.boardOffset - 15);
  loadResearch();
});
$("boardNext").addEventListener("click", () => {
  state.boardOffset += 15;
  loadResearch();
});
$("stockPrev").addEventListener("click", () => {
  state.stockOffset = Math.max(0, state.stockOffset - 30);
  loadResearch();
});
$("stockNext").addEventListener("click", () => {
  state.stockOffset += 30;
  loadResearch();
});
$("memberPrev").addEventListener("click", () => {
  state.memberOffset = Math.max(0, state.memberOffset - 30);
  loadResearch();
});
$("memberNext").addEventListener("click", () => {
  state.memberOffset += 30;
  loadResearch();
});
$("searchMembers").addEventListener("click", () => {
  state.memberOffset = 0;
  loadResearch();
});
$("memberState").addEventListener("change", () => {
  state.memberOffset = 0;
  loadResearch();
});
$("memberSearch").addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    e.preventDefault();
    state.memberOffset = 0;
    loadResearch();
  }
});
$("boardDialog").addEventListener("close", () => {
  state.board = null;
  state.memberOffset = 0;
});
for (const id of ["boardKind", "changedOnly"])
  $(id).addEventListener("change", () => {
    state.boardOffset = 0;
    state.board = null;
    loadResearch();
  });
$("searchBoards").addEventListener("click", () => {
  state.boardOffset = 0;
  state.board = null;
  loadResearch();
});
$("boardSearch").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    $("searchBoards").click();
  }
});
function fact(label, value, quality) {
  const n = node("div", null, "fact");
  const shown = node("strong", format(value));
  if (value && typeof value === "object")
    shown.title = `精确值：${value.exact || value.decimal || format(value)}`;
  n.append(node("span", label), shown, node("span", quality || ""));
  return n;
}
const profileNames = {
  "profile.closed_month_trend": "闭月趋势",
  "profile.closed_week_trend": "闭周趋势",
  "profile.compression": "压缩状态",
  "profile.drawdown60": "60会话回撤",
  "profile.extension": "延展状态",
  "profile.ma_structure": "均线结构",
  "profile.near_high20": "接近前高",
  "profile.participation": "成交参与",
  "profile.position": "价格位置",
  "profile.relative": "相对市场",
  "profile.trend": "趋势状态",
};
const profileLabel = (field) => profileNames[field] || field;
const qualityLabel = (quality) =>
  ({
    OBSERVED: "可观察",
    UNKNOWN_DATA: "未知",
    CONTRACT_UNBOUND: "合同未绑定",
    EVALUATED: "已计算",
  })[quality] ||
  quality ||
  "UNKNOWN";
function ruleText(item) {
  if (!item) return "来源缺行或未评价";
  const reasons = Object.entries(item.predicates || {})
    .filter(([, p]) => ["FALSE", "UNKNOWN"].includes(p.state))
    .map(
      ([key, p]) =>
        `${key} ${p.state} · ${p.formula || ""} · ${p.reason || p.reason_code || "原因未提供"}`,
    );
  return `${item.scenario_raw} · ${reasons.join("；") || item.main_reason || "所有条件已满足"}`;
}
function launchDisplayExplanation(change, mode, symbol) {
  const stateNow = change?.after?.scenario_raw;
  if (stateNow === "FALSE") return "来源启动条件未满足；这不是正式选股淘汰。";
  if (stateNow === "UNKNOWN") return "来源条件不可判；不能解释为未入选。";
  if (stateNow !== "TRUE") return "目标日来源状态尚未评价。";
  if (mode !== "D0")
    return "当前列表展示 D1 变化，来源启动真值不决定该列表；正式 Priority 尚未计算。";
  if (change.transition === "TRUE_TO_TRUE")
    return "相邻会话均为真，没有新增变化，因此不在 D0 新增列表。";
  if (change.transition !== "FALSE_TO_TRUE")
    return "当前为真，但前态不足以证明相邻会话新增；不在 D0 新增列表。";
  if (!state.data) return "新增列表尚未计算，不能判断是否显示。";
  return stockRows(state.data).some((item) => item.source_symbol === symbol)
    ? "位于当前 D0 新增代码页。"
    : "已知为 D0 新增，未在当前代码页；请检查分页。";
}
async function openStock(symbol, row = {}) {
  state.stockController?.abort();
  currentStockSymbol = symbol;
  currentStockSource = state.selected
    ? {
        target_session: state.selected.target_session,
        package_sha256: state.selected.package_sha256,
      }
    : null;
  if (!state.selected) {
    $("contextDialog").showModal();
    return;
  }
  if (/^BJ\.\d{6}$/.test(symbol) && row.change_state) {
    ++state.stockRequest;
    message("stockTitle", symbol);
    message(
      "stockMeta",
      `${state.selected.target_session} · 当前成员快照 · NON_PIT`,
    );
    message(
      "stockStatus",
      "该成员未映射到本次沪深来源池，无法读取同日个股研究；不推断停牌、退市或正式未入选。",
    );
    $("stockContent").replaceChildren(
      node(
        "div",
        `D1 ${row.change_state} · ${(row.unknown_reasons || []).join("、") || "来源数据不可用"}；LAUNCH / RECOVERY / Priority：NOT_EVALUATED。`,
        "rule",
      ),
    );
    if (!$("stockDialog").open) $("stockDialog").showModal();
    return;
  }
  if (!/^(SH|SZ)\.\d{6}$/.test(symbol)) {
    message("contextText", "请输入 SH. 或 SZ. 加六位数字的来源代码。");
    return;
  }
  const c = state.selected,
    token = ++state.stockRequest;
  const controller = new AbortController();
  state.stockController = controller;
  const stockApi = (path, body) => api(path, body, controller.signal);
  const researchReady = state.researchReady;
  const d1State =
    row.change_state ||
    (row.changed_anchor_event_count ? "CHANGED" : "NOT_EVALUATED");
  const visible =
    state.data &&
    stockRows(state.data).some((item) => item.source_symbol === symbol);
  const displayReason = visible
    ? "位于当前变化代码页"
    : d1State === "CHANGED"
      ? "已知有变化，不在当前代码页；请检查分页"
      : "本次尚未证明符合当前变化列表，或该列表未计算";
  const d1IsPending =
    c.mode === "D1" &&
    !row.change_state &&
    !row.changed_anchor_event_count &&
    Boolean(state.data || researchReady);
  message("stockTitle", symbol);
  message(
    "stockMeta",
    `${c.target_session} · RAW_UNADJUSTED · SOURCE_SYMBOL_ONLY · NON_PIT`,
  );
  message("stockStatus", "正在读取同日 Raw、Core 与 Profile…");
  $("stockContent").replaceChildren();
  if (!$("stockDialog").open) $("stockDialog").showModal();
  try {
    const data = await stockApi("/api/preview/stock", {
      symbol,
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      session_count: c.stock_history_session_count,
    });
    if (token !== state.stockRequest) return;
    const root = $("stockContent");
    const grid = node("div", null, "fact-grid");
    for (const [key, label] of [
      ["open", "开盘"],
      ["high", "最高"],
      ["low", "最低"],
      ["close", "收盘"],
    ])
      grid.append(
        fact(label, data.raw?.[key]?.value, data.raw?.[key]?.quality),
      );
    root.append(grid, node("h3", "价格与画像"));
    const core = node("div", null, "fact-grid");
    for (const [key, label] of [
      ["core.ma20", "MA20"],
      ["core.ma60", "MA60"],
      ["core.atr20", "ATR20"],
      ["core.ret20", "20 会话收益"],
    ]) {
      const f = data.core?.fields?.[key] || {};
      core.append(fact(label, f.value, f.reason || f.quality));
    }
    root.append(core, node("h3", "所选修订的持久画像"));
    const storedProfiles = node("div", "正在读取所选画像修订…", "rule");
    root.append(storedProfiles, node("h3", "即时计算的画像组件"));
    const profiles = node("div", null, "rule");
    const components = data.profile_preview?.components || [];
    for (const f of components)
      profiles.append(
        node(
          "div",
          `${profileLabel(f.field_id)} · ${format(f.value)} · ${qualityLabel(f.quality || f.status)} ${f.reason || ""}`,
        ),
      );
    if (!components.length)
      profiles.textContent = "Profile UNKNOWN · 本次没有可用组件。";
    root.append(profiles, node("h3", "日 / 周 / 月价格"));
    const periods = node("div", null, "rule");
    const periodControls = node("div", null, "period-controls");
    const periodResults = node("div", "正在读取历史价格…", "history-list");
    const history = { day: null, week: null, month: null, selected: "day" };
    function showHistory(kind) {
      history.selected = kind;
      periodControls.querySelectorAll("button").forEach((button) => {
        button.classList.toggle("selected", button.dataset.kind === kind);
      });
      const entries = history[kind];
      periodResults.replaceChildren();
      if (!entries) {
        periodResults.textContent = "正在读取或历史价格不可用。";
        return;
      }
      if (!entries.length) {
        periodResults.textContent = "该周期没有可用历史记录。";
        return;
      }
      for (const item of [...entries].reverse()) {
        const label = kind === "day" ? item.session_date : item.period_key;
        const o = kind === "day" ? item.raw_ohlc : item.ohlc;
        const val = (name) => (kind === "day" ? o?.[name]?.value : o?.[name]);
        const quality =
          kind === "day" ? item.price_quality : item.coverage_status;
        const reason =
          kind === "day"
            ? item.price_reason
            : (item.reason_codes || []).join(" / ");
        const line =
          val("close") == null
            ? `${label} · ${quality || "UNKNOWN"} · ${reason || "价格不可用"}`
            : `${label} · 开 ${format(val("open"))}　高 ${format(val("high"))}　低 ${format(val("low"))}　收 ${format(val("close"))} · ${quality || "UNKNOWN"}`;
        periodResults.append(node("div", line, "history-row"));
      }
    }
    for (const [kind, label] of [
      ["day", "日线"],
      ["week", "周线"],
      ["month", "月线"],
    ]) {
      const button = node("button", label, "button subtle");
      button.type = "button";
      button.dataset.kind = kind;
      button.addEventListener("click", () => showHistory(kind));
      periodControls.append(button);
    }
    periods.append(
      node("p", "RAW_UNADJUSTED · 未闭合周期不展示部分 OHLC。"),
      periodControls,
      periodResults,
    );
    root.append(periods, node("h3", "来源级规则与未展示原因"));
    const rules = node(
      "div",
      `LAUNCH / RECOVERY 正在读取；D1 ${d1IsPending ? "CALCULATING" : d1State}；正式 Priority、Risk、D2 与 Focus NOT_EVALUATED。`,
      "rule",
    );
    const launchInfo = node("p", "LAUNCH 正在读取…");
    const recoveryInfo = node("p", "RECOVERY 等待读取…");
    const membershipInfo = node("p", "当前成员快照正在读取…");
    rules.replaceChildren(
      node("strong", "来源级诊断"),
      launchInfo,
      recoveryInfo,
      membershipInfo,
      node("p", "正式 Priority / Risk / D2 / Focus：NOT_EVALUATED"),
    );
    const d1Info = node(
      "p",
      `D1 ${d1IsPending ? "CALCULATING" : d1State} · ${d1IsPending ? "正在读取该代码的精确历史窗口" : displayReason}。`,
    );
    root.append(rules, d1Info);
    message(
      "stockStatus",
      `已读取 ${data.window?.real_bar_count ?? "—"} / ${data.window?.requested_sessions ?? "—"} 个实际 Bar；来源级诊断不构成正式选股。`,
    );
    const query = new URLSearchParams({
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      namespace_id: c.namespace_id,
      symbol,
    });
    const profileQuery = new URLSearchParams({
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      profile_revision: c.profile_revision,
      symbol,
      limit: "1",
    });
    stockApi(`/api/source-symbol-price-profiles?${profileQuery}`)
      .then((result) => {
        if (token !== state.stockRequest) return;
        const item = result.rows?.[0];
        if (!item) {
          storedProfiles.textContent =
            "该代码在所选持久画像修订中没有记录；不能推断其他版本不存在。";
          return;
        }
        storedProfiles.replaceChildren();
        for (let i = 0; i < (item.profile_field_ids || []).length; i++) {
          storedProfiles.append(
            node(
              "div",
              `${profileLabel(item.profile_field_ids[i])} · ${qualityLabel(item.profile_statuses?.[i])} · ${qualityLabel(item.profile_qualities?.[i])} · ${item.profile_reasons?.[i] || "—"}`,
            ),
          );
        }
      })
      .catch((error) => {
        if (token === state.stockRequest)
          storedProfiles.textContent = `所选画像 UNKNOWN：${error.message}`;
      });
    stockApi(`/api/source-symbol/raw-periods?${query}`)
      .then((p) => {
        if (token !== state.stockRequest) return;
        history.week = p.periods?.week || [];
        history.month = p.periods?.month || [];
        if (history.selected !== "day") showHistory(history.selected);
      })
      .catch((e) => {
        if (token === state.stockRequest) {
          history.week = history.month = [];
          periods.append(node("p", `周 / 月 UNKNOWN：${e.message}`));
          if (history.selected !== "day") showHistory(history.selected);
        }
      });
    stockApi("/api/preview/stock-timeline", {
      symbol,
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      namespace_id: c.namespace_id,
      session_count: c.stock_history_session_count,
    })
      .then((timeline) => {
        if (token !== state.stockRequest) return;
        history.day = timeline.points || [];
        if (history.selected === "day") showHistory("day");
      })
      .catch((error) => {
        if (token === state.stockRequest && history.selected === "day")
          periodResults.textContent = `日线 UNKNOWN：${error.message}`;
      });
    const observationRequest = {
      source_observation_only: true,
      symbol,
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      namespace_id: c.namespace_id,
      research_cutoff: c.research_cutoff,
      industry_snapshot_id: c.industry_snapshot_id,
      concept_snapshot_id: c.concept_snapshot_id,
    };
    stockApi("/api/preview/stock-timeline", {
      ...observationRequest,
      diagnostic_scope: "LAUNCH",
    })
      .then((result) => {
        if (token !== state.stockRequest) return;
        const launch = result.d0_change;
        const membership = result.current_tdx_membership;
        const boards = (membership?.memberships || [])
          .map((b) => b.board_name)
          .join("、");
        const renderLaunch = () => {
          if (token === state.stockRequest)
            launchInfo.textContent = `LAUNCH 当前：${ruleText(launch?.after)}；前态 → 当前：${launch?.transition || "NOT_EVALUATED"}。${launchDisplayExplanation(launch, c.mode, symbol)}`;
        };
        renderLaunch();
        state.researchReady?.then(renderLaunch).catch(() => {});
        const membershipText =
          boards ||
          (membership?.status === "AVAILABLE_EMPTY"
            ? "当前快照未匹配成员关系，不代表历史板外"
            : membership?.reason || membership?.status || "NOT_EVALUATED");
        membershipInfo.textContent = `当前成员快照（NON_PIT）：${membershipText}`;
      })
      .catch((e) => {
        if (token === state.stockRequest)
          launchInfo.textContent = `LAUNCH UNKNOWN：${e.message}`;
      })
      .then(() => {
        if (token !== state.stockRequest) return null;
        recoveryInfo.textContent = "RECOVERY 正在读取…";
        return stockApi("/api/preview/stock-timeline", {
          ...observationRequest,
          diagnostic_scope: "RECOVERY",
        });
      })
      .then((result) => {
        if (token === state.stockRequest && result)
          recoveryInfo.textContent = `RECOVERY 当前：${ruleText(result.recovery)}`;
      })
      .catch((error) => {
        if (token === state.stockRequest)
          recoveryInfo.textContent = `RECOVERY UNKNOWN：${error.message}`;
      });
    if (state.data || state.researchReady) {
      const request = { ...requestBody(), explain_symbol: symbol };
      if (c.mode === "D1") request.d1_only_explanation = true;
      const requestD1 = async () => {
        try {
          const result = await stockApi(
            "/api/preview/sector-change-stocks",
            request,
          );
          if (token !== state.stockRequest || !result) return;
          const item = result.symbol_observation;
          const d1 = item?.d1;
          if (!d1) {
            d1Info.textContent = "D1 NOT_EVALUATED：未返回该代码的观察结果。";
            return;
          }
          const changed = d1.change_state === "CHANGED";
          const pageContains = (result.changed_stock_page?.stocks || []).some(
            (x) => x.source_symbol === symbol,
          );
          const reason = pageContains
            ? "位于当前变化代码页"
            : result.scope === "SOURCE_SYMBOL_ONLY_D1_SUPPORT_EXPLANATION"
              ? "已按该代码精确Raw历史计算；全池研究单独进行"
              : changed
                ? "已知有变化，不在当前代码页；请检查分页"
                : d1.change_state === "UNCHANGED"
                  ? "当前观察窗没有新增支撑变化"
                  : "本次未能判定为变化，原因见状态";
          const events = (d1.anchor_events || [])
            .map(
              (event) =>
                `${event.session} ${event.change_state} ${event.unknown_reason || ""}`,
            )
            .join("；");
          d1Info.textContent = `D1 ${d1.change_state} · ${reason} · ${(d1.unknown_reasons || []).join("、") || events || "无锚事件"}。正式 Priority 尚未计算。`;
        } catch (error) {
          if (token === state.stockRequest)
            d1Info.textContent = `D1 UNKNOWN：${error.message}`;
        }
      };
      if (c.mode === "D1") {
        const firstD1 = requestD1();
        if (researchReady)
          Promise.resolve(researchReady)
            .then(() => firstD1)
            .then(() => {
              if (token === state.stockRequest) return requestD1();
              return null;
            })
            .catch(() => {});
      } else {
        Promise.resolve(researchReady?.catch(() => null)).then(() => {
          if (token === state.stockRequest) return requestD1();
          return null;
        });
      }
    }
  } catch (error) {
    if (token === state.stockRequest)
      message("stockStatus", `个股读取失败：${error.message}`);
  }
}
$("stockSearch").addEventListener("submit", (e) => {
  e.preventDefault();
  openStock($("stockSymbol").value.trim().toUpperCase());
});
$("stockDialog").addEventListener("close", () => {
  ++state.stockRequest;
  state.stockController?.abort();
  state.stockController = null;
});
init();

function detailRows(container, entries) {
  container.replaceChildren();
  for (const [label, value] of entries) {
    const p = node("p");
    p.append(node("strong", `${label}：`), node("span", format(value)));
    container.append(p);
  }
}
async function loadExtra(page) {
  const token = ++extraRequest;
  const [target_session, package_sha256] = $("extraVersion").value.split("|");
  const c =
    target_session && package_sha256
      ? { target_session, package_sha256 }
      : state.selected;
  const id =
    page === "focus"
      ? "focusStatus"
      : page === "market"
        ? "marketResult"
        : "diagnosticResult";
  if (!c) {
    if (page === "focus") {
      ++cohortRequest;
      ++focusStockRequest;
      focusKeysLoading = false;
      $("cohortKey").replaceChildren();
      $("cohortKey").disabled = true;
      $("cohortSummary").replaceChildren();
      $("cohortRows").replaceChildren();
      $("focusStockResult").replaceChildren();
      $("cohortPrev").disabled = $("cohortNext").disabled = true;
      message("cohortPage", "—");
    }
    message(id, "请选择研究范围。");
    return;
  }
  message(id, "正在读取所选范围…");
  if (page === "focus") {
    ++cohortRequest;
    ++focusStockRequest;
    focusKeysLoading = true;
    $("cohortKey").disabled = true;
    $("cohortKey").replaceChildren();
    $("cohortSummary").replaceChildren();
    $("cohortRows").replaceChildren();
    $("cohortPrev").disabled = $("cohortNext").disabled = true;
    message("cohortPage", "—");
    $("focusStockResult").replaceChildren();
    message(
      "focusStockStatus",
      "输入来源代码后可独立读取 Raw 观察；正式 Focus 未计算。",
    );
  }
  const common = {
    target_session: c.target_session,
    package_sha256: c.package_sha256,
  };
  try {
    if (page === "market") {
      const data = await api(
        `/api/product/market?${new URLSearchParams(common)}`,
      );
      if (token !== extraRequest || activePage !== page) return;
      detailRows($(id), [
        ["状态", data.status],
        ["目标日 / 截止", `${data.target_session} / ${data.research_cutoff}`],
        ["公式", data.metric.formula],
        ["精确值", data.metric.exact?.fraction],
        ["来源分母", data.denominator.N_source],
        ["可评价分母", data.denominator.N_eligible],
        ["UNKNOWN", data.denominator.UNKNOWN],
        ["覆盖", data.denominator.coverage?.fraction],
        ["分类", data.denominator.counts],
        ["未知原因", data.denominator.unknown_reason_counts],
        ["来源包", data.source_cohort.package_sha256],
        [
          "来源质量 / 可得时间",
          `${data.source_cohort.source_time_quality} / ${data.source_cohort.source_available_at}`,
        ],
        ["窗口", data.official_calendar.lookback_sessions],
      ]);
    } else if (page === "focus") {
      const data = await api(
        `/api/product/cohort-keys?${new URLSearchParams(common)}`,
      );
      if (token !== extraRequest || activePage !== page) return;
      const select = $("cohortKey");
      select.replaceChildren();
      for (const key of data.keys)
        select.add(
          new Option(
            `${key.rule} · ${key.window} · ${key.history}/${key.d1_history} · ${key.profile_revision.slice(0, 12)} · ${key.count}`,
            JSON.stringify(key),
          ),
        );
      select.disabled = !data.keys.length;
      focusKeysLoading = false;
      message(
        id,
        data.keys.length
          ? `已保存 ${data.keys.length} 个规则/窗口键。选择后读取。`
          : "NO_STORED_FACT：此精确范围未保存冻结规则；不等于全部不符合。可到个股研究查看来源级观察。",
      );
      $("cohortSummary").replaceChildren();
      $("cohortRows").replaceChildren();
      cohortOffset = 0;
    } else {
      const version = state.versions.find(
        (v) =>
          v.session_date === c.target_session &&
          v.package_sha256 === c.package_sha256,
      );
      const profiles = state.profiles.filter(
        (p) =>
          p.target_session === c.target_session &&
          p.package_sha256 === c.package_sha256 &&
          (c.profile_revision
            ? p.profile_revision === c.profile_revision
            : true),
      );
      const context = await api(
        `/api/preview/sector-change-context?${new URLSearchParams({ target_session: c.target_session })}`,
      );
      if (token !== extraRequest || activePage !== page) return;
      const snapshots = context.snapshots || [];
      detailRows($(id), [
        ["目标日 / 来源包", `${c.target_session} / ${c.package_sha256}`],
        [
          "Raw 覆盖",
          version
            ? `${version.row_count} / ${version.expected_row_count} · ${JSON.stringify(version.bar_status_counts)} · ${version.version_status}`
            : "目录中未保存",
        ],
        [
          "来源捕获 / 质量",
          version
            ? `${version.capture_read_started_at} → ${version.capture_validated_at} · ${version.source_time_quality} · ${version.source_clock_role}`
            : "UNKNOWN",
        ],
        ["画像修订数", profiles.length || "目录中未保存"],
        ...profiles.map((p) => [
          `画像 ${p.profile_revision}`,
          `${p.source_symbol_count} / ${p.expected_source_symbol_count} · ${p.complete ? "已保存完整" : "不完整"} · ${p.history_session_count} 会话`,
        ]),
        ...snapshots.map((s) => [
          s.source_kind,
          `${s.member_rows} 条 · 观测 ${s.observed_at} · ${s.clock_quality} · ${s.source_quality} · SHA ${s.source_file_sha256}`,
        ]),
        ["官方日历修订", context.official_calendar_revision],
        ["研究截止", context.research_cutoff],
        ["PG 命名空间", context.namespace_id],
        [
          "本页会话最近读取错误",
          lastApiError
            ? `${lastApiError.time} · ${lastApiError.route} · ${lastApiError.reason}`
            : "本页会话未观察到；服务历史错误不可用",
        ],
        ["服务身份", "开发只读服务；未提供可信进程身份字段"],
      ]);
    }
  } catch (error) {
    if (token === extraRequest && activePage === page) {
      if (page === "focus") focusKeysLoading = false;
      message(id, `读取失败：${error.message}`);
    }
  }
}
async function readCohort() {
  const [target_session, package_sha256] = $("extraVersion").value.split("|");
  const c =
      target_session && package_sha256
        ? { target_session, package_sha256 }
        : state.selected,
    raw = $("cohortKey").value;
  if (!c || !raw) return;
  const token = ++cohortRequest,
    key = JSON.parse(raw),
    symbol = $("cohortSymbol").value.trim().toUpperCase();
  message("focusStatus", "正在读取冻结研究…");
  $("cohortSummary").replaceChildren();
  $("cohortRows").replaceChildren();
  try {
    const query = {
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      profile_revision: key.profile_revision,
      rule: key.rule,
      history: key.history,
      d1_history: key.d1_history,
      window: key.window,
      horizon: $("cohortHorizon").value,
      offset: cohortOffset,
    };
    if (symbol) query.symbol = symbol;
    const d = await api(`/api/product/cohort?${new URLSearchParams(query)}`);
    if (token !== cohortRequest || activePage !== "focus") return;
    if (d.storage_status === "NO_STORED_FACT") {
      message(
        "focusStatus",
        `NO_STORED_FACT：${d.reason}；不能解释为全部不符合。`,
      );
      message("cohortPage", "无已保存分母");
      $("cohortPrev").disabled = $("cohortNext").disabled = true;
      return;
    }
    message(
      "focusStatus",
      `${d.storage_status} · ${d.scope} · ${d.target_session} → ${d.endpoint_session || "未知"} · ${d.due ? "已到期" : "未到期"}`,
    );
    detailRows($("cohortSummary"), [
      ["完整来源分母", d.source_pool_count],
      ...d.groups.map((g) => [
        `${g.group} · ${g.weighting === "stock" ? "按代码" : "按事件"}`,
        `总数 ${g.count}；可评价 ${g.evaluable}；未知 ${g.unknown}；到期状态 ${JSON.stringify(g.maturity)}；原因 ${JSON.stringify(g.unevaluable_reasons)}`,
      ]),
      [
        "所选代码后继事实",
        d.frozen_candidate_followup
          ? `${d.frozen_candidate_followup.source_symbol} · ${d.frozen_candidate_followup.maturity} · 路径 ${d.frozen_candidate_followup.path_quality} · 收盘变化 ${format(d.frozen_candidate_followup.endpoint?.raw_close_relative_change?.value)} · 首次触及 ${d.frozen_candidate_followup.first_touch_status}`
          : "未选择来源代码",
      ],
      ...(d.d1_anchor_events || []).map((a) => [
        `锚 ${a.anchor_session}`,
        `${a.generation_status} · ${a.previous_state} → ${a.target_state} · ${a.change_classification} · 原因 ${a.reason || "无"} · 下界 ${format(a.lower)} · 首次触及 ${a.lower_touch_followup?.first_proved_touch || a.lower_touch_followup?.first_touch_status}`,
      ]),
    ]);
    const rows = $("cohortRows");
    rows.replaceChildren();
    for (const row of d.results) {
      const p = node("p");
      const button = node("button", row.source_symbol, "text-button");
      button.addEventListener("click", () => {
        $("cohortSymbol").value = row.source_symbol;
        cohortOffset = 0;
        void readCohort();
      });
      p.append(
        button,
        node(
          "span",
          ` · ${{ TRUE: "MATCH", FALSE: "NO_MATCH", UNKNOWN: "UNKNOWN" }[row.state] || row.state} · ${row.maturity} · ${format(row.reasons)} · ${format(row.endpoint_reason)}`,
        ),
      );
      rows.append(p);
    }
    message(
      "cohortPage",
      `从 ${cohortOffset + 1} 起 · 总分母 ${d.source_pool_count}`,
    );
    $("cohortPrev").disabled = cohortOffset === 0;
    $("cohortNext").disabled = cohortOffset + 30 >= d.source_pool_count;
  } catch (error) {
    if (token === cohortRequest && activePage === "focus")
      message("focusStatus", `读取失败：${error.message}`);
  }
}
$("readCohort").addEventListener("click", () => {
  cohortOffset = 0;
  void readCohort();
});
$("cohortPrev").addEventListener("click", () => {
  cohortOffset = Math.max(0, cohortOffset - 30);
  void readCohort();
});
$("cohortNext").addEventListener("click", () => {
  cohortOffset += 30;
  void readCohort();
});

$("extraVersion").addEventListener("change", () => {
  void loadExtra(activePage);
});

function focusSourceSelection() {
  const [target_session, package_sha256] = $("extraVersion").value.split("|");
  return target_session && package_sha256
    ? { target_session, package_sha256 }
    : state.selected;
}
$("stockToFocus").addEventListener("click", () => {
  if (!currentStockSource || !currentStockSymbol) return;
  $("extraVersion").value =
    `${currentStockSource.target_session}|${currentStockSource.package_sha256}`;
  $("cohortSymbol").value = currentStockSymbol;
  $("stockDialog").close();
  if ($("boardDialog").open) $("boardDialog").close();
  gotoPage("focus");
});
$("focusBackStock").addEventListener("click", () => {
  const c = focusSourceSelection(),
    symbol = $("cohortSymbol").value.trim().toUpperCase();
  if (!c || !/^(SH|SZ)\.\d{6}$/.test(symbol)) {
    message("focusStockStatus", "请选择来源范围并输入有效来源代码。");
    return;
  }
  gotoPage("stocks");
  $("stockSymbol").value = symbol;
  if (
    state.selected &&
    state.selected.target_session === c.target_session &&
    state.selected.package_sha256 === c.package_sha256
  ) {
    void openStock(symbol);
  } else {
    void openScopedSourceStock(symbol, c);
  }
});
async function openScopedSourceStock(symbol, source) {
  state.stockController?.abort();
  const controller = new AbortController(),
    token = ++state.stockRequest;
  state.stockController = controller;
  currentStockSymbol = symbol;
  currentStockSource = { ...source };
  message("stockTitle", symbol);
  message(
    "stockMeta",
    `${source.target_session} · RAW_UNADJUSTED · SOURCE_SYMBOL_ONLY · NON_PIT · 本次查看不改变研究日期`,
  );
  message("stockStatus", "正在读取所选来源范围的单股价格…");
  $("stockContent").replaceChildren();
  if (!$("stockDialog").open) $("stockDialog").showModal();
  try {
    const context = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: source.target_session })}`,
      undefined,
      controller.signal,
    );
    const sessionCount = context.window_options?.[0]?.pg_history_session_count;
    if (!sessionCount) throw new Error("官方会话窗口不可用");
    const data = await api(
      "/api/preview/stock",
      {
        symbol,
        target_session: source.target_session,
        package_sha256: source.package_sha256,
        session_count: sessionCount,
      },
      controller.signal,
    );
    if (token !== state.stockRequest) return;
    message(
      "stockStatus",
      `已读取 ${data.window?.real_bar_count ?? "—"} / ${data.window?.requested_sessions ?? sessionCount} 个 Bar；持久画像、正式 Focus 和 Priority 未在此范围加载。`,
    );
    detailRows($("stockContent"), [
      ["来源包", source.package_sha256],
      ...[
        ["open", "开盘"],
        ["high", "最高"],
        ["low", "最低"],
        ["close", "收盘"],
      ].map(([key, label]) => [
        label,
        `${format(data.raw?.[key]?.value)} · ${data.raw?.[key]?.quality || "UNKNOWN"} · ${data.raw?.[key]?.reason || "—"}`,
      ]),
    ]);
  } catch (error) {
    if (token === state.stockRequest)
      message("stockStatus", `来源观察不可用：${error.message}`);
  }
}
$("focusReadStock").addEventListener("click", async () => {
  const c = focusSourceSelection(),
    symbol = $("cohortSymbol").value.trim().toUpperCase();
  const token = ++focusStockRequest;
  $("focusStockResult").replaceChildren();
  if (!c || !/^(SH|SZ)\.\d{6}$/.test(symbol)) {
    message("focusStockStatus", "请选择来源范围并输入有效来源代码。");
    return;
  }
  message("focusStockStatus", "正在读取来源价格；不等待全池 D1。");
  try {
    const context = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: c.target_session })}`,
    );
    if (token !== focusStockRequest || activePage !== "focus") return;
    const sessionCount = context.window_options?.[0]?.pg_history_session_count;
    if (!sessionCount) throw new Error("官方会话窗口不可用");
    const data = await api("/api/preview/stock", {
      symbol,
      target_session: c.target_session,
      package_sha256: c.package_sha256,
      session_count: sessionCount,
    });
    if (token !== focusStockRequest || activePage !== "focus") return;
    message(
      "focusStockStatus",
      `${symbol} · ${c.target_session} · RAW_UNADJUSTED · SOURCE_SYMBOL_ONLY · NON_PIT；来源观察已计算，冻结规则及正式 Focus 的状态分别以规则结果为准。`,
    );
    detailRows($("focusStockResult"), [
      ["来源包", c.package_sha256],
      [
        "实际窗口",
        `${data.window?.real_bar_count ?? "—"} / ${data.window?.requested_sessions ?? sessionCount} 个 Bar`,
      ],
      ...[
        ["open", "开盘"],
        ["high", "最高"],
        ["low", "最低"],
        ["close", "收盘"],
      ].map(([k, label]) => [
        label,
        `${format(data.raw?.[k]?.value)} · ${data.raw?.[k]?.quality || "UNKNOWN"} · ${data.raw?.[k]?.reason || "—"}`,
      ]),
    ]);
  } catch (error) {
    if (token === focusStockRequest && activePage === "focus")
      message("focusStockStatus", `来源观察不可用：${error.message}`);
  }
});
for (const id of ["cohortKey", "cohortHorizon", "cohortSymbol"]) {
  $(id).addEventListener(id === "cohortSymbol" ? "input" : "change", () => {
    ++cohortRequest;
    ++focusStockRequest;
    $("cohortSummary").replaceChildren();
    $("cohortRows").replaceChildren();
    $("cohortPrev").disabled = $("cohortNext").disabled = true;
    message("cohortPage", "—");
    $("focusStockResult").replaceChildren();
    message(
      "focusStockStatus",
      "输入来源代码后可独立读取 Raw 观察；正式 Focus 未计算。",
    );
    message(
      "focusStatus",
      focusKeysLoading
        ? "正在读取冻结规则目录…"
        : $("cohortKey").options.length
          ? "选择已改变，请重新读取冻结研究。"
          : "NO_STORED_FACT：此来源范围没有可读取的冻结规则；可继续读取来源价格。",
    );
  });
}
