import { requestD1Material } from "./api.js";
import {
  buildD1MaterialRequest,
  byId,
  d1MaterialState,
  displayBoardName,
  displayStockCode,
  displayStockName,
  displayStockTitle,
} from "./state.js";
import { renderD1MaterialMarket, setD1MaterialStatus } from "./market.js";
import { renderD1Boards, renderD1Members } from "./sectors.js";
import { renderD1StockDetail, renderD1Stocks } from "./stocks.js";
const $ = (id) => document.getElementById(id);
const state = {
  versions: [],
  profiles: [],
  context: null,
  displayNameContext: null,
  selected: null,
  contextRequest: 0,
  contextController: null,
  stockRequest: 0,
  profileChangeRequest: 0,
  stockController: null,
  profileChangeController: null,
  d0Controller: null,
  d0Request: 0,
  d0Offset: 0,
  d0Result: null,
  d0Status: "NOT_LOADED",
  d0Error: null,
  d0BoardPage: 0,
  d0Board: null,
  d0BoardOffset: 0,
  d0BoardRequest: 0,
};
const names = {
  overview: ["今日总览", "从真实变化出发，了解市场中正在发生什么。", "01"],
  sectors: ["板块研究", "从行业与概念进入成员与个股。", "02"],
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
function boardKindLabel(kind) {
  return kind === "INDUSTRY" ? "行业" : kind === "CONCEPT" ? "概念" : "板块";
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
    for (let attempt = 0; ; attempt++) {
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
      if (result.display_name_context) {
        state.displayNameContext = result.display_name_context;
        const context = state.displayNameContext;
        message(
          "displayNameObservation",
          context.status === "AVAILABLE"
            ? `中文名为本机TDX观察标签（NON_PIT，${context.captured_at || "观察时钟未知"}）；不是稳定证券身份。`
            : "中文名来源暂无可用观察；以名称未知和来源代码展示。",
        );
      }
      if (result.code === "PREVIEW_CONCURRENCY_LIMIT" && attempt < 10) {
        await delayWithAbort(2000, signal);
        continue;
      }
      if (!response.ok || result.ok === false)
        throw new Error(
          `${result.code || response.status}：${result.message || "请求失败"}`,
        );
      return result;
    }
  } catch (error) {
    if (
      error.name !== "AbortError" &&
      !error.message.startsWith("D1_MATERIAL_BUSY：")
    )
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
function delayWithAbort(milliseconds, signal) {
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      window.clearTimeout(wait);
      reject(abortError());
    };
    const wait = window.setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) onAbort();
  });
}
async function waitForD1Material(body, signal, onBusy) {
  while (true) {
    try {
      return await requestD1Material(api, body, signal);
    } catch (error) {
      if (!error.message.startsWith("D1_MATERIAL_BUSY：")) throw error;
      onBusy();
      await delayWithAbort(5000, signal);
    }
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
  const changed = activePage !== page;
  activePage = page;
  const extra = ["focus", "market", "diagnostics"].includes(page);
  $("refreshResearch").hidden = extra;
  $("contextNotice").hidden = extra;
  $("d1MaterialFilters").hidden =
    !state.d1Requested || !["sectors", "stocks"].includes(page);
  $("d1MaterialQuality").hidden =
    !state.d1Requested || !["sectors", "stocks"].includes(page);
  $("d1SectorPanel").hidden = page !== "sectors" || !state.d1Requested;
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
  if (page === "sectors" && state.selected?.contextReady)
    d1MaterialState.data && renderD1Boards(d1MaterialState.data, openD1Board);
  if (page === "stocks" && state.selected?.contextReady)
    d1MaterialState.data && renderD1Stocks(d1MaterialState.data, openD1Stock);
  if (page === "market") renderMarketD0Summary();
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
function invalidateDate() {
  ++state.contextRequest;
  ++state.stockRequest;
  ++state.profileChangeRequest;
  ++state.d0Request;
  ++state.d0BoardRequest;
  ++d1MaterialState.request;
  ++extraRequest;
  ++cohortRequest;
  ++focusStockRequest;
  state.stockController?.abort();
  state.contextController?.abort();
  state.profileChangeController?.abort();
  state.d0Controller?.abort();
  d1MaterialState.controller?.abort();
  state.selected = null;
  state.context = null;
  state.d1Requested = false;
  state.d0Result = null;
  state.d0Status = "NOT_LOADED";
  state.d0Error = null;
  state.d0BoardPage = 0;
  d1MaterialState.data = null;
  d1MaterialState.board = null;
  d1MaterialState.boardOffset =
    d1MaterialState.stockOffset =
    d1MaterialState.memberOffset =
      0;
  currentStockSymbol = currentStockSource = null;
  for (const id of ["boardDialog", "stockDialog"])
    if ($(id).open) $(id).close();
  for (const id of [
    "overviewBoards",
    "overviewStocks",
    "d0BoardRows",
    "stockList",
    "memberList",
    "profileChangeList",
    "marketResult",
    "diagnosticResult",
    "cohortSummary",
    "cohortRows",
    "focusStockResult",
  ])
    $(id).replaceChildren();
  $("d0BoardSearch").value = "";
  $("d0BoardKind").value = "ALL";
  const d0EmptyRow = node("tr");
  const d0EmptyCell = node("td", "正在读取当前日期的 D0 变化板块。", "empty");
  d0EmptyCell.colSpan = 6;
  d0EmptyRow.append(d0EmptyCell);
  $("d0BoardRows").append(d0EmptyRow);
  $("boardRows").replaceChildren();
  $("d0Stocks").replaceChildren();
  message("d0Status", "当前日期的 D0 新增启动尚未读取。");
  state.d0Offset = 0;
  state.d0Board = null;
  state.d0BoardOffset = 0;
  $("d0Prev").disabled = $("d0Next").disabled = true;
  message("d0Page", "—");
  $("metrics").replaceChildren(
    countCard("来源代码池", "—", "正在读取当前日期"),
    countCard("目标日有价格", "—", "正在读取当前日期"),
    countCard("目标日缺价格", "—", "正在读取当前日期"),
    countCard("变化研究", "—", "尚未加载"),
  );
  message("boardSummary", "当前日期尚未读取。");
  message("d0BoardSummary", "等待当前日期的 D0 结果");
  message("d0BoardPage", "—");
  $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
  $("d1SectorPanel").hidden = true;
  renderMarketD0Summary();
  message("boardCoverage", "当前日期尚未读取。");
  message("profileChangeSummary", "当前日期尚未读取");
  message("profileChangeList", "正在核验相邻会话画像口径。");
  message("focusStatus", "当前日期尚未读取。");
  message("focusStockStatus", "当前日期尚未读取。");
  $("refreshResearch").disabled = false;
  for (const id of [
    "boardPrev",
    "boardNext",
    "stockPrev",
    "stockNext",
    "memberPrev",
    "memberNext",
    "cohortPrev",
    "cohortNext",
  ])
    $(id).disabled = true;
  $("cohortKey").replaceChildren();
  $("cohortKey").disabled = true;
  cohortOffset = 0;
  message("boardPage", "—");
  message("stockPage", "—");
  message("memberPage", "—");
  message("cohortPage", "—");
  message("d1MaterialStatus", "当前日期的深度研究尚未加载。");
  message("d1MaterialQuality", "");
  message("observationLabel", "正在读取当前日期…");
}

async function init() {
  try {
    const versions = [];
    let offset = 0;
    let total = null;
    do {
      const catalog = await api(
        `/api/preview/catalog?${new URLSearchParams({ offset, limit: 100 })}`,
      );
      if (catalog.offset !== offset || !Array.isArray(catalog.versions))
        throw new Error("来源日期目录分页不一致");
      if (total !== null && total !== catalog.total_version_count)
        throw new Error("读取期间来源日期目录发生变化，请重试");
      total = catalog.total_version_count;
      versions.push(...catalog.versions);
      const next = catalog.next_offset;
      if (next !== null && (!Number.isInteger(next) || next <= offset))
        throw new Error("来源日期目录下一页无效");
      offset = next;
    } while (offset !== null);
    if (versions.length !== total) throw new Error("来源日期目录未完整读取");
    state.versions = versions;
    state.profilesReady = api("/api/source-symbol-price-profiles/snapshots")
      .then((profiles) => {
        state.profiles = profiles.snapshots || [];
      })
      .catch(() => {
        state.profiles = [];
      });
    const dates = [...new Set(versions.map((v) => v.session_date))]
      .sort()
      .reverse();
    const select = $("researchDate");
    select.replaceChildren();
    for (const date of dates) select.add(new Option(date, date));
    select.disabled = !dates.length;
    const latest = dates.find((date) => uniqueRawVersion(date));
    if (!latest) {
      message("contextText", "没有可唯一绑定的完整来源日期。");
      return;
    }
    select.value = latest;
    await selectDate(latest);
  } catch (error) {
    message("contextText", `PG 目录不可用：${error.message}`);
  }
}

function uniqueRawVersion(date) {
  const closed = state.versions.filter(
    (v) => v.session_date === date && v.version_status === "CLOSED",
  );
  return closed.length === 1 ? closed[0] : null;
}

$("researchDate").addEventListener(
  "change",
  () => void selectDate($("researchDate").value),
);

async function selectDate(target_session) {
  invalidateDate();
  const token = state.contextRequest;
  const controller = new AbortController();
  state.contextController = controller;
  const version = uniqueRawVersion(target_session);
  if (!version) {
    message(
      "contextText",
      `${target_session} 的完整来源缺失或存在冲突，无法确定唯一来源。`,
    );
    message("observationLabel", "来源不可用");
    return;
  }
  const source_package_id = version.source_package_id;
  const selected = {
    target_session,
    source_package_id,
    contextReady: false,
  };
  state.selected = selected;
  message(
    "contextText",
    `${target_session} · 来源代码池 ${version.row_count} 条；成员快照和画像状态正在读取。`,
  );
  message("observationLabel", `${target_session} · 已保存行情`);
  $("metrics").replaceChildren(
    countCard("来源代码池", version.row_count, "已保存 Raw 行"),
    countCard(
      "目标日有价格",
      version.bar_status_counts?.TARGET_PRESENT,
      "来源代码",
    ),
    countCard(
      "目标日缺价格",
      version.bar_status_counts?.TARGET_MISSING,
      "不能推断停牌或退市",
    ),
    countCard("D0 新增", "读取中", "相邻会话 FALSE→TRUE"),
  );
  message("overviewBoards", "正在读取 D0 来源级新增启动。");
  message("overviewStocks", "正在读取 D0 来源级新增启动。");
  if (["focus", "market", "diagnostics"].includes(activePage))
    void loadExtra(activePage);
  try {
    const c = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session, source_package_id })}`,
      undefined,
      controller.signal,
    );
    if (token !== state.contextRequest || state.selected !== selected) return;
    state.context = c;
    const uniqueSnapshot = (kind) => {
      const matches = (c.snapshots || []).filter((s) => s.source_kind === kind);
      return matches.length === 1 ? matches[0] : null;
    };
    const industry = uniqueSnapshot("TDX_TDXHY_CFG");
    const concept = uniqueSnapshot("TDX_INFOHARBOR_BLOCK_DAT");
    Object.assign(selected, {
      profile_revision: null,
      profile_status: "NOT_COMPUTED",
      industry_snapshot_id: industry?.snapshot_id || null,
      concept_snapshot_id: concept?.snapshot_id || null,
      previous_session: c.previous_session,
      event_window_start: c.source_history_start,
      pg_history_session_count: c.source_history_session_count,
      stock_history_session_count: c.source_history_session_count,
      d1_history_session_count: c.d1_history_session_count,
      research_cutoff: c.research_cutoff,
      contextReady: true,
    });
    const snapshotText = [industry, concept]
      .filter(Boolean)
      .map(
        (item) =>
          `${item.source_kind === "TDX_TDXHY_CFG" ? "行业" : "概念"}观测 ${item.observed_at}`,
      )
      .join("；");
    message(
      "contextText",
      `${target_session} · 当前成员回看 NON_PIT ${snapshotText || "成员不可用"} · 画像 ${selected.profile_status}`,
    );
    configureD1Filters();
    selected.profileResolution = resolveProfileForDate(
      selected,
      token,
      snapshotText,
      controller.signal,
    );
    void loadD0Overview(selected);
  } catch (error) {
    if (token === state.contextRequest && error.name !== "AbortError")
      message(
        "contextText",
        `${target_session} 行情可用；画像或成员目录读取失败：${error.message}`,
      );
  }
}

async function resolveProfileForDate(c, token, snapshotText, signal) {
  try {
    const detail = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: c.target_session, source_package_id: c.source_package_id })}`,
      undefined,
      signal,
    );
    await state.profilesReady;
    if (token !== state.contextRequest || state.selected !== c) return;
    const observation = detail.profile_observation;
    const selection = observation?.profile_revision_selection;
    const revision =
      selection?.selected_version_match === true &&
      selection?.current_version_match_count === 1
        ? selection.selected_profile_revision
        : null;
    const matches = state.profiles.filter(
      (p) =>
        p.complete &&
        p.target_session === c.target_session &&
        p.source_package_id === c.source_package_id &&
        p.profile_revision === revision &&
        p.history_calendar_revision === detail.official_calendar_revision &&
        p.price_coordinate === "RAW_UNADJUSTED",
    );
    const profile = matches.length === 1 ? matches[0] : null;
    c.profile_revision = profile?.profile_revision || null;
    c.profile_status = profile
      ? "AVAILABLE"
      : revision
        ? "NOT_COMPARABLE"
        : observation?.status || "NOT_COMPUTED";
    message(
      "contextText",
      `${c.target_session} · 当前成员回看 NON_PIT ${snapshotText || "成员不可用"} · 画像 ${c.profile_status}`,
    );
    void loadProfileChanges();
    if (activePage === "diagnostics") void loadExtra("diagnostics");
  } catch (error) {
    if (
      token === state.contextRequest &&
      state.selected === c &&
      error.name !== "AbortError"
    ) {
      c.profile_status = "NOT_COMPUTED";
      message("profileChangeSummary", "NOT_COMPARABLE");
      message("profileChangeList", `画像语义不可核验：${error.message}`);
    }
  }
}

function provedD0Boards() {
  const rows = state.d0Result?.boards;
  if (!Array.isArray(rows)) return null;
  const seen = new Set();
  return rows.filter((board) => {
    if (!board || Number(board.new_member_count) <= 0) return false;
    const key = `${board.board_kind}:${board.board_native_id}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function missingD0Kinds() {
  return state.d0Result?.membership_status?.missing_membership_kinds || [];
}

function renderD0Boards() {
  const rows = $("d0BoardRows");
  rows.replaceChildren();
  const addEmptyRow = (text) => {
    const row = node("tr");
    const cell = node("td", text, "empty");
    cell.colSpan = 6;
    row.append(cell);
    rows.append(row);
  };
  const boards = provedD0Boards();
  if (!state.d0Result || !Array.isArray(state.d0Result.boards)) {
    const status =
      state.d0Status === "LOADING"
        ? "正在读取已保存 D0 事实；来源宽度可先独立查看。"
        : state.d0Status === "UNKNOWN"
          ? `D0 UNKNOWN：${state.d0Error || "缺少同源相邻会话或变化板块事实"}`
          : "等待当前日期的 D0 结果";
    message("d0BoardSummary", status);
    message("d0BoardPage", "—");
    $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
    addEmptyRow(status);
    return;
  }

  const missing = missingD0Kinds();
  if (boards.length === 0) {
    const status = missing.length
      ? `NO_PROVED_D0_CHANGE；成员关系 UNKNOWN：${missing.join("、")}`
      : "NO_PROVED_D0_CHANGE：当前同源范围没有已证 FALSE→TRUE 变化板块。";
    message(
      "d0BoardSummary",
      `${state.selected?.target_session || ""} · ${status}`,
    );
    message("d0BoardPage", "0 个已证变化板块");
    $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
    addEmptyRow(status);
    return;
  }

  const kind = $("d0BoardKind").value;
  const query = $("d0BoardSearch").value.trim().toLocaleLowerCase();
  const filtered = boards.filter((board) => {
    if (kind !== "ALL" && board.board_kind !== kind) return false;
    const searchable = displayBoardName(board).toLocaleLowerCase();
    return !query || searchable.includes(query);
  });
  const lastPage = Math.max(0, Math.ceil(filtered.length / 15) - 1);
  state.d0BoardPage = Math.min(state.d0BoardPage, lastPage);
  const start = state.d0BoardPage * 15;
  const page = filtered.slice(start, start + 15);
  const industryCount = boards.filter(
    (item) => item.board_kind === "INDUSTRY",
  ).length;
  const conceptCount = boards.filter(
    (item) => item.board_kind === "CONCEPT",
  ).length;
  const missingText = missing.length
    ? ` · 成员关系 UNKNOWN：${missing.join("、")}`
    : "";
  message(
    "d0BoardSummary",
    `${state.selected?.target_session || ""} · 已证变化板块 ${boards.length}（行业 ${industryCount} / 概念 ${conceptCount}） · 筛选 ${filtered.length} · 成员关系可重叠 · NON_PIT${missingText}`,
  );
  if (!filtered.length) {
    addEmptyRow("当前筛选没有匹配的已证 D0 变化板块；清除筛选可恢复完整列表。");
  } else {
    for (const [index, board] of page.entries()) {
      const row = node("tr");
      row.append(
        node("td", displayBoardName(board)),
        node("td", board.board_kind === "INDUSTRY" ? "行业" : "概念"),
        node("td", format(board.new_member_count)),
        node("td", board.observed_at || "UNKNOWN"),
        node("td", "NON_PIT"),
      );
      const action = node("td");
      const button = node("button", "查看新增成员 →", "text-button");
      button.type = "button";
      button.addEventListener("click", () => openD0Board(board));
      action.append(button);
      row.append(action);
      rows.append(row);
      row.setAttribute("data-d0-board-index", String(start + index));
    }
  }
  const totalPages = Math.max(1, Math.ceil(filtered.length / 15));
  message(
    "d0BoardPage",
    `第 ${state.d0BoardPage + 1}/${totalPages} 页 · ${page.length}/${filtered.length} 个板块`,
  );
  $("d0BoardPrev").disabled = state.d0BoardPage === 0;
  $("d0BoardNext").disabled = state.d0BoardPage >= lastPage;
}

function renderMarketD0Summary() {
  const root = $("marketD0Result");
  if (!root) return;
  root.replaceChildren();
  if (!state.d0Result) {
    root.append(
      node(
        "p",
        state.d0Status === "LOADING"
          ? "正在读取已保存 D0 事实；来源宽度已独立加载，读取完成后本页会自动更新。"
          : state.d0Status === "UNKNOWN"
            ? `D0 UNKNOWN：${state.d0Error || "缺少可核验的同源变化事实"}；来源宽度仍独立可用。`
            : "正在读取已保存 D0 事实；来源宽度已独立加载。",
      ),
    );
    return;
  }
  const count = state.d0Result.filter?.global_total;
  const boards = provedD0Boards();
  root.append(
    node(
      "p",
      `目标日 ${state.d0Result.target_session || state.selected?.target_session || "UNKNOWN"} · D0 新增 ${format(count)} 只独立来源代码；这不是正式选股结果。`,
    ),
  );
  if (boards === null) {
    root.append(
      node("p", "已证变化板块 UNKNOWN；独立股票数仍按 D0 全局去重结果显示。"),
    );
  } else {
    const industry = boards.filter(
      (board) => board.board_kind === "INDUSTRY",
    ).length;
    const concept = boards.filter(
      (board) => board.board_kind === "CONCEPT",
    ).length;
    root.append(
      node(
        "p",
        `已证变化板块 ${boards.length} 个：行业 ${industry} 个，概念 ${concept} 个。板块成员可重复归属；股票数不由板块成员数相加。${missingD0Kinds().length ? ` 成员关系 UNKNOWN：${missingD0Kinds().join("、")}。` : ""} NON_PIT。`,
      ),
    );
  }
}

function applyD0BoardFilters() {
  state.d0BoardPage = 0;
  renderD0Boards();
}

$("searchD0Boards").addEventListener("click", applyD0BoardFilters);
$("d0BoardKind").addEventListener("change", applyD0BoardFilters);
$("d0BoardSearch").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    applyD0BoardFilters();
  }
});
$("clearD0BoardFilters").addEventListener("click", () => {
  $("d0BoardSearch").value = "";
  $("d0BoardKind").value = "ALL";
  applyD0BoardFilters();
});
$("d0BoardPrev").addEventListener("click", () => {
  state.d0BoardPage = Math.max(0, state.d0BoardPage - 1);
  renderD0Boards();
});
$("d0BoardNext").addEventListener("click", () => {
  state.d0BoardPage += 1;
  renderD0Boards();
});

async function loadD0Overview(c) {
  const token = ++state.d0Request;
  const before = uniqueRawVersion(c.previous_session);
  if (!before || before.source_package_id !== c.source_package_id) {
    state.d0Result = null;
    state.d0Status = "UNKNOWN";
    state.d0Error = "相邻会话没有可确认的同源 Raw";
    message(
      "d0Status",
      "相邻会话没有可确认的同源 Raw，D0 新增状态为 UNKNOWN。",
    );
    message("overviewBoards", "相邻会话同源 Raw 不可用，板块变化 UNKNOWN。");
    message("overviewStocks", "相邻会话同源 Raw 不可用，个股变化 UNKNOWN。");
    $("metrics").lastElementChild.replaceWith(
      countCard("D0 新增", "UNKNOWN", "相邻会话同源 Raw 不可用"),
    );
    renderD0Boards();
    renderMarketD0Summary();
    return;
  }
  state.d0Controller?.abort();
  const controller = new AbortController();
  state.d0Controller = controller;
  if (!state.d0Result) state.d0Status = "LOADING";
  renderD0Boards();
  renderMarketD0Summary();
  $("d0Stocks").replaceChildren();
  $("d0Prev").disabled = $("d0Next").disabled = true;
  message("d0Page", "正在读取…");
  message("d0Status", "正在读取完整来源池的相邻会话新增启动…");
  try {
    const result = await api(
      "/api/preview/source-launch-watch",
      {
        before_session: c.previous_session,
        target_session: c.target_session,
        research_cutoff: c.research_cutoff,
        source_package_id: c.source_package_id,
        industry_snapshot_id: c.industry_snapshot_id,
        concept_snapshot_id: c.concept_snapshot_id,
        offset: state.d0Offset,
        limit: 30,
      },
      controller.signal,
    );
    if (token !== state.d0Request || state.selected !== c) return;
    state.d0Result = result;
    state.d0Status = Array.isArray(result.boards) ? "READY" : "UNKNOWN";
    state.d0Error = Array.isArray(result.boards)
      ? null
      : "D0 响应未包含完整变化板块事实";
    renderD0Boards();
    renderMarketD0Summary();
    if (state.d0Offset === 0) {
      const boards = result.boards
        .filter((item) => item.new_member_count > 0)
        .slice(0, 15);
      const uniqueStocks = [
        ...new Map(
          result.results.map((item) => [item.source_symbol, item]),
        ).values(),
      ].slice(0, 30);
      fillList(
        "overviewBoards",
        boards,
        "没有已证 D0 变化板块；成员缺失为 UNKNOWN。",
        (item, index) =>
          rowButton(
            item.board_name,
            `${boardKindLabel(item.board_kind)} · 观测 ${item.observed_at || "UNKNOWN"} · NON_PIT`,
            item.new_member_count,
            "D0 新增成员",
            () => openD0Board(item),
            index,
          ),
      );
      fillList(
        "overviewStocks",
        uniqueStocks,
        "没有已证 D0 新增股票；UNKNOWN 不代表无变化。",
        (item, index) =>
          rowButton(
            displayStockName(item),
            `代码 ${displayStockCode(item)} · D0 来源级新增启动 · 相邻会话 FALSE→TRUE`,
            "D0",
            "查看原因",
            () => openStock(item.source_symbol),
            index,
          ),
      );
    }
    const missing = result.membership_status?.missing_membership_kinds || [];
    if (state.d0Offset === 0 && missing.length) {
      $("overviewBoards").prepend(
        node(
          "p",
          `成员关系 UNKNOWN：${missing.join("、")}；其余类型独立展示。`,
          "rule",
        ),
      );
    }
    const count = result.filter?.global_total;
    $("metrics").lastElementChild.replaceWith(
      countCard("D0 新增", count, "独立来源代码 · 尚未排序为正式候选"),
    );
    message(
      "d0Status",
      `${c.target_session} · 新增 ${format(count)} 只 · 展示 ${result.results.length} 只 · NON_PIT${missing.length ? ` · 部分成员关系 UNKNOWN：${missing.join("、")}` : ""}`,
    );
    message(
      "d0Page",
      `偏移 ${result.pagination.offset} · 总数 ${format(count)}`,
    );
    $("d0Prev").disabled = result.pagination.offset === 0;
    $("d0Next").disabled = result.pagination.next_offset == null;
    fillList(
      "d0Stocks",
      result.results,
      "没有已证明的 D0 新增启动；未知状态不等于未入选。",
      (item, index) =>
        rowButton(
          displayStockName(item),
          `代码 ${displayStockCode(item)} · 来源级新增启动 · NON_PIT`,
          "D0",
          "查看原因",
          () => openStock(item.source_symbol),
          index,
        ),
    );
  } catch (error) {
    if (token === state.d0Request && error.name !== "AbortError") {
      message("d0Status", `D0 新增读取失败：${error.message}`);
      if (!state.d0Result) {
        state.d0Status = "UNKNOWN";
        state.d0Error = error.message;
        renderD0Boards();
        renderMarketD0Summary();
      }
      if (state.d0Offset === 0) {
        $("metrics").lastElementChild.replaceWith(
          countCard("D0 新增", "UNKNOWN", "来源读取失败"),
        );
        message("overviewBoards", `D0 UNKNOWN：${error.message}`);
        message("overviewStocks", `D0 UNKNOWN：${error.message}`);
      }
    }
  }
}
$("d0Retry").addEventListener("click", () => {
  if (state.selected?.contextReady) void loadD0Overview(state.selected);
});
$("d0Prev").addEventListener("click", () => {
  state.d0Offset = Math.max(0, state.d0Offset - 30);
  if (state.selected?.contextReady) void loadD0Overview(state.selected);
});
$("d0Next").addEventListener("click", () => {
  state.d0Offset += 30;
  if (state.selected?.contextReady) void loadD0Overview(state.selected);
});
async function openD0Board(board, offset = 0) {
  const c = state.selected;
  if (!c?.contextReady) return;
  state.d0Board = board;
  $("memberTools").style.display = "none";
  state.d0BoardOffset = offset;
  const token = ++state.d0BoardRequest;
  message("boardTitle", displayBoardName(board));
  message(
    "boardMeta",
    `${boardKindLabel(board.board_kind)} · 成员快照观测 ${board.observed_at || "UNKNOWN"} · NON_PIT 回看 · D0 新增`,
  );
  message("boardCoverage", "正在读取该板块 D0 新增成员…");
  message("memberList", "正在读取…");
  $("memberPrev").disabled = $("memberNext").disabled = true;
  if (!$("boardDialog").open) $("boardDialog").showModal();
  try {
    const result = await api("/api/preview/source-launch-watch", {
      before_session: c.previous_session,
      target_session: c.target_session,
      research_cutoff: c.research_cutoff,
      source_package_id: c.source_package_id,
      industry_snapshot_id: c.industry_snapshot_id,
      concept_snapshot_id: c.concept_snapshot_id,
      board_kind: board.board_kind,
      board_native_id: board.board_native_id,
      offset,
      limit: 5,
    });
    if (
      token !== state.d0BoardRequest ||
      c !== state.selected ||
      state.d0Board !== board ||
      !$("boardDialog").open
    )
      return;
    const pulseBoard = result.boards?.find(
      (item) =>
        item.board_kind === board.board_kind &&
        item.board_native_id === board.board_native_id,
    );
    const pulse = pulseBoard?.cross_period_pulse ?? board.cross_period_pulse;
    const pulseTransition = pulseBoard?.cross_period_pulse_transition;
    const pulseFollowup = pulseBoard?.cross_period_pulse_followup;
    const pulseSummary =
      pulse?.status === "AVAILABLE"
        ? `跨日 PULSE ${pulse.state} · dq5 ${pulse.dq5_pp?.decimal_4dp ?? "UNKNOWN"} · breadth Δ ${pulse.breadth_delta1?.decimal_4dp ?? "UNKNOWN"}`
        : `跨日 PULSE ${pulse?.status ?? "NOT_COMPUTED"} · ${(pulse?.reason_codes ?? ["PULSE_FACTS_NOT_SAVED_FOR_EXACT_INPUT"]).join("、")}`;
    const pulseTransitionLabels = {
      PULSE_APPEARED: "新出现",
      PULSE_PERSISTED: "连续成立",
      PULSE_CEASED: "本日不再成立",
      PULSE_NOT_PRESENT_BOTH: "连续未成立",
      PULSE_TRANSITION_UNKNOWN: "无法比较（含 UNKNOWN）",
    };
    const pulseTransitionReasons = [
      ...(pulseTransition?.previous_reason_codes ?? []),
      ...(pulseTransition?.target_reason_codes ?? []),
    ];
    const pulseTransitionSummary =
      pulseTransition?.status === "AVAILABLE"
        ? `相邻 PULSE ${c.previous_session}→${c.target_session} ${pulseTransitionLabels[pulseTransition.transition_state] ?? pulseTransition.transition_state}（${pulseTransition.previous_state}→${pulseTransition.target_state}）${pulseTransitionReasons.length ? ` · ${[...new Set(pulseTransitionReasons)].join("、")}` : ""}`
        : `相邻 PULSE ${c.previous_session}→${c.target_session} ${pulseTransition?.status ?? "NOT_COMPUTED"} · ${(pulseTransition?.reason_codes ?? ["PULSE_TRANSITION_FACTS_NOT_SAVED_FOR_EXACT_INPUT"]).join("、")}`;
    const pulseFollowupSummary = (pulseFollowup?.horizons ?? [])
      .map((item) => {
        const label = `T+${item.horizon_sessions ?? "?"} ${item.target_session ?? ""}`;
        const denominator = item.fixed_member_denominator;
        const observed = item.observed_member_count;
        if (item.status === "OBSERVED") {
          const percent = item.equal_weight_return_percent_4dp;
          const signed = Number(percent) >= 0 ? `+${percent}` : percent;
          return `${label} ${signed}%（${observed}/${denominator}）`;
        }
        if (item.status === "NOT_DUE") return `${label}尚未到期`;
        if (item.status === "NOT_COMPUTED") return `${label}尚未生产`;
        if (item.status === "UNKNOWN")
          return `${label}未知（${observed}/${denominator}，${(item.reason_codes ?? []).join("、")}）`;
        return `${label}${item.status ?? "INCOMPLETE"}（${(item.reason_codes ?? []).join("、")}）`;
      })
      .join(" · ");
    const pulseFollowupLine = pulseFollowupSummary
      ? `PULSE后继Raw观察（${pulseFollowup.research_mode ?? "NON_PIT_RETROSPECTIVE"}；固定成员分母）${pulseFollowupSummary}`
      : `PULSE后继Raw观察 ${pulseFollowup?.status ?? "NOT_COMPUTED"} · ${(pulseFollowup?.reason_codes ?? ["PULSE_FOLLOWUP_NOT_AVAILABLE"]).join("、")}`;
    const pulseFollowupComparison =
      result.sector_pulse_followup_comparison?.[board.board_kind];
    const comparisonReasonLabels = {
      T0_RAW_ROW_MISSING: "T0行情行缺失",
      T0_BAR_NOT_PRESENT: "T0日线缺失",
      TARGET_RAW_ROW_MISSING: "后继行情行缺失",
      TARGET_BAR_NOT_PRESENT: "后继日线缺失",
      EMPTY_FIXED_MEMBER_BASKET: "固定成员篮子为空",
      FOLLOWUP_SOURCE_IDENTITY_MISMATCH: "后继来源身份不匹配",
      FOLLOWUP_PULSE_STATE_MISMATCH: "PULSE状态不匹配",
      FOLLOWUP_BOARD_PARTITION_INCOMPLETE: "板块后继分区不完整",
      FOLLOWUP_HORIZON_FACT_MISSING: "后继窗口事实缺失",
    };
    const comparisonStateLabels = {
      TRUE: "PULSE TRUE",
      FALSE: "PULSE FALSE",
      UNKNOWN: "PULSE UNKNOWN",
    };
    const comparisonDistributionText = (distribution) => {
      if (distribution?.status !== "AVAILABLE") {
        return distribution?.status === "NOT_DUE"
          ? "尚未到期，无可比较收益"
          : distribution?.status === "NOT_COMPUTED"
            ? "尚未生产，无可比较收益"
            : distribution?.status === "INCOMPLETE"
              ? "输入不完整，无可比较收益"
              : "无可评价收益样本";
      }
      const signed = (value) =>
        `${String(value).startsWith("-") ? "" : "+"}${value}%`;
      return `均值 ${signed(distribution.mean_return_percent_4dp)}，中位 ${signed(distribution.median_return_percent_4dp)}，正/非正 ${distribution.positive_board_relation_count}/${distribution.nonpositive_board_relation_count}，范围 ${signed(distribution.minimum_return_percent_4dp)} 至 ${signed(distribution.maximum_return_percent_4dp)}`;
    };
    const comparisonGroupText = (group) => {
      if (!group) return "不可用";
      const coverage = group.complete_basket_coverage;
      const statusCounts = group.outcome_status_counts ?? {};
      const extraStates = [
        ["NOT_DUE", "未到期"],
        ["NOT_COMPUTED", "未生产"],
        ["INCOMPLETE", "不完整"],
      ]
        .filter(([key]) => statusCounts[key] > 0)
        .map(([key, label]) => `${label} ${statusCounts[key]}`)
        .join("、");
      const reasons = (group.unknown_reason_counts ?? [])
        .map(
          (item) =>
            `${comparisonReasonLabels[item.reason_code] ?? item.reason_code} ${item.board_relation_count}`,
        )
        .join("、");
      const incompleteReasons = (group.incomplete_reason_counts ?? [])
        .map(
          (item) =>
            `${comparisonReasonLabels[item.reason_code] ?? item.reason_code} ${item.board_relation_count}`,
        )
        .join("、");
      return `${comparisonStateLabels[group.pulse_state] ?? group.pulse_state} 完整篮子 ${coverage.observed_board_relations}/${coverage.all_board_relations}（${coverage.percent_4dp ?? "UNKNOWN"}%）；UNKNOWN ${statusCounts.UNKNOWN ?? 0}${extraStates ? `、${extraStates}` : ""}${reasons ? `，未知原因 ${reasons}` : ""}${incompleteReasons ? `，不完整原因 ${incompleteReasons}` : ""}；${comparisonDistributionText(group.distribution)}`;
    };
    const comparisonRows = (pulseFollowupComparison?.horizons ?? []).map(
      (item) => {
        const groups = item.pulse_states ?? {};
        const coverageDifference =
          item.true_minus_false_observed_coverage_percentage_points;
        const meanDifference =
          item.true_minus_false_mean_return_difference_percentage_points;
        return `T+${item.horizon_sessions}：${["TRUE", "FALSE", "UNKNOWN"]
          .map((status) => comparisonGroupText(groups[status]))
          .join(
            "；",
          )}；TRUE−FALSE完整篮子覆盖差 ${coverageDifference == null ? "不可比较" : `${coverageDifference}个百分点`}，OBSERVED均值差 ${meanDifference == null ? "不可比较" : `${meanDifference}个百分点`}`;
      },
    );
    const pulseFollowupComparisonLine =
      comparisonRows.length > 0
        ? `同行类型样本对照（${boardKindLabel(board.board_kind)}；统计板块关系、成员可能重叠；NON_PIT_EXPLORATORY；不验证预测）：${comparisonRows.join(" | ")}`
        : `同行类型样本对照 ${pulseFollowupComparison?.status ?? "NOT_COMPUTED"} · ${(pulseFollowupComparison?.reason_codes ?? ["PULSE_FOLLOWUP_COMPARISON_NOT_AVAILABLE"]).join("、")}`;
    message(
      "boardMeta",
      `${displayBoardName(board)} · 成员快照观测 ${board.observed_at || "UNKNOWN"} · NON_PIT 回看 · ${pulseSummary} · ${pulseTransitionSummary} · ${pulseFollowupLine} · ${pulseFollowupComparisonLine} · 正式 Rotation 未评估`,
    );
    message(
      "boardCoverage",
      `D0 新增成员 ${result.filter.filtered_total} · 全市场独立新增 ${result.filter.global_total} · 板块关系可重叠 · NON_PIT`,
    );
    fillList(
      "memberList",
      result.results,
      "没有已证明的新增成员；未知不等于无变化。",
      (item, index) =>
        rowButton(
          displayStockName(item),
          `代码 ${displayStockCode(item)} · 来源池内 · D0 FALSE→TRUE · NON_PIT`,
          "D0",
          "查看同日个股",
          () => openStock(item.source_symbol),
          index,
        ),
    );
    message(
      "memberPage",
      `偏移 ${result.pagination.offset} · 总数 ${result.filter.filtered_total}`,
    );
    $("memberPrev").disabled = offset === 0;
    $("memberNext").disabled = result.pagination.next_offset == null;
  } catch (error) {
    if (token === state.d0BoardRequest && c === state.selected)
      message("memberList", `D0 成员 UNKNOWN：${error.message}`);
  }
}
function supportRequest(c, symbol) {
  return {
    observation_mode: "D1",
    target_session: c.target_session,
    research_cutoff: c.research_cutoff,
    source_package_id: c.source_package_id,
    profile_revision: null,
    industry_snapshot_id: null,
    concept_snapshot_id: null,
    event_window_start: c.event_window_start,
    pg_history_session_count: c.pg_history_session_count,
    board_kind: "ALL",
    board_search: null,
    changed_only: false,
    sort_by: "changed_stocks",
    direction: "STRONGER",
    offset: 0,
    limit: 15,
    detail_board_kind: null,
    detail_board_native_id: null,
    member_state: "ALL",
    member_search: null,
    member_offset: 0,
    member_limit: 0,
    include_changed_stock_page: true,
    stock_offset: 0,
    stock_limit: 30,
    explain_symbol: symbol,
    d1_only_explanation: true,
  };
}
function configureD1Filters() {
  $("d1MaterialFilters").hidden =
    !state.d1Requested || !["sectors", "stocks"].includes(activePage);
  $("d1MaterialQuality").hidden =
    !state.d1Requested || !["sectors", "stocks"].includes(activePage);
  $("d1SectorPanel").hidden = activePage !== "sectors" || !state.d1Requested;
  $("profileChangeCard").hidden = false;
  $("memberState").replaceChildren(
    new Option("全部状态", "ALL"),
    new Option("材料变化", "MATERIAL_CHANGE"),
    new Option("未命中所选原因", "NO_SELECTED_REASON"),
    new Option("已知非材料变化", "KNOWN_NON_MATERIAL"),
    new Option("未知", "UNKNOWN"),
    new Option("不可比较", "NOT_COMPARABLE"),
    new Option("不可评价", "NOT_EVALUABLE"),
    new Option("不适用", "NOT_APPLICABLE"),
    new Option("无已证锚点", "NO_PROVED_ANCHOR"),
    new Option("不在目标来源池", "NOT_IN_SOURCE_POOL"),
  );
}
function openD1Board(board) {
  state.d0Board = null;
  $("memberTools").style.display = "";
  d1MaterialState.board = {
    board_kind: board.board_kind,
    board_native_id: board.board_native_id,
    board_name: board.board_name,
  };
  d1MaterialState.memberOffset = 0;
  $("boardKind").value = board.board_kind;
  $("memberSearch").value = "";
  $("memberState").value = "ALL";
  message("boardTitle", displayBoardName(board));
  message(
    "boardMeta",
    `${board.board_kind === "INDUSTRY" ? "行业" : "概念"} · 当前成员快照 NON_PIT · D1材料变化`,
  );
  message("memberList", "正在读取同一D1筛选范围的板块成员…");
  $("memberList").classList.remove("has-items");
  if (!$("boardDialog").open) $("boardDialog").showModal();
  void loadD1Material();
}
function openD1Stock(symbol, member = null) {
  void openStock(symbol, member || {});
}
function clearD1MaterialView(symbol = null) {
  message("d1MaterialQuality", "正在读取当前范围的质量与分组，旧结果已清除。");
  for (const id of ["stockList"]) {
    $(id).replaceChildren(node("p", "等待同一来源范围的D1全池读取…"));
    $(id).classList.remove("has-items");
  }
  const row = node("tr");
  const cell = node("td", "等待D1材料变化全池读取…", "empty");
  cell.colSpan = 6;
  row.append(cell);
  $("boardRows").replaceChildren(row);
  for (const id of ["boardSummary", "boardPage", "stockPage"])
    message(id, "正在读取…");
  if (!state.d0Board) message("memberPage", "正在读取…");
  for (const id of ["boardPrev", "boardNext", "stockPrev", "stockNext"])
    $(id).disabled = true;
  if (!state.d0Board)
    $("memberPrev").disabled = $("memberNext").disabled = true;
  if (d1MaterialState.board && !state.d0Board) {
    message("boardCoverage", "正在读取当前成员范围…");
    message("memberList", "正在读取当前成员范围，旧结果已清除。");
    $("memberList").classList.remove("has-items");
  }
  if (symbol) {
    message("stockStatus", "正在从同一D1全池事实读取所选个股…");
    $("stockContent").replaceChildren();
  }
}
async function loadD1Material(symbol = null) {
  if (!state.selected?.contextReady) {
    message("contextText", "当前日期的研究上下文尚未就绪。");
    return;
  }
  const token = ++d1MaterialState.request;
  d1MaterialState.controller?.abort();
  const controller = new AbortController();
  d1MaterialState.controller = controller;
  const hadFacts = Boolean(d1MaterialState.data);
  d1MaterialState.data = null;
  d1MaterialState.startedAt = performance.now();
  const requestStartedAt = d1MaterialState.startedAt;
  clearD1MaterialView(symbol);
  $("refreshResearch").disabled = true;
  const body = buildD1MaterialRequest(
    state.selected,
    d1MaterialState.board,
    {
      board: d1MaterialState.boardOffset,
      member: d1MaterialState.memberOffset,
      stock: d1MaterialState.stockOffset,
    },
    symbol,
  );
  setD1MaterialStatus(
    hadFacts
      ? "正在将结构、原因、板块与股票筛选应用到同一批D1保存事实…"
      : "正在读取PG已保存的完整来源池D1材料变化；页面不会显示缩小后的来源池结果。",
  );

  const timer = window.setInterval(() => {
    if (token !== d1MaterialState.request) return;
    const seconds = Math.floor(
      (performance.now() - d1MaterialState.startedAt) / 1000,
    );
    setD1MaterialStatus(
      `PG仍在读取完整来源池事实，已等待 ${seconds} 秒；筛选和分页不会裁剪来源池。`,
    );
  }, 1000);
  try {
    const data = await waitForD1Material(body, controller.signal, () =>
      setD1MaterialStatus("PG保存事实仍在读取；本次请求将自动继续。"),
    );
    if (token !== d1MaterialState.request) return;
    const responseSeconds = (performance.now() - requestStartedAt) / 1000;
    const renderStartedAt = performance.now();
    d1MaterialState.data = data;
    renderD1MaterialMarket(data, openD1Board, openD1Stock);
    renderD1Boards(data, openD1Board);
    renderD1Stocks(data, openD1Stock);
    if (!state.d0Board) renderD1Members(data, openD1Stock);
    const renderSeconds = (performance.now() - renderStartedAt) / 1000;
    const execution = data.execution || {};
    const scan = execution.d1_full_pool_scan_passes || 0;
    const scanSeconds = execution.d1_full_pool_scan_wall_seconds;
    setD1MaterialStatus(
      execution.reused_process_d1_material_facts
        ? `已复用同进程完整事实 · 本次全池扫描 ${scan} 次 · 首次扫描 ${scanSeconds ?? "—"} 秒 · 服务处理 ${execution.http_request_wall_seconds ?? "—"} 秒 · 请求往返/JSON ${responseSeconds.toFixed(3)} 秒 · 页面渲染 ${renderSeconds.toFixed(3)} 秒。`
        : `PG保存事实读取完成 · 全池重算 ${scan} 次 · 耗时 ${scanSeconds ?? "—"} 秒 · 请求往返/JSON ${responseSeconds.toFixed(3)} 秒 · 页面渲染 ${renderSeconds.toFixed(3)} 秒。`,
    );

    if (symbol) renderD1StockDetail(data.selected_stock);
  } catch (error) {
    if (token !== d1MaterialState.request || error.name === "AbortError")
      return;
    d1MaterialState.data = null;
    state.d1Requested = false;
    setD1MaterialStatus(
      `读取失败：${error.message}。可修正筛选或六位股票代码后重新读取。`,
    );

    if (symbol) {
      message("stockStatus", `个股材料读取失败：${error.message}`);
      $("stockContent").replaceChildren(
        node("p", "修正六位股票代码或研究范围后可重新读取。", "rule"),
      );
    }
    for (const id of ["stockList"])
      message(id, "读取失败；不能据此推断没有变化或正式未入选。");
    if (d1MaterialState.board && !state.d0Board)
      message("memberList", "成员读取失败；不能据此推断没有变化。");
  } finally {
    window.clearInterval(timer);
    if (token === d1MaterialState.request)
      $("refreshResearch").disabled = false;
  }
}
for (const id of ["d1StructureType", "d1MaterialReason"])
  $(id).addEventListener("change", () => {
    d1MaterialState.boardOffset = 0;
    d1MaterialState.stockOffset = 0;
    d1MaterialState.memberOffset = 0;
    if (state.selected?.contextReady) void loadD1Material();
  });
async function loadResearch() {
  if (activePage === "overview") gotoPage("sectors");
  state.d1Requested = true;
  $("d1MaterialFilters").hidden = !["sectors", "stocks"].includes(activePage);
  $("d1MaterialQuality").hidden = !["sectors", "stocks"].includes(activePage);
  $("d1SectorPanel").hidden = activePage !== "sectors";
  await loadD1Material();
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
  const previousDate = context.previous_session;
  if (!previousDate || previousDate >= context.target_session) {
    message("profileChangeSummary", "相邻官方会话不可用");
    message("profileChangeList", "当前范围没有可用的此前官方会话。");
    return;
  }
  const beforeRaw = uniqueRawVersion(previousDate);
  if (!context.profile_revision || !beforeRaw) {
    message("profileChangeSummary", "NOT_COMPARABLE");
    message(
      "profileChangeList",
      "当前画像语义不唯一，或相邻会话来源不可唯一绑定；不能判断画像状态变化。",
    );
    return;
  }
  let beforeRevision;
  try {
    const beforeContext = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: previousDate, source_package_id: beforeRaw.source_package_id })}`,
      undefined,
      controller.signal,
    );
    if (token !== state.profileChangeRequest || context !== state.selected)
      return;
    const selection =
      beforeContext.profile_observation?.profile_revision_selection;
    beforeRevision =
      selection?.selected_version_match === true &&
      selection?.current_version_match_count === 1
        ? selection.selected_profile_revision
        : null;
    const beforeProfiles = state.profiles.filter(
      (item) =>
        item.complete &&
        item.target_session === previousDate &&
        item.source_package_id === beforeRaw.source_package_id &&
        item.profile_revision === beforeRevision,
    );
    const afterProfiles = state.profiles.filter(
      (item) =>
        item.complete &&
        item.target_session === context.target_session &&
        item.source_package_id === context.source_package_id &&
        item.profile_revision === context.profile_revision,
    );
    const beforeProfile =
      beforeProfiles.length === 1 ? beforeProfiles[0] : null;
    const afterProfile = afterProfiles.length === 1 ? afterProfiles[0] : null;
    if (!beforeProfile || !afterProfile) {
      message("profileChangeSummary", "NOT_COMPARABLE");
      message(
        "profileChangeList",
        "相邻日缺少唯一适用的完整画像；不把来源或画像缺口当成股票状态变化。",
      );
      return;
    }
    if (
      beforeProfile.history_session_count !== afterProfile.history_session_count
    ) {
      message("profileChangeSummary", "NOT_COMPARABLE");
      message(
        "profileChangeList",
        `相邻画像窗口不等长：前一会话 ${beforeProfile.history_session_count} 个官方会话，目标会话 ${afterProfile.history_session_count} 个官方会话；现有Raw不足以提供同长度窗口，不把窗口差异当成股票状态变化。`,
      );
      return;
    }
    if (
      beforeProfile.history_calendar_id !== afterProfile.history_calendar_id ||
      beforeProfile.history_calendar_revision !==
        afterProfile.history_calendar_revision ||
      beforeProfile.price_coordinate !== afterProfile.price_coordinate
    ) {
      message("profileChangeSummary", "NOT_COMPARABLE");
      message(
        "profileChangeList",
        "相邻画像的官方日历或价格坐标合同不一致；不把口径差异当成股票状态变化。",
      );
      return;
    }
  } catch (error) {
    if (token === state.profileChangeRequest && error.name !== "AbortError") {
      message("profileChangeSummary", "NOT_COMPARABLE");
      message("profileChangeList", `相邻画像口径不可核验：${error.message}`);
    }
    return;
  }
  const query = new URLSearchParams({
    before_session: previousDate,
    after_session: context.target_session,
    before_source_package_id: beforeRaw.source_package_id,
    before_profile_revision: beforeRevision,
    after_source_package_id: context.source_package_id,
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
          displayStockName(item),
          `代码 ${displayStockCode(item)} · ${previousDate} → ${context.target_session} · ${types.join("、") || "状态不可比较"} · RAW_UNADJUSTED · NON_PIT`,
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

$("boardPrev").addEventListener("click", () => {
  d1MaterialState.boardOffset = Math.max(0, d1MaterialState.boardOffset - 15);
  void loadD1Material();
});
$("boardNext").addEventListener("click", () => {
  d1MaterialState.boardOffset += 15;
  void loadD1Material();
});
$("stockPrev").addEventListener("click", () => {
  d1MaterialState.stockOffset = Math.max(0, d1MaterialState.stockOffset - 30);
  void loadD1Material();
});
$("stockNext").addEventListener("click", () => {
  d1MaterialState.stockOffset += 30;
  void loadD1Material();
});
$("memberPrev").addEventListener("click", () => {
  if (state.d0Board)
    return void openD0Board(
      state.d0Board,
      Math.max(0, state.d0BoardOffset - 5),
    );
  d1MaterialState.memberOffset = Math.max(0, d1MaterialState.memberOffset - 30);
  void loadD1Material();
});
$("memberNext").addEventListener("click", () => {
  if (state.d0Board)
    return void openD0Board(state.d0Board, state.d0BoardOffset + 5);
  d1MaterialState.memberOffset += 30;
  void loadD1Material();
});
function filterMembers() {
  if (state.d0Board) return;
  d1MaterialState.memberOffset = 0;
  void loadD1Material();
}
$("searchMembers").addEventListener("click", filterMembers);
$("memberState").addEventListener("change", filterMembers);
$("memberSearch").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    filterMembers();
  }
});
$("boardDialog").addEventListener("close", () => {
  state.d0Board = null;
  ++state.d0BoardRequest;
  d1MaterialState.board = null;
  d1MaterialState.memberOffset = 0;
});
function filterBoards() {
  d1MaterialState.boardOffset = 0;
  d1MaterialState.board = null;
  void loadD1Material();
}
$("boardKind").addEventListener("change", filterBoards);
$("searchBoards").addEventListener("click", filterBoards);
$("boardSearch").addEventListener("keydown", (event) => {
  if (event.key === "Enter") {
    event.preventDefault();
    filterBoards();
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
function launchDisplayExplanation(change) {
  const stateNow = change?.after?.scenario_raw;
  if (stateNow === "FALSE") return "来源启动条件未满足；这不是正式选股淘汰。";
  if (stateNow === "UNKNOWN") return "来源条件不可判；不能解释为未入选。";
  if (stateNow !== "TRUE") return "目标日来源状态尚未评价。";
  if (change.transition === "FALSE_TO_TRUE")
    return "相邻会话由假转真，属于来源级新增启动。";
  if (change.transition === "TRUE_TO_TRUE")
    return "相邻会话均为真，没有新增启动。";
  return "前态不足以证明新增启动。";
}
const seedPredicateLabels = {
  research_universe: "来源研究范围",
  actual_bar: "目标日实际K线",
  non_st: "非ST状态",
  normal_listing_phase: "正常上市阶段",
  limited_price_rule: "涨跌幅规则适用",
  coordinate_ready: "价格坐标连续性",
  liquidity_mean20: "20日流动性",
  no_damage: "无价格损伤",
  no_severe_extension: "无严重乖离",
  bias_below_3: "乖离低于3",
  compression: "波动压缩",
  rps5_delta3_at_least_3: "RPS5三会话变化至少3",
  rps5_delta3_at_least_10: "RPS5三会话变化至少10",
  bull_transition: "均线多头转变",
  previous_ma20_cross: "前一会话MA20上穿",
};
function sourceSeedExplanation(seed) {
  if (!seed) return "来源级Seed：当前请求未返回诊断。";
  if (seed.status !== "SAVED") {
    const descriptions = {
      NOT_COMPUTED:
        "SEED_NOT_COMPUTED：该日期和精确来源捕获没有已存Seed；Raw、Core和D1状态独立。",
      NOT_COMPARABLE:
        "Seed NOT_COMPARABLE：缺少精确画像修订或已存事实来自其他修订。",
      INCOMPLETE: "Seed INCOMPLETE：分区完整性、窗口或来源键未闭合。",
      UNAVAILABLE: "Seed UNAVAILABLE：单股Seed只读请求失败。",
    };
    return `${descriptions[seed.status] || `Seed ${seed.status || "UNKNOWN"}`} ${seed.reason_code || ""} ${seed.message || ""} 正式候选资格仍为 NOT_EVALUATED。`;
  }
  const stateName = (value) =>
    ({ TRUE: "真", FALSE: "假", UNKNOWN: "未知" })[value] || "未知";
  const qualification = seed.qualification || {};
  const reasonList = (value) => (value?.reason_codes || []).join("、") || "无";
  const routes = [
    `来源形态 S1 OR S2 ${stateName(qualification.source_seed_shape?.state)}：${reasonList(qualification.source_seed_shape)}`,
    `安全门 ${stateName(qualification.safety?.state)}：${reasonList(qualification.safety)}`,
    `S1 ${stateName(qualification.s1?.state)}：${reasonList(qualification.s1)}`,
    `S2 ${stateName(qualification.s2?.state)}：${reasonList(qualification.s2)}`,
    `含安全门Seed safety AND (S1 OR S2) ${stateName(qualification.seed?.state)}：${reasonList(qualification.seed)}`,
  ].join("；");
  const unmet = (seed.predicates || [])
    .filter((item) => item.state !== "TRUE")
    .map((item) => {
      const name = seedPredicateLabels[item.predicate_id] || item.predicate_id;
      return `${name} ${stateName(item.state)}：${(item.reason_codes || []).join("、") || "原因未提供"}`;
    })
    .join("；");
  const quality = seed.quality || {};
  const qualityReasons = Object.entries(quality.fields || {})
    .filter(([, field]) => field.quality === "UNKNOWN" || field.reason)
    .map(
      ([fieldId, field]) =>
        `${fieldId} ${field.quality}${field.reason ? `：${field.reason}` : ""}`,
    )
    .join("；");
  return `${routes}。范围 NON_PIT_RETROSPECTIVE；价格坐标 RAW_UNADJUSTED。未满足/未知谓词：${unmet || "无"}。来源质量 ${quality.source_time_quality} / ${quality.source_clock_role}；目标日实际K线 ${quality.target_actual_quote ? "有" : "未知或缺失"}；收盘质量 ${quality.target_close_quality}${quality.target_close_reason ? `：${quality.target_close_reason}` : ""}${qualityReasons ? `；输入质量原因：${qualityReasons}` : ""}。正式候选资格 NOT_EVALUATED。`;
}
async function resolveStockCodeInput(value, context) {
  const input = value.trim().toUpperCase();
  if (/^(SH|SZ|BJ)\.\d{6}$/.test(input))
    return { source_symbol: input, display_name: null };
  if (!/^\d{6}$/.test(input))
    throw new Error("请输入六位数字股票代码，无需市场前缀。");
  if (!context?.contextReady)
    throw new Error("当前研究日期的真实来源池尚未就绪。");
  const result = await api(
    `/api/product/resolve-stock-code?${new URLSearchParams({
      target_session: context.target_session,
      source_package_id: context.source_package_id,
      stock_code: input,
    })}`,
  );
  return result;
}
async function openStock(symbol, row = {}) {
  state.stockController?.abort();
  const resolveToken = ++state.stockRequest;
  const selected = state.selected;
  if (!state.selected) {
    message("contextText", "当前日期的研究上下文尚未就绪。");
    return;
  }
  if (!state.selected.contextReady) {
    message("contextText", "当前日期的研究上下文尚未就绪。");
    return;
  }
  try {
    const resolved = await resolveStockCodeInput(symbol, selected);
    symbol = resolved.source_symbol;
    if (resolved.display_name)
      row = { ...row, display_name: resolved.display_name };
  } catch (error) {
    if (resolveToken === state.stockRequest)
      message("stockSearchStatus", `股票代码解析失败：${error.message}`);
    return;
  }
  if (resolveToken !== state.stockRequest || selected !== state.selected)
    return;
  currentStockSymbol = symbol;
  currentStockSource = {
    target_session: selected.target_session,
    source_package_id: selected.source_package_id,
  };
  if (
    /^BJ\.\d{6}$/.test(symbol) &&
    (row.change_state || row.source_pool_status)
  ) {
    ++state.stockRequest;
    message("stockTitle", displayStockTitle(row, symbol));
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
    message("stockSearchStatus", "请输入六位数字股票代码。");
    return;
  }
  const c = state.selected,
    token = ++state.stockRequest;
  const controller = new AbortController();
  state.stockController = controller;
  const stockApi = (path, body) => api(path, body, controller.signal);
  const d1State =
    row.change_state ||
    (row.changed_anchor_event_count ? "CHANGED" : "NOT_EVALUATED");
  const displayReason = "同日支撑变化按该代码的完整历史独立读取";
  const d1IsPending = true;
  message("stockTitle", displayStockTitle(row, symbol));
  message(
    "stockMeta",
    `${c.target_session} · RAW_UNADJUSTED · SOURCE_SYMBOL_ONLY · NON_PIT`,
  );
  message("stockStatus", "正在读取同日 Raw、Core 与 Profile…");
  $("stockContent").replaceChildren();
  if (!$("stockDialog").open) $("stockDialog").showModal();
  try {
    if (c.profileResolution) await c.profileResolution;
    if (token !== state.stockRequest || state.selected !== c) return;
    const data = await stockApi("/api/preview/stock", {
      symbol,
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      session_count: c.stock_history_session_count,
      profile_revision: c.profile_revision,
    });
    if (token !== state.stockRequest) return;
    message("stockTitle", displayStockTitle(data, symbol));
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
    root.append(core, node("h3", "已保存画像"));
    const storedProfiles = node(
      "div",
      c.profile_revision
        ? "正在读取适用画像…"
        : `画像 ${c.profile_status || "NOT_COMPUTED"}：没有可证明唯一适用的修订。`,
      "rule",
    );
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
    const seedInfo = node("p", sourceSeedExplanation(data.source_seed));
    rules.replaceChildren(
      node("strong", "来源级诊断"),
      seedInfo,
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
    root.append(node("h3", "D1 材料变化"));
    const materialDetail = node(
      "div",
      "正在读取同日完整来源池的材料变化…",
      "rule",
    );
    root.append(materialDetail);
    materialDetail.textContent = "按需读取PG已保存的完整来源池 D1 材料变化。";
    const deepen = node("button", "深入 D1 材料变化", "button subtle");
    deepen.type = "button";
    materialDetail.append(deepen);
    deepen.addEventListener("click", () => {
      deepen.disabled = true;
      materialDetail.textContent = "正在读取PG已保存的完整来源池 D1 事实…";
      const materialRequest = buildD1MaterialRequest(
        c,
        d1MaterialState.board,
        {
          board: d1MaterialState.boardOffset,
          member: d1MaterialState.memberOffset,
          stock: d1MaterialState.stockOffset,
        },
        symbol,
      );
      waitForD1Material(materialRequest, controller.signal, () => {
        if (token === state.stockRequest && state.selected === c)
          materialDetail.textContent =
            "PG保存事实仍在读取；本次材料将自动继续。";
      })
        .then((result) => {
          if (token === state.stockRequest && state.selected === c)
            renderD1StockDetail(result.selected_stock, materialDetail);
        })
        .catch((error) => {
          if (token === state.stockRequest && error.name !== "AbortError")
            materialDetail.textContent = `D1材料变化不可用：${error.message}`;
        });
    });
    message(
      "stockStatus",
      `已读取 ${data.window?.real_bar_count ?? "—"} / ${data.window?.requested_sessions ?? "—"} 个实际 Bar；来源级诊断不构成正式选股。`,
    );
    const query = new URLSearchParams({
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      symbol,
      session_count: c.stock_history_session_count,
    });
    const profileQuery = new URLSearchParams({
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      profile_revision: c.profile_revision,
      symbol,
      limit: "1",
    });
    if (c.profile_revision)
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
      source_package_id: c.source_package_id,
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
      source_package_id: c.source_package_id,
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
            launchInfo.textContent = `LAUNCH 当前：${ruleText(launch?.after)}；前态 → 当前：${launch?.transition || "NOT_EVALUATED"}。${launchDisplayExplanation(launch)}`;
        };
        renderLaunch();
        const membershipText =
          boards ||
          (membership?.status === "AVAILABLE_EMPTY"
            ? "当前快照未匹配成员关系，不代表历史板外"
            : membership?.reason || membership?.status || "NOT_EVALUATED");
        const missing = membership?.missing_membership_kinds || [];
        membershipInfo.textContent = `当前成员快照（NON_PIT）：${membershipText}${missing.length ? `；缺失 ${missing.join("、")}，对应关系 UNKNOWN` : ""}`;
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
    if (c.contextReady) {
      const request = supportRequest(c, symbol);
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
      void requestD1();
    }
  } catch (error) {
    if (token === state.stockRequest)
      message("stockStatus", `个股读取失败：${error.message}`);
  }
}
$("stockSearch").addEventListener("submit", (e) => {
  e.preventDefault();
  const stockCode = $("stockSymbol").value.trim();
  if (!/^\d{6}$/.test(stockCode)) {
    message("stockSearchStatus", "请输入六位数字股票代码，无需市场前缀。");
    return;
  }
  message("stockSearchStatus", "正在按所选交易日的真实来源池解析代码…");
  void openStock(stockCode);
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
  const c = state.selected;
  if (page === "market") renderMarketD0Summary();
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
    message(id, "当前日期尚未就绪。");
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
      "输入六位股票代码后可独立读取 Raw 观察；正式 Focus 未计算。",
    );
  }
  const common = {
    target_session: c.target_session,
    source_package_id: c.source_package_id,
  };
  try {
    if (page === "market") {
      const data = await api(
        `/api/product/market?${new URLSearchParams(common)}`,
      );
      if (token !== extraRequest || activePage !== page) return;
      if (["NOT_COMPUTED", "INCOMPLETE"].includes(data.status)) {
        message(
          id,
          `${data.status}：${data.reason || "该来源的市场宽度尚不可用。"}`,
        );
        return;
      }
      detailRows($(id), [
        ["状态", data.status],
        ["目标日 / 截止", `${data.target_session} / ${data.research_cutoff}`],
        ["观察范围 / 时态", `${data.analysis_scope} / ${data.pit_status}`],
        ["价格坐标", data.price_coordinate],
        ["公式", data.metric.formula],
        ["精确值", data.metric.exact?.fraction],
        ["来源分母", data.denominator.N_source],
        ["可评价分母", data.denominator.N_eligible],
        ["UNKNOWN", data.denominator.UNKNOWN],
        ["覆盖", data.denominator.coverage?.fraction],
        ["分类", data.denominator.counts],
        ["未知原因", data.denominator.unknown_reason_counts],
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
      for (const [index, key] of data.keys.entries())
        select.add(
          new Option(
            `${key.rule} · ${key.window} · 历史 ${key.history}/${key.d1_history} · 记录 ${index + 1} · ${key.count} 条`,
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
          v.source_package_id === c.source_package_id,
      );
      const profiles = state.profiles.filter(
        (p) =>
          p.target_session === c.target_session &&
          p.source_package_id === c.source_package_id &&
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
        ["目标日 / 来源包", `${c.target_session} / ${c.source_package_id}`],
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
          `${s.member_rows} 条 · 观测 ${s.observed_at} · ${s.clock_quality} · ${s.source_quality}`,
        ]),
        ["官方日历修订", context.official_calendar_revision],
        ["研究截止", context.research_cutoff],
        [
          "本次服务进程最近响应错误",
          context.last_service_error
            ? `${context.last_service_error.observed_at} · ${context.last_service_error.route} · HTTP ${context.last_service_error.status} · ${context.last_service_error.code}`
            : "本次进程未记录；重启前历史不可用",
        ],
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
  const c = state.selected,
    raw = $("cohortKey").value,
    stockCode = $("cohortSymbol").value.trim().toUpperCase();
  if (!c || !raw) return;
  const token = ++cohortRequest,
    key = JSON.parse(raw);
  message("focusStatus", "正在读取冻结研究…");
  $("cohortSummary").replaceChildren();
  $("cohortRows").replaceChildren();
  try {
    const resolved = stockCode
      ? await resolveStockCodeInput(stockCode, c)
      : null;
    const symbol = resolved?.source_symbol ?? null;
    if (token !== cohortRequest || activePage !== "focus") return;
    const query = {
      target_session: c.target_session,
      source_package_id: c.source_package_id,
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
          ? `${displayStockTitle(d.frozen_candidate_followup)} · ${d.frozen_candidate_followup.maturity} · 路径 ${d.frozen_candidate_followup.path_quality} · 收盘变化 ${format(d.frozen_candidate_followup.endpoint?.raw_close_relative_change?.value)} · 首次触及 ${d.frozen_candidate_followup.first_touch_status}`
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
      const button = node("button", displayStockName(row), "text-button");
      button.title = `代码 ${displayStockCode(row)}`;
      button.addEventListener("click", () => {
        $("cohortSymbol").value = displayStockCode(row);
        cohortOffset = 0;
        void readCohort();
      });
      p.append(
        button,
        node(
          "span",
          ` · 代码 ${displayStockCode(row)} · ${{ TRUE: "MATCH", FALSE: "NO_MATCH", UNKNOWN: "UNKNOWN" }[row.state] || row.state} · ${row.maturity} · ${format(row.reasons)} · ${format(row.endpoint_reason)}`,
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

function focusSourceSelection() {
  return state.selected;
}
$("stockToFocus").addEventListener("click", () => {
  if (!currentStockSource || !currentStockSymbol) return;
  $("cohortSymbol").value = displayStockCode({
    source_symbol: currentStockSymbol,
  });
  $("stockDialog").close();
  if ($("boardDialog").open) $("boardDialog").close();
  gotoPage("focus");
});
$("focusBackStock").addEventListener("click", () => {
  const c = focusSourceSelection(),
    stockCode = $("cohortSymbol").value.trim();
  if (!c || !/^\d{6}$/.test(stockCode)) {
    message("focusStockStatus", "请等待交易日就绪并输入六位数字股票代码。");
    return;
  }
  gotoPage("stocks");
  $("stockSymbol").value = stockCode;
  void openStock(stockCode);
});
$("focusReadStock").addEventListener("click", async () => {
  const c = focusSourceSelection(),
    stockCode = $("cohortSymbol").value.trim().toUpperCase();
  const token = ++focusStockRequest;
  $("focusStockResult").replaceChildren();
  if (!c || !/^\d{6}$/.test(stockCode)) {
    message("focusStockStatus", "请等待交易日就绪并输入六位数字股票代码。");
    return;
  }
  message(
    "focusStockStatus",
    "正在解析六位代码并读取该来源价格；不等待全池 D1。",
  );
  try {
    const resolved = await resolveStockCodeInput(stockCode, c);
    const symbol = resolved.source_symbol;
    if (token !== focusStockRequest || activePage !== "focus") return;
    const context = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session: c.target_session })}`,
    );
    if (token !== focusStockRequest || activePage !== "focus") return;
    const sessionCount = context.source_history_session_count;
    if (!sessionCount) throw new Error("官方会话窗口不可用");
    const data = await api("/api/preview/stock", {
      symbol,
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      session_count: sessionCount,
    });
    if (token !== focusStockRequest || activePage !== "focus") return;
    message(
      "focusStockStatus",
      `${displayStockTitle({ ...data, display_name: resolved.display_name }, stockCode)} · ${c.target_session} · RAW_UNADJUSTED · SOURCE_SYMBOL_ONLY · NON_PIT；来源观察已计算，冻结规则及正式 Focus 的状态分别以规则结果为准。`,
    );
    detailRows($("focusStockResult"), [
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
      "输入六位股票代码后可独立读取 Raw 观察；正式 Focus 未计算。",
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
