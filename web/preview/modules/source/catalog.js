/* Stock A预览页面source_catalog职责模块；保持原业务逻辑和交互。 */
"use strict";
async function loadCatalog() {
  invalidateTimeline();
  const catalogRequest = ++state.catalogRequest;
  state.pageRequest++;
  state.stockRequest++;
  clearResult();
  $("versionSelect").disabled = true;
  $("versionSelect").replaceChildren(new Option("正在读取PG目录…", ""));
  $("population").textContent = "正在从PostgreSQL只读读取来源日期与版本目录。";
  try {
    const data = await api("/api/preview/catalog?offset=0&limit=100");
    if (catalogRequest !== state.catalogRequest) return;
    state.versions = data.versions;
    $("versionSelect").replaceChildren(
      new Option("— 请选择目标日期与价格捕获 —", ""),
    );
    for (const v of data.versions) {
      const title = [
        `${v.session_date} · 捕获ID ${v.source_capture_id} · ${v.version_status} · `,
        `配对价格行 ${v.row_count}/${v.expected_row_count} · RAW OBSERVED ${v.raw_observed_value_row_count ?? 0} · QFQ OBSERVED ${v.qfq_observed_value_row_count ?? 0}`,
      ].join("");
      const option = new Option(
        title,
        `${v.session_date}|${v.source_capture_id}`,
      );
      $("versionSelect").append(option);
    }
    $("versionSelect").disabled = state.versions.length === 0;
    state.version = null;
    state.offset = 0;
    $("candidateRows").replaceChildren();
    $("pageCount").textContent = "—";
    $("pageStatus").textContent = state.versions.length
      ? "请选择PG日期与BaoStock双坐标捕获"
      : "PG中没有可选的BaoStock价格捕获";
    $("prevPage").disabled = true;
    $("nextPage").disabled = true;
    $("lastPage").disabled = true;
    $("population").textContent = state.versions.length
      ? `已读取${data.total_version_count}个真实PG日期/捕获。请选择明确捕获；价格来源候选不代表官方上市总体。`
      : "PG中没有可选的BaoStock价格捕获。";
  } catch (error) {
    if (catalogRequest !== state.catalogRequest) return;
    $("population").textContent = `目录读取失败：${error.message}`;
    $("candidateRows").replaceChildren();
    tableRow($("candidateRows"), ["PG目录不可用", "", "", "", ""]);
    $("versionSelect").disabled = true;
  }
}
function describePopulation(page) {
  const version = page.source_version;
  const versionText =
    version.status === "CLOSED"
      ? `来源捕获Raw记录闭合：${version.stored_raw_rows}/${version.capture_expected_rows}`
      : `来源捕获Raw部分入库：${version.stored_raw_rows}/${version.capture_expected_rows}；不得按全市场完成解释`;
  return `${page.target_session} · 来源捕获ID ${page.source_capture_id} · ${versionText}。当前筛选来源候选=${page.filtered_total}；有效上市总体=UNKNOWN（NOT_CONNECTED）。这只是该来源捕获的候选分母。`;
}
async function loadPage() {
  const v = selectedVersion();
  if (!v) {
    state.version = null;
    state.total = 0;
    state.pageRequest++;
    state.stockRequest++;
    clearResult();
    $("candidateRows").replaceChildren();
    $("pageCount").textContent = "—";
    $("pageStatus").textContent = "请选择PG日期和版本";
    $("population").textContent = "等待选择PG来源版本。";
    $("prevPage").disabled = true;
    $("nextPage").disabled = true;
    $("lastPage").disabled = true;
    return;
  }
  const requestId = ++state.pageRequest;
  state.stockRequest++;
  const key = `${v.target_session}|${v.source_capture_id}`;
  state.version = v;
  clearResult();
  $("candidateRows").replaceChildren();
  $("pageStatus").textContent = "正在读取PG分页…";
  const query = new URLSearchParams({
    target_session: v.target_session,
    source_capture_id: v.source_capture_id,
    offset: String(state.offset),
    limit: String(state.limit),
    market: $("marketSelect").value,
    board: $("boardSelect").value,
  });
  try {
    const data = await api(`/api/preview/source-page?${query}`);
    if (
      requestId !== state.pageRequest ||
      key !==
        `${selectedVersion()?.target_session}|${selectedVersion()?.source_capture_id}`
    )
      return;
    const p = data.page;
    state.total = p.filtered_total;
    $("population").textContent = describePopulation(p);
    $("pageCount").textContent =
      `本页 ${p.page_count} / 筛选分母 ${p.filtered_total}`;
    $("pageStatus").textContent =
      `偏移 ${p.offset}，后续 ${p.next_offset === null ? 0 : p.filtered_total - p.next_offset}`;
    $("prevPage").disabled = p.offset === 0;
    $("nextPage").disabled = p.next_offset === null;
    $("candidateRows").replaceChildren();
    if (!p.results.length) {
      tableRow($("candidateRows"), ["当前筛选没有来源记录", "", "", "", ""]);
      return;
    }
    for (const item of p.results) {
      const tr = document.createElement("tr");
      cell(tr, item.source_symbol);
      cell(tr, `${item.market} / ${text(item.board)}`);
      cell(tr, text(item.bar_status));
      cell(
        tr,
        `${item.source_object_status || "BAOSTOCK_SOURCE_SYMBOL_OBSERVED"} · ${item.stable_security_id_status || "STABLE_SECURITY_ID_UNVERIFIED"}`,
      );
      const td = document.createElement("td");
      const button = document.createElement("button");
      button.className = "row-select";
      button.type = "button";
      button.textContent = "查看";
      button.addEventListener("click", () => {
        $("symbolInput").value = item.source_symbol;
        $("stockForm").requestSubmit();
      });
      td.append(button);
      tr.append(td);
      $("candidateRows").append(tr);
    }
  } catch (error) {
    if (requestId !== state.pageRequest) return;
    $("population").textContent = `候选页读取失败：${error.message}`;
    $("pageStatus").textContent = "分页不可用";
    $("prevPage").disabled = true;
    $("nextPage").disabled = true;
    tableRow($("candidateRows"), ["PG分页失败", "", "", "", ""]);
  }
}
labels["TARGET_SESSION_BAR_MISSING"] = "目标日来源记录无Bar，不推断停牌或退市";
labels["CURRENT_SESSION_UNKNOWN"] = "目标日状态未知";
