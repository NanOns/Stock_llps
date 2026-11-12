/* Stock A预览页面core_profile职责模块；保持原业务逻辑和交互。 */
"use strict";
const coreNames = [
  "core.ma20",
  "core.ma60",
  "core.atr20",
  "core.ret20",
  "core.pos60",
];
const loadPageOriginal = loadPage;
loadPage = async function () {
  await loadPageOriginal();
  $("lastPage").disabled =
    state.total === 0 || state.offset + state.limit >= state.total;
};
$("lastPage").addEventListener("click", () => {
  if (state.total) {
    state.offset = Math.floor((state.total - 1) / state.limit) * state.limit;
    loadPage();
  }
});
async function queryStock(event) {
  event.preventDefault();
  const requestId = ++state.stockRequest;
  const keepTimeline = state.timelineNavigation;
  const previousTimelineContext = keepTimeline ? state.timelineContext : null;
  state.timelineNavigation = false;
  if (!keepTimeline) {
    state.timeline = null;
    state.timelineContext = null;
    state.chartPeriodContext = null;
    state.chartSelection = null;
    state.timelineRequest++;
    renderTimeline(null);
  }
  clearRawPeriodState("正在读取所选日期、来源代码和PG捕获版本的自然周/月…");
  const v = selectedVersion();
  if (!v) {
    setMessage("请先从真实PG目录选择目标日期和来源包版本。");
    return false;
  }
  const symbol = $("symbolInput").value.trim();
  const count = Number($("windowInput").value);
  const key = `${v.target_session}|${v.source_package_id}|${symbol}|${count}`;
  const requestIsCurrent = () => {
    const selected = selectedVersion();
    return (
      requestId === state.stockRequest &&
      key ===
        `${selected?.target_session}|${selected?.source_package_id}|${$("symbolInput").value.trim()}|${Number($("windowInput").value)}`
    );
  };
  if (!symbol || !Number.isInteger(count) || count < 1 || count > 75) {
    clearResult();
    setMessage("来源代码与会话数无效；会话数须为1至75。");
    return false;
  }
  clearResult();
  $("queryStock").disabled = true;
  setMessage("正在按显式日期、来源包ID和窗口只读读取PG并计算现役Core/Profile…");
  try {
    const raw = await api(
      `/api/preview/raw?${new URLSearchParams({ ...v, symbol })}`,
    );
    if (!requestIsCurrent()) return;
    const result = await api("/api/preview/stock", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        symbol,
        target_session: v.target_session,
        source_package_id: v.source_package_id,
        session_count: count,
      }),
    });
    if (!requestIsCurrent()) return;
    const a = raw.snapshot_identity,
      b = result.snapshot_identity;
    if (
      a.target_session !== b.target_session ||
      a.source_package_id !== b.source_package_id ||
      b.target_session !== v.target_session ||
      b.source_package_id !== v.source_package_id ||
      raw.symbol !== result.symbol
    )
      throw new Error("RAW与Core/Profile预览身份不一致，已拒绝合并展示。");
    $("stockResult").hidden = false;
    setMessage("研究预览已返回；数值为RAW_UNADJUSTED / NON_PIT。", true);
    $("resultTitle").textContent = `${symbol} · ${v.target_session}`;
    $("resultMode").textContent =
      `${result.status} · ${result.source_object_status} · ${result.target_bar_status} · ${result.price_coordinate} · ${result.identity_status}`;
    $("snapshotId").textContent =
      `TDX来源包ID ${v.source_package_id} · 目标会话 ${v.target_session} · RAW_UNADJUSTED`;
    $("windowText").textContent =
      `窗口 ${result.window.start_session} 至 ${result.window.end_session}：` +
      `实际Bar ${result.window.real_bar_count}/${result.window.requested_sessions}，` +
      `TDX缺Bar/读取失败 ${result.window.source_bar_gap_count}，` +
      `PG未保存历史会话 ${result.window.unavailable_history_row_count}；` +
      `Raw行覆盖 ${result.window.history_records_status}；` +
      `MA20/ATR20价格窗口 ${result.window.price_window_status} ` +
      `(${result.window.ma20_window_valid_bar_count}/${result.window.ma20_window_expected_bar_count}; ` +
      `${result.window.price_window_valid_bar_count}/${result.window.price_window_expected_bar_count})，` +
      `未使用未来会话。缺Bar不证明停牌、退市或上市前。`;
    rawItems(raw.raw, raw.source_version);
    renderFields($("coreRows"), result.core.fields, coreNames);
    renderFields(
      $("allCoreRows"),
      result.core.fields,
      Object.keys(result.core.fields).sort(),
    );
    $("stockProfileRows").replaceChildren();
    for (const component of result.profile_preview.components) {
      const componentKey = component.field_id || "";
      tableRow($("stockProfileRows"), [
        `${profileLabels[componentKey] || componentKey} · ${componentKey}`,
        component.status === "EVALUATED" ? component.quality : component.status,
        component.value,
        component.reason,
      ]);
    }
    try {
      await loadRawPeriods(v, symbol, requestIsCurrent);
    } catch (error) {
      if (!requestIsCurrent()) return;
      renderRawPeriods(null, error.message);
    }
    if (state.timeline) renderTimeline(state.timeline);
    if (
      keepTimeline &&
      state.chartPeriodContext?.target_session === v.target_session &&
      state.chartPeriodContext?.source_package_id === v.source_package_id &&
      state.chartPeriodContext?.symbol === symbol
    ) {
      await queryTimeline({
        origin: previousTimelineContext?.origin || "manual",
        returnSelection: previousTimelineContext?.returnSelection || null,
        returnBinding: previousTimelineContext?.returnBinding || null,
        persistedProfileSnapshot:
          previousTimelineContext?.persistedProfileSnapshot || null,
      });
    }
    return true;
  } catch (error) {
    if (!requestIsCurrent()) return;
    $("stockResult").hidden = Boolean(!keepTimeline || !state.timeline);
    setMessage(error.message);
    if (keepTimeline && state.timeline) renderTimeline(state.timeline);
    return false;
  } finally {
    if (requestIsCurrent()) $("queryStock").disabled = false;
  }
}
