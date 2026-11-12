/* Stock A预览页面history_charts职责模块；保持原业务逻辑和交互。 */
"use strict";
const SVG_NS = "http://www.w3.org/2000/svg";
function svgElement(name, attributes = {}, value = null) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [key, attribute] of Object.entries(attributes))
    node.setAttribute(key, String(attribute));
  if (value !== null) node.textContent = String(value);
  return node;
}
function exactPriceText(field) {
  const value = field?.value;
  if (value && typeof value === "object") {
    if (value.exact !== undefined && value.decimal !== undefined)
      return `${value.exact} (${value.decimal})`;
    return String(value.exact ?? value.decimal ?? "");
  }
  return value === null || value === undefined ? "" : String(value);
}
function chartPriceNumber(field) {
  const value = field?.value;
  if (value && typeof value === "object") {
    if (value.decimal !== undefined) return Number(value.decimal);
    if (value.exact !== undefined) {
      const exact = String(value.exact);
      const [numerator, denominator] = exact.split("/");
      return denominator
        ? Number(numerator) / Number(denominator)
        : Number(exact);
    }
  }
  return Number(value);
}
function validOhlc(fields, quality) {
  if (quality !== "OBSERVED") return null;
  const names = ["open", "high", "low", "close"];
  const result = {};
  for (const name of names) {
    const field = fields?.[name];
    if (field?.quality !== "OBSERVED") return null;
    const number = chartPriceNumber(field);
    const exact = exactPriceText(field);
    if (!Number.isFinite(number) || number <= 0 || !exact) return null;
    result[name] = { number, exact };
  }
  if (
    result.high.number <
      Math.max(result.open.number, result.low.number, result.close.number) ||
    result.low.number >
      Math.min(result.open.number, result.high.number, result.close.number)
  )
    return null;
  return result;
}
function dayOhlc(point) {
  return validOhlc(point.raw_ohlc, point.raw_ohlc?.quality);
}
function periodOhlc(period, binding) {
  const samePackage =
    Array.isArray(period.source_source_package_ids) &&
    period.source_source_package_ids.length > 0 &&
    period.source_source_package_ids.every(
      (value) => value === binding.source_package_id,
    );
  if (
    !samePackage ||
    period.calendar_closed !== true ||
    period.coverage_status !== "COMPLETE"
  )
    return null;
  const fields = Object.fromEntries(
    ["open", "high", "low", "close"].map((name) => [
      name,
      {
        value: period.ohlc?.[name],
        quality: period.ohlc?.[name] ? "OBSERVED" : "UNKNOWN",
      },
    ]),
  );
  return validOhlc(fields, "OBSERVED");
}
function pointDetail(point) {
  const fields = point.raw_ohlc || {};
  const values = dayOhlc(point);
  const source = state.timeline?.source_version || {};
  const root = $("chartDetail");
  renderPriceStructureObservation(point);
  root.replaceChildren();
  appendText(root, "h4", `${point.session_date} · 日K来源与Core观察`);
  const details = document.createElement("dl");
  const pairs = [
    [
      "RAW OHLC",
      values
        ? `O ${values.open.exact} · H ${values.high.exact} · L ${values.low.exact} · C ${values.close.exact}`
        : `不出蜡烛 · ${text(fields.reason || point.price_reason || point.raw_status)}`,
    ],
    [
      "价格质量",
      `${text(fields.quality || point.price_quality)} · ${text(fields.reason || point.price_reason)}`,
    ],
    ["Core MA20", timelineField(point.core_ma20)],
    [
      "收盘相对MA20",
      point.close_vs_ma20
        ? text(point.close_vs_ma20)
        : `UNKNOWN · ${text(point.relation_unknown_reason)}`,
    ],
    [
      "原始坐标 / 时间资格",
      `${state.timeline?.price_coordinate || "RAW_UNADJUSTED"} · ${state.timeline?.pit_status || "NON_PIT_RETROSPECTIVE"} · ${state.timeline?.identity_status || "SOURCE_SYMBOL_ONLY"}`,
    ],
    [
      "目标 / 代码 / 来源包ID",
      `${state.timeline?.target_session || "—"} · ${state.timeline?.symbol || "—"} · ${point.source_package_id}`,
    ],
    [
      "来源捕获ID",
      `${text(point.source_capture_id)} · 来源包 ${text(point.source_package_id)}`,
    ],
    [
      "MA20窗口",
      `${point.ma20_window?.valid_bar_count ?? 0}/${point.ma20_window?.required_valid_bar_count ?? 20}个有效Bar；${point.ma20_window?.official_session_count ?? 0}个官方会话`,
    ],
    [
      "研究边界",
      "NON_PIT来源代码级历史观察，不是复权价格、正式Profile或交易信号。",
    ],
  ];
  for (const [label, value] of pairs) {
    appendText(details, "dt", label);
    appendText(details, "dd", value);
  }
  root.append(details);
  if (!point.raw_row_present) {
    appendText(
      root,
      "p",
      "PG未保存该官方会话Raw行；不可发起单日Core请求。",
      "muted",
    );
    return;
  }
  const button = document.createElement("button");
  button.type = "button";
  button.className = "quiet";
  button.textContent = "查看当日Core / 周月";
  const exactOption = state.versions.some(
    (version) =>
      version.session_date === point.session_date &&
      version.source_package_id === point.source_package_id,
  );
  button.disabled = !exactOption;
  if (exactOption)
    button.addEventListener("click", () => openTimelineSession(point));
  else button.title = "目录中没有相同日期和来源包ID的版本，不能回退其他版本。";
  root.append(button);
}
function periodDetail(period, mode) {
  const root = $("chartDetail");
  root.replaceChildren();
  const type = mode === "week" ? "自然周" : "自然月";
  appendText(
    root,
    "h4",
    `${type} ${period.period_key} · ${period.calendar_start_session}—${period.calendar_end_session}`,
  );
  const ohlc = periodOhlc(period, state.rawPeriodContext || {});
  appendText(
    root,
    "p",
    ohlc
      ? `闭合且完整 · RAW_UNADJUSTED · O ${ohlc.open.exact} · H ${ohlc.high.exact} · L ${ohlc.low.exact} · C ${ohlc.close.exact}`
      : `不出完整周期蜡烛 · ${text(period.coverage_status)} · ${(period.reason_codes || []).map(text).join(" / ") || "周期未闭合或来源覆盖不足"}`,
  );
  appendText(
    root,
    "p",
    `官方会话 ${period.elapsed_expected_session_count}/${period.calendar_expected_session_count} · 有效Bar ${period.valid_price_bar_count} · 缺Bar ${period.target_missing_count} · 读取失败 ${period.read_failed_count} · PG未保存 ${period.missing_history_count} · 来源包ID ${state.rawPeriods?.snapshot_identity?.source_package_id || "UNKNOWN"}`,
    "muted",
  );
  if (!period.daily_rows?.length) {
    appendText(root, "p", "此周期没有可展开的闭合会话明细。", "muted");
    return;
  }
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  const table = document.createElement("table");
  table.className = "period-detail-table";
  const head = document.createElement("thead");
  tableRow(head, [
    "交易会话",
    "Bar / 价格质量",
    "RAW OHLC",
    "来源证据",
    "单日研究",
  ]);
  const body = document.createElement("tbody");
  for (const daily of period.daily_rows) {
    const row = tableRow(body, [
      daily.session,
      `${text(daily.bar_status)} · ${text(daily.price_quality)} · ${text(daily.bar_reason || daily.price_reason)}`,
      daily.valid_price_bar
        ? `O ${text(daily.open)} · H ${text(daily.high)} · L ${text(daily.low)} · C ${text(daily.close)}`
        : "无有效OHLC",
      `${text(daily.source_package_id)} · ${text(daily.source_capture_id)}`,
      "",
    ]);
    const action = row.lastElementChild;
    const point = state.timeline?.points?.find(
      (item) => item.session_date === daily.session,
    );
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.textContent =
      daily.bar_status === "HISTORY_ROW_NOT_STORED"
        ? "PG未保存"
        : "查看当日Core";
    const exactOption = state.versions.some(
      (version) =>
        version.session_date === daily.session &&
        version.source_package_id === state.rawPeriodContext?.source_package_id,
    );
    button.disabled = !point || !point.raw_row_present || !exactOption;
    if (!button.disabled)
      button.addEventListener("click", () => openTimelineSession(point));
    else
      button.title =
        "缺少该会话的已保存Raw行或精确目录版本，不会发起跨版本请求。";
    action.append(button);
  }
  table.append(head, body);
  wrap.append(table);
  root.append(wrap);
}
function chartContextForTimeline(data) {
  return {
    target_session: data?.selection?.target_session || data?.target_session,
    source_package_id:
      data?.selection?.source_package_id || data?.source_package_id,
    symbol: data?.selection?.source_symbol || data?.symbol,
  };
}
function chartSlotLabel(item, mode) {
  if (mode === "day") return item.point.session_date;
  return `${item.period.period_key} · ${item.period.calendar_start_session}—${item.period.calendar_end_session}`;
}
function makeCandleTitle(item, mode) {
  const label = chartSlotLabel(item, mode);
  if (!item.ohlc)
    return `${label} · ${item.quality || "UNKNOWN"} · ${item.reason || "不出蜡烛"}`;
  return `${label} · RAW_UNADJUSTED · O ${item.ohlc.open.exact} · H ${item.ohlc.high.exact} · L ${item.ohlc.low.exact} · C ${item.ohlc.close.exact}`;
}
function renderCandlestickChart(data) {
  const chart = $("candlestickChart");
  const identity = $("chartIdentity");
  const notice = $("chartNotice");
  const detail = $("chartDetail");
  if (!chart || !identity || !notice || !detail) return;
  chart.replaceChildren();
  const mode = state.chartMode;
  for (const [name, buttonId] of Object.entries({
    day: "chartModeDay",
    week: "chartModeWeek",
    month: "chartModeMonth",
  }))
    $(buttonId)?.setAttribute("aria-pressed", String(mode === name));
  if (!data) {
    identity.textContent = "";
    notice.textContent = "查询轨迹后可查看与历史证据绑定的K线图。";
    if (!state.chartSelection) detail.replaceChildren();
    renderPriceStructureObservation(null);
    return;
  }
  const timelineBinding = chartContextForTimeline(data);
  const periodBinding = state.chartPeriodContext || timelineBinding;
  identity.textContent = `${mode === "day" ? "日K轨迹" : mode === "week" ? "自然周K" : "自然月K"} · ${periodBinding.symbol} · 截止 ${periodBinding.target_session} · 来源包ID ${periodBinding.source_package_id} · RAW_UNADJUSTED / NON_PIT / SOURCE_SYMBOL_ONLY`;
  let items = [];
  let periods = [];
  if (mode === "day") {
    items = (data.points || []).map((point) => ({
      point,
      ohlc: dayOhlc(point),
      quality: point.raw_ohlc?.quality || point.price_quality,
      reason: point.raw_ohlc?.reason || point.price_reason || point.raw_status,
    }));
    notice.textContent = `实际官方会话 ${items.length} 个；有效RAW蜡烛 ${items.filter((item) => item.ohlc).length} 个。缺Bar、读取失败、未保存Raw、坏值均保留原会话位置且不补价；黄色线仅连接连续可观测Core MA20。`;
  } else {
    if (!rawPeriodsMatch(periodBinding)) {
      notice.textContent =
        "尚无与当前轨迹日期、来源包ID和来源代码一致的周期结果；切换后按当前身份进行一次只读读取。";
      if (!state.chartSelection) detail.replaceChildren();
      return;
    }
    const periodData = state.rawPeriods;
    periods = periodData.periods?.[mode] || [];
    items = periods.map((period) => ({
      period,
      ohlc: periodOhlc(period, periodBinding),
      quality: period.coverage_status,
      reason: (period.reason_codes || []).join(" / "),
    }));
    const unfinished = periods.filter(
      (period) => period.calendar_closed !== true,
    );
    notice.textContent =
      `目标 ${periodData.target_session} · 闭合周期 ${periods.filter((period) => period.calendar_closed).length} 个 · 完整K线 ${items.filter((item) => item.ohlc).length} 个 · 部分/未知 ${items.filter((item) => item.period.calendar_closed && !item.ohlc).length} 个` +
      (unfinished.length
        ? `。${unfinished.map((period) => `${period.period_key} 尚未闭合，正式OHLC不出图`).join("；")}`
        : "。") +
      "未出完整K线的周期仍占位显示；点击可查看覆盖原因与可用日线来源。";
  }
  if (!items.length) {
    appendText(
      chart,
      "p",
      "官方日历与所选来源上下文没有可显示的周期槽。",
      "muted",
    );
    if (!state.chartSelection) detail.replaceChildren();
    return;
  }
  const validItems = items.filter((item) => item.ohlc);
  if (!validItems.length)
    appendText(
      chart,
      "p",
      "当前真实窗口没有合法且可观测的完整OHLC；图中仅保留缺口，不补造价格。",
      "muted",
    );
  const ma20Numbers =
    mode === "day"
      ? items.map((item) => {
          const field = item.point.core_ma20;
          if (!item.ohlc || field?.quality !== "OBSERVED") return null;
          const number = chartPriceNumber(field);
          return Number.isFinite(number) && number > 0 ? number : null;
        })
      : [];
  const prices = validItems.flatMap((item) => [
    item.ohlc.high.number,
    item.ohlc.low.number,
  ]);
  prices.push(...ma20Numbers.filter((value) => value !== null));
  let low = prices.length ? Math.min(...prices) : 0;
  let high = prices.length ? Math.max(...prices) : 1;
  const padding =
    high === low ? Math.max(Math.abs(high) * 0.01, 0.01) : (high - low) * 0.06;
  if (prices.length) {
    low -= padding;
    high += padding;
  }
  const width = 1000;
  const height = 340;
  const margin = { left: 68, right: 14, top: 18, bottom: 48 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const y = (value) =>
    margin.top + ((high - value) / (high - low)) * plotHeight;
  const slot = plotWidth / items.length;
  const x = (index) => margin.left + (index + 0.5) * slot;
  const svg = svgElement("svg", {
    class: "candlestick-svg",
    viewBox: `0 0 ${width} ${height}`,
    role: "group",
    "aria-label": `${identity.textContent}；图中仅绘制真实且质量合格的RAW OHLC。`,
    preserveAspectRatio: "xMidYMid meet",
  });
  svg.append(svgElement("title", {}, identity.textContent));
  for (let tick = 0; tick <= 4 && prices.length; tick++) {
    const value = high - ((high - low) * tick) / 4;
    const yValue = y(value);
    svg.append(
      svgElement("line", {
        x1: margin.left,
        x2: width - margin.right,
        y1: yValue,
        y2: yValue,
        class: "candle-gridline",
      }),
      svgElement(
        "text",
        {
          x: margin.left - 8,
          y: yValue + 4,
          class: "candle-axis-label",
          "text-anchor": "end",
        },
        value.toFixed(2),
      ),
    );
  }
  if (mode === "day") {
    let segment = [];
    const flushSegment = () => {
      if (segment.length > 1)
        svg.append(
          svgElement("path", { d: segment.join(" "), class: "candle-ma20" }),
        );
      segment = [];
    };
    ma20Numbers.forEach((value, index) => {
      if (value === null) {
        flushSegment();
      } else {
        segment.push(`${segment.length ? "L" : "M"}${x(index)},${y(value)}`);
      }
    });
    flushSegment();
  }
  items.forEach((item, index) => {
    const group = svgElement("g", {
      class: `candle-hit-target ${item.ohlc && item.ohlc.close.number >= item.ohlc.open.number ? "candle-up" : "candle-down"}`,
      role: "button",
      tabindex: "0",
      "aria-label": makeCandleTitle(item, mode),
    });
    group.append(svgElement("title", {}, makeCandleTitle(item, mode)));
    if (item.ohlc) {
      const center = x(index);
      const bodyWidth = Math.max(3, Math.min(11, slot * 0.48));
      const highY = y(item.ohlc.high.number);
      const lowY = y(item.ohlc.low.number);
      const openY = y(item.ohlc.open.number);
      const closeY = y(item.ohlc.close.number);
      group.append(
        svgElement("line", {
          x1: center,
          x2: center,
          y1: highY,
          y2: lowY,
          class: "candle-wick",
        }),
        svgElement("rect", {
          x: center - bodyWidth / 2,
          y: Math.min(openY, closeY),
          width: bodyWidth,
          height: Math.max(2, Math.abs(closeY - openY)),
          class: "candle-body",
        }),
      );
    } else {
      const center = x(index);
      const mid = margin.top + plotHeight / 2;
      group.setAttribute("class", "candle-hit-target candle-gap-mark");
      group.append(
        svgElement("line", {
          x1: center - 3,
          x2: center + 3,
          y1: mid - 4,
          y2: mid + 4,
          class: "candle-gap-mark",
        }),
        svgElement("line", {
          x1: center + 3,
          x2: center - 3,
          y1: mid - 4,
          y2: mid + 4,
          class: "candle-gap-mark",
        }),
      );
    }
    const selectItem = () => {
      state.chartSelection = { mode, index };
      if (mode === "day") {
        pointDetail(item.point);
      } else {
        periodDetail(item.period, mode);
      }
    };
    group.addEventListener("click", selectItem);
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectItem();
      }
    });
    svg.append(group);
    if (
      index === 0 ||
      index === items.length - 1 ||
      index % Math.ceil(items.length / 5) === 0
    ) {
      const label =
        mode === "day"
          ? item.point.session_date.slice(5)
          : item.period.period_key;
      svg.append(
        svgElement(
          "text",
          {
            x: x(index),
            y: height - 15,
            class: "candle-date-label",
            "text-anchor": "middle",
          },
          label,
        ),
      );
    }
  });
  chart.append(svg);
  const legend = document.createElement("div");
  legend.className = "candle-legend";
  appendText(legend, "span", "空心：收盘不低于开盘");
  appendText(legend, "span", "实心：收盘低于开盘");
  appendText(legend, "span", "斜线缺口：无合法可观测OHLC，不补价");
  if (mode === "day")
    appendText(legend, "span", "橙线：Core MA20，仅连接连续有效点");
  chart.append(legend);
  if (state.chartSelection?.mode === mode) {
    const selected = items[state.chartSelection.index];
    if (selected) {
      if (mode === "day") pointDetail(selected.point);
      else periodDetail(selected.period, mode);
    }
  } else if (mode === "day" && items.length) {
    const index = items.length - 1;
    state.chartSelection = { mode, index };
    pointDetail(items[index].point);
  } else {
    detail.replaceChildren();
    appendText(
      detail,
      "p",
      "点击一根K线查看精确价格、周期覆盖、来源版本与单日研究入口。",
      "muted",
    );
  }
}
async function setChartMode(mode) {
  state.chartMode = mode;
  state.chartSelection = null;
  renderCandlestickChart(state.timeline);
  if (mode === "day" || !state.timeline) return;
  const binding =
    state.chartPeriodContext || chartContextForTimeline(state.timeline);
  if (rawPeriodsMatch(binding)) return;
  const version = {
    target_session: binding.target_session,
    source_package_id: binding.source_package_id,
  };
  try {
    await loadRawPeriods(
      version,
      binding.symbol,
      {},
      () =>
        state.timeline !== null &&
        rawPeriodCacheKey(
          state.chartPeriodContext || chartContextForTimeline(state.timeline),
        ) === rawPeriodCacheKey(binding),
    );
  } catch (error) {
    if (state.timeline) renderRawPeriods(null, error.message);
  }
}
function renderTimeline(data, errorMessage = "") {
  const panel = $("timelinePanel");
  const rows = $("timelineRows");
  const status = $("timelineStatus");
  const returnRoot = $("timelineReturn");
  rows.replaceChildren();
  returnRoot.replaceChildren();
  if (!data) {
    panel.hidden = true;
    if (errorMessage) {
      panel.hidden = false;
      status.textContent = `历史轨迹未返回：${errorMessage}`;
      renderTimelineReturnControls(state.timelineContext, selectedVersion());
    }
    renderCandlestickChart(null);
    renderMa20DirectionHistory(null);
    renderPriceRelationHistory(null);
    return;
  }
  panel.hidden = false;
  const counts = Object.entries(data.event_counts || {})
    .map(([event, count]) => `${text(event)} ${count}`)
    .join(" · ");
  status.textContent =
    `${data.symbol} · 截止 ${data.target_session} · ${data.total_session_count} 个官方会话 · ` +
    `Raw行 ${data.raw_row_count} · 有效价格Bar ${data.valid_price_bar_count} · ` +
    `MA20关系可评价 ${data.known_count}/${data.total_session_count} · ` +
    `未知 ${data.unknown_count} · 可比较跨日 ${data.comparable_transition_count} · ` +
    `不可比较 ${data.not_comparable_count} · 首次可评价 ${data.first_evaluable_count}次 ` +
    `（${data.first_evaluable_session || "尚无"}）。` +
    `事件：${counts || "无"}。${data.price_coordinate} / ${data.research_mode}；` +
    `${data.producer || "stock_a.research.core:calculate_core"} · ${data.semantic_version || "CORE_PRIMITIVES_FIELDSET_R2"}。` +
    "身份SOURCE_SYMBOL_ONLY/NON_PIT；选股NOT_EVALUATED/NOT_CONNECTED；严格PIT、复权与RPS资格UNKNOWN；Raw候选数不等于官方上市总体。";
  renderTimelineReturnControls(state.timelineContext, selectedVersion());
  renderMa20DirectionHistory(data.ma20_direction_history);
  renderPriceRelationHistory(data.price_relation_history);
  for (const point of data.points || []) {
    const tr = document.createElement("tr");
    const rawOhlc = point.raw_ohlc;
    const rawOhlcText = rawOhlc
      ? [
          `O ${timelineField(rawOhlc.open)}`,
          `H ${timelineField(rawOhlc.high)}`,
          `L ${timelineField(rawOhlc.low)}`,
          `C ${timelineField(rawOhlc.close)}`,
          rawOhlc.quality === "OBSERVED"
            ? "OBSERVED"
            : `${text(rawOhlc.quality)} · ${text(rawOhlc.reason)}`,
        ].join(" · ")
      : "UNKNOWN · 轨迹未提供RAW OHLC字段";
    cell(tr, point.session_date);
    cell(
      tr,
      `${text(point.raw_status)} · ${text(point.price_quality)}${point.price_reason ? ` · ${point.price_reason}` : ""}`,
    );
    cell(tr, rawOhlcText);
    cell(tr, timelineField(point.raw_close));
    cell(tr, timelineField(point.core_ma20));
    cell(
      tr,
      point.close_vs_ma20
        ? text(point.close_vs_ma20)
        : `UNKNOWN · ${text(point.relation_unknown_reason)}`,
    );
    cell(
      tr,
      `${text(point.comparison?.status)} · ${text(point.comparison?.previous_relation)} → ${text(point.comparison?.event)}${point.comparison?.reason ? ` · ${text(point.comparison.reason)}` : ""}`,
    );
    cell(
      tr,
      `${point.ma20_window.valid_bar_count}/${point.ma20_window.required_valid_bar_count} · ` +
        `MA20 ${point.ma20_window.official_session_count}会话；` +
        `prior_high20 ${point.prior_high20_window.valid_bar_count}/${point.prior_high20_window.required_valid_bar_count} · ` +
        `${point.prior_high20_window.official_session_count}会话`,
    );
    cell(
      tr,
      `${timelineField(point.core_prior_high20)} · ` +
        (point.close_vs_prior_high20
          ? priorHighRelationLabel(point.close_vs_prior_high20)
          : `UNKNOWN · ${text(point.prior_high20_relation_unknown_reason)}`),
    );
    const actionCell = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    const exactOption = state.versions.some(
      (version) =>
        version.session_date === point.session_date &&
        version.source_package_id === point.source_package_id,
    );
    button.disabled = point.raw_row_present !== true || !exactOption;
    if (!point.raw_row_present) {
      button.textContent = "PG未保存，不可展开";
      button.title =
        "该官方会话没有此来源代码的PG Raw行，不会发起单日Core请求。";
    } else if (!exactOption) {
      button.textContent = "缺少精确目录版本";
      button.title = "目录中没有同日期、来源包ID一致的版本，不能回退其他版本。";
    } else {
      button.textContent = "查看当日Core / 周月";
      button.addEventListener("click", () => openTimelineSession(point));
    }
    actionCell.append(button);
    tr.append(actionCell);
    rows.append(tr);
  }
  if (!data.points?.length)
    tableRow(rows, ["无官方会话轨迹", "", "", "", "", "", "", "", "", ""]);
  renderCandlestickChart(data);
}
async function queryTimeline(binding = null) {
  const requestId = ++state.timelineRequest;
  const version = selectedVersion();
  const symbol = $("symbolInput").value.trim();
  const sessionCount = Number($("windowInput").value);
  if (!version || !symbol || !Number.isInteger(sessionCount)) {
    state.timeline = null;
    state.timelineContext = null;
    $("queryTimeline").disabled = false;
    renderTimeline(
      null,
      "请先选择目标日和完整来源包版本，并填写来源代码及会话数。",
    );
    return;
  }
  state.timeline = null;
  state.chartSelection = null;
  clearRawPeriodState("正在按当前轨迹的来源身份读取周期证据。");
  const context = {
    ...version,
    symbol,
    session_count: sessionCount,
    origin: binding?.origin || "manual",
    persistedProfileSnapshot: binding?.persistedProfileSnapshot || null,
    returnSelection: binding?.returnSelection || null,
    returnBinding: binding?.returnBinding || null,
  };
  state.timelineContext = context;
  renderCandlestickChart(null);
  const requestKey = `${version.target_session}|${version.source_package_id}|${symbol}|${sessionCount}`;
  const requestIsCurrent = () => {
    const currentVersion = selectedVersion();
    return (
      requestId === state.timelineRequest &&
      state.timelineContext === context &&
      requestKey ===
        `${currentVersion?.target_session}|${currentVersion?.source_package_id}|${$("symbolInput").value.trim()}|${Number($("windowInput").value)}`
    );
  };
  $("queryTimeline").disabled = true;
  $("timelinePanel").hidden = false;
  $("timelineStatus").textContent =
    "正在按所选来源代码和截止日进行一次有界PG只读历史读取，并逐日复用现役Core…";
  $("timelineRows").replaceChildren();
  try {
    const data = await api("/api/preview/stock-timeline", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        symbol,
        target_session: version.target_session,
        source_package_id: version.source_package_id,
        session_count: sessionCount,
      }),
    });
    if (!requestIsCurrent()) return;
    const relationHistory = data.price_relation_history;
    if (
      data.snapshot_identity?.target_session !== version.target_session ||
      data.snapshot_identity?.source_package_id !== version.source_package_id ||
      data.selection?.source_symbol !== symbol ||
      data.selection?.target_session !== version.target_session ||
      data.selection?.source_package_id !== version.source_package_id
    )
      throw new Error("轨迹响应身份与精确变化快照不一致，已拒绝展示。");
    if (
      !relationHistory ||
      relationHistory.official_session_count !== data.total_session_count ||
      relationHistory.comparison_count !==
        Math.max(0, data.total_session_count - 1) * 5
    )
      throw new Error(
        "价格变化记录没有绑定当前完整轨迹身份或官方会话分母，已拒绝展示。",
      );
    const pointDates = (data.points || []).map((point) => point.session_date);
    for (const item of relationHistory.observations || []) {
      const index = pointDates.indexOf(item.session_date);
      if (index < 1 || pointDates[index - 1] !== item.previous_official_session)
        throw new Error(
          "价格变化记录不是同一精确轨迹中的紧邻官方会话，已拒绝展示。",
        );
    }
    const eventKeys = new Set(
      (relationHistory.events || []).map((item) => item.event_key),
    );
    if (
      [...eventKeys].some(
        (eventKey) =>
          !(relationHistory.observations || []).some(
            (item) =>
              `${item.session_date}|${item.field_id}|${item.event_kind}` ===
                eventKey && item.comparison_status === "RELATION_CHANGED",
          ),
      ) ||
      eventKeys.size !== relationHistory.event_count
    )
      throw new Error("价格变化事件与关系观察集合不一致，已拒绝展示。");
    validateMa20DirectionHistory(data);
    state.timeline = data;
    state.chartPeriodContext = chartContextForTimeline(data);
    renderTimeline(data);
    if (state.chartMode !== "day") {
      try {
        await loadRawPeriods(
          version,
          symbol,
          data.snapshot_identity,
          () => requestIsCurrent() && state.timeline === data,
        );
      } catch (error) {
        if (requestIsCurrent() && state.timeline === data)
          renderRawPeriods(null, error.message);
      }
    }
  } catch (error) {
    if (!requestIsCurrent()) return;
    renderTimeline(null, error.message);
  } finally {
    if (requestIsCurrent()) $("queryTimeline").disabled = false;
  }
}
