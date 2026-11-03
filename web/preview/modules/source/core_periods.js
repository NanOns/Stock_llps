/* Stock A预览页面core_periods职责模块；保持原业务逻辑和交互。 */
"use strict";
function rawItems(raw, formalSourceSet) {
  const pairs = [
    ["目标日Bar", `${text(raw.bar_status)} · ${text(raw.bar_reason)}`],
    [
      "原始OHLC",
      Object.entries(raw.values)
        .filter(([key]) => ["open", "high", "low", "close"].includes(key))
        .map(([key, value]) => `${key}=${text(value)}`)
        .join(" · "),
    ],
    [
      "原始金额",
      `${text(raw.values.amount)} · 字段质量 ${text(raw.quality.amount_quality)} · ` +
        `原值质量 ${text(raw.quality.raw_amount_quality)} · ${text(raw.quality.amount_unit_status)}`,
    ],
    [
      "原始数量",
      `${text(raw.values.volume)} · 字段质量 ${text(raw.quality.volume_quality)} · ` +
        `原值质量 ${text(raw.quality.raw_volume_quality)} · ${text(raw.quality.volume_unit_status)}`,
    ],
    [
      "价格质量",
      `${text(raw.quality.price_quality)} · ${text(raw.quality.price_reason)}`,
    ],
    ["来源包SHA", text(raw.source.package_sha256)],
    [
      "P03 SEALED / P04来源集合",
      `${text(formalSourceSet?.seal_status)} · ${text(formalSourceSet?.source_set_id)} · revision ${text(formalSourceSet?.revision_id)}`,
    ],
    [
      "来源对象 / 目标日Bar",
      `${text(raw.source_object_status)} · ${text(raw.target_bar_status)}`,
    ],
    [
      "来源时钟",
      `${text(raw.source.source_clock_role)} · ${text(raw.source.source_time_quality)}`,
    ],
    [
      "本机观察时钟",
      `${text(raw.source.observed_started_at)} → ${text(raw.source.observed_validated_at)}`,
    ],
    ["来源条目SHA", text(raw.source.source_entry_sha256)],
    [
      "身份与坐标",
      `${raw.identity_status} · ${raw.price_coordinate} · ${raw.pit_status}`,
    ],
    [
      "身份/上市/停复牌状态",
      `${text(raw.stable_security_id_status)} · ${text(raw.listing_status)} · ${text(raw.suspension_status)}`,
    ],
  ];
  const root = $("rawSummary");
  root.replaceChildren();
  for (const [label, value] of pairs) {
    const item = document.createElement("div");
    item.className = "raw-item";
    const span = document.createElement("span");
    span.textContent = label;
    const strong = document.createElement("strong");
    strong.textContent = value;
    item.append(span, strong);
    root.append(item);
  }
}
function renderFields(parent, fields, select) {
  parent.replaceChildren();
  for (const name of select) {
    const field = fields[name] || {};
    tableRow(parent, [name, field.value, field.quality, field.reason]);
  }
}
function formatOhlc(ohlc) {
  if (!ohlc || ohlc.open == null) return "未闭合，不展示部分OHLC";
  return `O ${text(ohlc.open)} · H ${text(ohlc.high)} · L ${text(ohlc.low)} · C ${text(ohlc.close)}`;
}
function renderRawPeriodRows(parent, periods) {
  parent.replaceChildren();
  for (const period of periods || []) {
    const row = tableRow(parent, [
      `${period.period_key} · ${period.calendar_start_session}—${period.calendar_end_session}`,
      `${period.calendar_closed ? "日历已闭合" : "未闭合"} · ${text(period.coverage_status)}${period.reason_codes?.length ? ` · ${period.reason_codes.join(" / ")}` : ""}`,
      formatOhlc(period.ohlc),
      `${period.valid_price_bar_count}/${period.calendar_expected_session_count} · 窗口内 ${period.elapsed_expected_session_count}`,
      `${period.target_missing_count} / ${period.read_failed_count} / ${period.missing_history_count}`,
      "",
    ]);
    const detailCell = row.lastElementChild;
    if (!period.calendar_closed || !period.daily_rows?.length) {
      detailCell.textContent = "无闭合周期明细";
      continue;
    }
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = `展开 ${period.daily_rows.length} 个会话的来源与质量`;
    const wrap = document.createElement("div");
    wrap.className = "table-wrap period-detail-wrap";
    const table = document.createElement("table");
    table.className = "period-detail-table";
    const head = document.createElement("thead");
    tableRow(head, [
      "交易会话",
      "Bar状态",
      "价格质量 / 原因",
      "原始OHLC",
      "包SHA-256",
      "捕获 / 内容 / 条目SHA",
    ]);
    const body = document.createElement("tbody");
    for (const daily of period.daily_rows) {
      const dailyOhlc = daily.valid_price_bar
        ? `O ${text(daily.open)} · H ${text(daily.high)} · L ${text(daily.low)} · C ${text(daily.close)}`
        : "无有效OHLC";
      tableRow(body, [
        daily.session,
        `${text(daily.bar_status)} · ${text(daily.bar_reason)}`,
        `${text(daily.price_quality)} · ${text(daily.price_reason)}`,
        dailyOhlc,
        daily.package_sha256,
        `${text(daily.source_capture_id)} · ${text(daily.source_content_id)} · ${text(daily.source_entry_sha256)}`,
      ]);
    }
    table.append(head, body);
    wrap.append(table);
    details.append(summary, wrap);
    detailCell.append(details);
  }
  if (!periods?.length) {
    tableRow(parent, [
      "无周期",
      "官方日历或Raw历史窗口没有可展示周期。",
      "",
      "",
      "",
      "",
    ]);
  }
}
function rawPeriodBinding(data) {
  return {
    target_session: data.target_session,
    package_sha256: data.package_sha256,
    symbol: data.symbol || data.source_symbol,
    namespace_id: data.namespace_id,
    source_set_id: data.snapshot_identity?.source_set_id ?? null,
  };
}
function rawPeriodCacheKey(binding) {
  return [
    binding.target_session,
    binding.package_sha256,
    binding.symbol,
    binding.namespace_id,
    binding.source_set_id || "NONE",
  ].join("|");
}
function rawPeriodsMatch(binding) {
  if (!state.rawPeriods || !state.rawPeriodContext) return false;
  return (
    rawPeriodCacheKey(state.rawPeriodContext) === rawPeriodCacheKey(binding)
  );
}
function clearRawPeriodState(
  message = "等待精确日期、代码及来源版本的周期查询。",
) {
  state.rawPeriodRequest++;
  state.rawPeriods = null;
  state.rawPeriodContext = null;
  renderRawPeriods(null, message);
}
async function loadRawPeriods(
  version,
  symbol,
  expectedIdentity,
  isCurrent = () => true,
) {
  const binding = {
    target_session: version.target_session,
    package_sha256: version.package_sha256,
    symbol,
    namespace_id: expectedIdentity?.namespace_id || null,
    source_set_id: expectedIdentity?.source_set_id || null,
  };
  if (!binding.namespace_id)
    throw new Error("周期查询缺少已核验的namespace身份。");
  if (!isCurrent()) return null;
  const requestId = ++state.rawPeriodRequest;
  const cacheKey = rawPeriodCacheKey(binding);
  const cached = state.rawPeriodCache.get(cacheKey);
  if (cached) {
    state.rawPeriods = cached;
    state.rawPeriodContext = rawPeriodBinding(cached);
    renderRawPeriods(cached);
    return cached;
  }
  state.rawPeriods = null;
  state.rawPeriodContext = null;
  renderRawPeriods(null, "正在读取精确来源身份下的自然周/月PG Raw…");
  const query = new URLSearchParams({
    target_session: binding.target_session,
    package_sha256: binding.package_sha256,
    symbol: binding.symbol,
    namespace_id: binding.namespace_id,
    source_set_id: binding.source_set_id || "NONE",
  });
  const data = await api(`/api/source-symbol/raw-periods?${query}`);
  if (requestId !== state.rawPeriodRequest || !isCurrent()) return null;
  const actual = rawPeriodBinding(data);
  if (rawPeriodCacheKey(actual) !== cacheKey) {
    throw new Error(
      "周期响应日期、代码、namespace、source_set或完整包SHA不匹配，已拒绝展示。",
    );
  }
  state.rawPeriods = data;
  state.rawPeriodContext = actual;
  state.rawPeriodCache.set(cacheKey, data);
  while (state.rawPeriodCache.size > 6) {
    state.rawPeriodCache.delete(state.rawPeriodCache.keys().next().value);
  }
  renderRawPeriods(data);
  return data;
}
function renderRawPeriods(data, errorMessage = null) {
  state.rawPeriods = data;
  state.rawPeriodContext = data ? rawPeriodBinding(data) : null;
  const status = $("rawPeriodStatus");
  const identity = $("rawPeriodIdentity");
  const trends = $("rawPeriodTrends");
  if (!data) {
    status.textContent = `来源代码级周期观察未返回：${errorMessage || "未知错误"}。每日Core结果仍独立可用。`;
    identity.textContent = "";
    trends.replaceChildren();
    renderRawPeriodRows($("rawWeekRows"), []);
    renderRawPeriodRows($("rawMonthRows"), []);
    renderCandlestickChart(state.timeline);
    return;
  }
  const window = data.history_window;
  status.textContent =
    `来源代码级 ${data.research_mode} · ${data.price_coordinate} · ${data.pit_status}；` +
    `窗口 ${window.start_session} 至 ${window.end_session}，官方会话 ${window.official_session_count}，` +
    `PG Raw 行 ${window.stored_raw_row_count}，有效价格Bar ${window.valid_price_bar_count}，` +
    `缺Bar ${window.target_missing_count}，读取失败 ${window.read_failed_count}，` +
    `未保存会话 ${window.missing_history_sessions.length}。`;
  identity.textContent =
    `${data.source_symbol} · 目标日 ${data.target_session} · 包SHA ${data.package_sha256} · ` +
    `日历 ${data.calendar_revision} · 复权/公司行为 ${data.adjustment_status}/${data.corporate_action_status} · ` +
    `正式闭周/月 ${data.formal_period_profile.closed_week}/${data.formal_period_profile.closed_month}：${data.formal_period_profile.reason}`;
  trends.replaceChildren();
  for (const kind of ["week", "month"]) {
    const item = data.trends[kind];
    const card = document.createElement("div");
    card.className = "period-trend-card";
    appendText(
      card,
      "strong",
      kind === "week" ? "闭合周趋势观察" : "闭合月趋势观察",
    );
    appendText(
      card,
      "span",
      item.status === "OBSERVED"
        ? `${item.value} · ${item.input_period_keys.join(" → ")} · ${item.rule_id}`
        : `UNKNOWN · ${item.complete_period_count}/${item.required_period_count} 个连续完整闭合周期 · ${item.reason}`,
    );
    appendText(
      card,
      "small",
      "仅为NON_PIT_RAW_DIAGNOSTIC；不代表正式Profile或交易信号。",
    );
    trends.append(card);
  }
  renderRawPeriodRows($("rawWeekRows"), data.periods.week);
  renderRawPeriodRows($("rawMonthRows"), data.periods.month);
  renderCandlestickChart(state.timeline);
}
