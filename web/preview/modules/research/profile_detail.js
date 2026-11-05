/* Stock A预览页面profile_detail职责模块；保持原业务逻辑和交互。 */
"use strict";
function clearProfileDetail() {
  $("profileDetail").hidden = true;
  $("profileDetail").replaceChildren();
}
function showProfileDetail(item) {
  const root = $("profileDetail");
  root.replaceChildren();
  root.hidden = false;
  appendText(
    root,
    "h3",
    `${item.source_symbol} · ${item.target_session} · 画像详情`,
  );
  const meta = document.createElement("dl");
  meta.className = "profile-detail-meta";
  const details = [
    ["来源范围", item.calculation_scope],
    ["命名空间", item.namespace_id],
    ["研究资格", `${item.identity_status} · ${item.pit_status}`],
    [
      "来源版本定位键",
      `${item.target_session} · ${item.package_sha256} · ${item.profile_revision}`,
    ],
    ["来源包SHA-256", item.package_sha256],
    [
      "画像修订 / 合同SHA",
      `${item.profile_revision} · ${item.producer_contract_sha256}`,
    ],
    [
      "窗口与日历",
      `${item.history_calendar_id} ${item.history_calendar_revision} · ${item.history_window_start} 至 ${item.history_window_end}`,
    ],
    [
      "窗口记录 / 有效价格Bar",
      `${item.history_stored_session_count}/${item.history_requested_session_count} · ${item.history_actual_price_bar_count}`,
    ],
    [
      "MA20 / ATR20实际有效Bar",
      `${item.ma20_window_valid_bar_count}/${item.ma20_window_expected_bar_count} · ${item.atr20_window_valid_bar_count}/${item.atr20_window_expected_bar_count}`,
    ],
    [
      "历史记录覆盖",
      `${item.history_records_status} · ${item.history_records_complete ? "记录齐全" : "存在未持久Raw行"}`,
    ],
    [
      "价格窗口状态",
      `${item.price_window_status} · ${item.price_window_reason || "无"}`,
    ],
    ["窗口输入摘要SHA-256", item.history_window_sha256],
    [
      "量额单位资格",
      `成交额 ${item.amount_unit_status} · 成交量 ${item.volume_unit_status}`,
    ],
  ];
  for (const [label, value] of details) {
    const dt = document.createElement("dt");
    dt.textContent = label;
    const dd = document.createElement("dd");
    dd.textContent = text(value);
    meta.append(dt, dd);
  }
  root.append(meta);
  appendText(root, "h3", "目标日PG Raw OHLC（按需联读）");
  const rawGrid = document.createElement("div");
  rawGrid.className = "raw-grid";
  for (const [label, value] of [
    ["开盘", item.target_raw_open],
    ["最高", item.target_raw_high],
    ["最低", item.target_raw_low],
    ["收盘", item.target_raw_close],
    ["价格质量", item.target_raw_price_quality],
    ["价格原因", item.target_raw_price_reason],
  ]) {
    const card = document.createElement("div");
    appendText(card, "span", label, "raw-label");
    appendText(card, "strong", text(value));
    rawGrid.append(card);
  }
  root.append(rawGrid);
  renderPricePositionDetail(root, item);
  appendText(root, "h3", "Core价格字段 / 质量 / 原因");
  const coreTable = document.createElement("table");
  coreTable.innerHTML =
    "<thead><tr><th>字段</th><th>NUMERIC值</th><th>精确分数</th><th>质量</th><th>原因</th></tr></thead>";
  const coreBody = document.createElement("tbody");
  for (const name of [
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "atr20",
    "clv",
    "prior_high20",
    "ret20",
    "pos60",
    "bias20_atr",
  ]) {
    tableRow(coreBody, [
      `core.${name}`,
      item[name],
      item[`${name}_exact_numerator`] == null
        ? "—"
        : `${item[`${name}_exact_numerator`]}/${item[`${name}_exact_denominator`]}`,
      item[`${name}_quality`],
      item[`${name}_reason`],
    ]);
  }
  coreTable.append(coreBody);
  const coreWrap = document.createElement("div");
  coreWrap.className = "table-wrap";
  coreWrap.append(coreTable);
  root.append(coreWrap);
  appendText(root, "h3", "Profile 11分项状态 / 质量 / 原因");
  const profileTable = document.createElement("table");
  profileTable.innerHTML =
    "<thead><tr><th>字段</th><th>状态</th><th>质量</th><th>原因</th></tr></thead>";
  const profileBody = document.createElement("tbody");
  item.profile_field_ids.forEach((field, index) => {
    tableRow(profileBody, [
      field,
      item.profile_statuses[index],
      item.profile_qualities[index],
      item.profile_reasons[index] || "—",
    ]);
  });
  profileTable.append(profileBody);
  const profileWrap = document.createElement("div");
  profileWrap.className = "table-wrap";
  profileWrap.append(profileTable);
  root.append(profileWrap);
  const actions = document.createElement("div");
  actions.className = "profile-detail-actions";
  const timelineButton = document.createElement("button");
  timelineButton.type = "button";
  timelineButton.textContent = "查看75会话 RAW K / Core";
  timelineButton.addEventListener("click", () => {
    void openProfileTimeline(item);
  });
  actions.append(timelineButton);
  const returnButton = document.createElement("button");
  returnButton.type = "button";
  returnButton.className = "quiet";
  returnButton.textContent = "返回筛选结果";
  returnButton.addEventListener("click", returnToProfileSearch);
  actions.append(returnButton);
  root.append(actions);
}
async function openProfileTimeline(item) {
  const root = $("profileDetail");
  const value = `${item.target_session}|${item.package_sha256}`;
  const version = state.versions.find(
    (candidate) =>
      candidate.session_date === item.target_session &&
      candidate.package_sha256 === item.package_sha256,
  );
  const option = Array.from($("versionSelect").options).find(
    (candidate) => candidate.value === value,
  );
  if (!version || !option || !item.namespace_id || !item.source_set_id) {
    appendText(
      root,
      "p",
      "当前PG目录没有这条画像对应的精确RAW来源版本；已停止跳转，没有回退其它版本。",
      "message",
    );
    return;
  }
  $("versionSelect").value = value;
  $("symbolInput").value = item.source_symbol;
  $("windowInput").value = "75";
  const loaded = await queryStock({ preventDefault() {} });
  if (!loaded) {
    appendText(
      root,
      "p",
      "精确来源版本的RAW / Core读取未完成；画像筛选上下文仍保留。",
      "message",
    );
    return;
  }
  const persistedProfileSnapshot = {
    target_session: item.target_session,
    namespace_id: item.namespace_id,
    source_set_id: item.source_set_id,
    package_sha256: item.package_sha256,
    profile_revision: item.profile_revision,
  };
  await queryTimeline({
    namespace_id: item.namespace_id,
    source_set_id: item.source_set_id,
    origin: "profile",
    persistedProfileSnapshot,
  });
  $("timelinePanel").scrollIntoView({ behavior: "smooth", block: "start" });
}
function returnToProfileSearch() {
  clearProfileDetail();
  $("profileBrowserTitle").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}
