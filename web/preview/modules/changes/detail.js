/* Stock A预览页面change_detail职责模块；保持原业务逻辑和交互。 */
"use strict";
function exactPairComparisonCandidate(item) {
  const binding = state.changeBinding;
  const supportsSourceBasket = ["exact-symbol", "change"].includes(
    binding?.origin,
  );
  const snapshot = supportsSourceBasket && binding.after_snapshot;
  if (
    !snapshot?.complete ||
    item.after_present !== true ||
    (binding.origin === "exact-symbol" &&
      item.source_symbol !== binding.symbol) ||
    item.after_source_set_id !== snapshot.source_set_id ||
    item.after_package_sha256 !== snapshot.package_sha256 ||
    item.after_profile_revision !== snapshot.profile_revision
  )
    return null;
  return {
    source_symbol: item.source_symbol,
    target_session: snapshot.target_session,
    namespace_id: binding.namespace_id,
    source_set_id: snapshot.source_set_id,
    package_sha256: snapshot.package_sha256,
    profile_revision: snapshot.profile_revision,
    origin_label:
      binding.origin === "change" ? "双日状态筛查" : "精确双日画像核对",
  };
}
function resolveChangeTimelineSide(item, side, binding, versions) {
  const snapshot = binding?.[`${side}_snapshot`];
  const member = item[`${side}_present`] === true;
  const rawStored = item[`${side}_window_status`] !== "HISTORY_ROW_NOT_STORED";
  const exactVersion = snapshot
    ? versions.find(
        (version) =>
          version.session_date === snapshot.target_session &&
          version.package_sha256 === snapshot.package_sha256,
      )
    : null;
  return { snapshot, member, rawStored, exactVersion };
}
function changeTimelineSnapshot(item, side) {
  return resolveChangeTimelineSide(
    item,
    side,
    state.changeBinding,
    state.versions,
  );
}
function openChangeTimeline(item, side) {
  const { snapshot, member, rawStored, exactVersion } = changeTimelineSnapshot(
    item,
    side,
  );
  if (!member || !snapshot?.complete || !rawStored || !exactVersion) return;
  const selected = selectedVersion();
  const fallback = state.changeBinding?.after_snapshot;
  const returnDate = selected?.target_session || fallback?.target_session;
  const returnSha = selected?.package_sha256 || fallback?.package_sha256;
  const returnVersion = state.versions.find(
    (version) =>
      version.session_date === returnDate &&
      version.package_sha256 === returnSha,
  );
  if (!returnVersion) return;
  const returnBinding =
    state.changeBinding?.origin === "exact-symbol"
      ? {
          ...state.changeBinding,
          before_snapshot: { ...state.changeBinding.before_snapshot },
          after_snapshot: { ...state.changeBinding.after_snapshot },
          change_filter: $("changeTypeSelect").value,
          profile_change_field: $("changeProfileFieldSelect").value,
          profile_change_kind: $("changeProfileKindSelect").value,
          before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
          after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
          change_offset: state.changeOffset,
          change_limit: state.changeLimit,
        }
      : state.changeBinding?.origin === "change"
        ? {
            ...state.changeBinding,
            before_snapshot: { ...state.changeBinding.before_snapshot },
            after_snapshot: { ...state.changeBinding.after_snapshot },
            change_type: $("changeTypeSelect").value,
            profile_field_id: $("changeProfileFieldSelect").value,
            profile_change_kind: $("changeProfileKindSelect").value,
            before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect")
              .value,
            after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
            change_offset: state.changeOffset,
            change_limit: state.changeLimit,
            return_origin: "change-list",
          }
        : null;
  $("versionSelect").value =
    `${snapshot.target_session}|${snapshot.package_sha256}`;
  $("symbolInput").value = item.source_symbol;
  $("windowInput").value = "75";
  state.stockRequest++;
  clearResult();
  $("stockResult").classList.add("timeline-only");
  $("stockResult").hidden = false;
  $("resultTitle").textContent =
    `${item.source_symbol} · ${snapshot.target_session}`;
  $("resultMode").textContent =
    "变化快照关联历史轨迹 · SOURCE_SYMBOL_ONLY · NON_PIT · RAW_UNADJUSTED";
  $("snapshotId").textContent =
    `source_set_id ${snapshot.source_set_id} · namespace ${state.changeBinding.namespace_id} · 包SHA ${snapshot.package_sha256}`;
  $("windowText").textContent =
    "此视图只读取该快照绑定的官方会话轨迹；每日Core/周月详情须从存在Raw行的轨迹点进入。";
  state.timeline = null;
  queryTimeline({
    origin: state.changeBinding?.origin || "change",
    source_set_id: snapshot.source_set_id,
    namespace_id: state.changeBinding.namespace_id,
    persistedProfileSnapshot: {
      target_session: snapshot.target_session,
      package_sha256: snapshot.package_sha256,
      source_set_id: snapshot.source_set_id,
      namespace_id: state.changeBinding.namespace_id,
      profile_revision: item[`${side}_profile_revision`],
      field_ids: item[`${side}_profile_field_ids`] || [],
      statuses: item[`${side}_profile_statuses`] || [],
      qualities: item[`${side}_profile_qualities`] || [],
      reasons: item[`${side}_profile_reasons`] || [],
      semantic_compatible: item.profile_compatible === true,
    },
    returnSelection: {
      target_session: returnVersion.session_date,
      package_sha256: returnVersion.package_sha256,
      namespace_id: state.changeBinding.namespace_id,
      symbol: item.source_symbol,
      session_count: 75,
    },
    returnBinding,
  });
}
function appendChangeTimelineLinks(root, item) {
  appendText(root, "h3", "前后封口快照 · 单股75日Core轨迹");
  const actions = document.createElement("div");
  actions.className = "timeline-change-actions";
  for (const [side, label] of [
    ["before", "起始快照"],
    ["after", "结束快照"],
  ]) {
    const { snapshot, member, rawStored, exactVersion } =
      changeTimelineSnapshot(item, side);
    const action = document.createElement("div");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet";
    button.disabled =
      !member || !snapshot?.complete || !rawStored || !exactVersion;
    button.textContent = snapshot
      ? `查看${label} ${snapshot.target_session} · 75日历史轨迹`
      : `查看${label}历史轨迹`;
    if (!member) {
      button.title = `${label}快照中没有该来源代码。`;
    } else if (!rawStored) {
      button.title = `${label}目标日没有保存该来源代码的Raw行，不能生成历史轨迹。`;
    } else if (!exactVersion) {
      button.title = `${label}快照的日期和完整包SHA不在当前PG目录中，不能回退版本。`;
    } else {
      button.addEventListener("click", () => openChangeTimeline(item, side));
    }
    action.append(button);
    actions.append(action);
  }
  root.append(actions);
  appendText(
    root,
    "p",
    "轨迹按变化快照的日期、完整包SHA、source_set与namespace绑定；缺Bar保留解释，未保存Raw会话不可展开。结果为SOURCE_SYMBOL_ONLY、NON_PIT研究，不表示选股资格。",
    "muted",
  );
}
function showChangeDetail(item) {
  state.changeDetailItem = item;
  const root = $("changeDetail");
  root.replaceChildren();
  root.hidden = false;
  appendText(
    root,
    "h3",
    `${item.source_symbol} · ${item.before_profile_revision || "无前侧记录"} → ${item.after_profile_revision || "无后侧记录"}`,
  );
  if (state.changeBinding?.origin === "breadth") {
    appendText(
      root,
      "p",
      `从来源池广度分类“${breadthMemberQueryLabel(state.changeBinding.breadth_query)}”打开。返回时保留原始分类、精确快照和分页位置。`,
      "muted",
    );
    const back = document.createElement("button");
    back.type = "button";
    back.className = "quiet";
    back.textContent = "返回广度分类成员";
    back.addEventListener("click", returnToProfileBreadthMembers);
    root.append(back);
  }
  if (state.changeBinding?.origin === "relation") {
    appendText(
      root,
      "p",
      `从RAW关系${priceRelationFieldLabels[state.relationQuery?.relation_field] || ""}筛选结果打开；返回会保留A类、B结束日条件、快照身份和偏移${state.relationOffset}。`,
      "muted",
    );
    const back = document.createElement("button");
    back.type = "button";
    back.className = "quiet";
    back.textContent = "返回RAW关系筛选结果";
    back.addEventListener("click", returnToRelationScreen);
    root.append(back);
  }
  if (item.pair_status) {
    const explanation = document.createElement("p");
    explanation.className = "population profile-pair-explanation";
    if (item.state_change_status === "NO_RECORDED_STATE_CHANGE") {
      explanation.textContent =
        "两侧均有已保存画像；所比较字段没有记录到符合当前变化分类的差异。此结果不表示收盘价相同，也不表示正式选股未入选。";
      if (item.raw_close_comparison_status === "DIFFERENT_VALUES")
        explanation.textContent +=
          " 可核RAW收盘数值不同，但未触发当前已记录状态变化。";
      if (
        item.close_ma20_comparison_status === "UNKNOWN" ||
        item.core_comparison_status === "PARTIALLY_COMPARABLE" ||
        item.profile_comparison_status === "PARTIALLY_COMPARABLE"
      )
        explanation.textContent +=
          " 部分价格关系、Core或Profile字段的质量为UNKNOWN/仅部分可比；未记录到分类变化不表示这些数值相同。";
    } else if (item.state_change_status === "RECORDED_CHANGE") {
      explanation.textContent = `现役变化分类记录到：${(item.change_types || []).map((type) => changeTypeLabels[type] || type).join("、")}。这属于来源代码级NON_PIT观察，不代表正式选股结论。`;
    } else if (item.state_change_status === "NOT_COMPARABLE") {
      explanation.textContent =
        "两侧都有画像，但Core或Profile语义不兼容；不可比较字段的空变化标签不能解释为无变化。仍可查看两侧有证据的Raw数值和逐字段质量。";
    } else if (item.pair_status === "BEFORE_ONLY") {
      explanation.textContent =
        "该来源代码只在起始快照有画像记录；没有后侧记录，不能据此推断停牌、退市或正式未入选。";
    } else if (item.pair_status === "AFTER_ONLY") {
      explanation.textContent =
        "该来源代码只在结束快照有画像记录；没有前侧记录，不能据此推断新上市或正式入选。";
    }
    root.append(explanation);
  }
  appendHighlightLookup(root, item);
  const meta = document.createElement("dl");
  meta.className = "profile-detail-meta";
  const pairMeta = item.pair_status
    ? [
        [
          "两侧画像记录",
          profilePairStatusLabels[item.pair_status] || item.pair_status,
        ],
        [
          "当前状态变化分类",
          profilePairStatusLabels[item.state_change_status] ||
            item.state_change_status,
        ],
        [
          "RAW OHLC可比性",
          profilePairStatusLabels[item.raw_ohlc_comparison_status] ||
            item.raw_ohlc_comparison_status,
        ],
        [
          "收盘数值对照",
          profilePairStatusLabels[item.raw_close_comparison_status] ||
            item.raw_close_comparison_status,
        ],
        [
          "Core / Profile数值质量",
          `${profilePairStatusLabels[item.core_comparison_status] || item.core_comparison_status} / ${profilePairStatusLabels[item.profile_comparison_status] || item.profile_comparison_status}`,
        ],
      ]
    : [];
  for (const [label, value] of [
    ...pairMeta,
    [
      "实际变化类型",
      (item.change_types || [])
        .map((type) => changeTypeLabels[type] || type)
        .join("、") || (item.pair_status ? "无（当前分类未记录）" : ""),
    ],
    [
      "Core/Profile语义可比",
      `${item.core_compatible ? "Core可比" : "Core不可比"} · ${item.profile_compatible ? "Profile可比" : "Profile不可比"}`,
    ],
    [
      "官方会话关系",
      sessionRelationLabel(state.changeBinding?.session_relation),
    ],
    [
      "来源身份与坐标",
      `${text(item.before_identity_status)} → ${text(item.after_identity_status)} · ${text(item.before_price_coordinate)} → ${text(item.after_price_coordinate)} · security_id ${text(item.after_stable_security_id_status)} · 上市 ${text(item.after_listing_status)} · 停牌 ${text(item.after_suspension_status)} · PIT ${text(item.after_pit_status)}`,
    ],
    [
      "收盘 / MA20关系",
      `起始快照收盘 ${text(item.before_raw_close)} (${text(item.before_raw_price_quality)} / ${text(item.before_raw_price_reason)}) vs MA20 ${text(item.before_ma20)} (${text(item.before_ma20_quality)} / ${text(item.before_ma20_reason)}) → ${text(item.before_close_ma20_relation)}；结束快照收盘 ${text(item.after_raw_close)} (${text(item.after_raw_price_quality)} / ${text(item.after_raw_price_reason)}) vs MA20 ${text(item.after_ma20)} (${text(item.after_ma20_quality)} / ${text(item.after_ma20_reason)}) → ${text(item.after_close_ma20_relation)}`,
    ],
    [
      "目标Bar / 窗口",
      `${text(item.before_bar_status)} → ${text(item.after_bar_status)} · ${text(item.before_window_status)} ${text(item.before_window_reason)} → ${text(item.after_window_status)} ${text(item.after_window_reason)}`,
    ],
    ["选股状态", "NOT_EVALUATED：来源变化观察不构成入选或未入选判定"],
  ])
    readinessDetail(meta, label, text(value));
  root.append(meta);
  if (
    item.before_bias20_atr_state &&
    item.after_bias20_atr_state &&
    item.bias20_atr_pair_compatibility
  ) {
    appendText(root, "h3", "双日 MA20 / ATR 精确距离状态");
    for (const [side, label] of [
      ["before", "起始"],
      ["after", "结束"],
    ]) {
      const endpointValue = (field) => text(item[`${side}_${field}`]);
      appendText(
        root,
        "p",
        `${label}：${changeBias20AtrStateText(item, side)} · 坐标 ${endpointValue("price_coordinate")} · 目标价质量 ${endpointValue("target_price_quality")} · RAW收盘 ${endpointValue("raw_close")} (${endpointValue("raw_price_quality")} / ${endpointValue("raw_price_reason")}) · MA20 ${endpointValue("ma20")} (${endpointValue("ma20_quality")} / ${endpointValue("ma20_reason")}) · ATR20 ${endpointValue("atr20")} (${endpointValue("atr20_quality")} / ${endpointValue("atr20_reason")})`,
        "population",
      );
    }
    appendText(
      root,
      "p",
      `两端可比性：${bias20AtrStateLabels[item.bias20_atr_pair_compatibility] || item.bias20_atr_pair_compatibility} · ${item.bias20_atr_transition_status} · ${text(item.bias20_atr_pair_reason)}。只有COMPARABLE且状态不同才表示本次两个已存快照的距离状态不同；不构成未来预警或交易信号。`,
      "muted",
    );
  }
  if (item.pair_status) appendProfilePairValues(root, item);
  appendPriceRelationTransitions(root, item);
  appendChangeTimelineLinks(root, item);
  if (["exact-symbol", "change"].includes(state.changeBinding?.origin)) {
    appendText(root, "h3", "加入结束日来源版多股临时对照");
    const candidate = exactPairComparisonCandidate(item);
    const add = makeComparisonAddButton(candidate);
    if (!candidate) {
      add.title =
        "结束快照没有该代码的完整SEALED画像身份，无法加入来源版对照。";
      appendText(
        root,
        "p",
        "该代码不在结束快照或缺少完整SEALED身份；原有临时对照篮与筛选保持不变。",
        "muted",
      );
    }
    root.append(add);
    appendText(
      root,
      "p",
      "需在同一结束日、namespace、source_set、完整包SHA和profile_revision下选2–3只；这是临时来源版对照，不代表正式Focus或选股名单。",
      "muted",
    );
  }
  appendText(root, "h3", "Profile11状态、质量与UNKNOWN原因");
  if (!item.profile_compatible && !item.pair_status) {
    appendText(
      root,
      "p",
      "字段身份或Profile语义不兼容，不能比较前后状态。",
      "muted",
    );
  } else {
    const table = document.createElement("table");
    table.innerHTML =
      "<thead><tr><th>字段</th><th>前日状态 / 质量 / 原因</th><th>后日状态 / 质量 / 原因</th></tr></thead>";
    const body = document.createElement("tbody");
    const fieldValues = (item, side) => {
      const ids = item[`${side}_profile_field_ids`] || [];
      const statuses = item[`${side}_profile_statuses`] || [];
      const qualities = item[`${side}_profile_qualities`] || [];
      const reasons = item[`${side}_profile_reasons`] || [];
      if (
        ids.length !== statuses.length ||
        ids.length !== qualities.length ||
        ids.length !== reasons.length
      )
        return new Map();
      return new Map(
        ids.map((field, index) => [
          field,
          {
            status: statuses[index],
            quality: qualities[index],
            reason: reasons[index],
          },
        ]),
      );
    };
    const beforeFields = fieldValues(item, "before");
    const afterFields = fieldValues(item, "after");
    const fieldIds = [
      ...new Set([
        ...(item.before_profile_field_ids || []),
        ...(item.after_profile_field_ids || []),
      ]),
    ];
    for (const field of fieldIds) {
      const beforeField = beforeFields.get(field);
      const afterField = afterFields.get(field);
      const describe = (value) =>
        value
          ? `${text(value.status)} / ${text(value.quality)} / ${text(value.reason)}`
          : "该侧无此字段";
      tableRow(body, [field, describe(beforeField), describe(afterField)]);
    }
    table.append(body);
    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    wrap.append(table);
    root.append(wrap);
  }
  appendText(root, "h3", "Core质量或原因变化");
  if (item.core_quality_changes?.length) {
    const table = document.createElement("table");
    table.innerHTML =
      "<thead><tr><th>字段</th><th>前日质量 / 原因</th><th>后日质量 / 原因</th></tr></thead>";
    const body = document.createElement("tbody");
    for (const change of item.core_quality_changes)
      tableRow(body, [
        change.field_id,
        `${text(change.before_quality)} / ${text(change.before_reason)}`,
        `${text(change.after_quality)} / ${text(change.after_reason)}`,
      ]);
    table.append(body);
    const wrap = document.createElement("div");
    wrap.className = "table-wrap";
    wrap.append(table);
    root.append(wrap);
  } else appendText(root, "p", "此对象没有Core质量或原因变化。", "muted");
  root.scrollIntoView({ behavior: "smooth", block: "start" });
}
function relationEndpointValue(item, side, field) {
  const isClose = field === "raw_close";
  return {
    value: item[`${side}_${isClose ? "raw_close" : field}`],
    quality:
      item[`${side}_${isClose ? "raw_price_quality" : `${field}_quality`}`],
    reason: item[`${side}_${isClose ? "raw_price_reason" : `${field}_reason`}`],
  };
}
function appendPriceRelationTransitions(root, item) {
  const transitions = item.price_relation_transitions;
  if (!transitions) return;
  appendText(root, "h3", "三类RAW价格关系的两端值与转移原因");
  appendText(
    root,
    "p",
    "收盘与排除当日的前20高点比较的是两个目标日各自的滚动阈值，不代表同一固定阻力位或突破确认。正式选股状态仍为NOT_EVALUATED。",
    "muted",
  );
  const table = document.createElement("table");
  table.innerHTML =
    "<thead><tr><th>关系字段</th><th>起始日数值 / 质量</th><th>结束日数值 / 质量</th><th>两端关系</th><th>转移与具体原因</th></tr></thead>";
  const body = document.createElement("tbody");
  for (const [field, definition] of Object.entries(transitions)) {
    const fields = {
      c_vs_ma20: ["raw_close", "ma20"],
      c_vs_prior_high20: ["raw_close", "prior_high20"],
      ma5_vs_ma20: ["ma5", "ma20"],
    }[field];
    const endpoint = (side) =>
      fields
        .map((valueField) => {
          const value = relationEndpointValue(item, side, valueField);
          return `${valueField}=${text(value.value)} (${text(value.quality)}; ${text(value.reason)})`;
        })
        .join(" vs ");
    tableRow(body, [
      priceRelationFieldLabels[field] || field,
      endpoint("before"),
      endpoint("after"),
      `${priceRelationStatusLabels[definition.before_relation] || definition.before_relation} → ${priceRelationStatusLabels[definition.after_relation] || definition.after_relation}`,
      `${priceRelationTransitionLabels[definition.transition_type] || definition.transition_type}${definition.transition_reason ? `：${definition.transition_reason}` : ""}`,
    ]);
  }
  table.append(body);
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  wrap.append(table);
  root.append(wrap);
}
function appendProfilePairValues(root, item) {
  appendText(root, "h3", "两端RAW OHLC（原生未复权价格）");
  appendText(
    root,
    "p",
    `前侧 ${text(state.changeBinding?.before_snapshot?.target_session)} · Raw行${item.before_raw_record_present ? "存在" : "缺失"} · ${text(item.before_raw_price_quality)} / ${text(item.before_raw_price_reason)}；后侧 ${text(state.changeBinding?.after_snapshot?.target_session)} · Raw行${item.after_raw_record_present ? "存在" : "缺失"} · ${text(item.after_raw_price_quality)} / ${text(item.after_raw_price_reason)}。量额单位未核实，不影响合法RAW OHLC价格展示。`,
    "muted",
  );
  const rawTable = document.createElement("table");
  rawTable.innerHTML =
    "<thead><tr><th>字段</th><th>起始快照</th><th>结束快照</th></tr></thead>";
  const rawBody = document.createElement("tbody");
  for (const [field, label] of [
    ["open", "开盘"],
    ["high", "最高"],
    ["low", "最低"],
    ["close", "收盘"],
  ])
    tableRow(rawBody, [
      label,
      `${text(item[`before_raw_${field}`])} / ${text(item.before_raw_price_quality)} / ${text(item.before_raw_price_reason)}`,
      `${text(item[`after_raw_${field}`])} / ${text(item.after_raw_price_quality)} / ${text(item.after_raw_price_reason)}`,
    ]);
  rawTable.append(rawBody);
  const rawWrap = document.createElement("div");
  rawWrap.className = "table-wrap";
  rawWrap.append(rawTable);
  root.append(rawWrap);
  appendText(root, "h3", "Core两日数值、质量与原因");
  const coreTable = document.createElement("table");
  coreTable.innerHTML =
    "<thead><tr><th>字段</th><th>起始快照：值 / 质量 / 原因</th><th>结束快照：值 / 质量 / 原因</th></tr></thead>";
  const coreBody = document.createElement("tbody");
  for (const [field, label] of Object.entries(corePriceLabels))
    tableRow(coreBody, [
      label,
      `${text(item[`before_${field}`])} / ${text(item[`before_${field}_quality`])} / ${text(item[`before_${field}_reason`])}`,
      `${text(item[`after_${field}`])} / ${text(item[`after_${field}_quality`])} / ${text(item[`after_${field}_reason`])}`,
    ]);
  coreTable.append(coreBody);
  const coreWrap = document.createElement("div");
  coreWrap.className = "table-wrap";
  coreWrap.append(coreTable);
  root.append(coreWrap);
}
async function compareSourceProfilePair(notice = null) {
  if (typeof notice !== "string") notice = null;
  const symbol = $("changeSymbolInput").value.trim().toUpperCase();
  const before = selectedChangeSnapshot("changeBeforeSelect");
  const after = selectedChangeSnapshot("changeAfterSelect");
  const root = $("changeDetail");
  resetChangeDetail();
  const requestId = ++state.pairRequest;
  if (!before || !after || before.target_session >= after.target_session) {
    root.hidden = false;
    appendText(root, "p", "请先选择日期递增的两个精确PG画像快照。", "message");
    return;
  }
  if (!/^(SH|SZ)\.[0-9]{6}$/.test(symbol)) {
    root.hidden = false;
    appendText(
      root,
      "p",
      "请输入SH.或SZ.加六位数字的精确来源代码。",
      "message",
    );
    return;
  }
  const params = new URLSearchParams({
    before_session: before.target_session,
    after_session: after.target_session,
    symbol,
    before_source_set_id: before.source_set_id,
    before_package_sha256: before.package_sha256,
    before_profile_revision: before.profile_revision,
    after_source_set_id: after.source_set_id,
    after_package_sha256: after.package_sha256,
    after_profile_revision: after.profile_revision,
  });
  root.hidden = false;
  appendText(
    root,
    "p",
    "正在读取这两份精确画像快照中的单个来源代码…",
    "population",
  );
  $("compareProfilePair").disabled = true;
  try {
    const data = await api(`/api/source-symbol-price-profile-pair?${params}`);
    if (requestId !== state.pairRequest) return;
    if (data.status === "NO_SAVED_SOURCE_CODE_MATCH") {
      state.changeBinding = {
        namespace_id: data.namespace_id,
        before_snapshot: data.before_snapshot,
        after_snapshot: data.after_snapshot,
        session_relation: data.session_relation,
        offset: 0,
        limit: 1,
        change_type: "ALL",
        symbol,
        before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
        after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
        origin: "exact-symbol",
      };
      root.replaceChildren();
      root.hidden = false;
      appendText(root, "h3", `${symbol} · 本两份已存来源集合没有这个代码`);
      appendText(
        root,
        "p",
        "两份精确PG画像快照中都没有该来源代码的持久画像记录。此结果不判断退市、停牌、官方上市总体或正式选股资格。",
        "population",
      );
      appendHighlightLookup(root, null, data.highlight_lookup);
      appendText(
        root,
        "p",
        `${before.target_session} · source_set ${before.source_set_id} · SHA ${before.package_sha256} · revision ${before.profile_revision} → ${after.target_session} · source_set ${after.source_set_id} · SHA ${after.package_sha256} · revision ${after.profile_revision}`,
        "muted",
      );
      if (notice) appendText(root, "p", notice, "muted");
      return;
    }
    state.changeBinding = {
      namespace_id: data.namespace_id,
      before_snapshot: data.before_snapshot,
      after_snapshot: data.after_snapshot,
      session_relation: data.session_relation,
      offset: 0,
      limit: 1,
      change_type: "ALL",
      symbol,
      before_bias20_atr_state: $("changeBeforeBias20AtrStateSelect").value,
      after_bias20_atr_state: $("changeAfterBias20AtrStateSelect").value,
      origin: "exact-symbol",
    };
    showChangeDetail({
      ...data.item,
      highlight_lookup: data.highlight_lookup,
    });
    if (notice) appendText(root, "p", notice, "muted");
  } catch (error) {
    if (requestId !== state.pairRequest) return;
    root.replaceChildren();
    root.hidden = false;
    appendText(root, "p", `双日画像对照未完成：${error.message}`, "message");
  } finally {
    if (requestId === state.pairRequest)
      $("compareProfilePair").disabled = false;
  }
}
function populateChangeSnapshots(snapshots) {
  populateChangeProfileFields();
  const complete = snapshots
    .filter((item) => item.complete)
    .slice()
    .sort(
      (a, b) =>
        a.target_session.localeCompare(b.target_session) ||
        a.source_set_id.localeCompare(b.source_set_id) ||
        a.profile_revision.localeCompare(b.profile_revision),
    );
  state.changeSnapshots = complete;
  for (const id of ["changeBeforeSelect", "changeAfterSelect"]) {
    const select = $(id);
    select.replaceChildren(new Option("— 选择完整PG画像快照 —", ""));
    for (const item of complete) {
      const option = new Option(
        `${item.target_session} · ${item.source_set_id.slice(0, 8)} · SHA ${item.package_sha256.slice(0, 8)} · rev ${item.profile_revision.slice(0, 12)} · ${item.source_symbol_count}/${item.expected_source_symbol_count}`,
        profileSnapshotKey(item),
      );
      option.title = `${item.target_session} · source_set ${item.source_set_id} · package SHA ${item.package_sha256} · profile revision ${item.profile_revision}`;
      select.append(option);
    }
    select.disabled = complete.length < 2;
  }
  const dates = [...new Set(complete.map((item) => item.target_session))];
  if (dates.length >= 2) {
    const beforeVersions = complete.filter(
      (item) => item.target_session === dates.at(-2),
    );
    const afterVersions = complete.filter(
      (item) => item.target_session === dates.at(-1),
    );
    $("searchChanges").disabled = false;
    if (beforeVersions.length === 1 && afterVersions.length === 1) {
      $("changeBeforeSelect").value = profileSnapshotKey(beforeVersions[0]);
      $("changeAfterSelect").value = profileSnapshotKey(afterVersions[0]);
      loadChanges();
      loadPriceRelations();
    } else {
      $("changeBeforeSelect").value = "";
      $("changeAfterSelect").value = "";
      clearRelationScreen(
        "至少一个日期有多个精确来源版本；请选择唯一前后快照后再查询RAW关系。",
      );
      $("changeSummary").textContent =
        "至少一个日期有多个精确来源版本；请在两个快照选择器中明确选择具体revision。";
      $("changeHighlightsCount").textContent = "等待精确快照";
      $("changeHighlightsSummary").textContent =
        "请选择日期递增的两个精确PG画像快照；不会自动猜测或回退latest。";
    }
  } else {
    $("searchChanges").disabled = true;
    clearRelationScreen(
      "需要两个不同日期且分母闭合的已保存画像版本才能查询RAW关系。",
    );
    $("changeSummary").textContent =
      "需要两个不同日期且分母闭合的已保存画像版本才能比较。";
    $("changeHighlightsCount").textContent = "等待精确快照";
    $("changeHighlightsSummary").textContent =
      "需要两个不同日期且分母闭合的已保存画像版本；未评价不表示没有变化。";
    $("changeHighlightsRows").replaceChildren();
    tableRow($("changeHighlightsRows"), [
      "等待精确的两日画像版本。",
      "",
      "",
      "",
      "",
      "",
    ]);
  }
}
function populateChangeProfileFields() {
  const fieldSelect = $("changeProfileFieldSelect");
  const selectedField = fieldSelect.value;
  fieldSelect.replaceChildren(new Option("不按字段质量/状态变化筛选", ""));
  for (const field of Object.keys(profileFieldLabels)) {
    fieldSelect.append(new Option(profileFieldLabel(field), field));
  }
  fieldSelect.value = Object.hasOwn(profileFieldLabels, selectedField)
    ? selectedField
    : "";
  const kindSelect = $("changeProfileKindSelect");
  kindSelect.disabled = !fieldSelect.value;
  if (!fieldSelect.value) {
    kindSelect.value = "";
  } else if (!kindSelect.value) {
    kindSelect.value = "ANY_STATE_CHANGED";
  }
}
function profileSnapshotKey(snapshot) {
  return [
    snapshot.target_session,
    snapshot.source_set_id,
    snapshot.package_sha256,
    snapshot.profile_revision,
  ].join("|");
}
function selectedChangeSnapshot(id) {
  const key = $(id).value;
  return state.changeSnapshots.find(
    (snapshot) => profileSnapshotKey(snapshot) === key,
  );
}
