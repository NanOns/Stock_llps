/* Stock A预览页面change_highlights职责模块；保持原业务逻辑和交互。 */
"use strict";
const changeTypeLabels = {
  BEFORE_ONLY: "仅起始快照存在",
  AFTER_ONLY: "仅结束快照存在",
  TARGET_BAR_STATUS_CHANGED: "目标Bar状态变化",
  PRICE_WINDOW_STATUS_CHANGED: "价格窗口变化",
  RAW_CLOSE_VS_MA20_CHANGED: "收盘/MA20关系变化",
  PROFILE_STATE_CHANGED: "Profile状态/质量变化",
  CORE_QUALITY_CHANGED: "Core质量/原因变化",
};
const bias20AtrStateLabels = {
  ALL: "全部状态（不筛选）",
  FAR_BELOW_ONE: "MA20下方超过1 ATR",
  NEAR_BELOW_ONE: "MA20下方1 ATR以内",
  AT_MA20: "等于MA20",
  NEAR_ABOVE_ONE: "MA20上方1 ATR以内",
  FAR_ABOVE_ONE: "MA20上方超过1 ATR",
  UNKNOWN: "未知（质量或坐标不可用）",
  ABSENT: "无该日持久画像",
  SINGLE_SIDE: "仅单侧有画像",
  PAIR_INCOMPARABLE: "两侧语义或坐标不可比",
  PAIR_UNKNOWN: "两侧质量不足以比较",
  COMPARABLE: "两侧可比",
};
const priceRelationFieldLabels = {
  c_vs_ma20: "收盘与MA20",
  c_vs_prior_high20: "收盘与排除当日的前20高点",
  ma5_vs_ma20: "MA5与MA20",
};
const priceRelationTransitionLabels = {
  ALL: "全部关系",
  STRICT_CROSS_UP: "严格上穿",
  STRICT_CROSS_DOWN: "严格下破",
  EQUAL_BOUNDARY: "经过等值边界",
  SAME_RELATION: "关系未变化",
  NOT_COMPARABLE: "不可比较",
  ONLY_BEFORE: "仅起始日有来源",
  ONLY_AFTER: "仅结束日有来源",
};
const priceRelationStatusLabels = {
  ABOVE: "高于",
  EQUAL: "等于",
  BELOW: "低于",
  UNKNOWN: "不可评价",
};
const priceMa20DirectionLabels = {
  ALL: "全部方向",
  INCREASED: "增加",
  UNCHANGED: "不变",
  DECREASED: "减少",
  UNKNOWN: "UNKNOWN（证据不足）",
};
const priceMa20DirectionReasonLabels = {
  BEFORE_PROFILE_MISSING: "起始日无画像",
  AFTER_PROFILE_MISSING: "结束日无画像",
  PRICE_COORDINATE_MISMATCH: "两端价格坐标不同",
  PRICE_COORDINATE_UNSUPPORTED: "价格坐标不支持",
  CORE_SEMANTICS_INCOMPARABLE: "Core语义不可比较",
  BEFORE_MA20_QUALITY_UNKNOWN: "起始日MA20质量未知",
  AFTER_MA20_QUALITY_UNKNOWN: "结束日MA20质量未知",
  BEFORE_MA20_VALUE_MISSING: "起始日MA20值缺失",
  AFTER_MA20_VALUE_MISSING: "结束日MA20值缺失",
  BEFORE_MA20_VALUE_INVALID: "起始日MA20数值无效",
  AFTER_MA20_VALUE_INVALID: "结束日MA20数值无效",
};
function priceMa20DirectionReasonText(reason) {
  const [code, detail] = String(reason || "UNKNOWN").split(/:(.*)/s);
  const label = priceMa20DirectionReasonLabels[code] || code;
  return detail ? `${label}：${detail}` : label;
}
const profilePairStatusLabels = {
  BOTH_PRESENT: "两侧均有持久画像",
  BEFORE_ONLY: "仅起始快照有画像",
  AFTER_ONLY: "仅结束快照有画像",
  NO_SAVED_SOURCE_CODE_MATCH: "两份已存来源集合都没有这个代码",
  RECORDED_CHANGE: "当前分类记录到状态变化",
  NO_RECORDED_STATE_CHANGE: "当前分类没有记录到状态变化",
  NOT_COMPARABLE: "来源或语义不可比",
  NOT_APPLICABLE: "不适用",
  COMPARABLE: "可比较",
  PARTIALLY_COMPARABLE: "部分可比较",
  UNKNOWN: "未知",
  SAME_VALUE: "数值相同",
  DIFFERENT_VALUES: "数值不同",
};
const corePriceLabels = {
  ma5: "MA5",
  ma10: "MA10",
  ma20: "MA20",
  ma60: "MA60",
  atr20: "ATR20",
  clv: "CLV",
  prior_high20: "排除当前日的前高20",
  ret20: "20会话收益",
  pos60: "60会话位置",
  bias20_atr: "MA20偏离ATR",
};
const highlightCategoryLabels = {
  CLOSE_MA20_BELOW_TO_ABOVE: "收盘由MA20下方转到上方",
  CLOSE_MA20_ABOVE_TO_BELOW: "收盘由MA20上方转到下方",
  CLOSE_MA20_EQUAL_TRANSITION: "收盘与MA20相等关系进入或离开",
  TARGET_BAR_STATUS: "目标Bar状态变化",
  PRICE_WINDOW_STATUS: "价格窗口状态或原因变化",
  PROFILE_STATE: "Profile状态或质量变化",
  CORE_QUALITY: "Core质量或原因变化",
  SOURCE_SET_MEMBERSHIP: "来源集合成员变化",
  RECORDED_CHANGE: "其它已记录变化",
};
function sessionRelationLabel(relation) {
  if (relation?.status === "CONSECUTIVE_OFFICIAL_SESSIONS")
    return `${relation.before_session}（前一官方会话）→ ${relation.after_session}（当日）`;
  if (relation?.status === "NONCONSECUTIVE_OFFICIAL_SESSIONS")
    return `${relation.before_session} → ${relation.after_session}（非相邻官方会话，仅描述所选日期关系变化）`;
  return `${relation?.before_session || "起始快照"} → ${relation?.after_session || "结束快照"}（官方会话相邻性未知，仅描述所选日期关系变化）`;
}
function changeRelationReason(item, relation) {
  const before = text(item.before_close_ma20_relation);
  const after = text(item.after_close_ma20_relation);
  if (
    relation?.status === "CONSECUTIVE_OFFICIAL_SESSIONS" &&
    item.highlight_category === "CLOSE_MA20_BELOW_TO_ABOVE"
  )
    return `前一官方会话收盘低于MA20 → 当日收盘高于MA20（两端价格与MA20均可观察）`;
  if (
    relation?.status === "CONSECUTIVE_OFFICIAL_SESSIONS" &&
    item.highlight_category === "CLOSE_MA20_ABOVE_TO_BELOW"
  )
    return `前一官方会话收盘高于MA20 → 当日收盘低于MA20（两端价格与MA20均可观察）`;
  return `所选两期关系：${before} → ${after}；仅当两端价格与MA20质量均为OBSERVED且Core语义、RAW坐标兼容时分类。`;
}
function highlightQualityText(item) {
  const price = `收盘 ${text(item.before_raw_price_quality)} → ${text(item.after_raw_price_quality)}`;
  const priceReason =
    item.after_raw_price_quality === "OBSERVED"
      ? item.before_raw_price_reason
      : item.after_raw_price_reason || item.before_raw_price_reason;
  const ma20 = `MA20 ${text(item.before_ma20_quality)} → ${text(item.after_ma20_quality)}`;
  const ma20Reason =
    item.after_ma20_quality === "OBSERVED"
      ? item.before_ma20_reason
      : item.after_ma20_reason || item.before_ma20_reason;
  const reasons = [priceReason, ma20Reason, item.after_window_reason]
    .filter(Boolean)
    .map(text);
  return [
    `${price}；${ma20}`,
    `目标Bar ${text(item.before_bar_status)} → ${text(item.after_bar_status)}`,
    `窗口 ${text(item.before_window_status)} → ${text(item.after_window_status)}`,
    `Core语义 ${item.core_compatible ? "兼容" : "UNKNOWN/不可比"} · Profile语义 ${item.profile_compatible ? "兼容" : "UNKNOWN/不可比"}`,
    reasons.length
      ? `原因：${[...new Set(reasons)].join("；")}`
      : "价格原因：—",
  ].join("；");
}
function appendHighlightSourceIdentity(parent, item, data) {
  const source = document.createElement("div");
  source.className = "highlight-identity";
  const short = (value, length = 8) =>
    value ? `${value.slice(0, length)}…` : "UNKNOWN";
  const identitySide = item.after_present ? "after" : "before";
  const identity = document.createElement("span");
  identity.textContent = `${text(item[`${identitySide}_identity_status`])} · SHA ${short(item[`${identitySide}_package_sha256`], 12)} · rev ${short(item.before_profile_revision, 12)}→${short(item.after_profile_revision, 12)}`;
  source.append(identity);
  const qualityIdentity = document.createElement("span");
  qualityIdentity.textContent = `security_id ${text(item[`${identitySide}_stable_security_id_status`])} · 上市 ${text(item[`${identitySide}_listing_status`])} · 停牌 ${text(item[`${identitySide}_suspension_status`])} · ${text(item[`${identitySide}_pit_status`])} · ${text(data.formal_listing_universe || "UNKNOWN_NOT_CONNECTED")}`;
  source.append(qualityIdentity);
  const details = document.createElement("details");
  const summary = document.createElement("summary");
  summary.textContent = "展开完整来源版本身份";
  details.append(summary);
  const full = document.createElement("div");
  full.textContent = `namespace ${data.namespace_id}；前目标日 ${data.before_snapshot.target_session}；后目标日 ${data.after_snapshot.target_session}；前包SHA ${item.before_package_sha256}；后包SHA ${item.after_package_sha256}；前 profile_revision ${item.before_profile_revision}；后 profile_revision ${item.after_profile_revision}；前后坐标 ${text(item.before_price_coordinate)} → ${text(item.after_price_coordinate)}`;
  details.append(full);
  source.append(details);
  parent.append(source);
}
function renderChangeHighlights(data, relation) {
  const root = $("changeHighlightsRows");
  const summary = $("changeHighlightsSummary");
  const count = $("changeHighlightsCount");
  root.replaceChildren();
  const highlights = data.highlights;
  if (!highlights || !Array.isArray(highlights.rows)) {
    count.textContent = "摘要不可用";
    summary.textContent =
      "服务端没有返回同版本重点变化摘要，不能用当前分页替代。";
    tableRow(root, [summary.textContent, "", "", "", "", ""]);
    return;
  }
  const validCounts =
    Number.isInteger(highlights.eligible_count) &&
    Number.isInteger(highlights.display_count) &&
    Number.isInteger(highlights.undisplayed_count) &&
    highlights.display_count === highlights.rows.length &&
    highlights.display_count <= 30 &&
    highlights.eligible_count ===
      highlights.display_count + highlights.undisplayed_count;
  if (!validCounts) {
    count.textContent = "摘要计数异常";
    summary.textContent = "重点变化计数未闭合；已隐藏结果，等待检查。";
    tableRow(root, [summary.textContent, "", "", "", "", ""]);
    return;
  }
  count.textContent = `${highlights.display_count}/30`;
  summary.textContent =
    highlights.eligible_count === 0
      ? "已查询、没有符合展示规则的变化。此结果与未计算、来源不可用分开；30只展示上限不是市场分母。"
      : `同版本变化全集按规则命中 ${highlights.eligible_count} 只；显示 ${highlights.display_count}/30，命中但未显示 ${highlights.undisplayed_count} 只。此列表不是全市场分母，原全集仍可搜索和分页。`;
  highlights.rows.forEach((item, index) => {
    const row = document.createElement("tr");
    cell(row, item.source_symbol);
    const reasonCell = document.createElement("td");
    const reasonList = document.createElement("div");
    reasonList.className = "highlight-reason-list";
    const primary = document.createElement("strong");
    primary.textContent =
      highlightCategoryLabels[item.highlight_category] ||
      item.highlight_category ||
      "已记录变化";
    reasonList.append(primary);
    if (item.highlight_category?.startsWith("CLOSE_MA20_")) {
      const transition = document.createElement("span");
      transition.textContent = changeRelationReason(item, relation);
      reasonList.append(transition);
    }
    const allReasons = document.createElement("span");
    allReasons.textContent = `全部变化原因：${(item.change_types || [])
      .map((type) => changeTypeLabels[type] || type)
      .join("、")}`;
    reasonList.append(allReasons);
    reasonCell.append(reasonList);
    row.append(reasonCell);
    cell(
      row,
      `${text(item.before_close_ma20_relation)} → ${text(item.after_close_ma20_relation)}`,
    );
    const qualityCell = document.createElement("td");
    qualityCell.className = "highlight-quality";
    qualityCell.textContent = highlightQualityText(item);
    row.append(qualityCell);
    const identityCell = document.createElement("td");
    appendHighlightSourceIdentity(identityCell, item, data);
    row.append(identityCell);
    const detail = document.createElement("td");
    const button = document.createElement("button");
    button.type = "button";
    button.className = "quiet highlight-detail-action";
    button.textContent = "查看变化详情";
    button.addEventListener("click", () =>
      showChangeDetail({
        ...item,
        highlight_lookup: {
          status: "DISPLAYED",
          reason: "SAME_PRODUCER_CANDIDATE",
          candidate: true,
          position: index + 1,
          category: item.highlight_category,
          priority: item.highlight_priority,
          eligible_count: highlights.eligible_count,
          display_count: highlights.display_count,
          undisplayed_count: highlights.undisplayed_count,
          display_limit: 30,
        },
      }),
    );
    detail.append(button);
    row.append(detail);
    root.append(row);
  });
  if (!highlights.rows.length)
    tableRow(root, ["已查询，没有符合展示规则的对象。", "", "", "", "", ""]);
}
function resetChangeDetail() {
  state.changeDetailItem = null;
  $("changeDetail").hidden = true;
  $("changeDetail").replaceChildren();
}
function appendHighlightLookup(root, item, lookup = item?.highlight_lookup) {
  if (!lookup) return;
  appendText(root, "h3", "首页重点变化榜核验（最多30条）");
  const messages = {
    DISPLAYED: `同一服务端生产者确认该代码符合重点候选规则，序位第${lookup.position}，已包含在本次榜单展示中。`,
    ELIGIBLE_NOT_DISPLAYED: `同一服务端生产者确认该代码符合重点候选规则，序位第${lookup.position}；超过首页最多${lookup.display_limit}条的展示上限，所以没有出现在首页榜单。`,
    OTHER_CHANGE_NOT_ELIGIBLE: `双日画像记录了${(item?.change_types || []).map((type) => changeTypeLabels[type] || type).join("、") || "实际变化"}；同一服务端重点候选生产者没有将它列为候选。`,
    NOT_TRIGGERED:
      "两端相关质量可比；当前比较语义没有记录会触发重点变化候选的差异。数值可能不同但仍未触发状态变化；这不等于正式选股未入选。",
    NO_SAVED_SOURCE_ROWS:
      "两端已存来源集合都没有这个代码的画像行，因此没有可进入本次重点变化候选的双日来源记录；不据此推断上市、退市、停牌或正式选股资格。",
    UNKNOWN_NOT_VERIFIED:
      "目前无法核实该代码的重点榜资格。存在单侧记录、字段质量UNKNOWN或语义不可比时，不从前30列表缺项推断排名或未计算。",
  };
  const message = messages[lookup?.status] || messages.UNKNOWN_NOT_VERIFIED;
  appendText(root, "p", message, "population highlight-lookup-message");
  if (lookup && Number.isInteger(lookup.eligible_count)) {
    appendText(
      root,
      "p",
      `同一双日版本候选 ${lookup.eligible_count} 只 · 展示 ${lookup.display_count}/${lookup.display_limit} · 符合候选但未展示 ${lookup.undisplayed_count} 只。该数值不是市场股票分母。`,
      "muted",
    );
  }
  if (lookup?.category) {
    appendText(
      root,
      "p",
      `服务端类别：${highlightCategoryLabels[lookup.category] || lookup.category} · 类别优先级 ${lookup.priority}。`,
      "muted",
    );
  }
}
