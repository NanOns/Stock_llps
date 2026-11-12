/* Stock A预览页面profile_comparison职责模块；保持原业务逻辑和交互。 */
"use strict";
function selectedProfileSnapshot() {
  const key = $("profileSnapshotSelect").value;
  return state.profileSnapshots.find(
    (item) => profileSnapshotKey(item) === key,
  );
}
function timelineField(field) {
  if (!field) return "—";
  if (field.quality !== "OBSERVED")
    return `UNKNOWN · ${text(field.reason || field.quality)}`;
  const value = field.value;
  if (value && typeof value === "object")
    return text(value.decimal ?? value.exact ?? value);
  return text(value);
}
function priorHighRelationLabel(relation) {
  const labels = {
    ABOVE: "高于前20会话最高价",
    BELOW: "低于前20会话最高价",
    EQUAL: "等于前20会话最高价",
  };
  return labels[relation] || "UNKNOWN";
}
function comparisonIdentityOf(item) {
  return {
    target_session: item.target_session,
    source_package_id: item.source_package_id,
    profile_revision: item.profile_revision,
  };
}
function sameComparisonIdentity(left, right) {
  if (!left || !right) return false;
  return ["target_session", "source_package_id", "profile_revision"].every(
    (key) => left[key] === right[key],
  );
}
function relationComparisonCandidate(item) {
  const data = state.relationData;
  const snapshot = data?.after_snapshot;
  if (!snapshot) return null;
  return {
    source_symbol: item.source_symbol,
    target_session: snapshot.target_session,
    source_package_id: item.after_source_package_id,
    profile_revision: item.after_profile_revision,
    origin_label: "两日RAW结构结果",
  };
}
function addComparisonCandidate(candidate) {
  if (
    !candidate?.source_symbol ||
    !candidate.target_session ||
    !Number.isSafeInteger(candidate.source_package_id) ||
    candidate.source_package_id <= 0 ||
    typeof candidate.profile_revision !== "string" ||
    !candidate.profile_revision ||
    !/^(SH|SZ)\.[0-9]{6}$/.test(candidate.source_symbol)
  ) {
    state.comparisonMessage =
      "来源行缺少完整研究版本身份或不是SH./SZ.来源代码，未加入。";
    renderComparisonBasket();
    return;
  }
  const identity = comparisonIdentityOf(candidate);
  const currentIdentity = state.comparisonBasket[0]
    ? comparisonIdentityOf(state.comparisonBasket[0])
    : null;
  if (currentIdentity && !sameComparisonIdentity(identity, currentIdentity)) {
    state.comparisonMessage =
      "该来源属于不同目标日、来源包或画像修订；请先清空对照篮再选。";
    renderComparisonBasket();
    return;
  }
  if (
    state.comparisonBasket.some(
      (item) => item.source_symbol === candidate.source_symbol,
    )
  ) {
    state.comparisonMessage = `${candidate.source_symbol} 已在临时对照篮中。`;
    renderComparisonBasket();
    return;
  }
  if (state.comparisonBasket.length >= 3) {
    state.comparisonMessage = "临时对照最多选择3个不同来源代码；请先移除一只。";
    renderComparisonBasket();
    return;
  }
  state.comparisonBasket.push({ ...candidate });
  state.comparisonMessage =
    `已加入 ${candidate.source_symbol}（${candidate.origin_label}）；选择身份固定为 ` +
    `${candidate.target_session} / ${candidate.source_package_id} / ${candidate.profile_revision}。`;
  invalidateComparisonResults(
    "选择已变化；请重新读取同一快照下的75个官方会话。",
  );
  renderComparisonBasket();
}
function makeComparisonAddButton(candidate) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "quiet";
  button.textContent = "加入对照";
  button.disabled = !candidate;
  if (!candidate) button.title = "此行没有完整画像版本身份。";
  button.addEventListener("click", () => addComparisonCandidate(candidate));
  return button;
}
function invalidateComparisonResults(message) {
  state.comparisonRequest++;
  state.comparisonTimelines = [];
  state.comparisonBaseSession = null;
  state.comparisonPeriodEntries = [];
  state.comparisonPeriodBaseKey = null;
  $("comparisonResults").hidden = true;
  $("comparisonEventHistory").hidden = true;
  $("comparisonIdentity").replaceChildren();
  $("comparisonCards").replaceChildren();
  $("comparisonHead").replaceChildren();
  $("comparisonRows").replaceChildren();
  $("comparisonPeriodHead").replaceChildren();
  $("comparisonPeriodRows").replaceChildren();
  $("comparisonPeriodCount").textContent = "等待周期";
  $("comparisonPeriodStatus").textContent = message;
  $("comparisonEventSummaryRows").replaceChildren();
  $("comparisonEventListRows").replaceChildren();
  $("comparisonEventMatrixHead").replaceChildren();
  $("comparisonEventMatrixRows").replaceChildren();
  $("comparisonStatus").textContent = message;
}
function renderComparisonBasket() {
  const root = $("comparisonBasket");
  root.replaceChildren();
  const basket = state.comparisonBasket;
  $("comparisonCount").textContent = `${basket.length}/3`;
  $("compareSymbols").disabled = basket.length < 2 || basket.length > 3;
  $("clearComparison").disabled =
    basket.length === 0 && state.comparisonTimelines.length === 0;
  $("comparisonNotice").textContent =
    state.comparisonMessage ||
    (basket.length
      ? `同一快照 ${basket[0].target_session} · 来源包ID ${basket[0].source_package_id} · profile_revision ${basket[0].profile_revision}。`
      : "从两日RAW关系结果或完整PG画像页加入同一来源快照的代码。");
  if (!basket.length) {
    appendText(root, "span", "尚未选择来源代码。", "muted");
    return;
  }
  for (const item of basket) {
    const chip = document.createElement("div");
    chip.className = "comparison-item";
    appendText(chip, "strong", item.source_symbol);
    appendText(chip, "span", item.origin_label, "muted");
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "quiet";
    remove.textContent = "移除";
    remove.addEventListener("click", () =>
      removeComparisonCandidate(item.source_symbol),
    );
    chip.append(remove);
    root.append(chip);
  }
}
function removeComparisonCandidate(sourceSymbol) {
  state.comparisonBasket = state.comparisonBasket.filter(
    (item) => item.source_symbol !== sourceSymbol,
  );
  state.comparisonMessage = `已移除 ${sourceSymbol}；原筛选条件和分页保持不变。`;
  invalidateComparisonResults(
    "选择已变化；请重新读取同一快照下的75个官方会话。",
  );
  renderComparisonBasket();
}
function clearComparisonBasket(
  message = "临时对照已清空；筛选结果和分页保持不变。",
) {
  if (state.timelineContext?.origin === "comparison") returnFromComparison();
  state.comparisonBasket = [];
  state.comparisonMessage = message;
  invalidateComparisonResults(message);
  renderComparisonBasket();
}
function preserveComparisonOnSnapshotChange(snapshot, sourceLabel) {
  if (!state.comparisonBasket.length) return;
  if (
    snapshot &&
    sameComparisonIdentity(
      snapshot,
      comparisonIdentityOf(state.comparisonBasket[0]),
    )
  )
    return;
  state.comparisonMessage = `${sourceLabel}已切换；临时对照仍绑定原完整快照身份。不同身份的来源代码会被拒绝，原篮和结果保持不变。`;
  renderComparisonBasket();
}
function exactFraction(raw) {
  if (raw === null || raw === undefined) return null;
  const source = String(raw).trim();
  const ratio = source.match(/^([+-]?\d+)\s*\/\s*([+-]?\d+)$/);
  if (ratio) {
    const denominator = BigInt(ratio[2]);
    if (denominator === 0n) return null;
    const sign = denominator < 0n ? -1n : 1n;
    return {
      numerator: BigInt(ratio[1]) * sign,
      denominator: denominator * sign,
    };
  }
  const decimal = source.match(/^([+-]?)(\d+)(?:\.(\d*))?(?:e([+-]?\d+))?$/i);
  if (!decimal) return null;
  const digits = `${decimal[2]}${decimal[3] || ""}`;
  let numerator = BigInt(digits || "0");
  if (decimal[1] === "-") numerator = -numerator;
  let scale = (decimal[3] || "").length - Number(decimal[4] || 0);
  if (!Number.isSafeInteger(scale) || Math.abs(scale) > 100) return null;
  if (scale < 0) {
    numerator *= 10n ** BigInt(-scale);
    scale = 0;
  }
  return { numerator, denominator: 10n ** BigInt(scale) };
}
function rawCloseFraction(field) {
  if (!field || field.quality !== "OBSERVED") return null;
  const value = field.value;
  const exact =
    value && typeof value === "object" ? (value.exact ?? value.decimal) : value;
  const fraction = exactFraction(exact);
  return fraction?.numerator > 0n ? fraction : null;
}
function normalizedIndexFraction(close, base) {
  if (!close || !base || base.numerator <= 0n) return null;
  return {
    numerator: 100n * close.numerator * base.denominator,
    denominator: close.denominator * base.numerator,
  };
}
function formatIndexFraction(fraction) {
  if (!fraction || fraction.numerator < 0n || fraction.denominator <= 0n)
    return "UNKNOWN";
  const doubledCents = fraction.numerator * 200n;
  const cents =
    (doubledCents + fraction.denominator) / (2n * fraction.denominator);
  const whole = cents / 100n;
  const decimal = String(cents % 100n).padStart(2, "0");
  return `${whole}.${decimal}`;
}
function comparisonPeriodCloseFraction(period) {
  if (
    !period ||
    period.calendar_closed !== true ||
    period.coverage_status !== "COMPLETE" ||
    period.reason_codes?.length ||
    period.valid_price_bar_count !== period.calendar_expected_session_count ||
    period.stored_raw_row_count !== period.calendar_expected_session_count ||
    period.elapsed_expected_session_count !==
      period.calendar_expected_session_count ||
    period.window_truncated_session_count !== 0 ||
    period.target_missing_count !== 0 ||
    period.read_failed_count !== 0 ||
    period.invalid_price_count !== 0 ||
    period.missing_history_count !== 0
  )
    return null;
  const close = period.ohlc?.close;
  const fraction = exactFraction(close?.exact ?? close?.decimal);
  return fraction?.numerator > 0n ? fraction : null;
}
function comparisonPeriodMetadataMatches(left, right) {
  return (
    left?.calendar_ordinal === right?.calendar_ordinal &&
    left?.calendar_start_session === right?.calendar_start_session &&
    left?.calendar_end_session === right?.calendar_end_session &&
    left?.calendar_expected_session_count ===
      right?.calendar_expected_session_count &&
    left?.elapsed_expected_session_count ===
      right?.elapsed_expected_session_count
  );
}
function comparisonPeriodAxis(entries, selectionCount, periodKind) {
  const periodName = periodKind === "WEEK" ? "week" : "month";
  const periodsByEntry = entries.map((entry) => {
    const periods = entry.data?.periods?.[periodName] || [];
    return new Map(periods.map((period) => [period.period_key, period]));
  });
  const keys = new Set(
    periodsByEntry.flatMap((periods) => [...periods.keys()]),
  );
  const rows = [...keys]
    .map((key) => {
      const periods = periodsByEntry.map((periods) => periods.get(key) || null);
      const reference = periods.find(Boolean);
      const aligned =
        entries.length === selectionCount &&
        periods.every(
          (period) =>
            period && comparisonPeriodMetadataMatches(period, reference),
        );
      return {
        key,
        periods,
        metadata: reference,
        aligned,
      };
    })
    .sort((left, right) => {
      const ordinalDifference =
        left.metadata.calendar_ordinal - right.metadata.calendar_ordinal;
      if (ordinalDifference) return ordinalDifference;
      return left.key < right.key ? -1 : left.key > right.key ? 1 : 0;
    });
  const baseRow =
    entries.length === selectionCount && entries.every((entry) => entry.data)
      ? rows.find(
          (row) =>
            row.aligned &&
            row.periods.every((period) =>
              comparisonPeriodCloseFraction(period),
            ),
        ) || null
      : null;
  const bases = new Map();
  if (baseRow) {
    baseRow.periods.forEach((period, index) => {
      bases.set(
        entries[index].selection.source_symbol,
        comparisonPeriodCloseFraction(period),
      );
    });
  }
  return {
    rows: rows.map((row) => ({
      ...row,
      cells: entries.map((entry, index) => {
        const period = row.periods[index];
        const close = period?.calendar_closed
          ? exactFraction(
              period.ohlc?.close?.exact ?? period.ohlc?.close?.decimal,
            )
          : null;
        const validClose = comparisonPeriodCloseFraction(period);
        let reason = null;
        let indexFraction = null;
        if (!entry.data) {
          reason = entry.pending
            ? "正在读取该代码的周期数据"
            : `周期不可用：${entry.error || "75会话轨迹未返回"}`;
        } else if (!period) {
          reason = "来源未返回该自然周期";
        } else if (!row.aligned) {
          reason = "官方周期范围与篮内来源不一致，拒绝横向比较";
        } else if (!period.calendar_closed) {
          reason = `自然周期未闭合：${period.reason_codes.join("、") || "官方日历期末晚于目标日"}`;
        } else if (period.coverage_status !== "COMPLETE") {
          reason = `周期覆盖${period.coverage_status}：${period.reason_codes.join("、") || "来源覆盖不完整"}`;
        } else if (!validClose) {
          reason = "完整闭合周期没有合法RAW收盘价";
        } else if (!baseRow) {
          reason = "没有所有篮内代码共同完整闭合的基准周期";
        } else if (
          row.metadata.calendar_ordinal < baseRow.metadata.calendar_ordinal
        ) {
          reason = `早于共同基准${baseRow.key}`;
        } else {
          indexFraction = normalizedIndexFraction(
            validClose,
            bases.get(entry.selection.source_symbol),
          );
          if (!indexFraction) reason = "RAW收盘或共同基准不是正数";
        }
        return { entry, period, close, indexFraction, reason };
      }),
    })),
    baseRow,
    bases,
  };
}
function comparisonPeriodPriceText(value) {
  if (!value) return "UNKNOWN";
  return text(value.decimal ?? value.exact ?? "UNKNOWN");
}
function comparisonFieldText(field) {
  if (!field || field.quality !== "OBSERVED") {
    return `UNKNOWN · ${text(field?.reason || field?.quality || "字段未提供")}`;
  }
  const value = field.value;
  const displayed =
    value && typeof value === "object"
      ? (value.decimal ?? value.exact ?? "—")
      : (value ?? "—");
  return `${text(displayed)} · OBSERVED`;
}
function comparisonRelationText(relation, reason) {
  return relation
    ? `${priceRelationLabels[relation] || text(relation)}`
    : `UNKNOWN · ${text(reason || "关系字段未提供")}`;
}
function comparisonAxis(entries, selectionCount) {
  const valid = entries.filter((entry) => entry.data);
  const sessionDates = Array.from(
    new Set(
      valid.flatMap((entry) =>
        entry.data.points.map((point) => point.session_date),
      ),
    ),
  ).sort();
  const maps = new Map(
    valid.map((entry) => [
      entry.selection.source_symbol,
      new Map(entry.data.points.map((point) => [point.session_date, point])),
    ]),
  );
  let baseSession = null;
  if (valid.length === selectionCount && valid.length >= 2) {
    baseSession =
      sessionDates.find((session) =>
        valid.every((entry) =>
          rawCloseFraction(
            maps.get(entry.selection.source_symbol)?.get(session)?.raw_close,
          ),
        ),
      ) || null;
  }
  const bases = new Map();
  if (baseSession) {
    for (const entry of valid) {
      bases.set(
        entry.selection.source_symbol,
        rawCloseFraction(
          maps.get(entry.selection.source_symbol).get(baseSession)?.raw_close,
        ),
      );
    }
  }
  return { valid, sessionDates, maps, baseSession, bases };
}
function renderComparisonSummary(entry) {
  const card = document.createElement("article");
  card.className = "comparison-card";
  appendText(
    card,
    "h3",
    `${entry.selection.source_symbol} · ${entry.selection.target_session}`,
  );
  if (!entry.data) {
    appendText(card, "p", `75会话未返回：${entry.error}`, "message");
    return card;
  }
  const data = entry.data;
  const target = data.points.find(
    (point) => point.session_date === entry.selection.target_session,
  );
  appendText(
    card,
    "p",
    `RAW收盘 ${comparisonFieldText(target?.raw_close)} · Bar ${text(target?.raw_status)} · ${text(target?.price_reason || "无价格原因")}`,
  );
  appendText(
    card,
    "p",
    `MA20 ${comparisonFieldText(target?.core_ma20)} · C/MA20 ${comparisonRelationText(target?.close_vs_ma20, target?.relation_unknown_reason)}`,
  );
  appendText(
    card,
    "p",
    `严格前20高点 ${comparisonFieldText(target?.core_prior_high20)} · C/前高20 ${comparisonRelationText(target?.close_vs_prior_high20, target?.prior_high20_relation_unknown_reason)}`,
  );
  appendText(
    card,
    "p",
    `MA5/10/20/60排列 ${formatMovingAverageOrder(target?.moving_average_order)} · 有效Bar ${data.valid_price_bar_count}/${data.total_session_count}`,
  );
  appendText(
    card,
    "p",
    `来源包ID ${data.source_package_id} · profile_revision ${entry.selection.profile_revision}`,
  );
  appendText(
    card,
    "p",
    `${data.price_coordinate} · ${data.identity_status} · ${data.pit_status} · 历史行 ${data.raw_row_count}/${data.total_session_count}`,
    "muted",
  );
  return card;
}
function comparisonEventFieldIds(entries) {
  const fieldIds = new Set();
  for (const entry of entries) {
    const history = entry.data?.price_relation_history;
    Object.keys(history?.field_statistics || {}).forEach((fieldId) =>
      fieldIds.add(fieldId),
    );
    (history?.observations || []).forEach((observation) => {
      if (observation.field_id) fieldIds.add(observation.field_id);
    });
  }
  const knownOrder = new Map(
    Object.keys(timelinePriceFieldLabels).map((fieldId, index) => [
      fieldId,
      index,
    ]),
  );
  return Array.from(fieldIds).sort((left, right) => {
    const leftRank = knownOrder.get(left) ?? Number.MAX_SAFE_INTEGER;
    const rightRank = knownOrder.get(right) ?? Number.MAX_SAFE_INTEGER;
    return leftRank - rightRank || left.localeCompare(right);
  });
}
function comparisonEventFieldLabel(fieldId) {
  const label = timelinePriceFieldLabels[fieldId] || fieldId;
  return label + "（" + fieldId + "）";
}
function comparisonEventIndexes(entries) {
  return entries.map((entry) => {
    const observations = new Map();
    for (const observation of entry.data?.price_relation_history
      ?.observations || []) {
      observations.set(
        observation.session_date + "|" + observation.field_id,
        observation,
      );
    }
    return { entry, observations };
  });
}
function comparisonEventFilterOptions(entries) {
  const eventKinds = new Set();
  for (const entry of entries) {
    for (const observation of entry.data?.price_relation_history
      ?.observations || []) {
      if (
        observation.comparison_status === "RELATION_CHANGED" &&
        observation.event_kind
      )
        eventKinds.add(observation.event_kind);
    }
  }
  const options = [
    { value: "ALL", label: "全部状态与事件" },
    { value: "RELATION_CHANGED", label: "所有关系变化" },
    { value: "NO_RELATION_CHANGE", label: "关系未变化" },
    { value: "NOT_COMPARABLE", label: "不可比 / UNKNOWN" },
    { value: "NOT_RECORDED", label: "未知 / 未记录" },
  ];
  for (const eventKind of Array.from(eventKinds).sort()) {
    options.push({
      value: "EVENT:" + eventKind,
      label:
        (priceRelationKindLabels[eventKind] || eventKind) +
        "（" +
        eventKind +
        "）",
    });
  }
  return options;
}
function setComparisonEventSelect(select, options, selectedValue) {
  select.replaceChildren();
  for (const item of options) {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    select.append(option);
  }
  select.value = options.some((item) => item.value === selectedValue)
    ? selectedValue
    : "ALL";
}
function comparisonEventFilterMatches(filter, observation) {
  if (filter === "ALL") return true;
  if (!observation) return filter === "NOT_RECORDED";
  if (filter.startsWith("EVENT:"))
    return (
      observation.comparison_status === "RELATION_CHANGED" &&
      observation.event_kind === filter.slice("EVENT:".length)
    );
  if (filter === "RELATION_CHANGED")
    return observation.comparison_status === "RELATION_CHANGED";
  return observation.comparison_status === filter;
}
function comparisonEventStatusLabel(status) {
  const labels = {
    RELATION_CHANGED: "真实关系变化",
    NO_RELATION_CHANGE: "关系未变化",
    NOT_COMPARABLE: "不可比较 / UNKNOWN",
    NOT_RECORDED: "UNKNOWN / NOT_RECORDED",
    NOT_RETURNED: "轨迹未返回",
  };
  return labels[status] || status;
}
function comparisonEventEvidence(observation) {
  const before = observation.before_relation || "UNKNOWN";
  const after = observation.after_relation || "UNKNOWN";
  const kind =
    observation.event_kind &&
    (priceRelationKindLabels[observation.event_kind] || observation.event_kind);
  const quality =
    "前态 " +
    priceRelationSideText(observation, "before") +
    "；当日 " +
    priceRelationSideText(observation, "after");
  const reason =
    observation.reason_summary || (observation.reason_codes || []).join("、");
  return {
    relation:
      (priceRelationLabels[before] || before) +
      " → " +
      (priceRelationLabels[after] || after),
    kind,
    quality,
    reason,
  };
}
