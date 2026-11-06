/* Stock A预览页面comparison_history职责模块；保持原业务逻辑和交互。 */
"use strict";
function validateMa20DirectionHistory(data) {
  const history = data.ma20_direction_history;
  const binding = history?.binding;
  const identityRef = data.snapshot_identity?.identity_sha256;
  if (
    !history ||
    !binding ||
    history.identity_ref !== identityRef ||
    binding.target_session !== data.target_session ||
    binding.source_symbol !== data.symbol ||
    binding.namespace_id !== data.namespace_id ||
    binding.package_sha256 !== data.package_sha256 ||
    binding.calendar_id !== data.calendar_id ||
    binding.calendar_revision !== data.calendar_revision ||
    binding.price_coordinate !== data.price_coordinate ||
    binding.coordinate_id !== data.points?.[0]?.coordinate_id
  )
    throw new Error("MA20方向记录没有绑定当前轨迹、日历和价格坐标。");
  for (const requestedCount of [5, 10]) {
    const item = history.windows?.[String(requestedCount)];
    const actualCount = Math.min(requestedCount, data.points.length);
    const expectedPairCount = Math.max(actualCount - 1, 0);
    const counts = item?.counts || {};
    const pairs = item?.pairs;
    if (
      !item ||
      item.requested_session_count !== requestedCount ||
      item.actual_session_count !== actualCount ||
      item.expected_pair_count !== expectedPairCount ||
      item.pair_count !== expectedPairCount ||
      !Array.isArray(pairs) ||
      pairs.length !== expectedPairCount ||
      ["UP", "SAME", "DOWN", "UNKNOWN"].reduce(
        (total, direction) => total + Number(counts[direction] || 0),
        0,
      ) !== expectedPairCount
    )
      throw new Error("MA20方向窗口的实际官方会话分母或四态计数不完整。");
    const firstPoint = data.points.length - actualCount;
    const observedCounts = { UP: 0, SAME: 0, DOWN: 0, UNKNOWN: 0 };
    for (let index = 0; index < pairs.length; index++) {
      const pair = pairs[index];
      const previous = data.points[firstPoint + index];
      const current = data.points[firstPoint + index + 1];
      if (
        pair.from_session !== previous.session_date ||
        pair.to_session !== current.session_date ||
        !Object.hasOwn(observedCounts, pair.direction)
      )
        throw new Error("MA20方向记录跨越了缺失或非紧邻官方会话。");
      observedCounts[pair.direction]++;
    }
    if (
      ["UP", "SAME", "DOWN", "UNKNOWN"].some(
        (direction) => counts[direction] !== observedCounts[direction],
      )
    )
      throw new Error("MA20方向四态计数与相邻会话明细不一致。");
  }
  return history;
}
function validateComparisonTimeline(data, selection) {
  if (
    data.price_coordinate !== "RAW_UNADJUSTED" ||
    data.symbol !== selection.source_symbol ||
    data.target_session !== selection.target_session ||
    data.package_sha256 !== selection.package_sha256 ||
    data.namespace_id !== selection.namespace_id ||
    data.selection?.source_symbol !== selection.source_symbol ||
    data.selection?.target_session !== selection.target_session ||
    data.selection?.package_sha256 !== selection.package_sha256 ||
    data.selection?.session_count !== 75 ||
    data.selection?.namespace_id !== selection.namespace_id ||
    data.snapshot_identity?.namespace_id !== selection.namespace_id ||
    data.snapshot_identity?.target_session !== selection.target_session ||
    data.snapshot_identity?.package_sha256 !== selection.package_sha256 ||
    data.source_version?.status !== "AVAILABLE" ||
    data.total_session_count > 75 ||
    !Array.isArray(data.points) ||
    data.points.length !== data.total_session_count
  )
    throw new Error(
      "75会话响应的代码、RAW坐标或精确来源包版本不匹配，已拒绝加入对照。",
    );
  validateMa20DirectionHistory(data);
  const dates = data.points.map((point) => point.session_date);
  if (
    dates.some((value, index) => index > 0 && dates[index - 1] >= value) ||
    data.points.some(
      (point) =>
        point.source_symbol !== selection.source_symbol ||
        point.package_sha256 !== selection.package_sha256 ||
        point.namespace_id !== selection.namespace_id,
    )
  )
    throw new Error("轨迹日期或逐日来源身份不完整，已拒绝同日合并。");
  const history = data.price_relation_history;
  const fieldIds = Object.keys(history?.field_statistics || {});
  const observations = history?.observations;
  if (
    !history ||
    history.identity_ref !== data.snapshot_identity.identity_sha256 ||
    fieldIds.length !== 5 ||
    history.comparison_count !==
      Math.max(0, data.total_session_count - 1) * 5 ||
    !Array.isArray(observations) ||
    observations.length !== history.comparison_count
  )
    throw new Error(
      "价格关系 observations 未绑定完整轨迹或官方会话×字段分母，已拒绝多股对齐。",
    );
  const dateIndex = new Map(dates.map((value, index) => [value, index]));
  const observationKeys = new Set();
  for (const observation of observations) {
    const index = dateIndex.get(observation.session_date);
    const key = observation.session_date + "|" + observation.field_id;
    if (
      index === undefined ||
      index < 1 ||
      dates[index - 1] !== observation.previous_official_session ||
      !fieldIds.includes(observation.field_id) ||
      observation.identity_ref !== history.identity_ref ||
      observationKeys.has(key)
    )
      throw new Error(
        "价格关系 observations 存在非紧邻日期、重复字段或不同响应身份，已拒绝多股对齐。",
      );
    observationKeys.add(key);
  }
  if (observationKeys.size !== history.comparison_count)
    throw new Error("价格关系 observation 键数与完整分母不一致。");
  const eventKeys = new Set(
    (history.events || []).map((event) => event.event_key),
  );
  if (
    eventKeys.size !== history.event_count ||
    [...eventKeys].some(
      (eventKey) =>
        !observations.some(
          (observation) =>
            observation.session_date +
              "|" +
              observation.field_id +
              "|" +
              observation.event_kind ===
              eventKey && observation.comparison_status === "RELATION_CHANGED",
        ),
    )
  )
    throw new Error(
      "价格关系 events 不是完整 observations 中的变化子集，已拒绝多股对齐。",
    );
  return data;
}
function validateComparisonRawPeriods(data, selection, timeline) {
  const identity = data?.snapshot_identity;
  const sourceVersion = data?.source_version;
  const responseSelection = data?.selection;
  if (
    data?.ok !== true ||
    data.research_mode !== "NON_PIT_RAW_DIAGNOSTIC" ||
    data.identity_status !== "SOURCE_SYMBOL_ONLY" ||
    data.pit_status !== "NON_PIT_RETROSPECTIVE" ||
    data.price_coordinate !== "RAW_UNADJUSTED" ||
    data.target_session !== selection.target_session ||
    data.source_symbol !== selection.source_symbol ||
    data.symbol !== selection.source_symbol ||
    data.package_sha256 !== selection.package_sha256 ||
    data.namespace_id !== selection.namespace_id ||
    data.calendar_id !== timeline.calendar_id ||
    data.calendar_revision !== timeline.calendar_revision ||
    data.history_window?.start_session !== timeline.session_start ||
    data.history_window?.end_session !== selection.target_session ||
    responseSelection?.source_symbol !== selection.source_symbol ||
    responseSelection?.target_session !== selection.target_session ||
    responseSelection?.package_sha256 !== selection.package_sha256 ||
    responseSelection?.namespace_id !== selection.namespace_id ||
    identity?.identity_sha256 == null ||
    identity.namespace_id !== selection.namespace_id ||
    identity.target_session !== selection.target_session ||
    identity.package_sha256 !== selection.package_sha256 ||
    identity.identity_type !== timeline.snapshot_identity.identity_type ||
    identity.application_id !== timeline.snapshot_identity.application_id ||
    identity.preview_mode !== timeline.snapshot_identity.preview_mode ||
    identity.core_source_sha256 !==
      timeline.snapshot_identity.core_source_sha256 ||
    identity.research_entry_source_sha256 !==
      timeline.snapshot_identity.research_entry_source_sha256 ||
    identity.profile_contract_sha256 !==
      timeline.snapshot_identity.profile_contract_sha256 ||
    identity.price_coordinate !== "RAW_UNADJUSTED" ||
    sourceVersion?.status !== "AVAILABLE" ||
    sourceVersion?.source_capture_id !==
      timeline.source_version?.source_capture_id ||
    sourceVersion?.source_content_id !==
      timeline.source_version?.source_content_id ||
    !Array.isArray(data.periods?.week) ||
    !Array.isArray(data.periods?.month)
  )
    throw new Error(
      "闭周/月响应的来源版本、日历、窗口或RAW坐标与75会话轨迹不一致。",
    );
  for (const [kind, periods] of Object.entries(data.periods)) {
    const seen = new Set();
    for (const period of periods) {
      if (
        period.kind !== kind.toUpperCase() ||
        typeof period.period_key !== "string" ||
        !Number.isSafeInteger(period.calendar_ordinal) ||
        !/^\d{4}-\d{2}-\d{2}$/.test(period.calendar_start_session || "") ||
        !/^\d{4}-\d{2}-\d{2}$/.test(period.calendar_end_session || "") ||
        !Number.isSafeInteger(period.calendar_expected_session_count) ||
        !Number.isSafeInteger(period.elapsed_expected_session_count) ||
        typeof period.calendar_closed !== "boolean" ||
        !["COMPLETE", "PARTIAL", "UNFINISHED"].includes(
          period.coverage_status,
        ) ||
        !Array.isArray(period.reason_codes) ||
        seen.has(period.period_key) ||
        !Array.isArray(period.source_package_sha256s) ||
        period.source_package_sha256s.some(
          (packageSha) => packageSha !== selection.package_sha256,
        )
      )
        throw new Error("闭周/月响应含错序、重复或来源身份不完整的周期行。");
      seen.add(period.period_key);
      for (const field of [
        "stored_raw_row_count",
        "valid_price_bar_count",
        "target_missing_count",
        "read_failed_count",
        "invalid_price_count",
        "missing_history_count",
        "window_truncated_session_count",
      ]) {
        if (!Number.isSafeInteger(period[field]) || period[field] < 0)
          throw new Error("闭周/月响应含无效的来源覆盖计数。");
      }
    }
  }
  return data;
}
function comparisonBasketRequestKey(basket) {
  return basket
    .map(
      (item) =>
        `${item.source_symbol}|${item.target_session}|${item.namespace_id}|${item.package_sha256}|${item.profile_revision}`,
    )
    .join(";");
}
function comparisonRequestMatches(
  requestId,
  currentRequestId,
  requestKey,
  currentKey,
) {
  return requestId === currentRequestId && requestKey === currentKey;
}
function comparisonRequestIsCurrent(requestId, requestKey) {
  return comparisonRequestMatches(
    requestId,
    state.comparisonRequest,
    requestKey,
    comparisonBasketRequestKey(state.comparisonBasket),
  );
}
async function loadComparisonRawPeriods(entries, requestId, requestKey) {
  const periodEntries = await Promise.all(
    entries.map(async (entry) => {
      if (!entry.data) {
        return {
          selection: entry.selection,
          data: null,
          error: entry.error,
          pending: false,
        };
      }
      try {
        const query = new URLSearchParams({
          target_session: entry.selection.target_session,
          package_sha256: entry.selection.package_sha256,
          symbol: entry.selection.source_symbol,
          namespace_id: entry.selection.namespace_id,
        });
        const response = await api(`/api/source-symbol/raw-periods?${query}`);
        return {
          selection: entry.selection,
          data: validateComparisonRawPeriods(
            response,
            entry.selection,
            entry.data,
          ),
          error: null,
          pending: false,
        };
      } catch (error) {
        return {
          selection: entry.selection,
          data: null,
          error: error.message,
          pending: false,
        };
      }
    }),
  );
  if (!comparisonRequestIsCurrent(requestId, requestKey)) return false;
  state.comparisonPeriodEntries = periodEntries;
  renderComparisonPeriodResults();
  return true;
}
async function compareSelectedSymbols() {
  const basket = state.comparisonBasket.map((item) => ({ ...item }));
  if (basket.length < 2 || basket.length > 3) {
    state.comparisonMessage =
      "请选择2至3个来自同一精确研究版本的不同来源代码。";
    renderComparisonBasket();
    return;
  }
  const identity = comparisonIdentityOf(basket[0]);
  if (
    basket.some(
      (item) => !sameComparisonIdentity(comparisonIdentityOf(item), identity),
    )
  ) {
    state.comparisonMessage = "选择篮包含不同来源版本；请清空后重新选择。";
    renderComparisonBasket();
    return;
  }
  const requestId = ++state.comparisonRequest;
  const requestKey = comparisonBasketRequestKey(basket);
  state.comparisonTimelines = [];
  state.comparisonBaseSession = null;
  state.comparisonPeriodEntries = [];
  state.comparisonPeriodBaseKey = null;
  $("comparisonResults").hidden = true;
  $("comparisonStatus").textContent =
    `正在并行读取${basket.length}只股票各75个官方会话…`;
  $("compareSymbols").disabled = true;
  const entries = await Promise.all(
    basket.map(async (selection) => {
      try {
        const data = await api("/api/preview/stock-timeline", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            symbol: selection.source_symbol,
            target_session: selection.target_session,
            package_sha256: selection.package_sha256,
            session_count: 75,
            namespace_id: selection.namespace_id,
          }),
        });
        return {
          selection,
          data: validateComparisonTimeline(data, selection),
          error: null,
        };
      } catch (error) {
        return { selection, data: null, error: error.message };
      }
    }),
  );
  if (!comparisonRequestIsCurrent(requestId, requestKey)) return;
  const reference = entries.find((entry) => entry.data)?.data;
  if (reference) {
    for (const entry of entries) {
      if (
        entry.data &&
        (entry.data.calendar_id !== reference.calendar_id ||
          entry.data.calendar_revision !== reference.calendar_revision ||
          entry.data.session_start !== reference.session_start ||
          entry.data.total_session_count !== reference.total_session_count)
      ) {
        entry.data = null;
        entry.error =
          "官方会话日历版本或窗口与其它代码不一致，已拒绝跨轴对齐。";
      }
    }
  }
  state.comparisonTimelines = entries;
  state.comparisonPeriodEntries = entries.map((entry) => ({
    selection: entry.selection,
    data: null,
    error: entry.data ? null : entry.error,
    pending: Boolean(entry.data),
  }));
  $("comparisonPeriodStatus").textContent =
    "正在按同一目标、精确来源版本和官方日历读取现役PG周/月周期…";
  $("comparisonResults").hidden = false;
  renderComparisonResults();
  $("comparisonResults").scrollIntoView({ behavior: "smooth", block: "start" });
  const periodsLoaded = await loadComparisonRawPeriods(
    entries,
    requestId,
    requestKey,
  );
  if (!periodsLoaded) return;
  renderComparisonBasket();
}

function openComparisonTimeline(sourceSymbol, sessionDate = null) {
  const entry = state.comparisonTimelines.find(
    (item) => item.selection.source_symbol === sourceSymbol,
  );
  if (!entry?.data) return;
  if (!state.comparisonReturnForm) {
    state.comparisonReturnForm = {
      version: $("versionSelect").value,
      symbol: $("symbolInput").value,
      window: $("windowInput").value,
      periodKind: state.comparisonPeriodKind,
      periodBaseKey: state.comparisonPeriodBaseKey,
      eventWindow: state.comparisonEventWindow,
      eventField: state.comparisonEventField,
      eventKind: state.comparisonEventKind,
      formValues: Object.fromEntries(
        Array.from(document.querySelectorAll("input, select"))
          .filter((element) => element.id)
          .map((element) => [element.id, element.value]),
      ),
      pgContext: {
        offset: state.offset,
        limit: state.limit,
        total: state.total,
        profileOffset: state.profileOffset,
        profileLimit: state.profileLimit,
        profileTotal: state.profileTotal,
        changeOffset: state.changeOffset,
        changeLimit: state.changeLimit,
        changeTotal: state.changeTotal,
        relationOffset: state.relationOffset,
        relationLimit: state.relationLimit,
        relationTotal: state.relationTotal,
        relationQuery: state.relationQuery ? { ...state.relationQuery } : null,
        profileBreadthMemberOffset: state.profileBreadthMemberOffset,
        profileBreadthMemberLimit: state.profileBreadthMemberLimit,
        profileBreadthMemberQuery: state.profileBreadthMemberQuery
          ? { ...state.profileBreadthMemberQuery }
          : null,
      },
    };
  }
  state.timelineRequest++;
  state.timeline = entry.data;
  state.timelineContext = {
    target_session: entry.selection.target_session,
    package_sha256: entry.selection.package_sha256,
    symbol: entry.selection.source_symbol,
    session_count: 75,
    namespace_id: entry.selection.namespace_id,
    origin: "comparison",
  };
  state.chartPeriodContext = chartContextForTimeline(entry.data);
  state.chartMode = "day";
  const pointIndex = sessionDate
    ? entry.data.points.findIndex((point) => point.session_date === sessionDate)
    : entry.data.points.findIndex(
        (point) => point.session_date === entry.selection.target_session,
      );
  state.chartSelection =
    pointIndex >= 0 ? { mode: "day", index: pointIndex } : null;
  clearRawPeriodState(
    "已复用多股对照已读取的75会话；闭周/月视图按需读现役PG Raw周期接口。",
  );
  clearResult();
  $("stockResult").classList.add("timeline-only");
  $("stockResult").hidden = false;
  $("resultTitle").textContent =
    `${sourceSymbol} · ${entry.selection.target_session}`;
  $("resultMode").textContent =
    "复用多股对照的现役75会话轨迹 · SOURCE_SYMBOL_ONLY · RAW_UNADJUSTED · NON_PIT";
  $("snapshotId").textContent = entry.selection.package_sha256;
  $("windowText").textContent = "75个官方会话";
  $("versionSelect").value =
    `${entry.selection.target_session}|${entry.selection.package_sha256}`;
  $("symbolInput").value = sourceSymbol;
  $("windowInput").value = "75";
  renderTimeline(entry.data);
  $("timelinePanel").scrollIntoView({ behavior: "smooth", block: "start" });
}
function returnFromComparison() {
  const returnForm = state.comparisonReturnForm;
  state.timelineRequest++;
  state.timeline = null;
  state.timelineContext = null;
  state.chartPeriodContext = null;
  state.chartSelection = null;
  state.chartMode = "day";
  clearRawPeriodState("返回多股对照；原临时选择、t0和筛选分页仍保留。");
  renderTimeline(null);
  clearResult();
  if (returnForm) {
    $("versionSelect").value = returnForm.version;
    $("symbolInput").value = returnForm.symbol;
    $("windowInput").value = returnForm.window;
    state.comparisonPeriodKind = returnForm.periodKind;
    $("comparisonPeriodKind").value = returnForm.periodKind;
    state.comparisonEventWindow = returnForm.eventWindow;
    state.comparisonEventField = returnForm.eventField;
    state.comparisonEventKind = returnForm.eventKind;
    $("comparisonEventWindow").value = String(returnForm.eventWindow);
    $("comparisonEventField").value = returnForm.eventField;
    $("comparisonEventKind").value = returnForm.eventKind;
    for (const [id, value] of Object.entries(returnForm.formValues || {})) {
      const control = $(id);
      if (control) control.value = value;
    }
    Object.assign(state, returnForm.pgContext || {});
  }
  state.comparisonReturnForm = null;
  renderComparisonPeriodResults();
  $("comparisonPeriodComparison").scrollIntoView({
    behavior: "smooth",
    block: "start",
  });
}
