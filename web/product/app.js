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
  profilesReady: null,
  profileDirectoryStatus: "NOT_LOADED",
  profileDirectoryError: null,
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
  d0Featured: null,
  d0Status: "NOT_LOADED",
  d0Error: null,
  d0BoardPage: 0,
  d0BoardView: "D0",
  d0PulseDirectory: null,
  d0PulseRequest: 0,
  d0Board: null,
  d0BoardOffset: 0,
  d0BoardRequest: 0,
  focusAutoOffset: 0,
  focusAutoGroup: "D0_NEW",
  focusAutoRequest: 0,
  focusAutoBoard: null,
  focusAutoSelectedItem: null,
  focusSegmentOffset: 0,
  focusSegmentEventOffset: 0,
  focusSegmentRule: null,
  focusSegmentRequest: 0,
  focusMarketKey: null,
  focusMarketRequest: 0,
  marketResult: null,
  boardComparison: [],
  boardComparisonRequest: 0,
};
const names = {
  overview: ["今日总览", "从真实变化出发，了解市场中正在发生什么。", "01"],
  sectors: ["板块研究", "从行业与概念进入成员与个股。", "02"],
  stocks: ["个股研究", "查看来源画像、价格结构和规则解释。", "03"],
  focus: ["关注跟踪", "读取来源级冻结规则与后继观察。", "04"],
  market: ["市场研究", "查看来源池宽度与成交参与。", "05"],
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
function marketLookbackSessionsText(sessions) {
  if (!sessions || typeof sessions !== "object" || Array.isArray(sessions))
    return "官方会话窗口不可用";
  const entries = Object.entries(sessions)
    .map(([key, session]) => {
      const match = /^t_minus_(\d+)$/.exec(key);
      const offset = key === "t" ? 0 : match ? Number(match[1]) : null;
      return typeof session === "string" && Number.isInteger(offset)
        ? { offset, session }
        : null;
    })
    .filter((item) => item !== null)
    .sort((left, right) => right.offset - left.offset);
  if (!entries.length) return "官方会话窗口不可用";
  return entries
    .map(({ offset, session }) =>
      offset === 0 ? `目标日 ${session}` : `T-${offset} ${session}`,
    )
    .join(" · ");
}
function marketClassText(value) {
  return (
    {
      STRONG: "强（STRONG）",
      NEUTRAL: "中性（NEUTRAL）",
      WEAK: "弱（WEAK）",
      EXPANDING: "成交扩张（EXPANDING）",
      NORMAL: "成交常态（NORMAL）",
      THIN: "成交偏薄（THIN）",
      UNKNOWN: "未知（UNKNOWN）",
    }[value] ||
    value ||
    "未知（UNKNOWN）"
  );
}
function marketStatusText(value) {
  return (
    {
      COMPLETE: "完整",
      PARTIAL_WITH_UNKNOWN: "部分可评价，含 UNKNOWN",
      UNKNOWN_ZERO_EVALUABLE: "无可评价比值",
      NOT_COMPUTED: "未计算",
      NOT_COMPARABLE: "版本不可比较",
      INCOMPLETE: "事实不完整",
      UNKNOWN: "未知",
    }[value] ||
    value ||
    "未知"
  );
}
function marketUnknownReasonText(reason) {
  return (
    {
      CURRENT_SESSION_UNKNOWN: "目标日成交金额缺失或未知",
      UNKNOWN_SESSION_GAP: "严格前20个actual窗口遇到未知会话缺口",
      PROFILE_NOT_STORED: "来源池代码缺少同版本画像",
    }[reason] || reason
  );
}
function marketBreadthCountText(counts) {
  const labels = {
    COMPARABLE_GAIN: "宽度改善",
    COMPARABLE_LOSS: "宽度转弱",
    COMPARABLE_BOTH_POSITIVE: "两期均正",
    COMPARABLE_BOTH_NONPOSITIVE: "两期均非正",
    UNKNOWN: "不可比较",
  };
  return Object.entries(counts || {})
    .map(([name, count]) => `${labels[name] || name} ${count}`)
    .join(" · ");
}
function marketBreadthReasonText(reasons) {
  const labels = {
    "RET1_T:TARGET_BAR_MISSING": "目标日缺价或未知",
    "RET1_T_MINUS_3:TARGET_BAR_MISSING": "t−3缺价或未知",
  };
  return Object.entries(reasons || {})
    .map(([reason, count]) => `${labels[reason] || reason} ${count}`)
    .join(" · ");
}
function pulseHistoryStateText(value) {
  return (
    {
      TRUE: "PULSE 成立（TRUE）",
      FALSE: "PULSE 未成立（FALSE）",
      UNKNOWN: "PULSE 未知（UNKNOWN）",
      NOT_COMPUTED: "PULSE 未计算",
      INCOMPLETE: "PULSE 输入不完整",
    }[value] ||
    value ||
    "PULSE 未知（UNKNOWN）"
  );
}
function pulseHistoryTransitionText(transition) {
  if (!transition) return "相邻转移：此连续窗口的首日，没有窗口内前一会话。";
  const labels = {
    PULSE_APPEARED: "新出现",
    PULSE_PERSISTED: "持续成立",
    PULSE_CEASED: "本日不再成立",
    PULSE_NOT_PRESENT_BOTH: "连续未成立",
    PULSE_TRANSITION_UNKNOWN: "无法比较（含 UNKNOWN）",
  };
  const reasons = (transition.reason_codes || []).join("、");
  if (transition.status === "AVAILABLE")
    return `相邻转移 ${labels[transition.transition_state] || transition.transition_state}（${transition.previous_state}→${transition.target_state}）${reasons ? ` · ${reasons}` : ""}`;
  return `相邻转移 ${transition.status || "NOT_COMPUTED"}${reasons ? ` · ${reasons}` : ""}`;
}
function pulseHistoryMarketText(market) {
  if (!market) return "同日市场环境：UNKNOWN（没有可读取的同源事实）。";
  const breadth = market.breadth || {};
  const participation = market.participation || {};
  const breadthText =
    breadth.status === "AVAILABLE"
      ? `宽度 ${breadth.eligible_count}/${breadth.source_count} 可比较 · 目标有价 ${breadth.target_present_count} · 缺价 ${breadth.target_missing_count} · 读取失败 ${breadth.read_failed_count} · 分类 ${Object.entries(
          breadth.classification_counts || {},
        )
          .map(([name, count]) => `${name} ${count}`)
          .join("、")}`
      : `宽度 ${breadth.status || "NOT_COMPUTED"} · ${(breadth.reason_codes || []).join("、")}`;
  const participationText =
    participation.status === "AVAILABLE" ||
    participation.status === "PARTIAL_WITH_UNKNOWN"
      ? `成交参与 ${participation.classification || "UNKNOWN"}/${participation.status} · 可评价 ${participation.assessable_count}/${participation.source_pool_count} · UNKNOWN ${participation.profile_unknown_count} · 缺画像 ${participation.profile_missing_count} · 中位数 ${participation.median || "UNKNOWN"} · Profile ${participation.profile_revision || "UNKNOWN"}`
      : `成交参与 ${participation.status || "NOT_COMPUTED"} · ${(participation.reason_codes || []).join("、")}`;
  return `同日市场背景（精确来源捕获）：${breadthText}；${participationText}`;
}
function pulseHistoryIdentityText(session) {
  const identity = session.identity;
  const participationProfile =
    session.market?.participation?.profile_revision || "UNKNOWN";
  if (!identity)
    return `来源包 / capture UNKNOWN · PULSE Profile UNKNOWN · 成交参与 Profile ${participationProfile}`;
  const pulseProfile =
    identity.profile_revision ||
    session.pulse?.profile_revision ||
    "NOT_COMPUTED";
  return `来源包 ${identity.source_package_id} · capture ${identity.source_capture_id} · PULSE Profile ${pulseProfile} · 成交参与 Profile ${participationProfile}`;
}
function renderPulseHistory(result) {
  const root = $("pulseHistory");
  root.replaceChildren(node("h3", "板块 PULSE 多会话演变"));
  root.append(
    node(
      "p",
      "按官方相邻会话顺序读取最多七日的已存事实。当前成员快照回看为 NON_PIT；PULSE三态和价格描述不代表正式 Rotation 或未来预测。",
      "muted",
    ),
  );
  root.append(
    node(
      "p",
      "正式 Rotation early / mature / diffusion 与阶段转移：NOT_EVALUATED。当前跨日 PULSE 事实没有历史冻结成员篮子、Seed / RPS20 分母及前一阶段状态证据，不能据 PULSE 出现或消失推导正式阶段。",
      "inline-status",
    ),
  );
  if (!result || !Array.isArray(result.sessions)) {
    const reasons =
      result?.reason_codes?.join("、") ||
      result?.error ||
      "PULSE_HISTORY_NOT_AVAILABLE";
    root.append(
      node(
        "p",
        `历史序列 ${result?.status || "NOT_COMPUTED"} · ${reasons}`,
        "inline-status",
      ),
    );
    return;
  }
  if (result.status !== "AVAILABLE")
    root.append(
      node(
        "p",
        `历史序列 ${result.status || "PARTIAL"} · ${(result.reason_codes || []).join("、")}`,
        "inline-status",
      ),
    );
  const items = node("ol", null, "pulse-history-list");
  for (const session of result.sessions || []) {
    const item = node("li", null, "pulse-history-row");
    const pulse = session.pulse || {};
    const heading = node("div", null, "pulse-history-heading");
    heading.append(
      node("strong", session.target_session),
      node(
        "span",
        session.status === "AVAILABLE"
          ? pulseHistoryStateText(pulse.state)
          : `PULSE ${session.status || "NOT_COMPUTED"}`,
      ),
    );
    item.append(heading);
    if (session.status === "AVAILABLE") {
      const pulseReasons = (pulse.reason_codes || []).join("、");
      item.append(
        node(
          "p",
          `dq5 ${pulse.dq5_pp?.decimal_4dp ?? "UNKNOWN"} 个百分点 · breadth Δ ${pulse.breadth_delta1?.decimal_4dp ?? "UNKNOWN"} · 成员 ${pulse.member_count ?? "UNKNOWN"}（已映射 ${pulse.mapped_count ?? "UNKNOWN"}/未映射 ${pulse.unmapped_count ?? "UNKNOWN"}）${pulseReasons ? ` · 原因 ${pulseReasons}` : ""}`,
        ),
        node("p", pulseHistoryTransitionText(session.transition)),
        node("p", pulseHistoryMarketText(session.market)),
      );
      const outcomes = (session.followups || []).map((outcome) => {
        const coverage = `固定 ${outcome.fixed_member_denominator ?? "UNKNOWN"} · 有效 ${outcome.observed_member_count ?? "UNKNOWN"} · UNKNOWN ${outcome.unknown_member_count ?? "UNKNOWN"}`;
        if (outcome.status === "OBSERVED")
          return `T+${outcome.horizon} ${outcome.target_session} 等权收盘 ${outcome.return_percent_4dp ?? "UNKNOWN"}%（${coverage}）`;
        const target = outcome.target_session
          ? ` ${outcome.target_session}`
          : "";
        const reasons = (outcome.reason_codes || []).join("、");
        return `T+${outcome.horizon}${target} ${outcome.status || "UNKNOWN"}（${coverage}）${reasons ? ` · ${reasons}` : ""}`;
      });
      item.append(
        node(
          "p",
          outcomes.length
            ? `固定T0成员篮后继（RAW_UNADJUSTED）：${outcomes.join("；")}`
            : "固定T0成员篮后继：NOT_COMPUTED。",
        ),
        node(
          "small",
          `${pulseHistoryIdentityText(session)} · ${session.pulse.membership_mode} · Rotation ${session.pulse.formal_rotation_state}`,
          "muted",
        ),
      );
    } else {
      item.append(
        node(
          "p",
          (session.reason_codes || []).join("、") || "精确PULSE事实不可用。",
        ),
        node("p", pulseHistoryTransitionText(session.transition)),
        node("p", pulseHistoryMarketText(session.market)),
        node("small", pulseHistoryIdentityText(session), "muted"),
      );
    }
    items.append(item);
  }
  root.append(items);
  root.append(
    node(
      "p",
      "下方成员按同一当前快照与当前研究日展示；点击成员可继续查看个股与关注观察。",
      "muted",
    ),
  );
}
function boardKindLabel(kind) {
  return kind === "INDUSTRY" ? "行业" : kind === "CONCEPT" ? "概念" : "板块";
}
function evidenceStateLabel(value) {
  return (
    {
      TRUE: "TRUE",
      FALSE: "FALSE",
      UNKNOWN: "UNKNOWN",
      NOT_COMPUTED: "未计算",
    }[value] || "UNKNOWN"
  );
}
function t0EarlyContexts(t0) {
  return Array.isArray(t0?.enrollments)
    ? t0.enrollments.flatMap((entry) =>
        Array.isArray(entry.early_contexts) ? entry.early_contexts : [],
      )
    : [];
}
function enrollmentScopeText(entry) {
  if (entry?.fact_type !== "RULE")
    return `来源捕获 ${entry?.source_capture_id ?? "UNKNOWN"}`;
  return `${entry.rule_version || "UNKNOWN_RULE_VERSION"} · Profile ${entry.profile_revision || "UNKNOWN"} · 历史 ${entry.history_session_count ?? "UNKNOWN"}/${entry.d1_history_session_count ?? "UNKNOWN"} · 事件窗 ${entry.event_window_start || "UNKNOWN"} · 捕获 ${entry.source_capture_id ?? "UNKNOWN"} · 日历 ${entry.calendar_revision || "UNKNOWN"}`;
}
function enrollmentFollowupText(followup) {
  if (!followup) return "NOT_COMPUTED / NO_STORED_LANDMARK";
  const identity = followup.fact_id
    ? ` · 同规则事实 #${followup.fact_id} ${followup.state || "UNKNOWN"} · Profile ${followup.profile_revision || "UNKNOWN"} · 历史 ${followup.history_session_count ?? "UNKNOWN"}/${followup.d1_history_session_count ?? "UNKNOWN"} · 事件窗 ${followup.event_window_start || "UNKNOWN"} · 捕获 ${followup.source_capture_id ?? "UNKNOWN"} · 日历 ${followup.calendar_revision || "UNKNOWN"}`
    : "";
  const reasons = Array.isArray(followup.rule_reasons)
    ? followup.rule_reasons.join("、")
    : "";
  return `${followup.status || "NOT_COMPUTED"} / ${followup.reason || "NO_STORED_LANDMARK"}${identity}${reasons ? ` · 规则原因 ${reasons}` : ""}`;
}
function enrollmentFollowupHistoryDetails(entry) {
  const followup = entry?.followup;
  if (!followup) return null;
  const history = Array.isArray(followup.history) ? followup.history : [];
  const statusLabels = {
    CONTINUES: "持续符合",
    EXIT_PROVED: "已证退出",
    PAUSED_UNKNOWN: "未知暂停",
    UNKNOWN: "状态未知",
    LEFT_CENSORED: "未知间隔后恢复匹配",
    FIRST_OBSERVED_LEFT_CENSORED: "左截断后首次观察为匹配",
    REENTRY: "已证退出后再次匹配",
    NO_MATCH: "当前不匹配",
    NOT_DUE: "尚未到期",
    INCOMPLETE: "观察窗口不完整",
    REENTRY_NOT_LINKED: "观察到匹配但再入父段未能证明",
  };
  const reasonLabels = {
    COMPARABLE_TRUE: "相邻可比事实为 TRUE",
    COMPARABLE_FALSE: "相邻可比事实为 FALSE",
    OBSERVED_FALSE_AFTER_UNKNOWN: "未知后观察到 FALSE",
    CURRENT_STATE_UNKNOWN: "当前事实状态为 UNKNOWN",
    NO_STORED_FACT: "该会话没有已存事实",
    RULE_CONTEXT_NOT_COMPARABLE: "规则上下文不可比较",
    AMBIGUOUS_OR_NONADJACENT_FACT: "事实不唯一或不是合法相邻事实",
    REENTRY_NOT_PROVED_AFTER_UNKNOWN: "未知间隔后重见 TRUE，无法证明重入",
    MATCH_AFTER_UNKNOWN_INTERVAL: "未知间隔后恢复匹配，连续性无法证明",
    MATCH_AFTER_PROVED_EXIT: "已证退出后再次匹配",
    CURRENT_STATE_FALSE: "当前事实为 FALSE",
    WAITING_FOR_FUTURE_SESSION: "等待后续官方交易会话",
    REENTRY_PARENT_NOT_PROVED: "未保存可核验的直接父段关系",
  };
  const details = node("details", null, "focus-enrollment-history");
  details.append(
    node(
      "summary",
      `${entry.kind === "DAILY_LANDMARK" ? "每日观察点" : "观察段"} #${entry.id} 逐会话状态（${history.length} 个后继会话）`,
    ),
  );
  if (!history.length) {
    details.append(
      node(
        "p",
        `${statusLabels[followup.status] || followup.status || "未计算"} · ${reasonLabels[followup.reason] || followup.reason || "尚无后继事实"} · 暂无已到期后继会话。`,
      ),
    );
  } else {
    const rows = node("ul");
    for (const item of history) {
      const status =
        item.status === "REENTRY" && entry.fact_type === "D0"
          ? "D0观察状态恢复（不是规则再入组）"
          : statusLabels[item.status] || item.status || "未知";
      const reason = reasonLabels[item.reason] || item.reason || "原因未记录";
      const state = item.state ? ` · 规则状态 ${item.state}` : "";
      const linkedSegment =
        item.status === "REENTRY" &&
        entry.fact_type === "RULE" &&
        item.enrollment_id
          ? ` · 新段登记 #${item.enrollment_id} · 父段 #${item.parent_enrollment_id ?? "UNKNOWN"}`
          : item.status === "REENTRY_NOT_LINKED"
            ? " · 没有可核验的新段父ID"
            : "";
      rows.append(
        node(
          "li",
          `${item.session || "日期未知"} · ${status}（${item.status || "UNKNOWN"}） · ${reason}${state}${linkedSegment}`,
        ),
      );
    }
    details.append(rows);
  }
  const forwardPaths = Array.isArray(followup.forward_paths)
    ? followup.forward_paths
    : [];
  details.append(node("strong", "同一登记 T0 的来源级后继价格"));
  if (forwardPaths.length) {
    const pathRows = node("ul");
    const forwardStatusLabels = {
      OBSERVED: "已观察到期端点",
      UNKNOWN: "UNKNOWN",
      NOT_DUE: "未到期",
      NOT_COMPUTED: "未计算",
    };
    for (const path of forwardPaths) {
      const value =
        path.status === "OBSERVED"
          ? exactPercentMetric({
              quality: path.endpoint_quality,
              numerator: path.endpoint_numerator,
              denominator: path.endpoint_denominator,
            })
          : `${forwardStatusLabels[path.status] || path.status || "UNKNOWN"}（${(path.reason_codes || []).join("、") || "无原因码"}）`;
      pathRows.append(
        node(
          "li",
          `T+${path.horizon} ${path.due_session || "到期会话未知"} · ${forwardStatusLabels[path.status] || path.status || "UNKNOWN"} · RAW_UNADJUSTED 收盘变化 ${value} · 端点 ${path.endpoint_quality || "UNKNOWN"} · 路径 ${path.path_quality || "UNKNOWN"}${path.maturity ? ` · ${path.maturity}` : ""}${path.reason_codes?.length ? ` · ${path.reason_codes.join("、")}` : ""} · ${path.path_rule_version || "UNKNOWN_PATH_VERSION"} / 日历 ${path.calendar_revision || "UNKNOWN"}`,
        ),
      );
    }
    details.append(pathRows);
  } else {
    details.append(
      node("p", "T+1/T+3/T+5 后继路径未读取：官方日历键或来源捕获不可用。"),
    );
  }
  if (followup.history_truncated) {
    details.append(
      node("p", "仅列出最早的 20 个官方后继会话；更远窗口未在本次读取。"),
    );
  }
  return details;
}
function enrollmentKindLabel(kind) {
  const kindLabels = {
    ENTRY: "已证入组",
    FIRST_OBSERVED_LEFT_CENSORED: "左截断观察段",
    DAILY_LANDMARK: "每日观察点（不是新入组）",
    REENTRY: "已证再入组",
  };
  return kindLabels[kind] || kind || "UNKNOWN";
}
function enrollmentParentText(entry) {
  return entry?.parent_enrollment_id
    ? ` · 父观察段 #${entry.parent_enrollment_id}`
    : "";
}
function enrollmentSummaryText(entry) {
  return `#${entry.id} ${entry.rule_id || entry.fact_type} ${enrollmentKindLabel(entry.kind)}${enrollmentParentText(entry)} · ${entry.reason || "UNKNOWN"} · ${enrollmentScopeText(entry)} · 后继 ${enrollmentFollowupText(entry.followup)}`;
}
function exactRatioText(numerator, denominator, unit = "") {
  if (numerator == null || denominator == null || Number(denominator) === 0)
    return "UNKNOWN";
  const value = Number(numerator) / Number(denominator);
  const decimal = Number.isFinite(value) ? value.toFixed(2) : "UNKNOWN";
  return `${decimal}${unit}（${numerator}/${denominator}）`;
}
function exactPercentMetric(metric) {
  if (
    metric?.quality !== "OBSERVED" ||
    metric.numerator == null ||
    metric.denominator == null ||
    Number(metric.denominator) === 0
  )
    return `UNKNOWN（${metric?.quality || "NO_STORED_VALUE"}${metric?.reason ? ` · ${metric.reason}` : ""}）`;
  const value = (Number(metric.numerator) / Number(metric.denominator)) * 100;
  if (!Number.isFinite(value)) return `UNKNOWN（${metric.quality}）`;
  return `${value > 0 ? "+" : ""}${value.toFixed(2)}%（${metric.numerator}/${metric.denominator}）`;
}
function t0BoardPositionText(board, prefix) {
  const first = board[`${prefix}_rank_first`];
  const last = board[`${prefix}_rank_last`];
  const valid = board[`${prefix}_valid_member_count`];
  if (!Number.isInteger(first) || !Number.isInteger(last))
    return `${prefix.toUpperCase()} ${board[`${prefix}_position`] || "UNKNOWN"}`;
  const rank = first === last ? `${first}` : `${first}–${last}并列`;
  const unknown = board[`${prefix}_unknown_member_count`] ?? "UNKNOWN";
  const outside =
    board[`${prefix}_out_of_source_pool_member_count`] ?? "UNKNOWN";
  const qualityDetails = [];
  if (board[`${prefix}_unknown_member_count`] != null)
    qualityDetails.push(`未知${unknown}`);
  if (board[`${prefix}_out_of_source_pool_member_count`] != null)
    qualityDetails.push(`来源池外${outside}`);
  return `${prefix.toUpperCase()}第${rank}/${valid ?? "UNKNOWN"}位可评价${qualityDetails.length ? ` · ${qualityDetails.join(" · ")}` : ""}`;
}
function t0BoardExamplesText(examples, count) {
  const rows = Array.isArray(examples) ? examples : [];
  if (!Number(count)) return "无";
  const shown = rows.map((board) => {
    const pulse = Array.isArray(board.pulse_states)
      ? board.pulse_states.join("/") || "UNKNOWN"
      : "UNKNOWN";
    const ranks = [];
    if (Number.isInteger(board.rps5_rank_first))
      ranks.push(t0BoardPositionText(board, "rps5"));
    if (Number.isInteger(board.rps20_rank_first))
      ranks.push(t0BoardPositionText(board, "rps20"));
    const conflicts = Array.isArray(board.direct_conflict_codes)
      ? board.direct_conflict_codes
          .map((code) => earlyCodeLabels[code] || code)
          .join("、")
      : "";
    const details = ranks.length ? ranks.join("；") : conflicts || "";
    return `${board.board_name || board.board_native_id || "名称未知"}（PULSE ${pulse}${details ? `；${details}` : ""}）`;
  });
  if (Number(count) > rows.length)
    shown.push(`其余${Number(count) - rows.length}板按需展开`);
  return shown.join("；") || `${count}板（名称未知）`;
}
function t0PriceAtDiscoveryText(price) {
  if (!price || typeof price !== "object") return "T0价格画像 UNKNOWN";
  const ret5 = exactPercentMetric(price.core_ret5);
  const ret20 = exactPercentMetric(price.core_ret20);
  const extension = price.profile_extension || {};
  const extensionText =
    extension.quality === "OBSERVED" && extension.value
      ? `${extension.value}（OBSERVED）`
      : `UNKNOWN（${extension.quality || "NO_STORED_VALUE"}${extension.reason ? ` · ${extension.reason}` : ""}）`;
  const bias = price.core_bias20_atr || {};
  const biasText =
    bias.quality === "OBSERVED"
      ? `${exactRatioText(bias.numerator, bias.denominator, " ATR")}`
      : `UNKNOWN（${bias.quality || "NO_STORED_VALUE"}${bias.reason ? ` · ${bias.reason}` : ""}）`;
  const damage = price.core_price_damage || {};
  const damageText =
    damage.quality === "OBSERVED"
      ? damage.value === true
        ? "TRUE（现存价格损伤反证）"
        : damage.value === false
          ? "FALSE（未见现存损伤标记）"
          : "UNKNOWN"
      : `UNKNOWN（${damage.quality || "NO_STORED_VALUE"}${damage.reason ? ` · ${damage.reason}` : ""}）`;
  return `截至T0 Core ret5 ${ret5}、ret20 ${ret20}（冻结窗口均含T0）；延伸 ${extensionText}；bias20 ${biasText}；价格损伤 ${damageText}`;
}
function rpsRankText(context, prefix) {
  const first = context[`${prefix}_rank_first`];
  const last = context[`${prefix}_rank_last`];
  if (!Number.isInteger(first) || !Number.isInteger(last))
    return `${prefix.toUpperCase()} ${evidenceStatusLabel(context[`${prefix}_quality`])} · 未排名`;
  const rank = first === last ? `第 ${first} 位` : `并列第 ${first}–${last} 位`;
  const valid = context[`${prefix}_valid_member_count`];
  const unknown = context[`${prefix}_unknown_member_count`];
  const outside = context[`${prefix}_out_of_source_pool_member_count`];
  return `${prefix.toUpperCase()} ${rank} / ${valid} 位可评价成员 · UNKNOWN ${unknown} · 来源池外 ${outside ?? "UNKNOWN"}`;
}
function t0EarlySummary(t0) {
  if (t0?.triage_summary)
    return t0TriageHeadlineText(t0.triage_summary, t0.followup_status);
  const contexts = t0EarlyContexts(t0);
  if (contexts.length) return t0ContextsSummary(contexts, t0);
  const registrations = Array.isArray(t0?.enrollments) ? t0.enrollments : [];
  if (registrations.length) {
    const missing = registrations.find(
      (entry) =>
        !Array.isArray(entry.early_contexts) || !entry.early_contexts.length,
    );
    return `T0板内观察 ${missing?.early_status || "NO_STORED_FACT"} · ${missing?.early_reason || "该日期没有已存板内观察事实"}`;
  }
  return "T0板内观察 NO_STORED_FACT · 当前对象没有已存观察登记";
}
function t0BoardRankHeadline(board, prefix) {
  const first = board[`${prefix}_rank_first`];
  const last = board[`${prefix}_rank_last`];
  const denominator = board[`${prefix}_valid_member_count`];
  if (!Number.isInteger(first) || !Number.isInteger(last))
    return `${prefix.toUpperCase()} ${board[`${prefix}_position`] || "UNKNOWN"}`;
  const rank = first === last ? `${first}` : `${first}–${last}`;
  return `${prefix.toUpperCase()} ${rank}/${denominator}`;
}
function t0BoardHeadlineList(examples, count) {
  const rows = Array.isArray(examples) ? examples : [];
  const text = rows.map((board) => {
    const pulse = Array.isArray(board.pulse_states)
      ? board.pulse_states.join("/") || "UNKNOWN"
      : "UNKNOWN";
    const basis = board.candidate_basis
      ? board.candidate_basis === "PULSE_TRUE_RPS5_RPS20_FRONT"
        ? "板级候选"
        : "D0/PULSE候选"
      : "";
    const rankDetails = [];
    if (Number.isInteger(board.rps5_rank_first))
      rankDetails.push(t0BoardRankHeadline(board, "rps5"));
    if (Number.isInteger(board.rps20_rank_first))
      rankDetails.push(t0BoardRankHeadline(board, "rps20"));
    const conflictDetails = Array.isArray(board.direct_conflict_codes)
      ? board.direct_conflict_codes
          .map((code) => earlyCodeLabels[code] || code)
          .join("、")
      : "";
    const details = rankDetails.length
      ? `（${rankDetails.join("，")}）`
      : conflictDetails
        ? `（${conflictDetails}）`
        : "";
    return `${board.board_name || board.board_native_id || "名称未知"}${basis ? ` ${basis}` : ""} ${pulse}${details}`;
  });
  if (Number(count) > rows.length)
    text.push(`另${Number(count) - rows.length}板按需展开`);
  return text.join("；") || "无";
}
function t0TriageHeadlineText(summary, followupStatus = null) {
  if (summary?.status !== "AVAILABLE")
    return t0TriageSummaryText(summary || { status: "NO_STORED_FACT" });
  const driver = summary.t0_driver_and_earliness;
  if (!driver) return t0TriageSummaryText(summary, followupStatus);
  const falseBoards = Number(driver.background_pulse_false_board_count || 0);
  const conflicts = Number(driver.named_conflict_board_count || 0);
  const unknown = Number(driver.unknown_relation_count || 0);
  const unknownReasons = (driver.unknown_reason_codes || []).map(
    (code) => earlyCodeLabels[code] || code,
  );
  const price = driver.price_at_t0 || {};
  const extension = price.profile_extension || {};
  const bias = price.core_bias20_atr || {};
  const damage = price.core_price_damage || {};
  const biasText =
    bias.quality === "OBSERVED"
      ? exactRatioText(bias.numerator, bias.denominator, " ATR")
      : `UNKNOWN/${bias.quality || "NO_STORED_VALUE"}`;
  const damageText =
    damage.quality === "OBSERVED"
      ? damage.value === true
        ? "TRUE"
        : damage.value === false
          ? "FALSE"
          : "UNKNOWN"
      : `UNKNOWN/${damage.quality || "NO_STORED_VALUE"}`;
  const transitionCount = Number(
    driver.pulse_transition_not_computed_relation_count || 0,
  );
  const legacy =
    summary.triage_state === "CURRENT_COUNTER"
      ? "CURRENT_COUNTER（旧分型）"
      : summary.triage_state || "UNKNOWN";
  return [
    `T0 R1 · 登记${driver.registration_count ?? 0}/${driver.relation_count ?? 0}板关系 · 同向${driver.same_direction_candidate_board_count ?? 0}板：${t0BoardHeadlineList(driver.same_direction_candidate_examples, driver.same_direction_candidate_board_count)}`,
    `其它背景${driver.other_background_board_count ?? 0}板（PULSE FALSE ${falseBoards}板/${driver.background_pulse_false_relation_count ?? 0}关系）：${t0BoardHeadlineList(driver.other_background_examples, driver.other_background_board_count)}`,
    `具名冲突${conflicts}板${conflicts ? `：${t0BoardHeadlineList(driver.named_conflict_examples, conflicts)}` : ""} · UNKNOWN ${unknown}${unknownReasons.length ? `（${unknownReasons.join("、")}）` : ""}`,
    `Core T0含入 ret5 ${exactPercentMetric(price.core_ret5)} / ret20 ${exactPercentMetric(price.core_ret20)} · 延伸 ${extension.quality === "OBSERVED" ? `${extension.value}/OBSERVED` : `UNKNOWN/${extension.quality || "NO_STORED_VALUE"}`} · bias20 ${biasText} · 损伤 ${damageText}`,
    `相邻PULSE ${transitionCount ? `NOT_COMPUTED ${transitionCount}` : "按已存状态"} · 后继 ${followupStatus || summary.followup_status || "NOT_COMPUTED"} · 旧分型 ${legacy} · PULSE FALSE仅作板块背景`,
  ].join(" · ");
}
function t0TriageSummaryText(summary, followupStatus = null) {
  const labels = {
    CURRENT_SAME_DIRECTION: "同日同向支持（静态协同）",
    MIXED: "支持与反证并存（MIXED）",
    CURRENT_COUNTER: "CURRENT_COUNTER（旧板关系归并）",
    UNKNOWN: "证据 UNKNOWN",
    NO_PROVED_CASE: "没有已证同向或反证",
    NO_T0_FACT: "没有已存T0登记",
  };
  if (summary.status !== "AVAILABLE")
    return "T0研判 无已存登记；不能据此解释为无信号";
  const driver = summary.t0_driver_and_earliness || {};
  const support = Number(driver.same_direction_candidate_board_count || 0);
  const background = Number(driver.other_background_board_count || 0);
  const falseRelations = Number(
    driver.background_pulse_false_relation_count || 0,
  );
  const conflicts = Number(driver.named_conflict_board_count || 0);
  const unknown = Number(driver.unknown_relation_count || 0);
  const unknownReasons = Array.isArray(driver.unknown_reason_codes)
    ? driver.unknown_reason_codes.map((code) => earlyCodeLabels[code] || code)
    : [];
  const transitionNotComputed = Number(
    driver.pulse_transition_not_computed_relation_count || 0,
  );
  const price = t0PriceAtDiscoveryText(driver.price_at_t0);
  const storedFollowup =
    followupStatus || summary.followup_status || "NOT_COMPUTED";
  const parts = [
    `早期解释 ${driver.version || "T0_DRIVER_AND_EARLINESS_R1"}`,
    `登记 ${driver.registration_count ?? summary.registration_count ?? 0} · 板关系 ${driver.relation_count ?? summary.relation_count ?? 0}`,
    `有同向证据的板块候选 ${support}（${t0BoardExamplesText(driver.same_direction_candidate_examples, support)}）`,
    `其它所属板块背景 ${background}板，其中PULSE FALSE ${driver.background_pulse_false_board_count ?? 0}板/${falseRelations}条关系（${t0BoardExamplesText(driver.other_background_examples, background)}）`,
    `具名板内条件冲突 ${conflicts}板/${driver.named_conflict_relation_count ?? 0}条关系（${t0BoardExamplesText(driver.named_conflict_examples, conflicts)}）`,
    `当前证据未知 ${unknown}条关系${unknownReasons.length ? `（${unknownReasons.join("、")}）` : ""}`,
    price,
    `相邻PULSE转移 ${transitionNotComputed ? `NOT_COMPUTED ${transitionNotComputed}条` : "按已存状态"} · 后继 ${storedFollowup}`,
  ];
  parts.push(
    `旧兼容分型 ${labels[summary.triage_state] || "证据 UNKNOWN"}；PULSE FALSE仅表示该板未满足PULSE，不单独证明个股启动失败 · NON_PIT`,
  );
  return parts.join(" · ");
}
function t0ContextsSummary(contexts, fallback = {}) {
  if (!contexts.length)
    return `T0板内观察 ${fallback?.status || "NOT_COMPUTED"} · ${fallback?.reason || "NO_STORED_FACT"}`;
  const pulse = { TRUE: 0, FALSE: 0, UNKNOWN: 0, NOT_COMPUTED: 0 };
  const alignment = {
    SUPPORT: 0,
    CONFLICT: 0,
    MIXED: 0,
    UNKNOWN: 0,
    NEUTRAL: 0,
  };
  const transitionMissing = { count: 0 };
  for (const context of contexts) {
    pulse[context.pulse_state] = (pulse[context.pulse_state] || 0) + 1;
    alignment[context.alignment_state] =
      (alignment[context.alignment_state] || 0) + 1;
    if (context.transition_status !== "AVAILABLE") transitionMissing.count += 1;
  }
  return `T0板内 ${contexts.length} 条登记×板块事实 · PULSE TRUE/FALSE/UNKNOWN ${pulse.TRUE}/${pulse.FALSE}/${pulse.UNKNOWN + pulse.NOT_COMPUTED} · 同向/冲突/混合 ${alignment.SUPPORT}/${alignment.CONFLICT}/${alignment.MIXED} · 相邻转移未计算 ${transitionMissing.count} · NON_PIT`;
}
const earlyCodeLabels = {
  D0_FALSE_TO_TRUE: "D0 同源新增启动",
  PULSE_TRUE: "当前板块 PULSE 为 TRUE",
  PULSE_APPEARED: "相邻 PULSE 新出现",
  PULSE_PERSISTED: "相邻 PULSE 连续成立",
  RPS5_FRONT_HALF: "RPS5 位于板内前半",
  RPS20_FRONT_HALF: "RPS20 位于板内前半",
  PULSE_FALSE: "当前板块 PULSE 为 FALSE",
  PULSE_CEASED: "相邻 PULSE 本日退场",
  RPS5_BACK_HALF: "RPS5 位于板内后半",
  RPS20_BACK_HALF: "RPS20 位于板内后半",
  RPS5_RPS20_MIXED: "RPS5 与 RPS20 板内位置分化",
  MEMBERSHIP_UNKNOWN: "成员快照不能证明完整映射",
  PULSE_CURRENT_UNKNOWN: "当前 PULSE 为 UNKNOWN",
  PULSE_NOT_COMPUTED: "当前 PULSE 尚未计算或没有已存事实",
  PULSE_TRANSITION_NOT_COMPUTED: "没有精确相邻 PULSE 转移事实",
  RPS5_UNKNOWN: "RPS5 值或板内位置未知",
  RPS20_UNKNOWN: "RPS20 值或板内位置未知",
};
const earlyCaseLabels = {
  D0_PULSE_APPEARED_FRONT: "D0 新增 + PULSE 新现 + RPS5/RPS20 同在板内前半",
  D0_PULSE_TRUE_FRONT: "D0 新增 + 当前 PULSE TRUE + RPS5/RPS20 同在板内前半",
  D0_PULSE_TRUE_BACK: "D0 新增 + 当前 PULSE TRUE，但 RPS5/RPS20 同在板内后半",
  D0_PULSE_TRUE_MIXED: "D0 新增 + 当前 PULSE TRUE，但 RPS5/RPS20 位置分化",
  D0_PULSE_FALSE_CONFLICT:
    "D0 新增与本板 PULSE FALSE/相邻退场不一致（仅板块条件冲突）",
  PULSE_TRUE_BACK: "当前 PULSE TRUE，但个股 RPS5/RPS20 同在板内后半",
  PULSE_UNKNOWN: "板块 PULSE 当前状态未知",
  NO_BOARD_MAPPING: "T0 对象没有可证的板块映射",
  BOARD_CONTEXT: "已保存板块上下文；当前没有更具体的同向或冲突标签",
};
const pulseReasonLabels = {
  BOARD_MEMBER_COUNT_BELOW_FIVE: "板块成员少于5只",
  COMMON_MEMBER_COUNT_BELOW_FIVE: "两期共同成员少于5只",
  COMPARISON_QUOTE_COUNT_BELOW_FIVE: "比较端可用报价少于5只",
  TARGET_QUOTE_COUNT_BELOW_FIVE: "目标端可用报价少于5只",
  COMMON_MEMBER_COVERAGE_BELOW_80_PERCENT: "两期共同成员覆盖低于80%",
  COMPARISON_QUOTE_COVERAGE_BELOW_80_PERCENT: "比较端报价覆盖低于80%",
  TARGET_QUOTE_COVERAGE_BELOW_80_PERCENT: "目标端报价覆盖低于80%",
  BREADTH_DELTA1_COMPARISON_VALUE_UNKNOWN: "比较端板块宽度未知",
  BREADTH_DELTA1_TARGET_VALUE_UNKNOWN: "目标端板块宽度未知",
  RET5_COMPARISON_VALUE_UNKNOWN: "比较端板块5会话收益未知",
  RET5_TARGET_VALUE_UNKNOWN: "目标端板块5会话收益未知",
};
function codeListText(codes) {
  if (!Array.isArray(codes) || !codes.length) return "无已证项目";
  return codes.map((code) => earlyCodeLabels[code] || code).join("；");
}
function t0HistoryValueText(value) {
  if (Array.isArray(value)) return value.length ? value.join("、") : "未列出";
  if (value && typeof value === "object") {
    const entries = Object.entries(value);
    return entries.length
      ? entries
          .map(([key, item]) => {
            const state =
              item && typeof item === "object"
                ? (item.state ?? item.status ?? JSON.stringify(item))
                : (item ?? "UNKNOWN");
            const reasons =
              item && typeof item === "object"
                ? Array.isArray(item.reason_codes)
                  ? item.reason_codes.join("、")
                  : item.reason
                : null;
            return `${key} ${state}${reasons ? ` · ${reasons}` : ""}`;
          })
          .join("；")
      : "未列出";
  }
  return value === undefined || value === null || value === ""
    ? "未列出"
    : String(value);
}
function t0HistoryMetricText(metric) {
  if (!metric || typeof metric !== "object") return "UNKNOWN";
  const firstValue = metric.rank_first;
  const lastValue = metric.rank_last;
  const first = Number(firstValue);
  const last = Number(lastValue);
  const hasFirst =
    firstValue !== undefined && firstValue !== null && firstValue !== "";
  const hasLast =
    lastValue !== undefined && lastValue !== null && lastValue !== "";
  const rank =
    hasFirst && Number.isInteger(first)
      ? hasLast && Number.isInteger(last) && last !== first
        ? `${first}–${last}`
        : String(first)
      : "UNKNOWN";
  const reason = metric.reason ? ` · 原因 ${metric.reason}` : "";
  const value =
    metric.numerator !== undefined &&
    metric.numerator !== null &&
    metric.denominator !== undefined &&
    metric.denominator !== null
      ? ` · RPS值 ${metric.numerator}/${metric.denominator}`
      : "";
  return `${metric.status || "UNKNOWN"} · 位次 ${rank} / 有效分母 ${metric.valid_member_count ?? "UNKNOWN"} · 未知 ${metric.unknown_member_count ?? "UNKNOWN"} · 来源池外 ${metric.out_of_source_pool_member_count ?? "UNKNOWN"} · 快照成员 ${metric.snapshot_member_count ?? "UNKNOWN"} · 位置 ${metric.position || "UNKNOWN"}${value}${reason}`;
}
function t0HistoryTransitionText(comparison, metric) {
  const detail = comparison?.[metric] || {};
  const stateLabels = {
    IMPROVED: "前移",
    WORSENED: "后移",
    UNCHANGED: "未变",
  };
  const state = detail.state || "UNKNOWN";
  return `${stateLabels[state] || state}${detail.reason ? `（${detail.reason}）` : ""}`;
}
function t0HistoryMarketText(context) {
  if (!context || typeof context !== "object") return "UNKNOWN";
  const statusLabels = {
    AVAILABLE: "可用",
    PARTIAL_WITH_UNKNOWN: "部分未知",
    NOT_COMPUTED: "未计算",
    AMBIGUOUS: "多版本",
    UNKNOWN: "未知",
  };
  const participationLabels = {
    NORMAL: "正常",
    HIGH_PARTICIPATION_LOW_EFFICIENCY: "参与高、效率低",
    HIGH_PARTICIPATION: "参与高",
    LOW_PARTICIPATION: "参与低",
    UNKNOWN: "未知",
  };
  const unitLabels = {
    UNIT_UNVERIFIED: "金额单位未核验",
    CNY: "金额单位 CNY",
    CNY_VALIDATED: "金额单位已核验 CNY",
  };
  const status = context.breadth_status || "NOT_COMPUTED";
  if (status !== "AVAILABLE")
    return `市场宽度 ${statusLabels[status] || status}`;
  const positive = context.breadth_positive_count ?? "UNKNOWN";
  const nonpositive = context.breadth_nonpositive_count ?? "UNKNOWN";
  const unknown = context.breadth_unknown_count ?? "UNKNOWN";
  const participation = context.participation || {};
  const participationStatus = context.participation_status || "NOT_COMPUTED";
  const participationClassification = participation.classification || "UNKNOWN";
  const amountUnit = participation.amount_unit_status || "UNKNOWN";
  return `来源池正/非正/未知 ${positive}/${nonpositive}/${unknown} · 有价 ${context.breadth_target_present_count ?? "UNKNOWN"}/${context.breadth_source_count ?? "UNKNOWN"} · 成交参与 ${statusLabels[participationStatus] || participationStatus} / ${participationLabels[participationClassification] || participationClassification} · 可评价 ${participation.assessable_count ?? "UNKNOWN"}/${participation.source_pool_count ?? "UNKNOWN"} · ${unitLabels[amountUnit] || amountUnit}`;
}
function t0RelativePositionHistoryDetails(history) {
  const root = node("div", null, "t0-relative-position-history-result");
  const data = history && typeof history === "object" ? history : {};
  const sessions = Array.isArray(data.sessions) ? data.sessions : [];
  const comparisons = Array.isArray(data.comparisons) ? data.comparisons : [];
  const targetSession = data.target_session || "UNKNOWN_SESSION";
  const t0Comparison = data.t0_comparison;
  root.append(
    node(
      "p",
      `跨会话相对位置 ${data.status || "NOT_COMPUTED"} · ${data.symbol || "UNKNOWN_SYMBOL"} · ${boardKindLabel(data.board_kind)} ${data.board_native_id || "UNKNOWN_BOARD"} · ${data.membership_mode || "UNKNOWN_MEMBERSHIP_MODE"} · ${data.research_mode || "UNKNOWN_RESEARCH_MODE"} · ${data.price_coordinate || "UNKNOWN_COORDINATE"}`,
    ),
    node(
      "p",
      "板内相对位置仅作描述性研究；历史成员为 NON_PIT，价格为 RAW_UNADJUSTED，不代表正式D2、Priority或交易建议。未保存T0/规则事实不解释为无信号。",
    ),
    node(
      "p",
      t0Comparison
        ? `评价 T0 ${targetSession}：${t0Comparison.from_session || "UNKNOWN"} → ${t0Comparison.to_session || "UNKNOWN"} · RPS5 ${t0HistoryTransitionText(t0Comparison, "rps5")} · RPS20 ${t0HistoryTransitionText(t0Comparison, "rps20")}。分组标签只使用这个前态到 T0 的比较及 T0/PULSE/来源登记；后继比较不回灌。`
        : `评价 T0 ${targetSession}：前一官方会话到 T0 的变化 ${data.t0_comparison_status || "NOT_COMPUTED"}（${data.t0_comparison_reason || "没有可用的精确前态比较"}）；后继比较不回灌。`,
    ),
    node(
      "p",
      "同一选定 T0 的 Focus 逐登记区域展示 CONTINUES、EXIT_PROVED、PAUSED_UNKNOWN、REENTRY 与已存 T+1/T+3/T+5；Cohort 仍按精确 T0/规则/来源读取。",
    ),
  );
  const reasonCodes = Array.isArray(data.reason_codes) ? data.reason_codes : [];
  if (reasonCodes.length)
    root.append(node("p", `范围原因：${reasonCodes.join("、")}`));
  else if (data.reason) root.append(node("p", `范围原因：${data.reason}`));

  if (sessions.length) {
    const table = node("table", undefined, "focus-s2-t1-table");
    const head = node("thead");
    const header = node("tr");
    for (const label of [
      "会话 / 捕获",
      "RPS5位次与分母",
      "RPS20位次与分母",
      "PULSE / 相邻转移",
      "市场背景",
      "T0登记 / 案例",
      "来源规则",
    ])
      header.append(node("th", label));
    head.append(header);
    const body = node("tbody");
    for (const session of sessions) {
      const row = node("tr");
      const timeRoleLabels = {
        PRIOR_T0_BASELINE: "前一官方会话（比较基准）",
        T0_LABEL: "评价 T0",
        POST_T0_CONTEXT: "T0 后继上下文",
      };
      row.append(
        node(
          "td",
          `${timeRoleLabels[session.relative_time_role] || "UNKNOWN 时点角色"} · ${session.target_session || "UNKNOWN_SESSION"} · capture ${session.source_capture_id ?? "UNKNOWN"}`,
        ),
        node("td", t0HistoryMetricText(session.rps5)),
        node("td", t0HistoryMetricText(session.rps20)),
        node(
          "td",
          `PULSE ${session.pulse_status || "UNKNOWN"} / ${session.pulse_state || "UNKNOWN"} · 转移 ${session.transition_status || "UNKNOWN"} / ${session.transition_state || "UNKNOWN"}`,
        ),
        node("td", t0HistoryMarketText(session.market_context)),
        node(
          "td",
          session.t0_observation_status === "AVAILABLE"
            ? `T0 ${session.t0_observation_status} · 案例 ${t0HistoryValueText(session.t0_cases)} · 相对关系 ${t0HistoryValueText(session.t0_alignment_states)}`
            : `T0 ${session.t0_observation_status || "UNKNOWN"} · 未保存/未知（不推断无信号） · 案例 ${t0HistoryValueText(session.t0_cases)}`,
        ),
        node(
          "td",
          `规则 ${session.rule_status || "UNKNOWN"} · ${t0HistoryValueText(session.rule_states)}`,
        ),
      );
      body.append(row);
    }
    table.append(head, body);
    const tableScroll = node(
      "div",
      null,
      "focus-s2-t1-table-scroll table-scroll",
    );
    tableScroll.append(table);
    root.append(node("h5", "逐会话已存事实"), tableScroll);
  } else {
    root.append(
      node(
        "p",
        `${data.status || "NOT_COMPUTED"} · 当前板块没有可显示的已存跨会话位次事实；这不表示无信号。`,
      ),
    );
  }

  if (comparisons.length) {
    const table = node("table", undefined, "focus-s2-t1-table");
    const head = node("thead");
    const header = node("tr");
    for (const label of [
      "时点 / 比较会话",
      "同来源池可比",
      "RPS5变化",
      "RPS20变化",
    ])
      header.append(node("th", label));
    head.append(header);
    const body = node("tbody");
    for (const comparison of comparisons) {
      const comparable = comparison.source_pool_comparable;
      const comparableText =
        comparable === true
          ? "是"
          : comparable === false
            ? "否"
            : (comparable ?? "UNKNOWN");
      const row = node("tr");
      const roleLabel =
        comparison.relative_time_role === "T0_LABEL_INPUT"
          ? "T0 分组输入"
          : "T0 后继上下文（不参与分组）";
      row.append(
        node(
          "td",
          `${roleLabel} · ${comparison.from_session || "UNKNOWN"} → ${comparison.to_session || "UNKNOWN"}`,
        ),
        node("td", String(comparableText)),
        node("td", t0HistoryTransitionText(comparison, "rps5")),
        node("td", t0HistoryTransitionText(comparison, "rps20")),
      );
      body.append(row);
    }
    table.append(head, body);
    const tableScroll = node(
      "div",
      null,
      "focus-s2-t1-table-scroll table-scroll",
    );
    tableScroll.append(table);
    root.append(node("h5", "T0 前态标签及后继描述"), tableScroll);
  }
  return root;
}
function t0EarlyObservationDetails(
  registrations,
  triageSummary = null,
  sourceSymbol = null,
  requestIdentity = {},
) {
  const panel = node("div", null, "rule t0-early-observations");
  panel.append(
    node("strong", "T0 板块驱动与早期性观察 · T0_DRIVER_AND_EARLINESS_R1"),
    node(
      "p",
      "解释视角并列保留旧分型及全部板内原始支持、反证、UNKNOWN。同向候选来自既存D0-PULSE同向案例，或当前PULSE TRUE且同板RPS5、RPS20均在前半；PULSE FALSE只表示所属板块未满足当前PULSE条件。Seed收益、延伸、bias与损伤沿用已存T0精确事实；成员回看为NON_PIT，不能证明首启、板块因果、龙头或未来收益。",
    ),
  );
  if (!registrations.length) {
    panel.append(node("p", "NO_STORED_FACT：当前对象没有已存T0登记。"));
    return panel;
  }

  const hasLoadedContexts = registrations.some(
    (registration) =>
      Array.isArray(registration.early_contexts) &&
      registration.early_contexts.length,
  );
  if (!hasLoadedContexts && triageSummary?.status === "AVAILABLE") {
    panel.append(
      node("p", t0TriageSummaryText(triageSummary)),
      node(
        "p",
        "完整板内关系按需读取；打开后逐登记显示同向板块、反证板块、UNKNOWN原因、来源版本和后继状态。",
      ),
    );
    const expand = node("button", "展开完整板内事实", "button subtle");
    expand.type = "button";
    expand.addEventListener("click", async () => {
      expand.disabled = true;
      expand.textContent = "正在读取精确来源版本…";
      try {
        const result = await api("/api/preview/t0-early-observations", {
          symbol: sourceSymbol,
          target_session: requestIdentity.target_session,
          source_package_id: requestIdentity.source_package_id,
          source_capture_id: requestIdentity.source_capture_id,
        });
        for (const registration of registrations) {
          registration.early_contexts =
            result.by_enrollment?.[String(registration.id)] || [];
          registration.early_status = registration.early_contexts.length
            ? "AVAILABLE"
            : "NO_STORED_FACT";
        }
        panel.replaceWith(
          t0EarlyObservationDetails(
            registrations,
            triageSummary,
            sourceSymbol,
            requestIdentity,
          ),
        );
      } catch (error) {
        expand.disabled = false;
        expand.textContent = `读取失败：${error.message}；重试完整板内事实`;
      }
    });
    panel.append(expand);
    return panel;
  }
  if (triageSummary?.status === "AVAILABLE")
    panel.append(node("p", t0TriageSummaryText(triageSummary)));

  for (const registration of registrations) {
    const contexts = Array.isArray(registration.early_contexts)
      ? registration.early_contexts
      : [];
    panel.append(node("h4", enrollmentSummaryText(registration)));
    if (!contexts.length) {
      panel.append(
        node(
          "p",
          `登记 #${registration.id} 的板内观察 ${registration.early_status || "NOT_COMPUTED"} · ${registration.early_reason || "NO_STORED_FACT"}`,
        ),
      );
      continue;
    }
    for (const context of contexts) {
      const board = context.catalog_id
        ? `${boardKindLabel(context.board_kind)} ${context.board_name || context.board_native_id}`
        : `板块映射 ${context.membership_status || "UNKNOWN"}`;
      const pulseText =
        context.pulse_status === "AVAILABLE"
          ? `PULSE ${context.pulse_state}`
          : `PULSE ${context.pulse_status || "UNKNOWN"}（${context.pulse_state || "UNKNOWN"}）`;
      const pulseReasons = Array.isArray(context.pulse_reason_codes)
        ? context.pulse_reason_codes
            .map((reason) => pulseReasonLabels[reason] || reason)
            .join("、")
        : "";
      const transitionEndpointReasons = [
        ...(context.previous_reason_codes || []),
        ...(context.target_reason_codes || []),
      ];
      const transitionReasonText = transitionEndpointReasons.length
        ? ` · 端点原因 ${codeListText(transitionEndpointReasons)}`
        : "";
      const transitionText =
        context.transition_status === "AVAILABLE"
          ? `相邻变化 ${context.transition_state}（${context.previous_state || "UNKNOWN"}→${context.target_state || "UNKNOWN"}）${transitionReasonText}`
          : `相邻变化 ${context.transition_status || "NOT_COMPUTED"}（${(context.transition_reason_codes || []).join("、") || "PULSE_TRANSITION_FACTS_NOT_SAVED_FOR_EXACT_INPUT"}）`;
      const driverRoles = [];
      if (context.driver_same_direction_candidate) {
        driverRoles.push(
          context.driver_candidate_basis === "PULSE_TRUE_RPS5_RPS20_FRONT"
            ? "板级同向候选（当前PULSE TRUE、RPS5/20同在前半；D0是否新增另列）"
            : "D0与PULSE同日同向候选",
        );
      }
      if (context.driver_other_board_background)
        driverRoles.push("其它所属板块背景/不协同");
      if (context.driver_pulse_false_background)
        driverRoles.push("本板PULSE FALSE（仅为板块背景）");
      const directConflicts = Array.isArray(context.driver_named_conflict_codes)
        ? context.driver_named_conflict_codes
            .map((code) => earlyCodeLabels[code] || code)
            .join("、")
        : "";
      if (directConflicts) driverRoles.push(`具名冲突：${directConflicts}`);
      if (context.driver_has_unknown) driverRoles.push("当前字段仍有UNKNOWN");
      const rps5Value = exactRatioText(
        context.rps5_numerator,
        context.rps5_denominator,
      );
      const rps20Value = exactRatioText(
        context.rps20_numerator,
        context.rps20_denominator,
      );
      const delta3 = exactRatioText(
        context.rps5_delta3_numerator,
        context.rps5_delta3_denominator,
        " pp",
      );
      const pulseMetrics =
        context.pulse_status === "AVAILABLE"
          ? ` · dq5 ${exactRatioText(context.pulse_dq5_numerator, context.pulse_dq5_denominator, " pp")} · breadth Δ ${exactRatioText(context.pulse_breadth_delta1_numerator, context.pulse_breadth_delta1_denominator, " pp")}`
          : "";
      panel.append(
        node(
          "p",
          `${board} · ${pulseText}${pulseReasons ? `（${pulseReasons}）` : ""} · ${transitionText}${pulseMetrics} · 成员 ${context.membership_quality || context.snapshot_source_quality || "UNKNOWN"} · ${context.membership_mode || "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"}`,
        ),
        node(
          "p",
          `D0 ${context.d0_before_state || "UNKNOWN"}→${context.d0_after_state || "UNKNOWN"} ${context.d0_transition || "NOT_COMPUTED"} · RPS5 ${rps5Value}；${rpsRankText(context, "rps5")} · RPS20 ${rps20Value}；${rpsRankText(context, "rps20")} · RPS5 Δ3 ${delta3}`,
        ),
        node(
          "p",
          `研究判断 ${earlyCaseLabels[context.research_case] || context.research_case || "UNKNOWN"} · 板内位置 ${context.relative_position || "UNKNOWN"} · 同向/冲突关系 ${context.alignment_state || "UNKNOWN"}`,
        ),
        node(
          "p",
          `T0_DRIVER_AND_EARLINESS_R1：${driverRoles.join("；") || "其它板块背景/中性"}。只有明确的RPS后半/位置分化或已存PULSE退场列为具名冲突；不会把PULSE FALSE单独升格为个股失败。`,
        ),
        node("p", `支持证据：${codeListText(context.support_codes)}`),
        node(
          "p",
          `原始反证代码（完整保留）：${codeListText(context.counter_codes)}`,
        ),
        node(
          "p",
          `当前未知原因：${codeListText(context.driver_unknown_reason_codes || context.unknown_codes)}`,
        ),
        node(
          "p",
          `下一步：${context.next_check_code || "WAIT_FOR_NEXT_STORED_SESSION"} · NON_PIT_EXPLORATORY · RAW_UNADJUSTED；后继未到期时仍为 NOT_DUE。`,
        ),
      );
      if (context.board_kind && context.board_native_id) {
        const historyPanel = node("div", null, "t0-relative-position-history");
        const historyButton = node(
          "button",
          "读取本板跨会话位次",
          "button subtle",
        );
        historyButton.type = "button";
        const identityReady =
          sourceSymbol &&
          requestIdentity.target_session &&
          requestIdentity.source_package_id &&
          requestIdentity.source_capture_id;
        historyButton.disabled = !identityReady;
        historyButton.addEventListener("click", async () => {
          if (!identityReady) return;
          historyButton.disabled = true;
          historyButton.textContent = "正在读取同一股票与板块…";
          try {
            const result = await api("/api/preview/t0-early-observations", {
              symbol: sourceSymbol,
              target_session: requestIdentity.target_session,
              source_package_id: requestIdentity.source_package_id,
              source_capture_id: requestIdentity.source_capture_id,
              board_kind: context.board_kind,
              board_native_id: context.board_native_id,
            });
            historyPanel
              .querySelector(".t0-relative-position-history-result")
              ?.remove();
            historyPanel.append(
              t0RelativePositionHistoryDetails(
                result.relative_position_history,
              ),
            );
            historyButton.disabled = false;
            historyButton.textContent = "重新读取本板跨会话位次";
          } catch (error) {
            historyButton.disabled = false;
            historyButton.textContent = `读取失败：${error.message}；重试跨会话位次`;
          }
        });
        historyPanel.append(historyButton);
        if (!identityReady)
          historyPanel.append(
            node("p", "缺少精确目标日、来源包或捕获身份，不能读取跨会话事实。"),
          );
        panel.append(historyPanel);
      }
    }
  }
  return panel;
}
function stockObservationSummary(item) {
  if (item.research_reading_order) {
    const transition =
      item.d0_change?.transition || item.transition || "UNKNOWN";
    return `${item.observation_reason || "已保存来源观察"} · D0 ${transition} · ${t0EarlySummary(item.t0_observation)}`;
  }
  const order = item.observation_order || {};
  const summary = [
    `${order.layer_label || "证据 UNKNOWN"}`,
    `Seed ${evidenceStateLabel(order.seed_rule_state)}`,
    `D1 锚 ${evidenceStateLabel(order.d1_anchor_state)}`,
    `九规则已知 ${order.known_rule_count ?? 0}/${order.expected_rule_count ?? 9}`,
    `D1 价格 ${order.d1_target_price_quality || "UNKNOWN"}`,
    `PULSE ${order.pulse_status || "UNKNOWN"}`,
  ].join(" · ");
  return item.t0_observation
    ? `${summary} · ${t0EarlySummary(item.t0_observation)}`
    : summary;
}
function boardPulseSummary(board) {
  const pulse = board.cross_period_pulse || {};
  return pulse.status === "AVAILABLE"
    ? `PULSE ${evidenceStateLabel(pulse.state)}`
    : `PULSE ${pulse.status || "UNKNOWN"}`;
}
function boardPulseFollowupSummary(board) {
  const followup = board.cross_period_pulse_followup || {};
  const t1 = (followup.horizons || []).find(
    (item) => item.horizon_sessions === 1,
  );
  if (!t1) return `T+1 ${followup.status || "NOT_COMPUTED"}`;
  const members = `有效 ${t1.observed_member_count ?? "UNKNOWN"} · 未知 ${t1.unknown_member_count ?? "UNKNOWN"} / 固定 ${t1.fixed_member_denominator ?? "UNKNOWN"}`;
  if (t1.status === "OBSERVED")
    return `T+1 ${t1.target_session} ${t1.equal_weight_return_percent_4dp}% · ${members}`;
  if (t1.status === "UNKNOWN")
    return `T+1 ${t1.target_session} UNKNOWN · ${members} · ${(t1.reason_codes || []).join("、")}`;
  if (t1.status === "NOT_DUE")
    return `T+1${t1.target_session ? ` ${t1.target_session}` : ""} 尚未到期 · 固定成员 ${t1.fixed_member_denominator ?? "UNKNOWN"}`;
  if (t1.status === "NOT_COMPUTED")
    return `T+1 ${t1.target_session} 尚未生产 · 固定成员 ${t1.fixed_member_denominator ?? "UNKNOWN"}`;
  return `T+1 ${t1.target_session} ${t1.status || "INCOMPLETE"} · ${(t1.reason_codes || []).join("、")}`;
}
function boardSourceQualitySummary(board) {
  const source = board.membership_source_quality || {};
  return `成员源 ${source.quality || "UNKNOWN"} · 快照未映射 ${source.source_unmapped_rows ?? "UNKNOWN"}`;
}
function membershipScopeSummary(status) {
  const snapshots = Array.isArray(status?.snapshots) ? status.snapshots : [];
  const sourceSummary = snapshots
    .map(
      (snapshot) =>
        `${boardKindLabel(snapshot.board_kind)} ${snapshot.source_quality_counts?.quality || "UNKNOWN"}（快照未映射 ${snapshot.source_quality_counts?.unmapped_source_rows ?? "UNKNOWN"}）`,
    )
    .join("；");
  return `D0映射 ${status?.mapped_distinct_source_symbol_count ?? "UNKNOWN"}/${status?.global_new_stock_count ?? "UNKNOWN"} 只；未映射代码 ${status?.unmapped_distinct_source_symbol_count ?? "UNKNOWN"} · ${sourceSummary || "成员快照 UNKNOWN"}`;
}
function membershipPageScopeSummary(status) {
  const pageCount = status?.page_symbol_count;
  const mapped = status?.mapped_distinct_source_symbol_count;
  const unmapped =
    Number.isInteger(pageCount) && Number.isInteger(mapped)
      ? Math.max(0, pageCount - mapped)
      : "UNKNOWN";
  const snapshots = Array.isArray(status?.snapshots) ? status.snapshots : [];
  const sourceSummary = snapshots
    .map((snapshot) => {
      const kind =
        snapshot.board_kind ||
        (snapshot.source_kind === "TDX_TDXHY_CFG" ? "INDUSTRY" : "CONCEPT");
      const quality = snapshot.source_quality_counts || {};
      return `${boardKindLabel(kind)} ${quality.quality || "UNKNOWN"}（快照未映射 ${quality.unmapped_source_rows ?? "UNKNOWN"}）`;
    })
    .join("；");
  return `本页成员映射 ${mapped ?? "UNKNOWN"}/${pageCount ?? "UNKNOWN"} 只；本页未映射 ${unmapped} · ${sourceSummary || "成员快照 UNKNOWN"}`;
}
function evidenceReasons(fact) {
  const reasons = Array.isArray(fact?.reasons) ? fact.reasons : [];
  return reasons.length ? reasons.join("、") : "未提供原因";
}
function storedFactStateLabel(status, fact) {
  return status === "NOT_COMPUTED" ? "未计算" : evidenceStateLabel(fact?.state);
}
function storedFactReasons(status, fact) {
  return status === "NOT_COMPUTED" ? "该项没有已存事实" : evidenceReasons(fact);
}
function evidenceStatusLabel(status) {
  return (
    {
      AVAILABLE: "已保存可读",
      AVAILABLE_EMPTY: "已核验为空",
      PARTIAL: "部分已保存",
      PARTIAL_MEMBERSHIP_UNKNOWN: "成员关系部分未知",
      UNKNOWN: "UNKNOWN",
      NOT_COMPUTED: "未计算",
      NOT_COMPARABLE: "版本或窗口不匹配",
      AMBIGUOUS: "范围不唯一",
      INCOMPLETE: "事实不完整",
    }[status] ||
    status ||
    "UNKNOWN"
  );
}
function homeObservationFor(row, evidence, context) {
  const order = row.research_reading_order || row.observation_order;
  if (Number.isInteger(order?.rank))
    return {
      status: order.home_featured ? "FEATURED" : "CAPACITY_NOT_SHOWN",
      rank: order.rank,
      reason: row.home_selection_reason || row.observation_reason || "",
    };
  const cached = state.d0Featured;
  if (
    cached?.target_session === context.target_session &&
    cached?.source_package_id === context.source_package_id
  ) {
    const homeRow = cached.rows.get(row.source_symbol);
    if (homeRow) {
      return {
        status: "FEATURED",
        rank:
          homeRow.research_reading_order?.rank ??
          homeRow.observation_order?.rank,
        reason:
          homeRow.home_selection_reason || "首页容量内的已证 D0 新增观察。",
      };
    }
    if (evidence.d0?.transition_kind === "D0_NEW")
      return {
        status:
          cached.candidate_count > cached.home_featured_count
            ? "CAPACITY_NOT_SHOWN"
            : "INCOMPLETE",
        candidate_count: cached.candidate_count,
        home_featured_count: cached.home_featured_count,
        reason:
          cached.candidate_count > cached.home_featured_count
            ? `D0 新增共 ${cached.candidate_count} 只；首页容量为 ${cached.home_featured_count} 只，该代码未在首页展示。`
            : "已存 D0 新增与首页完整列表不一致，首页位置保持 UNKNOWN。",
      };
  }
  if (["CONTINUING", "NOT_NEW"].includes(evidence.d0?.transition_kind))
    return { status: "NOT_D0_NEW", reason: "目标日不是 FALSE→TRUE 新增。" };
  if (evidence.d0?.transition_kind === "D0_NEW")
    return {
      status: "NOT_EVALUATED",
      reason: "已保存为 D0 新增；首页顺位尚未由当前首页结果核验。",
    };
  return {
    status:
      evidence.d0?.transition_kind === "NOT_COMPUTED"
        ? "NOT_COMPUTED"
        : "UNKNOWN",
    reason: "缺少可用于判定首页容量的同日完整首页结果。",
  };
}
function renderStoredD0Evidence(row) {
  const evidence = row.research_evidence;
  const panel = node("div", null, "rule");
  if (!evidence) {
    panel.textContent =
      "该来源代码没有可读取的同日保存研究证据；请区分未计算、UNKNOWN 与未满足。";
    return panel;
  }

  const d0 = evidence.d0 || {};
  const before = d0.before || row.before || {};
  const after = d0.after || row.after || {};
  const launchScenario = d0.scenarios?.LAUNCH_CONFIRM || {};
  const recoveryScenario = d0.scenarios?.RECOVERY_TURN || {};
  const seed = evidence.seed || {};
  const d1 = evidence.d1 || {};
  const rules = evidence.rules || {};
  const sector = evidence.sector_context || {};
  const home = row.home_observation || {};
  const transitionText =
    {
      D0_NEW: "D0 新增",
      CONTINUING: "持续满足，不是新增",
      NOT_NEW: "目标日未形成新增（当前状态不满足新增条件）",
      UNKNOWN: "UNKNOWN，证据不足",
      NOT_COMPUTED: "未计算",
    }[d0.transition_kind] || evidenceStatusLabel(d0.status);
  panel.append(
    node(
      "strong",
      `同日已保存研究证据 · ${evidenceStatusLabel(evidence.status)}`,
    ),
    node(
      "p",
      `D0 LAUNCH_CONFIRM ${before.state || "UNKNOWN"} → ${after.state || launchScenario.state || "UNKNOWN"} · ${d0.transition || "NOT_COMPUTED"} · ${transitionText}；前态原因 ${before.reason || "无已存原因"}；今态原因 ${after.reason || evidenceReasons(launchScenario)}。`,
    ),
    node(
      "p",
      `今日 LAUNCH_CONFIRM ${evidenceStateLabel(launchScenario.state)}（${evidenceReasons(launchScenario)}）· RECOVERY_TURN ${evidenceStateLabel(recoveryScenario.state)}（${evidenceReasons(recoveryScenario)}）· 正式入选/未入选判定 ${d0.formal_selection_status || "NOT_EVALUATED"}。`,
    ),
    node(
      "p",
      `首页观察：${home.status || "UNKNOWN"} · ${home.reason || "首页容量位置未核验"}${home.rank ? ` · 第 ${home.rank} 位` : ""}。`,
    ),
    node(
      "p",
      `Seed ${evidenceStatusLabel(seed.status)} · S1 ${storedFactStateLabel(seed.status, seed.s1)}（${storedFactReasons(seed.status, seed.s1)}）· S2 ${storedFactStateLabel(seed.status, seed.s2)}（${storedFactReasons(seed.status, seed.s2)}）· Seed ${storedFactStateLabel(seed.status, seed.source_seed)}（${storedFactReasons(seed.status, seed.source_seed)}）· 安全 ${storedFactStateLabel(seed.status, seed.safety)}（${storedFactReasons(seed.status, seed.safety)}）· 收盘质量 ${seed.target_close_quality || "UNKNOWN"} · Profile ${seed.scope?.profile_revision || "UNKNOWN"} / ${seed.scope?.history_session_count ?? "UNKNOWN"} 会话 · ${seed.scope?.calendar_revision || "日历 UNKNOWN"}${seed.reason_codes?.length ? ` · ${seed.reason_codes.join("、")}` : ""}`,
    ),
    node(
      "p",
      `已存 D1 ${evidenceStatusLabel(d1.status)} · 窗口 ${d1.scope?.history_window_start || "UNKNOWN"} 至 ${d1.scope?.history_window_end || "UNKNOWN"}（${d1.scope?.history_session_count ?? "UNKNOWN"} 会话）· 结构事实 ${d1.structure_fact_count ?? 0}/${d1.expected_structure_fact_count ?? 5} · 目标价格 ${d1.target_price_quality || "UNKNOWN"} · 材料锚 ${d1.material_change_anchor_count ?? 0} · 已存明细 ${d1.persisted_detail_count ?? 0} · 缺失 ${d1.missing_structure_types?.join("、") || "无"} · ${(d1.reason_codes || []).join("、") || "无已记录缺项原因"}`,
    ),
    node(
      "p",
      `九规则 ${evidenceStatusLabel(rules.status)} · TRUE/FALSE ${rules.known_state_count ?? 0}/${rules.expected_rule_count ?? 9} · 缺失 ${rules.missing_rule_ids?.join("、") || "无"} · ${(rules.reason_codes || []).join("、") || "范围可用"}`,
    ),
  );

  const d1Details = node("details", null, "evidence-details");
  d1Details.append(node("summary", "展开五类 D1 已存结构状态"));
  if (!d1.structures?.length)
    d1Details.append(
      node(
        "p",
        d1.status === "NOT_COMPUTED"
          ? "该日期没有已存 D1 结构事实；本次读取未重新计算。"
          : "该日期没有可读取的 D1 结构行；状态保持 UNKNOWN。",
      ),
    );
  for (const structure of d1.structures || []) {
    d1Details.append(
      node(
        "p",
        `${structure.structure_type} · ${structure.generation_state || "UNKNOWN"} · ${structure.relation || "无结构方向"} · 锚 ${structure.anchor_created_session || "无"} / 生效 ${structure.anchor_available_from_session || "无"} · 材料变化 ${structure.material_change_anchor_count ?? 0} · ${structure.generation_reason || structure.target_price_reason || "无原因码"}`,
      ),
    );
  }
  panel.append(d1Details);

  const ruleDetails = node("details", null, "evidence-details");
  ruleDetails.append(node("summary", "展开九规则及各自保存原因"));
  for (const [ruleId, fact] of Object.entries(rules.facts || {})) {
    ruleDetails.append(
      node(
        "p",
        `${ruleId} · ${storedFactStateLabel(rules.status, fact)} · ${storedFactReasons(rules.status, fact)} · 事件数 ${fact.event_count ?? 0}`,
      ),
    );
  }
  panel.append(ruleDetails);

  const memberships = Array.isArray(sector.memberships)
    ? sector.memberships
    : [];
  panel.append(
    node(
      "p",
      `当前板块关系 ${sector.membership_count ?? memberships.length} 条 · ${evidenceStatusLabel(sector.status)} · ${sector.membership_mode || "CURRENT_MEMBERSHIP_RETROSPECTIVE_NON_PIT"} · 缺失类型 ${(sector.missing_membership_kinds || row.missing_membership_kinds || []).join("、") || "无"} · 可读 PULSE ${sector.pulse_available_count ?? 0} 条 · 未计算 ${sector.pulse_not_computed_count ?? 0} 条。`,
    ),
  );
  if (memberships.length) {
    const membershipDetails = node("details", null, "evidence-details");
    membershipDetails.append(
      node("summary", "展开当前行业 / 概念成员与 PULSE"),
    );
    for (const membership of memberships) {
      const pulse = membership.pulse_context || {};
      membershipDetails.append(
        node(
          "p",
          `${boardKindLabel(membership.board_kind)} ${membership.board_name || membership.board_native_id || "未知板块"} · PULSE ${evidenceStatusLabel(pulse.status)}/${evidenceStateLabel(pulse.state)} · dq5 ${pulse.dq5_pp?.decimal_4dp ?? "UNKNOWN"} · breadth Δ ${pulse.breadth_delta1?.decimal_4dp ?? "UNKNOWN"} · ${pulse.reason_codes?.join("、") || pulse.transition_reason_codes?.join("、") || "无原因码"} · NON_PIT`,
        ),
      );
    }
    panel.append(membershipDetails);
  } else {
    panel.append(
      node(
        "p",
        `板块关系 ${evidenceStatusLabel(sector.status)} · ${(sector.missing_membership_kinds || row.missing_membership_kinds || []).join("、") || row.unmapped_reason || "当前快照未发现关系"}；不推断历史板外。`,
      ),
    );
  }
  return panel;
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
function applyDisplayNameContext(context) {
  if (!context) return;
  state.displayNameContext = context;
  message(
    "displayNameObservation",
    context.status === "AVAILABLE"
      ? `中文名为本机TDX观察标签（NON_PIT，${context.captured_at || "观察时钟未知"}）；不是稳定证券身份。`
      : "中文名来源暂无可用观察；以名称未知和来源代码展示。",
  );
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
let cohortRpsPulseData = null;
let cohortRpsPulseRequested = false;
let currentStockSymbol = null;
let currentStockSource = null;
function gotoPage(page) {
  const changed = activePage !== page;
  activePage = page;
  const extra = ["focus", "market", "diagnostics"].includes(page);
  $("refreshResearch").hidden = extra;
  $("contextNotice").hidden =
    extra && state.profileDirectoryStatus !== "READ_FAILED";
  $("d1MaterialFilters").hidden =
    !state.d1Requested || !["sectors", "stocks"].includes(page);
  $("d1MaterialQuality").hidden =
    !state.d1Requested || !["sectors", "stocks"].includes(page);
  $("d1SectorPanel").hidden = page !== "sectors" || !state.d1Requested;
  ++extraRequest;
  ++cohortRequest;
  ++focusStockRequest;
  ++state.focusAutoRequest;
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
    d1MaterialState.data &&
      renderD1Boards(d1MaterialState.data, openD1Board, toggleBoardComparison);
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
  ++state.focusAutoRequest;
  ++state.focusSegmentRequest;
  state.focusSegmentOffset = 0;
  state.focusSegmentEventOffset = 0;
  ++state.stockRequest;
  ++state.profileChangeRequest;
  ++state.focusMarketRequest;
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
  state.focusMarketKey = null;
  state.focusAutoBoard = null;
  state.d1Requested = false;
  state.d0Result = null;
  state.d0Featured = null;
  state.d0Status = "NOT_LOADED";
  state.d0Error = null;
  state.marketResult = null;
  state.boardComparison = [];
  ++state.boardComparisonRequest;
  message(
    "boardCompareStatus",
    "选择两个板块后比较成员交集、独有集合、去重分母、D0新增、Seed与目标价质量。",
  );
  state.d0BoardPage = 0;
  state.d0PulseDirectory = null;
  ++state.d0PulseRequest;
  d1MaterialState.data = null;
  d1MaterialState.board = null;
  d1MaterialState.boardOffset =
    d1MaterialState.stockOffset =
    d1MaterialState.memberOffset =
      0;
  currentStockSymbol = currentStockSource = null;
  focusKeysLoading = false;
  cohortRpsPulseData = null;
  cohortRpsPulseRequested = false;
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
    "cohortDateComparison",
    "cohortSymbolForward",
    "cohortRows",
    "cohortEnrollmentSummary",
    "cohortRpsPulseSummary",
    "focusStockResult",
    "focusAutoRows",
    "focusS2T1CohortTables",
  ])
    $(id).replaceChildren();
  $("cohortRpsGroup").replaceChildren(new Option("全部非空组合", "ALL"));
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
  message("focusMarketContext", "当前日期尚未读取。");
  message("focusS2T1CohortStatus", "当前日期尚未读取。");
  message("cohortEnrollmentStatus", "当前日期尚未读取。");
  message("cohortRpsPulseStatus", "当前日期尚未读取。");
  message("focusStockStatus", "当前日期尚未读取。");
  message("focusAutoStatus", "当前日期尚未读取。");
  message("focusAutoPage", "—");
  message("focusSegmentStatus", "研究日期已切换，正在重新读取已存观察段。");
  message("focusSegmentPage", "—");
  $("focusSegmentRows").replaceChildren();
  $("focusSegmentPrev").disabled = $("focusSegmentNext").disabled = true;
  message("focusSegmentEventStatus", "研究日期已切换，正在读取当日事件事实。");
  message("focusSegmentEventPage", "—");
  $("focusSegmentEventCounts").replaceChildren();
  $("focusSegmentEventRows").replaceChildren();
  $("focusSegmentEventPrev").disabled = $("focusSegmentEventNext").disabled =
    true;
  message("focusAutoBoardFilter", "当前未按板块筛选；等待当前日期。");
  message("stockSearchStatus", "");
  $("focusAutoPrev").disabled = $("focusAutoNext").disabled = true;
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
    void loadProfileDirectory();
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

function loadProfileDirectory() {
  state.profileDirectoryStatus = "LOADING";
  state.profileDirectoryError = null;
  state.profilesReady = api("/api/source-symbol-price-profiles/snapshots")
    .then((profiles) => {
      if (!Array.isArray(profiles.snapshots))
        throw new Error("画像目录返回格式无效");
      state.profiles = profiles.snapshots;
      state.profileDirectoryStatus = profiles.snapshots.length
        ? "READY"
        : "EMPTY";
      return true;
    })
    .catch((error) => {
      state.profiles = [];
      state.profileDirectoryStatus = "READ_FAILED";
      state.profileDirectoryError = error;
      return false;
    });
  return state.profilesReady;
}

function profileDirectoryRetryButton() {
  const retry = node("button", "重试读取画像目录", "button subtle");
  retry.type = "button";
  retry.addEventListener("click", () => void retryProfileDirectory());
  return retry;
}

function showProfileDirectoryReadFailure(context = state.selected) {
  if (context) {
    context.profile_revision = null;
    context.profile_status = "DIRECTORY_READ_FAILED";
  }
  const detail = state.profileDirectoryError?.message || "服务暂不可用";
  const text = `画像目录读取失败（DIRECTORY_READ_FAILED），尚未判断画像可比性：${detail}`;
  message("profileChangeSummary", "DIRECTORY_READ_FAILED");
  message("profileChangeList", text);
  $("contextText").replaceChildren(
    node("span", `${context?.target_session || "当前日期"} · ${text} `),
    profileDirectoryRetryButton(),
  );
  $("contextNotice").hidden = false;
}

async function retryProfileDirectory() {
  if (state.profileDirectoryStatus === "LOADING") return;
  const context = state.selected;
  const detail = state.context;
  const token = state.contextRequest;
  if (!context?.contextReady || !detail) return;
  message("contextText", `${context.target_session} · 正在重试读取画像目录…`);
  $("contextNotice").hidden = false;
  const ready = await loadProfileDirectory();
  if (token !== state.contextRequest || state.selected !== context) return;
  if (!ready) {
    showProfileDirectoryReadFailure(context);
    return;
  }
  context.profileResolution = resolveProfileForDate(
    context,
    token,
    context.snapshotText,
    detail,
  );
}

$("researchDate").addEventListener(
  "change",
  () => void selectDate($("researchDate").value),
);

async function selectDate(target_session) {
  invalidateDate();
  state.focusAutoSelectedItem = null;
  state.focusMarketKey = null;
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
  if (["market", "diagnostics"].includes(activePage))
    void loadExtra(activePage);
  if (activePage === "focus")
    message("focusAutoStatus", "正在读取交易日来源上下文…");
  try {
    const c = await api(
      `/api/preview/sector-change-context?${new URLSearchParams({ target_session, source_package_id })}`,
      undefined,
      controller.signal,
    );
    if (token !== state.contextRequest || state.selected !== selected) return;
    applyDisplayNameContext(c.display_name_context);
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
      d1_history_session_count: Number.isInteger(c.d1_history_session_count)
        ? c.d1_history_session_count
        : null,
      research_cutoff: c.research_cutoff,
      contextReady: true,
    });
    if (activePage === "focus") void loadExtra("focus");
    const snapshotText = [industry, concept]
      .filter(Boolean)
      .map(
        (item) =>
          `${item.source_kind === "TDX_TDXHY_CFG" ? "行业" : "概念"}观测 ${item.observed_at}`,
      )
      .join("；");
    selected.snapshotText = snapshotText;
    message(
      "contextText",
      `${target_session} · 当前成员回看 NON_PIT ${snapshotText || "成员不可用"} · 画像 ${selected.profile_status}`,
    );
    configureD1Filters();
    selected.profileResolution = resolveProfileForDate(
      selected,
      token,
      snapshotText,
      c,
    );
    void loadD0Overview(selected);
    if (state.d0BoardView === "PULSE") void loadD0PulseDirectoryPage();
  } catch (error) {
    if (token === state.contextRequest && error.name !== "AbortError") {
      message(
        "contextText",
        `${target_session} 行情可用；画像或成员目录读取失败：${error.message}`,
      );
      if (activePage === "focus")
        message("focusAutoStatus", `来源上下文不可用：${error.message}`);
    }
  }
}

async function resolveProfileForDate(
  c,
  token,
  snapshotText,
  detail = state.context,
) {
  try {
    if (token !== state.contextRequest || state.selected !== c) return;
    const directoryReady = await state.profilesReady;
    if (token !== state.contextRequest || state.selected !== c) return;
    if (!directoryReady || state.profileDirectoryStatus === "READ_FAILED") {
      showProfileDirectoryReadFailure(c);
      return;
    }
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
    $("contextNotice").hidden = ["focus", "market", "diagnostics"].includes(
      activePage,
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
      message("profileChangeSummary", "画像口径读取失败");
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

function setD0BoardView(view) {
  state.d0BoardView = view === "PULSE" ? "PULSE" : "D0";
  state.d0BoardPage = 0;
  state.d0PulseDirectory = null;
  $("viewD0Boards").setAttribute(
    "aria-pressed",
    String(state.d0BoardView === "D0"),
  );
  $("viewPulseBoards").setAttribute(
    "aria-pressed",
    String(state.d0BoardView === "PULSE"),
  );
  $("viewD0Boards").className =
    state.d0BoardView === "D0" ? "button primary" : "button subtle";
  $("viewPulseBoards").className =
    state.d0BoardView === "PULSE" ? "button primary" : "button subtle";
  $("d0BoardSearch").placeholder =
    state.d0BoardView === "PULSE"
      ? "搜索全部 PULSE 板块名称或编号"
      : "搜索板块中文名";
  if (state.d0BoardView === "PULSE") void loadD0PulseDirectoryPage();
  else renderD0Boards();
}

async function loadD0PulseDirectoryPage() {
  const context = state.selected;
  if (!context?.contextReady) {
    renderD0Boards();
    return;
  }
  const token = ++state.d0PulseRequest;
  state.d0PulseDirectory = { status: "LOADING" };
  renderD0Boards();
  try {
    if (context.profileResolution) await context.profileResolution;
    if (token !== state.d0PulseRequest || state.selected !== context) return;
    if (
      !context.profile_revision ||
      !context.industry_snapshot_id ||
      !context.concept_snapshot_id
    ) {
      state.d0PulseDirectory = {
        status: "NOT_COMPUTED",
        reason_codes: ["PULSE_DIRECTORY_PROFILE_OR_SNAPSHOT_NOT_BOUND"],
      };
      renderD0Boards();
      return;
    }
    const query = new URLSearchParams({
      target_session: context.target_session,
      research_cutoff: context.research_cutoff,
      source_package_id: String(context.source_package_id),
      profile_revision: context.profile_revision,
      industry_snapshot_id: String(context.industry_snapshot_id),
      concept_snapshot_id: String(context.concept_snapshot_id),
      board_kind: $("d0BoardKind").value,
      search: $("d0BoardSearch").value.trim(),
      offset: String(state.d0BoardPage * 15),
      limit: "15",
    });
    const result = await api(`/api/product/sector-pulse-directory?${query}`);
    if (
      token !== state.d0PulseRequest ||
      state.selected !== context ||
      state.d0BoardView !== "PULSE"
    )
      return;
    state.d0PulseDirectory = result;
    renderD0Boards();
  } catch (error) {
    if (token !== state.d0PulseRequest || state.selected !== context) return;
    state.d0PulseDirectory = {
      status: "READ_FAILED",
      error: error.message,
    };
    renderD0Boards();
  }
}

function missingD0Kinds() {
  return state.d0Result?.membership_status?.missing_membership_kinds || [];
}

function renderPulseCohortSummary(comparison) {
  const root = $("pulseCohortSummary");
  root.replaceChildren();
  root.hidden = true;
  if (!comparison) return;

  root.hidden = false;
  root.append(node("h3", "同一T0板块分组后继观察"));
  root.append(
    node(
      "p",
      "仅统计本页T0与同类板块关系；OBSERVED收益只来自完整固定成员篮。跨日期、行业与概念不合并。NON_PIT、RAW_UNADJUSTED；不表示预测能力或正式Rotation。",
      "muted",
    ),
  );
  if (comparison.status !== "AVAILABLE") {
    root.append(
      node(
        "p",
        `分组结果 ${comparison.status || "NOT_COMPUTED"} · ${(comparison.reason_codes || []).join("、") || "PG中尚无完整分组事实"}`,
        "status-line",
      ),
    );
  }

  const stateLabels = {
    TRUE: "T0 PULSE TRUE",
    FALSE: "T0 PULSE FALSE",
    UNKNOWN: "T0 PULSE UNKNOWN",
    PULSE_APPEARED: "PULSE_APPEARED 新现",
    PULSE_PERSISTED: "PULSE_PERSISTED 持续",
    PULSE_CEASED: "PULSE_CEASED 消失",
    PULSE_NOT_PRESENT_BOTH: "PULSE_NOT_PRESENT_BOTH",
    PULSE_TRANSITION_UNKNOWN: "PULSE_TRANSITION_UNKNOWN",
    NO_STORED_TRANSITION: "NO_STORED_TRANSITION（不推断新现）",
  };
  const axisStates = {
    PULSE_STATE: ["TRUE", "FALSE", "UNKNOWN"],
    ADJACENT_TRANSITION: [
      "PULSE_APPEARED",
      "PULSE_PERSISTED",
      "PULSE_CEASED",
      "PULSE_NOT_PRESENT_BOTH",
      "PULSE_TRANSITION_UNKNOWN",
      "NO_STORED_TRANSITION",
    ],
  };
  const countText = (counts) =>
    `OBSERVED/UNKNOWN/NOT_COMPUTED/NOT_DUE/INCOMPLETE ${counts.OBSERVED ?? 0}/${counts.UNKNOWN ?? 0}/${counts.NOT_COMPUTED ?? 0}/${counts.NOT_DUE ?? 0}/${counts.INCOMPLETE ?? 0}`;
  const percentText = (value) =>
    value === null || value === undefined ? "UNKNOWN" : `${value}%`;
  const exactPercent = (value) => {
    if (!value) return "";
    const [numerator, denominator = "1"] = String(value).split("/");
    return `${numerator}/${denominator}%`;
  };

  for (const [kind, kindSummary] of Object.entries(comparison.by_kind || {})) {
    const section = node("section", undefined, "pulse-cohort-kind");
    section.append(
      node(
        "h4",
        `${kind === "INDUSTRY" ? "行业" : "概念"} · T0完整分母 ${kindSummary.expected_board_count ?? "UNKNOWN"} · ${kindSummary.status}`,
      ),
    );
    for (const [axis, stateList] of Object.entries(axisStates)) {
      const heading = node(
        "h5",
        axis === "PULSE_STATE" ? "按T0 PULSE三态" : "按已存相邻变化",
      );
      section.append(heading);
      const table = node("table", undefined, "pulse-cohort-table");
      const thead = node("thead");
      const header = node("tr");
      for (const label of ["T0分组", "完整组分母", "T+1", "T+3", "T+5"])
        header.append(node("th", label));
      thead.append(header);
      table.append(thead);
      const tbody = node("tbody");
      for (const cohortState of stateList) {
        const row = node("tr");
        row.append(node("th", stateLabels[cohortState] || cohortState));
        const firstHorizon = kindSummary.horizons?.[0];
        const firstGroup = firstHorizon?.cohorts?.[axis]?.[cohortState];
        row.append(
          node("td", firstGroup?.total_board_relations ?? "NOT_COMPUTED"),
        );
        for (const horizon of [1, 3, 5]) {
          const horizonResult = kindSummary.horizons?.find(
            (item) => item.horizon_sessions === horizon,
          );
          const group = horizonResult?.cohorts?.[axis]?.[cohortState];
          const cell = node("td", undefined, "pulse-cohort-result");
          if (!group) {
            cell.append(node("strong", "NOT_COMPUTED"));
            row.append(cell);
            continue;
          }
          const counts = group.outcome_status_counts || {};
          const distribution = group.distribution || {};
          const coverage = group.complete_basket_coverage || {};
          const memberQuality = group.fixed_member_quality || {};
          cell.append(
            node("strong", group.status || "UNKNOWN"),
            node("small", countText(counts)),
            node(
              "small",
              `完整篮覆盖 ${coverage.observed_board_relations ?? 0}/${coverage.all_board_relations ?? 0}（${percentText(coverage.percent_4dp)}）`,
            ),
          );
          if (distribution.sample_count > 0) {
            const exact = exactPercent(
              distribution.median_return_percent_exact,
            );
            const exactDetails = node(
              "details",
              undefined,
              "pulse-cohort-exact",
            );
            exactDetails.append(
              node("summary", "查看精确收益比"),
              node("code", exact || "UNKNOWN"),
            );
            cell.append(
              node(
                "small",
                `OBSERVED中位 ${percentText(distribution.median_return_percent_4dp)}；正/非正 ${distribution.positive_board_count}/${distribution.nonpositive_board_count}`,
              ),
              exactDetails,
            );
          } else {
            cell.append(node("small", "无OBSERVED收益样本；不填零收益"));
          }
          if (memberQuality.evaluable_fixed_member_denominator_sum > 0) {
            cell.append(
              node(
                "small",
                `固定成员报价 ${memberQuality.observed_member_count_sum}/${memberQuality.evaluable_fixed_member_denominator_sum}（${percentText(memberQuality.coverage_percent_4dp)}）；固定篮成员总分母 ${memberQuality.fixed_member_denominator_sum}`,
              ),
            );
          } else {
            cell.append(
              node(
                "small",
                `固定篮成员总分母 ${memberQuality.fixed_member_denominator_sum ?? 0}；报价质量 ${counts.NOT_DUE ? "NOT_DUE" : "NOT_COMPUTED"}`,
              ),
            );
          }
          const reasons = group.reason_counts || [];
          if (reasons.length) {
            const details = node("details", undefined, "pulse-cohort-reasons");
            details.append(
              node("summary", `失败/分组原因 ${reasons.length} 项`),
            );
            for (const reason of reasons) {
              details.append(
                node(
                  "small",
                  `${reason.reason_scope}/${reason.quality_status} · ${reason.reason_code} × ${reason.board_relation_count}`,
                ),
              );
            }
            cell.append(details);
          }
          row.append(cell);
        }
        tbody.append(row);
      }
      table.append(tbody);
      const tableScroll = node(
        "div",
        undefined,
        "table-scroll pulse-cohort-table-scroll",
      );
      tableScroll.tabIndex = 0;
      tableScroll.setAttribute("role", "region");
      tableScroll.setAttribute(
        "aria-label",
        `${kind === "INDUSTRY" ? "行业" : "概念"}${axis === "PULSE_STATE" ? "PULSE状态" : "相邻变化"}期限表，可横向滚动`,
      );
      tableScroll.append(table);
      section.append(tableScroll);
    }
    root.append(section);
  }
  root.append(
    node(
      "p",
      "板块成员可能跨板重叠；本组为当前T0的NON_PIT描述性回看，不构成足量独立日期预测验证。价格缺失按固定篮保留 UNKNOWN。",
      "muted pulse-cohort-caution",
    ),
  );
}

function renderD0Boards() {
  const rows = $("d0BoardRows");
  rows.replaceChildren();
  const d0ScopeStatus = state.d0Result?.stored_evidence_status?.d0 || "UNKNOWN";
  const explicitPulseMode = state.d0BoardView === "PULSE";
  const savedPulseDirectory = state.d0Result?.pulse_followup_directory;
  const fallbackPulseMode =
    !explicitPulseMode &&
    d0ScopeStatus !== "AVAILABLE" &&
    savedPulseDirectory?.status === "AVAILABLE";
  const pulseDirectory = explicitPulseMode
    ? state.d0PulseDirectory
    : savedPulseDirectory;
  const pulseOnly = explicitPulseMode || fallbackPulseMode;
  renderPulseCohortSummary(
    explicitPulseMode ? pulseDirectory?.cohort_comparison : null,
  );
  $("viewD0Boards").setAttribute("aria-pressed", String(!pulseOnly));
  $("viewPulseBoards").setAttribute("aria-pressed", String(pulseOnly));
  $("viewD0Boards").className = pulseOnly ? "button subtle" : "button primary";
  $("viewPulseBoards").className = pulseOnly
    ? "button primary"
    : "button subtle";
  $("d0BoardMode").textContent = pulseOnly
    ? "全部已存 T0 PULSE · 当前成员快照 NON_PIT"
    : "FALSE→TRUE · 当前成员快照 NON_PIT";
  $("d0BoardNameHeading").textContent = pulseOnly ? "板块" : "变化板块";
  $("d0BoardDescription").textContent = pulseOnly
    ? "按官方会话浏览全部已存行业与概念 PULSE；点击板块查看连续状态、市场背景、到期后继与成员。"
    : "先浏览同日已证 D0 新增变化；成员快照回看始终标为 NON_PIT。";
  $("d0BoardCountHeading").textContent = pulseOnly
    ? "T+1 后继结果"
    : "D0 新增成员";
  $("d0BoardPulseHeading").textContent = pulseOnly ? "T0 PULSE" : "跨日 PULSE";
  const addEmptyRow = (text) => {
    const row = node("tr");
    const cell = node("td", text, "empty");
    cell.colSpan = 7;
    row.append(cell);
    rows.append(row);
  };
  if (
    explicitPulseMode &&
    (!pulseDirectory || pulseDirectory.status === "LOADING")
  ) {
    const status = "正在读取PG中全部已存 PULSE 板块目录…";
    message("d0BoardSummary", status);
    message("d0BoardPage", "—");
    $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
    addEmptyRow(status);
    return;
  }
  if (
    explicitPulseMode &&
    !["AVAILABLE", "PARTIAL"].includes(pulseDirectory?.status)
  ) {
    const status = `PULSE目录 ${pulseDirectory?.status || "NOT_COMPUTED"} · ${(pulseDirectory?.reason_codes || []).join("、") || pulseDirectory?.error || "NO_STORED_FACT"}`;
    message(
      "d0BoardSummary",
      `${state.selected?.target_session || ""} · ${status}`,
    );
    message("d0BoardPage", "总数 UNKNOWN");
    $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
    addEmptyRow(status);
    return;
  }
  const boards = explicitPulseMode
    ? pulseDirectory.boards || []
    : pulseOnly
      ? pulseDirectory?.boards || []
      : provedD0Boards();
  if (
    !explicitPulseMode &&
    (!state.d0Result || !Array.isArray(state.d0Result.boards))
  ) {
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

  if (d0ScopeStatus !== "AVAILABLE" && !pulseOnly) {
    const status = `NO_STORED_FACT / D0 ${d0ScopeStatus}：没有已存完整相邻事实；总数未知，不解释为零变化。`;
    message(
      "d0BoardSummary",
      `${state.selected?.target_session || ""} · ${status}`,
    );
    message("d0BoardPage", `总数 UNKNOWN（${d0ScopeStatus}）`);
    $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
    addEmptyRow(status);
    return;
  }

  const missing = pulseOnly ? [] : missingD0Kinds();
  if (boards.length === 0) {
    const status = pulseOnly
      ? "当前范围没有可读取的完整已存 PULSE 板块目录。"
      : missing.length
        ? `NO_PROVED_D0_CHANGE；成员关系 UNKNOWN：${missing.join("、")}`
        : "NO_PROVED_D0_CHANGE：当前同源范围没有已证 FALSE→TRUE 变化板块。";
    message(
      "d0BoardSummary",
      `${state.selected?.target_session || ""} · ${status}`,
    );
    message(
      "d0BoardPage",
      pulseOnly ? "0 个已存 PULSE 板块" : "0 个已证变化板块",
    );
    $("d0BoardPrev").disabled = $("d0BoardNext").disabled = true;
    addEmptyRow(status);
    return;
  }

  const kind = $("d0BoardKind").value;
  const query = $("d0BoardSearch").value.trim().toLocaleLowerCase();
  const filtered = explicitPulseMode
    ? boards
    : boards.filter((board) => {
        if (kind !== "ALL" && board.board_kind !== kind) return false;
        const searchable =
          `${displayBoardName(board)} ${board.board_native_id || ""}`.toLocaleLowerCase();
        return !query || searchable.includes(query);
      });
  const filteredTotal = explicitPulseMode
    ? Number(pulseDirectory.filtered_total || 0)
    : filtered.length;
  const lastPage = Math.max(0, Math.ceil(filteredTotal / 15) - 1);
  state.d0BoardPage = Math.min(state.d0BoardPage, lastPage);
  const start = state.d0BoardPage * 15;
  const page = explicitPulseMode ? boards : filtered.slice(start, start + 15);
  const industryCount = explicitPulseMode
    ? (pulseDirectory.partitions?.INDUSTRY?.actual_board_count ?? "UNKNOWN")
    : boards.filter((item) => item.board_kind === "INDUSTRY").length;
  const conceptCount = explicitPulseMode
    ? (pulseDirectory.partitions?.CONCEPT?.actual_board_count ?? "UNKNOWN")
    : boards.filter((item) => item.board_kind === "CONCEPT").length;
  const missingText = missing.length
    ? ` · 成员关系 UNKNOWN：${missing.join("、")}`
    : "";
  const orderText = pulseOnly
    ? " · 按板块类型、名称和编号排序"
    : state.d0Result.board_selection?.precedence?.[0]
      ? ` · ${state.d0Result.board_selection.precedence[0]}`
      : " · 按 D0 新增成员关系数降序";
  const membershipQuality = membershipPageScopeSummary(
    state.d0Result?.membership_status,
  );
  const summary = pulseOnly
    ? `${state.selected?.target_session || ""} · 已存 PULSE ${pulseDirectory.total_board_count ?? boards.length}（行业 ${industryCount} / 概念 ${conceptCount}） · PULSE TRUE/FALSE/UNKNOWN ${pulseDirectory.pulse_state_counts?.TRUE ?? "UNKNOWN"}/${pulseDirectory.pulse_state_counts?.FALSE ?? "UNKNOWN"}/${pulseDirectory.pulse_state_counts?.UNKNOWN ?? "UNKNOWN"} · T+1 OBSERVED/UNKNOWN/NOT_DUE ${pulseDirectory.t1_status_counts?.OBSERVED ?? 0}/${pulseDirectory.t1_status_counts?.UNKNOWN ?? 0}/${pulseDirectory.t1_status_counts?.NOT_DUE ?? 0} · NON_PIT`
    : `${state.selected?.target_session || ""} · 已证变化板块 ${boards.length}（行业 ${industryCount} / 概念 ${conceptCount}） · 首页精选 ${state.d0Result.board_selection?.home_featured_count ?? Math.min(boards.length, 15)} · 筛选 ${filtered.length}${orderText} · ${membershipQuality} · 成员关系可重叠 · NON_PIT${missingText}`;
  message(
    "d0BoardSummary",
    pulseOnly ? `${summary} · 筛选 ${filteredTotal}` : summary,
  );
  if (!filtered.length) {
    addEmptyRow(
      pulseOnly
        ? "当前筛选没有匹配的已存 PULSE 板块；清除筛选可恢复完整列表。"
        : "当前筛选没有匹配的已证 D0 变化板块；清除筛选可恢复完整列表。",
    );
  } else {
    for (const [index, board] of page.entries()) {
      const row = node("tr");
      const name = node("td");
      name.append(
        node("strong", displayBoardName(board)),
        node(
          "small",
          `${board.home_selection_reason || board.observation_basis} · ${boardSourceQualitySummary(board)}`,
        ),
      );
      row.append(
        name,
        node("td", board.board_kind === "INDUSTRY" ? "行业" : "概念"),
        node(
          "td",
          pulseOnly
            ? boardPulseFollowupSummary(board)
            : format(board.new_member_count),
        ),
        node(
          "td",
          pulseOnly
            ? `${boardPulseSummary(board)} · ${board.cross_period_pulse?.dq5_pp?.decimal_4dp ?? "UNKNOWN"} · breadth Δ ${board.cross_period_pulse?.breadth_delta1?.decimal_4dp ?? "UNKNOWN"}`
            : boardPulseSummary(board),
        ),
        node("td", board.observed_at || "UNKNOWN"),
        node("td", "NON_PIT"),
      );
      const action = node("td");
      const button = node(
        "button",
        pulseOnly
          ? "查看连续 PULSE 与成员 →"
          : board.home_featured
            ? "首页精选 · 查看成员 →"
            : "完整列表 · 查看成员 →",
        "text-button",
      );
      button.type = "button";
      button.addEventListener("click", () => openD0Board(board));
      action.append(button);
      row.append(action);
      rows.append(row);
      row.setAttribute("data-d0-board-index", String(start + index));
    }
  }
  const totalPages = Math.max(1, Math.ceil(filteredTotal / 15));
  message(
    "d0BoardPage",
    `第 ${state.d0BoardPage + 1}/${totalPages} 页 · ${page.length}/${filteredTotal} 个${pulseOnly ? "PULSE板块" : "板块"}`,
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
  const sourceChange = state.d0Result.source_change_status || {};
  const sourceKeySet = sourceChange.source_key_set || {};
  const transitions = sourceChange.transition_counts || {};
  const transitionText = Object.entries(transitions)
    .map(([name, value]) => `${name} ${format(value)}`)
    .join(" · ");
  const missingEndpoints = {
    BEFORE_ONLY: sourceKeySet.before_only_count,
    AFTER_ONLY: sourceKeySet.after_only_count,
  };
  const boards = provedD0Boards();
  const targetSourceCount = sourceKeySet.after_count;
  const d0Share =
    Number.isFinite(Number(count)) &&
    Number.isFinite(Number(targetSourceCount)) &&
    Number(targetSourceCount) > 0
      ? ((Number(count) / Number(targetSourceCount)) * 100).toFixed(2) + "%"
      : "UNKNOWN";
  root.append(
    node(
      "p",
      `目标日 ${state.d0Result.target_session || state.selected?.target_session || "UNKNOWN"} · D0 新增 ${format(count)} 只独立来源代码；这不是正式选股结果。`,
    ),
  );
  root.append(
    node(
      "p",
      `完整同源交集九态变化（${format(sourceKeySet.comparable_intersection_count)} 个可比较来源代码）：${transitionText || "UNKNOWN"}；前态缺失 ${format(missingEndpoints.BEFORE_ONLY)}、目标态缺失 ${format(missingEndpoints.AFTER_ONLY)}。`,
    ),
  );
  const market = state.marketResult;
  root.append(
    node(
      "p",
      `目标Raw分母 ${format(targetSourceCount)} · D0新增占来源池 ${d0Share}；市场环境宽度 ${market?.status === "AVAILABLE" ? market.metric?.exact?.fraction || "已证数值" : market?.status || "尚未读取"}（来源分母 ${format(market?.denominator?.N_source)}）。`,
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
    const relationTotal = boards.reduce(
      (sum, board) => sum + Number(board.new_member_count || 0),
      0,
    );
    const concentrated = [...boards]
      .sort(
        (left, right) =>
          Number(right.new_member_count || 0) -
            Number(left.new_member_count || 0) ||
          String(left.board_name || "").localeCompare(
            String(right.board_name || ""),
            "zh-Hans-CN",
          ),
      )
      .slice(0, 5)
      .map((board) => {
        const count = Number(board.new_member_count || 0);
        const percent = relationTotal
          ? ((count / relationTotal) * 100).toFixed(1)
          : "0.0";
        return `${board.board_name} ${count}/${relationTotal}条板块关系（${percent}%）`;
      })
      .join("；");
    root.append(
      node(
        "p",
        `扩散/集中观察：已证板块内D0新增关系 ${format(relationTotal)} 条，对应全市场去重 D0 新增 ${format(count)} 只；关系允许重叠。关系数前五：${concentrated || "无已证变化板块"}。这是现存成员关系分布，不是交易强度或选股评分。`,
      ),
    );
  }
}

async function loadFocusMarketContext(context) {
  if (!context?.target_session || !context?.source_package_id) return;
  const key = `${context.target_session}:${context.source_package_id}`;
  if (state.focusMarketKey === key) return;
  state.focusMarketKey = key;
  const request = ++state.focusMarketRequest;
  message("focusMarketContext", "读取已保存的市场宽度事实…");
  try {
    const params = new URLSearchParams({
      target_session: context.target_session,
      source_package_id: String(context.source_package_id),
    });
    const result = await api(`/api/product/market?${params}`);
    if (request !== state.focusMarketRequest || state.selected !== context)
      return;
    if (!result.denominator) {
      message(
        "focusMarketContext",
        `市场宽度 ${result.status || "UNKNOWN"}：${result.reason || "精确日期与来源没有已保存的宽度事实。"}`,
      );
      return;
    }
    const counts = result.denominator.counts || {};
    const positive =
      Number(counts.COMPARABLE_GAIN || 0) +
      Number(counts.COMPARABLE_BOTH_POSITIVE || 0);
    message(
      "focusMarketContext",
      `市场环境（来源宽度 ${result.status}）· Raw ${format(result.denominator.N_source)} · 可评价 ${format(result.denominator.N_eligible)} · 目标日正收益 ${format(positive)} · UNKNOWN ${format(result.denominator.UNKNOWN)}。观察分组以 Raw 池为分母；两种分母不等同，正式市场阶段为 NOT_READY。`,
    );
  } catch (error) {
    if (request === state.focusMarketRequest && state.selected === context) {
      state.focusMarketKey = null;
      message("focusMarketContext", `市场宽度 UNKNOWN：${error.message}`);
    }
  }
}

function s2ContinuitySummaryText(outcome) {
  if (!outcome) return "同源S2跨日观察未读取 · NOT_COMPUTED";
  const continuity = outcome.s2_continuity;
  if (!continuity) return "同源S2跨日观察未返回 · NOT_COMPUTED";
  if (continuity.status === "AVAILABLE") {
    return `同源S2观察 ${continuity.target_session} ${continuity.t0_state || "UNKNOWN"} → ${continuity.successor_session} ${continuity.t1_state || "UNKNOWN"}（${continuity.transition || "UNKNOWN"}）· 画像 ${continuity.t0_profile_revision || "UNKNOWN"} → ${continuity.t1_profile_revision || "UNKNOWN"} · 后验谓词观察，不改变正式Seed、Focus资格或买入判断`;
  }
  if (continuity.status === "NOT_DUE") {
    return `同源S2跨日观察 NOT_DUE · ${continuity.target_session || "目标日未知"} · ${continuity.reason_codes?.join("、") || "等待合法后继会话"}`;
  }
  return `同源S2跨日观察 ${continuity.status || "UNKNOWN"} · ${continuity.reason_codes?.join("、") || "后继同源画像事实不可用"}`;
}

function s2ContinuityReasonText(outcome) {
  const continuity = outcome?.s2_continuity;
  if (!continuity) return "同源S2两端原因：未读取。";
  const before = continuity.t0_reasons || [];
  const after = continuity.t1_reasons || [];
  const statusReasons = continuity.reason_codes || [];
  const parts = [
    before.length ? `T0：${before.join("、")}` : "T0：无额外原因码",
  ];
  if (continuity.successor_session) {
    parts.push(after.length ? `T+1：${after.join("、")}` : "T+1：无额外原因码");
  }
  if (statusReasons.length) parts.push(`范围：${statusReasons.join("、")}`);
  return parts.join("；");
}

function formatCohortReturn(value) {
  if (!value || typeof value.percent !== "string") return "不可评价";
  const parsed = Number(value.percent);
  return `${parsed > 0 ? "+" : ""}${value.percent}%`;
}

function formatCohortCoverage(row) {
  const observed = row.endpoint_observed_count ?? 0;
  const denominator = row.member_count ?? 0;
  const percent = denominator
    ? `（${((observed / denominator) * 100).toFixed(1)}%）`
    : "（无分母）";
  return `${format(observed)} / ${format(denominator)} ${percent} · 端点UNKNOWN ${format(row.endpoint_unknown_count ?? 0)}`;
}

function formatCohortCenter(row) {
  return `${formatCohortReturn(row.mean_return)} / ${formatCohortReturn(row.median_return)}`;
}

function formatCohortMedian(value) {
  if (typeof value !== "string") return "不可评价";
  try {
    const [numeratorText, denominatorText = "1"] = value.split("/", 2);
    const numerator = BigInt(numeratorText);
    const denominator = BigInt(denominatorText);
    if (denominator === 0n) return "不可评价";
    const negative = numerator < 0n !== denominator < 0n;
    const absoluteNumerator = numerator < 0n ? -numerator : numerator;
    const absoluteDenominator = denominator < 0n ? -denominator : denominator;
    const scaled =
      (absoluteNumerator * 1000000n + absoluteDenominator / 2n) /
      absoluteDenominator;
    const whole = scaled / 10000n;
    const decimal = String(scaled % 10000n).padStart(4, "0");
    const sign = negative && scaled !== 0n ? "-" : "";
    return `${sign}${whole}.${decimal}%`;
  } catch {
    return "不可评价";
  }
}

const cohortMetricKey = Symbol("cohortMetric");

function cohortMetricValue(value, kind = "return") {
  return { [cohortMetricKey]: true, value, kind };
}

function appendCohortMetric(container, value, kind = "return") {
  const metric = node("span", undefined, "cohort-fraction");
  const percentage = formatCohortMedian(value);
  metric.append(
    node("span", kind === "coverage" ? `覆盖率 ${percentage}` : percentage),
  );
  if (typeof value === "string") {
    const exact = node("details", undefined, "cohort-exact-value");
    exact.append(
      node(
        "summary",
        kind === "coverage" ? "查看精确覆盖率比" : "查看精确收益比",
      ),
      node("code", value),
    );
    metric.append(exact);
  }
  container.append(metric);
}

function rpsChangeLabel(value) {
  return (
    {
      IMPROVED: "前移",
      WORSENED: "后退",
      UNCHANGED: "不变",
      UNKNOWN: "UNKNOWN",
      NOT_COMPARABLE: "NOT_COMPARABLE",
      NOT_COMPUTED: "NOT_COMPUTED",
    }[value] ||
    value ||
    "UNKNOWN"
  );
}

function pulseGroupLabel(group) {
  const board = group.board_kind === "INDUSTRY" ? "行业" : "概念";
  return `${board} · RPS5 ${rpsChangeLabel(group.rps5_change_state)} × RPS20 ${rpsChangeLabel(group.rps20_change_state)} × PULSE ${group.pulse_status}:${group.pulse_state} · ${cohortGroupLabel(group.rule_state)}`;
}

const cohortRpsScenarios = [
  {
    id: "DOUBLE_IMPROVED_PULSE_FALSE",
    label: "RPS5/RPS20双前移 · PULSE FALSE",
    matches: (group) =>
      group.rps5_change_state === "IMPROVED" &&
      group.rps20_change_state === "IMPROVED" &&
      group.pulse_status === "AVAILABLE" &&
      group.pulse_state === "FALSE",
  },
  {
    id: "DOUBLE_IMPROVED_PULSE_TRUE",
    label: "RPS5/RPS20双前移 · PULSE TRUE",
    matches: (group) =>
      group.rps5_change_state === "IMPROVED" &&
      group.rps20_change_state === "IMPROVED" &&
      group.pulse_status === "AVAILABLE" &&
      group.pulse_state === "TRUE",
  },
  {
    id: "RPS5_UP_RPS20_DOWN_PULSE_TRUE",
    label: "RPS5前移、RPS20后退 · PULSE TRUE",
    matches: (group) =>
      group.rps5_change_state === "IMPROVED" &&
      group.rps20_change_state === "WORSENED" &&
      group.pulse_status === "AVAILABLE" &&
      group.pulse_state === "TRUE",
  },
  {
    id: "DOUBLE_WORSENED_PULSE_TRUE",
    label: "RPS5/RPS20双后退 · PULSE TRUE",
    matches: (group) =>
      group.rps5_change_state === "WORSENED" &&
      group.rps20_change_state === "WORSENED" &&
      group.pulse_status === "AVAILABLE" &&
      group.pulse_state === "TRUE",
  },
  {
    id: "QUALITY_UNKNOWN",
    label: "不可比 / UNKNOWN / NOT_COMPUTED 质量分母",
    matches: (group) =>
      !["IMPROVED", "WORSENED", "UNCHANGED"].includes(
        group.rps5_change_state,
      ) ||
      !["IMPROVED", "WORSENED", "UNCHANGED"].includes(
        group.rps20_change_state,
      ) ||
      group.pulse_status !== "AVAILABLE" ||
      !["TRUE", "FALSE"].includes(group.pulse_state),
  },
];

function appendCohortRpsExamples(list, examples, type) {
  if (!Array.isArray(examples) || !examples.length) return;
  for (const example of examples) {
    const item = node("li");
    const button = node("button", example.source_symbol, "text-button");
    button.title = "在当前Focus/Cohort中打开该来源代码";
    button.addEventListener("click", () => {
      $("cohortSymbol").value = example.source_symbol.slice(3);
      cohortOffset = 0;
      void readCohort();
    });
    if (type === "return") {
      item.append(button, node("span", ` · ${example.board_native_id} · `));
      appendCohortMetric(item, example.return_fraction);
      item.append(node("span", ` · ${example.endpoint_session}`));
    } else {
      const detail = ` · ${example.board_native_id} · ${example.outcome_state} · ${example.endpoint_reason || (example.path_reasons || []).join("、") || "无失败原因"}`;
      item.append(button, node("span", detail));
    }
    list.append(item);
  }
}

function renderCohortRpsPulse(summary) {
  const root = $("cohortRpsPulseSummary");
  const status = $("cohortRpsPulseStatus");
  const groupSelect = $("cohortRpsGroup");
  root.replaceChildren();
  if (!summary) {
    status.textContent =
      "分组统计尚未读取；普通Cohort不会触发RPS×PULSE分组聚合。";
    groupSelect.replaceChildren(new Option("全部非空组合", "ALL"));
    return;
  }
  const priorGroup = groupSelect.value;
  const groups = Array.isArray(summary.groups) ? summary.groups : [];
  groupSelect.replaceChildren(new Option("全部非空组合", "ALL"));
  const availableScenarios = cohortRpsScenarios.filter((scenario) =>
    groups.some(scenario.matches),
  );
  for (const scenario of availableScenarios)
    groupSelect.add(new Option(scenario.label, scenario.id));
  if (availableScenarios.some((scenario) => scenario.id === priorGroup))
    groupSelect.value = priorGroup;
  else if (availableScenarios.length)
    groupSelect.value = availableScenarios[0].id;
  else groupSelect.value = "ALL";

  const groupedRelations = groups.reduce(
    (total, group) => total + group.relation_count,
    0,
  );
  const rps5Distribution = {};
  const rps20Distribution = {};
  const pulseDistribution = {};
  for (const group of groups) {
    const count = group.relation_count;
    rps5Distribution[group.rps5_change_state] =
      (rps5Distribution[group.rps5_change_state] || 0) + count;
    rps20Distribution[group.rps20_change_state] =
      (rps20Distribution[group.rps20_change_state] || 0) + count;
    const pulseKey = `${group.pulse_status}:${group.pulse_state}`;
    pulseDistribution[pulseKey] = (pulseDistribution[pulseKey] || 0) + count;
  }

  const rootSummary = `${summary.status || "UNKNOWN"} · ${summary.target_session || "UNKNOWN_T0"} · 捕获 ${summary.source_capture_id ?? "UNKNOWN"} · 来源登记 ${format(summary.source_registration_count)} · T0观察关系 ${format(summary.t0_observation_relation_rows)} · 缺T0观察登记 ${format(summary.registration_without_t0_observation_count)} · 全板类映射关系 ${format(summary.mapped_board_relation_count)} · 当前筛选关系 ${format(summary.selected_board_relation_count)} · 未知成员 ${format(summary.unknown_membership_relation_count)} · 无成员记录 ${format(summary.no_stored_membership_relation_count)} · 当前板类非空组合 ${format(groups.length)}`;
  const closure = `${format(groupedRelations)}/${format(summary.selected_board_relation_count)} 关系`;
  status.textContent = `${rootSummary} · 组内完整分母 ${closure}${summary.change_fact_status === "INCOMPLETE" ? ` · RPS关系事实未闭合 ${summary.missing_change_fact_count}` : ""}`;
  if (!groups.length) {
    root.append(
      node(
        "p",
        `${summary.status || "NO_NONEMPTY_GROUPS"} · 此冻结规则、日期和板类没有非空RPS×PULSE关系组合。`,
      ),
    );
    return;
  }

  root.append(
    node(
      "p",
      `完整状态分布 · RPS5 ${JSON.stringify(rps5Distribution)} · RPS20 ${JSON.stringify(rps20Distribution)} · PULSE ${JSON.stringify(pulseDistribution)} · 分组关系合计 ${closure}。分组表之间的关系数可因同一股票的不同板块或登记而重复。`,
    ),
  );

  const selectedGroup = groupSelect.value;
  const selectedRuleState = $("cohortRpsRuleState").value;
  const visibleGroups = groups.filter(
    (group) =>
      (selectedGroup === "ALL" ||
        cohortRpsScenarios
          .find((scenario) => scenario.id === selectedGroup)
          ?.matches(group)) &&
      (selectedRuleState === "ALL" || group.rule_state === selectedRuleState),
  );
  if (!visibleGroups.length) {
    root.append(node("p", "当前筛选下没有已存非空组；不会创建零样本分组。"));
    return;
  }

  for (const group of visibleGroups) {
    const section = node("section", null, "cohort-rps-pulse-group");
    section.append(
      node("h4", pulseGroupLabel(group)),
      node(
        "p",
        `完整登记×股票×板块×T0关系 ${format(group.relation_count)} · 去重股票×板×T0 ${format(group.unique_stock_board_t0_count)} · 独立证券 ${format(group.independent_security_count)} · T+5已到期独立T0 ${format(group.independent_t0_date_count)} / 已存同组T0 ${format(group.independent_t0_saved_date_count)}（未到期 ${format(group.independent_t0_not_due_date_count)}）· ${group.independent_t0_date_status || "NOT_COMPUTED"} · 来源原因 ${JSON.stringify(group.source_pool_reasons || {})} · RPS5原因 ${JSON.stringify(group.rps5_reasons || {})} · RPS20原因 ${JSON.stringify(group.rps20_reasons || {})}`,
      ),
    );
    const scroll = node("div", null, "cohort-rps-pulse-table-scroll");
    const table = node("table", null, "cohort-rps-pulse-table");
    const head = node("thead");
    const header = node("tr");
    for (const label of [
      "期限",
      "到期 / T+5日期质量",
      "完整关系分母",
      "Path已存 / 缺失",
      "端点OBSERVED / UNKNOWN / INCOMPLETE",
      "正 / 非正",
      "中位收益",
      "覆盖（全部 / 已到期）",
      "失败原因与真实例子",
    ])
      header.append(node("th", label));
    head.append(header);
    const body = node("tbody");
    for (const horizon of [1, 3, 5]) {
      const outcome = group.outcomes?.[String(horizon)];
      if (!outcome) continue;
      const row = node("tr");
      const maturity = outcome.due
        ? `到期 ${outcome.endpoint_session}`
        : "NOT_DUE";
      const independentDates =
        horizon === 5
          ? outcome.independent_t0_date_status === "NOT_COMPUTED"
            ? " · 同组独立T0日期 NOT_COMPUTED"
            : ` · 同组T+5已到期独立T0 ${format(outcome.independent_t0_date_count)}日 / 已存同组T0 ${format(outcome.independent_t0_saved_date_count)}（未到期 ${format(outcome.independent_t0_not_due_date_count)}）· ${outcome.independent_t0_date_status}`
          : "";
      const pathSummary = `${format(outcome.path_stored_count)} / ${format(outcome.path_missing_count)} · 未到期 ${format(outcome.not_due_count)}`;
      const endpointSummary = `${format(outcome.endpoint_observed_count)} / ${format(outcome.endpoint_unknown_count)} / ${format(outcome.incomplete_count)}`;
      row.append(
        node("td", `T+${horizon}`),
        node("td", `${maturity}${independentDates}`),
        node("td", format(outcome.relation_denominator)),
        node("td", pathSummary),
        node("td", endpointSummary),
        node(
          "td",
          `${format(outcome.positive_count)} / ${format(outcome.non_positive_count)}`,
        ),
        (() => {
          const cell = node("td");
          appendCohortMetric(cell, outcome.median_return);
          return cell;
        })(),
        node(
          "td",
          `${outcome.coverage_fraction || "—"} · ${outcome.coverage_of_due || "—"}`,
        ),
      );
      const reasonCell = node("td");
      const reasons = Object.entries(outcome.reason_counts || {})
        .map(([reason, count]) => `${reason} ${count}`)
        .join("；");
      reasonCell.append(node("span", reasons || "无已存失败原因"));
      if (outcome.examples?.length) {
        const details = node("details");
        details.append(node("summary", "负收益关系例子"));
        const list = node("ul", null, "cohort-rps-pulse-examples");
        appendCohortRpsExamples(list, outcome.examples, "return");
        details.append(list);
        reasonCell.append(details);
      }
      if (outcome.failure_examples?.length) {
        const details = node("details");
        details.append(node("summary", "未知 / 缺失关系例子"));
        const list = node("ul", null, "cohort-rps-pulse-examples");
        appendCohortRpsExamples(list, outcome.failure_examples, "failure");
        details.append(list);
        reasonCell.append(details);
      }
      row.append(reasonCell);
      body.append(row);
    }
    table.append(head, body);
    scroll.append(table);
    section.append(scroll);
    root.append(section);
  }
}

function renderFocusS2T1Cohort(summary) {
  const status = $("focusS2T1CohortStatus");
  const tables = $("focusS2T1CohortTables");
  tables.replaceChildren();
  if (!summary || summary.status !== "AVAILABLE") {
    const state = summary?.status || "UNKNOWN";
    const detail = summary?.reason_codes?.join("、") || "没有可证明的完整统计";
    status.textContent =
      state === "NOT_DUE"
        ? `T+1后继 ${state} · ${detail}；当前日期仍显示自身Seed/D0状态，不借用前一日统计。`
        : `S2固定组T+1摘要 ${state} · ${detail}；未知或未计算不解释为零。`;
    return;
  }

  status.textContent =
    `${summary.target_session}固定T0 → ${summary.endpoint_session}已存T+1 · ` +
    `完整来源分母 ${format(summary.source_pool_count)} · ` +
    `画像窗口 ${summary.history_session_counts.join(" → ")} · ` +
    `Seed ${summary.producer_contract} / ${summary.producer_version} · ` +
    `日历 ${summary.calendar_revision} · ${summary.research_mode} / ${summary.price_coordinate}`;

  const overallHeading = node("h4", "T0固定S2三态组");
  const overallTable = node("table");
  overallTable.className = "focus-s2-t1-table focus-s2-t1-table--groups";
  const overallHead = node("thead");
  const overallHeader = node("tr");
  [
    "T0 S2组",
    "组分母",
    "T+1可评价覆盖 / 端点UNKNOWN",
    "均值 / 中位 OBSERVED_ONLY",
    "正 / 非正",
    "最小～最大",
  ].forEach((label) => overallHeader.append(node("th", label)));
  overallHead.append(overallHeader);
  overallTable.append(overallHead);
  const overallBody = node("tbody");
  for (const row of summary.groups) {
    const tr = node("tr");
    [
      row.t0_s2_state,
      format(row.member_count),
      formatCohortCoverage(row),
      formatCohortCenter(row),
      `${format(row.positive_count)} / ${format(row.non_positive_count)}`,
      `${formatCohortReturn(row.min_return)} ～ ${formatCohortReturn(row.max_return)}`,
    ].forEach((value) => tr.append(node("td", value)));
    overallBody.append(tr);
  }
  overallTable.append(overallBody);
  const overallScroll = node(
    "div",
    "",
    "table-scroll focus-s2-t1-table-scroll",
  );
  overallScroll.append(overallTable);

  const trueRows = summary.t0_true_t1_states;
  const trueHeading = node(
    "h4",
    `T0 S2 TRUE：${format(summary.groups.find((row) => row.t0_s2_state === "TRUE")?.member_count)}只的T+1事后分层`,
  );
  const trueTable = node("table");
  trueTable.className = "focus-s2-t1-table focus-s2-t1-table--transitions";
  const trueHead = node("thead");
  const trueHeader = node("tr");
  [
    "T+1 S2状态（事后）",
    "组内人数",
    "T+1收盘覆盖 / 端点UNKNOWN",
    "均值 / 中位 OBSERVED_ONLY",
    "正 / 非正",
    "冻结S2未满足原因（股票频数）",
  ].forEach((label) => trueHeader.append(node("th", label)));
  trueHead.append(trueHeader);
  trueTable.append(trueHead);
  const trueBody = node("tbody");
  for (const row of trueRows) {
    const reasons = row.failure_reasons.length
      ? row.failure_reasons
          .map((item) => `${item.code}（${format(item.stock_count)}）`)
          .join("；")
      : row.t1_s2_state === "TRUE"
        ? "无不持续原因"
        : "没有已存原因码";
    const tr = node("tr");
    [
      row.t1_s2_state,
      format(row.member_count),
      formatCohortCoverage(row),
      formatCohortCenter(row),
      `${format(row.positive_count)} / ${format(row.non_positive_count)}`,
      reasons,
    ].forEach((value) => tr.append(node("td", value)));
    trueBody.append(tr);
  }
  trueTable.append(trueBody);
  const trueScroll = node("div", "", "table-scroll focus-s2-t1-table-scroll");
  trueScroll.append(trueTable);
  tables.append(overallHeading, overallScroll, trueHeading, trueScroll);
}

function seedT1SummaryText(outcome, includeContinuity = true) {
  if (!outcome) return "个股Seed T+1结果未读取 · NOT_COMPUTED";
  const continuityPrefix = includeContinuity
    ? `${s2ContinuitySummaryText(outcome)} · `
    : "";
  const sourceSeed = outcome.source_seed?.state || "UNKNOWN";
  const s1 = outcome.s1?.state || "UNKNOWN";
  const s2 = outcome.s2?.state || "UNKNOWN";
  const safety = outcome.safety?.state || "UNKNOWN";
  const formalSeed = outcome.formal_seed?.state || "UNKNOWN";
  const condition = `来源形态Seed ${sourceSeed}（S1 ${s1} / S2 ${s2}）· Safety ${safety} · 正式Seed ${formalSeed} · D0 ${outcome.d0_transition_status || "UNKNOWN"} · 九规则 ${outcome.rule_status || "UNKNOWN"}`;
  const endpoint = outcome.endpoint || {};
  const reason =
    [
      ...new Set(
        [
          ...(outcome.reason_codes || []),
          endpoint.reason,
          outcome.t1_price?.reason,
        ].filter(Boolean),
      ),
    ].join("、") || "无原因码";
  if (
    outcome.status === "OBSERVED" &&
    outcome.t0_price?.close &&
    outcome.t1_price?.close &&
    endpoint.return_numerator != null &&
    endpoint.return_denominator != null
  ) {
    return `${continuityPrefix}T+1 ${outcome.successor_session} · RAW_UNADJUSTED 收盘 ${outcome.t1_price.close} / T0 ${outcome.t0_price.close} · close(T+1)/close(T0)-1 = ${endpoint.return_numerator}/${endpoint.return_denominator} · ${condition} · NON_PIT`;
  }
  if (outcome.status === "NOT_DUE")
    return `${continuityPrefix}个股T+1 NOT_DUE · ${condition} · NON_PIT / RAW_UNADJUSTED`;
  return `${continuityPrefix}个股T+1 ${outcome.status || "UNKNOWN"}${outcome.successor_session ? ` ${outcome.successor_session}` : ""} · ${outcome.t1_price?.quality || "UNKNOWN"} ${reason} · ${condition} · NON_PIT / RAW_UNADJUSTED`;
}

function seedT1ReasonText(outcome) {
  if (!outcome) return "Seed冻结原因：未读取。";
  const reasons = outcome.condition_reasons || {};
  const labels = [
    ["来源形态Seed", reasons.source_seed],
    ["S1", reasons.s1],
    ["S2", reasons.s2],
    ["Safety", reasons.safety],
  ];
  const parts = labels
    .filter(([, codes]) => Array.isArray(codes) && codes.length)
    .map(([label, codes]) => `${label}：${codes.join("、")}`);
  return parts.length ? parts.join("；") : "Seed冻结条件没有额外原因码。";
}

function sourceD0SummaryText(d0) {
  if (!d0) return "D0事实未返回。";
  const scenarios = d0.scenarios || {};
  const launch = scenarios.LAUNCH_CONFIRM?.state || "UNKNOWN";
  const recovery = scenarios.RECOVERY_TURN?.state || "UNKNOWN";
  if (d0.status === "NOT_COMPUTED")
    return `D0相邻转移 NOT_COMPUTED；单日 LAUNCH_CONFIRM ${launch} / RECOVERY_TURN ${recovery} 仅为当日原始谓词，不证明 ENTRY。`;
  return `D0相邻转移 ${d0.status || "UNKNOWN"} · LAUNCH_CONFIRM ${launch} / RECOVERY_TURN ${recovery}。`;
}

function formalD0AdmissionSummaryText(admission) {
  if (!admission)
    return "正式D0准入 NOT_COMPUTED · 正式episode、D2、Risk与Priority未计算。";
  const scenarioNames = {
    LAUNCH_CONFIRM: "LAUNCH",
    RECOVERY_TURN: "RECOVERY",
    STRONG_PULLBACK: "PULLBACK",
    TREND_CONTINUE: "CONTINUE",
  };
  const states = admission.scenarios || {};
  const scenarios = Object.entries(states).map(([name, item]) => {
    const raw = item.raw_state || item.raw_status || "NOT_COMPUTED";
    const formal = item.formal_state || "NOT_COMPUTED";
    const reasons = [
      item.reason_code,
      ...(item.reason_codes || []),
      ...(item.raw_reason_codes || []),
    ]
      .filter(Boolean)
      .join("、");
    return `${scenarioNames[name] || name} ${item.message_zh || `Raw ${raw} → 正式 ${formal}/${item.admission_status || "NOT_COMPUTED"}`}${reasons ? ` · ${reasons}` : ""}`;
  });
  return [
    scenarios.join("；") || "LAUNCH / RECOVERY 原始事实 NOT_COMPUTED",
    `Episode ${admission.formal_episode_status || "NOT_COMPUTED"} · D2 ${admission.d2_status || "NOT_COMPUTED"} · Risk ${admission.risk_status || "NOT_COMPUTED"} · Priority ${admission.priority_status || "NOT_COMPUTED"}`,
    `准入原因 ${(admission.reason_codes || []).join("、") || "无额外原因码"}`,
  ].join("；");
}

function formalD0AdmissionHeadline(admission) {
  if (!admission) return "正式D0 / D2 / Risk / Priority NOT_COMPUTED";
  const launch = admission.scenarios?.LAUNCH_CONFIRM || {};
  const recovery = admission.scenarios?.RECOVERY_TURN || {};
  const blockers = [launch.reason_code, ...(launch.reason_codes || [])]
    .filter(Boolean)
    .slice(0, 3)
    .join("、");
  return `正式D0 LAUNCH ${launch.raw_state || "NOT_COMPUTED"}→${launch.formal_state || "NOT_COMPUTED"}/${launch.admission_status || "NOT_COMPUTED"}；RECOVERY ${recovery.raw_state || "NOT_COMPUTED"}→${recovery.formal_state || "NOT_COMPUTED"}/${recovery.admission_status || "NOT_COMPUTED"}${blockers ? ` · ${blockers}` : ""}；Episode / D2 / Risk / Priority NOT_COMPUTED`;
}

function sourceCandidateCohortKey(rule, ruleScope, sourceCaptureId) {
  if (!ruleScope || !$("cohortKey")) return null;
  const matches = Array.from($("cohortKey").options).filter((option) => {
    try {
      const key = JSON.parse(option.value);
      return (
        key.rule === rule &&
        key.capture_count === 1 &&
        Number(key.source_capture_id) === Number(sourceCaptureId) &&
        key.profile_revision === ruleScope.profile_revision &&
        Number(key.history) === Number(ruleScope.history_session_count) &&
        Number(key.d1_history) === Number(ruleScope.d1_history_session_count) &&
        key.window === ruleScope.event_window_start &&
        key.rule_version === ruleScope.rule_version &&
        key.calendar_revision === ruleScope.calendar_revision
      );
    } catch {
      return false;
    }
  });
  return matches.length === 1 ? matches[0].value : null;
}

function sourceCandidateFollowupControls(
  facts,
  ruleScope,
  symbol,
  sourceCaptureId,
) {
  const candidates = Object.entries(facts || {}).filter(
    ([, fact]) => fact?.state === "TRUE" && fact?.failure_line_candidate?.value,
  );
  if (!candidates.length) return null;
  const controls = node("div", null, "source-followup-controls");
  controls.append(
    node(
      "p",
      "来源候选线是价格路径诊断，不表示正式D2失效、Focus退出或交易建议。",
    ),
  );
  for (const [rule] of candidates) {
    const button = node(
      "button",
      `查看 ${rule} 后续触线与 T+1/T+3/T+5`,
      "text-button",
    );
    button.type = "button";
    button.addEventListener("click", async () => {
      const currentContext = state.selected;
      const sourceCode = String(symbol || "").toUpperCase();
      try {
        const resolved = await resolveStockCodeInput(
          sourceCode.split(".").pop(),
          currentContext,
        );
        if (resolved.source_symbol !== sourceCode) {
          message(
            "focusStatus",
            `UNKNOWN：六位代码解析到 ${resolved.source_symbol}，与候选线来源 ${sourceCode} 不一致。`,
          );
          return;
        }
      } catch (error) {
        message(
          "focusStatus",
          `UNKNOWN：无法唯一核对来源代码，${error.message}`,
        );
        return;
      }
      const rawKey = sourceCandidateCohortKey(rule, ruleScope, sourceCaptureId);
      if (!rawKey) {
        message(
          "focusStatus",
          `NOT_COMPUTED：${rule} 没有唯一匹配的已存T0冻结键，未读取其它版本。`,
        );
        return;
      }
      $("cohortKey").value = rawKey;
      $("cohortHorizon").value = "5";
      $("cohortSymbol").value = String(symbol || "")
        .split(".")
        .pop();
      cohortOffset = 0;
      void readCohort();
    });
    controls.append(button);
  }
  return controls;
}

function focusEvidenceDetails(item, context) {
  const details = node("details", null, "focus-evidence");
  details.append(node("summary", "查看规则条件、板块关系与板内比较"));
  const evidence = item.research_evidence || {};
  const seed = evidence.seed || {};
  const rules = evidence.rules || {};
  const facts = rules.facts || {};
  const d1 = evidence.d1 || {};
  const version = item.source_version || {};
  const t0 = item.t0_observation || {};
  const formalD0Admission =
    t0.formal_d0_admission || evidence.formal_d0_admission;
  const registrations = Array.isArray(t0.enrollments) ? t0.enrollments : [];
  details.append(
    node(
      "p",
      `T0 ${t0.status || "UNKNOWN"} · ${t0.target_session || context.target_session} · 包 ${t0.source_package_id ?? "UNKNOWN"} / 捕获 ${t0.source_capture_id ?? "UNKNOWN"} · 画像 ${t0.profile_revision || "UNKNOWN"} · ${t0.price_coordinate || "UNKNOWN"} · NON_PIT_EXPLORATORY`,
    ),
    node(
      "p",
      registrations.length
        ? `已存研究登记：${registrations.map(enrollmentSummaryText).join("；")}`
        : `研究登记：${t0.reason || t0.status || "NO_STORED_FACT"}`,
    ),
    ...registrations.map(enrollmentFollowupHistoryDetails).filter(Boolean),
    node(
      "p",
      item.seed_t1_outcome?.d0_transition_status === "NOT_COMPUTED"
        ? `D0相邻转移 NOT_COMPUTED · Raw来源质量 ${version.source_time_quality || "UNKNOWN"} / ${version.source_clock_role || "UNKNOWN"}`
        : `D0 ${item.d0_change?.before_state || "UNKNOWN"}→${item.d0_change?.after_state || "UNKNOWN"} · ${item.transition || item.seed_t1_outcome?.d0_transition_status || "UNKNOWN"} · Raw来源质量 ${version.source_time_quality || "UNKNOWN"} / ${version.source_clock_role || "UNKNOWN"}`,
    ),
    node("p", formalD0AdmissionSummaryText(formalD0Admission)),
    node(
      "p",
      `来源形态Seed ${seed.source_seed?.state || "UNKNOWN"} · S1 ${seed.s1?.state || "UNKNOWN"} · S2 ${seed.s2?.state || "UNKNOWN"} · Safety ${seed.safety?.state || "UNKNOWN"} · 正式Seed ${item.seed_t1_outcome?.formal_seed?.state || "UNKNOWN"} · 目标收盘 ${seed.target_close_quality || "UNKNOWN"} ${seed.target_close_reason || ""}`,
    ),
    node("p", s2ContinuitySummaryText(item.seed_t1_outcome)),
    node("p", s2ContinuityReasonText(item.seed_t1_outcome)),
    node("p", seedT1SummaryText(item.seed_t1_outcome, false)),
    node("p", `Seed冻结原因：${seedT1ReasonText(item.seed_t1_outcome)}`),
  );
  details.append(
    t0EarlyObservationDetails(
      registrations,
      t0.triage_summary,
      item.source_symbol || item.symbol || null,
      {
        target_session: t0.target_session || context.target_session,
        source_package_id: t0.source_package_id ?? context.source_package_id,
        source_capture_id: t0.source_capture_id ?? context.source_capture_id,
      },
    ),
  );
  const ruleList = node("ul", null, "focus-rule-list");
  for (const [rule, fact] of Object.entries(facts)) {
    const reason = Array.isArray(fact?.reasons) ? fact.reasons.join("、") : "";
    ruleList.append(
      node(
        "li",
        `${rule}：${fact?.state || "UNKNOWN"}${reason ? ` · ${reason}` : " · 未提供原因码"}`,
      ),
    );
  }
  if (Object.keys(facts).length) details.append(ruleList);
  const lineHints = Object.entries(facts)
    .filter(([, fact]) => fact?.failure_line_candidate?.value)
    .map(([rule, fact]) => {
      const line = fact.failure_line_candidate;
      return `${rule} ${line.value.numerator}/${line.value.denominator} · ${line.source} · ${line.price_coordinate} · NON_PIT研究线索`;
    });
  details.append(
    node(
      "p",
      `D1 ${d1.status || "UNKNOWN"} · 结构事实 ${d1.structure_fact_count ?? 0}/${d1.expected_structure_fact_count ?? 5} · 材料锚 ${d1.material_change_anchor_count ?? "UNKNOWN"} · 目标价格 ${d1.target_price_quality || "UNKNOWN"} ${Array.isArray(d1.target_price_reasons) ? d1.target_price_reasons.join("、") : ""}`,
    ),
    node(
      "p",
      lineHints.length
        ? `已保存冻结线索：${lineHints.join("；")}`
        : "没有可显示的合法冻结线索；非TRUE规则或缺少合法原始价格线不会生成失效价。",
    ),
    node(
      "p",
      `来源登记后续 ${t0.followup_status || "NOT_COMPUTED"} / ${t0.followup_reason || "NO_STORED_LANDMARK"}；${formalD0AdmissionSummaryText(formalD0Admission)}`,
    ),
  );
  const candidateFollowupControls = sourceCandidateFollowupControls(
    facts,
    rules.scope || evidence.rule_scope,
    item.source_symbol || item.symbol,
    version.source_capture_id || t0.source_capture_id,
  );
  if (candidateFollowupControls) details.append(candidateFollowupControls);
  const memberships = item.also_in_sectors || [];
  const boardLinks = node("div", null, "focus-board-links");
  if (memberships.length) {
    boardLinks.append(node("strong", "当前行业 / GN（NON_PIT）："));
    for (const membership of memberships) {
      const pulse = membership.pulse_context || {};
      const pulseText =
        pulse.status === "AVAILABLE"
          ? `PULSE ${pulse.state || "UNKNOWN"}`
          : `PULSE ${pulse.status || "UNKNOWN"}`;
      const label = `${membership.board_kind === "INDUSTRY" ? "行业" : "GN"} ${membership.board_name} · 成员源 ${membership.source_quality || membership.membership_quality || "UNKNOWN"} · ${pulseText}`;
      const button = node("button", label, "text-button focus-board-link");
      button.type = "button";
      button.addEventListener("click", () => {
        state.focusAutoBoard = {
          target_session: context.target_session,
          board_kind: membership.board_kind,
          board_native_id: membership.board_native_id,
          board_name: membership.board_name,
        };
        state.focusAutoOffset = 0;
        void loadFocusAuto(context, 0);
      });
      boardLinks.append(button);
    }
  } else if (Array.isArray(item.also_in_sectors)) {
    boardLinks.append(
      node("p", "当前成员关系 UNKNOWN 或没有映射记录；不能解释为无成员。"),
    );
  }
  details.append(boardLinks);
  if (item.board_comparison) {
    for (const metric of ["RPS5", "RPS20"]) {
      const comparison = item.board_comparison[metric] || {};
      details.append(
        node(
          "p",
          comparison.status === "AVAILABLE"
            ? `${metric} 板内已证分数 ${comparison.numerator}/${comparison.denominator} · 严格高于 ${comparison.higher_member_count} 只 · 并列 ${comparison.tied_member_count} 只 · 可比较成员 ${comparison.valid_member_count} · NON_PIT`
            : `${metric} 板内比较 ${comparison.status || "UNKNOWN"} · ${comparison.reason || comparison.quality || "有效分子或正分母缺失"}`,
        ),
      );
    }
  } else if (Array.isArray(item.also_in_sectors) && state.focusAutoBoard) {
    details.append(
      node("p", "所选板块没有可用的同版本RPS比较；缺价与未知值未按零排序。"),
    );
  } else if (Array.isArray(item.also_in_sectors)) {
    details.append(
      node(
        "p",
        "选择上方行业或GN标签，可查看该板块内同版本RPS5/RPS20、并列与缺失分母。",
      ),
    );
  }
  for (const membership of memberships) {
    const pulse = membership.pulse_context || {};
    if (pulse.status !== "AVAILABLE") continue;
    const metrics = ["dq5_pp", "breadth_delta1", "ret5_target", "ret5_prior"]
      .map((key) => {
        const value = pulse[key];
        return value
          ? `${key} ${value.numerator}/${value.denominator}`
          : `${key} 未知`;
      })
      .join(" · ");
    details.append(
      node(
        "p",
        `${membership.board_name} PULSE ${pulse.state || "UNKNOWN"} · ${metrics} · 当前成员回看 NON_PIT；它是可能的反证上下文，不代表该股票观察条件已满足。`,
      ),
    );
  }
  return details;
}

async function loadFocusAuto(context, offset = 0) {
  const token = ++state.focusAutoRequest;
  state.focusAutoOffset = offset;
  const root = $("focusAutoRows");
  root.replaceChildren();
  renderFocusS2T1Cohort({ status: "LOADING" });
  $("focusAutoPrev").disabled = $("focusAutoNext").disabled = true;
  message("focusAutoPage", "—");
  if (!context?.contextReady) {
    message("focusAutoStatus", "正在读取交易日来源上下文…");
    return;
  }
  if (state.focusAutoBoard?.target_session !== context?.target_session)
    state.focusAutoBoard = null;
  const boardFilter = $("focusAutoBoardFilter");
  boardFilter.replaceChildren();
  if (state.focusAutoBoard) {
    boardFilter.append(
      node(
        "span",
        `板块筛选：${state.focusAutoBoard.board_kind === "INDUSTRY" ? "行业" : "GN"} ${state.focusAutoBoard.board_name} · ${state.focusAutoBoard.board_native_id}`,
      ),
    );
    const clear = node("button", "清除板块筛选", "text-button");
    clear.type = "button";
    clear.addEventListener("click", () => {
      state.focusAutoBoard = null;
      state.focusAutoOffset = 0;
      if (state.selected) void loadFocusAuto(state.selected, 0);
    });
    boardFilter.append(clear);
  } else {
    boardFilter.textContent =
      "当前未按板块筛选；可从观察对象的行业/GN标签进入板内名单。";
  }
  void loadFocusMarketContext(context);
  if (!context?.previous_session || !context?.source_package_id) {
    message("focusAutoStatus", "相邻同源会话不可用；自动观察范围 UNKNOWN。");
    renderFocusS2T1Cohort({
      status: "UNKNOWN",
      reason_codes: ["ADJACENT_SOURCE_CONTEXT_UNAVAILABLE"],
    });
    return;
  }
  message(
    "focusAutoStatus",
    "正在读取已保存 D0 与来源级 Seed、D1、九规则观察…",
  );
  try {
    const result = await api("/api/preview/source-launch-watch", {
      before_session: context.previous_session,
      target_session: context.target_session,
      research_cutoff: context.research_cutoff,
      source_package_id: context.source_package_id,
      industry_snapshot_id: context.industry_snapshot_id,
      concept_snapshot_id: context.concept_snapshot_id,
      ...(Number.isInteger(context.d1_history_session_count) &&
      context.d1_history_session_count >= 1 &&
      context.d1_history_session_count <= 75
        ? { d1_history_session_count: context.d1_history_session_count }
        : {}),
      watch_group: state.focusAutoGroup,
      ...(state.focusAutoBoard
        ? {
            board_kind: state.focusAutoBoard.board_kind,
            board_native_id: state.focusAutoBoard.board_native_id,
          }
        : {}),
      offset,
      limit: 15,
    });
    if (
      token !== state.focusAutoRequest ||
      state.selected !== context ||
      activePage !== "focus"
    )
      return;
    renderFocusS2T1Cohort(result.s2_t1_cohort);
    const scope = result.stored_evidence_status || {};
    if (
      ["d0", "seed", "d1", "rules"].every(
        (key) => scope[key] === "NOT_COMPUTED",
      )
    ) {
      message(
        "focusAutoStatus",
        "NO_STORED_FACT / NOT_COMPUTED：该日期的精确来源研究范围尚未保存，不能解释为零变化。",
      );
      root.textContent = "尚无该范围的已存观察事实。";
      return;
    }
    const results = Array.isArray(result.results) ? result.results : [];
    for (const [index, item] of results.entries()) {
      const details = [
        String(item.watch_group || state.focusAutoGroup),
        item.observation_reason || "已保存的来源研究事实",
        `D0 ${item.d0_change?.transition || item.transition || item.seed_t1_outcome?.d0_transition_status || "UNKNOWN"}`,
        `T0 ${item.t0_observation?.status || "UNKNOWN"} · NON_PIT`,
        seedT1SummaryText(item.seed_t1_outcome),
        t0EarlySummary(item.t0_observation),
      ];
      const formalAdmissionHeadline = formalD0AdmissionHeadline(
        item.t0_observation?.formal_d0_admission,
      );
      root.append(
        rowButton(
          displayStockName(item) === "名称未知"
            ? displayStockCode(item)
            : displayStockName(item),
          `第 ${item.research_reading_order?.rank || item.observation_order?.rank || offset + index + 1} 位 · ${details.join(" · ")}`,
          `T0 ${item.t0_observation?.status || "UNKNOWN"}`,
          `${item.t0_observation?.followup_status || "NOT_COMPUTED"} · ${formalAdmissionHeadline}`,
          () => {
            state.focusAutoSelectedItem = item;
            $("cohortSymbol").value = displayStockCode(item);
            $("focusReadStock").click();
          },
          index,
        ),
        focusEvidenceDetails(item, context),
      );
    }
    const group = result.filter?.watch_group || state.focusAutoGroup;
    const total =
      result.pagination?.total_count ??
      result.watch_groups?.counts?.[group] ??
      results.length;
    const groups = result.watch_groups?.counts || {};
    const d0GroupCount =
      scope.d0 === "AVAILABLE"
        ? format(groups.D0_NEW)
        : `UNKNOWN（D0 ${scope.d0 || "UNKNOWN"}）`;
    const currentGroupCount =
      group === "D0_NEW" && scope.d0 !== "AVAILABLE"
        ? `UNKNOWN（D0 ${scope.d0 || "UNKNOWN"}）`
        : format(total);
    const pageMemberships = results.flatMap(
      (item) => item.also_in_sectors || [],
    );
    const availablePulseCount = pageMemberships.filter(
      (membership) => membership.pulse_context?.status === "AVAILABLE",
    ).length;
    const groupLabels = {
      D0_NEW: "D0新增",
      SEED_RULE: "Seed或规则TRUE",
      D1_MATERIAL: "D1材料",
      UNKNOWN: "UNKNOWN待核对",
      KNOWN_NO_SIGNAL: "已计算无信号",
    };
    message(
      "focusAutoStatus",
      context.target_session +
        " · 目标Raw来源池 " +
        format(result.watch_groups?.target_source_pool_denominator) +
        " · D0新增 " +
        d0GroupCount +
        " / Seed或规则TRUE " +
        format(groups.SEED_RULE) +
        " / D1材料 " +
        format(groups.D1_MATERIAL) +
        " / UNKNOWN待核对 " +
        format(groups.UNKNOWN) +
        " / 已计算无信号 " +
        format(groups.KNOWN_NO_SIGNAL) +
        " · 当前分组 " +
        `${groupLabels[group] || group} ${currentGroupCount}` +
        ` · 分母闭合 ${result.watch_groups?.partition_closed ? "是" : "UNKNOWN"}` +
        ` · 本页已存PULSE ${availablePulseCount}/${pageMemberships.length}` +
        " · 来源研究 NON_PIT",
    );
    message(
      "focusAutoPage",
      `偏移 ${result.pagination?.offset ?? offset} · 本页 ${results.length} · 总数 ${currentGroupCount}`,
    );
    $("focusAutoPrev").disabled = offset === 0;
    $("focusAutoNext").disabled = result.pagination?.next_offset == null;
    if (!results.length)
      root.textContent =
        group === "D0_NEW" && scope.d0 !== "AVAILABLE"
          ? `NO_STORED_FACT / D0 ${scope.d0 || "UNKNOWN"}：没有已存完整D0相邻事实；总数未知，不解释为零变化。`
          : group === "KNOWN_NO_SIGNAL"
            ? "没有已计算无观察线索的代码；这与 UNKNOWN 分组分开。"
            : group === "UNKNOWN"
              ? "当前范围没有待核对代码；这不表示其它来源分组为零。"
              : state.focusAutoBoard
                ? "所选行业/GN在当前观察分组中没有已证成员；成员关系与观察资格保持分开。"
                : `当前日期的${groupLabels[group] || group}分组没有记录。`;
  } catch (error) {
    if (token === state.focusAutoRequest && state.selected === context) {
      renderFocusS2T1Cohort({
        status: "UNKNOWN",
        reason_codes: ["S2_T1_COHORT_READ_FAILED"],
      });
      message("focusAutoStatus", `自动来源观察不可用：${error.message}`);
    }
  }
}

function sourceObservationEventLabel(event) {
  return (
    {
      ENTRY: "新登记",
      FIRST_OBSERVED_LEFT_CENSORED: "左截断首观",
      DAILY_LANDMARK: "活动段每日观察点",
      REENTRY: "合法重入",
      CONTINUES: "持续观察",
      EXIT_PROVED: "已证退出",
      PAUSED_UNKNOWN: "未知暂停",
      LEFT_CENSORED: "未知后恢复匹配",
      NO_MATCH: "终止后未重入",
      UNKNOWN: "状态未知",
      REENTRY_NOT_LINKED: "重入父段未证",
      INCOMPLETE: "事实不完整",
      NOT_COMPUTED: "未计算",
    }[event] ||
    event ||
    "UNKNOWN"
  );
}

function sourceObservationStatusLabel(status) {
  return (
    {
      ACTIVE: "持续观察",
      PAUSED: "未知暂停",
      EXITED: "已证退出",
      EXITED_UNKNOWN: "退出后未知",
      UNKNOWN: "状态未知",
      INCOMPLETE: "事实不完整",
      NOT_COMPUTED: "未计算",
      NOT_DUE: "截止内无后继会话",
    }[status] ||
    status ||
    "UNKNOWN"
  );
}

function sourceObservationOutcomeLabel(status) {
  return (
    {
      CONTINUOUS_MATCH_THROUGH_HORIZON: "持续TRUE至期限",
      CONTINUOUS_THROUGH_CUTOFF: "截止前持续TRUE（期限未到）",
      FIRST_EXIT_PROVED: "首次已证退出",
      PAUSED_UNKNOWN: "期限内未知暂停",
      INCOMPARABLE_OR_INCOMPLETE: "不可比或事实不完整",
      NOT_COMPUTED: "未计算",
      NOT_DUE: "未到期",
    }[status] ||
    status ||
    "UNKNOWN"
  );
}

function renderSourceObservationOutcomeSummary(summary) {
  const root = $("focusSegmentOutcomeSummary");
  root.replaceChildren();
  if (!summary) {
    message("focusSegmentOutcomeStatus", "观察期限结局 UNKNOWN。");
    return;
  }
  const denominator = summary.denominator || {};
  const riskSet = denominator.enrollment_kind_counts || {};
  const excluded = ["FIRST_OBSERVED_LEFT_CENSORED", "DAILY_LANDMARK"]
    .map((kind) => `${kind} ${riskSet[kind]?.enrollment_count ?? 0}段`)
    .join(" · ");
  const dueLabel = summary.horizon_session || "官方期限日期未提供";
  const statusLabel =
    summary.status === "NO_OBSERVED_SAMPLE"
      ? "NO_OBSERVED_SAMPLE"
      : summary.aggregate_status || summary.status;
  message(
    "focusSegmentOutcomeStatus",
    `${summary.t0_session} · T+${summary.horizon}目标 ${dueLabel} · 研究截止 ${summary.research_cutoff} · ${statusLabel} · RETROSPECTIVE_OUTCOME_STRATIFICATION_NON_PIT`,
  );
  root.append(
    node(
      "p",
      `完整T0登记 ${denominator.full_t0_enrollment_count ?? 0} 段 / ${denominator.full_t0_independent_source_symbol_count ?? 0} 个来源代码；ENTRY/REENTRY风险集 ${denominator.primary_entry_reentry_risk_set_count ?? 0} 段 / ${denominator.primary_entry_reentry_source_symbol_count ?? 0} 个来源代码。另列且不混作首次入组：${excluded}。每个结局组保留独立T0日期支持和Forward可评价数。`,
    ),
  );
  const groups = Array.isArray(summary.groups) ? summary.groups : [];
  if (!groups.length) {
    root.append(
      node(
        "p",
        summary.status === "NO_OBSERVED_SAMPLE"
          ? "所选T0没有已登记ENTRY/REENTRY观察段；这不代表来源池为零。"
          : "当前T0没有可显示的观察结局分组。",
      ),
    );
    return;
  }
  const laneKeys = [
    ...new Set(groups.map((group) => `${group.rule_id}\t${group.lane}`)),
  ];
  for (const laneKey of laneKeys) {
    const [rule, lane] = laneKey.split("\t");
    const laneGroups = groups.filter(
      (group) => group.rule_id === rule && group.lane === lane,
    );
    const first = laneGroups[0];
    const counts = laneGroups
      .map(
        (group) =>
          `${sourceObservationOutcomeLabel(group.outcome_status)} ${group.enrollment_count ?? 0}`,
      )
      .join(" · ");
    root.append(
      node(
        "p",
        `${rule} · ${lane} · 完整风险集 ${first?.risk_set_enrollment_count ?? 0}段 / ${first?.risk_set_independent_source_symbol_count ?? 0}个代码 · ${counts}`,
        "focus-segment-outcome-lane",
      ),
    );
    const table = node("table", undefined, "focus-segment-outcome-table");
    const head = node("thead");
    const header = node("tr");
    for (const title of [
      "期限结局",
      "登记段 / 去重代码 / 闭合段",
      "Forward端点",
      "RAW_CLOSE_RETURN",
      "RAW_MFE",
      "RAW_MAE",
      "RAW_MDD_CLOSE",
      "登记T0日期支持",
    ])
      header.append(node("th", title));
    head.append(header);
    table.append(head);
    const body = node("tbody");
    for (const group of laneGroups.filter(
      (item) => item.enrollment_count > 0,
    )) {
      const row = node("tr");
      row.append(
        node("td", sourceObservationOutcomeLabel(group.outcome_status)),
        node(
          "td",
          `${group.enrollment_count} / ${group.independent_source_symbol_count} / ${group.evidence_closed_count}`,
        ),
        node(
          "td",
          `OBSERVED ${group.forward_status_counts?.OBSERVED ?? 0} · UNKNOWN ${group.forward_status_counts?.UNKNOWN ?? 0} · 未算 ${group.forward_status_counts?.NOT_COMPUTED ?? 0} · 未到期 ${group.forward_status_counts?.NOT_DUE ?? 0}${group.pre_due_status_counts ? ` · 截止前退出 ${group.pre_due_status_counts.FIRST_EXIT_PROVED ?? 0} / 未知 ${group.pre_due_status_counts.PAUSED_UNKNOWN ?? 0} / 持续 ${group.pre_due_status_counts.CONTINUOUS_THROUGH_CUTOFF ?? 0}` : ""}`,
        ),
      );
      for (const metric of [
        "RAW_CLOSE_RETURN",
        "RAW_MFE",
        "RAW_MAE",
        "RAW_MDD_CLOSE",
      ]) {
        const value = group.metrics?.[metric] || {};
        row.append(
          node(
            "td",
            `n=${value.evaluated_count ?? 0} / ${value.independent_t0_date_count ?? 0}日 · p50 ${value.p50_nearest_rank ?? "—"} · ${value.minimum ?? "—"}…${value.maximum ?? "—"}`,
          ),
        );
      }
      row.append(
        node(
          "td",
          `${group.independent_t0_date_count ?? 0} · 最近秩分位 / 后验描述`,
        ),
      );
      body.append(row);
      const reasons = [
        ...Object.keys(group.observation_reason_counts || {}),
        ...Object.keys(group.forward_reason_counts || {}),
      ];
      if (reasons.length) {
        const reasonRow = node("tr");
        const reasonCell = node(
          "td",
          `质量原因：${[...new Set(reasons)].join("、")}`,
        );
        reasonCell.colSpan = 8;
        reasonRow.append(reasonCell);
        body.append(reasonRow);
      }
    }
    table.append(body);
    root.append(table);
  }
}

function renderSourceObservationSegment(item, context) {
  const card = node("article", undefined, "focus-observation-segment");
  const code = item.source_symbol || "UNKNOWN_SYMBOL";
  const history = Array.isArray(item.history) ? item.history : [];
  const eventMode = Boolean(item.daily_event_code);
  const shownEvent = eventMode ? item.daily_event_code : item.latest_event;
  card.append(
    node("h3", `${code} · ${item.rule_id} · 登记 #${item.enrollment_id}`),
    node(
      "p",
      `${item.t0_session} · ${item.enrollment_kind} · ${sourceObservationStatusLabel(item.status)} · ${shownEvent ? sourceObservationEventLabel(shownEvent) : "当前状态未计算"}`,
    ),
    node(
      "p",
      `来源包 ${item.source_package_id} / T0捕获 ${item.anchor_capture_id} / T0事实 #${item.anchor_rule_fact_id} · ${item.rule_version} · Profile ${item.profile_revision || "UNKNOWN"} · NON_PIT_EXPLORATORY / RAW_UNADJUSTED / SOURCE_SYMBOL_ONLY`,
      "muted",
    ),
    node(
      "p",
      eventMode
        ? `${item.daily_event_session} · 当日${sourceObservationEventLabel(item.daily_event_code)} · 三态 ${item.daily_event_state || "UNKNOWN"} · 生命周期 ${item.daily_event_lifecycle || "NOT_COMPUTED"} · 质量 ${item.daily_event_quality_status} · 捕获 ${item.daily_event_capture_id ?? "UNKNOWN"} · 事实 #${item.daily_event_evidence_fact_id ?? "UNKNOWN"} · ${item.daily_event_reason || "无原因码"}`
        : `${item.latest_observation_session || "无已存观察日"} · 三态 ${item.latest_state || "UNKNOWN"} · 生命周期 ${item.latest_lifecycle || "NOT_COMPUTED"} · 捕获 ${item.latest_capture_id ?? "UNKNOWN"} · 事实 #${item.latest_evidence_fact_id ?? "UNKNOWN"} · ${item.latest_reason || "无原因码"}`,
    ),
  );
  if (item.parent_enrollment_id) {
    card.append(
      node(
        "p",
        `直接父观察段 #${item.parent_enrollment_id} · ${item.enrollment_reason || "REENTRY"}`,
      ),
    );
  } else {
    card.append(node("p", `登记原因：${item.enrollment_reason || "UNKNOWN"}`));
  }
  if (item.horizon_outcome) {
    const outcome = item.horizon_outcome;
    const forward = outcome.forward || {};
    card.append(
      node(
        "p",
        `T+${outcome.horizon}后验观察结局：${sourceObservationOutcomeLabel(outcome.status)} · 目标 ${outcome.horizon_session || "UNKNOWN"} · 截止 ${context.research_cutoff} · ${outcome.reason_code || "无质量原因"}`,
      ),
    );
    if (outcome.status === "NOT_DUE" && outcome.pre_due_status !== "NOT_DUE") {
      card.append(
        node(
          "p",
          `期限尚未成熟；截至 ${outcome.pre_due_observed_through || context.research_cutoff} 的已存状态：${sourceObservationOutcomeLabel(outcome.pre_due_status)} · ${outcome.pre_due_reason_code || "无质量原因"}`,
        ),
      );
    }
    const firstExit = outcome.first_exit || outcome.pre_due_first_exit;
    if (firstExit) {
      card.append(
        node(
          "p",
          `首次已证退出 ${firstExit.session} · 观察事实 #${firstExit.observation_fact_id ?? "UNKNOWN"} / 规则证据 #${firstExit.evidence_rule_fact_id ?? "UNKNOWN"} · 捕获 ${firstExit.capture_id ?? "UNKNOWN"} · ${firstExit.reason_code || "无原因码"}`,
        ),
      );
    }
    const unknownSessions =
      outcome.unknown_sessions?.length > 0
        ? outcome.unknown_sessions
        : outcome.pre_due_unknown_sessions || [];
    if (unknownSessions.length) {
      card.append(
        node(
          "p",
          `期限内未知/左截断会话：${unknownSessions
            .map(
              (entry) => `${entry.session} · ${entry.reason_code || "UNKNOWN"}`,
            )
            .join("；")}`,
        ),
      );
    }
    card.append(
      node(
        "p",
        `原T0已存Forward：端点 ${forward.status || "UNKNOWN"} ${forward.endpoint_return || "—"} · MFE ${forward.mfe || "—"} · MAE ${forward.mae || "—"} · MDD_CLOSE ${forward.mdd_close || "—"} · 原因 ${(forward.reason_codes || []).join("、") || "无"} · RETROSPECTIVE_OUTCOME_STRATIFICATION_NON_PIT，不回写T0特征。`,
        "muted",
      ),
    );
  }
  if (Array.isArray(item.child_enrollments) && item.child_enrollments.length) {
    card.append(
      node(
        "p",
        `截至 ${context.target_session} 已证子观察段：${item.child_enrollments
          .map(
            (child) =>
              `#${child.enrollment_id} · ${child.t0_session} · 事实 #${child.rule_fact_id}`,
          )
          .join("；")}`,
      ),
    );
  }

  const timeline = node("details", undefined, "focus-observation-timeline");
  timeline.append(
    node("summary", `逐官方会话历史 · ${history.length} 条已存事实`),
  );
  if (!history.length) {
    timeline.append(
      node(
        "p",
        `${sourceObservationStatusLabel(item.status)} · 截止 ${context.target_session} 前没有已保存的逐会话事实。`,
      ),
    );
  } else {
    const rows = node("ol");
    for (const row of history) {
      const parentText = row.linked_enrollment_id
        ? ` · 新段 #${row.linked_enrollment_id}`
        : "";
      rows.append(
        node(
          "li",
          `${row.session} · ${sourceObservationEventLabel(row.event)} / ${row.lifecycle} · ${row.state || "无已存三态"} · ${row.status} / ${row.comparability_status} · 捕获 ${row.source_capture_id ?? "UNKNOWN"} · 规则事实 #${row.fact_id ?? "UNKNOWN"}${parentText} · ${row.reason}`,
        ),
      );
    }
    timeline.append(rows);
  }
  card.append(timeline);
  if (Array.isArray(item.parent_history) && item.parent_enrollment_id) {
    const parentTimeline = node(
      "details",
      undefined,
      "focus-observation-timeline",
    );
    parentTimeline.append(
      node(
        "summary",
        `父观察段 #${item.parent_enrollment_id} 历史 · ${item.parent_history.length} 条事实`,
      ),
    );
    const rows = node("ol");
    for (const row of item.parent_history) {
      rows.append(
        node(
          "li",
          `${row.session} · ${sourceObservationEventLabel(row.event)} / ${row.lifecycle} · ${row.state || "无已存三态"} · 捕获 ${row.source_capture_id ?? "UNKNOWN"} · 事实 #${row.fact_id ?? "UNKNOWN"} · ${row.reason}`,
        ),
      );
    }
    parentTimeline.append(rows);
    card.append(parentTimeline);
  }

  const drilldown = node("details", undefined, "focus-observation-drilldown");
  drilldown.append(node("summary", "查看同一 T0 的 Cohort / Forward"));
  const readButton = node(
    "button",
    "读取已存 T0 Cohort 与 Forward",
    "text-button",
  );
  readButton.type = "button";
  const result = node("div", undefined, "focus-observation-drilldown-result");
  readButton.addEventListener("click", async () => {
    if (!item.profile_revision || !item.event_window_start) {
      result.replaceChildren(
        node("p", "T0冻结键不完整；Cohort / Forward 为 NOT_COMPUTED。"),
      );
      return;
    }
    readButton.disabled = true;
    result.replaceChildren(node("p", "正在按精确 T0 键读取已存事实…"));
    const key = {
      target_session: item.t0_session,
      source_package_id: item.source_package_id,
      profile_revision: item.profile_revision,
      history: item.history_session_count,
      d1_history: item.d1_history_session_count,
      window: item.event_window_start,
      rule: item.rule_id,
      symbol: code,
    };
    try {
      const cohort = await api(
        `/api/product/cohort?${new URLSearchParams({ ...key, horizon: 5, offset: 0 })}`,
      );
      if (!card.isConnected || state.selected !== context) return;
      const detail = node(
        "div",
        undefined,
        "focus-observation-drilldown-result",
      );
      const cohortStatus =
        Array.isArray(cohort.groups) && cohort.groups.length
          ? "T0分组可用"
          : "T0分组未知";
      detail.append(
        node(
          "p",
          `Cohort ${cohortStatus} · ${cohort.target_session || item.t0_session} · T0分组 ${cohort.groups?.length ?? 0} 项 · 来源规则全集 ${cohort.source_pool_count ?? "UNKNOWN"} 条事实 · 同一查询期限 T+5 ${cohort.due ? "已到期" : "NOT_DUE"} · ${cohort.scope || "NON_PIT_EXPLORATORY"} · RETROSPECTIVE_DESCRIPTIVE_NON_PIT`,
        ),
      );
      const exactFact = (cohort.selected_rules || []).find(
        (fact) =>
          fact.rule_id === item.rule_id &&
          String(fact.fact_id) === String(item.anchor_rule_fact_id) &&
          String(fact.enrollment?.id) === String(item.enrollment_id) &&
          fact.enrollment?.kind === item.enrollment_kind &&
          (fact.enrollment?.parent_enrollment_id ?? null) ===
            item.parent_enrollment_id,
      );
      if (exactFact) {
        detail.append(
          node(
            "p",
            `原始登记 T0 ${exactFact.rule_id} ${exactFact.state || "UNKNOWN"} · 规则事实 #${exactFact.fact_id} · 捕获 ${item.anchor_capture_id} · ${exactFact.enrollment.kind} 登记 #${exactFact.enrollment.id} · 父段 ${exactFact.enrollment.parent_enrollment_id ?? "无"}`,
          ),
        );
      } else {
        detail.append(
          node(
            "p",
            `Cohort未返回与原始登记事实 #${item.anchor_rule_fact_id}、登记 #${item.enrollment_id} 完全一致的记录；该登记的Cohort状态为 UNKNOWN。`,
          ),
        );
      }
      const forwardPaths = Array.isArray(item.forward_paths)
        ? item.forward_paths
        : [];
      detail.append(
        node(
          "p",
          `原登记 #${item.enrollment_id} 的 T0→Forward · 截至 ${item.forward_as_of || context.research_cutoff} · ${item.forward_classification || "RETROSPECTIVE_DESCRIPTIVE_NON_PIT"} · RAW_UNADJUSTED`,
        ),
      );
      const paths = node("ul");
      for (const path of forwardPaths) {
        paths.append(
          node(
            "li",
            `T+${path.horizon} · ${path.due_session || "官方到期日未知"} · ${path.status || "UNKNOWN"} · 端点 ${path.endpoint_quality || "UNKNOWN"} · 收盘回撤 ${path.mdd_close ?? path.path_metrics_status ?? "UNKNOWN"} · MFE ${path.mfe ?? path.path_metrics_status ?? "UNKNOWN"} · MAE ${path.mae ?? path.path_metrics_status ?? "UNKNOWN"} · 路径 ${path.path_quality || "UNKNOWN"} · ${(path.reason_codes || []).join("、")}`,
          ),
        );
      }
      detail.append(paths);
      const fepSection = node(
        "section",
        undefined,
        "focus-observation-drilldown-result",
      );
      fepSection.append(
        node(
          "p",
          `FEP 已存队列为描述性分组，不属于单条登记预测；目标 T0 ${item.t0_session} · RETROSPECTIVE_DESCRIPTIVE_NON_PIT。`,
        ),
      );
      const fepLane =
        item.enrollment_kind === "ENTRY"
          ? "ENTRY"
          : item.enrollment_kind === "DAILY_LANDMARK"
            ? "DAILY_LANDMARK"
            : item.enrollment_kind === "REENTRY"
              ? "REENTRY"
              : null;
      if (!fepLane) {
        fepSection.append(
          node(
            "p",
            `${item.enrollment_kind} 没有独立的现役 FEP lane；不将 ENTRY 或 DAILY_LANDMARK 分母套到此登记。`,
          ),
        );
      } else {
        try {
          const fepResults = await Promise.all(
            [1, 3, 5].map(async (horizon) => {
              const query = new URLSearchParams({
                source_package_id: String(item.source_package_id),
                rule: item.rule_id,
                horizon: String(horizon),
                lane: fepLane,
              });
              return {
                horizon,
                result: await api(`/api/product/fep-baseline?${query}`),
              };
            }),
          );
          if (!card.isConnected || state.selected !== context) return;
          for (const entry of fepResults) {
            const groups = (entry.result.rows || []).filter(
              (fact) =>
                fact.target_session === item.t0_session &&
                fact.rule_id === item.rule_id &&
                String(fact.source_capture_id) ===
                  String(item.anchor_capture_id) &&
                fact.profile_revision === item.profile_revision &&
                fact.history_session_count === item.history_session_count &&
                fact.d1_history_session_count ===
                  item.d1_history_session_count &&
                fact.event_window_start === item.event_window_start &&
                fact.calendar_revision === item.calendar_revision &&
                fact.summary_scope === "T0" &&
                fact.metric_code === "RAW_CLOSE_RETURN" &&
                fact.condition_code === "ALL" &&
                ["MATCH", "NO_MATCH", "UNKNOWN"].includes(fact.group_code),
            );
            fepSection.append(
              node(
                "p",
                `T+${entry.horizon} · lane ${fepLane} · ${entry.result.status || "UNKNOWN"} · 同日 T0 描述性分组 ${groups.length} 项 · ${entry.result.capability || "DESCRIPTIVE_ONLY"}`,
              ),
            );
            for (const fact of groups) {
              const groupLabel =
                {
                  MATCH: "T0匹配",
                  NO_MATCH: "T0未匹配",
                  UNKNOWN: "T0未知",
                }[fact.group_code] || fact.group_code;
              fepSection.append(
                node(
                  "p",
                  `${groupLabel} ${fact.status} · 成员 ${fact.member_count ?? "UNKNOWN"} · 到期 ${fact.due_count ?? "UNKNOWN"} · 已观察 ${fact.observed_count ?? "UNKNOWN"} · 未知 ${fact.unknown_count ?? "UNKNOWN"} · 未计算 ${fact.not_computed_count ?? "UNKNOWN"} · 未到期 ${fact.not_due_count ?? "UNKNOWN"} · 中位数 ${fact.p50 ?? "UNKNOWN"}`,
                ),
              );
            }
            if (fepLane === "REENTRY") {
              const comparison = entry.result.same_t0_comparison;
              const returnPair = comparison?.metrics?.find(
                (metric) => metric.metric_code === "RAW_CLOSE_RETURN",
              );
              const datePair = returnPair?.dates?.find(
                (pair) => pair.target_session === item.t0_session,
              );
              fepSection.append(
                node(
                  "p",
                  `同 T0 ENTRY 对照 · ${datePair?.status || "NO_COMPARABLE_PAIR"} · REENTRY−ENTRY p50 ${datePair?.reentry_minus_entry_p50 ?? "未计算"} · 同规则、来源捕获、画像、日历、RAW_UNADJUSTED 与质量政策 · 纯描述性，不表示因果或策略优势。`,
                ),
              );
              const pathMetrics = (entry.result.rows || []).filter(
                (fact) =>
                  fact.target_session === item.t0_session &&
                  fact.rule_id === item.rule_id &&
                  String(fact.source_capture_id) ===
                    String(item.anchor_capture_id) &&
                  fact.profile_revision === item.profile_revision &&
                  fact.history_session_count === item.history_session_count &&
                  fact.d1_history_session_count ===
                    item.d1_history_session_count &&
                  fact.event_window_start === item.event_window_start &&
                  fact.calendar_revision === item.calendar_revision &&
                  fact.summary_scope === "T0" &&
                  fact.group_code === "MATCH" &&
                  fact.condition_code === "ALL" &&
                  ["RAW_MFE", "RAW_MAE", "RAW_MDD_CLOSE"].includes(
                    fact.metric_code,
                  ),
              );
              for (const fact of pathMetrics) {
                fepSection.append(
                  node(
                    "p",
                    `${fact.metric_code} · ${fact.status} · 可评价 ${fact.observed_count ?? "UNKNOWN"}/${fact.member_count ?? "UNKNOWN"} · UNKNOWN ${fact.unknown_count ?? "UNKNOWN"} · 未计算 ${fact.not_computed_count ?? "UNKNOWN"} · 未到期 ${fact.not_due_count ?? "UNKNOWN"} · 最近秩 p25/p50/p75 ${fact.p25 ?? "未计算"}/${fact.p50 ?? "未计算"}/${fact.p75 ?? "未计算"}`,
                  ),
                );
              }
            }
          }
        } catch (error) {
          if (card.isConnected && state.selected === context) {
            fepSection.append(
              node("p", `FEP描述统计读取为 UNKNOWN：${error.message}`),
            );
          }
        }
      }
      detail.append(fepSection);
      result.replaceChildren(detail);
    } catch (error) {
      if (card.isConnected && state.selected === context)
        result.replaceChildren(
          node("p", `T0 Cohort / Forward 读取失败：${error.message}`),
        );
    } finally {
      if (card.isConnected) readButton.disabled = false;
    }
  });
  drilldown.append(readButton, result);
  card.append(drilldown);
  const openStockButton = node(
    "button",
    "在个股研究打开来源代码",
    "text-button",
  );
  openStockButton.type = "button";
  openStockButton.addEventListener("click", () => {
    $("stockSymbol").value = code.split(".").pop();
    gotoPage("stocks");
    void openStock(code.split(".").pop());
  });
  card.append(openStockButton);
  return card;
}

async function loadFocusObservationSegments(context, offset = 0) {
  const token = ++state.focusSegmentRequest;
  state.focusSegmentOffset = offset;
  const root = $("focusSegmentRows");
  const eventRoot = $("focusSegmentEventRows");
  $("focusSegmentOutcomeSummary").replaceChildren();
  message("focusSegmentOutcomeStatus", "正在读取所选T0的后验期限结局…");
  root.replaceChildren();
  eventRoot.replaceChildren();
  $("focusSegmentEventCounts").replaceChildren();
  $("focusSegmentPrev").disabled = $("focusSegmentNext").disabled = true;
  $("focusSegmentEventPrev").disabled = $("focusSegmentEventNext").disabled =
    true;
  message("focusSegmentPage", "—");
  message("focusSegmentEventPage", "—");
  if (!context?.contextReady || !context?.source_package_id) {
    message(
      "focusSegmentOutcomeStatus",
      "T0日期或来源包不可用；观察结局 UNKNOWN。",
    );
    message(
      "focusSegmentStatus",
      "研究日期或来源包不可用；观察段状态 UNKNOWN。",
    );
    message(
      "focusSegmentEventStatus",
      "事件日期或来源包不可用；事件状态 UNKNOWN。",
    );
    return;
  }
  message("focusSegmentStatus", "正在读取已完成的PG来源观察段…");
  const requestedRule = state.focusSegmentRule;
  const status = $("focusSegmentStatusFilter").value;
  const selectedEvent = $("focusSegmentEventFilter").value;
  const eventOffset = state.focusSegmentEventOffset;
  const query = {
    as_of: context.target_session,
    research_cutoff: context.research_cutoff,
    source_package_id: context.source_package_id,
    rule: requestedRule || "ALL",
    status,
    event: selectedEvent,
    offset,
    limit: 20,
    event_offset: eventOffset,
    event_limit: 20,
    outcome_horizon: $("focusSegmentOutcomeHorizon").value,
  };
  try {
    const result = await api(
      `/api/product/source-observation-segments?${new URLSearchParams(query)}`,
    );
    if (
      token !== state.focusSegmentRequest ||
      state.selected !== context ||
      activePage !== "focus"
    )
      return;
    renderSourceObservationOutcomeSummary(result.observation_outcomes);
    const options = result.available_rules || [];
    const selectedRule =
      requestedRule === "ALL" ||
      options.some((item) => item.rule_id === requestedRule)
        ? requestedRule || "ALL"
        : options.some((item) => item.rule_id === "D1_T0")
          ? "D1_T0"
          : options[0]?.rule_id || "ALL";
    const select = $("focusSegmentRule");
    select.replaceChildren(new Option("全部已有规则", "ALL"));
    for (const item of options) {
      select.add(
        new Option(
          `${item.rule_id} · ${item.enrollment_count} 段`,
          item.rule_id,
        ),
      );
    }
    select.disabled = !options.length;
    select.value = selectedRule;
    if (selectedRule !== requestedRule) {
      state.focusSegmentRule = selectedRule;
      void loadFocusObservationSegments(context, 0);
      return;
    }
    state.focusSegmentRule = selectedRule;
    const counts = result.status_counts || {};
    const summary = Object.entries(counts)
      .map(
        ([name, value]) =>
          `${sourceObservationStatusLabel(name)} ${value.enrollment_count}`,
      )
      .join(" · ");
    const denominator = result.denominator || {};
    const nextSessionStatus =
      result.next_official_session_status === "NOT_DUE"
        ? " · 截止后的官方后继 NOT_DUE（行情未读取）"
        : "";
    message(
      "focusSegmentStatus",
      `${context.target_session} · 来源包 ${context.source_package_id} · ${result.rule_id || selectedRule} · 已登记观察段 ${denominator.enrollment_count ?? 0} / 独立来源代码 ${denominator.independent_source_symbol_count ?? 0} · ${summary || "没有已存状态分布"} · ${result.identity_status || "SOURCE_SYMBOL_ONLY"} / ${result.scope || "NON_PIT_EXPLORATORY"} / ${result.price_coordinate || "RAW_UNADJUSTED"}${nextSessionStatus}`,
    );
    const items = Array.isArray(result.results) ? result.results : [];
    for (const item of items)
      root.append(renderSourceObservationSegment(item, context));
    const daily = result.daily_events || {};
    const dailyDenominator = daily.denominator || {};
    const statusFilterLabel =
      status === "ALL" ? "全部当前状态" : sourceObservationStatusLabel(status);
    const eventFilterLabel =
      selectedEvent === "ALL"
        ? "全部当日事件"
        : sourceObservationEventLabel(selectedEvent);
    const dailyStatusLabel =
      {
        AVAILABLE: "日事件数据可用",
        INCOMPLETE: "日事件事实不完整",
        NOT_COMPUTED: "当日来源规则事实未计算",
      }[daily.status] ||
      daily.status ||
      "UNKNOWN";
    const incompleteLabel =
      daily.status === "AVAILABLE"
        ? "已存来源段范围内可报告真实0值"
        : "缺失或不完整事实不能解释为0变化";
    message(
      "focusSegmentEventStatus",
      `${context.target_session} · ${dailyStatusLabel} · 列表筛选：${statusFilterLabel} / ${eventFilterLabel} · 当日已存观察事实 ${dailyDenominator.observed_segment_count ?? 0} 段 / ${dailyDenominator.independent_source_symbol_count ?? 0} 个独立来源代码 · 应观察 ${dailyDenominator.due_segment_count ?? 0} 段（已存 ${dailyDenominator.due_observed_segment_count ?? 0}，缺失 ${dailyDenominator.missing_due_segment_count ?? 0}） · 输入规则事实 ${dailyDenominator.input_rule_fact_count ?? 0} 条 / ${dailyDenominator.input_rule_source_symbol_count ?? 0} 个代码 · 当前风险集 ${dailyDenominator.current_risk_set_segment_count ?? 0} 段 / ${dailyDenominator.current_risk_set_source_symbol_count ?? 0} 个代码 · ${incompleteLabel}`,
    );
    const eventCounts = daily.event_counts || {};
    const countNodes = Object.entries(eventCounts).map(([event, value]) =>
      node(
        "span",
        `${sourceObservationEventLabel(event)} ${value.segment_count ?? 0} 段 / ${value.source_symbol_count ?? 0} 个来源代码`,
      ),
    );
    const qualityCounts = daily.quality_counts || {};
    for (const quality of ["UNKNOWN", "INCOMPLETE", "NOT_COMPUTED"]) {
      const count = qualityCounts[quality] ?? 0;
      if (count)
        countNodes.push(
          node("span", `${sourceObservationStatusLabel(quality)} ${count} 段`),
        );
    }
    $("focusSegmentEventCounts").replaceChildren(...countNodes);
    const eventItems = Array.isArray(daily.results) ? daily.results : [];
    for (const item of eventItems)
      eventRoot.append(renderSourceObservationSegment(item, context));
    const eventPage = daily.pagination || {};
    message(
      "focusSegmentEventPage",
      `偏移 ${eventPage.offset ?? eventOffset} · 本页 ${eventItems.length} · 总事件/数据状态 ${eventPage.total_count ?? 0}`,
    );
    $("focusSegmentEventPrev").disabled = eventOffset === 0;
    $("focusSegmentEventNext").disabled = eventPage.next_offset == null;
    if (!eventItems.length) {
      eventRoot.textContent =
        daily.status === "AVAILABLE"
          ? selectedEvent === "ALL"
            ? "该日期在已登记来源段范围内没有应观察的状态事实。"
            : `所选事件在已存来源段范围内为 0 条（${sourceObservationEventLabel(selectedEvent)}）。`
          : `当日事实状态为 ${dailyStatusLabel}；当前没有可列出的事件记录，不能据此视为零变化。`;
    }
    const page = result.pagination || {};
    message(
      "focusSegmentPage",
      `偏移 ${page.offset ?? offset} · 本页 ${items.length} · 总段数 ${page.total_count ?? 0}`,
    );
    $("focusSegmentPrev").disabled = offset === 0;
    $("focusSegmentNext").disabled = page.next_offset == null;
    if (!items.length) {
      root.textContent =
        result.status === "NO_OBSERVED_SAMPLE"
          ? "NO_OBSERVED_SAMPLE：所选日期、规则与来源包没有已登记观察段；不代表来源池为零。"
          : `当前状态没有记录（${status}）；其它状态保持独立。`;
    }
  } catch (error) {
    if (token === state.focusSegmentRequest && state.selected === context) {
      message(
        "focusSegmentOutcomeStatus",
        `后验观察期限只读失败：${error.message}`,
      );
      $("focusSegmentOutcomeSummary").replaceChildren();
      message("focusSegmentStatus", `来源观察段只读失败：${error.message}`);
      root.textContent = "读取失败；没有触发重算或写入。";
      message("focusSegmentEventStatus", `当日事件只读失败：${error.message}`);
      eventRoot.textContent = "事件读取失败；没有触发重算或写入。";
    }
  }
}

$("focusAutoPrev").addEventListener("click", () => {
  state.focusAutoOffset = Math.max(0, state.focusAutoOffset - 15);
  if (state.selected) void loadFocusAuto(state.selected, state.focusAutoOffset);
});
$("focusAutoNext").addEventListener("click", () => {
  state.focusAutoOffset += 15;
  if (state.selected) void loadFocusAuto(state.selected, state.focusAutoOffset);
});
$("focusAutoGroup").addEventListener("change", () => {
  state.focusAutoGroup = $("focusAutoGroup").value;
  state.focusAutoOffset = 0;
  if (state.selected) void loadFocusAuto(state.selected, 0);
});
$("focusSegmentRule").addEventListener("change", () => {
  state.focusSegmentRule = $("focusSegmentRule").value;
  state.focusSegmentOffset = 0;
  state.focusSegmentEventOffset = 0;
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});
$("focusSegmentStatusFilter").addEventListener("change", () => {
  state.focusSegmentOffset = 0;
  state.focusSegmentEventOffset = 0;
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});
$("focusSegmentRefresh").addEventListener("click", () => {
  state.focusSegmentOffset = 0;
  state.focusSegmentEventOffset = 0;
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});
$("focusSegmentOutcomeHorizon").addEventListener("change", () => {
  state.focusSegmentOffset = 0;
  state.focusSegmentEventOffset = 0;
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});
$("focusSegmentPrev").addEventListener("click", () => {
  state.focusSegmentOffset = Math.max(0, state.focusSegmentOffset - 20);
  if (state.selected)
    void loadFocusObservationSegments(state.selected, state.focusSegmentOffset);
});
$("focusSegmentNext").addEventListener("click", () => {
  state.focusSegmentOffset += 20;
  if (state.selected)
    void loadFocusObservationSegments(state.selected, state.focusSegmentOffset);
});
$("focusSegmentEventFilter").addEventListener("change", () => {
  state.focusSegmentEventOffset = 0;
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});
$("focusSegmentEventPrev").addEventListener("click", () => {
  state.focusSegmentEventOffset = Math.max(
    0,
    state.focusSegmentEventOffset - 20,
  );
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});
$("focusSegmentEventNext").addEventListener("click", () => {
  state.focusSegmentEventOffset += 20;
  if (state.selected) void loadFocusObservationSegments(state.selected, 0);
});

function applyD0BoardFilters() {
  state.d0BoardPage = 0;
  if (state.d0BoardView === "PULSE") void loadD0PulseDirectoryPage();
  else renderD0Boards();
}

$("viewD0Boards").addEventListener("click", () => setD0BoardView("D0"));
$("viewPulseBoards").addEventListener("click", () => setD0BoardView("PULSE"));
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
  if (state.d0BoardView === "PULSE") void loadD0PulseDirectoryPage();
  else renderD0Boards();
});
$("d0BoardNext").addEventListener("click", () => {
  state.d0BoardPage += 1;
  if (state.d0BoardView === "PULSE") void loadD0PulseDirectoryPage();
  else renderD0Boards();
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
  message("d0Status", "正在从PG读取本页D0新增与T0研判摘要…");
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
        watch_group: "D0_NEW",
        ...(Number.isInteger(c.d1_history_session_count) &&
        c.d1_history_session_count >= 1 &&
        c.d1_history_session_count <= 75
          ? { d1_history_session_count: c.d1_history_session_count }
          : {}),
        offset: state.d0Offset,
        limit: 30,
      },
      controller.signal,
    );
    if (token !== state.d0Request || state.selected !== c) return;
    state.d0Result = result;
    if (state.d0Offset === 0) {
      const candidateCount = result.observation_ordering?.candidate_count;
      const homeFeaturedCount =
        result.observation_ordering?.home_featured_count ??
        (Number.isInteger(candidateCount)
          ? Math.min(candidateCount, result.results?.length || 0)
          : null);
      const homeRows = Array.isArray(result.results) ? result.results : [];
      state.d0Featured =
        result.stored_evidence_status?.d0 === "AVAILABLE" &&
        Number.isInteger(candidateCount) &&
        Number.isInteger(homeFeaturedCount) &&
        result.pagination?.offset === 0 &&
        homeRows.length === Math.min(candidateCount, homeFeaturedCount)
          ? {
              target_session: c.target_session,
              source_package_id: c.source_package_id,
              candidate_count: candidateCount,
              home_featured_count: homeFeaturedCount,
              rows: new Map(homeRows.map((item) => [item.source_symbol, item])),
            }
          : null;
    }
    const d0ScopeStatus = result.stored_evidence_status?.d0 || "UNKNOWN";
    const d0Saved = d0ScopeStatus === "AVAILABLE";
    state.d0Status =
      d0Saved && Array.isArray(result.boards) ? "READY" : "UNKNOWN";
    state.d0Error = d0Saved
      ? null
      : `该目标日D0相邻事实 ${d0ScopeStatus}；不能解释为零变化。`;
    renderD0Boards();
    renderMarketD0Summary();
    if (state.d0Offset === 0) {
      const boards = result.boards
        .filter((item) => item.new_member_count > 0)
        .slice(0, 15);
      const uniqueStocks = result.results;
      fillList(
        "overviewBoards",
        boards,
        d0Saved
          ? "没有已证 D0 变化板块；成员缺失为 UNKNOWN。"
          : `NO_STORED_FACT / D0 ${d0ScopeStatus}：没有已存完整D0相邻事实，不解释为零变化。`,
        (item, index) =>
          rowButton(
            item.board_name,
            `第 ${item.observation_rank || index + 1} 位 · ${item.home_selection_reason || "按 D0 新增成员关系数排序"} · ${boardKindLabel(item.board_kind)} · ${boardSourceQualitySummary(item)} · ${boardPulseSummary(item)} · 观测 ${item.observed_at || "UNKNOWN"} · NON_PIT`,
            item.new_member_count,
            "D0 新增成员",
            () => openD0Board(item),
            index,
          ),
      );
      fillList(
        "overviewStocks",
        uniqueStocks,
        d0Saved
          ? "没有已证 D0 新增股票；UNKNOWN 不代表无变化。"
          : `NO_STORED_FACT / D0 ${d0ScopeStatus}：没有已存完整D0相邻事实，不解释为零变化。`,
        (item, index) =>
          rowButton(
            displayStockName(item),
            `${stockObservationSummary(item)} · ${item.home_selection_reason || ""}`,
            `第 ${item.research_reading_order?.rank || "?"} 位`,
            "T0研判阅读顺序",
            () => openStock(item.source_symbol, item),
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
      countCard(
        "D0 新增",
        d0Saved ? count : "UNKNOWN",
        d0Saved
          ? "独立来源代码 · 证据分层观察，不是正式候选"
          : `精确D0事实 ${d0ScopeStatus} · 未知不解释为零变化`,
      ),
    );
    const ordering = result.observation_ordering || {};
    const evidence = result.stored_evidence_status || {};
    message(
      "d0Status",
      d0Saved
        ? `${c.target_session} · D0 FALSE→TRUE ${format(count)} 只 · 首页 ${ordering.home_featured_count ?? Math.min(count || 0, 30)}/${ordering.candidate_count ?? count} · Seed ${evidence.seed || "UNKNOWN"} / D1 ${evidence.d1 || "UNKNOWN"} / 九规则 ${evidence.rules || "UNKNOWN"} · ${membershipPageScopeSummary(result.membership_status)} · 未进首页者按同一顺序分页读取；NON_PIT${missing.length ? ` · 部分成员关系 UNKNOWN：${missing.join("、")}` : ""}`
        : `${c.target_session} · D0 ${d0ScopeStatus}：没有已存完整相邻事实，计数 UNKNOWN；不会显示成零变化。`,
    );
    message(
      "d0Page",
      d0Saved
        ? `偏移 ${result.pagination.offset} · 总数 ${format(count)}`
        : `偏移 ${result.pagination.offset} · 总数 UNKNOWN（${d0ScopeStatus}）`,
    );
    $("d0Prev").disabled = !d0Saved || result.pagination.offset === 0;
    $("d0Next").disabled = !d0Saved || result.pagination.next_offset == null;
    fillList(
      "d0Stocks",
      result.results,
      "没有已证明的 D0 新增启动；未知状态不等于未入选。",
      (item, index) =>
        rowButton(
          displayStockName(item),
          `代码 ${displayStockCode(item)} · ${stockObservationSummary(item)} · ${item.home_selection_reason || "完整结果分页"}`,
          `第 ${item.research_reading_order?.rank || "?"} 位`,
          item.research_reading_order?.home_featured
            ? "首页优先观察"
            : "完整分页",
          () => openStock(item.source_symbol, item),
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
    `${boardKindLabel(board.board_kind)} · 成员快照观测 ${board.observed_at || "UNKNOWN"} · NON_PIT 回看 · ${board.pulse_followup_only ? "连续PULSE与到期T+1/T+3/T+5" : "D0新增"}`,
  );
  message(
    "boardCoverage",
    board.pulse_followup_only
      ? "D0 NOT_COMPUTED；读取当前快照成员与已存PULSE后继事实…"
      : "正在读取该板块 D0 新增成员…",
  );
  renderPulseHistory({
    status: "LOADING",
    reason_codes: ["正在读取已存板块会话事实"],
  });
  message("memberList", "正在读取…");
  $("memberPrev").disabled = $("memberNext").disabled = true;
  if (!$("boardDialog").open) $("boardDialog").showModal();
  try {
    let result;
    if (board.pulse_followup_only) {
      const comparison =
        state.d0PulseDirectory?.cohort_comparison?.by_kind?.[
          board.board_kind
        ] ??
        state.d0Result?.pulse_followup_directory?.comparison?.[
          board.board_kind
        ];
      result = {
        boards: [board],
        sector_pulse_followup_comparison: comparison
          ? { [board.board_kind]: comparison }
          : {},
        filter: { filtered_total: null, global_total: null },
      };
    } else {
      result = await api("/api/preview/source-launch-watch", {
        before_session: c.previous_session,
        target_session: c.target_session,
        research_cutoff: c.research_cutoff,
        source_package_id: c.source_package_id,
        industry_snapshot_id: c.industry_snapshot_id,
        concept_snapshot_id: c.concept_snapshot_id,
        ...(Number.isInteger(c.d1_history_session_count) &&
        c.d1_history_session_count >= 1 &&
        c.d1_history_session_count <= 75
          ? { d1_history_session_count: c.d1_history_session_count }
          : {}),
        board_kind: board.board_kind,
        board_native_id: board.board_native_id,
        offset: 0,
        limit: 1,
      });
    }
    if (
      token !== state.d0BoardRequest ||
      c !== state.selected ||
      state.d0Board !== board ||
      !$("boardDialog").open
    )
      return;
    const researchQuery = {
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      d1_history_session_count: Number.isInteger(c.d1_history_session_count)
        ? c.d1_history_session_count
        : c.stock_history_session_count || c.source_history_session_count || 1,
      industry_snapshot_id: c.industry_snapshot_id,
      concept_snapshot_id: c.concept_snapshot_id,
      ...(typeof c.profile_revision === "string"
        ? { profile_revision: c.profile_revision }
        : {}),
      board_kind: board.board_kind,
      board_native_id: board.board_native_id,
      offset,
      limit: 20,
    };
    const saved = await api(
      `/api/product/research-day?${new URLSearchParams(researchQuery)}`,
    );
    if (
      token !== state.d0BoardRequest ||
      c !== state.selected ||
      state.d0Board !== board ||
      !$("boardDialog").open
    )
      return;
    const observation = saved.selected_board_observation;
    const pulseBoard = result.boards?.find(
      (item) =>
        item.board_kind === board.board_kind &&
        item.board_native_id === board.board_native_id,
    );
    const pulse = pulseBoard?.cross_period_pulse ?? board.cross_period_pulse;
    let pulseTransition = pulseBoard?.cross_period_pulse_transition;
    let pulseFollowup = pulseBoard?.cross_period_pulse_followup;
    let pulseHistory = null;
    try {
      if (!pulse?.profile_revision && c.profileResolution)
        await c.profileResolution;
      const historyProfileRevision =
        pulse?.profile_revision || c.profile_revision;
      const historySnapshotId =
        pulse?.snapshot_id ||
        board.snapshot_id ||
        (board.board_kind === "INDUSTRY"
          ? c.industry_snapshot_id
          : c.concept_snapshot_id);
      if (
        !historyProfileRevision ||
        !Number.isInteger(Number(historySnapshotId)) ||
        Number(historySnapshotId) <= 0 ||
        !c.research_cutoff
      ) {
        pulseHistory = {
          status: "NOT_COMPUTED",
          reason_codes: ["PULSE_HISTORY_PROFILE_OR_SNAPSHOT_NOT_BOUND"],
        };
      } else {
        const historyQuery = new URLSearchParams({
          target_session: c.target_session,
          research_cutoff: c.research_cutoff,
          source_package_id: String(c.source_package_id),
          profile_revision: historyProfileRevision,
          snapshot_id: String(Number(historySnapshotId)),
          board_kind: board.board_kind,
          board_native_id: board.board_native_id,
        });
        pulseHistory = await api(
          `/api/product/sector-pulse-history?${historyQuery}`,
        );
      }
    } catch (error) {
      pulseHistory = { status: "READ_FAILED", error: error.message };
    }
    if (
      token !== state.d0BoardRequest ||
      c !== state.selected ||
      state.d0Board !== board ||
      !$("boardDialog").open
    )
      return;
    renderPulseHistory(pulseHistory);
    const currentPulseHistory = pulseHistory?.sessions?.find(
      (item) => item.target_session === c.target_session,
    );
    if (!pulseTransition && currentPulseHistory?.transition)
      pulseTransition = currentPulseHistory.transition;
    if (board.pulse_followup_only && currentPulseHistory) {
      pulseFollowup = {
        ...pulseFollowup,
        horizons: (currentPulseHistory.followups || []).map((item) => ({
          horizon_sessions: item.horizon,
          target_session: item.target_session,
          status: item.status,
          fixed_member_denominator: item.fixed_member_denominator,
          observed_member_count: item.observed_member_count,
          unknown_member_count: item.unknown_member_count,
          equal_weight_return: item.equal_weight_return,
          equal_weight_return_percent_4dp: item.return_percent_4dp,
          reason_codes: item.reason_codes || [],
        })),
        research_mode: pulse?.research_mode || "NON_PIT_RETROSPECTIVE",
        price_coordinate: pulse?.price_coordinate || "RAW_UNADJUSTED",
      };
    }
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
      ...(pulseTransition?.reason_codes ?? []),
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
        const unknown = item.unknown_member_count;
        const memberCoverage = `固定成员 ${denominator ?? "UNKNOWN"} · 有效 ${observed ?? "UNKNOWN"} · 未知 ${unknown ?? "UNKNOWN"}`;
        if (item.status === "OBSERVED") {
          const percent = item.equal_weight_return_percent_4dp;
          const signed = Number(percent) >= 0 ? `+${percent}` : percent;
          return `${label} 等权收盘 ${signed}% · ${memberCoverage}`;
        }
        if (item.status === "NOT_DUE")
          return `${label}尚未到期 · ${memberCoverage}`;
        if (item.status === "NOT_COMPUTED")
          return `${label}尚未生产 · ${memberCoverage}`;
        if (item.status === "UNKNOWN")
          return `${label}未知 · ${memberCoverage} · ${(item.reason_codes ?? []).join("、")}`;
        return `${label}${item.status ?? "INCOMPLETE"} · ${memberCoverage} · ${(item.reason_codes ?? []).join("、")}`;
      })
      .join(" · ");
    const pulseFollowupLine = pulseFollowupSummary
      ? `PULSE后继收盘观察（${pulseFollowup.research_mode ?? "NON_PIT_RETROSPECTIVE"}；${pulseFollowup.price_coordinate ?? "RAW_UNADJUSTED"}；描述性回看，不是事前胜率）${pulseFollowupSummary}`
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
      return `${comparisonStateLabels[group.pulse_state] ?? group.pulse_state} 完整篮子 ${coverage.observed_board_relations}/${coverage.all_board_relations}（${coverage.percent_4dp ?? "UNKNOWN"}%）；OBSERVED板块 ${statusCounts.OBSERVED ?? 0} · UNKNOWN板块 ${statusCounts.UNKNOWN ?? 0} · 固定成员 ${group.fixed_member_denominator_sum ?? "UNKNOWN"} · 有效成员 ${group.observed_member_count_sum ?? "UNKNOWN"} · 未知成员 ${group.unknown_member_count_sum ?? "UNKNOWN"}${extraStates ? ` · ${extraStates}` : ""}${reasons ? `，未知原因 ${reasons}` : ""}${incompleteReasons ? `，不完整原因 ${incompleteReasons}` : ""}；${comparisonDistributionText(group.distribution)}`;
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
      `${displayBoardName(board)} · 成员快照观测 ${board.observed_at || "UNKNOWN"} · ${boardSourceQualitySummary(pulseBoard || board)} · NON_PIT 回看 · ${pulseSummary} · ${pulseTransitionSummary} · ${pulseFollowupLine} · ${pulseFollowupComparisonLine} · 正式 Rotation 未评估`,
    );
    message(
      "boardCoverage",
      observation?.membership_only
        ? `当前成员 ${format(observation.snapshot_member_count)} · 来源池内/池外 ${format(observation.source_pool.inside)}/${format(observation.source_pool.outside)} · D0/Seed/D1 NOT_COMPUTED · ${observation.reason} · 成员为当前快照 NON_PIT`
        : observation?.status === "AVAILABLE"
          ? `当前成员 ${format(observation.snapshot_member_count)} · 来源池内/池外 ${format(observation.source_pool.inside)}/${format(observation.source_pool.outside)} · D0 TRUE/FALSE/UNKNOWN/缺事实 ${format(observation.d0.true)}/${format(observation.d0.false)}/${format(observation.d0.unknown)}/${format(observation.d0.missing)}（分母 ${format(observation.d0.target_source_pool_denominator)}） · Seed TRUE/FALSE/UNKNOWN/缺事实 ${format(observation.seed.true)}/${format(observation.seed.false)}/${format(observation.seed.unknown)}/${format(observation.seed.missing)}（分母 ${format(observation.seed.target_source_pool_denominator)}；${observation.seed.scope_status}） · D1材料变化成员 ${format(observation.d1.material_change_members)} / 锚 ${format(observation.d1.material_anchor_count)} · 目标价质量 ${JSON.stringify(observation.d1.target_price_quality_counts)} · 跨${boardKindLabel(observation.cross_kind_overlap.kind)}重叠代码 ${format(observation.cross_kind_overlap.member_count)} · D0 FALSE→TRUE本板 ${format(observation.d0.transitions?.FALSE_TO_TRUE)} / API本板 ${format(result.filter?.filtered_total)} / 全市场 ${format(result.filter?.global_total)} · 成员为当前快照NON_PIT且关系可重叠`
          : `D0 新增成员 ${format(result.filter?.filtered_total)} · 全市场独立新增 ${format(result.filter?.global_total)} · 板块全成员观察 ${observation?.status || saved.storage_status || "UNKNOWN"}：${observation?.reason || saved.reason || "事实不可用"} · ${membershipScopeSummary(result.membership_status)} · NON_PIT`,
    );
    if (observation?.status !== "AVAILABLE") {
      message(
        "memberList",
        "所选板块全成员研究事实不可用；未知状态不解释为零变化。",
      );
      message("memberPage", "UNKNOWN");
      return;
    }
    const overlapText = observation.cross_kind_overlap.top_boards.length
      ? observation.cross_kind_overlap.top_boards
          .map((item) => `${item.board_name} ${item.overlap_member_count}只`)
          .join("；")
      : "无已证跨类型重叠或对侧快照不可用";
    message(
      "boardCoverage",
      `${$("boardCoverage").textContent} · 跨类型交集最多5个板块：${overlapText} · Seed状态 ${observation.seed.scope_status} · D1 ${observation.d1.scope_status}`,
    );
    fillList(
      "memberList",
      observation.members,
      "当前成员快照没有成员。",
      (item, index) =>
        rowButton(
          displayStockName(item) === "名称未知"
            ? displayStockCode(item)
            : displayStockName(item),
          `代码 ${displayStockCode(item)} · ${item.source_pool_status} · D0 ${evidenceStateLabel(item.d0.after_state)} ${item.d0.transition} · Seed ${evidenceStateLabel(item.seed.source_seed)} / S1 ${evidenceStateLabel(item.seed.s1)} / S2 ${evidenceStateLabel(item.seed.s2)} / Safety ${evidenceStateLabel(item.seed.safety)} · D1 ${item.d1.target_price_quality} · 材料锚 ${item.d1.material_anchor_count}`,
          `RPS5 ${item.seed.rps5_board_rank ? `${item.seed.rps5_board_rank}/${item.seed.rps5_board_rank_denominator}位（并列${item.seed.rps5_board_tie_count}只）` : "未排名"} · RPS20 ${item.seed.rps20_board_rank ? `${item.seed.rps20_board_rank}/${item.seed.rps20_board_rank_denominator}位（并列${item.seed.rps20_board_tie_count}只）` : "未排名"}`,
          `RPS值 ${item.seed.rps5 || "UNKNOWN"} / ${item.seed.rps20 || "UNKNOWN"}（完整来源池）；板块排名分母为有OBSERVED值成员 · ${t0TriageHeadlineText(item.t0_early_triage_summary || { status: item.t0_early_status || "NO_STORED_FACT" })} · 本板 ${t0ContextsSummary(item.t0_early_observations || [], { status: item.t0_early_status || "NO_STORED_FACT" })}`,
          () => openStock(item.source_symbol, item),
          index,
        ),
    );
    message(
      "memberPage",
      `偏移 ${observation.pagination.offset} · 当前快照成员 ${observation.pagination.total_count}`,
    );
    $("memberPrev").disabled = observation.pagination.offset === 0;
    $("memberNext").disabled = observation.pagination.next_offset == null;
  } catch (error) {
    if (token === state.d0BoardRequest && c === state.selected)
      message("memberList", `D0 成员 UNKNOWN：${error.message}`);
  }
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
async function toggleBoardComparison(board) {
  const boardKey = board.board_kind + ":" + board.board_native_id;
  const existing = state.boardComparison.findIndex(
    (item) => item.board_kind + ":" + item.board_native_id === boardKey,
  );
  if (existing >= 0) {
    state.boardComparison.splice(existing, 1);
    ++state.boardComparisonRequest;
    message(
      "boardCompareStatus",
      state.boardComparison.length
        ? "已选择1个板块：" +
            displayBoardName(state.boardComparison[0]) +
            "；再选择另一个板块。"
        : "选择两个板块后比较成员交集、独有集合、去重分母、D0新增、Seed与目标价质量。",
    );
    return;
  }
  if (state.boardComparison.length >= 2) {
    message(
      "boardCompareStatus",
      "已选两个板块；点击已选板块的“比较 +”取消后再选择其他板块。",
    );
    return;
  }
  state.boardComparison.push({
    board_kind: board.board_kind,
    board_native_id: board.board_native_id,
    board_name: board.board_name,
  });
  if (state.boardComparison.length < 2) {
    message(
      "boardCompareStatus",
      "已选择1/2：" +
        displayBoardName(state.boardComparison[0]) +
        "；请选择第二个板块。",
    );
    return;
  }
  const selected = [...state.boardComparison];
  const request = ++state.boardComparisonRequest;
  const context = state.selected;
  message(
    "boardCompareStatus",
    "正在比较 " +
      displayBoardName(selected[0]) +
      " 与 " +
      displayBoardName(selected[1]) +
      " 的精确快照成员和已保存研究事实…",
  );
  try {
    const result = await api("/api/product/board-comparison", {
      target_session: context.target_session,
      research_cutoff: context.research_cutoff,
      source_package_id: context.source_package_id,
      d1_history_session_count: context.d1_history_session_count,
      industry_snapshot_id: context.industry_snapshot_id,
      concept_snapshot_id: context.concept_snapshot_id,
      board_a: {
        board_kind: selected[0].board_kind,
        board_native_id: selected[0].board_native_id,
      },
      board_b: {
        board_kind: selected[1].board_kind,
        board_native_id: selected[1].board_native_id,
      },
    });
    if (request !== state.boardComparisonRequest || context !== state.selected)
      return;
    const a = result.comparison?.board_a || {};
    const b = result.comparison?.board_b || {};
    const intersection = result.comparison?.intersection || {};
    const aOnly = result.comparison?.board_a_only || {};
    const bOnly = result.comparison?.board_b_only || {};
    const union = result.comparison?.union_deduplicated || {};
    const describe = (name, stats) =>
      name +
      "：成员 " +
      format(stats.membership_count) +
      "；来源池内 " +
      format(stats.source_pool_member_count) +
      "；D0新增 " +
      format(stats.d0_new_count) +
      "；Seed TRUE " +
      format(stats.seed?.true_count) +
      "/" +
      format(stats.seed?.stored_member_denominator) +
      "；目标价OBSERVED " +
      format(stats.target_price?.observed_count) +
      "/" +
      format(stats.target_price?.stored_member_denominator);
    message(
      "boardCompareStatus",
      "交集 " +
        format(result.membership_overlap_count) +
        " · A独有 " +
        format(aOnly.membership_count) +
        " · B独有 " +
        format(bOnly.membership_count) +
        " · 去重并集 " +
        format(result.membership_union_deduplicated_count) +
        "；" +
        describe(displayBoardName(selected[0]), a) +
        "；" +
        describe(displayBoardName(selected[1]), b) +
        "；交集D0新增 " +
        format(intersection.d0_new_count) +
        " · 去重并集D0新增 " +
        format(union.d0_new_count) +
        "；Seed " +
        result.seed_scope_status +
        " / 目标价 " +
        result.d1_scope_status +
        " · NON_PIT当前快照，不作为选股结论。",
    );
  } catch (error) {
    if (request === state.boardComparisonRequest)
      message("boardCompareStatus", "双板块比较不可用：" + error.message);
  }
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
    renderD1Boards(data, openD1Board, toggleBoardComparison);
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
      Math.max(0, state.d0BoardOffset - 20),
    );
  d1MaterialState.memberOffset = Math.max(0, d1MaterialState.memberOffset - 30);
  void loadD1Material();
});
$("memberNext").addEventListener("click", () => {
  if (state.d0Board)
    return void openD0Board(state.d0Board, state.d0BoardOffset + 20);
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
    message("stockSearchStatus", "研究日期上下文尚未就绪，请稍后重试。");
    return;
  }
  if (!state.selected.contextReady) {
    message("contextText", "当前日期的研究上下文尚未就绪。");
    message("stockSearchStatus", "研究日期上下文尚未就绪，请稍后重试。");
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
    message("stockSearchStatus", "该成员已打开；未映射到本次沪深来源池。");
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
  message("stockTitle", displayStockTitle(row, symbol));
  message(
    "stockMeta",
    `${c.target_session} · RAW_UNADJUSTED · SOURCE_SYMBOL_ONLY · NON_PIT`,
  );
  message("stockStatus", "正在读取同日 Raw、Core 与 Profile…");
  $("stockContent").replaceChildren();
  message(
    "stockSearchStatus",
    `${displayStockTitle(row, symbol)} 的详情已打开。`,
  );
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
      d1_history_session_count: Number.isInteger(c.d1_history_session_count)
        ? c.d1_history_session_count
        : null,
      industry_snapshot_id: c.industry_snapshot_id,
      concept_snapshot_id: c.concept_snapshot_id,
    });
    if (token !== state.stockRequest) return;
    const storedEvidence = data.stored_research_evidence;
    const d1State = storedEvidence?.d1?.status || "NOT_COMPUTED";
    const d1Scope = storedEvidence?.d1?.scope || {};
    const evidenceRow = {
      ...row,
      source_symbol: symbol,
      research_evidence: storedEvidence || row.research_evidence,
      home_observation: homeObservationFor(
        { ...row, source_symbol: symbol },
        storedEvidence || {},
        c,
      ),
    };
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
    const history = {
      day: null,
      dayError: null,
      week: null,
      month: null,
      selected: "day",
      periodStatus: "LOADING",
    };
    const dayErrorNotice = node("p", null, "rule");
    dayErrorNotice.hidden = true;
    function showHistory(kind) {
      history.selected = kind;
      periodControls.querySelectorAll("button").forEach((button) => {
        button.classList.toggle("selected", button.dataset.kind === kind);
      });
      const entries = history[kind];
      periodResults.replaceChildren();
      if (!entries) {
        periodResults.textContent =
          kind === "day" && history.dayError
            ? "日线读取失败，详见上方提示。"
            : history.periodStatus === "READ_FAILED" && kind !== "day"
              ? "周 / 月历史读取失败，状态为 UNKNOWN；原因见上方。"
              : "正在读取或历史价格不可用。";
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
      dayErrorNotice,
      periodResults,
    );
    root.append(periods, node("h3", "来源级规则与未展示原因"));
    const rules = node("div", null, "rule");
    const seedInfo = node("p", sourceSeedExplanation(data.source_seed));
    rules.replaceChildren(
      node("strong", "来源级诊断"),
      seedInfo,
      renderStoredD0Evidence(evidenceRow),
      node("p", "正式 Priority / Risk / D2 / Focus：NOT_EVALUATED"),
    );
    const d1Info = node(
      "p",
      `D1 已存结构 ${evidenceStatusLabel(d1State)} · ${d1Scope.history_window_start || "UNKNOWN"} 至 ${d1Scope.history_window_end || "UNKNOWN"} · ${storedEvidence?.d1?.structure_fact_count ?? 0}/${storedEvidence?.d1?.expected_structure_fact_count ?? 5} 类；正式 D1 事件评估不在本次替代计算。`,
    );
    root.append(rules, d1Info);
    root.append(node("h3", "已保存来源研究登记"));
    root.append(
      focusEvidenceDetails(
        {
          t0_observation: data.t0_observation,
          research_evidence: storedEvidence,
          seed_t1_outcome: data.seed_t1_outcome,
          source_version: data.source_version,
          d0_change: {
            before_state: storedEvidence?.d0?.before?.state,
            after_state: storedEvidence?.d0?.after?.state,
            transition: storedEvidence?.d0?.transition,
          },
        },
        c,
      ),
    );
    root.append(node("h3", "D1 材料变化"));
    const materialDetail = node("div", null, "rule");
    root.append(materialDetail);
    const materialStatus = node(
      "p",
      "按需读取PG已保存的完整来源池 D1 材料变化。",
    );
    const deepen = node("button", "深入 D1 材料变化", "button subtle");
    deepen.type = "button";
    materialDetail.append(materialStatus, deepen);
    deepen.addEventListener("click", () => {
      deepen.disabled = true;
      deepen.textContent = "正在读取 D1 材料变化…";
      materialStatus.textContent = "正在读取PG已保存的完整来源池 D1 事实…";
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
          materialStatus.textContent =
            "PG保存事实仍在读取；本次材料将自动继续。";
      })
        .then((result) => {
          if (token === state.stockRequest && state.selected === c)
            renderD1StockDetail(result.selected_stock, materialDetail);
        })
        .catch((error) => {
          if (
            token === state.stockRequest &&
            state.selected === c &&
            error.name !== "AbortError"
          ) {
            materialStatus.textContent = `D1材料变化不可用：${error.message}`;
            deepen.disabled = false;
            deepen.textContent = "重试深入 D1 材料变化";
          }
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
        if (!Array.isArray(p.periods?.week) || !Array.isArray(p.periods?.month))
          throw new Error("周月周期响应缺少完整结果");
        history.week = p.periods.week;
        history.month = p.periods.month;
        history.periodStatus = "READY";
        if (history.selected !== "day") showHistory(history.selected);
      })
      .catch((e) => {
        if (token === state.stockRequest) {
          history.periodStatus = "READ_FAILED";
          history.week = history.month = null;
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
        history.dayError = null;
        dayErrorNotice.textContent = "";
        dayErrorNotice.hidden = true;
        if (history.selected === "day") showHistory("day");
      })
      .catch((error) => {
        if (token === state.stockRequest) {
          history.dayError = error.message;
          dayErrorNotice.textContent = `日线 UNKNOWN：${error.message}`;
          dayErrorNotice.hidden = false;
          if (history.selected === "day") showHistory("day");
        }
      });
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
    const row = node("div", undefined, "cohort-detail-row");
    row.append(node("strong", `${label}：`));
    if (value?.[cohortMetricKey])
      appendCohortMetric(row, value.value, value.kind);
    else row.append(node("span", format(value)));
    container.append(row);
  }
}

function renderCohortEnrollmentGroups(result) {
  const root = $("cohortEnrollmentSummary");
  root.replaceChildren();
  if (result.status !== "AVAILABLE") {
    root.append(
      node(
        "p",
        `${result.status || "NO_STORED_FACT"} · ${result.reason_codes?.join("、") || "当前精确T0规则没有可读取的登记事实。"}`,
      ),
    );
    return;
  }
  const risk = result.risk_set || {};
  const stateCounts = risk.rule_state_counts || {};
  const enrollmentCounts = risk.enrollment_counts || {};
  const unregistered = risk.unregistered_counts || {};
  root.append(
    node(
      "p",
      `${result.target_session} · ${result.rule} · 来源捕获 ${result.source_capture_id} · ${result.scope} · ${result.price_coordinate} · ${result.identity_status} · 截止 ${result.research_cutoff}`,
    ),
    node(
      "p",
      `${result.comparison_status} · 当前配对只含同一T0和规则，不跨日合并；独立T0日期 ${result.independent_t0_date_count} · ${result.independent_date_status}`,
    ),
    node(
      "p",
      `完整来源规则分母 ${format(risk.source_pool_count)}（TRUE ${format(stateCounts.TRUE)} / FALSE ${format(stateCounts.FALSE)} / UNKNOWN ${format(stateCounts.UNKNOWN)}）；登记 ENTRY ${format(enrollmentCounts.ENTRY)}、REENTRY ${format(enrollmentCounts.REENTRY)}、左截断 ${format(enrollmentCounts.FIRST_OBSERVED_LEFT_CENSORED)}、DAILY_LANDMARK ${format(enrollmentCounts.DAILY_LANDMARK)}。DAILY_LANDMARK是观察点，不是新增入组。`,
    ),
    node(
      "p",
      `无登记类别：PAUSED_UNKNOWN ${format(unregistered.PAUSED_UNKNOWN)}（最近已知规则状态为TRUE且该事实有活动登记者）；UNKNOWN生命周期未证 ${format(unregistered.UNKNOWN_LIFECYCLE_UNPROVED)}；FALSE无登记 ${format(unregistered.FALSE_NO_ENROLLMENT)}；TRUE无登记 ${format(unregistered.TRUE_WITHOUT_ENROLLMENT)}。`,
    ),
  );

  const labels = {
    ENTRY: "ENTRY · 已证相邻FALSE→TRUE",
    REENTRY: "REENTRY · 已证退出后再次匹配",
    FIRST_OBSERVED_LEFT_CENSORED: "FIRST_OBSERVED_LEFT_CENSORED · 左截断",
    DAILY_LANDMARK: "DAILY_LANDMARK · 活动段每日观察点",
  };
  for (const group of result.groups || []) {
    const details = node(
      "details",
      undefined,
      "evidence-details cohort-enrollment-group",
    );
    details.append(
      node(
        "summary",
        `${labels[group.kind] || group.kind} · 完整登记分母 ${format(group.enrollment_count)} · 去重来源证券 ${format(group.independent_security_count)}${group.kind === "REENTRY" ? ` · 已核父段 ${format(group.parent_proved_count)}` : ""}`,
      ),
    );
    const table = node(
      "table",
      undefined,
      "compact-table cohort-enrollment-table",
    );
    const thead = node("thead");
    const header = node("tr");
    for (const title of [
      "期限",
      "到期 / 有价 / 缺失",
      "正 / 非正",
      "中位收益",
      "冻结候选线后继",
    ]) {
      header.append(node("th", title));
    }
    thead.append(header);
    table.append(thead);
    const tbody = node("tbody");
    for (const outcome of group.forward || []) {
      const line = group.candidate_line_followup?.find(
        (item) => item.horizon === outcome.horizon,
      );
      const row = node("tr");
      row.append(
        node(
          "th",
          `T+${outcome.horizon}${outcome.endpoint_session ? ` · ${outcome.endpoint_session}` : ""} · ${outcome.status}`,
        ),
        node(
          "td",
          `分母 ${format(outcome.risk_denominator)}；有价 ${format(outcome.observed_count)}；UNKNOWN ${format(outcome.unknown_count)}；未计算 ${format(outcome.not_computed_count)}；未到期 ${format(outcome.not_due_count)}`,
        ),
        node(
          "td",
          `${format(outcome.positive_count)} / ${format(outcome.non_positive_count)}`,
        ),
        (() => {
          const cell = node("td");
          appendCohortMetric(cell, outcome.median_exact_return);
          return cell;
        })(),
      );
      const lineCell = node("td");
      if (group.line_scope === "NOT_APPLICABLE" || !line) {
        lineCell.append(node("span", "NOT_APPLICABLE"));
      } else if (!line.candidate_line_count) {
        lineCell.append(node("span", "无合法冻结线；不纳入触线分母。"));
      } else {
        const touch = line.touch_status_counts || {};
        const returned = line.return_status_counts || {};
        const complete =
          touch.UNKNOWN === 0 &&
          touch.NOT_DUE === 0 &&
          returned.UNKNOWN === 0 &&
          returned.NOT_DUE === 0;
        const touchRate = complete
          ? ` · 已证触线 ${format(touch.OBSERVED)}/${format(line.candidate_line_count)}（${((touch.OBSERVED / line.candidate_line_count) * 100).toFixed(2)}%）`
          : ` · 已证触线 ${format(touch.OBSERVED)}/${format(line.candidate_line_count)}`;
        const returnRate = complete
          ? ` · 已证触线后站回 ${format(returned.OBSERVED)}/${format(line.candidate_line_count)}（${((returned.OBSERVED / line.candidate_line_count) * 100).toFixed(2)}%）`
          : ` · 已证触线后站回 ${format(returned.OBSERVED)}/${format(line.candidate_line_count)}`;
        lineCell.append(
          node(
            "span",
            `合法冻结线 ${format(line.candidate_line_count)}${touchRate}；UNKNOWN ${format(touch.UNKNOWN)}、未到期 ${format(touch.NOT_DUE)}、未触 ${format(touch.NOT_OBSERVED)}${returnRate}；UNKNOWN ${format(returned.UNKNOWN)}、未到期 ${format(returned.NOT_DUE)}、未站回 ${format(returned.NOT_OBSERVED)}。SOURCE_PRICE_DIAGNOSTIC，不是正式失效或重入。`,
          ),
        );
      }
      row.append(lineCell);
      tbody.append(row);

      const examples = outcome.examples || {};
      for (const [label, example] of [
        ["实际正收益样例", examples.positive],
        ["实际非正收益样例", examples.non_positive],
      ]) {
        if (!example?.source_symbol) continue;
        const sample = node("div", undefined, "muted cohort-example-metric");
        const code = displayStockCode({ source_symbol: example.source_symbol });
        const button = node("button", `${label} ${code}`, "text-button");
        button.title = "在Focus/Cohort中打开该来源代码";
        button.addEventListener("click", () => {
          $("cohortSymbol").value = code;
          $("focusBackStock").click();
        });
        sample.append(button, node("span", " · "));
        appendCohortMetric(sample, example.exact_return);
        lineCell.append(sample);
      }
    }
    table.append(tbody);
    const tableScroll = node("div", undefined, "cohort-enrollment-scroll");
    tableScroll.tabIndex = 0;
    tableScroll.setAttribute("role", "region");
    tableScroll.setAttribute(
      "aria-label",
      `${labels[group.kind] || group.kind}期限和冻结线后继表，可横向滚动`,
    );
    tableScroll.append(table);
    details.append(tableScroll);
    root.append(details);
  }
  root.append(
    node(
      "p",
      "每组T+N分母闭合为有价 + UNKNOWN + NOT_COMPUTED + NOT_DUE；正/非正只在有价端点中划分。中位数按精确有理数计算。T+5已到期范围最多两个独立T0，当前每次仅展示所选日期，状态为 INSUFFICIENT_INDEPENDENT_DATES。收益是SOURCE_SYMBOL_ONLY、RAW_UNADJUSTED、NON_PIT描述，不表示Focus Episode、D2、Priority、预测或策略有效性。",
      "muted",
    ),
  );
}

function cohortGroupLabel(state) {
  return (
    { TRUE: "MATCH", FALSE: "NO_MATCH", UNKNOWN: "UNKNOWN" }[state] || state
  );
}
function enrollmentDatewiseDifferenceLabel(comparison) {
  if (comparison?.status !== "DESCRIPTIVE_OBSERVED_SUBSETS")
    return comparison?.status || "不可比较";
  return (
    {
      ENTRY_MEDIAN_HIGHER: "ENTRY 中位更高",
      REENTRY_MEDIAN_HIGHER: "REENTRY 中位更高",
      MEDIANS_EQUAL: "两组中位相同",
    }[comparison.direction] ||
    comparison.direction ||
    "已计算描述差异"
  );
}
function renderEnrollmentDatewiseComparison(root, result) {
  const comparison = result?.enrollment_datewise;
  root.append(
    node("h3", "同规则 ENTRY / REENTRY 按 T0 日期对照"),
    node(
      "p",
      `${comparison?.status || "NO_STORED_FACT"} · 有同日两类登记的 T0 ${format(comparison?.paired_t0_date_count)} · ${comparison?.rule_version || "UNKNOWN_RULE_VERSION"} / ${comparison?.path_rule_version || "UNKNOWN_PATH_VERSION"} · ${comparison?.price_coordinate || "UNKNOWN"} / ${comparison?.scope || "UNKNOWN"}`,
    ),
    node(
      "p",
      "每个 T0 和期限独立展示完整 ENTRY、REENTRY 风险集；左截断与 DAILY_LANDMARK 单列，不并入两组。状态按 OBSERVED、UNKNOWN、NOT_COMPUTED、NOT_DUE 闭合。中位差定义为 ENTRY−REENTRY，仅比较各组已观察有价子样本；不合并不同日期，不计算总体胜率、显著性或策略效果。",
      "muted",
    ),
  );
  if (!Array.isArray(comparison?.dates) || !comparison.dates.length) {
    root.append(
      node(
        "p",
        `${comparison?.status || "NO_COMPARABLE_STORED_SCOPE"} · 当前规则没有可读取的唯一完整 T0 范围。`,
      ),
    );
    return;
  }
  for (const item of comparison.dates) {
    const details = node("details", null, "evidence-details cohort-date-group");
    const counts = item.enrollment_counts || {};
    const horizonHeads = (item.horizons || []).map((horizon) => {
      const status = enrollmentDatewiseDifferenceLabel(horizon.comparison);
      return `T+${horizon.horizon} ${horizon.status} ${status}`;
    });
    details.append(
      node(
        "summary",
        `${item.target_session} · ${item.status} · ENTRY ${format(counts.ENTRY)} / REENTRY ${format(counts.REENTRY)} · 左截断 ${format(counts.FIRST_OBSERVED_LEFT_CENSORED)} / 每日观察 ${format(counts.DAILY_LANDMARK)} · ${horizonHeads.join("；")}`,
      ),
    );
    if (item.status !== "PAIRED_RISK_SET") {
      details.append(
        node(
          "p",
          `${(item.reason_codes || []).join("、") || "无同日ENTRY与REENTRY配对"}；该日期不生成两组差异。`,
        ),
      );
    }
    for (const horizon of item.horizons || []) {
      const section = node("section", null, "evidence-details");
      section.append(
        node(
          "h4",
          `T+${horizon.horizon} · 到期 ${horizon.endpoint_session || "未知"} · ${horizon.status}`,
        ),
      );
      for (const group of horizon.groups || []) {
        const outcomes = group.outcomes || {};
        const groupSummary = node("div", undefined, "cohort-result-summary");
        groupSummary.append(
          node(
            "span",
            `${group.kind} · 风险集 ${format(group.risk_denominator)} / 去重证券 ${format(group.independent_security_count)} · 有价 ${format(outcomes.OBSERVED)}（`,
          ),
        );
        if (group.coverage_exact)
          appendCohortMetric(groupSummary, group.coverage_exact, "coverage");
        else groupSummary.append(node("span", "无登记分母"));
        groupSummary.append(
          node(
            "span",
            `） / UNKNOWN ${format(outcomes.UNKNOWN)} / NOT_COMPUTED ${format(outcomes.NOT_COMPUTED)} / NOT_DUE ${format(outcomes.NOT_DUE)} · 正/非正 ${format(group.positive_count)}/${format(group.non_positive_count)} · 有价中位 `,
          ),
        );
        appendCohortMetric(groupSummary, group.median_exact_return);
        section.append(groupSummary);
        for (const [label, example] of [
          [`${group.kind} 正例`, group.examples?.positive],
          [`${group.kind} 非正例`, group.examples?.non_positive],
        ]) {
          if (!example?.source_symbol) continue;
          const code = displayStockCode({
            source_symbol: example.source_symbol,
          });
          const button = node("button", `${label} ${code}`, "text-button");
          button.title = "在Focus/Cohort中打开该来源代码";
          button.type = "button";
          button.addEventListener("click", () => {
            $("cohortSymbol").value = code;
            $("focusBackStock").click();
          });
          const sample = node("div", undefined, "cohort-example-metric");
          sample.append(button, node("span", " · "));
          appendCohortMetric(sample, example.exact_return);
          section.append(sample);
        }
      }
      const difference = horizon.comparison || {};
      const differenceText = node(
        "div",
        undefined,
        "muted cohort-result-summary",
      );
      if (difference.status === "DESCRIPTIVE_OBSERVED_SUBSETS") {
        differenceText.append(
          node(
            "span",
            `${enrollmentDatewiseDifferenceLabel(difference)} · ENTRY−REENTRY 有价中位差 `,
          ),
        );
        appendCohortMetric(differenceText, difference.median_difference_exact);
      } else {
        differenceText.textContent = `${difference.status || "不可比较"} · 未生成两组中位差`;
      }
      section.append(differenceText);
      details.append(section);
    }
    root.append(details);
  }
}
function renderCohortDateComparison(root, result) {
  root.replaceChildren();
  root.append(
    node("h3", "跨 T0 日期描述对照"),
    node(
      "p",
      `${result?.rule || "UNKNOWN_RULE"} · ${result?.rule_version || "UNKNOWN_RULE_VERSION"} · T+${result?.horizon ?? "?"} · ${result?.path_rule_version || "UNKNOWN_PATH_VERSION"} · ${result?.price_coordinate || "UNKNOWN"} · ${result?.scope || "UNKNOWN"}`,
    ),
    node(
      "p",
      `${result?.comparison_status || "UNKNOWN"} · 已到期独立 T0 日期 ${format(result?.independent_t0_date_count)}。逐日展示 MATCH / NO_MATCH / UNKNOWN 与覆盖质量；不把股票数当独立日期，不计算跨日胜率、显著性或因果效果。`,
    ),
  );
  renderEnrollmentDatewiseComparison(root, result);
  if (result?.status !== "AVAILABLE" || !Array.isArray(result.dates)) {
    root.append(
      node(
        "p",
        `${result?.status || "NO_STORED_FACT"} · 没有可读取的完整冻结日期摘要。`,
      ),
    );
    return;
  }
  for (const item of result.dates) {
    const details = node("details", null, "evidence-details cohort-date-group");
    details.append(
      node(
        "summary",
        `${item.target_session} · ${item.status} · 分母 ${format(item.source_pool_count)} · 捕获 ${item.source_capture_id ?? "UNKNOWN"} · Profile ${item.profile_revision || "UNKNOWN"} · 日历 ${item.calendar_revision || "UNKNOWN"}`,
      ),
    );
    if (item.status !== "AVAILABLE") {
      details.append(
        node(
          "p",
          `${(item.reason_codes || []).join("、") || "该日没有可比较的唯一完整 T0 键"}；此日期未进入已到期比较。`,
        ),
      );
    }
    for (const group of item.groups || []) {
      const groupSummary = node("div", undefined, "cohort-result-summary");
      groupSummary.append(
        node(
          "span",
          `${group.group} · 分母 ${format(group.member_count)} · 有价 ${format(group.evaluable_count)} / 未知 ${format(group.unknown_count)} · 正/非正 ${format(group.positive_count)}/${format(group.non_positive_count)} · 中位 `,
        ),
      );
      appendCohortMetric(groupSummary, group.median);
      groupSummary.append(
        node(
          "span",
          ` · 到期 ${JSON.stringify(group.maturity)} · 路径质量 ${JSON.stringify(group.path_quality)} · 不可评价原因 ${JSON.stringify(group.unevaluable_reasons)}`,
        ),
      );
      details.append(groupSummary);
    }
    root.append(details);
  }
}

function formatFepPercent(value) {
  if (value == null) return "未计算";
  const [numeratorText, denominatorText] = String(value).split("/");
  const numerator = BigInt(numeratorText);
  const denominator = denominatorText ? BigInt(denominatorText) : 1n;
  if (denominator === 0n) return "UNKNOWN";
  const scale = 1000000n;
  const absolute = numerator < 0n ? -numerator : numerator;
  const scaled = (absolute * 100n * scale) / denominator;
  const sign = numerator < 0n ? "−" : "";
  return `${sign}${scaled / scale}.${String(scaled % scale).padStart(6, "0")}%`;
}

function renderFepLaneBaseline(root, result, lane) {
  root.replaceChildren();
  root.append(node("h4", `FEP 条件统计（描述性）· ${lane}`));
  root.append(
    node(
      "p",
      "SOURCE_STOCK · RAW_CLOSE_RETURN · NON_PIT_EXPLORATORY · RAW_UNADJUSTED · DESCRIPTIVE_ONLY。这里只读已存PG统计，不输出预测概率、模型支持或Priority资格。",
      "muted",
    ),
  );
  if (result?.status !== "AVAILABLE" || !Array.isArray(result.rows)) {
    root.append(
      node(
        "p",
        `${result?.status || "UNKNOWN"} · ${(result?.reason_codes || []).join("、") || "当前来源、规则、期限和lane没有唯一已存FEP结果。"} · 模型状态 ${result?.model_capability || "NOT_ENABLED"}`,
      ),
    );
    return;
  }

  const rows = result.rows;
  const equalDate = rows.find(
    (row) =>
      row.summary_scope === "EQUAL_DATE" &&
      row.group_code === "MATCH_VS_NO_MATCH" &&
      row.metric_code === "RAW_CLOSE_RETURN" &&
      row.condition_code === "ALL",
  );
  const dateRows = rows.filter(
    (row) =>
      row.summary_scope === "T0" &&
      row.metric_code === "RAW_CLOSE_RETURN" &&
      ["MATCH", "NO_MATCH", "UNKNOWN"].includes(row.group_code),
  );
  const t0Contexts = [
    ...new Map(
      dateRows
        .filter((row) => row.group_code === "MATCH")
        .map((row) => [row.target_session, row]),
    ).values(),
  ];
  if (t0Contexts.length) {
    const totals = t0Contexts.reduce(
      (sum, row) => ({
        sourcePool: sum.sourcePool + (row.source_pool_count || 0),
        match: sum.match + (row.t0_match_count || 0),
        noMatch: sum.noMatch + (row.t0_no_match_count || 0),
        unknown: sum.unknown + (row.t0_unknown_count || 0),
        entry: sum.entry + (row.entry_count || 0),
        daily: sum.daily + (row.daily_landmark_count || 0),
        reentry: sum.reentry + (row.reentry_count || 0),
        leftCensored: sum.leftCensored + (row.left_censored_count || 0),
      }),
      {
        sourcePool: 0,
        match: 0,
        noMatch: 0,
        unknown: 0,
        entry: 0,
        daily: 0,
        reentry: 0,
        leftCensored: 0,
      },
    );
    root.append(
      node(
        "p",
        `${t0Contexts.length}个独立T0 · 完整来源池 ${format(totals.sourcePool)}（每T0约${format(Math.round(totals.sourcePool / t0Contexts.length))}） · 三态 TRUE/FALSE/UNKNOWN ${format(totals.match)}/${format(totals.noMatch)}/${format(totals.unknown)} · ENTRY/DAILY_LANDMARK/REENTRY/左截断 ${format(totals.entry)}/${format(totals.daily)}/${format(totals.reentry)}/${format(totals.leftCensored)}。REENTRY与左截断不计入ENTRY。`,
      ),
    );
  }
  if (equalDate) {
    root.append(
      node(
        "p",
        `T+${result.horizon} · ${equalDate.period_start_session}–${equalDate.period_end_session} · ${equalDate.status} · 可比较独立日期 ${format(equalDate.valid_date_count)} / ${format(equalDate.requested_date_count)} · 日期等权的同日最近秩p50差 ` +
          `${formatFepPercent(equalDate.equal_date_mean_median_difference)} · 日期等权观察样本正收益占比差 ${formatFepPercent(equalDate.equal_date_mean_positive_fraction_difference)} · ${result.rule_id} ${result.rule_version}`,
        "cohort-result-summary",
      ),
    );
    root.append(
      node(
        "p",
        `后继日期未到期 ${format(equalDate.not_due_date_count)} · 质量未知 ${format(equalDate.unknown_date_count)} · 到期未计算 ${format(equalDate.not_computed_date_count)} · 模型 ${result.model_capability || "NOT_ENABLED"}。这些数值是事后描述，不是未来个股概率。`,
        "muted",
      ),
    );
  }
  const sameT0Comparison = result.same_t0_comparison;
  if (lane === "REENTRY" && sameT0Comparison) {
    root.append(
      node(
        "p",
        `同 T0 ENTRY / REENTRY 配对 · ${sameT0Comparison.status} · ${sameT0Comparison.requested_t0_date_count} 个独立T0日期；仅当来源捕获、规则与版本、画像、期限、日历、坐标和质量政策一致且两组有可评价值时配对。差异只作描述，不表示因果或策略优势。`,
        "cohort-result-summary",
      ),
    );
    const metricLabels = {
      RAW_CLOSE_RETURN: "端点收益",
      RAW_MFE: "MFE",
      RAW_MAE: "MAE",
      RAW_MDD_CLOSE: "MDD_CLOSE",
    };
    for (const metric of sameT0Comparison.metrics || []) {
      root.append(
        node(
          "p",
          `${metricLabels[metric.metric_code] || metric.metric_code} · ${metric.status} · 同日可比 ${format(metric.paired_t0_date_count)}/${format(metric.requested_t0_date_count)} 天 · 未匹配 ${format(metric.no_comparable_pair_date_count)} · NOT_DUE ${format(metric.not_due_date_count)} · UNKNOWN ${format(metric.unknown_date_count)} · NOT_COMPUTED ${format(metric.not_computed_date_count)} · 不完整 ${format(metric.incomplete_date_count)} · 日期等权 REENTRY−ENTRY p50差 ${formatFepPercent(metric.equal_date_mean_p50_difference)}。`,
          "muted",
        ),
      );
      for (const pair of metric.dates || []) {
        const entry = pair.entry;
        const reentry = pair.reentry;
        root.append(
          node(
            "p",
            `${pair.target_session} · ${pair.status} · ENTRY ${entry ? `${entry.status} ${format(entry.observed_count)}/${format(entry.member_count)} p25/p50/p75 ${formatFepPercent(entry.p25)}/${formatFepPercent(entry.p50)}/${formatFepPercent(entry.p75)}` : "无已存T0行"} · REENTRY ${reentry ? `${reentry.status} ${format(reentry.observed_count)}/${format(reentry.member_count)} p25/p50/p75 ${formatFepPercent(reentry.p25)}/${formatFepPercent(reentry.p50)}/${formatFepPercent(reentry.p75)}` : "无已存T0行"} · REENTRY−ENTRY p50 ${formatFepPercent(pair.reentry_minus_entry_p50)}`,
            "muted",
          ),
        );
      }
    }
  }
  const conditions = rows.filter(
    (row) =>
      row.summary_scope === "EQUAL_DATE" &&
      row.group_code === "MATCH_VS_NO_MATCH" &&
      row.metric_code === "RAW_CLOSE_RETURN" &&
      row.condition_code !== "ALL",
  );
  if (conditions.length) {
    root.append(
      node("p", "固定来源宽度条件（质量不闭合的日期保留 UNKNOWN）："),
    );
    for (const condition of conditions) {
      root.append(
        node(
          "p",
          `${condition.condition_code} · ${condition.status} · 独立日期 ${format(condition.valid_date_count)} / ${format(condition.condition_date_count)} · 日期等权p50差 ${formatFepPercent(condition.equal_date_mean_median_difference)}`,
          "muted",
        ),
      );
    }
  }

  const dates = [...new Set(dateRows.map((row) => row.target_session))].sort();
  for (const session of dates) {
    const sessionRows = dateRows.filter(
      (row) => row.target_session === session,
    );
    const match = sessionRows.find((row) => row.group_code === "MATCH");
    const noMatch = sessionRows.find((row) => row.group_code === "NO_MATCH");
    const unknown = sessionRows.find((row) => row.group_code === "UNKNOWN");
    const details = node("details", null, "evidence-details cohort-date-group");
    details.append(
      node(
        "summary",
        `${session} · ${match?.market_condition_code || "B_FIXED_UNKNOWN"} · ${match?.profile_family || "UNKNOWN_PROFILE"} · 历史窗 ${match?.history_session_count ?? "?"}/${match?.d1_history_session_count ?? "?"} · 捕获 ${match?.source_capture_id ?? "UNKNOWN"} · ${match?.source_time_quality || "UNKNOWN"}`,
      ),
    );
    for (const group of [match, noMatch, unknown].filter(Boolean)) {
      details.append(
        node(
          "p",
          `${group.group_code} · 样本 ${format(group.member_count)} · 有价 ${format(group.observed_count)} · 未到期 ${format(group.not_due_count)} · 未知 ${format(group.unknown_count)} · 未计算 ${format(group.not_computed_count)} · p25/p50/p75 ${formatFepPercent(group.p25)} / ${formatFepPercent(group.p50)} / ${formatFepPercent(group.p75)} · 正/非正 ${format(group.positive_count)}/${format(group.non_positive_count)}${group.reason_codes?.length ? ` · 原因 ${group.reason_codes.join("、")}` : ""}`,
        ),
      );
      const pathLabels = {
        RAW_MFE: "MFE",
        RAW_MAE: "MAE",
        RAW_MDD_CLOSE: "MDD_CLOSE",
      };
      for (const metricCode of Object.keys(pathLabels)) {
        const path = rows.find(
          (row) =>
            row.summary_scope === "T0" &&
            row.target_session === session &&
            row.group_code === group.group_code &&
            row.metric_code === metricCode,
        );
        if (!path) continue;
        details.append(
          node(
            "p",
            `  ${pathLabels[metricCode]} · ${path.status} · 可评价 ${format(path.observed_count)} / ${format(path.member_count)} · 未到期 ${format(path.not_due_count)} · UNKNOWN ${format(path.unknown_count)} · 未计算 ${format(path.not_computed_count)} · p25/p50/p75 ${formatFepPercent(path.p25)} / ${formatFepPercent(path.p50)} / ${formatFepPercent(path.p75)}${path.reason_codes?.length ? ` · 原因 ${path.reason_codes.join("、")}` : ""}`,
            "muted",
          ),
        );
      }
    }
    if (match) {
      details.append(
        node(
          "p",
          `精确Profile ${match.profile_revision} · 规则 ${match.rule_version} · 日历 ${match.calendar_revision} · Forward ${match.path_rule_version} · 市场宽度 ${match.market_metric || "UNKNOWN"} (${format(match.market_eligible_count)}/${format(match.market_source_count)})`,
          "muted",
        ),
      );
    }
    root.append(details);
  }
}

function cohortDateComparisonDisclosure(context, key, token) {
  const details = node(
    "details",
    undefined,
    "cohort-date-comparison-disclosure",
  );
  details.append(node("summary", "按需读取跨 T0 ENTRY / REENTRY 逐日对照"));
  const content = node("div", undefined, "cohort-date-comparison-content");
  content.append(
    node(
      "p",
      "普通 Cohort 支持 T+1/3/5/10/20；此跨 T0 ENTRY / REENTRY 对照仅支持 T+1/3/5。展开后才读取当前冻结规则的已存逐日对照。",
      "muted",
    ),
  );
  details.append(content);
  let requested = false;
  let fepRequested = false;
  const loadFep = async (container, horizon) => {
    if (fepRequested) return;
    fepRequested = true;
    container.replaceChildren(node("p", "正在读取已存FEP描述统计…", "muted"));
    try {
      const results = await Promise.all(
        ["ENTRY", "DAILY_LANDMARK", "REENTRY"].map(async (lane) => {
          const query = new URLSearchParams({
            source_package_id: context.source_package_id,
            rule: key.rule,
            horizon,
            lane,
          });
          return {
            lane,
            result: await api(`/api/product/fep-baseline?${query}`),
          };
        }),
      );
      if (
        token !== cohortRequest ||
        activePage !== "focus" ||
        state.selected !== context ||
        !details.isConnected ||
        $("cohortHorizon").value !== horizon
      )
        return;
      container.replaceChildren();
      for (const item of results) {
        const section = node("section", undefined, "cohort-result-summary");
        renderFepLaneBaseline(section, item.result, item.lane);
        container.append(section);
      }
    } catch (error) {
      if (
        token !== cohortRequest ||
        activePage !== "focus" ||
        state.selected !== context ||
        !details.isConnected
      )
        return;
      fepRequested = false;
      container.replaceChildren(node("p", `FEP读取失败：${error.message}`));
      const retry = node("button", "重试读取FEP描述统计", "button subtle");
      retry.type = "button";
      retry.addEventListener("click", () => loadFep(container, horizon));
      container.append(retry);
    }
  };
  details.addEventListener("toggle", async () => {
    if (!details.open || requested) return;
    requested = true;
    const horizon = $("cohortHorizon").value;
    if (!["1", "3", "5"].includes(horizon)) {
      content.replaceChildren(
        node(
          "p",
          `跨 T0 ENTRY / REENTRY 对照不支持 T+${horizon}；普通 Cohort 的 T+${horizon} 读取仍然有效。请选择 T+1、T+3 或 T+5 后重新读取 Cohort。`,
        ),
      );
      return;
    }
    content.replaceChildren(node("p", "正在读取跨 T0 对照…", "muted"));
    try {
      const comparisonQuery = {
        source_package_id: context.source_package_id,
        rule: key.rule,
        horizon,
      };
      const comparison = await api(
        `/api/product/cohort-date-comparison?${new URLSearchParams(comparisonQuery)}`,
      );
      if (
        token !== cohortRequest ||
        activePage !== "focus" ||
        state.selected !== context ||
        !details.isConnected
      )
        return;
      renderCohortDateComparison(content, comparison);
      const fepContent = node(
        "div",
        undefined,
        "cohort-date-comparison-content",
      );
      content.append(fepContent);
      void loadFep(fepContent, horizon);
    } catch (error) {
      if (
        token !== cohortRequest ||
        activePage !== "focus" ||
        state.selected !== context ||
        !details.isConnected
      )
        return;
      content.replaceChildren(node("p", `读取失败：${error.message}`));
      requested = false;
    }
  });
  return details;
}
function renderCohortSymbolForward(result, lineFollowup) {
  const root = $("cohortSymbolForward");
  root.replaceChildren();
  root.append(node("h3", "所选代码同一 T0 的 T+1 / T+3 / T+5"));
  if (lineFollowup) {
    const line = lineFollowup.line || {};
    const fraction = line.value
      ? `${line.value.numerator}/${line.value.denominator}`
      : "无合法候选线";
    const touchSummary =
      lineFollowup.status === "AVAILABLE"
        ? `来源候选线 ${fraction} · ${line.source || "UNKNOWN_SOURCE"} · ${lineFollowup.first_touch_status || "UNKNOWN"}${lineFollowup.first_proved_touch ? ` ${lineFollowup.first_proved_touch}` : lineFollowup.first_proved_touch_upper_bound ? ` 上界 ${lineFollowup.first_proved_touch_upper_bound}` : ""} · 重新站回 ${lineFollowup.first_return_above_status || "UNKNOWN"}${lineFollowup.first_proved_return_above ? ` ${lineFollowup.first_proved_return_above}` : lineFollowup.first_proved_return_above_upper_bound ? ` 上界 ${lineFollowup.first_proved_return_above_upper_bound}` : ""} · ${lineFollowup.maturity || "UNKNOWN"} · ${lineFollowup.path_quality || "UNKNOWN"}`
        : `${lineFollowup.status || "UNKNOWN"} · ${lineFollowup.reason_code || "无候选线或未满足TRUE风险集"}`;
    root.append(
      node(
        "p",
        `来源级候选线后续：${touchSummary}。TOUCHED/RETURNED_ABOVE不是正式D2 INVALIDATED/REENTRY，也不表示Focus退出或买卖建议。`,
      ),
    );
    if (
      Array.isArray(lineFollowup.observations) &&
      lineFollowup.observations.length
    ) {
      const touchRows = node("ul");
      for (const observation of lineFollowup.observations) {
        const close = observation.close || {};
        const closeText =
          close.quality === "OBSERVED"
            ? `${close.numerator}/${close.denominator}`
            : `${close.quality || "UNKNOWN"}${close.reason ? ` · ${close.reason}` : ""}`;
        touchRows.append(
          node(
            "li",
            `T+${observation.offset ?? "?"} ${observation.session || ""} · ${observation.state || "UNKNOWN"} · RAW_UNADJUSTED close ${closeText}`,
          ),
        );
      }
      root.append(touchRows);
    }
  }
  if (result?.status !== "AVAILABLE") {
    root.append(
      node(
        "p",
        `${result?.status || "UNKNOWN"} · ${(result?.reason_codes || []).join("、") || "没有精确T0规则事实"}`,
      ),
    );
    return;
  }
  root.append(
    node(
      "p",
      `${result.target_session} · ${result.source_symbol || "UNKNOWN_SYMBOL"} · ${cohortGroupLabel(result.rule_state)} · 规则事实 #${result.rule_fact_id} · 捕获 ${result.source_capture_id} · 日历 ${result.calendar_revision} · RAW_UNADJUSTED / NON_PIT_EXPLORATORY`,
    ),
  );
  const rows = node("ul");
  for (const path of result.paths || []) {
    const value =
      path.status === "OBSERVED"
        ? exactPercentMetric({
            quality: path.endpoint_quality,
            numerator: path.endpoint_numerator,
            denominator: path.endpoint_denominator,
          })
        : `${path.status || "UNKNOWN"}（${(path.reason_codes || []).join("、") || "无原因码"}）`;
    rows.append(
      node(
        "li",
        `T+${path.horizon} 到期 ${path.due_session || "未知"} · ${path.status || "UNKNOWN"} · 收盘变化 ${value} · 端点 ${path.endpoint_quality || "UNKNOWN"} · 路径 ${path.path_quality || "UNKNOWN"}${path.maturity ? ` · ${path.maturity}` : ""}${path.path_reasons?.length ? ` · ${path.path_reasons.join("、")}` : ""} · ${path.path_rule_version || "UNKNOWN"} / ${path.calendar_revision || "UNKNOWN_CALENDAR"}`,
      ),
    );
  }
  root.append(rows);
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
      cohortRpsPulseData = null;
      cohortRpsPulseRequested = false;
      renderCohortRpsPulse(null);
      $("cohortDateComparison").replaceChildren();
      $("cohortSymbolForward").replaceChildren();
      $("cohortRows").replaceChildren();
      $("cohortEnrollmentSummary").replaceChildren();
      message("cohortEnrollmentStatus", "当前日期尚未就绪。");
      $("focusStockResult").replaceChildren();
      $("focusAutoRows").replaceChildren();
      message("focusAutoStatus", "当前日期尚未就绪。");
      $("focusSegmentRows").replaceChildren();
      message("focusSegmentStatus", "当前日期尚未就绪。");
      $("focusSegmentPrev").disabled = $("focusSegmentNext").disabled = true;
      $("focusAutoPrev").disabled = $("focusAutoNext").disabled = true;
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
    cohortRpsPulseData = null;
    cohortRpsPulseRequested = false;
    renderCohortRpsPulse(null);
    $("cohortDateComparison").replaceChildren();
    $("cohortSymbolForward").replaceChildren();
    $("cohortRows").replaceChildren();
    $("cohortEnrollmentSummary").replaceChildren();
    message("cohortEnrollmentStatus", "选择冻结规则后可按需读取。");
    $("cohortPrev").disabled = $("cohortNext").disabled = true;
    message("cohortPage", "—");
    $("focusStockResult").replaceChildren();
    message(
      "focusStockStatus",
      "输入六位股票代码后可独立读取 Raw 观察；正式 Focus 未计算。",
    );
    void loadFocusAuto(c, 0);
    void loadFocusObservationSegments(c, 0);
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
      state.marketResult = data;
      renderMarketD0Summary();
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
        [
          "宽度分类",
          `${marketClassText(data.market_axes?.breadth?.classification)} · B_fixed_source`,
        ],
        ["来源分母", data.denominator.N_source],
        [
          "来源读取覆盖",
          `目标日有价 ${data.source_cohort.target_present_count} · 缺价 ${data.source_cohort.target_missing_count} · 读取失败 ${data.source_cohort.read_failed_count} · Raw行 ${data.source_cohort.raw_row_count}`,
        ],
        ["可评价分母", data.denominator.N_eligible],
        ["UNKNOWN", data.denominator.UNKNOWN],
        ["覆盖", data.denominator.coverage?.fraction],
        ["宽度分类分母", marketBreadthCountText(data.denominator.counts)],
        [
          "宽度未知原因",
          marketBreadthReasonText(data.denominator.unknown_reason_counts),
        ],
        [
          "成交参与中位数",
          data.market_axes?.participation?.metric?.exact
            ? `${data.market_axes.participation.metric.exact.display_decimal_12dp} · ${marketClassText(data.market_axes.participation.metric.classification)}`
            : `${marketStatusText(data.market_axes?.participation?.status)} · ${data.market_axes?.participation?.reason || "尚无同日保存事实。"}`,
        ],
        [
          "成交参与分母",
          data.market_axes?.participation?.denominator
            ? `${marketStatusText(data.market_axes.participation.status)} · 画像 ${data.market_axes.participation.denominator.profile_count}/${data.market_axes.participation.denominator.source_pool_count} · 可评价 ${data.market_axes.participation.denominator.assessable_count} · UNKNOWN ${data.market_axes.participation.denominator.unknown_count}`
            : "无已保存成交参与分母",
        ],
        [
          "成交参与未知原因",
          data.market_axes?.participation?.denominator?.unknown_reason_counts
            ? Object.entries(
                data.market_axes.participation.denominator
                  .unknown_reason_counts,
              )
                .map(
                  ([reason, count]) =>
                    `${marketUnknownReasonText(reason)} ${count}`,
                )
                .join("；")
            : "无",
        ],
        [
          "成交参与口径",
          data.market_axes?.participation?.metric?.exact
            ? `amount_ratio20中位数 · ${data.market_axes.participation.price_coordinate} · ${data.market_axes.participation.pit_status} · 原生金额单位 ${data.market_axes.participation.profile?.amount_unit_status || "UNKNOWN"}（比值无量纲）`
            : "RAW_UNADJUSTED · NON_PIT_RETROSPECTIVE",
        ],
        [
          "趋势轴",
          `${data.market_axes?.trend?.status || "NOT_COMPUTED"} · ${data.market_axes?.trend?.reason || "无合法PRIMARY_MARKET参考序列。"}`,
        ],
        [
          "压力轴",
          `${data.market_axes?.pressure?.status || "UNKNOWN"} · ${data.market_axes?.pressure?.reason || "缺少日期有效限价制度事实。"}`,
        ],
        [
          "综合研究环境",
          `${data.market_axes?.composite?.status || "UNKNOWN"}${data.market_axes?.composite?.label ? ` · ${data.market_axes.composite.label}` : ""} · 可能标签 ${data.market_axes?.composite?.possible_labels?.join("、") || "UNKNOWN"}`,
        ],
        [
          "来源质量 / 可得时间",
          `${data.source_cohort.source_time_quality} / ${data.source_cohort.source_available_at}`,
        ],
        [
          "窗口",
          marketLookbackSessionsText(data.official_calendar?.lookback_sessions),
        ],
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
      const profileCount =
        state.profileDirectoryStatus === "READ_FAILED"
          ? `DIRECTORY_READ_FAILED · ${state.profileDirectoryError?.message || "读取失败"}`
          : state.profileDirectoryStatus === "LOADING"
            ? "目录读取中"
            : profiles.length
              ? profiles.length
              : state.profileDirectoryStatus === "EMPTY"
                ? "目录为空"
                : "所选日期无已保存修订";
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
        ["画像修订数", profileCount],
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
async function readCohort(includeRpsPulseGroups = cohortRpsPulseRequested) {
  cohortRpsPulseRequested = includeRpsPulseGroups === true;
  const c = state.selected,
    raw = $("cohortKey").value,
    stockCode = $("cohortSymbol").value.trim().toUpperCase();
  if (!c || !raw) return;
  const token = ++cohortRequest,
    key = JSON.parse(raw);
  message("focusStatus", "正在读取冻结研究…");
  $("cohortSummary").replaceChildren();
  $("cohortEnrollmentSummary").replaceChildren();
  message("cohortEnrollmentStatus", "选择冻结规则后可按需读取。");
  $("cohortRpsPulseSummary").replaceChildren();
  if (cohortRpsPulseRequested)
    message(
      "cohortRpsPulseStatus",
      "正在读取当前T0、冻结规则及同组独立日期的已存RPS×PULSE结果。",
    );
  else renderCohortRpsPulse(null);
  $("cohortDateComparison").replaceChildren();
  $("cohortSymbolForward").replaceChildren();
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
      rps_board_kind: $("cohortRpsBoardKind").value,
    };
    if (cohortRpsPulseRequested) query.include_rps_pulse_groups = "true";
    if (symbol) query.symbol = symbol;
    const d = await api(`/api/product/cohort?${new URLSearchParams(query)}`);
    if (token !== cohortRequest || activePage !== "focus") return;
    $("cohortDateComparison").append(
      cohortDateComparisonDisclosure(c, key, token),
    );
    cohortRpsPulseData = d.rps_pulse_groups ?? null;
    renderCohortRpsPulse(cohortRpsPulseData);
    if (symbol) {
      try {
        const forwardQuery = {
          target_session: c.target_session,
          source_package_id: c.source_package_id,
          profile_revision: key.profile_revision,
          history: key.history,
          d1_history: key.d1_history,
          window: key.window,
          rule: key.rule,
          symbol,
        };
        const forward = await api(
          `/api/product/cohort-symbol-forward?${new URLSearchParams(forwardQuery)}`,
        );
        if (token !== cohortRequest || activePage !== "focus") return;
        renderCohortSymbolForward(forward, d.frozen_candidate_followup);
      } catch (error) {
        if (token === cohortRequest && activePage === "focus")
          $("cohortSymbolForward").replaceChildren(
            node("h3", "所选代码同一 T0 的 T+1 / T+3 / T+5"),
            node("p", `读取失败：${error.message}`),
          );
      }
    }
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
    const candidateFollowup = d.frozen_candidate_followup;
    const candidateFollowupSummary = !candidateFollowup
      ? "未选择来源代码；T+1/T+3/T+5按已存路径读取。"
      : candidateFollowup.status === "AVAILABLE"
        ? `${displayStockTitle(candidateFollowup)} · 候选线 ${candidateFollowup.line?.value?.numerator || "?"}/${candidateFollowup.line?.value?.denominator || "?"} · ${candidateFollowup.path_quality || "UNKNOWN"} · 首次触及 ${candidateFollowup.first_touch_status || "UNKNOWN"} · 重新站回 ${candidateFollowup.first_return_above_status || "UNKNOWN"}；后继路径见下方。`
        : `${candidateFollowup.status || "UNKNOWN"} · ${candidateFollowup.reason_code || "当前规则没有可用TRUE候选线"}；后继路径不进入触线风险集。`;
    detailRows($("cohortSummary"), [
      ["完整来源分母", d.source_pool_count],
      ...d.groups.flatMap((g) => [
        [
          `${g.group} · ${g.weighting === "stock" ? "按代码" : "按事件"}`,
          [
            `总数 ${g.count}；可评价 ${g.evaluable}；未知 ${g.unknown}`,
            `正/非正 ${g.positive}/${g.non_positive}`,
            `路径质量 ${JSON.stringify(g.path_quality)}`,
            `到期状态 ${JSON.stringify(g.maturity)}`,
            `原因 ${JSON.stringify(g.unevaluable_reasons)}`,
          ].join("；"),
        ],
        [`${g.group} 中位收益`, cohortMetricValue(g.median)],
      ]),
      ["所选代码冻结线触及诊断", candidateFollowupSummary],
      ...(d.selected_rules || []).map((fact) => [
        `所选代码 · ${fact.rule_id}`,
        `${{ TRUE: "MATCH", FALSE: "NO_MATCH", UNKNOWN: "UNKNOWN" }[fact.state] || fact.state} · ${format(fact.reasons)} · ${fact.enrollment ? `登记 #${fact.enrollment.id} ${enrollmentKindLabel(fact.enrollment.kind)}${enrollmentParentText(fact.enrollment)} · ${fact.enrollment.reason}` : "NO_STORED_FACT（无该规则登记）"} · 候选线触及诊断 ${candidateFollowup?.status || "NOT_COMPUTED"}；价格后继见下方`,
      ]),
      ...(!d.due
        ? [
            [
              "待验证",
              "NOT_DUE：需要真实官方后继会话及同源价格；当前不代表收益或正式PIT验证。",
            ],
          ]
        : []),
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
    $("cohortPrev").disabled = Boolean(symbol) || cohortOffset === 0;
    $("cohortNext").disabled = Boolean(symbol) || d.page.next_offset === null;
  } catch (error) {
    if (token === cohortRequest && activePage === "focus")
      message("focusStatus", `读取失败：${error.message}`);
  }
}
$("readCohortEnrollment").addEventListener("click", async () => {
  const c = state.selected;
  const raw = $("cohortKey").value;
  if (!c || !raw) {
    message("cohortEnrollmentStatus", "先选择已保存的T0日期和冻结规则。");
    return;
  }
  const token = cohortRequest;
  const key = JSON.parse(raw);
  message(
    "cohortEnrollmentStatus",
    "正在读取同T0登记、PG Forward和冻结候选线分组…",
  );
  $("cohortEnrollmentSummary").replaceChildren();
  try {
    const query = {
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      profile_revision: key.profile_revision,
      history: key.history,
      d1_history: key.d1_history,
      window: key.window,
      rule: key.rule,
    };
    const result = await api(
      `/api/product/cohort-enrollment-lifecycle?${new URLSearchParams(query)}`,
    );
    if (token !== cohortRequest || activePage !== "focus") return;
    renderCohortEnrollmentGroups(result);
    message(
      "cohortEnrollmentStatus",
      `${result.status || "UNKNOWN"} · ${result.target_session || c.target_session} · ${result.rule || key.rule} · READ_ONLY`,
    );
  } catch (error) {
    if (token === cohortRequest && activePage === "focus")
      message("cohortEnrollmentStatus", `读取失败：${error.message}`);
  }
});
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
    const sessionCount = c.stock_history_session_count;
    if (!Number.isInteger(sessionCount) || sessionCount <= 0)
      throw new Error("所选日期的股票历史窗口不可用");
    const data = await api("/api/preview/stock", {
      symbol,
      target_session: c.target_session,
      source_package_id: c.source_package_id,
      session_count: sessionCount,
      profile_revision: c.profile_revision,
      d1_history_session_count: c.d1_history_session_count,
      industry_snapshot_id: c.industry_snapshot_id,
      concept_snapshot_id: c.concept_snapshot_id,
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
      [
        "T0研究登记",
        (data.t0_observation?.enrollments || [])
          .map(enrollmentSummaryText)
          .join("；") ||
          `${data.t0_observation?.status || "UNKNOWN"} · ${data.t0_observation?.reason || "NO_STORED_FACT"}`,
      ],
      ["D0来源事实", sourceD0SummaryText(data.t0_observation?.d0_context)],
      [
        "正式D0准入与D2 / Risk / Priority",
        formalD0AdmissionSummaryText(
          data.t0_observation?.formal_d0_admission ||
            data.formal_d0_admission ||
            data.stored_research_evidence?.formal_d0_admission,
        ),
      ],
      [
        "Seed / S1 / S2 / Safety",
        (() => {
          const seed = data.stored_research_evidence?.seed || {};
          return `来源形态Seed ${seed.source_seed?.state || "UNKNOWN"}；S1 ${seed.s1?.state || "UNKNOWN"}；S2 ${seed.s2?.state || "UNKNOWN"}；Safety ${seed.safety?.state || "UNKNOWN"}；正式Seed ${data.seed_t1_outcome?.formal_seed?.state || "UNKNOWN"}`;
        })(),
      ],
      ["同源S2跨日观察", s2ContinuitySummaryText(data.seed_t1_outcome)],
      ["S2与Seed T+1价格", seedT1SummaryText(data.seed_t1_outcome, false)],
      ["S2两端原因", s2ContinuityReasonText(data.seed_t1_outcome)],
      ["Seed冻结原因", seedT1ReasonText(data.seed_t1_outcome)],
      [
        "D1材料与目标价",
        (() => {
          const d1 = data.stored_research_evidence?.d1 || {};
          return `${d1.status || "UNKNOWN"}；材料锚 ${d1.material_change_anchor_count ?? "UNKNOWN"}；目标价 ${d1.target_price_quality || "UNKNOWN"} ${Array.isArray(d1.target_price_reasons) ? d1.target_price_reasons.join("、") : ""}`;
        })(),
      ],
      ...Object.entries(data.stored_research_evidence?.rules?.facts || {}).map(
        ([rule, fact]) => [
          `冻结规则 ${rule}`,
          `${fact.state || "UNKNOWN"} · ${(fact.reasons || []).join("、") || "无原因码"}${fact.failure_line_candidate?.value ? ` · 研究失效线 ${fact.failure_line_candidate.value.numerator}/${fact.failure_line_candidate.value.denominator} · ${fact.failure_line_candidate.source} · NON_PIT` : " · 无合法失效线"}`,
        ],
      ),
      [
        "后续状态",
        `${data.t0_observation?.followup_status || "NOT_COMPUTED"}；正式 Focus Episode、D2、Risk 与 Priority 未计算。`,
      ],
    ]);
    const stockEvidence = data.stored_research_evidence || {};
    const stockFollowupControls = sourceCandidateFollowupControls(
      stockEvidence.rules?.facts,
      stockEvidence.rules?.scope,
      symbol,
      data.t0_observation?.source_capture_id,
    );
    if (stockFollowupControls)
      $("focusStockResult").append(stockFollowupControls);
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
    $("cohortEnrollmentSummary").replaceChildren();
    message(
      "cohortEnrollmentStatus",
      "所选范围已改变，旧登记结果已清除；请重新读取。",
    );
    cohortRpsPulseData = null;
    cohortRpsPulseRequested = false;
    renderCohortRpsPulse(null);
    $("cohortDateComparison").replaceChildren();
    $("cohortSymbolForward").replaceChildren();
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
$("readCohortRpsPulse").addEventListener("click", () => {
  cohortRpsPulseRequested = true;
  void readCohort(true);
});
$("cohortRpsBoardKind").addEventListener("change", () => {
  cohortOffset = 0;
  $("cohortRpsGroup").value = "ALL";
  void readCohort();
});
$("cohortRpsRuleState").addEventListener("change", () => {
  renderCohortRpsPulse(cohortRpsPulseData);
});
$("cohortRpsGroup").addEventListener("change", () => {
  renderCohortRpsPulse(cohortRpsPulseData);
});
