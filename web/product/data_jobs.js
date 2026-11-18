const STATUS_LABELS = {
  AVAILABLE: "可查看",
  BLOCKED_NOT_AUTHORIZED: "受限：未获行情执行授权",
  BLOCKED_SOURCE_NOT_AVAILABLE: "受限：来源不可用",
  BLOCKED_BAOSTOCK_REQUEST_NOT_ENABLED: "BaoStock请求未启用；其它来源继续",
  RESET_SOURCE_PREFLIGHT_REQUIRED: "清理前来源核验待执行",
  RESET_PREVIEW_STALE: "清理范围已变化，请重新预览",
  BLOCKED_HISTORY_INSUFFICIENT: "受限：历史会话不足",
  HISTORY_INSUFFICIENT: "历史会话不足",
  READY: "可执行",
  READY_SOURCE_CHECK_REQUIRED: "等待开始后核验来源",
  READY_WITH_INCOMPLETE_UNITS: "部分输出受限",
  RUNNING: "运行中",
  COMPLETED: "完成",
  ALREADY_COMPLETE: "同输入已完成",
  NO_CHANGE: "无变化",
  CLEARED: "已清理",
  NOT_COMPUTED: "未计算",
  NOT_DUE: "未到期",
  INCOMPLETE: "不完整",
  BLOCKED: "受限",
  FAILED: "失败",
  INTERRUPTED: "中断",
  NOT_STARTED: "未开始",
  SKIPPED_BY_USER: "用户跳过",
  SKIPPED_NOT_IMPLEMENTED: "生产者未实现",
};

const STAGE_LABELS = {
  BUSINESS_RESET: "清理业务事实",
  SOURCE_RAW: "来源与 Raw",
  SOURCE_BAOSTOCK: "BaoStock补充来源",
  SOURCE_SECTOR_MEMBERS: "行业 / GN成员来源",
  CORE_PROFILE: "Core / Profile",
  STATIC_RESEARCH: "静态研究",
  D0_D1: "D0 / D1",
  MARKET_BREADTH: "市场宽度",
  MARKET_PARTICIPATION: "成交参与",
  FOCUS_FORMAL: "Focus正式准入",
  SECTOR_PULSE: "行业 / 概念 PULSE",
  SECTOR_TRANSITION: "行业 / 概念相邻变化",
  SECTOR_FOLLOWUP: "板块跨日跟踪",
};

const LAYER_LABELS = {
  SOURCE: "第一层 · 基础来源",
  BASE: "第二层 · 基础画像与指标",
  RESEARCH: "第三层 · 研究算法",
};

const CLOSED_UNIT_STATUSES = [
  "COMPLETED",
  "ALREADY_COMPLETE",
  "NO_CHANGE",
  "CLEARED",
  "SKIPPED_BY_USER",
  "SKIPPED_NOT_IMPLEMENTED",
];

function byId(id) {
  return document.getElementById(id);
}

function element(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined && text !== null) item.textContent = String(text);
  if (className) item.className = className;
  return item;
}

function statusLabel(status) {
  return STATUS_LABELS[status] || status || "未知";
}

function count(value) {
  return value === null || value === undefined
    ? "未知"
    : Number(value).toLocaleString("zh-CN");
}

function plainValue(value) {
  if (value === null || value === undefined || value === "") return "未知";
  if (typeof value === "string" || typeof value === "number") {
    return String(value);
  }
  if (Array.isArray(value)) return value.map(plainValue).join("、");
  return JSON.stringify(value);
}

function dependencySummary(dependency) {
  if (typeof dependency === "string") return `依赖项：${dependency}`;
  if (!dependency || typeof dependency !== "object") {
    return `依赖项：${plainValue(dependency)}`;
  }
  const table =
    dependency.table ||
    dependency.business_table ||
    dependency.table_name ||
    dependency.relation_table;
  const relation =
    dependency.relation ||
    dependency.relation_type ||
    dependency.dependency_type;
  const relationLabels = {
    FK: "外键依赖",
    FOREIGN_KEY: "外键依赖",
    DOWNSTREAM: "下游派生依赖",
    DERIVED_FROM: "派生来源依赖",
    READER: "读取依赖",
    API: "接口读取依赖",
    SQL: "SQL读取依赖",
  };
  const reason =
    dependency.reason ||
    dependency.reason_text ||
    dependency.reason_code ||
    dependency.description;
  const parts = [];
  if (table) parts.push(`业务表：${plainValue(table)}`);
  if (relation) {
    const relationText = plainValue(relation);
    parts.push(
      `依赖关系：${relationLabels[relationText.toUpperCase()] || relationText}`,
    );
  }
  if (reason) parts.push(`原因：${plainValue(reason)}`);
  return parts.join("；") || "依赖关系：未提供详细信息";
}

function renderProgress(root, job) {
  root.replaceChildren();
  if (!job) {
    root.append(
      element(
        "p",
        "PostgreSQL中还没有数据任务记录。受限预检不会创建任务。",
        "muted",
      ),
    );
    return;
  }
  const units = Array.isArray(job.progress) ? job.progress : [];
  const completedUnits = units.length
    ? units.filter((unit) => CLOSED_UNIT_STATUSES.includes(unit.status)).length
    : job.completed_units;
  const plannedUnits = job.planned_units ?? (units.length || null);
  const workflowMode =
    job.daily_mode ||
    job.initialization_mode ||
    job.request?.daily_mode ||
    job.request?.initialization_mode;
  const workflowLabel =
    workflowMode === "FULL_THREE_LAYERS"
      ? "完整三层日更"
      : workflowMode === "SOURCE_ONLY"
        ? "仅第一层来源"
        : workflowMode === "RESET_ALL_BUSINESS_FACTS"
          ? "清空业务事实后重初始化"
          : workflowMode === "REUSE_RAW"
            ? "复用Raw补算派生"
            : workflowMode || "";
  const heading = element("div", null, "job-progress-heading");
  heading.append(
    element(
      "div",
      (job.mode === "INITIALIZE" ? "数据初始化" : "数据日更") +
        (workflowLabel ? ` · ${workflowLabel}` : "") +
        " · " +
        (job.target_session || "目标会话未知"),
      "job-progress-title",
    ),
    element(
      "p",
      statusLabel(job.status) +
        " · 已闭合单元 " +
        count(completedUnits) +
        "/" +
        count(plannedUnits) +
        "（" +
        (plannedUnits && completedUnits !== null && completedUnits !== undefined
          ? Math.floor((completedUnits / plannedUnits) * 100) + "%"
          : "未知") +
        "）",
      "job-progress-summary",
    ),
    element(
      "p",
      "开始 " +
        (job.started_at || "未开始") +
        " · 更新 " +
        (job.updated_at || "未知") +
        " · 结束 " +
        (job.completed_at || "运行中"),
      "muted",
    ),
  );
  root.append(heading);
  if (String(job.request_key || "").startsWith("SYNTHETIC_ORACLE")) {
    root.append(
      element(
        "p",
        "合成Oracle测试任务：只展示控制表与截止内已存行数，不代表真实初始化或日更成功。",
        "job-synthetic-note",
      ),
    );
  }
  if (job.last_error_code || job.last_error_message) {
    root.append(
      element(
        "p",
        (job.last_error_code || "任务异常") +
          "：" +
          (job.last_error_message || "原因未知"),
        "job-error",
      ),
    );
  }
  if (!units.length) {
    root.append(element("p", "此任务没有阶段与业务表单元。", "muted"));
    return;
  }
  const groups = new Map();
  for (const unit of units) {
    if (!groups.has(unit.stage_code)) groups.set(unit.stage_code, []);
    groups.get(unit.stage_code).push(unit);
  }
  for (const [stage, rows] of groups) {
    const done = rows.filter((row) =>
      CLOSED_UNIT_STATUSES.includes(row.status),
    ).length;
    const details = document.createElement("details");
    details.className = "job-stage";
    details.open = rows.some((row) =>
      [
        "RUNNING",
        "FAILED",
        "INTERRUPTED",
        "INCOMPLETE",
        "BLOCKED",
        "NOT_COMPUTED",
      ].includes(row.status),
    );
    const summaryNode = document.createElement("summary");
    summaryNode.textContent =
      (STAGE_LABELS[stage] || stage) + " · 已闭合 " + done + "/" + rows.length;
    details.append(summaryNode);
    const tableWrap = element("div", null, "job-table-scroll");
    const table = document.createElement("table");
    table.className = "job-progress-table";
    const header = document.createElement("tr");
    for (const title of [
      "业务表",
      "计划会话",
      "已核验会话",
      "预期行",
      "PG读回行",
      "新增 / 变更",
      "删除行",
      "质量",
      "状态 / 原因",
    ]) {
      header.append(element("th", title));
    }
    const thead = document.createElement("thead");
    thead.append(header);
    table.append(thead);
    const tbody = document.createElement("tbody");
    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.append(
        element("td", String(row.business_table || "")),
        element("td", row.target_session || "未知"),
        element(
          "td",
          row.verified_at &&
            ["COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE", "CLEARED"].includes(
              row.status,
            )
            ? row.target_session
            : "—",
        ),
        element("td", count(row.expected_rows)),
        element("td", count(row.verified_rows)),
        element(
          "td",
          count(row.inserted_rows) + " / " + count(row.updated_rows),
        ),
        element("td", count(row.deleted_rows)),
        element("td", row.quality_status || "未知"),
        element(
          "td",
          statusLabel(row.status) +
            (row.error_code ? " · " + row.error_code : "") +
            (row.error_message ? "：" + row.error_message : ""),
        ),
      );
      tbody.append(tr);
    }
    table.append(tbody);
    tableWrap.append(table);
    details.append(tableWrap);
    root.append(details);
  }
}

function renderRecent(root, jobs) {
  root.replaceChildren();
  if (!Array.isArray(jobs) || !jobs.length) {
    root.append(element("p", "暂无已提交任务记录。", "muted"));
    return;
  }
  const table = document.createElement("table");
  table.className = "job-recent-table";
  const head = document.createElement("tr");
  for (const title of [
    "模式",
    "目标会话",
    "输入范围",
    "状态",
    "更新时间",
    "错误",
  ]) {
    head.append(element("th", title));
  }
  const thead = document.createElement("thead");
  thead.append(head);
  table.append(thead);
  const tbody = document.createElement("tbody");
  for (const job of jobs) {
    const windowLabel =
      "Raw " +
      job.history_session_count +
      " · D0 " +
      job.d0_output_session_count +
      " · D1 " +
      job.d1_output_session_count;
    const tr = document.createElement("tr");
    tr.append(
      element(
        "td",
        job.mode === "INITIALIZE"
          ? job.initialization_mode === "RESET_ALL_BUSINESS_FACTS"
            ? "重置后初始化"
            : "复用Raw初始化"
          : job.daily_mode || "手动日更",
      ),
      element("td", job.target_session || "未知"),
      element("td", windowLabel),
      element("td", statusLabel(job.status)),
      element("td", job.updated_at || "未知"),
      element(
        "td",
        [job.last_error_code, job.last_error_message]
          .filter(Boolean)
          .join("：") || "无",
      ),
    );
    tbody.append(tr);
  }
  table.append(tbody);
  root.append(table);
}

function currentPageBody(page) {
  const modules = selectedModules(page);
  const moduleOutputSessionCounts = Object.fromEntries(
    modules
      .filter((item) => item.outputSessionCount !== null)
      .map((item) => [item.code, item.outputSessionCount]),
  );
  const countFor = (codes) =>
    modules.find((item) => codes.includes(item.code))?.outputSessionCount || 0;
  if (page === "initialize") {
    return {
      mode: "INITIALIZE",
      initialization_mode: byId("dataInitMode").value,
      target_session: byId("dataInitTarget").value || undefined,
      history_session_count: Number(byId("dataInitHistory").value),
      profile_history_session_count:
        Number(byId("dataInitProfileHistory").value) || null,
      source_modules: modules
        .filter((item) => item.layer === "SOURCE")
        .map((item) => item.code),
      base_modules: modules
        .filter((item) => item.layer === "BASE")
        .map((item) => item.code),
      research_modules: modules
        .filter((item) => item.layer === "RESEARCH")
        .map((item) => item.code),
      module_output_session_counts: moduleOutputSessionCounts,
      d0_output_session_count: countFor(["D0", "SOURCE_D0", "D0_RESEARCH"]),
      d1_output_session_count: countFor(["D1", "SOURCE_D1", "D1_RESEARCH"]),
      operation: byId("dataInitOperation").value,
    };
  }
  return {
    mode: "MANUAL_DAILY",
    daily_mode: byId("dataDailyMode").value,
    source_modules: modules
      .filter((item) => item.layer === "SOURCE")
      .map((item) => item.code),
    base_modules: modules
      .filter((item) => item.layer === "BASE")
      .map((item) => item.code),
    research_modules: modules
      .filter((item) => item.layer === "RESEARCH")
      .map((item) => item.code),
    output_session_count:
      modules.find((item) => item.outputSessionCount !== null)
        ?.outputSessionCount || 1,
    module_output_session_counts: Object.fromEntries(
      modules
        .filter((item) => item.outputSessionCount !== null)
        .map((item) => [item.code, item.outputSessionCount]),
    ),
    operation: byId("dataDailyOperation").value,
  };
}

function selectedModules(page) {
  const root = byId(
    page === "initialize" ? "dataInitModules" : "dataDailyModules",
  );
  return [...root.querySelectorAll("[data-module-code]:checked")].map(
    (input) => ({
      code: input.dataset.moduleCode,
      layer: input.dataset.moduleLayer,
      cleanupAllowed: input.dataset.cleanupAllowed === "true",
      outputSessionCount: input.dataset.outputCountId
        ? Number(byId(input.dataset.outputCountId).value)
        : null,
    }),
  );
}

export function mountDataJobPages(api) {
  const state = {
    page: null,
    plan: null,
    latestJobId: null,
    timer: null,
    loading: false,
    planBody: null,
    resetPreviewBody: null,
    resetResumeFingerprint: null,
    resetDialogOpen: false,
    startPending: false,
    profileHistoryTouched: false,
  };

  function normalizeModules(capabilities) {
    const raw = capabilities.modules ||
      capabilities.available_modules ||
      capabilities.module_options || [
        ...(capabilities.source_modules || []).map((item) => ({
          layer: "SOURCE",
          ...item,
        })),
        ...(capabilities.base_modules || []).map((item) => ({
          layer: "BASE",
          ...item,
        })),
        ...(capabilities.research_modules || []).map((item) => ({
          layer: "RESEARCH",
          ...item,
        })),
      ];
    if (Array.isArray(raw)) return raw;
    if (raw && typeof raw === "object") {
      return Object.entries(raw).flatMap(([layer, entries]) =>
        Array.isArray(entries)
          ? entries.map((entry) => ({ layer, ...entry }))
          : [],
      );
    }
    return [];
  }

  function renderModulePicker(page, capabilities) {
    const root = byId(
      page === "initialize" ? "dataInitModules" : "dataDailyModules",
    );
    const previous = new Map(
      [...root.querySelectorAll("[data-module-code]")].map((input) => [
        input.dataset.moduleCode,
        input.checked,
      ]),
    );
    const previousCounts = new Map(
      [...root.querySelectorAll("[data-output-for]")].map((input) => [
        input.dataset.outputFor,
        input.value,
      ]),
    );
    const modules = normalizeModules(capabilities).filter((item) => {
      const layer = String(item.layer || item.layer_code || "").toUpperCase();
      return Object.hasOwn(LAYER_LABELS, layer);
    });
    root.replaceChildren();
    if (!modules.length) {
      root.append(
        element(
          "p",
          "服务尚未返回可执行模块清单；当前不允许按猜测启动生产者。",
          "job-error",
        ),
      );
      return;
    }
    for (const layer of ["SOURCE", "BASE", "RESEARCH"]) {
      const rows = modules.filter(
        (item) =>
          String(item.layer || item.layer_code || "").toUpperCase() === layer,
      );
      if (!rows.length) continue;
      const group = document.createElement("fieldset");
      group.className = "job-module-group";
      const legend = element("legend", LAYER_LABELS[layer]);
      group.append(legend);
      const options = element("div", null, "job-module-options");
      for (const item of rows) {
        const code = String(item.code || item.module_code || "");
        if (!code) continue;
        const implemented = item.implemented !== false;
        const available = item.available !== false && implemented;
        const wrapper = element("div", null, "job-module-option");
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.dataset.moduleCode = code;
        checkbox.dataset.moduleLayer = layer;
        checkbox.dataset.moduleAvailable = String(available);
        checkbox.dataset.cleanupAllowed = String(item.cleanup_allowed === true);
        checkbox.dataset.cleanupReason = item.cleanup_reason || "";
        checkbox.disabled = !available;
        const defaultSelected =
          page === "daily"
            ? byId("dataDailyMode").value === "SOURCE_ONLY"
              ? layer === "SOURCE"
              : true
            : Boolean(item.default_selected);
        checkbox.checked = previous.has(code)
          ? previous.get(code)
          : defaultSelected && available;
        const moduleLabel = element("label", null, "job-module-label");
        moduleLabel.append(
          checkbox,
          element("span", item.label || item.title || code),
        );
        wrapper.append(moduleLabel);
        if (!available) {
          const reason = item.reason || item.status || "当前不可执行";
          checkbox.dataset.unavailableReason = reason;
          const note = element("small", reason, "job-module-unavailable");
          note.dataset.moduleReason = "true";
          wrapper.append(note);
        } else if (layer !== "SOURCE") {
          const countId = `dataJobCount_${page}_${code.replace(/[^A-Za-z0-9_-]/g, "_")}`;
          const countLabel = element("span", "最近输出");
          const countInput = document.createElement("input");
          countInput.type = "number";
          countInput.min = "1";
          countInput.max = "32767";
          countInput.step = "1";
          countInput.value =
            previousCounts.get(code) || (page === "daily" ? "1" : "5");
          countInput.id = countId;
          countInput.setAttribute(
            "aria-label",
            `${item.label || code}输出会话数`,
          );
          countInput.dataset.outputFor = code;
          countInput.disabled = !checkbox.checked;
          checkbox.dataset.outputCountId = countId;
          checkbox.addEventListener("change", () => {
            countInput.disabled = !checkbox.checked;
            invalidateStart(page);
          });
          countInput.addEventListener("input", () => invalidateStart(page));
          wrapper.append(countLabel, countInput, element("span", "个官方会话"));
        }
        checkbox.addEventListener("change", () => {
          if (page === "daily") byId("dataDailyMode").value = "CUSTOM";
          invalidateStart(page);
        });
        options.append(wrapper);
      }
      group.append(options);
      root.append(group);
    }
    updateCleanupAvailability(page);
  }

  function applyDailyPreset() {
    const sourceOnly = byId("dataDailyMode").value === "SOURCE_ONLY";
    if (byId("dataDailyMode").value === "CUSTOM") {
      invalidateStart("daily");
      return;
    }
    for (const checkbox of byId("dataDailyModules").querySelectorAll(
      "[data-module-code]",
    )) {
      const isSource = checkbox.dataset.moduleLayer === "SOURCE";
      const available = checkbox.dataset.moduleAvailable === "true";
      checkbox.checked = available && (sourceOnly ? isSource : true);
      if (checkbox.dataset.outputCountId) {
        byId(checkbox.dataset.outputCountId).disabled = !checkbox.checked;
      }
    }
    byId("dataDailyOperation").value = "FILL_MISSING";
    invalidateStart("daily");
  }

  function updateInitializationMode() {
    const reset = byId("dataInitMode").value === "RESET_ALL_BUSINESS_FACTS";
    if (
      reset &&
      ["CLEAR_DERIVED", "CLEAR_AND_REGENERATE"].includes(
        byId("dataInitOperation").value,
      )
    ) {
      byId("dataInitOperation").value = "FILL_MISSING";
    }
    if (reset) {
      const modules = byId("dataInitModules");
      for (const moduleCode of ["TDX_RAW", "TDX_SECTOR_MEMBERS"]) {
        const checkbox = modules.querySelector(
          `[data-module-code="${moduleCode}"]`,
        );
        if (checkbox) checkbox.checked = true;
      }
    }
    byId("dataInitResetNotice").hidden = !reset;
    byId("dataInitOperationWrap").hidden = reset;
    byId("dataInitOperation").disabled = reset;
    updateCleanupAvailability("initialize");
    invalidateStart("initialize");
  }

  function updateCleanupAvailability(page) {
    const root = byId(
      page === "initialize" ? "dataInitModules" : "dataDailyModules",
    );
    const operation = byId(
      page === "initialize" ? "dataInitOperation" : "dataDailyOperation",
    ).value;
    const cleanupRequested = ["CLEAR_DERIVED", "CLEAR_AND_REGENERATE"].includes(
      operation,
    );
    for (const checkbox of root.querySelectorAll("[data-module-code]")) {
      const wrapper = checkbox.closest(".job-module-option");
      const available = checkbox.dataset.moduleAvailable === "true";
      const sourceLayer = checkbox.dataset.moduleLayer === "SOURCE";
      const cleanupAllowed = checkbox.dataset.cleanupAllowed === "true";
      const canClean = !sourceLayer && cleanupAllowed;
      const allowed = available && (!cleanupRequested || canClean);
      checkbox.disabled = !allowed;
      if (!allowed && checkbox.checked) checkbox.checked = false;
      if (checkbox.dataset.outputCountId) {
        const outputCount = byId(checkbox.dataset.outputCountId);
        outputCount.disabled = !checkbox.checked;
      }

      let reason = wrapper.querySelector("[data-module-reason]");
      if (cleanupRequested && !canClean) {
        if (!reason) {
          reason = element("small", null, "job-module-unavailable");
          reason.dataset.moduleReason = "true";
          wrapper.append(reason);
        }
        const configuredReason = checkbox.dataset.cleanupReason;
        const cleanupReason = sourceLayer
          ? "基础来源为保留层，不允许通过派生清理操作删除。"
          : /[\u3400-\u9fff]/.test(configuredReason)
            ? configuredReason
            : `该模块未声明受控派生清理能力，不能清理。${configuredReason ? `（原因码：${configuredReason}）` : ""}`;
        reason.textContent = available
          ? cleanupReason
          : `模块当前不可执行；${cleanupReason}`;
      } else if (reason && checkbox.dataset.moduleAvailable === "false") {
        reason.textContent =
          checkbox.dataset.unavailableReason || "当前不可执行";
      } else if (reason) {
        reason.remove();
      }
    }
  }

  function invalidateStart(page) {
    byId(page === "initialize" ? "startDataInit" : "startDataDaily").disabled =
      true;
    state.planBody = null;
    if (page === "initialize") {
      state.resetResumeFingerprint = null;
      state.resetPreviewBody = null;
    }
  }

  function requestBodyForPage(page) {
    const body = currentPageBody(page);
    if (page === "initialize" && state.resetResumeFingerprint) {
      return {
        ...body,
        reset_preview_fingerprint: state.resetResumeFingerprint,
      };
    }
    return body;
  }

  function populateTargets(capabilities) {
    const select = byId("dataInitTarget");
    const cutoff = capabilities.research_cutoff || "9999-12-31";
    const sessions = (
      capabilities.available_target_sessions ||
      capabilities.official_calendar_sessions ||
      capabilities.raw_coverage?.closed_sessions ||
      []
    ).filter((session) => String(session) <= cutoff);
    const previous = select.value;
    select.replaceChildren();
    if (!sessions.length) {
      select.append(element("option", "没有已存官方会话", ""));
      select.disabled = true;
      return;
    }
    select.disabled = false;
    for (const session of sessions) {
      const option = element("option", session);
      option.value = session;
      select.append(option);
    }
    select.value = sessions.includes(previous)
      ? previous
      : sessions[sessions.length - 1];
  }

  function renderCapabilities(capabilities) {
    const raw = capabilities.raw_coverage || {};
    const summary =
      "PG闭合Raw " +
      count(raw.stored_session_count) +
      " 个官方会话 · " +
      (raw.first_stored_session || "未知") +
      " 至 " +
      (raw.last_stored_session || "未知") +
      " · 研究截止 " +
      capabilities.research_cutoff;
    for (const id of ["dataInitCoverage", "dataDailyCoverage"]) {
      byId(id).textContent = summary;
    }
    const completedSession =
      raw.last_completed_research_session ||
      raw.latest_complete_profile_session;
    byId("dataDailyCoverage").textContent =
      "最近完成研究会话 " +
      (completedSession || "未知") +
      " · 来源包 " +
      (raw.last_completed_source_package_id || "未知") +
      " · 外部来源最新会话未核实（当前冻结） · " +
      summary;
    const gate = capabilities.execution_authorized
      ? "本机运行许可已启用；开始后仍会核验来源、会话窗口与PG覆盖。"
      : "当前未授权真实行情执行。预检只读PG与官方日历，不请求来源、不写行情；初始化和新日日更无法启动。";
    for (const id of ["dataInitGate", "dataDailyGate"]) {
      byId(id).textContent = gate;
      byId(id).classList.toggle(
        "job-gate-blocked",
        !capabilities.execution_authorized,
      );
    }
    byId("dataInitStatus").textContent = capabilities.execution_authorized
      ? "待预检"
      : "受限";
    byId("dataDailyStatus").textContent = capabilities.execution_authorized
      ? "未启动"
      : "受限";
    populateTargets(capabilities);
    if (!state.profileHistoryTouched) {
      const profileWindow =
        capabilities.current_profile_history_session_count ??
        capabilities.core_max_actual_lookback;
      const normalizedProfileWindow = Number(profileWindow);
      byId("dataInitProfileHistory").value =
        Number.isInteger(normalizedProfileWindow) && normalizedProfileWindow > 0
          ? String(normalizedProfileWindow)
          : "";
    }
    renderModulePicker("initialize", capabilities);
    renderModulePicker("daily", capabilities);
    renderRecent(byId("dataInitRecent"), capabilities.recent_jobs);
    renderRecent(byId("dataDailyRecent"), capabilities.recent_jobs);
  }

  function renderBusinessResetPreview(root, preview, body) {
    if (!preview) return;
    root.replaceChildren();
    const title = element(
      "h3",
      preview.resume_existing_reset
        ? "恢复既有重初始化任务"
        : "业务事实清理与重初始化预览",
    );
    root.append(title);
    const database =
      preview.database_name ||
      preview.database_identity ||
      preview.database ||
      "未提供";
    const schema = preview.schema || preview.schema_name || "未提供";
    const targets =
      preview.target_sessions ||
      (preview.target_session ? [preview.target_session] : []);
    const targetRange =
      Array.isArray(targets) && targets.length
        ? `${targets.length}个官方会话（${targets[0]} 至 ${targets[targets.length - 1]}）`
        : "未提供";
    const sourceModules = Array.isArray(body.source_modules)
      ? body.source_modules
      : [];
    const rawCount = body.history_session_count;
    const outputs = Object.entries(body.module_output_session_counts || {}).map(
      ([module, sessions]) => `${module}：${sessions}个输出会话`,
    );
    const scope = document.createElement("dl");
    scope.className = "job-cleanup-preview";
    for (const [label, value] of [
      ["PG数据库", database],
      ["业务schema", schema],
      ["目标官方会话", targetRange],
      ["基础Raw目标", `${rawCount || "未提供"}个官方会话`],
      [
        "Core/Profile历史输入窗口",
        body.profile_history_session_count
          ? `${body.profile_history_session_count}个官方会话`
          : "未提供当前算法窗口",
      ],
      ["来源选择", sourceModules.join("、") || "未选择"],
      ["画像与算法输出", outputs.join("；") || "未选择派生模块"],
      ["范围预检", preview.complete === true ? "完整" : "未完成"],
      [
        "必需来源可用性",
        preview.source_ready === true || preview.sources_ready === true
          ? "已确认可用"
          : preview.source_preflight_required_before_delete === true
            ? preview.resume_existing_reset
              ? "恢复前逐项核验；清理步骤已提交"
              : "任务启动后逐项核验；未通过则保留现有事实"
            : "未证明可重建",
      ],
    ]) {
      scope.append(element("dt", label), element("dd", value));
    }
    root.append(scope);

    const tableRows = Array.isArray(preview.affected_tables)
      ? preview.affected_tables
      : [];
    root.append(
      element(
        "h3",
        preview.resume_existing_reset
          ? "当前已重建表与PG读回行数"
          : "将清除的业务表与预计行数",
      ),
    );
    if (!tableRows.length) {
      root.append(
        element(
          "p",
          "后端尚未返回精确业务表清单；在清单完整前不能确认或清理。",
          "job-error",
        ),
      );
    } else {
      const table = document.createElement("table");
      const head = document.createElement("tr");
      head.append(element("th", "业务表"), element("th", "预计行数"));
      const thead = document.createElement("thead");
      thead.append(head);
      table.append(thead);
      const tbody = document.createElement("tbody");
      for (const row of tableRows) {
        const name =
          typeof row === "string"
            ? row
            : row.table || row.table_name || row.business_table || "未知";
        const rows =
          typeof row === "string" ? null : (row.row_count ?? row.rows);
        const tr = document.createElement("tr");
        tr.append(element("td", name), element("td", count(rows)));
        tbody.append(tr);
      }
      table.append(tbody);
      root.append(table);
    }
    root.append(
      element("p", "预计清理总行数：" + count(preview.affected_rows)),
    );
    const retained =
      preview.retained_objects ||
      preview.retained_identity_migration_config ||
      preview.preserved_objects ||
      preview.retained ||
      [];
    const retainedItems = Array.isArray(retained)
      ? retained.map(plainValue)
      : Object.entries(retained).map(
          ([name, value]) => `${name}：${plainValue(value)}`,
        );
    root.append(
      element("h3", "将保留的运行基础"),
      element(
        "p",
        retainedItems.join("；") ||
          "后端尚未明确身份、迁移记录及运行配置保留范围。",
        retainedItems.length ? "" : "job-error",
      ),
    );
    if (preview.blocked_reasons?.length) {
      root.append(
        element(
          "p",
          "预检阻断：" + preview.blocked_reasons.join("、"),
          "job-error",
        ),
      );
    }
    if (preview.irreversible_warning) {
      root.append(element("p", preview.irreversible_warning, "job-error"));
    } else {
      root.append(
        element(
          "p",
          preview.resume_existing_reset
            ? "原清理事务已经提交；本次只验证来源并从已完成PG单元继续，不会再次清空。"
            : "此操作会先清除已列出的业务事实，清除结果无法直接撤销；只有预检完整且所选必需来源可以合法重建时才会继续。",
          "job-error",
        ),
      );
    }
  }

  function resetPreviewReady(preview) {
    if (
      preview?.complete !== true ||
      (preview.source_ready !== true &&
        preview.sources_ready !== true &&
        preview.source_preflight_required_before_delete !== true) ||
      typeof preview.reset_preview_fingerprint !== "string" ||
      preview.reset_preview_fingerprint.length === 0 ||
      !Number.isFinite(Number(preview.affected_rows))
    ) {
      return false;
    }
    const tables = preview.affected_tables;
    const retained =
      preview.retained_objects ||
      preview.retained_identity_migration_config ||
      preview.preserved_objects ||
      preview.retained;
    const retainedPresent = Array.isArray(retained)
      ? retained.length > 0
      : retained && typeof retained === "object"
        ? Object.keys(retained).length > 0
        : Boolean(retained);
    return (
      Array.isArray(tables) &&
      tables.length > 0 &&
      tables.every((row) => {
        const name =
          typeof row === "string"
            ? row
            : row?.table || row?.table_name || row?.business_table;
        const rowCount =
          typeof row === "string" ? null : (row?.row_count ?? row?.rows);
        return (
          Boolean(name) &&
          rowCount !== null &&
          rowCount !== undefined &&
          Number.isFinite(Number(rowCount))
        );
      }) &&
      retainedPresent
    );
  }

  function renderPlan(plan, page) {
    state.plan = plan;
    state.planBody = JSON.stringify(requestBodyForPage(page));
    const root = byId(page === "initialize" ? "dataInitPlan" : "dataDailyPlan");
    root.replaceChildren();
    byId(
      page === "initialize" ? "dataInitStatus" : "dataDailyStatus",
    ).textContent = statusLabel(plan.status);
    if (plan.baostock_request_status) {
      const request = plan.baostock_request_status;
      const budget = plan.baostock_request_budget;
      const budgetText = budget
        ? `登录批次需要 ${budget.required_login_batches}，当前允许 ${budget.allowed_login_batches}，代码会话数 ${budget.symbol_session_units}。`
        : "尚无可用的现存PG Raw批次预算。";
      root.append(
        element(
          "p",
          `BaoStock来源可用性：${request.source_availability}；请求策略：${request.request_policy}；本次采集授权：${request.execution_authorized ? "允许" : "未授权"}。${budgetText}未联网检查来源状态，也未发送网络请求。`,
          "muted",
        ),
      );
    }
    root.append(
      element(
        "p",
        "计划状态：" +
          statusLabel(plan.status) +
          (plan.target_session ? " · 目标会话 " + plan.target_session : ""),
        plan.executable ? "job-plan-ready" : "job-plan-blocked",
      ),
    );
    if (page === "daily") {
      byId("startDataDaily").textContent = plan.resume_existing_job
        ? "恢复未完成日更"
        : "手动开始日更";
    } else {
      const resetPreview = plan.reset_preview || plan.business_reset_preview;
      byId("startDataInit").textContent =
        currentPageBody(page).initialization_mode ===
          "RESET_ALL_BUSINESS_FACTS" &&
        !plan.executable &&
        resetPreviewReady(resetPreview)
          ? "查看清理确认"
          : "开始数据初始化";
    }
    if (
      page === "initialize" &&
      currentPageBody(page).initialization_mode === "RESET_ALL_BUSINESS_FACTS"
    ) {
      const preview = plan.reset_preview || plan.business_reset_preview;
      if (preview) {
        byId("startDataInit").textContent =
          plan.resume_existing_job || preview.resume_existing_reset
            ? "恢复未完成重初始化"
            : !plan.executable && resetPreviewReady(preview)
              ? "查看清理确认"
              : "清空后重初始化";
        const previewRoot = element("section", null, "job-reset-summary");
        renderBusinessResetPreview(previewRoot, preview, currentPageBody(page));
        root.append(previewRoot);
      } else {
        root.append(
          element(
            "p",
            "后端尚未返回全库重初始化预览；未提供完整表/行数和来源可用性时不能启动。",
            "job-error",
          ),
        );
      }
    }
    if (plan.resume_existing_job) {
      root.append(
        element(
          "p",
          "检测到未完成任务，将沿用原任务输入并从已提交读回的单元继续。",
          "muted",
        ),
      );
    }
    if (plan.raw_history_session_count_available !== undefined) {
      root.append(
        element(
          "p",
          "基础Raw窗口 " +
            plan.raw_history_session_count_available +
            " 个本次计算输入官方会话；精确可复用同包已闭合 " +
            plan.raw_history_sessions_in_pg +
            " 个，未在PG中找到 " +
            (plan.missing_raw_sessions || []).length +
            " 个；截止内PG总Raw会话 " +
            (plan.source_coverage?.pg_closed_raw_session_count ?? "未知") +
            " 个；当前画像来源包 " +
            (plan.source_coverage?.active_pg_source_package_id ||
              "未唯一绑定") +
            "。来源覆盖：" +
            (plan.source_coverage?.status || "未知") +
            "。",
        ),
      );
    }
    if (Number.isInteger(currentPageBody(page).profile_history_session_count)) {
      root.append(
        element(
          "p",
          "Core/Profile每个输出日分别使用 " +
            currentPageBody(page).profile_history_session_count +
            " 个官方历史会话；该窗口独立于Raw总量和输出保存范围。",
          "muted",
        ),
      );
    }
    if (plan.market_breadth_sessions?.length) {
      root.append(
        element(
          "p",
          "市场宽度计划覆盖 " +
            plan.market_breadth_sessions.length +
            " 个会话（" +
            plan.market_breadth_sessions[0] +
            " 至 " +
            plan.market_breadth_sessions[
              plan.market_breadth_sessions.length - 1
            ] +
            "）；成交参与计划覆盖 " +
            (plan.market_participation_sessions || []).length +
            " 个已选Profile输出日。",
          "muted",
        ),
      );
    }
    const selectedResearchModules = new Set(
      (currentPageBody(page).research_modules || []).map((item) =>
        String(item).toUpperCase(),
      ),
    );
    if (
      plan.d1_calculation_session_count &&
      (selectedResearchModules.has("D1") || plan.legacy_request === true)
    ) {
      root.append(
        element(
          "p",
          "D1算法窗口沿用PG中唯一已存合同 " +
            plan.d1_calculation_session_count +
            " 个会话；此值与D1输出保存范围分开。",
        ),
      );
    } else if (
      plan.d1_output_session_count > 0 &&
      (selectedResearchModules.has("D1") || plan.legacy_request === true)
    ) {
      root.append(element("p", "没有唯一已存D1算法窗口；D1输出保留为未计算。"));
    }
    const outputs = [];
    if (plan.d0_output_sessions?.length) {
      outputs.push(
        "D0：" +
          plan.d0_output_sessions[0] +
          " 至 " +
          plan.d0_output_sessions[plan.d0_output_sessions.length - 1] +
          "（" +
          plan.d0_output_sessions.length +
          "日）",
      );
    }
    if (plan.d1_output_sessions?.length) {
      outputs.push(
        "D1：" +
          plan.d1_output_sessions[0] +
          " 至 " +
          plan.d1_output_sessions[plan.d1_output_sessions.length - 1] +
          "（" +
          plan.d1_output_sessions.length +
          "日）",
      );
    }
    if (outputs.length) root.append(element("p", outputs.join("；")));
    const moduleOutputSessions = plan.module_output_sessions || {};
    const moduleTargets = selectedModules(page).map((item) => {
      const sessions = moduleOutputSessions[item.code];
      const range =
        Array.isArray(sessions) && sessions.length
          ? `${sessions[0]} 至 ${sessions[sessions.length - 1]}（${sessions.length}个官方会话）`
          : "计划未返回目标会话";
      return `${item.code}：${range}`;
    });
    if (moduleTargets.length) {
      const list = document.createElement("ul");
      list.className = "job-reasons";
      for (const target of moduleTargets) list.append(element("li", target));
      root.append(element("h3", "所选模块目标会话"), list);
    }
    const currentBody = currentPageBody(page);
    const isCleanupOperation = [
      "CLEAR_DERIVED",
      "CLEAR_AND_REGENERATE",
    ].includes(currentBody.operation);
    const plannedModules = Array.isArray(plan.selected_modules)
      ? plan.selected_modules
      : selectedModules(page);
    const plannedModuleCodes = plannedModules.map((item) =>
      typeof item === "string" ? item : item.module_code || item.code || "",
    );
    const sectorModuleSelected = plannedModuleCodes.some((code) =>
      ["SECTOR_PULSE", "SECTOR_FOLLOWUP"].some((sectorCode) =>
        String(code).toUpperCase().includes(sectorCode),
      ),
    );
    const snapshotPair = plan.sector_snapshot_pair;
    if (snapshotPair && sectorModuleSelected) {
      const snapshotIds = snapshotPair.snapshot_ids || {};
      const snapshotDetails = [
        `观察时间：${snapshotPair.observed_at || "未知"}`,
        `行业快照：${snapshotIds.INDUSTRY || "不可用"}`,
        `GN概念快照：${snapshotIds.CONCEPT || "不可用"}`,
        `成员时态：${snapshotPair.membership_mode || "UNKNOWN"}`,
      ];
      for (const snapshot of snapshotPair.snapshots || []) {
        snapshotDetails.push(
          `${snapshot.source_kind || "来源"}：${snapshot.source_quality || "UNKNOWN"} · ${snapshot.research_mode || "时态未知"}`,
        );
      }
      root.append(
        element("h3", "行业 / GN概念快照配对"),
        element("p", snapshotDetails.join("；"), "muted"),
      );
    }
    const stageUnits = Array.isArray(plan.stage_units) ? plan.stage_units : [];
    const notReadyUnits = stageUnits.filter((unit) =>
      ["NOT_DUE", "NOT_COMPUTED"].includes(unit.status || unit.status_code),
    );
    if (notReadyUnits.length) {
      const list = document.createElement("ul");
      list.className = "job-reasons";
      for (const unit of notReadyUnits) {
        const module = unit.module_code || unit.stage_code || "研究模块";
        const targets = Array.isArray(unit.target_sessions)
          ? unit.target_sessions.join("、")
          : unit.target_session || "目标会话未知";
        const reason =
          unit.reason_code || unit.error_code || unit.reason || unit.message;
        list.append(
          element(
            "li",
            `${module} · ${targets} · ${statusLabel(unit.status || unit.status_code)}${reason ? ` · ${reason}` : ""}`,
          ),
        );
      }
      root.append(element("h3", "未到期 / 未计算单元"), list);
    }
    const newModuleRequest =
      Array.isArray(currentBody.source_modules) ||
      Array.isArray(currentBody.base_modules) ||
      Array.isArray(currentBody.research_modules) ||
      Array.isArray(plan.selected_modules);
    const legacyRequest =
      plan.legacy_request === true ||
      plan.request_kind === "LEGACY" ||
      plan.request_version === 1 ||
      !newModuleRequest;
    const d0ExplicitlyDisabled =
      plan.d0_skipped_by_user === true ||
      plan.d0_enabled === false ||
      currentBody.d0_enabled === false;
    const d1ExplicitlyDisabled =
      plan.d1_skipped_by_user === true ||
      plan.d1_enabled === false ||
      currentBody.d1_enabled === false;
    if (
      plan.d0_output_session_count === 0 &&
      (legacyRequest || d0ExplicitlyDisabled)
    ) {
      root.append(
        element("p", "D0：SKIPPED_BY_USER（用户关闭D0输出）", "muted"),
      );
    }
    if (
      plan.d1_output_session_count === 0 &&
      (legacyRequest || d1ExplicitlyDisabled)
    ) {
      root.append(
        element("p", "D1：SKIPPED_BY_USER（用户关闭D1输出）", "muted"),
      );
    }
    const incomplete = [
      ...(plan.d0_insufficient_previous_sessions || []).map(
        (day) => "D0 " + day + "：前序Raw不足",
      ),
      ...(plan.d1_insufficient_history_sessions || []).map(
        (day) => "D1 " + day + "：算法窗口Raw不足",
      ),
      ...(plan.profile_insufficient_history_sessions || []).map(
        (day) => "Profile " + day + "：历史窗口不足",
      ),
      ...(plan.market_breadth_insufficient_history_sessions || []).map(
        (day) => "市场宽度 " + day + "：T-4官方Raw窗口不足",
      ),
      ...(plan.market_participation_insufficient_sessions || []).map(
        (day) => "成交参与 " + day + "：宽度或Profile未闭合",
      ),
    ];
    if (incomplete.length) {
      const list = document.createElement("ul");
      list.className = "job-reasons";
      for (const reason of incomplete) list.append(element("li", reason));
      root.append(list);
    }
    const reasons = plan.blocked_reasons || [];
    if (reasons.length) {
      const messages = {
        DATA_EXECUTION_NOT_AUTHORIZED: "当前任务未获真实行情执行授权。",
        NO_AUTHORIZED_NEW_OFFICIAL_SESSION: "研究截止内没有待更新的官方会话。",
        D1_HISTORY_WINDOW_NOT_ESTABLISHED: "PG中尚无唯一的D1计算窗口。",
        D1_HISTORY_WINDOW_AMBIGUOUS: "PG中存在多个D1计算窗口，不能自动选择。",
        HISTORY_INSUFFICIENT_FOR_SOME_OUTPUT_DATES:
          "部分输出日期缺少所需前序会话。",
      };
      root.append(
        element(
          "p",
          reasons.map((code) => messages[code] || code).join(" "),
          plan.executable ? "muted" : "job-error",
        ),
      );
    }
    for (const item of plan.unplanned_producers || []) {
      root.append(
        element(
          "p",
          (item.stage_code || "研究生产者") +
            "：" +
            statusLabel(item.status) +
            " · " +
            (item.message || item.reason_code || "所需输入未绑定"),
          "muted",
        ),
      );
    }
    if (plan.cleanup_preview) {
      const preview = plan.cleanup_preview;
      const details = document.createElement("dl");
      details.className = "job-cleanup-preview";
      const dependents = Array.isArray(preview.dependents)
        ? preview.dependents.map(dependencySummary)
        : preview.dependents
          ? [dependencySummary(preview.dependents)]
          : [];
      const entries = [
        ["模块", preview.module_label || preview.module_code || "未知"],
        [
          "来源 / 算法版本",
          [preview.source_version, preview.algorithm_version]
            .filter(Boolean)
            .join(" / ") || "按计划输入",
        ],
        ["目标会话", (preview.target_sessions || []).join("、") || "未知"],
        ["影响表", (preview.affected_tables || []).join("、") || "未知"],
        ["预计影响行", count(preview.affected_rows)],
        ["下游依赖", dependents.join("；") || "无 / 未声明"],
        [
          "基础来源Raw",
          preview.raw_rows_untouched === true ? "不变" : "需要后端明确说明",
        ],
      ];
      for (const [label, value] of entries) {
        details.append(element("dt", label), element("dd", value));
      }
      root.append(element("h3", "派生结果清理预览"), details);
    }
    const body = currentPageBody(page);
    const derivedModules =
      body.base_modules.length + body.research_modules.length;
    const selected = selectedModules(page);
    const cleanupRequested = ["CLEAR_DERIVED", "CLEAR_AND_REGENERATE"].includes(
      body.operation,
    );
    if (!(body.source_modules.length + derivedModules)) {
      root.append(element("p", "至少选择一个实际可用模块。", "job-error"));
    }
    if (cleanupRequested && body.source_modules.length) {
      root.append(
        element(
          "p",
          "基础来源（包括Raw）不能从此处清理；取消来源选择并仅选择一个派生模块。",
          "job-error",
        ),
      );
    }
    if (cleanupRequested && derivedModules !== 1) {
      root.append(
        element(
          "p",
          "局部清理必须只选择一个基础画像或研究模块，以便核对精确依赖。",
          "job-error",
        ),
      );
    }
    if (
      cleanupRequested &&
      selected.some((item) => item.layer === "SOURCE" || !item.cleanupAllowed)
    ) {
      root.append(
        element(
          "p",
          "清理计划包含未标记 cleanup_allowed=true 的模块；请只选择明确允许清理的派生模块。",
          "job-error",
        ),
      );
    }
    const cleanupScopeValid =
      !cleanupRequested ||
      (body.source_modules.length === 0 &&
        derivedModules === 1 &&
        selected.every(
          (item) => item.layer !== "SOURCE" && item.cleanupAllowed,
        ));
    const resetPreview = plan.reset_preview || plan.business_reset_preview;
    const resetReviewOnly =
      page === "initialize" &&
      currentBody.initialization_mode === "RESET_ALL_BUSINESS_FACTS" &&
      !plan.executable &&
      resetPreviewReady(resetPreview);
    const resetReady =
      page !== "initialize" ||
      currentBody.initialization_mode !== "RESET_ALL_BUSINESS_FACTS" ||
      resetPreviewReady(resetPreview);
    byId(page === "initialize" ? "startDataInit" : "startDataDaily").disabled =
      (!plan.executable && !resetReviewOnly) ||
      !(
        body.source_modules.length +
        body.base_modules.length +
        body.research_modules.length
      ) ||
      !cleanupScopeValid ||
      (cleanupRequested &&
        (!plan.cleanup_preview || plan.cleanup_preview.complete !== true)) ||
      !resetReady;
  }

  async function loadJob(jobId, page) {
    if (!page || !["initialize", "daily"].includes(page)) return;
    const path = jobId
      ? "/api/product/data-jobs?id=" + encodeURIComponent(jobId)
      : "/api/product/data-jobs/latest";
    try {
      const result = await api(path);
      const job = result.job || null;
      state.latestJobId = job?.job_id || null;
      renderProgress(
        byId(page === "initialize" ? "dataInitProgress" : "dataDailyProgress"),
        job,
      );
      if (job) {
        byId(
          page === "initialize" ? "dataInitStatus" : "dataDailyStatus",
        ).textContent = statusLabel(job.status);
      }
      if (job && job.status === "RUNNING") schedulePoll();
    } catch (error) {
      byId(
        page === "initialize" ? "dataInitProgress" : "dataDailyProgress",
      ).replaceChildren(
        element("p", "任务状态读取失败：" + error.message, "job-error"),
      );
    }
  }

  function schedulePoll() {
    if (state.timer) window.clearTimeout(state.timer);
    state.timer = window.setTimeout(() => {
      if (["initialize", "daily"].includes(state.page)) {
        void loadJob(state.latestJobId, state.page);
      }
    }, 5000);
  }

  async function load(page) {
    if (!["initialize", "daily"].includes(page) || state.loading) return;
    state.page = page;
    state.loading = true;
    const planRoot = byId(
      page === "initialize" ? "dataInitPlan" : "dataDailyPlan",
    );
    planRoot.replaceChildren(
      element("p", "正在读取PG任务状态与截止内Raw覆盖…", "muted"),
    );
    try {
      const capabilities = await api("/api/product/data-jobs/capabilities");
      let resumableResetJob = null;
      if (page === "initialize") {
        resumableResetJob = (capabilities.recent_jobs || []).find(
          (job) =>
            job.mode === "INITIALIZE" &&
            job.initialization_mode === "RESET_ALL_BUSINESS_FACTS" &&
            [
              "RUNNING",
              "FAILED",
              "INTERRUPTED",
              "INCOMPLETE",
              "BLOCKED",
            ].includes(job.status) &&
            typeof job.reset_preview_fingerprint === "string",
        );
        state.resetResumeFingerprint =
          resumableResetJob?.reset_preview_fingerprint || null;
      }
      renderCapabilities(capabilities);
      if (resumableResetJob) {
        byId("dataInitMode").value = "RESET_ALL_BUSINESS_FACTS";
        byId("dataInitTarget").value = resumableResetJob.target_session || "";
        byId("dataInitHistory").value = String(
          resumableResetJob.history_session_count || "",
        );
        byId("dataInitProfileHistory").value = String(
          resumableResetJob.profile_history_session_count || "",
        );
        const savedCounts = Object.fromEntries(
          (resumableResetJob.selected_modules || []).map((code, index) => [
            code,
            resumableResetJob.module_output_session_counts?.[index],
          ]),
        );
        for (const checkbox of byId("dataInitModules").querySelectorAll(
          "[data-module-code]",
        )) {
          checkbox.checked = (
            resumableResetJob.selected_modules || []
          ).includes(checkbox.dataset.moduleCode);
          const countInput = byId(checkbox.dataset.outputCountId || "");
          if (
            countInput &&
            Number.isInteger(savedCounts[checkbox.dataset.moduleCode])
          ) {
            countInput.value = String(savedCounts[checkbox.dataset.moduleCode]);
            countInput.disabled = !checkbox.checked;
          }
        }
        byId("dataInitOperation").value =
          resumableResetJob.operation || "FILL_MISSING";
        updateInitializationMode();
        state.resetResumeFingerprint =
          resumableResetJob.reset_preview_fingerprint;
      }
      await loadJob(null, page);
      const result = await api(
        "/api/product/data-jobs/plan",
        requestBodyForPage(page),
      );
      renderPlan(result.plan, page);
    } catch (error) {
      planRoot.replaceChildren(
        element("p", "预检失败：" + error.message, "job-error"),
      );
      byId(
        page === "initialize" ? "startDataInit" : "startDataDaily",
      ).disabled = true;
    } finally {
      state.loading = false;
    }
  }

  async function preview(page) {
    try {
      const body = requestBodyForPage(page);
      const result = await api("/api/product/data-jobs/plan", body);
      renderPlan(result.plan, page);
    } catch (error) {
      renderPlan(
        {
          status: "FAILED",
          executable: false,
          blocked_reasons: [error.message],
        },
        page,
      );
    }
  }

  function openResetConfirmation(body) {
    const preview =
      state.plan?.reset_preview || state.plan?.business_reset_preview;
    const ready = resetPreviewReady(preview);
    if (!ready) {
      throw new Error("全库清理范围或必需来源尚未完整核验；没有执行清理。");
    }
    state.resetPreviewBody = JSON.stringify(body);
    renderBusinessResetPreview(byId("dataInitResetSummary"), preview, body);
    byId("dataInitResetStepOne").hidden = false;
    byId("dataInitResetStepTwo").hidden = true;
    byId("dataInitResetReviewed").checked = false;
    byId("dataInitResetPhrase").value = "";
    byId("dataInitResetFinalAck").checked = false;
    byId("dataInitResetContinue").disabled = true;
    byId("dataInitResetConfirm").disabled = true;
    state.resetDialogOpen = true;
    byId("dataInitResetDialog").showModal();
  }

  function updateResetConfirmAvailability() {
    byId("dataInitResetContinue").disabled =
      !byId("dataInitResetReviewed").checked || state.startPending;
    byId("dataInitResetConfirm").disabled =
      byId("dataInitResetPhrase").value !== "清空本项目全部业务事实" ||
      !byId("dataInitResetFinalAck").checked ||
      state.startPending;
  }

  async function start(page, { resetConfirmed = false } = {}) {
    if (state.startPending) return;
    state.startPending = true;
    updateResetConfirmAvailability();
    const button = byId(
      page === "initialize" ? "startDataInit" : "startDataDaily",
    );
    button.disabled = true;
    try {
      const body = requestBodyForPage(page);
      if (state.planBody !== JSON.stringify(body)) {
        await preview(page);
      }
      const resetMode =
        page === "initialize" &&
        body.initialization_mode === "RESET_ALL_BUSINESS_FACTS";
      if (!state.plan?.executable) {
        const preview =
          state.plan?.reset_preview || state.plan?.business_reset_preview;
        if (resetMode && resetPreviewReady(preview) && !resetConfirmed) {
          openResetConfirmation(body);
          return;
        }
        if (resetMode && resetConfirmed) {
          throw new Error(
            "当前来源/执行前置条件未通过；未创建任务，也未清理业务事实。",
          );
        }
        return;
      }
      if (resetMode && !resetConfirmed) {
        openResetConfirmation(body);
        return;
      }
      let requestBody = body;
      if (resetMode) {
        const preview =
          state.plan.reset_preview || state.plan.business_reset_preview;
        if (
          state.resetPreviewBody !== JSON.stringify(body) ||
          !preview?.reset_preview_fingerprint
        ) {
          throw new Error("初始化范围已改变；请重新检查清理预览后再确认。");
        }
        requestBody = {
          ...body,
          reset_preview_fingerprint: preview.reset_preview_fingerprint,
          reset_confirmed: true,
        };
        state.resetResumeFingerprint = preview.reset_preview_fingerprint;
      }
      if (["CLEAR_DERIVED", "CLEAR_AND_REGENERATE"].includes(body.operation)) {
        if (
          selectedModules(page).some(
            (item) => item.layer === "SOURCE" || !item.cleanupAllowed,
          )
        ) {
          throw new Error("清理模块未声明 cleanup_allowed=true；未执行清理。");
        }
        if (
          body.source_modules.length ||
          body.base_modules.length + body.research_modules.length !== 1
        ) {
          throw new Error("清理仅允许一个派生模块且不可包含基础来源。");
        }
        const preview = state.plan.cleanup_preview;
        if (!preview || preview.complete !== true) {
          throw new Error("清理范围没有完整预检；未执行清理。");
        }
        const summary = [
          `模块：${preview.module_label || preview.module_code || "未知"}`,
          `目标会话：${(preview.target_sessions || []).join("、") || "未知"}`,
          `影响表：${(preview.affected_tables || []).join("、") || "未知"}`,
          `预计行数：${count(preview.affected_rows)}`,
          `Raw来源行保持：${preview.raw_rows_untouched === true ? "是" : "未证明"}`,
        ].join("\n");
        if (
          preview.raw_rows_untouched !== true ||
          !window.confirm(`确认只处理以下派生结果？\n${summary}`)
        )
          return;
      }
      const result = await api("/api/product/data-jobs/start", requestBody);
      state.latestJobId = result.job_id || result.job?.job_id || null;
      if (resetMode) byId("dataInitResetDialog").close();
      await loadJob(state.latestJobId, page);
      const recent = await api("/api/product/data-jobs/capabilities");
      renderRecent(
        byId(page === "initialize" ? "dataInitRecent" : "dataDailyRecent"),
        recent.recent_jobs,
      );
    } catch (error) {
      byId(
        page === "initialize" ? "dataInitPlan" : "dataDailyPlan",
      ).replaceChildren(
        element("p", "启动被拒绝：" + error.message, "job-error"),
      );
    } finally {
      state.startPending = false;
      updateResetConfirmAvailability();
      if (!state.resetDialogOpen) await preview(page);
    }
  }

  byId("planDataInit").addEventListener(
    "click",
    () => void preview("initialize"),
  );
  byId("startDataInit").addEventListener(
    "click",
    () => void start("initialize"),
  );
  byId("planDataDaily").addEventListener("click", () => void preview("daily"));
  byId("startDataDaily").addEventListener("click", () => void start("daily"));
  byId("refreshDataInit").addEventListener(
    "click",
    () => void load("initialize"),
  );
  byId("refreshDataDaily").addEventListener("click", () => void load("daily"));
  byId("dataInitOperation").addEventListener("change", () => {
    updateCleanupAvailability("initialize");
  });
  byId("dataDailyOperation").addEventListener("change", () => {
    if (
      ["CLEAR_DERIVED", "CLEAR_AND_REGENERATE"].includes(
        byId("dataDailyOperation").value,
      )
    ) {
      byId("dataDailyMode").value = "CUSTOM";
    }
    updateCleanupAvailability("daily");
  });
  byId("dataInitMode").addEventListener("change", updateInitializationMode);
  byId("dataInitProfileHistory").addEventListener("input", () => {
    state.profileHistoryTouched = true;
  });
  byId("dataDailyMode").addEventListener("change", applyDailyPreset);
  byId("dataInitResetReviewed").addEventListener(
    "change",
    updateResetConfirmAvailability,
  );
  byId("dataInitResetPhrase").addEventListener(
    "input",
    updateResetConfirmAvailability,
  );
  byId("dataInitResetFinalAck").addEventListener(
    "change",
    updateResetConfirmAvailability,
  );
  byId("dataInitResetContinue").addEventListener("click", () => {
    byId("dataInitResetStepOne").hidden = true;
    byId("dataInitResetStepTwo").hidden = false;
    byId("dataInitResetPhrase").focus();
  });
  byId("dataInitResetConfirm").addEventListener("click", () => {
    void start("initialize", { resetConfirmed: true });
  });
  byId("dataInitResetDialog").addEventListener("close", () => {
    state.resetDialogOpen = false;
  });
  for (const button of document.querySelectorAll("[data-history-sessions]")) {
    button.addEventListener("click", () => {
      byId("dataInitHistory").value = button.dataset.historySessions;
    });
  }
  for (const id of [
    "dataInitTarget",
    "dataInitHistory",
    "dataInitProfileHistory",
    "dataInitMode",
    "dataDailyMode",
    "dataInitOperation",
    "dataDailyOperation",
  ]) {
    byId(id).addEventListener("input", () => {
      byId("startDataInit").disabled = true;
      byId("startDataDaily").disabled = true;
      state.planBody = null;
    });
    byId(id).addEventListener("change", () => {
      byId("startDataInit").disabled = true;
      byId("startDataDaily").disabled = true;
      state.planBody = null;
    });
  }
  return { load };
}
