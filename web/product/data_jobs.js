const STATUS_LABELS = {
  AVAILABLE: "可查看",
  BLOCKED_NOT_AUTHORIZED: "受限：未获行情执行授权",
  BLOCKED_SOURCE_NOT_AVAILABLE: "受限：来源不可用",
  BLOCKED_HISTORY_INSUFFICIENT: "受限：历史会话不足",
  HISTORY_INSUFFICIENT: "历史会话不足",
  READY: "可执行",
  READY_SOURCE_CHECK_REQUIRED: "等待开始后核验来源",
  READY_WITH_INCOMPLETE_UNITS: "部分输出受限",
  RUNNING: "运行中",
  COMPLETED: "完成",
  ALREADY_COMPLETE: "同输入已完成",
  NO_CHANGE: "无变化",
  NOT_COMPUTED: "未计算",
  INCOMPLETE: "不完整",
  BLOCKED: "受限",
  FAILED: "失败",
  INTERRUPTED: "中断",
  NOT_STARTED: "未开始",
  SKIPPED_BY_USER: "用户跳过",
  SKIPPED_NOT_IMPLEMENTED: "生产者未实现",
};

const STAGE_LABELS = {
  SOURCE_RAW: "来源与 Raw",
  CORE_PROFILE: "Core / Profile",
  STATIC_RESEARCH: "静态研究",
  D0_D1: "D0 / D1",
  MARKET_BREADTH: "市场宽度",
  MARKET_PARTICIPATION: "成交参与",
  FOCUS_FORMAL: "Focus正式准入",
};

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
  const heading = element("div", null, "job-progress-heading");
  heading.append(
    element(
      "div",
      (job.mode === "INITIALIZE" ? "数据初始化" : "数据日更") +
        " · " +
        (job.target_session || "目标会话未知"),
      "job-progress-title",
    ),
    element(
      "p",
      statusLabel(job.status) +
        " · 已核验单元 " +
        count(job.completed_units) +
        "/" +
        count(job.planned_units) +
        "（" +
        (job.planned_units
          ? Math.floor((job.completed_units / job.planned_units) * 100) + "%"
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
  const units = Array.isArray(job.progress) ? job.progress : [];
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
      [
        "COMPLETED",
        "ALREADY_COMPLETE",
        "NO_CHANGE",
        "SKIPPED_BY_USER",
        "SKIPPED_NOT_IMPLEMENTED",
      ].includes(row.status),
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
            ["COMPLETED", "ALREADY_COMPLETE", "NO_CHANGE"].includes(row.status)
            ? row.target_session
            : "—",
        ),
        element("td", count(row.expected_rows)),
        element("td", count(row.verified_rows)),
        element(
          "td",
          count(row.inserted_rows) + " / " + count(row.updated_rows),
        ),
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
      element("td", job.mode === "INITIALIZE" ? "初始化" : "手动日更"),
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
  if (page === "initialize") {
    const d0 = byId("dataInitD0Enabled").checked;
    const d1 = byId("dataInitD1Enabled").checked;
    return {
      mode: "INITIALIZE",
      target_session: byId("dataInitTarget").value || undefined,
      history_session_count: Number(byId("dataInitHistory").value),
      d0_output_session_count: d0 ? Number(byId("dataInitD0Count").value) : 0,
      d1_output_session_count: d1 ? Number(byId("dataInitD1Count").value) : 0,
    };
  }
  return { mode: "MANUAL_DAILY" };
}

export function mountDataJobPages(api) {
  const state = {
    page: null,
    plan: null,
    latestJobId: null,
    timer: null,
    loading: false,
  };

  function populateTargets(capabilities) {
    const select = byId("dataInitTarget");
    const sessions = capabilities.raw_coverage.closed_sessions || [];
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
    renderRecent(byId("dataInitRecent"), capabilities.recent_jobs);
    renderRecent(byId("dataDailyRecent"), capabilities.recent_jobs);
  }

  function renderPlan(plan, page) {
    state.plan = plan;
    const root = byId(page === "initialize" ? "dataInitPlan" : "dataDailyPlan");
    root.replaceChildren();
    byId(
      page === "initialize" ? "dataInitStatus" : "dataDailyStatus",
    ).textContent = statusLabel(plan.status);
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
            " 个官方会话；PG已闭合 " +
            plan.raw_history_sessions_in_pg +
            " 个，未在PG中找到 " +
            (plan.missing_raw_sessions || []).length +
            " 个；当前画像来源包 " +
            (plan.source_coverage?.active_pg_source_package_id ||
              "未唯一绑定") +
            "。来源覆盖：" +
            (plan.source_coverage?.status || "未知") +
            "。",
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
    if (plan.d1_calculation_session_count) {
      root.append(
        element(
          "p",
          "D1算法窗口沿用PG中唯一已存合同 " +
            plan.d1_calculation_session_count +
            " 个会话；此值与D1输出保存范围分开。",
        ),
      );
    } else if (plan.d1_output_session_count > 0) {
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
    if (plan.d0_output_session_count === 0) {
      root.append(
        element("p", "D0：SKIPPED_BY_USER（用户关闭D0输出）", "muted"),
      );
    }
    if (plan.d1_output_session_count === 0) {
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
    byId(page === "initialize" ? "startDataInit" : "startDataDaily").disabled =
      !plan.executable;
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
      renderCapabilities(capabilities);
      await loadJob(null, page);
      const result = await api(
        "/api/product/data-jobs/plan",
        currentPageBody(page),
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
      const result = await api(
        "/api/product/data-jobs/plan",
        currentPageBody(page),
      );
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

  async function start(page) {
    const button = byId(
      page === "initialize" ? "startDataInit" : "startDataDaily",
    );
    button.disabled = true;
    try {
      const result = await api(
        "/api/product/data-jobs/start",
        currentPageBody(page),
      );
      state.latestJobId = result.job_id || result.job?.job_id || null;
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
      await preview(page);
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
  for (const button of document.querySelectorAll("[data-history-sessions]")) {
    button.addEventListener("click", () => {
      byId("dataInitHistory").value = button.dataset.historySessions;
    });
  }
  for (const [toggleId, countId] of [
    ["dataInitD0Enabled", "dataInitD0Count"],
    ["dataInitD1Enabled", "dataInitD1Count"],
  ]) {
    byId(toggleId).addEventListener("change", (event) => {
      byId(countId).disabled = !event.target.checked;
    });
  }
  for (const id of [
    "dataInitTarget",
    "dataInitHistory",
    "dataInitD0Enabled",
    "dataInitD0Count",
    "dataInitD1Enabled",
    "dataInitD1Count",
  ]) {
    byId(id).addEventListener("input", () => {
      byId("startDataInit").disabled = true;
    });
    byId(id).addEventListener("change", () => {
      byId("startDataInit").disabled = true;
    });
  }
  return { load };
}
