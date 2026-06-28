// Theme: light default, persist to localStorage
(function initTheme() {
  const saved = localStorage.getItem("maestro.theme");
  if (saved === "dark") document.documentElement.setAttribute("data-theme", "dark");
})();

const storageKey = "maestro.console.targets.v1";

const state = {
  targets: loadTargets(),
  cards: [],
  targetFilter: "",
  activeTarget: null,
  actor: null,
  cluster: null,
  versions: [],
};

const elements = {
  targetList: document.querySelector("#target-list"),
  targetCount: document.querySelector("#target-count"),
  targetSearch: document.querySelector("#target-search"),
  deploymentName: document.querySelector("#deployment-name"),
  workflowID: document.querySelector("#workflow-id"),
  flinkDashboardLink: document.querySelector("#flink-dashboard-link"),
  actorStatus: document.querySelector("#actor-status"),
  lastUpdated: document.querySelector("#last-updated"),
  flash: document.querySelector("#flash"),
  operationList: document.querySelector("#operation-list"),
  healthGrid: document.querySelector("#health-grid"),
  clusterStatus: document.querySelector("#cluster-status"),
  clusterDetail: document.querySelector("#cluster-detail"),
  versionsTable: document.querySelector("#versions-table"),
  targetDialog: document.querySelector("#target-dialog"),
  actionDialog: document.querySelector("#action-dialog"),
  deploySwitcherBtn: document.querySelector("#deploy-switcher-btn"),
  deployDropdown: document.querySelector("#deploy-dropdown"),
  cmdPalette: document.querySelector("#cmd-palette"),
  cmdSearch: document.querySelector("#cmd-search"),
  cmdResults: document.querySelector("#cmd-results"),
};

// Deployment dropdown toggle
document.querySelector("#deploy-switcher-btn")?.addEventListener("click", (e) => {
  e.stopPropagation();
  document.querySelector("#deploy-dropdown").classList.toggle("open");
});
document.addEventListener("click", (e) => {
  const dropdown = document.querySelector("#deploy-dropdown");
  if (dropdown && !dropdown.contains(e.target) && !e.target.closest("#deploy-switcher-btn")) {
    dropdown.classList.remove("open");
  }
});

let autoRefreshTimer = null;
function setAutoRefresh(on) {
  clearInterval(autoRefreshTimer);
  autoRefreshTimer = on ? setInterval(() => loadActiveTarget({ quiet: true }), 30_000) : null;
  const btn = document.querySelector("#auto-refresh-btn");
  btn.textContent = on ? "Live ●" : "Live";
  btn.classList.toggle("live", on);
}

function loadTargets() {
  try {
    return JSON.parse(localStorage.getItem(storageKey)) || [];
  } catch {
    return [];
  }
}

function saveTargets() {
  localStorage.setItem(storageKey, JSON.stringify(state.targets));
}

function matchesTargetFilter(target, filterValue) {
  if (!filterValue) return true;
  const haystack = [target.name, target.environment, target.namespace, target.owner, target.serviceAccount, target.nodePool]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return haystack.includes(filterValue);
}

async function loadDeploymentInventory(optimisticTargets = []) {
  const localTargets = loadTargets();
  const localByKey = new Map(localTargets.map((target) => [targetKey(target), target]));
  const deployments = [];
  let pageToken = "";

  try {
    do {
      const query = new URLSearchParams({ limit: "500" });
      if (pageToken) query.set("pageToken", pageToken);
      const page = await request(`/api/v1/deployments?${query}`);
      deployments.push(...(page.deployments || []));
      pageToken = page.nextPageToken || "";
    } while (pageToken);

    state.targets = deployments.map(({ identity, startedAt }) => ({
      ...identity,
      startedAt,
      ...(localByKey.get(targetKey(identity)) || {}),
    }));
    for (const t of optimisticTargets) {
      if (!state.targets.some((item) => targetKey(item) === targetKey(t))) state.targets.unshift(t);
    }
    saveTargets();
    renderTargets();

    if (state.targets.length) {
      const selected = state.activeTarget
        ? state.targets.find((t) => targetKey(t) === targetKey(state.activeTarget))
        : state.targets[0];
      if (selected) activateTarget(selected);
    }
  } catch (error) {
    state.targets = localTargets;
    renderTargets();
    if (state.targets.length) activateTarget(state.targets[0]);
    flash(`Deployment inventory unavailable: ${error.message}`, "error");
  }
}

function targetKey(target) {
  return `${target.environment}/${target.namespace}/${target.name}`;
}

function apiPath(target, suffix = "") {
  const parts = [target.environment, target.namespace, target.name].map(encodeURIComponent);
  return `/api/v1/deployments/${parts.join("/")}${suffix}`;
}

function clusterPath(target, suffix) {
  return `/api/v1/clusters/${encodeURIComponent(target.environment)}/${encodeURIComponent(target.namespace)}${suffix}`;
}

function generatedKey(prefix) {
  return `${prefix}-${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14)}-${crypto.randomUUID().slice(0, 8)}`;
}

function parsePairs(value) {
  return value.split("\n").reduce((result, line) => {
    const trimmed = line.trim();
    if (!trimmed) return result;
    const separator = trimmed.indexOf("=");
    if (separator < 1) throw new Error(`Expected key=value, received "${trimmed}"`);
    result[trimmed.slice(0, separator).trim()] = trimmed.slice(separator + 1).trim();
    return result;
  }, {});
}

function shortDigest(value = "") {
  if (!value) return "—";
  const digest = value.split("@").pop();
  return digest.length > 20 ? `${digest.slice(0, 17)}…` : digest;
}

function formatDate(value) {
  if (!value || value.startsWith("0001-")) return "—";
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
}

function dashboardURLFor(target, actor) {
  if (!target) return "";
  const configured = target.flinkDashboardUrl || actor?.identity?.flinkDashboardUrl;
  if (configured) return configured;
  if (!actor?.currentVersion) return "";
  return `http://localhost:8081/#/job/${encodeURIComponent(target.name)}`;
}

function statusTone(status) {
  if (["IDLE", "SUCCEEDED", "HEALTHY"].includes(status)) return "success";
  if (["FAILED", "REJECTED", "FROZEN"].includes(status)) return "danger";
  if (["OPERATING", "RUNNING", "QUEUED", "SUSPENDED"].includes(status)) return "warning";
  return "neutral";
}

function setBadge(element, text, tone = statusTone(text)) {
  element.textContent = text;
  element.className = `status-badge ${tone}`;
}

function flash(message, tone = "success") {
  elements.flash.textContent = message;
  elements.flash.className = `flash visible ${tone}`;
  clearTimeout(flash.timer);
  flash.timer = setTimeout(() => { elements.flash.className = "flash"; }, 5000);
}

async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
  });
  const text = await response.text();
  let body = null;
  if (text) {
    try { body = JSON.parse(text); } catch { body = { error: text }; }
  }
  if (!response.ok) throw new Error(body?.error || `${response.status} ${response.statusText}`);
  return body;
}

function renderTargets() {
  const filterValue = state.targetFilter.trim().toLowerCase();
  const visible = state.targets.filter((t) => matchesTargetFilter(t, filterValue));
  elements.targetCount.textContent = filterValue ? `${visible.length}/${state.targets.length}` : String(state.targets.length);
  if (!state.targets.length) {
    elements.targetList.innerHTML = '<div class="empty-state">No targets yet.</div>';
    renderEnvironments();
    return;
  }
  if (!visible.length) {
    elements.targetList.innerHTML = '<div class="empty-state">No targets match filter.</div>';
    renderEnvironments();
    return;
  }
  elements.targetList.innerHTML = visible.map((target) => `
    <div class="target-row">
      <button class="target ${state.activeTarget && targetKey(target) === targetKey(state.activeTarget) ? "active" : ""}"
        data-target="${targetKey(target)}">
        <img src="/ui/logo-sm.png" class="maestro-logo" width="24" height="24" alt="" style="flex-shrink:0;border-radius:6px;">
        <div class="target-details">
          <strong>${escapeHTML(target.name)}</strong>
          <small>${escapeHTML(target.environment)} / ${escapeHTML(target.namespace)}</small>
          <div class="target-meta">
            <span>${target.owner ? escapeHTML(target.owner) : "Inventory"}</span>
            <time>${target.startedAt ? escapeHTML(formatDate(target.startedAt)) : "Local only"}</time>
          </div>
        </div>
      </button>
    </div>
  `).join("");

  // Update switcher pill text
  const switcherName = document.querySelector("#switcher-name");
  const switcherEnv = document.querySelector("#switcher-env");
  if (switcherName) switcherName.textContent = state.activeTarget?.name || "Select deployment";
  if (switcherEnv) switcherEnv.textContent = state.activeTarget ? `${state.activeTarget.environment}` : "";

  renderEnvironments();
}

async function loadCards() {
  try {
    state.cards = await request("/api/v1/deployments/summary") || [];
    renderEnvironments();
  } catch {
    // ponytail: fall back to targets without health data
    state.cards = state.targets.map((t) => ({
      identity: t, workflowId: "", status: "UNKNOWN", pendingOperations: 0,
    }));
    renderEnvironments();
  }
}

function cardForTarget(target) {
  return state.cards.find((c) =>
    c.identity.environment === target.environment &&
    c.identity.namespace === target.namespace &&
    c.identity.name === target.name
  );
}

function healthDot(card) {
  if (!card || card.status === "UNKNOWN" || card.status === "UNREACHABLE") return '<span class="health-dot neutral"></span>';
  if (card.healthy === true) return '<span class="health-dot good"></span>';
  if (card.healthy === false) return '<span class="health-dot bad"></span>';
  return '<span class="health-dot neutral"></span>';
}

function renderEnvironments() {
  const grid = document.querySelector("#env-grid");
  const allItems = state.cards.length ? state.cards : state.targets.map((t) => ({
    identity: t, status: "UNKNOWN", pendingOperations: 0,
  }));
  if (!allItems.length) {
    grid.innerHTML = `
      <div class="env-empty">
        <strong>No deployments registered</strong>
        <p>Use the button below or the + icon in the sidebar to onboard your first Flink job. You can also paste a GitHub raw YAML URL to import from a manifest.</p>
        <button class="button primary" id="env-empty-add">Register deployment</button>
      </div>`;
    document.querySelector("#env-empty-add")?.addEventListener("click", () => elements.targetDialog.showModal());
    return;
  }
  const envMap = new Map();
  for (const card of allItems) {
    const env = card.identity.environment || "unknown";
    if (!envMap.has(env)) envMap.set(env, []);
    envMap.get(env).push(card);
  }
  grid.innerHTML = [...envMap.entries()].map(([env, cards]) => `
    <div class="env-section">
      <div class="env-section-header">
        <div class="env-section-title">
          <span class="env-badge">${escapeHTML(env)}</span>
          <span class="env-count">${cards.length} deployment${cards.length !== 1 ? "s" : ""}</span>
        </div>
        <button class="button secondary" data-env-register="${escapeHTML(env)}">+ Register</button>
      </div>
      <div class="card-grid">
        ${cards.map((card) => {
          const id = card.identity;
          const key = `${id.environment}/${id.namespace}/${id.name}`;
          const isActive = state.activeTarget && targetKey(state.activeTarget) === key;
          return `
          <button class="app-card${isActive ? " active" : ""}" data-card-target="${escapeHTML(key)}">
            <div class="app-card-header">
              ${healthDot(card)}
              <strong>${escapeHTML(id.name)}</strong>
            </div>
            <div class="app-card-meta">
              <span>${escapeHTML(id.namespace)}</span>
              <span class="status-badge ${statusTone(card.status)}">${escapeHTML(card.status || "UNKNOWN")}</span>
            </div>
            <div class="app-card-stats">
              ${card.version ? `<span>v${card.version}</span>` : "<span>—</span>"}
              ${card.parallelism ? `<span>∥ ${card.parallelism}</span>` : ""}
              ${card.pendingOperations ? `<span class="pending-badge">${card.pendingOperations} pending</span>` : ""}
            </div>
            ${card.imageDigest ? `<code class="app-card-image">${escapeHTML(shortDigest(card.imageDigest))}</code>` : ""}
            ${card.error ? `<small class="app-card-error">${escapeHTML(card.error)}</small>` : ""}
          </button>`;
        }).join("")}
      </div>
    </div>
  `).join("");
}

function renderActor() {
  const actor = state.actor;
  const target = state.activeTarget;
  const dashboardURL = dashboardURLFor(target, actor);
  elements.deploymentName.textContent = target?.name || "Select a deployment";
  elements.workflowID.textContent = target
    ? `flink-deployment/${target.environment}/${target.namespace}/${target.name}`
    : "Add or select a target to inspect its actor.";
  elements.flinkDashboardLink.href = dashboardURL || "#";
  elements.flinkDashboardLink.classList.toggle("disabled", !dashboardURL);
  elements.flinkDashboardLink.setAttribute("aria-disabled", dashboardURL ? "false" : "true");

  renderLedger();

  if (!actor) {
    setBadge(elements.actorStatus, "UNKNOWN", "neutral");
    ["version", "parallelism", "health", "pending"].forEach((name) => {
      document.querySelector(`#metric-${name}`).textContent = "—";
    });
    document.querySelector("#metric-image").textContent = "No version reported";
    document.querySelector("#metric-slots").textContent = "No resource shape";
    document.querySelector("#metric-health-detail").textContent = "Awaiting actor state";
    document.querySelector("#metric-autoscaler").textContent = "Autoscaler unknown";
    renderHealth(null);
    renderOperations([]);
    renderSavepoint(null);
    return;
  }

  setBadge(elements.actorStatus, actor.status || "UNKNOWN");
  const version = actor.currentVersion;
  document.querySelector("#metric-version").textContent = version ? `v${version.versionId}` : "None";
  document.querySelector("#metric-image").textContent = version ? shortDigest(version.spec.imageDigest) : "No active version";
  document.querySelector("#metric-parallelism").textContent = version?.spec.parallelism ?? "—";
  const resources = version?.spec.resources;
  document.querySelector("#metric-slots").textContent = resources
    ? `${resources.taskManagerCount || 0} managers · ${(resources.taskManagerCount || 0) * (resources.slotsPerManager || 0)} slots`
    : "No resource shape";
  document.querySelector("#metric-health").textContent = version?.healthSummary?.healthy ? "Healthy" : version ? "Degraded" : "—";
  document.querySelector("#metric-health-detail").textContent = version?.healthSummary?.message || "Latest health-gate result";
  document.querySelector("#metric-pending").textContent = actor.pendingOperations ?? 0;
  document.querySelector("#metric-autoscaler").textContent =
    `Autoscaler ${actor.autoscalerEnabled ? "enabled" : "disabled"}${actor.autoscalerFrozen ? " · frozen" : ""}`;

  renderHealth(version?.healthSummary);
  renderOperations(actor.recentOperations || []);
  renderSavepoint(actor.lastSavepoint);
}

function renderHealth(health) {
  const values = health ? [
    ["Running", health.running ? "PASS" : "FAIL", health.running],
    ["Checkpoint", health.checkpointCompleted ? "PASS" : "FAIL", health.checkpointCompleted],
    ["Sink", health.sinkHealthy ? "PASS" : "FAIL", health.sinkHealthy],
    ["Restarts", String(health.restartCount), health.restartCount <= 3],
    ["Backpressure", `${((health.backpressureRatio || 0) * 100).toFixed(1)}%`, health.backpressureRatio <= 0.75],
    ["Kafka lag", Number(health.kafkaLag || 0).toLocaleString(), true],
  ] : [
    ["Running", "—"], ["Checkpoint", "—"], ["Sink", "—"],
    ["Restarts", "—"], ["Backpressure", "—"], ["Kafka lag", "—"],
  ];
  if (elements.healthGrid) {
    elements.healthGrid.innerHTML = values.map(([label, value, good]) => `
      <div class="health-item">
        <span>${label}</span>
        <strong class="${good === undefined ? "" : good ? "good" : "bad"}">${value}</strong>
      </div>
    `).join("");
  }
  const runtimeBadge = document.querySelector("#runtime-badge");
  if (runtimeBadge) setBadge(runtimeBadge, health ? (health.healthy ? "HEALTHY" : "DEGRADED") : "NO DATA");
}

function renderOperations(operations) {
  if (!operations.length) {
    elements.operationList.innerHTML = '<div class="empty-state">No recent operations reported.</div>';
    return;
  }
  elements.operationList.innerHTML = operations.map((op) => `
    <div class="operation">
      <span class="operation-dot ${statusTone(op.status)}"></span>
      <div class="operation-copy">
        <strong>${escapeHTML(op.commandType || "Operation")}</strong>
        <small>${escapeHTML(op.result || op.operationId)}</small>
      </div>
      <span class="status-badge ${statusTone(op.status)}">${escapeHTML(op.status)}</span>
      <time>${formatDate(op.completedAt || op.startedAt)}</time>
    </div>
  `).join("");
}

function renderSavepoint(savepoint) {
  const card = document.querySelector("#savepoint-card");
  if (!savepoint) {
    card.innerHTML = "<strong>No savepoint</strong><p>The actor has not reported a savepoint.</p>";
    return;
  }
  card.innerHTML = `
    <strong>Version ${savepoint.deploymentVersion}</strong>
    <p>${formatDate(savepoint.createdAt)} · parallelism ${savepoint.parallelism}</p>
    <code>${escapeHTML(savepoint.uri)}</code>
  `;
}

function renderLedger() {
  const actor = state.actor;
  const health = actor?.currentVersion?.healthSummary;
  const items = [
    { id: "ledger-running", val: health ? (health.running ? "PASS" : "FAIL") : "—", good: health?.running },
    { id: "ledger-checkpoint", val: health ? (health.checkpointCompleted ? "PASS" : "FAIL") : "—", good: health?.checkpointCompleted },
    { id: "ledger-sink", val: health ? (health.sinkHealthy ? "PASS" : "FAIL") : "—", good: health?.sinkHealthy },
    { id: "ledger-restarts", val: health ? String(health.restartCount) : "—", good: health ? health.restartCount <= 3 : undefined },
    { id: "ledger-bp", val: health ? `${((health.backpressureRatio || 0) * 100).toFixed(0)}%` : "—", good: health ? health.backpressureRatio <= 0.75 : undefined },
    { id: "ledger-lag", val: health ? Number(health.kafkaLag || 0).toLocaleString() : "—", good: health ? true : undefined },
  ];
  for (const { id, val, good } of items) {
    const el = document.getElementById(id);
    if (el) el.textContent = val;
    const dot = el?.parentElement?.querySelector(".ledger-dot");
    if (dot) dot.className = `ledger-dot ${good === undefined ? "neutral" : good ? "good" : "bad"}`;
  }
  const cluster = state.cluster;
  const freezeEl = document.getElementById("ledger-freeze");
  const freezeDot = document.getElementById("ledger-freeze-dot");
  if (freezeEl) freezeEl.textContent = cluster ? (cluster.frozen ? "FROZEN" : "OPEN") : "—";
  if (freezeDot) freezeDot.className = `ledger-dot ${!cluster ? "neutral" : cluster.frozen ? "bad" : "good"}`;
}

function renderConfig() {
  const spec = state.actor?.currentVersion?.spec;
  const specEl = document.getElementById("config-spec");
  if (!spec) {
    specEl.innerHTML = '<div class="empty-state">No active version.</div>';
  } else {
    const rows = [
      ["Image", spec.imageDigest || "—"],
      ["Flink version", spec.flinkVersion || "—"],
      ["Git ref", spec.gitRef || "—"],
      ["Parallelism", spec.parallelism || "—"],
      ["Max parallelism", spec.maxParallelism || "—"],
    ];
    if (spec.resources) {
      rows.push(
        ["TM CPU", spec.resources.taskManagerCpu || "—"],
        ["TM Memory (MiB)", spec.resources.taskManagerMemoryMiB || "—"],
        ["TM Count", spec.resources.taskManagerCount || "—"],
        ["Slots/TM", spec.resources.slotsPerManager || "—"],
      );
    }
    if (spec.jobArgs && Object.keys(spec.jobArgs).length) {
      rows.push(["Job args", Object.entries(spec.jobArgs).map(([k, v]) => `${k}=${v}`).join(", ")]);
    }
    if (spec.flinkConfig && Object.keys(spec.flinkConfig).length) {
      rows.push(["Flink config", Object.entries(spec.flinkConfig).map(([k, v]) => `${k}=${v}`).join(", ")]);
    }
    specEl.innerHTML = rows.map(([k, v]) =>
      `<div class="config-row"><span class="config-key">${escapeHTML(k)}</span><span class="config-val">${escapeHTML(v)}</span></div>`
    ).join("");
  }
  const autoEl = document.getElementById("config-autoscaler");
  if (state.actor) {
    autoEl.innerHTML = `
      <div>Autoscaler: <strong>${state.actor.autoscalerEnabled ? "Enabled" : "Disabled"}</strong></div>
      <div>Frozen: <strong>${state.actor.autoscalerFrozen ? "Yes" : "No"}</strong></div>
    `;
  } else {
    autoEl.innerHTML = '<div class="empty-state">No data.</div>';
  }
}

function renderCluster() {
  const cluster = state.cluster;
  if (!cluster) {
    setBadge(elements.clusterStatus, "UNKNOWN", "neutral");
    elements.clusterDetail.textContent = "Load a target to inspect namespace mutation controls.";
    return;
  }
  setBadge(elements.clusterStatus, cluster.frozen ? "FROZEN" : "OPEN", cluster.frozen ? "danger" : "success");
  elements.clusterDetail.textContent = cluster.frozen
    ? `Frozen by ${cluster.requester || "unknown"}${cluster.reason ? `: ${cluster.reason}` : ""}`
    : "Runtime mutations are permitted for this namespace.";
  renderLedger();
}

function renderVersions() {
  if (!state.versions.length) {
    elements.versionsTable.innerHTML = '<tr><td colspan="6" class="empty-cell">No recorded versions.</td></tr>';
    return;
  }
  elements.versionsTable.innerHTML = state.versions.map((version) => `
    <tr>
      <td><strong>v${version.versionId}</strong></td>
      <td>${formatDate(version.createdAt)}</td>
      <td><code>${escapeHTML(shortDigest(version.spec.imageDigest))}</code></td>
      <td>${version.spec.parallelism}</td>
      <td><span class="status-badge ${version.healthSummary?.healthy ? "success" : "danger"}">${version.healthSummary?.healthy ? "HEALTHY" : "DEGRADED"}</span></td>
      <td><button class="text-button" data-rollback-version="${version.versionId}">Rollback</button></td>
    </tr>
  `).join("");
}

async function loadActiveTarget({ quiet = false } = {}) {
  if (!state.activeTarget) {
    if (!quiet) flash("Add or select a deployment target first.", "error");
    return;
  }
  document.querySelector("#refresh-button").disabled = true;
  try {
    const [actor, cluster] = await Promise.all([
      request(apiPath(state.activeTarget, "/actor")),
      request(clusterPath(state.activeTarget, "/actor")),
    ]);
    state.actor = actor;
    state.cluster = cluster;
    elements.lastUpdated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    renderActor();
    renderCluster();
  } catch (error) {
    state.actor = null;
    state.cluster = null;
    renderActor();
    renderCluster();
    if (!quiet) flash(error.message, "error");
  } finally {
    document.querySelector("#refresh-button").disabled = false;
  }
}

async function loadVersions() {
  if (!state.activeTarget) {
    flash("Select a deployment first.", "error");
    return;
  }
  try {
    state.versions = (await request(apiPath(state.activeTarget, "/versions"))) || [];
    renderVersions();
  } catch (error) {
    flash(error.message, "error");
  }
}

function activateTarget(target) {
  state.activeTarget = target;
  state.actor = null;
  state.cluster = null;
  state.versions = [];
  document.querySelector("#deploy-dropdown")?.classList.remove("open");
  renderTargets();
  renderActor();
  renderCluster();
  renderVersions();
  loadActiveTarget();
}

function switchView(name) {
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === `${name}-view`));
  document.querySelectorAll(".nav-item").forEach((item) => item.classList.toggle("active", item.dataset.view === name));
  if (name === "environments") {
    loadCards();
  }
}

function switchActorTab(tabId) {
  document.querySelectorAll(".actor-tab").forEach((t) => t.classList.toggle("active", t.dataset.actorTab === tabId));
  document.querySelectorAll(".actor-tab-panel").forEach((p) => p.classList.toggle("active", p.id === tabId));
  if (tabId === "tab-versions") loadVersions();
  if (tabId === "tab-config") renderConfig();
}

function openAction(command, options = {}) {
  if (!state.activeTarget) {
    flash("Select a deployment first.", "error");
    return;
  }
  const form = document.querySelector("#action-form");
  form.reset();
  form.elements.command.value = command;
  form.elements.requester.value = "operator";
  form.elements.idempotencyKey.value = generatedKey(command);
  form.elements.targetVersion.value = options.targetVersion || "";
  form.elements.parallelism.value = options.parallelism || "";
  const titles = {
    savepoint: "Create savepoint",
    suspend: "Suspend deployment",
    resume: "Resume deployment",
    rollback: "Rollback deployment",
    scale: "Scale deployment",
    "autoscaler-enable": "Enable autoscaler",
    "autoscaler-freeze": "Freeze autoscaler",
    "continue-as-new": "Compact actor history",
    freeze: "Freeze namespace",
    unfreeze: "Unfreeze namespace",
  };
  document.querySelector("#action-title").textContent = titles[command] || "Run operation";
  document.querySelector("#action-version-label").classList.toggle("hidden", command !== "rollback");
  document.querySelector("#action-parallelism-label").classList.toggle("hidden", command !== "scale");
  elements.actionDialog.showModal();
}

async function submitAction(form) {
  const data = new FormData(form);
  const command = data.get("command");

  if (command === "freeze" || command === "unfreeze") {
    try {
      await request(clusterPath(state.activeTarget, command === "freeze" ? "/freeze" : "/unfreeze"), {
        method: "POST",
        body: JSON.stringify({ requester: data.get("requester") || "operator", reason: data.get("reason") }),
      });
      elements.actionDialog.close();
      flash(command === "freeze" ? "Namespace freeze requested." : "Namespace unfreeze requested.");
      setTimeout(loadActiveTarget, 400);
    } catch (error) {
      flash(error.message, "error");
    }
    return;
  }

  const commandPath = {
    "autoscaler-enable": "autoscaler/enable",
    "autoscaler-freeze": "autoscaler/freeze",
    "continue-as-new": "continue-as-new",
  }[command] || command;
  const body = {
    requester: data.get("requester"),
    reason: data.get("reason"),
    approved: data.get("approved") === "on",
  };
  if (command === "rollback") body.targetVersion = Number(data.get("targetVersion"));
  if (command === "scale") body.parallelism = Number(data.get("parallelism"));
  try {
    const result = await request(apiPath(state.activeTarget, `/${commandPath}`), {
      method: "POST",
      headers: { "Idempotency-Key": data.get("idempotencyKey") },
      body: JSON.stringify(body),
    });
    elements.actionDialog.close();
    flash(`Command accepted: ${result.operationId}`);
    setTimeout(loadActiveTarget, 600);
  } catch (error) {
    flash(error.message, "error");
  }
}

async function submitDeployment(form) {
  if (!state.activeTarget) {
    flash("Select a deployment first.", "error");
    return;
  }
  const data = new FormData(form);
  try {
    const body = {
      requester: data.get("requester"),
      approved: data.get("approved") === "on",
      reason: data.get("reason"),
      spec: {
        imageDigest: data.get("imageDigest"),
        gitRef: data.get("gitRef"),
        flinkVersion: data.get("flinkVersion"),
        jobArgs: parsePairs(data.get("jobArgs")),
        flinkConfig: parsePairs(data.get("flinkConfig")),
        parallelism: Number(data.get("parallelism")),
        maxParallelism: Number(data.get("maxParallelism")),
        resources: {
          taskManagerCpu: Number(data.get("taskManagerCpu")),
          taskManagerMemoryMiB: Number(data.get("taskManagerMemoryMiB")),
          taskManagerCount: Number(data.get("taskManagerCount")),
          slotsPerManager: Number(data.get("slotsPerManager")),
        },
        stateCompatibility: {
          jobGraphCompatible: data.get("jobGraphCompatible") === "on",
          operatorUidsStable: data.get("operatorUidsStable") === "on",
          allowNonRestored: data.get("allowNonRestored") === "on",
          freshStartApproved: data.get("freshStartApproved") === "on",
        },
        autoscalerEnabled: data.get("autoscalerEnabled") === "on",
      },
    };
    const result = await request(apiPath(state.activeTarget, "/deploy"), {
      method: "POST",
      headers: { "Idempotency-Key": data.get("idempotencyKey") },
      body: JSON.stringify(body),
    });
    flash(`Rollout queued: ${result.operationId}`);
    switchView("overview");
    setTimeout(loadActiveTarget, 600);
  } catch (error) {
    flash(error.message, "error");
  }
}

// ponytail: regex YAML parser — handles name/namespace/labels/serviceAccount only. Use a real parser if you need more fields.
async function fetchYAML(url) {
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`Fetch failed: ${resp.status} ${resp.statusText}`);
  const yaml = await resp.text();
  const get = (pattern) => (yaml.match(pattern) || [])[1]?.trim() || "";
  return {
    name: get(/^\s{2,4}name:\s+(.+)$/m),
    namespace: get(/^\s{2,4}namespace:\s+(.+)$/m),
    environment: get(/(?:maestro\.flink\/environment|flink\.io\/environment|environment):\s+(.+)/),
    serviceAccount: get(/serviceAccountName?:\s+(.+)/),
  };
}

function escapeHTML(value) {
  return String(value ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;",
  }[c]));
}

// --- event handlers ---

document.addEventListener("click", (event) => {
  const actorTabBtn = event.target.closest("[data-actor-tab]");
  if (actorTabBtn) { switchActorTab(actorTabBtn.dataset.actorTab); return; }

  const viewButton = event.target.closest("[data-view], [data-view-link]");
  if (viewButton) { switchView(viewButton.dataset.view || viewButton.dataset.viewLink); return; }

  const cardBtn = event.target.closest("[data-card-target]");
  if (cardBtn) {
    const target = state.targets.find((t) => targetKey(t) === cardBtn.dataset.cardTarget);
    if (target) { activateTarget(target); switchView("overview"); switchActorTab("tab-overview"); }
    return;
  }

  const targetButton = event.target.closest("[data-target]");
  if (targetButton) {
    const target = state.targets.find((t) => targetKey(t) === targetButton.dataset.target);
    if (target) activateTarget(target);
    return;
  }

  const envRegisterBtn = event.target.closest("[data-env-register]");
  if (envRegisterBtn) {
    const form = document.querySelector("#target-form");
    form.reset();
    form.elements.environment.value = envRegisterBtn.dataset.envRegister;
    elements.targetDialog.showModal();
  }

  const commandButton = event.target.closest("[data-command]");
  if (commandButton) openAction(commandButton.dataset.command);

  const rollbackButton = event.target.closest("[data-open-rollback]");
  if (rollbackButton) openAction("rollback", { targetVersion: state.actor?.lastHealthyVersion?.versionId });

  const versionRollback = event.target.closest("[data-rollback-version]");
  if (versionRollback) openAction("rollback", { targetVersion: versionRollback.dataset.rollbackVersion });

  if (event.target.closest("[data-open-deploy]")) {
    const form = document.querySelector("#deploy-form");
    form.elements.idempotencyKey.value = generatedKey("deploy");
    const spec = state.actor?.currentVersion?.spec;
    if (spec) {
      form.elements.imageDigest.value = spec.imageDigest || "";
      form.elements.flinkVersion.value = spec.flinkVersion || "2.2";
      form.elements.gitRef.value = spec.gitRef || "";
      form.elements.parallelism.value = spec.parallelism || 4;
      form.elements.maxParallelism.value = spec.maxParallelism || 128;
      const r = spec.resources || {};
      form.elements.taskManagerCpu.value = r.taskManagerCpu || 2;
      form.elements.taskManagerMemoryMiB.value = r.taskManagerMemoryMiB || 4096;
      form.elements.taskManagerCount.value = r.taskManagerCount || 2;
      form.elements.slotsPerManager.value = r.slotsPerManager || 4;
      form.elements.jobArgs.value = Object.entries(spec.jobArgs || {}).map(([k, v]) => `${k}=${v}`).join("\n");
      form.elements.flinkConfig.value = Object.entries(spec.flinkConfig || {}).map(([k, v]) => `${k}=${v}`).join("\n");
      form.elements.autoscalerEnabled.checked = !!spec.autoscalerEnabled;
      const s = spec.stateCompatibility || {};
      form.elements.jobGraphCompatible.checked = s.jobGraphCompatible !== false;
      form.elements.operatorUidsStable.checked = s.operatorUidsStable !== false;
      form.elements.allowNonRestored.checked = !!s.allowNonRestored;
      form.elements.freshStartApproved.checked = !!s.freshStartApproved;
      flash(`Deploy form pre-filled from v${state.actor.currentVersion.versionId}.`);
    }
    switchView("deploy");
  }
});

document.querySelector("#theme-toggle").addEventListener("click", () => {
  const dark = document.documentElement.getAttribute("data-theme") === "dark";
  if (dark) {
    document.documentElement.removeAttribute("data-theme");
    localStorage.setItem("maestro.theme", "light");
    document.querySelector("#theme-toggle").textContent = "☀️";
  } else {
    document.documentElement.setAttribute("data-theme", "dark");
    localStorage.setItem("maestro.theme", "dark");
    document.querySelector("#theme-toggle").textContent = "🌙";
  }
});
// Set initial icon
if (document.documentElement.getAttribute("data-theme") === "dark") {
  document.querySelector("#theme-toggle").textContent = "🌙";
}

document.querySelector("#dropdown-add-btn")?.addEventListener("click", () => {
  elements.targetDialog.showModal();
  document.querySelector("#deploy-dropdown")?.classList.remove("open");
});
document.querySelector("#auto-refresh-btn").addEventListener("click", () => setAutoRefresh(!autoRefreshTimer));
document.querySelector("#refresh-button").addEventListener("click", () => {
  loadActiveTarget();
  if (autoRefreshTimer) setAutoRefresh(true);
});
document.querySelector("#load-versions-button").addEventListener("click", loadVersions);
document.querySelector("#freeze-button").addEventListener("click", () => openAction("freeze"));
document.querySelector("#unfreeze-button").addEventListener("click", () => openAction("unfreeze"));
elements.targetSearch.addEventListener("input", (event) => {
  state.targetFilter = event.target.value;
  renderTargets();
});

document.querySelector("#fetch-yaml-btn").addEventListener("click", async () => {
  const urlInput = document.querySelector('[name="yamlUrl"]');
  const url = urlInput.value.trim();
  if (!url) { flash("Enter a YAML URL first.", "error"); return; }
  const btn = document.querySelector("#fetch-yaml-btn");
  btn.textContent = "Fetching…";
  btn.disabled = true;
  try {
    const parsed = await fetchYAML(url);
    const form = document.querySelector("#target-form");
    if (parsed.name) form.elements.name.value = parsed.name;
    if (parsed.namespace) form.elements.namespace.value = parsed.namespace;
    if (parsed.environment) form.elements.environment.value = parsed.environment;
    if (parsed.serviceAccount) form.elements.serviceAccount.value = parsed.serviceAccount;
    flash("YAML imported — verify fields then register.");
  } catch (e) {
    flash(e.message, "error");
  } finally {
    btn.textContent = "Fetch & import";
    btn.disabled = false;
  }
});

document.querySelector("#target-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (event.submitter?.value === "cancel") { elements.targetDialog.close(); return; }
  const form = event.currentTarget;
  const data = new FormData(form);
  const target = Object.fromEntries(data.entries());
  try {
    await request(apiPath(target), {
      method: "PUT",
      body: JSON.stringify({
        owner: target.owner,
        serviceAccount: target.serviceAccount,
        nodePool: target.nodePool,
        flinkDashboardUrl: target.flinkDashboardUrl,
      }),
    });
    const existingIndex = state.targets.findIndex((item) => targetKey(item) === targetKey(target));
    if (existingIndex >= 0) state.targets[existingIndex] = target;
    else state.targets.push(target);
    saveTargets();
    elements.targetDialog.close();
    await loadDeploymentInventory([target]);
    flash(`Registered ${target.name}.`);
  } catch (error) {
    flash(error.message, "error");
  }
});

document.querySelector("#action-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (event.submitter?.value === "cancel") { elements.actionDialog.close(); return; }
  submitAction(event.currentTarget);
});

document.querySelector("#deploy-form").addEventListener("submit", (event) => {
  event.preventDefault();
  submitDeployment(event.currentTarget);
});

// --- command palette ---

const commands = [
  { id: "scale", label: "Scale deployment", shortcut: "" },
  { id: "savepoint", label: "Create savepoint", shortcut: "" },
  { id: "suspend", label: "Suspend deployment", shortcut: "" },
  { id: "resume", label: "Resume deployment", shortcut: "" },
  { id: "rollback", label: "Rollback deployment", shortcut: "" },
  { id: "freeze", label: "Freeze namespace", shortcut: "" },
  { id: "unfreeze", label: "Unfreeze namespace", shortcut: "" },
  { id: "autoscaler-enable", label: "Enable autoscaler", shortcut: "" },
  { id: "autoscaler-freeze", label: "Freeze autoscaler", shortcut: "" },
  { id: "continue-as-new", label: "Compact history", shortcut: "" },
  { id: "nav-environments", label: "Go to Environments", shortcut: "" },
  { id: "nav-overview", label: "Go to Actor overview", shortcut: "" },
  { id: "nav-deploy", label: "Go to Deploy form", shortcut: "" },
  { id: "toggle-theme", label: "Toggle dark/light mode", shortcut: "" },
  { id: "refresh", label: "Refresh data", shortcut: "" },
  { id: "add-deployment", label: "Register new deployment", shortcut: "" },
];

function openCmdPalette() {
  const dialog = document.querySelector("#cmd-palette");
  if (!dialog) return;
  dialog.showModal();
  const search = document.querySelector("#cmd-search");
  search.value = "";
  search.focus();
  renderCmdResults("");
}

function renderCmdResults(filter) {
  const results = document.querySelector("#cmd-results");
  const filtered = commands.filter(c =>
    c.label.toLowerCase().includes(filter.toLowerCase())
  );
  results.innerHTML = filtered.map((cmd, i) => `
    <div class="cmd-result${i === 0 ? " active" : ""}" data-cmd="${cmd.id}">
      <span class="cmd-label">${cmd.label}</span>
      ${cmd.shortcut ? `<span class="cmd-shortcut">${cmd.shortcut}</span>` : ""}
    </div>
  `).join("");
}

function executeCmdPaletteAction(id) {
  const dialog = document.querySelector("#cmd-palette");
  dialog?.close();

  if (id.startsWith("nav-")) {
    switchView(id.replace("nav-", ""));
    return;
  }
  if (id === "toggle-theme") {
    document.querySelector("#theme-toggle")?.click();
    return;
  }
  if (id === "refresh") {
    loadActiveTarget();
    return;
  }
  if (id === "add-deployment") {
    document.querySelector("#target-dialog")?.showModal();
    return;
  }
  // All other commands open the action dialog
  openAction(id);
}

// Command palette event listeners
document.querySelector("#cmd-palette-btn")?.addEventListener("click", openCmdPalette);

document.querySelector("#cmd-search")?.addEventListener("input", (e) => {
  renderCmdResults(e.target.value);
});

document.querySelector("#cmd-results")?.addEventListener("click", (e) => {
  const result = e.target.closest("[data-cmd]");
  if (result) executeCmdPaletteAction(result.dataset.cmd);
});

// Keyboard navigation in command palette
document.querySelector("#cmd-search")?.addEventListener("keydown", (e) => {
  const results = document.querySelectorAll(".cmd-result");
  const active = document.querySelector(".cmd-result.active");
  const activeIndex = [...results].indexOf(active);

  if (e.key === "ArrowDown") {
    e.preventDefault();
    results[activeIndex]?.classList.remove("active");
    results[Math.min(activeIndex + 1, results.length - 1)]?.classList.add("active");
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    results[activeIndex]?.classList.remove("active");
    results[Math.max(activeIndex - 1, 0)]?.classList.add("active");
  } else if (e.key === "Enter") {
    e.preventDefault();
    const sel = document.querySelector(".cmd-result.active");
    if (sel) executeCmdPaletteAction(sel.dataset.cmd);
  }
});

// Global Cmd+K / Ctrl+K shortcut
document.addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "k") {
    e.preventDefault();
    openCmdPalette();
  }
  if (e.key === "Escape") {
    document.querySelector("#deploy-dropdown")?.classList.remove("open");
  }
});

// Close cmd palette on backdrop click
document.querySelector("#cmd-palette")?.addEventListener("click", (e) => {
  if (e.target === e.currentTarget) e.currentTarget.close();
});

// --- init ---

renderTargets();
renderActor();
renderCluster();
renderVersions();
document.querySelector('[name="idempotencyKey"]').value = generatedKey("deploy");
loadDeploymentInventory();
loadCards();
