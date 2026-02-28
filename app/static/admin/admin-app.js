const ORG_TYPES = [
  "gallery",
  "museum",
  "cinema",
  "bookshop",
  "cultural centre",
  "art centre",
  "house",
  "social community center",
  "other",
];

const app = document.getElementById("app");

let state = window.APP_INITIAL_STATE || {
  stats: { pending: 0, approved: 0, maybe: 0, rejected: 0, total: 0, active_total: 0, queue_total: 0, open_issues: 0 },
  queue_total: 0,
  queue: [],
  active_orgs: [],
  strategies: [],
  discovery_latest: null,
  discovery_runs: [],
};

const ui = {
  tab: "queue",
  currentQueueId: state.queue?.[0]?.id ?? null,
  notice: "",
  isBusy: false,
  queueDrafts: {},
  strategyDraft: "",
  manualDraft: {
    name: "",
    homepage: "",
    events_url: "",
    borough: "",
    category: "gallery",
    description: "",
  },
};

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDate(value) {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function setNotice(message) {
  ui.notice = message;
}

function syncCurrentQueue() {
  const queue = state.queue || [];
  if (!queue.length) {
    ui.currentQueueId = null;
    return;
  }

  const exists = queue.some((item) => item.id === ui.currentQueueId);
  if (!exists) {
    ui.currentQueueId = queue[0].id;
  }
}

function ensureQueueDraft(org) {
  if (!org) return { feedback: "", events_url: "" };
  if (!ui.queueDrafts[org.id]) {
    ui.queueDrafts[org.id] = {
      feedback: org.notes || "",
      events_url: org.events_url || "",
    };
  }
  return ui.queueDrafts[org.id];
}

async function apiRequest(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload.error || `Request failed (${response.status})`;
    throw new Error(message);
  }
  return payload;
}

async function refreshState() {
  ui.isBusy = true;
  render();
  try {
    state = await apiRequest("/api/admin/state");
    syncCurrentQueue();
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function saveQueueAction(action) {
  const current = (state.queue || []).find((item) => item.id === ui.currentQueueId);
  if (!current) return;

  const draft = ensureQueueDraft(current);

  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest(`/api/admin/review/${current.id}`, {
      method: "POST",
      body: JSON.stringify({
        action,
        feedback: (draft.feedback || "").trim(),
        events_url: (draft.events_url || "").trim(),
      }),
    });

    state = payload.state;
    syncCurrentQueue();

    if (action === "resolve") {
      setNotice("Issue marked resolved.");
    } else if (action === "snooze") {
      setNotice("Issue snoozed.");
    } else {
      setNotice("Issue kept open.");
    }
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function addManualOrg(form) {
  const body = {
    name: form.name.value.trim(),
    homepage: form.homepage.value.trim(),
    events_url: form.events_url.value.trim(),
    borough: form.borough.value.trim(),
    category: form.category.value,
    description: form.description.value.trim(),
    source: "manual",
  };

  if (!body.name || !body.homepage || !body.borough) {
    setNotice("Name, homepage, and borough are required.");
    render();
    return;
  }

  ui.isBusy = true;
  render();
  try {
    await apiRequest("/api/orgs", {
      method: "POST",
      body: JSON.stringify(body),
    });

    ui.manualDraft = {
      name: "",
      homepage: "",
      events_url: "",
      borough: "",
      category: "gallery",
      description: "",
    };

    await refreshState();
    setNotice(`Added "${body.name}".`);
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function saveStrategy() {
  const text = ui.strategyDraft.trim();
  if (!text) {
    setNotice("Write your strategy note first.");
    render();
    return;
  }

  ui.isBusy = true;
  render();
  try {
    await apiRequest("/api/admin/strategies", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
    ui.strategyDraft = "";
    await refreshState();
    setNotice("Strategy note saved.");
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function toggleStrategy(strategyId, active) {
  ui.isBusy = true;
  render();
  try {
    await apiRequest(`/api/admin/strategies/${strategyId}`, {
      method: "PATCH",
      body: JSON.stringify({ active }),
    });
    await refreshState();
  } catch (error) {
    setNotice(error.message);
    ui.isBusy = false;
    render();
  }
}

async function runDiscoveryNow() {
  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest("/api/admin/discovery/run", {
      method: "POST",
      body: JSON.stringify({
        max_queries: 10,
        max_results_per_query: 6,
        max_candidates: 30,
        request_timeout: 10,
      }),
    });

    state = payload.state || state;
    const summary = payload.summary || {};
    if (summary.status === "skipped") {
      setNotice(summary.reason || "Discovery skipped.");
    } else {
      const upsertedCount = Number(summary.upserted_count || 0);
      const candidateCount = Number(summary.candidate_count || 0);
      setNotice(`Discovery complete: ${upsertedCount} upserted from ${candidateCount} candidates.`);
    }
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

function renderQueue() {
  const queue = state.queue || [];
  const current = queue.find((item) => item.id === ui.currentQueueId) || null;

  const list = queue
    .map((item) => {
      const active = item.id === ui.currentQueueId;
      return `
      <button class="queue-item ${active ? "active" : ""}" data-action="pick-queue" data-id="${item.id}">
        <div class="queue-item-head">
          <strong>${escapeHtml(item.name)}</strong>
          <span>#${escapeHtml(item.id)}</span>
        </div>
        <p class="queue-reason">${escapeHtml(item.queue_reason || "Needs review")}</p>
      </button>`;
    })
    .join("");

  let card = '<div class="empty-card">No orgs currently need manual review.</div>';

  if (current) {
    const draft = ensureQueueDraft(current);
    card = `
      <article class="candidate-card">
        <div class="candidate-head">
          <div>
            <p class="candidate-index">Issue queue item</p>
            <h3>${escapeHtml(current.name)}</h3>
          </div>
          <span class="status pending">${escapeHtml(current.issue_state || "open")}</span>
        </div>

        <div class="meta-grid">
          <div><label>Reason</label><p>${escapeHtml(current.queue_reason || "-")}</p></div>
          <div><label>Borough</label><p>${escapeHtml(current.borough || "-")}</p></div>
          <div><label>Type</label><p>${escapeHtml(current.category || "-")}</p></div>
          <div><label>Last crawled</label><p>${escapeHtml(formatDate(current.last_crawled_at))}</p></div>
          <div><label>Last successful extract</label><p>${escapeHtml(formatDate(current.last_successful_event_extract_at))}</p></div>
          <div><label>Failure/empty streak</label><p>${escapeHtml(current.consecutive_failures || 0)} / ${escapeHtml(current.consecutive_empty_extracts || 0)}</p></div>
        </div>

        <div class="links">
          ${current.homepage ? `<a href="${escapeHtml(current.homepage)}" target="_blank" rel="noreferrer">Open website</a>` : ""}
          ${current.events_url ? `<a href="${escapeHtml(current.events_url)}" target="_blank" rel="noreferrer">Open events page</a>` : ""}
        </div>

        <label class="feedback-label" for="queue-events-url">Events URL fix (optional)</label>
        <input id="queue-events-url" data-action="queue-events-url-input" data-id="${current.id}" value="${escapeHtml(draft.events_url)}" placeholder="https://.../events" />

        <label class="feedback-label" for="queue-feedback">Admin note</label>
        <textarea id="queue-feedback" data-action="queue-feedback-input" data-id="${current.id}" placeholder="What did you change or decide?">${escapeHtml(draft.feedback)}</textarea>

        <div class="card-actions">
          <button class="primary-btn" data-action="queue-save" data-mode="resolve" ${ui.isBusy ? "disabled" : ""}>Mark resolved</button>
          <button class="ghost-btn" data-action="queue-save" data-mode="snooze" ${ui.isBusy ? "disabled" : ""}>Snooze</button>
          <button class="ghost-btn" data-action="queue-save" data-mode="open" ${ui.isBusy ? "disabled" : ""}>Keep open</button>
        </div>
      </article>
    `;
  }

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Review Queue</h2>
        <p>Traditional rolling queue. Only orgs with active crawl/event-source issues appear here.</p>
      </header>

      <div class="queue-layout">
        <div class="queue-list">
          ${list || '<div class="empty-card">Queue is empty.</div>'}
        </div>
        <div class="queue-card">${card}</div>
      </div>
    </section>
  `;
}

function renderActive() {
  const rows = (state.active_orgs || [])
    .map(
      (item) => `
      <tr>
        <td>${escapeHtml(item.name)}</td>
        <td>${escapeHtml(item.borough || "-")}</td>
        <td>${escapeHtml(item.category || "-")}</td>
        <td>${item.events_url ? `<a href="${escapeHtml(item.events_url)}" target="_blank" rel="noreferrer">Link</a>` : "-"}</td>
        <td>${escapeHtml(formatDate(item.created_at))}</td>
        <td>${escapeHtml(formatDate(item.last_crawled_at))}</td>
        <td>${escapeHtml(formatDate(item.last_successful_event_extract_at))}</td>
        <td>${escapeHtml(item.consecutive_failures || 0)}</td>
        <td>${escapeHtml(item.consecutive_empty_extracts || 0)}</td>
        <td>${item.is_new ? "New" : ""}</td>
      </tr>
    `
    )
    .join("");

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>All Active Orgs</h2>
        <p>Flat table extract.</p>
      </header>

      <div class="table-wrap">
        <table class="flat-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Borough</th>
              <th>Type</th>
              <th>Events URL</th>
              <th>Created At</th>
              <th>Last Crawled</th>
              <th>Last Success</th>
              <th>Failures</th>
              <th>Empty</th>
              <th>New</th>
            </tr>
          </thead>
          <tbody>
            ${rows || '<tr><td colspan="10">No active orgs.</td></tr>'}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderAdd() {
  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Add Specific Organization</h2>
        <p>Manual additions go straight into the main org database.</p>
      </header>

      <form class="stack-form" data-action="manual-submit">
        <label>Organization name
          <input name="name" value="${escapeHtml(ui.manualDraft.name)}" placeholder="e.g. The Common Press" required />
        </label>
        <label>Homepage
          <input name="homepage" type="url" value="${escapeHtml(ui.manualDraft.homepage)}" placeholder="https://..." required />
        </label>
        <label>Events URL (optional)
          <input name="events_url" type="url" value="${escapeHtml(ui.manualDraft.events_url)}" placeholder="https://.../events" />
        </label>
        <label>Borough
          <input name="borough" value="${escapeHtml(ui.manualDraft.borough)}" placeholder="Hackney" required />
        </label>
        <label>Category
          <select name="category">
            ${ORG_TYPES.map((item) => `<option value="${escapeHtml(item)}" ${ui.manualDraft.category === item ? "selected" : ""}>${escapeHtml(item)}</option>`).join("")}
          </select>
        </label>
        <label>Description
          <textarea name="description" placeholder="Why this source matters.">${escapeHtml(ui.manualDraft.description)}</textarea>
        </label>
        <button class="primary-btn" type="submit" ${ui.isBusy ? "disabled" : ""}>Add org</button>
      </form>
    </section>
  `;
}

function renderStrategies() {
  const strategies = state.strategies || [];
  const strategyItems = strategies
    .map(
      (item) => `
      <div class="strategy-item">
        <button class="toggle ${item.active ? "on" : "off"}" data-action="toggle-strategy" data-id="${item.id}" data-next="${item.active ? "false" : "true"}">
          ${item.active ? "Active" : "Paused"}
        </button>
        <div>
          <p>${escapeHtml(item.text)}</p>
          <small>${escapeHtml(formatDate(item.created_at))}</small>
        </div>
      </div>
    `
    )
    .join("");

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Search Strategy Log</h2>
        <p>Freeform strategy notes that guide future org discovery.</p>
      </header>

      <label class="feedback-label" for="strategy-input">New strategy note</label>
      <textarea id="strategy-input" data-action="strategy-input" placeholder="Example: prioritize non-commercial art spaces in East London.">${escapeHtml(ui.strategyDraft)}</textarea>
      <button class="primary-btn" data-action="save-strategy" ${ui.isBusy ? "disabled" : ""}>Save strategy note</button>

      <section class="strategy-list">
        <h3>Saved strategy notes (${strategies.length})</h3>
        ${strategyItems || '<p class="empty-small">No strategy notes yet.</p>'}
      </section>
    </section>
  `;
}

function runStatusMeta(status) {
  if (status === "success") return { label: "Success", className: "approved" };
  if (status === "failed") return { label: "Failed", className: "rejected" };
  return { label: "Running", className: "pending" };
}

function renderDiscovery() {
  const latest = state.discovery_latest || null;
  const runs = state.discovery_runs || [];
  const latestMeta = runStatusMeta(String(latest?.status || "running"));
  const latestDetails = latest?.details || {};

  const latestSummary = latest
    ? `
      <div class="strategy-summary">
        <h3>Latest run</h3>
        <p><strong>Run #${escapeHtml(latest.id)}</strong> · ${escapeHtml(String(latest.trigger || "manual"))}</p>
        <p><span class="status ${latestMeta.className}">${latestMeta.label}</span></p>
        <p>Started: ${escapeHtml(formatDate(latest.started_at))}</p>
        <p>Finished: ${escapeHtml(formatDate(latest.finished_at))}</p>
        <p>Queries: ${escapeHtml(latest.query_count ?? latestDetails.query_count ?? "-")}</p>
        <p>Candidates: ${escapeHtml(latest.result_count ?? latestDetails.candidate_count ?? "-")}</p>
        <p>Upserted: ${escapeHtml(latest.upserted_count ?? latestDetails.upserted_count ?? "-")}</p>
        <p>Query errors: ${escapeHtml(latestDetails.query_errors ?? "-")}</p>
        ${latest.error ? `<p>Error: ${escapeHtml(latest.error)}</p>` : ""}
      </div>
    `
    : '<div class="empty-card">No discovery runs recorded yet.</div>';

  const rows = runs
    .map((run) => {
      const details = run.details || {};
      const meta = runStatusMeta(String(run.status || "running"));
      return `
        <tr>
          <td>${escapeHtml(run.id)}</td>
          <td>${escapeHtml(run.trigger || "-")}</td>
          <td><span class="status ${meta.className}">${meta.label}</span></td>
          <td>${escapeHtml(formatDate(run.started_at))}</td>
          <td>${escapeHtml(formatDate(run.finished_at))}</td>
          <td>${escapeHtml(run.query_count ?? details.query_count ?? "-")}</td>
          <td>${escapeHtml(run.result_count ?? details.candidate_count ?? "-")}</td>
          <td>${escapeHtml(run.upserted_count ?? details.upserted_count ?? "-")}</td>
          <td>${escapeHtml(details.query_errors ?? "-")}</td>
        </tr>
      `;
    })
    .join("");

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Discovery</h2>
        <p>Run and monitor automatic org discovery jobs.</p>
      </header>

      <div class="card-actions">
        <button class="primary-btn" data-action="run-discovery" ${ui.isBusy ? "disabled" : ""}>Run discovery now</button>
      </div>

      ${latestSummary}

      <section class="strategy-list">
        <h3>Recent runs (${runs.length})</h3>
        <div class="table-wrap">
          <table class="flat-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Trigger</th>
                <th>Status</th>
                <th>Started</th>
                <th>Finished</th>
                <th>Queries</th>
                <th>Candidates</th>
                <th>Upserted</th>
                <th>Errors</th>
              </tr>
            </thead>
            <tbody>
              ${rows || '<tr><td colspan="9">No runs yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </section>
    </section>
  `;
}

function render() {
  syncCurrentQueue();

  const body =
    ui.tab === "queue"
      ? renderQueue()
      : ui.tab === "active"
        ? renderActive()
        : ui.tab === "add"
          ? renderAdd()
          : ui.tab === "strategy"
            ? renderStrategies()
            : renderDiscovery();

  const queueTotal = state.queue_total || state.queue?.length || 0;
  const activeTotal = state.stats?.active_total || state.active_orgs?.length || 0;

  app.innerHTML = `
    <div class="curation-shell">
      <header class="top">
        <div class="top-row">
          <div>
            <p class="eyebrow">London Tasteful Events</p>
            <h1>Org Curation Console</h1>
            <p class="sub">Rolling issue queue + flat active-org extract.</p>
          </div>
          <button class="ghost-btn" data-action="refresh" ${ui.isBusy ? "disabled" : ""}>Refresh</button>
        </div>

        <div class="metrics">
          <div class="metric"><span>Queue</span><strong>${escapeHtml(queueTotal)}</strong></div>
          <div class="metric"><span>Active Orgs</span><strong>${escapeHtml(activeTotal)}</strong></div>
          <div class="metric"><span>Open Issues</span><strong>${escapeHtml(state.stats?.open_issues || 0)}</strong></div>
          <div class="metric"><span>Total</span><strong>${escapeHtml(state.stats?.total || 0)}</strong></div>
        </div>
      </header>

      <nav class="tab-nav">
        <button class="tab ${ui.tab === "queue" ? "active" : ""}" data-action="switch-tab" data-tab="queue">Review Queue</button>
        <button class="tab ${ui.tab === "active" ? "active" : ""}" data-action="switch-tab" data-tab="active">All Active Orgs</button>
        <button class="tab ${ui.tab === "add" ? "active" : ""}" data-action="switch-tab" data-tab="add">Add Org</button>
        <button class="tab ${ui.tab === "strategy" ? "active" : ""}" data-action="switch-tab" data-tab="strategy">Strategies</button>
        <button class="tab ${ui.tab === "discovery" ? "active" : ""}" data-action="switch-tab" data-tab="discovery">Discovery</button>
      </nav>

      ${ui.notice ? `<div class="notice">${escapeHtml(ui.notice)}</div>` : ""}
      ${body}
    </div>
  `;
}

app.addEventListener("click", async (event) => {
  const target = event.target.closest("[data-action]");
  if (!target) return;

  const action = target.dataset.action;

  if (action === "switch-tab") {
    ui.tab = target.dataset.tab;
    render();
    return;
  }

  if (action === "refresh") {
    await refreshState();
    return;
  }

  if (action === "pick-queue") {
    ui.currentQueueId = Number(target.dataset.id);
    render();
    return;
  }

  if (action === "queue-save") {
    await saveQueueAction(target.dataset.mode || "resolve");
    return;
  }

  if (action === "save-strategy") {
    await saveStrategy();
    return;
  }

  if (action === "toggle-strategy") {
    await toggleStrategy(Number(target.dataset.id), target.dataset.next === "true");
    return;
  }

  if (action === "run-discovery") {
    await runDiscoveryNow();
  }
});

app.addEventListener("input", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.dataset.action === "queue-feedback-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "" };
    draft.feedback = target.value;
    ui.queueDrafts[id] = draft;
    return;
  }

  if (target.dataset.action === "queue-events-url-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "" };
    draft.events_url = target.value;
    ui.queueDrafts[id] = draft;
    return;
  }

  if (target.id === "strategy-input") {
    ui.strategyDraft = target.value;
    return;
  }

  const form = target.closest("form[data-action='manual-submit']");
  if (form && target.name && target.name in ui.manualDraft) {
    ui.manualDraft[target.name] = target.value;
  }
});

app.addEventListener("submit", async (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) return;
  if (form.dataset.action !== "manual-submit") return;
  event.preventDefault();
  await addManualOrg(form);
});

render();
