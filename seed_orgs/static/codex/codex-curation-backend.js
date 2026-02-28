const BATCH_SIZE = 20;

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

let state = window.CODEX_INITIAL_STATE || {
  batch_number: 1,
  batch_size: 0,
  reviewed_count: 0,
  pending_total: 0,
  batch_complete: false,
  stats: { pending: 0, approved: 0, maybe: 0, rejected: 0, total: 0 },
  active_batch: [],
  strategies: [],
  approved_preview: [],
};

const ui = {
  tab: "queue",
  currentCandidateId: state.active_batch[0]?.id ?? null,
  notice: "",
  isBusy: false,
  feedbackDrafts: {},
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

function statusLabel(status) {
  if (status === "approved") return "Approved";
  if (status === "rejected") return "Rejected";
  if (status === "maybe") return "Parked";
  return "Pending";
}

function statusClass(status) {
  if (status === "approved") return "approved";
  if (status === "rejected") return "rejected";
  if (status === "maybe") return "parked";
  return "pending";
}

function formatDate(value) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function setNotice(message) {
  ui.notice = message;
}

function syncCurrentCandidate() {
  const batch = state.active_batch || [];
  if (!batch.length) {
    ui.currentCandidateId = null;
    return;
  }

  const exists = batch.some((item) => item.id === ui.currentCandidateId);
  if (exists) return;

  const firstPending = batch.find((item) => item.status === "pending");
  ui.currentCandidateId = firstPending ? firstPending.id : batch[0].id;
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
    state = await apiRequest(`/api/codex/state?batch_size=${BATCH_SIZE}`);
    syncCurrentCandidate();
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function saveReview() {
  const candidate = (state.active_batch || []).find((item) => item.id === ui.currentCandidateId);
  if (!candidate) return;

  const feedback = (ui.feedbackDrafts[candidate.id] || "").trim();
  if (!feedback) {
    setNotice("Write a quick freeform note first.");
    render();
    return;
  }

  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest(`/api/codex/review/${candidate.id}`, {
      method: "POST",
      body: JSON.stringify({ feedback }),
    });

    state = payload.state;
    const nextPending = (state.active_batch || []).find((item) => item.status === "pending");
    if (nextPending) ui.currentCandidateId = nextPending.id;
    setNotice(`Saved as ${statusLabel(payload.status).toLowerCase()}.`);
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function loadNextBatch() {
  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest("/api/codex/next-batch", {
      method: "POST",
      body: JSON.stringify({ batch_size: BATCH_SIZE }),
    });
    state = payload.state;
    ui.currentCandidateId = state.active_batch[0]?.id ?? null;
    setNotice(`Loaded batch #${state.batch_number}.`);
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
    setNotice(`Added \"${body.name}\".`);
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
    await apiRequest("/api/codex/strategies", {
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
    await apiRequest(`/api/codex/strategies/${strategyId}`, {
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

function renderQueue() {
  const batch = state.active_batch || [];
  const reviewedCount = state.reviewed_count || 0;
  const current = batch.find((item) => item.id === ui.currentCandidateId) || null;

  const chips = batch
    .map((item, index) => {
      const active = item.id === ui.currentCandidateId;
      const reviewed = item.status !== "pending";
      const badge = reviewed ? statusLabel(item.status).charAt(0) : "•";
      return `
      <button class="batch-chip ${active ? "active" : ""} ${reviewed ? "done" : ""}" data-action="pick-candidate" data-id="${item.id}">
        ${index + 1}<span>${badge}</span>
      </button>`;
    })
    .join("");

  let card = '<div class="empty-card">No candidate available in this batch.</div>';
  if (current) {
    const currentIndex = batch.findIndex((item) => item.id === current.id);
    const feedback = ui.feedbackDrafts[current.id] ?? current.notes ?? "";

    card = `
      <article class="candidate-card">
        <div class="candidate-head">
          <div>
            <p class="candidate-index">Candidate ${currentIndex + 1} of ${batch.length}</p>
            <h3>${escapeHtml(current.name)}</h3>
          </div>
          <span class="status ${statusClass(current.status)}">${statusLabel(current.status)}</span>
        </div>

        <div class="meta-grid">
          <div><label>Borough</label><p>${escapeHtml(current.borough || "-")}</p></div>
          <div><label>Category</label><p>${escapeHtml(current.category || "-")}</p></div>
          <div><label>Source</label><p>${escapeHtml(current.source || "-")}</p></div>
          <div><label>Reviewed</label><p>${escapeHtml(formatDate(current.reviewed_at) || "Not yet")}</p></div>
        </div>

        <p class="candidate-note">${escapeHtml(current.description || "No description yet.")}</p>

        <div class="links">
          ${current.homepage ? `<a href="${escapeHtml(current.homepage)}" target="_blank" rel="noreferrer">Open website</a>` : ""}
          ${current.events_url ? `<a href="${escapeHtml(current.events_url)}" target="_blank" rel="noreferrer">Open events page</a>` : ""}
        </div>

        <label class="feedback-label" for="feedback-input">Your freeform feedback</label>
        <textarea id="feedback-input" data-action="feedback-input" placeholder="Approve/reject/park in plain text; include corrections when needed.">${escapeHtml(feedback)}</textarea>

        <div class="card-actions">
          <button class="primary-btn" data-action="save-review" ${ui.isBusy ? "disabled" : ""}>Interpret and save note</button>
          <button class="ghost-btn" data-action="next-candidate" ${currentIndex >= batch.length - 1 ? "disabled" : ""}>Next candidate</button>
        </div>
      </article>
    `;
  }

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Batch Review (Max 20)</h2>
        <p>No new orgs appear until every org in this batch is reviewed.</p>
      </header>

      <div class="progress">
        <div class="progress-bar" style="width: ${batch.length ? (reviewedCount / batch.length) * 100 : 0}%"></div>
      </div>

      <div class="batch-strip">${chips}</div>
      ${card}

      <footer class="batch-footer">
        <button class="primary-btn" data-action="load-next-batch" ${state.batch_complete && !ui.isBusy ? "" : "disabled"}>Load next batch</button>
        <p>${state.batch_complete ? "Batch complete. Next batch will apply your feedback signals." : `Finish all ${batch.length} reviews to unlock the next batch.`}</p>
      </footer>
    </section>
  `;
}

function renderAdd() {
  const approved = state.approved_preview || [];
  const items = approved
    .map(
      (item) => `
      <div class="approved-item">
        <div>
          <strong>${escapeHtml(item.name)}</strong>
          <p>${escapeHtml(item.borough || "-")} - ${escapeHtml(item.category || "-")}</p>
        </div>
        ${item.events_url ? `<a href="${escapeHtml(item.events_url)}" target="_blank" rel="noreferrer">Events page</a>` : ""}
      </div>
    `
    )
    .join("");

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Add Specific Organization</h2>
        <p>Manual additions are saved in the shared database.</p>
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
        <button class="primary-btn" type="submit" ${ui.isBusy ? "disabled" : ""}>Add approved org</button>
      </form>

      <section class="approved-list">
        <h3>Approved orgs (${state.stats?.approved || 0})</h3>
        ${items || '<p class="empty-small">No approved orgs yet.</p>'}
      </section>
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
        <p>Strategy notes are persisted and used for next-batch ranking.</p>
      </header>

      <label class="feedback-label" for="strategy-input">New strategy note</label>
      <textarea id="strategy-input" data-action="strategy-input" placeholder="Example: Focus on Hackney/Islington bookshops; avoid chain venues.">${escapeHtml(ui.strategyDraft)}</textarea>
      <button class="primary-btn" data-action="save-strategy" ${ui.isBusy ? "disabled" : ""}>Save strategy note</button>

      <section class="strategy-list">
        <h3>Saved strategy notes (${strategies.length})</h3>
        ${strategyItems || '<p class="empty-small">No strategy notes yet.</p>'}
      </section>
    </section>
  `;
}

function render() {
  syncCurrentCandidate();

  const body =
    ui.tab === "queue"
      ? renderQueue()
      : ui.tab === "add"
        ? renderAdd()
        : renderStrategies();

  app.innerHTML = `
    <div class="curation-shell">
      <header class="top">
        <div class="top-row">
          <div>
            <p class="eyebrow">Codex Workstream</p>
            <h1>Org Curation Console</h1>
            <p class="sub">Persistent backend mode (Postgres-ready) for shared curation.</p>
          </div>
          <button class="ghost-btn" data-action="refresh" ${ui.isBusy ? "disabled" : ""}>Refresh</button>
        </div>

        <div class="metrics">
          <div class="metric"><span>Batch</span><strong>#${escapeHtml(state.batch_number || 1)}</strong></div>
          <div class="metric"><span>Reviewed</span><strong>${escapeHtml(state.reviewed_count || 0)}/${escapeHtml(state.batch_size || 0)}</strong></div>
          <div class="metric"><span>Approved</span><strong>${escapeHtml(state.stats?.approved || 0)}</strong></div>
          <div class="metric"><span>Pending</span><strong>${escapeHtml(state.pending_total || 0)}</strong></div>
        </div>
      </header>

      <nav class="tab-nav">
        <button class="tab ${ui.tab === "queue" ? "active" : ""}" data-action="switch-tab" data-tab="queue">Review Queue</button>
        <button class="tab ${ui.tab === "add" ? "active" : ""}" data-action="switch-tab" data-tab="add">Add Org</button>
        <button class="tab ${ui.tab === "strategy" ? "active" : ""}" data-action="switch-tab" data-tab="strategy">Strategies</button>
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

  if (action === "pick-candidate") {
    ui.currentCandidateId = Number(target.dataset.id);
    render();
    return;
  }

  if (action === "next-candidate") {
    const batch = state.active_batch || [];
    const index = batch.findIndex((item) => item.id === ui.currentCandidateId);
    const next = batch[index + 1];
    if (next) ui.currentCandidateId = next.id;
    render();
    return;
  }

  if (action === "save-review") {
    await saveReview();
    return;
  }

  if (action === "load-next-batch") {
    await loadNextBatch();
    return;
  }

  if (action === "save-strategy") {
    await saveStrategy();
    return;
  }

  if (action === "toggle-strategy") {
    await toggleStrategy(Number(target.dataset.id), target.dataset.next === "true");
  }
});

app.addEventListener("input", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.id === "feedback-input" && ui.currentCandidateId) {
    ui.feedbackDrafts[ui.currentCandidateId] = target.value;
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
