const ORG_TYPES = [
  "bookshop",
  "cinema",
  "gallery",
  "live_music_venue",
  "theatre",
  "museum",
  "makers_space",
  "park",
  "garden",
  "cultural_centre",
  "university",
  "learned_society",
  "promoter",
  "festival",
  "organisation",
];

const ORG_TYPE_LABELS = {
  bookshop: "Bookshop",
  cinema: "Cinema",
  gallery: "Gallery",
  live_music_venue: "Live music venue",
  theatre: "Theatre",
  museum: "Museum",
  makers_space: "Makers space",
  park: "Park",
  garden: "Garden",
  cultural_centre: "Cultural centre",
  university: "University",
  learned_society: "Learned society",
  promoter: "Promoter",
  festival: "Festival",
  organisation: "Organisation",
};

const BOROUGH_OPTIONS = [
  "Hackney",
  "Tower Hamlets",
  "Southwark",
  "Lambeth",
  "Camden",
  "Islington",
  "Westminster",
  "City of Westminster",
  "Kensington and Chelsea",
  "Hammersmith and Fulham",
  "Lewisham",
  "Greenwich",
  "Wandsworth",
  "Haringey",
  "Newham",
  "City of London",
  "Waltham Forest",
  "Barking and Dagenham",
  "Croydon",
  "Ealing",
  "Brent",
  "Enfield",
  "Hounslow",
  "Richmond upon Thames",
  "Kingston upon Thames",
  "Bromley",
  "Barnet",
  "Redbridge",
  "Harrow",
  "Havering",
  "Hillingdon",
  "Merton",
  "Sutton",
  "Bexley",
];

const app = document.getElementById("app");

let state = window.APP_INITIAL_STATE || {
  stats: { pending: 0, approved: 0, maybe: 0, rejected: 0, total: 0, active_total: 0, queue_total: 0, open_issues: 0 },
  queue_total: 0,
  queue: [],
  active_orgs: [],
  discovery_latest: null,
  discovery_runs: [],
};

const ui = {
  tab: "queue",
  currentQueueId: state.queue?.[0]?.id ?? null,
  notice: "",
  isBusy: false,
  queueDrafts: {},
  activeFilterType: "",
  activeFilterBorough: "",
  manualDraft: {
    name: "",
    homepage: "",
    events_url: "",
    borough: "",
    org_type: "gallery",
    description: "",
  },
  bulkImport: {
    file: null,
    fileName: "",
    lastResult: null,
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
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toLocaleString();
}

function orgTypeLabel(value) {
  const clean = String(value || "").trim();
  if (!clean) return "Organisation";
  return ORG_TYPE_LABELS[clean] || clean.replace(/_/g, " ");
}

function setNotice(message) {
  ui.notice = String(message || "").trim();
}

function renderSelectOptions(values, selectedValue, emptyLabel = "Unspecified") {
  const selected = String(selectedValue || "").trim();
  const uniqueValues = [];
  const seen = new Set();
  for (const value of values || []) {
    const clean = String(value || "").trim();
    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    uniqueValues.push(clean);
  }
  if (selected && !seen.has(selected)) uniqueValues.unshift(selected);

  return `
    <option value="">${escapeHtml(emptyLabel)}</option>
    ${uniqueValues
      .map((value) => `<option value="${escapeHtml(value)}" ${value === selected ? "selected" : ""}>${escapeHtml(value)}</option>`)
      .join("")}
  `;
}

function renderBoroughOptions(selectedValue) {
  return renderSelectOptions(BOROUGH_OPTIONS, selectedValue, "Select borough");
}

function renderTypeOptions(selectedValue) {
  const selected = String(selectedValue || "").trim();
  const values = ORG_TYPES.slice();
  if (selected && !values.includes(selected)) values.unshift(selected);
  return `<option value="">Select type</option>${values
    .map((value) => `<option value="${escapeHtml(value)}" ${value === selected ? "selected" : ""}>${escapeHtml(orgTypeLabel(value))}</option>`)
    .join("")}`;
}

function syncCurrentQueue() {
  const queue = state.queue || [];
  if (!queue.length) {
    ui.currentQueueId = null;
    return;
  }
  if (!queue.some((item) => item.id === ui.currentQueueId)) {
    ui.currentQueueId = queue[0].id;
  }
}

function ensureQueueDraft(org) {
  if (!org) return { feedback: "", events_url: "", name: "", borough: "", org_type: "organisation" };
  if (!ui.queueDrafts[org.id]) {
    ui.queueDrafts[org.id] = {
      feedback: org.notes || "",
      events_url: org.events_url || "",
      name: org.name || "",
      borough: org.borough || "",
      org_type: org.org_type || "organisation",
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
    throw new Error(payload.error || `Request failed (${response.status})`);
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
        name: (draft.name || "").trim(),
        borough: (draft.borough || "").trim(),
        org_type: (draft.org_type || "").trim(),
      }),
    });

    state = payload.state || state;
    syncCurrentQueue();
    if (action === "resolve") setNotice("Issue marked resolved.");
    else if (action === "snooze") setNotice("Issue snoozed.");
    else setNotice("Issue kept open.");
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function moveActiveOrgToQueue(orgId) {
  if (!orgId) return;
  const org = (state.active_orgs || []).find((item) => item.id === orgId) || null;

  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest(`/api/admin/review/${orgId}`, {
      method: "POST",
      body: JSON.stringify({ action: "open", review_needed_reason: "Manual review requested from Active Orgs" }),
    });
    state = payload.state || state;
    syncCurrentQueue();
    setNotice(`Moved "${org?.name || `Org #${orgId}`}" to Review Queue.`);
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
    org_type: form.org_type.value,
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
    await apiRequest("/api/orgs", { method: "POST", body: JSON.stringify(body) });

    ui.manualDraft = {
      name: "",
      homepage: "",
      events_url: "",
      borough: "",
      org_type: "gallery",
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

function renderImportSummary(result) {
  if (!result) return "";
  const summary = result.summary || {};
  const apply = result.apply || null;
  const reasonCounts = summary.review_reason_counts || {};
  const reasonRows = Object.entries(reasonCounts)
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .map(([reason, count]) => `<li>${escapeHtml(reason)}: ${escapeHtml(count)}</li>`)
    .join("");

  return `
    <section class="strategy-summary">
      <h3>${escapeHtml(result.mode === "apply" ? "Bulk Import Applied" : "Bulk Import Preview")}</h3>
      <p>Total rows: <strong>${escapeHtml(summary.total_rows ?? 0)}</strong></p>
      <p>Planned rows: <strong>${escapeHtml(summary.planned_rows ?? 0)}</strong> (safe ${escapeHtml(summary.safe_rows ?? 0)}, review ${escapeHtml(summary.review_rows ?? 0)})</p>
      <p>Deduped against existing DB: <strong>${escapeHtml(summary.existing_db_matches ?? 0)}</strong></p>
      ${
        apply
          ? `<p>Inserted new: <strong>${escapeHtml(apply.inserted_new ?? 0)}</strong>, merged existing: <strong>${escapeHtml(apply.merged_existing ?? 0)}</strong>, queued for review: <strong>${escapeHtml(apply.review_opened ?? 0)}</strong></p>
             <p>Apply errors: <strong>${escapeHtml(apply.error_count ?? 0)}</strong></p>`
          : ""
      }
      <div class="inference">
        <h4>Review reasons</h4>
        <ul>${reasonRows || "<li>None</li>"}</ul>
      </div>
    </section>
  `;
}

async function uploadBulkCsv(applyChanges) {
  if (!ui.bulkImport.file) {
    setNotice("Choose a CSV file first.");
    render();
    return;
  }

  const formData = new FormData();
  formData.append("file", ui.bulkImport.file, ui.bulkImport.file.name || "orgs.csv");
  formData.append("apply", applyChanges ? "true" : "false");
  formData.append("source", "csv_admin_import");

  ui.isBusy = true;
  render();
  try {
    const response = await fetch("/api/admin/import/csv", { method: "POST", body: formData });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.error || `Request failed (${response.status})`);

    const result = payload.result || null;
    ui.bulkImport.lastResult = result;

    if (applyChanges) {
      state = payload.state || state;
      syncCurrentQueue();
      const inserted = Number(result?.apply?.inserted_new || 0);
      const queued = Number(result?.apply?.review_opened || 0);
      setNotice(`Bulk import complete: ${inserted} inserted, ${queued} moved to review queue.`);
    } else {
      const planned = Number(result?.summary?.planned_rows || 0);
      const deduped = Number(result?.summary?.existing_db_matches || 0);
      setNotice(`Bulk preview ready: ${planned} rows planned, ${deduped} deduped against existing DB.`);
    }
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function runDiscoveryNow() {
  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest("/api/admin/discovery/run", { method: "POST", body: JSON.stringify({}) });
    state = payload.state || state;
    const summary = payload.summary || {};
    if (summary.status === "skipped") {
      setNotice(summary.reason || "Discovery skipped.");
    } else {
      setNotice(`Discovery complete: ${Number(summary.upserted_count || 0)} upserted from ${Number(summary.candidate_count || 0)} candidates.`);
    }
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

async function cleanupDiscoveryNow() {
  ui.isBusy = true;
  render();
  try {
    const payload = await apiRequest("/api/admin/discovery/cleanup", {
      method: "POST",
      body: JSON.stringify({ days: 7, dry_run: false, limit: 1500 }),
    });
    state = payload.state || state;
    const summary = payload.summary || {};
    setNotice(`Cleanup complete: ${Number(summary.updated || 0)} updated (${Number(summary.flagged || 0)} flagged, ${Number(summary.newly_inactivated || 0)} newly inactivated).`);
  } catch (error) {
    setNotice(error.message);
  } finally {
    ui.isBusy = false;
    render();
  }
}

function runStatusMeta(status) {
  if (status === "success") return { label: "Success", className: "approved" };
  if (status === "failed") return { label: "Failed", className: "rejected" };
  return { label: "Running", className: "pending" };
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
          <div><label>Status</label><p>${escapeHtml(current.status || "-")}</p></div>
          <div><label>Last crawled</label><p>${escapeHtml(formatDate(current.last_crawled_at))}</p></div>
          <div><label>Last successful extract</label><p>${escapeHtml(formatDate(current.last_successful_event_extract_at))}</p></div>
          <div><label>Failure/empty streak</label><p>${escapeHtml(current.consecutive_failures || 0)} / ${escapeHtml(current.consecutive_empty_extracts || 0)}</p></div>
        </div>

        <div class="links">
          ${current.homepage ? `<a href="${escapeHtml(current.homepage)}" target="_blank" rel="noreferrer">Open website</a>` : ""}
          ${current.events_url ? `<a href="${escapeHtml(current.events_url)}" target="_blank" rel="noreferrer">Open events page</a>` : ""}
        </div>

        <label class="feedback-label" for="queue-name">Org name</label>
        <input id="queue-name" data-action="queue-name-input" data-id="${current.id}" value="${escapeHtml(draft.name)}" placeholder="Organization name" />

        <label class="feedback-label" for="queue-borough">Borough</label>
        <select id="queue-borough" data-action="queue-borough-input" data-id="${current.id}">${renderBoroughOptions(draft.borough)}</select>

        <label class="feedback-label" for="queue-org-type">Type</label>
        <select id="queue-org-type" data-action="queue-org-type-input" data-id="${current.id}">${renderTypeOptions(draft.org_type)}</select>

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
        <p>Rolling queue for org records that need manual triage.</p>
      </header>
      <div class="queue-layout">
        <div class="queue-list">${list || '<div class="empty-card">Queue is empty.</div>'}</div>
        <div class="queue-card">${card}</div>
      </div>
    </section>
  `;
}

function renderActive() {
  const activeOrgs = state.active_orgs || [];
  const total = activeOrgs.length;

  const byType = {};
  const byBorough = {};
  for (const item of activeOrgs) {
    const type = item.org_type || "organisation";
    const borough = item.borough || "unknown";
    byType[type] = (byType[type] || 0) + 1;
    byBorough[borough] = (byBorough[borough] || 0) + 1;
  }

  const typeOptions = Object.entries(byType)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, count]) => `<option value="${escapeHtml(name)}" ${ui.activeFilterType === name ? "selected" : ""}>${escapeHtml(orgTypeLabel(name))} (${count})</option>`)
    .join("");

  const boroughOptions = Object.entries(byBorough)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([name, count]) => `<option value="${escapeHtml(name)}" ${ui.activeFilterBorough === name ? "selected" : ""}>${escapeHtml(name)} (${count})</option>`)
    .join("");

  const filtered = activeOrgs.filter((item) => {
    if (ui.activeFilterType && (item.org_type || "organisation") !== ui.activeFilterType) return false;
    if (ui.activeFilterBorough && (item.borough || "unknown") !== ui.activeFilterBorough) return false;
    return true;
  });

  const rows = filtered
    .map((item) => `
      <tr>
        <td>${escapeHtml(item.name)}</td>
        <td>${escapeHtml(item.borough || "-")}</td>
        <td>${escapeHtml(orgTypeLabel(item.org_type || "organisation"))}</td>
        <td>${item.events_url ? `<a href="${escapeHtml(item.events_url)}" target="_blank" rel="noreferrer">Link</a>` : "-"}</td>
        <td>${escapeHtml(formatDate(item.created_at))}</td>
        <td><button class="ghost-btn mini-btn" data-action="queue-from-active" data-id="${item.id}" ${ui.isBusy ? "disabled" : ""}>&#x2192;</button></td>
      </tr>
    `)
    .join("");

  return `
    <section class="panel">
      <header class="panel-head panel-head-row">
        <div>
          <h2>All Active Orgs</h2>
          <p>Flat table extract.</p>
        </div>
        <button class="primary-btn" data-action="go-add-org" ${ui.isBusy ? "disabled" : ""}>Add org</button>
      </header>

      <div class="active-filters">
        <select data-action="active-filter-type">
          <option value="" ${!ui.activeFilterType ? "selected" : ""}>All types (${total})</option>
          ${typeOptions}
        </select>
        <select data-action="active-filter-borough">
          <option value="" ${!ui.activeFilterBorough ? "selected" : ""}>All boroughs (${total})</option>
          ${boroughOptions}
        </select>
      </div>

      <div class="table-wrap">
        <table class="flat-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Borough</th>
              <th>Type</th>
              <th>Events URL</th>
              <th>Created At</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            ${rows || '<tr><td colspan="6">No active orgs.</td></tr>'}
          </tbody>
        </table>
      </div>
    </section>
  `;
}

function renderAdd() {
  const importSummary = renderImportSummary(ui.bulkImport.lastResult);

  return `
    <section class="panel">
      <header class="panel-head panel-head-row">
        <div>
          <h2>Add Specific Organization</h2>
          <p>Manual additions go straight into the main org database.</p>
        </div>
        <button class="ghost-btn" data-action="back-to-active" ${ui.isBusy ? "disabled" : ""}>Back to Active Orgs</button>
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
          <select name="borough" required>${renderBoroughOptions(ui.manualDraft.borough)}</select>
        </label>
        <label>Type
          <select name="org_type">${renderTypeOptions(ui.manualDraft.org_type)}</select>
        </label>
        <label>Description
          <textarea name="description" placeholder="Why this source matters.">${escapeHtml(ui.manualDraft.description)}</textarea>
        </label>
        <button class="primary-btn" type="submit" ${ui.isBusy ? "disabled" : ""}>Add org</button>
      </form>

      <section class="approved-list">
        <h3>Bulk Add (CSV)</h3>
        <p class="sub">Guarded dedupe against existing DB. Risky rows are auto-opened in Review Queue.</p>
        <label class="feedback-label" for="bulk-csv-input">CSV file</label>
        <input id="bulk-csv-input" type="file" accept=".csv,text/csv" data-action="bulk-csv-file" ${ui.isBusy ? "disabled" : ""} />
        <p class="sub">${ui.bulkImport.fileName ? `Selected: ${escapeHtml(ui.bulkImport.fileName)}` : "No file selected."}</p>
        <div class="card-actions">
          <button class="ghost-btn" type="button" data-action="bulk-csv-dry-run" ${ui.isBusy ? "disabled" : ""}>Preview import</button>
          <button class="primary-btn" type="button" data-action="bulk-csv-apply" ${ui.isBusy ? "disabled" : ""}>Import CSV</button>
        </div>
        ${importSummary}
      </section>
    </section>
  `;
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
        <button class="ghost-btn" data-action="cleanup-discovery" ${ui.isBusy ? "disabled" : ""}>Cleanup recent discovery</button>
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

  let body = renderQueue();
  if (ui.tab === "active") body = renderActive();
  if (ui.tab === "add") body = renderAdd();
  if (ui.tab === "discovery") body = renderDiscovery();

  const navTab = ui.tab === "add" ? "active" : ui.tab;
  const openIssues = state.stats?.open_issues || 0;
  const activeTotal = state.stats?.active_total || state.active_orgs?.length || 0;
  const discoveryTotal = (state.discovery_runs || []).length;

  app.innerHTML = `
    <div class="curation-shell">
      <header class="top">
        <div class="top-row">
          <div>
            <p class="eyebrow">London Tasteful Events</p>
            <h1>Org Curation Console</h1>
            <p class="sub">Lean queue + active list + import + discovery.</p>
          </div>
        </div>

        <div class="metrics">
          <button class="metric metric-nav ${navTab === "queue" ? "active" : ""}" data-action="switch-tab" data-tab="queue" ${ui.isBusy ? "disabled" : ""}>
            <span>Open Issues</span>
            <strong>${escapeHtml(openIssues)}</strong>
          </button>
          <button class="metric metric-nav ${navTab === "active" ? "active" : ""}" data-action="switch-tab" data-tab="active" ${ui.isBusy ? "disabled" : ""}>
            <span>Active Orgs</span>
            <strong>${escapeHtml(activeTotal)}</strong>
          </button>
          <button class="metric metric-nav ${navTab === "discovery" ? "active" : ""}" data-action="switch-tab" data-tab="discovery" ${ui.isBusy ? "disabled" : ""}>
            <span>Discovery Runs</span>
            <strong>${escapeHtml(discoveryTotal)}</strong>
          </button>
        </div>
      </header>

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
  if (action === "go-add-org") {
    ui.tab = "add";
    render();
    return;
  }
  if (action === "back-to-active") {
    ui.tab = "active";
    render();
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
  if (action === "run-discovery") {
    await runDiscoveryNow();
    return;
  }
  if (action === "cleanup-discovery") {
    await cleanupDiscoveryNow();
    return;
  }
  if (action === "bulk-csv-dry-run") {
    await uploadBulkCsv(false);
    return;
  }
  if (action === "bulk-csv-apply") {
    await uploadBulkCsv(true);
    return;
  }
  if (action === "queue-from-active") {
    await moveActiveOrgToQueue(Number(target.dataset.id));
  }
});

app.addEventListener("change", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.dataset.action === "bulk-csv-file" && target instanceof HTMLInputElement) {
    const file = target.files && target.files.length ? target.files[0] : null;
    ui.bulkImport.file = file;
    ui.bulkImport.fileName = file ? file.name : "";
    ui.bulkImport.lastResult = null;
    render();
    return;
  }

  if (target.dataset.action === "active-filter-type") {
    ui.activeFilterType = target.value;
    render();
    return;
  }

  if (target.dataset.action === "active-filter-borough") {
    ui.activeFilterBorough = target.value;
    render();
  }
});

app.addEventListener("input", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) return;

  if (target.dataset.action === "queue-feedback-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "", name: "", borough: "", org_type: "organisation" };
    draft.feedback = target.value;
    ui.queueDrafts[id] = draft;
    return;
  }

  if (target.dataset.action === "queue-events-url-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "", name: "", borough: "", org_type: "organisation" };
    draft.events_url = target.value;
    ui.queueDrafts[id] = draft;
    return;
  }

  if (target.dataset.action === "queue-name-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "", name: "", borough: "", org_type: "organisation" };
    draft.name = target.value;
    ui.queueDrafts[id] = draft;
    return;
  }

  if (target.dataset.action === "queue-borough-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "", name: "", borough: "", org_type: "organisation" };
    draft.borough = target.value;
    ui.queueDrafts[id] = draft;
    return;
  }

  if (target.dataset.action === "queue-org-type-input") {
    const id = Number(target.dataset.id);
    if (!id) return;
    const draft = ui.queueDrafts[id] || { feedback: "", events_url: "", name: "", borough: "", org_type: "organisation" };
    draft.org_type = target.value;
    ui.queueDrafts[id] = draft;
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
