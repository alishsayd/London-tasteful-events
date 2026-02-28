import { CODEX_SEED_CANDIDATES } from "./codex-curation-seed.js";

const STORAGE_KEY = "codex.londonTastefulEvents.curation.v1";
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

const LONDON_BOROUGHS = [
  "Barking and Dagenham",
  "Barnet",
  "Bexley",
  "Brent",
  "Bromley",
  "Camden",
  "Croydon",
  "Ealing",
  "Enfield",
  "Greenwich",
  "Hackney",
  "Hammersmith and Fulham",
  "Haringey",
  "Harrow",
  "Havering",
  "Hillingdon",
  "Hounslow",
  "Islington",
  "Kensington and Chelsea",
  "Kingston upon Thames",
  "Lambeth",
  "Lewisham",
  "Merton",
  "Newham",
  "Redbridge",
  "Richmond upon Thames",
  "Southwark",
  "Sutton",
  "Tower Hamlets",
  "Waltham Forest",
  "Wandsworth",
  "Westminster",
  "City of London",
];

const TYPE_ALIASES = {
  gallery: ["gallery"],
  museum: ["museum"],
  cinema: ["cinema", "film"],
  bookshop: ["bookshop", "book store", "bookstore", "books"],
  "cultural centre": ["cultural centre", "cultural center", "culture centre", "culture center"],
  "art centre": ["art centre", "art center", "arts centre", "arts center"],
  house: ["house museum", "house"],
  "social community center": ["social", "community center", "community centre"],
  other: ["other"],
};

const STOP_WORDS = new Set([
  "the",
  "and",
  "that",
  "from",
  "with",
  "this",
  "more",
  "less",
  "like",
  "very",
  "into",
  "about",
  "while",
  "their",
  "your",
  "good",
  "great",
  "events",
  "event",
  "london",
  "source",
  "sites",
  "site",
  "page",
  "pages",
]);

const app = document.getElementById("app");

let state = loadState();
const ui = {
  tab: "queue",
  currentCandidateId: state.activeBatchIds[0] ?? null,
  notice: "",
  draftFeedbacks: {},
  strategyDraft: "",
  manualDraft: {
    name: "",
    websiteUrl: "",
    eventsUrl: "",
    borough: "",
    type: "gallery",
    notes: "",
  },
};

function nowIso() {
  return new Date().toISOString();
}

function escapeHtml(input) {
  return String(input ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function isState(value) {
  return value && typeof value === "object" && Array.isArray(value.candidates) && Array.isArray(value.activeBatchIds);
}

function createInitialState() {
  return {
    batchNumber: 1,
    activeBatchIds: CODEX_SEED_CANDIDATES.slice(0, BATCH_SIZE).map((candidate) => candidate.id),
    candidates: CODEX_SEED_CANDIDATES,
    manualOrganizations: [],
    searchStrategies: [],
  };
}

function loadState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return createInitialState();
    const parsed = JSON.parse(raw);
    if (!isState(parsed)) return createInitialState();

    const currentById = new Map(parsed.candidates.map((candidate) => [candidate.id, candidate]));
    const mergedCandidates = CODEX_SEED_CANDIDATES.map((seed) => currentById.get(seed.id) ?? seed);
    const activeBatchIds = parsed.activeBatchIds.filter((id) => mergedCandidates.some((candidate) => candidate.id === id));

    return {
      ...parsed,
      candidates: mergedCandidates,
      activeBatchIds: activeBatchIds.length ? activeBatchIds : mergedCandidates.slice(0, BATCH_SIZE).map((candidate) => candidate.id),
      manualOrganizations: parsed.manualOrganizations ?? [],
      searchStrategies: parsed.searchStrategies ?? [],
      batchNumber: Number.isFinite(parsed.batchNumber) ? parsed.batchNumber : 1,
    };
  } catch {
    return createInitialState();
  }
}

function persistState() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function statusLabel(status) {
  if (status === "approved") return "Approved";
  if (status === "rejected") return "Rejected";
  return "Parked";
}

function statusClass(status) {
  if (status === "approved") return "approved";
  if (status === "rejected") return "rejected";
  return "parked";
}

function domainFromText(value) {
  try {
    const normalized = value.startsWith("http") ? value : `https://${value}`;
    const url = new URL(normalized);
    return url.hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return null;
  }
}

function candidateDomain(candidate) {
  return domainFromText(candidate.websiteUrl) || domainFromText(candidate.eventsUrl);
}

function extractBorough(text) {
  const lower = text.toLowerCase();
  for (const borough of LONDON_BOROUGHS) {
    if (lower.includes(borough.toLowerCase())) return borough;
  }
  return null;
}

function extractType(text) {
  const lower = text.toLowerCase();
  for (const type of ORG_TYPES) {
    if (TYPE_ALIASES[type].some((alias) => lower.includes(alias))) return type;
  }
  return null;
}

function extractFirstUrl(text) {
  const match = text.match(/https?:\/\/[^\s)]+/i);
  return match ? match[0] : null;
}

function includesAny(text, terms) {
  return terms.some((term) => text.includes(term));
}

function extractKeywords(text) {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .map((word) => word.trim())
    .filter((word) => word.length >= 5 && !STOP_WORDS.has(word))
    .slice(0, 8);
}

function inferFeedback(feedback, candidate) {
  const lower = feedback.toLowerCase();
  const rejectSignals = ["reject", "skip", "exclude", "not a fit", "outside london", "too commercial", "duplicate", "not relevant"];
  const approveSignals = ["approve", "keep", "include", "strong", "great", "good fit", "add this", "yes"];
  const parkSignals = ["park", "hold", "later", "unsure", "unclear", "maybe"];

  let status = "parked";
  if (includesAny(lower, rejectSignals)) status = "rejected";
  else if (includesAny(lower, approveSignals)) status = "approved";
  else if (includesAny(lower, parkSignals)) status = "parked";

  const inferredBorough = extractBorough(feedback);
  const inferredType = extractType(feedback);
  const rawUrl = extractFirstUrl(feedback);
  const inferredEventsUrl =
    rawUrl && includesAny(rawUrl.toLowerCase(), ["/event", "/events", "/whatson", "/whats-on", "/programme"]) ? rawUrl : null;

  const strategyUpdates = [];
  if (status === "approved") {
    strategyUpdates.push(`Boost similar orgs to "${candidate.name}".`);
    if (inferredBorough) strategyUpdates.push(`Boost borough: ${inferredBorough}.`);
    if (inferredType) strategyUpdates.push(`Boost type: ${inferredType}.`);
  }
  if (status === "rejected") {
    const domain = candidateDomain(candidate);
    if (domain && includesAny(lower, ["block", "avoid domain", "never show"])) strategyUpdates.push(`Block domain: ${domain}.`);
    if (includesAny(lower, ["outside london", "not london"])) strategyUpdates.push("Prioritize explicit London-only institutions.");
    if (includesAny(lower, ["commercial", "chain", "mainstream"])) strategyUpdates.push("Down-rank chain/commercial venues.");
  }
  if (!strategyUpdates.length) strategyUpdates.push("No direct strategy update inferred.");

  return {
    status,
    inferredBorough,
    inferredType,
    inferredEventsUrl,
    strategyUpdates,
  };
}

function buildLearningProfile() {
  const preferredBoroughs = new Set();
  const preferredTypes = new Set();
  const blockedDomains = new Set();
  const includeKeywords = new Set();
  const excludeKeywords = new Set();

  for (const candidate of state.candidates) {
    if (!candidate.review) continue;
    const feedback = candidate.review.feedback.toLowerCase();
    const domain = candidateDomain(candidate);

    if (candidate.review.status === "approved") {
      preferredBoroughs.add(candidate.review.inferredBorough || candidate.borough);
      preferredTypes.add(candidate.review.inferredType || candidate.type);
      for (const keyword of extractKeywords(feedback)) includeKeywords.add(keyword);
    }

    if (candidate.review.status === "rejected") {
      if (domain && includesAny(feedback, ["block", "avoid domain", "never show"])) blockedDomains.add(domain);
      if (includesAny(feedback, ["outside london", "not london"])) excludeKeywords.add("outside london");
      if (includesAny(feedback, ["commercial", "chain", "mainstream"])) {
        excludeKeywords.add("commercial");
        excludeKeywords.add("chain");
      }
      for (const keyword of extractKeywords(feedback)) excludeKeywords.add(keyword);
    }
  }

  for (const strategy of state.searchStrategies) {
    if (!strategy.active) continue;
    const text = strategy.text.toLowerCase();
    const borough = extractBorough(strategy.text);
    const type = extractType(strategy.text);
    const domain = domainFromText(strategy.text);

    if (borough) preferredBoroughs.add(borough);
    if (type) preferredTypes.add(type);
    if (domain && includesAny(text, ["avoid", "exclude", "block"])) blockedDomains.add(domain);

    for (const keyword of extractKeywords(text)) {
      if (includesAny(text, ["avoid", "exclude", "not"])) excludeKeywords.add(keyword);
      else includeKeywords.add(keyword);
    }
  }

  return { preferredBoroughs, preferredTypes, blockedDomains, includeKeywords, excludeKeywords };
}

function scoreCandidate(candidate, profile) {
  let score = 0;
  const text = `${candidate.name} ${candidate.notes} ${candidate.foundVia}`.toLowerCase();
  const domain = candidateDomain(candidate);

  if (profile.preferredBoroughs.has(candidate.borough)) score += 3;
  if (profile.preferredTypes.has(candidate.type)) score += 3;
  if (candidate.confidence === "high") score += 2;
  if (candidate.confidence === "medium") score += 1;
  if (candidate.confidence === "low") score -= 1;
  if (domain && profile.blockedDomains.has(domain)) score -= 8;

  for (const keyword of profile.includeKeywords) {
    if (text.includes(keyword)) score += 1;
  }
  for (const keyword of profile.excludeKeywords) {
    if (text.includes(keyword)) score -= 2;
  }

  return score;
}

function selectNextBatchIds() {
  const profile = buildLearningProfile();
  const pending = state.candidates.filter((candidate) => candidate.review === null);
  if (!pending.length) return [];

  const pool = pending.filter((candidate) => {
    const domain = candidateDomain(candidate);
    if (!domain) return true;
    return !profile.blockedDomains.has(domain);
  });

  const ranked = (pool.length ? pool : pending)
    .map((candidate, index) => ({ candidate, index, score: scoreCandidate(candidate, profile) }))
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return left.index - right.index;
    })
    .slice(0, BATCH_SIZE);

  return ranked.map((entry) => entry.candidate.id);
}

function formatDate(value) {
  return new Date(value).toLocaleString();
}

function getActiveBatch() {
  const byId = new Map(state.candidates.map((candidate) => [candidate.id, candidate]));
  return state.activeBatchIds.map((id) => byId.get(id)).filter(Boolean);
}

function syncCurrentCandidate(activeBatch) {
  if (!activeBatch.length) {
    ui.currentCandidateId = null;
    return;
  }
  const exists = activeBatch.some((candidate) => candidate.id === ui.currentCandidateId);
  if (exists) return;
  const firstPending = activeBatch.find((candidate) => candidate.review === null);
  ui.currentCandidateId = firstPending ? firstPending.id : activeBatch[0].id;
}

function setNotice(message) {
  ui.notice = message;
}

function renderQueue(activeBatch, currentCandidate, reviewedCount, allReviewed) {
  const chips = activeBatch
    .map((candidate, index) => {
      const reviewed = candidate.review !== null;
      const active = candidate.id === ui.currentCandidateId;
      const badge = candidate.review ? statusLabel(candidate.review.status).charAt(0) : "•";
      return `
      <button class="batch-chip ${reviewed ? "done" : ""} ${active ? "active" : ""}" data-action="select-candidate" data-id="${escapeHtml(candidate.id)}">
        ${index + 1}<span>${badge}</span>
      </button>
    `;
    })
    .join("");

  let card = `<div class="empty-card">No candidate selected in this batch.</div>`;
  if (currentCandidate) {
    const currentIndex = activeBatch.findIndex((candidate) => candidate.id === currentCandidate.id);
    const feedback = ui.draftFeedbacks[currentCandidate.id] ?? currentCandidate.review?.feedback ?? "";
    const reviewPanel = currentCandidate.review
      ? `
      <section class="inference">
        <h4>What the system inferred</h4>
        <ul>
          <li>Status: ${statusLabel(currentCandidate.review.status)}</li>
          <li>Borough update: ${escapeHtml(currentCandidate.review.inferredBorough ?? "No change")}</li>
          <li>Type update: ${escapeHtml(currentCandidate.review.inferredType ?? "No change")}</li>
          <li>Events URL update: ${escapeHtml(currentCandidate.review.inferredEventsUrl ?? "No change")}</li>
          <li>Reviewed at: ${escapeHtml(formatDate(currentCandidate.review.reviewedAt))}</li>
        </ul>
        <p class="small-title">Strategy updates from this note</p>
        <ul>${currentCandidate.review.strategyUpdates.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
      </section>
    `
      : "";

    card = `
      <article class="candidate-card">
        <div class="candidate-head">
          <div>
            <p class="candidate-index">Candidate ${currentIndex + 1} of ${activeBatch.length}</p>
            <h3>${escapeHtml(currentCandidate.name)}</h3>
          </div>
          ${
            currentCandidate.review
              ? `<span class="status ${statusClass(currentCandidate.review.status)}">${statusLabel(currentCandidate.review.status)}</span>`
              : `<span class="status pending">Pending</span>`
          }
        </div>
        <div class="meta-grid">
          <div><label>Borough</label><p>${escapeHtml(currentCandidate.borough)}</p></div>
          <div><label>Type</label><p>${escapeHtml(currentCandidate.type)}</p></div>
          <div><label>Found via</label><p>${escapeHtml(currentCandidate.foundVia)}</p></div>
          <div><label>Confidence</label><p>${escapeHtml(currentCandidate.confidence)}</p></div>
        </div>
        <p class="candidate-note">${escapeHtml(currentCandidate.notes)}</p>
        <div class="links">
          <a href="${escapeHtml(currentCandidate.websiteUrl)}" target="_blank" rel="noreferrer">Open website</a>
          <a href="${escapeHtml(currentCandidate.eventsUrl)}" target="_blank" rel="noreferrer">Open events page</a>
          <a href="${escapeHtml(currentCandidate.sourceUrl)}" target="_blank" rel="noreferrer">Open source trail</a>
        </div>

        <label class="feedback-label" for="feedback-input">Your freeform feedback</label>
        <textarea id="feedback-input" data-action="feedback-input" placeholder="Example: Approve. Borough should be Hackney, events URL is https://.../events. More like this around East London bookshops.">${escapeHtml(
          feedback
        )}</textarea>
        <div class="card-actions">
          <button class="primary-btn" data-action="save-feedback">Interpret and save note</button>
          <button class="ghost-btn" data-action="next-candidate" ${currentIndex >= activeBatch.length - 1 ? "disabled" : ""}>Next candidate</button>
        </div>
        ${reviewPanel}
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
        <div class="progress-bar" style="width: ${activeBatch.length ? (reviewedCount / activeBatch.length) * 100 : 0}%"></div>
      </div>
      <div class="batch-strip">${chips}</div>
      ${card}
      <footer class="batch-footer">
        <button class="primary-btn" data-action="load-next-batch" ${allReviewed ? "" : "disabled"}>Load next batch</button>
        <p>${allReviewed ? "Batch complete. Next batch uses your feedback-driven updates." : `Finish all ${activeBatch.length} reviews to unlock the next batch.`}</p>
      </footer>
    </section>
  `;
}

function renderAdd(approvedFromReviews, approvedTotal) {
  const approvedItems = [...state.manualOrganizations, ...approvedFromReviews]
    .slice(0, 50)
    .map(
      (item) => `
      <div class="approved-item">
        <div>
          <strong>${escapeHtml(item.name)}</strong>
          <p>${escapeHtml(item.borough)} - ${escapeHtml(item.type)}</p>
        </div>
        <a href="${escapeHtml(item.eventsUrl)}" target="_blank" rel="noreferrer">Events page</a>
      </div>
    `
    )
    .join("");

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Add Specific Organization</h2>
        <p>Manual additions go directly into approved targets for later scraping.</p>
      </header>

      <form class="stack-form" data-action="manual-org-submit">
        <label>Organization name
          <input name="name" value="${escapeHtml(ui.manualDraft.name)}" placeholder="e.g. The Common Press" required />
        </label>
        <label>Website URL
          <input name="websiteUrl" type="url" value="${escapeHtml(ui.manualDraft.websiteUrl)}" placeholder="https://..." required />
        </label>
        <label>Events URL (optional)
          <input name="eventsUrl" type="url" value="${escapeHtml(ui.manualDraft.eventsUrl)}" placeholder="https://.../events" />
        </label>
        <label>Borough
          <input name="borough" value="${escapeHtml(ui.manualDraft.borough)}" placeholder="Hackney" required />
        </label>
        <label>Type
          <select name="type">
            ${ORG_TYPES.map((type) => `<option value="${escapeHtml(type)}" ${ui.manualDraft.type === type ? "selected" : ""}>${escapeHtml(type)}</option>`).join("")}
          </select>
        </label>
        <label>Notes
          <textarea name="notes" placeholder="Why this source matters.">${escapeHtml(ui.manualDraft.notes)}</textarea>
        </label>
        <button type="submit" class="primary-btn">Add approved org</button>
      </form>

      <section class="approved-list">
        <h3>Approved target orgs (${approvedTotal})</h3>
        ${approvedItems || '<p class="empty-small">No approved orgs yet.</p>'}
      </section>
    </section>
  `;
}

function renderStrategy(profile) {
  const list = state.searchStrategies
    .map(
      (strategy) => `
      <div class="strategy-item">
        <button class="toggle ${strategy.active ? "on" : "off"}" data-action="toggle-strategy" data-id="${escapeHtml(strategy.id)}">${strategy.active ? "Active" : "Paused"}</button>
        <div>
          <p>${escapeHtml(strategy.text)}</p>
          <small>${escapeHtml(formatDate(strategy.createdAt))}</small>
        </div>
      </div>
    `
    )
    .join("");

  return `
    <section class="panel">
      <header class="panel-head">
        <h2>Search Strategy Log</h2>
        <p>Write freeform strategy notes. Active notes influence next-batch selection.</p>
      </header>

      <label class="feedback-label" for="strategy-input">New strategy note</label>
      <textarea id="strategy-input" data-action="strategy-input" placeholder="Example: Focus on Hackney/Islington bookshops and architecture foundations. Avoid chain venues.">${escapeHtml(
        ui.strategyDraft
      )}</textarea>
      <button class="primary-btn" data-action="save-strategy">Save strategy note</button>

      <section class="strategy-summary">
        <h3>Current learning profile</h3>
        <ul>
          <li>Preferred boroughs: ${escapeHtml([...profile.preferredBoroughs].join(", ") || "None yet")}</li>
          <li>Preferred types: ${escapeHtml([...profile.preferredTypes].join(", ") || "None yet")}</li>
          <li>Blocked domains: ${escapeHtml([...profile.blockedDomains].join(", ") || "None yet")}</li>
          <li>Include keywords: ${escapeHtml([...profile.includeKeywords].slice(0, 12).join(", ") || "None yet")}</li>
          <li>Exclude keywords: ${escapeHtml([...profile.excludeKeywords].slice(0, 12).join(", ") || "None yet")}</li>
        </ul>
      </section>

      <section class="strategy-list">
        <h3>Saved strategy notes (${state.searchStrategies.length})</h3>
        ${list || '<p class="empty-small">No strategy notes yet.</p>'}
      </section>
    </section>
  `;
}

function render() {
  const activeBatch = getActiveBatch();
  syncCurrentCandidate(activeBatch);
  const currentCandidate = activeBatch.find((candidate) => candidate.id === ui.currentCandidateId) || null;
  const reviewedCount = activeBatch.filter((candidate) => candidate.review !== null).length;
  const allReviewed = activeBatch.length > 0 && reviewedCount === activeBatch.length;
  const pendingCount = state.candidates.filter((candidate) => candidate.review === null).length;
  const approvedFromReviews = state.candidates.filter((candidate) => candidate.review?.status === "approved");
  const approvedTotal = approvedFromReviews.length + state.manualOrganizations.length;
  const profile = buildLearningProfile();

  const tabBody =
    ui.tab === "queue"
      ? renderQueue(activeBatch, currentCandidate, reviewedCount, allReviewed)
      : ui.tab === "add"
        ? renderAdd(approvedFromReviews, approvedTotal)
        : renderStrategy(profile);

  app.innerHTML = `
    <div class="curation-shell">
      <header class="top">
        <div class="top-row">
          <div>
            <p class="eyebrow">Codex Workstream</p>
            <h1>Org Curation Console</h1>
            <p class="sub">Isolated mobile queue for curation before any scraping pipeline.</p>
          </div>
          <button class="ghost-btn" data-action="reset-demo">Reset demo</button>
        </div>
        <div class="metrics">
          <div class="metric"><span>Batch</span><strong>#${escapeHtml(state.batchNumber)}</strong></div>
          <div class="metric"><span>Reviewed</span><strong>${escapeHtml(reviewedCount)}/${escapeHtml(activeBatch.length)}</strong></div>
          <div class="metric"><span>Approved</span><strong>${escapeHtml(approvedTotal)}</strong></div>
          <div class="metric"><span>Pending</span><strong>${escapeHtml(pendingCount)}</strong></div>
        </div>
      </header>

      <nav class="tab-nav">
        <button class="tab ${ui.tab === "queue" ? "active" : ""}" data-action="switch-tab" data-tab="queue">Review Queue</button>
        <button class="tab ${ui.tab === "add" ? "active" : ""}" data-action="switch-tab" data-tab="add">Add Org</button>
        <button class="tab ${ui.tab === "strategy" ? "active" : ""}" data-action="switch-tab" data-tab="strategy">Strategies</button>
      </nav>

      ${ui.notice ? `<div class="notice">${escapeHtml(ui.notice)}</div>` : ""}
      ${tabBody}
    </div>
  `;
}

function saveFeedback() {
  const activeBatch = getActiveBatch();
  const candidate = activeBatch.find((entry) => entry.id === ui.currentCandidateId);
  if (!candidate) return;

  const feedback = (ui.draftFeedbacks[candidate.id] ?? "").trim();
  if (!feedback) {
    setNotice("Write a quick freeform note first.");
    render();
    return;
  }

  const inference = inferFeedback(feedback, candidate);
  const review = {
    status: inference.status,
    feedback,
    inferredBorough: inference.inferredBorough,
    inferredType: inference.inferredType,
    inferredEventsUrl: inference.inferredEventsUrl,
    strategyUpdates: inference.strategyUpdates,
    reviewedAt: nowIso(),
  };

  state.candidates = state.candidates.map((entry) => {
    if (entry.id !== candidate.id) return entry;
    return {
      ...entry,
      borough: inference.inferredBorough || entry.borough,
      type: inference.inferredType || entry.type,
      eventsUrl: inference.inferredEventsUrl || entry.eventsUrl,
      review,
    };
  });

  const unresolved = activeBatch.filter((entry) => entry.id !== candidate.id && entry.review === null);
  if (unresolved.length) ui.currentCandidateId = unresolved[0].id;

  persistState();
  setNotice(`Saved "${candidate.name}" as ${statusLabel(inference.status).toLowerCase()}.`);
  render();
}

function loadNextBatch() {
  const activeBatch = getActiveBatch();
  const reviewedCount = activeBatch.filter((candidate) => candidate.review !== null).length;
  const allReviewed = activeBatch.length > 0 && reviewedCount === activeBatch.length;
  if (!allReviewed) return;

  const nextBatchIds = selectNextBatchIds();
  if (!nextBatchIds.length) {
    setNotice("No pending candidates left. Add orgs or strategy notes to continue.");
    render();
    return;
  }

  state.activeBatchIds = nextBatchIds;
  state.batchNumber += 1;
  ui.currentCandidateId = nextBatchIds[0];
  persistState();
  setNotice(`Loaded batch #${state.batchNumber} (${nextBatchIds.length} orgs).`);
  render();
}

function submitManualOrganization(form) {
  const payload = {
    name: form.name.value.trim(),
    websiteUrl: form.websiteUrl.value.trim(),
    eventsUrl: form.eventsUrl.value.trim(),
    borough: form.borough.value.trim(),
    type: form.type.value,
    notes: form.notes.value.trim(),
  };

  if (!payload.name || !payload.websiteUrl || !payload.borough) {
    setNotice("Name, website, and borough are required.");
    render();
    return;
  }

  const entry = {
    ...payload,
    eventsUrl: payload.eventsUrl || payload.websiteUrl,
    createdAt: nowIso(),
    id: `manual-${Date.now()}`,
  };

  state.manualOrganizations.unshift(entry);
  ui.manualDraft = { name: "", websiteUrl: "", eventsUrl: "", borough: "", type: "gallery", notes: "" };
  persistState();
  setNotice(`Added "${entry.name}" to approved targets.`);
  render();
}

function saveStrategy() {
  const text = ui.strategyDraft.trim();
  if (!text) {
    setNotice("Write your strategy note first.");
    render();
    return;
  }

  state.searchStrategies.unshift({
    id: `strategy-${Date.now()}`,
    text,
    active: true,
    createdAt: nowIso(),
  });

  ui.strategyDraft = "";
  persistState();
  setNotice("Strategy note saved and activated.");
  render();
}

function toggleStrategy(strategyId) {
  state.searchStrategies = state.searchStrategies.map((strategy) =>
    strategy.id === strategyId ? { ...strategy, active: !strategy.active } : strategy
  );
  persistState();
  render();
}

function resetDemo() {
  state = createInitialState();
  ui.currentCandidateId = state.activeBatchIds[0] ?? null;
  ui.draftFeedbacks = {};
  ui.strategyDraft = "";
  ui.manualDraft = { name: "", websiteUrl: "", eventsUrl: "", borough: "", type: "gallery", notes: "" };
  persistState();
  setNotice("Reset to seeded data and batch 1.");
  render();
}

app.addEventListener("click", (event) => {
  const target = event.target.closest("[data-action]");
  if (!target) return;

  const action = target.dataset.action;
  if (action === "switch-tab") {
    ui.tab = target.dataset.tab;
    render();
    return;
  }

  if (action === "select-candidate") {
    ui.currentCandidateId = target.dataset.id;
    render();
    return;
  }

  if (action === "save-feedback") {
    saveFeedback();
    return;
  }

  if (action === "next-candidate") {
    const activeBatch = getActiveBatch();
    const index = activeBatch.findIndex((candidate) => candidate.id === ui.currentCandidateId);
    const next = activeBatch[index + 1];
    if (next) ui.currentCandidateId = next.id;
    render();
    return;
  }

  if (action === "load-next-batch") {
    loadNextBatch();
    return;
  }

  if (action === "save-strategy") {
    saveStrategy();
    return;
  }

  if (action === "toggle-strategy") {
    toggleStrategy(target.dataset.id);
    return;
  }

  if (action === "reset-demo") {
    resetDemo();
  }
});

app.addEventListener("input", (event) => {
  const target = event.target;
  if (target.id === "feedback-input") {
    if (ui.currentCandidateId) ui.draftFeedbacks[ui.currentCandidateId] = target.value;
    return;
  }

  if (target.id === "strategy-input") {
    ui.strategyDraft = target.value;
    return;
  }

  const form = target.form;
  if (form && form.dataset.action === "manual-org-submit") {
    const field = target.name;
    if (field in ui.manualDraft) ui.manualDraft[field] = target.value;
  }
});

app.addEventListener("submit", (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) return;
  if (form.dataset.action !== "manual-org-submit") return;
  event.preventDefault();
  submitManualOrganization(form);
});

render();
