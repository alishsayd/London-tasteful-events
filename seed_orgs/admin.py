"""
Flask admin panel for reviewing candidate orgs.

Usage:
    python -m seed_orgs.admin          # start on port 5000
    python -m seed_orgs.admin --port 8080
"""

from __future__ import annotations

import argparse
import re
from datetime import date, datetime
from urllib.parse import urlparse

from flask import Flask, jsonify, render_template, request

from seed_orgs.db import (
    add_codex_strategy,
    get_codex_batch_state,
    get_codex_strategies,
    get_org,
    get_orgs,
    get_orgs_by_ids,
    get_pending_orgs,
    get_stats,
    init_db,
    save_codex_batch_state,
    set_codex_strategy_active,
    update_org,
    update_org_status,
    upsert_org,
)

app = Flask(__name__, template_folder="templates", static_folder="static")

BATCH_SIZE_DEFAULT = 20

BOROUGHS = {
    "barking and dagenham",
    "barnet",
    "bexley",
    "brent",
    "bromley",
    "camden",
    "croydon",
    "ealing",
    "enfield",
    "greenwich",
    "hackney",
    "hammersmith and fulham",
    "haringey",
    "harrow",
    "havering",
    "hillingdon",
    "hounslow",
    "islington",
    "kensington and chelsea",
    "kingston upon thames",
    "lambeth",
    "lewisham",
    "merton",
    "newham",
    "redbridge",
    "richmond upon thames",
    "southwark",
    "sutton",
    "tower hamlets",
    "waltham forest",
    "wandsworth",
    "westminster",
    "city of london",
}

CATEGORY_ALIASES = {
    "gallery": ["gallery"],
    "museum": ["museum"],
    "cinema": ["cinema", "film"],
    "bookshop": ["bookshop", "book store", "bookstore", "books"],
    "cultural centre": ["cultural centre", "cultural center", "culture centre", "culture center"],
    "art centre": ["art centre", "art center", "arts centre", "arts center"],
    "house": ["house museum", "house"],
    "social community center": ["social", "community center", "community centre"],
    "other": ["other"],
}

STOP_WORDS = {
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
}


def _json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _serialize_row(row: dict) -> dict:
    return {key: _json_safe(value) for key, value in row.items()}


def _normalize_domain(url_value: str | None) -> str | None:
    if not url_value:
        return None
    try:
        parsed = urlparse(url_value)
        host = parsed.netloc or parsed.path
        host = host.lower().replace("www.", "")
        return host or None
    except Exception:
        return None


def _extract_keywords(text_value: str) -> list[str]:
    return [
        token
        for token in re.sub(r"[^a-z0-9\s]", " ", text_value.lower()).split()
        if len(token) >= 5 and token not in STOP_WORDS
    ][:8]


def _extract_borough(text_value: str) -> str | None:
    lower = text_value.lower()
    for borough in BOROUGHS:
        if borough in lower:
            return borough.title() if borough != "city of london" else "City of London"
    return None


def _extract_category(text_value: str) -> str | None:
    lower = text_value.lower()
    for category, aliases in CATEGORY_ALIASES.items():
        if any(alias in lower for alias in aliases):
            return category
    return None


def _extract_events_url(text_value: str) -> str | None:
    match = re.search(r"https?://[^\s)]+", text_value)
    if not match:
        return None
    candidate = match.group(0)
    lowered = candidate.lower()
    if any(chunk in lowered for chunk in ["/event", "/events", "/whatson", "/whats-on", "/programme"]):
        return candidate
    return None


def _includes_any(haystack: str, needles: list[str]) -> bool:
    return any(needle in haystack for needle in needles)


def _infer_review_from_feedback(feedback: str, org: dict) -> dict:
    lower = feedback.lower()
    reject_signals = ["reject", "skip", "exclude", "not a fit", "outside london", "too commercial", "duplicate", "not relevant"]
    approve_signals = ["approve", "keep", "include", "strong", "great", "good fit", "add this", "yes"]
    maybe_signals = ["park", "hold", "later", "unsure", "unclear", "maybe"]

    status = "maybe"
    if _includes_any(lower, reject_signals):
        status = "rejected"
    elif _includes_any(lower, approve_signals):
        status = "approved"
    elif _includes_any(lower, maybe_signals):
        status = "maybe"

    inferred_borough = _extract_borough(feedback)
    inferred_category = _extract_category(feedback)
    inferred_events_url = _extract_events_url(feedback)

    strategy_updates: list[str] = []
    if status == "approved":
        strategy_updates.append(f'Boost orgs similar to "{org.get("name", "candidate")}".')
        if inferred_borough:
            strategy_updates.append(f"Boost borough: {inferred_borough}.")
        if inferred_category:
            strategy_updates.append(f"Boost category: {inferred_category}.")

    if status == "rejected":
        domain = _normalize_domain(org.get("homepage")) or _normalize_domain(org.get("events_url"))
        if domain and _includes_any(lower, ["block", "avoid domain", "never show"]):
            strategy_updates.append(f"Block domain: {domain}.")
        if _includes_any(lower, ["outside london", "not london"]):
            strategy_updates.append("Prioritize explicit London institutions only.")
        if _includes_any(lower, ["commercial", "chain", "mainstream"]):
            strategy_updates.append("Down-rank commercial and chain venues.")

    if not strategy_updates:
        strategy_updates.append("No explicit strategy change inferred.")

    return {
        "status": status,
        "inferred_borough": inferred_borough,
        "inferred_category": inferred_category,
        "inferred_events_url": inferred_events_url,
        "strategy_updates": strategy_updates,
    }


def _build_learning_profile() -> dict:
    approved = get_orgs(status="approved")
    rejected = get_orgs(status="rejected")
    strategies = [item for item in get_codex_strategies() if item.get("active")]

    preferred_boroughs: set[str] = set()
    preferred_categories: set[str] = set()
    blocked_domains: set[str] = set()
    include_keywords: set[str] = set()
    exclude_keywords: set[str] = set()

    for org in approved:
        if org.get("borough"):
            preferred_boroughs.add(str(org["borough"]))
        if org.get("category"):
            preferred_categories.add(str(org["category"]))
        if org.get("notes"):
            include_keywords.update(_extract_keywords(str(org["notes"])))

    for org in rejected:
        notes = str(org.get("notes") or "").lower()
        domain = _normalize_domain(org.get("homepage")) or _normalize_domain(org.get("events_url"))
        if domain and _includes_any(notes, ["block", "avoid domain", "never show"]):
            blocked_domains.add(domain)
        exclude_keywords.update(_extract_keywords(notes))

    for strategy in strategies:
        text_value = str(strategy.get("text") or "")
        lower = text_value.lower()
        borough = _extract_borough(text_value)
        category = _extract_category(text_value)
        domain = _normalize_domain(text_value)

        if borough:
            preferred_boroughs.add(borough)
        if category:
            preferred_categories.add(category)

        if domain and _includes_any(lower, ["avoid", "exclude", "block"]):
            blocked_domains.add(domain)

        keywords = _extract_keywords(lower)
        if _includes_any(lower, ["avoid", "exclude", "not"]):
            exclude_keywords.update(keywords)
        else:
            include_keywords.update(keywords)

    return {
        "preferred_boroughs": preferred_boroughs,
        "preferred_categories": preferred_categories,
        "blocked_domains": blocked_domains,
        "include_keywords": include_keywords,
        "exclude_keywords": exclude_keywords,
    }


def _score_org(org: dict, profile: dict) -> int:
    score = 0
    text_blob = " ".join(
        [
            str(org.get("name") or ""),
            str(org.get("description") or ""),
            str(org.get("source") or ""),
            str(org.get("notes") or ""),
        ]
    ).lower()

    borough = str(org.get("borough") or "")
    category = str(org.get("category") or "")
    domain = _normalize_domain(org.get("homepage")) or _normalize_domain(org.get("events_url"))

    if borough and borough in profile["preferred_boroughs"]:
        score += 3
    if category and category in profile["preferred_categories"]:
        score += 3

    if domain and domain in profile["blocked_domains"]:
        score -= 8

    for keyword in profile["include_keywords"]:
        if keyword in text_blob:
            score += 1

    for keyword in profile["exclude_keywords"]:
        if keyword in text_blob:
            score -= 2

    return score


def _select_next_batch_ids(batch_size: int) -> list[int]:
    pending = get_pending_orgs()
    if not pending:
        return []

    profile = _build_learning_profile()
    ranked = [
        {"id": int(org["id"]), "score": _score_org(org, profile), "index": index}
        for index, org in enumerate(pending)
    ]
    ranked.sort(key=lambda item: (-item["score"], item["index"]))

    return [item["id"] for item in ranked[:batch_size]]


def _ensure_active_batch(batch_size: int) -> tuple[dict, list[dict]]:
    state = get_codex_batch_state()
    active_ids = list(state.get("active_batch_ids") or [])

    if active_ids:
        batch = get_orgs_by_ids(active_ids)
        if batch:
            return state, batch

    next_ids = _select_next_batch_ids(batch_size)
    save_codex_batch_state(int(state.get("batch_number") or 1), next_ids)
    refreshed_state = get_codex_batch_state()
    refreshed_batch = get_orgs_by_ids(next_ids)
    return refreshed_state, refreshed_batch


def _batch_payload(state: dict, batch: list[dict]) -> dict:
    stats = get_stats()
    reviewed_count = sum(1 for org in batch if org.get("status") != "pending")
    pending_total = int(stats.get("pending", 0))
    batch_complete = len(batch) > 0 and reviewed_count == len(batch)

    strategies = [_serialize_row(item) for item in get_codex_strategies()]
    approved_preview = [_serialize_row(item) for item in get_orgs(status="approved")[:50]]

    return {
        "batch_number": int(state.get("batch_number") or 1),
        "active_batch_ids": list(state.get("active_batch_ids") or []),
        "batch_size": len(batch),
        "reviewed_count": reviewed_count,
        "batch_complete": batch_complete,
        "pending_total": pending_total,
        "stats": stats,
        "active_batch": [_serialize_row(item) for item in batch],
        "strategies": strategies,
        "approved_preview": approved_preview,
    }


@app.route("/")
def index():
    status = request.args.get("status", "pending")
    borough = request.args.get("borough")
    category = request.args.get("category")
    orgs = get_orgs(
        status=status if status != "all" else None,
        borough=borough if borough else None,
        category=category if category else None,
    )
    stats = get_stats()
    return render_template(
        "index.html",
        orgs=orgs,
        stats=stats,
        current_status=status,
        current_borough=borough,
        current_category=category,
    )


@app.route("/review")
def review():
    """One-at-a-time review mode — the fast path."""
    orgs = get_orgs(status="pending")
    if not orgs:
        return render_template("review_done.html", stats=get_stats())
    return render_template("review.html", org=orgs[0], remaining=len(orgs) - 1, stats=get_stats())


@app.route("/codex")
def codex_review():
    state, batch = _ensure_active_batch(BATCH_SIZE_DEFAULT)
    payload = _batch_payload(state, batch)
    return render_template("codex_review.html", stats=get_stats(), payload=payload)


@app.route("/api/orgs/<int:org_id>/status", methods=["POST"])
def set_status(org_id):
    data = request.json or {}
    status = data.get("status")
    notes = data.get("notes")
    if status not in ("approved", "rejected", "maybe", "pending"):
        return jsonify({"error": "invalid status"}), 400
    update_org_status(org_id, status, notes)
    return jsonify({"ok": True})


@app.route("/api/orgs/<int:org_id>", methods=["PATCH"])
def patch_org(org_id):
    data = request.json or {}
    update_org(org_id, **data)
    return jsonify({"ok": True})


@app.route("/api/orgs", methods=["POST"])
def add_org():
    data = request.json or {}
    if not data.get("name"):
        return jsonify({"error": "name required"}), 400
    org_id = upsert_org(
        name=data["name"],
        homepage=data.get("homepage"),
        events_url=data.get("events_url"),
        description=data.get("description"),
        borough=data.get("borough"),
        category=data.get("category"),
        source=data.get("source", "manual"),
    )
    return jsonify({"ok": True, "id": org_id})


@app.route("/api/orgs/bulk", methods=["POST"])
def bulk_add():
    data = request.json
    if not isinstance(data, list):
        return jsonify({"error": "expected a JSON array"}), 400

    ids = []
    for item in data:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        org_id = upsert_org(
            name=item["name"],
            homepage=item.get("homepage"),
            events_url=item.get("events_url"),
            description=item.get("description"),
            borough=item.get("borough"),
            category=item.get("category"),
            source=item.get("source", "bulk_import"),
        )
        ids.append(org_id)

    return jsonify({"ok": True, "count": len(ids), "ids": ids})


@app.route("/api/stats")
def stats():
    return jsonify(get_stats())


@app.route("/export")
def export():
    """Export approved orgs as JSON."""
    orgs = get_orgs(status="approved")
    return jsonify(orgs)


@app.route("/api/codex/state")
def codex_state():
    batch_size = request.args.get("batch_size", BATCH_SIZE_DEFAULT, type=int)
    batch_size = max(1, min(batch_size, 100))
    state, batch = _ensure_active_batch(batch_size)
    return jsonify(_batch_payload(state, batch))


@app.route("/api/codex/review/<int:org_id>", methods=["POST"])
def codex_review_org(org_id):
    data = request.json or {}
    feedback = str(data.get("feedback") or "").strip()

    org = get_org(org_id)
    if not org:
        return jsonify({"error": "org not found"}), 404

    inference = _infer_review_from_feedback(feedback, org)
    status = str(data.get("status") or inference["status"]).strip().lower()
    if status not in ("approved", "rejected", "maybe", "pending"):
        return jsonify({"error": "invalid status"}), 400

    updates = {
        "borough": data.get("borough") or inference["inferred_borough"],
        "category": data.get("category") or inference["inferred_category"],
        "events_url": data.get("events_url") or inference["inferred_events_url"],
    }
    safe_updates = {key: value for key, value in updates.items() if value}
    if safe_updates:
        update_org(org_id, **safe_updates)

    notes = feedback or data.get("notes") or org.get("notes")
    update_org_status(org_id, status, notes)

    refreshed_state = get_codex_batch_state()
    refreshed_batch = get_orgs_by_ids(refreshed_state.get("active_batch_ids") or [])

    return jsonify(
        {
            "ok": True,
            "org_id": org_id,
            "status": status,
            "inference": inference,
            "state": _batch_payload(refreshed_state, refreshed_batch),
        }
    )


@app.route("/api/codex/next-batch", methods=["POST"])
def codex_next_batch():
    data = request.json or {}
    batch_size = int(data.get("batch_size") or BATCH_SIZE_DEFAULT)
    batch_size = max(1, min(batch_size, 100))

    state, batch = _ensure_active_batch(batch_size)
    if batch and any(org.get("status") == "pending" for org in batch):
        return jsonify({"error": "complete all current batch reviews first"}), 400

    next_ids = _select_next_batch_ids(batch_size)
    batch_number = int(state.get("batch_number") or 1)
    if next_ids:
        batch_number += 1

    save_codex_batch_state(batch_number, next_ids)

    refreshed_state, refreshed_batch = _ensure_active_batch(batch_size)
    return jsonify({"ok": True, "state": _batch_payload(refreshed_state, refreshed_batch)})


@app.route("/api/codex/strategies", methods=["GET", "POST"])
def codex_strategies():
    if request.method == "GET":
        return jsonify({"strategies": [_serialize_row(item) for item in get_codex_strategies()]})

    data = request.json or {}
    text_value = str(data.get("text") or "").strip()
    if not text_value:
        return jsonify({"error": "text required"}), 400

    strategy_id = add_codex_strategy(text_value=text_value, active=True)
    strategies = get_codex_strategies()
    strategy = next((item for item in strategies if int(item["id"]) == strategy_id), None)
    return jsonify({"ok": True, "strategy": _serialize_row(strategy) if strategy else None})


@app.route("/api/codex/strategies/<int:strategy_id>", methods=["PATCH"])
def codex_strategy_toggle(strategy_id):
    data = request.json or {}
    if "active" not in data:
        return jsonify({"error": "active required"}), 400

    set_codex_strategy_active(strategy_id, bool(data["active"]))
    return jsonify({"ok": True})


def main():
    parser = argparse.ArgumentParser(description="Org review admin panel")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    init_db()
    print(f"\n  Org Review Panel:   http://{args.host}:{args.port}")
    print(f"  Review mode:        http://{args.host}:{args.port}/review")
    print(f"  Codex curation:     http://{args.host}:{args.port}/codex")
    print(f"  Export approved:    http://{args.host}:{args.port}/export\n")
    app.run(host=args.host, port=args.port, debug=True)


if __name__ == "__main__":
    main()
