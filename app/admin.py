"""
Flask admin panel for reviewing candidate orgs.

Usage:
    python -m app.admin          # start on port 5000
    python -m app.admin --port 8080
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from flask import Flask, jsonify, render_template, request

from app.db import (
    add_strategy,
    get_active_orgs,
    get_review_queue_orgs,
    get_strategies,
    get_org,
    get_stats,
    init_db,
    set_strategy_active,
    update_org,
    upsert_org,
)

app = Flask(__name__, template_folder="templates", static_folder="static")

# Ensure schema exists when running under WSGI/Gunicorn (main() is not executed there).
init_db()

def _json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value

def _serialize_row(row: dict) -> dict:
    return {key: _json_safe(value) for key, value in row.items()}

def _parse_dt(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None

def _is_new_org(row: dict) -> bool:
    created_at = _parse_dt(row.get("created_at"))
    if not created_at:
        return False
    return created_at >= (datetime.now(timezone.utc) - timedelta(days=7))

def _queue_reason(row: dict) -> str:
    custom_reason = str(row.get("review_needed_reason") or "").strip()
    if custom_reason:
        return custom_reason

    if not str(row.get("events_url") or "").strip():
        return "Missing events URL"

    failures = int(row.get("consecutive_failures") or 0)
    if failures >= 3:
        return f"{failures} consecutive crawl failures"

    empty_runs = int(row.get("consecutive_empty_extracts") or 0)
    if empty_runs >= 3:
        return f"{empty_runs} consecutive empty extracts"

    if str(row.get("issue_state") or "") == "open":
        return "Manually flagged for review"

    return "Needs manual review"

def _feedback_implies_reject(feedback: str) -> bool:
    lower = str(feedback or "").lower()
    if not lower.strip():
        return False

    reject_signals = [
        "reject",
        "no events",
        "doesn't have events",
        "does not have events",
        "not an event",
        "reservation-only",
        "reservation only",
        "restaurant only",
        "dining establishment",
        "remove this org",
        "skip this org",
        "not relevant",
    ]
    return any(signal in lower for signal in reject_signals)

def _state_payload() -> dict:
    queue_rows = get_review_queue_orgs(limit=300)
    active_rows = get_active_orgs()

    queue_payload = []
    for row in queue_rows:
        serialized = _serialize_row(row)
        serialized["queue_reason"] = _queue_reason(row)
        queue_payload.append(serialized)

    active_payload = []
    for row in active_rows:
        serialized = _serialize_row(row)
        serialized["is_new"] = _is_new_org(row)
        active_payload.append(serialized)

    return {
        "stats": get_stats(),
        "queue_total": len(queue_payload),
        "queue": queue_payload,
        "active_orgs": active_payload,
        "strategies": [_serialize_row(item) for item in get_strategies()],
    }

@app.route("/")
def home():
    payload = _state_payload()
    return render_template("admin.html", stats=get_stats(), payload=payload)


@app.route("/api/orgs", methods=["POST"])
def add_org():
    data = request.json or {}
    if not data.get("name"):
        return jsonify({"error": "name required"}), 400

    try:
        org_id = upsert_org(
            name=data["name"],
            homepage=data.get("homepage"),
            events_url=data.get("events_url"),
            description=data.get("description"),
            borough=data.get("borough"),
            category=data.get("category"),
            source=data.get("source", "manual"),
        )

        events_url = str(data.get("events_url") or "").strip()
        if not events_url:
            update_org(org_id, issue_state="open", review_needed_reason="Missing events URL", active=True, crawl_paused=False)
        else:
            update_org(org_id, issue_state="none", review_needed_reason=None, active=True, crawl_paused=False)
    except Exception as exc:
        return jsonify({"error": f"Failed to add org: {exc}"}), 500

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
        if not str(item.get("events_url") or "").strip():
            update_org(org_id, issue_state="open", review_needed_reason="Missing events URL", active=True, crawl_paused=False)
        ids.append(org_id)

    return jsonify({"ok": True, "count": len(ids), "ids": ids})

@app.route("/healthz")
def healthz():
    get_stats()
    return jsonify({"ok": True})

@app.route("/api/stats")
def stats():
    init_db()
    return jsonify(get_stats())

@app.route("/export")
def export():
    """Export active orgs as JSON."""
    return jsonify(get_active_orgs())

@app.route("/api/admin/state")
def admin_state():
    return jsonify(_state_payload())

@app.route("/api/admin/review/<int:org_id>", methods=["POST"])
def review_org(org_id):
    data = request.json or {}

    org = get_org(org_id)
    if not org:
        return jsonify({"error": "org not found"}), 404

    action = str(data.get("action") or "resolve").strip().lower()
    if action not in ("resolve", "snooze", "open"):
        return jsonify({"error": "invalid action"}), 400

    feedback = str(data.get("feedback") or "").strip()
    events_url = data.get("events_url")
    review_reason = data.get("review_needed_reason")

    updates = {}

    if isinstance(events_url, str):
        updates["events_url"] = events_url.strip() or None

    if isinstance(data.get("borough"), str):
        updates["borough"] = data.get("borough").strip() or None

    if isinstance(data.get("category"), str):
        updates["category"] = data.get("category").strip() or None

    if "crawl_paused" in data:
        updates["crawl_paused"] = bool(data.get("crawl_paused"))

    if "active" in data:
        updates["active"] = bool(data.get("active"))

    if feedback:
        updates["notes"] = feedback

    reason_text = None
    if isinstance(review_reason, str):
        reason_text = review_reason.strip() or None

    reject_intent = _feedback_implies_reject(feedback)

    if action == "resolve":
        updates["issue_state"] = "resolved"
        updates["review_needed_reason"] = reason_text
        updates["consecutive_failures"] = 0
        updates["consecutive_empty_extracts"] = 0

        if reject_intent:
            updates["status"] = "rejected"
            updates["active"] = False
            updates["crawl_paused"] = True
            if not updates.get("review_needed_reason"):
                updates["review_needed_reason"] = "Rejected by admin: not an events org"
    elif action == "snooze":
        updates["issue_state"] = "snoozed"
        updates["review_needed_reason"] = reason_text or _queue_reason(org)
    else:
        updates["issue_state"] = "open"
        updates["review_needed_reason"] = reason_text or _queue_reason(org)

    effective_events_url = updates.get("events_url")
    if effective_events_url is None:
        effective_events_url = org.get("events_url")

    if not str(effective_events_url or "").strip():
        if action == "open":
            updates["issue_state"] = "open"
            if not updates.get("review_needed_reason"):
                updates["review_needed_reason"] = "Missing events URL"
        elif action == "snooze":
            if not updates.get("review_needed_reason"):
                updates["review_needed_reason"] = "Missing events URL"

    update_org(org_id, **updates)

    return jsonify(
        {
            "ok": True,
            "org_id": org_id,
            "action": action,
            "state": _state_payload(),
        }
    )

@app.route("/api/admin/strategies", methods=["GET", "POST"])
def strategies():
    if request.method == "GET":
        return jsonify({"strategies": [_serialize_row(item) for item in get_strategies()]})

    data = request.json or {}
    text_value = str(data.get("text") or "").strip()
    if not text_value:
        return jsonify({"error": "text required"}), 400

    strategy_id = add_strategy(text_value=text_value, active=True)
    strategies = get_strategies()
    strategy = next((item for item in strategies if int(item["id"]) == strategy_id), None)
    return jsonify({"ok": True, "strategy": _serialize_row(strategy) if strategy else None})

@app.route("/api/admin/strategies/<int:strategy_id>", methods=["PATCH"])
def strategy_toggle(strategy_id):
    data = request.json or {}
    if "active" not in data:
        return jsonify({"error": "active required"}), 400

    set_strategy_active(strategy_id, bool(data["active"]))
    return jsonify({"ok": True})

def main():
    parser = argparse.ArgumentParser(description="Org review admin panel")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    init_db()
    print(f"\n  Org Curation:       http://{args.host}:{args.port}")
    print(f"  Export active:      http://{args.host}:{args.port}/export\n")
    app.run(host=args.host, port=args.port, debug=True)

if __name__ == "__main__":
    main()
