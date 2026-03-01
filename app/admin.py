"""Flask admin and public app for London Tasteful Events."""

from __future__ import annotations

import argparse
import hmac
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from flask import Flask, Response, jsonify, render_template, request

from app.db import (
    cleanup_recent_discovery_garbage,
    get_active_orgs,
    get_discovery_runs,
    get_latest_discovery_run,
    get_org,
    get_public_orgs,
    get_review_queue_orgs,
    get_stats,
    init_db,
    normalize_org_taxonomy,
    update_org,
    upsert_org,
)
from app.discover import run_discovery_cycle
from app.import_org_csv import run_csv_import

app = Flask(__name__, template_folder="templates", static_folder="static")

# Ensure schema exists when running under WSGI/Gunicorn.
init_db()


# -----------------------------
# Auth helpers
# -----------------------------

def _auth_required() -> Response:
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="London Tasteful Events Admin"'},
    )


def _auth_enabled() -> bool:
    return bool(str(os.getenv("ADMIN_PASSWORD") or "").strip())


def _is_allowed_without_auth(path: str) -> bool:
    return path in {"/", "/browse", "/healthz", "/favicon.ico"}


@app.before_request
def require_basic_auth():
    if _is_allowed_without_auth(request.path):
        return None

    if not _auth_enabled():
        return None

    expected_username = str(os.getenv("ADMIN_USERNAME") or "admin").strip() or "admin"
    expected_password = str(os.getenv("ADMIN_PASSWORD") or "").strip()
    auth = request.authorization

    if not auth or (auth.type or "").lower() != "basic":
        return _auth_required()

    provided_username = str(auth.username or "")
    provided_password = str(auth.password or "")
    if not hmac.compare_digest(provided_username, expected_username):
        return _auth_required()
    if not hmac.compare_digest(provided_password, expected_password):
        return _auth_required()
    return None


# -----------------------------
# Serialization helpers
# -----------------------------

def _json_safe(value: Any):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _serialize_row(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {key: _json_safe(value) for key, value in row.items()}


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
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

    missing = []
    if not str(row.get("events_url") or "").strip():
        missing.append("events URL")
    if not str(row.get("borough") or "").strip():
        missing.append("borough")
    if not str(row.get("org_type") or "").strip():
        missing.append("type")
    if missing:
        return "Missing " + ", ".join(missing)

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
    lower = str(feedback or "").lower().strip()
    if not lower:
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

    queue_payload: list[dict[str, Any]] = []
    for row in queue_rows:
        item = _serialize_row(row) or {}
        item["queue_reason"] = _queue_reason(row)
        queue_payload.append(item)

    active_payload: list[dict[str, Any]] = []
    for row in active_rows:
        item = _serialize_row(row) or {}
        item["is_new"] = _is_new_org(row)
        active_payload.append(item)

    return {
        "stats": get_stats(),
        "queue_total": len(queue_payload),
        "queue": queue_payload,
        "active_orgs": active_payload,
        "discovery_latest": _serialize_row(get_latest_discovery_run()),
        "discovery_runs": [_serialize_row(item) for item in get_discovery_runs(limit=8)],
    }


def _public_payload() -> dict:
    rows = get_public_orgs()
    orgs = []
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue

        orgs.append(
            {
                "id": int(row["id"]),
                "name": name,
                "borough": str(row.get("borough") or "").strip() or "Unspecified",
                "org_type": str(row.get("org_type") or "").strip() or "organisation",
                "primary_type": str(row.get("primary_type") or "").strip() or "organisation",
                "events_url": str(row.get("events_url") or "").strip() or None,
                "homepage": str(row.get("homepage") or "").strip() or None,
            }
        )

    orgs.sort(key=lambda item: (item["org_type"].lower(), item["borough"].lower(), item["name"].lower()))
    return {"orgs": orgs}


# -----------------------------
# Routes: public + admin
# -----------------------------

@app.route("/")
@app.route("/browse")
def browse():
    return render_template("public.html", payload=_public_payload())


@app.route("/admin")
def home():
    return render_template("admin.html", stats=get_stats(), payload=_state_payload())


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
            org_type=data.get("org_type") or data.get("type"),
            primary_type=data.get("primary_type"),
            parent_org_id=data.get("parent_org_id"),
            source=data.get("source", "manual"),
        )

        if not str(data.get("events_url") or "").strip():
            update_org(org_id, issue_state="open", review_needed_reason="Missing events URL", active=True, crawl_paused=False)
        else:
            update_org(org_id, issue_state="none", review_needed_reason=None, active=True, crawl_paused=False)

        return jsonify({"ok": True, "id": org_id})
    except Exception:
        app.logger.exception("Failed to add org via /api/orgs")
        return jsonify({"error": "Failed to add org"}), 500


@app.route("/healthz")
def healthz():
    init_db()
    return jsonify({"ok": True})


@app.route("/api/stats")
def stats():
    init_db()
    return jsonify(get_stats())


@app.route("/export")
def export():
    return jsonify(get_active_orgs())


@app.route("/api/admin/state")
def admin_state():
    return jsonify(_state_payload())


@app.route("/api/admin/review/<int:org_id>", methods=["POST"])
def review_org(org_id: int):
    data = request.json or {}

    org = get_org(org_id)
    if not org:
        return jsonify({"error": "org not found"}), 404

    action = str(data.get("action") or "resolve").strip().lower()
    if action not in {"resolve", "snooze", "open"}:
        return jsonify({"error": "invalid action"}), 400

    feedback = str(data.get("feedback") or "").strip()

    updates: dict[str, Any] = {}
    if isinstance(data.get("events_url"), str):
        updates["events_url"] = data.get("events_url").strip() or None
    if isinstance(data.get("name"), str):
        name_value = data.get("name").strip()
        if name_value:
            updates["name"] = name_value
    if isinstance(data.get("borough"), str):
        updates["borough"] = data.get("borough").strip() or None

    type_value = data.get("org_type")
    if isinstance(type_value, str):
        updates["org_type"] = type_value.strip() or None

    if "crawl_paused" in data:
        updates["crawl_paused"] = bool(data.get("crawl_paused"))
    if "active" in data:
        updates["active"] = bool(data.get("active"))
    if "parent_org_id" in data:
        updates["parent_org_id"] = data.get("parent_org_id")
    if feedback:
        updates["notes"] = feedback

    reason_text = None
    if isinstance(data.get("review_needed_reason"), str):
        reason_text = data.get("review_needed_reason").strip() or None

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
            updates["review_needed_reason"] = updates.get("review_needed_reason") or "Missing events URL"
        elif action == "snooze":
            updates["review_needed_reason"] = updates.get("review_needed_reason") or "Missing events URL"

    update_org(org_id, **updates)
    return jsonify({"ok": True, "org_id": org_id, "action": action, "state": _state_payload()})


@app.route("/api/admin/discovery/run", methods=["POST"])
def run_discovery_now():
    data = request.json or {}

    def _optional_int(name: str) -> int | None:
        if name not in data:
            return None
        try:
            value = int(data.get(name))
            return value if value > 0 else None
        except Exception:
            return None

    summary = run_discovery_cycle(
        trigger="manual",
        max_queries=_optional_int("max_queries"),
        max_results_per_query=_optional_int("max_results_per_query"),
        max_candidates=_optional_int("max_candidates"),
        request_timeout=_optional_int("request_timeout"),
        dry_run=bool(data.get("dry_run", False)),
        search_provider="openai_web",
    )
    return jsonify({"ok": True, "summary": summary, "state": _state_payload()})


@app.route("/api/admin/discovery/cleanup", methods=["POST"])
def cleanup_discovery_now():
    data = request.json or {}

    def _optional_int(name: str, default: int) -> int:
        try:
            value = int(data.get(name, default))
            return value if value > 0 else default
        except Exception:
            return default

    summary = cleanup_recent_discovery_garbage(
        days=_optional_int("days", 7),
        dry_run=bool(data.get("dry_run", False)),
        limit=_optional_int("limit", 1000),
    )
    return jsonify({"ok": True, "summary": summary, "state": _state_payload()})


@app.route("/api/admin/taxonomy/normalize", methods=["POST"])
def normalize_taxonomy_now():
    data = request.json or {}
    summary = normalize_org_taxonomy(dry_run=bool(data.get("dry_run", False)))
    return jsonify({"ok": True, "summary": summary, "state": _state_payload()})


@app.route("/api/admin/discovery/runs")
def discovery_runs():
    return jsonify(
        {
            "latest": _serialize_row(get_latest_discovery_run()),
            "runs": [_serialize_row(item) for item in get_discovery_runs(limit=20)],
        }
    )


@app.route("/api/admin/import/csv", methods=["POST"])
def import_csv():
    upload = request.files.get("file")
    if not upload or not str(upload.filename or "").strip():
        return jsonify({"error": "CSV file is required"}), 400

    try:
        raw = upload.read() or b""
        if not raw:
            return jsonify({"error": "Uploaded file is empty"}), 400
        if len(raw) > 5 * 1024 * 1024:
            return jsonify({"error": "CSV file too large (max 5MB)"}), 400

        apply_changes = str(request.form.get("apply") or "").strip().lower() in {"1", "true", "yes", "on"}
        source = str(request.form.get("source") or "").strip() or "csv_admin_import"
        csv_text = raw.decode("utf-8-sig", errors="replace")

        result = run_csv_import(csv_text=csv_text, apply=apply_changes, source=source)
        payload: dict[str, Any] = {"ok": True, "result": result}
        if apply_changes:
            payload["state"] = _state_payload()
        return jsonify(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception:
        app.logger.exception("Failed CSV admin import")
        return jsonify({"error": "Failed to import CSV"}), 500


# -----------------------------
# Entrypoint
# -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Org review admin panel")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    init_db()
    print(f"\n  Browse Orgs:        http://{args.host}:{args.port}")
    print(f"  Org Curation:       http://{args.host}:{args.port}/admin")
    print(f"  Export active:      http://{args.host}:{args.port}/export\n")
    app.run(host=args.host, port=args.port, debug=True)


if __name__ == "__main__":
    main()
