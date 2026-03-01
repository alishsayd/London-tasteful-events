"""Flask app for public browse + password-protected admin."""
from __future__ import annotations
import hmac
import os
from datetime import date, datetime
from typing import Any
from flask import Flask, Response, jsonify, render_template, request
from app.db import (
    finish_import_run,
    get_active_orgs,
    get_import_runs,
    get_org,
    get_public_orgs,
    get_review_queue_orgs,
    get_stats,
    init_db,
    normalize_org_taxonomy,
    start_import_run,
    update_org,
    upsert_org,
)
from app.import_org_csv import run_csv_import
app = Flask(__name__, template_folder="templates", static_folder="static")
init_db()
def _auth_enabled() -> bool:
    return bool(str(os.getenv("ADMIN_PASSWORD") or "").strip())
def _auth_required() -> Response:
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="London Tasteful Events Admin"'},
    )
@app.before_request
def require_basic_auth():
    if request.path in {"/", "/browse", "/healthz", "/favicon.ico"}:
        return None
    if not _auth_enabled():
        return None
    expected_user = str(os.getenv("ADMIN_USERNAME") or "admin").strip() or "admin"
    expected_pass = str(os.getenv("ADMIN_PASSWORD") or "").strip()
    auth = request.authorization
    if not auth or (auth.type or "").lower() != "basic":
        return _auth_required()
    if not hmac.compare_digest(str(auth.username or ""), expected_user):
        return _auth_required()
    if not hmac.compare_digest(str(auth.password or ""), expected_pass):
        return _auth_required()
    return None
def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value
def _serialize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: _json_safe(v) for k, v in row.items()}
def _queue_reason(row: dict[str, Any]) -> str:
    explicit = str(row.get("review_needed_reason") or "").strip()
    if explicit:
        return explicit
    missing: list[str] = []
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
    empty = int(row.get("consecutive_empty_extracts") or 0)
    if empty >= 3:
        return f"{empty} consecutive empty extracts"
    if str(row.get("issue_state") or "").strip() == "open":
        return "Manually flagged for review"
    return "Needs manual review"
def _feedback_implies_reject(feedback: str) -> bool:
    lower = str(feedback or "").lower().strip()
    if not lower:
        return False
    return any(
        signal in lower
        for signal in (
            "reject",
            "no events",
            "doesn't have events",
            "does not have events",
            "reservation-only",
            "reservation only",
            "restaurant only",
            "remove this org",
            "skip this org",
            "not relevant",
        )
    )
def _state_payload() -> dict[str, Any]:
    queue = [_serialize_row(row) or {} for row in get_review_queue_orgs(limit=300)]
    active = [_serialize_row(row) or {} for row in get_active_orgs()]
    runs = [_serialize_row(row) or {} for row in get_import_runs(limit=8)]
    for row in queue:
        row["queue_reason"] = _queue_reason(row)
    return {
        "stats": get_stats(),
        "queue_total": len(queue),
        "queue": queue,
        "active_orgs": active,
        "import_runs": runs,
    }
def _public_payload() -> dict[str, Any]:
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
    orgs.sort(key=lambda item: item["name"].lower())
    return {"orgs": orgs}
@app.route("/")
@app.route("/browse")
def browse():
    return render_template("public.html", payload=_public_payload())
@app.route("/admin")
def home():
    return render_template("admin.html", stats=get_stats(), payload=_state_payload())
@app.route("/healthz")
def healthz():
    init_db()
    return jsonify({"ok": True})
@app.route("/api/stats")
def stats():
    return jsonify(get_stats())
@app.route("/export")
def export():
    return jsonify(get_active_orgs())
@app.route("/api/admin/state")
def admin_state():
    return jsonify(_state_payload())
@app.route("/api/orgs", methods=["POST"])
def add_org():
    data = request.json or {}
    name = str(data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    try:
        org_id = upsert_org(
            name=name,
            homepage=data.get("homepage"),
            events_url=data.get("events_url"),
            description=data.get("description"),
            borough=data.get("borough"),
            org_type=data.get("org_type") or data.get("type"),
            primary_type=data.get("primary_type"),
            parent_org_id=data.get("parent_org_id"),
            source=data.get("source") or "manual",
        )
        if str(data.get("events_url") or "").strip():
            update_org(org_id, issue_state="none", review_needed_reason=None, active=True, crawl_paused=False)
        else:
            update_org(org_id, issue_state="open", review_needed_reason="Missing events URL", active=True, crawl_paused=False)
        return jsonify({"ok": True, "id": org_id})
    except Exception:
        app.logger.exception("Failed to add org")
        return jsonify({"error": "Failed to add org"}), 500
@app.route("/api/admin/review/<int:org_id>", methods=["POST"])
def review_org(org_id: int):
    org = get_org(org_id)
    if not org:
        return jsonify({"error": "org not found"}), 404
    data = request.json or {}
    action = str(data.get("action") or "resolve").strip().lower()
    if action not in {"resolve", "snooze", "open"}:
        return jsonify({"error": "invalid action"}), 400
    feedback = str(data.get("feedback") or "").strip()
    updates: dict[str, Any] = {}
    if isinstance(data.get("name"), str) and data.get("name").strip():
        updates["name"] = data.get("name").strip()
    if isinstance(data.get("events_url"), str):
        updates["events_url"] = data.get("events_url").strip() or None
    if isinstance(data.get("borough"), str):
        updates["borough"] = data.get("borough").strip() or None
    if isinstance(data.get("org_type"), str):
        updates["org_type"] = data.get("org_type").strip() or None
    if "active" in data:
        updates["active"] = bool(data.get("active"))
    if "crawl_paused" in data:
        updates["crawl_paused"] = bool(data.get("crawl_paused"))
    if "parent_org_id" in data:
        updates["parent_org_id"] = data.get("parent_org_id")
    if feedback:
        updates["notes"] = feedback
    custom_reason = data.get("review_needed_reason") if isinstance(data.get("review_needed_reason"), str) else None
    custom_reason = custom_reason.strip() if custom_reason else None
    reject_intent = _feedback_implies_reject(feedback)
    if action == "resolve":
        updates["issue_state"] = "resolved"
        updates["review_needed_reason"] = custom_reason
        updates["consecutive_failures"] = 0
        updates["consecutive_empty_extracts"] = 0
        if reject_intent:
            updates["status"] = "rejected"
            updates["active"] = False
            updates["crawl_paused"] = True
            updates["review_needed_reason"] = updates.get("review_needed_reason") or "Rejected by admin: not an events org"
    elif action == "snooze":
        updates["issue_state"] = "snoozed"
        updates["review_needed_reason"] = custom_reason or _queue_reason(org)
    else:
        updates["issue_state"] = "open"
        updates["review_needed_reason"] = custom_reason or _queue_reason(org)
    effective_events = updates.get("events_url") if "events_url" in updates else org.get("events_url")
    if not str(effective_events or "").strip() and action in {"open", "snooze"}:
        updates["review_needed_reason"] = updates.get("review_needed_reason") or "Missing events URL"
    update_org(org_id, **updates)
    return jsonify({"ok": True, "org_id": org_id, "action": action, "state": _state_payload()})
@app.route("/api/admin/taxonomy/normalize", methods=["POST"])
def normalize_taxonomy_now():
    data = request.json or {}
    summary = normalize_org_taxonomy(dry_run=bool(data.get("dry_run", False)))
    return jsonify({"ok": True, "summary": summary, "state": _state_payload()})
@app.route("/api/admin/import/csv", methods=["POST"])
def import_csv():
    upload = request.files.get("file")
    if not upload or not str(upload.filename or "").strip():
        return jsonify({"error": "CSV file is required"}), 400
    raw = upload.read() or b""
    if not raw:
        return jsonify({"error": "Uploaded file is empty"}), 400
    if len(raw) > 5 * 1024 * 1024:
        return jsonify({"error": "CSV file too large (max 5MB)"}), 400
    apply_changes = str(request.form.get("apply") or "").strip().lower() in {"1", "true", "yes", "on"}
    source = str(request.form.get("source") or "").strip() or "csv_admin_import"
    file_name = str(upload.filename or "").strip() or None
    csv_text = raw.decode("utf-8-sig", errors="replace")
    run_id: int | None = None
    try:
        run_id = start_import_run(trigger="manual", source=source, file_name=file_name, row_count=max(0, csv_text.count("\n") - 1))
        result = run_csv_import(csv_text=csv_text, apply=apply_changes, source=source)
        finish_import_run(run_id, status="success", summary=result)
    except ValueError as exc:
        if run_id:
            finish_import_run(run_id, status="failed", error=str(exc))
        return jsonify({"error": str(exc)}), 400
    except Exception:
        if run_id:
            finish_import_run(run_id, status="failed", error="Failed to import CSV")
        app.logger.exception("CSV import failed")
        return jsonify({"error": "Failed to import CSV"}), 500
    payload: dict[str, Any] = {"ok": True, "result": result}
    if apply_changes:
        payload["state"] = _state_payload()
    return jsonify(payload)
def main() -> None:
    init_db()
    app.run(host="127.0.0.1", port=int(os.getenv("PORT", "5000")), debug=True)
if __name__ == "__main__":
    main()
