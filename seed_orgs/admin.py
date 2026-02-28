"""
Flask admin panel for reviewing candidate orgs.

Usage:
    python -m seed_orgs.admin          # start on port 5000
    python -m seed_orgs.admin --port 8080
"""

import argparse
from flask import Flask, render_template, request, jsonify, redirect, url_for

from seed_orgs.db import (
    init_db, get_orgs, update_org_status, update_org,
    upsert_org, get_stats,
)

app = Flask(__name__,
            template_folder="templates",
            static_folder="static")


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
    return render_template("index.html", orgs=orgs, stats=stats,
                           current_status=status, current_borough=borough,
                           current_category=category)


@app.route("/review")
def review():
    """One-at-a-time review mode — the fast path."""
    orgs = get_orgs(status="pending")
    if not orgs:
        return render_template("review_done.html", stats=get_stats())
    return render_template("review.html", org=orgs[0],
                           remaining=len(orgs) - 1, stats=get_stats())


@app.route("/api/orgs/<int:org_id>/status", methods=["POST"])
def set_status(org_id):
    data = request.json
    status = data.get("status")
    notes = data.get("notes")
    if status not in ("approved", "rejected", "maybe", "pending"):
        return jsonify({"error": "invalid status"}), 400
    update_org_status(org_id, status, notes)
    return jsonify({"ok": True})


@app.route("/api/orgs/<int:org_id>", methods=["PATCH"])
def patch_org(org_id):
    data = request.json
    update_org(org_id, **data)
    return jsonify({"ok": True})


@app.route("/api/orgs", methods=["POST"])
def add_org():
    data = request.json
    if not data.get("name"):
        return jsonify({"error": "name required"}), 400
    org_id = upsert_org(
        name=data["name"],
        homepage=data.get("homepage"),
        events_url=data.get("events_url"),
        description=data.get("description"),
        borough=data.get("borough"),
        category=data.get("category"),
        source="manual",
    )
    return jsonify({"ok": True, "id": org_id})


@app.route("/api/orgs/bulk", methods=["POST"])
def bulk_add():
    """Add multiple orgs at once from JSON array."""
    data = request.json
    if not isinstance(data, list):
        return jsonify({"error": "expected a JSON array"}), 400
    ids = []
    for item in data:
        if not item.get("name"):
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


def main():
    parser = argparse.ArgumentParser(description="Org review admin panel")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    init_db()
    print(f"\n  Org Review Panel: http://{args.host}:{args.port}")
    print(f"  Review mode:      http://{args.host}:{args.port}/review")
    print(f"  Export approved:   http://{args.host}:{args.port}/export\n")
    app.run(host=args.host, port=args.port, debug=True)


if __name__ == "__main__":
    main()
