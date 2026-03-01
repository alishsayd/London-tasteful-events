"""Lean CSV import for admin bulk-add flow."""

from __future__ import annotations

import csv
import io
import re
from typing import Any
from urllib.parse import urlparse

from app.db import get_db, upsert_org, update_org


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _tokenize(value: Any) -> str:
    text_value = _clean(value).lower()
    text_value = re.sub(r"[^a-z0-9]+", " ", text_value)
    return re.sub(r"\s+", " ", text_value).strip()


def _canonical_url(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
    except Exception:
        return ""
    if parsed.scheme not in {"http", "https"}:
        return ""
    host = (parsed.netloc or "").lower().replace("www.", "")
    if not host:
        return ""
    path = (parsed.path or "/").rstrip("/") or "/"
    return f"{parsed.scheme}://{host}{path}"


def _normalize_org_type(value: Any) -> str | None:
    raw = _tokenize(value)
    if not raw:
        return None
    mapping = {
        "live music venue": "live_music_venue",
        "makerspace": "makers_space",
        "makers space": "makers_space",
        "maker space": "makers_space",
        "cultural center": "cultural_centre",
        "learned society": "learned_society",
        "one off event": "festival",
        "one-off event": "festival",
        "one_off_event": "festival",
        "community cinema": "cinema",
        "bookshop events": "bookshop",
        "social community center": "cultural_centre",
        "community space": "cultural_centre",
        "lecture series": "learned_society",
        "education": "learned_society",
        "other": "organisation",
        "poetry readings": "organisation",
    }
    if raw in mapping:
        return mapping[raw]
    snake = raw.replace(" ", "_")
    return snake or None


def _header_value(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    for key in keys:
        if key in lowered:
            return _clean(lowered[key])
    return ""


def _normalize_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    name = _header_value(raw, ("name", "org", "organisation", "organization", "entity"))
    if not name:
        return None

    homepage = _canonical_url(_header_value(raw, ("homepage", "website", "url", "site", "official_url")))
    events_url = _canonical_url(_header_value(raw, ("events url", "events_url", "event_url", "whats_on_url", "whatson_url")))

    return {
        "name": name,
        "homepage": homepage or None,
        "events_url": events_url or None,
        "borough": _header_value(raw, ("borough", "area", "district")) or None,
        "org_type": _normalize_org_type(_header_value(raw, ("org_type", "type", "category"))),
        "description": _header_value(raw, ("description", "notes", "summary")) or None,
    }


def _read_rows(csv_text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("CSV is missing a header row")

    rows: list[dict[str, Any]] = []
    for raw in reader:
        if not isinstance(raw, dict):
            continue
        row = _normalize_row(raw)
        if row:
            rows.append(row)
    return rows


def _load_existing() -> dict[str, dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    by_homepage: dict[str, dict[str, Any]] = {}
    by_events: dict[str, dict[str, Any]] = {}

    with get_db() as conn:
        rows = conn.execute(text("SELECT id, name, homepage, events_url FROM orgs")).mappings().all()

    for row in rows:
        row_dict = dict(row)
        name_key = _tokenize(row_dict.get("name"))
        homepage = _canonical_url(row_dict.get("homepage"))
        events = _canonical_url(row_dict.get("events_url"))
        if name_key and name_key not in by_name:
            by_name[name_key] = row_dict
        if homepage and homepage not in by_homepage:
            by_homepage[homepage] = row_dict
        if events and events not in by_events:
            by_events[events] = row_dict

    return {"by_name": by_name, "by_homepage": by_homepage, "by_events": by_events}


def _match_existing(row: dict[str, Any], indexes: dict[str, Any]) -> dict[str, Any] | None:
    events = _canonical_url(row.get("events_url"))
    homepage = _canonical_url(row.get("homepage"))
    name_key = _tokenize(row.get("name"))

    if events and events in indexes["by_events"]:
        return indexes["by_events"][events]
    if homepage and homepage in indexes["by_homepage"]:
        return indexes["by_homepage"][homepage]
    if name_key and name_key in indexes["by_name"]:
        return indexes["by_name"][name_key]
    return None


def _review_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not _clean(row.get("events_url")):
        reasons.append("missing_events_url")
    if not _clean(row.get("borough")):
        reasons.append("missing_borough")
    if not _clean(row.get("org_type")):
        reasons.append("missing_org_type")
    if not _clean(row.get("homepage")):
        reasons.append("missing_homepage")
    return reasons


def _plan_rows(rows: list[dict[str, Any]], indexes: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    planned: list[dict[str, Any]] = []
    seen: set[str] = set()

    reason_counts: dict[str, int] = {}
    safe_rows = 0
    review_rows = 0
    existing_matches = 0

    for row in rows:
        key = _canonical_url(row.get("events_url")) or _canonical_url(row.get("homepage")) or _tokenize(row.get("name"))
        if not key or key in seen:
            continue
        seen.add(key)

        existing = _match_existing(row, indexes)
        reasons = _review_reasons(row)
        is_safe = not reasons

        if existing:
            existing_matches += 1

        if is_safe:
            safe_rows += 1
        else:
            review_rows += 1
            for reason in reasons:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1

        planned.append({
            "row": row,
            "existing_id": int(existing["id"]) if existing else None,
            "reasons": reasons,
        })

    summary = {
        "total_rows": len(rows),
        "planned_rows": len(planned),
        "safe_rows": safe_rows,
        "review_rows": review_rows,
        "existing_db_matches": existing_matches,
        "review_reason_counts": reason_counts,
    }
    return planned, summary


def _apply_plan(planned: list[dict[str, Any]], source: str) -> dict[str, int]:
    inserted_new = 0
    merged_existing = 0
    review_opened = 0
    error_count = 0

    for item in planned:
        row = item["row"]
        reasons = item["reasons"]
        try:
            org_id = upsert_org(
                name=row.get("name"),
                homepage=row.get("homepage"),
                events_url=row.get("events_url"),
                description=row.get("description"),
                borough=row.get("borough"),
                org_type=row.get("org_type"),
                source=source,
            )
            if item.get("existing_id"):
                merged_existing += 1
            else:
                inserted_new += 1

            if reasons:
                review_opened += 1
                update_org(
                    org_id,
                    issue_state="open",
                    review_needed_reason=f"CSV import: {', '.join(reasons)}",
                    active=True,
                    crawl_paused=False,
                )
            else:
                update_org(
                    org_id,
                    issue_state="none",
                    review_needed_reason=None,
                    active=True,
                    crawl_paused=False,
                )
        except Exception:
            error_count += 1

    return {
        "inserted_new": inserted_new,
        "merged_existing": merged_existing,
        "review_opened": review_opened,
        "error_count": error_count,
    }


def run_csv_import(*, csv_text: str, apply: bool = False, source: str = "csv_admin_import") -> dict[str, Any]:
    text_value = _clean(csv_text)
    if not text_value:
        raise ValueError("CSV content is empty")

    rows = _read_rows(csv_text)
    indexes = _load_existing()
    planned, summary = _plan_rows(rows, indexes)

    sample = [
        {
            "name": item["row"].get("name"),
            "borough": item["row"].get("borough"),
            "org_type": item["row"].get("org_type"),
            "events_url": item["row"].get("events_url"),
            "existing_id": item.get("existing_id"),
            "reasons": item.get("reasons") or [],
        }
        for item in planned[:40]
    ]

    if not apply:
        return {"mode": "preview", "summary": summary, "apply": None, "sample": sample}

    apply_result = _apply_plan(planned, source=source)
    return {"mode": "apply", "summary": summary, "apply": apply_result, "sample": sample}


# Local import for SQLAlchemy text to keep top imports minimal.
from sqlalchemy import text  # noqa: E402
