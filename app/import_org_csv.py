"""Hardened CSV import for admin bulk ingestion."""
from __future__ import annotations
import csv
import io
from typing import Any
from sqlalchemy import text
from app.db import (
    BAD_NAME_PHRASES,
    BLOCKED_DOMAIN_SUFFIXES,
    _canonical_url,
    _clean,
    _domain,
    _normalize_org_type,
    _token,
    get_db,
    update_org,
    upsert_org,
)
GENERIC_NAME_SNIPPETS = ("what's on", "whats on", "book your tickets", "subscribe", "support", "overview", "events listings", "click here")
def _header_value(row: dict[str, Any], keys: tuple[str, ...]) -> str:
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    for key in keys:
        if key in lowered:
            return _clean(lowered[key])
    return ""
def _normalize_row(raw: dict[str, Any], line_no: int) -> dict[str, Any] | None:
    name = _header_value(raw, ("name", "org", "organisation", "organization", "entity"))
    if not name:
        return None
    return {
        "line_no": line_no,
        "name": name,
        "homepage": _canonical_url(_header_value(raw, ("homepage", "website", "url", "site", "official_url"))) or None,
        "events_url": _canonical_url(_header_value(raw, ("events url", "events_url", "event_url", "whats_on_url", "whatson_url"))) or None,
        "borough": _header_value(raw, ("borough", "area", "district")) or None,
        "org_type": _normalize_org_type(_header_value(raw, ("org_type", "type", "category"))) or None,
        "description": _header_value(raw, ("description", "notes", "summary")) or None,
    }
def _read_rows(csv_text: str) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("CSV is missing a header row")
    out: list[dict[str, Any]] = []
    for line_no, raw in enumerate(reader, start=2):
        if not isinstance(raw, dict):
            continue
        row = _normalize_row(raw, line_no)
        if row:
            out.append(row)
    return out
def _blocked_domain(host: str) -> bool:
    clean_host = _clean(host).lower().replace("www.", "")
    return bool(clean_host) and any(clean_host == suffix or clean_host.endswith(f".{suffix}") for suffix in BLOCKED_DOMAIN_SUFFIXES)
def _is_generic_name(name: str) -> bool:
    key = _token(name)
    return (
        not key
        or (len(key.split()) == 1 and len(key) < 4)
        or any(snippet in key for snippet in GENERIC_NAME_SNIPPETS)
        or any(phrase in key for phrase in BAD_NAME_PHRASES)
    )
def _domain_name_key(row: dict[str, Any]) -> str:
    d = _domain(row.get("homepage")) or _domain(row.get("events_url"))
    n = _token(row.get("name"))
    return f"{d}|{n}" if d and n else ""
def _load_existing_indexes() -> dict[str, dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    by_homepage: dict[str, dict[str, Any]] = {}
    by_events: dict[str, dict[str, Any]] = {}
    by_domain_name: dict[str, dict[str, Any]] = {}
    with get_db() as conn:
        rows = conn.execute(text("SELECT id, name, homepage, events_url FROM orgs")).mappings().all()
    for row in rows:
        item = dict(row)
        name_key = _token(item.get("name"))
        homepage = _canonical_url(item.get("homepage"))
        events = _canonical_url(item.get("events_url"))
        domain_name = _domain_name_key(item)
        if name_key and name_key not in by_name:
            by_name[name_key] = item
        if homepage and homepage not in by_homepage:
            by_homepage[homepage] = item
        if events and events not in by_events:
            by_events[events] = item
        if domain_name and domain_name not in by_domain_name:
            by_domain_name[domain_name] = item
    return {"by_name": by_name, "by_homepage": by_homepage, "by_events": by_events, "by_domain_name": by_domain_name}
def _match_existing(row: dict[str, Any], idx: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    events = _canonical_url(row.get("events_url"))
    homepage = _canonical_url(row.get("homepage"))
    domain_name = _domain_name_key(row)
    name_key = _token(row.get("name"))
    if events and events in idx["by_events"]:
        return idx["by_events"][events], "events_url"
    if homepage and homepage in idx["by_homepage"]:
        return idx["by_homepage"][homepage], "homepage"
    if domain_name and domain_name in idx["by_domain_name"]:
        return idx["by_domain_name"][domain_name], "domain_name"
    if name_key and name_key in idx["by_name"]:
        return idx["by_name"][name_key], "name"
    return None, None
def _hard_reject_reasons(row: dict[str, Any]) -> list[str]:
    name = _clean(row.get("name"))
    homepage = _canonical_url(row.get("homepage"))
    events = _canonical_url(row.get("events_url"))
    host = _domain(homepage) or _domain(events)
    reasons: list[str] = []
    if not name:
        reasons.append("missing_name")
    if not homepage and not events:
        reasons.append("missing_urls")
    if host and _blocked_domain(host):
        reasons.append("blocked_domain")
    if _is_generic_name(name):
        reasons.append("non_entity_name")
    return reasons
def _review_reasons(row: dict[str, Any]) -> list[str]:
    out: list[str] = []
    if not _clean(row.get("events_url")):
        out.append("missing_events_url")
    if not _clean(row.get("borough")):
        out.append("missing_borough")
    if not _clean(row.get("org_type")):
        out.append("missing_org_type")
    return out
def _ingest_key(row: dict[str, Any]) -> str:
    return _canonical_url(row.get("events_url")) or _canonical_url(row.get("homepage")) or _domain_name_key(row) or _token(row.get("name"))
def _plan_rows(rows: list[dict[str, Any]], idx: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    planned: list[dict[str, Any]] = []
    seen: set[str] = set()
    skipped_rows = skipped_duplicates = existing_matches = safe_rows = review_rows = 0
    skip_counts: dict[str, int] = {}
    review_counts: dict[str, int] = {}
    for row in rows:
        rejects = _hard_reject_reasons(row)
        if rejects:
            skipped_rows += 1
            for reason in rejects:
                skip_counts[reason] = skip_counts.get(reason, 0) + 1
            continue
        key = _ingest_key(row)
        if not key or key in seen:
            skipped_rows += 1
            skipped_duplicates += 1
            skip_counts["duplicate_in_csv"] = skip_counts.get("duplicate_in_csv", 0) + 1
            continue
        seen.add(key)
        existing, match_kind = _match_existing(row, idx)
        if existing:
            existing_matches += 1
        reasons = _review_reasons(row)
        if reasons:
            review_rows += 1
            for reason in reasons:
                review_counts[reason] = review_counts.get(reason, 0) + 1
        else:
            safe_rows += 1
        planned.append({"row": row, "existing_id": int(existing["id"]) if existing else None, "match_kind": match_kind, "reasons": reasons})
    return planned, {
        "total_rows": len(rows),
        "accepted_rows": len(rows) - skipped_rows,
        "planned_rows": len(planned),
        "safe_rows": safe_rows,
        "review_rows": review_rows,
        "existing_db_matches": existing_matches,
        "skipped_rows": skipped_rows,
        "skipped_duplicates": skipped_duplicates,
        "skip_reason_counts": skip_counts,
        "review_reason_counts": review_counts,
    }
def _apply_plan(planned: list[dict[str, Any]], source: str) -> dict[str, int]:
    inserted_new = merged_existing = review_opened = error_count = 0
    for item in planned:
        row, reasons = item["row"], item["reasons"]
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
                update_org(org_id, issue_state="open", review_needed_reason=f"CSV import: {', '.join(reasons)}", active=True, crawl_paused=False)
            else:
                update_org(org_id, issue_state="none", review_needed_reason=None, active=True, crawl_paused=False)
        except Exception:
            error_count += 1
    return {"inserted_new": inserted_new, "merged_existing": merged_existing, "review_opened": review_opened, "error_count": error_count}
def run_csv_import(*, csv_text: str, apply: bool = False, source: str = "csv_admin_import") -> dict[str, Any]:
    if not _clean(csv_text):
        raise ValueError("CSV content is empty")
    rows = _read_rows(csv_text)
    planned, summary = _plan_rows(rows, _load_existing_indexes())
    sample = [
        {
            "line_no": item["row"].get("line_no"),
            "name": item["row"].get("name"),
            "borough": item["row"].get("borough"),
            "org_type": item["row"].get("org_type"),
            "events_url": item["row"].get("events_url"),
            "existing_id": item.get("existing_id"),
            "match_kind": item.get("match_kind"),
            "reasons": item.get("reasons") or [],
        }
        for item in planned[:60]
    ]
    if not apply:
        return {"mode": "preview", "summary": summary, "apply": None, "sample": sample}
    return {"mode": "apply", "summary": summary, "apply": _apply_plan(planned, source=source), "sample": sample}
