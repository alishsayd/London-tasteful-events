"""Hardened CSV import for admin bulk ingestion."""

from __future__ import annotations

import csv
import io
import re
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import text

from app.db import BLOCKED_DOMAIN_SUFFIXES, BAD_NAME_PHRASES, ORG_TYPE_ALIASES, ORG_TYPES, get_db, update_org, upsert_org

GENERIC_NAME_SNIPPETS = (
    "what's on",
    "whats on",
    "book your tickets",
    "subscribe",
    "support",
    "overview",
    "events listings",
    "click here",
)


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


def _domain(value: Any) -> str:
    canonical = _canonical_url(value)
    if not canonical:
        return ""
    return (urlparse(canonical).netloc or "").lower().replace("www.", "")


def _blocked_domain(host: str) -> bool:
    clean_host = _clean(host).lower().replace("www.", "")
    if not clean_host:
        return False
    return any(clean_host == suffix or clean_host.endswith(f".{suffix}") for suffix in BLOCKED_DOMAIN_SUFFIXES)


def _normalize_org_type(value: Any) -> str | None:
    raw = _tokenize(value)
    if not raw:
        return None
    if raw in ORG_TYPE_ALIASES:
        return ORG_TYPE_ALIASES[raw]
    snake = raw.replace(" ", "_")
    return snake if snake in ORG_TYPES else None


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

    homepage = _canonical_url(_header_value(raw, ("homepage", "website", "url", "site", "official_url")))
    events_url = _canonical_url(_header_value(raw, ("events url", "events_url", "event_url", "whats_on_url", "whatson_url")))

    return {
        "line_no": line_no,
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
    for idx, raw in enumerate(reader, start=2):
        if not isinstance(raw, dict):
            continue
        row = _normalize_row(raw, idx)
        if row:
            rows.append(row)
    return rows


def _is_generic_name(name: str) -> bool:
    key = _tokenize(name)
    if not key:
        return True
    if len(key.split()) == 1 and len(key) < 4:
        return True
    if any(snippet in key for snippet in GENERIC_NAME_SNIPPETS):
        return True
    if any(phrase in key for phrase in BAD_NAME_PHRASES):
        return True
    return False


def _domain_name_key(row: dict[str, Any]) -> str:
    domain = _domain(row.get("homepage")) or _domain(row.get("events_url"))
    name_key = _tokenize(row.get("name"))
    if domain and name_key:
        return f"{domain}|{name_key}"
    return ""


def _load_existing_indexes() -> dict[str, dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    by_homepage: dict[str, dict[str, Any]] = {}
    by_events: dict[str, dict[str, Any]] = {}
    by_domain_name: dict[str, dict[str, Any]] = {}

    with get_db() as conn:
        rows = conn.execute(text("SELECT id, name, homepage, events_url, org_type, borough FROM orgs")).mappings().all()

    for row in rows:
        row_dict = dict(row)
        name_key = _tokenize(row_dict.get("name"))
        homepage = _canonical_url(row_dict.get("homepage"))
        events = _canonical_url(row_dict.get("events_url"))
        domain_key = _domain_name_key(row_dict)

        if name_key and name_key not in by_name:
            by_name[name_key] = row_dict
        if homepage and homepage not in by_homepage:
            by_homepage[homepage] = row_dict
        if events and events not in by_events:
            by_events[events] = row_dict
        if domain_key and domain_key not in by_domain_name:
            by_domain_name[domain_key] = row_dict

    return {
        "by_name": by_name,
        "by_homepage": by_homepage,
        "by_events": by_events,
        "by_domain_name": by_domain_name,
    }


def _match_existing(row: dict[str, Any], indexes: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    events = _canonical_url(row.get("events_url"))
    homepage = _canonical_url(row.get("homepage"))
    domain_key = _domain_name_key(row)
    name_key = _tokenize(row.get("name"))

    if events and events in indexes["by_events"]:
        return indexes["by_events"][events], "events_url"
    if homepage and homepage in indexes["by_homepage"]:
        return indexes["by_homepage"][homepage], "homepage"
    if domain_key and domain_key in indexes["by_domain_name"]:
        return indexes["by_domain_name"][domain_key], "domain_name"
    if name_key and name_key in indexes["by_name"]:
        return indexes["by_name"][name_key], "name"
    return None, None


def _hard_reject_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    name = _clean(row.get("name"))
    if not name:
        reasons.append("missing_name")

    homepage = _canonical_url(row.get("homepage"))
    events = _canonical_url(row.get("events_url"))
    if not homepage and not events:
        reasons.append("missing_urls")

    host = _domain(homepage) or _domain(events)
    if host and _blocked_domain(host):
        reasons.append("blocked_domain")

    if _is_generic_name(name):
        reasons.append("non_entity_name")

    return reasons


def _review_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not _clean(row.get("events_url")):
        reasons.append("missing_events_url")
    if not _clean(row.get("borough")):
        reasons.append("missing_borough")
    if not _clean(row.get("org_type")):
        reasons.append("missing_org_type")
    return reasons


def _ingest_key(row: dict[str, Any]) -> str:
    events = _canonical_url(row.get("events_url"))
    homepage = _canonical_url(row.get("homepage"))
    domain_name = _domain_name_key(row)
    name_key = _tokenize(row.get("name"))
    return events or homepage or domain_name or name_key


def _plan_rows(rows: list[dict[str, Any]], indexes: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    planned: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    skipped_rows = 0
    skipped_duplicates = 0
    existing_matches = 0
    safe_rows = 0
    review_rows = 0

    skip_reason_counts: dict[str, int] = {}
    review_reason_counts: dict[str, int] = {}

    for row in rows:
        reject_reasons = _hard_reject_reasons(row)
        if reject_reasons:
            skipped_rows += 1
            for reason in reject_reasons:
                skip_reason_counts[reason] = skip_reason_counts.get(reason, 0) + 1
            continue

        key = _ingest_key(row)
        if not key or key in seen_keys:
            skipped_rows += 1
            skipped_duplicates += 1
            skip_reason_counts["duplicate_in_csv"] = skip_reason_counts.get("duplicate_in_csv", 0) + 1
            continue
        seen_keys.add(key)

        existing, match_kind = _match_existing(row, indexes)
        if existing:
            existing_matches += 1

        reasons = _review_reasons(row)
        if reasons:
            review_rows += 1
            for reason in reasons:
                review_reason_counts[reason] = review_reason_counts.get(reason, 0) + 1
        else:
            safe_rows += 1

        planned.append(
            {
                "row": row,
                "existing_id": int(existing["id"]) if existing else None,
                "match_kind": match_kind,
                "reasons": reasons,
            }
        )

    summary = {
        "total_rows": len(rows),
        "accepted_rows": len(rows) - skipped_rows,
        "planned_rows": len(planned),
        "safe_rows": safe_rows,
        "review_rows": review_rows,
        "existing_db_matches": existing_matches,
        "skipped_rows": skipped_rows,
        "skipped_duplicates": skipped_duplicates,
        "skip_reason_counts": skip_reason_counts,
        "review_reason_counts": review_reason_counts,
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
                update_org(org_id, issue_state="none", review_needed_reason=None, active=True, crawl_paused=False)

        except Exception:
            error_count += 1

    return {
        "inserted_new": inserted_new,
        "merged_existing": merged_existing,
        "review_opened": review_opened,
        "error_count": error_count,
    }


def run_csv_import(*, csv_text: str, apply: bool = False, source: str = "csv_admin_import") -> dict[str, Any]:
    if not _clean(csv_text):
        raise ValueError("CSV content is empty")

    rows = _read_rows(csv_text)
    indexes = _load_existing_indexes()
    planned, summary = _plan_rows(rows, indexes)

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

    apply_result = _apply_plan(planned, source=source)
    return {"mode": "apply", "summary": summary, "apply": apply_result, "sample": sample}
