"""Guarded CSV importer for organisation records.

Usage:
  Dry run (default):
    python -m app.import_org_csv --csv london_event_entities_merged_deduped.csv

  Apply:
    python -m app.import_org_csv --csv london_event_entities_merged_deduped.csv --apply
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import text

from app.db import ORG_TYPE_VALUES, get_db, init_db, update_org, upsert_org

ORG_TYPE_SET = set(ORG_TYPE_VALUES)

ORG_TYPE_ALIASES = {
    "live music venue": "live_music_venue",
    "live_music_venue": "live_music_venue",
    "makerspace": "makers_space",
    "maker space": "makers_space",
    "makers space": "makers_space",
    "cultural centre": "cultural_centre",
    "cultural center": "cultural_centre",
    "learned society": "learned_society",
    "one off event": "festival",
    "one-off event": "festival",
    "one_off_event": "festival",
}

KNOWN_BOROUGHS = {
    "Hackney",
    "Tower Hamlets",
    "Southwark",
    "Lambeth",
    "Camden",
    "Islington",
    "Westminster",
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
}

BOROUGH_ALIASES = {
    "city of westminster": "Westminster",
    "westminster": "Westminster",
}

NAME_QUALIFIER_RE = re.compile(
    r"\s*\((?:events?|programme|program|public events?|community events?|talks?(?:\s*&\s*culture)?|"
    r"short courses|programming hub|organisation|venue org|nearby)\)\s*$",
    flags=re.IGNORECASE,
)
NAME_TRAILING_RE = re.compile(
    r"\s+(?:public\s+events?|community\s+events?|events?|programme|program)$",
    flags=re.IGNORECASE,
)

NAME_STOPWORDS = {
    "the",
    "london",
    "of",
    "and",
    "for",
    "in",
    "at",
    "uk",
    "co",
    "company",
    "ltd",
    "limited",
    "llp",
    "plc",
}


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_name(value: str | None) -> str:
    cleaned = _clean_text(value).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned.startswith("the "):
        cleaned = cleaned[4:]
    return cleaned


def _canonical_url(value: str | None) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    host = parsed.netloc.lower().replace("www.", "")
    path = (parsed.path or "/").rstrip("/") or "/"
    return f"{parsed.scheme}://{host}{path}"


def _url_domain(value: str | None) -> str:
    canonical = _canonical_url(value)
    if not canonical:
        return ""
    parsed = urlparse(canonical)
    return (parsed.netloc or "").lower().replace("www.", "")


def _is_root_url(value: str | None) -> bool:
    canonical = _canonical_url(value)
    if not canonical:
        return False
    parsed = urlparse(canonical)
    return (parsed.path or "/") in {"", "/"}


def _normalize_org_type(value: str | None) -> str:
    token = _normalize_name(value).replace(" ", "_")
    if token in ORG_TYPE_SET:
        return token
    alias = ORG_TYPE_ALIASES.get(_normalize_name(value))
    if alias:
        return alias
    return token


def _normalize_borough(value: str | None) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    lowered = raw.lower()
    if lowered in {"-", "multiple", "various", "all", "all boroughs"}:
        return ""
    if lowered in BOROUGH_ALIASES:
        return BOROUGH_ALIASES[lowered]

    for borough in KNOWN_BOROUGHS:
        if lowered == borough.lower():
            return borough
    return raw


def _name_tokens(name: str) -> set[str]:
    tokens = [part for part in name.split(" ") if part]
    return {token for token in tokens if token not in NAME_STOPWORDS}


def _names_likely_same(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    if len(left) >= 6 and left in right:
        return True
    if len(right) >= 6 and right in left:
        return True
    left_tokens = _name_tokens(left)
    right_tokens = _name_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    overlap = left_tokens & right_tokens
    if len(overlap) >= 2:
        score = len(overlap) / min(len(left_tokens), len(right_tokens))
        if score >= 0.75:
            return True
    return False


def _strong_name_match(left: str, right: str) -> bool:
    if not left or not right:
        return False
    if left == right:
        return True
    if len(left) >= 8 and left in right:
        return True
    if len(right) >= 8 and right in left:
        return True
    return False


def _normalize_import_name(value: str | None) -> tuple[str, bool]:
    name = _clean_text(value)
    changed = False

    next_name = NAME_QUALIFIER_RE.sub("", name).strip()
    if next_name != name:
        name = next_name
        changed = True

    next_name = NAME_TRAILING_RE.sub("", name).strip()
    if next_name != name:
        name = next_name
        changed = True

    name = re.sub(r"\s{2,}", " ", name).strip(" -")
    return name, changed


@dataclass
class CsvRow:
    row_num: int
    name: str
    events_url: str
    homepage: str
    borough: str
    org_type: str
    description: str
    original_org_type: str
    original_borough: str
    name_was_normalized: bool
    reasons: list[str] = field(default_factory=list)
    matched_existing_id: int | None = None
    matched_by: str | None = None

    @property
    def source_domain(self) -> str:
        return _url_domain(self.homepage) or _url_domain(self.events_url)

    @property
    def name_key(self) -> str:
        return _normalize_name(self.name)


def _load_existing() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, name, homepage, events_url, borough, org_type, source_domain, active
                FROM orgs
                ORDER BY id ASC
                """
            )
        ).mappings().all()
    return [dict(row) for row in rows]


def _build_existing_indexes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_id: dict[int, dict[str, Any]] = {}
    by_homepage: dict[str, list[int]] = defaultdict(list)
    by_events: dict[str, list[int]] = defaultdict(list)
    by_name: dict[str, list[int]] = defaultdict(list)
    by_domain: dict[str, list[int]] = defaultdict(list)

    for raw in rows:
        row = dict(raw)
        row_id = int(row["id"])
        row["name_key"] = _normalize_name(row.get("name"))
        row["homepage_key"] = _canonical_url(row.get("homepage"))
        row["events_key"] = _canonical_url(row.get("events_url"))
        row["domain"] = _clean_text(row.get("source_domain")) or _url_domain(row.get("homepage")) or _url_domain(row.get("events_url"))
        by_id[row_id] = row
        if row["homepage_key"]:
            by_homepage[row["homepage_key"]].append(row_id)
        if row["events_key"]:
            by_events[row["events_key"]].append(row_id)
        if row["name_key"]:
            by_name[row["name_key"]].append(row_id)
        if row["domain"]:
            by_domain[row["domain"]].append(row_id)

    return {
        "by_id": by_id,
        "by_homepage": by_homepage,
        "by_events": by_events,
        "by_name": by_name,
        "by_domain": by_domain,
    }


def _match_existing(row: CsvRow, indexes: dict[str, Any]) -> tuple[int | None, str | None]:
    homepage_key = _canonical_url(row.homepage)
    events_key = _canonical_url(row.events_url)
    name_key = row.name_key
    domain = row.source_domain

    if homepage_key and indexes["by_homepage"].get(homepage_key):
        return indexes["by_homepage"][homepage_key][0], "homepage"
    if events_key and indexes["by_events"].get(events_key):
        for row_id in indexes["by_events"][events_key]:
            existing_name = indexes["by_id"][row_id].get("name_key") or ""
            if _strong_name_match(existing_name, name_key) or _names_likely_same(existing_name, name_key):
                return row_id, "events_url+name"

    if name_key and indexes["by_name"].get(name_key):
        candidates = indexes["by_name"][name_key]
        if domain:
            for row_id in candidates:
                if indexes["by_id"][row_id].get("domain") == domain:
                    return row_id, "name+domain"
        return candidates[0], "name"

    if domain and indexes["by_domain"].get(domain):
        for row_id in indexes["by_domain"][domain]:
            existing_name = indexes["by_id"][row_id].get("name_key") or ""
            if _names_likely_same(existing_name, name_key):
                return row_id, "domain+name_similarity"

    return None, None


def _review_reasons(row: CsvRow) -> list[str]:
    reasons: list[str] = []
    if not row.borough:
        reasons.append("missing_or_ambiguous_borough")
    if row.org_type not in ORG_TYPE_SET:
        reasons.append("noncanonical_org_type")
    if not row.events_url:
        reasons.append("missing_events_url")
    elif _is_root_url(row.events_url):
        reasons.append("events_url_is_root")
    if row.name_was_normalized:
        reasons.append("name_normalized")
    if "/" in row.name or "+" in row.name:
        reasons.append("name_contains_symbol")
    if row.homepage and row.events_url:
        home_domain = _url_domain(row.homepage)
        events_domain = _url_domain(row.events_url)
        if home_domain and events_domain and home_domain != events_domain:
            reasons.append("cross_domain_homepage_events")
    return reasons


def _read_csv_rows(path: str) -> list[CsvRow]:
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return _read_csv_rows_from_reader(reader)


def _read_csv_rows_from_text(csv_text: str) -> list[CsvRow]:
    reader = csv.DictReader(io.StringIO(csv_text))
    return _read_csv_rows_from_reader(reader)


def _read_csv_rows_from_reader(reader: csv.DictReader) -> list[CsvRow]:
    out: list[CsvRow] = []
    required = {"name", "events_url", "homepage", "borough", "org_type", "description"}
    fieldnames = {str(item or "").strip() for item in (reader.fieldnames or [])}
    missing_cols = sorted(required - fieldnames)
    if missing_cols:
        raise ValueError(f"Missing required CSV columns: {', '.join(missing_cols)}")

    for row_num, raw in enumerate(reader, start=2):
        normalized_name, name_changed = _normalize_import_name(raw.get("name"))
        org_type_raw = _clean_text(raw.get("org_type"))
        borough_raw = _clean_text(raw.get("borough"))

        out.append(
            CsvRow(
                row_num=row_num,
                name=normalized_name,
                events_url=_canonical_url(raw.get("events_url")),
                homepage=_canonical_url(raw.get("homepage")),
                borough=_normalize_borough(raw.get("borough")),
                org_type=_normalize_org_type(raw.get("org_type")),
                description=_clean_text(raw.get("description")),
                original_org_type=org_type_raw,
                original_borough=borough_raw,
                name_was_normalized=name_changed,
            )
        )

    return out


def _index_new_row(indexes: dict[str, Any], row_id: int, row: CsvRow) -> None:
    row_payload = {
        "id": row_id,
        "name": row.name,
        "name_key": row.name_key,
        "homepage_key": _canonical_url(row.homepage),
        "events_key": _canonical_url(row.events_url),
        "domain": row.source_domain,
    }
    indexes["by_id"][row_id] = row_payload
    if row_payload["homepage_key"]:
        indexes["by_homepage"][row_payload["homepage_key"]].append(row_id)
    if row_payload["events_key"]:
        indexes["by_events"][row_payload["events_key"]].append(row_id)
    if row_payload["name_key"]:
        indexes["by_name"][row_payload["name_key"]].append(row_id)
    if row_payload["domain"]:
        indexes["by_domain"][row_payload["domain"]].append(row_id)


def build_import_plan(rows: list[CsvRow], existing_indexes: dict[str, Any]) -> dict[str, Any]:
    csv_seen: set[tuple[str, str]] = set()

    skipped_csv_duplicates: list[CsvRow] = []
    existing_matches: list[CsvRow] = []
    safe_rows: list[CsvRow] = []
    review_rows: list[CsvRow] = []

    for row in rows:
        dedupe_key = (row.name_key, row.source_domain)
        if dedupe_key in csv_seen:
            skipped_csv_duplicates.append(row)
            continue

        csv_seen.add(dedupe_key)

        match_id, match_by = _match_existing(row, existing_indexes)
        if match_id is not None:
            row.matched_existing_id = int(match_id)
            row.matched_by = str(match_by or "unknown")
            existing_matches.append(row)
            continue

        row.reasons = _review_reasons(row)
        if row.reasons:
            review_rows.append(row)
        else:
            safe_rows.append(row)

    reason_counts = Counter(reason for row in review_rows for reason in row.reasons)
    noncanonical_counts = Counter(row.original_org_type for row in rows if row.org_type not in ORG_TYPE_SET)
    planned_type_counts = Counter(row.org_type for row in safe_rows + review_rows)

    return {
        "safe_rows": safe_rows,
        "review_rows": review_rows,
        "existing_matches": existing_matches,
        "skipped_csv_duplicates": skipped_csv_duplicates,
        "summary": {
            "total_rows": len(rows),
            "planned_rows": len(safe_rows) + len(review_rows),
            "safe_rows": len(safe_rows),
            "review_rows": len(review_rows),
            "existing_db_matches": len(existing_matches),
            "csv_duplicates_skipped": len(skipped_csv_duplicates),
            "review_reason_counts": dict(sorted(reason_counts.items())),
            "planned_org_type_counts": dict(sorted(planned_type_counts.items())),
            "noncanonical_input_org_types": dict(sorted(noncanonical_counts.items())),
        },
        "samples": {
            "safe": [{"name": row.name, "org_type": row.org_type, "borough": row.borough} for row in safe_rows[:15]],
            "review": [
                {"name": row.name, "org_type": row.org_type, "borough": row.borough, "reasons": row.reasons}
                for row in review_rows[:20]
            ],
            "existing_matches": [
                {
                    "name": row.name,
                    "matched_existing_id": row.matched_existing_id,
                    "matched_by": row.matched_by,
                }
                for row in existing_matches[:20]
            ],
        },
    }


def apply_import_plan(
    plan: dict[str, Any],
    *,
    source: str,
    existing_indexes: dict[str, Any],
) -> dict[str, Any]:
    inserted_new = 0
    merged_existing = 0
    review_opened = 0
    errors: list[dict[str, Any]] = []

    existing_ids = set(existing_indexes["by_id"].keys())
    queue_rows = list(plan["review_rows"])
    safe_rows = list(plan["safe_rows"])

    ordered_rows = safe_rows + queue_rows
    for row in ordered_rows:
        target_org_type = row.org_type if row.org_type in ORG_TYPE_SET else "organisation"
        try:
            org_id = upsert_org(
                name=row.name,
                homepage=row.homepage or None,
                events_url=row.events_url or None,
                description=row.description or None,
                borough=row.borough or None,
                org_type=target_org_type,
                source=source,
            )
        except Exception as exc:
            errors.append({"row_num": row.row_num, "name": row.name, "error": str(exc)[:260]})
            continue

        org_id = int(org_id)
        if org_id in existing_ids:
            merged_existing += 1
        else:
            inserted_new += 1
            existing_ids.add(org_id)

        _index_new_row(existing_indexes, org_id, row)

        if row.reasons:
            update_org(
                org_id,
                issue_state="open",
                review_needed_reason=f"CSV import review: {', '.join(row.reasons)}",
                active=True,
                crawl_paused=False,
            )
            review_opened += 1

    return {
        "inserted_new": inserted_new,
        "merged_existing": merged_existing,
        "review_opened": review_opened,
        "errors": errors[:80],
        "error_count": len(errors),
    }


def run_csv_import(
    *,
    csv_path: str | None = None,
    csv_text: str | None = None,
    apply: bool = False,
    source: str = "csv_curated_import",
) -> dict[str, Any]:
    if not csv_path and csv_text is None:
        raise ValueError("csv_path or csv_text is required")
    if csv_path and csv_text is not None:
        raise ValueError("provide either csv_path or csv_text, not both")

    init_db()
    rows = _read_csv_rows(csv_path) if csv_path else _read_csv_rows_from_text(csv_text or "")
    existing = _load_existing()
    indexes = _build_existing_indexes(existing)
    plan = build_import_plan(rows, indexes)

    output: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "csv_path": csv_path,
        "existing_org_count": len(existing),
        "summary": plan["summary"],
        "samples": plan["samples"],
    }

    if apply:
        output["apply"] = apply_import_plan(plan, source=_clean_text(source) or "csv_curated_import", existing_indexes=indexes)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Guarded CSV importer for London event entities")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--apply", action="store_true", help="Write changes to DB (default is dry-run)")
    parser.add_argument("--source", default="csv_curated_import", help="Source label for inserted rows")
    args = parser.parse_args()

    output = run_csv_import(csv_path=args.csv, apply=args.apply, source=args.source)
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
