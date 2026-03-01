"""Database helpers for org curation and discovery runs."""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "orgs.db"


# -----------------------------
# DB setup
# -----------------------------

def _normalize_database_url(raw_url: str | None) -> str:
    if not raw_url:
        return f"sqlite:///{DEFAULT_SQLITE_PATH}"
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgresql://") and "+psycopg" not in raw_url:
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_url


DATABASE_URL = _normalize_database_url(os.getenv("DATABASE_URL"))
ENGINE = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
IS_POSTGRES = ENGINE.dialect.name.startswith("postgres")


@contextmanager
def get_db():
    with ENGINE.begin() as conn:
        yield conn


# -----------------------------
# Taxonomy
# -----------------------------

PRIMARY_TYPE_VALUES = ("venue", "institution", "organisation")

ORG_TYPE_VALUES = (
    "bookshop",
    "cinema",
    "gallery",
    "live_music_venue",
    "theatre",
    "museum",
    "makers_space",
    "park",
    "garden",
    "cultural_centre",
    "university",
    "learned_society",
    "promoter",
    "festival",
    "organisation",
)

VENUE_ORG_TYPES = {
    "bookshop",
    "cinema",
    "gallery",
    "live_music_venue",
    "theatre",
    "museum",
    "makers_space",
    "park",
    "garden",
}

INSTITUTION_ORG_TYPES = {"cultural_centre", "university", "learned_society"}

ORG_TYPE_LABELS = {
    "bookshop": "Bookshop",
    "cinema": "Cinema",
    "gallery": "Gallery",
    "live_music_venue": "Live music venue",
    "theatre": "Theatre",
    "museum": "Museum",
    "makers_space": "Makers space",
    "park": "Park",
    "garden": "Garden",
    "cultural_centre": "Cultural centre",
    "university": "University",
    "learned_society": "Learned society",
    "promoter": "Promoter",
    "festival": "Festival",
    "organisation": "Organisation",
}

ORG_TYPE_ALIASES = {
    "live music venue": "live_music_venue",
    "makers space": "makers_space",
    "makerspace": "makers_space",
    "maker space": "makers_space",
    "cultural center": "cultural_centre",
    "cultural centre": "cultural_centre",
    "learned society": "learned_society",
    "one off event": "festival",
    "one-off event": "festival",
    "one_off_event": "festival",
}

NAME_STOPWORDS = {
    "the",
    "london",
    "uk",
    "of",
    "and",
    "for",
    "in",
    "at",
    "co",
    "company",
    "ltd",
    "limited",
    "llp",
    "plc",
}

UNIVERSITY_HINTS = ("university", "college", "soas", "lse", "imperial")
LEARNED_SOCIETY_HINTS = ("society", "institute", "royal", "gresham", "rsa")

CULTURAL_CENTRE_HINTS = (
    "arts centre",
    "arts center",
    "cultural centre",
    "cultural center",
    "cultural institute",
    "japan house",
    "goethe",
    "cervantes",
    "istituto",
    "institut",
    "alliance",
)

BLOCKED_DOMAIN_SUFFIXES = (
    "github.com",
    "bsky.app",
    "bsky.social",
    "blueskyweb.xyz",
    "x.com",
    "twitter.com",
    "instagram.com",
    "facebook.com",
    "youtube.com",
    "linkedin.com",
    "uk.linkedin.com",
    "eventbrite.com",
    "ticketmaster.com",
    "ticketmaster.co.uk",
    "feverup.com",
    "designmynight.com",
    "secretldn.com",
    "timeout.com",
    "culturecalling.com",
    "eventindustrynews.com",
    "ianvisits.co.uk",
    "lectures.london",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "wikipedia.org",
    "gov.uk",
)

BAD_NAME_PHRASES = (
    "museums and collections",
    "programmes and exhibitions",
    "book your tickets",
    "courses and meetings",
    "support ianvisits",
    "subscribe to read",
    "events listings",
    "overview",
)


# -----------------------------
# Shared helpers
# -----------------------------

def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_name(value: str | None) -> str:
    lowered = _clean_text(value).lower()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    if lowered.startswith("the "):
        lowered = lowered[4:]
    return lowered


def _name_tokens(value: str | None) -> set[str]:
    return {token for token in _normalize_name(value).split(" ") if token and token not in NAME_STOPWORDS}


def _contains_phrase(value: str | None, hint: str) -> bool:
    left = _normalize_name(value)
    right = _normalize_name(hint)
    if not left or not right:
        return False
    return bool(re.search(rf"(?:^|\s){re.escape(right)}(?:\s|$)", left))


def _contains_any(value: str | None, hints: tuple[str, ...]) -> bool:
    return any(_contains_phrase(value, hint) for hint in hints)


def _normalize_org_type(value: str | None) -> str:
    raw = _normalize_name(value).replace(" ", "_")
    if raw in ORG_TYPE_VALUES:
        return raw
    alias = ORG_TYPE_ALIASES.get(_normalize_name(value))
    return alias or raw


def _normalize_primary_type(value: str | None) -> str:
    return _normalize_name(value).replace(" ", "_")


def _display_org_type(value: str | None) -> str:
    normalized = _normalize_org_type(value)
    return ORG_TYPE_LABELS.get(normalized, "Organisation")


def _domain_from_url(value: str | None) -> str | None:
    raw = _clean_text(value)
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
        host = parsed.netloc or parsed.path
        host = host.lower().replace("www.", "")
        return host or None
    except Exception:
        return None


def _canonical_url(value: str | None) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""
    try:
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"}:
            return ""
        host = (parsed.netloc or "").lower().replace("www.", "")
        if not host:
            return ""
        path = (parsed.path or "/").rstrip("/") or "/"
        return f"{parsed.scheme}://{host}{path}"
    except Exception:
        return ""


def _domain_matches_suffix(domain: str | None, suffixes: tuple[str, ...]) -> bool:
    host = str(domain or "").strip().lower().replace("www.", "")
    if not host:
        return False
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in suffixes)


def _names_likely_same_entity(left: str | None, right: str | None) -> bool:
    left_norm = _normalize_name(left)
    right_norm = _normalize_name(right)
    if not left_norm or not right_norm:
        return False
    if left_norm == right_norm:
        return True
    if len(left_norm) >= 6 and left_norm in right_norm:
        return True
    if len(right_norm) >= 6 and right_norm in left_norm:
        return True

    left_tokens = _name_tokens(left_norm)
    right_tokens = _name_tokens(right_norm)
    if not left_tokens or not right_tokens:
        return False

    overlap = left_tokens & right_tokens
    if len(overlap) >= 2:
        ratio = len(overlap) / min(len(left_tokens), len(right_tokens))
        if ratio >= 0.75:
            return True
    return False


def _best_description(existing: str | None, candidate: str | None, row: dict[str, Any]) -> str:
    current = _clean_text(existing)
    next_value = _clean_text(candidate)
    if next_value and len(next_value) >= len(current):
        return next_value
    if current:
        return current

    name = _clean_text(row.get("name")) or "This organisation"
    borough = _clean_text(row.get("borough"))
    kind = _display_org_type(row.get("org_type")).lower()
    if borough and kind:
        return f"{name} is a London {kind} in {borough}."
    if kind:
        return f"{name} is a London {kind}."
    return f"{name} is a London organisation."


def _primary_type_for_org_type(org_type: str | None) -> str:
    normalized = _normalize_org_type(org_type)
    if normalized in VENUE_ORG_TYPES:
        return "venue"
    if normalized in INSTITUTION_ORG_TYPES:
        return "institution"
    return "organisation"


def _infer_org_type_from_name(name: str | None) -> str:
    name_norm = _normalize_name(name)
    if not name_norm:
        return "organisation"

    if _contains_any(name_norm, UNIVERSITY_HINTS):
        return "university"
    if _contains_any(name_norm, LEARNED_SOCIETY_HINTS):
        return "learned_society"
    if _contains_any(name_norm, ("bookshop", "bookstore", "books")):
        return "bookshop"
    if _contains_any(name_norm, ("cinema", "film")):
        return "cinema"
    if _contains_any(name_norm, ("gallery",)):
        return "gallery"
    if _contains_any(name_norm, ("museum", "archive")):
        return "museum"
    if _contains_any(name_norm, ("theatre", "theater")):
        return "theatre"
    if _contains_any(name_norm, ("makerspace", "maker space", "workshop")):
        return "makers_space"
    if _contains_any(name_norm, ("park",)):
        return "park"
    if _contains_any(name_norm, ("garden", "conservatory")):
        return "garden"
    if _contains_any(name_norm, ("promoter", "presents", "productions")):
        return "promoter"
    if _contains_any(name_norm, ("festival", "biennale", "triennale", "carnival")):
        return "festival"
    if _contains_any(name_norm, ("music", "jazz", "orchestra", "club")):
        return "live_music_venue"
    if _contains_any(name_norm, CULTURAL_CENTRE_HINTS):
        return "cultural_centre"
    return "organisation"


def _resolve_org_type(*, name: str | None, org_type: str | None = None, category: str | None = None) -> str:
    explicit = _normalize_org_type(org_type)
    if explicit in ORG_TYPE_VALUES:
        return explicit

    # category is legacy and intentionally ignored for taxonomy decisions.
    _ = category

    inferred = _infer_org_type_from_name(name)
    if inferred in ORG_TYPE_VALUES:
        return inferred
    return "organisation"


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def _active_filter_sql() -> str:
    if IS_POSTGRES:
        return "active IS TRUE AND (crawl_paused IS FALSE OR crawl_paused IS NULL)"
    return "coalesce(active, 1) = 1 AND coalesce(crawl_paused, 0) = 0"


def _queue_condition_sql() -> str:
    return """
    (
        coalesce(issue_state, 'none') <> 'resolved'
        AND (
            issue_state = 'open'
            OR events_url IS NULL
            OR trim(events_url) = ''
            OR borough IS NULL
            OR trim(borough) = ''
            OR org_type IS NULL
            OR trim(org_type) = ''
            OR coalesce(consecutive_failures, 0) >= 3
            OR coalesce(consecutive_empty_extracts, 0) >= 3
        )
    )
    """


def _decode_discovery_row(row: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(row)
    raw_details = decoded.get("details_json")
    parsed = None
    if isinstance(raw_details, str) and raw_details.strip():
        try:
            parsed = json.loads(raw_details)
        except Exception:
            parsed = None
    decoded["details"] = parsed
    decoded.pop("details_json", None)
    return decoded


# -----------------------------
# Schema and bootstrap
# -----------------------------

def _ensure_org_type_constraint_postgres(conn) -> None:
    allowed = ", ".join(f"'{value}'" for value in ORG_TYPE_VALUES)
    conn.execute(
        text(
            f"""
            UPDATE orgs
            SET org_type = 'organisation'
            WHERE org_type IS NULL
               OR trim(org_type) = ''
               OR lower(trim(org_type)) NOT IN ({allowed})
            """
        )
    )

    checks = conn.execute(
        text(
            """
            SELECT c.conname, pg_get_constraintdef(c.oid) AS constraint_def
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE c.contype = 'c'
              AND t.relname = 'orgs'
              AND n.nspname = current_schema()
            """
        )
    ).mappings().all()

    org_checks = [row for row in checks if "org_type" in str(row.get("constraint_def") or "").lower()]
    if len(org_checks) == 1:
        definition = str(org_checks[0].get("constraint_def") or "").lower()
        if all(f"'{value}'" in definition for value in ORG_TYPE_VALUES):
            return

    for row in org_checks:
        name = str(row.get("conname") or "").strip()
        if name:
            conn.execute(text(f'ALTER TABLE orgs DROP CONSTRAINT IF EXISTS "{name}"'))

    conn.execute(
        text(
            f"""
            ALTER TABLE orgs
            ADD CONSTRAINT orgs_org_type_check
            CHECK (org_type IN ({allowed}))
            """
        )
    )


def init_db() -> None:
    org_id_def = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    run_id_def = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"

    with get_db() as conn:
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS orgs (
                    id {org_id_def},
                    name TEXT NOT NULL,
                    homepage TEXT,
                    events_url TEXT,
                    description TEXT,
                    borough TEXT,
                    primary_type TEXT NOT NULL DEFAULT 'organisation'
                        CHECK(primary_type IN ('venue', 'institution', 'organisation')),
                    org_type TEXT NOT NULL DEFAULT 'organisation'
                        CHECK(org_type IN ('bookshop', 'cinema', 'gallery', 'live_music_venue', 'theatre', 'museum', 'makers_space', 'park', 'garden', 'cultural_centre', 'university', 'learned_society', 'promoter', 'festival', 'organisation')),
                    parent_org_id BIGINT,
                    source TEXT,
                    source_domain TEXT,
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending', 'approved', 'rejected', 'maybe')),
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    reviewed_at TIMESTAMP,
                    active BOOLEAN NOT NULL DEFAULT TRUE,
                    crawl_paused BOOLEAN NOT NULL DEFAULT FALSE,
                    last_crawled_at TIMESTAMP,
                    last_successful_event_extract_at TIMESTAMP,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0,
                    issue_state TEXT NOT NULL DEFAULT 'none',
                    review_needed_reason TEXT,
                    FOREIGN KEY (parent_org_id) REFERENCES orgs(id)
                )
                """
            )
        )

        if IS_POSTGRES:
            alter_statements = [
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS primary_type TEXT NOT NULL DEFAULT 'organisation'",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS org_type TEXT NOT NULL DEFAULT 'organisation'",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS parent_org_id BIGINT",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS source_domain TEXT",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS crawl_paused BOOLEAN NOT NULL DEFAULT FALSE",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS issue_state TEXT NOT NULL DEFAULT 'none'",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS review_needed_reason TEXT",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMP",
                "ALTER TABLE orgs ADD COLUMN IF NOT EXISTS last_successful_event_extract_at TIMESTAMP",
            ]
        else:
            alter_statements = [
                "ALTER TABLE orgs ADD COLUMN primary_type TEXT NOT NULL DEFAULT 'organisation'",
                "ALTER TABLE orgs ADD COLUMN org_type TEXT NOT NULL DEFAULT 'organisation'",
                "ALTER TABLE orgs ADD COLUMN parent_org_id INTEGER",
                "ALTER TABLE orgs ADD COLUMN source_domain TEXT",
                "ALTER TABLE orgs ADD COLUMN active BOOLEAN NOT NULL DEFAULT 1",
                "ALTER TABLE orgs ADD COLUMN crawl_paused BOOLEAN NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN issue_state TEXT NOT NULL DEFAULT 'none'",
                "ALTER TABLE orgs ADD COLUMN review_needed_reason TEXT",
                "ALTER TABLE orgs ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN last_crawled_at TIMESTAMP",
                "ALTER TABLE orgs ADD COLUMN last_successful_event_extract_at TIMESTAMP",
            ]

        for statement in alter_statements:
            try:
                conn.execute(text(statement))
            except Exception:
                pass

        conn.execute(text("UPDATE orgs SET active = TRUE WHERE active IS NULL" if IS_POSTGRES else "UPDATE orgs SET active = 1 WHERE active IS NULL"))
        conn.execute(text("UPDATE orgs SET crawl_paused = FALSE WHERE crawl_paused IS NULL" if IS_POSTGRES else "UPDATE orgs SET crawl_paused = 0 WHERE crawl_paused IS NULL"))
        conn.execute(text("UPDATE orgs SET issue_state = 'none' WHERE issue_state IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_failures = 0 WHERE consecutive_failures IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_empty_extracts = 0 WHERE consecutive_empty_extracts IS NULL"))
        conn.execute(text("UPDATE orgs SET org_type = 'organisation' WHERE org_type IS NULL OR trim(org_type) = ''"))
        conn.execute(text("UPDATE orgs SET primary_type = 'organisation' WHERE primary_type IS NULL OR trim(primary_type) = ''"))

        if IS_POSTGRES:
            _ensure_org_type_constraint_postgres(conn)

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_active ON orgs(active, crawl_paused)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_issue_state ON orgs(issue_state)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_org_type ON orgs(org_type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_source_domain ON orgs(source_domain)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_created_at ON orgs(created_at DESC)"))

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS discovery_runs (
                    id {run_id_def},
                    trigger TEXT NOT NULL DEFAULT 'scheduled',
                    status TEXT NOT NULL DEFAULT 'running'
                        CHECK(status IN ('running', 'success', 'failed')),
                    query_count INTEGER NOT NULL DEFAULT 0,
                    result_count INTEGER NOT NULL DEFAULT 0,
                    upserted_count INTEGER NOT NULL DEFAULT 0,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    finished_at TIMESTAMP,
                    error TEXT,
                    details_json TEXT
                )
                """
            )
        )

        if IS_POSTGRES:
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS trigger TEXT NOT NULL DEFAULT 'scheduled'"))
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS details_json TEXT"))
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS query_count INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS result_count INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS upserted_count INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS error TEXT"))
            conn.execute(text("ALTER TABLE discovery_runs ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP"))
        else:
            for statement in [
                "ALTER TABLE discovery_runs ADD COLUMN trigger TEXT NOT NULL DEFAULT 'scheduled'",
                "ALTER TABLE discovery_runs ADD COLUMN details_json TEXT",
                "ALTER TABLE discovery_runs ADD COLUMN query_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE discovery_runs ADD COLUMN result_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE discovery_runs ADD COLUMN upserted_count INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE discovery_runs ADD COLUMN error TEXT",
                "ALTER TABLE discovery_runs ADD COLUMN finished_at TIMESTAMP",
            ]:
                try:
                    conn.execute(text(statement))
                except Exception:
                    pass

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_discovery_runs_started_at ON discovery_runs(started_at DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_discovery_runs_status ON discovery_runs(status)"))


# -----------------------------
# Orgs: CRUD and listing
# -----------------------------

def upsert_org(
    name: str,
    homepage: str | None = None,
    events_url: str | None = None,
    description: str | None = None,
    borough: str | None = None,
    category: str | None = None,
    org_type: str | None = None,
    primary_type: str | None = None,
    parent_org_id: int | None = None,
    source: str | None = None,
) -> int:
    if not _clean_text(name):
        raise ValueError("name is required")

    # category is intentionally ignored in the refactored taxonomy model.
    _ = category

    clean_name = _clean_text(name)
    clean_homepage = _canonical_url(homepage) or None
    clean_events_url = _canonical_url(events_url) or None
    clean_borough = _clean_text(borough) or None
    source_domain = _domain_from_url(clean_homepage) or _domain_from_url(clean_events_url)

    resolved_org_type = _resolve_org_type(name=clean_name, org_type=org_type)
    resolved_primary = _normalize_primary_type(primary_type)
    if resolved_primary not in PRIMARY_TYPE_VALUES:
        resolved_primary = _primary_type_for_org_type(resolved_org_type)

    with get_db() as conn:
        where_clauses = [
            "lower(name) = lower(:name)",
            "coalesce(homepage, '') = coalesce(:homepage, '')",
            "coalesce(events_url, '') = coalesce(:events_url, '')",
        ]
        params = {
            "name": clean_name,
            "homepage": clean_homepage,
            "events_url": clean_events_url,
        }
        if source_domain is not None:
            where_clauses.append("source_domain = :source_domain")
            params["source_domain"] = source_domain

        rows = conn.execute(
            text(
                f"""
                SELECT * FROM orgs
                WHERE {' OR '.join(where_clauses)}
                ORDER BY id ASC
                """
            ),
            params,
        ).mappings().all()

        existing: dict[str, Any] | None = None
        norm_name = _normalize_name(clean_name)
        for row in rows:
            row_dict = dict(row)
            if _canonical_url(row_dict.get("homepage")) and _canonical_url(row_dict.get("homepage")) == (clean_homepage or ""):
                existing = row_dict
                break
            if _canonical_url(row_dict.get("events_url")) and _canonical_url(row_dict.get("events_url")) == (clean_events_url or ""):
                existing = row_dict
                break

        if existing is None:
            for row in rows:
                row_dict = dict(row)
                row_domain = row_dict.get("source_domain") or _domain_from_url(row_dict.get("homepage")) or _domain_from_url(row_dict.get("events_url"))
                if row_domain and source_domain and row_domain == source_domain and _names_likely_same_entity(row_dict.get("name"), clean_name):
                    existing = row_dict
                    break

        if existing is None:
            for row in rows:
                row_dict = dict(row)
                if _normalize_name(row_dict.get("name")) == norm_name:
                    existing = row_dict
                    break

        if existing:
            updates: dict[str, Any] = {}

            if not _clean_text(existing.get("homepage")) and clean_homepage:
                updates["homepage"] = clean_homepage
            if not _clean_text(existing.get("events_url")) and clean_events_url:
                updates["events_url"] = clean_events_url
            if not _clean_text(existing.get("borough")) and clean_borough:
                updates["borough"] = clean_borough
            if not _clean_text(existing.get("source")) and _clean_text(source):
                updates["source"] = _clean_text(source)
            if source_domain and source_domain != _clean_text(existing.get("source_domain")):
                updates["source_domain"] = source_domain

            current_org_type = _normalize_org_type(existing.get("org_type"))
            if current_org_type not in ORG_TYPE_VALUES:
                updates["org_type"] = resolved_org_type
            elif current_org_type == "organisation" and resolved_org_type != "organisation":
                updates["org_type"] = resolved_org_type

            target_org_type = updates.get("org_type") or current_org_type
            target_primary = _primary_type_for_org_type(target_org_type)
            current_primary = _normalize_primary_type(existing.get("primary_type"))
            if current_primary not in PRIMARY_TYPE_VALUES or current_primary != target_primary:
                updates["primary_type"] = target_primary

            if parent_org_id is not None:
                updates["parent_org_id"] = int(parent_org_id)

            next_description = _best_description(existing.get("description"), description, {
                **existing,
                "name": clean_name,
                "borough": clean_borough,
                "org_type": target_org_type,
            })
            if next_description != _clean_text(existing.get("description")):
                updates["description"] = next_description

            if updates:
                _update_org_row(conn, int(existing["id"]), updates)
            return int(existing["id"])

        params = {
            "name": clean_name,
            "homepage": clean_homepage,
            "events_url": clean_events_url,
            "description": _best_description(None, description, {"name": clean_name, "borough": clean_borough, "org_type": resolved_org_type}),
            "borough": clean_borough,
            "org_type": resolved_org_type,
            "primary_type": resolved_primary,
            "parent_org_id": int(parent_org_id) if parent_org_id is not None else None,
            "source": _clean_text(source) or None,
            "source_domain": source_domain,
        }

        if IS_POSTGRES:
            inserted = conn.execute(
                text(
                    """
                    INSERT INTO orgs
                    (name, homepage, events_url, description, borough, org_type, primary_type, parent_org_id, source, source_domain)
                    VALUES
                    (:name, :homepage, :events_url, :description, :borough, :org_type, :primary_type, :parent_org_id, :source, :source_domain)
                    RETURNING id
                    """
                ),
                params,
            ).mappings().first()
            if inserted:
                return int(inserted["id"])
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO orgs
                    (name, homepage, events_url, description, borough, org_type, primary_type, parent_org_id, source, source_domain)
                    VALUES
                    (:name, :homepage, :events_url, :description, :borough, :org_type, :primary_type, :parent_org_id, :source, :source_domain)
                    """
                ),
                params,
            )
            fallback = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if fallback:
                return int(fallback["id"])

    raise RuntimeError("Failed to upsert org")


def _update_org_row(conn, org_id: int, updates: dict[str, Any]) -> None:
    set_clause = ", ".join(f"{key} = :{key}" for key in updates)
    conn.execute(text(f"UPDATE orgs SET {set_clause} WHERE id = :org_id"), {**updates, "org_id": int(org_id)})


def get_org(org_id: int) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(text("SELECT * FROM orgs WHERE id = :id"), {"id": int(org_id)}).mappings().first()
    return dict(row) if row else None


def update_org(org_id: int, **fields: Any) -> None:
    allowed = {
        "name",
        "homepage",
        "events_url",
        "description",
        "borough",
        "org_type",
        "primary_type",
        "parent_org_id",
        "notes",
        "status",
        "source",
        "active",
        "crawl_paused",
        "last_crawled_at",
        "last_successful_event_extract_at",
        "consecutive_failures",
        "consecutive_empty_extracts",
        "issue_state",
        "review_needed_reason",
        "source_domain",
    }
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return

    current = get_org(org_id) or {}

    if "name" in updates:
        updates["name"] = _clean_text(updates.get("name")) or current.get("name")
    if "homepage" in updates:
        updates["homepage"] = _canonical_url(updates.get("homepage")) or None
    if "events_url" in updates:
        updates["events_url"] = _canonical_url(updates.get("events_url")) or None
    if "borough" in updates:
        updates["borough"] = _clean_text(updates.get("borough")) or None

    if "description" in updates:
        updates["description"] = _best_description(current.get("description"), updates.get("description"), {**current, **updates})

    if "org_type" in updates:
        updates["org_type"] = _resolve_org_type(name=updates.get("name") or current.get("name"), org_type=updates.get("org_type"))
        if "primary_type" not in updates:
            updates["primary_type"] = _primary_type_for_org_type(updates.get("org_type"))

    if "primary_type" in updates:
        primary = _normalize_primary_type(updates.get("primary_type"))
        if primary not in PRIMARY_TYPE_VALUES:
            primary = _primary_type_for_org_type(updates.get("org_type") or current.get("org_type"))
        updates["primary_type"] = primary

    if "parent_org_id" in updates and updates.get("parent_org_id") is not None:
        try:
            updates["parent_org_id"] = int(updates["parent_org_id"])
        except Exception:
            updates["parent_org_id"] = None

    if "homepage" in updates or "events_url" in updates or "source_domain" not in updates:
        homepage = updates.get("homepage") if "homepage" in updates else current.get("homepage")
        events_url = updates.get("events_url") if "events_url" in updates else current.get("events_url")
        updates["source_domain"] = _domain_from_url(homepage) or _domain_from_url(events_url)

    set_clause = ", ".join(f"{key} = :{key}" for key in updates)
    params = {**updates, "org_id": int(org_id)}
    if "status" in updates:
        set_clause += ", reviewed_at = CASE WHEN :status = 'pending' THEN NULL ELSE CURRENT_TIMESTAMP END"

    with get_db() as conn:
        conn.execute(text(f"UPDATE orgs SET {set_clause} WHERE id = :org_id"), params)


def get_active_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    sql = f"SELECT * FROM orgs WHERE {_active_filter_sql()} ORDER BY org_type ASC, borough ASC, name ASC, created_at DESC, id DESC"
    params: dict[str, Any] = {}
    if limit and int(limit) > 0:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)

    with get_db() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(row) for row in rows]


def get_public_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    return get_active_orgs(limit=limit)


def get_review_queue_orgs(limit: int = 250) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT *
                FROM orgs
                WHERE {_active_filter_sql()}
                  AND coalesce(issue_state, 'none') <> 'snoozed'
                  AND {_queue_condition_sql()}
                ORDER BY
                    CASE
                        WHEN issue_state = 'open' THEN 0
                        WHEN events_url IS NULL OR trim(events_url) = '' THEN 1
                        WHEN org_type IS NULL OR trim(org_type) = '' THEN 2
                        WHEN borough IS NULL OR trim(borough) = '' THEN 3
                        WHEN coalesce(consecutive_failures, 0) >= 3 THEN 4
                        WHEN coalesce(consecutive_empty_extracts, 0) >= 3 THEN 5
                        ELSE 6
                    END,
                    created_at DESC,
                    id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()
    return [dict(row) for row in rows]


def get_stats() -> dict[str, int]:
    with get_db() as conn:
        by_status = conn.execute(text("SELECT status, COUNT(*) AS count FROM orgs GROUP BY status")).mappings().all()
        total = int(conn.execute(text("SELECT COUNT(*) FROM orgs")).scalar_one())
        active_total = int(conn.execute(text(f"SELECT COUNT(*) FROM orgs WHERE {_active_filter_sql()}")) .scalar_one())
        queue_total = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM orgs
                    WHERE {_active_filter_sql()}
                      AND coalesce(issue_state, 'none') <> 'snoozed'
                      AND {_queue_condition_sql()}
                    """
                )
            ).scalar_one()
        )
        open_issues = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM orgs
                    WHERE {_active_filter_sql()}
                      AND issue_state = 'open'
                    """
                )
            ).scalar_one()
        )

    stats = {"pending": 0, "approved": 0, "maybe": 0, "rejected": 0}
    for row in by_status:
        status = str(row.get("status") or "").strip().lower()
        if status in stats:
            stats[status] = int(row.get("count") or 0)

    return {
        **stats,
        "total": total,
        "active_total": active_total,
        "queue_total": queue_total,
        "open_issues": open_issues,
    }


# -----------------------------
# Discovery runs
# -----------------------------

def has_recent_running_discovery(max_age_minutes: int = 90) -> bool:
    with get_db() as conn:
        row = conn.execute(
            text(
                """
                SELECT started_at
                FROM discovery_runs
                WHERE status = 'running' AND finished_at IS NULL
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()

    if not row:
        return False

    started = _as_datetime(row.get("started_at"))
    if not started:
        return True
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - started) < timedelta(minutes=max_age_minutes)


def start_discovery_run(query_count: int, trigger: str = "scheduled", details: dict[str, Any] | None = None) -> int:
    details_json = json.dumps(details or {}, ensure_ascii=False)

    with get_db() as conn:
        if IS_POSTGRES:
            inserted = conn.execute(
                text(
                    """
                    INSERT INTO discovery_runs (trigger, status, query_count, details_json)
                    VALUES (:trigger, 'running', :query_count, :details_json)
                    RETURNING id
                    """
                ),
                {"trigger": trigger, "query_count": int(query_count), "details_json": details_json},
            ).mappings().first()
            if inserted:
                return int(inserted["id"])
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO discovery_runs (trigger, status, query_count, details_json)
                    VALUES (:trigger, 'running', :query_count, :details_json)
                    """
                ),
                {"trigger": trigger, "query_count": int(query_count), "details_json": details_json},
            )
            row = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if row:
                return int(row["id"])

    raise RuntimeError("Failed to create discovery run")


def finish_discovery_run(
    run_id: int,
    status: str,
    result_count: int,
    upserted_count: int,
    error: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    status_value = status if status in {"running", "success", "failed"} else "failed"
    details_json = json.dumps(details, ensure_ascii=False) if details is not None else None

    clauses = [
        "status = :status",
        "result_count = :result_count",
        "upserted_count = :upserted_count",
        "error = :error",
        "finished_at = CURRENT_TIMESTAMP",
    ]
    params: dict[str, Any] = {
        "run_id": int(run_id),
        "status": status_value,
        "result_count": int(result_count),
        "upserted_count": int(upserted_count),
        "error": _clean_text(error)[:4000] or None,
    }
    if details_json is not None:
        clauses.insert(4, "details_json = :details_json")
        params["details_json"] = details_json

    with get_db() as conn:
        conn.execute(
            text(
                f"""
                UPDATE discovery_runs
                SET {', '.join(clauses)}
                WHERE id = :run_id
                """
            ),
            params,
        )


def get_latest_discovery_run() -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM discovery_runs
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()
    if not row:
        return None
    return _decode_discovery_row(dict(row))


def get_latest_running_discovery_run() -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM discovery_runs
                WHERE status = 'running' AND finished_at IS NULL
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """
            )
        ).mappings().first()
    if not row:
        return None
    return _decode_discovery_row(dict(row))


def get_discovery_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM discovery_runs
                ORDER BY started_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()
    return [_decode_discovery_row(dict(row)) for row in rows]


# -----------------------------
# Maintenance
# -----------------------------

def cleanup_recent_discovery_garbage(days: int = 7, dry_run: bool = False, limit: int = 1000) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))

    with get_db() as conn:
        rows = conn.execute(
            text(
                """
                SELECT *
                FROM orgs
                WHERE created_at >= :since
                  AND lower(coalesce(source, '')) LIKE 'auto_discovery%'
                ORDER BY created_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {"since": since, "limit": int(limit)},
        ).mappings().all()

    flagged: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        name_key = _normalize_name(row_dict.get("name"))
        domain = _clean_text(row_dict.get("source_domain")) or _domain_from_url(row_dict.get("homepage")) or _domain_from_url(row_dict.get("events_url"))

        reasons: list[str] = []
        if _domain_matches_suffix(domain, BLOCKED_DOMAIN_SUFFIXES):
            reasons.append("blocked_domain")
        if any(phrase in name_key for phrase in BAD_NAME_PHRASES):
            reasons.append("bad_name")

        if reasons:
            flagged.append({"row": row_dict, "reasons": reasons})

    updated = 0
    newly_inactivated = 0
    if not dry_run:
        with get_db() as conn:
            for item in flagged:
                row = item["row"]
                reasons = item["reasons"]
                updates = {
                    "issue_state": "open",
                    "review_needed_reason": f"Discovery cleanup: {', '.join(reasons)}",
                }
                if "blocked_domain" in reasons:
                    updates["active"] = False
                    updates["crawl_paused"] = True
                    if bool(row.get("active", True)):
                        newly_inactivated += 1

                _update_org_row(conn, int(row["id"]), updates)
                updated += 1

    sample = [
        {
            "id": int(item["row"]["id"]),
            "name": _clean_text(item["row"].get("name")),
            "reasons": item["reasons"],
        }
        for item in flagged[:20]
    ]

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "days": int(days),
        "scanned": len(rows),
        "flagged": len(flagged),
        "updated": updated if not dry_run else len(flagged),
        "newly_inactivated": newly_inactivated if not dry_run else 0,
        "sample": sample,
    }


def normalize_org_taxonomy(dry_run: bool = False) -> dict[str, Any]:
    with get_db() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, name, org_type, primary_type, parent_org_id
                FROM orgs
                ORDER BY id ASC
                """
            )
        ).mappings().all()

    updates: list[dict[str, Any]] = []
    transitions: dict[str, int] = {}
    for row in rows:
        row_dict = dict(row)
        current_type = _normalize_org_type(row_dict.get("org_type"))
        target_type = _resolve_org_type(name=row_dict.get("name"), org_type=row_dict.get("org_type"))

        current_primary = _normalize_primary_type(row_dict.get("primary_type"))
        target_primary = _primary_type_for_org_type(target_type)

        if current_type == target_type and current_primary == target_primary:
            continue

        updates.append(
            {
                "id": int(row_dict["id"]),
                "from_org_type": current_type or None,
                "to_org_type": target_type,
                "from_primary_type": current_primary or None,
                "to_primary_type": target_primary,
            }
        )
        key = f"{current_type or '(empty)'} -> {target_type}"
        transitions[key] = transitions.get(key, 0) + 1

    if not dry_run and updates:
        with get_db() as conn:
            conn.execute(
                text(
                    """
                    UPDATE orgs
                    SET org_type = :org_type,
                        primary_type = :primary_type
                    WHERE id = :id
                    """
                ),
                [
                    {
                        "id": item["id"],
                        "org_type": item["to_org_type"],
                        "primary_type": item["to_primary_type"],
                    }
                    for item in updates
                ],
            )

    with get_db() as conn:
        null_org_type_count = int(
            conn.execute(text("SELECT COUNT(*) FROM orgs WHERE org_type IS NULL OR trim(org_type) = ''")).scalar_one()
        )
        forbidden_org_type_count = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM orgs
                    WHERE lower(trim(coalesce(org_type, ''))) IN ('one-off_event', 'one_off_event', 'other', 'poetry_readings')
                    """
                )
            ).scalar_one()
        )

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "scanned": len(rows),
        "updated": len(updates),
        "org_type_transitions": transitions,
        "sample": updates[:20],
        "null_org_type_count": null_org_type_count,
        "forbidden_org_type_count": forbidden_org_type_count,
    }
