from __future__ import annotations
import json
import os
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from sqlalchemy import create_engine, inspect, text
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "orgs.db"
def _db_url(raw: str | None) -> str:
    if not raw:
        return f"sqlite:///{DEFAULT_SQLITE_PATH}"
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql+psycopg://", 1)
    if raw.startswith("postgresql://") and "+psycopg" not in raw:
        return raw.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw
ENGINE = create_engine(_db_url(os.getenv("DATABASE_URL")), future=True, pool_pre_ping=True)
IS_POSTGRES = ENGINE.dialect.name.startswith("postgres")
BINARY_TRUE = "TRUE" if IS_POSTGRES else "1"
BINARY_FALSE = "FALSE" if IS_POSTGRES else "0"
CONFLICT_KIND_LABELS = {
    "homepage": "homepage",
    "events_url": "events URL",
    "domain_name": "domain + name",
}
@contextmanager
def get_db():
    with ENGINE.begin() as conn:
        yield conn
class DedupeConflictError(ValueError):
    def __init__(self, org_id: int, kind: str, name: str | None = None):
        self.org_id = int(org_id)
        self.kind = kind
        self.name = _clean(name) or None
        label = CONFLICT_KIND_LABELS.get(kind, kind.replace("_", " "))
        name_part = f" ({self.name})" if self.name else ""
        super().__init__(f"Conflicts with existing org #{self.org_id}{name_part} by {label}.")
    def to_payload(self) -> dict[str, Any]:
        return {
            "error": str(self),
            "conflict_org_id": self.org_id,
            "conflict_kind": self.kind,
            "conflict_name": self.name,
        }
ORG_TYPES = (
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
VENUE_TYPES = {"bookshop", "cinema", "gallery", "live_music_venue", "theatre", "museum", "makers_space", "park", "garden"}
INSTITUTION_TYPES = {"cultural_centre", "university", "learned_society"}
PRIMARY_TYPES = ("venue", "institution", "organisation")
ORG_TYPE_ALIASES = {
    "live music venue": "live_music_venue",
    "makerspace": "makers_space",
    "makers space": "makers_space",
    "maker space": "makers_space",
    "cultural center": "cultural_centre",
    "learned society": "learned_society",
    "one-off event": "festival",
    "one off event": "festival",
    "one_off_event": "festival",
    "community cinema": "cinema",
    "bookshop events": "bookshop",
    "social community center": "cultural_centre",
    "social community centre": "cultural_centre",
    "community space": "cultural_centre",
    "lecture series": "learned_society",
    "education": "learned_society",
    "poetry readings": "organisation",
    "other": "organisation",
    "music venue": "live_music_venue",
    "workshop space": "makers_space",
    "arts centre": "cultural_centre",
    "arts center": "cultural_centre",
    "cultural institute": "cultural_centre",
}
BLOCKED_DOMAIN_SUFFIXES = (
    "github.com",
    "bsky.app",
    "bsky.social",
    "twitter.com",
    "x.com",
    "instagram.com",
    "facebook.com",
    "youtube.com",
    "linkedin.com",
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
    "book your tickets",
    "subscribe",
    "support",
    "overview",
    "events listings",
    "museums and collections",
    "courses and meetings",
)
def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
def _token(value: Any) -> str:
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
    return (urlparse(canonical).netloc or "").lower().replace("www.", "") if canonical else ""
def _domain_name_key(name: Any, homepage: Any = None, events_url: Any = None) -> str:
    domain = _domain(homepage) or _domain(events_url)
    name_key = _token(name)
    return f"{domain}|{name_key}" if domain and name_key else ""
def _org_keys(name: Any, homepage: Any = None, events_url: Any = None) -> dict[str, str | None]:
    homepage_key = _canonical_url(homepage) or None
    events_url_key = _canonical_url(events_url) or None
    name_key = _token(name) or None
    domain_name_key = _domain_name_key(name, homepage, events_url) or None
    return {
        "homepage_key": homepage_key,
        "events_url_key": events_url_key,
        "name_key": name_key,
        "domain_name_key": domain_name_key,
    }
def _normalize_org_type(value: Any) -> str:
    raw = _token(value)
    if not raw:
        return ""
    if raw in ORG_TYPE_ALIASES:
        return ORG_TYPE_ALIASES[raw]
    snake = raw.replace(" ", "_")
    return snake if snake in ORG_TYPES else ""
def _contains(name: str, term: str) -> bool:
    return bool(re.search(rf"(?:^|\s){re.escape(term)}(?:\s|$)", name))
def _contains_any(name: str, terms: tuple[str, ...]) -> bool:
    return any(_contains(name, term) for term in terms)
def _infer_org_type(name: Any) -> str:
    n = _token(name)
    if not n:
        return "organisation"
    if _contains_any(n, ("university", "college", "soas", "lse", "imperial")):
        return "university"
    if _contains_any(n, ("society", "institute", "gresham", "rsa")):
        return "learned_society"
    if _contains_any(n, ("bookshop", "bookstore")):
        return "bookshop"
    if _contains_any(n, ("cinema",)):
        return "cinema"
    if _contains_any(n, ("gallery",)):
        return "gallery"
    if _contains_any(n, ("museum", "archive")):
        return "museum"
    if _contains_any(n, ("theatre", "theater")):
        return "theatre"
    if _contains_any(n, ("makerspace", "maker space", "workshop")):
        return "makers_space"
    if _contains_any(n, ("park",)):
        return "park"
    if _contains_any(n, ("garden", "conservatory")):
        return "garden"
    if _contains_any(n, ("festival", "biennale", "triennale", "carnival")):
        return "festival"
    if _contains_any(n, ("promoter", "productions", "presents")):
        return "promoter"
    if _contains_any(n, ("arts centre", "arts center", "cultural centre", "cultural center", "cultural institute", "japan house", "goethe", "cervantes", "institut", "istituto", "alliance")):
        return "cultural_centre"
    if _contains_any(n, ("music", "jazz", "orchestra", "club")):
        return "live_music_venue"
    return "organisation"
def _resolve_org_type(name: Any, org_type: Any = None) -> str:
    return _normalize_org_type(org_type) or _infer_org_type(name)
def _primary_for(org_type: str) -> str:
    if org_type in VENUE_TYPES:
        return "venue"
    if org_type in INSTITUTION_TYPES:
        return "institution"
    return "organisation"
def _normalize_primary(primary: Any, fallback_type: str) -> str:
    raw = _token(primary).replace(" ", "_")
    return raw if raw in PRIMARY_TYPES else _primary_for(fallback_type)
def _description(name: str, org_type: str, borough: str | None, existing: Any = None, candidate: Any = None) -> str:
    current = _clean(existing)
    next_value = _clean(candidate)
    if next_value and len(next_value) >= len(current):
        return next_value
    if current:
        return current
    kind = org_type.replace("_", " ")
    return f"{name} is a London {kind} in {borough}." if borough else f"{name} is a London {kind}."
def _active_sql() -> str:
    return f"active = {BINARY_TRUE} AND coalesce(crawl_paused, {BINARY_FALSE}) = {BINARY_FALSE}"
def _queue_sql() -> str:
    return (
        "(coalesce(issue_state,'none') <> 'resolved' AND (issue_state='open' OR events_url IS NULL OR trim(events_url)='' "
        "OR borough IS NULL OR trim(borough)='' OR org_type IS NULL OR trim(org_type)='' "
        "OR coalesce(consecutive_failures,0) >= 3 OR coalesce(consecutive_empty_extracts,0) >= 3))"
    )
def _table_columns(conn, table: str) -> set[str]:
    return {str(col.get("name") or "").strip() for col in inspect(conn).get_columns(table)}
def _ensure_columns(conn, table: str, specs: tuple[str, ...]) -> None:
    existing = _table_columns(conn, table)
    for spec in specs:
        column = spec.split()[0]
        if column in existing:
            continue
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {spec}"))
        existing.add(column)
def _create_unique_key_indexes(conn) -> None:
    for sql in (
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_orgs_homepage_key ON orgs(homepage_key) WHERE homepage_key IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_orgs_events_url_key ON orgs(events_url_key) WHERE events_url_key IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_orgs_domain_name_key ON orgs(domain_name_key) WHERE domain_name_key IS NOT NULL",
    ):
        conn.execute(text(sql if IS_POSTGRES else sql.split(" WHERE ", 1)[0]))
def _sync_org_keys(conn) -> None:
    rows = conn.execute(
        text("SELECT id, name, homepage, events_url, homepage_key, events_url_key, name_key, domain_name_key FROM orgs ORDER BY id ASC")
    ).mappings().all()
    seen_home: set[str] = set()
    seen_events: set[str] = set()
    seen_domain_name: set[str] = set()
    updates: list[dict[str, Any]] = []
    for row in rows:
        keys = _org_keys(row.get("name"), row.get("homepage"), row.get("events_url"))
        homepage_key = keys["homepage_key"] if keys["homepage_key"] and keys["homepage_key"] not in seen_home else None
        events_url_key = keys["events_url_key"] if keys["events_url_key"] and keys["events_url_key"] not in seen_events else None
        domain_name_key = keys["domain_name_key"] if keys["domain_name_key"] and keys["domain_name_key"] not in seen_domain_name else None
        if homepage_key:
            seen_home.add(homepage_key)
        if events_url_key:
            seen_events.add(events_url_key)
        if domain_name_key:
            seen_domain_name.add(domain_name_key)
        name_key = keys["name_key"]
        if (
            row.get("homepage_key") == homepage_key
            and row.get("events_url_key") == events_url_key
            and row.get("name_key") == name_key
            and row.get("domain_name_key") == domain_name_key
        ):
            continue
        updates.append(
            {
                "id": int(row["id"]),
                "homepage_key": homepage_key,
                "events_url_key": events_url_key,
                "name_key": name_key,
                "domain_name_key": domain_name_key,
            }
        )
    if updates:
        conn.execute(
            text(
                "UPDATE orgs SET homepage_key=:homepage_key, events_url_key=:events_url_key, "
                "name_key=:name_key, domain_name_key=:domain_name_key WHERE id=:id"
            ),
            updates,
        )
def _conflicting_org(conn, org_id: int | None = None, **keys: Any) -> dict[str, Any] | None:
    for field, kind in (("events_url_key", "events_url"), ("homepage_key", "homepage"), ("domain_name_key", "domain_name")):
        value = keys.get(field)
        if not value:
            continue
        params: dict[str, Any] = {field: value}
        sql = f"SELECT id, name FROM orgs WHERE {field}=:{field}"
        if org_id is not None:
            sql += " AND id<>:id"
            params["id"] = int(org_id)
        row = conn.execute(text(sql + " ORDER BY id ASC LIMIT 1"), params).mappings().first()
        if row:
            return {"id": int(row["id"]), "name": _clean(row.get("name")), "kind": kind}
    return None
def _decode_json(value: Any) -> Any:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return json.loads(value)
        except Exception:
            return None
    return None
def init_db() -> None:
    org_id = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    run_id = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    with get_db() as conn:
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS orgs ("
                f"id {org_id}, name TEXT NOT NULL, homepage TEXT, events_url TEXT, description TEXT, borough TEXT, "
                f"primary_type TEXT NOT NULL DEFAULT 'organisation', org_type TEXT NOT NULL DEFAULT 'organisation', "
                f"parent_org_id BIGINT, source TEXT, source_domain TEXT, status TEXT NOT NULL DEFAULT 'pending', notes TEXT, "
                f"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, reviewed_at TIMESTAMP, "
                f"active BOOLEAN NOT NULL DEFAULT {BINARY_TRUE}, crawl_paused BOOLEAN NOT NULL DEFAULT {BINARY_FALSE}, "
                f"last_crawled_at TIMESTAMP, last_successful_event_extract_at TIMESTAMP, "
                f"consecutive_failures INTEGER NOT NULL DEFAULT 0, consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0, "
                f"issue_state TEXT NOT NULL DEFAULT 'none', review_needed_reason TEXT"
                f")"
            )
        )
        _ensure_columns(
            conn,
            "orgs",
            (
            "primary_type TEXT NOT NULL DEFAULT 'organisation'",
            "org_type TEXT NOT NULL DEFAULT 'organisation'",
            "parent_org_id BIGINT",
            "source_domain TEXT",
            "homepage_key TEXT",
            "events_url_key TEXT",
            "name_key TEXT",
            "domain_name_key TEXT",
            f"active BOOLEAN NOT NULL DEFAULT {BINARY_TRUE}",
            f"crawl_paused BOOLEAN NOT NULL DEFAULT {BINARY_FALSE}",
            "issue_state TEXT NOT NULL DEFAULT 'none'",
            "review_needed_reason TEXT",
            "consecutive_failures INTEGER NOT NULL DEFAULT 0",
            "consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0",
            "last_crawled_at TIMESTAMP",
            "last_successful_event_extract_at TIMESTAMP",
            ),
        )
        conn.execute(text(f"UPDATE orgs SET active = {BINARY_TRUE} WHERE active IS NULL"))
        conn.execute(text(f"UPDATE orgs SET crawl_paused = {BINARY_FALSE} WHERE crawl_paused IS NULL"))
        conn.execute(text("UPDATE orgs SET issue_state='none' WHERE issue_state IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_failures=0 WHERE consecutive_failures IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_empty_extracts=0 WHERE consecutive_empty_extracts IS NULL"))
        conn.execute(text("UPDATE orgs SET org_type='organisation' WHERE org_type IS NULL OR trim(org_type)=''"))
        conn.execute(text("UPDATE orgs SET primary_type='organisation' WHERE primary_type IS NULL OR trim(primary_type)=''"))
        _sync_org_keys(conn)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_active ON orgs(active,crawl_paused)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_issue ON orgs(issue_state)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_type ON orgs(org_type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_domain ON orgs(source_domain)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_name_key ON orgs(name_key)"))
        _create_unique_key_indexes(conn)
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS import_runs ("
                f"id {run_id}, trigger TEXT NOT NULL DEFAULT 'manual', source TEXT, file_name TEXT, status TEXT NOT NULL DEFAULT 'running', "
                f"row_count INTEGER NOT NULL DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP, "
                f"summary_json TEXT, error TEXT"
                f")"
            )
        )
        _ensure_columns(
            conn,
            "import_runs",
            (
            "trigger TEXT NOT NULL DEFAULT 'manual'",
            "source TEXT",
            "file_name TEXT",
            "status TEXT NOT NULL DEFAULT 'running'",
            "row_count INTEGER NOT NULL DEFAULT 0",
            "finished_at TIMESTAMP",
            "summary_json TEXT",
            "error TEXT",
            ),
        )
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_runs_created ON import_runs(created_at DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_import_runs_status ON import_runs(status)"))
def _find_existing_org(conn, name: str, homepage: str | None, events_url: str | None) -> tuple[dict[str, Any] | None, str | None]:
    keys = _org_keys(name, homepage, events_url)
    for field, kind in (("events_url_key", "events_url"), ("homepage_key", "homepage"), ("domain_name_key", "domain_name")):
        value = keys.get(field)
        if not value:
            continue
        row = conn.execute(text(f"SELECT * FROM orgs WHERE {field}=:{field} ORDER BY id ASC LIMIT 1"), {field: value}).mappings().first()
        if row:
            return dict(row), kind
    name_key = keys.get("name_key")
    if not name_key:
        return None, None
    row = conn.execute(text("SELECT * FROM orgs WHERE name_key=:name_key ORDER BY id ASC LIMIT 1"), {"name_key": name_key}).mappings().first()
    return (dict(row), "name") if row else (None, None)
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
    return_meta: bool = False,
) -> int | dict[str, Any]:
    _ = category
    clean_name = _clean(name)
    if not clean_name:
        raise ValueError("name is required")
    home = _canonical_url(homepage) or None
    events = _canonical_url(events_url) or None
    boro = _clean(borough) or None
    src = _clean(source) or None
    src_domain = _domain(home) or _domain(events) or None
    resolved_type = _resolve_org_type(clean_name, org_type)
    resolved_primary = _normalize_primary(primary_type, resolved_type)
    keys = _org_keys(clean_name, home, events)
    with get_db() as conn:
        existing, match_kind = _find_existing_org(conn, clean_name, home, events)
        if existing:
            updates: dict[str, Any] = {}
            if home and not _clean(existing.get("homepage")):
                updates["homepage"] = home
            if events and not _clean(existing.get("events_url")):
                updates["events_url"] = events
            if boro and not _clean(existing.get("borough")):
                updates["borough"] = boro
            if src and not _clean(existing.get("source")):
                updates["source"] = src
            if src_domain and not _clean(existing.get("source_domain")):
                updates["source_domain"] = src_domain
            if parent_org_id is not None:
                updates["parent_org_id"] = int(parent_org_id)
            current_type = _resolve_org_type(existing.get("name"), existing.get("org_type"))
            if current_type == "organisation" and resolved_type != "organisation":
                updates["org_type"] = resolved_type
            target_type = updates.get("org_type") or current_type
            target_primary = _primary_for(target_type)
            if _clean(existing.get("primary_type")) != target_primary:
                updates["primary_type"] = target_primary
            merged_description = _description(clean_name, target_type, boro or _clean(existing.get("borough")) or None, existing.get("description"), description)
            if merged_description != _clean(existing.get("description")):
                updates["description"] = merged_description
            updates.update({k: v for k, v in keys.items() if v or k == "name_key"})
            conflict = _conflicting_org(conn, int(existing["id"]), **updates)
            if conflict:
                raise DedupeConflictError(conflict["id"], conflict["kind"], conflict.get("name"))
            if updates:
                set_sql = ", ".join(f"{key}=:{key}" for key in updates)
                conn.execute(text(f"UPDATE orgs SET {set_sql} WHERE id=:id"), {**updates, "id": int(existing["id"])})
            org_id = int(existing["id"])
            return {"id": org_id, "created": False, "match_kind": match_kind} if return_meta else org_id
        conflict = _conflicting_org(conn, **keys)
        if conflict:
            raise DedupeConflictError(conflict["id"], conflict["kind"], conflict.get("name"))
        params = {
            "name": clean_name,
            "homepage": home,
            "events_url": events,
            "description": _description(clean_name, resolved_type, boro, None, description),
            "borough": boro,
            "org_type": resolved_type,
            "primary_type": resolved_primary,
            "parent_org_id": int(parent_org_id) if parent_org_id is not None else None,
            "source": src,
            "source_domain": src_domain,
            **keys,
        }
        if IS_POSTGRES:
            row = conn.execute(
                text(
                    "INSERT INTO orgs (name,homepage,events_url,description,borough,org_type,primary_type,parent_org_id,source,source_domain,homepage_key,events_url_key,name_key,domain_name_key) "
                    "VALUES (:name,:homepage,:events_url,:description,:borough,:org_type,:primary_type,:parent_org_id,:source,:source_domain,:homepage_key,:events_url_key,:name_key,:domain_name_key) RETURNING id"
                ),
                params,
            ).mappings().first()
            if row:
                org_id = int(row["id"])
                return {"id": org_id, "created": True, "match_kind": None} if return_meta else org_id
        else:
            conn.execute(
                text(
                    "INSERT INTO orgs (name,homepage,events_url,description,borough,org_type,primary_type,parent_org_id,source,source_domain,homepage_key,events_url_key,name_key,domain_name_key) "
                    "VALUES (:name,:homepage,:events_url,:description,:borough,:org_type,:primary_type,:parent_org_id,:source,:source_domain,:homepage_key,:events_url_key,:name_key,:domain_name_key)"
                ),
                params,
            )
            row = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if row:
                org_id = int(row["id"])
                return {"id": org_id, "created": True, "match_kind": None} if return_meta else org_id
    raise RuntimeError("upsert failed")
def get_org(org_id: int) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(text("SELECT * FROM orgs WHERE id=:id"), {"id": int(org_id)}).mappings().first()
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
        "source_domain",
        "active",
        "crawl_paused",
        "last_crawled_at",
        "last_successful_event_extract_at",
        "consecutive_failures",
        "consecutive_empty_extracts",
        "issue_state",
        "review_needed_reason",
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    current = get_org(org_id) or {}
    if "name" in updates:
        updates["name"] = _clean(updates.get("name")) or _clean(current.get("name"))
    if "homepage" in updates:
        updates["homepage"] = _canonical_url(updates.get("homepage")) or None
    if "events_url" in updates:
        updates["events_url"] = _canonical_url(updates.get("events_url")) or None
    if "borough" in updates:
        updates["borough"] = _clean(updates.get("borough")) or None
    if "source" in updates:
        updates["source"] = _clean(updates.get("source")) or None
    if "org_type" in updates:
        updates["org_type"] = _resolve_org_type(updates.get("name") or current.get("name"), updates.get("org_type"))
        if "primary_type" not in updates:
            updates["primary_type"] = _primary_for(updates["org_type"])
    if "primary_type" in updates:
        fallback = updates.get("org_type") or _resolve_org_type(current.get("name"), current.get("org_type"))
        updates["primary_type"] = _normalize_primary(updates.get("primary_type"), fallback)
    if "description" in updates:
        name_value = updates.get("name") or _clean(current.get("name"))
        type_value = updates.get("org_type") or _resolve_org_type(current.get("name"), current.get("org_type"))
        borough_value = updates.get("borough") if "borough" in updates else _clean(current.get("borough")) or None
        updates["description"] = _description(name_value, type_value, borough_value, current.get("description"), updates.get("description"))
    if "parent_org_id" in updates:
        try:
            updates["parent_org_id"] = int(updates["parent_org_id"]) if updates["parent_org_id"] is not None else None
        except Exception:
            updates["parent_org_id"] = None
    if "homepage" in updates or "events_url" in updates or "source_domain" not in updates:
        homepage_value = updates.get("homepage") if "homepage" in updates else current.get("homepage")
        events_value = updates.get("events_url") if "events_url" in updates else current.get("events_url")
        updates["source_domain"] = _domain(homepage_value) or _domain(events_value) or None
    key_name = updates.get("name") or current.get("name")
    key_home = updates.get("homepage") if "homepage" in updates else current.get("homepage")
    key_events = updates.get("events_url") if "events_url" in updates else current.get("events_url")
    updates.update(_org_keys(key_name, key_home, key_events))
    with get_db() as conn:
        conflict = _conflicting_org(conn, int(org_id), **updates)
        if conflict:
            raise DedupeConflictError(conflict["id"], conflict["kind"], conflict.get("name"))
        set_sql = ", ".join(f"{key}=:{key}" for key in updates)
        if "status" in updates:
            set_sql += ", reviewed_at=CASE WHEN :status='pending' THEN NULL ELSE CURRENT_TIMESTAMP END"
        conn.execute(text(f"UPDATE orgs SET {set_sql} WHERE id=:id"), {**updates, "id": int(org_id)})
def get_active_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    sql = f"SELECT * FROM orgs WHERE {_active_sql()} ORDER BY name ASC"
    params: dict[str, Any] = {}
    if limit and int(limit) > 0:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)
    with get_db() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
    return [dict(row) for row in rows]
def get_public_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    return get_active_orgs(limit)
def get_review_queue_orgs(limit: int = 250) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text(
                f"SELECT * FROM orgs WHERE {_active_sql()} AND coalesce(issue_state,'none') <> 'snoozed' AND {_queue_sql()} "
                f"ORDER BY CASE WHEN issue_state='open' THEN 0 WHEN events_url IS NULL OR trim(events_url)='' THEN 1 "
                f"WHEN borough IS NULL OR trim(borough)='' THEN 2 WHEN org_type IS NULL OR trim(org_type)='' THEN 3 ELSE 4 END, "
                f"created_at DESC, id DESC LIMIT :limit"
            ),
            {"limit": int(limit)},
        ).mappings().all()
    return [dict(row) for row in rows]
def get_stats() -> dict[str, int]:
    with get_db() as conn:
        status_rows = conn.execute(text("SELECT status, COUNT(*) AS c FROM orgs GROUP BY status")).mappings().all()
        total = int(conn.execute(text("SELECT COUNT(*) FROM orgs")).scalar_one())
        active_total = int(conn.execute(text(f"SELECT COUNT(*) FROM orgs WHERE {_active_sql()}")) .scalar_one())
        queue_total = int(
            conn.execute(text(f"SELECT COUNT(*) FROM orgs WHERE {_active_sql()} AND coalesce(issue_state,'none') <> 'snoozed' AND {_queue_sql()}"))
            .scalar_one()
        )
        open_issues = int(conn.execute(text(f"SELECT COUNT(*) FROM orgs WHERE {_active_sql()} AND issue_state='open'")) .scalar_one())
    out = {"pending": 0, "approved": 0, "maybe": 0, "rejected": 0}
    for row in status_rows:
        key = _clean(row.get("status")).lower()
        if key in out:
            out[key] = int(row.get("c") or 0)
    return {**out, "total": total, "active_total": active_total, "queue_total": queue_total, "open_issues": open_issues}
def normalize_org_taxonomy(dry_run: bool = False) -> dict[str, Any]:
    with get_db() as conn:
        rows = conn.execute(text("SELECT id,name,org_type,primary_type FROM orgs ORDER BY id ASC")).mappings().all()
    updates: list[dict[str, Any]] = []
    transitions: dict[str, int] = {}
    for row in rows:
        r = dict(row)
        current_type = _clean(r.get("org_type"))
        target_type = _resolve_org_type(r.get("name"), current_type)
        current_primary = _clean(r.get("primary_type"))
        target_primary = _primary_for(target_type)
        if current_type == target_type and current_primary == target_primary:
            continue
        updates.append(
            {
                "id": int(r["id"]),
                "from_org_type": current_type or "(empty)",
                "to_org_type": target_type,
                "from_primary_type": current_primary or "(empty)",
                "to_primary_type": target_primary,
            }
        )
        key = f"{current_type or '(empty)'} -> {target_type}"
        transitions[key] = transitions.get(key, 0) + 1
    if updates and not dry_run:
        with get_db() as conn:
            conn.execute(
                text("UPDATE orgs SET org_type=:org_type, primary_type=:primary_type WHERE id=:id"),
                [{"id": item["id"], "org_type": item["to_org_type"], "primary_type": item["to_primary_type"]} for item in updates],
            )
    with get_db() as conn:
        null_org_type_count = int(conn.execute(text("SELECT COUNT(*) FROM orgs WHERE org_type IS NULL OR trim(org_type)=''")) .scalar_one())
        forbidden_org_type_count = int(
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM orgs WHERE lower(trim(coalesce(org_type,''))) IN ('one-off_event','one_off_event','other','poetry_readings')"
                )
            ).scalar_one()
        )
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "scanned": len(rows),
        "updated": len(updates),
        "org_type_transitions": transitions,
        "sample": updates[:40],
        "null_org_type_count": null_org_type_count,
        "forbidden_org_type_count": forbidden_org_type_count,
    }
def start_import_run(*, trigger: str = "manual", source: str | None = None, file_name: str | None = None, row_count: int = 0) -> int:
    params = {
        "trigger": _clean(trigger) or "manual",
        "source": _clean(source) or None,
        "file_name": _clean(file_name) or None,
        "row_count": int(row_count),
    }
    with get_db() as conn:
        if IS_POSTGRES:
            row = conn.execute(
                text(
                    "INSERT INTO import_runs (trigger,source,file_name,status,row_count) "
                    "VALUES (:trigger,:source,:file_name,'running',:row_count) RETURNING id"
                ),
                params,
            ).mappings().first()
            if row:
                return int(row["id"])
        else:
            conn.execute(
                text(
                    "INSERT INTO import_runs (trigger,source,file_name,status,row_count) "
                    "VALUES (:trigger,:source,:file_name,'running',:row_count)"
                ),
                params,
            )
            row = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if row:
                return int(row["id"])
    raise RuntimeError("failed to start import run")
def finish_import_run(run_id: int, *, status: str, summary: dict[str, Any] | None = None, error: str | None = None) -> None:
    safe_status = status if status in {"running", "success", "failed"} else "failed"
    with get_db() as conn:
        conn.execute(
            text(
                "UPDATE import_runs SET status=:status, summary_json=:summary_json, error=:error, finished_at=CURRENT_TIMESTAMP WHERE id=:id"
            ),
            {
                "id": int(run_id),
                "status": safe_status,
                "summary_json": json.dumps(summary or {}),
                "error": _clean(error)[:4000] or None,
            },
        )
def get_import_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text("SELECT * FROM import_runs ORDER BY created_at DESC, id DESC LIMIT :limit"),
            {"limit": int(limit)},
        ).mappings().all()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["summary"] = _decode_json(item.get("summary_json"))
        item.pop("summary_json", None)
        out.append(item)
    return out
