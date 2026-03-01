from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import create_engine, text

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


@contextmanager
def get_db():
    with ENGINE.begin() as conn:
        yield conn


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
    "instagram.com",
    "facebook.com",
    "youtube.com",
    "linkedin.com",
    "eventbrite.com",
    "ticketmaster.com",
    "feverup.com",
    "designmynight.com",
    "secretldn.com",
    "timeout.com",
    "culturecalling.com",
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
    "book your tickets",
    "courses and meetings",
    "support ianvisits",
    "subscribe to read",
    "events listings",
    "overview",
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


def _blocked_domain(host: str) -> bool:
    h = _clean(host).lower().replace("www.", "")
    return bool(h) and any(h == suffix or h.endswith(f".{suffix}") for suffix in BLOCKED_DOMAIN_SUFFIXES)


def _normalize_org_type(value: Any) -> str:
    raw = _token(value)
    if not raw:
        return ""
    if raw in ORG_TYPE_ALIASES:
        return ORG_TYPE_ALIASES[raw]
    snake = raw.replace(" ", "_")
    return snake if snake in ORG_TYPES else ""


def _contains(text_value: str, term: str) -> bool:
    return bool(re.search(rf"(?:^|\s){re.escape(term)}(?:\s|$)", text_value))


def _contains_any(text_value: str, terms: tuple[str, ...]) -> bool:
    return any(_contains(text_value, term) for term in terms)


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
    e, c = _clean(existing), _clean(candidate)
    if c and len(c) >= len(e):
        return c
    if e:
        return e
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


def _decode_run(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    try:
        out["details"] = json.loads(out.get("details_json") or "")
    except Exception:
        out["details"] = None
    out.pop("details_json", None)
    return out


def _add_column(conn, table: str, spec: str) -> None:
    try:
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {'IF NOT EXISTS ' if IS_POSTGRES else ''}{spec}"))
    except Exception:
        pass


def init_db() -> None:
    org_id = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    run_id = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"

    with get_db() as conn:
        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS orgs ("
                f"id {org_id}, name TEXT NOT NULL, homepage TEXT, events_url TEXT, description TEXT, borough TEXT, "
                f"primary_type TEXT NOT NULL DEFAULT 'organisation', org_type TEXT NOT NULL DEFAULT 'organisation', parent_org_id BIGINT, "
                f"source TEXT, source_domain TEXT, status TEXT NOT NULL DEFAULT 'pending', notes TEXT, "
                f"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, reviewed_at TIMESTAMP, "
                f"active BOOLEAN NOT NULL DEFAULT {BINARY_TRUE}, crawl_paused BOOLEAN NOT NULL DEFAULT {BINARY_FALSE}, "
                f"last_crawled_at TIMESTAMP, last_successful_event_extract_at TIMESTAMP, "
                f"consecutive_failures INTEGER NOT NULL DEFAULT 0, consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0, "
                f"issue_state TEXT NOT NULL DEFAULT 'none', review_needed_reason TEXT"
                f")"
            )
        )

        for spec in (
            "primary_type TEXT NOT NULL DEFAULT 'organisation'",
            "org_type TEXT NOT NULL DEFAULT 'organisation'",
            "parent_org_id BIGINT",
            "source_domain TEXT",
            f"active BOOLEAN NOT NULL DEFAULT {BINARY_TRUE}",
            f"crawl_paused BOOLEAN NOT NULL DEFAULT {BINARY_FALSE}",
            "issue_state TEXT NOT NULL DEFAULT 'none'",
            "review_needed_reason TEXT",
            "consecutive_failures INTEGER NOT NULL DEFAULT 0",
            "consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0",
            "last_crawled_at TIMESTAMP",
            "last_successful_event_extract_at TIMESTAMP",
        ):
            _add_column(conn, "orgs", spec)

        conn.execute(text(f"UPDATE orgs SET active = {BINARY_TRUE} WHERE active IS NULL"))
        conn.execute(text(f"UPDATE orgs SET crawl_paused = {BINARY_FALSE} WHERE crawl_paused IS NULL"))
        conn.execute(text("UPDATE orgs SET issue_state='none' WHERE issue_state IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_failures=0 WHERE consecutive_failures IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_empty_extracts=0 WHERE consecutive_empty_extracts IS NULL"))
        conn.execute(text("UPDATE orgs SET org_type='organisation' WHERE org_type IS NULL OR trim(org_type)=''"))
        conn.execute(text("UPDATE orgs SET primary_type='organisation' WHERE primary_type IS NULL OR trim(primary_type)=''"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_active ON orgs(active,crawl_paused)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_issue ON orgs(issue_state)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_type ON orgs(org_type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_domain ON orgs(source_domain)"))

        conn.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS discovery_runs ("
                f"id {run_id}, trigger TEXT NOT NULL DEFAULT 'scheduled', status TEXT NOT NULL DEFAULT 'running', "
                f"query_count INTEGER NOT NULL DEFAULT 0, result_count INTEGER NOT NULL DEFAULT 0, upserted_count INTEGER NOT NULL DEFAULT 0, "
                f"started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP, error TEXT, details_json TEXT"
                f")"
            )
        )
        for spec in (
            "trigger TEXT NOT NULL DEFAULT 'scheduled'",
            "status TEXT NOT NULL DEFAULT 'running'",
            "query_count INTEGER NOT NULL DEFAULT 0",
            "result_count INTEGER NOT NULL DEFAULT 0",
            "upserted_count INTEGER NOT NULL DEFAULT 0",
            "finished_at TIMESTAMP",
            "error TEXT",
            "details_json TEXT",
        ):
            _add_column(conn, "discovery_runs", spec)

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_runs_started ON discovery_runs(started_at DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_runs_status ON discovery_runs(status)"))


def _find_candidates(conn, name: str, homepage: str | None, events_url: str | None, source_domain: str | None) -> list[dict[str, Any]]:
    clauses, params = ["lower(name)=lower(:name)"], {"name": name}
    if homepage:
        clauses.insert(0, "homepage=:homepage")
        params["homepage"] = homepage
    if events_url:
        clauses.insert(0, "events_url=:events_url")
        params["events_url"] = events_url
    if source_domain:
        clauses.append("source_domain=:source_domain")
        params["source_domain"] = source_domain
    rows = conn.execute(text(f"SELECT * FROM orgs WHERE {' OR '.join(clauses)} ORDER BY id ASC LIMIT 20"), params).mappings().all()
    return [dict(row) for row in rows]


def _pick_existing(rows: list[dict[str, Any]], homepage: str | None, events_url: str | None, source_domain: str | None) -> dict[str, Any] | None:
    if not rows:
        return None
    if events_url:
        for row in rows:
            if _canonical_url(row.get("events_url")) == events_url:
                return row
    if homepage:
        for row in rows:
            if _canonical_url(row.get("homepage")) == homepage:
                return row
    if source_domain:
        for row in rows:
            if _clean(row.get("source_domain")) == source_domain:
                return row
    return rows[0]


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

    with get_db() as conn:
        rows = _find_candidates(conn, clean_name, home, events, src_domain)
        existing = _pick_existing(rows, home, events, src_domain)

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
            next_type = updates.get("org_type") or current_type
            next_primary = _primary_for(next_type)
            if _clean(existing.get("primary_type")) != next_primary:
                updates["primary_type"] = next_primary
            next_desc = _description(clean_name, next_type, boro or _clean(existing.get("borough")) or None, existing.get("description"), description)
            if next_desc != _clean(existing.get("description")):
                updates["description"] = next_desc

            if updates:
                set_sql = ", ".join(f"{k}=:{k}" for k in updates)
                conn.execute(text(f"UPDATE orgs SET {set_sql} WHERE id=:id"), {**updates, "id": int(existing["id"])})
            return int(existing["id"])

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
        }

        if IS_POSTGRES:
            row = conn.execute(
                text(
                    "INSERT INTO orgs (name,homepage,events_url,description,borough,org_type,primary_type,parent_org_id,source,source_domain) "
                    "VALUES (:name,:homepage,:events_url,:description,:borough,:org_type,:primary_type,:parent_org_id,:source,:source_domain) RETURNING id"
                ),
                params,
            ).mappings().first()
            if row:
                return int(row["id"])
        else:
            conn.execute(
                text(
                    "INSERT INTO orgs (name,homepage,events_url,description,borough,org_type,primary_type,parent_org_id,source,source_domain) "
                    "VALUES (:name,:homepage,:events_url,:description,:borough,:org_type,:primary_type,:parent_org_id,:source,:source_domain)"
                ),
                params,
            )
            row = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if row:
                return int(row["id"])

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
        n = updates.get("name") or _clean(current.get("name"))
        t = updates.get("org_type") or _resolve_org_type(current.get("name"), current.get("org_type"))
        b = updates.get("borough") if "borough" in updates else _clean(current.get("borough")) or None
        updates["description"] = _description(n, t, b, current.get("description"), updates.get("description"))

    if "parent_org_id" in updates:
        try:
            updates["parent_org_id"] = int(updates["parent_org_id"]) if updates["parent_org_id"] is not None else None
        except Exception:
            updates["parent_org_id"] = None

    if "homepage" in updates or "events_url" in updates or "source_domain" not in updates:
        h = updates.get("homepage") if "homepage" in updates else current.get("homepage")
        e = updates.get("events_url") if "events_url" in updates else current.get("events_url")
        updates["source_domain"] = _domain(h) or _domain(e) or None

    set_sql = ", ".join(f"{k}=:{k}" for k in updates)
    if "status" in updates:
        set_sql += ", reviewed_at=CASE WHEN :status='pending' THEN NULL ELSE CURRENT_TIMESTAMP END"

    with get_db() as conn:
        conn.execute(text(f"UPDATE orgs SET {set_sql} WHERE id=:id"), {**updates, "id": int(org_id)})


def get_active_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    sql, params = f"SELECT * FROM orgs WHERE {_active_sql()} ORDER BY name ASC", {}
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


def get_latest_running_discovery_run() -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(text("SELECT * FROM discovery_runs WHERE status='running' AND finished_at IS NULL ORDER BY started_at DESC, id DESC LIMIT 1")).mappings().first()
    return _decode_run(dict(row)) if row else None


def start_discovery_run(query_count: int, trigger: str = "scheduled", details: dict[str, Any] | None = None) -> int:
    params = {"trigger": _clean(trigger) or "scheduled", "query_count": int(query_count), "details_json": json.dumps(details or {})}
    with get_db() as conn:
        if IS_POSTGRES:
            row = conn.execute(text("INSERT INTO discovery_runs (trigger,status,query_count,details_json) VALUES (:trigger,'running',:query_count,:details_json) RETURNING id"), params).mappings().first()
            if row:
                return int(row["id"])
        else:
            conn.execute(text("INSERT INTO discovery_runs (trigger,status,query_count,details_json) VALUES (:trigger,'running',:query_count,:details_json)"), params)
            row = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if row:
                return int(row["id"])
    raise RuntimeError("failed to start discovery run")


def finish_discovery_run(run_id: int, status: str, result_count: int, upserted_count: int, error: str | None = None, details: dict[str, Any] | None = None) -> None:
    safe_status = status if status in {"running", "success", "failed"} else "failed"
    clauses = ["status=:status", "result_count=:result_count", "upserted_count=:upserted_count", "error=:error", "finished_at=CURRENT_TIMESTAMP"]
    params: dict[str, Any] = {
        "id": int(run_id),
        "status": safe_status,
        "result_count": int(result_count),
        "upserted_count": int(upserted_count),
        "error": _clean(error)[:4000] or None,
    }
    if details is not None:
        clauses.insert(4, "details_json=:details_json")
        params["details_json"] = json.dumps(details)
    with get_db() as conn:
        conn.execute(text(f"UPDATE discovery_runs SET {', '.join(clauses)} WHERE id=:id"), params)


def get_latest_discovery_run() -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(text("SELECT * FROM discovery_runs ORDER BY started_at DESC, id DESC LIMIT 1")).mappings().first()
    return _decode_run(dict(row)) if row else None


def get_discovery_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(text("SELECT * FROM discovery_runs ORDER BY started_at DESC, id DESC LIMIT :limit"), {"limit": int(limit)}).mappings().all()
    return [_decode_run(dict(row)) for row in rows]


def cleanup_recent_discovery_garbage(days: int = 7, dry_run: bool = False, limit: int = 1000) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))
    with get_db() as conn:
        rows = conn.execute(
            text("SELECT * FROM orgs WHERE created_at >= :since AND lower(coalesce(source,'')) LIKE 'auto_discovery%' ORDER BY created_at DESC, id DESC LIMIT :limit"),
            {"since": since, "limit": int(limit)},
        ).mappings().all()

    flagged: list[dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        host = _clean(r.get("source_domain")) or _domain(r.get("homepage")) or _domain(r.get("events_url"))
        name_key = _token(r.get("name"))
        reasons: list[str] = []
        if _blocked_domain(host):
            reasons.append("blocked_domain")
        if any(p in name_key for p in BAD_NAME_PHRASES):
            reasons.append("bad_name")
        if reasons:
            flagged.append({"row": r, "reasons": reasons})

    updated = 0
    newly_inactivated = 0
    if not dry_run:
        with get_db() as conn:
            for item in flagged:
                r, reasons = item["row"], item["reasons"]
                updates: dict[str, Any] = {"issue_state": "open", "review_needed_reason": f"Discovery cleanup: {', '.join(reasons)}"}
                if "blocked_domain" in reasons:
                    if bool(r.get("active", True)):
                        newly_inactivated += 1
                    updates["active"] = False
                    updates["crawl_paused"] = True
                set_sql = ", ".join(f"{k}=:{k}" for k in updates)
                conn.execute(text(f"UPDATE orgs SET {set_sql} WHERE id=:id"), {**updates, "id": int(r["id"])})
                updated += 1

    sample = [{"id": int(item["row"]["id"]), "name": _clean(item["row"].get("name")), "reasons": item["reasons"]} for item in flagged[:30]]
    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "days": int(days),
        "scanned": len(rows),
        "flagged": len(flagged),
        "updated": len(flagged) if dry_run else updated,
        "newly_inactivated": 0 if dry_run else newly_inactivated,
        "flagged_names": [_clean(item["row"].get("name")) for item in flagged],
        "sample": sample,
    }


def normalize_org_taxonomy(dry_run: bool = False) -> dict[str, Any]:
    with get_db() as conn:
        rows = conn.execute(text("SELECT id,name,org_type,primary_type FROM orgs ORDER BY id ASC")).mappings().all()

    updates: list[dict[str, Any]] = []
    transitions: dict[str, int] = {}
    for row in rows:
        r = dict(row)
        from_type = _clean(r.get("org_type"))
        to_type = _resolve_org_type(r.get("name"), from_type)
        to_primary = _primary_for(to_type)
        from_primary = _clean(r.get("primary_type"))
        if from_type == to_type and from_primary == to_primary:
            continue
        updates.append({"id": int(r["id"]), "from_org_type": from_type or "(empty)", "to_org_type": to_type, "from_primary_type": from_primary or "(empty)", "to_primary_type": to_primary})
        key = f"{from_type or '(empty)'} -> {to_type}"
        transitions[key] = transitions.get(key, 0) + 1

    if updates and not dry_run:
        with get_db() as conn:
            conn.execute(text("UPDATE orgs SET org_type=:org_type, primary_type=:primary_type WHERE id=:id"), [{"id": item["id"], "org_type": item["to_org_type"], "primary_type": item["to_primary_type"]} for item in updates])

    with get_db() as conn:
        null_org_type_count = int(conn.execute(text("SELECT COUNT(*) FROM orgs WHERE org_type IS NULL OR trim(org_type)=''")) .scalar_one())
        forbidden_org_type_count = int(conn.execute(text("SELECT COUNT(*) FROM orgs WHERE lower(trim(coalesce(org_type,''))) IN ('one-off_event','one_off_event','other','poetry_readings')")).scalar_one())

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


def has_recent_running_discovery(max_age_minutes: int = 90) -> bool:
    run = get_latest_running_discovery_run()
    if not run:
        return False
    started = run.get("started_at")
    if isinstance(started, str):
        try:
            started_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
            if started_dt.tzinfo is None:
                started_dt = started_dt.replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - started_dt) < timedelta(minutes=max_age_minutes)
        except Exception:
            return True
    return True
