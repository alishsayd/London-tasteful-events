"""Database helpers for candidate organisations and admin review workflow."""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import bindparam, create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "orgs.db"
BOOTSTRAP_ORGS_PATH = PROJECT_ROOT / "orgs_bootstrap.json"
LEGACY_BOOTSTRAP_ORGS_PATH = PROJECT_ROOT / "seed_data.json"


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


def _domain_from_url(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = urlparse(value)
        host = parsed.netloc or parsed.path
        host = host.lower().replace("www.", "")
        return host or None
    except Exception:
        return None


def _canonical_url(value: str | None) -> str | None:
    if not value:
        return None
    try:
        parsed = urlparse(value)
        host = (parsed.netloc or parsed.path).lower().replace("www.", "")
        path = (parsed.path or "").rstrip("/").lower()
        if not host:
            return None
        return f"{host}{path}" if path else host
    except Exception:
        return None


def _normalize_name(value: str | None) -> str:
    lowered = (value or "").strip().lower()
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    if lowered.startswith("the "):
        lowered = lowered[4:]
    return lowered


NAME_STOPWORDS = {
    "the",
    "london",
    "uk",
    "centre",
    "center",
    "cultural",
    "institute",
    "foundation",
    "house",
}


def _name_tokens(normalized_name: str | None) -> list[str]:
    tokens = [token for token in str(normalized_name or "").split(" ") if token]
    return [token for token in tokens if token not in NAME_STOPWORDS]


def _name_signature(normalized_name: str | None) -> str:
    tokens = _name_tokens(normalized_name)
    if not tokens:
        return ""
    return " ".join(tokens[:3])


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

    left_tokens = set(_name_tokens(left_norm))
    right_tokens = set(_name_tokens(right_norm))
    if not left_tokens or not right_tokens:
        return False

    intersection = left_tokens & right_tokens
    if len(intersection) >= 2:
        overlap = len(intersection) / min(len(left_tokens), len(right_tokens))
        if overlap >= 0.75:
            return True
    return False


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
            OR coalesce(consecutive_failures, 0) >= 3
            OR coalesce(consecutive_empty_extracts, 0) >= 3
        )
    )
    """


def _as_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, str) and value:
        normalized = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except Exception:
            return None
    return None


def _max_dt(left, right):
    left_dt = _as_datetime(left)
    right_dt = _as_datetime(right)
    if not left_dt:
        return right
    if not right_dt:
        return left
    return right if right_dt >= left_dt else left


def _description_template(row: dict[str, Any]) -> str:
    name = str(row.get("name") or "This venue").strip()
    borough = str(row.get("borough") or "").strip()
    category = str(row.get("category") or "cultural venue").strip()

    if borough and category:
        return f"{name} is a London {category} in {borough}."
    if category:
        return f"{name} is a London {category}."
    if borough:
        return f"{name} is a cultural venue in {borough}, London."
    return f"{name} is a London cultural venue."


def _best_description(current: str | None, candidate: str | None, row_fallback: dict[str, Any]) -> str:
    current_clean = (current or "").strip()
    candidate_clean = (candidate or "").strip()

    if current_clean and candidate_clean:
        return candidate_clean if len(candidate_clean) > len(current_clean) else current_clean
    if candidate_clean:
        return candidate_clean
    if current_clean:
        return current_clean
    return _description_template(row_fallback)


def _issue_priority(value: str | None) -> int:
    lookup = {"open": 4, "none": 3, "snoozed": 2, "resolved": 1}
    return lookup.get(str(value or "none"), 3)


CLEANUP_BLOCKED_DOMAIN_SUFFIXES = (
    "ianvisits.co.uk",
    "lectures.london",
    "github.com",
    "bsky.app",
    "bsky.social",
    "blueskyweb.xyz",
    "theguardian.com",
    "guardian.co.uk",
    "ft.com",
    "eventindustrynews.com",
    "culturecalling.com",
    "london.com",
)

CLEANUP_BAD_NAME_PHRASES = (
    "book your tickets",
    "courses and meetings",
    "support ianvisits",
    "subscribe to read",
    "events listings",
    "arts events listings",
    "overview",
)


def _domain_matches_suffix(value: str | None, suffixes: tuple[str, ...]) -> bool:
    host = str(value or "").strip().lower().replace("www.", "")
    if not host:
        return False
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in suffixes)


def _discovery_cleanup_reasons(row: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    name_value = str(row.get("name") or "").strip()
    name_key = _normalize_name(name_value)
    homepage = str(row.get("homepage") or "").strip()
    events_url = str(row.get("events_url") or "").strip()
    domain = (
        str(row.get("source_domain") or "").strip().lower()
        or str(_domain_from_url(homepage) or "").strip().lower()
        or str(_domain_from_url(events_url) or "").strip().lower()
    )

    if _domain_matches_suffix(domain, CLEANUP_BLOCKED_DOMAIN_SUFFIXES):
        reasons.append("non-entity source domain")

    if not name_key:
        reasons.append("missing org name")
    else:
        if any(phrase in name_key for phrase in CLEANUP_BAD_NAME_PHRASES):
            reasons.append("navigation/paywall or non-entity title")
        if name_key in {"github", "bluesky", "bluesky social", "guardian", "the guardian"}:
            reasons.append("platform/publisher title")
        if name_key.startswith(("book ", "support ", "subscribe ")):
            reasons.append("cta title")

    bad_path_hints = ("/article", "/news", "/blog", "/reviews", "/review", "/opinion", "/calendar/", "/event/")
    lower_home = homepage.lower()
    lower_events = events_url.lower()
    if any(hint in lower_home for hint in bad_path_hints) and "whatson" not in lower_home and "whats-on" not in lower_home:
        reasons.append("article/program homepage")
    if any(hint in lower_events for hint in bad_path_hints) and "whatson" not in lower_events and "whats-on" not in lower_events:
        reasons.append("article/program events url")

    # Keep only unique reasons while preserving insertion order.
    unique: list[str] = []
    for reason in reasons:
        if reason not in unique:
            unique.append(reason)
    return unique


@contextmanager
def get_db():
    with ENGINE.begin() as conn:
        yield conn


def _update_org_row(conn, org_id: int, updates: dict[str, Any]) -> None:
    if not updates:
        return
    set_clause = ", ".join(f"{key} = :{key}" for key in updates)
    params = {**updates, "org_id": int(org_id)}
    conn.execute(text(f"UPDATE orgs SET {set_clause} WHERE id = :org_id"), params)


def _enrich_row_fields(row: dict[str, Any]) -> dict[str, Any]:
    updates: dict[str, Any] = {}

    source_domain = row.get("source_domain") or _domain_from_url(row.get("homepage")) or _domain_from_url(row.get("events_url"))
    if source_domain and source_domain != row.get("source_domain"):
        updates["source_domain"] = source_domain

    description = _best_description(row.get("description"), None, row)
    if description != (row.get("description") or "").strip():
        updates["description"] = description

    issue_state = str(row.get("issue_state") or "none")
    events_url = str(row.get("events_url") or "").strip()
    if not events_url:
        if issue_state != "open":
            updates["issue_state"] = "open"
        if not str(row.get("review_needed_reason") or "").strip():
            updates["review_needed_reason"] = "Missing events URL"

    return updates


def _merge_group(conn, group_rows: list[dict[str, Any]]) -> None:
    if len(group_rows) <= 1:
        only = group_rows[0]
        updates = _enrich_row_fields(only)
        _update_org_row(conn, int(only["id"]), updates)
        return

    def score(row: dict[str, Any]) -> int:
        desc_len = len(str(row.get("description") or "").strip())
        score_value = desc_len
        if str(row.get("events_url") or "").strip():
            score_value += 120
        if str(row.get("homepage") or "").strip():
            score_value += 40
        if str(row.get("borough") or "").strip():
            score_value += 20
        if str(row.get("category") or "").strip():
            score_value += 20
        if bool(row.get("active", True)):
            score_value += 10
        return score_value

    ordered = sorted(group_rows, key=lambda item: (-score(item), int(item["id"])))
    primary = dict(ordered[0])
    duplicate_ids = [int(item["id"]) for item in ordered[1:]]

    merged = dict(primary)

    for candidate in ordered[1:]:
        for key in ("homepage", "events_url", "borough", "category", "source", "source_domain", "review_needed_reason"):
            if not str(merged.get(key) or "").strip() and str(candidate.get(key) or "").strip():
                merged[key] = candidate.get(key)

        merged["description"] = _best_description(merged.get("description"), candidate.get("description"), merged)

        if not str(merged.get("notes") or "").strip() and str(candidate.get("notes") or "").strip():
            merged["notes"] = candidate.get("notes")

        merged["active"] = bool(merged.get("active", True) or candidate.get("active", True))
        merged["crawl_paused"] = bool(merged.get("crawl_paused", False) and candidate.get("crawl_paused", False))

        merged["consecutive_failures"] = max(int(merged.get("consecutive_failures") or 0), int(candidate.get("consecutive_failures") or 0))
        merged["consecutive_empty_extracts"] = max(
            int(merged.get("consecutive_empty_extracts") or 0), int(candidate.get("consecutive_empty_extracts") or 0)
        )

        merged["last_crawled_at"] = _max_dt(merged.get("last_crawled_at"), candidate.get("last_crawled_at"))
        merged["last_successful_event_extract_at"] = _max_dt(
            merged.get("last_successful_event_extract_at"), candidate.get("last_successful_event_extract_at")
        )

        current_state = str(merged.get("issue_state") or "none")
        candidate_state = str(candidate.get("issue_state") or "none")
        if _issue_priority(candidate_state) > _issue_priority(current_state):
            merged["issue_state"] = candidate_state

    if not str(merged.get("source_domain") or "").strip():
        merged["source_domain"] = _domain_from_url(merged.get("homepage")) or _domain_from_url(merged.get("events_url"))

    merged["description"] = _best_description(merged.get("description"), None, merged)

    if not str(merged.get("events_url") or "").strip():
        merged["issue_state"] = "open"
        if not str(merged.get("review_needed_reason") or "").strip():
            merged["review_needed_reason"] = "Missing events URL"

    updates = {
        "homepage": merged.get("homepage"),
        "events_url": merged.get("events_url"),
        "description": merged.get("description"),
        "borough": merged.get("borough"),
        "category": merged.get("category"),
        "source": merged.get("source"),
        "source_domain": merged.get("source_domain"),
        "notes": merged.get("notes"),
        "active": merged.get("active", True),
        "crawl_paused": merged.get("crawl_paused", False),
        "last_crawled_at": merged.get("last_crawled_at"),
        "last_successful_event_extract_at": merged.get("last_successful_event_extract_at"),
        "consecutive_failures": merged.get("consecutive_failures", 0),
        "consecutive_empty_extracts": merged.get("consecutive_empty_extracts", 0),
        "issue_state": merged.get("issue_state") or "none",
        "review_needed_reason": merged.get("review_needed_reason"),
    }

    _update_org_row(conn, int(primary["id"]), updates)

    if duplicate_ids:
        statement = text("DELETE FROM orgs WHERE id IN :ids").bindparams(bindparam("ids", expanding=True))
        conn.execute(statement, {"ids": duplicate_ids})


def _dedupe_and_enrich() -> None:
    with get_db() as conn:
        rows = [dict(item) for item in conn.execute(text("SELECT * FROM orgs ORDER BY id ASC")).mappings().all()]
        if not rows:
            return

        parent: dict[int, int] = {}
        seen: dict[tuple, int] = {}

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra = find(a)
            rb = find(b)
            if ra != rb:
                parent[rb] = ra

        for row in rows:
            row_id = int(row["id"])
            parent[row_id] = row_id

        for row in rows:
            row_id = int(row["id"])
            name_key = _normalize_name(row.get("name"))
            signature = _name_signature(name_key)
            domain = row.get("source_domain") or _domain_from_url(row.get("homepage")) or _domain_from_url(row.get("events_url"))
            homepage_key = _canonical_url(row.get("homepage"))
            events_key = _canonical_url(row.get("events_url"))

            keys: list[tuple] = []
            if domain and name_key:
                keys.append(("domain_name", domain, name_key))
            if domain and signature:
                keys.append(("domain_signature", domain, signature))
            if homepage_key:
                keys.append(("homepage", homepage_key))
            if events_key:
                keys.append(("events", events_key))
            if not domain and name_key:
                keys.append(("name_only", name_key))

            for key in keys:
                existing = seen.get(key)
                if existing is None:
                    seen[key] = row_id
                else:
                    union(row_id, existing)

        groups: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            root = find(int(row["id"]))
            groups.setdefault(root, []).append(row)

        for group_rows in groups.values():
            _merge_group(conn, group_rows)


def init_db() -> None:
    """Create schema and bootstrap initial org data when database is empty."""
    org_id_def = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    strategy_id_def = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
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
                    category TEXT,
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
                    review_needed_reason TEXT
                )
                """
            )
        )

        if IS_POSTGRES:
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS crawl_paused BOOLEAN NOT NULL DEFAULT FALSE"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS last_crawled_at TIMESTAMP"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS last_successful_event_extract_at TIMESTAMP"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS consecutive_failures INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS issue_state TEXT NOT NULL DEFAULT 'none'"))
            conn.execute(text("ALTER TABLE orgs ADD COLUMN IF NOT EXISTS review_needed_reason TEXT"))
        else:
            for statement in [
                "ALTER TABLE orgs ADD COLUMN active BOOLEAN NOT NULL DEFAULT 1",
                "ALTER TABLE orgs ADD COLUMN crawl_paused BOOLEAN NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN last_crawled_at TIMESTAMP",
                "ALTER TABLE orgs ADD COLUMN last_successful_event_extract_at TIMESTAMP",
                "ALTER TABLE orgs ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN consecutive_empty_extracts INTEGER NOT NULL DEFAULT 0",
                "ALTER TABLE orgs ADD COLUMN issue_state TEXT NOT NULL DEFAULT 'none'",
                "ALTER TABLE orgs ADD COLUMN review_needed_reason TEXT",
            ]:
                try:
                    conn.execute(text(statement))
                except Exception:
                    pass

        if IS_POSTGRES:
            conn.execute(text("UPDATE orgs SET active = TRUE WHERE active IS NULL"))
            conn.execute(text("UPDATE orgs SET crawl_paused = FALSE WHERE crawl_paused IS NULL"))
        else:
            conn.execute(text("UPDATE orgs SET active = 1 WHERE active IS NULL"))
            conn.execute(text("UPDATE orgs SET crawl_paused = 0 WHERE crawl_paused IS NULL"))

        conn.execute(text("UPDATE orgs SET consecutive_failures = 0 WHERE consecutive_failures IS NULL"))
        conn.execute(text("UPDATE orgs SET consecutive_empty_extracts = 0 WHERE consecutive_empty_extracts IS NULL"))
        conn.execute(text("UPDATE orgs SET issue_state = 'none' WHERE issue_state IS NULL"))

        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_status ON orgs(status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_borough ON orgs(borough)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_category ON orgs(category)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_source_domain ON orgs(source_domain)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_active ON orgs(active, crawl_paused)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_orgs_issue_state ON orgs(issue_state)"))

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS strategies (
                    id {strategy_id_def},
                    text TEXT NOT NULL,
                    active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

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

        # Legacy migration: keep previously logged strategy notes.
        try:
            conn.execute(
                text(
                    """
                    INSERT INTO strategies (text, active, created_at)
                    SELECT legacy.text, legacy.active, legacy.created_at
                    FROM codex_strategies AS legacy
                    LEFT JOIN strategies AS current
                      ON current.text = legacy.text AND current.created_at = legacy.created_at
                    WHERE current.id IS NULL
                    """
                )
            )
        except Exception:
            pass

    _bootstrap_if_empty()
    _dedupe_and_enrich()


def _resolve_bootstrap_file() -> Path | None:
    for candidate in (BOOTSTRAP_ORGS_PATH, LEGACY_BOOTSTRAP_ORGS_PATH):
        if candidate.exists():
            return candidate
    return None


def _bootstrap_if_empty() -> None:
    bootstrap_enabled = os.getenv("AUTO_BOOTSTRAP_ORGS", os.getenv("AUTO_SEED_ORGS", "true")).strip().lower() not in {
        "0",
        "false",
        "no",
    }
    bootstrap_file = _resolve_bootstrap_file()
    if not bootstrap_enabled or bootstrap_file is None:
        return

    stats = get_stats()
    if stats.get("total", 0) > 0:
        return

    with bootstrap_file.open() as handle:
        payload = json.load(handle)

    if not isinstance(payload, list):
        return

    for item in payload:
        if not isinstance(item, dict) or not item.get("name"):
            continue
        upsert_org(
            name=item.get("name"),
            homepage=item.get("homepage"),
            events_url=item.get("events_url"),
            description=item.get("description"),
            borough=item.get("borough"),
            category=item.get("category"),
            source=item.get("source", "bootstrap_file"),
        )


def upsert_org(
    name: str,
    homepage: str | None = None,
    events_url: str | None = None,
    description: str | None = None,
    borough: str | None = None,
    category: str | None = None,
    source: str | None = None,
) -> int:
    """Insert an org, or merge into an existing matching org."""
    if not name:
        raise ValueError("name is required")

    source_domain = _domain_from_url(homepage) or _domain_from_url(events_url)
    norm_name = _normalize_name(name)

    with get_db() as conn:
        # PostgreSQL (psycopg3) can reject ":param IS NOT NULL" checks as ambiguous
        # when the same bind is also used in equality predicates. Build this predicate
        # dynamically to avoid typing ambiguity and keep SQLite behavior identical.
        where_clauses = [
            "lower(name) = lower(:name)",
            "coalesce(homepage, '') = coalesce(:homepage, '')",
            "coalesce(events_url, '') = coalesce(:events_url, '')",
        ]
        params = {"name": name, "homepage": homepage, "events_url": events_url}
        if source_domain is not None:
            where_clauses.append("source_domain = :source_domain")
            params["source_domain"] = source_domain

        query = f"""
            SELECT * FROM orgs
            WHERE {' OR '.join(where_clauses)}
            ORDER BY id ASC
        """
        candidates = conn.execute(text(query), params).mappings().all()

        existing = None
        for row in candidates:
            row_name = _normalize_name(row.get("name"))
            row_domain = row.get("source_domain") or _domain_from_url(row.get("homepage")) or _domain_from_url(row.get("events_url"))
            name_matches = bool(row_name and norm_name and _names_likely_same_entity(row_name, norm_name))
            if name_matches:
                if row_domain and source_domain and row_domain == source_domain:
                    existing = dict(row)
                    break
                if not row_domain or not source_domain:
                    existing = dict(row)
                    break

        if existing:
            updates: dict[str, Any] = {}

            if not str(existing.get("homepage") or "").strip() and str(homepage or "").strip():
                updates["homepage"] = homepage
            if not str(existing.get("events_url") or "").strip() and str(events_url or "").strip():
                updates["events_url"] = events_url
            if not str(existing.get("borough") or "").strip() and str(borough or "").strip():
                updates["borough"] = borough
            if not str(existing.get("category") or "").strip() and str(category or "").strip():
                updates["category"] = category
            if not str(existing.get("source") or "").strip() and str(source or "").strip():
                updates["source"] = source

            best_description = _best_description(
                existing.get("description"), description, {**existing, "name": name, "borough": borough, "category": category}
            )
            if best_description != (existing.get("description") or "").strip():
                updates["description"] = best_description

            next_domain = source_domain or existing.get("source_domain") or _domain_from_url(existing.get("homepage")) or _domain_from_url(existing.get("events_url"))
            if next_domain and next_domain != existing.get("source_domain"):
                updates["source_domain"] = next_domain

            if updates:
                _update_org_row(conn, int(existing["id"]), updates)

            return int(existing["id"])

        params = {
            "name": name,
            "homepage": homepage,
            "events_url": events_url,
            "description": _best_description(None, description, {"name": name, "borough": borough, "category": category}),
            "borough": borough,
            "category": category,
            "source": source,
            "source_domain": source_domain,
        }

        if IS_POSTGRES:
            inserted = conn.execute(
                text(
                    """
                    INSERT INTO orgs (name, homepage, events_url, description, borough, category, source, source_domain)
                    VALUES (:name, :homepage, :events_url, :description, :borough, :category, :source, :source_domain)
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
                    INSERT INTO orgs (name, homepage, events_url, description, borough, category, source, source_domain)
                    VALUES (:name, :homepage, :events_url, :description, :borough, :category, :source, :source_domain)
                    """
                ),
                params,
            )
            fallback = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if fallback:
                return int(fallback["id"])

    raise RuntimeError("Failed to upsert org")


def get_org(org_id: int) -> dict[str, Any] | None:
    with get_db() as conn:
        row = conn.execute(text("SELECT * FROM orgs WHERE id = :org_id"), {"org_id": org_id}).mappings().first()
        return dict(row) if row else None


def update_org(org_id: int, **fields: Any) -> None:
    allowed = {
        "name",
        "homepage",
        "events_url",
        "description",
        "borough",
        "category",
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

    if "homepage" in updates or "events_url" in updates or "source_domain" not in updates:
        homepage = updates.get("homepage")
        events_url = updates.get("events_url")
        if homepage is None or events_url is None:
            current = get_org(org_id) or {}
            homepage = homepage if homepage is not None else current.get("homepage")
            events_url = events_url if events_url is not None else current.get("events_url")
        updates["source_domain"] = _domain_from_url(homepage) or _domain_from_url(events_url)

    if "description" in updates:
        current = get_org(org_id) or {}
        updates["description"] = _best_description(current.get("description"), updates.get("description"), {**current, **updates})

    set_clause = ", ".join(f"{key} = :{key}" for key in updates)
    params = {**updates, "org_id": org_id}

    if "status" in updates:
        set_clause += ", reviewed_at = CASE WHEN :status = 'pending' THEN NULL ELSE CURRENT_TIMESTAMP END"

    with get_db() as conn:
        conn.execute(text(f"UPDATE orgs SET {set_clause} WHERE id = :org_id"), params)


def get_stats() -> dict[str, int]:
    with get_db() as conn:
        rows = conn.execute(text("SELECT status, COUNT(*) AS count FROM orgs GROUP BY status")).mappings().all()
        active_total = int(conn.execute(text(f"SELECT COUNT(*) FROM orgs WHERE {_active_filter_sql()}")) .scalar_one())
        queue_total = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) FROM orgs
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
                    SELECT COUNT(*) FROM orgs
                    WHERE {_active_filter_sql()}
                      AND coalesce(issue_state, 'none') = 'open'
                    """
                )
            ).scalar_one()
        )

    stats: dict[str, int] = {str(row["status"]): int(row["count"]) for row in rows}
    stats["total"] = sum(stats.values())
    stats["active_total"] = active_total
    stats["queue_total"] = queue_total
    stats["open_issues"] = open_issues
    return stats


def get_review_queue_orgs(limit: int = 250) -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT * FROM orgs
                WHERE {_active_filter_sql()}
                  AND coalesce(issue_state, 'none') <> 'snoozed'
                  AND {_queue_condition_sql()}
                ORDER BY
                    CASE
                        WHEN coalesce(issue_state, 'none') = 'open' THEN 0
                        WHEN events_url IS NULL OR trim(events_url) = '' THEN 1
                        WHEN coalesce(consecutive_failures, 0) >= 3 THEN 2
                        WHEN coalesce(consecutive_empty_extracts, 0) >= 3 THEN 3
                        ELSE 4
                    END,
                    coalesce(consecutive_failures, 0) DESC,
                    coalesce(consecutive_empty_extracts, 0) DESC,
                    created_at DESC,
                    id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()
        return [dict(row) for row in rows]


def get_active_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    query = f"""
        SELECT * FROM orgs
        WHERE {_active_filter_sql()}
        ORDER BY created_at DESC, id DESC
    """
    params: dict[str, Any] = {}
    if limit:
        query += " LIMIT :limit"
        params["limit"] = int(limit)

    with get_db() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        return [dict(row) for row in rows]


def get_public_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    query = f"""
        SELECT * FROM orgs
        WHERE {_active_filter_sql()}
          AND coalesce(issue_state, 'none') IN ('none', 'resolved')
          AND coalesce(status, 'pending') <> 'rejected'
        ORDER BY category ASC, borough ASC, name ASC, created_at DESC, id DESC
    """
    params: dict[str, Any] = {}
    if limit:
        query += " LIMIT :limit"
        params["limit"] = int(limit)

    with get_db() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        return [dict(row) for row in rows]


def cleanup_recent_discovery_garbage(days: int = 7, dry_run: bool = False, limit: int = 1000) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days)))

    with get_db() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM orgs
                WHERE source = 'auto_discovery'
                ORDER BY created_at DESC, id DESC
                LIMIT :limit
                """
            ),
            {"limit": int(limit)},
        ).mappings().all()

    scoped_rows: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        created_at = _as_datetime(row.get("created_at"))
        if created_at and created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        if created_at and created_at < cutoff:
            continue
        scoped_rows.append(row)

    flagged: list[dict[str, Any]] = []
    for row in scoped_rows:
        reasons = _discovery_cleanup_reasons(row)
        if not reasons:
            continue
        flagged.append(
            {
                "id": int(row["id"]),
                "name": row.get("name"),
                "source_domain": row.get("source_domain"),
                "reasons": reasons,
            }
        )

    updated = 0
    if not dry_run:
        for item in flagged:
            org = get_org(int(item["id"])) or {}
            reason_text = "; ".join(item["reasons"][:3])
            note = str(org.get("notes") or "").strip()
            cleanup_note = f"Auto-cleanup: {reason_text}"
            merged_note = cleanup_note if not note else f"{note}\n{cleanup_note}"
            update_org(
                int(item["id"]),
                status="rejected",
                active=False,
                crawl_paused=True,
                issue_state="resolved",
                review_needed_reason=f"Auto-cleanup: {reason_text}",
                notes=merged_note[:2000],
            )
            updated += 1

        _dedupe_and_enrich()

    return {
        "ok": True,
        "dry_run": bool(dry_run),
        "days": int(days),
        "scanned": len(scoped_rows),
        "flagged": len(flagged),
        "updated": updated,
        "sample": flagged[:30],
    }


def get_strategies() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text("SELECT id, text, active, created_at FROM strategies ORDER BY created_at DESC, id DESC")
        ).mappings().all()
    return [
        {
            "id": int(row["id"]),
            "text": row["text"],
            "active": bool(row["active"]),
            "created_at": row["created_at"],
        }
        for row in rows
    ]


def add_strategy(text_value: str, active: bool = True) -> int:
    with get_db() as conn:
        if IS_POSTGRES:
            inserted = conn.execute(
                text(
                    """
                    INSERT INTO strategies (text, active)
                    VALUES (:text, :active)
                    RETURNING id
                    """
                ),
                {"text": text_value, "active": active},
            ).mappings().first()
            if inserted:
                return int(inserted["id"])
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO strategies (text, active)
                    VALUES (:text, :active)
                    """
                ),
                {"text": text_value, "active": active},
            )
            fallback = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if fallback:
                return int(fallback["id"])

    raise RuntimeError("Failed to insert strategy")


def set_strategy_active(strategy_id: int, active: bool) -> None:
    with get_db() as conn:
        conn.execute(
            text("UPDATE strategies SET active = :active WHERE id = :strategy_id"),
            {"active": active, "strategy_id": strategy_id},
        )


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

    started_at = _as_datetime(row.get("started_at"))
    if not started_at:
        return True
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    return (datetime.now(timezone.utc) - started_at) < timedelta(minutes=max_age_minutes)


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
            fallback = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if fallback:
                return int(fallback["id"])

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
    error_value = (error or "").strip()[:4000] or None

    set_clauses = [
        "status = :status",
        "result_count = :result_count",
        "upserted_count = :upserted_count",
        "error = :error",
        "finished_at = CURRENT_TIMESTAMP",
    ]
    params = {
        "run_id": int(run_id),
        "status": status_value,
        "result_count": int(result_count),
        "upserted_count": int(upserted_count),
        "error": error_value,
    }
    if details_json is not None:
        set_clauses.insert(4, "details_json = :details_json")
        params["details_json"] = details_json

    with get_db() as conn:
        conn.execute(
            text(
                f"""
                UPDATE discovery_runs
                SET {", ".join(set_clauses)}
                WHERE id = :run_id
                """
            ),
            params,
        )


def _decode_discovery_row(row: dict[str, Any]) -> dict[str, Any]:
    decoded = dict(row)
    raw_details = decoded.get("details_json")
    parsed_details = None
    if isinstance(raw_details, str) and raw_details.strip():
        try:
            parsed_details = json.loads(raw_details)
        except Exception:
            parsed_details = None

    decoded["details"] = parsed_details
    decoded.pop("details_json", None)
    return decoded


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
