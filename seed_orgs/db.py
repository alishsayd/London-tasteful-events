"""Database helpers for candidate organisations and codex review workflow."""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import bindparam, create_engine, text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "orgs.db"
SEED_DATA_PATH = PROJECT_ROOT / "seed_data.json"


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


@contextmanager
def get_db():
    with ENGINE.begin() as conn:
        yield conn


def _active_filter_sql() -> str:
    if IS_POSTGRES:
        return "active IS TRUE AND (crawl_paused IS FALSE OR crawl_paused IS NULL)"
    return "coalesce(active, 1) = 1 AND coalesce(crawl_paused, 0) = 0"


def _queue_condition_sql() -> str:
    return """
    (
        issue_state = 'open'
        OR events_url IS NULL
        OR trim(events_url) = ''
        OR coalesce(consecutive_failures, 0) >= 3
        OR coalesce(consecutive_empty_extracts, 0) >= 3
    )
    """


def init_db() -> None:
    """Create schema and seed initial org data when database is empty."""
    org_id_def = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    strategy_id_def = "BIGSERIAL PRIMARY KEY" if IS_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"

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
            # SQLite path for local/dev only; most environments here are Postgres on Render.
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
                CREATE TABLE IF NOT EXISTS codex_strategies (
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
                """
                CREATE TABLE IF NOT EXISTS codex_batch_state (
                    id INTEGER PRIMARY KEY,
                    batch_number INTEGER NOT NULL DEFAULT 1,
                    active_batch_ids TEXT NOT NULL DEFAULT '[]',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )

        if IS_POSTGRES:
            conn.execute(
                text(
                    """
                    INSERT INTO codex_batch_state (id, batch_number, active_batch_ids)
                    VALUES (1, 1, '[]')
                    ON CONFLICT (id) DO NOTHING
                    """
                )
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT OR IGNORE INTO codex_batch_state (id, batch_number, active_batch_ids)
                    VALUES (1, 1, '[]')
                    """
                )
            )

    _seed_if_empty()


def _seed_if_empty() -> None:
    seed_enabled = os.getenv("AUTO_SEED_ORGS", "true").strip().lower() not in {"0", "false", "no"}
    if not seed_enabled or not SEED_DATA_PATH.exists():
        return

    stats = get_stats()
    if stats.get("total", 0) > 0:
        return

    with SEED_DATA_PATH.open() as handle:
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
            source=item.get("source", "seed_data"),
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
    """Insert an org if it doesn't already exist (by normalized name+homepage)."""
    if not name:
        raise ValueError("name is required")

    with get_db() as conn:
        existing = conn.execute(
            text(
                """
                SELECT id FROM orgs
                WHERE lower(name) = lower(:name)
                  AND coalesce(homepage, '') = coalesce(:homepage, '')
                LIMIT 1
                """
            ),
            {"name": name, "homepage": homepage},
        ).mappings().first()

        if existing:
            return int(existing["id"])

        params = {
            "name": name,
            "homepage": homepage,
            "events_url": events_url,
            "description": description,
            "borough": borough,
            "category": category,
            "source": source,
            "source_domain": _domain_from_url(homepage) or _domain_from_url(events_url),
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


def get_orgs(status: str | None = None, borough: str | None = None, category: str | None = None) -> list[dict[str, Any]]:
    with get_db() as conn:
        query = "SELECT * FROM orgs WHERE 1=1"
        params: dict[str, Any] = {}

        if status:
            query += " AND status = :status"
            params["status"] = status
        if borough:
            query += " AND borough = :borough"
            params["borough"] = borough
        if category:
            query += " AND category = :category"
            params["category"] = category

        query += " ORDER BY created_at DESC, id DESC"
        rows = conn.execute(text(query), params).mappings().all()
        return [dict(row) for row in rows]


def get_orgs_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []

    normalized = [int(item) for item in ids]
    statement = text("SELECT * FROM orgs WHERE id IN :ids").bindparams(bindparam("ids", expanding=True))

    with get_db() as conn:
        rows = conn.execute(statement, {"ids": normalized}).mappings().all()

    by_id = {int(row["id"]): dict(row) for row in rows}
    return [by_id[item] for item in normalized if item in by_id]


def get_pending_orgs(limit: int | None = None) -> list[dict[str, Any]]:
    with get_db() as conn:
        query = "SELECT * FROM orgs WHERE status = 'pending' ORDER BY created_at DESC, id DESC"
        params: dict[str, Any] = {}
        if limit:
            query += " LIMIT :limit"
            params["limit"] = int(limit)
        rows = conn.execute(text(query), params).mappings().all()
        return [dict(row) for row in rows]


def update_org_status(org_id: int, status: str, notes: str | None = None) -> None:
    with get_db() as conn:
        conn.execute(
            text(
                """
                UPDATE orgs
                SET status = :status,
                    notes = :notes,
                    reviewed_at = CASE WHEN :status = 'pending' THEN NULL ELSE CURRENT_TIMESTAMP END
                WHERE id = :org_id
                """
            ),
            {"status": status, "notes": notes, "org_id": org_id},
        )


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
    }
    updates = {key: value for key, value in fields.items() if key in allowed}
    if not updates:
        return

    if "homepage" in updates or "events_url" in updates:
        homepage = updates.get("homepage")
        events_url = updates.get("events_url")
        if homepage is None or events_url is None:
            current = get_org(org_id) or {}
            homepage = homepage if homepage is not None else current.get("homepage")
            events_url = events_url if events_url is not None else current.get("events_url")
        updates["source_domain"] = _domain_from_url(homepage) or _domain_from_url(events_url)

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


def get_codex_queue_orgs(limit: int = 250) -> list[dict[str, Any]]:
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


def get_codex_strategies() -> list[dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            text("SELECT id, text, active, created_at FROM codex_strategies ORDER BY created_at DESC, id DESC")
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


def add_codex_strategy(text_value: str, active: bool = True) -> int:
    with get_db() as conn:
        if IS_POSTGRES:
            inserted = conn.execute(
                text(
                    """
                    INSERT INTO codex_strategies (text, active)
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
                    INSERT INTO codex_strategies (text, active)
                    VALUES (:text, :active)
                    """
                ),
                {"text": text_value, "active": active},
            )
            fallback = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
            if fallback:
                return int(fallback["id"])

    raise RuntimeError("Failed to insert strategy")


def set_codex_strategy_active(strategy_id: int, active: bool) -> None:
    with get_db() as conn:
        conn.execute(
            text("UPDATE codex_strategies SET active = :active WHERE id = :strategy_id"),
            {"active": active, "strategy_id": strategy_id},
        )


def get_codex_batch_state() -> dict[str, Any]:
    with get_db() as conn:
        row = conn.execute(
            text("SELECT batch_number, active_batch_ids, updated_at FROM codex_batch_state WHERE id = 1")
        ).mappings().first()

    if not row:
        return {"batch_number": 1, "active_batch_ids": [], "updated_at": None}

    active_batch_ids_raw = row["active_batch_ids"] or "[]"
    try:
        ids = json.loads(active_batch_ids_raw)
    except Exception:
        ids = []

    normalized_ids = [int(item) for item in ids if isinstance(item, int) or (isinstance(item, str) and item.isdigit())]
    return {
        "batch_number": int(row["batch_number"]),
        "active_batch_ids": normalized_ids,
        "updated_at": row["updated_at"],
    }


def save_codex_batch_state(batch_number: int, active_batch_ids: list[int]) -> None:
    ids_json = json.dumps([int(item) for item in active_batch_ids])
    with get_db() as conn:
        conn.execute(
            text(
                """
                UPDATE codex_batch_state
                SET batch_number = :batch_number,
                    active_batch_ids = :active_batch_ids,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
                """
            ),
            {"batch_number": int(batch_number), "active_batch_ids": ids_json},
        )
