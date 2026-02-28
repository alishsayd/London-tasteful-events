"""SQLite database for candidate organisations."""

import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "orgs.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS orgs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                homepage TEXT,
                events_url TEXT,
                description TEXT,
                borough TEXT,
                category TEXT,
                source TEXT,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending', 'approved', 'rejected', 'maybe')),
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reviewed_at TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_orgs_status ON orgs(status);
            CREATE INDEX IF NOT EXISTS idx_orgs_borough ON orgs(borough);
            CREATE INDEX IF NOT EXISTS idx_orgs_category ON orgs(category);
        """)


def upsert_org(name, homepage=None, events_url=None, description=None,
               borough=None, category=None, source=None):
    """Insert an org if it doesn't already exist (by name + homepage)."""
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM orgs WHERE name = ? AND homepage = ?",
            (name, homepage)
        ).fetchone()
        if existing:
            return existing["id"]
        cursor = conn.execute(
            """INSERT INTO orgs (name, homepage, events_url, description,
                                borough, category, source)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (name, homepage, events_url, description, borough, category, source)
        )
        return cursor.lastrowid


def get_orgs(status=None, borough=None, category=None):
    with get_db() as conn:
        query = "SELECT * FROM orgs WHERE 1=1"
        params = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if borough:
            query += " AND borough = ?"
            params.append(borough)
        if category:
            query += " AND category = ?"
            params.append(category)
        query += " ORDER BY created_at DESC"
        return [dict(row) for row in conn.execute(query, params).fetchall()]


def update_org_status(org_id, status, notes=None):
    with get_db() as conn:
        conn.execute(
            """UPDATE orgs SET status = ?, notes = ?,
               reviewed_at = CURRENT_TIMESTAMP WHERE id = ?""",
            (status, notes, org_id)
        )


def update_org(org_id, **fields):
    allowed = {"name", "homepage", "events_url", "description",
               "borough", "category", "notes", "status"}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [org_id]
    with get_db() as conn:
        conn.execute(f"UPDATE orgs SET {set_clause} WHERE id = ?", values)


def get_stats():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM orgs GROUP BY status"
        ).fetchall()
        stats = {row["status"]: row["count"] for row in rows}
        stats["total"] = sum(stats.values())
        return stats
