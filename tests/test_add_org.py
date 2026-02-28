"""Regression test: POST /api/orgs with both homepage and events_url must not 500."""

import os
import sys
import threading

# Use a throwaway SQLite DB for testing
os.environ.pop("DATABASE_URL", None)

# Force fresh module state
TEST_DB = os.path.join(os.path.dirname(__file__), ".test_orgs.db")
os.environ["DATABASE_URL"] = ""  # triggers SQLite default

import importlib
import app.db as db

# Point at test DB
db.DEFAULT_SQLITE_PATH = TEST_DB
db.DATABASE_URL = f"sqlite:///{TEST_DB}"
db.ENGINE = db.create_engine(db.DATABASE_URL, future=True, pool_pre_ping=True)
db.IS_POSTGRES = False
db.init_db()

from app.admin import app

client = app.test_client()


def teardown_module():
    try:
        os.remove(TEST_DB)
    except OSError:
        pass


def test_add_org_with_both_urls_returns_200():
    """The exact bug: POST with both homepage and events_url should not 500."""
    resp = client.post("/api/orgs", json={
        "name": "Japan House",
        "homepage": "https://www.japanhouselondon.uk",
        "events_url": "https://www.japanhouselondon.uk/whats-on",
        "borough": "Kensington and Chelsea",
        "category": "cultural centre",
        "description": "Cultural centre for Japanese arts and design.",
    })
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.get_data(as_text=True)}"
    data = resp.get_json()
    assert data["ok"] is True
    assert "id" in data


def test_add_org_with_homepage_only_returns_200():
    resp = client.post("/api/orgs", json={
        "name": "Test Gallery",
        "homepage": "https://example.com",
        "borough": "Hackney",
        "category": "gallery",
    })
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.get_data(as_text=True)}"


def test_add_org_duplicate_returns_200():
    """Adding the same org twice (upsert path) should also succeed."""
    for _ in range(2):
        resp = client.post("/api/orgs", json={
            "name": "Japan House",
            "homepage": "https://www.japanhouselondon.uk",
            "events_url": "https://www.japanhouselondon.uk/whats-on",
            "borough": "Kensington and Chelsea",
            "category": "cultural centre",
        })
        assert resp.status_code == 200


def test_add_org_missing_name_returns_400():
    resp = client.post("/api/orgs", json={"homepage": "https://example.com"})
    assert resp.status_code == 400


def test_healthz_does_not_500():
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_state_endpoint_after_add():
    """The refresh call that happens in the UI after add should not 500."""
    client.post("/api/orgs", json={
        "name": "Refresh Test Org",
        "homepage": "https://refresh-test.example.com",
        "events_url": "https://refresh-test.example.com/events",
        "borough": "Camden",
        "category": "gallery",
    })
    resp = client.get("/api/admin/state")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"


def test_init_db_is_idempotent():
    """init_db() called multiple times must not cause errors or re-run heavy ops."""
    db._INIT_DONE = False
    db.init_db()
    assert db._INIT_DONE is True
    # Second call should be a no-op
    db.init_db()
    assert db._INIT_DONE is True


if __name__ == "__main__":
    passed = 0
    failed = 0
    for name, func in list(globals().items()):
        if name.startswith("test_") and callable(func):
            try:
                func()
                print(f"  PASS  {name}")
                passed += 1
            except Exception as e:
                print(f"  FAIL  {name}: {e}")
                failed += 1
    print(f"\n{passed} passed, {failed} failed")
    teardown_module()
    sys.exit(1 if failed else 0)
