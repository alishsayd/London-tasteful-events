"""Regression tests for discovery run DB helpers.

Covers the AmbiguousParameter bug in finish_discovery_run where a CASE WHEN
:param IS NULL pattern caused psycopg to fail with 'could not determine data
type of parameter $5'.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

# Force SQLite for testing before any app imports.
_tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_tmp.close()
os.environ["DATABASE_URL"] = f"sqlite:///{_tmp.name}"

from app import db  # noqa: E402


class TestFinishDiscoveryRun(unittest.TestCase):
    """Ensure finish_discovery_run works with and without details."""

    @classmethod
    def setUpClass(cls):
        db.init_db()

    def _create_run(self, details_json: str = "{}") -> int:
        with db.get_db() as conn:
            conn.execute(
                db.text(
                    "INSERT INTO discovery_runs (trigger, status, query_count, details_json) "
                    "VALUES ('manual', 'running', 1, :dj)"
                ),
                {"dj": details_json},
            )
            row = conn.execute(
                db.text("SELECT MAX(id) AS mid FROM discovery_runs")
            ).mappings().first()
            return int(row["mid"])

    def _get_run(self, run_id: int) -> dict:
        with db.get_db() as conn:
            row = conn.execute(
                db.text("SELECT * FROM discovery_runs WHERE id = :id"),
                {"id": run_id},
            ).mappings().first()
            return dict(row) if row else {}

    def test_finish_with_details_none_preserves_existing(self):
        """When details=None, the existing details_json must not be overwritten."""
        original = {"queries": ["test query"]}
        run_id = self._create_run(json.dumps(original))

        db.finish_discovery_run(
            run_id=run_id,
            status="success",
            result_count=5,
            upserted_count=2,
            details=None,
        )

        row = self._get_run(run_id)
        self.assertEqual(row["status"], "success")
        self.assertEqual(row["result_count"], 5)
        self.assertEqual(row["upserted_count"], 2)
        self.assertIsNotNone(row["finished_at"])
        # details_json must be unchanged
        self.assertEqual(json.loads(row["details_json"]), original)

    def test_finish_with_details_updates_value(self):
        """When details is provided, details_json must be updated."""
        run_id = self._create_run('{"old": true}')
        new_details = {"candidate_count": 10, "upserted_count": 3}

        db.finish_discovery_run(
            run_id=run_id,
            status="success",
            result_count=10,
            upserted_count=3,
            details=new_details,
        )

        row = self._get_run(run_id)
        self.assertEqual(json.loads(row["details_json"]), new_details)

    def test_finish_with_error(self):
        """Error string is stored and status set to failed."""
        run_id = self._create_run()

        db.finish_discovery_run(
            run_id=run_id,
            status="failed",
            result_count=0,
            upserted_count=0,
            error="Something went wrong",
        )

        row = self._get_run(run_id)
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["error"], "Something went wrong")

    @classmethod
    def tearDownClass(cls):
        try:
            os.unlink(_tmp.name)
        except OSError:
            pass


if __name__ == "__main__":
    unittest.main()
