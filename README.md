# London Tasteful Events

Stateful admin backend for curating London organizations before event extraction.

## Runtime

- App: Flask + SQLAlchemy (`seed_orgs`)
- Database: PostgreSQL in production (`DATABASE_URL`), SQLite locally by default
- Deployment: Render (`render.yaml`)

## Local Development

1. `python3 -m venv .venv && source .venv/bin/activate`
2. `pip install -r requirements.txt`
3. `python -m seed_orgs.admin`
4. Open `http://127.0.0.1:5000/`

Default local DB file: `orgs.db` (created automatically).

## Environment Variables

- `DATABASE_URL`: required in production
- `AUTO_SEED_ORGS`: defaults to `true`; seeds from `seed_data.json` when DB is empty

## Deploy (Render)

- Blueprint: `render.yaml`
- Web service: `london-tasteful-events-admin`
- Health check: `GET /healthz`
- Start command: `gunicorn seed_orgs.wsgi:app --bind 0.0.0.0:$PORT --workers 2 --threads 4`

## API (Current)

- `GET /api/codex/state`
- `POST /api/codex/review/<org_id>`
- `GET|POST /api/codex/strategies`
- `PATCH /api/codex/strategies/<strategy_id>`
- `POST /api/orgs`
- `POST /api/orgs/bulk`
- `GET /api/stats`
- `GET /export`

## Collaboration

See [`docs/parallel-collaboration.md`](docs/parallel-collaboration.md) for branch naming and file ownership boundaries to reduce merge conflicts when Codex and Claude work in parallel.
