# London Tasteful Events

Stateful admin backend for curating London organizations before event extraction.

## Runtime

- App: Flask + SQLAlchemy (`app`)
- Database: PostgreSQL in production (`DATABASE_URL`), SQLite locally by default
- Deployment: Render (`render.yaml`)

## Local Development

1. `python3 -m venv .venv && source .venv/bin/activate`
2. `pip install -r requirements.txt`
3. `python -m app.admin`
4. Open `http://127.0.0.1:5000/`

Default local DB file: `orgs.db` (created automatically).

## Environment Variables

- `DATABASE_URL`: required in production
- `AUTO_BOOTSTRAP_ORGS`: defaults to `true`; seeds from `orgs_bootstrap.json` when DB is empty
- `DISCOVERY_SEARCH_PROVIDER`: `duckduckgo` (default) or `openai_web`
- `OPENAI_API_KEY`: required when `DISCOVERY_SEARCH_PROVIDER=openai_web`
- `DISCOVERY_OPENAI_MODEL`: defaults to `gpt-5`
- `DISCOVERY_OPENAI_EXTERNAL_WEB_ACCESS`: defaults to `true`
- `DISCOVERY_OPENAI_FALLBACK_TO_DUCKDUCKGO`: defaults to `true`

## Deploy (Render)

- Blueprint: `render.yaml`
- Web service: `london-tasteful-events-admin`
- Health check: `GET /healthz`
- Start command: `gunicorn app.wsgi:app --bind 0.0.0.0:$PORT --workers 2 --threads 4`

## API (Current)

- `GET /api/admin/state`
- `POST /api/admin/review/<org_id>`
- `GET|POST /api/admin/strategies`
- `PATCH /api/admin/strategies/<strategy_id>`
- `POST /api/admin/discovery/run` (supports optional `search_provider`)
- `POST /api/orgs`
- `POST /api/orgs/bulk`
- `GET /api/stats`
- `GET /export`

## Collaboration

See [`docs/parallel-collaboration.md`](docs/parallel-collaboration.md) for branch naming and file ownership boundaries to reduce merge conflicts for parallel agent development.
