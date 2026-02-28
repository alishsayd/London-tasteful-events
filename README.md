# London-tasteful-events

Admin backend for curating London organizations before event extraction.

## What runs where

- **Primary app (stateful):** Flask admin + API with Postgres (deploy on Render)
- **Optional static pages:** GitHub Pages files at `/`, `/codex-curation-console.html`, `/claude-review.html`

## Local development

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Run the admin server:
   - `python -m seed_orgs.admin`
4. Open:
   - `http://127.0.0.1:5000/review` (Claude review flow)
   - `http://127.0.0.1:5000/codex` (Codex persistent queue)

By default local runs use SQLite file `orgs.db`.

## Production environment variables

- `DATABASE_URL` (required in production)
- `AUTO_SEED_ORGS` (`true` by default; seeds from `seed_data.json` if DB is empty)

## Deployment (Render)

This repository includes a Render blueprint at `render.yaml`:

- Web service: `london-tasteful-events-admin`
- Postgres database: `london-tasteful-events-db`

Start command:

- `gunicorn seed_orgs.wsgi:app --bind 0.0.0.0:$PORT --workers 2 --threads 4`

## API endpoints (core)

- `GET /api/codex/state`
- `POST /api/codex/review/<org_id>`
- `POST /api/codex/next-batch`
- `GET|POST /api/codex/strategies`
- `PATCH /api/codex/strategies/<strategy_id>`
- `POST /api/orgs` (manual org add)
