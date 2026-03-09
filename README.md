# London Tasteful Events

CSV-first Flask app for curating and publishing London event-source organizations.

## Current Product Scope

The project currently manages **organizations only**. It does not parse or crawl events yet.

Two user surfaces exist:

- Public browse app at `/` and `/browse`
- Password-protected admin console at `/admin`

The operating model is:

1. Manually discover candidate orgs externally
2. Import them through CSV or add them manually in admin
3. Let the system dedupe and route incomplete rows into review
4. Publish only vetted active orgs on the public page

## Runtime

- App: Flask + SQLAlchemy (`app`)
- Database: PostgreSQL in production via `DATABASE_URL`, SQLite locally by default
- Deployment: Render (`render.yaml`)
- Production entrypoint: `gunicorn app.wsgi:app --bind 0.0.0.0:$PORT --workers 2 --threads 4`

## Local Development

1. `python3 -m venv .venv && source .venv/bin/activate`
2. `pip install -r requirements.txt`
3. `python -m app.admin`
4. Open [http://127.0.0.1:5000/](http://127.0.0.1:5000/)

Default local DB file: `orgs.db`.

## Environment Variables

- `DATABASE_URL`
  - Required in production
  - If unset locally, the app uses SQLite at `orgs.db`
- `ADMIN_USERNAME`
  - Optional
  - Defaults to `admin`
- `ADMIN_PASSWORD`
  - Required in production
  - When set, all admin/API routes require HTTP Basic Auth except public routes
- `ALLOW_INSECURE_ADMIN`
  - Optional local-only escape hatch
  - If truthy, admin/API routes stay open even when `ADMIN_PASSWORD` is unset
  - Intended only for local development

## Auth Model

Public routes stay open:

- `/`
- `/browse`
- `/healthz`
- `/favicon.ico`
- `/api/flag/<org_id>`

Everything else requires admin auth unless you are running locally without `ADMIN_PASSWORD`.

In non-local environments, admin auth now fails closed if `ADMIN_PASSWORD` is missing.

## Public Experience

The public page is a lightweight directory of active orgs.

- Shows active orgs that are not crawl-paused
- Lets users filter by org type and borough
- Links org names directly to each org's events page
- Allows public users to flag an org for admin review

## Admin Experience

The admin console is the operational tool for maintaining the org set.

- Review Queue
  - Edit `name`, `borough`, `org_type`, and `events_url`
  - Resolve, snooze, or keep issues open
- Active Orgs
  - Browse the current active set
  - Move any org back into the queue for manual review
- Add Org
  - Add a single org manually
  - Manual add dedupes against existing records
- Bulk CSV Import
  - Preview import before apply
  - Reject blocked domains and obvious non-entity rows
  - Dedupe by canonical URL/domain-name keys
  - Open review issues for incomplete rows
- Import History
  - Shows recent import runs and summary counts
- Taxonomy Normalize
  - Recomputes canonical `org_type` and `primary_type`

## Data Rules

Each org has:

- exactly one `primary_type`
- exactly one canonical `org_type`
- optional `homepage`
- optional `events_url`
- optional `borough`

The app uses canonical dedupe keys in the DB layer:

- `homepage_key`
- `events_url_key`
- `domain_name_key`
- `name_key`

Conflicting writes return structured `409` responses instead of silently creating duplicates.

## API

### Public

- `GET /api/stats`
- `GET /export`
- `POST /api/flag/<org_id>`

### Admin

- `GET /api/admin/state`
- `POST /api/orgs`
- `POST /api/admin/review/<org_id>`
- `POST /api/admin/taxonomy/normalize`
- `POST /api/admin/import/csv`

Notes:

- `POST /api/orgs` may return `deduped_existing: true` when the submitted org matches an existing record.
- Mutating admin endpoints return `409` with structured conflict metadata when a write would collide with an existing org.

## Deploy (Render)

- Blueprint: `render.yaml`
- Service: `london-tasteful-events-admin`
- Health check: `GET /healthz`

After deployment, verify that `ADMIN_PASSWORD` is configured in Render.

## Collaboration

See [`docs/parallel-collaboration.md`](docs/parallel-collaboration.md) for branch naming and file ownership boundaries when multiple agents are working in parallel.
