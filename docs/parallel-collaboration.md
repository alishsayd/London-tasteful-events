# Parallel Collaboration Contract

This repo is shared by two coding agents (Codex and Claude) on the same `main` branch.

## Branch Naming

- Codex branches: `codex/<topic>`
- Claude branches: `claude/<topic>`

## File Ownership (Default)

- Codex primary ownership:
  - `seed_orgs/db.py`
  - `seed_orgs/admin.py`
  - `render.yaml`
  - deployment/runtime configuration
- Claude primary ownership:
  - `seed_orgs/static/codex/*`
  - `seed_orgs/templates/*`
  - UI/UX polish and frontend behavior

If a task needs cross-boundary changes, split into two commits:
1. backend contract change
2. frontend/template adaptation

## Merge Safety Rules

- Keep commits focused to one thread.
- Avoid formatting-only edits in shared files.
- Do not rewrite or reorder large blocks in `seed_orgs/db.py` or `seed_orgs/admin.py` unless required.
- When touching shared API contracts, include endpoint and payload notes in commit message.

## API Contract Discipline

For every API change:

- List changed endpoint(s)
- List request/response shape changes
- State whether change is backward compatible

## Fast Conflict Checks

Run before push:

- `git fetch origin`
- `git diff --name-only origin/main...HEAD`
- `python3 -m py_compile seed_orgs/admin.py seed_orgs/db.py seed_orgs/discover.py`

## Release Rule

Deploy only after both are true:

1. backend compile check passes
2. app route smoke check passes (`/`, `/healthz`, `/api/stats`)
