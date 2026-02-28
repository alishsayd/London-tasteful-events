# Parallel Collaboration Contract

This repo is shared by two coding agents working on the same `main` branch.

## Branch Naming

- Principal agent branches: `principal/<topic>`
- Secondary agent branches: `assistant/<topic>`

## File Ownership (Default)

- Backend ownership:
  - `app/db.py`
  - `app/admin.py`
  - `render.yaml`
  - deployment/runtime configuration
- Frontend ownership:
  - `app/static/admin/*`
  - `app/templates/*`
  - UI behavior and polish

If a task needs cross-boundary changes, split into two commits:
1. backend contract change
2. frontend/template adaptation

## Merge Safety Rules

- Keep commits focused to one thread.
- Avoid formatting-only edits in shared files.
- Do not rewrite or reorder large blocks in `app/db.py` or `app/admin.py` unless required.
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
- `python3 -m py_compile app/admin.py app/db.py app/discover.py`

## Release Rule

Deploy only after both are true:

1. backend compile check passes
2. app route smoke check passes (`/`, `/healthz`, `/api/stats`)
