# Codex Workstream Isolation

This prototype is isolated from other parallel workstreams.

## Scope Boundary

1. Codex files are prefixed with `codex-`.
2. No shared build config or shared module edits are required.
3. Data storage key is `codex.londonTastefulEvents.curation.v1`.

## Entry Point

`codex-curation-console.html`

## Conflict Avoidance

1. Keep Codex edits inside `codex-*` files.
2. Avoid modifying generic shared files such as `claude-review.html`, `seed_orgs/*`, and root configs unless explicitly required.
3. Use Codex-prefixed names for any additional files.
