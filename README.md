# test-walera

A self-contained showcase for [Walera](https://github.com/slavnycoder/walera) —
the Go service that streams PostgreSQL row changes over SSE. This repo wires
Walera up against a tiny hierarchical TODO app to demonstrate the realistic
client-integration shape: SSE events as a diff source over an IndexedDB
mirror, optimistic UI updates with rollback, and bulk transactional ops.

```
browser ──fetch (mutations only)──► backend ──UPDATE──► postgres
   ▲                                                       │
   │  Dexie ◄──── apply tx changes ────────────────────────┘ WAL
   │                                                          │
   └─── SSE (initial_data + tx) ◄────── walera ◄──────────────┘
                                              │
                                              └── /auth/sessions ──► backend
                                                  (returns whitelist + snapshot)
```

## Stack

| Service    | Port | What it does                                                    |
| ---------- | ---- | --------------------------------------------------------------- |
| postgres   | 5432 | PG 18 with `wal_level=logical` and publication `cdc_sse_streamer`. |
| backend    | 8000 | FastAPI: mutation API + Walera auth backend (initial_data source). |
| walera     | 8080 | `ghcr.io/slavnycoder/walera:latest`. Tails WAL, fans out SSE.    |
| frontend   | 8081 | Caddy serving `frontend/web/index.html` (vanilla JS + Dexie).   |

## Run

```bash
make up   # docker compose up -d --build
```

Open <http://localhost:8081> in **two tabs**. Try:

- Click a checkbox → optimistic IN_PROGRESS, server runs a random 2-7 s
  task, then commits COMPLETED. Both tabs update via SSE.
- Click an already-COMPLETED row to uncomplete it (PENDING).
- **Complete all** → one transaction flips every PENDING row to
  IN_PROGRESS; each row then completes on its own independent timer.
- **Uncomplete all** → one transaction reverts every COMPLETED row to
  PENDING. IN_PROGRESS rows are left alone.

```bash
make logs    # follow everything
make down    # stop (keeps the pg volume)
make reset   # stop + drop pg volume (cold start)
make psql    # psql into the postgres container
```

## Data model

```
todo_lists ─< tasks ─< subtasks
```

Every child mutation bumps its parent's `updated_at` via a `BEFORE/AFTER`
trigger. The cascade reaches `todo_lists.updated_at` within the same
transaction, so a single subtask change produces one Postgres tx with
`subtasks + tasks + todo_lists` rows — and Walera fans it out as one
SSE event to every subscriber of `todo_lists:<id>`.

This pattern is exactly what Walera's
[writer-side discipline](https://github.com/slavnycoder/walera/blob/master/README.md#writer-side-discipline)
prescribes: clients subscribe to a root entity, the backend co-writes
related rows in one transaction so the broker can route by commit
boundary.

## How the frontend uses SSE

The classic recipe ("treat SSE as a hint to refresh, fetch state from the
primary API") works but wastes bandwidth and pins the client to a second
endpoint. This demo shows the **walera-native** alternative — the initial
snapshot rides the same SSE channel as the live diffs:

1. On boot: `whoami()` round-trip resolves the user identity banner.
   No REST hydrate.
2. SSE opens against `walera/sse/v1/todo_lists/1`. Walera calls the
   backend's `/auth/sessions`, which returns the whitelist **plus** an
   `initial_data` field containing the full `todo_lists:1` subtree.
   Walera compacts that JSON and sends it as the first SSE frame
   (`event: initial_data`).
3. The browser parses `initial_data` and writes the subtree into a
   Dexie (IndexedDB) database in one transaction — this is the seed.
4. Each subsequent `tx` event is parsed and applied to Dexie inside one
   IndexedDB transaction:
   - `insert` → `put` the full row.
   - `update` → merge with the existing row (pgoutput may emit only
     modified columns, so a naive overwrite would erase fields).
   - `delete` → `delete` by PK.
5. Dexie's `liveQuery(loadState)` re-renders the UI on every write.
   Because IndexedDB is per-origin, two tabs of the showcase share the
   same database and react to each other's writes — though each tab
   also runs its own SSE subscription for symmetry.
6. On SSE reconnect, the disconnect window may have lost events.
   Walera emits a fresh `initial_data` frame on every open, so we just
   re-apply it as the new ground truth — no separate REST refresher.
   Walera makes **no continuity guarantee** across reconnect, and
   `initial_data` is the contract for closing that gap.

The auth backend ships `initial_data` on every `/auth/sessions` response,
open or refresh, but Walera only ever emits the SSE `initial_data` frame
on the open-time call — it ignores the field on background refreshes, so
a TTL refresh can never silently overwrite the live diff stream.

The optimistic overlay is a `Map<"kind:id", status>` consulted at render
time. It gets cleared when the real status from Dexie catches up to the
optimistic guess, or on server failure.

## Failure injection (testing rollback)

The backend injects a **30% failure rate** on every state transition
into `IN_PROGRESS`:

- `POST /api/tasks/{id}/start`
- `POST /api/subtasks/{id}/start`
- `POST /api/lists/{id}/complete-all`

The injected `503 flaky: try again` fires **before** any DB write, so
no state changes occur on server. The frontend then drops the
optimistic entry, calls `render(lastState)`, and the UI snaps back to
the actual Dexie state.

Uncomplete paths (single and bulk) are deterministic — they're the
recovery flow, not the place to add chaos.

## Auth

A single hardcoded token (`demo-token`) is shared between frontend and
backend. Walera's auth contract is a single endpoint:

- `POST /auth/sessions` — Bearer → whitelist (+ `initial_data` when the
  body carries a `todo_lists:<id>` channel). Walera calls this **both**
  at SSE handshake and on every periodic refresh (every `ttl_seconds`)
  while the stream is live, replaying the exact same bearer each time.
  The `initial_data` payload only ever becomes an SSE frame on the
  handshake call — Walera ignores it on refresh. Since it's the same
  request both times, there's no separate refresh endpoint, no signing
  secret, and no server-side session state: a token that stops
  authenticating here (logout, expiry) ends the live stream with
  `auth_revoked` on the next refresh.

The frontend uses `demo-token` over `Authorization: Bearer ...` both to
open the SSE connection and to resolve the identity banner. The backend
serves `/auth/sessions` from `backend/app.py`. On boot the frontend
hits it directly for the banner:

```
user=u_demo  tables: todo_lists(id,title,updated_at)  tasks(...)  subtasks(...)
```

If the token is wrong the banner turns red and SSE never opens.

## File layout

```
├── Makefile                    # up / down / reset / logs / psql
├── docker-compose.yml
├── .env                        # ports + db password (committed dev defaults)
├── db/
│   ├── 001_publication.sql     # CREATE PUBLICATION cdc_sse_streamer
│   └── 002_schema.sql          # tables, cascading triggers, seed
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                  # FastAPI: mutation API + auth backend
└── frontend/
    ├── Caddyfile
    ├── tsconfig.json
    ├── src/
    │   ├── walera-client.ts    # reusable, project-agnostic SSE wrapper
    │   └── fetch-event-source.d.ts
    └── web/
        ├── index.html          # app glue: Dexie + walera-client.js
        └── walera-client.js    # compiled — Caddy serves this directly
```

The wrapper is intentionally separated from the app glue so it can be
lifted into another project unchanged. Re-compile after editing the
`.ts` source:

```bash
cd frontend && npx -y -p typescript@5 tsc -p tsconfig.json
```

## What's intentionally absent

- No bundler / npm. Frontend uses native ES modules and esm.sh CDN.
- No `Last-Event-ID` resume. Walera doesn't replay; the
  `initial_data` frame on every open is the reconnect refresher.
- No REST `GET /api/lists/<id>` endpoint. The initial snapshot rides
  the SSE channel via `initial_data`; the only HTTP calls the
  frontend makes against the product API are mutations.
- No multi-user / token switcher. One hardcoded user keeps the demo
  focused on the SSE-diff mechanics.
- No wildcard subscription (`/sse/v1/todo_lists/all`). Exact-PK
  subscription on `todo_lists:1` is enough to show transactional
  delivery.
