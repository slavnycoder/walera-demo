# test-walera

A self-contained showcase for [Walera](https://github.com/slavnycoder/walera) —
the Go service that streams PostgreSQL row changes over SSE. This repo wires
Walera up against a tiny hierarchical TODO app to demonstrate the realistic
client-integration shape: SSE events as a diff source over an IndexedDB
mirror, optimistic UI updates with rollback, and bulk transactional ops.

```
browser ──fetch──► backend ──UPDATE──► postgres
   ▲                                       │
   │  Dexie ◄──── apply tx changes ────────┘ WAL
   │                                          │
   └─── SSE ◄────────── walera ◄──────────────┘
```

## Stack

| Service    | Port | What it does                                                    |
| ---------- | ---- | --------------------------------------------------------------- |
| postgres   | 5432 | PG 18 with `wal_level=logical` and publication `cdc_sse_streamer`. |
| backend    | 8000 | FastAPI: product API + Walera auth backend.                     |
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
primary API") works but wastes bandwidth. This demo shows the
**diff-source** alternative:

1. On boot: `whoami()` round-trip → `hydrate()` pulls the full subtree
   from `/api/lists/1` and writes it into a Dexie (IndexedDB) database.
2. SSE opens. Each `tx` event is parsed and applied to Dexie inside one
   IndexedDB transaction:
   - `insert` → `put` the full row.
   - `update` → merge with the existing row (pgoutput may emit only
     modified columns, so a naive overwrite would erase fields).
   - `delete` → `delete` by PK.
3. Dexie's `liveQuery(loadState)` re-renders the UI on every write.
   Because IndexedDB is per-origin, two tabs of the showcase share the
   same database and react to each other's writes — though each tab
   also runs its own SSE subscription for symmetry.
4. On SSE reconnect, the disconnect window may have lost events. We
   call `hydrate()` again to close the gap. Walera makes **no continuity
   guarantee** across reconnect — clients must resync via REST.

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
backend. Walera's auth contract has two endpoints:

- `POST /auth/sessions` — Bearer → whitelist. Called **once** by walera at
  SSE handshake. Walera drops the bearer from memory after this returns.
- `POST /auth/permissions` — refresh, authenticated by HMAC-SHA256 over
  `user_id||channel||ts||nonce` using the shared
  `WALERA_AUTH_SIGNING_SECRET` (see `.env`). The bearer never crosses this
  endpoint, so a memory dump of walera contains zero user tokens past
  handshake; revocation propagates as soon as the backend returns 403/404
  on the next refresh (driven by walera's per-subscriber TTL).

The frontend still uses `demo-token` over `Authorization: Bearer ...` to
open the SSE connection. The backend serves both endpoints from
`backend/app.py`. On boot the frontend resolves user identity for the
banner:

```
user=u_demo  roots=[todo_lists]  tables: todo_lists(id,title,updated_at)  tasks(...)  subtasks(...)
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
│   └── app.py                  # FastAPI: /api + /auth/permissions
└── frontend/
    ├── Caddyfile
    └── web/index.html          # vanilla ES modules + Dexie + fetch-event-source
```

## What's intentionally absent

- No bundler / npm. Frontend uses native ES modules and esm.sh CDN.
- No `Last-Event-ID` resume. Walera doesn't replay; we hydrate on
  reconnect.
- No multi-user / token switcher. One hardcoded user keeps the demo
  focused on the SSE-diff mechanics.
- No wildcard subscription (`/sse/v1/todo_lists/all`). Exact-PK
  subscription on `todo_lists:1` is enough to show transactional
  delivery.
