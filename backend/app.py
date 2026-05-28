"""Showcase backend for test-walera.

Two responsibilities, one process:

1. Product API (`/api/...`) — mutates tasks / subtasks. Starting a task
   immediately commits status=IN_PROGRESS and schedules an asynchronous
   transition to COMPLETED after a random 2-7 s sleep. Both commits bump
   the todo_lists root via DB triggers, so Walera delivers each transition
   to every subscriber of `todo_lists:<id>`.

   There is no REST `GET /api/lists/<id>` here on purpose — the initial
   snapshot for the subscribed list is delivered by Walera itself as the
   first SSE frame (`event: initial_data`), built inside /auth/sessions
   below. Mutation endpoints stay because mutations are the client →
   server direction, which SSE does not carry.

2. Walera auth backend — implements the HMAC-refresh contract:
   - POST /auth/sessions    (Bearer → whitelist + initial_data; one-shot handshake)
   - POST /auth/permissions (HMAC-signed refresh; identifies user by user_id)

   A single demo token authorises the `todo_lists / tasks / subtasks`
   whitelist. After handshake walera never sees the bearer again — every
   refresh is authenticated by HMAC-SHA256 over user_id||channel||ts||nonce
   using the shared WALERA_AUTH_SIGNING_SECRET.

   The /auth/sessions response carries an `initial_data` field whenever
   the channel is `todo_lists:<id>`. Walera compacts that JSON and
   forwards it verbatim to the subscriber as the first SSE frame,
   before any `tx` events. `initial_data` is documented as open-time
   only, so refresh responses (which go to /auth/permissions) do not
   include it.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import random
import sys
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from threading import Lock
from typing import Any

import psycopg
from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("backend")

DSN = os.environ["DATABASE_URL"]

SIGNING_SECRET = os.environ.get("WALERA_AUTH_SIGNING_SECRET", "").encode()
SIGNING_KID = os.environ.get("WALERA_AUTH_SIGNING_KID", "v1")
if len(SIGNING_SECRET) < 32:
    sys.stderr.write(
        "backend: FATAL: WALERA_AUTH_SIGNING_SECRET must be set and ≥32 bytes "
        "(got %d)\n" % len(SIGNING_SECRET)
    )
    sys.exit(1)

# Single demo token. Whitelist contains every column of every table the UI
# renders. `roots` declares which tables the user is allowed to subscribe to
# as anchor channels (`todo_lists:<id>`).
PERMISSIONS: dict[str, dict[str, Any]] = {
    "demo-token": {
        "user_id": "u_demo",
        "tables": {
            "todo_lists": ["id", "title", "updated_at"],
            "tasks":      ["id", "todo_list_id", "title", "status", "updated_at"],
            "subtasks":   ["id", "task_id", "title", "status", "updated_at"],
        },
        "roots": ["todo_lists"],
        "ttl_seconds": 60,
    },
}

# user_id → whitelist reverse index, populated at import.
USERS_BY_ID: dict[str, dict[str, Any]] = {p["user_id"]: p for p in PERMISSIONS.values()}

# Replay protection — bounded LRU of recent nonces.
_NONCE_TTL_SECONDS = 300
_NONCE_MAX = 4096
_TS_WINDOW_SECONDS = 60
_nonce_lock = Lock()
_nonce_cache: "OrderedDict[str, float]" = OrderedDict()


def _check_nonce(nonce: str) -> bool:
    """Return True if nonce is fresh; prune by age + size on every call."""
    now = time.time()
    with _nonce_lock:
        cutoff = now - _NONCE_TTL_SECONDS
        while _nonce_cache:
            oldest = next(iter(_nonce_cache))
            if _nonce_cache[oldest] >= cutoff:
                break
            _nonce_cache.popitem(last=False)
        if nonce in _nonce_cache:
            return False
        _nonce_cache[nonce] = now
        while len(_nonce_cache) > _NONCE_MAX:
            _nonce_cache.popitem(last=False)
        return True


def _expected_sig(user_id: str, channel: str, ts: int, nonce: str) -> str:
    mac = hmac.new(SIGNING_SECRET, digestmod=hashlib.sha256)
    mac.update(user_id.encode())
    mac.update(b"\n")
    mac.update(channel.encode())
    mac.update(b"\n")
    mac.update(str(ts).encode())
    mac.update(b"\n")
    mac.update(nonce.encode())
    return mac.hexdigest()

_pool: AsyncConnectionPool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = AsyncConnectionPool(DSN, min_size=1, max_size=10, open=False)
    await _pool.open()
    log.info("pg pool ready")
    try:
        yield
    finally:
        await _pool.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Walera auth backend — HMAC-refresh wire
# ---------------------------------------------------------------------------

@app.post("/auth/sessions")
async def auth_open_session(
    body: dict[str, Any] = Body(default_factory=dict),
    authorization: str | None = Header(default=None),
):
    """Bearer → whitelist + initial_data. Called exactly once by walera at
    SSE handshake. Walera drops the bearer from memory after this returns;
    subsequent refreshes for the same subscriber arrive on
    POST /auth/permissions with an HMAC signature instead of the bearer.

    When the requested channel is `todo_lists:<id>` we attach the full
    list subtree as `initial_data`. Walera compacts the JSON and emits
    it to the subscriber as the first SSE frame, so the browser never
    needs a separate REST round-trip to seed its local mirror.
    """
    token = ""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[len("Bearer "):].strip()
    if not token:
        raise HTTPException(status_code=401, detail="missing_bearer")

    perms = PERMISSIONS.get(token)
    if perms is None:
        raise HTTPException(status_code=401, detail="unauthorized")

    channel = body.get("channel", "") if isinstance(body, dict) else ""
    initial_data: Any = None
    if channel:
        table, _, pk = channel.partition(":")
        if not table or table not in perms["tables"]:
            raise HTTPException(status_code=403, detail="forbidden")
        if table == "todo_lists" and pk and pk != "all":
            try:
                list_id = int(pk)
            except ValueError:
                raise HTTPException(status_code=404, detail="bad_channel")
            initial_data = await _build_list_snapshot(list_id)
            if initial_data is None:
                raise HTTPException(status_code=404, detail="list_not_found")

    response = dict(perms)
    if initial_data is not None:
        response["initial_data"] = initial_data
    return response


@app.post("/auth/permissions")
async def auth_refresh(request: Request):
    """HMAC-authenticated refresh. Walera proves it's the same service that
    completed a handshake by signing user_id||channel||ts||nonce with
    WALERA_AUTH_SIGNING_SECRET. We verify the signature, sanity-check ts /
    nonce, and return the current whitelist for that user_id.

    Sentinel user_id="_health" is reserved for walera's CheckAuth liveness
    probe — it returns a synthetic whitelist so a successful round-trip
    proves both backend reachability AND that walera_secret is in sync.
    """
    sig = request.headers.get("X-Walera-Sig", "")
    kid = request.headers.get("X-Walera-Kid", "")
    if kid != SIGNING_KID or not sig:
        raise HTTPException(status_code=401, detail="bad_sig_header")

    body = await request.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=401, detail="bad_body")

    user_id = body.get("user_id", "")
    channel = body.get("channel", "")
    ts = body.get("ts", 0)
    nonce = body.get("nonce", "")
    if not isinstance(user_id, str) or not isinstance(channel, str):
        raise HTTPException(status_code=401, detail="bad_types")
    if not isinstance(ts, int) or not isinstance(nonce, str) or not nonce:
        raise HTTPException(status_code=401, detail="bad_types")

    if abs(int(time.time()) - ts) > _TS_WINDOW_SECONDS:
        raise HTTPException(status_code=401, detail="ts_window")

    expected = _expected_sig(user_id, channel, ts, nonce)
    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=401, detail="bad_sig")

    if not _check_nonce(nonce):
        raise HTTPException(status_code=401, detail="replay")

    if user_id == "_health":
        return {
            "user_id": "_health",
            "tables": {"_health": ["id"]},
            "roots": ["_health"],
            "ttl_seconds": 60,
        }

    perms = USERS_BY_ID.get(user_id)
    if perms is None:
        raise HTTPException(status_code=404, detail="unknown_user")
    return perms


# ---------------------------------------------------------------------------
# Product API
# ---------------------------------------------------------------------------

async def _pool_required() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("pool not initialised")
    return _pool


async def _build_list_snapshot(list_id: int) -> dict[str, Any] | None:
    """Assemble the full todo_lists/<id> subtree for embedding into the
    /auth/sessions `initial_data` payload. Returns None when the list is
    absent so the caller can map that to a 404 on the auth response.
    """
    pool = await _pool_required()
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, title, updated_at FROM todo_lists WHERE id = %s",
            (list_id,),
        )
        head = await cur.fetchone()
        if head is None:
            return None

        await cur.execute(
            "SELECT id, title, status, updated_at FROM tasks "
            "WHERE todo_list_id = %s ORDER BY id",
            (list_id,),
        )
        tasks = await cur.fetchall()

        task_ids = [t["id"] for t in tasks]
        subtasks_by_task: dict[int, list[dict[str, Any]]] = {tid: [] for tid in task_ids}
        if task_ids:
            await cur.execute(
                "SELECT id, task_id, title, status, updated_at FROM subtasks "
                "WHERE task_id = ANY(%s) ORDER BY id",
                (task_ids,),
            )
            for s in await cur.fetchall():
                subtasks_by_task[s["task_id"]].append(
                    {
                        "id": s["id"],
                        "title": s["title"],
                        "status": s["status"],
                        "updated_at": s["updated_at"].isoformat(),
                    }
                )

        return {
            "id": head["id"],
            "title": head["title"],
            "updated_at": head["updated_at"].isoformat(),
            "tasks": [
                {
                    "id": t["id"],
                    "title": t["title"],
                    "status": t["status"],
                    "updated_at": t["updated_at"].isoformat(),
                    "subtasks": subtasks_by_task[t["id"]],
                }
                for t in tasks
            ],
        }


_ALLOWED_TABLES = {"tasks", "subtasks"}


async def _set_status(table: str, row_id: int, new_status: str, require_status: str) -> bool:
    assert table in _ALLOWED_TABLES
    pool = await _pool_required()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            f"UPDATE {table} SET status = %s, updated_at = now() "
            f"WHERE id = %s AND status = %s RETURNING id",
            (new_status, row_id, require_status),
        )
        row = await cur.fetchone()
        await conn.commit()
        return row is not None


async def _complete_after(table: str, row_id: int, delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        ok = await _set_status(table, row_id, "COMPLETED", "IN_PROGRESS")
        log.info("completion %s id=%s after %.2fs ok=%s", table, row_id, delay, ok)
    except Exception:
        log.exception("completion failed for %s id=%s", table, row_id)


FLAKY_FAIL_RATE = 0.30


def _maybe_flake(label: str) -> None:
    if random.random() < FLAKY_FAIL_RATE:
        log.info("flaky %s -> 503 (injected)", label)
        raise HTTPException(status_code=503, detail="flaky: try again")


async def _start(table: str, row_id: int) -> dict[str, Any]:
    _maybe_flake(f"start {table}/{row_id}")
    ok = await _set_status(table, row_id, "IN_PROGRESS", "PENDING")
    if not ok:
        raise HTTPException(status_code=409, detail="row not in PENDING state")
    delay = random.uniform(2.0, 7.0)
    asyncio.create_task(_complete_after(table, row_id, delay))
    return {"ok": True, "delay_seconds": round(delay, 2)}


@app.post("/api/tasks/{task_id}/start")
async def start_task(task_id: int):
    return await _start("tasks", task_id)


@app.post("/api/subtasks/{subtask_id}/start")
async def start_subtask(subtask_id: int):
    return await _start("subtasks", subtask_id)


async def _uncomplete(table: str, row_id: int) -> dict[str, Any]:
    ok = await _set_status(table, row_id, "PENDING", "COMPLETED")
    if not ok:
        raise HTTPException(status_code=409, detail="row not in COMPLETED state")
    return {"ok": True}


@app.post("/api/tasks/{task_id}/uncomplete")
async def uncomplete_task(task_id: int):
    return await _uncomplete("tasks", task_id)


@app.post("/api/subtasks/{subtask_id}/uncomplete")
async def uncomplete_subtask(subtask_id: int):
    return await _uncomplete("subtasks", subtask_id)


@app.post("/api/lists/{list_id}/complete-all")
async def complete_all(list_id: int):
    # Bulk start: flip every PENDING task + subtask under this list to
    # IN_PROGRESS in one transaction, then schedule independent async
    # completions for each (random 2-7 s, same as the single-row /start).
    # All trigger bumps target the same todo_lists PK, so Walera's
    # one-root-per-tx rule is satisfied for both the bulk-start tx and
    # every individual completion tx that follows.
    _maybe_flake(f"complete-all list/{list_id}")
    pool = await _pool_required()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE tasks SET status = 'IN_PROGRESS', updated_at = now() "
            "WHERE todo_list_id = %s AND status = 'PENDING' RETURNING id",
            (list_id,),
        )
        task_ids = [r[0] for r in await cur.fetchall()]
        await cur.execute(
            "UPDATE subtasks SET status = 'IN_PROGRESS', updated_at = now() "
            "WHERE task_id IN (SELECT id FROM tasks WHERE todo_list_id = %s) "
            "AND status = 'PENDING' RETURNING id",
            (list_id,),
        )
        subtask_ids = [r[0] for r in await cur.fetchall()]
        await conn.commit()

    for tid in task_ids:
        asyncio.create_task(_complete_after("tasks", tid, random.uniform(2.0, 7.0)))
    for sid in subtask_ids:
        asyncio.create_task(_complete_after("subtasks", sid, random.uniform(2.0, 7.0)))

    return {
        "ok": True,
        "tasks_started": len(task_ids),
        "subtasks_started": len(subtask_ids),
    }


@app.post("/api/lists/{list_id}/uncomplete-all")
async def uncomplete_all(list_id: int):
    # Single transaction: revert every COMPLETED task + subtask under this
    # list back to PENDING. IN_PROGRESS rows are left alone so a running
    # completion timer keeps its semantics. No flake injection — uncomplete
    # is the recovery path, it should be deterministic.
    pool = await _pool_required()
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE tasks SET status = 'PENDING', updated_at = now() "
            "WHERE todo_list_id = %s AND status = 'COMPLETED'",
            (list_id,),
        )
        tasks_reverted = cur.rowcount
        await cur.execute(
            "UPDATE subtasks SET status = 'PENDING', updated_at = now() "
            "WHERE task_id IN (SELECT id FROM tasks WHERE todo_list_id = %s) "
            "AND status = 'COMPLETED'",
            (list_id,),
        )
        subtasks_reverted = cur.rowcount
        await conn.commit()
    return {
        "ok": True,
        "tasks_reverted": tasks_reverted,
        "subtasks_reverted": subtasks_reverted,
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True}
