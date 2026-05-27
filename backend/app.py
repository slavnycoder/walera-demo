"""Showcase backend for test-walera.

Two responsibilities, one process:

1. Product API (`/api/...`) — reads the todo_list tree and starts tasks /
   subtasks. Starting a task immediately commits status=IN_PROGRESS and
   schedules an asynchronous transition to COMPLETED after a random 2-7 s
   sleep. Both commits bump the todo_lists root via DB triggers, so Walera
   delivers each transition to every subscriber of `todo_lists:<id>`.

2. Walera auth backend (`GET /auth/permissions`) — implements the contract
   from walera/docs/auth.md. A single demo token authorises the entire
   `todo_lists / tasks / subtasks` whitelist.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("backend")

DSN = os.environ["DATABASE_URL"]

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
# Walera auth backend
# ---------------------------------------------------------------------------

@app.get("/auth/permissions")
async def auth_permissions(
    channel: str = Query(""),
    authorization: str | None = Header(default=None),
):
    if channel == "_health":
        return {
            "user_id": "u_service",
            "tables": {"_health": ["id"]},
            "roots": ["_health"],
            "ttl_seconds": 60,
        }

    token = ""
    if authorization and authorization.startswith("Bearer "):
        token = authorization[len("Bearer "):].strip()

    perms = PERMISSIONS.get(token)
    if perms is None:
        raise HTTPException(status_code=401, detail="unauthorized")

    table = channel.split(":", 1)[0]
    if table not in perms["tables"]:
        raise HTTPException(status_code=403, detail="forbidden")
    return perms


# ---------------------------------------------------------------------------
# Product API
# ---------------------------------------------------------------------------

async def _pool_required() -> AsyncConnectionPool:
    if _pool is None:
        raise RuntimeError("pool not initialised")
    return _pool


@app.get("/api/lists/{list_id}")
async def get_list(list_id: int):
    pool = await _pool_required()
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, title, updated_at FROM todo_lists WHERE id = %s",
            (list_id,),
        )
        head = await cur.fetchone()
        if head is None:
            raise HTTPException(status_code=404, detail="list not found")

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
