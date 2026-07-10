# rj_iplanrio__eai_history — Fix Summary

**Date:** March 26, 2026  
**Branch:** `staging/fix_eai_history`  
**Status:** Validated locally — ready for deploy

---

## Non-Technical Summary

### What stopped working

The AI assistant used by the Prefeitura do Rio (EAI — "rio.bot") keeps a record of every conversation with every citizen. This record is saved to a database table in BigQuery called `history`. Since **March 6, 2026**, no new conversations were being saved. The table was frozen in time.

### Why it broke

On March 6, the engineering team migrated the conversation storage database (Cloud SQL / PostgreSQL). Before the migration, conversations were stored as binary blobs (`bytea`). After the migration, they moved to a structured JSON format (`JSONB`).

This change broke the pipeline in three compounding ways:

1. **The cursor was wrong.** The pipeline tracks "where it left off" by storing the ID of the last conversation checkpoint. Checkpoint IDs are random UUIDs — they have no time ordering. So the pipeline was using a meaningless value to detect new data.

2. **Conversations were split across two tables.** With the new LangGraph v4 format (the framework that runs the AI agent), the messages of a conversation are no longer stored in the main checkpoint row. Instead, they live in a separate table called `checkpoint_blobs`. The pipeline was only reading the main checkpoint table and finding empty message containers.

3. **An attempted band-aid filter was on the wrong column.** An early fix tried to filter only checkpoints that had messages — but it looked in the wrong field of the JSON (`channel_values` instead of `channel_versions`), returning zero results.

### How it was fixed

- The cursor was changed to use the actual **timestamp** stored in the checkpoint (`last_update`), which has correct temporal ordering.
- A **PostgreSQL IMMUTABLE function** and an index were created to allow the database to efficiently find checkpoints by timestamp.
- The SQL query was rewritten to **join the `checkpoints` table with `checkpoint_blobs`**, fetching the actual message data where it now lives (encoded as binary msgpack, decoded in Python).
- The message blobs are transported safely as base64 strings and then decoded in Python using the LangGraph serializer.

### Result

After the fix, a test run against production data recovered **2,052 users and 40,800 messages** covering the period from March 24 onward. The data format is fully compatible with the downstream `history.sql` dbt model that powers analytics.

---

## Technical Summary

### Timeline of Root Causes

| Date | Event |
|---|---|
| Before March 6, 2026 | Pipeline working normally, storing message history to BQ |
| March 6, 2026 | Cloud SQL DB migration: `checkpoint` column type changed from `bytea` → `JSONB`; LangGraph upgraded to v4 format |
| March 6, 2026+ | Pipeline running but writing 0 new rows to BQ `history` table |

---

### Root Cause 1 — UUID Cursor Has No Temporal Order

**Problem:** `get_last_update()` queried BQ to find the most recent `checkpoint_id` and used it as the "cursor" for the next run. `checkpoint_id` is a **UUIDv4**, which is randomly generated — it has no temporal relationship. `MAX(uuid_v4)` is lexicographic, not chronological.

**Effect:** After the DB migration, even though new checkpoints existed, the UUID comparison never advanced. The query effectively fetched zero new users.

**Fix:** Changed the cursor to use `MAX(last_update)` — the actual ISO 8601 timestamp stored in `checkpoints.checkpoint->>'ts'`.

**Supporting infrastructure added:**
- `checkpoint_ts_immutable(jsonb)` — an `IMMUTABLE` PostgreSQL wrapper function around `(checkpoint->>'ts')::timestamptz`, required because PostgreSQL only allows expression indexes on immutable functions.
- `idx_checkpoints_ts` — a B-tree expression index on both staging and prod using this function, making the `WHERE checkpoint_ts_immutable(c.checkpoint) > $last_update` filter fast even over millions of rows.

---

### Root Cause 2 — LangGraph v4: `channel_values` Is Always Empty

**Problem:** In LangGraph v4 (the version deployed after the migration), checkpoints use a **delta format**. The `checkpoints.checkpoint->'channel_values'` JSON field is always `{}` (empty). Only the channels written in that specific step are recorded — and for most recent checkpoints (routing steps), no channels are written.

An attempted fix used `AND checkpoint->'channel_values' ? 'messages'` to filter checkpoints that had message data. This returned **0 rows** because `channel_values` is always empty in v4.

**The actual location of message data:**  
LangGraph v4 stores all channel data in a separate table, `checkpoint_blobs`:

```
(thread_id, checkpoint_ns, channel, version, type='msgpack', blob=bytea)
```

The correct pointer is `checkpoints.checkpoint->'channel_versions'->>'messages'`, which is a version string that acts as a foreign key into `checkpoint_blobs`.

---

### Root Cause 3 — `aget_tuple()` Broken for v4 Checkpoints

A second attempted fix used `PostgresSaver.aget_tuple(config)` to load checkpoints via the LangGraph library. This failed with:

```
NotImplementedError: Unknown serialization type: None
```

In the v4 schema, the `type` column of the `checkpoints` table is `NULL` (no longer `'json'`). The library's serde dispatcher calls `serde.loads_typed((None, data))` which isn't handled.

**Fix:** Bypassed `aget_tuple()` entirely. Messages are fetched with a direct SQL JOIN and decoded with `checkpointer.serde.loads_typed((blob_type, blob_bytes))` using the `blob_type='msgpack'` from `checkpoint_blobs.type`.

---

### Root Cause 4 — `ConnectorLoopError` with Per-User Async Queries

A third attempt added per-user SQL queries inside `_get_single_user_history()`, which is called inside `asyncio.gather()`. This raised:

```
ConnectorLoopError: Running event loop does not match 'connector._loop'
```

`PostgresEngine._pool` (the underlying connection pool) is **bound to a specific asyncio event loop** at creation time. It cannot be used from tasks running in a different loop or via `gather()` tasks.

**Fix:** All database reads were consolidated into a single bulk query in the main coroutine (`get_history_bulk_from_last_update`), before `asyncio.gather()` is called.

---

### Final Solution Architecture

**Bulk SQL Query (single DB round-trip):**

```sql
SELECT DISTINCT ON (c.thread_id)
    c.thread_id,
    c.checkpoint_id,
    c.checkpoint->>'ts'                AS ts,
    cb.type                            AS blob_type,
    encode(cb.blob, 'base64')         AS blob_b64
FROM "public"."checkpoints" c
JOIN "public"."checkpoint_blobs" cb
  ON  cb.thread_id = c.thread_id
  AND cb.channel   = 'messages'
  AND cb.version   = c.checkpoint->'channel_versions'->>'messages'
WHERE checkpoint_ts_immutable(c.checkpoint) > '{last_update}'::timestamptz
ORDER BY c.thread_id, checkpoint_ts_immutable(c.checkpoint) DESC
```

- `DISTINCT ON (thread_id)` with `ORDER BY ... DESC` picks the **most recent** checkpoint per thread.
- The JOIN to `checkpoint_blobs` on `(thread_id, channel='messages', version=channel_versions.messages)` fetches the latest message blob for that thread.
- `encode(blob, 'base64')` transports binary msgpack as ASCII text (required by `PostgresLoader`, which is text-only).

**Per-User Decoding (no DB calls, pure Python):**

```python
blob_bytes = base64.b64decode(blob_b64)
messages = self._checkpointer.serde.loads_typed((blob_type, blob_bytes))
payload = to_gateway_format(messages=messages, thread_id=user_id, ...)
```

**Why this is the best solution:**
1. **Single query** — one DB round-trip for all users, regardless of user count.
2. **No library dependency** — bypasses the broken `aget_tuple()` path entirely.
3. **Event-loop safe** — no DB calls inside `asyncio.gather()` tasks.
4. **Index-backed** — the expression index on `checkpoint_ts_immutable(checkpoint)` makes the `WHERE` clause efficient.
5. **Correct semantics** — `DISTINCT ON` with descending timestamp guarantees we pick the most up-to-date message snapshot per thread.

---

### Validation Results

| Metric | Value |
|---|---|
| Test window | March 24–26, 2026 (2-day probe) |
| Users found | 2,052 |
| Messages extracted | 40,800 |
| Message types | `user_message`, `assistant_message`, `reasoning_message`, `tool_call_message`, `tool_return_message`, `usage_statistics` |
| Null `id` messages | 2,052 (all `usage_statistics`) — correctly filtered by `history.sql` |
| `last_update` nulls | 0 |
| Compatible with `history.sql` | Yes — all fields expected by CTEs are present |

---

### Pending Actions

- [ ] Remove test CSV save block from `history.py`
- [ ] Clean `flow.py __main__` back to bare `rj_iplanrio__eai_history()`
- [ ] Delete `output_test.csv` and `diag.py`
- [ ] Commit and push to `staging/fix_eai_history`
- [ ] Deploy and trigger prod backfill with `last_update="2026-03-06T00:00:00+00:00"`
