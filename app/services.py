"""Core business logic.

Concurrency design (all guarantees are enforced inside one PostgreSQL database):

* Publish / config-update of active experiments take the layer row lock
  (SELECT ... FOR UPDATE) before validating bucket-range occupancy, so
  concurrent publishes cannot both pass validation. A partial exclusion
  constraint on int4range provides a second, hard guarantee.
* Assign locks the candidate experiment row (FOR UPDATE) before inserting, so
  it serializes with END (which takes the same row lock and flips its
  assignments to effective = FALSE). A partial unique index on
  (layer_id, user_key) WHERE effective guarantees at most one effective
  assignment per user per layer, whatever the interleaving.
* Exposure registration relies on UNIQUE (assignment_id, event_no) plus
  ON CONFLICT DO NOTHING; identical retries are idempotent, conflicting
  content is rejected, and ended (non-effective) assignments are refused.
"""

from __future__ import annotations

import secrets

import asyncpg
from fastapi import HTTPException

from .hashing import TOTAL_BUCKETS, bucket_for, pick_group


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _conditions_met(conditions: dict, attributes: dict) -> bool:
    """Full match: every condition key/value must be present and equal."""
    for key, value in conditions.items():
        if key not in attributes or attributes[key] != value:
            return False
    return True


async def _experiment_view(conn, experiment_id: int) -> dict:
    exp = await conn.fetchrow(
        """SELECT e.*, l.key AS layer_key
           FROM experiments e JOIN layers l ON l.id = e.layer_id
           WHERE e.id = $1""",
        experiment_id,
    )
    if exp is None:
        raise HTTPException(404, f"experiment {experiment_id} not found")
    groups = await conn.fetch(
        "SELECT name, weight FROM experiment_groups WHERE experiment_id = $1 ORDER BY seq",
        experiment_id,
    )
    return {
        "id": exp["id"],
        "layer_key": exp["layer_key"],
        "key": exp["key"],
        "version": exp["version"],
        "status": exp["status"],
        "bucket_start": exp["bucket_start"],
        "bucket_end": exp["bucket_end"],
        "interval": f"[{exp['bucket_start']}, {exp['bucket_end']})",
        "conditions": exp["conditions"],
        "groups": [dict(g) for g in groups],
        "created_at": exp["created_at"],
        "updated_at": exp["updated_at"],
    }


async def _current_assignment(conn, layer_id: int, user_key: str):
    return await conn.fetchrow(
        """SELECT a.id, a.user_key, a.bucket, a.group_name, a.snapshot,
                  a.effective, a.created_at
           FROM assignments a
           WHERE a.layer_id = $1 AND a.user_key = $2 AND a.effective""",
        layer_id,
        user_key,
    )


def _assigned_payload(a) -> dict:
    snap = a["snapshot"]
    return {
        "assigned": True,
        "assignment_id": a["id"],
        "layer_key": snap["layer_key"],
        "user_key": a["user_key"],
        "bucket": a["bucket"],
        "experiment_id": snap["experiment_id"],
        "experiment_key": snap["experiment_key"],
        "experiment_version": snap["experiment_version"],
        "group": a["group_name"],
        "snapshot": snap,
        "assigned_at": a["created_at"],
    }


def _unassigned_payload(body, bucket: int, reason: str, experiment=None) -> dict:
    payload = {
        "assigned": False,
        "reason": reason,
        "layer_key": body.layer_key,
        "user_key": body.user_key,
        "bucket": bucket,
    }
    if experiment is not None:
        payload["candidate_experiment_id"] = experiment["id"]
        payload["candidate_experiment_key"] = experiment["key"]
    return payload


async def _replace_groups(conn, experiment_id: int, groups) -> None:
    await conn.execute(
        "DELETE FROM experiment_groups WHERE experiment_id = $1", experiment_id
    )
    await conn.executemany(
        "INSERT INTO experiment_groups (experiment_id, seq, name, weight) "
        "VALUES ($1, $2, $3, $4)",
        [(experiment_id, i, g.name, g.weight) for i, g in enumerate(groups)],
    )


async def _assert_no_overlap(
    conn, layer_id: int, exclude_id: int, bucket_start: int, bucket_end: int
) -> None:
    clash = await conn.fetchrow(
        """SELECT id, key FROM experiments
           WHERE layer_id = $1 AND status = 'active' AND id <> $2
             AND bucket_start < $4 AND bucket_end > $3
           LIMIT 1""",
        layer_id,
        exclude_id,
        bucket_start,
        bucket_end,
    )
    if clash is not None:
        raise HTTPException(
            409,
            f"bucket range [{bucket_start}, {bucket_end}) overlaps active "
            f"experiment '{clash['key']}' (id={clash['id']})",
        )


# ---------------------------------------------------------------------------
# layers
# ---------------------------------------------------------------------------

async def create_layer(pool, body) -> dict:
    salt = body.salt or secrets.token_hex(16)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO layers (key, salt) VALUES ($1, $2) "
                "RETURNING id, key, salt, created_at",
                body.key,
                salt,
            )
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, f"layer '{body.key}' already exists")
    return dict(row)


async def get_layer(pool, layer_key: str) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, key, salt, created_at FROM layers WHERE key = $1", layer_key
        )
    if row is None:
        raise HTTPException(404, f"layer '{layer_key}' not found")
    return dict(row)


# ---------------------------------------------------------------------------
# experiments
# ---------------------------------------------------------------------------

async def create_experiment(pool, layer_key: str, body) -> dict:
    async with pool.acquire() as conn:
        async with conn.transaction():
            layer = await conn.fetchrow(
                "SELECT id FROM layers WHERE key = $1", layer_key
            )
            if layer is None:
                raise HTTPException(404, f"layer '{layer_key}' not found")
            try:
                async with conn.transaction():  # savepoint
                    exp = await conn.fetchrow(
                        """INSERT INTO experiments
                               (layer_id, key, bucket_start, bucket_end, conditions)
                           VALUES ($1, $2, $3, $4, $5)
                           RETURNING id""",
                        layer["id"],
                        body.key,
                        body.bucket_start,
                        body.bucket_end,
                        body.conditions,
                    )
            except asyncpg.UniqueViolationError:
                raise HTTPException(
                    409,
                    f"experiment '{body.key}' already exists in layer '{layer_key}'",
                )
            await _replace_groups(conn, exp["id"], body.groups)
            return await _experiment_view(conn, exp["id"])


async def list_experiments(pool, layer_key: str) -> list[dict]:
    async with pool.acquire() as conn:
        layer = await conn.fetchrow(
            "SELECT id FROM layers WHERE key = $1", layer_key
        )
        if layer is None:
            raise HTTPException(404, f"layer '{layer_key}' not found")
        rows = await conn.fetch(
            "SELECT id FROM experiments WHERE layer_id = $1 ORDER BY id",
            layer["id"],
        )
        return [await _experiment_view(conn, r["id"]) for r in rows]


async def get_experiment(pool, experiment_id: int) -> dict:
    async with pool.acquire() as conn:
        return await _experiment_view(conn, experiment_id)


async def update_experiment(pool, experiment_id: int, body) -> dict:
    """Update bucket range / conditions / groups of a draft or active experiment.

    Bumps the version. Existing assignments keep their snapshot; only users
    assigned after the change see the new rules.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            exp = await conn.fetchrow(
                "SELECT id, layer_id, status FROM experiments WHERE id = $1",
                experiment_id,
            )
            if exp is None:
                raise HTTPException(404, f"experiment {experiment_id} not found")
            if exp["status"] == "ended":
                raise HTTPException(409, "ended experiments cannot be modified")
            # Serialize with concurrent publishes/updates in this layer.
            await conn.fetchrow(
                "SELECT id FROM layers WHERE id = $1 FOR UPDATE", exp["layer_id"]
            )
            # Re-read under the layer lock: a publish may have interleaved.
            exp = await conn.fetchrow(
                "SELECT id, layer_id, status FROM experiments WHERE id = $1",
                experiment_id,
            )
            if exp["status"] == "ended":
                raise HTTPException(409, "ended experiments cannot be modified")
            if exp["status"] == "active":
                await _assert_no_overlap(
                    conn,
                    exp["layer_id"],
                    experiment_id,
                    body.bucket_start,
                    body.bucket_end,
                )
            try:
                async with conn.transaction():  # savepoint
                    row = await conn.fetchrow(
                        """UPDATE experiments
                           SET bucket_start = $2, bucket_end = $3, conditions = $4,
                               version = version + 1, updated_at = now()
                           WHERE id = $1 AND status IN ('draft', 'active')
                           RETURNING id""",
                        experiment_id,
                        body.bucket_start,
                        body.bucket_end,
                        body.conditions,
                    )
            except asyncpg.ExclusionViolationError:
                raise HTTPException(
                    409, "bucket range overlaps another active experiment"
                )
            if row is None:
                raise HTTPException(409, "experiment was ended concurrently")
            await _replace_groups(conn, experiment_id, body.groups)
            return await _experiment_view(conn, experiment_id)


async def publish_experiment(pool, experiment_id: int) -> dict:
    """Atomically validate bucket occupancy and activate the experiment."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            exp = await conn.fetchrow(
                "SELECT id, layer_id, status, bucket_start, bucket_end "
                "FROM experiments WHERE id = $1",
                experiment_id,
            )
            if exp is None:
                raise HTTPException(404, f"experiment {experiment_id} not found")
            # Lock the layer row: serializes concurrent publishes in this layer
            # so two overlapping experiments can never both become active.
            await conn.fetchrow(
                "SELECT id FROM layers WHERE id = $1 FOR UPDATE", exp["layer_id"]
            )
            # Re-read under the layer lock: another publish may have interleaved.
            exp = await conn.fetchrow(
                "SELECT id, layer_id, status, bucket_start, bucket_end "
                "FROM experiments WHERE id = $1",
                experiment_id,
            )
            if exp["status"] == "active":
                return await _experiment_view(conn, experiment_id)  # idempotent
            if exp["status"] != "draft":
                raise HTTPException(
                    409, f"cannot publish experiment in status '{exp['status']}'"
                )
            groups = await conn.fetch(
                "SELECT weight FROM experiment_groups WHERE experiment_id = $1",
                experiment_id,
            )
            total = sum(g["weight"] for g in groups)
            if total != TOTAL_BUCKETS:
                raise HTTPException(
                    409, f"group weights must sum to {TOTAL_BUCKETS}, got {total}"
                )
            await _assert_no_overlap(
                conn,
                exp["layer_id"],
                experiment_id,
                exp["bucket_start"],
                exp["bucket_end"],
            )
            try:
                async with conn.transaction():  # savepoint
                    await conn.execute(
                        "UPDATE experiments SET status = 'active', updated_at = now() "
                        "WHERE id = $1",
                        experiment_id,
                    )
            except asyncpg.ExclusionViolationError:
                # Backstop from the exclusion constraint (should be unreachable
                # thanks to the layer lock, but guarantees correctness anyway).
                raise HTTPException(
                    409, "bucket range overlaps another active experiment"
                )
            return await _experiment_view(conn, experiment_id)


async def end_experiment(pool, experiment_id: int) -> dict:
    """End an experiment: stop new assignments, retire existing ones (they stay
    queryable), and free the bucket range for other experiments."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            exp = await conn.fetchrow(
                "SELECT id, status FROM experiments WHERE id = $1 FOR UPDATE",
                experiment_id,
            )
            if exp is None:
                raise HTTPException(404, f"experiment {experiment_id} not found")
            if exp["status"] == "ended":
                return {
                    "id": experiment_id,
                    "status": "ended",
                    "already_ended": True,
                    "deactivated_assignments": 0,
                }
            if exp["status"] != "active":
                raise HTTPException(
                    409, f"cannot end experiment in status '{exp['status']}'"
                )
            await conn.execute(
                "UPDATE experiments SET status = 'ended', updated_at = now() "
                "WHERE id = $1",
                experiment_id,
            )
            result = await conn.execute(
                "UPDATE assignments SET effective = FALSE "
                "WHERE experiment_id = $1 AND effective",
                experiment_id,
            )
            deactivated = int(result.split()[-1])
            return {
                "id": experiment_id,
                "status": "ended",
                "already_ended": False,
                "deactivated_assignments": deactivated,
            }


# ---------------------------------------------------------------------------
# assignment
# ---------------------------------------------------------------------------

async def assign(pool, body) -> dict:
    async with pool.acquire() as conn:
        layer = await conn.fetchrow(
            "SELECT id, key, salt FROM layers WHERE key = $1", body.layer_key
        )
        if layer is None:
            raise HTTPException(404, f"layer '{body.layer_key}' not found")
        bucket = bucket_for(layer["salt"], body.user_key)

        # An effective assignment persists: config changes only affect users
        # who have not been assigned yet.
        existing = await _current_assignment(conn, layer["id"], body.user_key)
        if existing is not None:
            return _assigned_payload(existing)

        async with conn.transaction():
            candidate = await conn.fetchrow(
                """SELECT id FROM experiments
                   WHERE layer_id = $1 AND status = 'active'
                     AND bucket_start <= $2 AND bucket_end > $2
                   ORDER BY id LIMIT 1""",
                layer["id"],
                bucket,
            )
            if candidate is None:
                return _unassigned_payload(body, bucket, "no_experiment_covers_bucket")
            # Lock the experiment row: serializes with END, so an assignment
            # can never stay effective after its experiment has ended.
            exp = await conn.fetchrow(
                """SELECT id, key, version, bucket_start, bucket_end, conditions, status
                   FROM experiments WHERE id = $1 FOR UPDATE""",
                candidate["id"],
            )
            if exp["status"] != "active":
                return _unassigned_payload(body, bucket, "no_experiment_covers_bucket")
            if not _conditions_met(exp["conditions"], body.attributes):
                return _unassigned_payload(
                    body, bucket, "conditions_not_met", experiment=exp
                )
            groups = await conn.fetch(
                "SELECT name, weight FROM experiment_groups "
                "WHERE experiment_id = $1 ORDER BY seq",
                exp["id"],
            )
            group_name = pick_group(layer["salt"], exp["id"], body.user_key, groups)
            snapshot = {
                "layer_key": layer["key"],
                "salt": layer["salt"],
                "experiment_id": exp["id"],
                "experiment_key": exp["key"],
                "experiment_version": exp["version"],
                "bucket_start": exp["bucket_start"],
                "bucket_end": exp["bucket_end"],
                "interval": f"[{exp['bucket_start']}, {exp['bucket_end']})",
                "conditions": exp["conditions"],
                "groups": [dict(g) for g in groups],
                "user_bucket": bucket,
                "group": group_name,
            }
            try:
                async with conn.transaction():  # savepoint
                    await conn.fetchrow(
                        """INSERT INTO assignments
                               (layer_id, experiment_id, user_key, bucket,
                                group_name, snapshot)
                           VALUES ($1, $2, $3, $4, $5, $6)
                           RETURNING id""",
                        layer["id"],
                        exp["id"],
                        body.user_key,
                        bucket,
                        group_name,
                        snapshot,
                    )
            except asyncpg.UniqueViolationError:
                # A concurrent assign for the same (layer, user) committed
                # first; fall through and return the winning assignment.
                pass
            winner = await _current_assignment(conn, layer["id"], body.user_key)
            if winner is None:
                raise HTTPException(409, "assignment raced; please retry")
            return _assigned_payload(winner)


async def get_assignment(pool, assignment_id: int) -> dict:
    async with pool.acquire() as conn:
        a = await conn.fetchrow(
            """SELECT a.id, a.user_key, a.bucket, a.group_name, a.snapshot,
                      a.effective, a.created_at, e.status AS experiment_status
               FROM assignments a JOIN experiments e ON e.id = a.experiment_id
               WHERE a.id = $1""",
            assignment_id,
        )
    if a is None:
        raise HTTPException(404, f"assignment {assignment_id} not found")
    payload = _assigned_payload(a)
    payload["effective"] = a["effective"]
    payload["experiment_status"] = a["experiment_status"]
    return payload


async def get_current_assignment(pool, layer_key: str, user_key: str) -> dict:
    async with pool.acquire() as conn:
        layer = await conn.fetchrow(
            "SELECT id FROM layers WHERE key = $1", layer_key
        )
        if layer is None:
            raise HTTPException(404, f"layer '{layer_key}' not found")
        a = await _current_assignment(conn, layer["id"], user_key)
        if a is None:
            return {
                "assigned": False,
                "layer_key": layer_key,
                "user_key": user_key,
            }
        return _assigned_payload(a)


# ---------------------------------------------------------------------------
# exposures
# ---------------------------------------------------------------------------

async def register_exposure(pool, body) -> tuple[dict, int]:
    """Register an exposure. Returns (payload, http_status).

    * unknown assignment            -> 404
    * ended (non-effective) assign. -> 409
    * same event_no + same content  -> 200, idempotent
    * same event_no + other content -> 409
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            a = await conn.fetchrow(
                "SELECT id, effective FROM assignments WHERE id = $1 FOR UPDATE",
                body.assignment_id,
            )
            if a is None:
                raise HTTPException(
                    404, f"assignment {body.assignment_id} not found"
                )
            if not a["effective"]:
                raise HTTPException(
                    409,
                    "assignment is no longer effective (experiment ended); "
                    "exposure rejected",
                )
            row = await conn.fetchrow(
                """INSERT INTO exposures (assignment_id, event_no, content)
                   VALUES ($1, $2, $3)
                   ON CONFLICT (assignment_id, event_no) DO NOTHING
                   RETURNING id, created_at""",
                body.assignment_id,
                body.event_no,
                body.content,
            )
            if row is not None:
                return (
                    {
                        "exposure_id": row["id"],
                        "assignment_id": body.assignment_id,
                        "event_no": body.event_no,
                        "deduplicated": False,
                        "created_at": row["created_at"],
                    },
                    201,
                )
            existing = await conn.fetchrow(
                """SELECT id, created_at,
                          (content IS NOT DISTINCT FROM $3::jsonb) AS same
                   FROM exposures
                   WHERE assignment_id = $1 AND event_no = $2""",
                body.assignment_id,
                body.event_no,
                body.content,
            )
            if existing["same"]:
                return (
                    {
                        "exposure_id": existing["id"],
                        "assignment_id": body.assignment_id,
                        "event_no": body.event_no,
                        "deduplicated": True,
                        "created_at": existing["created_at"],
                    },
                    200,
                )
            raise HTTPException(
                409,
                f"event_no {body.event_no} already registered with different "
                "content for this assignment",
            )


async def list_exposures(pool, assignment_id: int) -> dict:
    async with pool.acquire() as conn:
        a = await conn.fetchrow(
            "SELECT id FROM assignments WHERE id = $1", assignment_id
        )
        if a is None:
            raise HTTPException(404, f"assignment {assignment_id} not found")
        rows = await conn.fetch(
            "SELECT id, event_no, content, created_at FROM exposures "
            "WHERE assignment_id = $1 ORDER BY event_no",
            assignment_id,
        )
        return {
            "assignment_id": assignment_id,
            "exposures": [dict(r) for r in rows],
        }
