"""End-to-end tests against a running service (default http://127.0.0.1:8000).

Usage:  BASE_URL=http://127.0.0.1:8000 python3 tests/test_e2e.py

Covers: layer/experiment CRUD, publish validation (incl. concurrent publishes),
stable assignment & persistence, config-change isolation, exposure idempotency
and conflicts, experiment end semantics, and assign/end race invariants.
"""

import asyncio
import os
import uuid

import httpx

BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")

_passed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global _passed
    if not cond:
        raise AssertionError(f"FAIL: {name} {detail}")
    _passed += 1
    print(f"  ok: {name}")


def uniq(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


async def main() -> None:
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30) as c:
        # ---- health -------------------------------------------------------
        r = await c.get("/health")
        check("health", r.status_code == 200 and r.json()["status"] == "ok")

        # ---- layer --------------------------------------------------------
        layer = uniq("layer")
        r = await c.post("/layers", json={"key": layer})
        check("create layer", r.status_code == 201 and r.json()["salt"])
        r = await c.post("/layers", json={"key": layer})
        check("duplicate layer -> 409", r.status_code == 409)
        r = await c.get(f"/layers/{layer}")
        check("get layer", r.status_code == 200 and r.json()["key"] == layer)

        # ---- experiment validation ---------------------------------------
        bad = await c.post(
            f"/layers/{layer}/experiments",
            json={
                "key": "bad-weights",
                "bucket_start": 0,
                "bucket_end": 100,
                "groups": [{"name": "a", "weight": 9000}],
            },
        )
        check("weights != 10000 -> 422", bad.status_code == 422, bad.text)

        bad = await c.post(
            f"/layers/{layer}/experiments",
            json={
                "key": "bad-range",
                "bucket_start": 500,
                "bucket_end": 500,
                "groups": [{"name": "a", "weight": 10000}],
            },
        )
        check("empty range -> 422", bad.status_code == 422, bad.text)

        # ---- create + publish --------------------------------------------
        r = await c.post(
            f"/layers/{layer}/experiments",
            json={
                "key": "exp-a",
                "bucket_start": 0,
                "bucket_end": 10000,
                "conditions": {"country": "CN"},
                "groups": [
                    {"name": "control", "weight": 5000},
                    {"name": "treatment", "weight": 5000},
                ],
            },
        )
        check("create exp-a", r.status_code == 201, r.text)
        exp_a = r.json()["id"]
        check("exp-a draft", r.json()["status"] == "draft")
        check("exp-a version 1", r.json()["version"] == 1)

        r = await c.post(
            f"/layers/{layer}/experiments",
            json={
                "key": "exp-overlap",
                "bucket_start": 4000,
                "bucket_end": 9000,
                "groups": [{"name": "only", "weight": 10000}],
            },
        )
        check("overlapping draft allowed", r.status_code == 201, r.text)
        exp_overlap = r.json()["id"]

        r = await c.post(f"/experiments/{exp_a}/publish")
        check("publish exp-a", r.status_code == 200 and r.json()["status"] == "active")
        r = await c.post(f"/experiments/{exp_a}/publish")
        check("re-publish idempotent", r.status_code == 200)

        r = await c.post(f"/experiments/{exp_overlap}/publish")
        check("overlapping publish -> 409", r.status_code == 409, r.text)

        # ---- concurrent publishes: exactly one wins -----------------------
        layer_c = uniq("layerc")
        await c.post("/layers", json={"key": layer_c})
        exp_ids = []
        for i in range(5):
            r = await c.post(
                f"/layers/{layer_c}/experiments",
                json={
                    "key": f"race-{i}",
                    "bucket_start": 0,
                    "bucket_end": 1000,
                    "groups": [{"name": "g", "weight": 10000}],
                },
            )
            exp_ids.append(r.json()["id"])
        results = await asyncio.gather(
            *(c.post(f"/experiments/{eid}/publish") for eid in exp_ids)
        )
        oks = [r for r in results if r.status_code == 200]
        conflicts = [r for r in results if r.status_code == 409]
        check(
            "concurrent publishes: exactly one active",
            len(oks) == 1 and len(conflicts) == 4,
            f"oks={len(oks)} conflicts={len(conflicts)}",
        )

        # ---- assignment: conditions, stability, persistence ---------------
        user1 = uniq("u1")
        r = await c.post(
            "/assign", json={"layer_key": layer, "user_key": user1, "attributes": {}}
        )
        body = r.json()
        check(
            "no matching attrs -> unassigned",
            r.status_code == 200
            and body["assigned"] is False
            and body["reason"] == "conditions_not_met",
            r.text,
        )

        r = await c.post(
            "/assign",
            json={
                "layer_key": layer,
                "user_key": user1,
                "attributes": {"country": "CN", "extra": 1},
            },
        )
        a1 = r.json()
        check(
            "matching attrs -> assigned",
            a1["assigned"] is True
            and a1["experiment_key"] == "exp-a"
            and a1["group"] in ("control", "treatment")
            and 0 <= a1["bucket"] < 10000,
            r.text,
        )
        check(
            "snapshot carries rules",
            a1["snapshot"]["experiment_version"] == 1
            and a1["snapshot"]["conditions"] == {"country": "CN"}
            and sum(g["weight"] for g in a1["snapshot"]["groups"]) == 10000,
        )

        r = await c.post(
            "/assign",
            json={"layer_key": layer, "user_key": user1, "attributes": {"country": "CN"}},
        )
        check(
            "repeat assign -> same assignment",
            r.json()["assigned"] and r.json()["assignment_id"] == a1["assignment_id"],
        )

        # config change: only affects not-yet-assigned users
        r = await c.put(
            f"/experiments/{exp_a}",
            json={
                "bucket_start": 0,
                "bucket_end": 10000,
                "conditions": {"country": "US"},
                "groups": [
                    {"name": "control", "weight": 3000},
                    {"name": "treatment", "weight": 7000},
                ],
            },
        )
        check("update active exp -> version 2", r.json()["version"] == 2, r.text)

        r = await c.post(
            "/assign",
            json={"layer_key": layer, "user_key": user1, "attributes": {"country": "US"}},
        )
        a1b = r.json()
        check(
            "old assignment persists after config change",
            a1b["assignment_id"] == a1["assignment_id"]
            and a1b["snapshot"]["experiment_version"] == 1,
            r.text,
        )

        user2 = uniq("u2")
        r = await c.post(
            "/assign",
            json={"layer_key": layer, "user_key": user2, "attributes": {"country": "US"}},
        )
        a2 = r.json()
        check(
            "new user gets new rules (version 2)",
            a2["assigned"] and a2["snapshot"]["experiment_version"] == 2,
            r.text,
        )

        # concurrent assigns of one user -> single assignment
        user3 = uniq("u3")
        results = await asyncio.gather(
            *(
                c.post(
                    "/assign",
                    json={
                        "layer_key": layer,
                        "user_key": user3,
                        "attributes": {"country": "US"},
                    },
                )
                for _ in range(20)
            )
        )
        ids = {r.json()["assignment_id"] for r in results if r.json()["assigned"]}
        check("concurrent assign -> one assignment id", len(ids) == 1, str(ids))

        # ---- exposures -----------------------------------------------------
        aid = a1["assignment_id"]
        r = await c.post(
            "/exposures",
            json={"assignment_id": aid, "event_no": 1, "content": {"page": "home"}},
        )
        check("exposure created", r.status_code == 201, r.text)
        exp_id_1 = r.json()["exposure_id"]

        r = await c.post(
            "/exposures",
            json={"assignment_id": aid, "event_no": 1, "content": {"page": "home"}},
        )
        check(
            "same content retry -> idempotent",
            r.status_code == 200
            and r.json()["deduplicated"] is True
            and r.json()["exposure_id"] == exp_id_1,
            r.text,
        )

        r = await c.post(
            "/exposures",
            json={"assignment_id": aid, "event_no": 1, "content": {"page": "other"}},
        )
        check("different content -> 409", r.status_code == 409, r.text)

        r = await c.post(
            "/exposures",
            json={"assignment_id": 99999999, "event_no": 1, "content": {}},
        )
        check("unknown assignment -> 404", r.status_code == 404, r.text)

        r = await c.get(f"/assignments/{aid}/exposures")
        check("list exposures", len(r.json()["exposures"]) == 1)

        # ---- end experiment ------------------------------------------------
        r = await c.post(f"/experiments/{exp_a}/end")
        check("end exp-a", r.status_code == 200 and r.json()["status"] == "ended")
        check("ended >= 2 assignments", r.json()["deactivated_assignments"] >= 2)
        r = await c.post(f"/experiments/{exp_a}/end")
        check("re-end idempotent", r.json().get("already_ended") is True)

        r = await c.get(f"/assignments/{aid}")
        check(
            "old assignment still queryable",
            r.status_code == 200
            and r.json()["effective"] is False
            and r.json()["experiment_status"] == "ended",
            r.text,
        )

        r = await c.post(
            "/exposures",
            json={"assignment_id": aid, "event_no": 2, "content": {"page": "home"}},
        )
        check("exposure on ended assignment -> 409", r.status_code == 409, r.text)

        user4 = uniq("u4")
        r = await c.post(
            "/assign",
            json={"layer_key": layer, "user_key": user4, "attributes": {"country": "US"}},
        )
        check(
            "no new assignments after end",
            r.json()["assigned"] is False
            and r.json()["reason"] == "no_experiment_covers_bucket",
            r.text,
        )

        # user can enter another experiment in the same layer
        r = await c.post(
            f"/layers/{layer}/experiments",
            json={
                "key": "exp-b",
                "bucket_start": 0,
                "bucket_end": 10000,
                "groups": [{"name": "only", "weight": 10000}],
            },
        )
        exp_b = r.json()["id"]
        r = await c.post(f"/experiments/{exp_b}/publish")
        check("publish exp-b on freed range", r.status_code == 200, r.text)

        r = await c.post(
            "/assign",
            json={"layer_key": layer, "user_key": user1, "attributes": {}},
        )
        a1_new = r.json()
        check(
            "user re-assigned to new experiment",
            a1_new["assigned"]
            and a1_new["assignment_id"] != aid
            and a1_new["experiment_key"] == "exp-b",
            r.text,
        )
        r = await c.get(f"/layers/{layer}/users/{user1}/assignment")
        check(
            "current assignment is the new one",
            r.json()["assignment_id"] == a1_new["assignment_id"],
        )

        # ---- assign/end race: never two effective assignments --------------
        layer_r = uniq("layerr")
        await c.post("/layers", json={"key": layer_r})
        r = await c.post(
            f"/layers/{layer_r}/experiments",
            json={
                "key": "race-exp",
                "bucket_start": 0,
                "bucket_end": 10000,
                "groups": [{"name": "g", "weight": 10000}],
            },
        )
        exp_r = r.json()["id"]
        await c.post(f"/experiments/{exp_r}/publish")

        user_r = uniq("ur")
        tasks = [
            c.post(
                "/assign",
                json={"layer_key": layer_r, "user_key": user_r, "attributes": {}},
            )
            for _ in range(30)
        ]
        tasks.append(c.post(f"/experiments/{exp_r}/end"))
        results = await asyncio.gather(*tasks)
        assigned_ids = {
            r.json()["assignment_id"]
            for r in results[:-1]
            if r.status_code == 200 and r.json().get("assigned")
        }
        check(
            "race: all assigns agree on one id",
            len(assigned_ids) <= 1,
            str(assigned_ids),
        )
        check("race: end succeeded", results[-1].status_code == 200)
        r = await c.get(f"/layers/{layer_r}/users/{user_r}/assignment")
        check(
            "race: no effective assignment left after end",
            r.json()["assigned"] is False,
            r.text,
        )
        r = await c.post(
            "/assign",
            json={"layer_key": layer_r, "user_key": user_r, "attributes": {}},
        )
        check("race: still unassigned afterwards", r.json()["assigned"] is False)

    print(f"\nAll {_passed} checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
