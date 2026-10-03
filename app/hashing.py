import hashlib

# Each mutex layer owns exactly 10000 buckets, and group weights sum to 10000.
TOTAL_BUCKETS = 10_000


def _hash_int(*parts: str) -> int:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    return int(digest, 16)


def bucket_for(salt: str, user_key: str) -> int:
    """Stable bucket in [0, 10000) for a user within a layer (fixed per-layer salt)."""
    return _hash_int("bucket", salt, user_key) % TOTAL_BUCKETS


def pick_group(salt: str, experiment_id: int, user_key: str, groups) -> str:
    """Stable group pick: hash into [0, 10000), walk cumulative weights (sum = 10000).

    `groups` must be ordered by seq; each item exposes ["name"] and ["weight"].
    """
    value = _hash_int("group", salt, str(experiment_id), user_key) % TOTAL_BUCKETS
    cumulative = 0
    for g in groups:
        cumulative += g["weight"]
        if value < cumulative:
            return g["name"]
    raise ValueError("group weights do not sum to 10000")
