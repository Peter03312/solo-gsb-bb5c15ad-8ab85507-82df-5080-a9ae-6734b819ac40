-- Schema for the anonymous experiment traffic-splitting service.
-- Applied automatically by docker-compose (mounted into /docker-entrypoint-initdb.d).

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- A mutex layer owns 10000 buckets [0, 10000) and a fixed hash salt.
CREATE TABLE IF NOT EXISTS layers (
    id          BIGSERIAL PRIMARY KEY,
    key         TEXT NOT NULL UNIQUE,
    salt        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- An experiment occupies a half-open bucket range [bucket_start, bucket_end)
-- inside one layer, plus attribute match conditions and weighted groups.
CREATE TABLE IF NOT EXISTS experiments (
    id           BIGSERIAL PRIMARY KEY,
    layer_id     BIGINT NOT NULL REFERENCES layers(id),
    key          TEXT NOT NULL,
    version      INTEGER NOT NULL DEFAULT 1,
    bucket_start INTEGER NOT NULL,
    bucket_end   INTEGER NOT NULL,
    conditions   JSONB NOT NULL DEFAULT '{}'::jsonb,
    status       TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'ended')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (layer_id, key),
    CHECK (bucket_start >= 0),
    CHECK (bucket_end <= 10000),
    CHECK (bucket_start < bucket_end)
);

-- Hard guarantee: half-open ranges of ACTIVE experiments in one layer never overlap,
-- even under concurrent publishes. Ended experiments release their range.
ALTER TABLE experiments
    ADD CONSTRAINT experiments_active_no_overlap
    EXCLUDE USING gist (
        layer_id WITH =,
        (int4range(bucket_start, bucket_end, '[)')) WITH &&
    ) WHERE (status = 'active');

CREATE TABLE IF NOT EXISTS experiment_groups (
    id            BIGSERIAL PRIMARY KEY,
    experiment_id BIGINT NOT NULL REFERENCES experiments(id) ON DELETE CASCADE,
    seq           INTEGER NOT NULL,
    name          TEXT NOT NULL,
    weight        INTEGER NOT NULL CHECK (weight >= 0),
    UNIQUE (experiment_id, seq),
    UNIQUE (experiment_id, name)
);

-- A user's assignment in a layer. `effective` rows are the ones currently in force;
-- ending an experiment flips its assignments to effective = FALSE.
CREATE TABLE IF NOT EXISTS assignments (
    id            BIGSERIAL PRIMARY KEY,
    layer_id      BIGINT NOT NULL REFERENCES layers(id),
    experiment_id BIGINT NOT NULL REFERENCES experiments(id),
    user_key      TEXT NOT NULL,
    bucket        INTEGER NOT NULL,
    group_name    TEXT NOT NULL,
    snapshot      JSONB NOT NULL,
    effective     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hard guarantee: at most one EFFECTIVE assignment per (layer, user),
-- even when assign races with experiment end.
CREATE UNIQUE INDEX IF NOT EXISTS assignments_one_effective_per_user
    ON assignments (layer_id, user_key) WHERE effective;

CREATE INDEX IF NOT EXISTS assignments_experiment_idx ON assignments (experiment_id);
CREATE INDEX IF NOT EXISTS assignments_lookup_idx ON assignments (layer_id, user_key);

-- Exposure events, idempotent per (assignment_id, event_no).
CREATE TABLE IF NOT EXISTS exposures (
    id            BIGSERIAL PRIMARY KEY,
    assignment_id BIGINT NOT NULL REFERENCES assignments(id),
    event_no      BIGINT NOT NULL,
    content       JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (assignment_id, event_no)
);
