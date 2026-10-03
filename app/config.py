import os

# PostgreSQL DSN. In docker-compose this points at the `db` service.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://exp:exp@localhost:5432/experiments",
)
