"""Persist and query applied migrations.

`MigrationRecorder` manages a small Postgres table that tracks which migration
files have been applied. It is intentionally minimal: create the table (if
missing), insert/delete rows, list them in a deterministic order, and check
membership.
"""

from typing import ClassVar

from tortoise import Tortoise


def _quote_identifier(ident: str) -> str:
    """Safely quote an SQL identifier for Postgres.

    This is done by doubling internal quotes and wrapping in double quotes.

    Example:
      user -> "user"
      weird"name -> "weird""name"

    This should only use this for identifiers we control. If the identifier
    comes from untrusted user input, it must be validated from a list
    first.

    """
    if not isinstance(ident, str):
        msg = "Identifier must be a string"
        raise TypeError(msg)
    # Minimal validation: no control chars
    if any(c.isspace() for c in ident):
        msg = "Identifier must not contain whitespace"
        raise ValueError(msg)
    # Escape double quotes by doubling them (Postgres rule)
    safe = ident.replace('"', '""')
    return f'"{safe}"'


class MigrationRecorder:
    """Utility for storing migration application state in the database."""

    # Constant table name (quoted in queries to avoid case issues).
    TABLE_NAME: ClassVar[str] = "tortoisemarch_applied_migrations"

    # --------------------------- DDL ----------------------------

    @classmethod
    async def ensure_table(cls) -> None:
        """Create the registry table if it does not already exist.

        Schema:
            id          BIGSERIAL PRIMARY KEY
            name        TEXT NOT NULL UNIQUE      -- e.g. '0001_initial'
            applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        """
        conn = Tortoise.get_connection("default")
        table = _quote_identifier(cls.TABLE_NAME)
        await conn.execute_script(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGSERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
        )

    # ---------------------- DML / queries ----------------------

    @classmethod
    async def list_applied(cls) -> list[str]:
        """Return applied migration names ordered deterministically.

        We order by (applied_at, id) to avoid ties when multiple migrations
        are applied within the same second.
        """
        conn = Tortoise.get_connection("default")
        table = _quote_identifier(cls.TABLE_NAME)
        rows = await conn.execute_query_dict(
            f"SELECT name FROM {table} ORDER BY applied_at, id;",  # noqa:S608
        )
        return [row["name"] for row in rows]

    @classmethod
    async def record_applied(cls, name: str) -> None:
        """Insert a migration name into the registry (idempotent)."""
        conn = Tortoise.get_connection("default")
        table = _quote_identifier(cls.TABLE_NAME)
        await conn.execute_query(
            f"""
            INSERT INTO {table} (name)
            VALUES ($1)
            ON CONFLICT (name) DO NOTHING;
            """,  # noqa:S608
            [name],
        )

    @classmethod
    async def unrecord_applied(cls, name: str) -> None:
        """Remove a migration from the registry (e.g., for manual rollbacks)."""
        conn = Tortoise.get_connection("default")
        table = _quote_identifier(cls.TABLE_NAME)
        await conn.execute_query(
            f"DELETE FROM {table} WHERE name = $1;",  # noqa:S608
            [name],
        )

    @classmethod
    async def is_applied(cls, name: str) -> bool:
        """Return True if the given migration name exists in the registry."""
        conn = Tortoise.get_connection("default")
        table = _quote_identifier(cls.TABLE_NAME)
        rows = await conn.execute_query_dict(
            f"SELECT 1 AS ok FROM {table} WHERE name = $1 LIMIT 1;",  # noqa:S608
            [name],
        )
        return bool(rows)
