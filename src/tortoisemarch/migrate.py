"""Apply unapplied migrations, or render their SQL, for TortoiseMarch."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg
import click
from tortoise import Tortoise

from tortoisemarch.conf import load_config
from tortoisemarch.exceptions import (
    ConfigError,
    InvalidMigrationError,
    MigrationConnectionError,
)
from tortoisemarch.loader import import_module_from_path, iter_migration_files
from tortoisemarch.recorder import MigrationRecorder
from tortoisemarch.schema_editor import PostgresSchemaEditor


def _format_db_target(conf: dict | None) -> str:
    """Best-effort extraction of the DB target string for error messages."""
    default_conn: Any = (conf or {}).get("connections", {}).get("default")
    if isinstance(default_conn, str):
        return default_conn
    if isinstance(default_conn, dict):
        host = default_conn.get("host", "localhost")
        port = default_conn.get("port", "5432")
        user = default_conn.get("user", "postgres")
        database = default_conn.get("database", default_conn.get("dbname", ""))
        return f"postgresql://{user}@{host}:{port}/{database}"
    return "<unknown>"


@asynccontextmanager
async def tortoise_context(conf: dict):
    """Init and teardown Tortoise for migrations."""
    await Tortoise.init(config=conf)
    try:
        yield
    finally:
        await Tortoise.close_connections()


async def migrate(
    tortoise_conf: dict | None = None,
    location: Path | None = None,
    *,
    sql: bool = False,
    fake: bool = False,
) -> str | None:
    """Apply all unapplied migrations in order.

    Args:
        tortoise_conf: Optional Tortoise ORM config. If omitted, read from pyproject.
        location: Migrations directory. If omitted, read from pyproject.
        sql: If True, print and return the SQL that would run (no execution).
        fake: If True, mark migrations as applied without running them.

    Returns:
        Concatenated SQL string when `sql=True`, otherwise `None`.

    Exceptions:
        ConfigError: If options are invalid (e.g., sql and fake together).
        InvalidMigrationError: If a migration module is malformed.
        MigrationConnectionError: If the database is unreachable.

    """
    if sql and fake:
        msg = "Options --sql and --fake are mutually exclusive."
        raise ConfigError(msg)

    # Resolve config and migrations location
    if not (tortoise_conf and location):
        config = load_config()
        tortoise_conf = config["tortoise_orm"]
        location = config["location"] or Path("tortoisemarch/migrations")
    location = Path(location)

    sql_accum: list[str] = []

    try:
        async with tortoise_context(tortoise_conf):
            conn = Tortoise.get_connection("default")
            schema_editor = PostgresSchemaEditor()

            await MigrationRecorder.ensure_table()
            applied = set(await MigrationRecorder.list_applied())

            pending_files = [
                file
                for file in iter_migration_files(location)
                if file.stem not in applied
            ]
            if not pending_files:
                click.echo("✅ No pending migrations.")
                return "" if sql else None
            for file in pending_files:
                name = file.stem
                click.echo(f"🚀 Migration {name}")

                module = import_module_from_path(file, f"tm_mig_run_{name}")
                Migration = getattr(module, "Migration", None)  # noqa: N806
                if Migration is None:
                    msg = f"Migration file '{file.name}' has no 'Migration' class."
                    raise InvalidMigrationError(msg)

                if fake:
                    await MigrationRecorder.record_applied(name)
                    click.echo(f"⚡️ Marked {name} as applied (fake)")
                    continue

                if sql:
                    statements = await Migration.to_sql(conn, schema_editor)
                    text = "\n".join(statements)
                    if text:
                        click.echo(text)
                        sql_accum.append(text)
                    click.echo(f"💡 Migration {name} SQL displayed (not executed)")
                    continue

                async with conn._in_transaction():  # noqa: SLF001
                    tx = Tortoise.get_connection("default")
                    await Migration.apply(tx, schema_editor)
                    await MigrationRecorder.record_applied(name)
                click.echo(f"✅ Migration {name} applied")

    except (OSError, asyncpg.PostgresError) as exc:
        target = _format_db_target(tortoise_conf)
        msg = (
            "Could not connect to the database for migrations.\n\n"
            f"Target: {target}\n"
            f"Underlying error: {exc.__class__.__name__}: {exc}\n\n"
            "Check that:\n"
            "  • the PostgreSQL server is running,\n"
            "  • the host/port in your Tortoise config are correct,\n"
            "  • any Docker containers / tunnels are up."
        )
        raise MigrationConnectionError(msg) from exc

    return "\n".join(sql_accum) if sql else None
