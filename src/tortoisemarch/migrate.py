"""Apply unapplied migrations, or render their SQL, for TortoiseMarch."""

from contextlib import asynccontextmanager
from pathlib import Path

import click
from tortoise import Tortoise

from tortoisemarch.conf import load_config
from tortoisemarch.exceptions import ConfigError, InvalidMigrationError
from tortoisemarch.loader import import_module_from_path, iter_migration_files
from tortoisemarch.recorder import MigrationRecorder
from tortoisemarch.schema_editor import PostgresSchemaEditor


@asynccontextmanager
async def tortoise_context(config: dict):
    """Ensure Tortoise connections open/close safely."""
    await Tortoise.init(config=config)
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

    Raises:
        ConfigError: If options are invalid (e.g., sql and fake together).
        InvalidMigrationError: If a migration module is malformed.

    """
    if sql and fake:
        msg = "Options --sql and --fake are mutually exclusive."
        raise ConfigError(msg)

    # Resolve config
    if not (tortoise_conf and location):
        config = load_config()
        tortoise_conf = config["tortoise_orm"]
        location = config["location"] or Path("tortoisemarch/migrations")
    location = Path(location)

    sql_accum: list[str] = []

    async with tortoise_context(tortoise_conf):
        conn = Tortoise.get_connection("default")
        schema_editor = PostgresSchemaEditor()

        await MigrationRecorder.ensure_table()
        applied = set(await MigrationRecorder.list_applied())

        pending_files = [
            file for file in iter_migration_files(location) if file.stem not in applied
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

            await Migration.apply(conn, schema_editor)
            await MigrationRecorder.record_applied(name)
            click.echo(f"✅ Migration {name} applied")

    return "\n".join(sql_accum) if sql else None
