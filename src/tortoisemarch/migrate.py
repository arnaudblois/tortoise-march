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


def resolve_target_name(target: str, all_names: list[str]) -> str:
    """Resolve a user-provided target (number or prefix) to a migration name."""
    matches = [n for n in all_names if n.startswith(target)]
    if not matches:
        msg = f"Unknown migration target '{target}'. Available: {', '.join(all_names)}"
        raise InvalidMigrationError(msg)
    if len(matches) > 1:
        msg = f"Ambiguous migration target '{target}'. Matches: {', '.join(matches)}"
        raise InvalidMigrationError(msg)
    return matches[0]


def plan_route(
    applied: set[str],
    all_names: list[str],
    target_name: str | None,
) -> tuple[str, list[str]]:
    """Compute direction and list of migrations to apply/unapply."""
    if target_name is None:
        pending = [n for n in all_names if n not in applied]
        return ("forward", pending)

    applied_order = [n for n in all_names if n in applied]
    current_idx = len(applied_order) - 1  # -1 when none applied
    target_idx = all_names.index(target_name)

    if target_idx == current_idx:
        return ("noop", [])

    if target_idx > current_idx:
        return ("forward", all_names[current_idx + 1 : target_idx + 1])

    # backward
    return ("backward", list(reversed(all_names[target_idx + 1 : current_idx + 1])))


async def migrate(  # noqa: C901, PLR0912, PLR0915
    tortoise_conf: dict | None = None,
    location: Path | None = None,
    *,
    sql: bool = False,
    fake: bool = False,
    target: str | None = None,
) -> str | None:
    """Apply all unapplied migrations in order.

    Args:
        tortoise_conf: Optional Tortoise ORM config. If omitted, read from pyproject.
        location: Migrations directory. If omitted, read from pyproject.
        sql: If True, print and return the SQL that would run (no execution).
        fake: If True, mark migrations as applied/unapplied without running them.
        target: Optional migration name/number to migrate to (forward/backward).

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

            files = list(iter_migration_files(location))
            all_names = [f.stem for f in files]
            name_to_file = {f.stem: f for f in files}
            target_name = None
            if target:
                target_name = resolve_target_name(target, all_names)
            else:
                # default: apply all pending
                pending = [n for n in all_names if n not in applied]
                target_name = pending[-1] if pending else None

            if target_name is None:
                click.echo("✅ No pending migrations.")
                return "" if sql else None

            direction, names = plan_route(applied, all_names, target_name)

            if direction == "noop":
                click.echo(f"✅ Already at migration {target_name}")
                return "" if sql else None

            if direction == "forward":
                for name in names:
                    file = name_to_file[name]
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
                        click.echo(
                            f"💡 Migration {name} SQL displayed (not executed)",
                        )
                        continue

                    async with conn._in_transaction():  # noqa: SLF001
                        tx = Tortoise.get_connection("default")
                        await Migration.apply(tx, schema_editor)
                        await MigrationRecorder.record_applied(name)
                    click.echo(f"✅ Migration {name} applied")
            else:
                for name in names:
                    file = name_to_file[name]
                    click.echo(f"⏪ Rolling back {name}")

                    module = import_module_from_path(file, f"tm_mig_run_{name}")
                    Migration = getattr(module, "Migration", None)  # noqa: N806
                    if Migration is None:
                        msg = f"Migration file '{file.name}' has no 'Migration' class."
                        raise InvalidMigrationError(msg)

                    if sql:
                        # Capture SQL emitted during unapply without executing.
                        class _Recorder:
                            def __init__(self):
                                self.statements: list[str] = []

                            async def execute_script(self, sql_text: str) -> None:
                                for stmt in sql_text.split("\n"):
                                    if stmt.strip():
                                        self.statements.append(stmt)

                            async def execute(self, sql_text: str) -> None:
                                self.statements.append(sql_text)

                        recorder = _Recorder()
                        await Migration.unapply(recorder, schema_editor)
                        sql_accum.extend(recorder.statements)
                        for stmt in recorder.statements:
                            click.echo(stmt)
                        click.echo(f"💡 Rollback {name} SQL displayed (not executed)")
                        continue

                    if fake:
                        await MigrationRecorder.unrecord_applied(name)
                        click.echo(f"⚡️ Marked {name} as unapplied (fake)")
                        continue

                    async with conn._in_transaction():  # noqa: SLF001
                        tx = Tortoise.get_connection("default")
                        await Migration.unapply(tx, schema_editor)
                        await MigrationRecorder.unrecord_applied(name)
                    click.echo(f"✅ Rolled back {name}")

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
