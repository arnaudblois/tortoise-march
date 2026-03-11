"""Apply unapplied migrations, or render their SQL, for TortoiseMarch."""

from contextlib import asynccontextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any

import asyncpg
import click
from tortoise import Tortoise

from tortoisemarch.base import BaseMigration
from tortoisemarch.conf import resolve_runtime_config
from tortoisemarch.exceptions import (
    ConfigError,
    InvalidMigrationError,
    MigrationConnectionError,
)
from tortoisemarch.loader import (
    apply_migration_to_state,
    import_module_from_path,
    iter_migration_files,
)
from tortoisemarch.model_state import ProjectState
from tortoisemarch.recorder import MigrationRecorder
from tortoisemarch.schema_editor import PostgresSchemaEditor
from tortoisemarch.utils import safe_module_fragment


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


def _prefixed_name(label: str | None, stem: str) -> str:
    """Return a recorder name prefixed with the include label."""
    return f"{label}:{stem}" if label else stem


def migration_checksum(path: Path) -> str:
    """Return the sha256 checksum for a migration file.

    We hash raw bytes so we detect any content edit. This keeps migration
    history immutable once a migration has been recorded as applied.
    """
    return sha256(path.read_bytes()).hexdigest()


def validate_applied_migration_checksums(
    applied_checksums: dict[str, str],
    current_checksums: dict[str, str],
) -> None:
    """Ensure applied migrations still exist and match on-disk checksums."""
    for name, recorded_checksum in applied_checksums.items():
        current_checksum = current_checksums.get(name)
        if current_checksum is None:
            msg = (
                f"Applied migration '{name}' is missing from disk.\n"
                "We treat applied migrations as immutable history, so we cannot "
                "continue safely."
            )
            raise InvalidMigrationError(msg)
        if current_checksum != recorded_checksum:
            msg = (
                f"Checksum mismatch for applied migration '{name}'.\n"
                "The file was modified after being applied. "
                "Create a new migration instead of editing applied history."
            )
            raise InvalidMigrationError(msg)


def plan_route(
    applied: set[str],
    all_names: list[str],
    target_name: str | None,
) -> tuple[str, list[str]]:
    """Compute direction and list of migrations to apply/unapply."""
    current_idx = _current_applied_index(applied, all_names)

    if target_name is None:
        pending = all_names[current_idx + 1 :]
        if not pending:
            return ("noop", [])
        return ("forward", pending)

    target_idx = all_names.index(target_name)

    if target_idx == current_idx:
        return ("noop", [])

    if target_idx > current_idx:
        return ("forward", all_names[current_idx + 1 : target_idx + 1])

    # backward
    return ("backward", list(reversed(all_names[target_idx + 1 : current_idx + 1])))


def _current_applied_index(applied: set[str], all_names: list[str]) -> int:
    """Return the index of the last contiguous applied migration.

    Migration application is strictly ordered. If we find a missing migration
    followed by later applied migrations, recorder history is inconsistent and
    we fail fast instead of silently skipping work.
    """
    current_idx = -1
    for idx, name in enumerate(all_names):
        if name in applied:
            current_idx = idx
            continue

        later_applied = [n for n in all_names[idx + 1 :] if n in applied]
        if later_applied:
            msg = (
                "Applied migration history contains gaps.\n"
                f"Missing migration: {name}\n"
                "Later migrations recorded as applied: "
                + ", ".join(later_applied)
                + "\n"
                "Repair recorder history so applied migrations are a contiguous "
                "prefix before running migrate."
            )
            raise InvalidMigrationError(msg)
        break

    return current_idx


def _load_state_for_names(
    ordered_names: list[str],
    *,
    name_to_file: dict[str, Path],
    name_to_label: dict[str, str | None],
) -> ProjectState:
    """Rebuild the project state for the given ordered migration names."""
    state = ProjectState()
    for index, name in enumerate(ordered_names):
        file = name_to_file[name]
        label = name_to_label[name]
        prefix = safe_module_fragment(label) if label else "main"
        module = import_module_from_path(
            file,
            f"tm_mig_state_{index}_{prefix}_{file.stem}",
        )
        apply_migration_to_state(state, module)
    return state


async def migrate(  # noqa: C901, PLR0912, PLR0913, PLR0915
    tortoise_conf: dict | None = None,
    location: Path | None = None,
    *,
    sql: bool = False,
    fake: bool = False,
    target: str | None = None,
    rewrite_history: bool = False,
) -> str | None:
    """Apply all unapplied migrations in order.

    Args:
        tortoise_conf: Optional Tortoise ORM config. If omitted, read from pyproject.
        location: Migrations directory. If omitted, read from pyproject.
        sql: If True, print and return the SQL that would run (no execution).
        fake: If True, mark migrations as applied/unapplied without running them.
        target: Optional migration name/number to migrate to (forward/backward).
        rewrite_history: If True, clear recorded migration history before
            planning and then re-record from current files while faking. This is
            intended for local development only.

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
    if rewrite_history and not fake:
        msg = "Option --rewrite-history requires --fake."
        raise ConfigError(msg)

    tortoise_conf, location, include_locations = resolve_runtime_config(
        tortoise_conf=tortoise_conf,
        location=location,
    )

    sql_accum: list[str] = []

    try:
        async with tortoise_context(tortoise_conf):
            conn = Tortoise.get_connection("default")
            schema_editor = PostgresSchemaEditor()

            await MigrationRecorder.ensure_table()
            applied_checksums = await MigrationRecorder.list_applied_with_checksums()
            applied = set(applied_checksums)

            sources: list[tuple[str | None, Path]] = [
                (entry["label"], entry["path"]) for entry in include_locations
            ]
            sources.append((None, Path(location)))

            all_names: list[str] = []
            name_to_file: dict[str, Path] = {}
            name_to_label: dict[str, str | None] = {}

            for label, path in sources:
                for file in iter_migration_files(path):
                    name = _prefixed_name(label, file.stem)
                    if name in name_to_file:
                        msg = f"Duplicate migration name detected: {name}"
                        raise InvalidMigrationError(msg)
                    all_names.append(name)
                    name_to_file[name] = file
                    name_to_label[name] = label

            current_checksums = {
                name: migration_checksum(file) for name, file in name_to_file.items()
            }
            if rewrite_history:
                click.secho(
                    "⚠️  Rewriting migration recorder history "
                    "(--rewrite-history). "
                    "Use this only in development.",
                    fg="yellow",
                )
                await MigrationRecorder.clear_all()
                applied_checksums = {}
                applied = set()
            else:
                validate_applied_migration_checksums(
                    applied_checksums,
                    current_checksums,
                )
            target_name = resolve_target_name(target, all_names) if target else None

            direction, names = plan_route(applied, all_names, target_name)
            current_idx = _current_applied_index(applied, all_names)

            if direction == "noop":
                if target_name is None:
                    click.echo("✅ No pending migrations.")
                else:
                    click.echo(f"✅ Already at migration {target_name}")
                return "" if sql else None

            if direction == "forward":
                state = _load_state_for_names(
                    all_names[: current_idx + 1],
                    name_to_file=name_to_file,
                    name_to_label=name_to_label,
                )
                for name in names:
                    file = name_to_file[name]
                    label = name_to_label[name]
                    click.echo(f"🚀 Migration {name}")

                    prefix = safe_module_fragment(label) if label else "main"
                    module = import_module_from_path(
                        file,
                        f"tm_mig_run_{prefix}_{file.stem}",
                    )
                    Migration = getattr(module, "Migration", None)  # noqa: N806
                    if Migration is None:
                        msg = f"Migration file '{file.name}' has no 'Migration' class."
                        raise InvalidMigrationError(msg)

                    if fake:
                        await MigrationRecorder.record_applied(
                            name,
                            current_checksums[name],
                        )
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
                        if issubclass(Migration, BaseMigration):
                            await Migration.apply(
                                tx,
                                schema_editor,
                                state=state,
                                connection_name="default",
                            )
                        else:
                            await Migration.apply(tx, schema_editor)
                        await MigrationRecorder.record_applied(
                            name,
                            current_checksums[name],
                        )
                    click.echo(f"✅ Migration {name} applied")
            else:
                state = _load_state_for_names(
                    all_names[: current_idx + 1],
                    name_to_file=name_to_file,
                    name_to_label=name_to_label,
                )
                for name in names:
                    file = name_to_file[name]
                    label = name_to_label[name]
                    click.echo(f"⏪ Rolling back {name}")

                    prefix = safe_module_fragment(label) if label else "main"
                    module = import_module_from_path(
                        file,
                        f"tm_mig_run_{prefix}_{file.stem}",
                    )
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

                    previous_state = _load_state_for_names(
                        all_names[: all_names.index(name)],
                        name_to_file=name_to_file,
                        name_to_label=name_to_label,
                    )
                    async with conn._in_transaction():  # noqa: SLF001
                        tx = Tortoise.get_connection("default")
                        if issubclass(Migration, BaseMigration):
                            await Migration.unapply(
                                tx,
                                schema_editor,
                                state=state,
                                previous_state=previous_state,
                                connection_name="default",
                            )
                        else:
                            await Migration.unapply(tx, schema_editor)
                        await MigrationRecorder.unrecord_applied(name)
                    state = previous_state
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
