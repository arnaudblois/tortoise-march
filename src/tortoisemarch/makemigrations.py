"""Generate migration files by diffing current models against migration state.

Workflow:
1) Load config (Tortoise ORM settings + migrations location).
2) Sanity-check existing migration filenames for numeric conflicts.
3) If --empty was requested, write a data-migration stub and exit.
4) Build `old_state` by replaying all migrations; build `new_state` from
   the live Tortoise apps registry; diff them into operations.
5) Write a new numbered migration if there are operations; otherwise print
   “No changes detected.”

Notes:
- Confirmed renames can be passed via `rename_map` to avoid add/remove churn.

"""

import copy
import re
from pathlib import Path
from typing import Any

import click
from tortoise import Tortoise
from tortoise.exceptions import ConfigurationError

from tortoisemarch.conf import load_config
from tortoisemarch.differ import diff_states, score_candidate
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.extractor import extract_project_state
from tortoisemarch.loader import load_migration_state
from tortoisemarch.model_state import ProjectState
from tortoisemarch.operations import AddField, AlterField, CreateModel, RenameModel
from tortoisemarch.schema_filtering import FK_TYPES
from tortoisemarch.writer import write_migration

# Matches files like "0001_initial.py" and captures the number as group(1)
_MIGRATION_RE = re.compile(r"^(\d{4})_.*\.py$")


def _has_db_initialisable_default(opts: dict[str, Any]) -> bool:
    """Return True if the field has a default that the database can apply directly.

    `db_default` is always accepted. Literal `default` values are accepted.
    `None` and callable defaults (sentinel `"python_callable"`) are rejected.
    """
    if "db_default" in opts and opts["db_default"] is not None:
        return True
    if "default" not in opts:
        return False
    default = opts["default"]
    return default not in (None, "python_callable")


def _validate_index_columns(state: ProjectState) -> None:
    """Ensure Meta.indexes/unique_together refer to real fields/columns."""
    problems: list[str] = []
    for ms in state.model_states.values():
        meta = ms.meta or {}
        indexes = meta.get("indexes")
        if not indexes:
            continue

        logical_names = {fs.name.lower() for fs in ms.field_states.values()}
        physical_names: set[str] = set()
        for fs in ms.field_states.values():
            db_column = fs.options.get("db_column")
            if db_column:
                physical_names.add(str(db_column).lower())
            else:
                physical_names.add(fs.name.lower())
            if fs.field_type in FK_TYPES or fs.field_type == "OneToOneFieldInstance":
                physical_names.add(f"{fs.name.lower()}_id")

        for cols, _unique in indexes:
            for col in cols:
                cname = str(col).lower()
                if cname not in logical_names and cname not in physical_names:
                    problems.append(f"{ms.name}.{col}")

    if problems:
        msg = (
            "Invalid index definitions detected (unknown fields/columns):\n"
            "  - " + "\n  - ".join(problems) + "\n\n"
            "Fix Meta.indexes / unique_together to reference existing fields "
            "or explicit db_column names."
        )
        raise InvalidMigrationError(msg)


def _validate_non_nullable_adds_and_warn_alters(  # noqa: C901, PLR0912
    operations: list[Any],
) -> None:
    """Guard unsafe non-nullable schema changes during migration generation.

    - Adding a non-nullable field without a DB-level default to an existing table
      is forbidden and raises `InvalidMigrationError`.
    - Making a nullable field non-nullable without a default prompts for
      confirmation, as it requires a prior data backfill.
    """
    created_models: set[str] = set()
    for op in operations:
        if isinstance(op, CreateModel):
            created_models.add(op.name)

    problems_add: list[str] = []
    risky_alters: list[str] = []

    for op in operations:
        if isinstance(op, AddField):
            if op.model_name in created_models:
                continue
            opts = op.options
            if opts.get("null") is True:
                continue
            if _has_db_initialisable_default(opts):
                continue
            problems_add.append(f"{op.model_name}.{op.field_name}")

        elif isinstance(op, AlterField):
            old_null = op.old_options.get("null")
            new_null = op.new_options.get("null")
            if old_null is True and new_null is False:
                opts = op.new_options
                if not _has_db_initialisable_default(opts):
                    risky_alters.append(f"{op.model_name}.{op.field_name}")

    if problems_add:
        msg = (
            "Cannot generate migration:\n"
            "You added a non-nullable field without a default "
            "(cannot backfill existing rows).\n"
            "Note: Python defaults (e.g. timezone.now) are not database defaults "
            "and cannot backfill existing rows automatically.\n"
            "Fix by adding a default or use this safe sequence:\n"
            "  1) Add the field as nullable.\n"
            "  2) Create a data migration to backfill it "
            "(run: tortoisemarch makemigrations --empty).\n"
            "  3) Make the field non-nullable and re-run makemigrations.\n\n"
            "Problems:\n  - " + "\n  - ".join(problems_add)
        )
        raise InvalidMigrationError(msg)

    if risky_alters:
        click.echo(
            "⚠️  About to make fields NOT NULL without a default.\n"
            "Note: Python defaults (e.g. timezone.now) are not database defaults.\n"
            "This is allowed only if you backfill NULL values first "
            "(typically via a data migration / RunPython).\n\n"
            "Affected fields:\n  - " + "\n  - ".join(risky_alters),
        )
        proceed = _safe_input("Proceed anyway? [y/N]", default=False)
        if not proceed:
            msg = (
                "Migration cancelled.\n"
                "Backfill NULL values first (data migration), "
                "then re-run makemigrations."
            )
            raise InvalidMigrationError(msg)


def _normalize_alter_options(opts: dict[str, Any]) -> dict[str, Any]:
    """Normalize AlterField options for diffing."""
    normalized = dict(opts)
    if "type" not in normalized and "field_type" in normalized:
        normalized["type"] = normalized["field_type"]
    normalized.pop("field_type", None)
    return normalized


def _is_supported_type_change(
    old_type: str | None,
    new_type: str | None,
) -> bool:
    """Return True if a type change is explicitly supported."""
    if not old_type or not new_type:
        return False
    if old_type == new_type:
        return True
    int_rank = {"SmallIntField": 0, "IntField": 1, "BigIntField": 2}
    if old_type in int_rank and new_type in int_rank:
        return int_rank[new_type] > int_rank[old_type]
    return old_type == "CharField" and new_type == "CharField"


def _validate_safe_alters(operations: list[Any]) -> None:  # noqa: C901
    """Reject AlterField ops that change unsupported schema attributes."""
    related_table_renames: set[tuple[str | None, str | None]] = set()
    for op in operations:
        if isinstance(op, RenameModel):
            related_table_renames.add((op.old_db_table, op.new_db_table))

    problems: list[str] = []
    for op in operations:
        if not isinstance(op, AlterField):
            continue

        old_opts = _normalize_alter_options(op.old_options)
        new_opts = _normalize_alter_options(op.new_options)

        diffs = {
            key
            for key in set(old_opts) | set(new_opts)
            if old_opts.get(key) != new_opts.get(key)
        }

        if not diffs:
            continue

        # Allowed no-op metadata changes.
        diffs -= {"auto_now", "auto_now_add", "related_model"}

        old_type = old_opts.get("type")
        new_type = new_opts.get("type")

        if "type" in diffs and _is_supported_type_change(old_type, new_type):
            diffs.remove("type")

        if "max_length" in diffs and old_type == new_type == "CharField":
            diffs.remove("max_length")

        if "related_table" in diffs:
            pair = (old_opts.get("related_table"), new_opts.get("related_table"))
            if pair in related_table_renames:
                diffs.remove("related_table")

        if "db_column" in diffs and op.new_name:
            diffs.remove("db_column")

        # Allowed schema changes handled by SchemaEditor.
        diffs -= {"null", "default", "index"}

        if diffs:
            keys = ", ".join(sorted(diffs))
            problems.append(f"{op.model_name}.{op.field_name} ({keys})")

    if problems:
        msg = (
            "Cannot generate migration:\n"
            "Unsupported AlterField changes detected. Use a custom "
            "RunSQL/RunPython migration for these changes.\n\n"
            "Problems:\n  - " + "\n  - ".join(problems)
        )
        raise InvalidMigrationError(msg)


def _input_int(prompt: str, default: int = 0, max_value: int | None = None) -> int:
    try:
        raw = input(f"{prompt} ").strip()
    except (EOFError, KeyboardInterrupt):
        return default
    if not raw:
        return default
    try:
        val = int(raw, 10)
    except ValueError:
        return default
    if max_value is not None and (val < 0 or val > max_value):
        return default
    return val


def _summarize_opts(opts: dict[str, Any]) -> str:
    keys = [
        "type",
        "null",
        "unique",
        "index",
        "primary_key",
        "max_length",
        "max_digits",
        "decimal_places",
        "db_column",
        "related_table",
        "to_field",
        "on_delete",
        "referenced_type",
    ]
    return ", ".join(
        f"{k}={opts[k]!r}" for k in keys if k in opts and opts[k] is not None
    )


def _detect_conflicts(migrations_dir: Path) -> list[tuple[int, list[str]]]:
    """Detect conflicting migration numbers (same NNNN across multiple files).

    Returns:
        A sorted list of (number, [filenames...]) for any conflicts found.
        Non-numbered .py files are ignored by this check (but should not exist).

    Example:
        [(4, ['0004_add_author.py', '0004_legacy_fix.py'])]

    """
    buckets: dict[int, list[str]] = {}

    for f in migrations_dir.glob("*.py"):
        if f.name == "__init__.py":
            continue
        m = _MIGRATION_RE.match(f.name)
        if not m:
            # Ignore non-conforming files here; writer/loader validate names elsewhere
            continue
        n = int(m.group(1))
        buckets.setdefault(n, []).append(f.name)

    conflicts = [(n, names) for n, names in buckets.items() if len(names) > 1]
    return sorted(conflicts, key=lambda t: t[0])


def _choose_renames_interactive(  # noqa: C901
    from_state,
    to_state,
    *,
    min_score: float = 40.0,
) -> dict[str, dict[str, str]]:
    """Return {model_key: {old_name: new_name}} after interactive selection.

    We:
      1) Build candidate lists (same-type) for each removed field with scores.
      2) Order removed fields by their best candidate score (desc) so we ask
         the most likely first.
      3) For each removed field, first offer a quick Y/n for the best pair,
         then (if declined) show a numbered menu of remaining candidates plus
         0 = none.
    """
    rename_map: dict[str, dict[str, str]] = {}

    for model_key in from_state.model_states.keys() & to_state.model_states.keys():
        old_fields = from_state.model_states[model_key].field_states
        new_fields = to_state.model_states[model_key].field_states

        removed = [n for n in old_fields if n not in new_fields]
        added = [n for n in new_fields if n not in old_fields]
        if not removed or not added:
            continue

        # Build candidates per removed field
        candidates: dict[str, list[tuple[str, float]]] = {}
        for old_name in removed:
            old_fs = old_fields[old_name]
            scored: list[tuple[str, float]] = []
            for new_name in added:
                new_fs = new_fields[new_name]
                s = score_candidate(old_name, old_fs, new_name, new_fs)
                if s >= min_score:
                    scored.append((new_name, s))
            scored.sort(key=lambda t: t[1], reverse=True)
            if scored:
                candidates[old_name] = scored

        if not candidates:
            continue

        # Process removed fields in order of best-guess confidence
        order = sorted(
            candidates.keys(),
            key=lambda name: candidates[name][0][1] if candidates[name] else -1.0,
            reverse=True,
        )
        used_new: set[str] = set()
        model_label = (
            to_state.model_states.get(model_key) or from_state.model_states[model_key]
        ).name

        for old_name in order:
            options = [(n, s) for (n, s) in candidates[old_name] if n not in used_new]
            if not options:
                continue

            best_new, best_score = options[0]
            old_fs = old_fields[old_name]
            summary_old = _summarize_opts({"type": old_fs.field_type, **old_fs.options})
            new_fs = new_fields[best_new]
            summary_new = _summarize_opts({"type": new_fs.field_type, **new_fs.options})
            click.echo(
                f"🔎 [{model_label}] Possible rename: {old_name} → {best_new}  "
                f"(score {best_score:.1f})\n"
                f"    old: {summary_old}\n"
                f"    new: {summary_new}",
            )
            accept = _safe_input("    Accept this rename? [Y/n]", default=True)
            if accept:
                rename_map.setdefault(model_key, {})[old_name] = best_new
                used_new.add(best_new)
                continue

            # Offer menu of remaining candidates (including the best as #1)
            click.echo(f"    Choose target for '{old_name}' (0 = none):")
            for idx, (n, s) in enumerate(options, start=1):
                fs_new = new_fields[n]
                summary = _summarize_opts({"type": fs_new.field_type, **fs_new.options})
                click.echo(f"      {idx}) {n}  (score {s:.1f})  {summary}")
            choice = _input_int("    Enter number", default=0, max_value=len(options))
            if choice:
                chosen = options[choice - 1][0]
                rename_map.setdefault(model_key, {})[old_name] = chosen
                used_new.add(chosen)
    return rename_map


def _safe_input(prompt: str, *, default: bool = False) -> bool:
    """Ask a yes/no question via input(), returning a bool safely.

    Behavior:
      - Returns `default` on empty input, EOF, or Ctrl-C.
      - Accepts: y/yes/1/true/t  -> True
                 n/no/0/false/f  -> False
    """
    try:
        raw = input(f"{prompt} ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        return default

    if not raw:
        return default

    truthy = {"y", "yes", "1", "true", "t"}
    falsy = {"n", "no", "0", "false", "f"}

    if raw in truthy:
        return True
    if raw in falsy:
        return False
    return default


def _tortoise_conf_for_introspection(conf: dict[str, Any]) -> dict[str, Any]:
    """Return a *copy* of the Tortoise config using in-memory SQLite.

    We only need models and metadata for makemigrations, not a real DB.
    So we keep the same apps, but point every connection to sqlite://:memory:.
    """
    conf_copy: dict[str, Any] = copy.deepcopy(conf)
    conns = conf_copy.setdefault("connections", {})
    for name in list(conns.keys()):
        conns[name] = "sqlite://:memory:"
    return conf_copy


def _validate_related_models(
    state: ProjectState,
    *,
    app_labels: set[str],
    module_prefixes: set[str],
) -> None:
    """Ensure related_model strings reference known app labels when dotted."""
    allowed_prefixes = app_labels | module_prefixes
    for ms in state.model_states.values():
        for fs in ms.field_states.values():
            rm = getattr(fs, "related_model", None)
            if not rm or not isinstance(rm, str):
                continue
                if "." not in rm:
                    continue
                prefix = rm.split(".", 1)[0]
                if prefix not in allowed_prefixes:
                    msg = (
                        f"Foreign key {ms.name}.{fs.name} refers to '{rm}', "
                        f"but no app named '{prefix}' is registered. "
                        f"Registered apps: {sorted(app_labels)}. "
                        "Update the FK string to use a valid app label."
                    )
                raise InvalidMigrationError(msg)


async def makemigrations(
    tortoise_conf: dict | None = None,
    location: Path | None = None,
    name: str | None = None,
    *,
    empty: bool = False,
) -> None:
    """Run the makemigrations workflow.

    Args:
        tortoise_conf: Optional Tortoise ORM config mapping. If omitted, it is
            loaded from `pyproject.toml` via `load_config()`.
        location: Optional migrations directory. If omitted, it is loaded from
            `pyproject.toml` (and created by the writer if needed).
        empty: If True, create an empty migration file with a RunPython stub.
        name: Optional suffix for the migration filename (slugified by writer).

    Raises:
        InvalidMigrationError: if conflicting migration numbers are found.

    """
    # 1) Resolve configuration
    if not (tortoise_conf and location):
        config = load_config()
        tortoise_conf = config["tortoise_orm"]
        location = config["location"] or Path("tortoisemarch/migrations")
    location = Path(location)
    location.mkdir(parents=True, exist_ok=True)
    init_file = location / "__init__.py"
    init_file.touch(exist_ok=True)

    if init_file.stat().st_size == 0:
        init_file.write_text(
            '"""Directory for TortoiseMarch migrations."""\n',
            encoding="utf-8",
        )

    tortoise_conf = _tortoise_conf_for_introspection(conf=tortoise_conf)

    # Init the Tortoise apps so extractor can see the models
    try:
        await Tortoise.init(config=tortoise_conf)
    except ConfigurationError as exc:
        await Tortoise._reset_apps()  # noqa: SLF001
        msg = f"Tortoise could not initialise apps due to configuration errors. {exc}"
        raise InvalidMigrationError(msg) from exc

    # 2) Pre-check for conflicts
    pre_conflicts = _detect_conflicts(migrations_dir=location)
    if pre_conflicts:
        lines = [f"  {num:04d}: {', '.join(names)}" for num, names in pre_conflicts]
        msg = "Conflicting migration numbers detected:\n" + "\n".join(lines)
        await Tortoise.close_connections()
        raise InvalidMigrationError(msg)

    # 3) Empty migration scaffold
    if empty:
        click.echo("✍️  Creating empty migration...")
        write_migration([], migrations_dir=location, name=name, empty=True)
        await Tortoise.close_connections()
        return

    # 4) Compute operations by diffing old vs new state
    old_state = load_migration_state(migration_dir=location)
    new_state = extract_project_state(apps=Tortoise.apps)
    module_prefixes: set[str] = set()
    for models in Tortoise.apps.values():
        for cls in models.values():
            mod = getattr(cls, "__module__", "")
            if mod:
                module_prefixes.add(mod.split(".", 1)[0])

    _validate_related_models(
        new_state,
        app_labels=set(Tortoise.apps.keys()),
        module_prefixes=module_prefixes,
    )
    _validate_index_columns(new_state)
    rename_map = _choose_renames_interactive(old_state, new_state)
    operations = diff_states(old_state, new_state, rename_map=rename_map or {})
    _validate_non_nullable_adds_and_warn_alters(operations)
    _validate_safe_alters(operations)

    # 5) Write or noop
    if operations:
        click.echo(f"✍️  Writing migration with {len(operations)} operations...")
        write_migration(operations, migrations_dir=location, name=name)
    else:
        click.echo("✅ No changes detected.")

    await Tortoise.close_connections()
