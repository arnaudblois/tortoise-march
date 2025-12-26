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

from tortoisemarch.conf import load_config
from tortoisemarch.differ import diff_states, score_candidate
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.extractor import extract_project_state
from tortoisemarch.loader import load_migration_state
from tortoisemarch.writer import write_migration

# Matches files like "0001_initial.py" and captures the number as group(1)
_MIGRATION_RE = re.compile(r"^(\d{4})_.*\.py$")


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
            '"""Directory for TortoiseMarch migrations."""\n', encoding="utf-8"
        )

    tortoise_conf = _tortoise_conf_for_introspection(conf=tortoise_conf)

    # Init the Tortoise apps so extractor can see the models
    await Tortoise.init(config=tortoise_conf)

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
    rename_map = _choose_renames_interactive(old_state, new_state)
    operations = diff_states(old_state, new_state, rename_map=rename_map or {})

    # 5) Write or noop
    if operations:
        click.echo(f"✍️  Writing migration with {len(operations)} operations...")
        write_migration(operations, migrations_dir=location, name=name)
    else:
        click.echo("✅ No changes detected.")

    await Tortoise.close_connections()
