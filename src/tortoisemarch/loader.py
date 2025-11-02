"""Load and apply all migrations to reconstruct the full schema state.

This module scans the migration directory for migration files, imports them,
executes their `Migration.mutate_state(state)` (classmethod) in order, and
returns a `ProjectState` representing the schema after all migrations.
"""

import importlib.util
import re
import sys
from collections.abc import Iterable
from pathlib import Path
from types import ModuleType

from tortoisemarch.exceptions import DiscoveryError, InvalidMigrationError
from tortoisemarch.model_state import ProjectState

__all__ = [
    "apply_migration_to_state",
    "import_module_from_path",
    "iter_migration_files",
    "load_migration_state",
]

# Matches files like "0001_initial.py" and captures the number as group(1)
_MIGRATION_RE = re.compile(r"^(\d{4})_.*\.py$")


def import_module_from_path(file_path: Path, module_name: str) -> ModuleType:
    """Import a module from a filesystem path.

    Args:
        file_path: Path to the Python module (.py).
        module_name: Unique module name to register in sys.modules.

    Returns:
        The imported module object.

    Raises:
        DiscoveryError: If import machinery cannot load/execute the module.

    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:  # pragma: no cover - extremely rare
        msg = f"Cannot load spec for module '{module_name}' from '{file_path}'."
        raise DiscoveryError(msg)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        msg = (
            f"Error importing migration module '{module_name}' "
            "from '{file_path}': {exc}"
        )
        raise DiscoveryError(msg) from exc
    return module


def apply_migration_to_state(state: ProjectState, module: ModuleType) -> None:
    """Apply a migration module to the given state.

    The module must define a `Migration` class with a classmethod
    `mutate_state(state: ProjectState) -> None`.
    """
    Migration = getattr(module, "Migration", None)  # noqa: N806
    if Migration is None:
        msg = f"Migration module '{module.__name__}' has no 'Migration' class."
        raise InvalidMigrationError(msg)

    mutate = getattr(Migration, "mutate_state", None)
    if mutate is None or not callable(mutate):
        msg = (
            f"'Migration' in module '{module.__name__}' has no callable 'mutate_state'."
        )
        raise InvalidMigrationError(msg)

    # Our BaseMigration.mutate_state is a @classmethod; call it directly on the class.
    mutate(state)


def iter_migration_files(migration_dir: Path) -> Iterable[Path]:
    """Yield migration files in numeric order, validating their names.

    Includes only files matching 'NNNN_*.py' (e.g. '0001_initial.py'), skipping
    '__init__.py'. Raises if non-conforming .py files are present.
    """
    if not migration_dir.exists():
        msg = f"Migrations directory does not exist: {migration_dir}"
        raise DiscoveryError(msg)
    if not migration_dir.is_dir():
        msg = f"Migrations path is not a directory: {migration_dir}"
        raise DiscoveryError(msg)

    numbered: list[tuple[int, Path]] = []
    invalid: list[Path] = []

    for file in migration_dir.glob("*.py"):
        if file.name == "__init__.py":
            continue
        m = _MIGRATION_RE.match(file.name)
        if not m:
            invalid.append(file)
            continue
        number = int(m.group(1))
        numbered.append((number, file))

    if invalid:
        bad = ", ".join(f.name for f in sorted(invalid))
        msg = (
            "Found non-conforming migration filenames. Expected 'NNNN_*.py'. "
            f"Offenders: {bad}"
        )
        raise InvalidMigrationError(msg)

    for _, path in sorted(numbered, key=lambda t: t[0]):
        yield path


def load_migration_state(migration_dir: Path) -> ProjectState:
    """Load the full project state by applying all migration files in order.

    Args:
        migration_dir: Directory containing numbered migration files.

    Returns:
        A `ProjectState` representing the result of applying all migrations.

    Raises:
        DiscoveryError: If directory cannot be read/imported.
        InvalidMigrationError: If a migration module is malformed.

    """
    state = ProjectState()
    for file in iter_migration_files(migration_dir):
        # Make the module name unique and stable for the session
        mod_name = f"tm_mig_{file.stem}"
        module = import_module_from_path(file_path=file, module_name=mod_name)
        apply_migration_to_state(state, module)
    return state
