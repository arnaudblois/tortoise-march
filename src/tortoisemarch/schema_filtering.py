"""Utilities for rendering migration operations as valid Python code.

Includes helpers for sanitising option values, compacting option dictionaries,
and filtering out non-schema field types.
"""

from enum import Enum
from typing import Any

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import FieldState

FK_TYPES = {"ForeignKeyFieldInstance", "OneToOneFieldInstance"}

NON_SCHEMA_FIELD_TYPES: set[str] = {
    # Reverse relations (never stored in DB)
    "BackwardFKRelation",
    "BackwardOneToOneRelation",
    "BackwardManyToManyRelation",
    # Many-to-many fields imply a separate through table
    "ManyToManyFieldInstance",
    "ManyToManyRelation",
}


def _value_for_migration_code(v: Any) -> Any:
    """Return a Python-literal-safe representation of a migration value.

    Normalises enums, recursively sanitises containers, and rejects values
    that cannot be represented deterministically in generated code.
    """
    if isinstance(v, Enum):
        return _value_for_migration_code(v.value)

    if v is None or isinstance(v, (bool, int, float, str)):
        return v

    if isinstance(v, (list, tuple)):
        out = [_value_for_migration_code(x) for x in v]
        return type(v)(out)

    if isinstance(v, dict):
        return {k: _value_for_migration_code(val) for k, val in v.items()}

    # "callable" sentinel is preserved and handled by the schema editor
    if v == "callable":
        return v

    msg = (
        "Unsupported migration value for code generation: "
        f"{v!r} (type={type(v).__name__}). "
        "Hint: if this is an Enum, store .value. "
        "If this is a callable default, represent it as 'callable'.",
    )
    raise InvalidMigrationError(msg)


def compact_opts_for_code(opts: dict[str, Any]) -> dict[str, Any]:
    """Remove non-meaningful options to keep migrations readable."""
    out: dict[str, Any] = {}
    for k, v in opts.items():
        if v is None:
            continue
        if v is False and k != "default":
            continue
        if v in ({}, [], ()):
            continue
        out[k] = _value_for_migration_code(v)
    return out


def is_schema_field_type(field_type: str) -> bool:
    """Return True if the field type represents a physical schema column."""
    return field_type not in NON_SCHEMA_FIELD_TYPES


def column_sort_key(fs: FieldState) -> tuple[int, int, str]:  # noqa: PLR0911
    """Return a stable, human-friendly sort key for CreateModel column ordering."""
    col = (fs.options.get("db_column") or fs.name).lower()

    if fs.options.get("primary_key"):
        return (0, 0, col)

    # Timestamps: created -> updated -> deleted
    if fs.name == "created_at":
        return (1, 0, col)
    if fs.name == "updated_at":
        return (1, 1, col)
    if fs.name == "deleted_at":
        return (1, 2, col)

    # Audit actors: creator -> updater -> deleter
    if fs.name in {"creator", "created_by"}:
        return (2, 0, col)
    if fs.name in {"updater", "updated_by"}:
        return (2, 1, col)
    if fs.name in {"deleter", "deleted_by"}:
        return (2, 2, col)

    if fs.field_type in FK_TYPES:
        return (4, 0, col)

    return (3, 0, col)
