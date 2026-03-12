"""Typed PostgreSQL extension declarations for migration state.

We model extensions explicitly because some Postgres schema objects, such as
GiST-based exclusion constraints on UUID equality, depend on extension-provided
operator classes. Making extensions first-class keeps those dependencies visible
in model metadata, diffing, and generated migrations.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, order=True)
class PostgresExtension:
    """Declare one required PostgreSQL extension."""

    name: str

    def __post_init__(self) -> None:
        """Normalize and validate the extension name."""
        normalized = str(self.name).strip().lower()
        if not normalized:
            msg = "PostgresExtension requires a non-empty extension name."
            raise ValueError(msg)
        object.__setattr__(self, "name", normalized)

    def to_dict(self) -> dict[str, str]:
        """Serialize the extension to a stable plain-data payload."""
        return {"type": "postgres_extension", "name": self.name}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PostgresExtension":
        """Rehydrate an extension from serialized state or migration payloads."""
        return cls(name=str(data.get("name", "")))

    def describe(self) -> dict[str, str]:
        """Return a normalized payload for extractor compatibility."""
        return {"type": "PostgresExtension", "name": self.name}

    def deconstruct(self) -> tuple[str, list[Any], dict[str, str]]:
        """Return a migration-friendly constructor payload."""
        return (
            "tortoisemarch.extensions.PostgresExtension",
            [],
            {"name": self.name},
        )

    def __repr__(self) -> str:
        """Render a constructor-like repr for migration readability."""
        return f"PostgresExtension({self.name!r})"


def normalize_postgres_extension(
    value: PostgresExtension | str | dict[str, Any],
    *,
    error_context: str,
) -> PostgresExtension:
    """Normalize one extension payload into a typed extension object."""
    if isinstance(value, PostgresExtension):
        return value
    if isinstance(value, str):
        return PostgresExtension(value)
    if isinstance(value, dict):
        return PostgresExtension.from_dict(value)

    msg = f"{error_context} must be a PostgresExtension. Got {value!r}."
    raise ValueError(msg)


def normalize_postgres_extensions(
    values: Any,
    *,
    error_context: str,
) -> list[PostgresExtension]:
    """Normalize, deduplicate, and sort extension payloads deterministically."""
    deduped: dict[str, PostgresExtension] = {}
    for value in values or ():
        extension = normalize_postgres_extension(value, error_context=error_context)
        deduped[extension.name] = extension
    return [deduped[name] for name in sorted(deduped)]
