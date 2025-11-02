"""Custom exception types for TortoiseMarch.

All library-specific errors derive from `TortoiseMarchError`. Catch that base
class if you want to handle any TortoiseMarch failure in a single place.
"""

__all__ = [
    "ConfigError",
    "DiscoveryError",
    "InvalidMigrationError",
    "MigrationApplyError",
    "NotReversibleMigrationError",
    "RecorderError",
    "StateError",
    "TortoiseMarchError",
]


class TortoiseMarchError(Exception):
    """Base class for all TortoiseMarch exceptions."""


class ConfigError(TortoiseMarchError):
    """Configuration problem.

    Examples:
        - Missing or invalid `[tool.tortoisemarch]` in pyproject.toml
        - Unable to resolve `tortoise_orm` reference
        - Bad or unsupported configuration values

    """


class InvalidMigrationError(TortoiseMarchError):
    """A migration file is malformed or inconsistent.

    Raised when a migration cannot be parsed, is numbered incorrectly,
    or contains incompatible/unknown operations.
    """


class MigrationApplyError(TortoiseMarchError):
    """Applying (or unapplying) a migration failed at runtime."""


class NotReversibleMigrationError(TortoiseMarchError):
    """Operation cannot be reversed.

    Use this for actions like `RemoveModel` or destructive data ops where a
    reverse step is not well-defined.
    """


class StateError(TortoiseMarchError):
    """Project/model state could not be derived or is invalid."""


class RecorderError(TortoiseMarchError):
    """Failure in the migration recorder (applied-migrations registry)."""


class DiscoveryError(TortoiseMarchError):
    """Failed to discover or import models/migrations/modules."""
