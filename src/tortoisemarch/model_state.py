"""Classes used to represent the project state.

This is used to diff the current state as defined in the code
versus what is reconstructed from the migrations.
"""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class FieldState:
    """Snapshot of a field's schema-relevant properties.

    This captures everything we need to diff and render SQL:
    core flags (null/unique/index/primary_key), string/numeric precision,
    DB column override, and FK metadata when applicable.
    """

    # Core identity
    name: str
    field_type: str  # abstract type name, e.g. "UUIDField", "CharField"

    # Common constraints/defaults
    null: bool = False
    default: Any = None
    unique: bool = False
    index: bool = False
    primary_key: bool = False

    # Optional scalar attributes
    db_column: str | None = None
    max_length: int | None = None
    max_digits: int | None = None
    decimal_places: int | None = None
    auto_now: bool = False
    auto_now_add: bool = False

    # Relational attributes (for FK / O2O; M2M handled via through tables)
    related_table: str | None = None  # table referenced in FK
    related_model: str | None = None  # dotted path of related model (informational)
    to_field: str | None = None  # referenced column (usually 'id')
    on_delete: str | None = None  # normalized ON DELETE action
    referenced_type: str | None = None  # 'IntField'/'UUIDField'/... (strict set)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict (stable for migrations/state files)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FieldState":
        """Rehydrate a FieldState from a dict."""
        return cls(**data)

    @property
    def options(self) -> dict[str, Any]:
        """Return only schema options (exclude identity keys).

        Notes:
            `field_type` is intentionally excluded. When callers need it, they can
            add it back via an explicit wrapper (e.g., `_options_with_type` in differ).

        """
        output = asdict(self)
        output.pop("name", None)
        output.pop("field_type", None)
        return output


@dataclass
class ModelState:
    """Represents a single model's schema during diffing."""

    name: str
    db_table: str
    field_states: dict[str, FieldState]
    meta: dict[str, Any] | None = None  # reserved for future per-model options

    def to_dict(self) -> dict[str, Any]:
        """Serialize the model state to a plain dict (including nested fields)."""
        return {
            "name": self.name,
            "db_table": self.db_table,
            "field_states": {k: v.to_dict() for k, v in self.field_states.items()},
            "meta": self.meta or {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelState":
        """Rehydrate a ModelState from a dict (including nested FieldStates)."""
        fs = {k: FieldState.from_dict(v) for k, v in data["field_states"].items()}
        return cls(
            name=data["name"],
            db_table=data["db_table"],
            field_states=fs,
            meta=data.get("meta") or {},
        )


@dataclass
class ProjectState:
    """Represents the complete set of models in the project for diffing."""

    model_states: dict[str, ModelState] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the whole project state to a dict."""
        return {
            "model_states": {k: v.to_dict() for k, v in self.model_states.items()},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProjectState":
        """Rehydrate a ProjectState from a dict."""
        ms = {k: ModelState.from_dict(v) for k, v in data["model_states"].items()}
        return cls(model_states=ms)

    def get_model(self, name: str) -> ModelState:
        """Return the ModelState by name or raise a helpful error."""
        try:
            return self.model_states[name]
        except KeyError as error:
            msg = f"Model '{name}' not found in project state."
            raise ValueError(msg) from error
