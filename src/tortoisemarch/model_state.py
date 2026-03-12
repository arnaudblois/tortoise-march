"""Classes used to represent the project state.

This state is the single source of truth that powers diffing, migration replay,
and migration code generation. We keep model-level schema objects explicit so we
can preserve semantics like named constraints instead of collapsing everything
into a generic metadata bucket.
"""

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

from tortoisemarch.constraints import (
    ExclusionExpression,
    exclusion_expressions_to_dict,
    normalize_exclusion_expressions,
)
from tortoisemarch.extensions import (
    PostgresExtension,
    normalize_postgres_extensions,
)


class ConstraintKind(StrEnum):
    """Supported kinds of model-level constraints."""

    UNIQUE = "unique"
    CHECK = "check"
    EXCLUDE = "exclude"


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


@dataclass(eq=True, frozen=True)
class IndexState:
    """Snapshot of a model-level index.

    We keep index names explicit because unnamed-vs-named is meaningful for
    migration rendering and later rename detection.
    """

    columns: tuple[str, ...]
    name: str | None = None
    unique: bool = False
    index_type: str = ""
    extra: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize the index state to a stable plain dict."""
        return {
            "columns": list(self.columns),
            "name": self.name,
            "unique": self.unique,
            "index_type": self.index_type,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "IndexState":
        """Rehydrate an IndexState from plain data."""
        return cls(
            columns=tuple(data.get("columns", ()) or ()),
            name=data.get("name"),
            unique=bool(data.get("unique", False)),
            index_type=str(data.get("index_type", "")),
            extra=str(data.get("extra", "")),
        )

    @property
    def semantic_key(self) -> tuple[Any, ...]:
        """Return the payload used for semantic equality and rename detection."""
        return (
            self.columns,
            self.unique,
            self.index_type,
            self.extra,
        )


@dataclass(eq=True, frozen=True)
class ConstraintState:
    """Snapshot of a model-level constraint.

    Currently supported kinds:
    - ``unique`` with ``columns``
    - ``check`` with ``check``
    - ``exclude`` with typed ``expressions`` and ``index_type``
    """

    kind: ConstraintKind | str
    name: str | None = None
    columns: tuple[str, ...] = ()
    check: str | None = None
    expressions: tuple[ExclusionExpression, ...] = ()
    index_type: str = ""
    condition: str | None = None

    def _validate_unique(self) -> None:
        """Validate the payload for a unique constraint."""
        if not self.columns:
            msg = "Unique constraints require at least one column."
            raise ValueError(msg)
        if any(
            value
            for value in (
                self.check,
                self.expressions,
                self.index_type,
                self.condition,
            )
        ):
            msg = "Unique constraints cannot carry non-unique payload fields."
            raise ValueError(msg)

    def _validate_check(self) -> None:
        """Validate the payload for a check constraint."""
        if not self.check:
            msg = "Check constraints require a check expression."
            raise ValueError(msg)
        if any(
            value
            for value in (
                self.columns,
                self.expressions,
                self.index_type,
                self.condition,
            )
        ):
            msg = "Check constraints cannot carry column or exclusion payloads."
            raise ValueError(msg)

    def _validate_exclude(self) -> None:
        """Validate the payload for an exclusion constraint."""
        if not self.index_type:
            msg = "Exclusion constraints require an index_type."
            raise ValueError(msg)
        if self.columns or self.check is not None:
            msg = "Exclusion constraints cannot carry unique/check payload fields."
            raise ValueError(msg)

    def __post_init__(self) -> None:
        """Validate that the payload matches the constraint kind."""
        try:
            kind = ConstraintKind(self.kind)
        except ValueError as error:
            msg = f"Unsupported constraint kind: {self.kind!r}"
            raise ValueError(msg) from error
        object.__setattr__(self, "kind", kind)

        if self.kind == ConstraintKind.UNIQUE:
            self._validate_unique()
            return
        if self.kind == ConstraintKind.CHECK:
            self._validate_check()
            return
        object.__setattr__(
            self,
            "expressions",
            normalize_exclusion_expressions(
                self.expressions,
                error_context="Exclusion constraints",
            ),
        )
        self._validate_exclude()

    def to_dict(self) -> dict[str, Any]:
        """Serialize the constraint state to a stable plain dict."""
        data: dict[str, Any] = {
            "kind": self.kind.value,
            "name": self.name,
        }
        if self.columns:
            data["columns"] = list(self.columns)
        if self.check is not None:
            data["check"] = self.check
        if self.expressions:
            data["expressions"] = exclusion_expressions_to_dict(self.expressions)
        if self.index_type:
            data["index_type"] = self.index_type
        if self.condition is not None:
            data["condition"] = self.condition
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ConstraintState":
        """Rehydrate a ConstraintState from plain data."""
        return cls(
            kind=ConstraintKind(str(data["kind"])),
            name=data.get("name"),
            columns=tuple(data.get("columns", ()) or ()),
            check=data.get("check"),
            expressions=tuple(data.get("expressions", ()) or ()),
            index_type=str(data.get("index_type", "")),
            condition=data.get("condition"),
        )

    @property
    def semantic_key(self) -> tuple[Any, ...]:
        """Return the payload used for semantic equality and rename detection."""
        if self.kind == ConstraintKind.UNIQUE:
            return (self.kind, self.columns)
        if self.kind == ConstraintKind.CHECK:
            return (self.kind, self.check)
        return (self.kind, self.expressions, self.index_type, self.condition)

    def to_code(self) -> str:
        """Render a readable Python constructor expression for migration code."""
        parts = [f"kind={self.kind.value!r}"]
        if self.name is not None:
            parts.append(f"name={self.name!r}")
        if self.columns:
            parts.append(f"columns={self.columns!r}")
        if self.check is not None:
            parts.append(f"check={self.check!r}")
        if self.expressions:
            parts.append(f"expressions={self.expressions!r}")
        if self.index_type:
            parts.append(f"index_type={self.index_type!r}")
        if self.condition is not None:
            parts.append(f"condition={self.condition!r}")
        return f"ConstraintState({', '.join(parts)})"


@dataclass
class ModelState:
    """Represents a single model's schema during diffing."""

    name: str
    db_table: str
    field_states: dict[str, FieldState]
    indexes: list[IndexState] = field(default_factory=list)
    constraints: list[ConstraintState] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the model state to a plain dict (including nested fields)."""
        return {
            "name": self.name,
            "db_table": self.db_table,
            "field_states": {k: v.to_dict() for k, v in self.field_states.items()},
            "indexes": [index.to_dict() for index in self.indexes],
            "constraints": [constraint.to_dict() for constraint in self.constraints],
            "meta": self.meta,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelState":
        """Rehydrate a ModelState from a dict (including nested FieldStates)."""
        fs = {k: FieldState.from_dict(v) for k, v in data["field_states"].items()}
        return cls(
            name=data["name"],
            db_table=data["db_table"],
            field_states=fs,
            indexes=[
                IndexState.from_dict(value) for value in data.get("indexes", ()) or ()
            ],
            constraints=[
                ConstraintState.from_dict(value)
                for value in data.get("constraints", ()) or ()
            ],
            meta=data.get("meta") or {},
        )


@dataclass
class ProjectState:
    """Represents the complete set of models in the project for diffing."""

    model_states: dict[str, ModelState] = field(default_factory=dict)
    extensions: list[PostgresExtension] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Normalize project-level requirements into a deterministic ordering."""
        self.extensions = normalize_postgres_extensions(
            self.extensions,
            error_context="ProjectState extensions",
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize the whole project state to a dict."""
        return {
            "model_states": {k: v.to_dict() for k, v in self.model_states.items()},
            "extensions": [extension.to_dict() for extension in self.extensions],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProjectState":
        """Rehydrate a ProjectState from a dict."""
        ms = {k: ModelState.from_dict(v) for k, v in data["model_states"].items()}
        return cls(
            model_states=ms,
            extensions=[
                PostgresExtension.from_dict(value)
                for value in data.get("extensions", ()) or ()
            ],
        )

    def get_model(self, name: str) -> ModelState:
        """Return the ModelState by name or raise a helpful error."""
        try:
            return self.model_states[name]
        except KeyError as error:
            msg = f"Model '{name}' not found in project state."
            raise ValueError(msg) from error
