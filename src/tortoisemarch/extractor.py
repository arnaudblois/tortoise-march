"""Extract Tortoise ORM models into a ProjectState.

We walk the registered models and convert them into a diff-friendly
representation (ProjectState / ModelState / FieldState) that preserves
schema-critical details (primary_key, lengths/precision, etc) and
captures foreign-key metadata so we can generate correct FK SQL.
"""

import inspect
from typing import Any

from tortoise import Model, Tortoise, fields
from tortoise.fields.data import CharEnumFieldInstance, IntEnumFieldInstance

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import (
    ConstraintKind,
    ConstraintState,
    FieldState,
    IndexState,
    ModelState,
    ProjectState,
)
from tortoisemarch.schema_filtering import FK_TYPES

# ------------------------------- helpers --------------------------------

CANONICAL_FIELD_TYPES: list[tuple[str, type]] = [
    # Integers
    ("SmallIntField", fields.SmallIntField),
    ("IntField", fields.IntField),
    ("BigIntField", fields.BigIntField),
    # Booleans
    ("BooleanField", fields.BooleanField),
    # Character/text
    ("CharField", fields.CharField),
    ("TextField", fields.TextField),
    # Binary / bytes
    ("BinaryField", fields.BinaryField),
    # Numbers
    ("FloatField", fields.FloatField),
    ("DecimalField", fields.DecimalField),
    # UUID
    ("UUIDField", fields.UUIDField),
    # Date / time
    ("DatetimeField", fields.DatetimeField),
    ("DateField", fields.DateField),
    ("TimeField", fields.TimeField),
    ("TimedeltaField", fields.TimeDeltaField),
    # JSON
    ("JSONField", fields.JSONField),
    # Enums, we use *instance* classes, not the top-level factory
    ("IntEnumField", IntEnumFieldInstance),
    ("CharEnumField", CharEnumFieldInstance),
]

RELATIONAL_SENTINELS = {
    "ForeignKeyFieldInstance",
    "OneToOneFieldInstance",
    "ManyToManyFieldInstance",
    "BackwardFKRelation",
}
EXCLUSION_EXPRESSION_PARTS = 2


def _safe_default(value: Any) -> Any:
    """Normalize field defaults for migration state.

    Callable defaults (e.g. `uuid.uuid4`, `list`, `datetime.now`)
    are replaced with the string 'python_callable' because function objects
    are not stable across runs and cannot be meaningfully serialized.
    This avoids unreadable migration files and broken diffs.
    """
    return "python_callable" if callable(value) else value


def _field_type_name(field: Any) -> str:
    """Return a stable field type name (e.g. 'UUIDField', 'CharField').

    It is more difficult than simply returning field.__class__.__name__
    as it is perfectly possible for the user to have subclassed native
    Tortoise fields.
    """
    raw_name = field.__class__.__name__

    if raw_name in RELATIONAL_SENTINELS:
        return raw_name

    for canonical_name, field_type in CANONICAL_FIELD_TYPES:
        if isinstance(field, field_type):
            return canonical_name

    return raw_name  # custom/unsupported-type fallback


def _is_relational(field: Any) -> bool:
    """Best-effort detection for relational fields."""
    tname = _field_type_name(field)
    return tname in {
        "ForeignKeyFieldInstance",
        "OneToOneFieldInstance",
        "ManyToManyFieldInstance",
    }


def _normalize_on_delete(val: str | None) -> str | None:
    """Normalize on_delete to a common SQL spelling (Postgres-style)."""
    if not val:
        return None
    up = str(val).upper().replace("_", " ")
    aliases = {
        "CASCADE": "CASCADE",
        "SET NULL": "SET NULL",
        "SET DEFAULT": "SET DEFAULT",
        "RESTRICT": "RESTRICT",
        "NO ACTION": "NO ACTION",
        "DO NOTHING": "NO ACTION",  # treat DO NOTHING same as NO ACTION
    }
    return aliases.get(up, up)


def _resolve_related_bits(
    field: Any,
) -> tuple[str | None, str | None, str, str | None]:
    """Return (related_table, related_model_label, to_field, on_delete) for FK.

    - related_table: DB table name of the target model, if resolvable
    - related_model_label: 'pkg.module.ModelName' or raw string if unresolved
    - to_field: target field name (defaults to 'id')
    - on_delete: normalized SQL action or None
    """
    related_table: str | None = None
    related_model_label: str | None = None
    to_field = getattr(field, "to_field", "id")
    on_delete = _normalize_on_delete(getattr(field, "on_delete", None))

    related_model = getattr(field, "related_model", None)
    if related_model is None:
        return related_table, related_model_label, to_field, on_delete

    # Case 1: it's a class, maybe a Tortoise Model subclass
    if inspect.isclass(related_model):
        try:
            is_model = issubclass(related_model, Model)
        except TypeError:
            is_model = False

        if is_model:
            # Table name
            meta = getattr(related_model, "_meta", None)
            table = getattr(meta, "db_table", None)
            if table is None:
                name = getattr(related_model, "__name__", None)
                table = name.lower() if isinstance(name, str) else None
            related_table = table

            # Qualified label
            module = getattr(related_model, "__module__", None)
            name = getattr(related_model, "__name__", None)
            if isinstance(module, str) and isinstance(name, str):
                related_model_label = f"{module}.{name}"
            else:
                related_model_label = str(related_model)
        else:
            # Not a Model subclass (proxy, sentinel, etc.)
            related_model_label = str(related_model)

    else:
        # Likely a lazy string like "app.Model"
        related_model_label = str(related_model)

    return related_table, related_model_label, to_field, on_delete


def _infer_referenced_type(field: Any) -> str | None:
    """Return the referenced PK abstract type name for FK targets.

    Allowed types: SmallIntField, IntField, BigIntField, UUIDField
        and CharField.

    Returns:
        The abstract type of the referenced PK (string) or None if not resolvable.

    Raises:
        InvalidMigrationError: If the referenced PK type is unsupported for FKs.

    """
    related_model = getattr(field, "related_model", None)
    to_field = getattr(field, "to_field", "id")

    if not (
        related_model
        and isinstance(related_model, type)
        and issubclass(related_model, Model)
    ):
        return None

    pk_field = related_model._meta.fields_map.get(to_field)  # noqa: SLF001
    if pk_field is None:
        return None

    reference_fields = (
        ("SmallIntField", fields.SmallIntField),
        ("BigIntField", fields.BigIntField),
        ("IntField", fields.IntField),
        ("UUIDField", fields.UUIDField),
        ("CharField", fields.CharField),
    )
    for name, ref_field in reference_fields:
        if isinstance(pk_field, ref_field):
            return name

    # Fallback: keep the informative error message with the real class name
    tname = _field_type_name(pk_field)
    model_label = f"{related_model.__module__}.{related_model.__name__}"
    msg = (
        f"Unsupported FK target type '{tname}' on {model_label}.{to_field}. "
        "Only integer-based fields or UUIDField can be referenced."
    )
    raise InvalidMigrationError(msg)


# ------------------------------ extractors ------------------------------


def extract_field_state(name: str, field: Any) -> FieldState:
    """Convert a Tortoise field instance into a FieldState with rich options."""
    field_type = _field_type_name(field)

    # Base options common to most fields
    opts: dict[str, Any] = {
        "null": bool(getattr(field, "nullable", getattr(field, "null", False))),
        "default": _safe_default(getattr(field, "default", None)),
        "unique": bool(getattr(field, "unique", False)),
        "index": bool(getattr(field, "index", False)),
        "primary_key": bool(getattr(field, "pk", getattr(field, "primary_key", False))),
    }

    # Column override (Tortoise uses `source_field` for the DB column name)
    if getattr(field, "source_field", None):
        opts["db_column"] = field.source_field or None

    # String / numeric precision
    if hasattr(field, "max_length"):
        opts["max_length"] = field.max_length
    if hasattr(field, "max_digits"):
        opts["max_digits"] = field.max_digits
    if hasattr(field, "decimal_places"):
        opts["decimal_places"] = field.decimal_places

    # Relational fields: capture FK and One-to-One metadata
    # We skip M2M which is handled via through tables.
    if _is_relational(field) and field_type != "ManyToManyFieldInstance":
        related_table, related_model_label, to_field, on_delete = _resolve_related_bits(
            field,
        )
        ref_type = _infer_referenced_type(field)  # may be None if unresolved

        opts.update(
            {
                "related_table": related_table,  # preferred for SQL rendering
                "related_model": related_model_label,  # informational/debug
                "to_field": to_field,
                "on_delete": on_delete,
                "referenced_type": ref_type,  # 'IntField'/'UUIDField'/... or None
            },
        )
        if field_type == "OneToOneFieldInstance":
            # We always persist one-to-one as unique so generated SQL and diffs
            # remain stable even if upstream field metadata changes.
            opts["unique"] = True

    return FieldState(name=name, field_type=field_type, **opts)


def _normalize_columns(columns: Any) -> tuple[str, ...]:
    """Normalize a field/column sequence into lowercase tuple form."""
    if columns is None:
        return ()
    if isinstance(columns, str):
        return (columns.lower(),)
    return tuple(str(column).lower() for column in columns)


def _index_state_from_meta_entry(entry: Any) -> IndexState:
    """Convert a Meta.indexes entry into IndexState."""
    if isinstance(entry, (tuple, list)):
        columns = _normalize_columns(entry)
        if not columns:
            msg = "Meta.indexes cannot contain empty column definitions."
            raise InvalidMigrationError(msg)
        return IndexState(columns=columns)

    columns = _normalize_columns(getattr(entry, "fields", None))
    if not columns:
        msg = (
            "Only field-based indexes are currently supported. "
            f"Could not extract columns from {entry!r}."
        )
        raise InvalidMigrationError(msg)
    return IndexState(
        columns=columns,
        name=getattr(entry, "name", None),
        unique=bool(getattr(entry, "unique", False)),
        index_type=str(
            getattr(entry, "INDEX_TYPE", getattr(entry, "index_type", "")) or "",
        ),
        extra=str(getattr(entry, "extra", "") or ""),
    )


def _constraint_entry_data(entry: Any) -> tuple[str, dict[str, Any]]:
    """Return a normalized constraint class name and payload dict."""
    payload: dict[str, Any] = {}
    class_name = entry.__class__.__name__

    if hasattr(entry, "describe") and callable(entry.describe):
        described = entry.describe()
        if isinstance(described, dict):
            payload.update(described)
            class_name = str(described.get("type") or class_name)

    if hasattr(entry, "deconstruct") and callable(entry.deconstruct):
        path, args, kwargs = entry.deconstruct()
        class_name = path.rsplit(".", 1)[-1]
        payload.update(kwargs)
        if args and "check" not in payload and "condition" not in payload:
            payload["args"] = args

    for key in (
        "fields",
        "name",
        "condition",
        "check",
        "expressions",
        "index_type",
    ):
        if key not in payload and hasattr(entry, key):
            payload[key] = getattr(entry, key)

    return class_name, payload


def _coerce_check_expression(entry: Any, payload: dict[str, Any]) -> str:
    """Extract a stable SQL-ish check expression from a constraint object."""
    candidate = payload.get("check")
    if candidate is None:
        candidate = payload.get("condition")
    if candidate is None and payload.get("args"):
        candidate = payload["args"][0]
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    msg = (
        "CheckConstraint payloads must expose a string `check`/`condition` "
        f"expression. Could not extract one from {entry!r}."
    )
    raise InvalidMigrationError(msg)


def _coerce_exclusion_expressions(
    entry: Any,
    payload: dict[str, Any],
) -> tuple[tuple[str, str], ...]:
    """Extract normalized `(field_or_column, operator)` pairs."""
    raw_expressions = payload.get("expressions")
    if raw_expressions is None:
        raw_expressions = payload.get("fields")

    expressions: list[tuple[str, str]] = []
    for expression in raw_expressions or ():
        if not isinstance(expression, (tuple, list)) or (
            len(expression) != EXCLUSION_EXPRESSION_PARTS
        ):
            msg = (
                "ExclusionConstraint expressions must be "
                "(field_or_column, operator) pairs: "
                f"{entry!r}"
            )
            raise InvalidMigrationError(msg)
        field_name = str(expression[0]).strip().lower()
        operator = str(expression[1]).strip()
        if not field_name or not operator:
            msg = (
                "ExclusionConstraint expressions cannot contain empty values: "
                f"{entry!r}"
            )
            raise InvalidMigrationError(msg)
        expressions.append((field_name, operator))

    if not expressions:
        msg = f"ExclusionConstraint must define expressions: {entry!r}"
        raise InvalidMigrationError(msg)

    return tuple(expressions)


def _constraint_state_from_meta_entry(entry: Any) -> ConstraintState:
    """Convert a Meta.constraints entry into ConstraintState."""
    class_name, payload = _constraint_entry_data(entry)
    name = payload.get("name")
    if "UniqueConstraint" in class_name:
        condition = payload.get("condition")
        if condition:
            msg = (
                "Conditional unique constraints are not supported by the current "
                f"TortoiseMarch contract: {entry!r}"
            )
            raise InvalidMigrationError(msg)
        columns = _normalize_columns(payload.get("fields"))
        if not columns:
            msg = f"UniqueConstraint must define fields: {entry!r}"
            raise InvalidMigrationError(msg)
        return ConstraintState(kind=ConstraintKind.UNIQUE, name=name, columns=columns)

    if "CheckConstraint" in class_name:
        return ConstraintState(
            kind=ConstraintKind.CHECK,
            name=name,
            check=_coerce_check_expression(entry, payload),
        )

    if "ExclusionConstraint" in class_name:
        index_type = str(payload.get("index_type", "") or "").strip().lower()
        if not index_type:
            msg = f"ExclusionConstraint must define index_type: {entry!r}"
            raise InvalidMigrationError(msg)
        condition = payload.get("condition")
        if condition is not None:
            condition = str(condition).strip() or None
        return ConstraintState(
            kind=ConstraintKind.EXCLUDE,
            name=name,
            expressions=_coerce_exclusion_expressions(entry, payload),
            index_type=index_type,
            condition=condition,
        )

    msg = f"Unsupported model constraint type: {class_name}"
    raise InvalidMigrationError(msg)


def _extract_model_indexes(meta: Any, meta_config: Any) -> list[IndexState]:
    """Extract model-level indexes from runtime/meta configuration."""
    raw_indexes = (
        getattr(meta, "indexes", None) or getattr(meta_config, "indexes", None) or ()
    )
    indexes: list[IndexState] = []
    for entry in raw_indexes:
        index = _index_state_from_meta_entry(entry)
        if index not in indexes:
            indexes.append(index)
    return indexes


def _extract_model_constraints(meta: Any, meta_config: Any) -> list[ConstraintState]:
    """Extract model-level constraints and normalize legacy unique_together."""
    constraints: list[ConstraintState] = []

    raw_constraints = list(getattr(meta, "constraints", None) or ())
    if not raw_constraints:
        raw_constraints = list(getattr(meta_config, "constraints", None) or ())
    # We keep TortoiseMarch-only constraints in a namespaced Meta attribute so
    # projects can use Postgres features that Tortoise ORM does not expose yet.
    raw_constraints.extend(
        list(getattr(meta_config, "tortoisemarch_constraints", None) or ()),
    )

    for entry in raw_constraints:
        constraint = _constraint_state_from_meta_entry(entry)
        if constraint not in constraints:
            constraints.append(constraint)

    raw_unique_together = (
        getattr(meta, "unique_together", None)
        or getattr(meta_config, "unique_together", None)
        or ()
    )
    for entry in raw_unique_together:
        columns = _normalize_columns(entry)
        if not columns:
            continue
        constraint = ConstraintState(kind=ConstraintKind.UNIQUE, columns=columns)
        if constraint not in constraints:
            constraints.append(constraint)

    return constraints


def extract_model_state(model_cls: type[Model]) -> ModelState:
    """Convert a Tortoise model class into a ModelState.

    Preserves field order as defined by Tortoise (fields_map is ordered).

    Skips:
        - ManyToMany pseudo-fields (handled via through tables)
        - reverse relations (e.g. BackwardFKRelation)
        - scalar backing fields for FKs (e.g. `author_id` when we already
          have a forward ForeignKeyFieldInstance describing that column)
    """
    field_states: dict[str, FieldState] = {}
    meta = model_cls._meta  # noqa: SLF001

    # First pass: collect DB column names used by forward FK/O2O relations.
    # This lets us later ignore scalar backing fields pointing to the same column.
    fk_backing_columns: set[str] = set()
    for fname, field in meta.fields_map.items():
        tname = _field_type_name(field)
        if tname in FK_TYPES:
            # Tortoise uses `source_field` as the actual DB column; if missing,
            # conventions give `<name>_id`.
            db_column = getattr(field, "source_field", None) or f"{fname}_id"
            fk_backing_columns.add(db_column)

    # Second pass: build FieldState only for real DB fields
    for fname, field in meta.fields_map.items():
        tname = _field_type_name(field)

        # 1) Skip ManyToMany pseudo-fields entirely.
        if tname == "ManyToManyFieldInstance":
            continue

        # 2) Skip obvious reverse relations (no DB column; not a real field).
        #    Tortoise uses BackwardFKRelation and friends for these.
        if tname == "BackwardFKRelation":
            continue

        # 3) Skip scalar backing fields for FK columns if the FK already covers them.
        #    Example: "author_id" (UUIDField) vs "author" (ForeignKeyFieldInstance).
        if fname in fk_backing_columns and not _is_relational(field):
            # We already have a forward relation whose DB column is this name.
            # The FK FieldState will emit the correct column via its source_field.
            continue

        field_states[fname.lower()] = extract_field_state(fname, field)

    meta_config = getattr(model_cls, "Meta", None)

    return ModelState(
        name=model_cls.__name__,
        db_table=getattr(meta, "db_table", model_cls.__name__.lower()),
        field_states=field_states,
        indexes=_extract_model_indexes(meta, meta_config),
        constraints=_extract_model_constraints(meta, meta_config),
    )


def extract_project_state(
    *,
    apps: dict[str, dict[str, type[Model]]] | None = None,
) -> ProjectState:
    """Extract models from Tortoise into a ProjectState."""
    apps = apps or Tortoise.apps or {}
    # Count class names so we can disambiguate duplicates by app label and
    # avoid silently dropping models that share the same class name.
    name_counts: dict[str, int] = {}
    for models in apps.values():
        for model_cls in models.values():
            name = model_cls.__name__
            name_counts[name] = name_counts.get(name, 0) + 1

    model_states: dict[str, ModelState] = {}
    for app_label, models in apps.items():
        for model_cls in models.values():
            ms = extract_model_state(model_cls)
            name = model_cls.__name__
            key = name if name_counts.get(name, 0) == 1 else f"{app_label}.{name}"
            ms.name = key
            model_states[key] = ms
    return ProjectState(model_states=model_states)
