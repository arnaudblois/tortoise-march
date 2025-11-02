"""Extract Tortoise ORM models into a ProjectState.

We walk the registered models and convert them into a diff-friendly
representation (ProjectState / ModelState / FieldState) that preserves
schema-critical details (primary_key, lengths/precision, etc) and
captures foreign-key metadata so we can generate correct FK SQL.
"""

import inspect
from typing import Any

from tortoise import Model, Tortoise

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import FieldState, ModelState, ProjectState

# ------------------------------- helpers --------------------------------


def _safe_default(value: Any) -> Any:
    """Normalize field defaults for migration state.

    Callable defaults (e.g. `uuid.uuid4`, `list`, `datetime.now`)
    are replaced with the string 'callable' because function objects
    are not stable across runs and cannot be meaningfully serialized.
    This avoids unreadable migration files and broken diffs.
    """
    return "callable" if callable(value) else value


def _field_type_name(field: Any) -> str:
    """Return a stable field type name (e.g. 'UUIDField', 'CharField')."""
    return field.__class__.__name__


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
            is_model = issubclass(related_model, Model)  # type: ignore[name-defined]
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

    Allowed types: 'SmallIntField', 'IntField', 'BigIntField', 'UUIDField'.

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

    tname = _field_type_name(pk_field)
    allowed = {"SmallIntField", "IntField", "BigIntField", "UUIDField"}
    if tname not in allowed:
        model_label = f"{related_model.__module__}.{related_model.__name__}"
        msg = (
            f"Unsupported FK target type '{tname}' on {model_label}.{to_field}. "
            "Only integer-based fields or UUIDField can be referenced."
        )
        raise InvalidMigrationError(msg)
    return tname


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

    return FieldState(name=name, field_type=field_type, **opts)


def extract_model_state(model_cls: type[Model]) -> ModelState:
    """Convert a Tortoise model class into a ModelState.

    Preserves field order as defined by Tortoise (fields_map is ordered).
    Skips ManyToMany pseudo-fields; their through tables are handled elsewhere.
    """
    field_states: dict[str, FieldState] = {}
    model_cls_meta = model_cls._meta  # noqa: SLF001
    for fname, field in model_cls_meta.fields_map.items():
        if _field_type_name(field) == "ManyToManyFieldInstance":
            continue
        field_states[fname.lower()] = extract_field_state(fname, field)

    return ModelState(
        name=model_cls.__name__,
        db_table=getattr(model_cls_meta, "db_table", model_cls.__name__.lower()),
        field_states=field_states,
    )


def extract_project_state(
    *,
    apps: dict[str, dict[str, type[Model]]] | None = None,
) -> ProjectState:
    """Extract models from Tortoise into a ProjectState."""
    apps = apps or Tortoise.apps
    model_states: dict[str, ModelState] = {}
    for model_cls in apps.get("models", {}).values():
        ms = extract_model_state(model_cls)
        model_states[ms.name] = ms
    return ProjectState(model_states=model_states)
