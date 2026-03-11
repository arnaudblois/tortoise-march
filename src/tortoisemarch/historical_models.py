"""Build temporary historical Tortoise models from migration state.

We only materialize these models for `RunPython`. They are schema-driven model
classes meant for querying and updating rows against the historical schema, not
for recreating every bit of Python behavior from the original model modules.
"""

from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from types import ModuleType
from uuid import uuid4

from pypika_tortoise import Table
from tortoise import Model, fields
from tortoise.context import get_current_context
from tortoise.fields.relational import OnDelete

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import FieldState, ProjectState
from tortoisemarch.schema_filtering import FK_TYPES
from tortoisemarch.utils import safe_module_fragment


@dataclass(frozen=True)
class HistoricalApps:
    """Lookup wrapper exposed to `RunPython` callables."""

    models_by_key: dict[str, type[Model]]

    def get_model(self, name: str) -> type[Model]:
        """Return a historical model by exact state key or unique short name."""
        model = self.models_by_key.get(name)
        if model is not None:
            return model

        matches = [
            candidate
            for key, candidate in self.models_by_key.items()
            if key.rsplit(".", 1)[-1] == name
        ]
        if len(matches) == 1:
            return matches[0]
        if not matches:
            msg = f"Historical model {name!r} is not available in this migration state."
            raise InvalidMigrationError(msg)

        msg = (
            f"Historical model name {name!r} is ambiguous. "
            "Use the full project-state key instead."
        )
        raise InvalidMigrationError(msg)

    def __getitem__(self, name: str) -> type[Model]:
        """Support dict-style access for convenience."""
        return self.get_model(name)

    def keys(self) -> list[str]:
        """Return the available project-state model keys."""
        return sorted(self.models_by_key)


def _class_names_for_state(state: ProjectState) -> dict[str, str]:
    """Return stable Python class names for each historical model key."""
    counts: dict[str, int] = defaultdict(int)
    class_names: dict[str, str] = {}

    for state_key in state.model_states:
        short_name = (
            safe_module_fragment(state_key.rsplit(".", 1)[-1]) or "HistoricalModel"
        )
        if short_name[:1].isdigit():
            short_name = f"_{short_name}"

        counts[short_name] += 1
        suffix = counts[short_name]
        class_name = short_name if suffix == 1 else f"{short_name}_{suffix}"
        class_names[state_key] = class_name

    return class_names


def _field_common_kwargs(field_state: FieldState) -> dict[str, object]:
    """Return the field kwargs that remain meaningful for historical models."""
    kwargs: dict[str, object] = {
        "null": field_state.null,
        "unique": field_state.unique,
    }
    if field_state.primary_key:
        kwargs["primary_key"] = True
    if field_state.db_column:
        kwargs["source_field"] = field_state.db_column
    if field_state.default not in (None, "python_callable"):
        kwargs["default"] = field_state.default
    return kwargs


SCALAR_FIELD_FACTORIES = {
    "SmallIntField": fields.SmallIntField,
    "IntField": fields.IntField,
    "BigIntField": fields.BigIntField,
    "BooleanField": fields.BooleanField,
    "TextField": fields.TextField,
    "BinaryField": fields.BinaryField,
    "FloatField": fields.FloatField,
    "UUIDField": fields.UUIDField,
    "DateField": fields.DateField,
    "TimeField": fields.TimeField,
    "TimedeltaField": fields.TimeDeltaField,
    "JSONField": fields.JSONField,
}


def _special_scalar_field(
    field_state: FieldState,
    *,
    field_type: str,
    kwargs: dict[str, object],
) -> fields.Field | None:
    """Build scalar field types that need extra state-specific arguments."""
    if field_type == "CharField":
        return fields.CharField(max_length=field_state.max_length or 255, **kwargs)
    if field_type == "DecimalField":
        if field_state.max_digits is None or field_state.decimal_places is None:
            msg = (
                "Historical DecimalField reconstruction requires max_digits and "
                f"decimal_places on {field_state.name!r}."
            )
            raise InvalidMigrationError(msg)
        return fields.DecimalField(
            max_digits=field_state.max_digits,
            decimal_places=field_state.decimal_places,
            **kwargs,
        )
    if field_type == "DatetimeField":
        return fields.DatetimeField(
            auto_now=field_state.auto_now,
            auto_now_add=field_state.auto_now_add,
            **kwargs,
        )
    if field_type == "IntEnumField":
        # We only store the DB shape in migration state, not the original Python
        # enum type, so historical models fall back to the DB representation.
        return fields.IntField(**kwargs)
    if field_type == "CharEnumField":
        return fields.CharField(max_length=field_state.max_length or 255, **kwargs)
    return None


def _scalar_field_from_type(
    field_state: FieldState,
    *,
    field_type: str,
) -> fields.Field:
    """Build a Tortoise field instance for a schema-level scalar field."""
    kwargs = _field_common_kwargs(field_state)
    special_field = _special_scalar_field(
        field_state,
        field_type=field_type,
        kwargs=kwargs,
    )
    if special_field is not None:
        return special_field

    factory = SCALAR_FIELD_FACTORIES.get(field_type)
    if factory is not None:
        return factory(**kwargs)

    msg = (
        "Historical models do not know how to rebuild field type "
        f"{field_type!r} on {field_state.name!r}. "
        "Use simpler data access in that migration or extend the state builder."
    )
    raise InvalidMigrationError(msg)


def _on_delete_value(field_state: FieldState) -> OnDelete:
    """Return the Tortoise `OnDelete` enum for a historical FK/O2O field."""
    raw_value = (field_state.on_delete or "CASCADE").upper().replace(" ", "_")
    try:
        return OnDelete[raw_value]
    except KeyError as error:
        msg = (
            "Unsupported on_delete value "
            f"{field_state.on_delete!r} for historical models."
        )
        raise InvalidMigrationError(msg) from error


def _relation_field(
    field_state: FieldState,
    *,
    app_label: str,
    related_class_name: str,
) -> fields.Field:
    """Build a relational field when the related historical model is known."""
    reference = f"{app_label}.{related_class_name}"
    relation_kwargs: dict[str, object] = {
        "related_name": False,
        "null": field_state.null,
        "on_delete": _on_delete_value(field_state),
    }
    if field_state.db_column:
        relation_kwargs["source_field"] = field_state.db_column
    if field_state.to_field:
        relation_kwargs["to_field"] = field_state.to_field
    if field_state.default not in (None, "python_callable"):
        relation_kwargs["default"] = field_state.default
    if field_state.primary_key:
        relation_kwargs["primary_key"] = True
    if field_state.field_type == "OneToOneFieldInstance":
        relation_kwargs["unique"] = True
        return fields.OneToOneField(reference, **relation_kwargs)
    return fields.ForeignKeyField(reference, **relation_kwargs)


def _field_for_state(
    field_state: FieldState,
    *,
    app_label: str,
    related_models_by_table: dict[str, str],
    class_names: dict[str, str],
) -> fields.Field:
    """Build the Tortoise field for one historical field state."""
    if (
        field_state.field_type not in FK_TYPES
        and field_state.field_type != "OneToOneFieldInstance"
    ):
        return _scalar_field_from_type(field_state, field_type=field_state.field_type)

    related_table = (field_state.related_table or "").lower()
    related_state_key = related_models_by_table.get(related_table)
    if related_state_key is None:
        msg = (
            "Historical relation reconstruction could not resolve the related table "
            f"{field_state.related_table!r} for field {field_state.name!r}."
        )
        raise InvalidMigrationError(msg)

    return _relation_field(
        field_state,
        app_label=app_label,
        related_class_name=class_names[related_state_key],
    )


def _finalise_registered_models(
    app_models: dict[str, type[Model]],
    *,
    connection_name: str,
) -> None:
    """Attach the active DB connection/query state to registered historical models."""
    for model in app_models.values():
        meta = model._meta  # noqa: SLF001
        meta.default_connection = connection_name
        meta.finalise_model()
        meta.basetable = Table(name=meta.db_table, schema=meta.schema)
        basequery = meta.db.query_class.from_(meta.basetable)
        meta.basequery = basequery
        meta.basequery_all_fields = basequery.select(*meta.db_fields)


@contextmanager
def historical_apps_from_state(
    state: ProjectState,
    *,
    connection_name: str = "default",
) -> Iterator[HistoricalApps]:
    """Yield temporary Tortoise models built from the given historical state."""
    ctx = get_current_context()
    if ctx is None or ctx.apps is None:
        msg = "Historical models require an active Tortoise context."
        raise InvalidMigrationError(msg)

    app_label = f"_tortoisemarch_hist_{uuid4().hex}"
    module_name = f"tortoisemarch_historical_{uuid4().hex}"
    module = ModuleType(module_name)
    module.__models__ = []

    class_names = _class_names_for_state(state)
    related_models_by_table = {
        model_state.db_table.lower(): state_key
        for state_key, model_state in state.model_states.items()
    }

    state_key_to_class_name: dict[str, str] = {}
    for state_key, model_state in state.model_states.items():
        class_name = class_names[state_key]
        state_key_to_class_name[state_key] = class_name
        meta = type("Meta", (), {"table": model_state.db_table})
        attrs: dict[str, object] = {"__module__": module_name, "Meta": meta}

        for field_state in model_state.field_states.values():
            attrs[field_state.name] = _field_for_state(
                field_state,
                app_label=app_label,
                related_models_by_table=related_models_by_table,
                class_names=class_names,
            )

        model_class = type(class_name, (Model,), attrs)
        setattr(module, class_name, model_class)
        module.__models__.append(model_class)

    ctx.apps.init_app(app_label, [module], _init_relations=True)
    registered_models = ctx.apps[app_label]
    _finalise_registered_models(registered_models, connection_name=connection_name)

    historical_apps = HistoricalApps(
        models_by_key={
            state_key: registered_models[class_name]
            for state_key, class_name in state_key_to_class_name.items()
        },
    )

    try:
        yield historical_apps
    finally:
        ctx.apps.apps.pop(app_label, None)
