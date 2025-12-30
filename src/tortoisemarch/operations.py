"""Define schema migration operations.

Each operation describes *what* should change in the schema (e.g., create a
table, add/alter/rename a column). The concrete execution and SQL rendering
are delegated to the active `SchemaEditor` (e.g., Postgres).
"""

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.schema_filtering import (
    compact_opts_for_code,
    is_schema_field_type,
    column_sort_key,
)


def _lc(name: str) -> str:
    """Normalize a field name key for field_states."""
    return name.lower()


def _changed_only(old: dict, new: dict) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return Python code to recreate this operation."""
    keys = set(old) | set(new)
    old_out: dict[str, Any] = {}
    new_out: dict[str, Any] = {}
    for k in keys:
        if old.get(k) != new.get(k):
            old_out[k] = old.get(k)
            new_out[k] = new.get(k)
    return old_out, new_out


class Operation(ABC):
    """Base class for all schema migration operations."""

    @abstractmethod
    async def apply(self, conn, schema_editor) -> None:
        """Apply this operation using the provided `SchemaEditor`."""

    @abstractmethod
    async def unapply(self, conn, schema_editor) -> None:
        """Reverse this operation, when possible."""

    @abstractmethod
    def mutate_state(self, state: ProjectState) -> None:
        """Update the in-memory `ProjectState` to reflect this operation."""

    @abstractmethod
    def to_code(self) -> str:
        """Return a Python expression that reconstructs this operation."""

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL statements for this operation (no execution).

        Concrete operations override this to return a list of SQL strings.
        """
        _ = conn, schema_editor
        msg = f"{self.__class__.__name__} has no SQL renderer"
        raise NotImplementedError(msg)


# ---------------------------------------------------------------------------


@dataclass
class CreateModel(Operation):
    """Create a new table with the specified fields."""

    name: str
    db_table: str
    # fields is of the form [(field_name, field_type, options_dict), ...]
    fields: list[tuple[str, str, dict[str, Any]]]

    async def apply(self, conn, schema_editor) -> None:
        """Create the table."""
        await schema_editor.create_model(conn, self.db_table, self.fields)

    async def unapply(self, conn, schema_editor) -> None:
        """Drop the created table."""
        await schema_editor.drop_model(conn, self.db_table)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to create the table."""
        _ = conn
        return [schema_editor.sql_create_model(self.db_table, self.fields)]

    @classmethod
    def from_model_state(cls, model_state: ModelState) -> "CreateModel":
        """Set up the CreateModel operation from a ModelState."""
        field_list = list(model_state.field_states.values())

        # Exclude non-schema field types (reverse relations, M2M sentinels, etc.)
        field_list = [fs for fs in field_list if is_schema_field_type(fs.field_type)]

        field_list.sort(key=column_sort_key)

        fields: list[tuple[str, str, dict[str, Any]]] = [
            (fs.name, fs.field_type, compact_opts_for_code(dict(fs.options)))
            for fs in field_list
        ]
        return cls(name=model_state.name, db_table=model_state.db_table, fields=fields)

    def mutate_state(self, state: ProjectState) -> None:
        """Add the model and its fields to the in-memory state."""
        field_states: dict[str, FieldState] = {}
        for fname, ftype, opts in self.fields:
            field_states[_lc(fname)] = FieldState(
                name=fname,
                field_type=ftype,
                **dict(opts),  # shallow copy; values are primitives
            )

        state.model_states[self.name] = ModelState(
            name=self.name,
            db_table=self.db_table or self.name.lower(),
            field_states=field_states,
        )

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        safe_fields = [
            (name, ftype, compact_opts_for_code(dict(opts)))
            for (name, ftype, opts) in self.fields
        ]
        return (
            f"CreateModel(name={self.name!r}, "
            f"db_table={self.db_table!r}, "
            f"fields={safe_fields!r})"
        )


# ---------------------------------------------------------------------------


@dataclass
class RemoveModel(Operation):
    """Drop an existing model's table from the database and state."""

    name: str  # logical model name (for state)
    db_table: str  # actual DB table to drop (for DDL)

    async def apply(self, conn, schema_editor) -> None:
        """Drop the table."""
        await schema_editor.drop_model(conn, self.db_table)

    async def unapply(self, conn, schema_editor) -> None:
        """Raise an exception as removing a model is irreversible."""
        _ = conn, schema_editor
        msg = "RemoveModel cannot be reversed"
        raise RuntimeError(msg)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to drop the table."""
        _ = conn
        return [schema_editor.sql_drop_model(self.db_table)]

    def mutate_state(self, state: ProjectState) -> None:
        """Remove the model from the in-memory state."""
        state.model_states.pop(self.name, None)

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        return f"RemoveModel(name={self.name!r}, db_table={self.db_table!r})"


# ---------------------------------------------------------------------------


@dataclass
class AddField(Operation):
    """Add a new field (column) to an existing model."""

    model_name: str
    db_table: str
    field_name: str
    field_type: str
    options: dict[str, Any]

    async def apply(self, conn, schema_editor) -> None:
        """Add the column to the table."""
        await schema_editor.add_field(
            conn,
            self.db_table,
            self.field_name,
            self.field_type,
            self.options,
        )

    async def unapply(self, conn, schema_editor) -> None:
        """Drop the column that was added."""
        await schema_editor.remove_field(conn, self.db_table, self.field_name)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to add the column."""
        _ = conn
        return [
            schema_editor.sql_add_field(
                self.db_table,
                self.field_name,
                self.field_type,
                self.options,
            ),
        ]

    def mutate_state(self, state: ProjectState) -> None:
        """Add/define the field in the in-memory state."""
        model = state.model_states[self.model_name]
        model.field_states[_lc(self.field_name)] = FieldState(
            name=self.field_name,
            field_type=self.field_type,
            **dict(self.options),
        )

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        opts = compact_opts_for_code(dict(self.options))
        return (
            f"AddField(model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, "
            f"field_name={self.field_name!r}, "
            f"field_type={self.field_type!r}, "
            f"options={opts!r})"
        )


# ---------------------------------------------------------------------------


@dataclass
class RemoveField(Operation):
    """Remove a field (column) from an existing model."""

    model_name: str
    db_table: str
    field_name: str

    async def apply(self, conn, schema_editor) -> None:
        """Drop the column from the table."""
        await schema_editor.remove_field(conn, self.db_table, self.field_name)

    async def unapply(self, conn, schema_editor) -> None:
        """Raise an exception as this is irreversible."""
        _ = conn, schema_editor
        msg = "RemoveField cannot be reversed."
        raise RuntimeError(msg)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to drop the column."""
        _ = conn
        return [schema_editor.sql_remove_field(self.db_table, self.field_name)]

    def mutate_state(self, state: ProjectState) -> None:
        """Remove the field from the in-memory state."""
        model = state.model_states[self.model_name]
        model.field_states.pop(_lc(self.field_name), None)

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        return (
            f"RemoveField(model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, "
            f"field_name={self.field_name!r})"
        )


# ---------------------------------------------------------------------------


@dataclass
class AlterField(Operation):
    """Modify an existing field by changing type/constraints/default and/or renaming."""

    model_name: str
    db_table: str
    field_name: str
    old_options: dict[str, Any]
    new_options: dict[str, Any]
    new_name: str | None = None  # if set, treat as rename + alter

    async def apply(self, conn, schema_editor) -> None:
        """Apply the alterations (and optional rename) to the column."""
        await schema_editor.alter_field(
            conn,
            self.db_table,
            self.field_name,
            self.old_options,
            self.new_options,
            new_name=self.new_name,
        )

    async def unapply(self, conn, schema_editor) -> None:
        """Revert the alterations (and optional rename) on the column."""
        await schema_editor.alter_field(
            conn,
            self.db_table,
            self.new_name or self.field_name,
            self.new_options,
            self.old_options,
            new_name=self.field_name if self.new_name else None,
        )

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL statements to alter (and optionally rename) the column."""
        _ = conn
        return schema_editor.sql_alter_field(
            self.db_table,
            self.field_name,
            self.old_options,
            self.new_options,
            new_name=self.new_name,
        )

    def mutate_state(self, state: ProjectState) -> None:
        """Update the field's state with new options and handle rename."""
        model = state.model_states[self.model_name]

        src_key = _lc(self.field_name)
        dst_name = self.new_name or self.field_name
        dst_key = _lc(dst_name)

        prev = model.field_states.get(src_key)

        # Determine final field_type: prefer explicit type, then previous,
        #   then old_options, then a safe default.
        new_type = (
            self.new_options.get("type")
            or (prev.field_type if prev else None)
            or self.old_options.get("type")
            or "TextField"
        )

        # Strip internal 'type' key from options when building FieldState.
        clean_new_opts = {k: v for k, v in self.new_options.items() if k != "type"}

        new_fs = FieldState(
            name=dst_name,
            field_type=new_type,
            **clean_new_opts,
        )

        if self.new_name and src_key in model.field_states:
            model.field_states.pop(src_key)

        model.field_states[dst_key] = new_fs

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        old_opts = dict(self.old_options)
        new_opts = dict(self.new_options)

        old_changed, new_changed = _changed_only(old_opts, new_opts)
        old_changed["type"] = old_opts["type"]
        new_changed["type"] = new_opts["type"]

        # Never compact away 'type' (and keep max_length if CharField)
        def _compact(d: dict) -> dict:
            keep = {"type"}
            if d.get("type") == "CharField":
                keep.add("max_length")
            return {k: v for k, v in d.items() if (v is not None) or (k in keep)}

        old_changed = _compact(old_changed)
        new_changed = _compact(new_changed)
        base = (
            f"AlterField(model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, "
            f"field_name={self.field_name!r}, "
            f"old_options={old_changed!r}, "
            f"new_options={new_changed!r}"
        )
        if self.new_name:
            base += f", new_name={self.new_name!r}"
        base += ")"
        return base


# ---------------------------------------------------------------------------


@dataclass
class RenameField(Operation):
    """Rename a field (column) on an existing model."""

    model_name: str
    db_table: str
    old_name: str
    new_name: str

    async def apply(self, conn, schema_editor) -> None:
        """Rename the column on the table."""
        await schema_editor.rename_field(
            conn,
            self.db_table,
            self.old_name,
            self.new_name,
        )

    async def unapply(self, conn, schema_editor) -> None:
        """Revert the rename on the table."""
        await schema_editor.rename_field(
            conn,
            self.db_table,
            self.new_name,
            self.old_name,
        )

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to rename the column."""
        _ = conn
        return [
            schema_editor.sql_rename_field(
                self.db_table,
                self.old_name,
                self.new_name,
            ),
        ]

    def mutate_state(self, state: ProjectState) -> None:
        """Rename the field inside the in-memory state."""
        model = state.model_states[self.model_name]
        old_key = _lc(self.old_name)
        fs = model.field_states.pop(old_key)
        new_key = _lc(self.new_name)
        model.field_states[new_key] = FieldState(
            name=self.new_name,
            field_type=fs.field_type,
            **dict(fs.options),
        )

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        return (
            f"RenameField(model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, old_name={self.old_name!r}, "
            f"new_name={self.new_name!r})"
        )


@dataclass
class RunPython(Operation):
    """Execute arbitrary Python code during a migration (data-only).

    The callable signature is:

        async def forwards(conn, schema_editor): ...

    or a synchronous function with the same arguments. It may import models and
    use the Tortoise ORM as long as the caller has initialised it.
    """

    func: Callable[[Any, Any], Awaitable[None] | None]
    reverse_func: Callable[[Any, Any], Awaitable[None] | None] | None = None

    async def apply(self, conn, schema_editor) -> None:
        """Run the forwards callable (sync or async)."""
        result = self.func(conn, schema_editor)
        if hasattr(result, "__await__"):
            await result  # async forwards()
        # if sync, it already ran

    async def unapply(self, conn, schema_editor) -> None:
        """Run the reverse callable if provided, else refuse to reverse."""
        if self.reverse_func is None:
            msg = "RunPython operation has no reverse callable"
            raise RuntimeError(msg)

        result = self.reverse_func(conn, schema_editor)
        if hasattr(result, "__await__"):
            await result

    def mutate_state(self, state: ProjectState) -> None:
        """Do nothing as schema state is unchanged."""
        _ = state  # no-op

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """RunPython is a data-only operation, so it emits no SQL."""
        _ = conn, schema_editor
        return []

    def to_code(self) -> str:
        """Return a best-effort code repr for this operation.

        In practice, hand-written migrations will usually write
        `RunPython(forwards)` directly, so this is mostly for debugging.
        """
        fn_name = getattr(self.func, "__name__", repr(self.func))
        if self.reverse_func:
            rev_name = getattr(self.reverse_func, "__name__", repr(self.reverse_func))
            return f"RunPython({fn_name}, reverse_func={rev_name})"
        return f"RunPython({fn_name})"
