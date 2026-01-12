"""Define schema migration operations.

Each operation describes *what* should change in the schema (e.g., create a
table, add/alter/rename a column). The concrete execution and SQL rendering
are delegated to the active `SchemaEditor` (e.g., Postgres).
"""

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.schema_filtering import (
    _value_for_migration_code,
    column_sort_key,
    compact_opts_for_code,
    is_schema_field_type,
)


def _lc(name: str) -> str:
    """Return a normalized key for field_states lookup."""
    return name.lower()


def _changed_only(old: dict, new: dict) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return key/value pairs that differ between two option dicts."""
    keys = sorted(set(old) | set(new))
    old_out: dict[str, Any] = {}
    new_out: dict[str, Any] = {}
    for k in keys:
        if old.get(k) != new.get(k):
            old_out[k] = old.get(k)
            new_out[k] = new.get(k)
    return old_out, new_out


def _sanitize_options_for_code(opts: dict[str, Any]) -> dict[str, Any]:
    """Normalise option values so they are safe to embed in migration code."""
    clean: dict[str, Any] = {}
    for k, v in opts.items():
        clean[k] = _value_for_migration_code(v)
    return clean


def default_index_name(db_table: str, columns: tuple[str, ...], *, unique: bool) -> str:
    """Return a stable index name given table/columns/uniqueness."""
    cols = "_".join(c.lower() for c in columns)
    suffix = "uniq" if unique else "idx"
    return f"{db_table.lower()}_{cols}_{suffix}"


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
class RenameModel(Operation):
    """Rename a model and its underlying table (if needed).

    This operation updates:
    - the ProjectState key (old model name -> new model name)
    - the ModelState.name
    - the ModelState.db_table
    - FK metadata in other models that points to the renamed table/model
    """

    old_name: str
    new_name: str
    old_db_table: str
    new_db_table: str

    async def apply(self, conn, schema_editor) -> None:
        """Rename the table (and logically the model)."""
        # If only the model name changed but table stayed the same,
        # this is a no-op in SQL.
        if self.old_db_table != self.new_db_table:
            await schema_editor.rename_model(conn, self.old_db_table, self.new_db_table)

    async def unapply(self, conn, schema_editor) -> None:
        """Revert the rename."""
        if self.old_db_table != self.new_db_table:
            await schema_editor.rename_model(conn, self.new_db_table, self.old_db_table)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to rename the underlying table, if required."""
        _ = conn
        if self.old_db_table == self.new_db_table:
            return []
        return [schema_editor.sql_rename_model(self.old_db_table, self.new_db_table)]

    def mutate_state(self, state: ProjectState) -> None:
        """Update ProjectState to reflect this model + table rename."""
        ms = state.model_states.pop(self.old_name)

        state.model_states[self.new_name] = ModelState(
            name=self.new_name,
            db_table=self.new_db_table,
            field_states=ms.field_states,
        )

        # Rewrite FK metadata across all models.
        for other in state.model_states.values():
            for fs in other.field_states.values():
                if getattr(fs, "related_table", None) == self.old_db_table:
                    fs.related_table = self.new_db_table

                if getattr(fs, "related_model", None) == self.old_name:
                    fs.related_model = self.new_name

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        return (
            f"RenameModel(old_name={self.old_name!r}, new_name={self.new_name!r}, "
            f"old_db_table={self.old_db_table!r}, new_db_table={self.new_db_table!r})"
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
        await schema_editor.remove_field(
            conn,
            self.db_table,
            self.field_name,
        )

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
    db_column: str | None = None

    async def apply(self, conn, schema_editor) -> None:
        """Drop the column from the table."""
        await schema_editor.remove_field(
            conn,
            self.db_table,
            self.field_name,
            db_column=self.db_column,
        )

    async def unapply(self, conn, schema_editor) -> None:
        """Raise an exception as this is irreversible."""
        _ = conn, schema_editor
        msg = "RemoveField cannot be reversed."
        raise RuntimeError(msg)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to drop the column."""
        _ = conn
        return [
            schema_editor.sql_remove_field(
                self.db_table,
                self.field_name,
                db_column=self.db_column,
            ),
        ]

    def mutate_state(self, state: ProjectState) -> None:
        """Remove the field from the in-memory state."""
        model = state.model_states[self.model_name]
        model.field_states.pop(_lc(self.field_name), None)

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        base = (
            f"RemoveField(model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, "
            f"field_name={self.field_name!r}"
        )
        if self.db_column is not None:
            base += f", db_column={self.db_column!r}"
        base += ")"
        return base


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

        # Merge new options into the prior state so partial AlterField payloads
        # don't erase previously-known attributes; otherwise we lose flags like
        # primary_key/max_length and makemigrations keeps re-emitting changes.
        base_opts = (
            dict(prev.options)
            if prev
            else {k: v for k, v in self.old_options.items() if k != "type"}
        )
        for key, value in self.new_options.items():
            if key == "type":
                continue
            base_opts[key] = value

        new_fs = FieldState(
            name=dst_name,
            field_type=new_type,
            **base_opts,
        )

        if self.new_name and src_key in model.field_states:
            model.field_states.pop(src_key)

        model.field_states[dst_key] = new_fs

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        old_opts = _sanitize_options_for_code(dict(self.old_options))
        new_opts = _sanitize_options_for_code(dict(self.new_options))

        old_changed, new_changed = _changed_only(old_opts, new_opts)
        old_changed["type"] = old_opts["type"]
        new_changed["type"] = new_opts["type"]

        # Never compact away 'type' (and keep max_length if CharField)
        def _compact(d: dict) -> dict:
            """Compact options while preserving required type metadata."""
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
    old_db_column: str | None = None
    new_db_column: str | None = None

    async def apply(self, conn, schema_editor) -> None:
        """Rename the column on the table."""
        await schema_editor.rename_field(
            conn,
            self.db_table,
            self.old_name,
            self.new_name,
            old_db_column=self.old_db_column,
            new_db_column=self.new_db_column,
        )

    async def unapply(self, conn, schema_editor) -> None:
        """Revert the rename on the table."""
        await schema_editor.rename_field(
            conn,
            self.db_table,
            self.new_name,
            self.old_name,
            old_db_column=self.new_db_column,
            new_db_column=self.old_db_column,
        )

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to rename the column."""
        _ = conn
        return [
            schema_editor.sql_rename_field(
                self.db_table,
                self.old_name,
                self.new_name,
                old_db_column=self.old_db_column,
                new_db_column=self.new_db_column,
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
        base = (
            f"RenameField(model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, old_name={self.old_name!r}, "
            f"new_name={self.new_name!r}"
        )
        if self.old_db_column is not None:
            base += f", old_db_column={self.old_db_column!r}"
        if self.new_db_column is not None:
            base += f", new_db_column={self.new_db_column!r}"
        base += ")"
        return base


@dataclass
class CreateIndex(Operation):
    """Create an index on one or more columns."""

    model_name: str
    db_table: str
    columns: tuple[str, ...]
    unique: bool = False
    name: str | None = None

    def __post_init__(self) -> None:
        if self.name is None:
            self.name = default_index_name(
                self.db_table,
                self.columns,
                unique=self.unique,
            )

    async def apply(self, conn, schema_editor) -> None:
        """Create the index."""
        await schema_editor.create_index(
            conn,
            db_table=self.db_table,
            name=self.name,
            columns=self.columns,
            unique=self.unique,
        )

    async def unapply(self, conn, schema_editor) -> None:
        """Drop the created index."""
        await schema_editor.drop_index(conn, name=self.name)

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to create the index."""
        _ = conn
        return [
            schema_editor.sql_create_index(
                db_table=self.db_table,
                name=self.name,
                columns=self.columns,
                unique=self.unique,
            ),
        ]

    def mutate_state(self, state: ProjectState) -> None:
        """Track index metadata in the in-memory state."""
        model = state.model_states[self.model_name]
        meta = model.meta or {}
        indexes = list(meta.get("indexes", []))
        key = (tuple(self.columns), self.unique)
        if key not in indexes:
            indexes.append(key)
        meta["indexes"] = indexes
        model.meta = meta

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        args = (
            f"model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, "
            f"columns={tuple(self.columns)!r}"
        )
        if self.unique:
            args += ", unique=True"
        if self.name:
            args += f", name={self.name!r}"
        return f"CreateIndex({args})"


@dataclass
class RemoveIndex(Operation):
    """Drop an index created on a model."""

    model_name: str
    db_table: str
    name: str
    columns: tuple[str, ...] | None = None
    unique: bool = False

    async def apply(self, conn, schema_editor) -> None:
        """Drop the index."""
        await schema_editor.drop_index(conn, name=self.name)

    async def unapply(self, conn, schema_editor) -> None:
        """Recreate the index."""
        if self.columns is None:
            msg = "Cannot recreate index without columns."
            raise RuntimeError(msg)
        await schema_editor.create_index(
            conn,
            db_table=self.db_table,
            name=self.name,
            columns=self.columns,
            unique=self.unique,
        )

    async def to_sql(self, conn, schema_editor) -> list[str]:
        """Return SQL to drop the index."""
        _ = conn
        return [schema_editor.sql_drop_index(name=self.name)]

    def mutate_state(self, state: ProjectState) -> None:
        """Remove index metadata from the in-memory state."""
        model = state.model_states[self.model_name]
        meta = model.meta or {}
        target_cols = tuple(self.columns or ())
        indexes = [
            ix
            for ix in meta.get("indexes", [])
            if not (tuple(ix[0]) == target_cols and bool(ix[1]) == self.unique)
        ]
        meta["indexes"] = indexes
        model.meta = meta

    def to_code(self) -> str:
        """Return Python code to recreate this operation."""
        args = (
            f"model_name={self.model_name!r}, "
            f"db_table={self.db_table!r}, "
            f"name={self.name!r}"
        )
        if self.columns:
            args += f", columns={tuple(self.columns)!r}"
        if self.unique:
            args += ", unique=True"
        return f"RemoveIndex({args})"


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

    @staticmethod
    def _invoke_runpython_callable(
        func: Callable,
        conn: Any,
        schema_editor: Any,
    ) -> Any:
        """Invoke a RunPython callable with 0 args or (conn, schema_editor)."""
        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError):
            return func(conn, schema_editor)

        params = list(sig.parameters.values())
        if any(p.kind == p.VAR_POSITIONAL for p in params):
            return func(conn, schema_editor)

        positional = [
            p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
        if not positional:
            return func()
        if len(positional) >= 2:  # noqa: PLR2004
            return func(conn, schema_editor)

        msg = (
            "RunPython callable must accept 0 or 2 positional arguments "
            "(conn, schema_editor)."
        )
        raise TypeError(msg)

    async def apply(self, conn, schema_editor) -> None:
        """Run the forwards callable (sync or async)."""
        result = self._invoke_runpython_callable(self.func, conn, schema_editor)
        if inspect.isawaitable(result):
            await result

    async def unapply(self, conn, schema_editor) -> None:
        """Run the reverse callable if provided, else refuse to reverse."""
        if self.reverse_func is None:
            msg = "RunPython operation has no reverse callable"
            raise RuntimeError(msg)

        result = self._invoke_runpython_callable(
            self.reverse_func,
            conn,
            schema_editor,
        )
        if inspect.isawaitable(result):
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
