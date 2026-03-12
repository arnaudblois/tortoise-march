"""SchemaEditor abstraction for database schema manipulation.

Operations describe *what* to do (CreateModel, AddField, etc.).
SchemaEditor defines *how* to execute those operations for a backend (Postgres),
and how to render SQL without execution (used by migrate(sql=True)).

Foreign keys are supported: the FK column type matches the referenced PK
(limited to SmallInt/Int/BigInt/UUID), and inline
    REFERENCES "<related_table>" ("<to_field>") [ON DELETE <...>]
is emitted on CREATE/ADD.

Notes / limitations:
- Changing UNIQUE/PRIMARY KEY after creation typically requires named
  constraints (ADD/DROP CONSTRAINT). This implementation does not attempt to
  synthesize constraint names; it focuses on null/default/type/rename changes.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from tortoise import BaseDBAsyncClient

from tortoisemarch.constraints import FieldRef, RawSQL
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import ConstraintKind, ConstraintState
from tortoisemarch.operations import constraint_db_name, default_index_name
from tortoisemarch.schema_filtering import FK_TYPES, NON_SCHEMA_FIELD_TYPES

PY_CALLABLE_SENTINELS = {"python_callable"}


class SchemaEditor(ABC):
    """Abstract base class for all schema editors."""

    # ------------ EXECUTION API (runs against the DB) ------------

    async def _execute(self, conn: Any, sql: str) -> None:
        """Execute SQL on either a Tortoise client or a raw asyncpg connection."""
        if hasattr(conn, "execute_script"):
            # Tortoise BaseDBAsyncClient
            await conn.execute_script(sql)
        elif hasattr(conn, "execute"):
            # asyncpg.Connection
            await conn.execute(sql)
        else:
            msg = (
                f"Connection object {conn!r} has neither 'execute_script' nor 'execute'"
            )
            raise TypeError(msg)

    @abstractmethod
    async def create_model(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        fields: list[tuple[str, str, dict[str, Any]]],
    ) -> None:
        """Create a new table with the given fields."""

    @abstractmethod
    async def drop_model(self, conn: BaseDBAsyncClient, db_table: str) -> None:
        """Drop an existing table."""

    @abstractmethod
    async def add_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> None:
        """Add a new column to a table."""

    @abstractmethod
    async def remove_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        db_column: str | None = None,
    ) -> None:
        """Remove a column from a table."""

    @abstractmethod
    async def alter_field(  # noqa: PLR0913
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        old_options: dict[str, Any],
        new_options: dict[str, Any],
        new_name: str | None = None,
    ) -> None:
        """Alter constraints/defaults on a column and optionally rename it."""

    @abstractmethod
    async def rename_field(  # noqa: PLR0913
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
        old_db_column: str | None = None,
        new_db_column: str | None = None,
    ) -> None:
        """Rename a column."""

    @abstractmethod
    async def rename_model(self, conn, old_table: str, new_table: str) -> None:
        """Rename a table backing a model."""

    @abstractmethod
    async def add_extension(self, conn: BaseDBAsyncClient, name: str) -> None:
        """Install a database extension."""

    @abstractmethod
    async def drop_extension(self, conn: BaseDBAsyncClient, name: str) -> None:
        """Remove a database extension."""

    # ------------ RENDERING API (returns SQL strings only) ------------

    @abstractmethod
    def sql_for_field(self, field_type: str, options: dict[str, Any]) -> str:
        """Return the SQL column type for a given abstract field type."""

    @abstractmethod
    def sql_create_model(
        self,
        db_table: str,
        fields: list[tuple[str, str, dict[str, Any]]],
    ) -> str:
        """Return SQL to create a table."""

    @abstractmethod
    def sql_rename_model(self, old_table: str, new_table: str) -> str:
        """Return SQL to rename a table."""

    @abstractmethod
    def sql_drop_model(self, db_table: str) -> str:
        """Return SQL to drop a table."""

    @abstractmethod
    def sql_add_field(
        self,
        db_table: str,
        field_name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> str:
        """Return SQL to add a column."""

    @abstractmethod
    def sql_remove_field(
        self,
        db_table: str,
        field_name: str,
        db_column: str | None = None,
    ) -> str:
        """Return SQL to remove a column."""

    @abstractmethod
    def sql_alter_field(
        self,
        db_table: str,
        field_name: str,
        old_options: dict[str, Any],
        new_options: dict[str, Any],
        new_name: str | None = None,
    ) -> list[str]:
        """Return SQL to alter a column and optionally rename it."""

    @abstractmethod
    def sql_rename_field(
        self,
        db_table: str,
        old_name: str,
        new_name: str,
        old_db_column: str | None = None,
        new_db_column: str | None = None,
    ) -> str:
        """Return SQL to rename a column."""

    @abstractmethod
    def sql_create_index(
        self,
        db_table: str,
        name: str,
        columns: tuple[str, ...],
        *,
        unique: bool = False,
    ) -> str:
        """Return SQL to create an index."""

    @abstractmethod
    def sql_drop_index(self, name: str) -> str:
        """Return SQL to drop an index by name."""

    @abstractmethod
    def sql_rename_index(self, old_name: str, new_name: str) -> str:
        """Return SQL to rename an index by name."""

    @abstractmethod
    def sql_add_extension(self, name: str) -> str:
        """Return SQL to install a database extension."""

    @abstractmethod
    def sql_drop_extension(self, name: str) -> str:
        """Return SQL to remove a database extension."""

    @abstractmethod
    async def create_index(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        name: str,
        columns: tuple[str, ...],
        *,
        unique: bool = False,
    ) -> None:
        """Create an index."""

    @abstractmethod
    async def drop_index(self, conn: BaseDBAsyncClient, name: str) -> None:
        """Drop an index."""

    @abstractmethod
    async def rename_index(
        self,
        conn: BaseDBAsyncClient,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename an index."""

    @abstractmethod
    def sql_add_constraint(
        self,
        db_table: str,
        constraint: ConstraintState,
        field_column_map: dict[str, str] | None = None,
        fk_fields: tuple[str, ...] | None = None,
    ) -> str:
        """Return SQL to add a model-level constraint."""

    @abstractmethod
    def sql_drop_constraint(self, db_table: str, name: str) -> str:
        """Return SQL to drop a model-level constraint."""

    @abstractmethod
    def sql_rename_constraint(
        self,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> str:
        """Return SQL to rename a model-level constraint."""

    @abstractmethod
    async def add_constraint(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        constraint: ConstraintState,
        field_column_map: dict[str, str] | None = None,
        fk_fields: tuple[str, ...] | None = None,
    ) -> None:
        """Create a model-level constraint."""

    @abstractmethod
    async def drop_constraint(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        name: str,
    ) -> None:
        """Drop a model-level constraint."""

    @abstractmethod
    async def rename_constraint(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a model-level constraint."""


# =============================== POSTGRES ===============================


class PostgresSchemaEditor(SchemaEditor):
    """PostgreSQL implementation of SchemaEditor."""

    # ------------------------- helpers (shared) -------------------------

    @staticmethod
    def _q_ident(name: str) -> str:
        """Return a safely quoted identifier for Postgres."""
        return f'"{name}"'

    @staticmethod
    def _sql_string_literal(value: str) -> str:
        """Return a SQL string literal with embedded single quotes escaped.

        We use SQL-standard quote doubling instead of Python repr() because SQL
        string literals must always be single-quoted in generated migrations.
        """
        return "'" + value.replace("'", "''") + "'"

    def _render_default_sql(self, default: Any) -> str | None:
        """Render a Python default value to a PostgreSQL literal, if possible."""
        if isinstance(default, Enum):
            default = default.value
        if default is None:
            return None
        if isinstance(default, str) and default in PY_CALLABLE_SENTINELS:
            return None
        if isinstance(default, str) and default.startswith("db_default:"):
            return default.split(":", 1)[1]  # raw SQL expression
        if isinstance(default, bool):
            return "TRUE" if default else "FALSE"
        if isinstance(default, (int, float)):
            return str(default)
        if isinstance(default, str):
            return self._sql_string_literal(default)
        msg = f"Unsupported default literal: {default!r}"
        raise TypeError(msg)

    @staticmethod
    def _field_type_from_options(options: dict[str, Any] | None) -> str | None:
        """Return the abstract field type from options, if present."""
        if not options:
            return None
        return options.get("type") or options.get("field_type")

    def _resolve_column_name(
        self,
        field_name: str,
        field_type: str | None,
        options: dict[str, Any] | None,
    ) -> str:
        """Resolve the database column name for a field."""
        if options and options.get("db_column"):
            return options["db_column"]
        if field_type in FK_TYPES:
            return f"{field_name}_id"
        return field_name

    @staticmethod
    def _normalize_on_delete(val: str | None) -> str | None:
        """Normalize on_delete to a valid SQL clause."""
        if not val:
            return None
        up = str(val).upper().replace("_", " ")
        aliases = {
            "SET NULL": "SET NULL",
            "SET DEFAULT": "SET DEFAULT",
            "CASCADE": "CASCADE",
            "RESTRICT": "RESTRICT",
            "NO ACTION": "NO ACTION",
            "DO NOTHING": "NO ACTION",
        }
        return aliases.get(up, up)

    @staticmethod
    def _index_name(db_table: str, colname: str) -> str:
        """Return a stable index name for a single column."""
        return default_index_name(db_table, (colname,), unique=False)

    def _render_create_index_sql(
        self,
        db_table: str,
        columns: tuple[str, ...],
        *,
        unique: bool = False,
        name: str | None = None,
    ) -> str:
        """Return SQL to create an index."""
        idx_name = name or default_index_name(db_table, columns, unique=unique)
        cols_sql = ", ".join(self._q_ident(c) for c in columns)
        unique_sql = "UNIQUE " if unique else ""
        return (
            f"CREATE {unique_sql}INDEX {self._q_ident(idx_name)} "
            f"ON {self._q_ident(db_table.lower())} ({cols_sql});"
        )

    def _render_drop_index_sql(self, name: str) -> str:
        """Return SQL to drop an index by name."""
        return f"DROP INDEX IF EXISTS {self._q_ident(name)};"

    def _fk_constraint_name(self, db_table: str, column_name: str) -> str:
        """Return the default PostgreSQL FK constraint name for one column."""
        return f"{db_table.lower()}_{column_name.lower()}_fkey"

    def _render_drop_fk_constraint_sql(self, db_table: str, column_name: str) -> str:
        """Return SQL to drop the FK constraint backing one column."""
        return self.sql_drop_constraint(
            db_table=db_table,
            name=self._fk_constraint_name(db_table, column_name),
        )

    def _render_add_fk_constraint_sql(
        self,
        db_table: str,
        column_name: str,
        *,
        related_table: str | None,
        to_field: str | None,
        on_delete: str | None,
    ) -> str:
        """Return SQL to create the FK constraint backing one column."""
        if not related_table:
            msg = (
                "FK AlterField replay requires related_table when re-adding the "
                f"constraint for {db_table}.{column_name}."
            )
            raise InvalidMigrationError(msg)

        normalized_on_delete = self._normalize_on_delete(on_delete)
        constraint_name = self._fk_constraint_name(db_table, column_name)
        sql = (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"ADD CONSTRAINT {self._q_ident(constraint_name)} "
            f"FOREIGN KEY ({self._q_ident(column_name)}) "
            f"REFERENCES {self._q_ident(related_table)} "
            f"({self._q_ident(to_field or 'id')})"
        )
        if normalized_on_delete:
            sql += f" ON DELETE {normalized_on_delete}"
        return sql + ";"

    def _resolve_constraint_column(
        self,
        logical_name: str,
        *,
        field_column_map: dict[str, str] | None = None,
        fk_fields: tuple[str, ...] | None = None,
    ) -> str:
        """Resolve one logical constraint reference to its DB column name."""
        normalized = logical_name.lower()
        if field_column_map and normalized in field_column_map:
            return field_column_map[normalized]
        if normalized in {field.lower() for field in fk_fields or ()}:
            return f"{logical_name}_id"
        return logical_name

    def _render_constraint_sql(
        self,
        db_table: str,
        constraint: ConstraintState,
        field_column_map: dict[str, str] | None = None,
        fk_fields: tuple[str, ...] | None = None,
    ) -> str:
        """Render the body of a Postgres model-level constraint definition."""
        if constraint.kind == ConstraintKind.UNIQUE:
            columns = ", ".join(
                self._q_ident(
                    self._resolve_constraint_column(
                        column,
                        field_column_map=field_column_map,
                        fk_fields=fk_fields,
                    ),
                )
                for column in constraint.columns
            )
            return (
                f"CONSTRAINT {self._q_ident(constraint_db_name(db_table, constraint))} "
                f"UNIQUE ({columns})"
            )
        if constraint.kind == ConstraintKind.CHECK:
            return (
                f"CONSTRAINT {self._q_ident(constraint_db_name(db_table, constraint))} "
                f"CHECK ({constraint.check})"
            )
        if constraint.kind == ConstraintKind.EXCLUDE:
            rendered_expressions: list[str] = []
            for expression, operator in constraint.expressions:
                if isinstance(expression, FieldRef):
                    column_name = self._resolve_constraint_column(
                        expression.name,
                        field_column_map=field_column_map,
                        fk_fields=fk_fields,
                    )
                    rendered = self._q_ident(column_name)
                elif isinstance(expression, RawSQL):
                    rendered = expression.sql
                else:
                    msg = (
                        "Unsupported exclusion expression node in ConstraintState: "
                        f"{expression!r}"
                    )
                    raise InvalidMigrationError(msg)
                rendered_expressions.append(f"{rendered} WITH {operator}")
            expressions = ", ".join(
                rendered_expressions,
            )
            sql = (
                f"CONSTRAINT {self._q_ident(constraint_db_name(db_table, constraint))} "
                f"EXCLUDE USING {constraint.index_type} ({expressions})"
            )
            if constraint.condition:
                sql += f" WHERE ({constraint.condition})"
            return sql
        msg = f"Unsupported constraint kind: {constraint.kind!r}"
        raise InvalidMigrationError(msg)

    def _column_def(
        self,
        db_table: str,
        name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> str:
        """Build a single column definition for CREATE / ADD COLUMN."""
        # Base SQL type (FKs resolve to referenced PK type)
        base_sql_type = self.sql_for_field(field_type, options)

        # Physical column name (db_column override supported)
        colname = self._resolve_column_name(name, field_type, options)

        parts: list[str] = [f"{self._q_ident(colname)} {base_sql_type}"]

        # ---- constraints -------------------------------------------------

        if options.get("primary_key"):
            parts.append("PRIMARY KEY")

        # null=False → NOT NULL (default is NOT NULL unless explicitly nullable)
        if not options.get("null", False) and not options.get("primary_key"):
            parts.append("NOT NULL")

        # We enforce uniqueness for OneToOne fields even if legacy migration
        # options omitted `unique=True`, because one-to-one semantics require it.
        is_unique = bool(options.get("unique")) or field_type == "OneToOneFieldInstance"
        if is_unique and not options.get("primary_key"):
            constraint = ConstraintState(kind=ConstraintKind.UNIQUE, columns=(colname,))
            parts.append(
                "CONSTRAINT "
                f"{self._q_ident(constraint_db_name(db_table, constraint))} "
                "UNIQUE",
            )

        # ---- default -----------------------------------------------------

        if "default" in options:
            rendered = self._render_default_sql(options["default"])
            if rendered is not None:
                parts.append(f"DEFAULT {rendered}")

        # ---- foreign key -------------------------------------------------

        if field_type in FK_TYPES:
            related_table = options.get("related_table")
            to_field = options.get("to_field", "id")

            if not related_table:
                msg = f"FK field {name!r} missing related_table"
                raise InvalidMigrationError(msg)

            parts.append(
                f"REFERENCES {self._q_ident(related_table)} "
                f"({self._q_ident(to_field)})",
            )

            on_delete = self._normalize_on_delete(options.get("on_delete"))
            if on_delete:
                parts.append(f"ON DELETE {on_delete}")

        return " ".join(parts)

    @staticmethod
    def _should_index(options: dict[str, Any]) -> bool:
        """Return True if an index should be created for this field."""
        return bool(options.get("index")) and not any(
            options.get(flag) for flag in ("unique", "primary_key")
        )

    # ----------------------- rendering (no execute) ---------------------

    def sql_create_model(
        self,
        db_table: str,
        fields: list[tuple[str, str, dict[str, Any]]],
    ) -> str:
        """Write the SQL to create a model."""
        cols = [
            self._column_def(db_table, name, ftype, opts)
            for name, ftype, opts in fields
        ]
        col_sql = ", ".join(cols)
        statements = [
            f"CREATE TABLE {self._q_ident(db_table.lower())} ({col_sql});",
        ]

        for name, ftype, opts in fields:
            if self._should_index(opts):
                colname = self._resolve_column_name(name, ftype, opts)
                statements.append(
                    self._render_create_index_sql(db_table, (colname,), unique=False),
                )

        return "\n".join(statements)

    def sql_rename_model(self, old_table: str, new_table: str) -> str:
        """Return the SQL to rename a model's backing table."""
        return f'ALTER TABLE "{old_table}" RENAME TO "{new_table}"'

    def sql_drop_model(self, db_table: str) -> str:
        """Write the SQL to drop a model."""
        return f"DROP TABLE IF EXISTS {self._q_ident(db_table.lower())};"

    def sql_add_field(
        self,
        db_table: str,
        field_name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> str:
        """Return the SQL to add a column to the table."""
        sql = (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"ADD COLUMN {self._column_def(db_table, field_name, field_type, options)};"
        )
        if self._should_index(options):
            colname = self._resolve_column_name(field_name, field_type, options)
            sql += (
                f"\n{self._render_create_index_sql(db_table, (colname,), unique=False)}"
            )
        return sql

    def sql_remove_field(
        self,
        db_table: str,
        field_name: str,
        db_column: str | None = None,
    ) -> str:
        """Write the SQL to remove a field."""
        colname = db_column or field_name
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"DROP COLUMN IF EXISTS {self._q_ident(colname)};"
        )

    def _sql_alter_type_if_supported(
        self,
        db_table: str,
        column_name: str,
        old: dict[str, Any],
        new: dict[str, Any],
    ) -> list[str]:
        """Best-effort ALTER TYPE for common safe cases (e.g., VARCHAR length).

        With compact/sparse options, missing keys mean "unknown/implicit", not a
        concrete default. We only emit SQL when we can confidently detect a change.
        """
        stmts: list[str] = []

        old_type = old.get("type") or old.get("field_type")
        new_type = new.get("type") or new.get("field_type")

        # If type is missing on either side, we can't safely infer this path here.
        if old_type is None or new_type is None:
            return []

        # Safe integer widening (SMALLINT -> INT -> BIGINT).
        int_rank = {"SmallIntField": 0, "IntField": 1, "BigIntField": 2}
        if old_type in int_rank and new_type in int_rank:
            if int_rank[new_type] > int_rank[old_type]:
                new_sql_type = self.sql_for_field(new_type, new)
                stmts.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(column_name)} "
                    f"TYPE {new_sql_type};",
                )
            return stmts

        if old_type == "CharField" and new_type == "TextField":
            stmts.append(
                f"ALTER TABLE {self._q_ident(db_table.lower())} "
                f"ALTER COLUMN {self._q_ident(column_name)} "
                "TYPE TEXT;",
            )
            return stmts

        if old_type != "CharField" or new_type != "CharField":
            return []

        # IMPORTANT: if either side does not specify max_length, treat as unknown.
        # Do NOT assume 255, because that will mask real changes after compaction.
        if "max_length" not in old or "max_length" not in new:
            return []

        old_len = old["max_length"]
        new_len = new["max_length"]

        if old_len != new_len:
            stmts.append(
                f"ALTER TABLE {self._q_ident(db_table.lower())} "
                f"ALTER COLUMN {self._q_ident(column_name)} "
                f"TYPE VARCHAR({new_len});",
            )

        return stmts

    def sql_alter_field(  # noqa: C901, PLR0912, PLR0915
        self,
        db_table: str,
        field_name: str,
        old_options: dict[str, Any],
        new_options: dict[str, Any],
        new_name: str | None = None,
    ) -> list[str]:
        """Return the sql to alter the field in Postgres."""
        statements: list[str] = []
        old_type = self._field_type_from_options(old_options)
        new_type = self._field_type_from_options(new_options)
        field_type = new_type or old_type
        column_opts: dict[str, Any] = {}
        db_column = new_options.get("db_column") or old_options.get("db_column")
        if db_column is not None:
            column_opts["db_column"] = db_column
        column_name = self._resolve_column_name(
            field_name,
            field_type,
            column_opts or None,
        )

        # Type changes (limited safe cases)
        type_stmts = self._sql_alter_type_if_supported(
            db_table=db_table,
            column_name=column_name,
            old=old_options,
            new=new_options,
        )
        statements += type_stmts

        # NULL / NOT NULL
        if old_options.get("null", False) != new_options.get("null", False):
            if new_options.get("null", False):
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(column_name)} DROP NOT NULL;",
                )
            else:
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(column_name)} SET NOT NULL;",
                )

        # DEFAULT
        # Render defaults to SQL so we only emit changes when the actual SQL
        # literal differs (e.g., callable vs literal string).
        if old_options.get("default") != new_options.get("default"):
            old_rendered = self._render_default_sql(old_options.get("default"))
            new_rendered = self._render_default_sql(new_options.get("default"))
            if old_rendered == new_rendered:
                # No-op if both render to the same SQL (or both non-renderable).
                pass
            elif new_rendered is not None:
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(column_name)} "
                    f"SET DEFAULT {new_rendered};",
                )
            elif old_rendered is not None:
                # New default is effectively NULL/unset: drop existing default.
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(column_name)} DROP DEFAULT;",
                )

        old_unique = bool(old_options.get("unique")) and not old_options.get(
            "primary_key",
            False,
        )
        new_unique = bool(new_options.get("unique")) and not new_options.get(
            "primary_key",
            False,
        )
        old_col = self._resolve_column_name(field_name, field_type, old_options)
        new_col = self._resolve_column_name(
            new_name or field_name,
            field_type,
            new_options,
        )

        if old_unique and (not new_unique or old_col != new_col):
            statements.append(
                self.sql_drop_constraint(
                    db_table=db_table,
                    name=constraint_db_name(
                        db_table,
                        ConstraintState(kind=ConstraintKind.UNIQUE, columns=(old_col,)),
                    ),
                ),
            )

        # (Optional) RENAME as a separate statement at the end
        if new_name and new_name != field_name:
            statements.append(self.sql_rename_field(db_table, old_col, new_col))

        if new_unique and (not old_unique or old_col != new_col):
            statements.append(
                self.sql_add_constraint(
                    db_table=db_table,
                    constraint=ConstraintState(
                        kind=ConstraintKind.UNIQUE,
                        columns=(new_col,),
                    ),
                ),
            )

        # Index creation/drop based on index flag changes
        old_index = self._should_index(old_options)
        new_index = self._should_index(new_options)
        old_colname = old_col
        new_colname = new_col

        if old_index and (not new_index or old_colname != new_colname):
            statements.append(
                self._render_drop_index_sql(
                    default_index_name(db_table, (old_colname,), unique=False),
                ),
            )

        old_is_fk = old_type in FK_TYPES
        new_is_fk = new_type in FK_TYPES
        if old_is_fk and new_is_fk:
            old_on_delete = self._normalize_on_delete(old_options.get("on_delete"))
            new_on_delete = self._normalize_on_delete(new_options.get("on_delete"))
            if old_on_delete != new_on_delete:
                statements.append(
                    self._render_drop_fk_constraint_sql(
                        db_table=db_table,
                        column_name=old_col,
                    ),
                )

        if new_index and (not old_index or old_colname != new_colname):
            statements.append(
                self._render_create_index_sql(db_table, (new_colname,), unique=False),
            )

        if old_is_fk and new_is_fk:
            old_on_delete = self._normalize_on_delete(old_options.get("on_delete"))
            new_on_delete = self._normalize_on_delete(new_options.get("on_delete"))
            if old_on_delete != new_on_delete:
                statements.append(
                    self._render_add_fk_constraint_sql(
                        db_table=db_table,
                        column_name=new_col,
                        related_table=new_options.get("related_table"),
                        to_field=new_options.get("to_field"),
                        on_delete=new_options.get("on_delete"),
                    ),
                )

        return statements

    def sql_rename_field(
        self,
        db_table: str,
        old_name: str,
        new_name: str,
        old_db_column: str | None = None,
        new_db_column: str | None = None,
    ) -> str:
        """Write the SQL to rename a field."""
        old_name = old_db_column or old_name
        new_name = new_db_column or new_name
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"RENAME COLUMN {self._q_ident(old_name)} TO {self._q_ident(new_name)};"
        )

    def sql_create_index(
        self,
        db_table: str,
        name: str,
        columns: tuple[str, ...],
        *,
        unique: bool = False,
    ) -> str:
        """Return SQL to create an index (possibly unique)."""
        return self._render_create_index_sql(
            db_table,
            columns,
            unique=unique,
            name=name,
        )

    def sql_drop_index(self, name: str) -> str:
        """Return SQL to drop an index by name."""
        return self._render_drop_index_sql(name)

    def sql_rename_index(self, old_name: str, new_name: str) -> str:
        """Return SQL to rename an index by name."""
        return (
            f"ALTER INDEX {self._q_ident(old_name)} "
            f"RENAME TO {self._q_ident(new_name)};"
        )

    def sql_add_extension(self, name: str) -> str:
        """Return SQL to install a PostgreSQL extension."""
        return f"CREATE EXTENSION IF NOT EXISTS {self._q_ident(name)};"

    def sql_drop_extension(self, name: str) -> str:
        """Return SQL to remove a PostgreSQL extension."""
        return f"DROP EXTENSION IF EXISTS {self._q_ident(name)};"

    def sql_add_constraint(
        self,
        db_table: str,
        constraint: ConstraintState,
        field_column_map: dict[str, str] | None = None,
        fk_fields: tuple[str, ...] | None = None,
    ) -> str:
        """Return SQL to add a model-level constraint."""
        rendered_constraint = self._render_constraint_sql(
            db_table,
            constraint,
            field_column_map,
            fk_fields,
        )
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"ADD {rendered_constraint};"
        )

    def sql_drop_constraint(self, db_table: str, name: str) -> str:
        """Return SQL to drop a model-level constraint."""
        _ = db_table
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"DROP CONSTRAINT IF EXISTS {self._q_ident(name)};"
        )

    def sql_rename_constraint(
        self,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> str:
        """Return SQL to rename a model-level constraint."""
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"RENAME CONSTRAINT {self._q_ident(old_name)} TO {self._q_ident(new_name)};"
        )

    # -------------------------- execution (async) -----------------------

    async def create_model(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        fields: list[tuple[str, str, dict[str, Any]]],
    ) -> None:
        """Execute the SQL to create a model."""
        sql = self.sql_create_model(db_table, fields)
        for stmt in sql.split("\n"):
            if stmt.strip():
                await self._execute(conn, stmt)

    async def rename_model(self, conn, old_table: str, new_table: str) -> None:
        """Execute the SQL to rename a model."""
        sql = self.sql_rename_model(old_table=old_table, new_table=new_table)
        await self._execute(conn, sql)

    async def add_extension(self, conn: BaseDBAsyncClient, name: str) -> None:
        """Execute the SQL to install a PostgreSQL extension."""
        await self._execute(conn, self.sql_add_extension(name))

    async def drop_extension(self, conn: BaseDBAsyncClient, name: str) -> None:
        """Execute the SQL to remove a PostgreSQL extension."""
        await self._execute(conn, self.sql_drop_extension(name))

    async def drop_model(self, conn: BaseDBAsyncClient, db_table: str) -> None:
        """Execute the SQL to drop a model."""
        sql = self.sql_drop_model(db_table)
        await self._execute(conn, sql)

    async def add_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> None:
        """Execute the SQL to add a field."""
        sql = self.sql_add_field(db_table, field_name, field_type, options)
        for stmt in sql.split("\n"):
            if stmt.strip():
                await self._execute(conn, stmt)

    async def remove_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        db_column: str | None = None,
    ) -> None:
        """Execute the SQL to remove a field."""
        sql = self.sql_remove_field(
            db_table,
            field_name,
            db_column=db_column,
        )
        await self._execute(conn, sql)

    async def alter_field(  # noqa: PLR0913
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        old_options: dict[str, Any],
        new_options: dict[str, Any],
        new_name: str | None = None,
    ) -> None:
        """Execute the PG SQL statement for field alteration."""
        for stmt in self.sql_alter_field(
            db_table,
            field_name,
            old_options,
            new_options,
            new_name,
        ):
            await self._execute(conn, stmt)

    async def rename_field(  # noqa: PLR0913
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
        old_db_column: str | None = None,
        new_db_column: str | None = None,
    ) -> None:
        """Execute the SQL to rename a column."""
        sql = self.sql_rename_field(
            db_table,
            old_name,
            new_name,
            old_db_column=old_db_column,
            new_db_column=new_db_column,
        )
        await self._execute(conn, sql)

    async def create_index(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        name: str,
        columns: tuple[str, ...],
        *,
        unique: bool = False,
    ) -> None:
        """Execute SQL to create an index."""
        sql = self.sql_create_index(
            db_table=db_table,
            name=name,
            columns=columns,
            unique=unique,
        )
        await self._execute(conn, sql)

    async def drop_index(self, conn: BaseDBAsyncClient, name: str) -> None:
        """Execute SQL to drop an index."""
        sql = self.sql_drop_index(name=name)
        await self._execute(conn, sql)

    async def rename_index(
        self,
        conn: BaseDBAsyncClient,
        old_name: str,
        new_name: str,
    ) -> None:
        """Execute SQL to rename an index."""
        sql = self.sql_rename_index(old_name=old_name, new_name=new_name)
        await self._execute(conn, sql)

    async def add_constraint(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        constraint: ConstraintState,
        field_column_map: dict[str, str] | None = None,
        fk_fields: tuple[str, ...] | None = None,
    ) -> None:
        """Execute SQL to add a model-level constraint."""
        sql = self.sql_add_constraint(
            db_table=db_table,
            constraint=constraint,
            field_column_map=field_column_map,
            fk_fields=fk_fields,
        )
        await self._execute(conn, sql)

    async def drop_constraint(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        name: str,
    ) -> None:
        """Execute SQL to drop a model-level constraint."""
        sql = self.sql_drop_constraint(db_table=db_table, name=name)
        await self._execute(conn, sql)

    async def rename_constraint(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Execute SQL to rename a model-level constraint."""
        sql = self.sql_rename_constraint(
            db_table=db_table,
            old_name=old_name,
            new_name=new_name,
        )
        await self._execute(conn, sql)

    # ------------------------- type mapping -----------------------------

    def sql_for_field(self, field_type: str, options: dict[str, Any]) -> str:
        """Map abstract field types to PostgreSQL SQL types.

        For FK/O2O we require options['referenced_type'] to be one of the
        following: SmallIntField, IntField, BigIntField, UUIDField, CharField.
        """
        if field_type in NON_SCHEMA_FIELD_TYPES:
            msg = f"Non-schema field type leaked into schema: {field_type}"
            raise InvalidMigrationError(msg)
        # --- Relational fields use the referenced type ----------------------
        if field_type in FK_TYPES:
            ref = options.get("referenced_type")
            fk_map = {
                "SmallIntField": "SMALLINT",
                "IntField": "INTEGER",
                "BigIntField": "BIGINT",
                "UUIDField": "UUID",
                "CharField": f'VARCHAR({options.get("max_length", 255)})',
            }
            if ref not in fk_map:
                msg = (
                    f"Unsupported FK referenced_type {ref!r}. "
                    "Only integer-based fields or UUIDField can be referenced."
                )
                raise InvalidMigrationError(msg)
            return fk_map[ref]

        # --- Base mapping for scalar fields ---------------------------------
        mapping = {
            # Integers
            "IntField": "INTEGER",
            "SmallIntField": "SMALLINT",
            "BigIntField": "BIGINT",
            # Booleans
            "BooleanField": "BOOLEAN",
            # Character/text
            "CharField": f'VARCHAR({options.get("max_length", 255)})',
            "TextField": "TEXT",
            # Binary / bytes
            "BinaryField": "BYTEA",
            # Numbers
            "FloatField": "DOUBLE PRECISION",
            "DecimalField": (
                f'NUMERIC({options.get("max_digits", 18)}, '
                f'{options.get("decimal_places", 6)})'
            ),
            # UUID
            "UUIDField": "UUID",
            # Date / time
            "DatetimeField": "TIMESTAMPTZ",
            "DateField": "DATE",
            "TimeField": "TIME",
            "TimedeltaField": "INTERVAL",
            # JSON
            "JSONField": "JSONB",
            # Enums (modeled as chars/ints in Tortoise)
            "CharEnumField": f'VARCHAR({options.get("max_length", 255)})',
            "IntEnumField": "INTEGER",
        }

        if field_type not in mapping:
            msg = f"Unknown field type: {field_type}"
            raise InvalidMigrationError(msg)
        base = mapping[field_type]

        # --- Auto-increment / identity for integer PKs ----------------------
        # Tortoise sets `generated=True` on integer PKs. We mirror that with
        # PostgreSQL IDENTITY syntax instead of SERIAL.
        if (
            field_type in {"IntField", "SmallIntField", "BigIntField"}
            and options.get("primary_key")
            and options.get("generated")
        ):
            return f"{base} GENERATED BY DEFAULT AS IDENTITY"

        return base
