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
from typing import Any

from tortoise import BaseDBAsyncClient

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.operations import default_index_name
from tortoisemarch.schema_filtering import FK_TYPES, NON_SCHEMA_FIELD_TYPES

PY_CALLABLE_SENTINELS = {"callable", "python_callable", "callable_handled_by_python"}


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
    async def rename_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Rename a column."""

    @abstractmethod
    async def rename_model(self, conn, old_table: str, new_table: str) -> None:
        """Rename a table backing a model."""

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
    def sql_remove_field(self, db_table: str, field_name: str) -> str:
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
    def sql_rename_field(self, db_table: str, old_name: str, new_name: str) -> str:
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


# =============================== POSTGRES ===============================


class PostgresSchemaEditor(SchemaEditor):
    """PostgreSQL implementation of SchemaEditor."""

    # ------------------------- helpers (shared) -------------------------

    @staticmethod
    def _q_ident(name: str) -> str:
        """Return a safely quoted identifier for Postgres."""
        return f'"{name}"'

    def _render_default_sql(self, default: Any) -> str | None:
        """Render a Python default value to a PostgreSQL literal, if possible."""
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
            return repr(default)  # quoted string literal
        msg = f"Unsupported default literal: {default!r}"
        raise TypeError(msg)

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

    def _column_def(
        self,
        name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> str:
        """Build a single column definition for CREATE / ADD COLUMN."""
        # Base SQL type (FKs resolve to referenced PK type)
        base_sql_type = self.sql_for_field(field_type, options)

        # Physical column name (db_column override supported)
        colname = options.get("db_column") or name

        parts: list[str] = [f"{self._q_ident(colname)} {base_sql_type}"]

        # ---- constraints -------------------------------------------------

        if options.get("primary_key"):
            parts.append("PRIMARY KEY")

        # null=False → NOT NULL (default is NOT NULL unless explicitly nullable)
        if not options.get("null", False):
            parts.append("NOT NULL")

        if options.get("unique"):
            parts.append("UNIQUE")

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
        cols = [self._column_def(name, ftype, opts) for name, ftype, opts in fields]
        col_sql = ", ".join(cols)
        statements = [
            f"CREATE TABLE {self._q_ident(db_table.lower())} ({col_sql});",
        ]

        for name, _, opts in fields:
            if self._should_index(opts):
                colname = opts.get("db_column") or name
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
            f"ADD COLUMN {self._column_def(field_name, field_type, options)};"
        )
        if self._should_index(options):
            colname = options.get("db_column") or field_name
            sql += (
                f"\n{self._render_create_index_sql(db_table, (colname,), unique=False)}"
            )
        return sql

    def sql_remove_field(self, db_table: str, field_name: str) -> str:
        """Write the SQL to remove a field."""
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"DROP COLUMN IF EXISTS {self._q_ident(field_name)};"
        )

    def _sql_alter_type_if_supported(
        self,
        db_table: str,
        field_name: str,
        old: dict[str, Any],
        new: dict[str, Any],
    ) -> list[str]:
        """Best-effort ALTER TYPE for common safe cases (e.g., VARCHAR length).

        With compact/sparse options, missing keys mean "unknown/implicit", not a
        concrete default. We only emit SQL when we can confidently detect a change.
        """
        stmts: list[str] = []

        old_type = old.get("type")
        new_type = new.get("type")

        # If type is missing on either side, we can't safely infer this path here.
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
                f"ALTER COLUMN {self._q_ident(field_name)} "
                f"TYPE VARCHAR({new_len});",
            )

        return stmts

    def sql_alter_field(
        self,
        db_table: str,
        field_name: str,
        old_options: dict[str, Any],
        new_options: dict[str, Any],
        new_name: str | None = None,
    ) -> list[str]:
        """Return the sql to alter the field in Postgres."""
        statements: list[str] = []

        # Type changes (limited safe cases)
        statements += self._sql_alter_type_if_supported(
            db_table=db_table,
            field_name=field_name,
            old=old_options,
            new=new_options,
        )

        # NULL / NOT NULL
        if old_options.get("null", False) != new_options.get("null", False):
            if new_options.get("null", False):
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} DROP NOT NULL;",
                )
            else:
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} SET NOT NULL;",
                )

        # DEFAULT
        if old_options.get("default") != new_options.get("default"):
            rendered = self._render_default_sql(new_options.get("default"))
            if rendered is not None:
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} SET DEFAULT {rendered};",
                )
            else:
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} DROP DEFAULT;",
                )

        # (Optional) RENAME as a separate statement at the end
        if new_name and new_name != field_name:
            statements.append(
                self.sql_rename_field(db_table, field_name, new_name),
            )

        # Index creation/drop based on index flag changes
        old_index = self._should_index(old_options)
        new_index = self._should_index(new_options)
        old_colname = old_options.get("db_column") or field_name
        new_colname = new_options.get("db_column") or new_name or field_name

        if old_index and (not new_index or old_colname != new_colname):
            statements.append(
                self._render_drop_index_sql(
                    default_index_name(db_table, (old_colname,), unique=False),
                ),
            )

        if new_index and (not old_index or old_colname != new_colname):
            statements.append(
                self._render_create_index_sql(db_table, (new_colname,), unique=False),
            )

        return statements

    def sql_rename_field(self, db_table: str, old_name: str, new_name: str) -> str:
        """Write the SQL to rename a field."""
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
    ) -> None:
        """Execute the SQL to remove a field."""
        sql = self.sql_remove_field(db_table, field_name)
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

    async def rename_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Execute the SQL to rename a column."""
        sql = self.sql_rename_field(db_table, old_name, new_name)
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
