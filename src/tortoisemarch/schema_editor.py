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


class SchemaEditor(ABC):
    """Abstract base class for all schema editors."""

    # ------------ EXECUTION API (runs against the DB) ------------

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


# =============================== POSTGRES ===============================


class PostgresSchemaEditor(SchemaEditor):
    """PostgreSQL implementation of SchemaEditor."""

    # ------------------------- helpers (shared) -------------------------

    @staticmethod
    def _q_ident(name: str) -> str:
        """Return a safely quoted identifier for Postgres."""
        return f'"{name}"'

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

    def _column_def(self, name: str, field_type: str, options: dict[str, Any]) -> str:
        """Build a single column definition for CREATE/ALTER statements."""
        # Base SQL type (FK uses referenced PK type).
        base_sql_type = self.sql_for_field(field_type, options)

        # Physical column name (support db_column override).
        colname = options.get("db_column") or name

        parts: list[str] = [f"{self._q_ident(colname)} {base_sql_type}"]

        # Inline constraints
        if options.get("primary_key"):
            parts.append("PRIMARY KEY")
        if not options.get("null", False):
            parts.append("NOT NULL")
        if options.get("unique"):
            parts.append("UNIQUE")

        # Default (literal values only; callables are not rendered)
        if "default" in options and options["default"] not in [None, "callable"]:
            parts.append(f"DEFAULT {options['default']!r}")

        # Inline FK references
        if field_type in {"ForeignKeyFieldInstance", "OneToOneFieldInstance"}:
            related_table = options.get("related_table")
            to_field = options.get("to_field", "id")
            if related_table:
                parts.append(
                    f"REFERENCES {self._q_ident(related_table)} "
                    f"({self._q_ident(to_field)})",
                )
                od = self._normalize_on_delete(options.get("on_delete"))
                if od:
                    parts.append(f"ON DELETE {od}")

        return " ".join(parts)

    # ----------------------- rendering (no execute) ---------------------

    def sql_create_model(
        self,
        db_table: str,
        fields: list[tuple[str, str, dict[str, Any]]],
    ) -> str:
        """Write the SQL to create a model."""
        cols = [self._column_def(name, ftype, opts) for name, ftype, opts in fields]
        col_sql = ", ".join(cols)
        return f"CREATE TABLE {self._q_ident(db_table.lower())} ({col_sql});"

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
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"ADD COLUMN {self._column_def(field_name, field_type, options)};"
        )

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

        We do *not* attempt arbitrary rewrites (e.g., int -> uuid) here.
        """
        stmts: list[str] = []

        old_type = old.get("type")
        new_type = new.get("type")
        if old_type == new_type == "CharField":
            old_len = old.get("max_length", 255)
            new_len = new.get("max_length", 255)
            if old_len != new_len:
                stmts.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} "
                    f"TYPE VARCHAR({new_len});",
                )

        # You could add more cases here (e.g., Decimal precision changes),
        # but many require USING casts and/or data validation.

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
            if "default" in new_options and new_options["default"] not in [
                None,
                "callable",
            ]:
                default = new_options["default"]
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} "
                    f"SET DEFAULT {default!r};",
                )
            else:
                statements.append(
                    f"ALTER TABLE {self._q_ident(db_table.lower())} "
                    f"ALTER COLUMN {self._q_ident(field_name)} DROP DEFAULT;",
                )

        # (Optional) RENAME as a separate statement at the end
        if new_name and new_name != field_name:
            statements.append(self.sql_rename_field(db_table, field_name, new_name))

        return statements

    def sql_rename_field(self, db_table: str, old_name: str, new_name: str) -> str:
        """Write the SQL to rename a field."""
        return (
            f"ALTER TABLE {self._q_ident(db_table.lower())} "
            f"RENAME COLUMN {self._q_ident(old_name)} TO {self._q_ident(new_name)};"
        )

    # -------------------------- execution (async) -----------------------

    async def create_model(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        fields: list[tuple[str, str, dict[str, Any]]],
    ) -> None:
        """Execute the SQL to create a model."""
        await conn.execute_script(self.sql_create_model(db_table, fields))

    async def drop_model(self, conn: BaseDBAsyncClient, db_table: str) -> None:
        """Execute the SQL to drop a model."""
        await conn.execute_script(self.sql_drop_model(db_table))

    async def add_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
        field_type: str,
        options: dict[str, Any],
    ) -> None:
        """Execute the SQL to add a field."""
        await conn.execute_script(
            self.sql_add_field(db_table, field_name, field_type, options),
        )

    async def remove_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        field_name: str,
    ) -> None:
        """Execute the SQL to remove a field."""
        await conn.execute_script(self.sql_remove_field(db_table, field_name))

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
            await conn.execute_script(stmt)

    async def rename_field(
        self,
        conn: BaseDBAsyncClient,
        db_table: str,
        old_name: str,
        new_name: str,
    ) -> None:
        """Execute the SQL to rename a column."""
        await conn.execute_script(self.sql_rename_field(db_table, old_name, new_name))

    # ------------------------- type mapping -----------------------------

    def sql_for_field(self, field_type: str, options: dict[str, Any]) -> str:
        """Map abstract field types to PostgreSQL SQL types.

        For FK/O2O we require options['referenced_type'] to be one of the
        following: SmallIntField, IntField, BigIntField, UUIDField, CharField.
        """
        # --- Relational fields use the referenced type ----------------------
        if field_type in {"ForeignKeyFieldInstance", "OneToOneFieldInstance"}:
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

        base = mapping.get(field_type, "TEXT")

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
