"""Base class used for migration files."""

from typing import ClassVar

from tortoise import BaseDBAsyncClient

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import ProjectState
from tortoisemarch.operations import Operation
from tortoisemarch.schema_editor import SchemaEditor


class BaseMigration:
    """Base class for migrations.

    Subclasses must define a list of operations. We use `@classmethod`
    so migrations can be applied directly on the class without needing
    to instantiate it, since migrations are static declarations rather
    than stateful objects.
    """

    operations: ClassVar[list[Operation]] = []

    # ---------------------------- internals -----------------------------

    @classmethod
    def _validated_ops(cls) -> list[Operation]:
        """Return the operation list after basic validation.

        Raises:
            InvalidMigrationError: if `operations` is not a list of Operation.

        """
        ops = cls.operations
        if not isinstance(ops, list):
            msg = f"{cls.__name__}.operations must be a list, got {type(ops).__name__}"
            raise InvalidMigrationError(msg)
        for i, op in enumerate(ops):
            if not isinstance(op, Operation):
                msg = (
                    f"{cls.__name__}.operations[{i}] is not an Operation "
                    f"(got {type(op).__name__})"
                )
                raise InvalidMigrationError(msg)
        return ops

    # ----------------------------- actions ------------------------------

    @classmethod
    async def apply(cls, conn: BaseDBAsyncClient, schema_editor: SchemaEditor) -> None:
        """Apply all operations in order."""
        for op in cls._validated_ops():
            await op.apply(conn, schema_editor)

    @classmethod
    async def unapply(
        cls,
        conn: BaseDBAsyncClient,
        schema_editor: SchemaEditor,
    ) -> None:
        """Reverse all operations in reverse order, if reversible.

        Raises:
            NotReversibleMigrationError: if any operation cannot be reversed.

        """
        for op in reversed(cls._validated_ops()):
            await op.unapply(conn, schema_editor)

    @classmethod
    def mutate_state(cls, state: ProjectState) -> None:
        """Mutate the in-memory ProjectState by applying all operations."""
        for op in cls._validated_ops():
            op.mutate_state(state)

    @classmethod
    async def to_sql(
        cls,
        conn: BaseDBAsyncClient,
        schema_editor: SchemaEditor,
    ) -> list[str]:
        """Render SQL for all operations without executing them.

        Returns:
            A flat list of SQL statements.

        Notes:
            If an operation does not implement SQL rendering, a comment stub is
            included to make that explicit in previews.

        """
        statements: list[str] = []
        for op in cls._validated_ops():
            try:
                stmts = await op.to_sql(conn, schema_editor)
            except NotImplementedError:
                statements.append(f"-- No SQL preview for {op.__class__.__name__}")
                continue

            if isinstance(stmts, str):
                statements.append(stmts)
            elif stmts:
                statements.extend(stmts)

        return statements
