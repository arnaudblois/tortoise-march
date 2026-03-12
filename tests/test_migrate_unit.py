"""Unit-level checks for migrate planning/target resolution."""

from pathlib import Path
from typing import ClassVar

import pytest

from tortoisemarch.base import BaseMigration
from tortoisemarch.exceptions import (
    ConfigError,
    InvalidMigrationError,
    MigrationConnectionError,
    NotReversibleMigrationError,
    TortoiseMarchError,
)
from tortoisemarch.migrate import (
    migrate,
    plan_route,
    resolve_target_name,
    tortoise_context,
    validate_applied_migration_checksums,
)
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateModel,
    Operation,
    RemoveField,
    RemoveIndex,
    RemoveModel,
    RunPython,
)
from tortoisemarch.schema_editor import PostgresSchemaEditor


def test_resolve_target_name_full_and_prefix():
    """Full names and unique prefixes should resolve deterministically."""
    all_names = ["0001_initial", "0002_add_user", "0003_add_group"]

    assert resolve_target_name("0002_add_user", all_names) == "0002_add_user"
    assert resolve_target_name("0003", all_names) == "0003_add_group"


def test_resolve_target_name_ambiguous_or_missing_raises():
    """Unknown or ambiguous prefixes raise InvalidMigrationError."""
    all_names = ["0001_initial", "0002_alpha", "0002_beta"]

    with pytest.raises(InvalidMigrationError):
        resolve_target_name("9999", all_names)

    with pytest.raises(InvalidMigrationError):
        resolve_target_name("0002", all_names)


def test_plan_route_forward_backward_and_noop():
    """Plan should return correct direction and ordered names."""
    all_names = ["0001_initial", "0002", "0003"]

    # Forward from none applied to target 0002
    direction, names = plan_route(set(), all_names, "0002")
    assert direction == "forward"
    assert names == ["0001_initial", "0002"]

    # Backward when 0003 already applied to target 0001
    direction, names = plan_route(
        {"0001_initial", "0002", "0003"},
        all_names,
        "0001_initial",
    )
    assert direction == "backward"
    assert names == ["0003", "0002"]

    # No-op when already at target
    direction, names = plan_route({"0001_initial", "0002"}, all_names, "0002")
    assert direction == "noop"
    assert names == []

    # Default mode should apply all remaining migrations in order.
    direction, names = plan_route({"0001_initial"}, all_names, None)
    assert direction == "forward"
    assert names == ["0002", "0003"]


def test_plan_route_rejects_gapped_applied_history():
    """Gapped recorder history should fail fast instead of skipping migrations."""
    all_names = ["0001_initial", "0002", "0003"]
    with pytest.raises(InvalidMigrationError, match="contains gaps"):
        plan_route({"0001_initial", "0003"}, all_names, None)


def test_validate_applied_migration_checksums_accepts_exact_match():
    """Checksum validation should pass when every applied migration matches."""
    validate_applied_migration_checksums(
        applied_checksums={
            "0001_initial": "abc123",
            "0002_add_user": "def456",
        },
        current_checksums={
            "0001_initial": "abc123",
            "0002_add_user": "def456",
            "0003_new": "zzz999",
        },
    )


def test_validate_applied_migration_checksums_raises_on_missing_file():
    """Applied migrations missing on disk should fail fast."""
    with pytest.raises(InvalidMigrationError, match="missing from disk"):
        validate_applied_migration_checksums(
            applied_checksums={"0001_initial": "abc123"},
            current_checksums={},
        )


def test_validate_applied_migration_checksums_raises_on_mismatch():
    """Applied migrations with edited files should fail fast."""
    with pytest.raises(InvalidMigrationError, match="Checksum mismatch"):
        validate_applied_migration_checksums(
            applied_checksums={"0001_initial": "abc123"},
            current_checksums={"0001_initial": "changed"},
        )


def test_migration_connection_error_inherits_library_base() -> None:
    """We keep connection failures catchable via the library base class."""
    assert issubclass(MigrationConnectionError, TortoiseMarchError)


@pytest.mark.asyncio
async def test_migrate_rewrite_history_requires_fake():
    """History rewrite mode should require explicit fake-apply semantics."""
    with pytest.raises(ConfigError, match="requires --fake"):
        await migrate(
            tortoise_conf={"connections": {"default": "sqlite://:memory:"}},
            location=Path("migrations"),
            rewrite_history=True,
        )


@pytest.mark.asyncio
async def test_irreversible_operations_raise_not_reversible_error():
    """We raise the documented exception for irreversible operations."""
    operations = [
        RemoveModel(name="Foo", db_table="foo"),
        RemoveField(model_name="Foo", db_table="foo", field_name="bar"),
        RemoveIndex(model_name="Foo", db_table="foo", name="foo_bar_idx"),
        AlterField(
            model_name="Foo",
            db_table="foo",
            field_name="bar",
            old_options={"type": "CharField", "max_length": 255},
            new_options={"type": "TextField"},
            new_name="body",
        ),
        RunPython(lambda: None),
    ]

    for operation in operations:
        with pytest.raises(NotReversibleMigrationError):
            await operation.unapply(None, None)


class _Recorder:
    """Capture SQL via execute/execute_script for rollback previews."""

    def __init__(self):
        self.statements: list[str] = []

    async def execute_script(self, sql_text: str) -> None:
        for stmt in sql_text.split("\n"):
            if stmt.strip():
                self.statements.append(stmt)

    async def execute(self, sql_text: str) -> None:
        self.statements.append(sql_text)


@pytest.mark.asyncio
async def test_unapply_sql_for_create_and_addfield():
    """Rollback SQL should include drop column then drop table."""

    class Migration(BaseMigration):
        operations: ClassVar[list[Operation]] = [
            CreateModel(
                name="Foo",
                db_table="foo",
                fields=[
                    ("id", "IntField", {"primary_key": True}),
                ],
            ),
            AddField(
                model_name="Foo",
                db_table="foo",
                field_name="bar",
                field_type="CharField",
                options={"max_length": 50, "null": True},
            ),
        ]

    rec = _Recorder()
    ed = PostgresSchemaEditor()

    await Migration.unapply(rec, ed)
    # Should first drop the added column, then drop the table.
    assert rec.statements == [
        'ALTER TABLE "foo" DROP COLUMN IF EXISTS "bar";',
        'DROP TABLE IF EXISTS "foo";',
    ]


@pytest.mark.asyncio
async def test_unapply_to_sql_skips_runpython_side_effects():
    """Rollback SQL previews must not execute reverse RunPython callables."""
    seen: list[str | None] = []

    async def backwards(apps):
        seen.append(apps)

    class Migration(BaseMigration):
        operations: ClassVar[list[Operation]] = [
            RunPython(lambda: None, reverse_func=backwards),
        ]

    statements = await Migration.unapply_to_sql(
        conn=None,
        schema_editor=PostgresSchemaEditor(),
    )

    assert seen == []
    assert statements == ["-- No SQL preview for RunPython reverse callable"]


@pytest.mark.asyncio
async def test_unapply_sql_for_addfield_uses_db_column_override():
    """Rollback SQL should target the physical column name when it is overridden."""

    class Migration(BaseMigration):
        operations: ClassVar[list[Operation]] = [
            AddField(
                model_name="Book",
                db_table="book",
                field_name="author",
                field_type="ForeignKeyFieldInstance",
                options={
                    "db_column": "author_id",
                    "related_table": "author",
                    "to_field": "id",
                    "on_delete": "CASCADE",
                    "referenced_type": "UUIDField",
                },
            ),
        ]

    statements = await Migration.unapply_to_sql(
        conn=None,
        schema_editor=PostgresSchemaEditor(),
    )

    assert statements == ['ALTER TABLE "book" DROP COLUMN IF EXISTS "author_id";']


@pytest.mark.asyncio
async def test_tortoise_context_passes_through_app_models(monkeypatch):
    """Migration context should pass app model declarations through unchanged."""
    captured: dict[str, dict] = {}

    async def fake_init(cls, *, config: dict) -> None:
        captured["config"] = config

    async def fake_close_connections(cls) -> None:
        return None

    monkeypatch.setattr(
        "tortoisemarch.migrate.Tortoise.init",
        classmethod(fake_init),
    )
    monkeypatch.setattr(
        "tortoisemarch.migrate.Tortoise.close_connections",
        classmethod(fake_close_connections),
    )

    conf = {
        "connections": {"default": "sqlite://:memory:"},
        "apps": {
            "models": {
                "models": ("tests.models",),
                "default_connection": "default",
            },
        },
    }
    async with tortoise_context(conf):
        pass

    assert captured["config"]["apps"]["models"]["models"] == ("tests.models",)


@pytest.mark.asyncio
async def test_unapply_sql_for_alter_field():
    """Rollback for AlterField should emit DROP DEFAULT and DROP NOT NULL."""

    class Migration(BaseMigration):
        operations: ClassVar[list[Operation]] = [
            CreateModel(
                name="Foo",
                db_table="foo",
                fields=[("id", "IntField", {"primary_key": True})],
            ),
            AddField(
                model_name="Foo",
                db_table="foo",
                field_name="bar",
                field_type="CharField",
                options={"max_length": 50, "null": True, "default": None},
            ),
            AlterField(
                model_name="Foo",
                db_table="foo",
                field_name="bar",
                old_options={
                    "type": "CharField",
                    "max_length": 50,
                    "null": True,
                    "default": None,
                },
                new_options={
                    "type": "CharField",
                    "max_length": 50,
                    "null": False,
                    "default": "x",
                },
            ),
        ]

    rec = _Recorder()
    ed = PostgresSchemaEditor()

    await Migration.unapply(rec, ed)

    # First undo AlterField: drop default then drop NOT NULL (order per schema_editor)
    assert rec.statements == [
        'ALTER TABLE "foo" ALTER COLUMN "bar" DROP NOT NULL;',
        'ALTER TABLE "foo" ALTER COLUMN "bar" DROP DEFAULT;',
        'ALTER TABLE "foo" DROP COLUMN IF EXISTS "bar";',
        'DROP TABLE IF EXISTS "foo";',
    ]
