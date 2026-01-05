"""Unit-level checks for migrate planning/target resolution."""

from typing import ClassVar

import pytest

from tortoisemarch.base import BaseMigration
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.migrate import plan_route, resolve_target_name
from tortoisemarch.operations import AddField, AlterField, CreateModel, Operation
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
