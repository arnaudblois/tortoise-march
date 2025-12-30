"""Test for the migration writer."""

from enum import StrEnum

from tortoisemarch.operations import CreateModel
from tortoisemarch.writer import write_migration


class Status(StrEnum):
    """Test Enum to check it is correctly handled by the migration writer."""

    ACTIVE = "active"
    INACTIVE = "inactive"


def test_write_migration_serializes_enum_default_to_literal(tmp_path):
    """Test that default to Enum are correctly handled."""
    ops = [
        CreateModel(
            name="User",
            db_table="user",
            fields=[
                ("id", "UUIDField", {"primary_key": True, "default": "callable"}),
                ("status", "CharField", {"default": Status.ACTIVE, "max_length": 16}),
            ],
        ),
    ]

    out_path = write_migration(ops, migrations_dir=tmp_path)
    code = (tmp_path / out_path.split("/")[-1]).read_text(encoding="utf-8")

    # The key property: the invalid repr "<Status.ACTIVE: 'active'>"
    # must not appear in the generated file.
    assert "<Status" not in code
    assert "default" in code
    assert (
        '{"default": "active", "max_length": 16}' in code
    )  # we expect enum.value to be emitted
