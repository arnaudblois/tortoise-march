"""Test for the migration writer."""

from enum import StrEnum
from pathlib import Path

from tortoisemarch.model_state import FieldState, ModelState
from tortoisemarch.operations import CreateModel, RenameModel
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
                (
                    "id",
                    "UUIDField",
                    {"primary_key": True, "default": "python_callable"},
                ),
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


def test_empty_migration_runpython_stub_is_valid(tmp_path: Path):
    """Test that we can create an empty RunPython migration."""
    path = Path(write_migration([], tmp_path, empty=True))
    content = path.read_text(encoding="utf-8")

    # Import present
    assert "from tortoisemarch.operations import RunPython" in content

    # Suggested invocation matches your RunPython signature
    assert "RunPython(forwards, reverse_func=backwards)" in content

    # Stub functions are present
    assert "async def forwards():" in content
    assert "async def backwards():" in content


def test_auto_name_includes_rename_model(tmp_path: Path):
    """Test that default migration name includes RenameModel operations."""
    # Simulate an existing initial migration so numbering starts at 0002.
    (tmp_path / "__init__.py").touch()
    (tmp_path / "0001_initial.py").write_text("# initial\n", encoding="utf-8")

    ops = [
        RenameModel(
            old_name="CompanyUser",
            new_name="CompanyMember",
            old_db_table="company_user",
            new_db_table="company_member",
        ),
    ]

    out_path = write_migration(ops, migrations_dir=tmp_path)
    fname = Path(out_path).name

    assert fname.startswith("0002_rename_companyuser_to_companymember")


def test_compaction_removes_redundant_pk_flags(tmp_path: Path):
    """Primary keys should not serialize redundant unique/index/null flags."""
    ms = ModelState(
        name="Item",
        db_table="item",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
                index=True,
                null=False,
            ),
        },
    )

    op = CreateModel.from_model_state(ms)
    _, _, opts = op.fields[0]
    assert opts == {"primary_key": True}
