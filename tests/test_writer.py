"""Test for the migration writer."""

from enum import StrEnum
from pathlib import Path

from tortoisemarch.model_state import FieldState, ModelState
from tortoisemarch.operations import CreateIndex, CreateModel, RenameModel
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

    assert path.name.startswith("0001_initial")

    # Import present
    assert "from tortoisemarch.operations import RunPython" in content

    # Suggested invocation matches your RunPython signature
    assert "RunPython(forwards, reverse_func=backwards)" in content

    # Stub functions are present
    assert "async def forwards(" in content
    assert "async def backwards(" in content


def test_empty_migration_after_initial_is_data_migration(tmp_path: Path):
    """Empty migrations after the first should default to data_migration."""
    (tmp_path / "__init__.py").touch()
    (tmp_path / "0001_initial.py").write_text("# initial\n", encoding="utf-8")

    path = Path(write_migration([], tmp_path, empty=True))
    assert path.name.startswith("0002_data_migration")


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


def test_auto_name_prioritizes_models_over_indexes(tmp_path: Path):
    """CreateModel operations should win over CreateIndex in auto names."""
    (tmp_path / "__init__.py").touch()
    (tmp_path / "0001_initial.py").write_text("# initial\n", encoding="utf-8")

    ops = [
        CreateIndex(
            model_name="Book",
            db_table="book",
            columns=("author_id", "title"),
            name="book_author_id_title_idx",
        ),
        CreateModel(
            name="Book",
            db_table="book",
            fields=[
                (
                    "id",
                    "UUIDField",
                    {"primary_key": True, "default": "python_callable"},
                ),
            ],
        ),
        CreateIndex(
            model_name="Book",
            db_table="book",
            columns=("author_id", "title"),
            unique=True,
            name="book_author_id_title_uniq",
        ),
        CreateModel(
            name="Author",
            db_table="author",
            fields=[
                (
                    "id",
                    "UUIDField",
                    {"primary_key": True, "default": "python_callable"},
                ),
            ],
        ),
    ]

    out_path = write_migration(ops, migrations_dir=tmp_path)
    fname = Path(out_path).name

    assert fname.startswith("0002_create_book_create_author")
    assert fname.endswith("_and_more.py")


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
