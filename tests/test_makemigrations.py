"""Test the `makemigrations` command."""

import asyncio
import importlib
import sys
import textwrap
from pathlib import Path

import pytest
from tortoise import Tortoise

from tortoisemarch import makemigrations as mm
from tortoisemarch.makemigrations import makemigrations


def newest_migration_text(migrations_dir) -> str:
    """Return the content of the latest migration created."""
    files = sorted(f for f in migrations_dir.glob("*.py") if f.name != "__init__.py")
    assert files, "No migration files found"
    return files[-1].read_text()


async def run_makemigrations(migrations_dir) -> None:
    """Ensure (re)import of models for Tortoise before calling makemigrations."""
    # Always reload models from disk to avoid stale class attributes after renames.
    if "models" in sys.modules:
        del sys.modules["models"]
    importlib.import_module("models")

    await Tortoise._reset_apps()  # noqa: SLF001
    tortoise_orm = {
        "connections": {
            "default": "postgres://postgres:test@localhost:5445/testdb",
        },
        "apps": {"models": {"models": ["models"], "default_connection": "default"}},
    }
    await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)


@pytest.mark.asyncio
async def test_makemigrations_integration(tmp_path: Path, snapshot):  # noqa: PLR0915
    """Integration test simulating a sequence of model evolutions.

    1) Create Book model.
    2) Add Author model (without 'active' field).
    3) Add 'active' field to Author.
    4) Rename Author to Writer and add an index.

    Asserts:
      - migration files count after each step (includes __init__.py)
      - the newest migration contains the expected operation(s)
    """
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str) -> None:
        (models_dir / "__init__.py").write_text(code)

    # --- Step 1: Book model ---------------------------------------------------
    model_code_1 = textwrap.dedent(
        """
        from tortoise import fields, models

        class PrimaryKeyField(fields.UUIDField):
            def __init__(self, **kwargs):
                kwargs.setdefault("primary_key", True)
                super().__init__(**kwargs)

        class Book(models.Model):
            id = PrimaryKeyField()
            title = fields.CharField(max_length=100)
        """,
    )
    write_models(model_code_1)
    await run_makemigrations(migrations_dir)

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert all_py_names == {"__init__.py", "0001_initial.py"}

    mig_text = newest_migration_text(migrations_dir)
    # We remove the first line to avoid having to deal with the creation
    # datetime in the initial docstring.
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot
    assert 'CreateModel(name="Book"' in mig_text.replace("\n", "").replace(" ", "")
    assert "PrinaryKeyField" not in mig_text
    assert "fields=" in mig_text

    # --- Step 2: Add Author model --------------------------------------------
    model_code_2 = textwrap.dedent(
        """
        from tortoise import fields, models

        class PrimaryKeyField(fields.UUIDField):
            def __init__(self, **kwargs):
                kwargs.setdefault("primary_key", True)
                super().__init__(**kwargs)

        class Book(models.Model):
            id = PrimaryKeyField()
            title = fields.CharField(max_length=100)

        class Author(models.Model):
            id = fields.IntField(primary_key=True)
            name = fields.CharField(max_length=100)
        """,
    )
    write_models(model_code_2)
    await asyncio.sleep(0)  # yield once; not strictly necessary but harmless
    await run_makemigrations(migrations_dir)

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert all_py_names == {
        "__init__.py",
        "0001_initial.py",
        "0002_create_author.py",
    }

    mig_text = newest_migration_text(migrations_dir)
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot
    assert 'CreateModel(name="Author"' in mig_text.replace("\n", "").replace(" ", "")

    # --- Step 3: Add 'active' to Author --------------------------------------
    model_code_3 = textwrap.dedent(
        """
        from tortoise import fields, models

        class PrimaryKeyField(fields.UUIDField):
            def __init__(self, **kwargs):
                kwargs.setdefault("primary_key", True)
                super().__init__(**kwargs)

        class Book(models.Model):
            id = PrimaryKeyField()
            title = fields.CharField(max_length=100)

        class Author(models.Model):
            id = fields.IntField(primary_key=True)
            name = fields.CharField(max_length=100)
            active = fields.BooleanField(default=True)
        """,
    )
    write_models(model_code_3)
    await asyncio.sleep(0)
    await run_makemigrations(migrations_dir)

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert all_py_names == {
        "__init__.py",
        "0001_initial.py",
        "0002_create_author.py",
        "0003_add_author_active.py",
    }

    mig_text = newest_migration_text(migrations_dir)

    assert "\n".join(mig_text.split("\n")[1:]) == snapshot
    # We expect an AddField op targeting Author.active
    assert "AddField(" in mig_text.replace("\n", "").replace(" ", "")
    assert 'model_name="Author"' in mig_text
    assert 'field_name="active"' in mig_text

    # --- Step 4: Rename Author -> Writer and add Meta index -----------------
    model_code_4 = textwrap.dedent(
        """
        from tortoise import fields, models

        class PrimaryKeyField(fields.UUIDField):
            def __init__(self, **kwargs):
                kwargs.setdefault("primary_key", True)
                super().__init__(**kwargs)

        class Book(models.Model):
            id = PrimaryKeyField()
            title = fields.CharField(max_length=100)

        class Writer(models.Model):
            id = fields.IntField(primary_key=True)
            name = fields.CharField(max_length=100)
            active = fields.BooleanField(default=True)

            class Meta:
                table = "writer"
                indexes = (("name",),)
    """,
    )
    write_models(model_code_4)
    await asyncio.sleep(0)
    await run_makemigrations(migrations_dir)

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert any("rename_author_to_writer" in name for name in all_py_names)

    mig_text = newest_migration_text(migrations_dir)
    flat = mig_text.replace(" ", "").replace("\n", "")
    assert "RenameModel(" in mig_text
    assert "CreateIndex" in mig_text
    assert 'columns=("name",)' in flat
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot


@pytest.mark.asyncio
async def test_makemigrations_emits_renamefield_for_manual_rename(
    tmp_path,
    monkeypatch,
    snapshot,
):
    """Test makemigrations with a field rename."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str):
        (models_dir / "__init__.py").write_text(textwrap.dedent(code))
        # avoid stale bytecode reuse
        pycache = models_dir / "__pycache__"
        if pycache.exists():
            for f in pycache.glob("__init__.*.pyc"):
                f.unlink()

        if "models" in sys.modules:
            del sys.modules["models"]

    # Step 1: initial model with `title`
    write_models(
        """
        from uuid import uuid4
        from tortoise import fields, models

        class Book(models.Model):
            id = fields.UUIDField(primary_key=True, default=uuid4)
            title = fields.CharField(max_length=200)
        """,
    )
    await run_makemigrations(migrations_dir)

    # Step 2: rename field `title` -> `name` (same type/options)
    write_models(
        """
        from uuid import uuid4
        from tortoise import fields, models

        class Book(models.Model):
            id = fields.UUIDField(primary_key=True, default=uuid4)
            name = fields.CharField(max_length=200)
        """,
    )

    # Always accept the suggested rename
    monkeypatch.setattr(mm, "_safe_input", lambda *_, **__: True)

    await run_makemigrations(migrations_dir)
    mig_text = newest_migration_text(migrations_dir)
    assert "RenameField" in mig_text
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot

    # Step 3: add another unrelated field
    write_models(
        """
        from uuid import uuid4
        from tortoise import fields, models

        class Book(models.Model):
            id = fields.UUIDField(primary_key=True, default=uuid4)
            another_field = fields.CharField(max_length=200, default="")
        """,
    )

    # Reject the suggested rename, and indicate there is no match
    monkeypatch.setattr(mm, "_safe_input", lambda *_, **__: False)
    monkeypatch.setattr(mm, "_input_int", lambda *_, **__: 0)

    await run_makemigrations(migrations_dir)
    mig_text = newest_migration_text(migrations_dir)
    assert "AddField" in mig_text
    assert "RemoveField" in mig_text
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot


async def test_makemigrations_emits_renamemodel_for_model_rename(
    tmp_path,
    snapshot,
):
    """Makemigrations emits RenameModel instead of drop/create for model renames."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str) -> None:
        """Write models module and clear import caches."""
        (models_dir / "__init__.py").write_text(textwrap.dedent(code))
        pycache = models_dir / "__pycache__"
        if pycache.exists():
            for f in pycache.glob("__init__.*.pyc"):
                f.unlink()
        if "models" in sys.modules:
            del sys.modules["models"]

    # Step 1: initial models
    write_models(
        """
        from tortoise import fields, models

        class Author(models.Model):
            id = fields.IntField(primary_key=True)
            name = fields.CharField(max_length=100)

        class Book(models.Model):
            id = fields.IntField(primary_key=True)
            author = fields.ForeignKeyField(
                "models.Author",
                related_name="books",
                null=True,
            )
        """,
    )
    await run_makemigrations(migrations_dir)

    # Step 2: rename Author -> Writer
    write_models(
        """
        from tortoise import fields, models

        class Writer(models.Model):
            id = fields.IntField(primary_key=True)
            name = fields.CharField(max_length=100)

        class Book(models.Model):
            id = fields.IntField(primary_key=True)
            author = fields.ForeignKeyField(
                "models.Writer",
                related_name="books",
                null=True,
            )
        """,
    )
    await run_makemigrations(migrations_dir)

    mig_text = newest_migration_text(migrations_dir)

    assert "RenameModel" in mig_text

    flat = mig_text.replace("\n", "").replace(" ", "")
    assert "RenameModel(" in flat
    assert "old_name='Author'" in flat or 'old_name="Author"' in flat
    assert "new_name='Writer'" in flat or 'new_name="Writer"' in flat
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot


@pytest.mark.asyncio
async def test_makemigrations_emits_createindex_for_meta_indexes(
    tmp_path: Path,
    snapshot,
):
    """Meta-level indexes should generate CreateIndex operations."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str) -> None:
        (models_dir / "__init__.py").write_text(textwrap.dedent(code))
        pycache = models_dir / "__pycache__"
        if pycache.exists():
            for f in pycache.glob("__init__.*.pyc"):
                f.unlink()
        if "models" in sys.modules:
            del sys.modules["models"]

    write_models(
        """
        from tortoise import fields, models

        class Book(models.Model):
            id = fields.IntField(primary_key=True)
            slug = fields.CharField(max_length=50)

            class Meta:
                indexes = (("slug", "id"),)
        """,
    )

    await run_makemigrations(migrations_dir)
    mig_text = newest_migration_text(migrations_dir)
    assert "CreateIndex" in mig_text
    assert 'columns=("slug","id")' in mig_text.replace(" ", "")

    # Ignore timestamp line for snapshot stability
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot
