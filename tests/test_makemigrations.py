"""Test the `makemigrations` command."""

import asyncio
import importlib
import sys
import textwrap
from pathlib import Path

import pytest
from tortoise import Tortoise

from tortoisemarch.makemigrations import makemigrations


@pytest.mark.asyncio
async def test_makemigrations_integration(tmp_path: Path, snapshot):
    """Integration test simulating a sequence of model evolutions.

    1) Create Book model.
    2) Add Author model (without 'active' field).
    3) Add 'active' field to Author.

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

    async def run_makemigrations() -> None:
        # Ensure (re)import of models for Tortoise to pick them up
        if "models" in sys.modules:
            importlib.reload(sys.modules["models"])
        else:
            importlib.import_module("models")

        await Tortoise._reset_apps()  # noqa: SLF001
        tortoise_orm = {
            "connections": {
                "default": "postgres://postgres:test@localhost:5445/testdb",
            },
            "apps": {"models": {"models": ["models"], "default_connection": "default"}},
        }
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)

    def newest_migration_text() -> str:
        files = sorted(
            f for f in migrations_dir.glob("*.py") if f.name != "__init__.py"
        )
        assert files, "No migration files found"
        return files[-1].read_text()

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
    await run_makemigrations()

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert all_py_names == {"__init__.py", "0001_initial.py"}

    mig_text = newest_migration_text()
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
    await run_makemigrations()

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert all_py_names == {
        "__init__.py",
        "0001_initial.py",
        "0002_create_author.py",
    }

    mig_text = newest_migration_text()
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
    await run_makemigrations()

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert all_py_names == {
        "__init__.py",
        "0001_initial.py",
        "0002_create_author.py",
        "0003_add_author_active.py",
    }

    mig_text = newest_migration_text()

    assert "\n".join(mig_text.split("\n")[1:]) == snapshot
    # We expect an AddField op targeting Author.active
    assert "AddField(" in mig_text.replace("\n", "").replace(" ", "")
    assert 'model_name="Author"' in mig_text
    assert 'field_name="active"' in mig_text
