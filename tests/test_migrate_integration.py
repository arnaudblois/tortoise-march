"""Test module for migrate command."""

import importlib
import sys
import textwrap
from pathlib import Path

import pytest
from tortoise import Tortoise

from tortoisemarch.makemigrations import makemigrations
from tortoisemarch.migrate import migrate


def _tortoise_conf(models_module: str = "models") -> dict:
    """Return a test Tortoise config."""
    return {
        "connections": {"default": "postgres://postgres:test@localhost:5445/testdb"},
        "apps": {
            "models": {
                "models": [models_module],
                "default_connection": "default",
            },
        },
    }


@pytest.mark.asyncio
async def test_migrate_roundtrip_with_exact_sql(tmp_path: Path):
    """Evolve schema step-by-step.

    At each step:
    1) generate a migration
    2) preview SQL and assert the EXACT statements
    3) apply migration
    4) verify data integrity
    """
    # Make the migrations dir importable as a package
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    # Create a models package we can overwrite each step
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    async def prepare_models(models_code: str) -> dict:
        # Write models and (re)init Tortoise before importing models so they bind
        (models_dir / "__init__.py").write_text(models_code)
        await Tortoise._reset_apps()  # noqa: SLF001
        tortoise_orm = _tortoise_conf("models")
        await Tortoise.init(config=tortoise_orm)
        if "models" in sys.modules:
            importlib.reload(sys.modules["models"])
        else:
            importlib.import_module("models")
        # Generate migration for current state
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)
        await Tortoise.close_connections()
        return tortoise_orm

    # ---------------- Step 1: Create Book ----------------
    tortoise_orm = await prepare_models(
        textwrap.dedent(
            """
            from uuid import uuid4
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                title = fields.CharField(max_length=200)
            """,
        ),
    )

    # Preview SQL and assert exact string
    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == (
        'CREATE TABLE "book" ('
        '"id" UUID PRIMARY KEY NOT NULL UNIQUE, '
        '"title" VARCHAR(200) NOT NULL'
        ");"
    )

    # Apply migration
    await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

    # Insert a row to ensure schema works
    await Tortoise._reset_apps()  # noqa: SLF001
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Book  # noqa: PLC0415

    b = await Book.create(title="The Hobbit")
    assert b.id is not None
    await Tortoise.close_connections()

    # ---------------- Step 2: Add Author ----------------
    tortoise_orm = await prepare_models(
        textwrap.dedent(
            """
            from uuid import uuid4
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                title = fields.CharField(max_length=200)

            class Author(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                name = fields.CharField(max_length=200)
            """,
        ),
    )

    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == (
        'CREATE TABLE "author" ('
        '"id" UUID PRIMARY KEY NOT NULL UNIQUE, '
        '"name" VARCHAR(200) NOT NULL'
        ");"
    )

    # Apply and verify pre-existing data survived
    await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)
    await Tortoise._reset_apps()  # noqa: SLF001
    await Tortoise.init(config=_tortoise_conf("models"))
    assert await Book.all().count() == 1
    await Tortoise.close_connections()

    # -------- Step 3: Add Author.active (BOOLEAN DEFAULT True) --------
    tortoise_orm = await prepare_models(
        textwrap.dedent(
            """
            from uuid import uuid4
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                title = fields.CharField(max_length=200)

            class Author(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                name = fields.CharField(max_length=200)
                active = fields.BooleanField(default=True)
            """,
        ),
    )

    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == (
        'ALTER TABLE "author" ADD COLUMN "active" BOOLEAN NOT NULL DEFAULT True;'
    )

    # Apply and check defaults/data
    await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)
    await Tortoise._reset_apps()  # noqa: SLF001
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Author  # noqa: PLC0415

    a = await Author.create(name="Tolkien")
    assert a.active is True
    books = await Book.all()
    assert books[0].title == "The Hobbit"
    await Tortoise.close_connections()
