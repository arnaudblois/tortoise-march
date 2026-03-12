"""Test module for migrate command."""

import importlib
import sys
import textwrap
from pathlib import Path

import pytest
from tortoise import Tortoise

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.makemigrations import makemigrations
from tortoisemarch.migrate import migrate, tortoise_context
from tortoisemarch.recorder import MigrationRecorder


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


async def test_migrate_roundtrip_with_exact_sql(tmp_path: Path):  # noqa: PLR0915
    """Evolve schema step-by-step and assert exact SQL.

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
        """Overwrite the models file (with cache busting), generate migrations.

        CPython reuses .pyc bytecode when the file size and (second-resolution)
        mtime haven't changed, which often happens in these rapid test steps.
        Without clearing the cached .pyc and invalidating import caches, the
        old model definitions may be reused and schema diffs will be wrong.
        """
        (models_dir / "__init__.py").write_text(models_code)

        # Ensure Python does not reuse stale bytecode
        # Otherwise this causes some
        pycache_dir = models_dir / "__pycache__"
        if pycache_dir.exists():
            for f in pycache_dir.glob("__init__.*.pyc"):
                f.unlink()

        importlib.invalidate_caches()

        if "models" in sys.modules:
            del sys.modules["models"]

        tortoise_orm = _tortoise_conf("models")
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)
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
        '"id" UUID PRIMARY KEY, '
        '"title" VARCHAR(200) NOT NULL'
        ");"
    )

    # Apply migration
    await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

    # Insert a row to ensure schema works
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
                is_superuser = fields.BooleanField(default=False)
            """,
        ),
    )

    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == (
        'CREATE TABLE "author" ('
        '"id" UUID PRIMARY KEY, '
        '"is_superuser" BOOLEAN NOT NULL DEFAULT FALSE, '
        '"name" VARCHAR(200) NOT NULL'
        ");"
    )

    # Apply and verify pre-existing data survived
    await migrate(tortoise_orm, migrations_dir)
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Book  # noqa: PLC0415

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
                is_superuser = fields.BooleanField(default=False)
                active = fields.BooleanField(default=True)
            """,
        ),
    )

    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == (
        'ALTER TABLE "author" ADD COLUMN "active" BOOLEAN NOT NULL DEFAULT TRUE;'
    )

    # Apply and check defaults/data
    await migrate(tortoise_orm, migrations_dir)
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Author, Book  # noqa: PLC0415

    a = await Author.create(name="Tolkien")
    assert a.active is True
    books = await Book.all()
    assert books[0].title == "The Hobbit"
    await Tortoise.close_connections()

    # -------- Step 4: Add nullable FK Book.author -> Author --------
    tortoise_orm = await prepare_models(
        textwrap.dedent(
            """
            from uuid import uuid4
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                title = fields.CharField(max_length=200)
                author = fields.ForeignKeyField(
                    "models.Author",
                    related_name="books",
                    null=True,
                    on_delete=fields.CASCADE,
                )

            class Author(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                name = fields.CharField(max_length=200)
                is_superuser = fields.BooleanField(default=False)
                active = fields.BooleanField(default=True)
            """,
        ),
    )

    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    # Note: null=True -> no NOT NULL in SQL
    assert sql == (
        'ALTER TABLE "book" ADD COLUMN '
        '"author_id" UUID REFERENCES "author" ("id") ON DELETE CASCADE;'
    )

    # Apply and verify FK works + existing data survives
    await migrate(tortoise_orm, migrations_dir)
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Author, Book  # noqa: PLC0415

    # We already have 1 Book + 1 Author from previous steps
    assert await Book.all().count() == 1
    assert await Author.all().count() == 1

    new_author = await Author.create(name="Pratchett")
    new_book = await Book.create(title="Small Gods", author=new_author)

    # Now we're using the *current* Book class, so author_id exists
    assert new_book.author_id == new_author.id
    titles = {b.title for b in await Book.all()}
    assert titles == {"The Hobbit", "Small Gods"}
    await Tortoise.close_connections()

    # -------- Step 5: Widen Book.title from 200 to 300 chars --------
    tortoise_orm = await prepare_models(
        textwrap.dedent(
            """
            from uuid import uuid4
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                title = fields.CharField(max_length=300)
                author = fields.ForeignKeyField(
                    "models.Author",
                    related_name="books",
                    null=True,
                    on_delete=fields.CASCADE,
                )

            class Author(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                name = fields.CharField(max_length=200)
                is_superuser = fields.BooleanField(default=False)
                active = fields.BooleanField(default=True)
            """,
        ),
    )
    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == ('ALTER TABLE "book" ALTER COLUMN "title" TYPE VARCHAR(300);')

    # Apply and ensure data still present
    await migrate(tortoise_orm, migrations_dir)
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Author, Book  # noqa: PLC0415

    titles = {b.title for b in await Book.all()}
    assert titles == {"The Hobbit", "Small Gods"}
    authors = {a.name for a in await Author.all()}
    assert authors == {"Tolkien", "Pratchett"}
    await Tortoise.close_connections()

    # -------- Step 6: Make Author.name nullable --------
    tortoise_orm = await prepare_models(
        textwrap.dedent(
            """
            from uuid import uuid4
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                title = fields.CharField(max_length=300)
                author = fields.ForeignKeyField(
                    "models.Author",
                    related_name="books",
                    null=True,
                    on_delete=fields.CASCADE,
                )

            class Author(models.Model):
                id = fields.UUIDField(primary_key=True, default=uuid4)
                name = fields.CharField(max_length=200, null=True)
                is_superuser = fields.BooleanField(default=False)
                active = fields.BooleanField(default=True)
            """,
        ),
    )

    sql = await migrate(tortoise_conf=tortoise_orm, location=migrations_dir, sql=True)
    assert sql == ('ALTER TABLE "author" ALTER COLUMN "name" DROP NOT NULL;')

    # Apply and verify we can create an author with no name
    await migrate(tortoise_orm, migrations_dir)
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Author, Book  # noqa: PLC0415

    nameless = await Author.create(name=None)
    assert nameless.name is None
    # Existing books still present
    titles = {b.title for b in await Book.all()}
    assert titles == {"The Hobbit", "Small Gods"}
    await Tortoise.close_connections()


async def test_migrate_rolls_back_and_not_recorded_on_failure(tmp_path: Path):
    """Migration failure should roll back DDL and not mark as applied."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    # Write a migration that creates a table then raises
    boom = migrations_dir / "0001_boom.py"
    boom.write_text(
        textwrap.dedent(
            """
            class Migration:
                @staticmethod
                async def apply(conn, schema_editor):
                    await conn.execute_script('CREATE TABLE "boom" (id INT);')
                    raise RuntimeError("boom")

                @staticmethod
                async def unapply(conn, schema_editor):
                    await conn.execute_script('DROP TABLE IF EXISTS "boom";')

                @staticmethod
                async def to_sql(conn, schema_editor):
                    return ['CREATE TABLE "boom" (id INT);']
            """,
        ),
    )

    tortoise_orm = _tortoise_conf("models")

    with pytest.raises(RuntimeError):
        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

    # Ensure migration not recorded
    async with tortoise_context(tortoise_orm):
        applied = await MigrationRecorder.list_applied()
        assert "0001_boom" not in applied
        # Table should not exist
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_name='boom');",
            [],
        )
        assert rows
        assert rows[0].get("exists") is False


async def test_runpython_data_migration_uses_orm(tmp_path: Path):
    """RunPython migrations should be able to use the Tortoise ORM."""
    # Setup: migrations + models packages
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    # --- Step 1: create Book model and generate initial migration ---
    models_code = textwrap.dedent(
        """
        from uuid import uuid4
        from tortoise import fields, models

        class Book(models.Model):
            id = fields.UUIDField(primary_key=True, default=uuid4)
            title = fields.CharField(max_length=200)
        """,
    )
    (models_dir / "__init__.py").write_text(models_code)

    if "models" in sys.modules:
        del sys.modules["models"]

    tortoise_orm = _tortoise_conf("models")

    # Generate initial migration (0001_initial) - makemigrations uses sqlite in-memory
    await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)

    # Apply the initial migration to create the table in Postgres
    await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

    # Insert some data using the ORM (now we init Tortoise against Postgres)
    await Tortoise.init(config=_tortoise_conf("models"))
    from models import Book  # noqa: PLC0415

    await Book.create(title="The Hobbit")
    await Book.create(title="Dune")
    books = await Book.all().order_by("title")
    assert [b.title for b in books] == ["Dune", "The Hobbit"]
    await Tortoise.close_connections()

    # --- Step 2: create a manual RunPython migration that uses the ORM ---

    data_migration_path = migrations_dir / "0002_uppercase_titles.py"
    data_migration_path.write_text(
        textwrap.dedent(
            """
            from tortoisemarch.base import BaseMigration
            from tortoisemarch.operations import RunPython

            async def forwards(conn, schema_editor):
                # Use the ORM to mutate data
                from models import Book
                books = await Book.all()
                for book in books:
                    book.title = book.title.upper()
                    await book.save()

            class Migration(BaseMigration):
                operations = [
                    RunPython(forwards),
                ]
            """,
        ),
    )

    # At this point:
    # - 0001_* has been recorded as applied
    # - 0002_uppercase_titles is pending

    # Run the data migration
    await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

    # Verify the RunPython code actually ran and used the ORM
    await Tortoise.init(config=_tortoise_conf("models"))
    books = await Book.all().order_by("title")
    titles = [b.title for b in books]
    assert titles == ["DUNE", "THE HOBBIT"]
    await Tortoise.close_connections()


async def test_runpython_uses_historical_models_for_schema_state(
    tmp_path: Path,
):
    """RunPython should see the schema state at that migration step."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str) -> None:
        """Overwrite models and clear import/bytecode caches."""
        (models_dir / "__init__.py").write_text(code)

        pycache_dir = models_dir / "__pycache__"
        if pycache_dir.exists():
            for file in pycache_dir.glob("__init__.*.pyc"):
                file.unlink()

        importlib.invalidate_caches()
        sys.modules.pop("models", None)

    try:
        write_models(
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
        tortoise_orm = _tortoise_conf("models")
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)
        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        await Tortoise.init(config=tortoise_orm)
        from models import Book  # noqa: PLC0415

        await Book.create(title="Dune")
        await Tortoise.close_connections()

        # The live model no longer matches the database schema here: it expects
        # `heading`, while the DB still has `title`. RunPython must therefore use
        # the migration state instead of importing the live model.
        write_models(
            textwrap.dedent(
                """
                from uuid import uuid4
                from tortoise import fields, models

                class Book(models.Model):
                    id = fields.UUIDField(primary_key=True, default=uuid4)
                    heading = fields.CharField(max_length=200)
                    subtitle = fields.CharField(max_length=200, null=True)
                """,
            ),
        )

        (migrations_dir / "0002_add_subtitle_and_backfill.py").write_text(
            textwrap.dedent(
                """
                from tortoisemarch.base import BaseMigration
                from tortoisemarch.operations import AddField, RunPython

                async def forwards(apps):
                    Book = apps.get_model("Book")
                    for book in await Book.all().order_by("title"):
                        book.subtitle = book.title.upper()
                        await book.save(update_fields=["subtitle"])

                class Migration(BaseMigration):
                    operations = [
                        AddField(
                            model_name="Book",
                            db_table="book",
                            field_name="subtitle",
                            field_type="CharField",
                            options={"max_length": 200, "null": True},
                        ),
                        RunPython(forwards),
                    ]
                """,
            ),
        )

        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        async with tortoise_context(tortoise_orm):
            conn = Tortoise.get_connection("default")
            rows = await conn.execute_query_dict(
                'SELECT title, subtitle FROM "book" ORDER BY title',
                [],
            )

        assert rows == [{"title": "Dune", "subtitle": "DUNE"}]
    finally:
        sys.path.remove(str(tmp_path))


async def test_runpython_reverse_uses_historical_models(tmp_path: Path):
    """Rollback RunPython should see the pre-unapply historical schema."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str) -> None:
        """Overwrite models and clear import/bytecode caches."""
        (models_dir / "__init__.py").write_text(code)

        pycache_dir = models_dir / "__pycache__"
        if pycache_dir.exists():
            for file in pycache_dir.glob("__init__.*.pyc"):
                file.unlink()

        importlib.invalidate_caches()
        sys.modules.pop("models", None)

    try:
        write_models(
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
        tortoise_orm = _tortoise_conf("models")
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)
        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        await Tortoise.init(config=tortoise_orm)
        from models import Book  # noqa: PLC0415

        await Book.create(title="dune")
        await Tortoise.close_connections()

        write_models(
            textwrap.dedent(
                """
                from uuid import uuid4
                from tortoise import fields, models

                class Book(models.Model):
                    id = fields.UUIDField(primary_key=True, default=uuid4)
                    heading = fields.CharField(max_length=200)
                """,
            ),
        )

        (migrations_dir / "0002_uppercase_titles.py").write_text(
            textwrap.dedent(
                """
                from tortoisemarch.base import BaseMigration
                from tortoisemarch.operations import RunPython

                async def forwards(apps):
                    Book = apps.get_model("Book")
                    for book in await Book.all():
                        book.title = book.title.upper()
                        await book.save(update_fields=["title"])

                async def backwards(apps):
                    Book = apps.get_model("Book")
                    for book in await Book.all():
                        book.title = book.title.lower()
                        await book.save(update_fields=["title"])

                class Migration(BaseMigration):
                    operations = [
                        RunPython(forwards, reverse_func=backwards),
                    ]
                """,
            ),
        )

        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        async with tortoise_context(tortoise_orm):
            conn = Tortoise.get_connection("default")
            rows = await conn.execute_query_dict(
                'SELECT title FROM "book" ORDER BY title',
                [],
            )
        assert rows == [{"title": "DUNE"}]

        await migrate(
            tortoise_conf=tortoise_orm,
            location=migrations_dir,
            target="0001_initial",
        )

        async with tortoise_context(tortoise_orm):
            conn = Tortoise.get_connection("default")
            rows = await conn.execute_query_dict(
                'SELECT title FROM "book" ORDER BY title',
                [],
            )
        assert rows == [{"title": "dune"}]
    finally:
        sys.path.remove(str(tmp_path))


async def test_rollback_sql_preview_does_not_execute_runpython_reverse(
    tmp_path: Path,
):
    """Rollback SQL previews must not run reverse Python code."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    def write_models(code: str) -> None:
        """Overwrite models and clear import/bytecode caches."""
        (models_dir / "__init__.py").write_text(code)

        pycache_dir = models_dir / "__pycache__"
        if pycache_dir.exists():
            for file in pycache_dir.glob("__init__.*.pyc"):
                file.unlink()

        importlib.invalidate_caches()
        sys.modules.pop("models", None)

    try:
        write_models(
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
        tortoise_orm = _tortoise_conf("models")
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)
        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        await Tortoise.init(config=tortoise_orm)
        from models import Book  # noqa: PLC0415

        await Book.create(title="dune")
        await Tortoise.close_connections()

        (migrations_dir / "0002_uppercase_titles.py").write_text(
            textwrap.dedent(
                """
                from tortoisemarch.base import BaseMigration
                from tortoisemarch.operations import RunPython

                async def forwards(apps):
                    Book = apps.get_model("Book")
                    for book in await Book.all():
                        book.title = book.title.upper()
                        await book.save(update_fields=["title"])

                async def backwards(apps):
                    Book = apps.get_model("Book")
                    for book in await Book.all():
                        book.title = book.title.lower()
                        await book.save(update_fields=["title"])

                class Migration(BaseMigration):
                    operations = [
                        RunPython(forwards, reverse_func=backwards),
                    ]
                """,
            ),
        )

        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        async with tortoise_context(tortoise_orm):
            conn = Tortoise.get_connection("default")
            rows = await conn.execute_query_dict(
                'SELECT title FROM "book" ORDER BY title',
                [],
            )
        assert rows == [{"title": "DUNE"}]

        sql_preview = await migrate(
            tortoise_conf=tortoise_orm,
            location=migrations_dir,
            target="0001_initial",
            sql=True,
        )

        assert sql_preview == "-- No SQL preview for RunPython reverse callable"

        async with tortoise_context(tortoise_orm):
            conn = Tortoise.get_connection("default")
            rows = await conn.execute_query_dict(
                'SELECT title FROM "book" ORDER BY title',
                [],
            )
        assert rows == [{"title": "DUNE"}]
    finally:
        sys.path.remove(str(tmp_path))


async def test_migrate_applies_extensions_before_exclusion_constraints(
    tmp_path: Path,
):
    """Extension operations should replay before dependent exclusion constraints."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").write_text("")

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))

    try:
        (models_dir / "__init__.py").write_text(
            textwrap.dedent(
                """
                from tortoise import fields, models

                from tortoisemarch.constraints import (
                    ExclusionConstraint,
                    FieldRef,
                    RawSQL,
                )
                from tortoisemarch.extensions import PostgresExtension

                class Practitioner(models.Model):
                    id = fields.UUIDField(primary_key=True)

                class Booking(models.Model):
                    id = fields.UUIDField(primary_key=True)
                    practitioner = fields.ForeignKeyField(
                        "models.Practitioner",
                        related_name="bookings",
                    )
                    start_at = fields.DatetimeField()
                    end_at = fields.DatetimeField()

                    class Meta:
                        tortoisemarch_extensions = (
                            PostgresExtension("btree_gist"),
                        )
                        tortoisemarch_constraints = (
                            ExclusionConstraint(
                                expressions=(
                                    (FieldRef("practitioner"), "="),
                                    (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                                ),
                                name="bookings_no_overlap_per_practitioner",
                                index_type="gist",
                            ),
                        )
                """,
            ),
        )

        sys.modules.pop("models", None)
        tortoise_orm = _tortoise_conf("models")
        await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)

        sql_preview = await migrate(
            tortoise_conf=tortoise_orm,
            location=migrations_dir,
            sql=True,
        )
        assert sql_preview is not None
        assert 'CREATE EXTENSION IF NOT EXISTS "btree_gist";' in sql_preview
        assert 'ADD CONSTRAINT "bookings_no_overlap_per_practitioner"' in sql_preview
        assert sql_preview.index('CREATE EXTENSION IF NOT EXISTS "btree_gist";') < (
            sql_preview.index('ADD CONSTRAINT "bookings_no_overlap_per_practitioner"')
        )

        await migrate(tortoise_conf=tortoise_orm, location=migrations_dir)

        async with tortoise_context(tortoise_orm):
            conn = Tortoise.get_connection("default")
            extension_rows = await conn.execute_query_dict(
                "SELECT extname FROM pg_extension WHERE extname = 'btree_gist';",
                [],
            )
            constraint_rows = await conn.execute_query_dict(
                "SELECT conname FROM pg_constraint "
                "WHERE conname = 'bookings_no_overlap_per_practitioner';",
                [],
            )

        assert extension_rows == [{"extname": "btree_gist"}]
        assert constraint_rows == [
            {"conname": "bookings_no_overlap_per_practitioner"},
        ]
    finally:
        sys.path.remove(str(tmp_path))


async def test_migrate_to_target_forward_and_backward(tmp_path: Path):  # noqa: PLR0915
    """Migrate up to a target and back down again."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    # Minimal models module so Tortoise.init can register an app
    placeholder = tmp_path / "placeholder_models.py"
    placeholder.write_text(
        textwrap.dedent(
            """
            from tortoise import models


            class Placeholder(models.Model):
                class Meta:
                    table = "__placeholder__"
            """,
        ),
    )
    sys.path.insert(0, str(tmp_path))

    def write_migration(filename: str, body: str) -> None:
        (migrations_dir / filename).write_text(textwrap.dedent(body))

    write_migration(
        "0001_initial.py",
        """
        from typing import ClassVar
        from tortoisemarch.base import BaseMigration
        from tortoisemarch.operations import CreateModel


        class Migration(BaseMigration):
            operations: ClassVar[list] = [
                CreateModel(
                    name="Foo",
                    db_table="foo",
                    fields=[
                        ("id", "IntField", {"primary_key": True}),
                    ],
                ),
            ]
        """,
    )

    write_migration(
        "0002_add_bar.py",
        """
        from typing import ClassVar
        from tortoisemarch.base import BaseMigration
        from tortoisemarch.operations import AddField


        class Migration(BaseMigration):
            operations: ClassVar[list] = [
                AddField(
                    model_name="Foo",
                    db_table="foo",
                    field_name="bar",
                    field_type="CharField",
                    options={"max_length": 50, "null": True},
                ),
            ]
        """,
    )

    conf = {
        "connections": {"default": "postgres://postgres:test@localhost:5445/testdb"},
        "apps": {
            "models": {
                "models": ["placeholder_models"],
                "default_connection": "default",
            },
        },
    }

    # Migrate to 0001
    await migrate(tortoise_conf=conf, location=migrations_dir, target="0001")
    async with tortoise_context(conf):
        applied = await MigrationRecorder.list_applied()
        assert applied == ["0001_initial"]
        conn = Tortoise.get_connection("default")
        recorder_rows = await conn.execute_query_dict(
            "SELECT name FROM tortoisemarch_applied_migrations ORDER BY name;",
        )
        assert [r["name"] for r in recorder_rows] == ["0001_initial"]
        rows = await conn.execute_query_dict(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='foo' ORDER BY column_name;",
        )
        cols = [r["column_name"] for r in rows]
        assert cols == ["id"]
        # Verify bar truly absent and recorder unchanged
        has_bar = await conn.execute_query_dict(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='foo' AND column_name='bar';",
        )
        assert has_bar == []
        recorder_rows = await conn.execute_query_dict(
            "SELECT name FROM tortoisemarch_applied_migrations ORDER BY name;",
        )
        assert [r["name"] for r in recorder_rows] == ["0001_initial"]

    # Migrate forward to 0002
    await migrate(tortoise_conf=conf, location=migrations_dir, target="0002")
    async with tortoise_context(conf):
        applied = await MigrationRecorder.list_applied()
        assert applied == ["0001_initial", "0002_add_bar"]
        conn = Tortoise.get_connection("default")
        recorder_rows = await conn.execute_query_dict(
            "SELECT name FROM tortoisemarch_applied_migrations ORDER BY name;",
        )
        assert [r["name"] for r in recorder_rows] == ["0001_initial", "0002_add_bar"]
        rows = await conn.execute_query_dict(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='foo' ORDER BY column_name;",
        )
        cols = [r["column_name"] for r in rows]
        assert cols == ["bar", "id"]

    # Preview rollback SQL from 0002 -> 0001
    sql_preview = await migrate(
        tortoise_conf=conf,
        location=migrations_dir,
        target="0001",
        sql=True,
    )
    assert sql_preview == 'ALTER TABLE "foo" DROP COLUMN IF EXISTS "bar";'

    # Roll back to 0001 (real)
    await migrate(tortoise_conf=conf, location=migrations_dir, target="0001")
    async with tortoise_context(conf):
        applied = await MigrationRecorder.list_applied()
        assert applied == ["0001_initial"]
        conn = Tortoise.get_connection("default")
        recorder_rows = await conn.execute_query_dict(
            "SELECT name FROM tortoisemarch_applied_migrations ORDER BY name;",
        )
        assert [r["name"] for r in recorder_rows] == ["0001_initial"]
        rows = await conn.execute_query_dict(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='foo' ORDER BY column_name;",
        )
        cols = [r["column_name"] for r in rows]
        assert cols == ["id"]

    # Fake rollback back to 0002 (state only)
    await migrate(tortoise_conf=conf, location=migrations_dir, target="0002", fake=True)
    async with tortoise_context(conf):
        applied = await MigrationRecorder.list_applied()
        assert applied == ["0001_initial", "0002_add_bar"]
        conn = Tortoise.get_connection("default")
        rows = await conn.execute_query_dict(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='foo' ORDER BY column_name;",
        )
        cols = [r["column_name"] for r in rows]
        # Schema unchanged by fake move
        assert cols == ["id"]
        recorder_rows = await conn.execute_query_dict(
            "SELECT name FROM tortoisemarch_applied_migrations ORDER BY name;",
        )
        assert [r["name"] for r in recorder_rows] == ["0001_initial", "0002_add_bar"]

    sys.path.remove(str(tmp_path))


async def test_migrate_raises_if_applied_file_is_modified(tmp_path: Path):
    """Migrate should fail when an applied migration file is edited."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    placeholder = tmp_path / "placeholder_models.py"
    placeholder.write_text(
        textwrap.dedent(
            """
            from tortoise import models


            class Placeholder(models.Model):
                class Meta:
                    table = "__placeholder__"
            """,
        ),
    )
    sys.path.insert(0, str(tmp_path))

    migration_path = migrations_dir / "0001_initial.py"
    migration_path.write_text(
        textwrap.dedent(
            """
            from typing import ClassVar
            from tortoisemarch.base import BaseMigration
            from tortoisemarch.operations import CreateModel


            class Migration(BaseMigration):
                operations: ClassVar[list] = [
                    CreateModel(
                        name="Foo",
                        db_table="foo",
                        fields=[
                            ("id", "IntField", {"primary_key": True}),
                        ],
                    ),
                ]
            """,
        ),
    )

    conf = {
        "connections": {"default": "postgres://postgres:test@localhost:5445/testdb"},
        "apps": {
            "models": {
                "models": ["placeholder_models"],
                "default_connection": "default",
            },
        },
    }

    await migrate(tortoise_conf=conf, location=migrations_dir)

    # We append a harmless comment to simulate history edits after apply.
    migration_path.write_text(f"{migration_path.read_text()}\n# edited after apply\n")

    with pytest.raises(InvalidMigrationError, match="Checksum mismatch"):
        await migrate(tortoise_conf=conf, location=migrations_dir)

    sys.path.remove(str(tmp_path))
