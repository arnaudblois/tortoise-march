"""Test the `makemigrations` command."""

import importlib
import sys
import textwrap
from pathlib import Path

import pytest

from tortoisemarch import makemigrations as mm
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.makemigrations import (
    _init_tortoise_apps,
    _validate_related_models,
    makemigrations,
)
from tortoisemarch.model_state import FieldState, ModelState, ProjectState


def newest_migration_text(migrations_dir) -> str:
    """Return the content of the latest migration created."""
    files = sorted(f for f in migrations_dir.glob("*.py") if f.name != "__init__.py")
    assert files, "No migration files found"
    return files[-1].read_text()


async def run_makemigrations(migrations_dir, *, check_only: bool = False) -> None:
    """Ensure (re)import of models for Tortoise before calling makemigrations."""
    # Always reload models from disk to avoid stale class attributes after renames.
    if "models" in sys.modules:
        del sys.modules["models"]
    importlib.import_module("models")

    tortoise_orm = {
        "connections": {
            "default": "postgres://postgres:test@localhost:5445/testdb",
        },
        "apps": {"models": {"models": ["models"], "default_connection": "default"}},
    }
    await makemigrations(
        tortoise_conf=tortoise_orm,
        location=migrations_dir,
        check_only=check_only,
    )


async def run_makemigrations_with_modules(
    migrations_dir,
    modules: dict[str, list[str]],
):
    """Run makemigrations with a custom app/module mapping."""
    # Clear any previously imported modules to avoid stale state
    for mod in {mod for mods in modules.values() for mod in mods}:
        if mod in sys.modules:
            del sys.modules[mod]
        importlib.import_module(mod)
    tortoise_orm = {
        "connections": {
            "default": "postgres://postgres:test@localhost:5445/testdb",
        },
        "apps": {
            label: {"models": mods, "default_connection": "default"}
            for label, mods in modules.items()
        },
    }
    await makemigrations(tortoise_conf=tortoise_orm, location=migrations_dir)


@pytest.mark.asyncio
async def test_init_tortoise_apps_passes_through_app_models(monkeypatch):
    """Tortoise init should receive app model declarations unchanged."""
    captured: dict[str, dict] = {}

    async def fake_init(cls, *, config: dict) -> None:
        captured["config"] = config

    monkeypatch.setattr(
        "tortoisemarch.makemigrations.Tortoise.init",
        classmethod(fake_init),
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
    await _init_tortoise_apps(conf)

    assert captured["config"]["apps"]["models"]["models"] == ("tests.models",)


async def test_makemigrations_integration(tmp_path: Path, snapshot):
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
    await run_makemigrations(migrations_dir)

    all_py_names = {str(x).split("/")[-1] for x in migrations_dir.glob("*.py")}
    assert any("rename_author_to_writer" in name for name in all_py_names)

    mig_text = newest_migration_text(migrations_dir)
    flat = mig_text.replace(" ", "").replace("\n", "")
    assert "RenameModel(" in mig_text
    assert "CreateIndex" in mig_text
    assert 'columns=("name",)' in flat
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot


async def test_makemigrations_multi_app_with_cross_fk(tmp_path: Path, snapshot):
    """Integration check: models across app labels with cross-app FK."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    apps_dir = tmp_path / "apps"
    apps_dir.mkdir()
    sys.path.insert(0, str(apps_dir))

    catalog_mod = apps_dir / "catalog_models.py"
    catalog_mod.write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Team(models.Model):
                id = fields.IntField(primary_key=True)
                name = fields.CharField(max_length=100)

                class Meta:
                    table = "team"
            """,
        ),
    )

    accounts_mod = apps_dir / "accounts_models.py"
    accounts_mod.write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Member(models.Model):
                id = fields.IntField(primary_key=True)
                user_id = fields.IntField()
                team = fields.ForeignKeyField("catalog.Team", related_name="members")

                class Meta:
                    table = "member"
            """,
        ),
    )

    modules = {
        "catalog": ["catalog_models"],
        "accounts": ["accounts_models"],
    }

    await run_makemigrations_with_modules(migrations_dir, modules)

    mig_text = newest_migration_text(migrations_dir)
    flat = mig_text.replace(" ", "").replace("\n", "")
    assert 'CreateModel(name="Team"' in flat
    assert 'CreateModel(name="Member"' in flat
    # Cross-app FK should target team table
    assert '"related_table": "team"' in mig_text
    assert "\n".join(mig_text.split("\n")[1:]) == snapshot
    sys.path.remove(str(apps_dir))


async def test_makemigrations_allows_duplicate_model_names_across_apps(tmp_path: Path):
    """Models with the same class name across apps should both appear."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    apps_dir = tmp_path / "apps"
    apps_dir.mkdir()
    sys.path.insert(0, str(apps_dir))

    (apps_dir / "catalog_models.py").write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.IntField(primary_key=True)

                class Meta:
                    table = "catalog_book"
            """,
        ),
    )

    (apps_dir / "sales_models.py").write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.IntField(primary_key=True)

                class Meta:
                    table = "sales_book"
            """,
        ),
    )

    modules = {
        "catalog": ["catalog_models"],
        "sales": ["sales_models"],
    }

    await run_makemigrations_with_modules(migrations_dir, modules)

    mig_text = newest_migration_text(migrations_dir)
    flat = mig_text.replace(" ", "").replace("\n", "").replace('"', "'")
    assert "CreateModel(name='catalog.Book'" in flat
    assert "CreateModel(name='sales.Book'" in flat
    assert "db_table='catalog_book'" in flat
    assert "db_table='sales_book'" in flat
    sys.path.remove(str(apps_dir))


@pytest.mark.asyncio
async def test_makemigrations_raises_on_duplicate_db_tables(tmp_path: Path):
    """Conflicting db_table names across apps should raise."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    apps_dir = tmp_path / "apps"
    apps_dir.mkdir()
    sys.path.insert(0, str(apps_dir))

    (apps_dir / "catalog_models.py").write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.IntField(primary_key=True)

                class Meta:
                    table = "shared_book"
            """,
        ),
    )

    (apps_dir / "sales_models.py").write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Book(models.Model):
                id = fields.IntField(primary_key=True)

                class Meta:
                    table = "shared_book"
            """,
        ),
    )

    modules = {
        "catalog": ["catalog_models"],
        "sales": ["sales_models"],
    }

    with pytest.raises(InvalidMigrationError) as excinfo:
        await run_makemigrations_with_modules(migrations_dir, modules)

    msg = str(excinfo.value).lower()
    assert "conflicting db_table names" in msg
    assert "shared_book" in msg
    assert "catalog.book" in msg
    assert "sales.book" in msg
    sys.path.remove(str(apps_dir))


@pytest.mark.asyncio
async def test_makemigrations_raises_on_invalid_app_label_in_fk(tmp_path: Path):
    """Ensure we fail fast when FK strings use unknown app labels."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "__init__.py").touch()

    apps_dir = tmp_path / "apps"
    apps_dir.mkdir()
    sys.path.insert(0, str(apps_dir))

    apps_dir.joinpath("team_models.py").write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Team(models.Model):
                id = fields.IntField(primary_key=True)
                name = fields.CharField(max_length=100)
            """,
        ),
    )

    apps_dir.joinpath("member_models.py").write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Member(models.Model):
                id = fields.IntField(primary_key=True)
                # Wrong app label in related_model
                team = fields.ForeignKeyField("models.Team", related_name="members")
            """,
        ),
    )

    modules = {
        "catalog": ["team_models"],
        "accounts": ["member_models"],
    }

    with pytest.raises(InvalidMigrationError) as excinfo:
        await run_makemigrations_with_modules(migrations_dir, modules)

    assert "No app with name" in str(excinfo.value)
    sys.path.remove(str(apps_dir))


def test_validate_related_models_rejects_unknown_app_label():
    """Reject dotted related_model strings with unknown app labels."""
    state = ProjectState(
        model_states={
            "books.Book": ModelState(
                name="books.Book",
                db_table="books_book",
                field_states={
                    "author": FieldState(
                        name="author",
                        field_type="ForeignKeyFieldInstance",
                        related_model="unknown.Author",
                        related_table="unknown_author",
                    ),
                },
            ),
        },
    )

    with pytest.raises(InvalidMigrationError) as excinfo:
        _validate_related_models(
            state,
            app_labels={"books"},
            module_prefixes={"books"},
        )

    msg = str(excinfo.value)
    assert "unknown.Author" in msg
    assert "books.Book.author" in msg


async def test_makemigrations_allows_self_referential_fk(tmp_path: Path, snapshot):
    """Ensure self-referential FKs do not trigger dependency cycle errors."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()

    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "__init__.py").touch()
    sys.path.insert(0, str(tmp_path))
    try:
        (models_dir / "__init__.py").write_text(
            textwrap.dedent(
                """
                from tortoise import fields, models

                class Node(models.Model):
                    id = fields.IntField(primary_key=True)
                    parent = fields.ForeignKeyField(
                        "models.Node",
                        null=True,
                        related_name="children",
                    )
                """,
            ),
        )

        await run_makemigrations(migrations_dir)

        mig_text = newest_migration_text(migrations_dir)
        assert "CreateModel" in mig_text
        assert "ForeignKeyFieldInstance" in mig_text
        assert (
            "'related_table': 'node'" in mig_text
            or '"related_table": "node"' in mig_text
        )
        assert "\n".join(mig_text.split("\n")[1:]) == snapshot
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
        if "models" in sys.modules:
            del sys.modules["models"]


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


async def test_makemigrations_renders_expression_exclusion_constraints_without_churn(
    tmp_path: Path,
):
    """Expression-node exclusion constraints should round-trip without churn."""
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

    try:
        write_models(
            """
            from tortoise import fields, models

            from tortoisemarch.constraints import (
                ExclusionConstraint,
                FieldRef,
                RawSQL,
            )

            class Practitioner(models.Model):
                id = fields.IntField(primary_key=True)

            class Booking(models.Model):
                practitioner = fields.ForeignKeyField(
                    "models.Practitioner",
                    related_name="bookings",
                )
                start_at = fields.DatetimeField()
                end_at = fields.DatetimeField()

                class Meta:
                    tortoisemarch_constraints = (
                        ExclusionConstraint(
                            expressions=(
                                (FieldRef("practitioner"), "="),
                                (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                            ),
                            name="bookings_no_overlap_per_practitioner",
                            index_type="gist",
                            condition=(
                                "status IN "
                                "('held', 'confirmed', 'completed', 'no_show')"
                            ),
                        ),
                    )
            """,
        )

        await run_makemigrations(migrations_dir)
        mig_text = newest_migration_text(migrations_dir)

        assert "FieldRef(" in mig_text
        assert "RawSQL(" in mig_text
        assert "ConstraintState(" in mig_text
        assert "bookings_no_overlap_per_practitioner" in mig_text

        before = sorted(f.name for f in migrations_dir.glob("*.py"))
        await run_makemigrations(migrations_dir)
        after = sorted(f.name for f in migrations_dir.glob("*.py"))

        assert before == after
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
        if "models" in sys.modules:
            del sys.modules["models"]


async def test_makemigrations_orders_extensions_before_constraints(tmp_path: Path):
    """Extension requirements should be emitted before dependent constraints."""
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

    try:
        write_models(
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
        )

        await run_makemigrations(migrations_dir)
        mig_text = newest_migration_text(migrations_dir)

        assert "from tortoisemarch.extensions import PostgresExtension" in mig_text
        assert "AddExtension(" in mig_text
        assert "AddConstraint(" in mig_text
        assert mig_text.index("AddExtension(") < mig_text.index("AddConstraint(")

        before = sorted(f.name for f in migrations_dir.glob("*.py"))
        await run_makemigrations(migrations_dir)
        after = sorted(f.name for f in migrations_dir.glob("*.py"))

        assert before == after
    finally:
        if str(tmp_path) in sys.path:
            sys.path.remove(str(tmp_path))
        if "models" in sys.modules:
            del sys.modules["models"]


@pytest.mark.asyncio
async def test_makemigrations_check_only_errors(tmp_path: Path):
    """Check-only should fail when changes would create a migration file."""
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
        """,
    )
    await run_makemigrations(migrations_dir)

    write_models(
        """
        from tortoise import fields, models

        class Book(models.Model):
            id = fields.IntField(primary_key=True)
            title = fields.CharField(max_length=100, default="")
        """,
    )

    with pytest.raises(InvalidMigrationError) as excinfo:
        await run_makemigrations(migrations_dir, check_only=True)

    assert "check-only" in str(excinfo.value).lower()
    assert "0002_add_book_title.py" in str(excinfo.value)
    all_py_names = {p.name for p in migrations_dir.glob("*.py")}
    assert all_py_names == {"__init__.py", "0001_initial.py"}
    sys.path.remove(str(tmp_path))
