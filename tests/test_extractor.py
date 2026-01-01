"""Test suite for the state extraction from the model code."""

import sys
import textwrap

from tortoise import Tortoise, fields
from tortoise.models import Model

from tortoisemarch.extractor import extract_project_state
from tortoisemarch.model_state import ProjectState


class PrimaryKeyField(fields.UUIDField):
    """Shorthand field for primary key.

    This also tests that inherited FK fields are correctly handled.
    """

    def __init__(self, **kwargs):
        """Set primary_key attribute to True by default."""
        kwargs.setdefault("primary_key", True)
        super().__init__(**kwargs)


class Author(Model):
    """Test model of an Author."""

    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    active = fields.BooleanField(default=True)

    class Meta:
        table = "author"


class Book(Model):
    """Test model for a Book."""

    id = PrimaryKeyField()
    title = fields.CharField(max_length=200)
    author = fields.ForeignKeyField("models.Author", related_name="books")

    class Meta:
        table = "book"


async def test_extract_project_state_with_fk():
    """Test the extraction of the code into a ProjectState."""
    await Tortoise.init(
        db_url="sqlite://:memory:",
        modules={"models": [__name__]},
    )
    state = extract_project_state(apps=Tortoise.apps)
    assert isinstance(state, ProjectState)
    assert set(state.model_states.keys()) == {"Author", "Book"}

    author_model = state.get_model("Author")
    book_model = state.get_model("Book")

    assert author_model.db_table == "author"
    assert book_model.db_table == "book"

    author_fields = dict(author_model.field_states)
    book_fields = dict(book_model.field_states)

    # Author fields
    assert author_fields["id"].field_type == "IntField"
    assert author_fields["name"].field_type == "CharField"
    assert author_fields["active"].field_type == "BooleanField"
    assert author_fields["active"].default is True
    # Book fields
    # We only keep the logical FK field 'author', the backing column is in db_column
    assert set(book_fields.keys()) == {"id", "title", "author"}

    assert book_fields["id"].field_type == "UUIDField"
    assert book_fields["title"].field_type == "CharField"

    fk = book_fields["author"]
    # Logical type
    assert fk.field_type == "ForeignKeyFieldInstance"
    # Backing DB column name
    assert fk.db_column == "author_id"
    # FK target type inferred from Author.id (IntField)
    assert fk.referenced_type == "IntField"
    # Nullability
    assert fk.null is False
    # Sanity: relation metadata
    assert fk.related_table == "author"
    assert fk.to_field == "id"


async def test_extract_project_state_multiple_apps(tmp_path):
    """Ensure models registered under different app labels are extracted."""
    mod_catalog = tmp_path / "catalog_models.py"
    mod_catalog.write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class CatalogEntry(models.Model):
                id = fields.IntField(primary_key=True)
                name = fields.CharField(max_length=50)

                class Meta:
                    table = "catalog_entry"
            """,
        ),
    )

    mod_accounts = tmp_path / "accounts_models.py"
    mod_accounts.write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class AccountUser(models.Model):
                id = fields.IntField(primary_key=True)
                email = fields.CharField(max_length=100)

                class Meta:
                    table = "account_user"
            """,
        ),
    )

    sys.path.insert(0, str(tmp_path))
    try:
        await Tortoise.init(
            db_url="sqlite://:memory:",
            modules={
                "catalog": ["catalog_models"],
                "accounts": ["accounts_models"],
            },
        )
        state = extract_project_state(apps=Tortoise.apps)
        assert set(state.model_states.keys()) == {"CatalogEntry", "AccountUser"}
        assert state.get_model("CatalogEntry").db_table == "catalog_entry"
        assert state.get_model("AccountUser").db_table == "account_user"
    finally:
        await Tortoise._reset_apps()  # noqa: SLF001
        sys.path.remove(str(tmp_path))


def test_extract_model_state_respects_index_together(tmp_path):
    """index_together and unique_together should be captured as meta indexes."""
    mod = tmp_path / "indexed_models.py"
    mod.write_text(
        textwrap.dedent(
            """
            from tortoise import fields, models

            class Indexed(models.Model):
                a = fields.IntField()
                b = fields.IntField()
                c = fields.IntField()

                class Meta:
                    index_together = (("a", "b"),)
                    unique_together = (("b", "c"),)
            """,
        ),
    )

    sys.path.insert(0, str(tmp_path))
    try:
        from indexed_models import Indexed  # noqa: PLC0415

        state = extract_project_state(apps={"default": {"Indexed": Indexed}})
        ms = state.get_model("Indexed")
        indexes = set(ms.meta.get("indexes", []))
        assert (("a", "b"), False) in indexes
        assert (("b", "c"), True) in indexes
    finally:
        sys.path.remove(str(tmp_path))
