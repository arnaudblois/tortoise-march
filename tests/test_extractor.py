"""Test suite for the state extraction from the model code."""

import pytest
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


@pytest.mark.asyncio
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
    assert book_fields["id"].field_type == "UUIDField"
    assert book_fields["title"].field_type == "CharField"
    assert book_fields["author_id"].field_type == "IntField"
    assert book_fields["author_id"].null is False
