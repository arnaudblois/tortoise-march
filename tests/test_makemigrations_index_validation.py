"""Tests for validating Meta index columns during makemigrations."""

import pytest

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.makemigrations import _validate_index_columns
from tortoisemarch.model_state import FieldState, ModelState, ProjectState


def test_meta_index_with_unknown_field_raises():
    """Indexes referencing unknown fields should be rejected."""
    ms = ModelState(
        name="Book",
        db_table="book",
        field_states={
            "id": FieldState(name="id", field_type="UUIDField", primary_key=True),
            "published_at": FieldState(name="published_at", field_type="DatetimeField"),
            "author": FieldState(
                name="author",
                field_type="ForeignKeyFieldInstance",
                related_table="author",
                referenced_type="UUIDField",
                to_field="id",
            ),
        },
        meta={"indexes": [(("author", "created_at"), False)]},
    )
    state = ProjectState(model_states={"Book": ms})

    with pytest.raises(InvalidMigrationError) as exc:
        _validate_index_columns(state)

    assert "Book.created_at" in str(exc.value)


def test_meta_index_allows_physical_columns():
    """Indexes may refer to FK backing columns or explicit db_column names."""
    ms = ModelState(
        name="Book",
        db_table="book",
        field_states={
            "published_at": FieldState(
                name="published_at",
                field_type="DatetimeField",
                db_column="published_on",
            ),
            "author": FieldState(
                name="author",
                field_type="ForeignKeyFieldInstance",
                related_table="author",
                referenced_type="UUIDField",
                to_field="id",
            ),
        },
        meta={
            "indexes": [
                (("author_id",), False),
                (("published_on",), False),
            ],
        },
    )
    state = ProjectState(model_states={"Book": ms})

    _validate_index_columns(state)  # should not raise


def test_unique_together_columns_are_validated():
    """unique_together entries should be validated like indexes."""
    ms = ModelState(
        name="Book",
        db_table="book",
        field_states={
            "id": FieldState(name="id", field_type="UUIDField", primary_key=True),
            "title": FieldState(name="title", field_type="CharField"),
        },
        meta={"indexes": [(("title", "edition"), True)]},
    )
    state = ProjectState(model_states={"Book": ms})

    with pytest.raises(InvalidMigrationError) as exc:
        _validate_index_columns(state)

    assert "Book.edition" in str(exc.value)
