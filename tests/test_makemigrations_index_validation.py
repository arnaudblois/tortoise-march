"""Tests for validating Meta index columns during makemigrations."""

import pytest

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.makemigrations import _validate_index_columns
from tortoisemarch.model_state import (
    ConstraintState,
    FieldState,
    IndexState,
    ModelState,
    ProjectState,
)


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
        indexes=[IndexState(columns=("author", "created_at"))],
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
        indexes=[
            IndexState(columns=("author_id",)),
            IndexState(columns=("published_on",)),
        ],
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
        constraints=[ConstraintState(kind="unique", columns=("title", "edition"))],
    )
    state = ProjectState(model_states={"Book": ms})

    with pytest.raises(InvalidMigrationError) as exc:
        _validate_index_columns(state)

    assert "Book.edition" in str(exc.value)


def test_exclusion_constraint_columns_are_validated():
    """Exclusion constraints should be validated like other model-level objects."""
    ms = ModelState(
        name="Booking",
        db_table="booking",
        field_states={
            "room": FieldState(name="room", field_type="IntField"),
            "timespan": FieldState(name="timespan", field_type="CharField"),
        },
        constraints=[
            ConstraintState(
                kind="exclude",
                expressions=(("room", "="), ("resource", "&&")),
                index_type="gist",
            ),
        ],
    )
    state = ProjectState(model_states={"Booking": ms})

    with pytest.raises(InvalidMigrationError) as exc:
        _validate_index_columns(state)

    assert "Booking.resource" in str(exc.value)
