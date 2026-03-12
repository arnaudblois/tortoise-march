"""Tests for ProjectState / ModelState serialization."""

from tortoisemarch.constraints import FieldRef, RawSQL
from tortoisemarch.extensions import PostgresExtension
from tortoisemarch.model_state import (
    ConstraintKind,
    ConstraintState,
    FieldState,
    IndexState,
    ModelState,
    ProjectState,
)


def test_model_state_round_trips_indexes_and_constraints():
    """ModelState serialization should preserve explicit schema objects."""
    state = ProjectState(
        extensions=[PostgresExtension("btree_gist")],
        model_states={
            "Book": ModelState(
                name="Book",
                db_table="book",
                field_states={
                    "id": FieldState(
                        name="id",
                        field_type="IntField",
                        primary_key=True,
                    ),
                    "title": FieldState(
                        name="title",
                        field_type="CharField",
                        max_length=255,
                    ),
                },
                indexes=[
                    IndexState(columns=("title",), name="book_title_idx"),
                ],
                constraints=[
                    ConstraintState(
                        kind="unique",
                        name="book_title_uniq",
                        columns=("title",),
                    ),
                    ConstraintState(
                        kind="check",
                        name="book_title_length_check",
                        check="char_length(title) > 0",
                    ),
                    ConstraintState(
                        kind="exclude",
                        name="book_room_timespan_excl",
                        expressions=(
                            (FieldRef("room"), "="),
                            (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                        ),
                        index_type="gist",
                        condition="cancelled_at IS NULL",
                    ),
                ],
            ),
        },
    )

    restored = ProjectState.from_dict(state.to_dict())

    assert restored == state
    assert restored.extensions == [PostgresExtension("btree_gist")]


def test_constraint_state_coerces_strings_to_enum_and_serializes_as_strings():
    """Constraint kinds should be typed internally without changing wire format."""
    constraint = ConstraintState(kind="unique", columns=("email",))

    assert constraint.kind is ConstraintKind.UNIQUE
    assert constraint.to_dict()["kind"] == "unique"
