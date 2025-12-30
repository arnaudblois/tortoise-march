"""Tests for filtering reverse relations out of schema migrations.

Ensures that ORM-level reverse accessors never produce schema columns
and do not cause spurious migration operations.
"""

from tortoisemarch.differ import diff_states
from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.operations import CreateModel


def _ps(*models: ModelState) -> ProjectState:
    return ProjectState(model_states={m.name: m for m in models})


def test_create_model_drops_reverse_relations_from_schema():
    """Build a ProjectState from the given model states."""
    ms = ModelState(
        name="A",
        db_table="a",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
                index=True,
                default="python_callable",
            ),
            # Reverse accessor: must never become a DB column
            "reverse": FieldState(
                name="reverse",
                field_type="BackwardOneToOneRelation",
                null=True,
            ),
        },
    )

    op = CreateModel.from_model_state(ms)

    types = {ftype for _, ftype, _ in op.fields}
    assert "BackwardOneToOneRelation" not in types
    assert "UUIDField" in types


def test_diff_states_ignores_reverse_relations_no_churn():
    """Test that removing a reverse relation doesn't produce migration operations."""
    from_ms = ModelState(
        name="A",
        db_table="a",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
                index=True,
                default="python_callable",
            ),
            "reverse": FieldState(
                name="reverse",
                field_type="BackwardOneToOneRelation",
                null=True,
            ),
        },
    )

    to_ms = ModelState(
        name="A",
        db_table="a",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
                index=True,
                default="python_callable",
            ),
        },
    )

    ops = diff_states(_ps(from_ms), _ps(to_ms))
    assert ops == []
