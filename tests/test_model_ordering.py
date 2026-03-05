"""Tests for topological ordering of model creation and removal.

Verifies that CreateModel and RemoveModel operations respect foreign key
dependencies, including multi-level graphs and cycle detection.
"""

import pytest

from tortoisemarch.differ import diff_states
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.operations import CreateModel, RemoveModel


def _ps(*models: ModelState) -> ProjectState:
    """Build a ProjectState from the given model states."""
    return ProjectState(model_states={m.name: m for m in models})


def _pk() -> FieldState:
    """Return a minimal integer primary key field."""
    return FieldState(name="id", field_type="IntField", primary_key=True)


def _fk(name: str, related_table: str) -> FieldState:
    """Return a nullable FK field pointing at `related_table`."""
    return FieldState(
        name=name,
        field_type="ForeignKeyFieldInstance",
        null=True,
        related_table=related_table,
        to_field="id",
        on_delete="CASCADE",
    )


def _model(
    name: str,
    table: str,
    *,
    fk_name: str | None = None,
    fk_table: str | None = None,
) -> ModelState:
    """Build a model with an `id` PK and an optional FK field."""
    fields = {"id": _pk()}
    if fk_name and fk_table:
        fields[fk_name.lower()] = _fk(fk_name, fk_table)
    return ModelState(name=name, db_table=table, field_states=fields)


def test_create_model_ordering_respects_fk_dependencies():
    """Test that parent tables are created before tables that reference them."""
    parent = ModelState(
        name="Parent",
        db_table="parent",
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

    child = ModelState(
        name="Child",
        db_table="child",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
                index=True,
                default="python_callable",
            ),
            "parent": FieldState(
                name="parent",
                field_type="ForeignKeyFieldInstance",
                db_column="parent_id",
                related_table="parent",
                related_model="x.Parent",
                to_field="id",
                referenced_type="UUIDField",
                on_delete="CASCADE",
            ),
        },
    )

    ops = diff_states(ProjectState(model_states={}), _ps(parent, child))
    create_ops = [op for op in ops if isinstance(op, CreateModel)]

    assert [op.db_table for op in create_ops] == ["parent", "child"]


def test_remove_model_ordering_is_reverse_dependency_order():
    """Test that dependent tables are removed before the tables they reference."""
    parent = ModelState(
        name="Parent",
        db_table="parent",
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

    child = ModelState(
        name="Child",
        db_table="child",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
                index=True,
                default="python_callable",
            ),
            "parent": FieldState(
                name="parent",
                field_type="ForeignKeyFieldInstance",
                db_column="parent_id",
                related_table="parent",
                related_model="x.Parent",
                to_field="id",
                referenced_type="UUIDField",
                on_delete="CASCADE",
            ),
        },
    )

    ops = diff_states(_ps(parent, child), ProjectState(model_states={}))
    remove_ops = [op for op in ops if isinstance(op, RemoveModel)]

    assert [op.db_table for op in remove_ops] == ["child", "parent"]


def test_create_model_topological_sort_multi_level():
    """Test that CreateModel operations are ordered across multi-level FK chains."""
    a = ModelState(
        name="A",
        db_table="a",
        field_states={
            "id": FieldState(
                name="id",
                field_type="IntField",
                primary_key=True,
            ),
        },
    )

    b = ModelState(
        name="B",
        db_table="b",
        field_states={
            "id": FieldState(
                name="id",
                field_type="IntField",
                primary_key=True,
            ),
            "a": FieldState(
                name="a",
                field_type="ForeignKeyFieldInstance",
                related_table="a",
                to_field="id",
                referenced_type="IntField",
            ),
        },
    )

    c = ModelState(
        name="C",
        db_table="c",
        field_states={
            "id": FieldState(
                name="id",
                field_type="IntField",
                primary_key=True,
            ),
            "b": FieldState(
                name="b",
                field_type="ForeignKeyFieldInstance",
                related_table="b",
                to_field="id",
                referenced_type="IntField",
            ),
        },
    )

    ops = diff_states(
        ProjectState(model_states={}),
        ProjectState(model_states={"A": a, "B": b, "C": c}),
    )

    create_ops = [op.db_table for op in ops if isinstance(op, CreateModel)]
    assert create_ops == ["a", "b", "c"]


def test_create_model_cycle_detection():
    """Test that cyclic foreign key dependencies cause migration generation to fail."""
    a = ModelState(
        name="A",
        db_table="a",
        field_states={
            "id": FieldState(
                name="id",
                field_type="IntField",
                primary_key=True,
            ),
            "b": FieldState(
                name="b",
                field_type="ForeignKeyFieldInstance",
                related_table="b",
                to_field="id",
                referenced_type="IntField",
            ),
        },
    )

    b = ModelState(
        name="B",
        db_table="b",
        field_states={
            "id": FieldState(
                name="id",
                field_type="IntField",
                primary_key=True,
            ),
            "a": FieldState(
                name="a",
                field_type="ForeignKeyFieldInstance",
                related_table="a",
                to_field="id",
                referenced_type="IntField",
            ),
        },
    )

    with pytest.raises(InvalidMigrationError):
        diff_states(
            ProjectState(model_states={}),
            ProjectState(model_states={"A": a, "B": b}),
        )


def test_create_model_cycle_message_includes_exact_path_and_blocked_models():
    """Cycle errors should expose witness model paths, fields, and blocked models."""
    to_state = ProjectState(
        model_states={
            "TableA": _model("TableA", "table_a", fk_name="b_ref", fk_table="table_b"),
            "TableB": _model("TableB", "table_b", fk_name="c_ref", fk_table="table_c"),
            "TableC": _model("TableC", "table_c", fk_name="a_ref", fk_table="table_a"),
            "TableD": _model("TableD", "table_d", fk_name="c_ref", fk_table="table_c"),
        },
    )

    with pytest.raises(InvalidMigrationError) as excinfo:
        diff_states(ProjectState(model_states={}), to_state)

    msg = str(excinfo.value)
    assert "CreateModel dependency cycle detected (1 cycle)." in msg
    assert "Cycle 1 (models): TableA -> TableB -> TableC -> TableA" in msg
    assert (
        "Cycle 1 (fields): TableA.b_ref -> TableB.c_ref -> TableC.a_ref -> TableA"
    ) in msg
    assert "Blocked models (depend on cycles): TableD" in msg


def test_create_model_allows_self_referential_fk():
    """Self-referential FKs should not be treated as dependency cycles."""
    node = ModelState(
        name="Node",
        db_table="node",
        field_states={
            "id": FieldState(
                name="id",
                field_type="IntField",
                primary_key=True,
            ),
            "parent": FieldState(
                name="parent",
                field_type="ForeignKeyFieldInstance",
                related_table="node",
                to_field="id",
                referenced_type="IntField",
                null=True,
            ),
        },
    )

    ops = diff_states(
        ProjectState(model_states={}),
        ProjectState(model_states={"Node": node}),
    )
    create_ops = [op for op in ops if isinstance(op, CreateModel)]
    assert [op.db_table for op in create_ops] == ["node"]


def test_create_model_column_order_is_stable_and_human_friendly():
    """Test CreateModel orders columns deterministically with PK first and FKs last."""
    ms = ModelState(
        name="Thing",
        db_table="thing",
        field_states={
            "id": FieldState(
                name="id",
                field_type="UUIDField",
                primary_key=True,
                unique=True,
            ),
            "created_at": FieldState(
                name="created_at",
                field_type="DatetimeField",
            ),
            "name": FieldState(
                name="name",
                field_type="CharField",
                max_length=100,
            ),
            "owner": FieldState(
                name="owner",
                field_type="ForeignKeyFieldInstance",
                related_table="user",
                to_field="id",
                referenced_type="UUIDField",
            ),
        },
    )

    op = CreateModel.from_model_state(ms)

    cols = [name for name, _, _ in op.fields]

    # Primary key first
    assert cols[0] == "id"

    # Scalar fields before relational fields
    assert cols.index("name") < cols.index("owner")

    # FK fields are last
    assert cols[-1] == "owner"
