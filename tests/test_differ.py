"""Test suite for diff_states."""

from tortoisemarch.differ import diff_states
from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateModel,
    RemoveField,
    RemoveModel,
)


def model(name: str, fields: list[tuple[str, str, dict]]) -> ModelState:
    """Construct a ModelState from (name, type, options) tuples."""
    return ModelState(
        name=name,
        db_table=name.lower(),
        field_states={
            fname.lower(): FieldState(
                name=fname,
                field_type=ftype,
                **opts,  # let FieldState fill defaults; tests assert subsets
            )
            for fname, ftype, opts in fields
        },
    )


def project(model_states: dict[str, ModelState]) -> ProjectState:
    """Wrap a dict of ModelState into a ProjectState."""
    return ProjectState(model_states=model_states)


def test_create_model():
    """Creating a model should emit a CreateModel op with correct fields."""
    old = project({})
    new = project({"User": model("User", [("id", "IntField", {"null": False})])})

    ops = diff_states(old, new)
    assert len(ops) == 1
    operation = ops[0]
    assert isinstance(operation, CreateModel)
    assert operation.name == "User"
    assert operation.db_table == "user"

    assert len(operation.fields) == 1
    field_name, field_type, opts = operation.fields[0]
    assert field_name == "id"
    assert field_type == "IntField"
    assert opts.get("null") is False


def test_remove_model():
    """Removing a model should emit a RemoveModel with the right db_table."""
    old = project({"User": model("User", [("id", "IntField", {"null": False})])})
    new = project({})

    ops = diff_states(old, new)
    rm_ops = [op for op in ops if isinstance(op, RemoveModel)]
    assert rm_ops, "Expected a RemoveModel operation"
    rm = rm_ops[0]
    assert rm.name == "User"
    assert rm.db_table == "user"


def test_add_field():
    """Adding a field should emit AddField with db_table and options intact."""
    old = project({"User": model("User", [("id", "IntField", {"null": False})])})
    new = project(
        {
            "User": model(
                "User",
                [
                    ("id", "IntField", {"null": False}),
                    ("name", "CharField", {"null": False, "max_length": 100}),
                ],
            ),
        },
    )

    ops = diff_states(old, new)
    af_ops = [op for op in ops if isinstance(op, AddField) and op.field_name == "name"]
    assert af_ops, "Expected an AddField(name) operation"
    af = af_ops[0]
    assert af.model_name == "User"
    assert af.db_table == "user"
    assert af.field_type == "CharField"
    assert af.options.get("null") is False
    assert af.options.get("max_length") == 100  # noqa: PLR2004


def test_remove_field():
    """Test a field emits RemoveField with the correct db_table."""
    old = project(
        {
            "User": model(
                "User",
                [
                    ("id", "IntField", {"null": False}),
                    ("name", "CharField", {"null": False}),
                ],
            ),
        },
    )
    new = project({"User": model("User", [("id", "IntField", {"null": False})])})

    ops = diff_states(old, new)
    rf_ops = [
        op for op in ops if isinstance(op, RemoveField) and op.field_name == "name"
    ]
    assert rf_ops, "Expected a RemoveField(name) operation"
    rf = rf_ops[0]
    assert rf.model_name == "User"
    assert rf.db_table == "user"


def test_alter_field_type():
    """Changing a field type should emit AlterField with type in new_options."""
    old = project({"User": model("User", [("id", "IntField", {"null": False})])})
    new = project({"User": model("User", [("id", "CharField", {"null": False})])})

    ops = diff_states(old, new)
    al_ops = [op for op in ops if isinstance(op, AlterField) and op.field_name == "id"]
    assert al_ops, "Expected an AlterField(id) operation"
    al = al_ops[0]
    assert al.db_table == "user"
    assert al.new_options.get("type") == "CharField"


def test_alter_field_nullability():
    """Changing nullability should emit AlterField."""
    old = project({"User": model("User", [("id", "IntField", {"null": False})])})
    new = project({"User": model("User", [("id", "IntField", {"null": True})])})

    ops = diff_states(old, new)
    al_ops = [op for op in ops if isinstance(op, AlterField) and op.field_name == "id"]
    assert al_ops, "Expected an AlterField(id) operation"
    al = al_ops[0]
    assert al.db_table == "user"
    assert al.new_options.get("null") is True
