"""Test suite for diff_states."""

from tortoisemarch.differ import diff_states
from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateModel,
    RemoveField,
    RemoveModel,
    RenameModel,
)


def model(
    name: str,
    fields: list[tuple[str, str, dict]],
    *,
    db_table: str | None = None,
) -> ModelState:
    """Construct a ModelState from (name, type, options) tuples."""
    return ModelState(
        name=name,
        db_table=db_table or name.lower(),
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
    new = project({"User": model("User", [("id", "IntField", {"null": True})])})

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
    assert opts.get("null") is True


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


def test_alter_charfield_max_length():
    """Changing nullability should emit AlterField."""
    old = project(
        {"Book": model("Book", [("title", "CharField", {"max_length": 200})])},
    )
    new = project(
        {"Book": model("Book", [("title", "CharField", {"max_length": 300})])},
    )

    ops = diff_states(old, new)
    al_ops = [
        op for op in ops if isinstance(op, AlterField) and op.field_name == "title"
    ]
    assert al_ops, "Expected an AlterField(id) operation"
    al = al_ops[0]
    assert al.db_table == "book"
    assert al.new_options.get("max_length") == 300  # noqa: PLR2004


def test_diff_states_emits_alterfield_for_charfield_length_even_if_compacted():
    """Test diff_states works when changing max_length of a CharField."""
    old = project({"Book": model("Book", [("title", "CharField", {})])})
    new = project(
        {"Book": model("Book", [("title", "CharField", {"max_length": 300})])},
    )

    ops = diff_states(old, new)
    alters = [
        op for op in ops if isinstance(op, AlterField) and op.field_name == "title"
    ]
    assert alters
    assert alters[0].old_options["type"] == "CharField"
    assert alters[0].new_options["type"] == "CharField"
    assert alters[0].old_options["max_length"] == 255  # noqa: PLR2004
    assert alters[0].new_options["max_length"] == 300  # noqa: PLR2004


def test_detects_model_rename_with_custom_table_name():
    """Renaming a model + table should emit RenameModel, not drop/create."""
    shared_fields = [
        ("id", "IntField", {"primary_key": True}),
        (
            "company",
            "ForeignKeyFieldInstance",
            {
                "related_table": "company",
                "referenced_type": "IntField",
                "to_field": "id",
            },
        ),
        (
            "user",
            "ForeignKeyFieldInstance",
            {"related_table": "user", "referenced_type": "IntField", "to_field": "id"},
        ),
        ("role", "CharEnumField", {"default": "member"}),
        ("status", "CharEnumField", {"default": "invited"}),
        ("invited_at", "DatetimeField", {"null": True}),
        ("activated_at", "DatetimeField", {"null": True}),
        ("disabled_at", "DatetimeField", {"null": True}),
    ]

    old = project(
        {"CompanyUser": model("CompanyUser", shared_fields, db_table="company_user")},
    )
    new = project(
        {
            "CompanyMember": model(
                "CompanyMember",
                shared_fields,
                db_table="company_member",
            ),
        },
    )

    ops = diff_states(old, new)

    renames = [op for op in ops if isinstance(op, RenameModel)]
    assert renames
    rename = renames[0]
    assert rename.old_name == "CompanyUser"
    assert rename.new_name == "CompanyMember"
    assert rename.old_db_table == "company_user"
    assert rename.new_db_table == "company_member"

    assert not any(
        isinstance(op, RemoveModel) and op.name == "CompanyUser" for op in ops
    )
    assert not any(
        isinstance(op, CreateModel) and op.name == "CompanyMember" for op in ops
    )
