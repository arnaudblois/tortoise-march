"""Test suite for diff_states."""

from tortoisemarch.constraints import FieldRef, RawSQL
from tortoisemarch.differ import diff_states
from tortoisemarch.extensions import PostgresExtension
from tortoisemarch.model_state import (
    ConstraintState,
    FieldState,
    IndexState,
    ModelState,
    ProjectState,
)
from tortoisemarch.operations import (
    AddConstraint,
    AddExtension,
    AddField,
    AlterField,
    CreateIndex,
    CreateModel,
    RemoveConstraint,
    RemoveExtension,
    RemoveField,
    RemoveIndex,
    RemoveModel,
    RenameConstraint,
    RenameModel,
)


def model(  # noqa: PLR0913
    name: str,
    fields: list[tuple[str, str, dict]],
    *,
    db_table: str | None = None,
    meta: dict | None = None,
    indexes: list[IndexState] | None = None,
    constraints: list[ConstraintState] | None = None,
) -> ModelState:
    """Construct a ModelState from (name, type, options) tuples."""
    indexes = list(indexes or [])
    constraints = list(constraints or [])
    for columns, unique in (meta or {}).get("indexes", []):
        if unique:
            constraints.append(ConstraintState(kind="unique", columns=tuple(columns)))
        else:
            indexes.append(IndexState(columns=tuple(columns)))
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
        indexes=indexes,
        constraints=constraints,
        meta=meta or {},
    )


def project(
    model_states: dict[str, ModelState],
    *,
    extensions: list[PostgresExtension] | None = None,
) -> ProjectState:
    """Wrap a dict of ModelState into a ProjectState."""
    return ProjectState(
        model_states=model_states,
        extensions=list(extensions or []),
    )


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


def test_diff_states_emits_rename_plus_alter_for_charfield_to_textfield():
    """Confirmed rename with CharField -> TextField should emit one AlterField."""
    old_fields = [
        ("id", "IntField", {"primary_key": True}),
        ("author", "CharField", {"max_length": 128}),
    ]
    new_fields = [
        ("id", "IntField", {"primary_key": True}),
        ("book_author", "TextField", {}),
    ]
    old = project({"User": model("User", old_fields)})
    new = project({"User": model("User", new_fields)})

    ops = diff_states(
        old,
        new,
        rename_map={"User": {"author": "book_author"}},
    )

    alter_ops = [
        op for op in ops if isinstance(op, AlterField) and op.field_name == "author"
    ]
    assert len(alter_ops) == 1
    alter = alter_ops[0]
    assert alter.new_name == "book_author"
    assert alter.old_options.get("type") == "CharField"
    assert alter.new_options.get("type") == "TextField"
    assert not any(
        isinstance(op, RemoveField) and op.field_name == "author" for op in ops
    )
    assert not any(
        isinstance(op, AddField) and op.field_name == "book_author" for op in ops
    )


def test_fk_defaults_normalized_to_avoid_spurious_alter():
    """Missing implicit FK defaults should not trigger an AlterField diff."""
    old = project(
        {
            "Invitation": model(
                "Invitation",
                [
                    ("membership", "OneToOneFieldInstance", {}),
                ],
            ),
        },
    )
    new = project(
        {
            "Invitation": model(
                "Invitation",
                [
                    (
                        "membership",
                        "OneToOneFieldInstance",
                        {
                            "db_column": "membership_id",
                            "on_delete": "CASCADE",
                            "to_field": "id",
                        },
                    ),
                ],
            ),
        },
    )
    ops = diff_states(old, new)
    assert not any(isinstance(op, AlterField) for op in ops)


def test_meta_indexes_emit_create_and_remove_index():
    """Diffing meta-level indexes should emit CreateIndex/RemoveIndex."""
    old = project(
        {
            "Member": model(
                "Member",
                [
                    ("id", "IntField", {"primary_key": True}),
                    ("status", "CharField", {}),
                ],
                meta={"indexes": [(("status",), False)]},
            ),
        },
    )
    new = project(
        {
            "Member": model(
                "Member",
                [
                    ("id", "IntField", {"primary_key": True}),
                    ("status", "CharField", {}),
                    ("role", "CharField", {}),
                ],
                meta={"indexes": [(("status", "role"), False)]},
            ),
        },
    )

    ops = diff_states(old, new)

    assert any(isinstance(op, RemoveIndex) for op in ops)
    creates = [op for op in ops if isinstance(op, CreateIndex)]
    assert creates
    assert set(creates[0].columns) == {"status", "role"}


def test_meta_indexes_skip_field_index_duplicates():
    """Meta indexes duplicating field db_index should not emit CreateIndex.

    The index creation is already contained in field section of the related
    Operation, as a `index: True`.
    """
    ms = model(
        "DocumentFileVersion",
        [("sha256", "CharField", {"index": True, "max_length": 64})],
        meta={"indexes": [(("sha256",), False)]},
    )

    ops = diff_states(project({}), project({"DocumentFileVersion": ms}))
    assert any(isinstance(op, CreateModel) for op in ops)
    assert not any(isinstance(op, CreateIndex) for op in ops)


def test_meta_indexes_use_physical_fk_columns():
    """Index SQL should use DB column names for FK/OneToOne fields."""
    ms = model(
        "Member",
        [
            (
                "person",
                "ForeignKeyFieldInstance",
                {
                    "related_table": "person",
                    "referenced_type": "UUIDField",
                    "to_field": "id",
                },
            ),
            (
                "location",
                "ForeignKeyFieldInstance",
                {
                    "db_column": "loc_id",
                    "related_table": "location",
                    "referenced_type": "UUIDField",
                    "to_field": "id",
                },
            ),
        ],
        meta={"indexes": [(("person", "location"), False)]},
    )

    ops = diff_states(project({}), project({"Member": ms}))
    create_idx = [op for op in ops if isinstance(op, CreateIndex)]
    assert create_idx
    assert create_idx[0].columns == ("person_id", "loc_id")


def test_meta_indexes_respect_db_column_override():
    """Explicit db_column should be used when rendering indexes."""
    ms = model(
        "Member",
        [
            (
                "person",
                "ForeignKeyFieldInstance",
                {
                    "db_column": "p_id",
                    "related_table": "person",
                    "referenced_type": "UUIDField",
                    "to_field": "id",
                },
            ),
        ],
        meta={"indexes": [(("person",), False)]},
    )

    ops = diff_states(project({}), project({"Member": ms}))
    create_idx = [op for op in ops if isinstance(op, CreateIndex)]
    assert create_idx
    assert create_idx[0].columns == ("p_id",)


def test_unique_constraints_emit_add_remove_and_rename():
    """Unique constraints should diff as first-class constraint operations."""
    old = project(
        {
            "Member": model(
                "Member",
                [("email", "CharField", {"max_length": 255})],
                constraints=[
                    ConstraintState(
                        kind="unique",
                        name="member_email_old_uniq",
                        columns=("email",),
                    ),
                ],
            ),
        },
    )
    new = project(
        {
            "Member": model(
                "Member",
                [
                    ("email", "CharField", {"max_length": 255}),
                    ("tenant", "CharField", {"max_length": 50}),
                ],
                constraints=[
                    ConstraintState(
                        kind="unique",
                        name="member_email_new_uniq",
                        columns=("email",),
                    ),
                    ConstraintState(
                        kind="unique",
                        name="member_email_tenant_uniq",
                        columns=("email", "tenant"),
                    ),
                ],
            ),
        },
    )

    ops = diff_states(old, new)

    renames = [op for op in ops if isinstance(op, RenameConstraint)]
    assert len(renames) == 1
    assert renames[0].old_name == "member_email_old_uniq"
    assert renames[0].new_name == "member_email_new_uniq"

    adds = [op for op in ops if isinstance(op, AddConstraint)]
    assert len(adds) == 1
    assert adds[0].constraint == ConstraintState(
        kind="unique",
        name="member_email_tenant_uniq",
        columns=("email", "tenant"),
    )

    changed = project(
        {
            "Member": model(
                "Member",
                [
                    ("email", "CharField", {"max_length": 255}),
                    ("tenant", "CharField", {"max_length": 50}),
                ],
                constraints=[
                    ConstraintState(
                        kind="unique",
                        name="member_email_new_uniq",
                        columns=("email", "tenant"),
                    ),
                ],
            ),
        },
    )
    ops_changed = diff_states(new, changed)
    assert any(isinstance(op, RemoveConstraint) for op in ops_changed)
    assert any(isinstance(op, RenameConstraint) for op in ops_changed)


def test_check_constraints_emit_add_remove_and_rename():
    """Check constraints should diff semantically and detect pure renames."""
    old = project(
        {
            "Invoice": model(
                "Invoice",
                [("total", "IntField", {})],
                constraints=[
                    ConstraintState(
                        kind="check",
                        name="invoice_total_positive_old",
                        check="total >= 0",
                    ),
                ],
            ),
        },
    )
    new = project(
        {
            "Invoice": model(
                "Invoice",
                [("total", "IntField", {})],
                constraints=[
                    ConstraintState(
                        kind="check",
                        name="invoice_total_positive_new",
                        check="total >= 0",
                    ),
                ],
            ),
        },
    )

    ops = diff_states(old, new)
    assert len([op for op in ops if isinstance(op, RenameConstraint)]) == 1

    changed = project(
        {
            "Invoice": model(
                "Invoice",
                [("total", "IntField", {})],
                constraints=[
                    ConstraintState(
                        kind="check",
                        name="invoice_total_positive_new",
                        check="total > 0",
                    ),
                ],
            ),
        },
    )
    ops_changed = diff_states(new, changed)
    assert any(isinstance(op, RemoveConstraint) for op in ops_changed)
    assert any(isinstance(op, AddConstraint) for op in ops_changed)


def test_exclusion_constraints_emit_add_remove_and_rename():
    """Exclusion constraints should diff semantically and detect pure renames."""
    old = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("room", "IntField", {}),
                    ("timespan", "CharField", {"max_length": 255}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_room_timespan_old_excl",
                        expressions=(("room", "="), ("timespan", "&&")),
                        index_type="gist",
                        condition="cancelled_at IS NULL",
                    ),
                ],
            ),
        },
    )
    new = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("room", "IntField", {}),
                    ("timespan", "CharField", {"max_length": 255}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_room_timespan_new_excl",
                        expressions=(("room", "="), ("timespan", "&&")),
                        index_type="gist",
                        condition="cancelled_at IS NULL",
                    ),
                ],
            ),
        },
    )

    ops = diff_states(old, new)
    assert len([op for op in ops if isinstance(op, RenameConstraint)]) == 1

    changed = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("room", "IntField", {}),
                    ("timespan", "CharField", {"max_length": 255}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_room_timespan_new_excl",
                        expressions=(("room", "="), ("timespan", "-|-")),
                        index_type="gist",
                        condition="cancelled_at IS NULL",
                    ),
                ],
            ),
        },
    )
    ops_changed = diff_states(new, changed)
    assert any(isinstance(op, RemoveConstraint) for op in ops_changed)
    assert any(isinstance(op, AddConstraint) for op in ops_changed)


def test_exclusion_constraints_keep_typed_nodes_and_fk_hints():
    """AddConstraint should preserve typed expressions and FK column resolution."""
    new = project(
        {
            "Booking": model(
                "Booking",
                [
                    (
                        "practitioner",
                        "ForeignKeyFieldInstance",
                        {
                            "related_table": "practitioner",
                            "referenced_type": "IntField",
                            "to_field": "id",
                        },
                    ),
                    ("start_at", "DatetimeField", {}),
                    ("end_at", "DatetimeField", {}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_practitioner_window_excl",
                        expressions=(
                            (FieldRef("practitioner"), "="),
                            (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                        ),
                        index_type="gist",
                        condition="cancelled_at IS NULL",
                    ),
                ],
            ),
        },
    )

    ops = diff_states(project({}), new)

    adds = [op for op in ops if isinstance(op, AddConstraint)]
    assert len(adds) == 1
    assert adds[0].constraint.expressions == (
        (FieldRef("practitioner"), "="),
        (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
    )
    assert adds[0].field_column_map == {}
    assert adds[0].fk_fields == ("practitioner",)


def test_removed_constraints_keep_minimal_column_hints_for_rollbacks():
    """RemoveConstraint should keep only the rendering hints rollback needs."""
    fields = {
        "related_table": "practitioner",
        "referenced_type": "IntField",
        "to_field": "id",
    }
    old = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("practitioner", "ForeignKeyFieldInstance", fields),
                    ("slot", "CharField", {"max_length": 32, "db_column": "slot_key"}),
                ],
                constraints=[
                    ConstraintState(
                        kind="unique",
                        name="booking_practitioner_slot_uniq",
                        columns=("practitioner", "slot"),
                    ),
                ],
            ),
        },
    )
    new = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("practitioner", "ForeignKeyFieldInstance", fields),
                    ("slot", "CharField", {"max_length": 32, "db_column": "slot_key"}),
                ],
            ),
        },
    )

    ops = diff_states(old, new)

    removes = [op for op in ops if isinstance(op, RemoveConstraint)]
    assert len(removes) == 1
    assert removes[0].field_column_map == {"slot": "slot_key"}
    assert removes[0].fk_fields == ("practitioner",)


def test_added_unique_constraints_keep_conventional_fk_columns_out_of_map():
    """AddConstraint should treat conventional FK columns as FK hints, not overrides."""
    fields = {
        "related_table": "practitioner",
        "referenced_type": "IntField",
        "to_field": "id",
    }
    new = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("location", "ForeignKeyFieldInstance", fields),
                    ("practitioner", "ForeignKeyFieldInstance", fields),
                ],
                constraints=[
                    ConstraintState(
                        kind="unique",
                        name="booking_location_practitioner_uniq",
                        columns=("location", "practitioner"),
                    ),
                ],
            ),
        },
    )

    ops = diff_states(project({}), new)

    adds = [op for op in ops if isinstance(op, AddConstraint)]
    assert len(adds) == 1
    assert adds[0].field_column_map == {}
    assert adds[0].fk_fields == ("location", "practitioner")


def test_project_extensions_diff_before_dependent_constraints():
    """Project extensions should be added before dependent schema objects."""
    new = project(
        {
            "Practitioner": model(
                "Practitioner",
                [("id", "UUIDField", {"primary_key": True})],
            ),
            "Booking": model(
                "Booking",
                [
                    ("id", "UUIDField", {"primary_key": True}),
                    (
                        "practitioner",
                        "ForeignKeyFieldInstance",
                        {
                            "related_table": "practitioner",
                            "referenced_type": "UUIDField",
                            "to_field": "id",
                        },
                    ),
                    ("start_at", "DatetimeField", {}),
                    ("end_at", "DatetimeField", {}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_practitioner_window_excl",
                        expressions=(
                            (FieldRef("practitioner"), "="),
                            (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                        ),
                        index_type="gist",
                    ),
                ],
            ),
        },
        extensions=[PostgresExtension("btree_gist")],
    )

    ops = diff_states(project({}), new)

    assert isinstance(ops[0], AddExtension)
    assert ops[0].extension == PostgresExtension("btree_gist")
    assert any(isinstance(op, AddConstraint) for op in ops)
    assert next(i for i, op in enumerate(ops) if isinstance(op, AddExtension)) < next(
        i for i, op in enumerate(ops) if isinstance(op, AddConstraint)
    )


def test_project_extensions_remove_after_dependents():
    """Extensions should drop only after dependent constraints are removed."""
    old = project(
        {
            "Practitioner": model(
                "Practitioner",
                [("id", "UUIDField", {"primary_key": True})],
            ),
            "Booking": model(
                "Booking",
                [
                    ("id", "UUIDField", {"primary_key": True}),
                    (
                        "practitioner",
                        "ForeignKeyFieldInstance",
                        {
                            "related_table": "practitioner",
                            "referenced_type": "UUIDField",
                            "to_field": "id",
                        },
                    ),
                    ("start_at", "DatetimeField", {}),
                    ("end_at", "DatetimeField", {}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_practitioner_window_excl",
                        expressions=(
                            (FieldRef("practitioner"), "="),
                            (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
                        ),
                        index_type="gist",
                    ),
                ],
            ),
        },
        extensions=[PostgresExtension("btree_gist")],
    )
    new = project(
        {
            "Practitioner": model(
                "Practitioner",
                [("id", "UUIDField", {"primary_key": True})],
            ),
            "Booking": model(
                "Booking",
                [
                    ("id", "UUIDField", {"primary_key": True}),
                    (
                        "practitioner",
                        "ForeignKeyFieldInstance",
                        {
                            "related_table": "practitioner",
                            "referenced_type": "UUIDField",
                            "to_field": "id",
                        },
                    ),
                    ("start_at", "DatetimeField", {}),
                    ("end_at", "DatetimeField", {}),
                ],
            ),
        },
    )

    ops = diff_states(old, new)

    assert any(isinstance(op, RemoveConstraint) for op in ops)
    assert isinstance(ops[-1], RemoveExtension)
    assert next(i for i, op in enumerate(ops) if isinstance(op, RemoveConstraint)) < (
        next(i for i, op in enumerate(ops) if isinstance(op, RemoveExtension))
    )


def test_exclusion_constraints_treat_strings_and_fieldrefs_as_equivalent():
    """Legacy string expressions should compare equal to FieldRef expressions."""
    old = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("room", "IntField", {}),
                    ("timespan", "CharField", {"max_length": 255}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_room_timespan_excl",
                        expressions=(("room", "="), ("timespan", "&&")),
                        index_type="gist",
                    ),
                ],
            ),
        },
    )
    new = project(
        {
            "Booking": model(
                "Booking",
                [
                    ("room", "IntField", {}),
                    ("timespan", "CharField", {"max_length": 255}),
                ],
                constraints=[
                    ConstraintState(
                        kind="exclude",
                        name="booking_room_timespan_excl",
                        expressions=(
                            (FieldRef("room"), "="),
                            (FieldRef("timespan"), "&&"),
                        ),
                        index_type="gist",
                    ),
                ],
            ),
        },
    )

    assert diff_states(old, new) == []


def test_unique_together_becomes_constraint_not_unique_index():
    """unique_together-style state should emit AddConstraint rather than CreateIndex."""
    new = project(
        {
            "Book": model(
                "Book",
                [
                    ("title", "CharField", {"max_length": 255}),
                    ("edition", "IntField", {}),
                ],
                constraints=[
                    ConstraintState(kind="unique", columns=("title", "edition")),
                ],
            ),
        },
    )

    ops = diff_states(project({}), new)
    assert any(isinstance(op, AddConstraint) for op in ops)
    assert not any(
        isinstance(op, CreateIndex) and op.unique and op.columns == ("title", "edition")
        for op in ops
    )


def test_callable_default_sentinel_normalized_between_old_and_new():
    """Defaults 'callable' vs 'python_callable' should not trigger alters."""
    old = project(
        {
            "Book": model(
                "Book",
                [
                    (
                        "id",
                        "UUIDField",
                        {"default": "python_callable", "primary_key": True},
                    ),
                ],
            ),
        },
    )
    new = project(
        {
            "Book": model(
                "Book",
                [
                    (
                        "id",
                        "UUIDField",
                        {"default": "python_callable", "primary_key": True},
                    ),
                ],
            ),
        },
    )

    ops = diff_states(old, new)
    assert not any(isinstance(op, AlterField) for op in ops)


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


def test_model_rename_tracks_default_named_indexes_and_field_unique_constraints():
    """RenameModel should carry the physical renames Postgres will not do for us."""
    old = project(
        {
            "Author": model(
                "Author",
                [
                    ("id", "IntField", {"primary_key": True}),
                    ("email", "CharField", {"max_length": 255, "unique": True}),
                    ("slug", "CharField", {"max_length": 64, "index": True}),
                ],
                db_table="author",
                indexes=[IndexState(columns=("slug", "id"))],
            ),
        },
    )
    new = project(
        {
            "Writer": model(
                "Writer",
                [
                    ("id", "IntField", {"primary_key": True}),
                    ("email", "CharField", {"max_length": 255, "unique": True}),
                    ("slug", "CharField", {"max_length": 64, "index": True}),
                ],
                db_table="writer",
                indexes=[IndexState(columns=("slug", "id"))],
            ),
        },
    )

    ops = diff_states(old, new)

    renames = [op for op in ops if isinstance(op, RenameModel)]
    assert renames
    rename = renames[0]
    assert rename.index_renames == [
        ("author_slug_id_idx", "writer_slug_id_idx"),
        ("author_slug_idx", "writer_slug_idx"),
    ]
    assert rename.constraint_renames == [
        ("author_email_uniq", "writer_email_uniq"),
    ]
