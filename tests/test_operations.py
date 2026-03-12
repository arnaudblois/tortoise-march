"""Test that operations are correctly updating the ProjectSState."""

from enum import Enum

import pytest

from tortoisemarch.differ import diff_states
from tortoisemarch.exceptions import NotReversibleMigrationError
from tortoisemarch.model_state import (
    ConstraintState,
    FieldState,
    IndexState,
    ModelState,
    ProjectState,
)
from tortoisemarch.operations import (
    AddConstraint,
    AddField,
    AlterField,
    CreateIndex,
    CreateModel,
    RemoveConstraint,
    RemoveField,
    RemoveIndex,
    RemoveModel,
    RenameConstraint,
    RenameField,
    RenameModel,
    RunPython,
)
from tortoisemarch.schema_editor import PostgresSchemaEditor


def model(
    name: str,
    fields: list[tuple[str, str, dict]],
    db_table: str | None = None,
) -> ModelState:
    """Help shorthand to construct a ModelState from (name, type, options) tuples."""
    db_table = db_table or name.lower()
    return ModelState(
        name=name,
        db_table=db_table,
        field_states={
            fname.lower(): FieldState(name=fname, field_type=ftype, **opts)
            for fname, ftype, opts in fields
        },
    )


def test_create_model_mutates_state():
    """Test the state is mutated correctly after a CreateModel operation."""
    state = ProjectState()
    op = CreateModel(
        name="User",
        db_table="user",
        fields=[
            ("id", "IntField", {"null": False, "primary_key": True}),
            ("email", "CharField", {"null": False, "max_length": 200}),
        ],
    )
    op.mutate_state(state)

    assert "User" in state.model_states
    m = state.model_states["User"]
    assert m.name == "User"
    assert m.db_table == "user"
    assert set(m.field_states.keys()) == {"id", "email"}
    assert m.field_states["id"].primary_key is True
    assert m.field_states["email"].max_length == 200  # noqa: PLR2004


def test_remove_model_mutates_state():
    """Test the mutation for a RemoveModel op."""
    state = ProjectState(
        model_states={"User": model("User", [("id", "IntField", {"null": False})])},
    )
    op = RemoveModel(name="User", db_table="user")
    op.mutate_state(state)
    assert "User" not in state.model_states


def test_add_field_mutates_state():
    """Test the state mutation from a AddField op."""
    state = ProjectState(
        model_states={"User": model("User", [("id", "IntField", {"null": False})])},
    )
    op = AddField(
        model_name="User",
        db_table="user",
        field_name="email",
        field_type="CharField",
        options={"null": False, "max_length": 255, "unique": True},
    )
    op.mutate_state(state)

    fs = state.model_states["User"].field_states["email"]
    assert fs.field_type == "CharField"
    assert fs.null is False
    assert fs.max_length == 255  # noqa: PLR2004
    assert fs.unique is True


def test_remove_field_mutates_state():
    """Test the state mutation from a RemoveField op."""
    state = ProjectState(
        model_states={
            "User": model(
                "User",
                [
                    ("id", "IntField", {"null": False}),
                    ("email", "CharField", {"null": False}),
                ],
            ),
        },
    )
    op = RemoveField(model_name="User", db_table="user", field_name="email")
    op.mutate_state(state)
    assert "email" not in state.model_states["User"].field_states


def test_alter_field_mutates_state():
    """Test that the state mutation of AlterField is correct."""
    state = ProjectState(
        model_states={"User": model("User", [("id", "IntField", {"null": False})])},
    )
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="id",
        old_options={"type": "IntField", "null": False, "primary_key": True},
        new_options={"type": "CharField", "null": True},  # type change + nullability
    )
    op.mutate_state(state)

    fs = state.model_states["User"].field_states["id"]
    assert fs.field_type == "CharField"
    assert fs.null is True
    # unchanged flags should stay default/absent unless provided in new_options
    assert fs.primary_key is False


def test_rename_field_mutates_state():
    """Test the state mutation of a RenameField operation."""
    state = ProjectState(
        model_states={
            "User": model(
                "User",
                [
                    ("id", "IntField", {"null": False}),
                    ("name", "CharField", {"null": False}),
                ],
            ),
        },
    )
    op = RenameField(
        model_name="User",
        db_table="user",
        old_name="name",
        new_name="full_name",
    )
    op.mutate_state(state)

    fields = state.model_states["User"].field_states
    assert "name" not in fields
    assert "full_name" in fields
    assert fields["full_name"].field_type == "CharField"


def test_rename_field_mutates_state_updates_db_column_and_avoids_replay_churn():
    """RenameField should carry the destination db_column into replayed state."""
    fields = {
        "db_column": "author_id",
        "null": False,
        "related_table": "author",
        "to_field": "id",
        "on_delete": "CASCADE",
    }
    replayed = ProjectState(
        model_states={
            "Book": model(
                "Book",
                [("author", "ForeignKeyFieldInstance", fields)],
                db_table="book",
            ),
        },
    )
    RenameField(
        model_name="Book",
        db_table="book",
        old_name="author",
        new_name="writer",
        old_db_column="author_id",
        new_db_column="writer_id",
    ).mutate_state(replayed)

    fields = replayed.model_states["Book"].field_states
    fs = fields["writer"]
    assert fs.name == "writer"
    assert fs.db_column == "writer_id"
    fields = {
        "db_column": "writer_id",
        "null": False,
        "related_table": "author",
        "to_field": "id",
        "on_delete": "CASCADE",
    }
    current = ProjectState(
        model_states={
            "Book": model(
                "Book",
                [("writer", "ForeignKeyFieldInstance", fields)],
                db_table="book",
            ),
        },
    )

    ops = diff_states(replayed, current)
    assert not any(isinstance(op, AlterField) for op in ops)


# ---------------- Extra coverage for tricky paths ----------------


def test_alter_field_preserves_type_when_not_provided():
    """Test that if new_options lacks 'type', keep previous type in state."""
    state = ProjectState(
        model_states={
            "User": model(
                "User",
                [("email", "CharField", {"null": False, "max_length": 120})],
            ),
        },
    )
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="email",
        old_options={"type": "CharField", "null": False, "max_length": 120},
        new_options={"null": True, "max_length": 200},  # no 'type' here
    )
    op.mutate_state(state)

    fs = state.model_states["User"].field_states["email"]
    assert fs.field_type == "CharField"  # unchanged
    assert fs.null is True  # changed
    assert fs.max_length == 200  # changed  # noqa: PLR2004


def test_alter_field_with_rename_via_new_name():
    """AlterField supports rename+alter in a single operation."""
    state = ProjectState(
        model_states={
            "User": model(
                "User",
                [("name", "CharField", {"null": False, "max_length": 50})],
            ),
        },
    )
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="name",
        old_options={"type": "CharField", "null": False, "max_length": 50},
        new_options={"type": "CharField", "null": False, "max_length": 200},
        new_name="full_name",
    )
    op.mutate_state(state)

    fields = state.model_states["User"].field_states
    assert "name" not in fields
    assert "full_name" in fields
    fs = fields["full_name"]
    assert fs.field_type == "CharField"
    assert fs.max_length == 200  # noqa: PLR2004


def test_alter_field_to_code_serializes_enum_default():
    """Ensure AlterField.to_code normalises Enum defaults to literal values."""

    class Status(Enum):
        DRAFT = "draft"
        SENT = "sent"

    op = AlterField(
        model_name="Message",
        db_table="message",
        field_name="status",
        old_options={"type": "CharField", "default": None},
        new_options={"type": "CharField", "default": Status.DRAFT},
    )

    code = op.to_code()
    assert "<Status.DRAFT" not in code  # enum instance must not leak
    assert "'draft'" in code  # value rendered as literal


def test_alter_field_to_code_accepts_field_type_alias():
    """Code rendering should accept the same type alias the runtime accepts."""
    op = AlterField(
        model_name="Message",
        db_table="message",
        field_name="status",
        old_options={"field_type": "CharField", "default": None},
        new_options={"field_type": "CharField", "default": "draft"},
    )

    code = op.to_code()

    assert "field_type" not in code
    assert "type" in code
    assert "'draft'" in code


def test_alter_field_mutate_state_preserves_existing_options():
    """AlterField should not drop unchanged options in mutate_state."""
    max_length = 12
    field_opts = {"primary_key": True, "max_length": max_length, "null": False}
    state = ProjectState(
        model_states={
            "Book": model("Book", [("id", "CharField", field_opts)], db_table="book"),
        },
    )

    op = AlterField(
        model_name="Book",
        db_table="book",
        field_name="id",
        old_options={"type": "CharField"},
        new_options={"type": "CharField", "default": "python_callable"},
    )
    op.mutate_state(state)

    fs = state.model_states["Book"].field_states["id"]
    assert fs.primary_key is True
    assert fs.max_length == max_length
    assert fs.null is False
    assert fs.default == "python_callable"


def test_alter_field_mutate_state_prunes_stale_length_on_textfield_rename():
    """Replaying CharField -> TextField should not keep stale max_length."""
    fields = [("author", "CharField", {"null": False, "max_length": 255})]
    state = ProjectState(model_states={"User": model("User", fields, db_table="user")})
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="author",
        old_options={"type": "CharField", "max_length": 255},
        new_options={"type": "TextField"},
        new_name="book_author",
    )
    op.mutate_state(state)

    fields = state.model_states["User"].field_states
    assert "author" not in fields
    assert "book_author" in fields
    fs = fields["book_author"]
    assert fs.field_type == "TextField"
    assert fs.max_length is None


def test_replayed_charfield_to_textfield_rename_has_no_follow_up_alter():
    """Replay state should match model state and avoid max_length churn."""
    fields = [("author", "CharField", {"null": False, "max_length": 255})]
    replayed = ProjectState(
        model_states={"User": model("User", fields, db_table="user")},
    )
    AlterField(
        model_name="User",
        db_table="user",
        field_name="author",
        old_options={"type": "CharField", "max_length": 255},
        new_options={"type": "TextField"},
        new_name="book_author",
    ).mutate_state(replayed)

    fields = [("book_author", "TextField", {"null": False})]
    current = ProjectState(
        model_states={"User": model("User", fields, db_table="user")},
    )

    ops = diff_states(replayed, current)
    assert not any(op for op in ops if isinstance(op, AlterField))


def test_replayed_two_factor_secret_rename_to_text_has_no_churn():
    """Replay for _two_factor_secret -> two_factor_secret should stay canonical."""
    old_field = (
        "_two_factor_secret",
        "CharField",
        {"null": False, "max_length": 255, "default": "python_callable"},
    )
    replayed = ProjectState(
        model_states={"User": model("User", [old_field], db_table="user")},
    )
    AlterField(
        model_name="User",
        db_table="user",
        field_name="_two_factor_secret",
        old_options={"type": "CharField", "max_length": 255},
        new_options={"type": "TextField"},
        new_name="two_factor_secret",
    ).mutate_state(replayed)

    fs = replayed.model_states["User"].field_states["two_factor_secret"]
    assert fs.field_type == "TextField"
    assert fs.max_length is None
    assert fs.default == "python_callable"
    field = (
        "two_factor_secret",
        "TextField",
        {"null": False, "default": "python_callable"},
    )
    current = ProjectState(
        model_states={"User": model("User", [field], db_table="user")},
    )

    ops = diff_states(replayed, current)
    assert not any(isinstance(op, AlterField) for op in ops)


def test_alter_field_mutate_state_keeps_charfield_length_change():
    """CharField max_length changes should still survive replay as before."""
    fields = [("author", "CharField", {"null": False, "max_length": 120})]
    state = ProjectState(model_states={"User": model("User", fields, db_table="user")})
    op = AlterField(
        model_name="User",
        db_table="user",
        field_name="author",
        old_options={"type": "CharField", "max_length": 120, "null": False},
        new_options={"type": "CharField", "max_length": 200, "null": False},
    )
    op.mutate_state(state)

    fs = state.model_states["User"].field_states["author"]
    assert fs.field_type == "CharField"
    assert fs.max_length == 200  # noqa: PLR2004


def test_add_field_fk_metadata_preserved_in_state():
    """Ensure FK-related options make it into FieldState.options."""
    state = ProjectState(
        model_states={"Book": model("Book", [("id", "UUIDField", {})])},
    )

    op = AddField(
        model_name="Book",
        db_table="book",
        field_name="author",
        field_type="ForeignKeyFieldInstance",
        options={
            "db_column": "author_id",
            "null": False,
            "related_table": "author",
            "to_field": "id",
            "on_delete": "CASCADE",
            "referenced_type": "UUIDField",
        },
    )
    op.mutate_state(state)

    fs = state.model_states["Book"].field_states["author"]
    assert fs.field_type == "ForeignKeyFieldInstance"
    assert fs.db_column == "author_id"
    assert fs.related_table == "author"
    assert fs.to_field == "id"
    assert fs.on_delete == "CASCADE"
    assert fs.referenced_type == "UUIDField"


def test_to_code_is_stringy():
    """Writer relies on to_code() — just sanity-check it returns a string."""
    ops = [
        CreateModel("Thing", "thing", [("id", "IntField", {"primary_key": True})]),
        AddField("Thing", "thing", "name", "CharField", {"max_length": 20}),
        RemoveField("Thing", "thing", "name"),
        # FIX: include field_name for AlterField
        AlterField(
            "Thing",
            "thing",
            "id",
            {"type": "IntField"},
            {"type": "IntField", "null": True},
        ),
        RemoveModel("Thing", "thing"),
        RenameField("Thing", "thing", "old", "new"),
    ]
    for op in ops:
        s = op.to_code()
        assert isinstance(s, str)
        assert s.strip()  # non-empty


async def test_runpython_accepts_zero_arg_callable():
    """RunPython should accept callables that take no arguments."""
    called = {"value": False}

    async def forwards():
        called["value"] = True

    op = RunPython(forwards)
    await op.apply(None, None)

    assert called["value"] is True


async def test_runpython_accepts_apps_only_callable():
    """RunPython should provide historical apps to one-arg callables."""
    called = {"value": None}

    async def forwards(apps):
        called["value"] = apps

    marker = object()
    op = RunPython(forwards)
    await op.apply(None, None, apps=marker)

    assert called["value"] is marker


async def test_runpython_accepts_three_arg_callable():
    """RunPython should support (conn, schema_editor, apps)."""
    called: dict[str, object | None] = {
        "conn": None,
        "schema_editor": None,
        "apps": None,
    }

    async def forwards(conn, schema_editor, apps):
        called["conn"] = conn
        called["schema_editor"] = schema_editor
        called["apps"] = apps

    marker = object()
    op = RunPython(forwards)
    await op.apply("conn", "schema_editor", apps=marker)

    assert called == {
        "conn": "conn",
        "schema_editor": "schema_editor",
        "apps": marker,
    }


async def test_alter_field_text_widening_rename_is_not_reversible():
    """We reject rollback previews that would silently keep the widened type."""
    op = AlterField(
        model_name="Book",
        db_table="book",
        field_name="author",
        old_options={"type": "CharField", "max_length": 255, "null": False},
        new_options={"type": "TextField", "null": False},
        new_name="book_author",
    )

    with pytest.raises(NotReversibleMigrationError):
        await op.unapply(None, PostgresSchemaEditor())

    with pytest.raises(NotReversibleMigrationError):
        await op.to_sql_unapply(conn=None, schema_editor=PostgresSchemaEditor())


async def test_createindex_and_removeindex_to_code_and_mutation():
    """CreateIndex/RemoveIndex should render code and update index state."""
    state = ProjectState(
        model_states={
            "Item": ModelState(name="Item", db_table="item", field_states={}),
        },
    )

    ci = CreateIndex(
        model_name="Item",
        db_table="item",
        columns=("a", "b"),
        unique=False,
        name="item_a_b_idx",
    )
    sql = await ci.to_sql(conn=None, schema_editor=PostgresSchemaEditor())
    assert "CREATE INDEX" in sql[0]
    assert "item_a_b_idx" in sql[0]
    ci.mutate_state(state)
    assert state.model_states["Item"].indexes == [
        IndexState(columns=("a", "b"), name="item_a_b_idx"),
    ]

    ri = RemoveIndex(
        model_name="Item",
        db_table="item",
        name="item_a_b_idx",
        columns=("a", "b"),
        unique=False,
    )
    sql_drop = await ri.to_sql(conn=None, schema_editor=PostgresSchemaEditor())
    assert "DROP INDEX" in sql_drop[0]
    ri.mutate_state(state)
    assert not state.model_states["Item"].indexes


async def test_addfield_to_sql_unapply_uses_db_column_override():
    """Rollback SQL should use the configured physical column name."""
    add = AddField(
        model_name="Book",
        db_table="book",
        field_name="author",
        field_type="ForeignKeyFieldInstance",
        options={
            "db_column": "author_id",
            "related_table": "author",
            "to_field": "id",
            "on_delete": "CASCADE",
            "referenced_type": "UUIDField",
        },
    )

    sql = await add.to_sql_unapply(conn=None, schema_editor=PostgresSchemaEditor())

    assert sql == ['ALTER TABLE "book" DROP COLUMN IF EXISTS "author_id";']


async def test_constraint_operations_render_and_mutate_state():
    """Constraint operations should render code and update model constraint state."""
    state = ProjectState(
        model_states={
            "Item": ModelState(name="Item", db_table="item", field_states={}),
        },
    )
    constraint = ConstraintState(kind="unique", name="item_sku_uniq", columns=("sku",))

    add = AddConstraint(
        model_name="Item",
        db_table="item",
        constraint=constraint,
    )
    add_sql = await add.to_sql(conn=None, schema_editor=PostgresSchemaEditor())
    assert 'ADD CONSTRAINT "item_sku_uniq" UNIQUE ("sku")' in add_sql[0]
    add.mutate_state(state)
    assert state.model_states["Item"].constraints == [constraint]

    rename = RenameConstraint(
        model_name="Item",
        db_table="item",
        old_name="item_sku_uniq",
        new_name="item_stock_keeping_unit_uniq",
        old_constraint=constraint,
        new_constraint=ConstraintState(
            kind="unique",
            name="item_stock_keeping_unit_uniq",
            columns=("sku",),
        ),
    )
    rename_sql = await rename.to_sql(conn=None, schema_editor=PostgresSchemaEditor())
    assert rename_sql[0] == (
        'ALTER TABLE "item" RENAME CONSTRAINT "item_sku_uniq" '
        'TO "item_stock_keeping_unit_uniq";'
    )
    rename.mutate_state(state)
    assert state.model_states["Item"].constraints == [
        ConstraintState(
            kind="unique",
            name="item_stock_keeping_unit_uniq",
            columns=("sku",),
        ),
    ]

    remove = RemoveConstraint(
        model_name="Item",
        db_table="item",
        constraint=ConstraintState(
            kind="unique",
            name="item_stock_keeping_unit_uniq",
            columns=("sku",),
        ),
    )
    remove_sql = await remove.to_sql(conn=None, schema_editor=PostgresSchemaEditor())
    assert 'DROP CONSTRAINT IF EXISTS "item_stock_keeping_unit_uniq";' in remove_sql[0]
    remove.mutate_state(state)
    assert not state.model_states["Item"].constraints


async def test_exclusion_constraint_operations_render_and_mutate_state():
    """Exclusion constraints should render SQL and update state like others."""
    state = ProjectState(
        model_states={
            "Booking": ModelState(name="Booking", db_table="booking", field_states={}),
        },
    )
    constraint = ConstraintState(
        kind="exclude",
        name="booking_room_timespan_excl",
        expressions=(("room", "="), ("timespan", "&&")),
        index_type="gist",
        condition="cancelled_at IS NULL",
    )

    add = AddConstraint(
        model_name="Booking",
        db_table="booking",
        constraint=constraint,
    )
    add_sql = await add.to_sql(conn=None, schema_editor=PostgresSchemaEditor())
    assert (
        add_sql[0]
        == 'ALTER TABLE "booking" ADD CONSTRAINT "booking_room_timespan_excl" '
        'EXCLUDE USING gist ("room" WITH =, "timespan" WITH &&) '
        "WHERE (cancelled_at IS NULL);"
    )
    add.mutate_state(state)
    assert state.model_states["Booking"].constraints == [constraint]


def test_add_constraint_to_code_is_deterministic():
    """Constraint operations should render deterministic migration code."""
    op = AddConstraint(
        model_name="Invoice",
        db_table="invoice",
        constraint=ConstraintState(
            kind="check",
            name="invoice_total_check",
            check="total >= 0",
        ),
    )

    assert (
        op.to_code() == "AddConstraint(model_name='Invoice', db_table='invoice', "
        "constraint={'kind': 'check', 'name': 'invoice_total_check', "
        "'check': 'total >= 0'}, name='invoice_total_check')"
    )


def test_alter_field_to_code_orders_changed_opts():
    """AlterField.to_code should emit deterministically ordered option dicts."""
    op = AlterField(
        "Book",
        "book",
        "author",
        {"related_table": "author", "type": "ForeignKeyFieldInstance"},
        {
            "related_model": "models.Writer",
            "related_table": "writer",
            "type": "ForeignKeyFieldInstance",
        },
    )

    code = op.to_code()

    expected = (
        "AlterField(model_name='Book', db_table='book', field_name='author', "
        "old_options={'related_table': 'author', 'type': 'ForeignKeyFieldInstance'}, "
        "new_options={'related_model': 'models.Writer', 'related_table': 'writer', "
        "'type': 'ForeignKeyFieldInstance'})"
    )
    assert code == expected


# ------- Test RenameModel --------------------------------------------------


async def test_renamemodel_to_sql_emits_table_rename():
    """RenameModel renders ALTER TABLE when db_table changes."""
    editor = PostgresSchemaEditor()

    op = RenameModel(
        old_name="Author",
        new_name="Writer",
        old_db_table="author",
        new_db_table="writer",
    )

    sql = await op.to_sql(conn=None, schema_editor=editor)

    assert sql == ['ALTER TABLE "author" RENAME TO "writer"']


async def test_renamemodel_to_sql_emits_artifact_renames():
    """RenameModel should also rename default-derived indexes and constraints."""
    editor = PostgresSchemaEditor()

    op = RenameModel(
        old_name="Author",
        new_name="Writer",
        old_db_table="author",
        new_db_table="writer",
        index_renames=[("author_slug_idx", "writer_slug_idx")],
        constraint_renames=[("author_email_uniq", "writer_email_uniq")],
    )

    sql = await op.to_sql(conn=None, schema_editor=editor)

    assert sql == [
        'ALTER TABLE "author" RENAME TO "writer"',
        'ALTER INDEX "author_slug_idx" RENAME TO "writer_slug_idx";',
        (
            'ALTER TABLE "writer" RENAME CONSTRAINT "author_email_uniq" '
            'TO "writer_email_uniq";'
        ),
    ]


async def test_renamemodel_to_sql_noop_when_table_unchanged():
    """RenameModel renders no SQL when only the model name changes."""
    editor = PostgresSchemaEditor()

    op = RenameModel(
        old_name="Author",
        new_name="Writer",
        old_db_table="author",
        new_db_table="author",
    )

    sql = await op.to_sql(conn=None, schema_editor=editor)

    assert sql == []


async def test_renamemodel_to_sql_unapply_reverses_artifact_renames():
    """Rollback SQL should rename derived artifacts back before the table."""
    editor = PostgresSchemaEditor()

    op = RenameModel(
        old_name="Author",
        new_name="Writer",
        old_db_table="author",
        new_db_table="writer",
        index_renames=[("author_slug_idx", "writer_slug_idx")],
        constraint_renames=[("author_email_uniq", "writer_email_uniq")],
    )

    sql = await op.to_sql_unapply(conn=None, schema_editor=editor)

    assert sql == [
        (
            'ALTER TABLE "writer" RENAME CONSTRAINT "writer_email_uniq" '
            'TO "author_email_uniq";'
        ),
        'ALTER INDEX "writer_slug_idx" RENAME TO "author_slug_idx";',
        'ALTER TABLE "writer" RENAME TO "author"',
    ]


def test_renamemodel_mutate_state_renames_model_and_updates_fk_metadata():
    """RenameModel updates ProjectState and rewrites FK related_table."""
    author = ModelState(
        name="Author",
        db_table="author",
        field_states={
            "id": FieldState(name="id", field_type="IntField", primary_key=True),
            "name": FieldState(name="name", field_type="CharField", max_length=100),
        },
    )

    book = ModelState(
        name="Book",
        db_table="book",
        field_states={
            "id": FieldState(name="id", field_type="IntField", primary_key=True),
            "author": FieldState(
                name="author",
                field_type="ForeignKeyField",
                related_table="author",
                to_field="id",
                null=True,
            ),
        },
    )

    state = ProjectState(model_states={"Author": author, "Book": book})

    op = RenameModel(
        old_name="Author",
        new_name="Writer",
        old_db_table="author",
        new_db_table="writer",
    )

    op.mutate_state(state)

    assert "Author" not in state.model_states
    assert "Writer" in state.model_states

    writer = state.model_states["Writer"]
    assert writer.name == "Writer"
    assert writer.db_table == "writer"

    fk = state.model_states["Book"].field_states["author"]
    assert fk.options["related_table"] == "writer"
