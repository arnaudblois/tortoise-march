"""Test that operations are correctly updating the ProjectSState."""

from tortoisemarch.model_state import FieldState, ModelState, ProjectState
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateIndex,
    CreateModel,
    RemoveField,
    RemoveIndex,
    RemoveModel,
    RenameField,
    RenameModel,
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
    assert "user" not in state.model_states


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


async def test_createindex_and_removeindex_to_code_and_mutation():
    """CreateIndex/RemoveIndex should render code and update meta indexes."""
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
    assert (("a", "b"), False) in state.model_states["Item"].meta.get("indexes", [])

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
    assert not state.model_states["Item"].meta.get("indexes")


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
