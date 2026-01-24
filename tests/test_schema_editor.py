"""Test that the ops of the Schema Editor can be executed correctly."""

from enum import Enum

import asyncpg
import pytest

from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.operations import (
    AddField,
    AlterField,
    CreateModel,
    RemoveField,
    RemoveModel,
)
from tortoisemarch.schema_editor import PostgresSchemaEditor

DATABASE_URL = "postgres://postgres:test@localhost:5445/testdb"


@pytest.fixture
def schema_editor():
    """Create a Postgres SchemaEditor for testing."""
    return PostgresSchemaEditor()


async def test_create_and_remove_model(schema_editor):
    """Test CreateModel and RemoveModel ops work correctly."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Clean slate
        await conn.execute('DROP TABLE IF EXISTS "book" CASCADE')

        # Create Book model with id + title
        op = CreateModel(
            name="Book",
            db_table="book",
            fields=[
                ("id", "IntField", {"primary_key": True}),
                ("title", "CharField", {"null": False, "max_length": 200}),
            ],
        )
        await op.apply(conn, schema_editor)

        # Verify table exists
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_name='book')",
        )
        assert exists is True

        # Drop the table
        op = RemoveModel(name="Book", db_table="book")
        await op.apply(conn, schema_editor)

        # Verify table removed
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_name='book')",
        )
        assert exists is False
    finally:
        await conn.close()


async def test_add_and_remove_field(schema_editor):
    """Test that AddField and RemoveField ops are executed correctly."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Clean slate
        await conn.execute('DROP TABLE IF EXISTS "author" CASCADE')
        await conn.execute('CREATE TABLE "author" (id SERIAL PRIMARY KEY)')

        # Add "name" field
        op = AddField(
            model_name="Author",
            db_table="author",
            field_name="name",
            field_type="CharField",
            options={"null": False, "max_length": 100},
        )
        await op.apply(conn, schema_editor)

        # Verify column exists
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_name='author' AND column_name='name')",
        )
        assert exists is True

        # Remove "name" field
        op = RemoveField(model_name="Author", db_table="author", field_name="name")
        await op.apply(conn, schema_editor)

        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_name='author' AND column_name='name')",
        )
        assert exists is False
    finally:
        await conn.close()


async def test_alter_field_nullability_and_default(schema_editor):
    """Test that AlterField's null and default are honoured."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # Clean slate
        await conn.execute('DROP TABLE IF EXISTS "user_account" CASCADE')
        await conn.execute(
            'CREATE TABLE "user_account" (id SERIAL PRIMARY KEY, email VARCHAR(200))',
        )

        # Alter "email" field: set NOT NULL and add DEFAULT
        op = AlterField(
            model_name="UserAccount",
            db_table="user_account",
            field_name="email",
            old_options={"null": True, "default": None, "field_type": "CharField"},
            new_options={
                "null": False,
                "default": "unknown@example.com",
                "field_type": "CharField",
            },
        )
        await op.apply(conn, schema_editor)

        # Verify NOT NULL
        is_nullable = await conn.fetchval(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_name='user_account' AND column_name='email'",
        )
        assert is_nullable == "NO"

        # Verify default
        column_default = await conn.fetchval(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_name='user_account' AND column_name='email'",
        )
        assert "unknown@example.com" in (column_default or "")
    finally:
        await conn.close()


async def test_alter_field_fk_uses_db_column_and_applies(schema_editor):
    """FK alters should use the backing column name and apply cleanly."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute('DROP TABLE IF EXISTS "book" CASCADE')
        await conn.execute('DROP TABLE IF EXISTS "author" CASCADE')
        await conn.execute('CREATE TABLE "author" (id SERIAL PRIMARY KEY)')
        await conn.execute(
            'CREATE TABLE "book" ('
            "id SERIAL PRIMARY KEY, "
            'author_id INTEGER NOT NULL REFERENCES "author"(id))',
        )

        stmts = schema_editor.sql_alter_field(
            db_table="book",
            field_name="author",
            old_options={"type": "ForeignKeyFieldInstance", "null": False},
            new_options={"type": "ForeignKeyFieldInstance", "null": True},
        )
        assert any('ALTER COLUMN "author_id"' in stmt for stmt in stmts)

        op = AlterField(
            model_name="Book",
            db_table="book",
            field_name="author",
            old_options={"type": "ForeignKeyFieldInstance", "null": False},
            new_options={"type": "ForeignKeyFieldInstance", "null": True},
        )
        await op.apply(conn, schema_editor)

        is_nullable = await conn.fetchval(
            "SELECT is_nullable FROM information_schema.columns "
            "WHERE table_name='book' AND column_name='author_id'",
        )
        assert is_nullable == "YES"
    finally:
        await conn.close()


async def test_alter_field_accepts_enum_default(schema_editor):
    """Enum defaults should be coerced to their underlying value for SQL."""

    class Status(Enum):
        DRAFT = "draft"
        SENT = "sent"

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute('DROP TABLE IF EXISTS "msg" CASCADE')
        await conn.execute(
            'CREATE TABLE "msg" (id SERIAL PRIMARY KEY, status VARCHAR(20))',
        )

        op = AlterField(
            model_name="Msg",
            db_table="msg",
            field_name="status",
            old_options={
                "type": "CharField",
                "null": True,
                "max_length": 20,
                "default": None,
            },
            new_options={
                "type": "CharField",
                "null": False,
                "max_length": 20,
                "default": Status.SENT,
            },
        )

        await op.apply(conn, schema_editor)

        default_sql = await conn.fetchval(
            "SELECT column_default FROM information_schema.columns "
            "WHERE table_name='msg' AND column_name='status'",
        )
        assert "sent" in (default_sql or "")
    finally:
        await conn.close()


def test_alter_field_fk_respects_db_column_override():
    """db_column should win over FK default <name>_id resolution."""
    ed = PostgresSchemaEditor()
    stmts = ed.sql_alter_field(
        db_table="commerces_orders",
        field_name="person",
        old_options={
            "type": "ForeignKeyFieldInstance",
            "null": False,
            "db_column": "person_ref",
        },
        new_options={
            "type": "ForeignKeyFieldInstance",
            "null": True,
            "db_column": "person_ref",
        },
    )
    assert any('ALTER COLUMN "person_ref"' in stmt for stmt in stmts)


def test_sql_add_remove_rename_field_use_fk_column_names():
    """FK add/remove/rename should resolve to <name>_id by default."""
    ed = PostgresSchemaEditor()
    sql = ed.sql_add_field(
        db_table="commerces_orders",
        field_name="person",
        field_type="ForeignKeyFieldInstance",
        options={
            "related_table": "people",
            "to_field": "id",
            "referenced_type": "IntField",
        },
    )
    assert 'ADD COLUMN "person_id"' in sql

    sql = ed.sql_remove_field(
        db_table="commerces_orders",
        field_name="person",
        db_column="person_id",
    )
    assert 'DROP COLUMN IF EXISTS "person_id"' in sql

    sql = ed.sql_rename_field(
        db_table="commerces_orders",
        old_name="person",
        new_name="buyer",
        old_db_column="person_id",
        new_db_column="buyer_id",
    )
    assert 'RENAME COLUMN "person_id" TO "buyer_id"' in sql


def test_db_default_expr_is_unquoted_python_callable_is_not_emitted():
    """Test that DB default are emitted verbatim and Python callables are ignored."""
    ed = PostgresSchemaEditor()

    fields = [
        ("created_at", "DatetimeField", {"default": "db_default:now()"}),
        ("id", "UUIDField", {"primary_key": True, "default": "python_callable"}),
        ("label", "CharField", {"max_length": 10, "default": "hi"}),
        ("flag", "BooleanField", {"default": True}),
    ]

    sql = ed.sql_create_model("t", fields)

    assert '"created_at" TIMESTAMPTZ NOT NULL DEFAULT now()' in sql
    # python_callable should not generate DEFAULT
    frag = sql.split('"id" UUID', 1)[1].split(",", 1)[0]
    assert "DEFAULT" not in frag
    # string literal should be quoted
    assert "DEFAULT 'hi'" in sql
    assert "DEFAULT TRUE" in sql


def test_alter_default_set_expr_and_drop_default():
    """Test that ALTER statements set or drop DEFAULT when default values change."""
    ed = PostgresSchemaEditor()

    stmts = ed.sql_alter_field(
        db_table="t",
        field_name="created_at",
        old_options={"type": "DatetimeField", "default": "python_callable"},
        new_options={"type": "DatetimeField", "default": "db_default:now()"},
    )
    assert any("SET DEFAULT now()" in s for s in stmts)

    stmts = ed.sql_alter_field(
        db_table="t",
        field_name="created_at",
        old_options={"type": "DatetimeField", "default": "db_default:now()"},
        new_options={"type": "DatetimeField", "default": None},
    )
    assert any("DROP DEFAULT" in s for s in stmts)


def test_alter_field_rename_fk_uses_db_column():
    """Check that renaming a FK renames its DB column, not the logical field name."""
    ed = PostgresSchemaEditor()
    stmts = ed.sql_alter_field(
        db_table="books",
        field_name="author",
        old_options={
            "type": "ForeignKeyFieldInstance",
            "db_column": "author_id",
            "related_table": "authors",
            "related_model": "people.models.Author",
        },
        new_options={
            "type": "ForeignKeyFieldInstance",
            "db_column": "writer_id",
            "related_table": "writers",
            "related_model": "people.models.Writer",
        },
        new_name="writer",
    )
    assert stmts == ['ALTER TABLE "books" RENAME COLUMN "author_id" TO "writer_id";']


def test_sql_create_index_and_unique_index():
    """Index SQL should quote names and respect uniqueness and column lists."""
    ed = PostgresSchemaEditor()
    nonuniq = ed.sql_create_index(
        db_table="foo",
        name="foo_a_b_idx",
        columns=("a", "b"),
        unique=False,
    )
    uniq = ed.sql_create_index(
        db_table="foo",
        name="foo_a_b_uniq",
        columns=("a", "b"),
        unique=True,
    )
    assert nonuniq == 'CREATE INDEX "foo_a_b_idx" ON "foo" ("a", "b");'
    assert uniq == 'CREATE UNIQUE INDEX "foo_a_b_uniq" ON "foo" ("a", "b");'


def test_schema_editor_refuses_non_schema_field_types():
    """Test that non-schema field types are rejected by the schema editor."""
    ed = PostgresSchemaEditor()
    with pytest.raises(InvalidMigrationError):
        ed.sql_for_field("BackwardOneToOneRelation", {})


def test_schema_editor_refuses_unknown_field_types():
    """Test that unknown field types are rejected by the schema editor."""
    ed = PostgresSchemaEditor()
    with pytest.raises(InvalidMigrationError):
        ed.sql_for_field("TotallyUnknownFieldType", {})


def test_primary_key_column_omits_redundant_constraints():
    """PRIMARY KEY should not emit extra NOT NULL/UNIQUE."""
    ed = PostgresSchemaEditor()
    sql = ed.sql_create_model(
        "thing",
        [("id", "UUIDField", {"primary_key": True})],
    )
    assert "PRIMARY KEY" in sql
    assert "NOT NULL" not in sql
    assert "UNIQUE" not in sql


def test_unique_non_pk_still_emits_unique():
    """Non-PK unique columns should still render UNIQUE."""
    ed = PostgresSchemaEditor()
    sql = ed.sql_create_model(
        "thing",
        [
            ("id", "IntField", {"primary_key": True}),
            ("slug", "CharField", {"max_length": 20, "unique": True}),
        ],
    )
    assert "slug" in sql
    assert "UNIQUE" in sql


def test_sql_create_index_multi_column_and_unique():
    """sql_create_index should handle multi-column and unique flags."""
    ed = PostgresSchemaEditor()

    sql = ed.sql_create_index(
        db_table="thing",
        name="thing_multi_idx",
        columns=("a", "b"),
        unique=False,
    )
    assert 'CREATE INDEX "thing_multi_idx"' in sql
    assert '"a", "b"' in sql

    sql_unique = ed.sql_create_index(
        db_table="thing",
        name="thing_ab_uniq",
        columns=("a", "b"),
        unique=True,
    )
    assert "UNIQUE INDEX" in sql_unique


def test_sql_create_model_emits_indexes():
    """CreateModel SQL should include indexes for indexed fields."""
    ed = PostgresSchemaEditor()
    sql = ed.sql_create_model(
        "book",
        [
            ("id", "IntField", {"primary_key": True}),
            ("title", "CharField", {"max_length": 200, "index": True}),
            ("slug", "CharField", {"max_length": 100, "unique": True}),
        ],
    )

    assert 'CREATE INDEX "book_title_idx"' in sql
    # unique constraint should not emit a separate index
    assert "slug_idx" not in sql


def test_sql_add_field_with_index_appends_create_index():
    """AddField SQL should append CREATE INDEX when index=True."""
    ed = PostgresSchemaEditor()

    sql = ed.sql_add_field(
        db_table="author",
        field_name="email",
        field_type="CharField",
        options={"max_length": 255, "index": True},
    )

    assert "ADD COLUMN" in sql
    assert 'CREATE INDEX "author_email_idx"' in sql


def test_sql_alter_field_handles_index_changes():
    """AlterField SQL should drop/create indexes when index flag changes."""
    ed = PostgresSchemaEditor()

    stmts = ed.sql_alter_field(
        db_table="invitation",
        field_name="membership",
        old_options={"type": "IntField", "index": False},
        new_options={"type": "IntField", "index": True},
    )
    assert any("CREATE INDEX" in s for s in stmts)

    stmts = ed.sql_alter_field(
        db_table="invitation",
        field_name="membership",
        old_options={"type": "IntField", "index": True},
        new_options={"type": "IntField", "index": False},
    )
    assert any("DROP INDEX" in s for s in stmts)


def test_sql_alter_field_supports_integer_widening():
    """Integer widening should emit a TYPE change statement."""
    ed = PostgresSchemaEditor()

    stmts = ed.sql_alter_field(
        db_table="audit_log",
        field_name="id",
        old_options={"type": "IntField"},
        new_options={"type": "BigIntField"},
    )
    assert any("TYPE BIGINT" in s for s in stmts)

    stmts = ed.sql_alter_field(
        db_table="audit_log",
        field_name="id",
        old_options={"type": "SmallIntField"},
        new_options={"type": "IntField"},
    )
    assert any("TYPE INTEGER" in s for s in stmts)
