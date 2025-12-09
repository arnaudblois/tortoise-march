"""Test that the ops of the Schema Editor can be executed correctly."""

import asyncpg
import pytest

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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
