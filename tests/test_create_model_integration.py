"""Test that Models are created correctly."""

import pytest
from tortoise import Tortoise

from tortoisemarch.operations import CreateModel
from tortoisemarch.schema_editor import PostgresSchemaEditor

DATABASE_URL = "postgres://postgres:test@localhost:5445/testdb"


@pytest.mark.asyncio
async def test_create_model_table_exists():
    """Ensure that CreateModel creates the users table in Postgres."""
    await Tortoise.init(
        config={
            "connections": {"default": DATABASE_URL},
            "apps": {"models": {"models": [], "default_connection": "default"}},
        },
    )
    conn = Tortoise.get_connection("default")

    # Drop the table if it already exists
    await conn.execute_script("DROP TABLE IF EXISTS users CASCADE;")

    # Apply CreateModel
    op = CreateModel(
        name="User",
        db_table="users",
        fields=[
            ("id", "IntField", {"null": False, "primary_key": True}),
            ("email", "CharField", {"null": False, "unique": True, "max_length": 255}),
        ],
    )
    schema_editor = PostgresSchemaEditor()
    await op.apply(conn, schema_editor)

    # Check table existence using Tortoise's query API
    rows = await conn.execute_query_dict(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'users'
        ) AS exists;
        """,
    )
    assert rows[0]["exists"] is True

    # Clean up
    await conn.execute_script("DROP TABLE IF EXISTS users CASCADE;")
    await Tortoise.close_connections()
