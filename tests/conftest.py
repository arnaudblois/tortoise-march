"""Configuration script for Pytest."""

import sys

import pytest
from tortoise import Tortoise

DATABASE_URL = "postgres://postgres:test@localhost:5445/testdb"


async def _drop_schema():
    """Drop and recreate the public schema to ensure a clean DB."""
    await Tortoise.init(
        config={
            "connections": {"default": DATABASE_URL},
            "apps": {"models": {"models": [], "default_connection": "default"}},
        },
    )
    conn = Tortoise.get_connection("default")
    await conn.execute_script("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    await Tortoise.close_connections()


@pytest.fixture(autouse=True)
async def reset_tortoise():
    """Clean Tortoise state and DB between tests."""
    # Clear global ORM state
    await Tortoise._reset_apps()  # noqa: SLF001

    # Drop schema
    await _drop_schema()

    # Clear sys.modules caches of temp models
    for key in list(sys.modules.keys()):
        if key.startswith("models"):
            del sys.modules[key]

    yield

    # Safety close
    await Tortoise.close_connections()
