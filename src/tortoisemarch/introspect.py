"""Introspect the current state of a PostgreSQL database schema.

This module is primarily used for bootstrapping or verifying migration state.
It reads the database schema and reconstructs an in-memory ProjectState,
allowing comparisons with the model definitions in code.

Typical use cases:
- Initial migration generation for an existing database
- Schema drift detection between database and model code
- Manual sanity checks in development or CI

This is not required for normal migration diffing (which uses migration history),
but serves as a useful auxiliary tool.
"""

import asyncpg

from tortoisemarch.model_state import FieldState, ModelState, ProjectState


async def introspect_database(conn: asyncpg.Connection) -> ProjectState:
    """Inspect the current database schema.

    Returns a ProjectState object representing all user-defined
    tables and their fields.

    :param conn: An active asyncpg connection to the database.
    :return: ProjectState instance reconstructed from live database.
    """
    tables = await conn.fetch(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
    """,
    )

    model_states = {}

    for record in tables:
        table_name = record["table_name"]
        fields = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = $1;
        """,
            table_name,
        )

        field_states = {}
        for field in fields:
            name = field["column_name"]
            field_type = _infer_field_type(field["data_type"])
            field_states[name.lower()] = FieldState(
                name=field["column_name"],
                field_type=field_type,
                null=field["is_nullable"] == "YES",
                default=_parse_default(field["column_default"]),
                unique=False,  # Not handled yet
                index=False,  # Not handled yet
            )

        model_name = table_name.capitalize()  # crude heuristic
        model_states[model_name] = ModelState(
            db_table=table_name,
            name=model_name,
            field_states=field_states,
        )

    return ProjectState(model_states=model_states)


def _infer_field_type(data_type: str) -> str:
    """Map a PostgreSQL data type to a FieldState field_type string.

    :param data_type: PostgreSQL column type
    :return: Abstract field type string (e.g., 'IntField', 'CharField')
    """
    return {
        "integer": "IntField",
        "character varying": "CharField",
        "boolean": "BooleanField",
        "double precision": "FloatField",
        "text": "CharField",
    }.get(data_type, "CharField")


def _parse_default(default: str | None) -> str | None:
    """Normalize PostgreSQL default expressions into field default values.

    :param default: Default value expression from information_schema
    :return: Simplified default value or None
    """
    if default is None:
        return None
    if default.startswith("nextval("):  # auto-increment / serial
        return None
    return default.strip("'")
