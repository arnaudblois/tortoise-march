"""Test that the operations written are compacted (ie no useless fields)."""

from tortoisemarch.model_state import FieldState, ModelState
from tortoisemarch.operations import AddField, AlterField, CreateModel


def test_createmodel_codegen_is_compact():
    """Test the compaction for a CreateModel.

    A model snapshot should not dump every possible FieldState
    key with None/False. We only want relevant keys in the
    migration code.
    """
    ms = ModelState(
        name="Book",
        db_table="book",
        field_states={
            "title": FieldState(name="title", field_type="CharField", max_length=200),
            # default is meaningful; keep it
            "published_at": FieldState(
                name="published_at",
                field_type="DatetimeField",
                default="callable",
            ),
        },
    )

    op = CreateModel.from_model_state(ms)
    code = op.to_code()

    # Should include meaningful options
    assert "max_length" in code
    assert "default" in code

    # Should not include noise keys (these come from “full dump” behaviour)
    assert "decimal_places" not in code
    assert "max_digits" not in code
    assert "related_table" not in code
    assert "related_model" not in code
    assert "to_field" not in code
    assert "on_delete" not in code


def test_addfield_codegen_is_compact():
    """Test that AddField is also correctly compacted."""
    op = AddField(
        model_name="Book",
        db_table="book",
        field_name="title",
        field_type="CharField",
        options={
            "null": False,
            "unique": False,
            "index": False,
            "primary_key": False,
            "max_length": 200,
            "db_column": None,
            "max_digits": None,
            "decimal_places": None,
            "related_table": None,
        },
    )
    code = op.to_code()

    assert "max_length" in code
    assert "db_column" not in code
    assert "max_digits" not in code
    assert "decimal_places" not in code
    assert "related_table" not in code


def test_alterfield_codegen_emits_only_changed_keys():
    """Test that AlterField ops are correcty compacted.

    AlterField migrations should only focus on changed keys.
    """
    op = AlterField(
        model_name="Book",
        db_table="book",
        field_name="title",
        old_options={"type": "CharField", "max_length": 200, "null": False},
        new_options={"type": "CharField", "max_length": 300, "null": False},
    )
    code = op.to_code()

    # change is max_length only
    assert "max_length" in code
    assert "300" in code
    assert "null" not in code  # unchanged


def test_alterfield_keeps_false_when_it_is_the_change():
    """Test that changes are not compacted away for AlterField.

    If a change is True -> False (or False -> True), we must keep it.
    Compaction must not erase semantics.
    """
    op = AlterField(
        model_name="Author",
        db_table="author",
        field_name="active",
        old_options={"type": "BooleanField", "default": True},
        new_options={"type": "BooleanField", "default": False},
    )
    code = op.to_code()
    assert "default" in code
    assert "False" in code
