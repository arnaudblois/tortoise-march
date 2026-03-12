"""Tests for TortoiseMarch-owned constraint helpers."""

import pytest

from tortoisemarch.constraints import (
    ExclusionConstraint,
    FieldRef,
    RawSQL,
    normalize_exclusion_expression_node,
    normalize_exclusion_expressions,
)


def test_exclusion_constraint_normalizes_strings_to_fieldrefs():
    """Legacy string expressions should remain valid and normalize deterministically."""
    constraint = ExclusionConstraint(
        expressions=(("Room", "="), ("timespan", "&&")),
        index_type="GiSt",
    )

    assert constraint.expressions == (
        (FieldRef("room"), "="),
        (FieldRef("timespan"), "&&"),
    )
    assert constraint.index_type == "gist"


def test_exclusion_constraint_accepts_raw_sql_nodes():
    """RawSQL nodes should be preserved instead of quoted like identifiers."""
    constraint = ExclusionConstraint(
        expressions=(
            (FieldRef("practitioner"), "="),
            (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
        ),
        index_type="gist",
    )

    assert constraint.expressions[1] == (
        RawSQL("tstzrange(start_at, end_at, '[)')"),
        "&&",
    )


def test_exclusion_constraint_rejects_empty_raw_sql():
    """Empty RawSQL fragments should fail fast during normalization."""
    with pytest.raises(ValueError, match="non-empty SQL fragment"):
        ExclusionConstraint(
            expressions=((RawSQL("   "), "&&"),),
            index_type="gist",
        )


def test_expression_node_helpers_round_trip_structured_payloads():
    """Structured payloads from describe/deconstruct should round-trip cleanly."""
    constraint = ExclusionConstraint(
        expressions=(
            (FieldRef("practitioner"), "="),
            (RawSQL("tstzrange(start_at, end_at, '[)')"), "&&"),
        ),
        name="booking_practitioner_window_excl",
        index_type="gist",
        condition="cancelled_at IS NULL",
    )

    described = constraint.describe()
    _, _, kwargs = constraint.deconstruct()

    assert described["expressions"] == kwargs["expressions"]
    assert (
        normalize_exclusion_expressions(
            (
                (entry["expression"], entry["operator"])
                for entry in described["expressions"]
            ),
            error_context="ExclusionConstraint",
        )
        == constraint.expressions
    )


def test_normalize_exclusion_expression_node_accepts_structured_payloads():
    """Node-level coercion should accept the serialized helper payloads."""
    assert normalize_exclusion_expression_node(
        {"type": "field_ref", "name": "practitioner"},
    ) == FieldRef("practitioner")
    assert normalize_exclusion_expression_node(
        {"type": "raw_sql", "sql": "tstzrange(start_at, end_at, '[)')"},
    ) == RawSQL("tstzrange(start_at, end_at, '[)')")
