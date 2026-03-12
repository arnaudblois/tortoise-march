"""TortoiseMarch-owned constraint helpers.

We keep these helpers separate from Tortoise ORM's own constraint classes so
projects can declare Postgres features that Tortoise does not model yet while
still letting Tortoise March diff and render them cleanly.
"""

import re
from dataclasses import dataclass
from typing import Any

EXCLUSION_EXPRESSION_PARTS = 2
_BUFFERED_TSTZRANGE_RE = re.compile(
    r"\btstzrange\s*\([^)]*\b(?:interval|[-+])\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class FieldRef:
    """Represent a schema field/column reference inside an exclusion expression.

    We normalize these identifiers to lowercase because the rest of
    TortoiseMarch already treats logical field names and physical column names
    case-insensitively for diffing and replay.
    """

    name: str

    def __post_init__(self) -> None:
        """Normalize and validate the referenced field/column name."""
        normalized = str(self.name).strip().lower()
        if not normalized:
            msg = "FieldRef requires a non-empty field or column name."
            raise ValueError(msg)
        object.__setattr__(self, "name", normalized)

    def to_dict(self) -> dict[str, str]:
        """Serialize the field reference to a stable plain dict."""
        return {"type": "field_ref", "name": self.name}

    def __repr__(self) -> str:
        """Render a constructor-like repr for migration code readability."""
        return f"FieldRef({self.name!r})"


@dataclass(frozen=True)
class RawSQL:
    """Represent verbatim SQL inside an exclusion expression."""

    sql: str

    def __post_init__(self) -> None:
        """Normalize and validate the raw SQL fragment."""
        normalized = str(self.sql).strip()
        if not normalized:
            msg = "RawSQL requires a non-empty SQL fragment."
            raise ValueError(msg)
        object.__setattr__(self, "sql", normalized)

    def to_dict(self) -> dict[str, str]:
        """Serialize the SQL fragment to a stable plain dict."""
        return {"type": "raw_sql", "sql": self.sql}

    def __repr__(self) -> str:
        """Render a constructor-like repr for migration code readability."""
        return f"RawSQL({self.sql!r})"


type ExclusionExpressionNode = FieldRef | RawSQL
type ExclusionExpressionInput = str | FieldRef | RawSQL | dict[str, Any]
type ExclusionExpression = tuple[ExclusionExpressionNode, str]


def normalize_exclusion_expression_node(
    value: ExclusionExpressionInput,
) -> ExclusionExpressionNode:
    """Normalize one exclusion-expression node.

    Strings remain backwards-compatible and are treated as `FieldRef`.
    Structured dict payloads are accepted so migrations/state files can round
    trip without losing the node type.
    """
    if isinstance(value, (FieldRef, RawSQL)):
        return value

    if isinstance(value, str):
        return FieldRef(value)

    if isinstance(value, dict):
        node_type = str(value.get("type") or "").strip().lower()
        if node_type == "field_ref":
            return FieldRef(value.get("name", ""))
        if node_type == "raw_sql":
            return RawSQL(value.get("sql", ""))
        msg = f"Unsupported exclusion expression node type: {value!r}"
        raise ValueError(msg)

    msg = (
        "Exclusion expressions support only strings, FieldRef, or RawSQL nodes. "
        f"Got {value!r}."
    )
    raise ValueError(msg)


def normalize_exclusion_expressions(
    expressions: Any,
    *,
    error_context: str,
) -> tuple[ExclusionExpression, ...]:
    """Normalize exclusion expressions into typed `(node, operator)` pairs."""
    normalized_expressions: list[ExclusionExpression] = []
    for expression in expressions or ():
        if isinstance(expression, dict):
            node = normalize_exclusion_expression_node(expression.get("expression"))
            operator = str(expression.get("operator", "")).strip()
        else:
            if not isinstance(expression, (tuple, list)) or (
                len(expression) != EXCLUSION_EXPRESSION_PARTS
            ):
                msg = (
                    f"{error_context} expressions must be "
                    "(field_or_column, operator) pairs."
                )
                raise ValueError(msg)
            node = normalize_exclusion_expression_node(expression[0])
            operator = str(expression[1]).strip()
        if not operator:
            msg = f"{error_context} expressions cannot contain empty operators."
            raise ValueError(msg)
        normalized_expressions.append((node, operator))

    if not normalized_expressions:
        msg = f"{error_context} requires at least one expression."
        raise ValueError(msg)

    return tuple(normalized_expressions)


def validate_exclusion_expression_immutability(
    expressions: tuple[ExclusionExpression, ...],
) -> None:
    """Reject known PostgreSQL-invalid exclusion index expressions early.

    PostgreSQL implements exclusion constraints via indexes, so every indexed
    expression must be immutable. Buffered `tstzrange(...)` expressions that
    add or subtract intervals from `timestamptz` values fail at migration time
    with `functions in index expression must be marked IMMUTABLE`.

    We reject that pattern here with a clear library-level error so projects do
    not discover it only after generating and running a migration.
    """
    for node, _operator in expressions:
        if not isinstance(node, RawSQL):
            continue
        if _BUFFERED_TSTZRANGE_RE.search(node.sql):
            msg = (
                "Buffered tstzrange(...) exclusion expressions are not supported. "
                "PostgreSQL requires exclusion index expressions to be IMMUTABLE, "
                "and timestamptz +/- interval does not satisfy that requirement. "
                "Store the buffered range in a real column and reference that "
                "column with FieldRef(...) instead."
            )
            raise ValueError(msg)


def exclusion_expressions_to_dict(
    expressions: tuple[ExclusionExpression, ...],
) -> list[dict[str, Any]]:
    """Serialize exclusion expressions into a structured plain-data payload."""
    return [
        {"expression": node.to_dict(), "operator": operator}
        for node, operator in expressions
    ]


@dataclass(frozen=True)
class ExclusionConstraint:
    """Describe a Postgres exclusion constraint for `Meta.tortoisemarch_constraints`.

    Each expression is a `(node, operator)` pair where `node` is either:
    - `FieldRef("field_name")` for normal identifier-style references
    - `RawSQL("...")` for verbatim SQL expressions
    - a plain string, kept as a backwards-compatible alias for `FieldRef`
    """

    expressions: tuple[ExclusionExpression, ...]
    name: str | None = None
    index_type: str = "gist"
    condition: str | None = None

    def __post_init__(self) -> None:
        """Normalize inputs so migration state is deterministic."""
        normalized_expressions = normalize_exclusion_expressions(
            self.expressions,
            error_context="ExclusionConstraint",
        )
        validate_exclusion_expression_immutability(normalized_expressions)

        index_type = str(self.index_type).strip().lower()
        if not index_type:
            msg = "ExclusionConstraint requires an index_type."
            raise ValueError(msg)

        condition = self.condition.strip() if isinstance(self.condition, str) else None
        if isinstance(self.condition, str) and not condition:
            condition = None

        object.__setattr__(self, "expressions", tuple(normalized_expressions))
        object.__setattr__(self, "index_type", index_type)
        object.__setattr__(self, "condition", condition)

    def describe(self) -> dict[str, Any]:
        """Return a normalized payload for extractor compatibility."""
        return {
            "type": "ExclusionConstraint",
            "name": self.name,
            "expressions": exclusion_expressions_to_dict(self.expressions),
            "index_type": self.index_type,
            "condition": self.condition,
        }

    def deconstruct(self) -> tuple[str, list[Any], dict[str, Any]]:
        """Return a migration-friendly constructor payload."""
        return (
            "tortoisemarch.constraints.ExclusionConstraint",
            [],
            {
                "expressions": exclusion_expressions_to_dict(self.expressions),
                "name": self.name,
                "index_type": self.index_type,
                "condition": self.condition,
            },
        )
