"""Diff two ProjectState objects and produce schema operations.

State A (from_state): result of applying existing migrations.
State B (to_state):   current models in code.

This differ compares model/field presence and field attributes
(including primary_key, lengths/precision, FK metadata, etc.) and emits
operations to move from A -> B.

Renames are *not* applied automatically. Call `suggest_renames(...)` to get
candidate (old_name -> new_name) pairs, prompt the user, then pass the user-
confirmed mapping to `diff_states(..., rename_map=...)`.

Rules:
- If a confirmed rename has no attribute changes (only the name differs),
  emit a `RenameField`.
- If it has attribute changes too, emit a single `AlterField(..., new_name=...)`
  so the SchemaEditor can perform rename + alter in one logical step.
"""

from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from enum import Enum
from typing import Any

from tortoisemarch.constraints import FieldRef, RawSQL
from tortoisemarch.exceptions import InvalidMigrationError
from tortoisemarch.extensions import PostgresExtension
from tortoisemarch.model_state import (
    ConstraintKind,
    ConstraintState,
    FieldState,
    IndexState,
    ModelState,
    ProjectState,
)
from tortoisemarch.operations import (
    AddConstraint,
    AddExtension,
    AddField,
    AlterField,
    CreateIndex,
    CreateModel,
    Operation,
    RemoveConstraint,
    RemoveExtension,
    RemoveField,
    RemoveIndex,
    RemoveModel,
    RenameConstraint,
    RenameField,
    RenameModel,
    constraint_db_name,
    default_index_name,
)
from tortoisemarch.schema_filtering import FK_TYPES, is_schema_field_type

# ----------------------------- helpers ---------------------------------


@dataclass(frozen=True, slots=True)
class FKDependencyEdge:
    """Represent one FK dependency edge used in cycle diagnostics."""

    src_model: str
    src_table: str
    src_field: str
    dst_model: str
    dst_table: str
    field_type: str


def _options_with_type(fs) -> dict:
    """Return field options plus its abstract type under the key 'type'."""
    opts = dict(fs.options)
    if isinstance(opts.get("default"), Enum):
        opts["default"] = opts["default"].value
    opts.setdefault("type", fs.field_type)
    return opts


def _options_for_alter(fs) -> dict:
    """Return a complete, stable option set for AlterField diffing and rendering.

    Ensures the abstract field type is always present, and rehydrates
    essential implicit defaults (e.g. CharField max_length) so that
    ALTER statements can be generated deterministically.
    """
    opts = dict(fs.options)
    if isinstance(opts.get("default"), Enum):
        opts["default"] = opts["default"].value
    # always include abstract type for AlterField rendering/diffing
    opts["type"] = fs.field_type

    if opts.get("primary_key"):
        # Strip redundant flags for PKs (unique/index implied, null always False)
        for k in ("unique", "index", "null"):
            opts.pop(k, None)

    # For relational fields, fill in implicit defaults so missing keys
    # don't produce spurious alters when comparing to explicit options.
    if fs.field_type in FK_TYPES:
        # Default DB column name follows Tortoise conventions.
        opts["db_column"] = opts.get("db_column") or f"{fs.name}_id"
        opts["to_field"] = opts.get("to_field") or "id"
        # Tortoise defaults to CASCADE if not provided.
        opts["on_delete"] = opts.get("on_delete") or "CASCADE"
        # referenced_type may be None if unresolved; keep it stable.
        opts.setdefault("referenced_type", None)
        if fs.field_type == "OneToOneFieldInstance":
            # One-to-one implies uniqueness even if not explicitly set.
            opts["unique"] = True

    # if max_length was compacted away, rehydrate the default so comparisons
    # and SQL rendering remain deterministic
    if fs.field_type == "CharField":
        ml = opts.get("max_length")
        if ml is None:
            opts["max_length"] = 255

    return opts


def _same_except_name(old_fs, new_fs) -> bool:
    """Check if two FieldStates are identical aside from the name."""
    return (old_fs.field_type == new_fs.field_type) and (
        old_fs.options == new_fs.options
    )


def _base_name(n: str) -> str:
    """Normalize a column/field name for similarity comparison."""
    n = n.lower()
    n = n.removesuffix("_id")
    return "".join(ch if ch.isalnum() else " " for ch in n).strip()


def _name_similarity(a: str, b: str) -> float:
    """Return a name similarity ratio in [0, 1]."""
    return SequenceMatcher(None, _base_name(a), _base_name(b)).ratio()


def _table_similarity(a: str, b: str) -> float:
    """Return a similarity score for DB table names in [0, 1]."""
    return _name_similarity(a, b)


def _is_supported_rename_type_change(old_type: str, new_type: str) -> bool:
    """Return True when a rename candidate uses a supported type transition.

    We keep this aligned with makemigrations alter-validation rules so we do
    not suggest renames that would later be rejected as unsupported alters.
    """
    if old_type == new_type:
        return True

    int_rank = {"SmallIntField": 0, "IntField": 1, "BigIntField": 2}
    if old_type in int_rank and new_type in int_rank:
        return int_rank[new_type] > int_rank[old_type]

    return old_type == "CharField" and new_type == "TextField"


def _schema_fields(ms: ModelState) -> dict:
    """Return schema-relevant FieldStates for a model."""
    return {
        k: v for k, v in ms.field_states.items() if is_schema_field_type(v.field_type)
    }


def _pk_field(ms: ModelState):
    """Return the primary key FieldState for a model, if present."""
    for fs in _schema_fields(ms).values():
        if fs.options.get("primary_key"):
            return fs
    return None


def _model_rename_score(old_ms: ModelState, new_ms: ModelState) -> float:
    """Compute a similarity score indicating if two models are likely the same."""
    old_fields = _schema_fields(old_ms)
    new_fields = _schema_fields(new_ms)

    if not old_fields or not new_fields:
        return 0.0

    # Strict matching on (normalized field key + type) for now.
    matches = 0
    for k, old_fs in old_fields.items():
        new_fs = new_fields.get(k)
        if new_fs and new_fs.field_type == old_fs.field_type:
            matches += 1

    overlap = matches / max(len(old_fields), len(new_fields))

    score = overlap

    # Strong signal: same PK type
    old_pk = _pk_field(old_ms)
    new_pk = _pk_field(new_ms)
    if old_pk and new_pk and old_pk.field_type == new_pk.field_type:
        score += 0.2

    # Mild signal: same number of schema fields
    if len(old_fields) == len(new_fields):
        score += 0.1

    # Table-name similarity helps catch renames even if some fields diverged.
    score += 0.2 * _table_similarity(old_ms.db_table, new_ms.db_table)

    # Cap at 1.0
    return min(score, 1.0)


def _implicit_field_indexes(ms: ModelState) -> set[tuple[Any, ...]]:
    """Return indexes implicitly created by field-level index flags."""
    implicit: set[tuple[Any, ...]] = set()
    for fs in ms.field_states.values():
        opts = fs.options
        if opts.get("index") and not opts.get("unique") and not opts.get("primary_key"):
            cols = _physical_index_columns(ms, (fs.name,))
            implicit.add(IndexState(columns=cols).semantic_key)
    return implicit


def _normalize_index_state(ms: ModelState, index: IndexState) -> IndexState:
    """Return an IndexState using physical DB column names."""
    return IndexState(
        columns=_physical_index_columns(ms, tuple(index.columns)),
        name=index.name,
        unique=index.unique,
        index_type=index.index_type,
        extra=index.extra,
    )


def _model_indexes(ms: ModelState) -> list[IndexState]:
    """Return canonical model-level indexes excluding implicit field indexes."""
    implicit = _implicit_field_indexes(ms)
    indexes = [_normalize_index_state(ms, index) for index in ms.indexes]
    return [index for index in indexes if index.semantic_key not in implicit]


def _normalize_constraint_state(
    ms: ModelState,
    constraint: ConstraintState,
) -> ConstraintState:
    """Return a ConstraintState using physical DB column names where needed."""
    if constraint.kind == ConstraintKind.UNIQUE:
        return ConstraintState(
            kind=ConstraintKind.UNIQUE,
            name=constraint.name,
            columns=_physical_index_columns(ms, tuple(constraint.columns)),
        )
    if constraint.kind == ConstraintKind.EXCLUDE:
        expressions: list[tuple[FieldRef | RawSQL, str]] = []
        for expression, operator in constraint.expressions:
            if isinstance(expression, FieldRef):
                physical_column = _physical_index_columns(ms, (expression.name,))[0]
                expressions.append((FieldRef(physical_column), operator))
            else:
                expressions.append((expression, operator))
        return ConstraintState(
            kind=ConstraintKind.EXCLUDE,
            name=constraint.name,
            expressions=tuple(expressions),
            index_type=constraint.index_type,
            condition=constraint.condition,
        )
    return constraint


def _model_constraints(ms: ModelState) -> list[ConstraintState]:
    """Return canonical model-level constraints."""
    return [
        _normalize_constraint_state(ms, constraint) for constraint in ms.constraints
    ]


def _physical_index_columns(ms: ModelState, cols: tuple[str, ...]) -> tuple[str, ...]:
    """Map logical field names to physical DB column names for indexes."""
    physical: list[str] = []
    for col in cols:
        fs = ms.field_states.get(col.lower())
        if fs:
            if fs.options.get("db_column"):
                physical.append(fs.options["db_column"])
                continue
            if fs.field_type in FK_TYPES or fs.field_type == "OneToOneFieldInstance":
                physical.append(f"{fs.name}_id")
                continue
            physical.append(fs.name)
        else:
            physical.append(col)
    return tuple(physical)


def _field_column_map(ms: ModelState) -> dict[str, str]:
    """Return a stable logical-field to physical-column mapping for one model."""
    mapping: dict[str, str] = {}
    for field_state in _schema_fields(ms).values():
        mapping[field_state.name.lower()] = _physical_index_columns(
            ms,
            (field_state.name,),
        )[0]
    return mapping


def _project_extensions(state: ProjectState) -> dict[str, PostgresExtension]:
    """Return project extensions keyed by normalized name."""
    return {extension.name: extension for extension in state.extensions}


def _rename_pairs_for_model_indexes(
    old_model: ModelState,
    *,
    new_db_table: str,
) -> list[tuple[str, str]]:
    """Return default-name index renames needed after a table rename.

    Postgres keeps existing index names when a table is renamed. We rename only
    default-derived names here so later operations can keep deriving the same
    physical names from the new table name without drifting.
    """
    renames: set[tuple[str, str]] = set()

    for index in _model_indexes(old_model):
        if index.name is not None:
            continue
        old_name = default_index_name(
            old_model.db_table,
            index.columns,
            unique=index.unique,
        )
        new_name = default_index_name(
            new_db_table,
            index.columns,
            unique=index.unique,
        )
        if old_name != new_name:
            renames.add((old_name, new_name))

    for field_state in _schema_fields(old_model).values():
        if (
            not field_state.options.get("index")
            or field_state.options.get("unique")
            or field_state.options.get("primary_key")
        ):
            continue
        columns = _physical_index_columns(old_model, (field_state.name,))
        old_name = default_index_name(old_model.db_table, columns, unique=False)
        new_name = default_index_name(new_db_table, columns, unique=False)
        if old_name != new_name:
            renames.add((old_name, new_name))

    return sorted(renames)


def _rename_pairs_for_field_unique_constraints(
    old_model: ModelState,
    *,
    new_db_table: str,
) -> list[tuple[str, str]]:
    """Return derived field-unique constraint renames needed after a table rename.

    Field-level unique flags are not tracked in `ModelState.constraints`, so we
    handle them alongside the model rename itself.
    """
    renames: set[tuple[str, str]] = set()

    for field_state in _schema_fields(old_model).values():
        is_unique = (
            bool(field_state.options.get("unique"))
            or field_state.field_type == "OneToOneFieldInstance"
        )
        if not is_unique or field_state.options.get("primary_key"):
            continue

        columns = _physical_index_columns(old_model, (field_state.name,))
        constraint = ConstraintState(kind=ConstraintKind.UNIQUE, columns=columns)
        old_name = constraint_db_name(old_model.db_table, constraint)
        new_name = constraint_db_name(new_db_table, constraint)
        if old_name != new_name:
            renames.add((old_name, new_name))

    return sorted(renames)


def _detect_model_renames(  # noqa: C901
    removed: dict[str, ModelState],
    added: dict[str, ModelState],
    *,
    threshold: float = 0.90,
) -> list[tuple[str, str]]:
    """Return (old_name, new_name) pairs that look like model renames.

    We compute best match each way, then keep only mutual-best pairs
    above threshold.
    """
    best_for_old: dict[str, tuple[str, float]] = {}
    best_for_new: dict[str, tuple[str, float]] = {}

    for old_name, old_ms in removed.items():
        best = ("", 0.0)
        for new_name, new_ms in added.items():
            s = _model_rename_score(old_ms, new_ms)
            if s > best[1]:
                best = (new_name, s)
        if best[0]:
            best_for_old[old_name] = best

    for new_name, new_ms in added.items():
        best = ("", 0.0)
        for old_name, old_ms in removed.items():
            s = _model_rename_score(old_ms, new_ms)
            if s > best[1]:
                best = (old_name, s)
        if best[0]:
            best_for_new[new_name] = best

    pairs: list[tuple[str, str]] = []
    for old_name, (new_name, s) in best_for_old.items():
        if s < threshold:
            continue
        back = best_for_new.get(new_name)
        if back and back[0] == old_name and back[1] >= threshold:
            pairs.append((old_name, new_name))

    # Stable order for deterministic migrations
    pairs.sort(key=lambda t: (t[0].lower(), t[1].lower()))
    return pairs


def score_candidate(old_name: str, old_fs, new_name: str, new_fs) -> float:
    """Score how likely (old_name -> new_name) is a rename."""
    if not _is_supported_rename_type_change(old_fs.field_type, new_fs.field_type):
        return -1.0

    old_opts = _options_with_type(old_fs)
    new_opts = _options_with_type(new_fs)

    # Short-circuit: very strong signals
    if old_opts.get("db_column") and old_opts["db_column"] == new_opts.get("db_column"):
        return 100.0

    score = 50.0 * _name_similarity(old_name, new_name)

    if (
        old_fs.field_type in FK_TYPES
        and new_fs.field_type in FK_TYPES
        and old_opts.get("related_table") == new_opts.get("related_table")
    ):
        score += 20.0

    if old_opts.get("primary_key") == new_opts.get("primary_key"):
        score += 10.0

    score += 5.0 * sum(
        int(old_opts.get(k) == new_opts.get(k)) for k in ("null", "unique", "index")
    )

    if old_fs.field_type == "CharField":
        ol, nl = old_opts.get("max_length"), new_opts.get("max_length")
        if ol is not None and nl is not None and ol == nl:
            score += 10.0
    elif (
        old_fs.field_type == "DecimalField"
        and old_opts.get("max_digits") == new_opts.get("max_digits")
        and old_opts.get(
            "decimal_places",
        )
        == new_opts.get("decimal_places")
    ):
        score += 10.0

    # Small hints: name matches db_column on either side
    if old_opts.get("db_column") == new_name:
        score += 4.0
    if new_opts.get("db_column") == old_name:
        score += 4.0

    return score


def _build_fk_dependency_graph(
    model_states: dict[str, ModelState],
) -> tuple[dict[str, set[str]], dict[tuple[str, str], list[FKDependencyEdge]]]:
    """Build FK dependencies and edge metadata used for cycle diagnostics."""
    table_to_model = {ms.db_table.lower(): name for name, ms in model_states.items()}

    deps: dict[str, set[str]] = {name: set() for name in model_states}
    edges_by_pair: dict[tuple[str, str], list[FKDependencyEdge]] = defaultdict(list)

    for src_model, ms in model_states.items():
        for field_state in ms.field_states.values():
            if field_state.field_type not in FK_TYPES:
                continue
            related_table = (field_state.options.get("related_table") or "").lower()
            if related_table not in table_to_model:
                continue
            dst_model = table_to_model[related_table]
            if dst_model == src_model:
                # We keep self-FK edges out of ordering so self-referential
                # models remain creatable in a single CreateModel operation.
                continue
            deps[src_model].add(dst_model)
            edges_by_pair[(src_model, dst_model)].append(
                FKDependencyEdge(
                    src_model=src_model,
                    src_table=ms.db_table,
                    src_field=field_state.name,
                    dst_model=dst_model,
                    dst_table=model_states[dst_model].db_table,
                    field_type=field_state.field_type,
                ),
            )

    for edge_list in edges_by_pair.values():
        edge_list.sort(key=lambda edge: (edge.src_field.casefold(), edge.field_type))

    return deps, dict(edges_by_pair)


def _strongly_connected_components(
    deps: dict[str, set[str]],
    nodes: set[str],
) -> list[set[str]]:
    """Return SCCs for `nodes` in stable order using Tarjan's algorithm."""
    index = 0
    stack: list[str] = []
    on_stack: set[str] = set()
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    components: list[set[str]] = []

    def strongconnect(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for neighbor in sorted(deps.get(node, set()) & nodes, key=str.casefold):
            if neighbor not in indices:
                strongconnect(neighbor)
                lowlinks[node] = min(lowlinks[node], lowlinks[neighbor])
            elif neighbor in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[neighbor])

        if lowlinks[node] != indices[node]:
            return

        component: set[str] = set()
        while stack:
            member = stack.pop()
            on_stack.remove(member)
            component.add(member)
            if member == node:
                break
        components.append(component)

    for node in sorted(nodes, key=str.casefold):
        if node not in indices:
            strongconnect(node)

    components.sort(key=lambda component: min(component, key=str.casefold).casefold())
    return components


def _cycle_components(deps: dict[str, set[str]], nodes: set[str]) -> list[set[str]]:
    """Return SCCs that represent real cycles."""
    cycle_components: list[set[str]] = []
    for component in _strongly_connected_components(deps, nodes):
        if len(component) > 1:
            cycle_components.append(component)
            continue
        model_name = next(iter(component))
        if model_name in deps.get(model_name, set()):
            cycle_components.append(component)
    return cycle_components


def _witness_cycle_for_component(
    deps: dict[str, set[str]],
    component: set[str],
) -> list[str]:
    """Build one deterministic witness cycle path for an SCC."""
    start = min(component, key=str.casefold)
    path: list[str] = [start]

    def dfs(node: str) -> bool:
        neighbors = sorted(deps.get(node, set()) & component, key=str.casefold)
        for neighbor in neighbors:
            if neighbor == start and len(path) > 1:
                path.append(start)
                return True
            if neighbor in path:
                continue
            path.append(neighbor)
            if dfs(neighbor):
                return True
            path.pop()
        return False

    if dfs(start):
        return path

    # We should not hit this fallback for SCCs, but we keep it as a stable
    # guardrail to avoid raising a secondary internal error.
    if start in deps.get(start, set()):
        return [start, start]
    return [start, start]


def _witness_cycle_fields(
    cycle_path: list[str],
    edges_by_pair: dict[tuple[str, str], list[FKDependencyEdge]],
) -> str:
    """Render a cycle witness path using concrete FK field names."""
    field_hops: list[str] = []
    for index in range(len(cycle_path) - 1):
        src_model = cycle_path[index]
        dst_model = cycle_path[index + 1]
        edges = edges_by_pair.get((src_model, dst_model), [])
        if edges:
            edge = edges[0]
            field_hops.append(f"{edge.src_model}.{edge.src_field}")
            continue
        field_hops.append(src_model)

    field_hops.append(cycle_path[-1])
    return " -> ".join(field_hops)


def _toposort_models_by_fk(model_states: dict[str, ModelState]) -> list[str]:
    """Return model names ordered by foreign key dependencies.

    Models are sorted so that tables referenced by foreign keys are created
    before the tables that depend on them. Cycles are detected and rejected.
    """
    deps, edges_by_pair = _build_fk_dependency_graph(model_states)
    remaining_deps = {name: set(values) for name, values in deps.items()}

    # Kahn's algorithm for topological sorting
    ready = sorted([name for name, values in remaining_deps.items() if not values])
    out: list[str] = []
    while ready:
        model_name = ready.pop(0)
        out.append(model_name)
        for candidate in list(remaining_deps.keys()):
            if model_name in remaining_deps[candidate]:
                remaining_deps[candidate].remove(model_name)
                if (
                    not remaining_deps[candidate]
                    and candidate not in out
                    and candidate not in ready
                ):
                    ready.append(candidate)
                    ready.sort()

    if len(out) != len(model_states):
        unresolved = {
            model_name for model_name, values in remaining_deps.items() if values
        }
        components = _cycle_components(remaining_deps, unresolved)
        cycles = [
            _witness_cycle_for_component(remaining_deps, component)
            for component in components
        ]
        cycle_nodes = {node for cycle in cycles for node in cycle[:-1]}
        blocked_models = sorted(unresolved - cycle_nodes, key=str.casefold)

        cycle_label = "cycle" if len(cycles) == 1 else "cycles"
        lines = [
            f"CreateModel dependency cycle detected ({len(cycles)} {cycle_label}).",
        ]
        for idx, cycle in enumerate(cycles, start=1):
            lines.append(f"Cycle {idx} (models): {' -> '.join(cycle)}")
            lines.append(
                f"Cycle {idx} (fields): {_witness_cycle_fields(cycle, edges_by_pair)}",
            )
        if blocked_models:
            lines.append(
                f"Blocked models (depend on cycles): {', '.join(blocked_models)}",
            )
        lines.append(
            "Either break the cycle (store FK on one side only) "
            "or implement a 2-phase FK add.",
        )
        msg = "\n".join(lines)
        raise InvalidMigrationError(msg)

    return out


# ----------------------------- Diff States ---------------------------------
def _resolved_db_column(fs: FieldState) -> str | None:
    """Return the physical DB column name for a field, if derivable."""
    col = fs.options.get("db_column")
    if col:
        return col
    if fs.field_type in FK_TYPES:
        return f"{fs.name}_id"
    return None


def _rename_or_alter_field_op(  # noqa: PLR0913
    *,
    model_name: str,
    db_table: str,
    old_name: str,
    new_name: str,
    old_fs: FieldState,
    new_fs: FieldState,
) -> Operation:
    """Return a RenameField or AlterField op depending on attribute changes."""
    if _same_except_name(old_fs, new_fs):
        old_col = _resolved_db_column(old_fs)
        new_col = _resolved_db_column(new_fs)
        old_db_column = old_col if old_col and old_col != old_name else None
        new_db_column = new_col if new_col and new_col != new_name else None
        return RenameField(
            model_name=model_name,
            db_table=db_table,
            old_name=old_name,
            new_name=new_name,
            old_db_column=old_db_column,
            new_db_column=new_db_column,
        )
    return AlterField(
        model_name=model_name,
        db_table=db_table,
        field_name=old_name,
        old_options=_options_for_alter(old_fs),
        new_options=_options_for_alter(new_fs),
        new_name=new_name,
    )


def _diff_model_fields(
    ops: list[Operation],
    *,
    old_model: ModelState,
    new_model: ModelState,
    model_name: str,
    rename_map: dict[str, str] | None,
) -> None:
    """Append field operations to transform old_model -> new_model."""
    rename_map = rename_map or {}

    old_fields = _schema_fields(old_model)
    new_fields = _schema_fields(new_model)

    removed_names = set(old_fields.keys() - new_fields.keys())
    added_names = set(new_fields.keys() - old_fields.keys())

    # Apply user-confirmed renames first
    for old_name, new_name in list(rename_map.items()):
        if old_name in removed_names and new_name in added_names:
            old_fs = old_fields[old_name]
            new_fs = new_fields[new_name]
            ops.append(
                _rename_or_alter_field_op(
                    model_name=model_name,
                    db_table=new_model.db_table,
                    old_name=old_name,
                    new_name=new_name,
                    old_fs=old_fs,
                    new_fs=new_fs,
                ),
            )

            removed_names.remove(old_name)
            added_names.remove(new_name)
        # else: ignore invalid pairs silently

    # Remaining removed -> RemoveField
    for fname in sorted(removed_names):
        fs = old_fields[fname]
        col = _resolved_db_column(fs)
        db_column = col if col and col != fs.name else None
        ops.append(
            RemoveField(
                model_name=model_name,
                db_table=new_model.db_table,
                field_name=fname,
                db_column=db_column,
            ),
        )

    # Remaining added -> AddField
    for fname in sorted(added_names):
        fs = new_fields[fname]
        ops.append(
            AddField(
                model_name=model_name,
                db_table=new_model.db_table,
                field_name=fs.name,
                field_type=fs.field_type,
                options=fs.options,
            ),
        )

    # Unchanged names but modified attributes -> AlterField
    for fname in sorted(old_fields.keys() & new_fields.keys()):
        old_fs = old_fields[fname]
        new_fs = new_fields[fname]

        old_opts = _options_for_alter(old_fs)
        new_opts = _options_for_alter(new_fs)

        if old_opts != new_opts:
            ops.append(
                AlterField(
                    model_name=model_name,
                    db_table=new_model.db_table,
                    field_name=fname,
                    old_options=old_opts,
                    new_options=new_opts,
                ),
            )


def _diff_model_indexes(
    ops: list[Operation],
    *,
    old_model: ModelState,
    new_model: ModelState,
) -> None:
    """Append index operations to transform old_model indexes -> new_model indexes."""
    old_indexes = {index.semantic_key: index for index in _model_indexes(old_model)}
    new_indexes = {index.semantic_key: index for index in _model_indexes(new_model)}

    removed = [
        old_indexes[key] for key in sorted(old_indexes.keys() - new_indexes.keys())
    ]
    added = [
        new_indexes[key] for key in sorted(new_indexes.keys() - old_indexes.keys())
    ]

    ops.extend(
        CreateIndex(
            model_name=new_model.name,
            db_table=new_model.db_table,
            columns=index.columns,
            unique=index.unique,
            name=index.name,
        )
        for index in added
    )

    ops.extend(
        RemoveIndex(
            model_name=old_model.name,
            db_table=old_model.db_table,
            name=(
                index.name
                or default_index_name(
                    old_model.db_table,
                    index.columns,
                    unique=index.unique,
                )
            ),
            columns=index.columns,
            unique=index.unique,
        )
        for index in removed
    )


def _diff_model_constraints(
    ops: list[Operation],
    *,
    old_model: ModelState,
    new_model: ModelState,
    model_name: str,
) -> None:
    """Append constraint operations to transform old_model constraints -> new_model."""
    old_groups: dict[tuple[Any, ...], list[tuple[ConstraintState, ConstraintState]]] = (
        defaultdict(list)
    )
    new_groups: dict[tuple[Any, ...], list[tuple[ConstraintState, ConstraintState]]] = (
        defaultdict(list)
    )

    for constraint in old_model.constraints:
        normalized = _normalize_constraint_state(old_model, constraint)
        old_groups[normalized.semantic_key].append((normalized, constraint))
    for constraint in new_model.constraints:
        normalized = _normalize_constraint_state(new_model, constraint)
        new_groups[normalized.semantic_key].append((normalized, constraint))

    for group in old_groups.values():
        group.sort(key=lambda value: constraint_db_name(old_model.db_table, value[0]))
    for group in new_groups.values():
        group.sort(key=lambda value: constraint_db_name(new_model.db_table, value[0]))

    all_keys = sorted(set(old_groups) | set(new_groups))
    for key in all_keys:
        old_group = old_groups.get(key, [])
        new_group = new_groups.get(key, [])
        shared = min(len(old_group), len(new_group))

        for index in range(shared):
            old_normalized, old_constraint = old_group[index]
            new_normalized, new_constraint = new_group[index]
            old_name = constraint_db_name(old_model.db_table, old_normalized)
            new_name = constraint_db_name(new_model.db_table, new_normalized)
            if old_name != new_name:
                ops.append(
                    RenameConstraint(
                        model_name=model_name,
                        db_table=new_model.db_table,
                        old_name=old_name,
                        new_name=new_name,
                        old_constraint=old_constraint,
                        new_constraint=new_constraint,
                    ),
                )

        ops.extend(
            RemoveConstraint(
                model_name=model_name,
                db_table=new_model.db_table,
                constraint=constraint,
                name=constraint_db_name(old_model.db_table, normalized_constraint),
                field_column_map=_field_column_map(old_model),
            )
            for normalized_constraint, constraint in old_group[shared:]
        )

        ops.extend(
            AddConstraint(
                model_name=model_name,
                db_table=new_model.db_table,
                constraint=constraint,
                name=constraint_db_name(new_model.db_table, normalized_constraint),
                field_column_map=_field_column_map(new_model),
            )
            for normalized_constraint, constraint in new_group[shared:]
        )


# ----------------------------- Diff States ---------------------------------


def diff_states(
    from_state: ProjectState,
    to_state: ProjectState,
    *,
    rename_map: dict[str, dict[str, str]] | None = None,
) -> list[Operation]:
    """Compute operations to migrate from `from_state` to `to_state`."""
    ops: list[Operation] = []
    rename_map = rename_map or {}

    from_models = from_state.model_states
    to_models = to_state.model_states
    removed_extensions: list[PostgresExtension] = []

    old_extensions = _project_extensions(from_state)
    new_extensions = _project_extensions(to_state)
    ops.extend(
        AddExtension(extension=new_extensions[extension_name])
        for extension_name in sorted(new_extensions.keys() - old_extensions.keys())
    )
    removed_extensions.extend(
        old_extensions[extension_name]
        for extension_name in sorted(old_extensions.keys() - new_extensions.keys())
    )

    # ---- Identify removed/added models first
    removed: dict[str, ModelState] = {
        name: from_models[name] for name in (from_models.keys() - to_models.keys())
    }
    added: dict[str, ModelState] = {
        name: to_models[name] for name in (to_models.keys() - from_models.keys())
    }

    # ---- Detect model renames and emit RenameModel first
    model_renames = _detect_model_renames(removed, added)

    for old_name, new_name in model_renames:
        old_ms = removed.pop(old_name)
        new_ms = added.pop(new_name)
        ops.append(
            RenameModel(
                old_name=old_name,
                new_name=new_name,
                old_db_table=old_ms.db_table,
                new_db_table=new_ms.db_table,
                index_renames=_rename_pairs_for_model_indexes(
                    old_ms,
                    new_db_table=new_ms.db_table,
                ),
                constraint_renames=_rename_pairs_for_field_unique_constraints(
                    old_ms,
                    new_db_table=new_ms.db_table,
                ),
            ),
        )

    # ---- Models removed (after removing rename-pairs)
    ordered_removed_names = _toposort_models_by_fk(removed)
    for model_name in reversed(ordered_removed_names):
        old_model = removed[model_name]
        ops.append(RemoveModel(name=model_name, db_table=old_model.db_table))

    # ---- Models added (after removing rename-pairs)
    ordered_added_names = _toposort_models_by_fk(added)
    for name in ordered_added_names:
        new_model = added[name]
        ops.append(CreateModel.from_model_state(new_model))
        ops.extend(
            CreateIndex(
                model_name=new_model.name,
                db_table=new_model.db_table,
                columns=index.columns,
                unique=index.unique,
                name=index.name,
            )
            for index in sorted(
                _model_indexes(new_model),
                key=lambda value: (
                    value.columns,
                    value.unique,
                    value.name or "",
                    value.index_type,
                    value.extra,
                ),
            )
        )
        _diff_model_constraints(
            ops,
            old_model=ModelState(
                name=new_model.name,
                db_table=new_model.db_table,
                field_states={},
            ),
            new_model=new_model,
            model_name=new_model.name,
        )

    # ---- Models changed (same name in both)
    for model_name in sorted(from_models.keys() & to_models.keys()):
        _diff_model_fields(
            ops,
            old_model=from_models[model_name],
            new_model=to_models[model_name],
            model_name=model_name,
            rename_map=rename_map.get(model_name),
        )
        _diff_model_indexes(
            ops,
            old_model=from_models[model_name],
            new_model=to_models[model_name],
        )
        _diff_model_constraints(
            ops,
            old_model=from_models[model_name],
            new_model=to_models[model_name],
            model_name=model_name,
        )

    # ---- Renamed models may also have field changes
    for old_name, new_name in model_renames:
        old_model = from_models[old_name]
        new_model = to_models[new_name]

        # Allow user to key rename_map by either old or new model name.
        confirmed = rename_map.get(new_name) or rename_map.get(old_name) or {}
        _diff_model_fields(
            ops,
            old_model=old_model,
            new_model=new_model,
            model_name=new_name,
            rename_map=confirmed,
        )
        _diff_model_indexes(
            ops,
            old_model=old_model,
            new_model=new_model,
        )
        _diff_model_constraints(
            ops,
            old_model=old_model,
            new_model=new_model,
            model_name=new_name,
        )

    ops.extend(RemoveExtension(extension=extension) for extension in removed_extensions)

    return ops
