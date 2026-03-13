"""
Schema-based validator for ES|QL ASTs.

Usage::

    from pyesql import parse
    from pyesql.schema import Schema
    from pyesql.validator import SchemaValidator, SchemaValidationError

    schema = Schema.from_dict({
        "process.pid": "integer",
        "process.name": "keyword",
        "host.name": "keyword",
    })

    validator = SchemaValidator(schema)

    # Returns list of ValidationIssue; raises SchemaValidationError on errors.
    issues = validator.validate(parse("FROM logs | WHERE process.pid == 1"))

    # Or pass schema directly to parse():
    from pyesql import parse
    query = parse("FROM logs | WHERE process.pid == 1", schema=schema)

Strictness levels
-----------------
``"error"``  – collect the issue and raise :class:`SchemaValidationError` at the
               end of validation (default when a schema is supplied).
``"warn"``   – emit a :class:`SchemaValidationWarning` via :mod:`warnings`.
``"silent"`` – ignore the issue entirely (default when no schema is supplied).

Computed fields
---------------
Fields introduced by pipeline commands are automatically excluded from schema
checks because they don't originate from the source index:

- ``EVAL`` / ``STATS`` / ``INLINESTATS`` — assigned field names
- ``RENAME`` — the new name after ``AS``
- ``DISSECT`` — ``%{field}`` and ``%{+field}`` capture names
- ``GROK`` — both ``%{PATTERN:field}`` and ``(?<field>...)`` named-capture styles
- ``COMPLETION`` — the target field (e.g. ``COMPLETION result = prompt WITH {...}``)
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Literal

from .ast import (
    BooleanLiteral,
    Comparison,
    CompletionCommand,
    DecimalLiteral,
    DissectCommand,
    DropCommand,
    EvalCommand,
    FieldRef,
    GrokCommand,
    InlineStatsCommand,
    InList,
    IntegerLiteral,
    IsNull,
    KeepCommand,
    LikeExpr,
    MatchExpr,
    Node,
    NullLiteral,
    QualifiedNamePattern,
    Query,
    RenameCommand,
    RlikeExpr,
    StatsCommand,
    StringLiteral,
)
from .errors import EsqlError
from .schema import (
    BOOLEAN_TYPES,
    DATE_TYPES,
    NUMERIC_TYPES,
    STRING_LIKE_TYPES,
    Schema,
)
from .visitor import Visitor

Strictness = Literal["silent", "warn", "error"]


# ---------------------------------------------------------------------------
# Pipeline field tracker
# ---------------------------------------------------------------------------


def collect_computed_fields(query: Query) -> frozenset[str]:
    """
    Return the set of field names *introduced* by pipeline commands (EVAL,
    STATS, INLINESTATS, RENAME, DISSECT, GROK).

    These fields do not originate from the source index schema and should not
    be checked for schema existence.
    """
    computed: set[str] = set()
    for cmd in query.pipes:
        if isinstance(cmd, EvalCommand):
            for f in cmd.fields:
                if f.name is not None:
                    computed.add(str(f.name))
        elif isinstance(cmd, (StatsCommand, InlineStatsCommand)):
            for agg in cmd.stats:
                if agg.field.name is not None:
                    computed.add(str(agg.field.name))
        elif isinstance(cmd, RenameCommand):
            for clause in cmd.clauses:
                computed.add(str(clause.new_name))
        elif isinstance(cmd, DissectCommand):
            # Extract field names from %{field_name} and %{+field_name} tokens
            import re

            for name in re.findall(r"%\{[+?]?([^}]+)\}", cmd.pattern):
                computed.add(name.split("->")[0].strip())
        elif isinstance(cmd, GrokCommand):
            import re

            for pattern in cmd.patterns:
                # %{PATTERN:field_name} style
                for name in re.findall(r"%\{[^:}]+:([^}]+)\}", pattern):
                    computed.add(name)
                # (?<field_name>...) named capture style
                for name in re.findall(r"\(\?<([^>]+)>", pattern):
                    computed.add(name)
        elif isinstance(cmd, CompletionCommand):
            if cmd.target is not None:
                computed.add(str(cmd.target))
    return frozenset(computed)


# ---------------------------------------------------------------------------
# Issue dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ValidationIssue:
    """A single schema validation finding."""

    message: str
    field: str | None = None
    node: Node | None = dc_field(default=None, repr=False)

    def __str__(self) -> str:
        if self.field:
            return f"{self.message} (field: {self.field!r})"
        return self.message


@dataclass
class _ErrorIssue(ValidationIssue):
    pass


@dataclass
class _WarnIssue(ValidationIssue):
    pass


# ---------------------------------------------------------------------------
# Exceptions / warnings
# ---------------------------------------------------------------------------


class SchemaValidationError(EsqlError):
    """Raised when schema validation produces one or more errors."""

    def __init__(self, issues: list[ValidationIssue]) -> None:
        self.issues = issues
        lines = "\n".join(f"  - {i}" for i in issues)
        super().__init__(f"Schema validation failed with {len(issues)} error(s):\n{lines}")


class SchemaValidationWarning(UserWarning):
    """Emitted when schema validation produces a warning-level issue."""


# ---------------------------------------------------------------------------
# Type-compatibility helpers
# ---------------------------------------------------------------------------


def _is_literal(node: Node) -> bool:
    return isinstance(
        node, (NullLiteral, BooleanLiteral, IntegerLiteral, DecimalLiteral, StringLiteral)
    )


def _literal_compatible(field_type: str, literal: Node) -> bool:
    """Return ``True`` if *literal* is type-compatible with *field_type*."""
    if isinstance(literal, NullLiteral):
        return True  # NULL is valid for any field
    if isinstance(literal, BooleanLiteral):
        return field_type in BOOLEAN_TYPES
    if isinstance(literal, IntegerLiteral):
        # Duration literals (1d, 2h, …) are valid in date arithmetic
        if literal.unit is not None:
            return field_type in DATE_TYPES | NUMERIC_TYPES
        return field_type in NUMERIC_TYPES
    if isinstance(literal, DecimalLiteral):
        return field_type in NUMERIC_TYPES
    if isinstance(literal, StringLiteral):
        return field_type in STRING_LIKE_TYPES | DATE_TYPES
    # Arrays, parameters, function calls — skip type check
    return True


def _literal_kind(literal: Node) -> str:
    """Human-readable name for a literal node (for error messages)."""
    return type(literal).__name__.replace("Literal", "").lower()


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class SchemaValidator(Visitor):
    """
    Walks an ES|QL AST and checks all field references against a :class:`Schema`.

    Parameters
    ----------
    schema:
        The field schema to validate against.
    on_unknown:
        Behaviour when a referenced field is not present in the schema.
    on_type_mismatch:
        Behaviour when a literal value's type is incompatible with the
        schema-declared field type.
    """

    def __init__(
        self,
        schema: Schema,
        on_unknown: Strictness = "error",
        on_type_mismatch: Strictness = "error",
    ) -> None:
        self._schema = schema
        self._on_unknown = on_unknown
        self._on_type_mismatch = on_type_mismatch
        self._issues: list[ValidationIssue] = []
        self._computed: frozenset[str] = frozenset()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, node: Node) -> list[ValidationIssue]:
        """
        Walk *node* and return all :class:`ValidationIssue` objects found.

        - ``"warn"``-level issues are emitted via :mod:`warnings`.
        - ``"error"``-level issues cause :class:`SchemaValidationError` to be
          raised after the full tree has been walked (so all errors are
          collected at once).

        Returns the full issue list (warnings + errors) when no errors are
        raised (i.e. when all active checks are at ``"warn"`` or ``"silent"``
        level).
        """
        self._issues = []
        self._computed = collect_computed_fields(node) if isinstance(node, Query) else frozenset()
        self.visit(node)

        warn_issues = [i for i in self._issues if isinstance(i, _WarnIssue)]
        error_issues = [i for i in self._issues if isinstance(i, _ErrorIssue)]

        for w in warn_issues:
            warnings.warn(str(w), SchemaValidationWarning, stacklevel=2)

        if error_issues:
            raise SchemaValidationError(error_issues)

        return list(self._issues)

    # ------------------------------------------------------------------
    # Visitor overrides — expressions
    # ------------------------------------------------------------------

    def visit_FieldRef(self, node: FieldRef) -> None:
        """Validate that the referenced field exists in the schema."""
        field_path = str(node.name)
        if "*" in field_path:
            return  # wildcard refs — skip
        if field_path in self._computed:
            return  # introduced by EVAL/STATS/RENAME/etc. — skip
        if self._schema.get_field_type(field_path) is None:
            self._report(
                self._on_unknown,
                f"Unknown field '{field_path}'",
                field=field_path,
                node=node,
            )
        self.generic_visit(node)

    def visit_Comparison(self, node: Comparison) -> None:
        """Type-check literal sides of comparisons."""
        left, right = node.left, node.right
        if isinstance(left, FieldRef) and _is_literal(right):
            self._check_type_compat(left, right)
        elif isinstance(right, FieldRef) and _is_literal(left):
            self._check_type_compat(right, left)
        self.generic_visit(node)

    def visit_InList(self, node: InList) -> None:
        """Type-check each value in an IN (...) list."""
        if isinstance(node.expr, FieldRef):
            for value in node.values:
                if _is_literal(value):
                    self._check_type_compat(node.expr, value)
        self.generic_visit(node)

    def visit_LikeExpr(self, node: LikeExpr) -> None:
        """Warn when LIKE / NOT LIKE is applied to a non-string field."""
        if isinstance(node.expr, FieldRef):
            self._check_string_context(node.expr, "LIKE")
        self.generic_visit(node)

    def visit_RlikeExpr(self, node: RlikeExpr) -> None:
        """Warn when RLIKE / NOT RLIKE is applied to a non-string field."""
        if isinstance(node.expr, FieldRef):
            self._check_string_context(node.expr, "RLIKE")
        self.generic_visit(node)

    def visit_MatchExpr(self, node: MatchExpr) -> None:
        """Validate the field referenced in a MATCH expression."""
        field_path = str(node.field)
        if self._schema.get_field_type(field_path) is None:
            self._report(
                self._on_unknown,
                f"Unknown field '{field_path}'",
                field=field_path,
                node=node,
            )
        # value is a node; visit it for any nested field refs
        self.visit(node.value)

    def visit_IsNull(self, node: IsNull) -> None:
        """IS NULL / IS NOT NULL — field existence only; type is irrelevant."""
        self.generic_visit(node)

    # ------------------------------------------------------------------
    # Visitor overrides — structural commands
    # ------------------------------------------------------------------

    def visit_KeepCommand(self, node: KeepCommand) -> None:
        """Validate exact (non-wildcard) field patterns in KEEP."""
        for pattern in node.patterns:
            self._validate_pattern(pattern)

    def visit_DropCommand(self, node: DropCommand) -> None:
        """Validate exact (non-wildcard) field patterns in DROP."""
        for pattern in node.patterns:
            self._validate_pattern(pattern)

    def visit_RenameCommand(self, node: RenameCommand) -> None:
        """Validate the *source* field in each RENAME clause."""
        for clause in node.clauses:
            self._validate_pattern(clause.old_name)
        # new names are freshly created — no schema check

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _validate_pattern(self, pattern: QualifiedNamePattern) -> None:
        """Check existence for an exact-match QualifiedNamePattern."""
        if any("*" in part for part in pattern.parts):
            return  # wildcard — caller decides
        field_path = str(pattern)
        if field_path in self._computed:
            return  # computed field — skip
        if self._schema.get_field_type(field_path) is None:
            self._report(
                self._on_unknown,
                f"Unknown field '{field_path}'",
                field=field_path,
                node=pattern,
            )

    def _check_type_compat(self, field_ref: FieldRef, literal: Node) -> None:
        field_path = str(field_ref.name)
        field_type = self._schema.get_field_type(field_path)
        if field_type is None:
            return  # already flagged as unknown; skip type check
        if not _literal_compatible(field_type, literal):
            kind = _literal_kind(literal)
            self._report(
                self._on_type_mismatch,
                f"Type mismatch: field '{field_path}' is {field_type!r} "
                f"but compared to a {kind} literal",
                field=field_path,
                node=literal,
            )

    def _check_string_context(self, field_ref: FieldRef, op: str) -> None:
        field_path = str(field_ref.name)
        field_type = self._schema.get_field_type(field_path)
        if field_type is not None and field_type not in STRING_LIKE_TYPES:
            self._report(
                self._on_type_mismatch,
                f"{op} applied to non-string field '{field_path}' (type: {field_type!r})",
                field=field_path,
                node=field_ref,
            )

    def _report(
        self,
        level: Strictness,
        message: str,
        field: str | None = None,
        node: Node | None = None,
    ) -> None:
        if level == "silent":
            return
        cls = _WarnIssue if level == "warn" else _ErrorIssue
        self._issues.append(cls(message=message, field=field, node=node))
