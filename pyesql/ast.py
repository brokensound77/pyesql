"""
AST node definitions for ES|QL queries.

Every node is a dataclass.  Nodes are grouped by category:
  - Query / top-level
  - Source commands
  - Processing commands
  - Expressions
  - Literals & field refs
  - Helpers (OrderExpr, RenameClause, etc.)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


@dataclass
class Node:
    """Base class for all AST nodes."""

    def children(self) -> list[Node]:
        """Return child nodes for tree walking."""
        result: list[Node] = []
        for v in self.__dict__.values():
            if isinstance(v, Node):
                result.append(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, Node):
                        result.append(item)
        return result

    def __repr__(self) -> str:
        cls = type(self).__name__
        parts = []
        for k, v in self.__dict__.items():
            parts.append(f"{k}={v!r}")
        return f"{cls}({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Top-level
# ---------------------------------------------------------------------------


@dataclass
class Query(Node):
    """A complete ES|QL query: one source command + zero or more processing commands."""

    source: SourceCommand
    pipes: list[ProcessingCommand] = field(default_factory=list)
    # SET commands that precede the query
    settings: list[SetCommand] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Source commands
# ---------------------------------------------------------------------------


@dataclass
class SourceCommand(Node):
    pass


@dataclass
class IndexPattern(Node):
    cluster: str | None  # cross-cluster prefix, e.g. "remote"
    index: str  # index name / pattern
    selector: str | None = None  # after :: e.g. "failures"


@dataclass
class Metadata(Node):
    fields: list[str]


@dataclass
class FromCommand(SourceCommand):
    indices: list[IndexPattern | Subquery]
    metadata: Metadata | None = None


@dataclass
class Subquery(Node):
    """Parenthesized sub-query used inside FROM."""

    query: Query


@dataclass
class RowCommand(SourceCommand):
    fields: list[Field]


@dataclass
class ShowCommand(SourceCommand):
    """SHOW INFO"""

    pass


@dataclass
class TimeSeriesCommand(SourceCommand):
    """TS ..."""

    indices: list[IndexPattern]
    metadata: Metadata | None = None


@dataclass
class PromqlCommand(SourceCommand):
    query: str


@dataclass
class ExplainCommand(SourceCommand):
    """EXPLAIN (query)"""

    query: Query


# ---------------------------------------------------------------------------
# Processing commands
# ---------------------------------------------------------------------------


@dataclass
class ProcessingCommand(Node):
    pass


@dataclass
class EvalCommand(ProcessingCommand):
    fields: list[Field]


@dataclass
class WhereCommand(ProcessingCommand):
    condition: BoolExpr  # type alias defined later


@dataclass
class LimitCommand(ProcessingCommand):
    count: Expr  # type alias defined later


@dataclass
class SortCommand(ProcessingCommand):
    orders: list[OrderExpression]


@dataclass
class KeepCommand(ProcessingCommand):
    patterns: list[QualifiedNamePattern]


@dataclass
class DropCommand(ProcessingCommand):
    patterns: list[QualifiedNamePattern]


@dataclass
class RenameCommand(ProcessingCommand):
    clauses: list[RenameClause]


@dataclass
class StatsCommand(ProcessingCommand):
    stats: list[AggField]
    by: list[Field] = field(default_factory=list)


@dataclass
class InlineStatsCommand(ProcessingCommand):
    stats: list[AggField]
    by: list[Field] = field(default_factory=list)


@dataclass
class DissectCommand(ProcessingCommand):
    source: Expr  # type alias defined later
    pattern: str
    options: list[DissectOption] = field(default_factory=list)


@dataclass
class GrokCommand(ProcessingCommand):
    source: Expr  # type alias defined later
    patterns: list[str]


@dataclass
class EnrichCommand(ProcessingCommand):
    policy: str
    on: QualifiedNamePattern | None = None
    with_clauses: list[EnrichWithClause] = field(default_factory=list)


@dataclass
class MvExpandCommand(ProcessingCommand):
    field: QualifiedName  # defined later


@dataclass
class JoinCommand(ProcessingCommand):
    join_type: str  # "JOIN", "FULL JOIN", "LEFT JOIN", "RIGHT JOIN", "LOOKUP"
    table: IndexPattern
    alias: str | None = None
    conditions: list[QualifiedNamePattern] = field(default_factory=list)


@dataclass
class ChangePointCommand(ProcessingCommand):
    value: QualifiedName  # defined later
    key: QualifiedName | None = None
    target_type: QualifiedName | None = None
    target_pvalue: QualifiedName | None = None


@dataclass
class CompletionCommand(ProcessingCommand):
    prompt: Expr  # type alias defined later
    target: QualifiedName | None = None
    params: MapExpr | None = None


@dataclass
class SampleCommand(ProcessingCommand):
    probability: Expr  # type alias defined later


@dataclass
class RerankCommand(ProcessingCommand):
    query_text: Expr  # type alias defined later
    fields: list[Field]
    target: QualifiedName | None = None
    params: MapExpr | None = None


@dataclass
class ForkCommand(ProcessingCommand):
    branches: list[list[ProcessingCommand]]


@dataclass
class FuseCommand(ProcessingCommand):
    fuse_type: str | None = None
    configurations: list[FuseConfiguration] = field(default_factory=list)


@dataclass
class UriPartsCommand(ProcessingCommand):
    target: QualifiedName  # defined later
    source: Expr  # type alias defined later


@dataclass
class RegisteredDomainCommand(ProcessingCommand):
    target: QualifiedName  # defined later
    source: Expr  # type alias defined later


@dataclass
class MetricsInfoCommand(ProcessingCommand):
    pass


@dataclass
class TsInfoCommand(ProcessingCommand):
    pass


@dataclass
class LookupCommand(ProcessingCommand):
    table: IndexPattern
    match_fields: list[QualifiedNamePattern]


@dataclass
class InsistCommand(ProcessingCommand):
    patterns: list[QualifiedNamePattern]


@dataclass
class SetCommand(Node):
    name: str
    value: Expr  # type alias defined later


# ---------------------------------------------------------------------------
# Helper structures
# ---------------------------------------------------------------------------


@dataclass
class Field(Node):
    """An optionally-named expression: [name =] expr"""

    name: QualifiedName | None
    expr: BoolExpr  # type alias defined later


@dataclass
class AggField(Node):
    """An aggregation field with optional WHERE filter."""

    field: Field
    where: BoolExpr | None = None


@dataclass
class OrderExpression(Node):
    expr: BoolExpr  # type alias defined later
    order: str | None = None  # "ASC" | "DESC"
    nulls: str | None = None  # "FIRST" | "LAST"


@dataclass
class QualifiedName(Node):
    parts: list[str]  # e.g. ["field", "subfield"]

    def __str__(self) -> str:
        return ".".join(self.parts)


@dataclass
class QualifiedNamePattern(Node):
    parts: list[str]  # may contain "*" as a wildcard segment

    def __str__(self) -> str:
        return ".".join(self.parts)


@dataclass
class RenameClause(Node):
    old_name: QualifiedNamePattern
    new_name: QualifiedNamePattern


@dataclass
class DissectOption(Node):
    name: str
    value: Expr  # type alias defined later


@dataclass
class EnrichWithClause(Node):
    enrich_field: QualifiedNamePattern
    new_name: QualifiedNamePattern | None = None


@dataclass
class FuseConfiguration(Node):
    kind: str  # "SCORE", "KEY", "GROUP", "WITH"
    value: Any  # QualifiedName | List[QualifiedName] | MapExpr


# ---------------------------------------------------------------------------
# Expressions (type alias for readability)
# ---------------------------------------------------------------------------

type Expr = Node
type BoolExpr = Node


@dataclass
class LogicalNot(Node):
    operand: Node


@dataclass
class LogicalBinary(Node):
    operator: str  # "AND" | "OR"
    left: Node
    right: Node


@dataclass
class IsNull(Node):
    expr: Node
    negated: bool = False  # IS NOT NULL when True


@dataclass
class InList(Node):
    expr: Node
    values: list[Node]
    negated: bool = False


@dataclass
class LikeExpr(Node):
    expr: Node
    patterns: list[Node]
    negated: bool = False
    is_list: bool = False  # LIKE(p1, p2, ...)  vs  LIKE p


@dataclass
class RlikeExpr(Node):
    expr: Node
    patterns: list[Node]
    negated: bool = False
    is_list: bool = False


@dataclass
class MatchExpr(Node):
    """field[:type] : value"""

    field: QualifiedName
    field_type: str | None
    value: Node


@dataclass
class Comparison(Node):
    operator: str  # "==" | "!=" | "<" | "<=" | ">" | ">=" | "=~"
    left: Node
    right: Node


@dataclass
class ArithmeticUnary(Node):
    operator: str  # "+" | "-"
    operand: Node


@dataclass
class ArithmeticBinary(Node):
    operator: str  # "+" | "-" | "*" | "/" | "%"
    left: Node
    right: Node


@dataclass
class InlineCast(Node):
    expr: Node
    data_type: str


@dataclass
class FunctionCall(Node):
    name: str
    args: list[Node] = field(default_factory=list)
    star: bool = False  # COUNT(*)
    options: MapExpr | None = None


@dataclass
class FieldRef(Node):
    name: QualifiedName

    def __str__(self) -> str:
        return str(self.name)


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------


@dataclass
class NullLiteral(Node):
    pass


@dataclass
class BooleanLiteral(Node):
    value: bool


@dataclass
class IntegerLiteral(Node):
    value: int
    unit: str | None = None  # e.g. "d" for 1d (qualified integer)


@dataclass
class DecimalLiteral(Node):
    value: float


@dataclass
class StringLiteral(Node):
    value: str


@dataclass
class NumericArrayLiteral(Node):
    values: list[IntegerLiteral | DecimalLiteral]


@dataclass
class BooleanArrayLiteral(Node):
    values: list[BooleanLiteral]


@dataclass
class StringArrayLiteral(Node):
    values: list[StringLiteral]


@dataclass
class Parameter(Node):
    """? or ?name or ?1"""

    name: str | None  # None for bare ?


@dataclass
class DoubleParameter(Node):
    """?? or ??name or ??1"""

    name: str | None


# ---------------------------------------------------------------------------
# Map expressions
# ---------------------------------------------------------------------------


@dataclass
class MapEntry(Node):
    key: str
    value: Node | MapExpr


@dataclass
class MapExpr(Node):
    entries: list[MapEntry]
