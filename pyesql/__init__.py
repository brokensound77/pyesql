"""
pyesql — A Python parser for ES|QL queries.

Quick start::

    from pyesql import parse

    query = parse("FROM logs-* | WHERE @timestamp > NOW() - 1d | LIMIT 100")
    print(query.source)          # FromCommand(...)
    print(query.pipes)           # [WhereCommand(...), LimitCommand(...)]

Inspecting the AST::

    from pyesql import parse
    from pyesql.walker import find_all
    from pyesql.ast import FunctionCall

    query = parse("FROM logs | STATS count = COUNT(*) BY host.name")
    calls = find_all(query, FunctionCall)
    print([c.name for c in calls])   # ['COUNT']

Walking with a visitor::

    from pyesql import parse
    from pyesql.visitor import Visitor
    from pyesql.ast import FieldRef

    class FieldPrinter(Visitor):
        def visit_FieldRef(self, node):
            print("field:", node.name)
            self.generic_visit(node)

    FieldPrinter().visit(parse("FROM idx | WHERE status == 200"))
"""

from .ast import *  # noqa: F401, F403  – re-export all AST nodes
from .errors import EsqlError, EsqlParseError, EsqlSchemaError, EsqlSyntaxError
from .parser import parse
from .schema import Schema
from .validator import (
    SchemaValidationError,
    SchemaValidationWarning,
    SchemaValidator,
    ValidationIssue,
    collect_computed_fields,
)
from .visitor import Transformer, Visitor
from .walker import filter_nodes, find_all, find_first, walk

__all__ = [
    # Parser
    "parse",
    # Schema
    "Schema",
    "SchemaValidator",
    "ValidationIssue",
    "collect_computed_fields",
    # Visitors / walkers
    "Visitor",
    "Transformer",
    "walk",
    "find_all",
    "find_first",
    "filter_nodes",
    # Errors
    "EsqlError",
    "EsqlSyntaxError",
    "EsqlParseError",
    "EsqlSchemaError",
    "SchemaValidationError",
    "SchemaValidationWarning",
]

__version__ = "0.2.3"
__elasticsearch_version__ = "9.4"  # ES|QL spec this parser targets
