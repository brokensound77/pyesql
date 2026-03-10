# pyesql

A standalone Python parser for [ES|QL](https://www.elastic.co/guide/en/elasticsearch/reference/current/esql.html) (Elasticsearch Query Language) that produces an AST for testing and validation.

No Java, no ANTLR runtime. Pure Python.

## Installation

```bash
uv sync
```

To also install dev dependencies (required for testing):

```bash
uv sync --extra dev
```

## Quick start

```python
from pyesql import parse

query = parse("FROM logs-* | WHERE @timestamp > NOW() - 1d | LIMIT 100")

query.source          # FromCommand(indices=[IndexPattern(index='logs-*', ...)])
query.pipes           # [WhereCommand(...), LimitCommand(...)]
```

## Inspecting the AST

```python
from pyesql import parse
from pyesql.walker import find_all
from pyesql.ast import FunctionCall

query = parse("FROM logs | STATS count = COUNT(*), avg_ms = AVG(response_time) BY host")
calls = find_all(query, FunctionCall)
print([c.name for c in calls])   # ['COUNT', 'AVG']
```

## Walking with a Visitor

```python
from pyesql import parse
from pyesql.visitor import Visitor
from pyesql.ast import FieldRef, Comparison

class AuditVisitor(Visitor):
    def __init__(self):
        self.comparisons = []

    def visit_Comparison(self, node):
        self.comparisons.append((str(node.left), node.operator, node.right))
        self.generic_visit(node)

v = AuditVisitor()
v.visit(parse("FROM idx | WHERE status == 200 AND host != \"bad\""))
print(v.comparisons)
# [(FieldRef(name=...), '==', IntegerLiteral(value=200)), ...]
```

## Transforming the AST

```python
from pyesql.visitor import Transformer
from pyesql.ast import LimitCommand, IntegerLiteral

class CapLimit(Transformer):
    """Ensure LIMIT never exceeds 1000."""
    def visit_LimitCommand(self, node):
        if isinstance(node.count, IntegerLiteral) and node.count.value > 1000:
            node.count.value = 1000
        return node
```

## CLI

```bash
# Print parse tree
pyesql parse "FROM logs | WHERE status == 200 | LIMIT 10"

# JSON output
pyesql parse --format json "FROM logs | STATS COUNT(*) BY host"

# From file
pyesql parse -f my_query.esql

# From stdin
echo "FROM logs | LIMIT 5" | pyesql parse -
```

## Supported commands

| Category       | Commands |
|----------------|----------|
| Source         | `FROM`, `ROW`, `SHOW INFO`, `TS`, `PROMQL` |
| Filtering      | `WHERE` |
| Projection     | `KEEP`, `DROP`, `RENAME` |
| Computation    | `EVAL` |
| Aggregation    | `STATS`, `INLINESTATS` |
| Sorting/Paging | `SORT`, `LIMIT` |
| Text parsing   | `DISSECT`, `GROK` |
| Enrichment     | `ENRICH` |
| Joins          | `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`, `LOOKUP` |
| Multi-valued   | `MV_EXPAND` |
| Branching      | `FORK`, `FUSE` |
| ML / AI        | `COMPLETION`, `RERANK`, `CHANGE_POINT`, `SAMPLE` |
| Utilities      | `URI_PARTS`, `REGISTERED_DOMAIN`, `METRICS_INFO`, `TS_INFO` |
| Config         | `SET` |

## Error handling

`EsqlSyntaxError` is raised for both bad tokens (lexer) and structural problems (parser). It exposes `line`, `col`, and `text` attributes for precise reporting.

**Unterminated string literal** (lexer-level):

```python
from pyesql import parse
from pyesql.errors import EsqlSyntaxError

try:
    parse('FROM logs | WHERE message == "unclosed')
except EsqlSyntaxError as e:
    print(e)
    # Unterminated string literal at line 1, col 29 near '"unclosed'
    print(e.line, e.col)  # 1 29
```

**Missing expression after command keyword** (parser-level):

```python
try:
    parse("FROM logs | WHERE")
except EsqlSyntaxError as e:
    print(e)
    # Expected expression, got 'EOF' ('') at line 1, col 18
```

## Running tests

```bash
uv run pytest
```

Or via the Makefile:

```bash
make test        # run tests
make lint        # ruff check
make coverage    # tests with coverage report
```

## Package layout

```
pyesql/
├── __init__.py    public API: parse(), walk(), find_all(), Visitor, ...
├── ast.py         all AST node dataclasses
├── lexer.py       tokenizer (hand-written, no dependencies)
├── parser.py      recursive-descent parser
├── visitor.py     Visitor and Transformer base classes
├── walker.py      walk(), find_all(), find_first(), filter_nodes()
├── errors.py      EsqlSyntaxError, EsqlParseError
├── cli.py         `pyesql` command-line tool
tests/
└── test_parser.py
```

## Note on compatibility

Built based on elasticsearch commit: de1d740c6270076b1162cbc72bff7820d7c338e2. 
ES|QL is rapidly changing ... updates will be sporadic, at best ...
