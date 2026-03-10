# pyesql

A standalone Python parser for [ES|QL](https://www.elastic.co/guide/en/elasticsearch/reference/current/esql.html) (Elasticsearch Query Language) that produces an AST for testing and validation.

No Java, no ANTLR runtime. Pure Python ≥ 3.9.

## Installation

```bash
cd pyesql
pip install -e ".[dev]"
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

## Running tests

```bash
python -m pytest pyesql/tests/ -v
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
└── tests/
    └── test_parser.py
```
