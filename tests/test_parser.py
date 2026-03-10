"""
Tests for the pyesql parser.

Run with:  python -m pytest pyesql/tests/ -v
"""

import pytest

from pyesql import parse
from pyesql.ast import (
    AggField,
    ArithmeticBinary,
    ArithmeticUnary,
    BooleanArrayLiteral,
    ChangePointCommand,
    Comparison,
    DecimalLiteral,
    DissectCommand,
    DropCommand,
    EnrichCommand,
    EvalCommand,
    Field,
    FieldRef,
    ForkCommand,
    FromCommand,
    FunctionCall,
    GrokCommand,
    InlineCast,
    InlineStatsCommand,
    InList,
    IntegerLiteral,
    IsNull,
    JoinCommand,
    KeepCommand,
    LikeExpr,
    LimitCommand,
    LogicalBinary,
    LogicalNot,
    Metadata,
    MvExpandCommand,
    NullLiteral,
    NumericArrayLiteral,
    OrderExpression,
    Parameter,
    Query,
    RenameClause,
    RlikeExpr,
    RowCommand,
    SampleCommand,
    ShowCommand,
    SortCommand,
    StatsCommand,
    StringArrayLiteral,
    StringLiteral,
    WhereCommand,
)
from pyesql.errors import EsqlSyntaxError
from pyesql.visitor import Visitor
from pyesql.walker import find_all, find_first

# ---------------------------------------------------------------------------
# Source commands
# ---------------------------------------------------------------------------


class TestFromCommand:
    def test_simple_index(self):
        q = parse("FROM logs")
        assert isinstance(q, Query)
        assert isinstance(q.source, FromCommand)
        src: FromCommand = q.source
        assert len(src.indices) == 1
        assert src.indices[0].index == "logs"
        assert src.indices[0].cluster is None

    def test_wildcard_index(self):
        q = parse("FROM logs-*")
        assert q.source.indices[0].index == "logs-*"

    def test_multiple_indices(self):
        q = parse("FROM logs, metrics, traces-*")
        assert len(q.source.indices) == 3
        assert q.source.indices[1].index == "metrics"

    def test_cross_cluster(self):
        q = parse("FROM remote:logs-*")
        idx = q.source.indices[0]
        assert idx.cluster == "remote"
        assert idx.index == "logs-*"

    def test_metadata(self):
        q = parse("FROM logs METADATA _index, _id")
        meta: Metadata = q.source.metadata
        assert meta is not None
        assert meta.fields == ["_index", "_id"]

    def test_quoted_index(self):
        q = parse('FROM "my-index"')
        assert q.source.indices[0].index == "my-index"

    def test_dot_prefixed_index(self):
        q = parse("FROM .alerts-security.*")
        assert q.source.indices[0].index == ".alerts-security.*"

    def test_dot_prefixed_multiple(self):
        q = parse("FROM .ds-logs-*, .kibana_1")
        assert q.source.indices[0].index == ".ds-logs-*"
        assert q.source.indices[1].index == ".kibana_1"

    def test_no_pipes(self):
        q = parse("FROM idx")
        assert q.pipes == []


class TestRowCommand:
    def test_basic(self):
        q = parse("ROW x = 1, y = 2")
        assert isinstance(q.source, RowCommand)
        assert len(q.source.fields) == 2
        assert str(q.source.fields[0].name) == "x"

    def test_expression_field(self):
        q = parse("ROW a = 1 + 2")
        f: Field = q.source.fields[0]
        assert isinstance(f.expr, ArithmeticBinary)

    def test_no_name(self):
        q = parse("ROW 42")
        f: Field = q.source.fields[0]
        assert f.name is None
        assert isinstance(f.expr, IntegerLiteral)


class TestShowCommand:
    def test_show_info(self):
        q = parse("SHOW INFO")
        assert isinstance(q.source, ShowCommand)


# ---------------------------------------------------------------------------
# Processing commands
# ---------------------------------------------------------------------------


class TestWhereCommand:
    def test_simple_comparison(self):
        q = parse("FROM idx | WHERE status == 200")
        cmd: WhereCommand = q.pipes[0]
        assert isinstance(cmd, WhereCommand)
        cond: Comparison = cmd.condition
        assert cond.operator == "=="
        assert isinstance(cond.left, FieldRef)
        assert str(cond.left.name) == "status"
        assert isinstance(cond.right, IntegerLiteral)
        assert cond.right.value == 200

    def test_logical_and(self):
        q = parse("FROM idx | WHERE a == 1 AND b == 2")
        cond = q.pipes[0].condition
        assert isinstance(cond, LogicalBinary)
        assert cond.operator == "AND"

    def test_logical_or(self):
        q = parse("FROM idx | WHERE a == 1 OR b == 2")
        assert q.pipes[0].condition.operator == "OR"

    def test_logical_not(self):
        q = parse("FROM idx | WHERE NOT active")
        assert isinstance(q.pipes[0].condition, LogicalNot)

    def test_is_null(self):
        q = parse("FROM idx | WHERE field IS NULL")
        cond: IsNull = q.pipes[0].condition
        assert isinstance(cond, IsNull)
        assert not cond.negated

    def test_is_not_null(self):
        q = parse("FROM idx | WHERE field IS NOT NULL")
        assert q.pipes[0].condition.negated

    def test_in_list(self):
        q = parse("FROM idx | WHERE status IN (200, 404, 500)")
        cond: InList = q.pipes[0].condition
        assert isinstance(cond, InList)
        assert not cond.negated
        assert len(cond.values) == 3

    def test_not_in_list(self):
        q = parse("FROM idx | WHERE status NOT IN (404, 500)")
        assert q.pipes[0].condition.negated

    def test_like(self):
        q = parse('FROM idx | WHERE name LIKE "foo*"')
        cond: LikeExpr = q.pipes[0].condition
        assert isinstance(cond, LikeExpr)
        assert not cond.negated

    def test_not_like(self):
        q = parse('FROM idx | WHERE name NOT LIKE "foo*"')
        assert q.pipes[0].condition.negated

    def test_rlike(self):
        q = parse('FROM idx | WHERE name RLIKE "fo.*"')
        assert isinstance(q.pipes[0].condition, RlikeExpr)

    def test_nested_parens(self):
        q = parse("FROM idx | WHERE (a == 1 OR b == 2) AND c == 3")
        outer: LogicalBinary = q.pipes[0].condition
        assert outer.operator == "AND"

    def test_case_insensitive_eq(self):
        q = parse('FROM idx | WHERE name =~ "admin"')
        cond: Comparison = q.pipes[0].condition
        assert cond.operator == "=~"


class TestEvalCommand:
    def test_single_field(self):
        q = parse("FROM idx | EVAL doubled = value * 2")
        cmd: EvalCommand = q.pipes[0]
        assert isinstance(cmd, EvalCommand)
        assert len(cmd.fields) == 1
        f: Field = cmd.fields[0]
        assert str(f.name) == "doubled"
        assert isinstance(f.expr, ArithmeticBinary)

    def test_function_call(self):
        q = parse("FROM idx | EVAL ts = DATE_PARSE(timestamp)")
        f: Field = q.pipes[0].fields[0]
        fn: FunctionCall = f.expr
        assert isinstance(fn, FunctionCall)
        assert fn.name.upper() == "DATE_PARSE"

    def test_cast(self):
        q = parse("FROM idx | EVAL n = val::integer")
        f: Field = q.pipes[0].fields[0]
        cast: InlineCast = f.expr
        assert isinstance(cast, InlineCast)
        assert cast.data_type == "integer"

    def test_multiple_fields(self):
        q = parse("FROM idx | EVAL a = 1, b = 2, c = 3")
        assert len(q.pipes[0].fields) == 3


class TestLimitCommand:
    def test_limit(self):
        q = parse("FROM idx | LIMIT 100")
        cmd: LimitCommand = q.pipes[0]
        assert isinstance(cmd, LimitCommand)
        assert isinstance(cmd.count, IntegerLiteral)
        assert cmd.count.value == 100


class TestSortCommand:
    def test_simple(self):
        q = parse("FROM idx | SORT timestamp")
        cmd: SortCommand = q.pipes[0]
        assert isinstance(cmd, SortCommand)
        assert len(cmd.orders) == 1

    def test_asc_desc(self):
        q = parse("FROM idx | SORT timestamp DESC, name ASC")
        assert cmd_sort(q)[0].order == "DESC"
        assert cmd_sort(q)[1].order == "ASC"

    def test_nulls(self):
        q = parse("FROM idx | SORT value ASC NULLS FIRST")
        order: OrderExpression = cmd_sort(q)[0]
        assert order.nulls == "FIRST"

    def test_multiple_fields(self):
        q = parse("FROM idx | SORT a, b, c")
        assert len(cmd_sort(q)) == 3


def cmd_sort(q) -> list:
    return q.pipes[0].orders


class TestStatsCommand:
    def test_count_star(self):
        q = parse("FROM idx | STATS count = COUNT(*)")
        cmd: StatsCommand = q.pipes[0]
        fn: FunctionCall = cmd.stats[0].field.expr
        assert fn.star

    def test_with_by(self):
        q = parse("FROM idx | STATS avg_val = AVG(value) BY host")
        cmd: StatsCommand = q.pipes[0]
        assert len(cmd.by) == 1
        assert str(cmd.by[0].expr.name) == "host"

    def test_multiple_stats(self):
        q = parse("FROM idx | STATS a = COUNT(*), b = SUM(bytes) BY host")
        assert len(q.pipes[0].stats) == 2

    def test_no_stats_only_by(self):
        q = parse("FROM idx | STATS BY host, service")
        cmd: StatsCommand = q.pipes[0]
        assert cmd.stats == []
        assert len(cmd.by) == 2

    def test_agg_where(self):
        q = parse("FROM idx | STATS c = COUNT(*) WHERE status == 200 BY host")
        agg: AggField = q.pipes[0].stats[0]
        assert agg.where is not None


class TestKeepDropCommand:
    def test_keep(self):
        q = parse("FROM idx | KEEP host, status, @timestamp")
        cmd: KeepCommand = q.pipes[0]
        assert isinstance(cmd, KeepCommand)
        assert len(cmd.patterns) == 3

    def test_keep_wildcard(self):
        q = parse("FROM idx | KEEP host.*")
        assert "*" in str(q.pipes[0].patterns[0])

    def test_drop(self):
        q = parse("FROM idx | DROP _index, _score")
        cmd: DropCommand = q.pipes[0]
        assert isinstance(cmd, DropCommand)
        assert len(cmd.patterns) == 2


class TestRenameCommand:
    def test_as_syntax(self):
        q = parse("FROM idx | RENAME old_name AS new_name")
        clause: RenameClause = q.pipes[0].clauses[0]
        assert str(clause.old_name) == "old_name"
        assert str(clause.new_name) == "new_name"

    def test_assign_syntax(self):
        q = parse("FROM idx | RENAME new_name = old_name")
        clause: RenameClause = q.pipes[0].clauses[0]
        assert str(clause.new_name) == "new_name"
        assert str(clause.old_name) == "old_name"

    def test_multiple(self):
        q = parse("FROM idx | RENAME a AS b, c AS d")
        assert len(q.pipes[0].clauses) == 2


class TestDissectCommand:
    def test_basic(self):
        q = parse('FROM idx | DISSECT message "%{ts} %{msg}"')
        cmd: DissectCommand = q.pipes[0]
        assert isinstance(cmd, DissectCommand)
        assert cmd.pattern == "%{ts} %{msg}"


class TestGrokCommand:
    def test_basic(self):
        q = parse('FROM idx | GROK message "%{IP:client}"')
        cmd: GrokCommand = q.pipes[0]
        assert isinstance(cmd, GrokCommand)
        assert cmd.patterns[0] == "%{IP:client}"


class TestEnrichCommand:
    def test_simple(self):
        q = parse("FROM idx | ENRICH my-policy")
        cmd: EnrichCommand = q.pipes[0]
        assert cmd.policy == "my-policy"
        assert cmd.on is None
        assert cmd.with_clauses == []

    def test_on(self):
        q = parse("FROM idx | ENRICH geo-policy ON client_ip")
        cmd: EnrichCommand = q.pipes[0]
        assert str(cmd.on) == "client_ip"

    def test_with(self):
        q = parse("FROM idx | ENRICH geo-policy ON ip WITH country, city")
        assert len(q.pipes[0].with_clauses) == 2

    def test_with_alias(self):
        q = parse("FROM idx | ENRICH pol WITH alias = source_field")
        clause = q.pipes[0].with_clauses[0]
        assert str(clause.new_name) == "alias"
        assert str(clause.enrich_field) == "source_field"


class TestMvExpandCommand:
    def test_basic(self):
        q = parse("FROM idx | MV_EXPAND tags")
        cmd: MvExpandCommand = q.pipes[0]
        assert isinstance(cmd, MvExpandCommand)
        assert str(cmd.field) == "tags"


class TestJoinCommand:
    def test_join(self):
        q = parse("FROM idx | JOIN other ON id")
        cmd: JoinCommand = q.pipes[0]
        assert cmd.join_type == "JOIN"
        assert cmd.table.index == "other"
        assert str(cmd.conditions[0]) == "id"

    def test_left_join(self):
        q = parse("FROM idx | LEFT JOIN other ON id")
        assert q.pipes[0].join_type == "LEFT JOIN"

    def test_right_join(self):
        q = parse("FROM idx | RIGHT JOIN other ON id")
        assert q.pipes[0].join_type == "RIGHT JOIN"

    def test_full_join(self):
        q = parse("FROM idx | FULL JOIN other ON id")
        assert q.pipes[0].join_type == "FULL JOIN"

    def test_join_with_alias(self):
        q = parse("FROM idx | JOIN other AS o ON id")
        assert q.pipes[0].alias == "o"


class TestInlineStatsCommand:
    def test_inlinestats_keyword(self):
        q = parse("FROM idx | INLINESTATS count = COUNT(*) BY host")
        cmd: InlineStatsCommand = q.pipes[0]
        assert isinstance(cmd, InlineStatsCommand)
        assert len(cmd.by) == 1


class TestForkCommand:
    def test_fork(self):
        q = parse("FROM idx | FORK (WHERE status == 200) (WHERE status == 404)")
        cmd: ForkCommand = q.pipes[0]
        assert isinstance(cmd, ForkCommand)
        assert len(cmd.branches) == 2

    def test_fork_multi_pipe(self):
        q = parse("FROM idx | FORK (WHERE a == 1 | LIMIT 10) (SORT b)")
        assert len(q.pipes[0].branches[0]) == 2


class TestChangePointCommand:
    def test_basic(self):
        q = parse("FROM idx | CHANGE_POINT value")
        cmd: ChangePointCommand = q.pipes[0]
        assert str(cmd.value) == "value"
        assert cmd.key is None

    def test_with_on(self):
        q = parse("FROM idx | CHANGE_POINT value ON timestamp")
        assert str(q.pipes[0].key) == "timestamp"


class TestSampleCommand:
    def test_sample(self):
        q = parse("FROM idx | SAMPLE 0.1")
        cmd: SampleCommand = q.pipes[0]
        assert isinstance(cmd, SampleCommand)
        assert isinstance(cmd.probability, DecimalLiteral)
        assert cmd.probability.value == pytest.approx(0.1)


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------


class TestLiterals:
    def test_integer(self):
        q = parse("FROM idx | LIMIT 42")
        assert q.pipes[0].count.value == 42

    def test_negative_integer(self):
        q = parse("FROM idx | WHERE x == -5")
        rhs = q.pipes[0].condition.right
        # unary minus wraps the literal in expression context
        assert isinstance(rhs, ArithmeticUnary)
        assert rhs.operator == "-"
        assert rhs.operand.value == 5

    def test_decimal(self):
        q = parse("FROM idx | WHERE x == 3.14")
        assert q.pipes[0].condition.right.value == pytest.approx(3.14)

    def test_string(self):
        q = parse('FROM idx | WHERE name == "hello"')
        assert q.pipes[0].condition.right.value == "hello"

    def test_true(self):
        q = parse("FROM idx | WHERE active == true")
        assert q.pipes[0].condition.right.value is True

    def test_false(self):
        q = parse("FROM idx | WHERE active == false")
        assert q.pipes[0].condition.right.value is False

    def test_null(self):
        q = parse("FROM idx | WHERE x == null")
        assert isinstance(q.pipes[0].condition.right, NullLiteral)

    def test_integer_array(self):
        # Array literals use [] in EVAL context; IN list uses ()
        q = parse("FROM idx | EVAL arr = [1, 2, 3]")
        arr = q.pipes[0].fields[0].expr
        assert isinstance(arr, NumericArrayLiteral)
        assert len(arr.values) == 3

    def test_boolean_array(self):
        q = parse("FROM idx | EVAL arr = [true, false, true]")
        arr = q.pipes[0].fields[0].expr
        assert isinstance(arr, BooleanArrayLiteral)

    def test_string_array(self):
        q = parse('FROM idx | EVAL arr = ["a", "b", "c"]')
        arr = q.pipes[0].fields[0].expr
        assert isinstance(arr, StringArrayLiteral)

    def test_qualified_integer(self):
        q = parse("FROM idx | WHERE age > 1d")
        rhs = q.pipes[0].condition.right
        assert isinstance(rhs, IntegerLiteral)
        assert rhs.unit == "d"


class TestArithmetic:
    def test_addition(self):
        q = parse("FROM idx | EVAL x = a + b")
        expr: ArithmeticBinary = q.pipes[0].fields[0].expr
        assert expr.operator == "+"

    def test_precedence(self):
        # a + b * c  should give  a + (b * c)
        q = parse("FROM idx | EVAL x = a + b * c")
        expr: ArithmeticBinary = q.pipes[0].fields[0].expr
        assert expr.operator == "+"
        assert isinstance(expr.right, ArithmeticBinary)
        assert expr.right.operator == "*"

    def test_unary_minus(self):
        q = parse("FROM idx | EVAL x = -value")
        expr: ArithmeticUnary = q.pipes[0].fields[0].expr
        assert isinstance(expr, ArithmeticUnary)
        assert expr.operator == "-"

    def test_modulo(self):
        q = parse("FROM idx | EVAL x = a % b")
        assert q.pipes[0].fields[0].expr.operator == "%"


class TestFunctionCalls:
    def test_no_args(self):
        q = parse("FROM idx | EVAL t = NOW()")
        fn: FunctionCall = q.pipes[0].fields[0].expr
        assert fn.name.upper() == "NOW"
        assert fn.args == []

    def test_single_arg(self):
        q = parse("FROM idx | EVAL l = LENGTH(name)")
        fn: FunctionCall = q.pipes[0].fields[0].expr
        assert fn.name.upper() == "LENGTH"
        assert len(fn.args) == 1

    def test_multi_args(self):
        q = parse("FROM idx | EVAL r = SUBSTRING(name, 0, 5)")
        fn: FunctionCall = q.pipes[0].fields[0].expr
        assert len(fn.args) == 3

    def test_count_star(self):
        q = parse("FROM idx | STATS c = COUNT(*)")
        fn: FunctionCall = q.pipes[0].stats[0].field.expr
        assert fn.star

    def test_nested_function(self):
        q = parse("FROM idx | EVAL x = ABS(ROUND(value, 2))")
        outer: FunctionCall = q.pipes[0].fields[0].expr
        assert outer.name.upper() == "ABS"
        assert isinstance(outer.args[0], FunctionCall)


class TestParameters:
    def test_bare_param(self):
        q = parse("FROM idx | WHERE x == ?")
        assert isinstance(q.pipes[0].condition.right, Parameter)
        assert q.pipes[0].condition.right.name is None

    def test_named_param(self):
        q = parse("FROM idx | WHERE x == ?myParam")
        p: Parameter = q.pipes[0].condition.right
        assert p.name == "myParam"

    def test_positional_param(self):
        q = parse("FROM idx | WHERE x == ?1")
        assert q.pipes[0].condition.right.name == "1"


class TestMapExpression:
    def test_dissect_options(self):
        q = parse('FROM idx | DISSECT msg "%{a}" , append_separator = ","')
        opt = q.pipes[0].options[0]
        assert opt.name == "append_separator"
        assert isinstance(opt.value, StringLiteral)


# ---------------------------------------------------------------------------
# Pipeline / multi-command
# ---------------------------------------------------------------------------


class TestPipeline:
    def test_multi_pipe(self):
        q = parse("FROM logs | WHERE status == 200 | LIMIT 10")
        assert len(q.pipes) == 2

    def test_full_pipeline(self):
        q = parse(
            "FROM logs-* "
            "| WHERE @timestamp > NOW() - 1d "
            "| STATS count = COUNT(*) BY host "
            "| SORT count DESC "
            "| LIMIT 20"
        )
        assert isinstance(q.source, FromCommand)
        assert len(q.pipes) == 4

    def test_eval_and_where(self):
        q = parse("FROM idx | EVAL doubled = x * 2 | WHERE doubled > 10")
        assert isinstance(q.pipes[0], EvalCommand)
        assert isinstance(q.pipes[1], WhereCommand)


# ---------------------------------------------------------------------------
# SET command
# ---------------------------------------------------------------------------


class TestSetCommand:
    def test_set(self):
        q = parse('SET my_setting = "value"; FROM idx')
        assert len(q.settings) == 1
        assert q.settings[0].name == "my_setting"


# ---------------------------------------------------------------------------
# Walker and visitor utilities
# ---------------------------------------------------------------------------


class TestWalker:
    def test_find_all_function_calls(self):
        q = parse("FROM idx | EVAL x = ABS(value), y = ROUND(z, 2)")
        calls = find_all(q, FunctionCall)
        names = {c.name.upper() for c in calls}
        assert "ABS" in names
        assert "ROUND" in names

    def test_find_first(self):
        q = parse("FROM idx | WHERE status == 200")
        cmp = find_first(q, Comparison)
        assert cmp is not None
        assert cmp.operator == "=="

    def test_walk_all_nodes(self):
        from pyesql.walker import walk

        q = parse("FROM logs | LIMIT 10")
        nodes = list(walk(q))
        assert any(isinstance(n, FromCommand) for n in nodes)
        assert any(isinstance(n, LimitCommand) for n in nodes)


class TestVisitor:
    def test_field_collector(self):
        class Collector(Visitor):
            def __init__(self):
                self.fields = []

            def visit_FieldRef(self, node):
                self.fields.append(str(node.name))
                self.generic_visit(node)

        q = parse('FROM idx | WHERE status == 200 AND host == "web"')
        c = Collector()
        c.visit(q)
        assert "status" in c.fields
        assert "host" in c.fields


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    def test_empty_string(self):
        with pytest.raises(EsqlSyntaxError):
            parse("")

    def test_missing_pipe_command(self):
        with pytest.raises(EsqlSyntaxError):
            parse("FROM idx |")

    def test_unknown_source(self):
        with pytest.raises(EsqlSyntaxError):
            parse("SELECT * FROM table")

    def test_unclosed_paren(self):
        with pytest.raises(EsqlSyntaxError):
            parse("FROM idx | WHERE (status == 200")

    def test_unclosed_string(self):
        with pytest.raises(EsqlSyntaxError):
            parse('FROM idx | WHERE name == "unclosed')
