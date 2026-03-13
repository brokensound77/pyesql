"""
Recursive-descent parser for ES|QL.

Grammar reference: EsqlBaseParser.g4 + Expression.g4 (parser fragment)

Entry point:
    parse(text: str) -> Query
"""

from .ast import (
    AggField,
    ArithmeticBinary,
    ArithmeticUnary,
    BooleanArrayLiteral,
    BooleanLiteral,
    ChangePointCommand,
    Comparison,
    CompletionCommand,
    DecimalLiteral,
    DissectCommand,
    DissectOption,
    DoubleParameter,
    DropCommand,
    EnrichCommand,
    EnrichWithClause,
    EvalCommand,
    ExplainCommand,
    Field,
    FieldRef,
    ForkCommand,
    FromCommand,
    FunctionCall,
    FuseCommand,
    FuseConfiguration,
    GrokCommand,
    IndexPattern,
    InlineCast,
    InlineStatsCommand,
    InList,
    InsistCommand,
    IntegerLiteral,
    IsNull,
    JoinCommand,
    KeepCommand,
    LikeExpr,
    LimitCommand,
    LogicalBinary,
    LogicalNot,
    LookupCommand,
    MapEntry,
    MapExpr,
    MatchExpr,
    Metadata,
    MetricsInfoCommand,
    MvExpandCommand,
    NullLiteral,
    NumericArrayLiteral,
    OrderExpression,
    Parameter,
    ProcessingCommand,
    PromqlCommand,
    QualifiedName,
    QualifiedNamePattern,
    Query,
    RegisteredDomainCommand,
    RenameClause,
    RenameCommand,
    RerankCommand,
    RlikeExpr,
    RowCommand,
    SampleCommand,
    SetCommand,
    ShowCommand,
    SortCommand,
    SourceCommand,
    StatsCommand,
    StringArrayLiteral,
    StringLiteral,
    Subquery,
    TimeSeriesCommand,
    TsInfoCommand,
    UriPartsCommand,
    WhereCommand,
)
from .errors import EsqlSyntaxError
from .lexer import Token, TokenType, tokenize


class Parser:
    def __init__(self, tokens: list[Token]) -> None:
        self._tokens = tokens
        self._pos = 0

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    @property
    def _current(self) -> Token:
        return self._tokens[self._pos]

    @property
    def _peek_type(self) -> TokenType:
        return self._current.type

    def _peek_ahead(self, offset: int = 1) -> Token:
        idx = min(self._pos + offset, len(self._tokens) - 1)
        return self._tokens[idx]

    def _advance(self) -> Token:
        tok = self._tokens[self._pos]
        if tok.type != TokenType.EOF:
            self._pos += 1
        return tok

    def _check(self, *types: TokenType) -> bool:
        return self._peek_type in types

    def _match(self, *types: TokenType) -> Token | None:
        if self._peek_type in types:
            return self._advance()
        return None

    def _expect(self, *types: TokenType) -> Token:
        tok = self._current
        if tok.type not in types:
            expected = " or ".join(t.name for t in types)
            raise EsqlSyntaxError(
                f"Expected {expected}, got {tok.type.name!r}",
                tok.line,
                tok.col,
                tok.value,
            )
        return self._advance()

    def _error(self, msg: str) -> EsqlSyntaxError:
        tok = self._current
        return EsqlSyntaxError(msg, tok.line, tok.col, tok.value)

    # ------------------------------------------------------------------
    # Top-level
    # ------------------------------------------------------------------

    def parse(self) -> Query:
        settings: list[SetCommand] = []
        while self._check(TokenType.SET):
            settings.append(self._parse_set_command())
            self._expect(TokenType.SEMICOLON)

        source = self._parse_source_command()
        pipes: list[ProcessingCommand] = []

        while self._match(TokenType.PIPE):
            pipes.append(self._parse_processing_command())

        self._expect(TokenType.EOF)
        return Query(source=source, pipes=pipes, settings=settings)

    # ------------------------------------------------------------------
    # Source commands
    # ------------------------------------------------------------------

    def _parse_source_command(self) -> SourceCommand:
        tok = self._current
        if tok.type == TokenType.FROM:
            return self._parse_from()
        if tok.type == TokenType.ROW:
            return self._parse_row()
        if tok.type == TokenType.SHOW:
            return self._parse_show()
        if tok.type == TokenType.TS:
            return self._parse_time_series()
        if tok.type == TokenType.PROMQL:
            return self._parse_promql()
        if tok.type == TokenType.EXPLAIN:
            return self._parse_explain()
        raise self._error(
            f"Expected a source command (FROM, ROW, SHOW, TS, PROMQL), got {tok.type.name!r}"
        )

    def _parse_from(self) -> FromCommand:
        self._expect(TokenType.FROM)
        indices = self._parse_index_pattern_list()
        meta = None
        if self._check(TokenType.METADATA):
            meta = self._parse_metadata()
        return FromCommand(indices=indices, metadata=meta)

    def _parse_index_pattern_list(self) -> list:
        items = [self._parse_index_pattern_or_subquery()]
        while self._match(TokenType.COMMA):
            items.append(self._parse_index_pattern_or_subquery())
        return items

    def _parse_index_pattern_or_subquery(self):
        if self._check(TokenType.LP):
            return self._parse_subquery()
        return self._parse_index_pattern()

    def _parse_subquery(self) -> Subquery:
        self._expect(TokenType.LP)
        # Must start with FROM
        source = self._parse_from()
        pipes: list[ProcessingCommand] = []
        while self._match(TokenType.PIPE):
            pipes.append(self._parse_processing_command())
        self._expect(TokenType.RP)
        return Subquery(query=Query(source=source, pipes=pipes))

    def _parse_index_pattern(self) -> IndexPattern:
        """Parse cluster:index::selector or plain index string."""
        # Collect raw tokens to form an index pattern (may include hyphens, dots, *)
        cluster: str | None = None
        selector: str | None = None

        raw = self._parse_index_string()

        # Check for cross-cluster: cluster:index
        if self._check(TokenType.COLON):
            nxt = self._peek_ahead(1)
            if nxt.type in (
                TokenType.IDENTIFIER,
                TokenType.ID_PATTERN,
                TokenType.QUOTED_STRING,
                TokenType.ASTERISK,
            ) or _is_unquoted_source(nxt):
                self._advance()  # consume COLON
                cluster = raw
                raw = self._parse_index_string()

        # Check for selector ::
        if self._check(TokenType.CAST_OP):
            self._advance()
            selector = self._current.value
            self._advance()

        return IndexPattern(cluster=cluster, index=raw, selector=selector)

    def _parse_index_string(self) -> str:
        """Consume tokens that form an index name/pattern.

        Index names may start with '.' (e.g. .alerts-security.*) and may
        contain hyphens, dots, digits, and '*' wildcards.
        """
        tok = self._current
        if tok.type == TokenType.QUOTED_STRING:
            self._advance()
            return tok.value

        parts: list[str] = []

        # Allow a leading dot (.alerts-*, .ds-logs-*)
        if tok.type == TokenType.DOT:
            parts.append(".")
            self._advance()
            tok = self._current

        # Must now have an identifier-like token (or * for bare wildcard)
        valid_types = (
            TokenType.IDENTIFIER,
            TokenType.ID_PATTERN,
            TokenType.ASTERISK,
            TokenType.QUOTED_IDENTIFIER,
        )
        if tok.type not in valid_types and not _is_keyword_usable_as_name(tok.type):
            raise self._error(f"Expected index name, got {tok.type.name!r}")

        parts.append(tok.value)
        self._advance()

        # Greedily consume hyphens, dots, wildcards and following identifier/number segments
        while self._check(TokenType.MINUS, TokenType.DOT, TokenType.ASTERISK):
            parts.append(self._current.value)
            self._advance()
            if self._current.type in (
                TokenType.IDENTIFIER,
                TokenType.ID_PATTERN,
                TokenType.INTEGER,
                TokenType.DECIMAL,
            ):
                parts.append(self._current.value)
                self._advance()
        return "".join(parts)

    def _parse_metadata(self) -> Metadata:
        self._expect(TokenType.METADATA)
        fields = [self._parse_unquoted_identifier()]
        while self._match(TokenType.COMMA):
            fields.append(self._parse_unquoted_identifier())
        return Metadata(fields=fields)

    def _parse_row(self) -> RowCommand:
        self._expect(TokenType.ROW)
        return RowCommand(fields=self._parse_fields())

    def _parse_show(self) -> ShowCommand:
        self._expect(TokenType.SHOW)
        self._expect(TokenType.INFO)
        return ShowCommand()

    def _parse_time_series(self) -> TimeSeriesCommand:
        self._expect(TokenType.TS)
        indices_raw = self._parse_index_pattern_list()
        indices = [i for i in indices_raw if isinstance(i, IndexPattern)]
        meta = None
        if self._check(TokenType.METADATA):
            meta = self._parse_metadata()
        return TimeSeriesCommand(indices=indices, metadata=meta)

    def _parse_promql(self) -> PromqlCommand:
        self._expect(TokenType.PROMQL)
        # PromQL query is a raw string following the command keyword
        tok = self._expect(TokenType.QUOTED_STRING)
        return PromqlCommand(query=tok.value)

    def _parse_explain(self) -> ExplainCommand:
        self._expect(TokenType.EXPLAIN)
        self._expect(TokenType.LP)
        inner = self._parse_full_query()
        self._expect(TokenType.RP)
        return ExplainCommand(query=inner)

    def _parse_full_query(self) -> Query:
        source = self._parse_source_command()
        pipes: list[ProcessingCommand] = []
        while self._match(TokenType.PIPE):
            pipes.append(self._parse_processing_command())
        return Query(source=source, pipes=pipes)

    # ------------------------------------------------------------------
    # Processing commands
    # ------------------------------------------------------------------

    def _parse_processing_command(self) -> ProcessingCommand:
        tok = self._current
        tt = tok.type
        dispatch = {
            TokenType.EVAL: self._parse_eval,
            TokenType.WHERE: self._parse_where,
            TokenType.KEEP: self._parse_keep,
            TokenType.LIMIT: self._parse_limit,
            TokenType.STATS: self._parse_stats,
            TokenType.SORT: self._parse_sort,
            TokenType.DROP: self._parse_drop,
            TokenType.RENAME: self._parse_rename,
            TokenType.DISSECT: self._parse_dissect,
            TokenType.GROK: self._parse_grok,
            TokenType.ENRICH: self._parse_enrich,
            TokenType.MV_EXPAND: self._parse_mv_expand,
            TokenType.JOIN: self._parse_join,
            TokenType.LOOKUP: self._parse_lookup,
            TokenType.CHANGE_POINT: self._parse_change_point,
            TokenType.COMPLETION: self._parse_completion,
            TokenType.SAMPLE: self._parse_sample,
            TokenType.FORK: self._parse_fork,
            TokenType.RERANK: self._parse_rerank,
            TokenType.INLINE: self._parse_inline_stats,
            TokenType.INLINESTATS: self._parse_inlinestats_legacy,
            TokenType.FUSE: self._parse_fuse,
            TokenType.URI_PARTS: self._parse_uri_parts,
            TokenType.METRICS_INFO: self._parse_metrics_info,
            TokenType.REGISTERED_DOMAIN: self._parse_registered_domain,
            TokenType.TS_INFO: self._parse_ts_info,
            TokenType.MMR: self._parse_mmr_stub,
            TokenType.INSIST: self._parse_insist,
            # JOIN variants via LEFT/RIGHT/FULL
            TokenType.LEFT: self._parse_join_typed,
            TokenType.RIGHT: self._parse_join_typed,
            TokenType.FULL: self._parse_join_typed,
        }
        fn = dispatch.get(tt)
        if fn is None:
            raise self._error(f"Unknown processing command {tok.type.name!r} ({tok.value!r})")
        return fn()

    def _parse_eval(self) -> EvalCommand:
        self._expect(TokenType.EVAL)
        return EvalCommand(fields=self._parse_fields())

    def _parse_where(self) -> WhereCommand:
        self._expect(TokenType.WHERE)
        return WhereCommand(condition=self._parse_boolean_expression())

    def _parse_keep(self) -> KeepCommand:
        self._expect(TokenType.KEEP)
        return KeepCommand(patterns=self._parse_qualified_name_patterns())

    def _parse_limit(self) -> LimitCommand:
        self._expect(TokenType.LIMIT)
        return LimitCommand(count=self._parse_constant())

    def _parse_stats(self) -> StatsCommand:
        self._expect(TokenType.STATS)
        stats: list[AggField] = []
        if not self._check(TokenType.BY, TokenType.PIPE, TokenType.EOF):
            stats = self._parse_agg_fields()
        by: list[Field] = []
        if self._match(TokenType.BY):
            by = self._parse_fields()
        return StatsCommand(stats=stats, by=by)

    def _parse_sort(self) -> SortCommand:
        self._expect(TokenType.SORT)
        orders = [self._parse_order_expression()]
        while self._match(TokenType.COMMA):
            orders.append(self._parse_order_expression())
        return SortCommand(orders=orders)

    def _parse_drop(self) -> DropCommand:
        self._expect(TokenType.DROP)
        return DropCommand(patterns=self._parse_qualified_name_patterns())

    def _parse_rename(self) -> RenameCommand:
        self._expect(TokenType.RENAME)
        clauses = [self._parse_rename_clause()]
        while self._match(TokenType.COMMA):
            clauses.append(self._parse_rename_clause())
        return RenameCommand(clauses=clauses)

    def _parse_dissect(self) -> DissectCommand:
        self._expect(TokenType.DISSECT)
        source = self._parse_primary_expression()
        pattern_tok = self._expect(TokenType.QUOTED_STRING)
        options: list[DissectOption] = []
        if self._match(TokenType.COMMA):
            options = self._parse_dissect_options()
        return DissectCommand(source=source, pattern=pattern_tok.value, options=options)

    def _parse_dissect_options(self) -> list[DissectOption]:
        opts = []
        while True:
            name = self._parse_identifier_string()
            self._expect(TokenType.ASSIGN)
            val = self._parse_constant()
            opts.append(DissectOption(name=name, value=val))
            if not self._match(TokenType.COMMA):
                break
        return opts

    def _parse_grok(self) -> GrokCommand:
        self._expect(TokenType.GROK)
        source = self._parse_primary_expression()
        patterns = [self._expect(TokenType.QUOTED_STRING).value]
        while self._match(TokenType.COMMA):
            patterns.append(self._expect(TokenType.QUOTED_STRING).value)
        return GrokCommand(source=source, patterns=patterns)

    def _parse_enrich(self) -> EnrichCommand:
        self._expect(TokenType.ENRICH)
        # policy name: quoted string or unquoted (may contain hyphens: my-policy)
        if self._check(TokenType.QUOTED_STRING):
            policy = self._advance().value
        else:
            parts = [self._parse_identifier_string()]
            while self._check(TokenType.MINUS):
                # only consume the hyphen if followed by an identifier-like token
                nxt = self._peek_ahead(1)
                if nxt.type in _NAME_TYPES or _is_keyword_usable_as_name(nxt.type):
                    self._advance()  # consume -
                    parts.append("-")
                    parts.append(self._parse_identifier_string())
                else:
                    break
            policy = "".join(parts)
        on: QualifiedNamePattern | None = None
        if self._match(TokenType.ON):
            on = self._parse_qualified_name_pattern()
        with_clauses: list[EnrichWithClause] = []
        if self._match(TokenType.WITH):
            with_clauses = [self._parse_enrich_with_clause()]
            while self._match(TokenType.COMMA):
                with_clauses.append(self._parse_enrich_with_clause())
        return EnrichCommand(policy=policy, on=on, with_clauses=with_clauses)

    def _parse_enrich_with_clause(self) -> EnrichWithClause:
        first = self._parse_qualified_name_pattern()
        if self._match(TokenType.ASSIGN):
            old = self._parse_qualified_name_pattern()
            return EnrichWithClause(enrich_field=old, new_name=first)
        return EnrichWithClause(enrich_field=first)

    def _parse_mv_expand(self) -> MvExpandCommand:
        self._expect(TokenType.MV_EXPAND)
        return MvExpandCommand(field=self._parse_qualified_name())

    def _parse_join(self) -> JoinCommand:
        self._expect(TokenType.JOIN)
        return self._finish_join("JOIN")

    def _parse_join_typed(self) -> JoinCommand:
        modifier = self._advance().value.upper()  # LEFT | RIGHT | FULL
        # optional OUTER
        if self._check(TokenType.OUTER):
            self._advance()
        self._expect(TokenType.JOIN)
        return self._finish_join(f"{modifier} JOIN")

    def _finish_join(self, join_type: str) -> JoinCommand:
        table = self._parse_index_pattern()
        alias: str | None = None
        if self._match(TokenType.AS):
            alias = self._parse_identifier_string()
        self._expect(TokenType.ON)
        conditions = [self._parse_qualified_name_pattern()]
        while self._match(TokenType.COMMA):
            conditions.append(self._parse_qualified_name_pattern())
        return JoinCommand(join_type=join_type, table=table, alias=alias, conditions=conditions)

    def _parse_lookup(self) -> LookupCommand:
        self._expect(TokenType.LOOKUP)
        table = self._parse_index_pattern()
        self._expect(TokenType.ON)
        fields = [self._parse_qualified_name_pattern()]
        while self._match(TokenType.COMMA):
            fields.append(self._parse_qualified_name_pattern())
        return LookupCommand(table=table, match_fields=fields)

    def _parse_change_point(self) -> ChangePointCommand:
        self._expect(TokenType.CHANGE_POINT)
        value = self._parse_qualified_name()
        key: QualifiedName | None = None
        target_type: QualifiedName | None = None
        target_pvalue: QualifiedName | None = None
        if self._match(TokenType.ON):
            key = self._parse_qualified_name()
        if self._match(TokenType.AS):
            target_type = self._parse_qualified_name()
            self._expect(TokenType.COMMA)
            target_pvalue = self._parse_qualified_name()
        return ChangePointCommand(
            value=value, key=key, target_type=target_type, target_pvalue=target_pvalue
        )

    def _parse_completion(self) -> CompletionCommand:
        self._expect(TokenType.COMPLETION)
        target: QualifiedName | None = None
        # optional  target =
        if self._is_qualified_name_ahead() and self._peek_ahead(1).type == TokenType.ASSIGN:
            target = self._parse_qualified_name()
            self._expect(TokenType.ASSIGN)
        prompt = self._parse_primary_expression()
        params = self._parse_command_named_parameters()
        return CompletionCommand(prompt=prompt, target=target, params=params)

    def _parse_sample(self) -> SampleCommand:
        self._expect(TokenType.SAMPLE)
        return SampleCommand(probability=self._parse_constant())

    def _parse_fork(self) -> ForkCommand:
        self._expect(TokenType.FORK)
        branches: list[list[ProcessingCommand]] = []
        while self._check(TokenType.LP):
            self._expect(TokenType.LP)
            cmds = [self._parse_processing_command()]
            while self._match(TokenType.PIPE):
                cmds.append(self._parse_processing_command())
            self._expect(TokenType.RP)
            branches.append(cmds)
        return ForkCommand(branches=branches)

    def _parse_rerank(self) -> RerankCommand:
        self._expect(TokenType.RERANK)
        target: QualifiedName | None = None
        if self._is_qualified_name_ahead() and self._peek_ahead(1).type == TokenType.ASSIGN:
            target = self._parse_qualified_name()
            self._expect(TokenType.ASSIGN)
        query_text = self._parse_constant()
        self._expect(TokenType.ON)
        fields = self._parse_fields()
        params = self._parse_command_named_parameters()
        return RerankCommand(query_text=query_text, fields=fields, target=target, params=params)

    def _parse_inline_stats(self) -> InlineStatsCommand:
        self._expect(TokenType.INLINE)
        if self._check(TokenType.INLINESTATS):
            self._expect(TokenType.INLINESTATS)
        else:
            self._expect_keyword("stats")
        return self._finish_inline_stats()

    def _parse_inlinestats_legacy(self) -> InlineStatsCommand:
        self._expect(TokenType.INLINESTATS)
        return self._finish_inline_stats()

    def _finish_inline_stats(self) -> InlineStatsCommand:
        stats = self._parse_agg_fields()
        by: list[Field] = []
        if self._match(TokenType.BY):
            by = self._parse_fields()
        return InlineStatsCommand(stats=stats, by=by)

    def _parse_fuse(self) -> FuseCommand:
        self._expect(TokenType.FUSE)
        fuse_type: str | None = None
        if self._check(TokenType.IDENTIFIER):
            fuse_type = self._advance().value
        configs: list[FuseConfiguration] = []
        fuse_keywords = {TokenType.SCORE, TokenType.KEY, TokenType.GROUP, TokenType.WITH}
        while self._peek_type in fuse_keywords:
            configs.append(self._parse_fuse_configuration())
        return FuseCommand(fuse_type=fuse_type, configurations=configs)

    def _parse_fuse_configuration(self) -> FuseConfiguration:
        if self._check(TokenType.SCORE):
            self._advance()
            self._expect(TokenType.BY)
            val = self._parse_qualified_name()
            return FuseConfiguration(kind="SCORE", value=val)
        if self._check(TokenType.KEY):
            self._advance()
            self._expect(TokenType.BY)
            fields = [self._parse_qualified_name()]
            while self._match(TokenType.COMMA):
                fields.append(self._parse_qualified_name())
            return FuseConfiguration(kind="KEY", value=fields)
        if self._check(TokenType.GROUP):
            self._advance()
            self._expect(TokenType.BY)
            val = self._parse_qualified_name()
            return FuseConfiguration(kind="GROUP", value=val)
        # WITH
        self._expect(TokenType.WITH)
        val = self._parse_map_expression()
        return FuseConfiguration(kind="WITH", value=val)

    def _parse_uri_parts(self) -> UriPartsCommand:
        self._expect(TokenType.URI_PARTS)
        target = self._parse_qualified_name()
        self._expect(TokenType.ASSIGN)
        source = self._parse_primary_expression()
        return UriPartsCommand(target=target, source=source)

    def _parse_registered_domain(self) -> RegisteredDomainCommand:
        self._expect(TokenType.REGISTERED_DOMAIN)
        target = self._parse_qualified_name()
        self._expect(TokenType.ASSIGN)
        source = self._parse_primary_expression()
        return RegisteredDomainCommand(target=target, source=source)

    def _parse_metrics_info(self) -> MetricsInfoCommand:
        self._expect(TokenType.METRICS_INFO)
        return MetricsInfoCommand()

    def _parse_ts_info(self) -> TsInfoCommand:
        self._expect(TokenType.TS_INFO)
        return TsInfoCommand()

    def _parse_mmr_stub(self) -> ProcessingCommand:
        # Consume MMR and skip until next PIPE or EOF (best-effort)
        self._advance()
        while not self._check(TokenType.PIPE, TokenType.EOF):
            self._advance()
        from .ast import ProcessingCommand as PC

        return PC()  # stub

    def _parse_insist(self) -> InsistCommand:
        self._expect(TokenType.INSIST)
        return InsistCommand(patterns=self._parse_qualified_name_patterns())

    def _parse_set_command(self) -> SetCommand:
        self._expect(TokenType.SET)
        name = self._parse_identifier_string()
        self._expect(TokenType.ASSIGN)
        if self._check(TokenType.LEFT_BRACES):
            val = self._parse_map_expression()
        else:
            val = self._parse_constant()
        return SetCommand(name=name, value=val)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _parse_fields(self) -> list[Field]:
        fields = [self._parse_field()]
        while self._match(TokenType.COMMA):
            fields.append(self._parse_field())
        return fields

    def _parse_field(self) -> Field:
        # Optional: name = expr   OR just expr
        name: QualifiedName | None = None
        if self._is_qualified_name_ahead() and self._peek_type != TokenType.LP:
            # Lookahead: if followed by ASSIGN, it's a named field
            saved = self._pos
            try:
                qn = self._parse_qualified_name()
                if self._match(TokenType.ASSIGN):
                    name = qn
                else:
                    # Not an assignment – reset and parse as expression
                    self._pos = saved
            except EsqlSyntaxError:
                self._pos = saved
        expr = self._parse_boolean_expression()
        return Field(name=name, expr=expr)

    def _parse_agg_fields(self) -> list[AggField]:
        fields = [self._parse_agg_field()]
        while self._match(TokenType.COMMA):
            fields.append(self._parse_agg_field())
        return fields

    def _parse_agg_field(self) -> AggField:
        f = self._parse_field()
        where: object | None = None
        if self._match(TokenType.WHERE):
            where = self._parse_boolean_expression()
        return AggField(field=f, where=where)

    def _parse_order_expression(self) -> OrderExpression:
        expr = self._parse_boolean_expression()
        order: str | None = None
        nulls: str | None = None
        if self._check(TokenType.ASC, TokenType.DESC):
            order = self._advance().value.upper()
        if self._match(TokenType.NULLS):
            tok = self._expect(TokenType.FIRST, TokenType.LAST)
            nulls = tok.value.upper()
        return OrderExpression(expr=expr, order=order, nulls=nulls)

    def _parse_rename_clause(self) -> RenameClause:
        first = self._parse_qualified_name_pattern()
        if self._match(TokenType.AS):
            second = self._parse_qualified_name_pattern()
            return RenameClause(old_name=first, new_name=second)
        self._expect(TokenType.ASSIGN)
        second = self._parse_qualified_name_pattern()
        return RenameClause(new_name=first, old_name=second)

    def _parse_command_named_parameters(self) -> MapExpr | None:
        if self._match(TokenType.WITH):
            return self._parse_map_expression()
        return None

    # ------------------------------------------------------------------
    # Expressions – boolean level
    # ------------------------------------------------------------------

    def _parse_boolean_expression(self) -> object:
        if self._check(TokenType.NOT):
            self._advance()
            return LogicalNot(operand=self._parse_boolean_expression())
        return self._parse_or_expression()

    def _parse_or_expression(self) -> object:
        left = self._parse_and_expression()
        while self._check(TokenType.OR):
            self._advance()
            right = self._parse_and_expression()
            left = LogicalBinary(operator="OR", left=left, right=right)
        return left

    def _parse_and_expression(self) -> object:
        left = self._parse_unary_boolean()
        while self._check(TokenType.AND):
            self._advance()
            right = self._parse_unary_boolean()
            left = LogicalBinary(operator="AND", left=left, right=right)
        return left

    def _parse_unary_boolean(self) -> object:
        if self._check(TokenType.NOT):
            self._advance()
            return LogicalNot(operand=self._parse_unary_boolean())
        return self._parse_predicate()

    def _parse_predicate(self) -> object:
        left = self._parse_value_expression()

        # IS [NOT] NULL
        if self._check(TokenType.IS):
            self._advance()
            negated = bool(self._match(TokenType.NOT))
            self._expect(TokenType.NULL)
            return IsNull(expr=left, negated=negated)

        # [NOT] IN (...)
        negated = False
        if self._check(TokenType.NOT):
            # peek ahead – only consume NOT if followed by IN/LIKE/RLIKE
            nxt = self._peek_ahead(1)
            if nxt.type in (TokenType.IN, TokenType.LIKE, TokenType.RLIKE):
                self._advance()
                negated = True

        if self._check(TokenType.IN):
            self._advance()
            self._expect(TokenType.LP)
            values = [self._parse_value_expression()]
            while self._match(TokenType.COMMA):
                values.append(self._parse_value_expression())
            self._expect(TokenType.RP)
            return InList(expr=left, values=values, negated=negated)

        if self._check(TokenType.LIKE):
            self._advance()
            if self._check(TokenType.LP):
                self._advance()
                patterns = [self._parse_string_or_parameter()]
                while self._match(TokenType.COMMA):
                    patterns.append(self._parse_string_or_parameter())
                self._expect(TokenType.RP)
                return LikeExpr(expr=left, patterns=patterns, negated=negated, is_list=True)
            return LikeExpr(
                expr=left, patterns=[self._parse_string_or_parameter()], negated=negated
            )

        if self._check(TokenType.RLIKE):
            self._advance()
            if self._check(TokenType.LP):
                self._advance()
                patterns = [self._parse_string_or_parameter()]
                while self._match(TokenType.COMMA):
                    patterns.append(self._parse_string_or_parameter())
                self._expect(TokenType.RP)
                return RlikeExpr(expr=left, patterns=patterns, negated=negated, is_list=True)
            return RlikeExpr(
                expr=left, patterns=[self._parse_string_or_parameter()], negated=negated
            )

        # Match expression:  field[:type] : value
        if self._check(TokenType.COLON) and isinstance(left, FieldRef):
            # could also be  field::type : value
            field_type: str | None = None
            if self._check(TokenType.CAST_OP):
                self._advance()
                field_type = self._parse_identifier_string()
            self._expect(TokenType.COLON)
            value = self._parse_constant()
            return MatchExpr(field=left.name, field_type=field_type, value=value)

        if negated:
            # NOT without IN/LIKE/RLIKE – put NOT back, re-interpret
            # This shouldn't happen in valid ESQL but handle gracefully
            self._pos -= 1
        return left

    # ------------------------------------------------------------------
    # Expressions – value level
    # ------------------------------------------------------------------

    def _parse_value_expression(self) -> object:
        left = self._parse_operator_expression()
        if self._check(
            TokenType.EQ,
            TokenType.CIEQ,
            TokenType.NEQ,
            TokenType.LT,
            TokenType.LTE,
            TokenType.GT,
            TokenType.GTE,
        ):
            op = self._advance().value
            right = self._parse_operator_expression()
            return Comparison(operator=op, left=left, right=right)
        return left

    def _parse_operator_expression(self) -> object:
        """Entry point for arithmetic; dispatches to additive (lowest precedence)."""
        return self._parse_additive()

    def _parse_additive(self) -> object:
        """Handles + and - (lower precedence than * / %)."""
        left = self._parse_multiplicative()
        while self._check(TokenType.PLUS, TokenType.MINUS):
            op = self._advance().value
            right = self._parse_multiplicative()
            left = ArithmeticBinary(operator=op, left=left, right=right)
        return left

    def _parse_multiplicative(self) -> object:
        """Handles * / % (higher precedence than + -)."""
        left = self._parse_unary_arith()
        while self._check(TokenType.ASTERISK, TokenType.SLASH, TokenType.PERCENT):
            op = self._advance().value
            right = self._parse_unary_arith()
            left = ArithmeticBinary(operator=op, left=left, right=right)
        return left

    def _parse_unary_arith(self) -> object:
        """Handles unary + and - (highest arithmetic precedence)."""
        if self._check(TokenType.MINUS, TokenType.PLUS):
            op = self._advance().value
            operand = self._parse_unary_arith()
            return ArithmeticUnary(operator=op, operand=operand)
        return self._parse_cast()

    def _parse_cast(self) -> object:
        expr = self._parse_primary_expression()
        while self._check(TokenType.CAST_OP):
            self._advance()
            dt = self._parse_identifier_string()
            expr = InlineCast(expr=expr, data_type=dt)
        return expr

    # ------------------------------------------------------------------
    # Primary expressions
    # ------------------------------------------------------------------

    def _parse_primary_expression(self) -> object:
        tok = self._current

        # Parenthesized
        if tok.type == TokenType.LP:
            self._advance()
            inner = self._parse_boolean_expression()
            self._expect(TokenType.RP)
            return inner

        # Constants
        if tok.type in _CONSTANT_STARTS:
            return self._parse_constant()

        # Function call or field reference
        if tok.type in _NAME_TYPES or _is_keyword_usable_as_name(tok.type):
            # Peek ahead for LP → function call
            if self._is_function_call_ahead():
                return self._parse_function_call()
            return FieldRef(name=self._parse_qualified_name())

        raise self._error(f"Expected expression, got {tok.type.name!r} ({tok.value!r})")

    def _parse_function_call(self) -> FunctionCall:
        name = self._parse_function_name()
        self._expect(TokenType.LP)
        star = False
        args: list[object] = []
        options: MapExpr | None = None

        if self._check(TokenType.RP):
            pass  # zero args
        elif self._check(TokenType.ASTERISK):
            self._advance()
            star = True
        else:
            args.append(self._parse_boolean_expression())
            while self._match(TokenType.COMMA):
                if self._check(TokenType.LEFT_BRACES):
                    options = self._parse_map_expression()
                    break
                args.append(self._parse_boolean_expression())

        self._expect(TokenType.RP)
        return FunctionCall(name=name, args=args, star=star, options=options)

    def _parse_function_name(self) -> str:
        tok = self._current
        if tok.type in (TokenType.FIRST, TokenType.LAST):
            self._advance()
            return tok.value.upper()
        return self._parse_identifier_string()

    # ------------------------------------------------------------------
    # Constants / literals
    # ------------------------------------------------------------------

    def _parse_constant(self) -> object:
        tok = self._current

        if tok.type == TokenType.NULL:
            self._advance()
            return NullLiteral()

        if tok.type == TokenType.TRUE:
            self._advance()
            return BooleanLiteral(value=True)

        if tok.type == TokenType.FALSE:
            self._advance()
            return BooleanLiteral(value=False)

        if tok.type == TokenType.QUOTED_STRING:
            self._advance()
            return StringLiteral(value=tok.value)

        if tok.type == TokenType.DECIMAL:
            self._advance()
            return DecimalLiteral(value=float(tok.value))

        if tok.type == TokenType.INTEGER:
            self._advance()
            val = int(tok.value)
            # qualified integer: 1d, 2h, etc.
            if self._check(TokenType.IDENTIFIER):
                unit = self._advance().value
                return IntegerLiteral(value=val, unit=unit)
            return IntegerLiteral(value=val)

        if tok.type in (TokenType.PLUS, TokenType.MINUS):
            sign = self._advance().value
            inner = self._parse_constant()
            if isinstance(inner, (IntegerLiteral, DecimalLiteral)):
                if sign == "-":
                    if isinstance(inner, IntegerLiteral):
                        inner.value = -inner.value
                    else:
                        inner.value = -inner.value
            return inner

        if tok.type == TokenType.PARAM:
            self._advance()
            return Parameter(name=None)

        if tok.type == TokenType.NAMED_OR_POSITIONAL_PARAM:
            self._advance()
            return Parameter(name=tok.value[1:])  # strip leading ?

        if tok.type == TokenType.DOUBLE_PARAMS:
            self._advance()
            return DoubleParameter(name=None)

        if tok.type == TokenType.NAMED_OR_POSITIONAL_DOUBLE_PARAMS:
            self._advance()
            return DoubleParameter(name=tok.value[2:])

        # Array literals
        if tok.type == TokenType.OPENING_BRACKET:
            return self._parse_array_literal()

        raise self._error(f"Expected constant, got {tok.type.name!r} ({tok.value!r})")

    def _parse_array_literal(self) -> object:
        self._expect(TokenType.OPENING_BRACKET)
        first = self._parse_constant()
        values = [first]
        while self._match(TokenType.COMMA):
            values.append(self._parse_constant())
        self._expect(TokenType.CLOSING_BRACKET)

        if all(isinstance(v, (IntegerLiteral, DecimalLiteral)) for v in values):
            return NumericArrayLiteral(values=values)  # type: ignore[arg-type]
        if all(isinstance(v, BooleanLiteral) for v in values):
            return BooleanArrayLiteral(values=values)  # type: ignore[arg-type]
        if all(isinstance(v, StringLiteral) for v in values):
            return StringArrayLiteral(values=values)  # type: ignore[arg-type]
        # Mixed – return as NumericArrayLiteral best-effort
        return NumericArrayLiteral(values=values)  # type: ignore[arg-type]

    def _parse_string_or_parameter(self) -> object:
        if self._check(TokenType.QUOTED_STRING):
            tok = self._advance()
            return StringLiteral(value=tok.value)
        return self._parse_parameter()

    def _parse_parameter(self) -> object:
        tok = self._current
        if tok.type == TokenType.PARAM:
            self._advance()
            return Parameter(name=None)
        if tok.type == TokenType.NAMED_OR_POSITIONAL_PARAM:
            self._advance()
            return Parameter(name=tok.value[1:])
        raise self._error(f"Expected parameter, got {tok.type.name!r}")

    # ------------------------------------------------------------------
    # Map expressions
    # ------------------------------------------------------------------

    def _parse_map_expression(self) -> MapExpr:
        self._expect(TokenType.LEFT_BRACES)
        entries: list[MapEntry] = []
        if not self._check(TokenType.RIGHT_BRACES):
            entries.append(self._parse_map_entry())
            while self._match(TokenType.COMMA):
                if self._check(TokenType.RIGHT_BRACES):
                    break
                entries.append(self._parse_map_entry())
        self._expect(TokenType.RIGHT_BRACES)
        return MapExpr(entries=entries)

    def _parse_map_entry(self) -> MapEntry:
        key_tok = self._expect(TokenType.QUOTED_STRING)
        self._expect(TokenType.COLON)
        if self._check(TokenType.LEFT_BRACES):
            val = self._parse_map_expression()
        else:
            val = self._parse_constant()
        return MapEntry(key=key_tok.value, value=val)

    # ------------------------------------------------------------------
    # Names / identifiers
    # ------------------------------------------------------------------

    def _parse_qualified_name(self) -> QualifiedName:
        parts = [self._parse_identifier_string()]
        while self._check(TokenType.DOT):
            self._advance()
            parts.append(self._parse_identifier_string())
        return QualifiedName(parts=parts)

    def _parse_qualified_name_pattern(self) -> QualifiedNamePattern:
        parts = [self._parse_identifier_or_pattern_string()]
        while self._check(TokenType.DOT):
            self._advance()
            parts.append(self._parse_identifier_or_pattern_string())
        return QualifiedNamePattern(parts=parts)

    def _parse_qualified_name_patterns(self) -> list[QualifiedNamePattern]:
        patterns = [self._parse_qualified_name_pattern()]
        while self._match(TokenType.COMMA):
            patterns.append(self._parse_qualified_name_pattern())
        return patterns

    def _parse_identifier_string(self) -> str:
        tok = self._current
        if tok.type == TokenType.IDENTIFIER:
            self._advance()
            return tok.value
        if tok.type == TokenType.QUOTED_IDENTIFIER:
            self._advance()
            return tok.value
        if _is_keyword_usable_as_name(tok.type):
            self._advance()
            return tok.value
        raise self._error(f"Expected identifier, got {tok.type.name!r} ({tok.value!r})")

    def _parse_identifier_or_pattern_string(self) -> str:
        tok = self._current
        if tok.type == TokenType.ID_PATTERN:
            self._advance()
            return tok.value
        if tok.type == TokenType.ASTERISK:
            self._advance()
            return "*"
        return self._parse_identifier_string()

    def _parse_unquoted_identifier(self) -> str:
        tok = self._expect(TokenType.IDENTIFIER)
        return tok.value

    # ------------------------------------------------------------------
    # Lookahead helpers
    # ------------------------------------------------------------------

    def _is_qualified_name_ahead(self) -> bool:
        tt = self._peek_type
        return tt in _NAME_TYPES or _is_keyword_usable_as_name(tt)

    def _is_function_call_ahead(self) -> bool:
        """True if current position looks like  name ( ..."""
        saved = self._pos
        try:
            # skip over qualified name parts
            while self._peek_type in _NAME_TYPES or _is_keyword_usable_as_name(self._peek_type):
                self._advance()
                if not self._check(TokenType.DOT):
                    break
                self._advance()
            result = self._peek_type == TokenType.LP
        finally:
            self._pos = saved
        return result

    def _expect_keyword(self, keyword: str) -> Token:
        tok = self._current
        if tok.value.lower() != keyword:
            raise self._error(f"Expected keyword {keyword!r}")
        return self._advance()


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------

_CONSTANT_STARTS = {
    TokenType.NULL,
    TokenType.TRUE,
    TokenType.FALSE,
    TokenType.QUOTED_STRING,
    TokenType.INTEGER,
    TokenType.DECIMAL,
    TokenType.PARAM,
    TokenType.NAMED_OR_POSITIONAL_PARAM,
    TokenType.DOUBLE_PARAMS,
    TokenType.NAMED_OR_POSITIONAL_DOUBLE_PARAMS,
    TokenType.OPENING_BRACKET,
    TokenType.PLUS,
    TokenType.MINUS,
}

_NAME_TYPES = {
    TokenType.IDENTIFIER,
    TokenType.QUOTED_IDENTIFIER,
    TokenType.ID_PATTERN,
}

# Keywords that may legally appear as field/function names in expressions
_KEYWORD_AS_NAME = {
    TokenType.FROM,
    TokenType.INFO,
    TokenType.BY,
    TokenType.ON,
    TokenType.AS,
    TokenType.WITH,
    TokenType.ASC,
    TokenType.DESC,
    TokenType.FIRST,
    TokenType.LAST,
    TokenType.METADATA,
    TokenType.SCORE,
    TokenType.KEY,
    TokenType.GROUP,
    TokenType.TS,
    TokenType.MMR,
    TokenType.NULLS,
    # Many ESQL functions share names with keywords
    TokenType.SAMPLE,
    TokenType.SHOW,
}


def _is_keyword_usable_as_name(tt: TokenType) -> bool:
    return tt in _KEYWORD_AS_NAME


def _is_unquoted_source(tok: Token) -> bool:
    return tok.type in (
        TokenType.IDENTIFIER,
        TokenType.ID_PATTERN,
        TokenType.ASTERISK,
        TokenType.QUOTED_STRING,
    )


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------


def parse(
    text: str,
    *,
    schema: "Schema | None" = None,
    on_unknown: str = "error",
    on_type_mismatch: str = "error",
) -> Query:
    """Parse an ES|QL query string and return the root Query AST node.

    Parameters
    ----------
    text:
        The ES|QL query string.
    schema:
        Optional :class:`~pyesql.schema.Schema`.  When supplied, the returned
        AST is validated against it before being returned.  If validation finds
        errors a :class:`~pyesql.validator.SchemaValidationError` is raised.
    on_unknown:
        Strictness level for unknown field references when *schema* is given.
        One of ``"error"`` (default), ``"warn"``, or ``"silent"``.
    on_type_mismatch:
        Strictness level for type-incompatible literals when *schema* is given.
        One of ``"error"`` (default), ``"warn"``, or ``"silent"``.
    """
    tokens = tokenize(text)
    query = Parser(tokens).parse()

    if schema is not None:
        from .validator import SchemaValidator  # local import avoids circular dep

        SchemaValidator(
            schema,
            on_unknown=on_unknown,  # type: ignore[arg-type]
            on_type_mismatch=on_type_mismatch,  # type: ignore[arg-type]
        ).validate(query)

    return query


# TYPE_CHECKING-only import so the annotation above resolves without a
# circular dependency at runtime.
from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from .schema import Schema  # noqa: F401
