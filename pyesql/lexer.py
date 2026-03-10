"""
Tokenizer for ES|QL queries.

Produces a flat list of Token objects from a raw query string.
The lexer is case-insensitive for keywords (stored as uppercase).
"""

import re
from dataclasses import dataclass
from enum import Enum, auto

from .errors import EsqlSyntaxError


class TokenType(Enum):
    # Literals
    INTEGER = auto()
    DECIMAL = auto()
    QUOTED_STRING = auto()  # "..." or """..."""
    TRUE = auto()
    FALSE = auto()
    NULL = auto()

    # Identifiers / names
    IDENTIFIER = auto()  # unquoted
    QUOTED_IDENTIFIER = auto()  # `...`
    ID_PATTERN = auto()  # contains * wildcard

    # Keywords – source commands
    FROM = auto()
    ROW = auto()
    SHOW = auto()
    TS = auto()
    PROMQL = auto()
    EXPLAIN = auto()

    # Keywords – processing commands
    EVAL = auto()
    WHERE = auto()
    KEEP = auto()
    LIMIT = auto()
    STATS = auto()
    SORT = auto()
    DROP = auto()
    RENAME = auto()
    DISSECT = auto()
    GROK = auto()
    ENRICH = auto()
    MV_EXPAND = auto()
    JOIN = auto()
    LOOKUP = auto()
    CHANGE_POINT = auto()
    COMPLETION = auto()
    SAMPLE = auto()
    FORK = auto()
    RERANK = auto()
    INLINE = auto()
    INLINESTATS = auto()
    FUSE = auto()
    URI_PARTS = auto()
    METRICS_INFO = auto()
    REGISTERED_DOMAIN = auto()
    TS_INFO = auto()
    MMR = auto()
    INSIST = auto()

    # Keywords – misc
    METADATA = auto()
    INFO = auto()
    SET = auto()
    BY = auto()
    ON = auto()
    AS = auto()
    WITH = auto()
    ASC = auto()
    DESC = auto()
    NULLS = auto()
    FIRST = auto()
    LAST = auto()
    IN = auto()
    IS = auto()
    NOT = auto()
    AND = auto()
    OR = auto()
    LIKE = auto()
    RLIKE = auto()
    SCORE = auto()
    KEY = auto()
    GROUP = auto()
    FULL = auto()
    LEFT = auto()
    RIGHT = auto()
    OUTER = auto()
    USING = auto()

    # Operators
    EQ = auto()  # ==
    CIEQ = auto()  # =~
    NEQ = auto()  # !=
    LTE = auto()  # <=
    LT = auto()  # <
    GTE = auto()  # >=
    GT = auto()  # >
    ASSIGN = auto()  # =
    CAST_OP = auto()  # ::
    COLON = auto()  # :
    SEMICOLON = auto()
    PIPE = auto()  # |
    COMMA = auto()
    DOT = auto()
    PLUS = auto()
    MINUS = auto()
    ASTERISK = auto()
    SLASH = auto()
    PERCENT = auto()

    # Brackets
    LP = auto()  # (
    RP = auto()  # )
    OPENING_BRACKET = auto()  # [
    CLOSING_BRACKET = auto()  # ]
    LEFT_BRACES = auto()  # {
    RIGHT_BRACES = auto()  # }

    # Parameters
    PARAM = auto()  # ?
    NAMED_OR_POSITIONAL_PARAM = auto()  # ?name or ?1
    DOUBLE_PARAMS = auto()  # ??
    NAMED_OR_POSITIONAL_DOUBLE_PARAMS = auto()  # ??name or ??1

    # Special
    UNQUOTED_SOURCE = auto()  # used in FROM/index patterns
    EOF = auto()


# Maps lowercase keyword text -> TokenType
_KEYWORDS: dict[str, TokenType] = {
    "from": TokenType.FROM,
    "row": TokenType.ROW,
    "show": TokenType.SHOW,
    "ts": TokenType.TS,
    "promql": TokenType.PROMQL,
    "explain": TokenType.EXPLAIN,
    "eval": TokenType.EVAL,
    "where": TokenType.WHERE,
    "keep": TokenType.KEEP,
    "limit": TokenType.LIMIT,
    "stats": TokenType.STATS,
    "sort": TokenType.SORT,
    "drop": TokenType.DROP,
    "rename": TokenType.RENAME,
    "dissect": TokenType.DISSECT,
    "grok": TokenType.GROK,
    "enrich": TokenType.ENRICH,
    "mv_expand": TokenType.MV_EXPAND,
    "join": TokenType.JOIN,
    "lookup": TokenType.LOOKUP,
    "change_point": TokenType.CHANGE_POINT,
    "completion": TokenType.COMPLETION,
    "sample": TokenType.SAMPLE,
    "fork": TokenType.FORK,
    "rerank": TokenType.RERANK,
    "inline": TokenType.INLINE,
    "inlinestats": TokenType.INLINESTATS,
    "fuse": TokenType.FUSE,
    "uri_parts": TokenType.URI_PARTS,
    "metrics_info": TokenType.METRICS_INFO,
    "registered_domain": TokenType.REGISTERED_DOMAIN,
    "ts_info": TokenType.TS_INFO,
    "mmr": TokenType.MMR,
    "insist": TokenType.INSIST,
    "metadata": TokenType.METADATA,
    "info": TokenType.INFO,
    "set": TokenType.SET,
    "by": TokenType.BY,
    "on": TokenType.ON,
    "as": TokenType.AS,
    "with": TokenType.WITH,
    "asc": TokenType.ASC,
    "desc": TokenType.DESC,
    "nulls": TokenType.NULLS,
    "first": TokenType.FIRST,
    "last": TokenType.LAST,
    "in": TokenType.IN,
    "is": TokenType.IS,
    "not": TokenType.NOT,
    "and": TokenType.AND,
    "or": TokenType.OR,
    "like": TokenType.LIKE,
    "rlike": TokenType.RLIKE,
    "true": TokenType.TRUE,
    "false": TokenType.FALSE,
    "null": TokenType.NULL,
    "score": TokenType.SCORE,
    "key": TokenType.KEY,
    "group": TokenType.GROUP,
    "full": TokenType.FULL,
    "left": TokenType.LEFT,
    "right": TokenType.RIGHT,
    "outer": TokenType.OUTER,
    "using": TokenType.USING,
}


@dataclass
class Token:
    type: TokenType
    value: str
    line: int
    col: int

    def __repr__(self) -> str:
        return f"Token({self.type.name}, {self.value!r}, {self.line}:{self.col})"


def tokenize(text: str) -> list[Token]:
    """Tokenize an ES|QL query string into a list of Tokens."""
    tokens: list[Token] = []
    pos = 0
    line = 1
    line_start = 0
    n = len(text)

    def col() -> int:
        return pos - line_start + 1

    def error(msg: str) -> EsqlSyntaxError:
        return EsqlSyntaxError(msg, line, col(), text[pos : pos + 10])

    def add(ttype: TokenType, value: str, tok_line: int, tok_col: int) -> None:
        tokens.append(Token(ttype, value, tok_line, tok_col))

    while pos < n:
        tok_line = line
        tok_col = col()
        ch = text[pos]

        # Whitespace
        if ch in " \t\r\n":
            if ch == "\n":
                line += 1
                line_start = pos + 1
            pos += 1
            continue

        # Line comment
        if text[pos : pos + 2] == "//":
            end = text.find("\n", pos)
            if end == -1:
                pos = n
            else:
                pos = end  # newline handled next iteration
            continue

        # Multiline comment
        if text[pos : pos + 2] == "/*":
            end = text.find("*/", pos + 2)
            if end == -1:
                raise error("Unterminated block comment")
            # count newlines inside comment
            for c in text[pos : end + 2]:
                if c == "\n":
                    line += 1
                    line_start = pos + text[pos : end + 2].index("\n") + 1
            chunk = text[pos : end + 2]
            line_start = pos + chunk.rfind("\n") + 1 if "\n" in chunk else line_start
            pos = end + 2
            continue

        # Triple-quoted string  """..."""
        if text[pos : pos + 3] == '"""':
            end = text.find('"""', pos + 3)
            if end == -1:
                raise error("Unterminated triple-quoted string")
            raw = text[pos + 3 : end]
            # consume optional trailing quotes
            end += 3
            while end < n and text[end] == '"' and end - (pos + 3) < 2:
                end += 1
            for c in raw:
                if c == "\n":
                    line += 1
            add(TokenType.QUOTED_STRING, raw, tok_line, tok_col)
            pos = end
            continue

        # Quoted string  "..."
        if ch == '"':
            i = pos + 1
            buf = []
            while i < n:
                c = text[i]
                if c == "\\":
                    if i + 1 >= n:
                        raise error("Unterminated string escape")
                    esc = text[i + 1]
                    mapping = {"t": "\t", "n": "\n", "r": "\r", '"': '"', "\\": "\\"}
                    buf.append(mapping.get(esc, esc))
                    i += 2
                elif c == '"':
                    i += 1
                    break
                else:
                    if c == "\n":
                        line += 1
                        line_start = i + 1
                    buf.append(c)
                    i += 1
            else:
                raise error("Unterminated string literal")
            add(TokenType.QUOTED_STRING, "".join(buf), tok_line, tok_col)
            pos = i
            continue

        # Backtick-quoted identifier  `...`
        if ch == "`":
            i = pos + 1
            buf = []
            while i < n:
                c = text[i]
                if c == "`":
                    if i + 1 < n and text[i + 1] == "`":
                        buf.append("`")
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    buf.append(c)
                    i += 1
            else:
                raise error("Unterminated backtick identifier")
            add(TokenType.QUOTED_IDENTIFIER, "".join(buf), tok_line, tok_col)
            pos = i
            continue

        # Numbers
        if ch.isdigit() or (ch == "." and pos + 1 < n and text[pos + 1].isdigit()):
            m = re.match(
                r"""(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?""",
                text[pos:],
                re.IGNORECASE,
            )
            if m:
                raw = m.group(0)
                is_decimal = "." in raw or "e" in raw.lower()
                ttype = TokenType.DECIMAL if is_decimal else TokenType.INTEGER
                add(ttype, raw, tok_line, tok_col)
                pos += len(raw)
                continue

        # Two-character operators (must come before single-char)
        two = text[pos : pos + 2]
        if two == "::":
            add(TokenType.CAST_OP, "::", tok_line, tok_col)
            pos += 2
            continue
        if two == "==":
            add(TokenType.EQ, "==", tok_line, tok_col)
            pos += 2
            continue
        if two == "=~":
            add(TokenType.CIEQ, "=~", tok_line, tok_col)
            pos += 2
            continue
        if two == "!=":
            add(TokenType.NEQ, "!=", tok_line, tok_col)
            pos += 2
            continue
        if two == "<=":
            add(TokenType.LTE, "<=", tok_line, tok_col)
            pos += 2
            continue
        if two == ">=":
            add(TokenType.GTE, ">=", tok_line, tok_col)
            pos += 2
            continue
        if two == "??":
            # check for named/positional double param
            m = re.match(r"\?\?([a-zA-Z_]\w*|\d+)", text[pos:])
            if m:
                add(TokenType.NAMED_OR_POSITIONAL_DOUBLE_PARAMS, m.group(0), tok_line, tok_col)
                pos += len(m.group(0))
            else:
                add(TokenType.DOUBLE_PARAMS, "??", tok_line, tok_col)
                pos += 2
            continue

        # Single-char operators
        single_map = {
            "<": TokenType.LT,
            ">": TokenType.GT,
            "=": TokenType.ASSIGN,
            ":": TokenType.COLON,
            ";": TokenType.SEMICOLON,
            "|": TokenType.PIPE,
            ",": TokenType.COMMA,
            ".": TokenType.DOT,
            "+": TokenType.PLUS,
            "-": TokenType.MINUS,
            "*": TokenType.ASTERISK,
            "/": TokenType.SLASH,
            "%": TokenType.PERCENT,
            "(": TokenType.LP,
            ")": TokenType.RP,
            "[": TokenType.OPENING_BRACKET,
            "]": TokenType.CLOSING_BRACKET,
            "{": TokenType.LEFT_BRACES,
            "}": TokenType.RIGHT_BRACES,
        }
        if ch in single_map:
            add(single_map[ch], ch, tok_line, tok_col)
            pos += 1
            continue

        # Parameter ?name or ?1 or bare ?
        if ch == "?":
            m = re.match(r"\?([a-zA-Z_]\w*|\d+)", text[pos:])
            if m:
                add(TokenType.NAMED_OR_POSITIONAL_PARAM, m.group(0), tok_line, tok_col)
                pos += len(m.group(0))
            else:
                add(TokenType.PARAM, "?", tok_line, tok_col)
                pos += 1
            continue

        # Identifier or keyword (possibly with * for wildcard patterns)
        if ch.isalpha() or ch == "_" or ch == "@":
            pattern = r"[a-zA-Z_@][a-zA-Z0-9_]*(\*[a-zA-Z0-9_]*)*(\.[a-zA-Z_@*][a-zA-Z0-9_*]*)*\*?"
            m = re.match(pattern, text[pos:])
            if m:
                raw = m.group(0)
                lower = raw.lower()
                if "*" in raw:
                    ttype = TokenType.ID_PATTERN
                elif lower in _KEYWORDS:
                    ttype = _KEYWORDS[lower]
                else:
                    ttype = TokenType.IDENTIFIER
                add(ttype, raw, tok_line, tok_col)
                pos += len(raw)
                continue

        # Unquoted source patterns (e.g. index names with hyphens/slashes after FROM)
        # handled contextually in the parser using raw source scanning
        if ch == "*":
            add(TokenType.ASTERISK, "*", tok_line, tok_col)
            pos += 1
            continue

        raise error(f"Unexpected character {ch!r}")

    tokens.append(Token(TokenType.EOF, "", line, col()))
    return tokens
