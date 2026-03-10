"""Exceptions raised by the pyesql parser."""


class EsqlError(Exception):
    """Base exception for all pyesql errors."""


class EsqlSyntaxError(EsqlError):
    """Raised when a query cannot be parsed."""

    def __init__(self, message: str, line: int = 0, col: int = 0, text: str = ""):
        self.line = line
        self.col = col
        self.text = text
        loc = f" at line {line}, col {col}" if line or col else ""
        near = f" near {text!r}" if text else ""
        super().__init__(f"{message}{loc}{near}")


class EsqlParseError(EsqlError):
    """Raised for structural/semantic parse issues."""
