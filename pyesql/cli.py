"""
CLI entry point for pyesql.

Usage:
    pyesql parse "FROM logs | WHERE status == 200 | LIMIT 10"
    echo "FROM logs | LIMIT 5" | pyesql parse -
    pyesql parse -f query.esql
"""

import argparse
import json
import sys
from typing import Any

from . import __version__
from .ast import Node
from .errors import EsqlSyntaxError
from .parser import parse


def _node_to_dict(node: Any) -> Any:
    """Recursively convert AST nodes to JSON-serialisable dicts."""
    if isinstance(node, Node):
        d = {"_type": type(node).__name__}
        for k, v in node.__dict__.items():
            d[k] = _node_to_dict(v)
        return d
    if isinstance(node, list):
        return [_node_to_dict(i) for i in node]
    return node


def _cmd_parse(args: argparse.Namespace) -> int:
    if args.file:
        with open(args.file) as fh:
            text = fh.read()
    elif args.query == "-":
        text = sys.stdin.read()
    else:
        text = args.query

    try:
        tree = parse(text)
    except EsqlSyntaxError as exc:
        print(f"Syntax error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(json.dumps(_node_to_dict(tree), indent=2))
    elif args.format == "repr":
        print(repr(tree))
    else:
        # tree summary
        _print_tree(tree, indent=0)
    return 0


def _print_tree(node: Any, indent: int) -> None:
    prefix = "  " * indent
    if isinstance(node, Node):
        name = type(node).__name__
        # show short preview of key scalar fields
        preview_parts = []
        for k, v in node.__dict__.items():
            if isinstance(v, (str, int, float, bool)) and v is not None:
                preview_parts.append(f"{k}={v!r}")
        preview = f"  [{', '.join(preview_parts[:3])}]" if preview_parts else ""
        print(f"{prefix}{name}{preview}")
        for child in node.children():
            _print_tree(child, indent + 1)
    elif isinstance(node, list):
        for item in node:
            _print_tree(item, indent)


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="pyesql",
        description="ES|QL query parser",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    sub = parser.add_subparsers(dest="command", required=True)

    p_parse = sub.add_parser("parse", help="Parse a query and print the AST")
    p_parse.add_argument(
        "query", nargs="?", default="-", help="ES|QL query string (use - to read from stdin)"
    )
    p_parse.add_argument("-f", "--file", metavar="FILE", help="Read query from file instead")
    p_parse.add_argument(
        "--format",
        choices=["tree", "json", "repr"],
        default="tree",
        help="Output format (default: tree)",
    )

    args = parser.parse_args()
    if args.command == "parse":
        return _cmd_parse(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
