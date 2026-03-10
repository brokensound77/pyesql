"""
Utility functions for walking and querying AST trees.
"""

from collections.abc import Callable, Iterator

from .ast import Node


def walk(node: Node) -> Iterator[Node]:
    """Yield every node in the subtree rooted at *node* (depth-first, pre-order)."""
    yield node
    for child in node.children():
        yield from walk(child)


def find_all[T: Node](node: Node, node_type: type[T]) -> list[T]:
    """Return all nodes of the given type anywhere in the tree."""
    return [n for n in walk(node) if isinstance(n, node_type)]


def find_first[T: Node](node: Node, node_type: type[T]) -> T | None:
    """Return the first node of the given type, or None."""
    for n in walk(node):
        if isinstance(n, node_type):
            return n  # type: ignore[return-value]
    return None


def filter_nodes(node: Node, predicate: Callable[[Node], bool]) -> list[Node]:
    """Return all nodes satisfying *predicate*."""
    return [n for n in walk(node) if predicate(n)]
