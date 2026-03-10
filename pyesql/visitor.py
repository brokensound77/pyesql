"""
Visitor base class for ES|QL AST nodes.

Subclass Visitor and override visit_<ClassName> methods.
Call visitor.visit(node) to dispatch.

Example::

    class FieldCollector(Visitor):
        def __init__(self):
            self.fields = []

        def visit_FieldRef(self, node):
            self.fields.append(str(node.name))
            self.generic_visit(node)

    collector = FieldCollector()
    collector.visit(query)
    print(collector.fields)
"""

from typing import Any

from .ast import Node


class Visitor:
    """Base visitor.  Override visit_<ClassName>(self, node) for each node type."""

    def visit(self, node: Node) -> Any:
        method_name = f"visit_{type(node).__name__}"
        method = getattr(self, method_name, self.generic_visit)
        return method(node)

    def generic_visit(self, node: Node) -> None:
        """Default: visit all child nodes."""
        for child in node.children():
            self.visit(child)


class Transformer(Visitor):
    """
    Like Visitor but returns (possibly modified) nodes.

    Override visit_<ClassName> to return a replacement node.
    generic_visit rebuilds the node with transformed children.
    """

    def generic_visit(self, node: Node) -> Node:
        for attr, val in list(node.__dict__.items()):
            if isinstance(val, Node):
                setattr(node, attr, self.visit(val))
            elif isinstance(val, list):
                new_list = []
                for item in val:
                    if isinstance(item, Node):
                        new_list.append(self.visit(item))
                    else:
                        new_list.append(item)
                setattr(node, attr, new_list)
        return node
