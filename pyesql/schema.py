"""
Schema definitions for ES|QL field validation.

Supports three input formats, all normalised to a flat ``{dotted.path: type}``
dict internally:

- **Flat JSON** – ``{"process.pid": "integer", "host.name": "keyword"}``
- **Nested JSON** – ``{"process": {"pid": "integer"}, "host": {"name": "keyword"}}``
- **Elasticsearch mapping** – the ``mappings`` block (or full ``GET /<index>/_mapping``
  response) as returned by the Elasticsearch API.

Usage::

    from pyesql.schema import Schema

    # flat
    s = Schema.from_dict({"process.pid": "integer", "host.name": "keyword"})

    # nested
    s = Schema.from_dict({"process": {"pid": "integer"}, "host": {"name": "keyword"}})

    # ES mapping
    s = Schema.from_elasticsearch_mapping(mapping_dict)

    field_type = s.get_field_type("process.pid")   # "integer"
    field_type = s.get_field_type("unknown.field")  # None
"""

from __future__ import annotations

from typing import Any

#: All type strings accepted by the schema.
ESQL_TYPES: frozenset[str] = frozenset(
    {
        "boolean",
        "integer",
        "long",
        "short",
        "byte",
        "unsigned_long",
        "float",
        "double",
        "half_float",
        "scaled_float",
        "keyword",
        "text",
        "match_only_text",
        "wildcard",
        "constant_keyword",
        "date",
        "date_nanos",
        "ip",
        "version",
        "geo_point",
        "cartesian_point",
        "geo_shape",
        "cartesian_shape",
        "object",
        "nested",
        "flattened",
        "binary",
        "null",
        "aggregate_metric_double",
    }
)

# Convenience aliases accepted on input; normalised before storage.
_TYPE_ALIASES: dict[str, str] = {
    "string": "keyword",
    "int": "integer",
    "bool": "boolean",
    "number": "double",
    "numeric": "double",
    "str": "keyword",
}

# ---------------------------------------------------------------------------
# Type-compatibility groups (used by the validator)
# ---------------------------------------------------------------------------

NUMERIC_INTEGER_TYPES: frozenset[str] = frozenset(
    {"integer", "long", "short", "byte", "unsigned_long"}
)
NUMERIC_FLOAT_TYPES: frozenset[str] = frozenset({"float", "double", "half_float", "scaled_float"})
NUMERIC_TYPES: frozenset[str] = NUMERIC_INTEGER_TYPES | NUMERIC_FLOAT_TYPES

STRING_LIKE_TYPES: frozenset[str] = frozenset(
    {"keyword", "text", "match_only_text", "wildcard", "constant_keyword", "ip", "version"}
)
DATE_TYPES: frozenset[str] = frozenset({"date", "date_nanos"})
BOOLEAN_TYPES: frozenset[str] = frozenset({"boolean"})


def _normalize_type(raw: str) -> str:
    """Lowercase and resolve aliases."""
    t = raw.lower().strip()
    return _TYPE_ALIASES.get(t, t)


class Schema:
    """
    Flat field-type map for an ES|QL data source.

    All factory methods produce the same internal representation:
    ``{dotted.field.path: canonical_type_string}``.
    """

    def __init__(self, fields: dict[str, str]) -> None:
        """
        Create a schema from a pre-normalised flat ``{dotted.path: type}`` dict.

        Prefer :meth:`from_dict` or :meth:`from_elasticsearch_mapping`.
        """
        self._fields: dict[str, str] = {}
        for path, raw_type in fields.items():
            t = _normalize_type(raw_type)
            if t not in ESQL_TYPES:
                raise ValueError(
                    f"Unknown field type {raw_type!r} for field {path!r}. "
                    f"Valid types: {sorted(ESQL_TYPES)}"
                )
            self._fields[path] = t

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Schema:
        """
        Load a schema from a flat or nested (or mixed) JSON dict.

        Flat::

            {"process.pid": "integer", "process.name": "keyword"}

        Nested::

            {"process": {"pid": "integer", "name": "keyword"}}

        Mixed::

            {"process.pid": "integer", "host": {"name": "keyword"}}
        """
        fields: dict[str, str] = {}
        _flatten_dict(d, prefix="", out=fields)
        return cls(fields)

    @classmethod
    def from_elasticsearch_mapping(cls, mapping: dict[str, Any]) -> Schema:
        """
        Load a schema from an Elasticsearch index mapping.

        Accepts any of:

        - Full ``GET /<index>/_mapping`` response::

              {"my-index": {"mappings": {"properties": {...}}}}

        - Just the ``mappings`` block::

              {"mappings": {"properties": {...}}}

        - Just the properties block::

              {"properties": {...}}

        - Inline ``{"field": {"type": "keyword"}}`` (shorthand properties).
        """
        mapping = _unwrap_es_mapping(mapping)
        fields: dict[str, str] = {}
        _flatten_es_mapping(mapping.get("properties", {}), prefix="", out=fields)
        return cls(fields)

    # ------------------------------------------------------------------
    # Field lookup
    # ------------------------------------------------------------------

    def get_field_type(self, dotted_path: str) -> str | None:
        """
        Return the canonical type string for *dotted_path*, or ``None``.

        Wildcard paths (containing ``*``) always return ``None``; callers are
        responsible for deciding whether a wildcard pattern is acceptable.
        """
        if "*" in dotted_path:
            return None
        return self._fields.get(dotted_path)

    def __contains__(self, dotted_path: str) -> bool:
        return dotted_path in self._fields

    def __len__(self) -> int:
        return len(self._fields)

    def __repr__(self) -> str:
        return f"Schema({len(self._fields)} fields)"

    @property
    def fields(self) -> dict[str, str]:
        """Return a copy of the flat ``field → type`` mapping."""
        return dict(self._fields)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _flatten_dict(d: dict[str, Any], prefix: str, out: dict[str, str]) -> None:
    """Recursively flatten a nested/flat/mixed dict to dotted paths."""
    for key, value in d.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            _flatten_dict(value, full_key, out)
        elif isinstance(value, str):
            out[full_key] = value
        else:
            raise ValueError(
                f"Invalid schema value for field {full_key!r}: "
                f"expected str or dict, got {type(value).__name__}"
            )


def _flatten_es_mapping(properties: dict[str, Any], prefix: str, out: dict[str, str]) -> None:
    """Recursively flatten an ES mapping ``properties`` block."""
    for field_name, field_def in properties.items():
        full_key = f"{prefix}.{field_name}" if prefix else field_name
        field_type: str | None = field_def.get("type")
        nested_props: dict | None = field_def.get("properties")

        if nested_props:
            # object / nested — recurse; record the parent type if present
            if field_type:
                out[full_key] = field_type
            _flatten_es_mapping(nested_props, full_key, out)
        elif field_type:
            out[full_key] = field_type
        # Fields with no type and no properties (e.g. dynamic stubs) are skipped.


def _unwrap_es_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    """
    Normalise the various shapes of an ES mapping response to the block that
    contains a ``"properties"`` key.
    """
    # Full GET /<index>/_mapping: {"index-name": {"mappings": {...}}}
    if "mappings" not in mapping and "properties" not in mapping:
        if all(isinstance(v, dict) for v in mapping.values()):
            merged: dict[str, Any] = {"properties": {}}
            for idx_body in mapping.values():
                inner = idx_body.get("mappings", idx_body)
                for k, v in inner.get("properties", {}).items():
                    merged["properties"].setdefault(k, v)
            return merged

    # {"mappings": {"properties": {...}}}
    if "mappings" in mapping:
        return mapping["mappings"]

    # Already the right shape (has "properties" key, or is a raw properties dict)
    return mapping
