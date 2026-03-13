"""
Tests for schema validation (pyesql.schema + pyesql.validator).
"""

import warnings

import pytest

from pyesql import parse
from pyesql.schema import Schema
from pyesql.validator import (
    SchemaValidationError,
    SchemaValidationWarning,
    SchemaValidator,
    ValidationIssue,
)

# ---------------------------------------------------------------------------
# Fixtures / shared helpers
# ---------------------------------------------------------------------------

FLAT_SCHEMA_DICT = {
    "process.pid": "integer",
    "process.name": "keyword",
    "process.args": "keyword",
    "host.name": "keyword",
    "host.ip": "ip",
    "event.category": "keyword",
    "event.duration": "long",
    "@timestamp": "date",
    "bytes": "double",
    "active": "boolean",
    "score": "float",
}

NESTED_SCHEMA_DICT = {
    "process": {
        "pid": "integer",
        "name": "keyword",
        "args": "keyword",
    },
    "host": {
        "name": "keyword",
        "ip": "ip",
    },
    "event": {
        "category": "keyword",
        "duration": "long",
    },
    "@timestamp": "date",
    "bytes": "double",
    "active": "boolean",
    "score": "float",
}

ES_MAPPING = {
    "mappings": {
        "properties": {
            "process": {
                "properties": {
                    "pid": {"type": "integer"},
                    "name": {"type": "keyword"},
                    "args": {"type": "keyword"},
                }
            },
            "host": {
                "properties": {
                    "name": {"type": "keyword"},
                    "ip": {"type": "ip"},
                }
            },
            "event": {
                "properties": {
                    "category": {"type": "keyword"},
                    "duration": {"type": "long"},
                }
            },
            "@timestamp": {"type": "date"},
            "bytes": {"type": "double"},
            "active": {"type": "boolean"},
            "score": {"type": "float"},
        }
    }
}


@pytest.fixture
def schema() -> Schema:
    return Schema.from_dict(FLAT_SCHEMA_DICT)


def validator(s: Schema, on_unknown="error", on_type_mismatch="error") -> SchemaValidator:
    return SchemaValidator(s, on_unknown=on_unknown, on_type_mismatch=on_type_mismatch)


# ---------------------------------------------------------------------------
# Schema.from_dict
# ---------------------------------------------------------------------------


class TestSchemaFromDict:
    def test_flat_input(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        assert s.get_field_type("process.pid") == "integer"
        assert s.get_field_type("host.name") == "keyword"
        assert s.get_field_type("@timestamp") == "date"

    def test_nested_input(self):
        s = Schema.from_dict(NESTED_SCHEMA_DICT)
        assert s.get_field_type("process.pid") == "integer"
        assert s.get_field_type("host.ip") == "ip"
        assert s.get_field_type("event.category") == "keyword"

    def test_flat_and_nested_equivalent(self):
        flat = Schema.from_dict(FLAT_SCHEMA_DICT)
        nested = Schema.from_dict(NESTED_SCHEMA_DICT)
        assert flat.fields == nested.fields

    def test_mixed_flat_and_nested(self):
        s = Schema.from_dict(
            {
                "process.pid": "integer",
                "host": {"name": "keyword"},
            }
        )
        assert s.get_field_type("process.pid") == "integer"
        assert s.get_field_type("host.name") == "keyword"

    def test_unknown_field_returns_none(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        assert s.get_field_type("nonexistent.field") is None

    def test_wildcard_path_returns_none(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        assert s.get_field_type("process.*") is None
        assert s.get_field_type("host.n*") is None

    def test_type_aliases(self):
        s = Schema.from_dict({"name": "string", "count": "int", "flag": "bool"})
        assert s.get_field_type("name") == "keyword"
        assert s.get_field_type("count") == "integer"
        assert s.get_field_type("flag") == "boolean"

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Unknown field type"):
            Schema.from_dict({"field": "notatype"})

    def test_invalid_value_type_raises(self):
        with pytest.raises(ValueError, match="expected str or dict"):
            Schema.from_dict({"field": 42})  # type: ignore[arg-type]

    def test_contains(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        assert "process.pid" in s
        assert "unknown" not in s

    def test_len(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        assert len(s) == len(FLAT_SCHEMA_DICT)

    def test_repr(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        assert "Schema(" in repr(s)

    def test_fields_property_is_copy(self):
        s = Schema.from_dict(FLAT_SCHEMA_DICT)
        copy = s.fields
        copy["injected"] = "keyword"
        assert "injected" not in s


# ---------------------------------------------------------------------------
# Schema.from_elasticsearch_mapping
# ---------------------------------------------------------------------------


class TestSchemaFromESMapping:
    def test_standard_mapping(self):
        s = Schema.from_elasticsearch_mapping(ES_MAPPING)
        assert s.get_field_type("process.pid") == "integer"
        assert s.get_field_type("host.ip") == "ip"
        assert s.get_field_type("@timestamp") == "date"

    def test_just_mappings_block(self):
        s = Schema.from_elasticsearch_mapping(ES_MAPPING["mappings"])
        assert s.get_field_type("process.name") == "keyword"

    def test_just_properties_block(self):
        s = Schema.from_elasticsearch_mapping(ES_MAPPING["mappings"])
        assert s.get_field_type("active") == "boolean"

    def test_full_get_mapping_response(self):
        full_response = {"my-index": ES_MAPPING}
        s = Schema.from_elasticsearch_mapping(full_response)
        assert s.get_field_type("process.pid") == "integer"

    def test_multi_index_mapping_merges(self):
        response = {
            "logs-1": {"mappings": {"properties": {"host.name": {"type": "keyword"}}}},
            "logs-2": {"mappings": {"properties": {"bytes": {"type": "long"}}}},
        }
        s = Schema.from_elasticsearch_mapping(response)
        assert s.get_field_type("host.name") == "keyword"
        assert s.get_field_type("bytes") == "long"

    def test_nested_object_properties(self):
        mapping = {
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "text"},
                    },
                }
            }
        }
        s = Schema.from_elasticsearch_mapping(mapping)
        assert s.get_field_type("user.id") == "keyword"
        assert s.get_field_type("user.name") == "text"
        assert s.get_field_type("user") == "object"

    def test_equivalent_to_from_dict(self):
        from_dict = Schema.from_dict(FLAT_SCHEMA_DICT)
        from_mapping = Schema.from_elasticsearch_mapping(ES_MAPPING)
        assert from_dict.fields == from_mapping.fields


# ---------------------------------------------------------------------------
# SchemaValidator — field existence
# ---------------------------------------------------------------------------


class TestFieldExistence:
    def test_valid_field_no_issues(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE process.pid == 1"))
        assert issues == []

    def test_unknown_field_raises_by_default(self, schema):
        with pytest.raises(SchemaValidationError) as exc_info:
            validator(schema).validate(parse("FROM idx | WHERE nonexistent == 1"))
        assert any("nonexistent" in str(i) for i in exc_info.value.issues)

    def test_unknown_field_warn(self, schema):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            issues = validator(schema, on_unknown="warn").validate(
                parse("FROM idx | WHERE nonexistent == 1")
            )
        assert len(w) == 1
        assert issubclass(w[0].category, SchemaValidationWarning)
        assert len(issues) == 1

    def test_unknown_field_silent(self, schema):
        issues = validator(schema, on_unknown="silent").validate(
            parse("FROM idx | WHERE nonexistent == 1")
        )
        assert issues == []

    def test_multiple_unknown_fields_all_reported(self, schema):
        with pytest.raises(SchemaValidationError) as exc_info:
            validator(schema).validate(parse("FROM idx | WHERE foo == 1 AND bar == 2"))
        fields = {i.field for i in exc_info.value.issues}
        assert "foo" in fields
        assert "bar" in fields

    def test_dotted_field_known(self, schema):
        issues = validator(schema).validate(parse('FROM idx | WHERE host.name == "web-01"'))
        assert issues == []

    def test_dotted_field_unknown(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse('FROM idx | WHERE host.missing == "x"'))

    def test_eval_field_ref_validated(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | EVAL x = unknown_field + 1"))

    def test_eval_valid_field_no_issues(self, schema):
        issues = validator(schema).validate(parse("FROM idx | EVAL x = process.pid + 1"))
        assert issues == []

    def test_stats_by_field_validated(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | STATS count = COUNT(*) BY missing_field"))

    def test_sort_field_validated(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | SORT missing_field DESC"))

    def test_sort_valid_field_no_issues(self, schema):
        issues = validator(schema).validate(parse("FROM idx | SORT @timestamp DESC"))
        assert issues == []


# ---------------------------------------------------------------------------
# SchemaValidator — type compatibility
# ---------------------------------------------------------------------------


class TestTypeCompatibility:
    def test_integer_field_integer_literal_ok(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE process.pid == 42"))
        assert issues == []

    def test_integer_field_string_literal_error(self, schema):
        with pytest.raises(SchemaValidationError) as exc_info:
            validator(schema).validate(parse('FROM idx | WHERE process.pid == "abc"'))
        assert any("process.pid" in str(i) for i in exc_info.value.issues)

    def test_keyword_field_string_literal_ok(self, schema):
        issues = validator(schema).validate(parse('FROM idx | WHERE host.name == "web-01"'))
        assert issues == []

    def test_keyword_field_integer_literal_error(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | WHERE host.name == 42"))

    def test_boolean_field_boolean_literal_ok(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE active == true"))
        assert issues == []

    def test_boolean_field_integer_literal_error(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | WHERE active == 1"))

    def test_date_field_string_literal_ok(self, schema):
        issues = validator(schema).validate(parse('FROM idx | WHERE @timestamp == "2024-01-01"'))
        assert issues == []

    def test_ip_field_string_literal_ok(self, schema):
        issues = validator(schema).validate(parse('FROM idx | WHERE host.ip == "192.168.1.1"'))
        assert issues == []

    def test_null_literal_always_ok(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE process.pid == null"))
        assert issues == []

    def test_float_field_decimal_literal_ok(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE score == 3.14"))
        assert issues == []

    def test_float_field_integer_literal_ok(self, schema):
        # integer literal is valid for float field (implicit coercion)
        issues = validator(schema).validate(parse("FROM idx | WHERE score == 3"))
        assert issues == []

    def test_type_mismatch_warn(self, schema):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            validator(schema, on_type_mismatch="warn").validate(
                parse('FROM idx | WHERE process.pid == "abc"')
            )
        assert len(w) == 1
        assert issubclass(w[0].category, SchemaValidationWarning)

    def test_type_mismatch_silent(self, schema):
        issues = validator(schema, on_type_mismatch="silent").validate(
            parse('FROM idx | WHERE process.pid == "abc"')
        )
        assert issues == []

    def test_reversed_comparison(self, schema):
        # "abc" == process.pid — literal on the left
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse('FROM idx | WHERE "abc" == process.pid'))

    def test_in_list_type_check(self, schema):
        # process.pid IN (1, 2, 3) — ok
        issues = validator(schema).validate(parse("FROM idx | WHERE process.pid IN (1, 2, 3)"))
        assert issues == []

    def test_in_list_type_mismatch(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse('FROM idx | WHERE process.pid IN ("a", "b")'))

    def test_in_list_mixed_valid_null(self, schema):
        # null in an IN list is valid for any type
        issues = validator(schema).validate(parse("FROM idx | WHERE process.pid IN (1, null)"))
        assert issues == []


# ---------------------------------------------------------------------------
# SchemaValidator — LIKE / RLIKE
# ---------------------------------------------------------------------------


class TestLikeRlike:
    def test_like_on_keyword_ok(self, schema):
        issues = validator(schema).validate(parse('FROM idx | WHERE host.name LIKE "web-*"'))
        assert issues == []

    def test_like_on_integer_field_type_mismatch(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema, on_type_mismatch="error").validate(
                parse('FROM idx | WHERE process.pid LIKE "1*"')
            )

    def test_rlike_on_keyword_ok(self, schema):
        issues = validator(schema).validate(parse('FROM idx | WHERE host.name RLIKE "web-.*"'))
        assert issues == []

    def test_rlike_on_integer_field_type_mismatch(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema, on_type_mismatch="error").validate(
                parse('FROM idx | WHERE process.pid RLIKE "\\d+"')
            )

    def test_like_type_mismatch_warn(self, schema):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            validator(schema, on_type_mismatch="warn").validate(
                parse('FROM idx | WHERE process.pid LIKE "1*"')
            )
        assert any(issubclass(x.category, SchemaValidationWarning) for x in w)


# ---------------------------------------------------------------------------
# SchemaValidator — KEEP / DROP / RENAME
# ---------------------------------------------------------------------------


class TestStructuralCommands:
    def test_keep_valid_field(self, schema):
        issues = validator(schema).validate(parse("FROM idx | KEEP process.pid, host.name"))
        assert issues == []

    def test_keep_unknown_field_error(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | KEEP nonexistent"))

    def test_keep_wildcard_skipped(self, schema):
        issues = validator(schema).validate(parse("FROM idx | KEEP process.*"))
        assert issues == []

    def test_drop_valid_field(self, schema):
        issues = validator(schema).validate(parse("FROM idx | DROP @timestamp"))
        assert issues == []

    def test_drop_unknown_field_error(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | DROP nonexistent"))

    def test_rename_valid_source(self, schema):
        issues = validator(schema).validate(parse("FROM idx | RENAME host.name AS hostname"))
        assert issues == []

    def test_rename_unknown_source_error(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | RENAME ghost_field AS new_name"))

    def test_rename_new_name_not_checked(self, schema):
        # new_name doesn't need to exist in the schema
        issues = validator(schema).validate(parse("FROM idx | RENAME host.name AS brand_new_field"))
        assert issues == []


# ---------------------------------------------------------------------------
# SchemaValidator — IS NULL / IS NOT NULL
# ---------------------------------------------------------------------------


class TestIsNull:
    def test_is_null_any_type_ok(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE process.pid IS NULL"))
        assert issues == []

    def test_is_not_null_any_type_ok(self, schema):
        issues = validator(schema).validate(parse("FROM idx | WHERE host.name IS NOT NULL"))
        assert issues == []

    def test_is_null_unknown_field_error(self, schema):
        with pytest.raises(SchemaValidationError):
            validator(schema).validate(parse("FROM idx | WHERE unknown_field IS NULL"))


# ---------------------------------------------------------------------------
# parse() schema integration
# ---------------------------------------------------------------------------


class TestParseIntegration:
    def test_parse_no_schema_no_validation(self):
        # Should not raise even though there's nothing in a schema
        q = parse("FROM idx | WHERE any_field == 1")
        assert q is not None

    def test_parse_with_valid_schema(self, schema):
        q = parse(
            "FROM idx | WHERE process.pid == 1",
            schema=schema,
        )
        assert q is not None

    def test_parse_with_schema_unknown_field_raises(self, schema):
        with pytest.raises(SchemaValidationError):
            parse("FROM idx | WHERE ghost == 1", schema=schema)

    def test_parse_with_schema_type_mismatch_raises(self, schema):
        with pytest.raises(SchemaValidationError):
            parse('FROM idx | WHERE process.pid == "abc"', schema=schema)

    def test_parse_with_schema_on_unknown_warn(self, schema):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            q = parse(
                "FROM idx | WHERE ghost == 1",
                schema=schema,
                on_unknown="warn",
            )
        assert q is not None
        assert any(issubclass(x.category, SchemaValidationWarning) for x in w)

    def test_parse_with_schema_on_unknown_silent(self, schema):
        q = parse(
            "FROM idx | WHERE ghost == 1",
            schema=schema,
            on_unknown="silent",
        )
        assert q is not None

    def test_full_pipeline_valid(self, schema):
        q = parse(
            "FROM idx "
            '| WHERE process.pid > 0 AND host.name == "web" '
            "| STATS count = COUNT(*) BY host.name "
            "| SORT count DESC "
            "| LIMIT 10",
            schema=schema,
        )
        assert q is not None

    def test_full_pipeline_mixed_errors(self, schema):
        with pytest.raises(SchemaValidationError) as exc_info:
            parse(
                'FROM idx | WHERE ghost_field == "x" AND process.pid == "not_an_int"',
                schema=schema,
            )
        fields = {i.field for i in exc_info.value.issues}
        assert "ghost_field" in fields
        assert "process.pid" in fields


# ---------------------------------------------------------------------------
# ValidationIssue
# ---------------------------------------------------------------------------


class TestValidationIssue:
    def test_str_with_field(self):
        issue = ValidationIssue(message="test error", field="my.field")
        assert "my.field" in str(issue)
        assert "test error" in str(issue)

    def test_str_without_field(self):
        issue = ValidationIssue(message="generic error")
        assert str(issue) == "generic error"

    def test_error_issues_exposed_on_exception(self, schema):
        with pytest.raises(SchemaValidationError) as exc_info:
            validator(schema).validate(parse("FROM idx | WHERE bad1 == 1 AND bad2 == 2"))
        assert len(exc_info.value.issues) == 2
        assert all(isinstance(i, ValidationIssue) for i in exc_info.value.issues)
